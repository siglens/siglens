// Copyright (c) 2021-2024 SigScalr, Inc.
//
// This file is part of SigLens Observability Solution
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package processor

import (
	"errors"
	"io"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/utils"
)

type processor interface {
	Process(*iqr.IQR) (*iqr.IQR, error)
	Rewind()
}

type DataProcessor struct {
	streams   []*cachedStream
	less      func(*iqr.Record, *iqr.Record) bool
	processor processor

	inputOrderMatters bool
	isPermutingCmd    bool // This command may change the order of input.
	isBottleneckCmd   bool // This command must see all input before yielding any output.
	isTwoPassCmd      bool // A subset of bottleneck commands.
	finishedFirstPass bool // Only used for two-pass commands.
}

func (dp *DataProcessor) DoesInputOrderMatter() bool {
	return dp.inputOrderMatters
}

func (dp *DataProcessor) IsPermutingCmd() bool {
	return dp.isPermutingCmd
}

func (dp *DataProcessor) IsBottleneckCmd() bool {
	return dp.isBottleneckCmd
}

func (dp *DataProcessor) IsTwoPassCmd() bool {
	return dp.isTwoPassCmd
}

// Rewind sets up this DataProcessor to read the input streams from the
// beginning; however, it doesn't fully reset it to its initial state. For
// example, a two-pass command that finishes its first pass should remember
// whatever state information it got from the first pass.
func (dp *DataProcessor) Rewind() {
	for _, stream := range dp.streams {
		stream.Rewind()
	}

	dp.processor.Rewind()
}

func (dp *DataProcessor) Fetch() (*iqr.IQR, error) {
	var output *iqr.IQR

	for {
		gotEOF := false
		input, err := dp.getStreamInput()
		if err == io.EOF {
			gotEOF = true
		} else if err != nil {
			return nil, utils.TeeErrorf("DP.Fetch: failed to fetch input: %v", err)
		}

		output, err = dp.processor.Process(input)
		if err == io.EOF {
			gotEOF = true
		} else if err != nil {
			return nil, utils.TeeErrorf("DP.Fetch: failed to process input: %v", err)
		}

		if gotEOF {
			if dp.isTwoPassCmd && !dp.finishedFirstPass {
				dp.finishedFirstPass = true
				dp.Rewind()
				continue
			}

			return output, io.EOF
		} else if output != nil {
			if !dp.isBottleneckCmd || (dp.isTwoPassCmd && dp.finishedFirstPass) {
				return output, nil
			}
		}
	}
}

func (dp *DataProcessor) getStreamInput() (*iqr.IQR, error) {
	switch len(dp.streams) {
	case 0:
		return nil, errors.New("no streams")
	case 1:
		return dp.streams[0].Fetch()
	default:
		iqrs, streamIndices, err := dp.fetchFromAllStreamsWithData()
		if err != nil {
			return nil, utils.TeeErrorf("DP.getStreamInput: failed to fetch from all streams: %v", err)
		}

		if len(iqrs) == 0 {
			return nil, io.EOF
		}

		iqr, exhaustedIQRIndex, err := iqr.MergeIQRs(iqrs, dp.less)
		if err != nil && err != io.EOF {
			return nil, utils.TeeErrorf("DP.getStreamInput: failed to merge IQRs: %v", err)
		}

		for i, iqr := range iqrs {
			if i == exhaustedIQRIndex {
				dp.streams[streamIndices[i]].SetUnusedDataFromLastFetch(nil)
			} else {
				// The merging function already discarded whatever records were
				// used from this IQR, so the IQR is in a state that only has
				// unused records.
				dp.streams[streamIndices[i]].SetUnusedDataFromLastFetch(iqr)
			}
		}

		if err == io.EOF {
			return iqr, io.EOF
		}

		return iqr, nil
	}
}

func (dp *DataProcessor) fetchFromAllStreamsWithData() ([]*iqr.IQR, []int, error) {
	iqrs := make([]*iqr.IQR, 0, len(dp.streams))
	streamIndices := make([]int, 0, len(dp.streams))

	for i, stream := range dp.streams {
		if stream.IsExhausted() {
			continue
		}

		iqr, err := stream.Fetch()
		if err != nil && err != io.EOF {
			return nil, nil, utils.TeeErrorf("DP.fetchFromAllStreamsWithData: failed to fetch from stream %d: %v", i, err)
		}

		if iqr == nil {
			if err != io.EOF {
				return nil, nil, utils.TeeErrorf("DP.fetchFromAllStreamsWithData: stream %d returned nil IQR without EOF", i)
			}

			continue
		}

		iqrs = append(iqrs, iqr)
		streamIndices = append(streamIndices, i)
	}

	return iqrs, streamIndices, nil
}
