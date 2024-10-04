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
	"fmt"
	"io"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/utils"
)

type streamer interface {
	Fetch() (*iqr.IQR, error)
	Rewind()
}

type processor interface {
	Process(*iqr.IQR) (*iqr.IQR, error)
	Rewind()
}

type DataProcessor struct {
	streams   []streamer
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
	gotEOF := false

	for {
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

			if output == nil {
				return output, io.EOF
			} else {
				return output, nil
			}
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
	}

	// TODO: fetch from all streams and merge them.
	return nil, fmt.Errorf("not implemented")
}
