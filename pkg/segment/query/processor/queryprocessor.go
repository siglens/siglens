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
	"io"

	"github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
)

type QueryType uint8

const (
	InvalidQueryType QueryType = iota
	RecordsQuery
	StatsQuery
)

type QueryProcessor struct {
	DataProcessor
}

func NewQueryProcessor(queryType structs.QueryType, input streamer) (*QueryProcessor, error) {
	var limit uint64
	switch queryType {
	case structs.RRCCmd:
		limit = segutils.QUERY_EARLY_EXIT_LIMIT
	case structs.SegmentStatsCmd, structs.GroupByCmd:
		limit = segutils.QUERY_MAX_BUCKETS
	default:
		return nil, utils.TeeErrorf("NewQueryProcessor: invalid query type %v", queryType)
	}

	headDP := NewHeadDP(utils.NewOptionWithValue(limit))
	if headDP == nil {
		return nil, utils.TeeErrorf("NewQueryProcessor: failed to create head data processor")
	}

	headDP.streams = append(headDP.streams, &cachedStream{input, nil, false})

	return &QueryProcessor{
		DataProcessor: *headDP,
	}, nil
}

func (qp *QueryProcessor) GetFullResult() (*pipesearch.PipeSearchResponseOuter, error) {
	finalIQR, err := qp.DataProcessor.Fetch()
	if err != nil && err != io.EOF {
		return nil, utils.TeeErrorf("GetFullResult: failed initial fetch; err=%v", err)
	}

	var iqr *iqr.IQR
	for err != io.EOF {
		iqr, err = qp.DataProcessor.Fetch()
		if err != nil && err != io.EOF {
			return nil, utils.TeeErrorf("GetFullResult: failed to fetch; err=%v", err)
		}

		appendErr := finalIQR.Append(iqr)
		if appendErr != nil {
			return nil, utils.TeeErrorf("GetFullResult: failed to append; err=%v", appendErr)
		}
	}

	return finalIQR.AsResult()
}

// Usage:
// 1. Make channels for updates and the final result.
// 2. Call GetStreamedResult as a goroutine.
// 3. Read from the update channel and the final result channel.
//
// Once the final result is sent, no more updates will be sent.
func (qp *QueryProcessor) GetStreamedResult(updateChan chan *pipesearch.PipeSearchWSUpdateResponse,
	completeChan chan *pipesearch.PipeSearchCompleteResponse) {

	panic("not implemented") // TODO
}
