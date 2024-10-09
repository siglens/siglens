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

	"github.com/siglens/siglens/pkg/segment/query"
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
	chain []*DataProcessor // This shouldn't be modified after initialization.
}

func (qp *QueryProcessor) Cleanup() {
	for _, dp := range qp.chain {
		dp.processor.Cleanup()
	}
}

func NewQueryProcessor(searchNode *structs.ASTNode, firstAgg *structs.QueryAggregators,
	qid uint64) (*QueryProcessor, error) {

	searcher := &searchStream{
		qid:  qid,
		node: searchNode,
	}

	dataProcessors := make([]*DataProcessor, 0)
	for curAgg := firstAgg; curAgg != nil; curAgg = curAgg.Next {
		dataProcessor, err := asDataProcessor(curAgg)
		if err != nil {
			return nil, utils.TeeErrorf("NewQueryProcessor: cannot make data processor for %+v; err=%v",
				curAgg, err)
		}

		dataProcessors = append(dataProcessors, dataProcessor)
	}

	// Hook up the streams (searcher -> dataProcessors[0] -> ... -> dataProcessors[n-1]).
	if len(dataProcessors) > 0 {
		dataProcessors[0].streams = append(dataProcessors[0].streams, NewCachedStream(searcher))
	}
	for i := 1; i < len(dataProcessors); i++ {
		dataProcessors[i].streams = append(dataProcessors[i].streams, NewCachedStream(dataProcessors[i-1]))
	}

	var lastStreamer streamer = searcher
	if len(dataProcessors) > 0 {
		lastStreamer = dataProcessors[len(dataProcessors)-1]
	}

	_, queryType := query.GetNodeAndQueryTypes(&structs.SearchNode{}, firstAgg)

	return newQueryProcessorHelper(queryType, lastStreamer, dataProcessors)
}

func newQueryProcessorHelper(queryType structs.QueryType, input streamer,
	chain []*DataProcessor) (*QueryProcessor, error) {

	var limit uint64
	switch queryType {
	case structs.RRCCmd:
		limit = segutils.QUERY_EARLY_EXIT_LIMIT
	case structs.SegmentStatsCmd, structs.GroupByCmd:
		limit = segutils.QUERY_MAX_BUCKETS
	default:
		return nil, utils.TeeErrorf("newQueryProcessorHelper: invalid query type %v", queryType)
	}

	headDP := NewHeadDP(&structs.HeadExpr{MaxRows: limit})
	if headDP == nil {
		return nil, utils.TeeErrorf("newQueryProcessorHelper: failed to create head data processor")
	}

	headDP.streams = append(headDP.streams, &cachedStream{input, nil, false})

	return &QueryProcessor{
		DataProcessor: *headDP,
		chain:         chain,
	}, nil
}

func asDataProcessor(queryAgg *structs.QueryAggregators) (*DataProcessor, error) {
	if queryAgg == nil {
		return nil, utils.TeeErrorf("asDataProcessor: got nil query aggregator")
	}

	if queryAgg.BinExpr != nil {
		return NewBinDP(queryAgg.BinExpr), nil
	} else if queryAgg.DedupExpr != nil {
		return NewDedupDP(queryAgg.DedupExpr), nil
	} else if queryAgg.EvalExpr != nil {
		return NewEvalDP(queryAgg.EvalExpr), nil
	} else if queryAgg.FieldsExpr != nil {
		return NewFieldsDP(queryAgg.FieldsExpr), nil
	} else if queryAgg.FillNullExpr != nil {
		return NewFillnullDP(queryAgg.FillNullExpr), nil
	} else if queryAgg.GentimesExpr != nil {
		return NewGentimesDP(queryAgg.GentimesExpr), nil
	} else if queryAgg.HeadExpr != nil {
		return NewHeadDP(queryAgg.HeadExpr), nil
	} else if queryAgg.MakeMVExpr != nil {
		return NewMakemvDP(queryAgg.MakeMVExpr), nil
	} else if queryAgg.RareExpr != nil {
		return NewRareDP(queryAgg.RareExpr), nil
	} else if queryAgg.RegexExpr != nil {
		return NewRegexDP(queryAgg.RegexExpr), nil
	} else if queryAgg.RexExpr != nil {
		return NewRexDP(queryAgg.RexExpr), nil
	} else if queryAgg.SortExpr != nil {
		return NewSortDP(queryAgg.SortExpr), nil
	} else if queryAgg.StatsExpr != nil {
		return NewStatsDP(queryAgg.StatsExpr), nil
	} else if queryAgg.StreamstatsExpr != nil {
		return NewStreamstatsDP(queryAgg.StreamstatsExpr), nil
	} else if queryAgg.TailExpr != nil {
		return NewTailDP(queryAgg.TailExpr), nil
	} else if queryAgg.TimechartExpr != nil {
		return NewTimechartDP(queryAgg.TimechartExpr), nil
	} else if queryAgg.TopExpr != nil {
		return NewTopDP(queryAgg.TopExpr), nil
	} else if queryAgg.TransactionExpr != nil {
		return NewTransactionDP(queryAgg.TransactionExpr), nil
	} else if queryAgg.WhereExpr != nil {
		return NewWhereDP(queryAgg.WhereExpr), nil
	} else {
		return nil, utils.TeeErrorf("asDataProcessor: all commands are nil in %+v", queryAgg)
	}
}

func (qp *QueryProcessor) GetFullResult() (*structs.PipeSearchResponseOuter, error) {
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
func (qp *QueryProcessor) GetStreamedResult(updateChan chan *structs.PipeSearchWSUpdateResponse,
	completeChan chan *structs.PipeSearchCompleteResponse) {

	panic("not implemented") // TODO
}
