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
	"time"

	"github.com/dustin/go-humanize"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/query/summary"
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
	queryType structs.QueryType
	DataProcessor
	chain []*DataProcessor // This shouldn't be modified after initialization.
	qid   uint64
}

func (qp *QueryProcessor) Cleanup() {
	for _, dp := range qp.chain {
		dp.processor.Cleanup()
	}
}

func NewQueryProcessor(firstAgg *structs.QueryAggregators, queryInfo *query.QueryInformation,
	querySummary *summary.QuerySummary) (*QueryProcessor, error) {

	startTime := time.Now()
	sortMode := recentFirst // TODO: compute this from the query.
	searcher, err := NewSearcher(queryInfo, querySummary, sortMode, startTime)
	if err != nil {
		return nil, utils.TeeErrorf("NewQueryProcessor: cannot make searcher; err=%v", err)
	}

	firstProcessorAgg := firstAgg

	_, queryType := query.GetNodeAndQueryTypes(&structs.SearchNode{}, firstAgg)

	if queryType != structs.RRCCmd {
		// If query Type is GroupByCmd/SegmentStatsCmd, this agg must be a Stats Agg and will be processed by the searcher.
		if !firstAgg.HasStatsBlock() {
			return nil, utils.TeeErrorf("NewQueryProcessor: is not a RRCCmd, but first agg is not a stats agg. qType=%v", queryType)
		}

		// skip the first agg
		firstProcessorAgg = firstProcessorAgg.Next
	}

	dataProcessors := make([]*DataProcessor, 0)
	for curAgg := firstProcessorAgg; curAgg != nil; curAgg = curAgg.Next {
		dataProcessor := asDataProcessor(curAgg)
		if dataProcessor == nil {
			break
		}
		dataProcessor.qid = searcher.qid
		dataProcessors = append(dataProcessors, dataProcessor)
	}

	// Hook up the streams (searcher -> dataProcessors[0] -> ... -> dataProcessors[n-1]).
	if len(dataProcessors) > 0 && !dataProcessors[0].IsDataGenerator() {
		dataProcessors[0].streams = append(dataProcessors[0].streams, NewCachedStream(searcher))
	}
	for i := 1; i < len(dataProcessors); i++ {
		dataProcessors[i].streams = append(dataProcessors[i].streams, NewCachedStream(dataProcessors[i-1]))
	}

	var lastStreamer streamer = searcher
	if len(dataProcessors) > 0 {
		lastStreamer = dataProcessors[len(dataProcessors)-1]
	}

	return newQueryProcessorHelper(queryType, lastStreamer, dataProcessors, queryInfo.GetQid())
}

func newQueryProcessorHelper(queryType structs.QueryType, input streamer,
	chain []*DataProcessor, qid uint64) (*QueryProcessor, error) {

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
		queryType:     queryType,
		DataProcessor: *headDP,
		chain:         chain,
		qid:           qid,
	}, nil
}

func asDataProcessor(queryAgg *structs.QueryAggregators) *DataProcessor {
	if queryAgg == nil {
		return nil
	}

	if queryAgg.BinExpr != nil {
		return NewBinDP(queryAgg.BinExpr)
	} else if queryAgg.DedupExpr != nil {
		return NewDedupDP(queryAgg.DedupExpr)
	} else if queryAgg.EvalExpr != nil {
		return NewEvalDP(queryAgg.EvalExpr)
	} else if queryAgg.FieldsExpr != nil {
		return NewFieldsDP(queryAgg.FieldsExpr)
	} else if queryAgg.RenameExp != nil {
		return NewRenameDP(queryAgg.RenameExp)
	} else if queryAgg.FillNullExpr != nil {
		return NewFillnullDP(queryAgg.FillNullExpr)
	} else if queryAgg.GentimesExpr != nil {
		return NewGentimesDP(queryAgg.GentimesExpr)
	} else if queryAgg.HeadExpr != nil {
		return NewHeadDP(queryAgg.HeadExpr)
	} else if queryAgg.MakeMVExpr != nil {
		return NewMakemvDP(queryAgg.MakeMVExpr)
	} else if queryAgg.RareExpr != nil {
		return NewRareDP(queryAgg.RareExpr)
	} else if queryAgg.RegexExpr != nil {
		return NewRegexDP(queryAgg.RegexExpr)
	} else if queryAgg.RexExpr != nil {
		return NewRexDP(queryAgg.RexExpr)
	} else if queryAgg.SortExpr != nil {
		return NewSortDP(queryAgg.SortExpr)
	} else if queryAgg.GroupByRequest != nil {
		queryAgg.StatsExpr = &structs.StatsExpr{GroupByRequest: queryAgg.GroupByRequest}
		return NewStatsDP(queryAgg.StatsExpr)
	} else if queryAgg.MeasureOperations != nil {
		queryAgg.StatsExpr = &structs.StatsExpr{MeasureOperations: queryAgg.MeasureOperations}
		return NewStatsDP(queryAgg.StatsExpr)
	} else if queryAgg.StatsExpr != nil {
		return NewStatsDP(queryAgg.StatsExpr)
	} else if queryAgg.StreamstatsExpr != nil {
		return NewStreamstatsDP(queryAgg.StreamstatsExpr)
	} else if queryAgg.TailExpr != nil {
		return NewTailDP(queryAgg.TailExpr)
	} else if queryAgg.TimechartExpr != nil {
		return NewTimechartDP(queryAgg.TimechartExpr)
	} else if queryAgg.TopExpr != nil {
		return NewTopDP(queryAgg.TopExpr)
	} else if queryAgg.TransactionExpr != nil {
		return NewTransactionDP(queryAgg.TransactionExpr)
	} else if queryAgg.WhereExpr != nil {
		return NewWhereDP(queryAgg.WhereExpr)
	} else {
		return nil
	}
}

func (qp *QueryProcessor) GetFullResult() (*structs.PipeSearchResponseOuter, error) {
	finalIQR, err := qp.DataProcessor.Fetch()
	if err != nil && err != io.EOF {
		return nil, utils.TeeErrorf("GetFullResult: failed initial fetch; err=%v", err)
	}

	if finalIQR == nil {
		finalIQR = iqr.NewIQR(qp.qid)
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

	return finalIQR.AsResult(qp.queryType)
}

// Usage:
// 1. Make channels for updates and the final result.
// 2. Call GetStreamedResult as a goroutine.
// 3. Read from the update channel and the final result channel.
//
// Once the final result is sent, no more updates will be sent.
func (qp *QueryProcessor) GetStreamedResult(stateChan chan *query.QueryStateChanData) error {

	var finalIQR *iqr.IQR
	var err error
	totalRecords := 0

	var iqr *iqr.IQR
	completeResp := &structs.PipeSearchCompleteResponse{
		Qtype: qp.queryType.String(),
	}

	for err != io.EOF {
		iqr, err = qp.DataProcessor.Fetch()
		if err != nil && err != io.EOF {
			return utils.TeeErrorf("GetStreamedResult: failed to fetch; err=%v", err)
		}
		if iqr == nil {
			break
		}
		if finalIQR == nil {
			finalIQR = iqr
		} else {
			appendErr := finalIQR.Append(iqr)
			if appendErr != nil {
				return utils.TeeErrorf("GetStreamedResult: failed to append; err=%v", appendErr)
			}
		}

		if qp.queryType == structs.RRCCmd {
			result, wsErr := iqr.AsWSResult(qp.queryType, false)
			if wsErr != nil {
				return utils.TeeErrorf("GetStreamedResult: failed to convert iqr to result; wsErr: %v", err)
			}
			totalRecords += len(result.Hits.Hits)
			stateChan <- &query.QueryStateChanData{
				StateName:    query.QUERY_UPDATE,
				UpdateWSResp: result,
			}
		}
	}

	if qp.queryType != structs.RRCCmd {
		result, err := finalIQR.AsWSResult(qp.queryType, false)
		if err != nil {
			return utils.TeeErrorf("GetStreamedResult: failed to convert iqr to result; err: %v", err)
		}
		completeResp.MeasureResults = result.MeasureResults
		completeResp.MeasureFunctions = result.MeasureFunctions
		completeResp.GroupByCols = result.GroupByCols
		completeResp.BucketCount = result.BucketCount
	}

	progress, err := query.GetProgress(qp.qid)
	if err != nil {
		return utils.TeeErrorf("GetStreamedResult: failed to get progress; err: %v", err)
	}

	relation := "eq"
	if progress.UnitsSearched < progress.TotalUnits {
		relation = "gte"
	}

	completeResp.TotalMatched = utils.HitsCount{Value: uint64(totalRecords), Relation: relation}
	completeResp.State = query.COMPLETE.String()
	completeResp.TotalEventsSearched = humanize.Comma(int64(progress.TotalRecords))
	completeResp.TotalRRCCount = humanize.Comma(int64(totalRecords))

	stateChan <- &query.QueryStateChanData{
		StateName:      query.COMPLETE,
		CompleteWSResp: completeResp,
	}

	return nil
}
