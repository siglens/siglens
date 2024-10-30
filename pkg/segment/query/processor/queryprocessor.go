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
	"fmt"
	"io"
	"math"
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
	chain      []*DataProcessor // This shouldn't be modified after initialization.
	qid        uint64
	scrollFrom uint64
}

func (qp *QueryProcessor) Cleanup() {
	for _, dp := range qp.chain {
		dp.processor.Cleanup()
	}
}

func NewQueryProcessor(firstAgg *structs.QueryAggregators, queryInfo *query.QueryInformation,
	querySummary *summary.QuerySummary, scrollFrom int) (*QueryProcessor, error) {

	startTime := time.Now()
	sortMode := recentFirst // TODO: compute this from the query.
	searcher, err := NewSearcher(queryInfo, querySummary, sortMode, startTime)
	if err != nil {
		return nil, utils.TeeErrorf("NewQueryProcessor: cannot make searcher; err=%v", err)
	}

	err = query.InitScrollFrom(searcher.qid, uint64(scrollFrom))
	if err != nil {
		return nil, utils.TeeErrorf("NewQueryProcessor: failed to init scroll from; err=%v", err)
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
		dataProcessors = append(dataProcessors, dataProcessor)
	}

	if len(dataProcessors) > 0 && dataProcessors[0].IsDataGenerator() {
		query.InitProgressForRRCCmd(math.MaxUint64, searcher.qid) // TODO: Find a good way to handle data generators for progress
		dataProcessors[0].CheckAndSetQidForDataGenerator(searcher.qid)
		dataProcessors[0].SetLimitForDataGenerator(segutils.QUERY_EARLY_EXIT_LIMIT + uint64(scrollFrom))
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

	return newQueryProcessorHelper(queryType, lastStreamer, dataProcessors, queryInfo.GetQid(), scrollFrom)
}

func newQueryProcessorHelper(queryType structs.QueryType, input streamer,
	chain []*DataProcessor, qid uint64, scrollFrom int) (*QueryProcessor, error) {

	var limit uint64
	switch queryType {
	case structs.RRCCmd:
		limit = segutils.QUERY_EARLY_EXIT_LIMIT + uint64(scrollFrom)
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
		scrollFrom:    uint64(scrollFrom),
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
	} else if queryAgg.InputLookupExpr != nil {
		return NewInputLookupDP(queryAgg.InputLookupExpr)
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

	scrollComplete := false
	if qp.scrollFrom == 0 {
		scrollComplete = true
	}
	limit := segutils.QUERY_EARLY_EXIT_LIMIT + qp.scrollFrom

	var finalIQR *iqr.IQR
	var iqr *iqr.IQR
	var err error
	totalRecords := 0

	for err != io.EOF {
		iqr, err = qp.DataProcessor.Fetch()
		if err != nil && err != io.EOF {
			return nil, utils.TeeErrorf("GetFullResult: failed to fetch; err=%v", err)
		}

		if finalIQR == nil {
			finalIQR = iqr
		} else {
			appendErr := finalIQR.Append(iqr)
			if appendErr != nil {
				return nil, utils.TeeErrorf("GetFullResult: failed to append iqr to the finalIQR, err: %v", appendErr)
			}
		}
		if qp.queryType == structs.RRCCmd && iqr.NumberOfRecords() > 0 {
			err := query.IncRecordsSent(qp.qid, uint64(iqr.NumberOfRecords()))
			if err != nil {
				return nil, utils.TeeErrorf("GetStreamedResult: failed to increment records sent, err: %v", err)
			}
			totalRecords += iqr.NumberOfRecords()
		}

		if !scrollComplete && finalIQR.NumberOfRecords() >= int(qp.scrollFrom) {
			scrollComplete = true
			err := finalIQR.Discard(int(qp.scrollFrom))
			if err != nil {
				return nil, utils.TeeErrorf("GetFullResult: failed to discard %v rows, err=%v", int(qp.scrollFrom), err)
			}
		}
	}

	// Number of records in the finalIQR are less than scrollFrom, so discard all the rows.
	if !scrollComplete {
		err := finalIQR.Discard(finalIQR.NumberOfRecords())
		if err != nil {
			return nil, utils.TeeErrorf("GetFullResult: failed to discard all rows, err=%v", err)
		}
	}

	response, err := finalIQR.AsResult(qp.queryType)
	if err != nil {
		return nil, utils.TeeErrorf("GetFullResult: failed to get result; err=%v", err)
	}

	canScrollMore, relation, _, err := qp.getStatusParams(limit, uint64(totalRecords))
	if err != nil {
		return nil, utils.TeeErrorf("GetFullResult: failed to get status params; err=%v", err)
	}
	if qp.queryType == structs.RRCCmd {
		response.Hits.TotalMatched = utils.HitsCount{Value: uint64(finalIQR.NumberOfRecords()), Relation: relation}
		response.CanScrollMore = canScrollMore
	}

	return response, nil
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

	scrollComplete := false
	if qp.scrollFrom == 0 {
		scrollComplete = true
	}

	limit := segutils.QUERY_EARLY_EXIT_LIMIT + qp.scrollFrom

	for err != io.EOF {
		iqr, err = qp.DataProcessor.Fetch()
		if err != nil && err != io.EOF {
			return utils.TeeErrorf("GetStreamedResult: failed to fetch iqr, err: %v", err)
		}
		if iqr == nil {
			break
		}
		if finalIQR == nil {
			finalIQR = iqr
		} else {
			appendErr := finalIQR.Append(iqr)
			if appendErr != nil {
				return utils.TeeErrorf("GetStreamedResult: failed to append iqr to the finalIQR, err: %v", appendErr)
			}
		}

		if qp.queryType == structs.RRCCmd && iqr.NumberOfRecords() > 0 {
			err := query.IncRecordsSent(qp.qid, uint64(iqr.NumberOfRecords()))
			if err != nil {
				return utils.TeeErrorf("GetStreamedResult: failed to increment records sent, err: %v", err)
			}
			totalRecords += iqr.NumberOfRecords()
			if totalRecords < int(qp.scrollFrom) {
				continue
			} else {
				if !scrollComplete {
					scrolledRecords := (totalRecords - int(qp.scrollFrom))
					recordsToDiscard := iqr.NumberOfRecords() - scrolledRecords
					err := iqr.Discard(recordsToDiscard)
					if err != nil {
						return utils.TeeErrorf("GetStreamedResult: failed to discard %v rows in iqr, err: %v", qp.scrollFrom, err)
					}
					scrollComplete = true
				}
			}
			result, wsErr := iqr.AsWSResult(qp.queryType, limit)
			if wsErr != nil {
				return utils.TeeErrorf("GetStreamedResult: failed to get WSResult from iqr, wsErr: %v", err)
			}
			stateChan <- &query.QueryStateChanData{
				StateName:       query.QUERY_UPDATE,
				PercentComplete: result.Completion,
				UpdateWSResp:    result,
			}
		}
	}

	if qp.queryType != structs.RRCCmd {
		result, err := finalIQR.AsWSResult(qp.queryType, limit)
		if err != nil {
			return utils.TeeErrorf("GetStreamedResult: failed to get WSResult from iqr; err: %v", err)
		}
		completeResp.MeasureResults = result.MeasureResults
		completeResp.MeasureFunctions = result.MeasureFunctions
		completeResp.GroupByCols = result.GroupByCols
		completeResp.BucketCount = result.BucketCount
	}

	canScrollMore, relation, progress, err := qp.getStatusParams(limit, uint64(totalRecords))
	if err != nil {
		return utils.TeeErrorf("GetStreamedResult: failed to get status params, err: %v", err)
	}

	completeResp.TotalMatched = utils.HitsCount{Value: uint64(totalRecords), Relation: relation}
	completeResp.State = query.COMPLETE.String()
	completeResp.TotalEventsSearched = humanize.Comma(int64(progress.TotalRecords))
	completeResp.TotalRRCCount = totalRecords
	completeResp.CanScrollMore = canScrollMore

	stateChan <- &query.QueryStateChanData{
		StateName:      query.COMPLETE,
		CompleteWSResp: completeResp,
	}

	return nil
}

// Returns whether more data can be scrolled, relation, and the progress.
func (qp *QueryProcessor) getStatusParams(limit uint64, totalRecords uint64) (bool, string, structs.Progress, error) {
	progress, err := query.GetProgress(qp.qid)
	if err != nil {
		return false, "", structs.Progress{}, fmt.Errorf("getStatusParams: failed to get progress; err: %v", err)
	}

	relation := "eq"
	canScrollMore := false

	if len(qp.chain) > 0 && qp.chain[0].IsDataGenerator() {
		isEOF := qp.chain[0].IsEOFForDataGenerator()
		if isEOF {
			canScrollMore = false
		} else {
			canScrollMore = true
			relation = "gte"
		}
	} else {
		if totalRecords == limit {
			relation = "gte"
			canScrollMore = true
		}
	}

	return canScrollMore, relation, progress, nil
}
