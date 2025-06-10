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
	"runtime"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/segment/aggregations"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/colusage"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
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
	chain        []*DataProcessor // This shouldn't be modified after initialization.
	qid          uint64
	scrollFrom   uint64
	includeNulls bool
	querySummary *summary.QuerySummary
	queryInfo    *query.QueryInformation
	isLogsQuery  bool
	startTime    time.Time
}

func (qp *QueryProcessor) cleanupInputStreamForFirstDP() {
	if len(qp.chain) == 0 {
		return
	}

	qp.chain[0].CleanupInputStreams()
}

func (qp *QueryProcessor) Cleanup() {
	go qp.cleanupInputStreamForFirstDP()

	for _, dp := range qp.chain {
		go dp.Cleanup()
	}

	qp.querySummary.Cleanup()
}

func (qp *QueryProcessor) GetChainedDataProcessors() []*DataProcessor {
	chainedDP := make([]*DataProcessor, len(qp.chain))
	_ = copy(chainedDP, qp.chain)
	return chainedDP
}

func MutateForSearchSorter(queryAgg *structs.QueryAggregators) *structs.SortExpr {
	if queryAgg == nil {
		return nil
	}

	var sorterAgg *structs.QueryAggregators
	var prevAgg *structs.QueryAggregators
	for curAgg := queryAgg; curAgg != nil; curAgg = curAgg.Next {
		if curAgg.SortExpr != nil {
			sorterAgg = curAgg
			break
		}
		prevAgg = curAgg
	}

	if sorterAgg == nil {
		return nil
	}
	if prevAgg != nil {
		prevAgg.Next = nil // Get QueryAggs till the sorterAgg
		defer func() {
			prevAgg.Next = sorterAgg // Restore the original QueryAggs
		}()
		if !canUseSortIndex(queryAgg, sorterAgg) {
			return nil
		}
	}

	// TODO: Replace the sort with a head if the sort is fully handled by the
	// searcher. This is only the case for single-column sorts; for multi-column
	// sorts, the searcher will return results sorted by only first specified
	// column, so we still need the sort processor.
	sortExpr := sorterAgg.SortExpr

	return sortExpr
}

// Note: this has side-effects; see asDataProcessor().
// TODO: remove the side-effects.
func AggsToDataProcessors(firstAgg *structs.QueryAggregators, queryInfo *query.QueryInformation) []*DataProcessor {
	dataProcessors := make([]*DataProcessor, 0)
	for curAgg := firstAgg; curAgg != nil; curAgg = curAgg.Next {
		dataProcessor := asDataProcessor(curAgg, queryInfo)
		if dataProcessor == nil {
			break
		}
		dataProcessors = append(dataProcessors, dataProcessor)
	}

	return dataProcessors
}

func CanParallelSearch(dataProcessors []*DataProcessor) (bool, int) {
	canSplit := false
	for i, dp := range dataProcessors {
		if dp.DoesInputOrderMatter() {
			return false, 0
		}

		if dp.GeneratesData() {
			// TODO: in principle we could parallelize (so delete the early
			// "return false" but don't add an early "return true"), but it's
			// not implemented yet.
			return false, 0
		}

		if dp.IgnoresInputOrder() {
			canSplit = true
		}

		if dp.IsBottleneckCmd() {
			return canSplit, i
		}
	}
	return false, 0
}

func canUseSortIndex(queryAgg *structs.QueryAggregators, sorterAgg *structs.QueryAggregators) bool {
	queryCols := make(map[string]struct{})
	createdCols := make(map[string]struct{})

	colusage.AddQueryCols(queryAgg, queryCols, createdCols)

	for _, sortEle := range sorterAgg.SortExpr.SortEles {
		if _, isCreated := createdCols[sortEle.Field]; isCreated {
			return false
		}
	}

	return true
}

func NewQueryProcessor(firstAgg *structs.QueryAggregators, queryInfo *query.QueryInformation,
	querySummary *summary.QuerySummary, scrollFrom int, includeNulls bool, startTime time.Time, shouldDistribute bool, sizeLimit uint64) (*QueryProcessor, error) {

	if err := validateStreamStatsTimeWindow(firstAgg); err != nil {
		return nil, utils.TeeErrorf("NewQueryProcessor: %v", err)
	}

	firstProcessorAgg := firstAgg

	isLogsQuery := query.IsLogsQuery(firstAgg)
	_, queryType := query.GetNodeAndQueryTypes(&structs.SearchNode{}, firstAgg)
	fullQueryType := query.GetQueryTypeOfFullChain(firstAgg)

	if queryType != structs.RRCCmd {
		// If query Type is GroupByCmd/SegmentStatsCmd, this agg must be a Stats Agg and will be processed by the searcher.
		if !firstAgg.HasStatsBlock() {
			return nil, utils.TeeErrorf("NewQueryProcessor: is not a RRCCmd, but first agg is not a stats agg. qType=%v", queryType)
		}

		if queryType == structs.GroupByCmd {
			// If query Type is GroupByCmd and the StatisticExpr is not nil
			// Then the GroupByRequest will be processed by the searcher and
			// the StatisticExpr should be processed by the next DataProcessor
			// Note: The StatisticExpr will create a GroupByRequest
			if firstAgg.StatisticExpr != nil {
				nextAgg := &structs.QueryAggregators{
					GroupByRequest: firstAgg.GroupByRequest,
					StatisticExpr:  firstAgg.StatisticExpr,
				}
				nextAgg.Next = firstAgg.Next
				nextAgg.StatisticExpr.ExprSplitDone = true
				firstAgg.Next = nextAgg
			}
		}

		// skip the first agg
		firstProcessorAgg = firstProcessorAgg.Next
	}

	sortExpr := MutateForSearchSorter(firstAgg)

	firstDpChain := AggsToDataProcessors(firstProcessorAgg, queryInfo)
	canParallelize, mergeIndex := CanParallelSearch(AggsToDataProcessors(firstProcessorAgg, queryInfo))
	if firstAgg.HasStatsBlock() {
		// There's a different flow when the first agg is stats compared to
		// when when a later agg is stats. At some point we may want to unify
		// these two flows, but for now we can't use parallelism because the
		// first agg gets skipped (and handled in the different flow) when it's
		// stats.
		canParallelize = false
		mergeIndex = 0
	}
	if hooks.GlobalHooks.GetDistributedStreamsHook != nil {
		// TODO: remove this check once we have a way to handle parallelism
		// in this case.
		canParallelize = false
		mergeIndex = 0
	}
	sortMode := recentFirst // TODO: use query to determine recentFirst or recentLast
	if canParallelize {
		// If we can parallelize, we don't need to sort
		sortMode = anyOrder
		sortExpr = nil
	}

	searcher, err := NewSearcher(queryInfo, querySummary, sortMode, sortExpr, startTime)
	if err != nil {
		return nil, utils.TeeErrorf("NewQueryProcessor: cannot make searcher; err=%v", err)
	}

	err = query.InitScrollFrom(searcher.qid, uint64(scrollFrom))
	if err != nil {
		return nil, utils.TeeErrorf("NewQueryProcessor: failed to init scroll from; err=%v", err)
	}

	searcherStream := NewCachedStream(searcher)
	dataProcessorChains := make([][]*DataProcessor, 0)
	parallelism := 1
	if canParallelize {
		parallelism = runtime.GOMAXPROCS(0)
	}

	if canParallelize && firstDpChain[mergeIndex].IsMergeableBottleneckCmd() {
		firstDpChain = utils.Insert(firstDpChain, mergeIndex+1, NewMergeBottleneckDP())
		mergeIndex++
	}

	for i := 0; i < parallelism; i++ {
		var dataProcessors []*DataProcessor
		if i == 0 {
			dataProcessors = firstDpChain
		} else {
			dataProcessors = AggsToDataProcessors(firstProcessorAgg, queryInfo)
			// Merge the chains once parallelism is no longer possible.
			// e.g., if we merge into dp3 and parallelism=3, we eventually want
			// to get to this structure:
			//             dp1 -> dp2 ->\
			//          /                \
			// searcher -> dp1 -> dp2 -> dp3 -> dp4
			//          \                /
			//             dp1 -> dp2 ->/
			dataProcessors = dataProcessors[:mergeIndex]
			mergingDp := firstDpChain[mergeIndex]

			if len(dataProcessors) > 0 {
				// We need this to be a single-threaded stream because we may
				// Fetch() from it in parallel.
				lastStream := NewSingleThreadedStream(dataProcessors[len(dataProcessors)-1])
				mergingDp.streams = append(mergingDp.streams, NewCachedStream(lastStream))
			}
		}

		if mergeIndex > 0 {
			switch dataProcessors[mergeIndex-1].processor.(type) {
			case *statsProcessor:
				// We'll likely want to set this for other stats-like commands
				// as well when we implement parallelizing those. But I'm not
				// sure why this is an option because I'm not sure when we ever
				// need this flag to be false.
				err := dataProcessors[mergeIndex-1].SetStatsAsIqrStatsResults()
				if err != nil {
					return nil, utils.TeeErrorf("NewQueryProcessor: failed to set stats as IQR stats results; err=%v", err)
				}
			default:
				// Do nothing.
			}
		}

		if len(dataProcessors) > 0 && dataProcessors[0].IsDataGenerator() {
			query.InitProgressForRRCCmd(math.MaxUint64, searcher.qid) // TODO: Find a good way to handle data generators for progress
			dataProcessors[0].CheckAndSetQidForDataGenerator(searcher.qid)
			dataProcessors[0].SetLimitForDataGenerator(sutils.QUERY_EARLY_EXIT_LIMIT + uint64(scrollFrom))
		}

		// Hook up the streams (searcher -> dataProcessors[0] -> ... -> dataProcessors[n-1]).
		if len(dataProcessors) > 0 && !dataProcessors[0].IsDataGenerator() {
			dataProcessors[0].streams = append(dataProcessors[0].streams, searcherStream)
		}
		for m := 1; m < len(dataProcessors); m++ {
			var stream Streamer = dataProcessors[m-1]
			if canParallelize && m == mergeIndex {
				stream = NewSingleThreadedStream(stream)
			}

			dataProcessors[m].streams = append(dataProcessors[m].streams, NewCachedStream(stream))
		}

		if hook := hooks.GlobalHooks.GetDistributedStreamsHook; hook != nil {
			chainedDPAsAny, err := hook(dataProcessors, searcher, queryInfo, shouldDistribute)
			if err != nil {
				return nil, utils.TeeErrorf("NewQueryProcessor: GetDistributedStreamsHook failed; err=%v", err)
			}

			chainedDp, ok := chainedDPAsAny.([]*DataProcessor)
			if !ok {
				log.Errorf("NewQueryProcessor: GetDistributedStreamsHook returned invalid type, expected []*DataProcessor, got %T", chainedDPAsAny)
			} else {
				dataProcessors = chainedDp
			}
		}

		dataProcessorChains = append(dataProcessorChains, dataProcessors)
	}

	dataProcessors := dataProcessorChains[0]

	var lastStreamer Streamer = searcher
	if len(dataProcessors) > 0 {
		lastStreamer = dataProcessors[len(dataProcessors)-1]
	}

	// TODO: pass all the dataProcessorChains so cleanup happens properly.
	queryProcessor, err := newQueryProcessorHelper(fullQueryType, lastStreamer,
		dataProcessors, queryInfo.GetQid(), scrollFrom, includeNulls, shouldDistribute, sizeLimit)
	if err != nil {
		return nil, err
	}

	queryProcessor.startTime = startTime
	queryProcessor.querySummary = querySummary
	queryProcessor.queryInfo = queryInfo
	queryProcessor.isLogsQuery = isLogsQuery

	log.Debugf("qid=%v, Created QueryProcessor with: %s", queryInfo.GetQid(), queryProcessor.DataProcessor)

	return queryProcessor, nil
}

func newQueryProcessorHelper(queryType structs.QueryType, input Streamer,
	chain []*DataProcessor, qid uint64, scrollFrom int, includeNulls bool, shoulDistribute bool, sizeLimit uint64) (*QueryProcessor, error) {

	var limit uint64
	switch queryType {
	case structs.RRCCmd:
		defaultLimit := sutils.QUERY_EARLY_EXIT_LIMIT
		if sizeLimit == 0 {
			sizeLimit = defaultLimit
		}
		limit = sizeLimit
	case structs.SegmentStatsCmd, structs.GroupByCmd:
		limit = sutils.QUERY_MAX_BUCKETS
	default:
		return nil, utils.TeeErrorf("newQueryProcessorHelper: invalid query type %v", queryType)
	}

	var fetchDp *DataProcessor

	if len(chain) > 0 {
		fetchDp = chain[len(chain)-1]
	}

	if shoulDistribute {
		headDP := NewHeadDP(&structs.HeadExpr{MaxRows: limit})
		if headDP == nil {
			return nil, utils.TeeErrorf("newQueryProcessorHelper: failed to create head data processor")
		}

		headDP.streams = append(headDP.streams, NewCachedStream(input))

		scrollerDP := NewScrollerDP(uint64(scrollFrom), qid)
		if scrollerDP == nil {
			return nil, utils.TeeErrorf("newQueryProcessorHelper: failed to create scroller data processor")
		}
		scrollerDP.streams = append(scrollerDP.streams, NewCachedStream(headDP))

		fetchDp = scrollerDP
	}

	if fetchDp == nil {
		return nil, utils.TeeErrorf("newQueryProcessorHelper: the last data processor is nil")
	}

	return &QueryProcessor{
		queryType:     queryType,
		DataProcessor: *fetchDp,
		chain:         chain,
		qid:           qid,
		scrollFrom:    uint64(scrollFrom),
		includeNulls:  includeNulls,
	}, nil
}

// Note: this has side-effects for a NewStatisticExprDP.
// TODO: remove the side-effects.
func asDataProcessor(queryAgg *structs.QueryAggregators, queryInfo *query.QueryInformation) *DataProcessor {
	if queryAgg == nil {
		return nil
	}

	if queryAgg.BinExpr != nil {
		return NewBinDP(queryAgg.BinExpr)
	} else if queryAgg.StreamstatsExpr != nil {
		return NewStreamstatsDP(queryAgg.StreamstatsExpr)
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
	} else if queryAgg.MVExpandExpr != nil {
		return NewMVExpandDP(queryAgg.MVExpandExpr)
	} else if queryAgg.StatisticExpr != nil {
		return NewStatisticExprDP(queryAgg, queryInfo.IsDistributed())
	} else if queryAgg.RegexExpr != nil {
		return NewRegexDP(queryAgg.RegexExpr)
	} else if queryAgg.RexExpr != nil {
		return NewRexDP(queryAgg.RexExpr)
	} else if queryAgg.SortExpr != nil {
		return NewSortDP(queryAgg.SortExpr)
	} else if queryAgg.TimechartExpr != nil {
		timechartOptions := &timechartOptions{
			timeChartExpr: queryAgg.TimechartExpr,
			qid:           queryInfo.GetQid(),
			timeRange:     queryInfo.GetTimeRange(),
		}
		return NewTimechartDP(timechartOptions)
	} else if queryAgg.StatsExpr != nil {
		return NewStatsDP(queryAgg.StatsExpr)
	} else if queryAgg.TailExpr != nil {
		return NewTailDP(queryAgg.TailExpr)
	} else if queryAgg.TransactionExpr != nil {
		return NewTransactionDP(queryAgg.TransactionExpr)
	} else if queryAgg.WhereExpr != nil {
		return NewWhereDP(queryAgg.WhereExpr)
	} else {
		return nil
	}
}

func (qp *QueryProcessor) GetFullResult() (*structs.PipeSearchResponseOuter, error) {

	var finalIQR *iqr.IQR
	var iqr *iqr.IQR
	var err error
	totalRecords := 0

	defer qp.logQuerySummary()

	for err != io.EOF {
		iqr, err = qp.DataProcessor.Fetch()
		if err != nil && err != io.EOF {
			log.Errorf("GetFullResult: failed to fetch iqr; err=%v", err)
			return nil, utils.WrapErrorf(err, "GetFullResult: failed to fetch; err=%v", err)
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
				return nil, utils.TeeErrorf("GetFullResult: failed to increment records sent, err: %v", err)
			}
			totalRecords += iqr.NumberOfRecords()
		}
	}

	if finalIQR == nil {
		return createEmptyResponse(qp.queryType), nil
	}
	response, err := finalIQR.AsResult(qp.queryType, qp.includeNulls, qp.isLogsQuery)
	if err != nil {
		return nil, utils.TeeErrorf("GetFullResult: failed to get result; err=%v", err)
	}

	qp.querySummary.UpdateQueryTotalTime(time.Since(qp.startTime), response.BucketCount)

	canScrollMore, relation, _, err := qp.getStatusParams(uint64(totalRecords))
	if err != nil {
		return nil, utils.TeeErrorf("GetFullResult: failed to get status params; err=%v", err)
	}
	if qp.queryType == structs.RRCCmd {
		response.Hits.TotalMatched = utils.HitsCount{Value: uint64(totalRecords), Relation: relation}
		response.CanScrollMore = canScrollMore
	}

	return response, nil
}

// Usage:
// 1. Make a channel to receive updates.
// 2. Call GetStreamedResult as a goroutine.
// 3. Poll the channel.
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

	defer qp.logQuerySummary()

	for err != io.EOF {
		iqr, err = qp.DataProcessor.Fetch()
		if err != nil && err != io.EOF {
			log.Errorf("GetStreamedResult: failed to fetch iqr; err=%v", err)
			return utils.WrapErrorf(err, "GetStreamedResult: failed to fetch iqr, err: %v", err)
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
			result, wsErr := iqr.AsWSResult(qp.queryType, qp.scrollFrom, qp.includeNulls, qp.isLogsQuery)
			if wsErr != nil {
				return utils.TeeErrorf("GetStreamedResult: failed to get WSResult from iqr, wsErr: %v", err)
			}
			stateChan <- &query.QueryStateChanData{
				StateName:       query.QUERY_UPDATE,
				PercentComplete: result.Completion,
				UpdateWSResp:    result,
				Qid:             qp.qid,
			}
		}
	}

	if qp.queryType != structs.RRCCmd {
		result, err := finalIQR.AsWSResult(qp.queryType, qp.scrollFrom, qp.includeNulls, qp.isLogsQuery)
		if err != nil {
			return utils.TeeErrorf("GetStreamedResult: failed to get WSResult from iqr; err: %v", err)
		}
		completeResp.MeasureResults = result.MeasureResults
		completeResp.MeasureFunctions = result.MeasureFunctions
		completeResp.GroupByCols = result.GroupByCols
		completeResp.BucketCount = result.BucketCount
		completeResp.TotalMatched = result.Hits.TotalMatched
	}

	qp.querySummary.UpdateQueryTotalTime(time.Since(qp.startTime), completeResp.BucketCount)

	canScrollMore, relation, progress, err := qp.getStatusParams(uint64(totalRecords))
	if err != nil {
		return utils.TeeErrorf("GetStreamedResult: failed to get status params, err: %v", err)
	}

	if qp.queryType == structs.RRCCmd {
		completeResp.TotalMatched = utils.HitsCount{Value: uint64(progress.RecordsSent), Relation: relation}
	}
	completeResp.State = query.COMPLETE.String()
	completeResp.TotalEventsSearched = humanize.Comma(int64(progress.TotalRecords))
	completeResp.TotalRRCCount = progress.RecordsSent
	completeResp.CanScrollMore = canScrollMore

	stateChan <- &query.QueryStateChanData{
		StateName:      query.COMPLETE,
		CompleteWSResp: completeResp,
		Qid:            qp.qid,
	}

	return nil
}

// Returns whether more data can be scrolled, relation, and the progress.
func (qp *QueryProcessor) getStatusParams(totalRecords uint64) (bool, string, structs.Progress, error) {
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
		if totalRecords == sutils.QUERY_EARLY_EXIT_LIMIT {
			relation = "gte"
			canScrollMore = true
		}
	}

	return canScrollMore, relation, progress, nil
}

func (qp *QueryProcessor) logQuerySummary() {
	qp.querySummary.LogSummaryAndEmitMetrics(qp.qid, qp.queryInfo.GetPqid(), qp.queryInfo.ContainsKibana(), qp.queryInfo.GetOrgId())

	log.Infof("qid=%v, Finished execution in %+v", qp.qid, time.Since(qp.startTime))
}

func createEmptyResponse(queryType structs.QueryType) *structs.PipeSearchResponseOuter {
	response := &structs.PipeSearchResponseOuter{
		Hits: structs.PipeSearchResponse{
			TotalMatched: utils.HitsCount{
				Value:    0,
				Relation: "eq",
			},
			Hits: make([]map[string]interface{}, 0),
		},
		AllPossibleColumns: make([]string, 0),
		Errors:             nil,
		Qtype:              queryType.String(),
		CanScrollMore:      false,
		ColumnsOrder:       make([]string, 0),
	}

	// Add stats-specific fields for GroupBy and SegmentStats queries
	if queryType == structs.GroupByCmd || queryType == structs.SegmentStatsCmd {
		response.MeasureResults = make([]*structs.BucketHolder, 0)
		response.MeasureFunctions = make([]string, 0)
		response.GroupByCols = make([]string, 0)
		response.BucketCount = 0
	}

	return response
}

func validateStreamStatsTimeWindow(firstAgg *structs.QueryAggregators) error {
	hasSort := false
	timeSort := false
	timeSortAsc := true

	for curAgg := firstAgg; curAgg != nil; curAgg = curAgg.Next {
		if curAgg.HasSortBlock() {
			hasSort = true
			timeSort, timeSortAsc = aggregations.CheckIfTimeSort(curAgg)
		}
		if curAgg.HasTail() {
			timeSortAsc = !timeSortAsc
		}

		if curAgg.StreamstatsExpr != nil && curAgg.StreamstatsExpr.TimeWindow != nil {
			// If there's a non-timestamp sort before streamstats, return error
			if hasSort && !timeSort {
				return utils.TeeErrorf("streamstats with time_window requires records to maintain timestamp order")
			}
			curAgg.StreamstatsExpr.TimeSortAsc = timeSortAsc
		}
	}
	return nil
}
