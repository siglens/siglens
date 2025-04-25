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

package query

import (
	"errors"
	"fmt"
	"runtime"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/siglens/siglens/pkg/blob"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/segment/aggregations"
	segmetadata "github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/query/pqs"
	pqsmeta "github.com/siglens/siglens/pkg/segment/query/pqs/meta"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/reader/segread"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/search"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

const QUERY_INFO_REFRESH_LOOP_SECS = 300

var ExtractKibanaRequestsFn func([]string, uint64) map[string]*structs.SegmentSearchRequest

// Inits metadata layer and search limiter
func InitQueryNode(getMyIds func() []int64, extractKibanaRequestsFn func([]string, uint64) map[string]*structs.SegmentSearchRequest) error {
	ExtractKibanaRequestsFn = extractKibanaRequestsFn
	ticker := time.NewTicker(30 * time.Second)
	done := make(chan bool)
	startTime := time.Now()
	go func() {
		for {
			select {
			case <-done:
				log.Infof("Query server has successfully been initialized in %+v", time.Since(startTime))
				return
			case <-ticker.C:
				log.Infof("Query server is still being initialized with metadata. Total elapsed time %+v", time.Since(startTime))
			}
		}
	}()

	pqsmeta.InitPqsMeta()
	initMetadataRefresh()
	initGlobalMetadataRefresh(getMyIds)
	go initSyncSegMetaForAllIds(getMyIds, hooks.GlobalHooks.GetAllSegmentsHook)
	go runQueryInfoRefreshLoop(getMyIds)

	// Init specific writer components for kibana requests
	if !config.IsIngestNode() {
		writer.HostnameDir()
		writer.InitKibanaInternalData()
	}
	ticker.Stop()
	done <- true

	return nil
}

// ingest only nodes should call this to be fetching remote pqs information
func InitQueryInfoRefresh(getMyIds func() []int64) {
	go runQueryInfoRefreshLoop(getMyIds)
}

func InitQueryMetrics() {
	go queryMetricsLooper()
}

func queryMetricsLooper() {
	for {
		time.Sleep(1 * time.Minute)
		go func() {
			instrumentation.SetTotalSegmentMicroindexCount(segmetadata.GetTotalSMICount())
		}()
	}
}

func initSyncSegMetaForAllIds(getMyIds func() []int64, allSegmentsHook func() (map[string]struct{}, error)) {
	var allSegKeys map[string]struct{}
	var err error

	if allSegmentsHook != nil {
		allSegKeys, err = allSegmentsHook()
		if err != nil {
			log.Errorf("initSyncSegMetaForAllIds: Error in getting all SegKeys, err:%v", err)
			return
		}
	}

	totalAddedSmiCount := 0

	for _, myId := range getMyIds() {
		totalAddedSmiCount += syncSegMetaWithSegFullMeta(myId, allSegKeys)
	}

	if totalAddedSmiCount > 0 {
		err := blob.UploadIngestNodeDir()
		if err != nil {
			log.Errorf("initSyncSegMetaForAllIds: Error in uploading ingest node dir, err:%v", err)
		}
	}
}

func GetNodeAndQueryTypes(sNode *structs.SearchNode, aggs *structs.QueryAggregators) (structs.SearchNodeType, structs.QueryType) {
	return sNode.NodeType, getQueryTypeOfCurrentAgg(aggs)
}

func GetQueryTypeOfFullChain(aggs *structs.QueryAggregators) structs.QueryType {
	for agg := aggs; agg != nil; agg = agg.Next {
		switch qType := getQueryTypeOfCurrentAgg(agg); qType {
		case structs.SegmentStatsCmd, structs.GroupByCmd, structs.InvalidCmd:
			return qType
		case structs.RRCCmd:
			// Do nothing.
		}
	}

	return structs.RRCCmd
}

func getQueryTypeOfCurrentAgg(aggs *structs.QueryAggregators) structs.QueryType {
	if aggs == nil {
		return structs.RRCCmd
	}

	if aggs.GroupByRequest != nil && aggs.StreamStatsOptions == nil {
		if aggs.GroupByRequest.MeasureOperations != nil {
			if aggs.GroupByRequest.GroupByColumns == nil {
				return structs.SegmentStatsCmd
			} else {
				return structs.GroupByCmd
			}
		}
	}
	if aggs.MeasureOperations != nil && aggs.GroupByRequest == nil && aggs.StreamStatsOptions == nil {
		return structs.SegmentStatsCmd
	}

	return structs.RRCCmd
}

func IsLogsQuery(firstAgg *structs.QueryAggregators) bool {
	for curAgg := firstAgg; curAgg != nil; curAgg = curAgg.Next {
		_, qType := GetNodeAndQueryTypes(&structs.SearchNode{}, curAgg)
		if qType != structs.RRCCmd {
			return false
		}
		if firstAgg.IndexName == "traces" || firstAgg.IndexName == "service-dependency" || firstAgg.IndexName == "red-traces" {
			return false
		}
	}
	return true
}

func ApplyVectorArithmetic(aggs *structs.QueryAggregators, qid uint64) *structs.NodeResult {
	nodeRes := &structs.NodeResult{}

	if aggs.PipeCommandType != structs.VectorArithmeticExprType {
		nodeRes.ErrList = []error{errors.New("the query does not have a vector arithmetic expression")}
		return nodeRes
	}

	if aggs.VectorArithmeticExpr == nil {
		nodeRes.ErrList = []error{errors.New("the query does not have a vector arithmetic expression")}
		return nodeRes
	}

	vectorExpr := aggs.VectorArithmeticExpr

	result, err := vectorExpr.Evaluate(map[string]segutils.CValueEnclosure{})
	if err != nil {
		nodeRes.ErrList = []error{err}
		return nodeRes
	}

	nodeRes.VectorResultValue = result
	nodeRes.Qtype = "VectorArithmeticExprType"
	return nodeRes
}

func GenerateEvents(aggs *structs.QueryAggregators, qid uint64) *structs.NodeResult {

	if aggs.GenerateEvent == nil {
		return nil
	}

	nodeRes := &structs.NodeResult{}

	aggs.GenerateEvent.GeneratedRecords = make(map[string]map[string]interface{})
	aggs.GenerateEvent.GeneratedRecordsIndex = make(map[string]int)
	aggs.GenerateEvent.GeneratedColsIndex = make(map[string]int)
	aggs.GenerateEvent.GeneratedCols = make(map[string]bool)

	if aggs.GenerateEvent.GenTimes != nil {
		err := aggregations.PerformGenTimes(aggs)
		if err != nil {
			log.Errorf("qid=%d, Failed to generate times! Error: %v", qid, err)
		}
	} else if aggs.GenerateEvent.InputLookup != nil {
		err := aggregations.PerformInputLookup(aggs)
		if err != nil {
			log.Errorf("qid=%d, Failed to perform input lookup! Error: %v", qid, err)
		}
	}

	if aggs.HasGeneratedEventsWithoutSearch() {
		err := setTotalSegmentsToSearch(qid, 1)
		if err != nil {
			log.Errorf("qid=%d, Failed to set total segments to search! Error: %v", qid, err)
		}
		SetCurrentSearchResultCount(qid, len(aggs.GenerateEvent.GeneratedRecords))
		err = SetRawSearchFinished(qid)
		if err != nil {
			log.Errorf("qid=%d, Failed to set raw search finished! Error: %v", qid, err)
		}

		// Call this to for processQueryUpdate to be called
		IncrementNumFinishedSegments(1, qid, uint64(len(aggs.GenerateEvent.GeneratedRecords)), 0, "", false, nil)
	}

	return nodeRes
}

func InitQueryInfoAndSummary(searchNode *structs.SearchNode, timeRange *dtu.TimeRange, aggs *structs.QueryAggregators,
	qid uint64, qc *structs.QueryContext, dqid string, segEncToKeyBaseValue uint32) (*QueryInformation, *summary.QuerySummary, string, bool, []string,
	*segresults.SearchResults, int64, error) {

	kibanaIndices := qc.TableInfo.GetKibanaIndices()
	nonKibanaIndices := qc.TableInfo.GetQueryTables()
	containsKibana := false
	if len(kibanaIndices) != 0 {
		containsKibana = true
	}
	querytracker.UpdateQTUsage(nonKibanaIndices, searchNode, aggs, qc.RawQuery)
	parallelismPerFile := int64(runtime.GOMAXPROCS(0) / 2)
	if parallelismPerFile < 1 {
		parallelismPerFile = 1
	}
	_, qType := GetNodeAndQueryTypes(searchNode, aggs)
	querySummary := summary.InitQuerySummary(summary.LOGS, qid)
	pqid := querytracker.GetHashForQuery(searchNode)
	allSegFileResults, err := segresults.InitSearchResults(qc.SizeLimit, aggs, qType, qid)
	if err != nil {
		querySummary.Cleanup()
		log.Errorf("qid=%d, InitQueryInfoAndSummary: Failed to InitSearchResults! error %+v", qid, err)
		return nil, nil, "", false, nil, nil, 0, err
	}

	var dqs DistributedQueryServiceInterface
	if hook := hooks.GlobalHooks.InitDistributedQueryServiceHook; hook != nil {
		result := hook(querySummary, allSegFileResults, dqid, segEncToKeyBaseValue)
		dqs = result.(DistributedQueryServiceInterface)
	} else {
		dqs = InitDistQueryService(querySummary, allSegFileResults, dqid, segEncToKeyBaseValue)
	}

	queryInfo, err := InitQueryInformation(searchNode, aggs, timeRange, qc.TableInfo,
		qc.SizeLimit, parallelismPerFile, qid, dqs, qc.Orgid, qc.Scroll, containsKibana)
	if err != nil {
		querySummary.Cleanup()
		log.Errorf("qid=%d, InitQueryInfoAndSummary: Failed to InitQueryInformation! error %+v", qid, err)
		return nil, nil, "", false, nil, nil, 0, err
	}
	err = AssociateSearchInfoWithQid(qid, allSegFileResults, aggs, dqs, qType, qc.RawQuery)
	if err != nil {
		querySummary.Cleanup()
		log.Errorf("qid=%d, InitQueryInfoAndSummary: Failed to associate search results with qid! Error: %+v", qid, err)
		return nil, nil, "", false, nil, nil, 0, err
	}

	return queryInfo, querySummary, pqid, containsKibana, kibanaIndices, allSegFileResults, parallelismPerFile, nil
}

// TODO: after we move to the new query pipeline, some of these return values
// will not be needed, so they can be removed.
func PrepareToRunQuery(node *structs.ASTNode, timeRange *dtu.TimeRange, aggs *structs.QueryAggregators,
	qid uint64, qc *structs.QueryContext) (*time.Time, *summary.QuerySummary, *QueryInformation,
	string, *structs.SearchNode, *segresults.SearchResults, int64, bool, []string, error) {

	startTime, err := GetQueryStartTime(qid)
	if err != nil {
		// This should never happen
		log.Errorf("qid=%d, PrepareToRunQuery: Failed to get query start time! Error: %+v", qid, err)
		startTime = time.Now()
	}
	searchNode := ConvertASTNodeToSearchNode(node, qid)
	dqid := uuid.New().String()

	queryInfo, querySummary, pqid, containsKibana, kibanaIndices,
		allSegFileResults, parallelismPerFile, err := InitQueryInfoAndSummary(searchNode, timeRange, aggs, qid, qc, dqid, uint32(0))
	if err != nil {
		log.Errorf("qid=%d, PrepareToRunQuery: Failed to init query info and summary! Error: %+v", qid, err)
		return nil, nil, nil, "", nil, nil, 0, false, nil, err
	}

	return &startTime, querySummary, queryInfo, pqid, searchNode,
		allSegFileResults, parallelismPerFile, containsKibana, kibanaIndices, nil
}

func ApplyFilterOperator(node *structs.ASTNode, timeRange *dtu.TimeRange, aggs *structs.QueryAggregators,
	qid uint64, qc *structs.QueryContext) *structs.NodeResult {

	sTime, querySummary, queryInfo, pqid, searchNode, allSegFileResults, parallelismPerFile,
		containsKibana, kibanaIndices, err := PrepareToRunQuery(node, timeRange, aggs, qid, qc)
	if err != nil {
		log.Errorf("qid=%d Failed to prepare to run query! Error: %+v", qid, err)
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}
	defer querySummary.LogSummaryAndEmitMetrics(qid, pqid, containsKibana, qc.Orgid)

	_, qType := GetNodeAndQueryTypes(searchNode, aggs)

	// Kibana requests will not honor time range sent in the query
	// TODO: distibuted kibana requests?
	applyKibanaFilterOperator(kibanaIndices, allSegFileResults, parallelismPerFile, searchNode,
		qc.SizeLimit, aggs, qid, querySummary)
	switch qType {
	case structs.SegmentStatsCmd:
		return GetNodeResultsForSegmentStatsCmd(queryInfo, *sTime, allSegFileResults, nil, querySummary, qc.Orgid, false)
	case structs.RRCCmd, structs.GroupByCmd:
		bucketLimit := MAX_GRP_BUCKS
		if aggs != nil {
			if aggs.BucketLimit != 0 && aggs.BucketLimit < MAX_GRP_BUCKS {
				bucketLimit = aggs.BucketLimit
			}
			aggs.BucketLimit = bucketLimit
			if aggs.HasGenerateEvent() {
				nodeRes := GenerateEvents(aggs, qid)
				if aggs.HasGeneratedEventsWithoutSearch() {
					return nodeRes
				}
			}
		} else {
			aggs = structs.InitDefaultQueryAggregations()
			aggs.BucketLimit = bucketLimit
			queryInfo.aggs = aggs
		}
		allColsInAggs := aggs.GetAllColsInAggsIfStatsPresent()
		if len(allColsInAggs) > 0 {
			SetAllColsInAggsForQid(qid, allColsInAggs)
		}
		nodeRes := GetNodeResultsForRRCCmd(queryInfo, *sTime, allSegFileResults, querySummary)
		nodeRes.AllColumnsInAggs = allColsInAggs

		return nodeRes
	default:
		err := errors.New("unsupported query type")
		log.Errorf("qid=%d Failed to apply search! error %+v", qid, err)
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}
}

// Base function to apply operators on query segment requests
func GetNodeResultsFromQSRS(sortedQSRSlice []*QuerySegmentRequest, queryInfo *QueryInformation, sTime time.Time,
	allSegFileResults *segresults.SearchResults, querySummary *summary.QuerySummary, returnAggBuckets bool) *structs.NodeResult {
	applyFopAllRequests(sortedQSRSlice, queryInfo, allSegFileResults, querySummary)
	if !returnAggBuckets {
		err := queryInfo.Wait(querySummary)
		if err != nil {
			log.Errorf("qid=%d Failed to wait for all query segment requests to finish! Error: %+v", queryInfo.qid, err)
			return &structs.NodeResult{
				ErrList: []error{err},
			}
		}
	}
	querySummary.UpdateQueryTotalTime(time.Since(sTime), allSegFileResults.GetNumBuckets())
	SetQidAsFinished(queryInfo.qid)
	queryType := GetQueryType(queryInfo.qid)
	bucketLimit := MAX_GRP_BUCKS
	if queryInfo.aggs != nil {
		if queryInfo.aggs.BucketLimit != 0 && queryInfo.aggs.BucketLimit < MAX_GRP_BUCKS {
			bucketLimit = queryInfo.aggs.BucketLimit
		}
	}

	if returnAggBuckets {
		return &structs.NodeResult{
			ErrList:        allSegFileResults.GetAllErrors(),
			Qtype:          queryType.String(),
			GroupByBuckets: allSegFileResults.BlockResults.GroupByAggregation,
			TimeBuckets:    allSegFileResults.BlockResults.TimeAggregation,
		}
	}

	aggMeasureRes, aggMeasureFunctions, aggGroupByCols, _, bucketCount := allSegFileResults.GetGroupyByBuckets(bucketLimit)
	allSegResults := allSegFileResults.GetResults()
	scrollFrom := queryInfo.GetScrollFrom()
	currentSearchResultCount := len(allSegResults) - scrollFrom
	SetCurrentSearchResultCount(queryInfo.qid, currentSearchResultCount)

	return &structs.NodeResult{
		AllRecords:       allSegResults,
		ErrList:          allSegFileResults.GetAllErrors(),
		TotalResults:     allSegFileResults.GetQueryCount(),
		Histogram:        allSegFileResults.GetBucketResults(),
		SegEncToKey:      allSegFileResults.SegEncToKey,
		MeasureResults:   aggMeasureRes,
		MeasureFunctions: aggMeasureFunctions,
		GroupByCols:      aggGroupByCols,
		Qtype:            queryType.String(),
		BucketCount:      bucketCount,
	}
}

func getTotalRecordsToBeSearched(qsrs []*QuerySegmentRequest) uint64 {
	var totalRecsToSearch uint64
	for _, qsr := range qsrs {
		if qsr.sType == structs.RAW_SEARCH || qsr.sType == structs.PQS || qsr.sType == structs.SEGMENT_STATS_SEARCH {
			totalRecsToSearch += segmetadata.GetNumOfSearchedRecordsRotated(qsr.segKey)
		} else {
			totalRecsToSearch += writer.GetNumOfSearchedRecordsUnRotated(qsr.segKey)
		}
	}

	return totalRecsToSearch
}

func GetNodeResultsForRRCCmd(queryInfo *QueryInformation, sTime time.Time, allSegFileResults *segresults.SearchResults,
	querySummary *summary.QuerySummary) *structs.NodeResult {

	sortedQSRSlice, err := GetSortedQSRs(queryInfo, sTime, querySummary)
	if err != nil {
		log.Errorf("qid=%d GetNodeResultsForRRCCmd: Failed to get sorted QSRs! Error: %+v", queryInfo.qid, err)
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}

	return GetNodeResultsFromQSRS(sortedQSRSlice, queryInfo, sTime, allSegFileResults, querySummary, false)
}

func GetSortedQSRs(queryInfo *QueryInformation, sTime time.Time, querySummary *summary.QuerySummary) ([]*QuerySegmentRequest, error) {
	sortedQSRSlice, numRawSearch, distributedQueries, numPQS, err := getAllSegmentsInQuery(queryInfo, sTime)
	if err != nil {
		log.Errorf("qid=%d GetSortedQSRs: Failed to get all segments in query! Error: %+v", queryInfo.qid, err)
		return nil, err
	}
	log.Infof("qid=%d, GetSortedQSRs: Received %+v query segment requests. %+v raw search %+v pqs and %+v distribued query elapsed time: %+v",
		queryInfo.qid, len(sortedQSRSlice), numRawSearch, numPQS, distributedQueries, time.Since(sTime))
	err = setTotalSegmentsToSearch(queryInfo.qid, numRawSearch+numPQS+distributedQueries)
	if err != nil {
		log.Errorf("qid=%d GetSortedQSRs: Failed to set total segments to search! Error: %+v", queryInfo.qid, err)
	}
	totalRecsToBeSearched := getTotalRecordsToBeSearched(sortedQSRSlice)
	err = setTotalRecordsToBeSearched(queryInfo.qid, totalRecsToBeSearched)
	if err != nil {
		log.Errorf("qid=%d GetSortedQSRs: Failed to set total records to search! Error: %+v", queryInfo.qid, err)
	}
	querySummary.UpdateRemainingDistributedQueries(distributedQueries)

	return sortedQSRSlice, nil
}

func GetNodeResultsForSegmentStatsCmd(queryInfo *QueryInformation, sTime time.Time, allSegFileResults *segresults.SearchResults,
	qsrs []*QuerySegmentRequest, querySummary *summary.QuerySummary, orgid int64, getSstMap bool) *structs.NodeResult {

	sortedQSRSlice, numRawSearch, numDistributed, err := getAllSegmentsInAggs(queryInfo, qsrs, queryInfo.aggs,
		queryInfo.queryRange, queryInfo.indexInfo.GetQueryTables(), queryInfo.qid, sTime, orgid)
	if err != nil {
		log.Errorf("qid=%d GetNodeResultsForSegmentStatsCmd: Failed to get all segments in query! Error: %+v", queryInfo.qid, err)
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}

	err = setTotalSegmentsToSearch(queryInfo.qid, numRawSearch+numDistributed)
	if err != nil {
		log.Errorf("qid=%d GetNodeResultsForSegmentStatsCmd: Failed to set total segments to search! Error: %+v", queryInfo.qid, err)
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}
	totalRecsToBeSearched := getTotalRecordsToBeSearched(sortedQSRSlice)
	err = setTotalRecordsToBeSearched(queryInfo.qid, totalRecsToBeSearched)
	if err != nil {
		log.Errorf("qid=%d GetNodeResultsForSegmentStatsCmd: Failed to set total records to search! Error: %+v", queryInfo.qid, err)
	}

	var sstMap map[string]*structs.SegStats

	querySummary.UpdateRemainingDistributedQueries(numDistributed)
	log.Infof("qid=%d, GetNodeResultsForSegmentStatsCmd: Received %+v query segment aggs, with %+v raw search %v distributed, query elapsed time: %+v",
		queryInfo.qid, len(sortedQSRSlice), numRawSearch, numDistributed, time.Since(sTime))
	if queryInfo.aggs.MeasureOperations != nil {
		allSegFileResults.InitSegmentStatsResults(queryInfo.aggs.MeasureOperations)
		sstMap = applyAggOpOnSegments(sortedQSRSlice, allSegFileResults, queryInfo.qid, querySummary, queryInfo.sNodeType, queryInfo.aggs.MeasureOperations, getSstMap)
	}
	querySummary.UpdateQueryTotalTime(time.Since(sTime), allSegFileResults.GetNumBuckets())
	queryType := GetQueryType(queryInfo.qid)
	if !getSstMap {
		err = queryInfo.Wait(querySummary)
		if err != nil {
			log.Errorf("qid=%d GetNodeResultsForSegmentStatsCmd: Failed to wait for all query segment requests to finish! Error: %+v", queryInfo.qid, err)
			return &structs.NodeResult{
				ErrList: []error{err},
			}
		}
	}

	if getSstMap {
		return &structs.NodeResult{
			ErrList:     allSegFileResults.GetAllErrors(),
			Qtype:       queryType.String(),
			SegStatsMap: sstMap,
		}
	}

	humanizeValues := !config.IsNewQueryPipelineEnabled()
	aggMeasureRes, aggMeasureFunctions, aggGroupByCols, _, bucketCount := allSegFileResults.GetSegmentStatsResults(0, humanizeValues)
	SetQidAsFinished(queryInfo.qid)
	return &structs.NodeResult{
		ErrList:          allSegFileResults.GetAllErrors(),
		TotalResults:     allSegFileResults.GetQueryCount(),
		SegEncToKey:      allSegFileResults.SegEncToKey,
		MeasureResults:   aggMeasureRes,
		MeasureFunctions: aggMeasureFunctions,
		GroupByCols:      aggGroupByCols,
		Qtype:            queryType.String(),
		BucketCount:      bucketCount,
	}
}

func getSortedQSRResult(aggs *structs.QueryAggregators, allQSRs []*QuerySegmentRequest) []*QuerySegmentRequest {
	if aggs != nil && aggs.Sort != nil {
		if aggs.Sort.Ascending {
			// index 0 should have the latest time
			sort.Slice(allQSRs, func(i, j int) bool {
				return allQSRs[i].segKeyTsRange.StartEpochMs < allQSRs[j].segKeyTsRange.StartEpochMs
			})
		} else {
			// index 0 should have the earliest time
			sort.Slice(allQSRs, func(i, j int) bool {
				return allQSRs[i].segKeyTsRange.EndEpochMs > allQSRs[j].segKeyTsRange.EndEpochMs
			})
		}
	}
	return allQSRs
}

// Gets special kibana SSRs and applies raw search
func applyKibanaFilterOperator(kibanaIndices []string, allSegFileResults *segresults.SearchResults, parallelismPerFile int64, searchNode *structs.SearchNode,
	sizeLimit uint64, aggs *structs.QueryAggregators, qid uint64, qs *summary.QuerySummary) {
	if len(kibanaIndices) == 0 {
		return
	}
	kibanaSearchRequests := ExtractKibanaRequestsFn(kibanaIndices, qid)
	log.Infof("qid=%d, applyKibanaFilterOperator: Kibana request has %+v SSRs", qid, len(kibanaSearchRequests))

	tRange := &dtu.TimeRange{
		StartEpochMs: 0,
		EndEpochMs:   utils.GetCurrentTimeInMs(),
	}
	err := ApplyFilterOperatorInternal(allSegFileResults, kibanaSearchRequests, parallelismPerFile, searchNode, tRange,
		sizeLimit, aggs, qid, qs)
	if err != nil {
		log.Errorf("qid=%d, applyKibanaFilterOperator failed to apply filter opterator for kibana requests! %+v", qid, err)
		allSegFileResults.AddError(err)
	}
}

func reverseSortedQSRSlice(sortedQSRSlice []*QuerySegmentRequest) {
	lenSortedQSRSlice := len(sortedQSRSlice)

	for i := 0; i < lenSortedQSRSlice/2; i++ {
		sortedQSRSlice[i], sortedQSRSlice[lenSortedQSRSlice-i-1] = sortedQSRSlice[lenSortedQSRSlice-i-1], sortedQSRSlice[i]
	}
}

// loops over all inputted querySegmentRequests and apply search for each file. This function may exit early
func applyFopAllRequests(sortedQSRSlice []*QuerySegmentRequest, queryInfo *QueryInformation,
	allSegFileResults *segresults.SearchResults, qs *summary.QuerySummary) {

	// In order, search segKeys (either raw or pqs depending on above sType).
	// If no aggs, early exit at utils.QUERY_EARLY_EXIT_LIMIT
	// If sort, check if next segkey's time range will overlap with the recent best results
	// If there's aggs and they can be computed fully by agile trees, limit the number of
	// buckets to utils.QUERY_MAX_BUCKETS unless a sort or computation follows the aggs.

	limitAgileAggsTreeBuckets := canUseBucketLimitedAgileAggsTree(sortedQSRSlice, queryInfo)
	var agileTreeBuckets map[string]struct{}
	var agileTreeBuf []byte
	if config.IsAggregationsEnabled() && queryInfo.qType == structs.GroupByCmd &&
		queryInfo.sNodeType == structs.MatchAllQuery {
		agileTreeBuf = make([]byte, 300_000_000)
	}

	doBuckPull := false
	rrcsCompleted := false
	segsNotSent := int(0)
	recsSearchedSinceLastUpdate := uint64(0)
	allEmptySegsForPqid := map[string]bool{}
	sortedQSRSliceLen := len(sortedQSRSlice)
	var err error
	if sortedQSRSliceLen > 0 && queryInfo.persistentQuery {
		allEmptySegsForPqid, err = pqsmeta.GetAllEmptySegmentsForPqid(sortedQSRSlice[0].pqid)
		if err != nil {
			log.Errorf("qid=%d, Failed to get empty segments for pqid %+v! Error: %v", queryInfo.qid, sortedQSRSlice[0].pqid, err)
		}
	}

	// If we have a Transaction command, we want to search the segments from the oldest to the newest
	if queryInfo.aggs != nil && queryInfo.aggs.HasTransactionArgumentsInChain() {
		reverseSortedQSRSlice(sortedQSRSlice)
	}

	shouldRemoveUsageForSeg := !queryInfo.GetQueryType().IsRRCCmd() && allSegFileResults.GetAggs().HasStatsBlock()

	for idx, segReq := range sortedQSRSlice {
		if idx == sortedQSRSliceLen-1 {
			doBuckPull = true
		}

		isCancelled, err := checkForCancelledQuery(queryInfo.qid)
		if err != nil {
			log.Errorf("applyFopAllRequests: qid=%d, Failed to checkForCancelledQuery. Error: %v.", queryInfo.qid, err)
			log.Infof("applyFopAllRequests: qid=%d, Assumed to be cancelled and deleted. The raw search will not continue.", queryInfo.qid)
			return
		}
		if isCancelled {
			return
		}
		otherAggsPresent, timeAggs := checkAggTypes(segReq.aggs)
		eeType := allSegFileResults.ShouldSearchSegKey(segReq.segKeyTsRange, segReq.sNode.NodeType, otherAggsPresent, timeAggs)
		if eeType == segresults.EetEarlyExit {
			allSegFileResults.SetEarlyExit(true)
		} else if eeType == segresults.EetMatchAllAggs {
			allSegFileResults.SetEarlyExit(true)
			err := applyFopFastPathSingleRequest(segReq, allSegFileResults, qs)
			if err != nil {
				log.Errorf("qid=%d, Failed to apply fastpath for segKey %+v! Error: %v", queryInfo.qid, segReq.segKey, err)
				allSegFileResults.AddError(err)
			}
		} else {
			doAgileTree, str := canUseAgileTree(segReq, queryInfo)

			if doAgileTree {
				sTime := time.Now()

				if limitAgileAggsTreeBuckets {
					// Reuse the bucket keys from the previous segments so we
					// sync which buckets we're using across segments.
					str.SetBuckets(agileTreeBuckets)
					str.SetBucketLimit(segutils.QUERY_MAX_BUCKETS)
				}

				search.ApplyAgileTree(str, segReq.aggs, allSegFileResults, segReq.sizeLimit, queryInfo.qid,
					agileTreeBuf)

				if limitAgileAggsTreeBuckets {
					// Get the buckets so we can use its keys for the next
					// segment so that we sync which buckets we're using across
					// segments.
					agileTreeBuckets = str.GetBuckets()
				}

				str.Close()
				timeElapsed := time.Since(sTime)
				queryMetrics := &structs.QueryProcessingMetrics{}
				numRecs := segmetadata.GetNumOfSearchedRecordsRotated(segReq.segKey)
				queryMetrics.SetNumRecordsToRawSearch(numRecs)
				queryMetrics.SetNumRecordsMatched(numRecs)
				qs.UpdateSummary(summary.STREE, timeElapsed, queryMetrics)
			} else {
				if segReq.sType == structs.PQS {
					_, ok := allEmptySegsForPqid[segReq.segKey]
					if ok {
						log.Debugf("Skipping segKey %v for pqid %v", segReq.segKey, segReq.QueryInformation.pqid)
						IncrementNumFinishedSegments(1, queryInfo.qid, 0, 0, "", doBuckPull, nil)
						continue
					}
				}
				// else we continue with rawsearch
				err = applyFilterOperatorSingleRequest(segReq, allSegFileResults, qs)
				if err != nil {
					log.Errorf("qid=%d, Failed to apply filter operator for segKey %+v! Error: %v", queryInfo.qid, segReq.segKey, err)
					allSegFileResults.AddError(err)
				}
			}
		}
		var recsSearched uint64
		if segReq.sType == structs.RAW_SEARCH || segReq.sType == structs.PQS {
			recsSearched = segmetadata.GetNumOfSearchedRecordsRotated(segReq.segKey)
		} else {
			recsSearched = writer.GetNumOfSearchedRecordsUnRotated(segReq.segKey)
		}
		recsSearchedSinceLastUpdate += recsSearched

		segsNotSent++
		segenc := allSegFileResults.SegKeyToEnc[segReq.segKey]
		// We need to increment the number of finished segments and send message to QUERY_UPDATE channel
		// irrespective of whether a segment has Matched RRC or not. This is to ensure that the query Aggregations
		// which require all the Segments to be processed are updated correctly.
		IncrementNumFinishedSegments(segsNotSent, queryInfo.qid, recsSearchedSinceLastUpdate, segenc,
			"", doBuckPull, nil)
		segsNotSent = 0
		recsSearchedSinceLastUpdate = 0

		if !rrcsCompleted && areAllRRCsFound(allSegFileResults, sortedQSRSlice[idx+1:], segReq.aggs) {
			qs.SetRRCFinishTime()
			rrcsCompleted = true
		}

		if shouldRemoveUsageForSeg {
			removeUsageForSegmentHook(queryInfo.qid, segReq.segKey)
		}
	}

	if segsNotSent > 0 {
		doBucketPull := true // This is the last update, so flush the buckets.
		IncrementNumFinishedSegments(segsNotSent, queryInfo.qid, recsSearchedSinceLastUpdate, 0, "", doBucketPull, nil)
	}

	if !rrcsCompleted {
		qs.SetRRCFinishTime()
	}
	if sortedQSRSliceLen == 0 {
		IncrementNumFinishedSegments(0, queryInfo.qid, recsSearchedSinceLastUpdate, 0, "", false, nil)
	}
}

// Return true if we can use AgileAggsTrees for all the segments and we can
// limit the number of buckets.
func canUseBucketLimitedAgileAggsTree(sortedQSRSlice []*QuerySegmentRequest, queryInfo *QueryInformation) bool {
	if !queryInfo.aggs.CanLimitBuckets() {
		return false
	}

	for _, segReq := range sortedQSRSlice {
		canUse, agileTree := canUseAgileTree(segReq, queryInfo)

		if agileTree != nil {
			agileTree.Close()
		}

		if !canUse {
			return false
		}
	}

	return true
}

func canUseAgileTree(segReq *QuerySegmentRequest, queryInfo *QueryInformation) (bool, *segread.AgileTreeReader) {
	isSegFullyEncosed := segReq.queryRange.AreTimesFullyEnclosed(segReq.segKeyTsRange.StartEpochMs,
		segReq.segKeyTsRange.EndEpochMs)
	_, timeAggs := checkAggTypes(segReq.aggs)

	if config.IsAggregationsEnabled() && isSegFullyEncosed && queryInfo.qType == structs.GroupByCmd &&
		queryInfo.sNodeType == structs.MatchAllQuery && !timeAggs {
		return search.CanDoStarTree(segReq.segKey, segReq.aggs, queryInfo.qid)
	}

	return false, nil
}

// returns true if any element in qsrs would displace any of the RRCs
// displacing of RRCs will only happen if not all RRCs exist or if sort conditions will displace
// Returns true if no more raw search will to be performed
func areAllRRCsFound(sr *segresults.SearchResults, qsrs []*QuerySegmentRequest, aggs *structs.QueryAggregators) bool {

	if sr.ShouldContinueRRCSearch() {
		return false
	}

	if aggs == nil || aggs.Sort == nil {
		return true
	}

	for _, r := range qsrs {
		var willValBeAdded bool
		if aggs.Sort.Ascending {
			willValBeAdded = sr.BlockResults.WillValueBeAdded(float64(r.segKeyTsRange.StartEpochMs))
		} else {
			willValBeAdded = sr.BlockResults.WillValueBeAdded(float64(r.segKeyTsRange.EndEpochMs))
		}
		if willValBeAdded {
			return false
		}
	}
	return true
}

// Returns query segment requests, count of keys to raw search, count of distributed queries, count of keys in PQS
func getAllUnrotatedSegments(queryInfo *QueryInformation, sTime time.Time) ([]*QuerySegmentRequest, uint64, uint64, error) {
	allUnrotatedKeys, totalChecked, totalCount := writer.FilterUnrotatedSegmentsInQuery(queryInfo.queryRange,
		queryInfo.indexInfo.GetQueryTables(), queryInfo.GetOrgId())
	log.Infof("qid=%d, Unrotated query time filtering returned %v segment keys to search out of %+v. query elapsed time: %+v", queryInfo.qid, totalCount,
		totalChecked, time.Since(sTime))
	var err error

	qsrs, raw, pqs := filterUnrotatedSegKeysToQueryRequests(queryInfo, allUnrotatedKeys)
	qsrs, err = applyQsrsFilterHook(queryInfo, qsrs, false)
	if err != nil {
		log.Errorf("getAllUnrotatedSegments: qid=%d, failed to apply hook: %v", queryInfo.qid, err)
		return nil, 0, 0, err
	}

	return qsrs, raw, pqs, nil
}

// returns query segment requests, count of keys to raw search, and distributed query count
func getAllSegmentsInAggs(queryInfo *QueryInformation, qsrs []*QuerySegmentRequest, aggs *structs.QueryAggregators, timeRange *dtu.TimeRange, indexNames []string,
	qid uint64, sTime time.Time, orgid int64) ([]*QuerySegmentRequest, uint64, uint64, error) {

	if len(qsrs) != 0 {
		return qsrs, uint64(len(qsrs)), 0, nil
	}

	finalQsrs := make([]*QuerySegmentRequest, 0)
	numRawSearch := uint64(0)
	numDistributed := uint64(0)

	unrotatedQSR, unrotatedRawCount, err := getAllUnrotatedSegmentsInAggs(queryInfo, aggs, timeRange, indexNames, qid, sTime, orgid)
	if err != nil {
		log.Errorf("getAllSegmentsInAggs: qid=%d, Failed to get all unrotated segments: %v", queryInfo.qid, err)
		return nil, 0, 0, err
	}

	finalQsrs = append(finalQsrs, unrotatedQSR...)
	numRawSearch += unrotatedRawCount

	rotatedQSR, rotatedRawCount, err := getAllRotatedSegmentsInAggs(queryInfo, aggs, timeRange, indexNames, qid, sTime, orgid)
	if err != nil {
		log.Errorf("getAllSegmentsInAggs: qid=%d, Failed to get all rotated segments: %v", queryInfo.qid, err)
		return nil, 0, 0, err
	}

	if config.IsS3Enabled() {
		rotatedSegments := getRotatedSegments(rotatedQSR)
		if hook := hooks.GlobalHooks.AddUsageForRotatedSegmentsHook; hook != nil {
			hook(queryInfo.qid, rotatedSegments)
		}
	}

	finalQsrs = append(finalQsrs, rotatedQSR...)
	numRawSearch += rotatedRawCount

	if config.IsNewQueryPipelineEnabled() {
		numDistributed = queryInfo.dqs.GetNumNodesDistributedTo()
	} else {
		numDistributed, err = queryInfo.dqs.DistributeQuery(queryInfo)
		if err != nil {
			log.Errorf("qid=%d, Error in distributing query %+v", queryInfo.qid, err)
			return nil, 0, 0, err
		}
	}

	return finalQsrs, numRawSearch, numDistributed, nil
}

func getAllUnrotatedSegmentsInAggs(queryInfo *QueryInformation, aggs *structs.QueryAggregators, timeRange *dtu.TimeRange, indexNames []string,
	qid uint64, sTime time.Time, orgid int64) ([]*QuerySegmentRequest, uint64, error) {
	allUnrotatedKeys, totalChecked, totalCount := writer.FilterUnrotatedSegmentsInQuery(timeRange, indexNames, orgid)
	log.Infof("qid=%d, Unrotated query time filtering returned %v segment keys to search out of %+v. query elapsed time: %+v", qid, totalCount,
		totalChecked, time.Since(sTime))
	var err error

	qsrs, rawSearch := FilterAggSegKeysToQueryResults(queryInfo, allUnrotatedKeys, aggs, structs.UNROTATED_SEGMENT_STATS_SEARCH)
	qsrs, err = applyQsrsFilterHook(queryInfo, qsrs, false)
	if err != nil {
		log.Errorf("getAllUnrotatedSegmentsInAggs: qid=%d, failed to apply hook: %v", queryInfo.qid, err)
		return nil, 0, err
	}

	return qsrs, rawSearch, nil
}

func getAllRotatedSegmentsInAggs(queryInfo *QueryInformation, aggs *structs.QueryAggregators, timeRange *dtu.TimeRange, indexNames []string,
	qid uint64, sTime time.Time, orgid int64) ([]*QuerySegmentRequest, uint64, error) {
	// 1. metadata.FilterSegmentsByTime gives epoch range
	allPossibleKeys, tsPassedCount, totalPossible := segmetadata.FilterSegmentsByTime(timeRange, indexNames, orgid)
	log.Infof("qid=%d, Rotated query time filtering returned %v segment keys to search out of %+v. query elapsed time: %+v", qid, tsPassedCount,
		totalPossible, time.Since(sTime))

	qsrs, totalQsr := FilterAggSegKeysToQueryResults(queryInfo, allPossibleKeys, aggs, structs.SEGMENT_STATS_SEARCH)

	qsrs, err := applyQsrsFilterHook(queryInfo, qsrs, true)
	if err != nil {
		log.Errorf("getAllRotatedSegmentsInAggs: qid=%d, failed to apply hook: %v", queryInfo.qid, err)
		return nil, 0, err
	}

	return qsrs, totalQsr, nil
}

func canUseSSTForStats(searchType structs.SearchNodeType, segmentFullyEnclosed bool, aggs *structs.QueryAggregators) bool {
	aggHasEvalFunc := aggs.HasValueColRequest()
	aggHasValuesFunc := aggs.HasValuesFunc()
	aggHasListFunc := aggs.HasListFunc()
	return searchType == structs.MatchAllQuery && segmentFullyEnclosed &&
		!aggHasEvalFunc && !aggHasValuesFunc && !aggHasListFunc

}

func computeSegStatsFromRawRecords(segReq *QuerySegmentRequest, qs *summary.QuerySummary, allSegFileResults *segresults.SearchResults,
	qid uint64, nodeRes *structs.NodeResult) (map[string]*structs.SegStats, error) {
	var sstMap map[string]*structs.SegStats
	// run through micro index check for block tracker & generate SSR
	blocksToRawSearch, err := segReq.GetMicroIndexFilter()
	if err != nil {
		log.Errorf("qid=%d, computeSegStatsFromRawRecords: failed to get blocks to raw search! Defaulting to searching all blocks. SegKey %+v, err: %v",
			segReq.qid, segReq.segKey, err)
		blocksToRawSearch = segReq.GetEntireFileMicroIndexFilter()
	}
	sTime := time.Now()
	isQueryPersistent, err := querytracker.IsQueryPersistent([]string{segReq.tableName}, segReq.sNode)
	if err != nil {
		log.Errorf("qid=%d, computeSegStatsFromRawRecords: Failed to check if query is persistent! Error: %v", qid, err)
	}
	var rawSearchSSR map[string]*structs.SegmentSearchRequest
	if segReq.sType == structs.SEGMENT_STATS_SEARCH {
		rawSearchSSR = ExtractSSRFromSearchNode(segReq.sNode, blocksToRawSearch, segReq.queryRange, segReq.indexInfo.GetQueryTables(), qs, segReq.qid, isQueryPersistent, segReq.pqid)
	} else {
		rawSearchSSR = metadata.ExtractUnrotatedSSRFromSearchNode(segReq.sNode, segReq.queryRange, segReq.indexInfo.GetQueryTables(), blocksToRawSearch, qs, segReq.qid)
	}
	qs.UpdateExtractSSRTime(time.Since(sTime))

	// rawSearchSSR should be of size 1 or 0
	for _, req := range rawSearchSSR {
		req.ConsistentCValLenMap = segReq.ConsistentCValLenMap
		sstMap, err = search.RawComputeSegmentStats(req, segReq.parallelismPerFile, segReq.sNode, segReq.queryRange, segReq.aggs.MeasureOperations, allSegFileResults, qid, qs, nodeRes)
		if err != nil {
			return sstMap, fmt.Errorf("qid=%d, computeSegStatsFromRawRecords: Failed to get segment level stats for segKey %+v! Error: %v", qid, segReq.segKey, err)
		}
	}
	return sstMap, nil
}

func applyAggOpOnSegments(sortedQSRSlice []*QuerySegmentRequest, allSegFileResults *segresults.SearchResults, qid uint64, qs *summary.QuerySummary,
	searchType structs.SearchNodeType, measureOperations []*structs.MeasureAggregator, getSstMap bool) map[string]*structs.SegStats {

	nodeRes, err := GetOrCreateQuerySearchNodeResult(qid)
	if err != nil {
		log.Errorf("qid=%d, Failed to get or create query search node result! Error: %v", qid, err)
		return nil
	}

	statsRes := segresults.InitStatsResults()

	//assuming we will allow 100 measure Operations
	for _, segReq := range sortedQSRSlice {
		isCancelled, err := checkForCancelledQuery(qid)
		if err != nil {
			log.Errorf("applyAggOpOnSegments:: qid=%d Failed to checkForCancelledQuery. Error: %v", qid, err)
			log.Infof("applyAggOpOnSegments: qid=%d, Assumed to be cancelled and deleted. The raw search will not continue.", qid)
			break
		}
		if isCancelled {
			break
		}
		isSegmentFullyEnclosed := segReq.queryRange.AreTimesFullyEnclosed(segReq.segKeyTsRange.StartEpochMs, segReq.segKeyTsRange.EndEpochMs)

		// For Unrotated search, Check if the segment is rotated and update the search type accordingly
		if segReq.sType == structs.UNROTATED_SEGMENT_STATS_SEARCH {
			if !writer.IsSegKeyUnrotated(segReq.segKey) {
				// If the segment is not unrotated, we should search the rotated segment
				segReq.sType = structs.SEGMENT_STATS_SEARCH
			}
		}

		// Because segment only store statistical data such as min, max..., for some functions we should recompute raw data to get the results
		// If agg has evaluation functions, we should recompute raw data instead of using the previously stored statistical data in the segment

		var sstMap map[string]*structs.SegStats

		if canUseSSTForStats(searchType, isSegmentFullyEnclosed, segReq.aggs) {
			sstMap, err = segread.ReadSegStats(segReq.segKey, segReq.qid)
			if err != nil {
				log.Errorf("qid=%d, applyAggOpOnSegments: Failed to read segStats for segKey %+v! computing segStats from raw records. Error: %v",
					qid, segReq.segKey, err)
				allSegFileResults.AddError(err)
				// If we can't read the segment stats, we should compute it from raw records
				sstMap, err = computeSegStatsFromRawRecords(segReq, qs, allSegFileResults, qid, nodeRes)
				if err != nil {
					allSegFileResults.AddError(err)
				}
			} else {
				sstMap["*"] = &structs.SegStats{
					Count: uint64(segReq.TotalRecords),
				}
				allSegFileResults.AddResultCount(uint64(segReq.TotalRecords))
			}
		} else {
			sstMap, err = computeSegStatsFromRawRecords(segReq, qs, allSegFileResults, qid, nodeRes)
			if err != nil {
				allSegFileResults.AddError(err)
			}
		}

		statsRes.MergeSegStats(sstMap)

		totalRecsSearched := uint64(0)
		if segReq.sType == structs.SEGMENT_STATS_SEARCH {
			totalRecsSearched = segmetadata.GetNumOfSearchedRecordsRotated(segReq.segKey)
		} else if segReq.sType == structs.UNROTATED_SEGMENT_STATS_SEARCH {
			totalRecsSearched = writer.GetNumOfSearchedRecordsUnRotated(segReq.segKey)
		}
		segenc := allSegFileResults.GetAddSegEnc(segReq.segKey)
		IncrementNumFinishedSegments(1, qid, totalRecsSearched, segenc, "", true, sstMap)

		removeUsageForSegmentHook(qid, segReq.segKey)
	}

	finalSstMap := statsRes.GetSegStats()

	if !getSstMap {
		err = allSegFileResults.UpdateSegmentStats(finalSstMap, measureOperations)
		if err != nil {
			log.Errorf("qid=%d,  applyAggOpOnSegments : ReadSegStats: Failed to update segment stats for segKey! Error: %v", qid, err)
			allSegFileResults.AddError(err)
		}
	}

	if len(sortedQSRSlice) == 0 {
		IncrementNumFinishedSegments(0, qid, 0, 0, "", true, nil)
	}

	return finalSstMap
}

func getRotatedSegments(qsrs []*QuerySegmentRequest) map[string]struct{} {
	usedSegments := make(map[string]struct{})
	for _, qsr := range qsrs {
		usedSegments[qsr.GetSegKey()] = struct{}{}
	}

	return usedSegments
}

// return sorted slice of querySegmentRequests, count of raw search requests, distributed queries, and count of pqs request
func getAllSegmentsInQuery(queryInfo *QueryInformation, sTime time.Time) ([]*QuerySegmentRequest, uint64, uint64, uint64, error) {
	unsortedQsrs := make([]*QuerySegmentRequest, 0)
	numRawSearch := uint64(0)
	numDistributed := uint64(0)
	numPQS := uint64(0)

	unrotatedQSR, unrotatedRawCount, unrotatedPQSCount, err := getAllUnrotatedSegments(queryInfo, sTime)
	if err != nil {
		return nil, 0, 0, 0, err
	}

	unsortedQsrs = append(unsortedQsrs, unrotatedQSR...)
	numRawSearch += unrotatedRawCount
	numPQS += unrotatedPQSCount

	rotatedQSR, rotatedRawCount, rotatedPQS, err := getAllRotatedSegmentsInQuery(queryInfo, sTime)
	if err != nil {
		return nil, 0, 0, 0, err
	}

	if config.IsS3Enabled() {
		rotatedSegments := getRotatedSegments(rotatedQSR)
		if hook := hooks.GlobalHooks.AddUsageForRotatedSegmentsHook; hook != nil {
			hook(queryInfo.qid, rotatedSegments)
		}
	}

	unsortedQsrs = append(unsortedQsrs, rotatedQSR...)
	numRawSearch += rotatedRawCount
	numPQS += rotatedPQS

	if config.IsNewQueryPipelineEnabled() {
		numDistributed = queryInfo.dqs.GetNumNodesDistributedTo()
	} else if queryInfo.dqs != nil {
		numDistributed, err = queryInfo.dqs.DistributeQuery(queryInfo)
		if err != nil {
			log.Errorf("qid=%d, Error in distributing query %+v", queryInfo.qid, err)
			return nil, 0, 0, 0, err
		}
	}

	// Sort query segment results depending on aggs
	sortedQSRSlice := getSortedQSRResult(queryInfo.aggs, unsortedQsrs)

	return sortedQSRSlice, numRawSearch, numDistributed, numPQS, nil
}

// returns sorted order of querySegmentRequests, count of keys to raw search, count of distributed queries, and count of pqs keys to raw search
func getAllRotatedSegmentsInQuery(queryInfo *QueryInformation, sTime time.Time) ([]*QuerySegmentRequest, uint64, uint64, error) {
	// 1. metadata.FilterSegmentsByTime gives epoch range
	allPossibleKeys, tsPassedCount, totalPossible := segmetadata.FilterSegmentsByTime(queryInfo.queryRange,
		queryInfo.indexInfo.GetQueryTables(), queryInfo.GetOrgId())
	log.Infof("qid=%d, Rotated query time filtering returned %v segment keys to search out of %+v. query elapsed time: %+v", queryInfo.qid, tsPassedCount,
		totalPossible, time.Since(sTime))
	var err error
	qsrs := ConvertSegKeysToQueryRequests(queryInfo, allPossibleKeys)

	qsrs, err = applyQsrsFilterHook(queryInfo, qsrs, true)
	if err != nil {
		log.Errorf("getAllRotatedSegmentsInQuery: qid=%d, failed to apply hook: %v", queryInfo.qid, err)
		return nil, 0, 0, err
	}

	// 2. Whatever needed sorting of segKeys based on sorts & generation into QuerySegmentRequest
	qsrs, raw, pqs := FilterSegKeysToQueryResults(queryInfo, qsrs)
	return qsrs, raw, pqs, nil
}

func applyFilterOperatorSingleRequest(qsr *QuerySegmentRequest, allSegFileResults *segresults.SearchResults, qs *summary.QuerySummary) error {
	switch qsr.sType {
	case structs.PQS:
		return applyFilterOperatorPQSRequest(qsr, allSegFileResults, qs)
	case structs.RAW_SEARCH:
		return applyFilterOperatorRawSearchRequest(qsr, allSegFileResults, qs)
	case structs.UNROTATED_PQS:
		return applyFilterOperatorUnrotatedPQSRequest(qsr, allSegFileResults, qs)
	case structs.UNROTATED_RAW_SEARCH:
		return applyFilterOperatorUnrotatedRawSearchRequest(qsr, allSegFileResults, qs)
	case structs.UNKNOWN:
		log.Errorf("qid=%d, Got a unknown query segment request! SegKey %+v", qsr.qid, qsr.segKey)
		fallthrough
	default:
		return fmt.Errorf("qid=%d, applyFilterOperatorSingleRequest: unsupported segment type %+v", qsr.qid, qsr.sType)
	}
}

func applyFilterOperatorPQSRequest(qsr *QuerySegmentRequest, allSegFileResults *segresults.SearchResults, qs *summary.QuerySummary) error {
	spqmr, err := pqs.GetAllPersistentQueryResults(qsr.segKey, qsr.QueryInformation.pqid)
	if err != nil {
		qsr.sType = structs.RAW_SEARCH
		qsr.blkTracker = structs.InitEntireFileBlockTracker()
		return applyFilterOperatorRawSearchRequest(qsr, allSegFileResults, qs)
	}
	err = applyPQSToRotatedRequest(qsr, allSegFileResults, spqmr, qs)
	if err != nil {
		qsr.sType = structs.RAW_SEARCH
		qsr.blkTracker = structs.InitEntireFileBlockTracker()
		return applyFilterOperatorRawSearchRequest(qsr, allSegFileResults, qs)
	}

	// We are assuming that all the blocks we need to search are in spqmr, so no need to raw search anything.
	// But for that assumption to be true, there are two cases that we need to handle where there might be some missing blocks:
	// 1. If persistent query is enabled in the middle of the segment, we should not create a PQS for that segment.
	// 2. We should not do `BackFillPQSSegmetaEntry` for the segment if the segment is not fully enclosed in the PQS time range.
	// We should avoid any other cases where we might have missing blocks.
	return nil
}

// Returns map of filename to SSR.
func GetSSRsFromQSR(qsr *QuerySegmentRequest, querySummary *summary.QuerySummary) (map[string]*structs.SegmentSearchRequest, error) {
	// run through micro index check for block tracker & generate SSR
	blocksToRawSearch, err := qsr.GetMicroIndexFilter()
	if err != nil {
		log.Errorf("qid=%d, failed to get blocks to raw search! Defaulting to searching all blocks. SegKey %+v, err: %v",
			qsr.qid, qsr.segKey, err)
		blocksToRawSearch = qsr.GetEntireFileMicroIndexFilter()
	}

	isQueryPersistent, err := querytracker.IsQueryPersistent([]string{qsr.tableName}, qsr.sNode)
	if err != nil {
		log.Errorf("qid=%d, failed to check if query is persistent", qsr.qid)
	}

	sTime := time.Now()
	var rawSearchSSRs map[string]*structs.SegmentSearchRequest
	if writer.IsSegKeyUnrotated(qsr.segKey) {
		rawSearchSSRs = metadata.ExtractUnrotatedSSRFromSearchNode(qsr.sNode, qsr.queryRange,
			qsr.indexInfo.GetQueryTables(), blocksToRawSearch, querySummary, qsr.qid)
	} else {
		rawSearchSSRs = ExtractSSRFromSearchNode(qsr.sNode, blocksToRawSearch, qsr.queryRange,
			qsr.indexInfo.GetQueryTables(), querySummary, qsr.qid, isQueryPersistent, qsr.pqid)
	}
	querySummary.UpdateExtractSSRTime(time.Since(sTime))

	for _, req := range rawSearchSSRs {
		req.SType = qsr.sType
		req.ConsistentCValLenMap = qsr.ConsistentCValLenMap
	}

	return rawSearchSSRs, nil
}

func applyFilterOperatorRawSearchRequest(qsr *QuerySegmentRequest, allSegFileResults *segresults.SearchResults, qs *summary.QuerySummary) error {
	rawSearchSSRs, err := GetSSRsFromQSR(qsr, qs)
	if err != nil {
		log.Errorf("qid=%d, applyFilterOperatorRawSearchRequest: failed to get SSRs from QSR! SegKey %+v", qsr.qid, qsr.segKey)
		return err
	}

	err = ApplyFilterOperatorInternal(allSegFileResults, rawSearchSSRs, qsr.parallelismPerFile,
		qsr.sNode, qsr.queryRange, qsr.sizeLimit, qsr.aggs, qsr.qid, qs)

	for _, req := range rawSearchSSRs {
		if req.HasMatchedRrc {
			qsr.HasMatchedRrc = true
			break
		}
	}
	return err
}

func applyFilterOperatorUnrotatedPQSRequest(qsr *QuerySegmentRequest, allSegFileResults *segresults.SearchResults, qs *summary.QuerySummary) error {
	spqmr, err := writer.GetAllPersistentQueryResults(qsr.segKey, qsr.QueryInformation.pqid)
	if err != nil {
		qsr.sType = structs.UNROTATED_RAW_SEARCH
		qsr.blkTracker = structs.InitEntireFileBlockTracker()
		return applyFilterOperatorUnrotatedRawSearchRequest(qsr, allSegFileResults, qs)
	}
	err = applyPQSToUnrotatedRequest(qsr, allSegFileResults, spqmr, qs)
	if err != nil {
		qsr.sType = structs.UNROTATED_RAW_SEARCH
		qsr.blkTracker = structs.InitEntireFileBlockTracker()
		return applyFilterOperatorUnrotatedRawSearchRequest(qsr, allSegFileResults, qs)
	}

	// If all possible blocks we need to search are in spqmr, no need to raw search anything
	missingTRange := writer.GetTSRangeForMissingBlocks(qsr.segKey, qsr.segKeyTsRange, spqmr)
	if missingTRange == nil || !allSegFileResults.ShouldSearchRange(missingTRange.StartEpochMs, missingTRange.EndEpochMs) {
		return nil
	}
	qsr.sType = structs.UNROTATED_RAW_SEARCH
	qsr.blkTracker = structs.InitExclusionBlockTracker(spqmr) // blocks not found in pqs, that we need to raw search for a key
	return applyFilterOperatorUnrotatedRawSearchRequest(qsr, allSegFileResults, qs)
}

func applyFilterOperatorUnrotatedRawSearchRequest(qsr *QuerySegmentRequest, allSegFileResults *segresults.SearchResults, qs *summary.QuerySummary) error {
	// run through micro index check for block tracker & generate SSR
	blocksToRawSearch, err := qsr.GetMicroIndexFilter()
	if err != nil {
		log.Errorf("qid=%d, failed to get blocks to raw search! Defaulting to searching all blocks. SegKey %+v, err: %v", qsr.qid, qsr.segKey, err)
		blocksToRawSearch = qsr.GetEntireFileMicroIndexFilter()
	}
	sTime := time.Now()
	rawSearchSSR := metadata.ExtractUnrotatedSSRFromSearchNode(qsr.sNode, qsr.queryRange, qsr.indexInfo.GetQueryTables(), blocksToRawSearch, qs, qsr.qid)
	qs.UpdateExtractSSRTime(time.Since(sTime))
	for _, req := range rawSearchSSR {
		req.SType = qsr.sType
	}
	err = ApplyFilterOperatorInternal(allSegFileResults, rawSearchSSR, qsr.parallelismPerFile, qsr.sNode, qsr.queryRange,
		qsr.sizeLimit, qsr.aggs, qsr.qid, qs)

	for _, req := range rawSearchSSR {
		if req.HasMatchedRrc {
			qsr.HasMatchedRrc = true
			break
		}
	}
	return err
}

// loops over all segrequests and performs raw search
func ApplyFilterOperatorInternal(allSegFileResults *segresults.SearchResults, allSegRequests map[string]*structs.SegmentSearchRequest,
	parallelismPerFile int64, searchNode *structs.SearchNode, timeRange *dtu.TimeRange, sizeLimit uint64, aggs *structs.QueryAggregators,
	qid uint64, qs *summary.QuerySummary) error {

	nodeRes, err := GetOrCreateQuerySearchNodeResult(qid)
	if err != nil {
		return fmt.Errorf("ApplyFilterOperatorInternal: Failed to get or create query search node result! Error: %v", err)
	}
	for _, req := range allSegRequests {
		search.RawSearchSegmentFileWrapper(req, parallelismPerFile, searchNode, timeRange, sizeLimit, aggs, allSegFileResults, qid, qs, nodeRes)
	}

	return nil
}

// Returns sorted order of query segment requests, count of keys to raw search
func FilterAggSegKeysToQueryResults(qInfo *QueryInformation, allPossibleKeys map[string]map[string]*structs.SegmentByTimeAndColSizes,
	aggs *structs.QueryAggregators, segType structs.SegType) ([]*QuerySegmentRequest, uint64) {

	allAggSegmentRequests := make([]*QuerySegmentRequest, 0)
	aggSearchCount := uint64(0)
	for tableName, segKeys := range allPossibleKeys {
		for segKey, segTimeCs := range segKeys {
			if segTimeCs == nil {
				log.Errorf("qid=%d, FilterAggSegKeysToQueryResults reieved an empty segment time range. SegKey %+v", qInfo.qid, segKey)
				continue
			}
			qReq := &QuerySegmentRequest{
				QueryInformation:     *qInfo,
				segKey:               segKey,
				segKeyTsRange:        segTimeCs.TimeRange,
				tableName:            tableName,
				ConsistentCValLenMap: segTimeCs.ConsistentCValLenMap,
				TotalRecords:         segTimeCs.TotalRecords,
			}

			qReq.sType = segType
			qReq.blkTracker = structs.InitEntireFileBlockTracker()
			aggSearchCount++

			allAggSegmentRequests = append(allAggSegmentRequests, qReq)
		}
	}

	return allAggSegmentRequests, aggSearchCount
}

// Returns sorted order of query segment requests, count of keys to raw search, count of keys in PQS
func FilterSegKeysToQueryResults(qInfo *QueryInformation, qsrs []*QuerySegmentRequest) ([]*QuerySegmentRequest, uint64, uint64) {
	pqsCount := uint64(0)
	rawSearchCount := uint64(0)
	for _, qReq := range qsrs {
		if pqs.DoesSegKeyHavePqidResults(qReq.segKey, qInfo.pqid) {
			qReq.sType = structs.PQS
			pqsCount++
		} else {
			qReq.sType = structs.RAW_SEARCH
			qReq.blkTracker = structs.InitEntireFileBlockTracker()
			rawSearchCount++
		}
	}

	return qsrs, rawSearchCount, pqsCount
}

func ConvertSegKeysToQueryRequests(qInfo *QueryInformation, allPossibleKeys map[string]map[string]*structs.SegmentByTimeAndColSizes) []*QuerySegmentRequest {
	allSegRequests := make([]*QuerySegmentRequest, 0)
	for tableName, segKeys := range allPossibleKeys {
		for segKey, segTimeCs := range segKeys {
			if segTimeCs == nil {
				log.Errorf("qid=%d, ConvertSegKeysToQueryRequests received an empty segment time range. SegKey %+v", qInfo.qid, segKey)
				continue
			}
			qReq := &QuerySegmentRequest{
				QueryInformation:     *qInfo,
				segKey:               segKey,
				segKeyTsRange:        segTimeCs.TimeRange,
				tableName:            tableName,
				ConsistentCValLenMap: segTimeCs.ConsistentCValLenMap,
				TotalRecords:         segTimeCs.TotalRecords,
			}
			allSegRequests = append(allSegRequests, qReq)
		}
	}

	return allSegRequests
}

// Returns query segment requests, count of keys to raw search, count of keys in PQS
func filterUnrotatedSegKeysToQueryRequests(qInfo *QueryInformation, allPossibleKeys map[string]map[string]*structs.SegmentByTimeAndColSizes) ([]*QuerySegmentRequest, uint64, uint64) {

	allSegRequests := make([]*QuerySegmentRequest, 0)
	pqsCount := uint64(0)
	rawSearchCount := uint64(0)
	for tableName, segKeys := range allPossibleKeys {
		for segKey, segTimeCs := range segKeys {
			if segTimeCs == nil {
				log.Errorf("qid=%d, filterUnrotatedSegKeysToQueryRequests received an empty segment time range. SegKey %+v", qInfo.qid, segKey)
				continue
			}
			qReq := &QuerySegmentRequest{
				QueryInformation: *qInfo,
				segKey:           segKey,
				segKeyTsRange:    segTimeCs.TimeRange,
				tableName:        tableName,
			}
			if writer.DoesSegKeyHavePqidResults(segKey, qInfo.pqid) {
				qReq.sType = structs.UNROTATED_PQS
				pqsCount++
			} else {
				qReq.sType = structs.UNROTATED_RAW_SEARCH
				qReq.blkTracker = structs.InitEntireFileBlockTracker()
				rawSearchCount++
			}
			allSegRequests = append(allSegRequests, qReq)
		}
	}
	return allSegRequests, rawSearchCount, pqsCount
}

// gets search metadata for a segKey and runs raw search
func applyPQSToRotatedRequest(qsr *QuerySegmentRequest, allSearchResults *segresults.SearchResults, spqmr *pqmr.SegmentPQMRResults, qs *summary.QuerySummary) error {

	allBlocksToSearch, blkSummaries, err := segmetadata.GetSearchInfoAndSummaryForPQS(qsr.segKey, spqmr)
	if err != nil {
		log.Errorf("qid=%d, applyPQSToRotatedRequest: failed to get search info for pqs query %+v. Error: %+v",
			qsr.qid, qsr.segKey, err)
		return err
	}

	return ApplySinglePQSRawSearch(qsr, allSearchResults, spqmr, allBlocksToSearch,
		blkSummaries, qs)
}

// gets search metadata for a segKey and runs raw search
func applyPQSToUnrotatedRequest(qsr *QuerySegmentRequest, allSearchResults *segresults.SearchResults, spqmr *pqmr.SegmentPQMRResults, qs *summary.QuerySummary) error {

	allBlocksToSearch, blkSummaries, err := writer.GetSearchInfoForPQSQuery(qsr.segKey, spqmr)
	if err != nil {
		log.Errorf("qid=%d, applyPQSToUnrotatedRequest: failed to get search info for pqs query %+v. Error: %+v",
			qsr.qid, qsr.segKey, err)
		return err
	}
	return ApplySinglePQSRawSearch(qsr, allSearchResults, spqmr, allBlocksToSearch, blkSummaries, qs)
}

func ApplySinglePQSRawSearch(qsr *QuerySegmentRequest, allSearchResults *segresults.SearchResults,
	spqmr *pqmr.SegmentPQMRResults, allBlocksToSearch map[uint16]struct{},
	blkSummaries []*structs.BlockSummary, qs *summary.QuerySummary) error {

	if len(allBlocksToSearch) == 0 {
		log.Infof("qid=%d, ApplySinglePQSRawSearch: segKey %+v has 0 blocks in segment PQMR results", qsr.qid, qsr.segKey)
		return nil
	}
	req := &structs.SegmentSearchRequest{
		SegmentKey:         qsr.segKey,
		VirtualTableName:   qsr.tableName,
		AllPossibleColumns: qsr.colsToSearch,
		AllBlocksToSearch:  allBlocksToSearch,
		SearchMetadata: &structs.SearchMetadataHolder{
			BlockSummaries:    blkSummaries,
			SearchTotalMemory: uint64(len(blkSummaries) * 16), // TODO: add bitset size here
		},
		ConsistentCValLenMap: qsr.ConsistentCValLenMap,
	}
	nodeRes, err := GetOrCreateQuerySearchNodeResult(qsr.qid)
	if err != nil {
		return fmt.Errorf("qid=%d, ApplySinglePQSRawSearch: failed to get or create query search node result! Error: %v", qsr.qid, err)
	}
	search.RawSearchPQMResults(req, qsr.parallelismPerFile, qsr.queryRange, qsr.aggs, qsr.sizeLimit, spqmr, allSearchResults, qsr.qid, qs, nodeRes)

	if req.HasMatchedRrc {
		qsr.HasMatchedRrc = true
	}
	return nil
}

func applyFopFastPathSingleRequest(qsr *QuerySegmentRequest, allSegFileResults *segresults.SearchResults, qs *summary.QuerySummary) error {
	rawSearchSSRs, err := GetSSRsFromQSR(qsr, qs)
	if err != nil {
		log.Errorf("qid=%d, applyFopFastPathSingleRequest: failed to get SSRs from QSR! SegKey %+v",
			qsr.qid, qsr.segKey)
		return err
	}

	err = applyFopFastPathInternal(allSegFileResults, rawSearchSSRs, qsr.parallelismPerFile,
		qsr.sNode, qsr.queryRange, qsr.sizeLimit, qsr.aggs, qsr.qid, qs)

	for _, req := range rawSearchSSRs {
		if req.HasMatchedRrc {
			qsr.HasMatchedRrc = true
			break
		}
	}

	return err
}

// loops over all segrequests and performs raw search
func applyFopFastPathInternal(allSegFileResults *segresults.SearchResults,
	allSegRequests map[string]*structs.SegmentSearchRequest,
	parallelismPerFile int64, searchNode *structs.SearchNode, timeRange *dtu.TimeRange,
	sizeLimit uint64, aggs *structs.QueryAggregators,
	qid uint64, qs *summary.QuerySummary) error {

	for _, req := range allSegRequests {
		search.AggsFastPathWrapper(req, parallelismPerFile, searchNode, timeRange, sizeLimit,
			aggs, allSegFileResults, qid, qs)
	}
	return nil
}

// first bool is existience of non time aggs, second bool is existience of time agg
func checkAggTypes(aggs *structs.QueryAggregators) (bool, bool) {
	nonTime := false
	timeAgg := false
	if aggs != nil {
		if aggs.TimeHistogram != nil {
			timeAgg = true
		}
		if aggs.GroupByRequest != nil {
			nonTime = true
		}
	}
	return nonTime, timeAgg
}

func applyQsrsFilterHook(queryInfo *QueryInformation, qsrs []*QuerySegmentRequest, isRotated bool) ([]*QuerySegmentRequest, error) {
	if hook := hooks.GlobalHooks.FilterQsrsHook; hook != nil {
		qsrsAsAny, err := hook(qsrs, queryInfo, isRotated)
		if err != nil {
			log.Errorf("applyQsrsFilterHook: failed to apply hook: %v", err)
			return nil, err
		}

		var ok bool
		qsrs, ok = qsrsAsAny.([]*QuerySegmentRequest)
		if !ok {
			log.Errorf("applyQsrsFilterHook: got invalid type %T from hook", qsrsAsAny)
			return nil, err
		}
	}

	return qsrs, nil
}

func removeUsageForSegmentHook(qid uint64, segKey string) {
	if hook := hooks.GlobalHooks.RemoveUsageForRotatedSegmentForQidHook; hook != nil {
		hook(qid, segKey)
	}
}
