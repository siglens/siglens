/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package query

import (
	"errors"
	"fmt"
	"sort"
	"time"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/grpc/grpc_query"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/querytracker"
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
func InitQueryNode(getMyIds func() []uint64, extractKibanaRequestsFn func([]string, uint64) map[string]*structs.SegmentSearchRequest) error {
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
	initMetadataRefresh()
	initGlobalMetadataRefresh(getMyIds)
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
func InitQueryInfoRefresh(getMyIds func() []uint64) {
	go runQueryInfoRefreshLoop(getMyIds)
}

func InitQueryMetrics() {
	go queryMetricsLooper()
}

func queryMetricsLooper() {
	for {
		time.Sleep(1 * time.Minute)
		go func() {
			instrumentation.SetSegmentMicroindexCountGauge(metadata.GetTotalSMICount())
		}()
	}
}

func getQueryType(sNode *structs.SearchNode, aggs *structs.QueryAggregators) (structs.SearchNodeType, structs.QueryType) {
	if aggs != nil && aggs.GroupByRequest != nil {
		if aggs.GroupByRequest.MeasureOperations != nil && aggs.GroupByRequest.GroupByColumns == nil {
			return sNode.NodeType, structs.SegmentStatsCmd
		}
		if aggs != nil && aggs.GroupByRequest.MeasureOperations != nil && aggs.GroupByRequest.GroupByColumns != nil {
			return sNode.NodeType, structs.GroupByCmd
		}
	}
	if aggs != nil && aggs.MeasureOperations != nil && aggs.GroupByRequest == nil {
		return sNode.NodeType, structs.SegmentStatsCmd
	}
	return sNode.NodeType, structs.RRCCmd
}

func ApplyFilterOperator(node *structs.ASTNode, timeRange *dtu.TimeRange, aggs *structs.QueryAggregators,
	qid uint64, qc *structs.QueryContext) *structs.NodeResult {

	sTime := time.Now()
	searchNode := ConvertASTNodeToSearchNode(node, qid)
	kibanaIndices := qc.TableInfo.GetKibanaIndices()
	nonKibanaIndices := qc.TableInfo.GetQueryTables()
	containsKibana := false
	if len(kibanaIndices) != 0 {
		containsKibana = true
	}
	querytracker.UpdateQTUsage(nonKibanaIndices, searchNode, aggs)
	parallelismPerFile := int64(1)
	_, qType := getQueryType(searchNode, aggs)
	querySummary := summary.InitQuerySummary(summary.LOGS, qid)
	pqid := querytracker.GetHashForQuery(searchNode)
	defer querySummary.LogSummaryAndEmitMetrics(qid, pqid, containsKibana, qc.Orgid)
	allSegFileResults, err := segresults.InitSearchResults(qc.SizeLimit, aggs, qType, qid)
	if err != nil {
		log.Errorf("qid=%d Failed to InitSearchResults! error %+v", qid, err)
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}
	dqs := InitDistQueryService(querySummary, allSegFileResults)
	queryInfo, err := InitQueryInformation(searchNode, aggs, timeRange, qc.TableInfo,
		qc.SizeLimit, parallelismPerFile, qid, dqs, qc.Orgid)
	if err != nil {
		log.Errorf("qid=%d Failed to InitQueryInformation! error %+v", qid, err)
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}
	err = associateSearchInfoWithQid(qid, allSegFileResults, aggs, dqs, qType)
	if err != nil {
		log.Errorf("qid=%d Failed to associate search results with qid! Error: %+v", qid, err)
	}

	log.Infof("qid=%d, Extracted node type %v for query. ParallelismPerFile=%v. Starting search...",
		qid, searchNode.NodeType, parallelismPerFile)

	// Kibana requests will not honor time range sent in the query
	// TODO: distibuted kibana requests?
	applyKibanaFilterOperator(kibanaIndices, allSegFileResults, parallelismPerFile, searchNode,
		qc.SizeLimit, aggs, qid, querySummary)
	switch qType {
	case structs.SegmentStatsCmd:
		return getNodeResultsForSegmentStatsCmd(queryInfo, sTime, allSegFileResults, nil, querySummary, false, qc.Orgid)
	case structs.RRCCmd, structs.GroupByCmd:
		bucketLimit := MAX_GRP_BUCKS
		if aggs != nil {
			if aggs.BucketLimit != 0 && aggs.BucketLimit < MAX_GRP_BUCKS {
				bucketLimit = aggs.BucketLimit
			}
			aggs.BucketLimit = bucketLimit
		} else {
			aggs = structs.InitDefaultQueryAggregations()
			aggs.BucketLimit = bucketLimit
			queryInfo.aggs = aggs
		}
		return getNodeResultsForRRCCmd(queryInfo, sTime, allSegFileResults, querySummary, false, qc.Orgid)
	default:
		err := errors.New("unsupported query type")
		log.Errorf("qid=%d Failed to apply search! error %+v", qid, err)
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}
}

/*
Fast path for GRPC requests for unrotated requests

Assumptions:
  - no kibana indices
  - sNode may have regexes that needs to be compiled
*/
func ApplyUnrotatedQuery(sNode *structs.SearchNode, timeRange *dtu.TimeRange, aggs *structs.QueryAggregators,
	indexInfo *structs.TableInfo, sizeLimit uint64, qid uint64, orgid uint64) *structs.NodeResult {

	sTime := time.Now()
	sNode.AddQueryInfoForNode()

	querytracker.UpdateQTUsage(indexInfo.GetQueryTables(), sNode, aggs)
	_, qType := getQueryType(sNode, aggs)
	allSegFileResults, err := segresults.InitSearchResults(sizeLimit, aggs, qType, qid)
	if err != nil {
		log.Errorf("qid=%d Failed to InitSearchResults! error %+v", qid, err)
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}
	querySummary := summary.InitQuerySummary(summary.LOGS, qid)
	parallelismPerFile := int64(1)
	queryInfo, err := InitQueryInformation(sNode, aggs, timeRange, indexInfo,
		sizeLimit, parallelismPerFile, qid, nil, orgid)
	defer querySummary.LogSummaryAndEmitMetrics(qid, queryInfo.pqid, false, orgid)
	if err != nil {
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}
	err = associateSearchInfoWithQid(qid, allSegFileResults, aggs, nil, queryInfo.qType)
	if err != nil {
		log.Errorf("qid=%d Failed to associate search results with qid! Error: %+v", qid, err)
	}

	log.Infof("qid=%d, Extracted node type %v for query. ParallelismPerFile=%v. Starting search...",
		qid, sNode.NodeType, parallelismPerFile)
	switch queryInfo.qType {
	case structs.RRCCmd, structs.GroupByCmd:
		return getNodeResultsForRRCCmd(queryInfo, sTime, allSegFileResults, querySummary, true, orgid)
	case structs.SegmentStatsCmd:
		return getNodeResultsForSegmentStatsCmd(queryInfo, sTime, allSegFileResults, nil, querySummary, true, orgid)
	default:
		err := errors.New("unsupported query type")
		log.Errorf("qid=%d Failed to apply search! error %+v", qid, err)
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}
}

/*
Fast path for GRPC requests for rotated requests

Assumptions:
  - no kibana indices
  - sNode may have regexes that needs to be compiled
*/
func ApplyRotatedQuery(reqs []grpc_query.SegkeyRequest, sNode *structs.SearchNode, timeRange *dtu.TimeRange, aggs *structs.QueryAggregators,
	indexInfo *structs.TableInfo, sizeLimit uint64, qid uint64, orgid uint64) *structs.NodeResult {

	sTime := time.Now()
	sNode.AddQueryInfoForNode()

	querytracker.UpdateQTUsage(indexInfo.GetQueryTables(), sNode, aggs)
	_, qType := getQueryType(sNode, aggs)
	allSegFileResults, err := segresults.InitSearchResults(sizeLimit, aggs, qType, qid)
	if err != nil {
		log.Errorf("qid=%d Failed to InitSearchResults! error %+v", qid, err)
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}
	querySummary := summary.InitQuerySummary(summary.LOGS, qid)
	parallelismPerFile := int64(1)
	queryInfo, err := InitQueryInformation(sNode, aggs, timeRange, indexInfo,
		sizeLimit, parallelismPerFile, qid, nil, orgid)
	defer querySummary.LogSummaryAndEmitMetrics(qid, queryInfo.pqid, false, orgid)
	if err != nil {
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}
	err = associateSearchInfoWithQid(qid, allSegFileResults, aggs, nil, queryInfo.qType)
	if err != nil {
		log.Errorf("qid=%d Failed to associate search results with qid! Error: %+v", qid, err)
	}

	log.Infof("qid=%d, Extracted node type %v for query. ParallelismPerFile=%v. Starting search...",
		qid, sNode.NodeType, parallelismPerFile)
	switch queryInfo.qType {
	case structs.RRCCmd, structs.GroupByCmd:
		qsrs := convertSegKeysToQSR(queryInfo, reqs)
		qsrs, raw, pqs := filterSegKeysToQueryResults(queryInfo, qsrs)
		log.Infof("qid=%d, QueryType %+v Filtered %d segkeys to raw %d and %d pqs keys", qid, queryInfo.qType.String(), len(qsrs), raw, pqs)
		return getNodeResultsFromQSRS(qsrs, queryInfo, sTime, allSegFileResults, querySummary)
	case structs.SegmentStatsCmd:
		return getNodeResultsForSegmentStatsCmd(queryInfo, sTime, allSegFileResults, reqs,
			querySummary, false, orgid)
	default:
		err := errors.New("unsupported query type")
		log.Errorf("qid=%d Failed to apply search! error %+v", qid, err)
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}
}

func convertSegKeysToQSR(qI *queryInformation, segReqs []grpc_query.SegkeyRequest) []*querySegmentRequest {
	qsrs := make([]*querySegmentRequest, 0, len(segReqs))
	for _, segReq := range segReqs {
		qsrs = append(qsrs, &querySegmentRequest{
			queryInformation: *qI,
			segKey:           segReq.GetSegmentKey(),
			tableName:        segReq.GetTableName(),
			segKeyTsRange:    &dtu.TimeRange{StartEpochMs: segReq.GetStartEpochMs(), EndEpochMs: segReq.GetEndEpochMs()},
		})
	}
	return qsrs
}

func convertSegStatKeysToQSR(qI *queryInformation, segReqs []grpc_query.SegkeyRequest) []*querySegmentRequest {
	qsrs := make([]*querySegmentRequest, 0, len(segReqs))
	for _, segReq := range segReqs {
		qsrs = append(qsrs, &querySegmentRequest{
			queryInformation: *qI,
			segKey:           segReq.GetSegmentKey(),
			tableName:        segReq.GetTableName(),
			sType:            structs.SEGMENT_STATS_SEARCH,
			segKeyTsRange:    &dtu.TimeRange{StartEpochMs: segReq.GetStartEpochMs(), EndEpochMs: segReq.GetEndEpochMs()},
		})
	}
	return qsrs
}

// Base function to apply operators on query segment requests
func getNodeResultsFromQSRS(sortedQSRSlice []*querySegmentRequest, queryInfo *queryInformation, sTime time.Time,
	allSegFileResults *segresults.SearchResults, querySummary *summary.QuerySummary) *structs.NodeResult {
	applyFopAllRequests(sortedQSRSlice, queryInfo, allSegFileResults, querySummary)
	err := queryInfo.Wait(querySummary)
	if err != nil {
		log.Errorf("qid=%d Failed to wait for all query segment requests to finish! Error: %+v", queryInfo.qid, err)
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}
	querySummary.UpdateQueryTotalTime(time.Since(sTime), allSegFileResults.GetNumBuckets())
	setQidAsFinished(queryInfo.qid)
	queryType := GetQueryType(queryInfo.qid)
	bucketLimit := MAX_GRP_BUCKS
	if queryInfo.aggs != nil {
		if queryInfo.aggs.BucketLimit != 0 && queryInfo.aggs.BucketLimit < MAX_GRP_BUCKS {
			bucketLimit = queryInfo.aggs.BucketLimit
		}
	}
	aggMeasureRes, aggMeasureFunctions, aggGroupByCols, bucketCount := allSegFileResults.GetGroupyByBuckets(bucketLimit)
	return &structs.NodeResult{
		AllRecords:       allSegFileResults.GetResults(),
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

func getNodeResultsForRRCCmd(queryInfo *queryInformation, sTime time.Time, allSegFileResults *segresults.SearchResults,
	querySummary *summary.QuerySummary, unrotatedGRPC bool, orgid uint64) *structs.NodeResult {

	sortedQSRSlice, numRawSearch, distributedQueries, numPQS, err := getAllSegmentsInQuery(queryInfo, unrotatedGRPC, sTime, orgid)
	if err != nil {
		log.Errorf("qid=%d Failed to get all segments in query! Error: %+v", queryInfo.qid, err)
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}
	log.Infof("qid=%d, Received %+v query segment requests. %+v raw search %+v pqs and %+v distribued query elapsed time: %+v",
		queryInfo.qid, len(sortedQSRSlice), numRawSearch, numPQS, distributedQueries, time.Since(sTime))
	err = setTotalSegmentsToSearch(queryInfo.qid, numRawSearch+numPQS+distributedQueries)
	if err != nil {
		log.Errorf("qid=%d Failed to set total segments to search! Error: %+v", queryInfo.qid, err)
	}
	querySummary.UpdateRemainingDistributedQueries(distributedQueries)
	return getNodeResultsFromQSRS(sortedQSRSlice, queryInfo, sTime, allSegFileResults, querySummary)
}

func getNodeResultsForSegmentStatsCmd(queryInfo *queryInformation, sTime time.Time, allSegFileResults *segresults.SearchResults,
	reqs []grpc_query.SegkeyRequest, querySummary *summary.QuerySummary, unrotatedOnly bool, orgid uint64) *structs.NodeResult {
	sortedQSRSlice, numRawSearch, numDistributed := getAllSegmentsInAggs(queryInfo, reqs, queryInfo.aggs, queryInfo.queryRange, queryInfo.indexInfo.GetQueryTables(),
		queryInfo.qid, unrotatedOnly, sTime, orgid)
	err := setTotalSegmentsToSearch(queryInfo.qid, numRawSearch)
	if err != nil {
		log.Errorf("qid=%d Failed to set total segments to search! Error: %+v", queryInfo.qid, err)
	}
	querySummary.UpdateRemainingDistributedQueries(numDistributed)
	log.Infof("qid=%d, Received %+v query segment aggs, with %+v raw search %v distributed, query elapsed time: %+v",
		queryInfo.qid, len(sortedQSRSlice), numRawSearch, numDistributed, time.Since(sTime))
	if queryInfo.aggs.MeasureOperations != nil {
		allSegFileResults.InitSegmentStatsResults(queryInfo.aggs.MeasureOperations)
		applyAggOpOnSegments(sortedQSRSlice, allSegFileResults, queryInfo.qid, querySummary, queryInfo.sNodeType, queryInfo.aggs.MeasureOperations)
	}
	querySummary.UpdateQueryTotalTime(time.Since(sTime), allSegFileResults.GetNumBuckets())
	queryType := GetQueryType(queryInfo.qid)
	aggMeasureRes, aggMeasureFunctions, aggGroupByCols, bucketCount := allSegFileResults.GetSegmentStatsResults(0)
	err = queryInfo.Wait(querySummary)
	if err != nil {
		log.Errorf("qid=%d getNodeResultsForSegmentStatsCmd: Failed to wait for all query segment requests to finish! Error: %+v", queryInfo.qid, err)
		return &structs.NodeResult{
			ErrList: []error{err},
		}
	}
	setQidAsFinished(queryInfo.qid)
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

func getSortedQSRResult(aggs *structs.QueryAggregators, allQSRs []*querySegmentRequest) []*querySegmentRequest {
	if aggs != nil && aggs.Sort != nil {
		if aggs.Sort.Ascending {
			// index 0 should have the latest time
			sort.Slice(allQSRs, func(i, j int) bool {
				return allQSRs[i].segKeyTsRange.StartEpochMs < allQSRs[j].segKeyTsRange.StartEpochMs
			})
		} else {
			// index 0 should have the earliest time
			sort.Slice(allQSRs, func(i, j int) bool {
				return allQSRs[i].segKeyTsRange.StartEpochMs > allQSRs[j].segKeyTsRange.StartEpochMs
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
	err := applyFilterOperatorInternal(allSegFileResults, kibanaSearchRequests, parallelismPerFile, searchNode, tRange,
		sizeLimit, aggs, qid, qs)
	if err != nil {
		log.Errorf("qid=%d, applyKibanaFilterOperator failed to apply filter opterator for kibana requests! %+v", qid, err)
		allSegFileResults.AddError(err)
	}
}

func reverseSortedQSRSlice(sortedQSRSlice []*querySegmentRequest) {
	lenSortedQSRSlice := len(sortedQSRSlice)

	for i := 0; i < lenSortedQSRSlice/2; i++ {
		sortedQSRSlice[i], sortedQSRSlice[lenSortedQSRSlice-i-1] = sortedQSRSlice[lenSortedQSRSlice-i-1], sortedQSRSlice[i]
	}
}

// loops over all inputted querySegmentRequests and apply search for each file. This function may exit early
func applyFopAllRequests(sortedQSRSlice []*querySegmentRequest, queryInfo *queryInformation,
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
	var err error
	if len(sortedQSRSlice) > 0 && queryInfo.persistentQuery {
		allEmptySegsForPqid, err = pqsmeta.GetAllEmptySegmentsForPqid(sortedQSRSlice[0].pqid)
		if err != nil {
			log.Errorf("qid=%d, Failed to get empty segments for pqid %+v! Error: %v", queryInfo.qid, sortedQSRSlice[0].pqid, err)
		}
	}

	// If we have a Transaction command, we want to search the segments from the oldest to the newest
	if queryInfo.aggs != nil && queryInfo.aggs.HasTransactionArgumentsInChain() {
		reverseSortedQSRSlice(sortedQSRSlice)
	}

	for idx, segReq := range sortedQSRSlice {

		isCancelled, err := checkForCancelledQuery(queryInfo.qid)
		if err != nil {
			log.Errorf("qid=%d, Failed to checkForCancelledQuery. Error: %v", queryInfo.qid, err)
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
				numRecs := metadata.GetNumOfSearchedRecordsRotated(segReq.segKey)
				queryMetrics.SetNumRecordsToRawSearch(numRecs)
				queryMetrics.SetNumRecordsMatched(numRecs)
				qs.UpdateSummary(summary.STREE, timeElapsed, queryMetrics)
			} else {
				if segReq.sType == structs.PQS {
					_, ok := allEmptySegsForPqid[segReq.segKey]
					if ok {
						log.Debugf("Skipping segKey %v for pqid %v", segReq.segKey, segReq.queryInformation.pqid)
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
			recsSearched = metadata.GetNumOfSearchedRecordsRotated(segReq.segKey)
		} else {
			recsSearched = writer.GetNumOfSearchedRecordsUnRotated(segReq.segKey)
		}
		if idx == len(sortedQSRSlice)-1 {
			doBuckPull = true
		}
		recsSearchedSinceLastUpdate += recsSearched
		if segReq.HasMatchedRrc {
			segsNotSent++
			segenc := allSegFileResults.SegKeyToEnc[segReq.segKey]
			incrementNumFinishedSegments(segsNotSent, queryInfo.qid, recsSearchedSinceLastUpdate, segenc,
				doBuckPull, nil)
			segsNotSent = 0
			recsSearchedSinceLastUpdate = 0
		} else {
			segsNotSent++
		}
		if !rrcsCompleted && areAllRRCsFound(allSegFileResults, sortedQSRSlice[idx+1:], segReq.aggs) {
			qs.SetRRCFinishTime()
			rrcsCompleted = true
		}
	}

	if segsNotSent > 0 {
		doBucketPull := true // This is the last update, so flush the buckets.
		incrementNumFinishedSegments(segsNotSent, queryInfo.qid, recsSearchedSinceLastUpdate, 0, doBucketPull, nil)
	}

	if !rrcsCompleted {
		qs.SetRRCFinishTime()
	}
	if len(sortedQSRSlice) == 0 {
		incrementNumFinishedSegments(0, queryInfo.qid, recsSearchedSinceLastUpdate, 0, false, nil)
	}
}

// Return true if we can use AgileAggsTrees for all the segments and we can
// limit the number of buckets.
func canUseBucketLimitedAgileAggsTree(sortedQSRSlice []*querySegmentRequest, queryInfo *queryInformation) bool {
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

func canUseAgileTree(segReq *querySegmentRequest, queryInfo *queryInformation) (bool, *segread.AgileTreeReader) {
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
func areAllRRCsFound(sr *segresults.SearchResults, qsrs []*querySegmentRequest, aggs *structs.QueryAggregators) bool {

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
func getAllUnrotatedSegments(queryInfo *queryInformation, unrotatedGRPC bool, sTime time.Time, orgid uint64) ([]*querySegmentRequest, uint64, uint64, uint64, error) {
	allUnrotatedKeys, totalChecked, totalCount := writer.FilterUnrotatedSegmentsInQuery(queryInfo.queryRange, queryInfo.indexInfo.GetQueryTables(), orgid)
	log.Infof("qid=%d, Unrotated query time filtering returned %v segment keys to search out of %+v. query elapsed time: %+v", queryInfo.qid, totalCount,
		totalChecked, time.Since(sTime))

	var distCount uint64
	var err error
	if !unrotatedGRPC {
		distCount, err = queryInfo.dqs.DistributeUnrotatedQuery(queryInfo)
		if err != nil {
			log.Errorf("qid=%d, Failed to send unrotated query request! Error: %v", queryInfo.qid, err)
			return nil, 0, 0, 0, err
		}
	}
	qsr, raw, pqs := filterUnrotatedSegKeysToQueryRequests(queryInfo, allUnrotatedKeys)
	return qsr, raw, distCount, pqs, nil
}

// returns query segment requests, count of keys to raw search, and distributed query count
func getAllSegmentsInAggs(queryInfo *queryInformation, reqs []grpc_query.SegkeyRequest, aggs *structs.QueryAggregators, timeRange *dtu.TimeRange, indexNames []string,
	qid uint64, unrotatedGRPC bool, sTime time.Time, orgid uint64) ([]*querySegmentRequest, uint64, uint64) {

	if len(reqs) != 0 {
		qsrs := convertSegStatKeysToQSR(queryInfo, reqs)
		return qsrs, uint64(len(qsrs)), 0
	}
	if unrotatedGRPC {
		unrotatedQSR, unrotatedRawCount, unrotatedDistQueries := getAllUnrotatedSegmentsInAggs(queryInfo, aggs, timeRange, indexNames, qid, sTime, orgid, unrotatedGRPC)
		return unrotatedQSR, unrotatedRawCount, unrotatedDistQueries
	}

	// Do rotated time & index name filtering
	rotatedQSR, rotatedRawCount, rotatedDistQueries := getAllRotatedSegmentsInAggs(queryInfo, aggs, timeRange, indexNames, qid, sTime, orgid)
	unrotatedQSR, unrotatedRawCount, unrotatedDistQueries := getAllUnrotatedSegmentsInAggs(queryInfo, aggs, timeRange, indexNames, qid, sTime, orgid, unrotatedGRPC)
	allSegRequests := append(rotatedQSR, unrotatedQSR...)
	//get seg stats for allPossibleKeys
	return allSegRequests, rotatedRawCount + unrotatedRawCount, rotatedDistQueries + unrotatedDistQueries
}

func getAllUnrotatedSegmentsInAggs(queryInfo *queryInformation, aggs *structs.QueryAggregators, timeRange *dtu.TimeRange, indexNames []string,
	qid uint64, sTime time.Time, orgid uint64, unrotatedGRPC bool) ([]*querySegmentRequest, uint64, uint64) {
	allUnrotatedKeys, totalChecked, totalCount := writer.FilterUnrotatedSegmentsInQuery(timeRange, indexNames, orgid)
	log.Infof("qid=%d, Unrotated query time filtering returned %v segment keys to search out of %+v. query elapsed time: %+v", qid, totalCount,
		totalChecked, time.Since(sTime))
	var distCount uint64
	var err error
	if !unrotatedGRPC {
		distCount, err = queryInfo.dqs.DistributeUnrotatedQuery(queryInfo)
		if err != nil {
			log.Errorf("qid=%d, Failed to send unrotated query request! Error: %v", queryInfo.qid, err)
			return nil, 0, 0
		}
	}
	qsrs, rawSearch := filterAggSegKeysToQueryResults(queryInfo, allUnrotatedKeys, aggs, structs.UNROTATED_SEGMENT_STATS_SEARCH)
	return qsrs, rawSearch, distCount
}

func getAllRotatedSegmentsInAggs(queryInfo *queryInformation, aggs *structs.QueryAggregators, timeRange *dtu.TimeRange, indexNames []string,
	qid uint64, sTime time.Time, orgid uint64) ([]*querySegmentRequest, uint64, uint64) {
	// 1. metadata.FilterSegmentsByTime gives epoch range
	allPossibleKeys, tsPassedCount, totalPossible := metadata.FilterSegmentsByTime(timeRange, indexNames, orgid)
	log.Infof("qid=%d, Rotated query time filtering returned %v segment keys to search out of %+v. query elapsed time: %+v", qid, tsPassedCount,
		totalPossible, time.Since(sTime))

	qsrs, totalQsr := filterAggSegKeysToQueryResults(queryInfo, allPossibleKeys, aggs, structs.SEGMENT_STATS_SEARCH)
	currNodeQsrs, distributedRequests, err := queryInfo.dqs.DistributeRotatedRequests(queryInfo, qsrs)
	if err != nil {
		log.Errorf("qid=%d, Error in distributing rotated requests %+v", queryInfo.qid, err)
		return nil, 0, 0
	}
	return currNodeQsrs, totalQsr - distributedRequests, distributedRequests
}

func applyAggOpOnSegments(sortedQSRSlice []*querySegmentRequest, allSegFileResults *segresults.SearchResults, qid uint64, qs *summary.QuerySummary,
	searchType structs.SearchNodeType, measureOperations []*structs.MeasureAggregator) {
	// Use a global variable to store data that meets the conditions during the process of traversing segments
	runningEvalStats := make(map[string]interface{}, 0)
	//assuming we will allow 100 measure Operations
	for _, segReq := range sortedQSRSlice {
		isCancelled, err := checkForCancelledQuery(qid)
		if err != nil {
			log.Errorf("qid=%d, Failed to checkForCancelledQuery. Error: %v", qid, err)
		}
		if isCancelled {
			break
		}
		isSegmentFullyEnclosed := segReq.segKeyTsRange.AreTimesFullyEnclosed(segReq.segKeyTsRange.StartEpochMs, segReq.segKeyTsRange.EndEpochMs)

		// Because segment only store statistical data such as min, max..., for some functions we should recompute raw data to get the results
		// If agg has evaluation functions, we should recompute raw data instead of using the previously stored statistical data in the segment
		aggHasEvalFunc := segReq.aggs.HasValueColRequest()
		aggHasValuesFunc := segReq.aggs.HasValuesFunc()
		var sstMap map[string]*structs.SegStats
		if searchType == structs.MatchAllQuery && isSegmentFullyEnclosed && !aggHasEvalFunc && !aggHasValuesFunc {
			sstMap, err = segread.ReadSegStats(segReq.segKey, segReq.qid)
			if err != nil {
				log.Errorf("qid=%d,  applyAggOpOnSegments : ReadSegStats: Failed to get segment level stats for segKey %+v! Error: %v", qid, segReq.segKey, err)
				allSegFileResults.AddError(err)
				continue
			}
		} else {
			// run through micro index check for block tracker & generate SSR
			blocksToRawSearch, err := segReq.GetMicroIndexFilter()
			if err != nil {
				log.Errorf("qid=%d, failed to get blocks to raw search! Defaulting to searching all blocks. SegKey %+v", segReq.qid, segReq.segKey)
				blocksToRawSearch = segReq.GetEntireFileMicroIndexFilter()
			}
			sTime := time.Now()
			isQueryPersistent, err := querytracker.IsQueryPersistent([]string{segReq.tableName}, segReq.sNode)
			if err != nil {
				log.Errorf("qid=%d, applyAggOpOnSegments: Failed to check if query is persistent! Error: %v", qid, err)
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
				sstMap, err = search.RawComputeSegmentStats(req, segReq.parallelismPerFile, segReq.sNode, segReq.segKeyTsRange, segReq.aggs.MeasureOperations, allSegFileResults, qid, qs)
				if err != nil {
					log.Errorf("qid=%d,  applyAggOpOnSegments : ReadSegStats: Failed to get segment level stats for segKey %+v! Error: %v", qid, segReq.segKey, err)
					allSegFileResults.AddError(err)
				}
			}
		}
		err = allSegFileResults.UpdateSegmentStats(sstMap, measureOperations, runningEvalStats)
		if err != nil {
			log.Errorf("qid=%d,  applyAggOpOnSegments : ReadSegStats: Failed to update segment stats for segKey %+v! Error: %v", qid, segReq.segKey, err)
			allSegFileResults.AddError(err)
			continue
		}
		totalRecsSearched := uint64(0)
		if segReq.sType == structs.SEGMENT_STATS_SEARCH {
			totalRecsSearched = metadata.GetNumOfSearchedRecordsRotated(segReq.segKey)
		} else if segReq.sType == structs.UNROTATED_SEGMENT_STATS_SEARCH {
			totalRecsSearched = writer.GetNumOfSearchedRecordsUnRotated(segReq.segKey)
		}
		segenc := allSegFileResults.GetAddSegEnc(segReq.segKey)
		incrementNumFinishedSegments(1, qid, totalRecsSearched, segenc, true, sstMap)
	}

	if len(sortedQSRSlice) == 0 {
		incrementNumFinishedSegments(0, qid, 0, 0, true, nil)
	}
}

// return sorted slice of querySegmentRequests, count of raw search requests, distributed queries, and count of pqs request
func getAllSegmentsInQuery(queryInfo *queryInformation, unrotatedGRPC bool, sTime time.Time, orgid uint64) ([]*querySegmentRequest, uint64, uint64, uint64, error) {
	if unrotatedGRPC {
		unrotatedQSR, unrotatedRawCount, unrotatedDistQueries, unrotatedPQSCount, err := getAllUnrotatedSegments(queryInfo, unrotatedGRPC, sTime, orgid)
		if err != nil {
			return nil, 0, 0, 0, err
		}
		sortedQSRSlice := getSortedQSRResult(queryInfo.aggs, unrotatedQSR)
		return sortedQSRSlice, unrotatedRawCount, unrotatedDistQueries, unrotatedPQSCount, nil
	}

	unrotatedQSR, unrotatedRawCount, unrotatedDistQueries, unrotatedPQSCount, err := getAllUnrotatedSegments(queryInfo, unrotatedGRPC, sTime, orgid)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	rotatedQSR, rotatedRawCount, rotatedDistQueries, rotatedPQS, err := getAllRotatedSegmentsInQuery(queryInfo, sTime, orgid)
	if err != nil {
		return nil, 0, 0, 0, err
	}
	allSegRequests := append(rotatedQSR, unrotatedQSR...)
	// Sort query segment results depending on aggs
	sortedQSRSlice := getSortedQSRResult(queryInfo.aggs, allSegRequests)
	return sortedQSRSlice, rotatedRawCount + unrotatedRawCount, unrotatedDistQueries + rotatedDistQueries, unrotatedPQSCount + rotatedPQS, nil
}

// returns sorted order of querySegmentRequests, count of keys to raw search, count of distributed queries, and count of pqs keys to raw search
func getAllRotatedSegmentsInQuery(queryInfo *queryInformation, sTime time.Time, orgid uint64) ([]*querySegmentRequest, uint64, uint64, uint64, error) {
	// 1. metadata.FilterSegmentsByTime gives epoch range
	allPossibleKeys, tsPassedCount, totalPossible := metadata.FilterSegmentsByTime(queryInfo.queryRange, queryInfo.indexInfo.GetQueryTables(), orgid)
	log.Infof("qid=%d, Rotated query time filtering returned %v segment keys to search out of %+v. query elapsed time: %+v", queryInfo.qid, tsPassedCount,
		totalPossible, time.Since(sTime))

	qsrs := convertSegKeysToQueryRequests(queryInfo, allPossibleKeys)
	currNodeQsrs, distributedRequests, err := queryInfo.dqs.DistributeRotatedRequests(queryInfo, qsrs)
	if err != nil {
		log.Errorf("qid=%d, Error in distributing rotated requests %+v", queryInfo.qid, err)
		return nil, 0, 0, 0, err
	}

	// 2. Whatever needed sorting of segKeys based on sorts & generation into querySegmentRequest
	qsr, raw, pqs := filterSegKeysToQueryResults(queryInfo, currNodeQsrs)
	return qsr, raw, distributedRequests, pqs, nil
}

func applyFilterOperatorSingleRequest(qsr *querySegmentRequest, allSegFileResults *segresults.SearchResults, qs *summary.QuerySummary) error {
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
	}
	return fmt.Errorf("unsupported segment type %+v", qsr.sType)
}

func applyFilterOperatorPQSRequest(qsr *querySegmentRequest, allSegFileResults *segresults.SearchResults, qs *summary.QuerySummary) error {
	spqmr, err := pqs.GetAllPersistentQueryResults(qsr.segKey, qsr.queryInformation.pqid)
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

	// Get time range/blocks missing from sqpmr from metadata layer.
	missingTRange := metadata.GetTSRangeForMissingBlocks(qsr.segKey, qsr.segKeyTsRange, spqmr)
	if missingTRange == nil || !allSegFileResults.ShouldSearchRange(missingTRange.StartEpochMs, missingTRange.EndEpochMs) {
		return nil
	}
	qsr.sType = structs.RAW_SEARCH
	qsr.blkTracker = structs.InitExclusionBlockTracker(spqmr) // blocks not found in pqs, that we need to raw search for a key
	return applyFilterOperatorRawSearchRequest(qsr, allSegFileResults, qs)
}

func applyFilterOperatorRawSearchRequest(qsr *querySegmentRequest, allSegFileResults *segresults.SearchResults, qs *summary.QuerySummary) error {
	// run through micro index check for block tracker & generate SSR
	blocksToRawSearch, err := qsr.GetMicroIndexFilter()
	if err != nil {
		log.Errorf("qid=%d, failed to get blocks to raw search! Defaulting to searching all blocks. SegKey %+v", qsr.qid, qsr.segKey)
		blocksToRawSearch = qsr.GetEntireFileMicroIndexFilter()
	}

	isQueryPersistent, err := querytracker.IsQueryPersistent([]string{qsr.tableName}, qsr.sNode)
	if err != nil {
		log.Errorf("qid=%d, failed to check if query is persistent", qsr.qid)
	}

	sTime := time.Now()
	rawSearchSSR := ExtractSSRFromSearchNode(qsr.sNode, blocksToRawSearch, qsr.queryRange, qsr.indexInfo.GetQueryTables(), qs, qsr.qid, isQueryPersistent, qsr.pqid)
	qs.UpdateExtractSSRTime(time.Since(sTime))
	for _, req := range rawSearchSSR {
		req.SType = qsr.sType
	}
	err = applyFilterOperatorInternal(allSegFileResults, rawSearchSSR, qsr.parallelismPerFile, qsr.sNode, qsr.queryRange,
		qsr.sizeLimit, qsr.aggs, qsr.qid, qs)

	for _, req := range rawSearchSSR {
		if req.HasMatchedRrc {
			qsr.HasMatchedRrc = true
			break
		}
	}
	return err
}

func applyFilterOperatorUnrotatedPQSRequest(qsr *querySegmentRequest, allSegFileResults *segresults.SearchResults, qs *summary.QuerySummary) error {
	spqmr, err := writer.GetAllPersistentQueryResults(qsr.segKey, qsr.queryInformation.pqid)
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

func applyFilterOperatorUnrotatedRawSearchRequest(qsr *querySegmentRequest, allSegFileResults *segresults.SearchResults, qs *summary.QuerySummary) error {
	// run through micro index check for block tracker & generate SSR
	blocksToRawSearch, err := qsr.GetMicroIndexFilter()
	if err != nil {
		log.Errorf("qid=%d, failed to get blocks to raw search! Defaulting to searching all blocks. SegKey %+v", qsr.qid, qsr.segKey)
		blocksToRawSearch = qsr.GetEntireFileMicroIndexFilter()
	}
	sTime := time.Now()
	rawSearchSSR := metadata.ExtractUnrotatedSSRFromSearchNode(qsr.sNode, qsr.queryRange, qsr.indexInfo.GetQueryTables(), blocksToRawSearch, qs, qsr.qid)
	qs.UpdateExtractSSRTime(time.Since(sTime))
	for _, req := range rawSearchSSR {
		req.SType = qsr.sType
	}
	err = applyFilterOperatorInternal(allSegFileResults, rawSearchSSR, qsr.parallelismPerFile, qsr.sNode, qsr.queryRange,
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
func applyFilterOperatorInternal(allSegFileResults *segresults.SearchResults, allSegRequests map[string]*structs.SegmentSearchRequest,
	parallelismPerFile int64, searchNode *structs.SearchNode, timeRange *dtu.TimeRange, sizeLimit uint64, aggs *structs.QueryAggregators,
	qid uint64, qs *summary.QuerySummary) error {
	for _, req := range allSegRequests {
		search.RawSearchSegmentFileWrapper(req, parallelismPerFile, searchNode, timeRange, sizeLimit, aggs, allSegFileResults, qid, qs)
	}

	return nil
}

// Returns sorted order of query segment requests, count of keys to raw search
func filterAggSegKeysToQueryResults(qInfo *queryInformation, allPossibleKeys map[string]map[string]*dtu.TimeRange,
	aggs *structs.QueryAggregators, segType structs.SegType) ([]*querySegmentRequest, uint64) {

	allAggSegmentRequests := make([]*querySegmentRequest, 0)
	aggSearchCount := uint64(0)
	for tableName, segKeys := range allPossibleKeys {
		for segKey, tsRange := range segKeys {
			if tsRange == nil {
				log.Errorf("qid=%d, filterAggSegKeysToQueryResults reieved an empty segment time range. SegKey %+v", qInfo.qid, segKey)
				continue
			}
			qReq := &querySegmentRequest{
				queryInformation: *qInfo,
				segKey:           segKey,
				segKeyTsRange:    tsRange,
				tableName:        tableName,
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
func filterSegKeysToQueryResults(qInfo *queryInformation, qsegs []*querySegmentRequest) ([]*querySegmentRequest, uint64, uint64) {
	pqsCount := uint64(0)
	rawSearchCount := uint64(0)
	for _, qReq := range qsegs {
		if pqs.DoesSegKeyHavePqidResults(qReq.segKey, qInfo.pqid) {
			qReq.sType = structs.PQS
			pqsCount++
		} else {
			qReq.sType = structs.RAW_SEARCH
			qReq.blkTracker = structs.InitEntireFileBlockTracker()
			rawSearchCount++
		}
	}

	return qsegs, rawSearchCount, pqsCount
}

func convertSegKeysToQueryRequests(qInfo *queryInformation, allPossibleKeys map[string]map[string]*dtu.TimeRange) []*querySegmentRequest {
	allSegRequests := make([]*querySegmentRequest, 0)
	for tableName, segKeys := range allPossibleKeys {
		for segKey, tsRange := range segKeys {
			if tsRange == nil {
				log.Errorf("qid=%d, FilterSegKeysToQueryResults reieved an empty segment time range. SegKey %+v", qInfo.qid, segKey)
				continue
			}
			qReq := &querySegmentRequest{
				queryInformation: *qInfo,
				segKey:           segKey,
				segKeyTsRange:    tsRange,
				tableName:        tableName,
			}
			allSegRequests = append(allSegRequests, qReq)
		}
	}

	return allSegRequests
}

// Returns query segment requests, count of keys to raw search, count of keys in PQS
func filterUnrotatedSegKeysToQueryRequests(qInfo *queryInformation, allPossibleKeys map[string]map[string]*dtu.TimeRange) ([]*querySegmentRequest, uint64, uint64) {

	allSegRequests := make([]*querySegmentRequest, 0)
	pqsCount := uint64(0)
	rawSearchCount := uint64(0)
	for tableName, segKeys := range allPossibleKeys {
		for segKey, tsRange := range segKeys {
			if tsRange == nil {
				log.Errorf("qid=%d, FilterSegKeysToQueryResults reieved an empty segment time range. SegKey %+v", qInfo.qid, segKey)
				continue
			}
			qReq := &querySegmentRequest{
				queryInformation: *qInfo,
				segKey:           segKey,
				segKeyTsRange:    tsRange,
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
func applyPQSToRotatedRequest(qsr *querySegmentRequest, allSearchResults *segresults.SearchResults, spqmr *pqmr.SegmentPQMRResults, qs *summary.QuerySummary) error {

	searchMetadata, blkSummaries, err := metadata.GetSearchInfoForPQSQuery(qsr.segKey, spqmr)
	if err != nil {
		log.Errorf("qid=%d, applyRawSearchToPQSMatches: failed to get search info for pqs query %+v. Error: %+v",
			qsr.qid, qsr.segKey, err)
		return err
	}

	return applySinglePQSRawSearch(qsr, allSearchResults, spqmr, searchMetadata, blkSummaries, qs)
}

// gets search metadata for a segKey and runs raw search
func applyPQSToUnrotatedRequest(qsr *querySegmentRequest, allSearchResults *segresults.SearchResults, spqmr *pqmr.SegmentPQMRResults, qs *summary.QuerySummary) error {

	searchMetadata, blkSummaries, err := writer.GetSearchInfoForPQSQuery(qsr.segKey, spqmr)
	if err != nil {
		log.Errorf("qid=%d, applyRawSearchToPQSMatches: failed to get search info for pqs query %+v. Error: %+v",
			qsr.qid, qsr.segKey, err)
		return err
	}
	return applySinglePQSRawSearch(qsr, allSearchResults, spqmr, searchMetadata, blkSummaries, qs)
}

func applySinglePQSRawSearch(qsr *querySegmentRequest, allSearchResults *segresults.SearchResults, spqmr *pqmr.SegmentPQMRResults, searchMetadata map[uint16]*structs.BlockMetadataHolder,
	blkSummaries []*structs.BlockSummary, qs *summary.QuerySummary) error {
	if len(searchMetadata) == 0 {
		log.Infof("qid=%d, applyRawSearchToPQSMatches: segKey %+v has 0 blocks in segment PQMR results", qsr.qid, qsr.segKey)
		return nil
	}
	req := &structs.SegmentSearchRequest{
		SegmentKey:         qsr.segKey,
		VirtualTableName:   qsr.tableName,
		AllPossibleColumns: qsr.colsToSearch,
		AllBlocksToSearch:  searchMetadata,
		SearchMetadata: &structs.SearchMetadataHolder{
			BlockSummaries:    blkSummaries,
			SearchTotalMemory: uint64(len(blkSummaries) * 16), // TODO: add bitset size here
		},
	}
	search.RawSearchPQMResults(req, qsr.parallelismPerFile, qsr.queryRange, qsr.aggs, qsr.sizeLimit, spqmr, allSearchResults, qsr.qid, qs)

	if req.HasMatchedRrc {
		qsr.HasMatchedRrc = true
	}
	return nil
}

func applyFopFastPathSingleRequest(qsr *querySegmentRequest, allSegFileResults *segresults.SearchResults, qs *summary.QuerySummary) error {

	// run through micro index check for block tracker & generate SSR
	blocksToRawSearch, err := qsr.GetMicroIndexFilter()
	if err != nil {
		log.Errorf("qid=%d, applyFopFastPathSingleRequest failed to get blocks, Defaulting to searching all blocks. SegKey %+v", qsr.qid, qsr.segKey)
		blocksToRawSearch = qsr.GetEntireFileMicroIndexFilter()
	}

	sTime := time.Now()
	isQueryPersistent, err := querytracker.IsQueryPersistent([]string{qsr.tableName}, qsr.sNode)
	if err != nil {
		log.Errorf("qid=%d, applyFopFastPathSingleRequest: Failed to check if query is persistent!", qsr.qid)
	}
	rawSearchSSR := ExtractSSRFromSearchNode(qsr.sNode, blocksToRawSearch, qsr.queryRange, qsr.indexInfo.GetQueryTables(), qs, qsr.qid, isQueryPersistent, qsr.pqid)
	qs.UpdateExtractSSRTime(time.Since(sTime))
	for _, req := range rawSearchSSR {
		req.SType = qsr.sType
	}

	err = applyFopFastPathInternal(allSegFileResults, rawSearchSSR, qsr.parallelismPerFile, qsr.sNode, qsr.queryRange,
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
