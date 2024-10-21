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
	"container/heap"
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/results/blockresults"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	putils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type QueryState int

var numStates = 4

const MAX_GRP_BUCKS = 3000
const CANCEL_QUERY_AFTER_SECONDS = 5 * 60 // If 0, the query will never timeout

type QueryUpdateType int

const (
	QUERY_UPDATE_LOCAL QueryUpdateType = iota + 1
	QUERY_UPDATE_REMOTE
)

type QueryUpdate struct {
	QUpdate   QueryUpdateType
	SegKeyEnc uint16
	RemoteID  string
}

type QueryStateChanData struct {
	StateName       QueryState
	QueryUpdate     *QueryUpdate
	PercentComplete float64
}

const (
	RUNNING      QueryState = iota + 1
	QUERY_UPDATE            // flush segment counts & aggs & records (if matched)
	COMPLETE
	CANCELLED
	TIMEOUT
	ERROR
)

func (qs QueryState) String() string {
	switch qs {
	case RUNNING:
		return "RUNNING"
	case QUERY_UPDATE:
		return "QUERY_UPDATE"
	case COMPLETE:
		return "COMPLETE"
	case CANCELLED:
		return "CANCELLED"
	case TIMEOUT:
		return "TIMEOUT"
	case ERROR:
		return "ERROR"
	default:
		return fmt.Sprintf("UNKNOWN_QUERYSTATE_%d", qs)
	}
}

type RunningQueryState struct {
	isAsync                  bool
	isCancelled              bool
	timeoutCancelFunc        context.CancelFunc
	StateChan                chan *QueryStateChanData // channel to send state changes of query
	cleanupCallback          func()
	orgid                    uint64
	tableInfo                *structs.TableInfo
	timeRange                *dtu.TimeRange
	searchRes                *segresults.SearchResults
	rawRecords               []*utils.RecordResultContainer
	queryCount               *structs.QueryCount
	aggs                     *structs.QueryAggregators
	searchHistogram          map[string]*structs.AggregationResult
	QType                    structs.QueryType
	rqsLock                  *sync.Mutex
	dqs                      DistributedQueryServiceInterface
	totalSegments            uint64
	finishedSegments         uint64
	totalRecsSearched        uint64
	rawSearchIsFinished      bool
	currentSearchResultCount int
	nodeResult               *structs.NodeResult
	totalRecsToBeSearched    uint64
	AllColsInAggs            map[string]struct{}
	NewPipeLineResponse 	*structs.PipeSearchResponseOuter
}

var allRunningQueries = map[uint64]*RunningQueryState{}
var arqMapLock *sync.RWMutex = &sync.RWMutex{}

func (rQuery *RunningQueryState) IsAsync() bool {
	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	return rQuery.isAsync
}

func (rQuery *RunningQueryState) SendQueryStateComplete() {
	rQuery.StateChan <- &QueryStateChanData{StateName: COMPLETE}

	if rQuery.cleanupCallback != nil {
		rQuery.cleanupCallback()
	}
}

// Starts tracking the query state. If async is true, the RunningQueryState.StateChan will be defined & will be sent updates
// If async, updates will be sent for any update to RunningQueryState. Caller is responsible to call DeleteQuery
func StartQuery(qid uint64, async bool, cleanupCallback func()) (*RunningQueryState, error) {
	arqMapLock.Lock()
	defer arqMapLock.Unlock()
	if _, ok := allRunningQueries[qid]; ok {
		log.Errorf("StartQuery: qid %+v already exists!", qid)
		return nil, fmt.Errorf("qid has already been started")
	}

	var stateChan chan *QueryStateChanData
	if async {
		stateChan = make(chan *QueryStateChanData, numStates)
		stateChan <- &QueryStateChanData{StateName: RUNNING}
	}

	var timeoutCancelFunc context.CancelFunc
	// If the query runs too long, cancel it.
	if CANCEL_QUERY_AFTER_SECONDS != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(CANCEL_QUERY_AFTER_SECONDS)*time.Second)
		timeoutCancelFunc = cancel

		go func() {
			<-ctx.Done()
			arqMapLock.RLock()
			rQuery, ok := allRunningQueries[qid]
			arqMapLock.RUnlock()

			if ok && ctx.Err() == context.DeadlineExceeded {
				log.Infof("qid=%v Canceling query due to timeout (%v seconds)", qid, CANCEL_QUERY_AFTER_SECONDS)
				rQuery.StateChan <- &QueryStateChanData{StateName: TIMEOUT}
				CancelQuery(qid)
			}
		}()
	}

	runningState := &RunningQueryState{
		StateChan:         stateChan,
		cleanupCallback:   cleanupCallback,
		rqsLock:           &sync.Mutex{},
		isAsync:           async,
		timeoutCancelFunc: timeoutCancelFunc,
	}
	allRunningQueries[qid] = runningState
	return runningState, nil
}

// Removes reference to qid. If qid does not exist this is a noop
func DeleteQuery(qid uint64) {
	LogGlobalSearchErrors(qid)
	arqMapLock.Lock()
	rQuery, ok := allRunningQueries[qid]
	if ok {
		delete(allRunningQueries, qid)
	}
	arqMapLock.Unlock()

	if ok && !rQuery.isCancelled {
		rQuery.timeoutCancelFunc()

		if rQuery.cleanupCallback != nil {
			rQuery.cleanupCallback()
		}
	}
}

func AssociateSearchInfoWithQid(qid uint64, result *segresults.SearchResults, aggs *structs.QueryAggregators, dqs DistributedQueryServiceInterface,
	qType structs.QueryType) error {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("associateSearchResultWithQid: qid %+v does not exist!", qid)
		return fmt.Errorf("qid does not exist")
	}

	rQuery.rqsLock.Lock()
	rQuery.searchRes = result
	rQuery.aggs = aggs
	rQuery.dqs = dqs
	rQuery.QType = qType
	rQuery.rqsLock.Unlock()

	return nil
}

// increments the finished segments. If incr is 0, then the current query is finished and a histogram will be flushed
func IncrementNumFinishedSegments(incr int, qid uint64, recsSearched uint64,
	skEnc uint16, remoteId string, doBuckPull bool, sstMap map[string]*structs.SegStats) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("updateTotalSegmentsInQuery: qid %+v does not exist!", qid)
		return
	}

	rQuery.rqsLock.Lock()
	rQuery.finishedSegments += uint64(incr)
	perComp := float64(0)
	if rQuery.totalSegments != 0 {
		val := float64(rQuery.finishedSegments) / float64(rQuery.totalSegments) * 100
		perComp = toFixed(val, 3)
	}

	rQuery.totalRecsSearched += recsSearched
	if rQuery.searchRes != nil {
		rQuery.queryCount = rQuery.searchRes.GetQueryCount()
		rQuery.rawRecords = rQuery.searchRes.GetResultsCopy()
		if doBuckPull {
			rQuery.searchHistogram = rQuery.searchRes.GetBucketResults()
		}
		if sstMap != nil && rQuery.isAsync {
			rQuery.searchRes.AddSSTMap(sstMap, skEnc)
		}
	}
	rQuery.rqsLock.Unlock()
	if rQuery.isAsync {
		var queryUpdate QueryUpdate
		if remoteId != "" {
			queryUpdate = QueryUpdate{
				QUpdate:  QUERY_UPDATE_REMOTE,
				RemoteID: remoteId,
			}
		} else {
			queryUpdate = QueryUpdate{
				QUpdate:   QUERY_UPDATE_LOCAL,
				SegKeyEnc: skEnc,
			}
		}

		rQuery.StateChan <- &QueryStateChanData{
			StateName:       QUERY_UPDATE,
			QueryUpdate:     &queryUpdate,
			PercentComplete: perComp}
	}
}

func setTotalSegmentsToSearch(qid uint64, numSegments uint64) error {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("setTotalSegmentsToSearch: qid %+v does not exist!", qid)
		return fmt.Errorf("qid does not exist")
	}

	rQuery.rqsLock.Lock()
	rQuery.totalSegments = numSegments
	rQuery.rqsLock.Unlock()

	return nil
}

func GetTotalSegmentsToSearch(qid uint64) (uint64, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return 0, fmt.Errorf("qid=%v does not exist", qid)
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	return rQuery.totalSegments, nil
}

// This sets RawSearchIsFinished to true and sends a COMPLETE message to the query's StateChan
// If there are no aggregations
func SetQidAsFinished(qid uint64) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("SetQidAsFinished: qid %+v does not exist!", qid)
		return
	}

	rQuery.rqsLock.Lock()
	rQuery.rawSearchIsFinished = true
	rQuery.rqsLock.Unlock()

	// Only async queries need to send COMPLETE, but if we need to do post
	// aggregations, we'll send COMPLETE once we're done with those.
	if rQuery.isAsync && (rQuery.aggs == nil || rQuery.aggs.Next == nil) {
		rQuery.StateChan <- &QueryStateChanData{StateName: COMPLETE}
	}
}

func IsRawSearchFinished(qid uint64) (bool, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("IsRawSearchFinished: qid %+v does not exist!", qid)
		return false, fmt.Errorf("qid=%v does not exist", qid)
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	return rQuery.rawSearchIsFinished, nil
}

func SetRawSearchFinished(qid uint64) error {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("IsRawSearchFinished: qid %+v does not exist!", qid)
		return fmt.Errorf("qid=%v does not exist", qid)
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()

	rQuery.rawSearchIsFinished = true
	return nil
}

func SetCurrentSearchResultCount(qid uint64, count int) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("SetCurrentSearchResultCount: qid %+v does not exist!", qid)
		return
	}

	rQuery.rqsLock.Lock()
	rQuery.currentSearchResultCount = count
	rQuery.rqsLock.Unlock()
}

func GetCurrentSearchResultCount(qid uint64) (int, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("GetQuerySizeLimit: qid %+v does not exist!", qid)
		return 0, fmt.Errorf("qid does not exist")
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	return rQuery.currentSearchResultCount, nil
}

func SetCleanupCallback(qid uint64, cleanupCallback func()) error {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return putils.TeeErrorf("SetCleanupCallback: qid %+v does not exist!", qid)
	}

	rQuery.rqsLock.Lock()
	rQuery.cleanupCallback = cleanupCallback
	rQuery.rqsLock.Unlock()

	return nil
}

func (rQuery *RunningQueryState) SetSearchQueryInformation(qid uint64, tableInfo *structs.TableInfo, timeRange *dtu.TimeRange, orgid uint64) {
	rQuery.rqsLock.Lock()
	rQuery.tableInfo = tableInfo
	rQuery.timeRange = timeRange
	rQuery.orgid = orgid
	rQuery.rqsLock.Unlock()
}

func GetSearchQueryInformation(qid uint64) ([]string, *dtu.TimeRange, uint64, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		err := fmt.Errorf("GetSearchQueryInformation: qid %+v does not exist", qid)
		log.Errorf(err.Error())
		return nil, nil, 0, err
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	return rQuery.tableInfo.GetQueryTables(), rQuery.timeRange, rQuery.orgid, nil
}

// returns the total number of segments, the current number of search results, and if the raw search is finished
func GetQuerySearchStateForQid(qid uint64) (uint64, uint64, int, bool, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		err := fmt.Errorf("GetQueryStateInfoForQid: qid %+v does not exist", qid)
		log.Errorf(err.Error())
		return 0, 0, 0, false, err
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	return rQuery.totalSegments, rQuery.finishedSegments, rQuery.currentSearchResultCount, rQuery.rawSearchIsFinished, nil
}

func GetOrCreateQuerySearchNodeResult(qid uint64) (*structs.NodeResult, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		err := fmt.Errorf("GetOrCreateQuerySearchNodeResult: qid %+v does not exist", qid)
		log.Errorf(err.Error())
		return nil, err
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	if rQuery.nodeResult == nil {
		rQuery.nodeResult = &structs.NodeResult{
			GlobalSearchErrors: make(map[string]*structs.SearchErrorInfo),
		}
	}
	return rQuery.nodeResult, nil
}

func CancelQuery(qid uint64) {
	LogGlobalSearchErrors(qid)
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("CancelQuery: qid %+v does not exist!", qid)
		return
	}
	rQuery.rqsLock.Lock()
	rQuery.isCancelled = true
	if rQuery.cleanupCallback != nil {
		rQuery.cleanupCallback()
	}
	rQuery.rqsLock.Unlock()
}

func GetBucketsForQid(qid uint64) (map[string]*structs.AggregationResult, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("GetBucketsForQid: qid %+v does not exist!", qid)
		return nil, fmt.Errorf("qid does not exist")
	}

	if rQuery.searchHistogram == nil {
		return nil, fmt.Errorf("GetBucketsForQid: searchHistogram does not exist for qid %+v", qid)
	}
	return rQuery.searchHistogram, nil
}

func SetFinalStatsForQid(qid uint64, nodeResult *structs.NodeResult) error {
	arqMapLock.RLock()
	defer arqMapLock.RUnlock()

	rQuery, ok := allRunningQueries[qid]
	if !ok {
		log.Errorf("SetConvertedBucketsForQid: qid %+v does not exist!", qid)
		return fmt.Errorf("qid does not exist")
	}

	return rQuery.searchRes.SetFinalStatsFromNodeResult(nodeResult)
}

func SetAllColsInAggsForQid(qid uint64, allCols map[string]struct{}) {
	arqMapLock.RLock()
	defer arqMapLock.RUnlock()

	rQuery, ok := allRunningQueries[qid]
	if !ok {
		log.Errorf("SetAllColsInAggsForQid: qid %+v does not exist!", qid)
		return
	}

	rQuery.rqsLock.Lock()
	rQuery.AllColsInAggs = allCols
	rQuery.rqsLock.Unlock()
}

func GetAllColsInAggsForQid(qid uint64) (map[string]struct{}, error) {
	arqMapLock.RLock()
	defer arqMapLock.RUnlock()

	rQuery, ok := allRunningQueries[qid]
	if !ok {
		log.Errorf("GetAllColsInAggsForQid: qid %+v does not exist!", qid)
		return nil, fmt.Errorf("qid does not exist")
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	return rQuery.AllColsInAggs, nil
}

// gets the measure results for the running query.
// if the query is segment stats, it will delete the input segkeyenc
func GetMeasureResultsForQid(qid uint64, pullGrpBucks bool, skenc uint16, limit int) ([]*structs.BucketHolder, []string, []string, []string, int) {

	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	if !ok {
		log.Errorf("GetMeasureResultsForQid: qid %+v does not exist!", qid)
		arqMapLock.RUnlock()
		return nil, nil, nil, nil, 0
	}
	defer arqMapLock.RUnlock()

	if rQuery.searchRes == nil {
		return nil, nil, nil, nil, 0
	}

	if config.IsNewQueryPipelineEnabled() {
		resp := rQuery.NewPipeLineResponse
		return resp.MeasureResults, resp.MeasureFunctions, resp.GroupByCols, resp.ColumnsOrder, len(resp.MeasureResults) 
	}

	switch rQuery.QType {
	case structs.SegmentStatsCmd:
		return rQuery.searchRes.GetSegmentStatsResults(skenc)
	case structs.GroupByCmd:
		if pullGrpBucks {
			rowCnt := MAX_GRP_BUCKS
			if limit != -1 {
				rowCnt = limit
			}

			// If after stats block's group by there is a statistic block's group by, we should only keep the groupby cols of the statistic block
			bucketHolderArr, retMFuns, aggGroupByCols, columnsOrder, added := rQuery.searchRes.GetGroupyByBuckets(rowCnt)

			statisticGroupByCols := rQuery.searchRes.GetStatisticGroupByCols()
			// If there is only one group by in the agg, we do not need to change groupbycols
			if len(statisticGroupByCols) > 0 && !rQuery.searchRes.IsOnlyStatisticGroupBy() {
				aggGroupByCols = statisticGroupByCols
			}

			// Remove unused columns for Rename block
			aggGroupByCols = structs.RemoveUnusedGroupByCols(rQuery.searchRes.GetAggs(), aggGroupByCols)

			return bucketHolderArr, retMFuns, aggGroupByCols, GetFinalColsOrder(columnsOrder), added
		} else {
			return nil, nil, nil, nil, 0
		}
	default:
		return nil, nil, nil, nil, 0
	}
}

func GetQueryType(qid uint64) structs.QueryType {
	arqMapLock.RLock()
	defer arqMapLock.RUnlock()

	rQuery, ok := allRunningQueries[qid]
	if !ok {
		log.Errorf("GetQueryType: qid %+v does not exist!", qid)
		return structs.InvalidCmd
	}

	return rQuery.QType
}

// Get remote raw logs and columns based on the remoteID and all RRCs
func GetRemoteRawLogInfo(remoteID string, inrrcs []*utils.RecordResultContainer, qid uint64) ([]map[string]interface{}, []string, error) {
	arqMapLock.RLock()
	defer arqMapLock.RUnlock()

	rQuery, ok := allRunningQueries[qid]
	if !ok {
		log.Errorf("GetRemoteRawLogInfo: qid %+v does not exist!", qid)
		return nil, nil, fmt.Errorf("qid does not exist")
	}

	return rQuery.searchRes.GetRemoteInfo(remoteID, inrrcs, false)
}

func GetAllRemoteLogs(inrrcs []*utils.RecordResultContainer, qid uint64) ([]map[string]interface{}, []string, error) {
	arqMapLock.RLock()
	defer arqMapLock.RUnlock()

	rQuery, ok := allRunningQueries[qid]
	if !ok {
		log.Errorf("GetAllRemoteLogs: qid %+v does not exist!", qid)
		return nil, nil, fmt.Errorf("qid does not exist")
	}

	return rQuery.searchRes.GetRemoteInfo("", inrrcs, true)
}

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

// Function to truncate float64 to a given precision
func toFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

func checkForCancelledQuery(qid uint64) (bool, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("GetStateForQid: qid %+v does not exist!", qid)
		return false, fmt.Errorf("qid does not exist")
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()

	if rQuery.isCancelled {
		return true, nil
	}
	return false, nil
}

// returns the rrcs, query counts, map of segkey encoding, and errors
func GetRawRecordInfoForQid(scroll int, qid uint64) ([]*utils.RecordResultContainer, uint64, map[uint16]string, map[string]struct{}, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("GetRawRecordInforForQid: qid %+v does not exist!", qid)
		return nil, 0, nil, nil, fmt.Errorf("qid does not exist")
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	if rQuery.queryCount == nil || rQuery.rawRecords == nil {
		eres := make([]*utils.RecordResultContainer, 0)
		return eres, 0, nil, nil, nil
	}

	if len(rQuery.rawRecords) <= scroll {
		eres := make([]*utils.RecordResultContainer, 0)
		return eres, 0, nil, nil, nil
	}
	skCopy := make(map[uint16]string, len(rQuery.searchRes.SegEncToKey))
	for k, v := range rQuery.searchRes.SegEncToKey {
		skCopy[k] = v
	}
	return rQuery.rawRecords[scroll:], rQuery.queryCount.TotalCount, skCopy, rQuery.AllColsInAggs, nil
}

// returns rrcs, raw time buckets, raw groupby buckets, querycounts, map of segkey encoding, and errors
func GetQueryResponseForRPC(scroll int, qid uint64) ([]*utils.RecordResultContainer, *blockresults.TimeBuckets,
	*blockresults.GroupByBuckets, *segresults.RemoteStats, map[uint16]string, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("GetQueryResponseForRPC: qid %+v does not exist!", qid)
		return nil, nil, nil, nil, nil, fmt.Errorf("qid does not exist")
	}

	if rQuery.queryCount == nil || rQuery.rawRecords == nil {
		eres := make([]*utils.RecordResultContainer, 0)
		return eres, nil, nil, nil, nil, nil
	}
	var eres []*utils.RecordResultContainer
	if rQuery.rawRecords == nil {
		eres = make([]*utils.RecordResultContainer, 0)
	} else if len(rQuery.rawRecords) <= scroll {
		eres = make([]*utils.RecordResultContainer, 0)
	} else {
		eres = rQuery.rawRecords[scroll:]
	}
	skCopy := make(map[uint16]string, len(rQuery.searchRes.SegEncToKey))
	for k, v := range rQuery.searchRes.SegEncToKey {
		skCopy[k] = v
	}
	switch rQuery.QType {
	case structs.SegmentStatsCmd:
		// SegStats will be streamed back on each query update. So, we don't need to return anything here
		remoteStats, err := rQuery.searchRes.GetRemoteStats()
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("Error while getting remote stats: %v", err)
		}
		return eres, nil, nil, remoteStats, skCopy, nil
	case structs.GroupByCmd:
		timeBuckets, groupBuckets := rQuery.searchRes.GetRunningBuckets()
		return eres, timeBuckets, groupBuckets, nil, skCopy, nil
	default:
		return eres, nil, nil, nil, skCopy, nil
	}
}

// Gets the json encoding of segstats for RPC.
// Returns encoded segstats for the given segkeyEnc and qid, bool if the query is segstats or not, and error
func GetEncodedSegStatsForRPC(qid uint64, segKeyEnc uint16) ([]byte, bool, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("GetEncodedSegStatsForRPC: qid %+v does not exist!", qid)
		return nil, false, fmt.Errorf("qid does not exist")
	}

	if rQuery.QType != structs.SegmentStatsCmd {
		return nil, false, nil
	}
	retVal, err := rQuery.searchRes.GetEncodedSegStats(segKeyEnc)
	return retVal, true, err
}

// returns the query counts for the qid. If qid does not exist, this will return a QueryCount set to 0
func GetQueryCountInfoForQid(qid uint64) *structs.QueryCount {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("GetQueryCountInfoForQid: qid %+v does not exist!", qid)
		return zeroHitsQueryCount()
	}

	if rQuery.queryCount == nil {
		log.Infof("qid=%d, GetQueryCountInfoForQid: query count for qid %+v does not exist. Defaulting to 0", qid, qid)
		return zeroHitsQueryCount()
	}

	return rQuery.queryCount
}

// returns the query counts and searched count for the qid. If qid does not exist, this will return a QueryCount set to 0
func GetQueryInfoForQid(qid uint64) (*structs.QueryCount, uint64, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("GetQueryCountInfoForQid: qid %+v does not exist!", qid)
		return nil, 0, fmt.Errorf("qid does not exist")
	}

	if rQuery.queryCount == nil {
		log.Infof("qid=%d, GetQueryCountInfoForQid: query count for qid %+v does not exist. Defaulting to 0", qid, qid)
		return nil, 0, fmt.Errorf("query count does not eixst")
	}

	return rQuery.queryCount, rQuery.totalRecsSearched, nil
}

func zeroHitsQueryCount() *structs.QueryCount {
	return &structs.QueryCount{
		TotalCount: 0,
		Op:         utils.Equals,
		EarlyExit:  true,
	}
}

func GetTotalsRecsSearchedForQid(qid uint64) (uint64, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("GetTotalsRecsSreachedForQid: qid %+v does not exist!", qid)
		return 0, fmt.Errorf("qid does not exist")
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()

	return rQuery.totalRecsSearched, nil
}

func setTotalRecordsToBeSearched(qid uint64, totalRecs uint64) error {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return fmt.Errorf("setTotalRecordsToBeSearched: qid=%v does not exist!", qid)
	}

	rQuery.rqsLock.Lock()
	rQuery.totalRecsToBeSearched = totalRecs
	rQuery.rqsLock.Unlock()

	return nil
}

func GetTotalRecsToBeSearchedForQid(qid uint64) (uint64, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("GetTotalRecsToBeSearchedForQid: qid %+v does not exist!", qid)
		return 0, fmt.Errorf("qid does not exist")
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()

	return rQuery.totalRecsToBeSearched, nil
}

// Common function to retrieve these 2 parameters for a given qid
// Returns totalEventsSearched, totalPossibleEvents, error respectively
func GetTotalSearchedAndPossibleEventsForQid(qid uint64) (uint64, uint64, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("GetTotalSearchedAndPossibleEventsForQid: qid %+v does not exist!", qid)
		return 0, 0, fmt.Errorf("qid does not exist")
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()

	return rQuery.totalRecsSearched, rQuery.totalRecsToBeSearched, nil
}

// returns the length of rrcs that exist in *search.SearchResults
// this will be used to determine if more scrolling can be done
func GetNumMatchedRRCs(qid uint64) (uint64, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("GetNumMatchedRRCs: qid %+v does not exist!", qid)
		return 0, fmt.Errorf("qid does not exist")
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()

	if rQuery.rawRecords == nil {
		return 0, nil
	}
	return uint64(len(rQuery.rawRecords)), nil

}

func GetUniqueSearchErrors(qid uint64) (string, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	var result string
	if !ok {
		log.Errorf("GetQueryTotalErrors: qid %+v does not exist!", qid)
		return result, fmt.Errorf("qid does not exist")
	}
	searchErrors := rQuery.searchRes.GetAllErrors()
	occurred := map[string]bool{}

	if len(searchErrors) == 0 {
		return result, nil
	}

	for _, e := range searchErrors {
		err := e.Error()
		if !occurred[err] {
			occurred[err] = true
			result += err + ", "
		}
	}
	return result, nil
}

// The colIndex within this map may be larger than the length of the map
func GetFinalColsOrder(columnsOrder map[string]int) []string {
	if columnsOrder == nil {
		return []string{}
	}

	pq := make(putils.PriorityQueue, len(columnsOrder))
	i := 0
	for colName, colIndex := range columnsOrder {
		pq[i] = &putils.Item{
			Value:    colName,
			Priority: float64(-colIndex),
			Index:    i,
		}
		i++
	}

	heap.Init(&pq)
	colsArr := make([]string, 0)
	for pq.Len() > 0 {
		item := heap.Pop(&pq).(*putils.Item)
		colsArr = append(colsArr, item.Value)
	}
	return colsArr

}

func LogGlobalSearchErrors(qid uint64) {
	nodeRes, err := GetOrCreateQuerySearchNodeResult(qid)
	if err != nil {
		log.Errorf("LogGlobalSearchErrors: Error getting query search node result for qid=%v", qid)
		return
	}
	for errMsg, errInfo := range nodeRes.GlobalSearchErrors {
		if errInfo == nil {
			continue
		}
		putils.LogUsingLevel(errInfo.LogLevel, "qid=%v, %v, Count: %v, ExtraInfo: %v", qid, errMsg, errInfo.Count, errInfo.Error)
	}
}

func SetNewQueryPipelineResponse(response *structs.PipeSearchResponseOuter, qid uint64) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("SetQueryResponse: qid %+v does not exist!", qid)
		return
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	rQuery.NewPipeLineResponse = response
	rQuery.totalRecsSearched = rQuery.totalRecsToBeSearched
	rQuery.queryCount = &structs.QueryCount{
		TotalCount: uint64(len(response.Hits.Hits)),
		EarlyExit: true,
	}
	
	rQuery.StateChan <- &QueryStateChanData{
		StateName:       QUERY_UPDATE,
		QueryUpdate:     &QueryUpdate{QUpdate: QUERY_UPDATE_LOCAL},
		PercentComplete: 100,
	}
}

func GetNewQueryPipelineResponse(qid uint64) *structs.PipeSearchResponseOuter {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("GetQueryResponse: qid %+v does not exist!", qid)
		return nil
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	return rQuery.NewPipeLineResponse
}