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
	"context"
	"encoding/json"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/siglens/siglens/pkg/segment/results/blockresults"

	"github.com/dustin/go-humanize"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	putils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

type QueryState int

var queryStateChanSize = 10

const MAX_GRP_BUCKS = 3000

var MAX_RUNNING_QUERIES = uint64(runtime.GOMAXPROCS(0))

const PULL_QUERY_INTERVAL = 10 * time.Millisecond
const MAX_WAITING_QUERIES = 500

type QueryUpdateType int

const (
	QUERY_UPDATE_LOCAL QueryUpdateType = iota + 1
	QUERY_UPDATE_REMOTE
)

type QueryUpdate struct {
	QUpdate   QueryUpdateType
	SegKeyEnc uint32
	RemoteID  string
}

type QueryStateChanData struct {
	StateName       QueryState
	Qid             uint64
	QueryUpdate     *QueryUpdate
	PercentComplete float64
	UpdateWSResp    *structs.PipeSearchWSUpdateResponse
	CompleteWSResp  *structs.PipeSearchCompleteResponse
	HttpResponse    *structs.PipeSearchResponseOuter
	Error           error // Only used when the state is ERROR
}

type WaitStateData struct {
	qid    uint64
	rQuery *RunningQueryState
}

const (
	READY QueryState = iota + 1
	RUNNING
	QUERY_UPDATE // flush segment counts & aggs & records (if matched)
	COMPLETE
	CANCELLED
	TIMEOUT
	ERROR
	QUERY_RESTART
	WAITING
)

func InitMaxRunningQueries() {
	memConfig := config.GetMemoryConfig()
	totalMemoryInBytes := config.GetTotalMemoryAvailableToUse()
	searchMemoryInBytes := (totalMemoryInBytes * memConfig.SearchPercent) / 100
	maxConcurrentQueries := searchMemoryInBytes / memConfig.BytesPerQuery
	if maxConcurrentQueries < 2 {
		maxConcurrentQueries = 2
	}
	if maxConcurrentQueries < MAX_RUNNING_QUERIES {
		MAX_RUNNING_QUERIES = maxConcurrentQueries
	}
}

func (qs QueryState) String() string {
	switch qs {
	case READY:
		return "READY"
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
	case QUERY_RESTART:
		return "QUERY_RESTARTED"
	case WAITING:
		return "WAITING"
	default:
		return fmt.Sprintf("UNKNOWN_QUERYSTATE_%d", qs)
	}
}

type RunningQueryState struct {
	isAsync                  bool
	isCoordinator            bool
	isCancelled              bool
	startTime                time.Time
	timeoutCancelFunc        context.CancelFunc
	StateChan                chan *QueryStateChanData // channel to send state changes of query
	latestQueryState         QueryState
	cleanupCallback          func()
	qid                      uint64
	orgid                    int64
	tableInfo                *structs.TableInfo
	timeRange                *dtu.TimeRange
	astNode                  *structs.ASTNode
	qc                       *structs.QueryContext
	searchRes                *segresults.SearchResults
	rawRecords               []*utils.RecordResultContainer
	queryCount               *structs.QueryCount
	aggs                     *structs.QueryAggregators
	searchHistogram          map[string]*structs.AggregationResult
	QType                    structs.QueryType
	rqsLock                  *sync.RWMutex
	dqs                      DistributedQueryServiceInterface
	totalSegments            uint64
	finishedSegments         uint64
	totalRecsSearched        uint64
	rawSearchIsFinished      bool
	currentSearchResultCount int
	nodeResult               *structs.NodeResult
	totalRecsToBeSearched    uint64
	AllColsInAggs            map[string]struct{}
	pipeResp                 *structs.PipeSearchResponseOuter
	Progress                 *structs.Progress
	scrollFrom               uint64
	batchError               *putils.BatchError
	queryText                string
}

type QueryStats struct {
	ActiveQueries  []ActiveQueryInfo  `json:"activeQueries"`
	WaitingQueries []WaitingQueryInfo `json:"waitingQueries"`
}

type ActiveQueryInfo struct {
	QueryText     string  `json:"queryText"`
	ExecutionTime float64 `json:"executionTimeMs"`
}

type WaitingQueryInfo struct {
	QueryText   string  `json:"queryText"`
	WaitingTime float64 `json:"waitingTimeMs"`
}

var allRunningQueries = map[uint64]*RunningQueryState{}
var arqMapLock *sync.RWMutex = &sync.RWMutex{} // All running queries lock
var waitingQueries = []*WaitStateData{}
var waitingQueriesLock = &sync.Mutex{}

func init() {
	go logQueueSizesForever(5 * time.Minute)
}

func (rQuery *RunningQueryState) IsAsync() bool {
	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	return rQuery.isAsync
}

func (rQuery *RunningQueryState) SendQueryStateComplete() {
	rQuery.StateChan <- &QueryStateChanData{StateName: COMPLETE, Qid: rQuery.qid}

	if rQuery.cleanupCallback != nil {
		rQuery.cleanupCallback()
	}
}

func (rQuery *RunningQueryState) SetLatestQueryState(state QueryState) {
	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	rQuery.latestQueryState = state
}

func (rQuery *RunningQueryState) GetLatestQueryState() string {
	rQuery.rqsLock.RLock()
	defer rQuery.rqsLock.RUnlock()
	return rQuery.latestQueryState.String()
}

func (rQuery *RunningQueryState) GetQueryBatchError() *putils.BatchError {
	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	return rQuery.batchError
}

func (rQuery *RunningQueryState) GetStartTime() time.Time {
	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	return rQuery.startTime
}

func (rQuery *RunningQueryState) IsCoordinator() bool {
	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	return rQuery.isCoordinator
}

func GetQueryStartTime(qid uint64) (time.Time, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return time.Time{}, fmt.Errorf("GetQueryStartTime: qid: %v does not exist", qid)
	}

	return rQuery.GetStartTime(), nil
}

func GetActiveQueryCount() int {
	arqMapLock.RLock()
	defer arqMapLock.RUnlock()
	return len(allRunningQueries)
}

func getWaitingQueryCount() int {
	waitingQueriesLock.Lock()
	defer waitingQueriesLock.Unlock()
	return len(waitingQueries)
}

func logQueueSizesForever(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		log.Infof("ActiveQueryCount=%d, WaitingQueryCount=%d", GetActiveQueryCount(), getWaitingQueryCount())
	}
}

func withLockInitializeQuery(qid uint64, async bool, cleanupCallback func(), stateChan chan *QueryStateChanData) (*RunningQueryState, error) {
	if _, ok := allRunningQueries[qid]; ok {
		return nil, fmt.Errorf("withLockInitializeQuery: qid %+v already exists", qid)
	}

	if stateChan == nil {
		stateChan = make(chan *QueryStateChanData, queryStateChanSize)
	}

	runningState := &RunningQueryState{
		qid:               qid,
		startTime:         time.Now(),
		StateChan:         stateChan,
		cleanupCallback:   cleanupCallback,
		rqsLock:           &sync.RWMutex{},
		isAsync:           async,
		timeoutCancelFunc: nil,
		batchError:        putils.NewBatchErrorWithQid(qid),
		latestQueryState:  WAITING,
	}

	return runningState, nil
}

func addToWaitingQueriesQueue(wsData *WaitStateData) error {
	waitingQueriesLock.Lock()
	defer waitingQueriesLock.Unlock()
	if len(waitingQueries) >= MAX_WAITING_QUERIES {
		return fmt.Errorf("addToWaitingQueriesQueue: qid=%v cannot be started, Max number of waiting queries reached", wsData.qid)
	}
	waitingQueries = append(waitingQueries, wsData)

	return nil
}

// Starts tracking the query state. RunningQueryState.StateChan will be defined & can be used to send query updates.
// If forceRun is true, the query will be run immediately, otherwise it will be added to the waiting queue.
// Caller is responsible to call DeleteQuery.
func StartQuery(qid uint64, async bool, cleanupCallback func(), forceRun bool) (*RunningQueryState, error) {
	arqMapLock.Lock()
	defer arqMapLock.Unlock()

	runningState, err := withLockInitializeQuery(qid, async, cleanupCallback, nil)
	if err != nil {
		return nil, putils.TeeErrorf("StartQuery: qid=%v cannot be initialized, %v", qid, err)
	}

	wsData := &WaitStateData{qid, runningState}

	if forceRun {
		withLockRunQuery(wsData)
	} else {
		err := addToWaitingQueriesQueue(wsData)
		if err != nil {
			return nil, putils.TeeErrorf("StartQuery: qid=%v cannot be added to waiting queue, %v", qid, err)
		}
	}

	return runningState, nil
}

// Starts tracking the query state and sets the query as a coordinator.
// If StateChan is nil, a new channel will be created, otherwise the provided channel will be used for sending query updates.
// If forceRun is true, the query will be run immediately, otherwise it will be added to the waiting queue.
// Caller is responsible to call DeleteQuery.
func StartQueryAsCoordinator(qid uint64, async bool, cleanupCallback func(), astNode *structs.ASTNode,
	aggs *structs.QueryAggregators, qc *structs.QueryContext, StateChan chan *QueryStateChanData, forceRun bool) (*RunningQueryState, error) {
	arqMapLock.Lock()
	defer arqMapLock.Unlock()

	rQuery, err := withLockInitializeQuery(qid, async, cleanupCallback, StateChan)
	if err != nil {
		return nil, err
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()

	rQuery.isCoordinator = true
	rQuery.astNode = astNode
	rQuery.qc = qc
	rQuery.aggs = aggs

	wsData := &WaitStateData{qid, rQuery}

	if forceRun {
		withLockRunQuery(wsData)
	} else {
		err := addToWaitingQueriesQueue(wsData)
		if err != nil {
			return nil, putils.TeeErrorf("StartQueryAsCoordinator: qid=%v cannot be added to waiting queue, %v", qid, err)
		}
	}

	return rQuery, nil
}

func (rQuery *RunningQueryState) RestartQuery(forceRun bool) (*RunningQueryState, uint64, error) {
	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()

	if rQuery.isCancelled {
		return nil, 0, fmt.Errorf("qid=%v, RestartQuery: query is cancelled", rQuery.qid)
	}

	arqMapLock.Lock()
	rQuery.withLockDeleteQuery()
	arqMapLock.Unlock()

	if !rQuery.isCoordinator {
		return nil, 0, fmt.Errorf("qid=%v, RestartQuery: query is not a coordinator", rQuery.qid)
	}

	newQid := rutils.GetNextQid()

	newRQuery, err := StartQueryAsCoordinator(newQid, rQuery.isAsync, nil, rQuery.astNode, rQuery.aggs, rQuery.qc, rQuery.StateChan, forceRun)
	if err != nil {
		return nil, 0, err
	}
	log.Infof("qid=%v, Restarted query as qid=%v", rQuery.qid, newQid)

	return newRQuery, newQid, nil
}

func RestartAllRunningQueries() {
	arqMapLock.RLock()
	defer arqMapLock.RUnlock()

	for _, rQuery := range allRunningQueries {
		restartState := &QueryStateChanData{StateName: QUERY_RESTART, Qid: rQuery.qid}
		rQuery.StateChan <- restartState
	}
}

// Removes reference to qid. If qid does not exist this is a noop
func DeleteQuery(qid uint64) {
	// Can remove the logGlobalSearchErrors after we fully migrate
	// to the putils.BatchError
	_ = logGlobalSearchErrors(qid) // not checking err, since the query is getting deleted
	putils.LogAllErrorsWithQidAndDelete(qid)

	arqMapLock.Lock()
	defer arqMapLock.Unlock()

	rQuery := allRunningQueries[qid]

	rQuery.withLockDeleteQuery()
}

func (rQuery *RunningQueryState) withLockDeleteQuery() {
	if rQuery == nil {
		return
	}

	if !rQuery.isCancelled {
		rQuery.timeoutCancelFunc()

		if rQuery.cleanupCallback != nil {
			rQuery.cleanupCallback()
		}
	}

	delete(allRunningQueries, rQuery.qid)

	if hook := hooks.GlobalHooks.RemoveUsageForRotatedSegmentsHook; hook != nil {
		hook(rQuery.qid)
	}
}

func canRunQuery() bool {
	activeQueries := uint64(GetActiveQueryCount())
	return activeQueries < MAX_RUNNING_QUERIES
}

func initiateRunQuery(wsData *WaitStateData, segsRLockFunc, segsRUnlockFunc func()) {
	if segsRLockFunc != nil && segsRUnlockFunc != nil {
		segsRLockFunc()
		defer segsRUnlockFunc()
	}

	RunQuery(*wsData)
}

func getNextWaitStateData() *WaitStateData {
	waitingQueriesLock.Lock()
	defer waitingQueriesLock.Unlock()

	if len(waitingQueries) == 0 {
		return nil
	}

	wsData := waitingQueries[0]
	waitingQueries = waitingQueries[1:]
	return wsData
}

func PullQueriesToRun(ctx context.Context) {
	segmentsRLockFunc := hooks.GlobalHooks.AcquireOwnedSegmentRLockHook
	segmentsRUnlockFunc := hooks.GlobalHooks.ReleaseOwnedSegmentRLockHook

	for {
		select {
		case <-ctx.Done():
			log.Info("PullQueriesToRun exiting")
			return
		default:
			if canRunQuery() {
				wsData := getNextWaitStateData()
				if wsData == nil {
					time.Sleep(PULL_QUERY_INTERVAL)
					continue
				}
				initiateRunQuery(wsData, segmentsRLockFunc, segmentsRUnlockFunc)
			}
			time.Sleep(PULL_QUERY_INTERVAL)
		}
	}
}

func setupTimeoutCancelFunc(qid uint64) context.CancelFunc {
	var timeoutCancelFunc context.CancelFunc
	timeoutSecs := config.GetQueryTimeoutSecs()
	if timeoutSecs != 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSecs)*time.Second)
		timeoutCancelFunc = cancel

		go func() {
			<-ctx.Done()
			arqMapLock.RLock()
			rQuery, ok := allRunningQueries[qid]
			arqMapLock.RUnlock()

			if ok && ctx.Err() == context.DeadlineExceeded {
				log.Infof("qid=%v Canceling query due to timeout (%v seconds)", qid, timeoutSecs)
				rQuery.StateChan <- &QueryStateChanData{StateName: TIMEOUT, Qid: qid}
				CancelQuery(qid)
			}
		}()
	}

	return timeoutCancelFunc
}

func withLockRunQuery(wsData *WaitStateData) {
	if wsData.rQuery.isCancelled {
		return
	}

	allRunningQueries[wsData.qid] = wsData.rQuery

	wsData.rQuery.timeoutCancelFunc = setupTimeoutCancelFunc(wsData.qid)
	wsData.rQuery.startTime = time.Now()

	wsData.rQuery.StateChan <- &QueryStateChanData{StateName: READY, Qid: wsData.qid}
	wsData.rQuery.StateChan <- &QueryStateChanData{StateName: RUNNING, Qid: wsData.qid}
}

func RunQuery(wsData WaitStateData) {
	arqMapLock.Lock()
	defer arqMapLock.Unlock()

	withLockRunQuery(&wsData)
}

func AssociateSearchInfoWithQid(qid uint64, result *segresults.SearchResults, aggs *structs.QueryAggregators, dqs DistributedQueryServiceInterface,
	qType structs.QueryType, queryText string) error {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return fmt.Errorf("AssociateSearchInfoWithQid: qid: %v does not exist", qid)
	}

	rQuery.rqsLock.Lock()
	rQuery.searchRes = result
	rQuery.aggs = aggs
	rQuery.dqs = dqs
	rQuery.QType = qType
	rQuery.queryText = queryText
	rQuery.rqsLock.Unlock()

	return nil
}

func AssociateSearchResult(qid uint64, result *segresults.SearchResults) error {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return putils.TeeErrorf("AssociateSearchResult: qid %+v does not exist!", qid)
	}

	rQuery.rqsLock.Lock()
	rQuery.searchRes = result
	rQuery.rqsLock.Unlock()

	return nil
}

// increments the finished segments. If incr is 0, then the current query is finished and a histogram will be flushed
func IncrementNumFinishedSegments(incr int, qid uint64, recsSearched uint64,
	skEnc uint32, remoteId string, doBuckPull bool, sstMap map[string]*structs.SegStats) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("IncrementNumFinishedSegments: qid %+v does not exist!", qid)
		return
	}

	rQuery.rqsLock.Lock()
	rQuery.finishedSegments += uint64(incr)

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

	if rQuery.QType != structs.RRCCmd {
		if rQuery.Progress == nil {
			rQuery.Progress = &structs.Progress{
				TotalUnits:   rQuery.totalSegments,
				TotalRecords: rQuery.totalRecsToBeSearched,
			}
		}
		rQuery.Progress.UnitsSearched = rQuery.finishedSegments
		rQuery.Progress.RecordsSearched = rQuery.totalRecsSearched

		if rQuery.isAsync {
			wsResponse := CreateWSUpdateResponseWithProgress(qid, rQuery.QType, rQuery.Progress, rQuery.scrollFrom)
			rQuery.StateChan <- &QueryStateChanData{
				StateName:    QUERY_UPDATE,
				UpdateWSResp: wsResponse,
				Qid:          qid,
			}
		}
	}

}

func setTotalSegmentsToSearch(qid uint64, numSegments uint64) error {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return fmt.Errorf("setTotalSegmentsToSearch: qid %+v does not exist", qid)
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
		return 0, fmt.Errorf("GetTotalSegmentsToSearch: qid=%v does not exist", qid)
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
}

func IsRawSearchFinished(qid uint64) (bool, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return false, fmt.Errorf("IsRawSearchFinished: qid=%v does not exist", qid)
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
		return fmt.Errorf("SetRawSearchFinished: qid=%v does not exist", qid)
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
		return 0, fmt.Errorf("GetCurrentSearchResultCount: qid %+v does not exist!", qid)
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

func (rQuery *RunningQueryState) SetSearchQueryInformation(qid uint64, tableInfo *structs.TableInfo, timeRange *dtu.TimeRange, orgid int64) {
	rQuery.rqsLock.Lock()
	rQuery.tableInfo = tableInfo
	rQuery.timeRange = timeRange
	rQuery.orgid = orgid
	rQuery.rqsLock.Unlock()
}

func GetSearchQueryInformation(qid uint64) ([]string, *dtu.TimeRange, int64, error) {
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
	_ = logGlobalSearchErrors(qid) // not checking return err val, since query is getting deleted
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Debugf("CancelQuery: qid %+v does not exist!", qid)
		return
	}
	rQuery.rqsLock.Lock()
	rQuery.isCancelled = true
	if rQuery.cleanupCallback != nil {
		rQuery.cleanupCallback()
	}
	rQuery.rqsLock.Unlock()

	waitingQueriesLock.Lock()
	defer waitingQueriesLock.Unlock()
	for i, wsData := range waitingQueries {
		if wsData.qid == qid {
			waitingQueries = append(waitingQueries[:i], waitingQueries[i+1:]...)
			break
		}
	}

	rQuery.StateChan <- &QueryStateChanData{StateName: CANCELLED, Qid: qid}
}

func GetBucketsForQid(qid uint64) (map[string]*structs.AggregationResult, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return nil, fmt.Errorf("GetBucketsForQid: qid %+v does not exist!", qid)
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
		return fmt.Errorf("SetFinalStatsForQid: qid %+v does not exist!", qid)
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
		return nil, fmt.Errorf("GetAllColsInAggsForQid: qid: %v does not exist", qid)
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	return rQuery.AllColsInAggs, nil
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

func GetAllRemoteLogs(inrrcs []*utils.RecordResultContainer, qid uint64) ([]map[string]interface{}, []string, error) {
	arqMapLock.RLock()
	defer arqMapLock.RUnlock()

	rQuery, ok := allRunningQueries[qid]
	if !ok {
		return nil, nil, fmt.Errorf("GetAllRemoteLogs: qid %+v does not exist", qid)
	}

	if rQuery.searchRes == nil {
		return nil, nil, fmt.Errorf("GetAllRemoteLogs: qid %+v does not have searchRes", qid)
	}

	return rQuery.searchRes.GetRemoteInfo("", inrrcs, true)
}

func checkForCancelledQuery(qid uint64) (bool, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return false, fmt.Errorf("checkForCancelledQuery: qid: %v does not exist", qid)
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()

	if rQuery.isCancelled {
		return true, nil
	}
	return false, nil
}

// returns rrcs, raw time buckets, raw groupby buckets, querycounts, map of segkey encoding, and errors
func GetQueryResponseForRPC(scroll int, qid uint64) ([]*utils.RecordResultContainer, *blockresults.TimeBuckets,
	*blockresults.GroupByBuckets, *segresults.RemoteStats, map[uint32]string, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return nil, nil, nil, nil, nil, fmt.Errorf("GetQueryResponseForRPC: qid %+v does not exist", qid)
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
	skCopy := make(map[uint32]string, len(rQuery.searchRes.SegEncToKey))
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
func GetEncodedSegStatsForRPC(qid uint64, segKeyEnc uint32) ([]byte, bool, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return nil, false, fmt.Errorf("GetEncodedSegStatsForRPC: qid %+v does not exist!", qid)
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
		return nil, 0, fmt.Errorf("GetQueryCountInfoForQid: qid %+v does not exist!", qid)
	}

	if rQuery.queryCount == nil {
		return nil, 0, fmt.Errorf("GetQueryCountInfoForQid: query count for qid %+v does not exist. Defaulting to 0", qid)
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
		return 0, fmt.Errorf("GetTotalRecsToBeSearchedForQid: qid %+v does not exist!", qid)
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()

	return rQuery.totalRecsToBeSearched, nil
}

// returns the length of rrcs that exist in *search.SearchResults
// this will be used to determine if more scrolling can be done
func GetNumMatchedRRCs(qid uint64) (uint64, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return 0, fmt.Errorf("GetNumMatchedRRCs: qid %+v does not exist!", qid)
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()

	if rQuery.rawRecords == nil {
		return 0, nil
	}
	return uint64(len(rQuery.rawRecords)), nil

}

func logGlobalSearchErrors(qid uint64) error {
	nodeRes, err := GetOrCreateQuerySearchNodeResult(qid)
	if err != nil {
		return fmt.Errorf("logGlobalSearchErrors: Error getting query search node result for qid=%v", qid)
	}

	nodeRes.SearchErrorsLock.RLock()
	defer nodeRes.SearchErrorsLock.RUnlock()
	for errMsg, errInfo := range nodeRes.GlobalSearchErrors {
		if errInfo == nil {
			continue
		}
		putils.LogUsingLevel(errInfo.LogLevel, "qid=%v, %v, Count: %v, ExtraInfo: %v", qid, errMsg, errInfo.Count, errInfo.Error)
	}
	return nil
}

func SetPipeResp(response *structs.PipeSearchResponseOuter, qid uint64) error {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return putils.TeeErrorf("SetPipeResp: qid %+v does not exist!", qid)
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	rQuery.pipeResp = response
	rQuery.totalRecsSearched = rQuery.totalRecsToBeSearched
	rQuery.queryCount = &structs.QueryCount{
		TotalCount: uint64(len(response.Hits.Hits)),
		EarlyExit:  true,
	}

	rQuery.StateChan <- &QueryStateChanData{
		StateName:       QUERY_UPDATE,
		QueryUpdate:     &QueryUpdate{QUpdate: QUERY_UPDATE_LOCAL},
		PercentComplete: 100,
		Qid:             qid,
	}
	return nil
}

func GetPipeResp(qid uint64) *structs.PipeSearchResponseOuter {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("GetPipeResp: qid %+v does not exist!", qid)
		return nil
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	return rQuery.pipeResp
}

func SetQidAsFinishedForPipeRespQuery(qid uint64) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("SetQidAsFinishedForPipeRespQuery: qid %+v does not exist!", qid)
		return
	}

	rQuery.rqsLock.Lock()
	rQuery.rawSearchIsFinished = true
	rQuery.rqsLock.Unlock()

	// Only async queries need to send COMPLETE
	if rQuery.isAsync {
		rQuery.StateChan <- &QueryStateChanData{StateName: COMPLETE, Qid: qid}
	}
}

func InitProgressForRRCCmd(totalUnits uint64, qid uint64) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		log.Errorf("InitProgressForRRCCmd: qid %+v does not exist!", qid)
		return
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()

	if rQuery.Progress != nil {
		return
	}

	rQuery.Progress = &structs.Progress{
		TotalUnits:   totalUnits,
		TotalRecords: rQuery.totalRecsToBeSearched,
	}
}

func IncProgressForRRCCmd(recordsSearched uint64, unitsSearched uint64, qid uint64) error {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return putils.TeeErrorf("IncProgressForRRCCmd: qid %+v does not exist!", qid)
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()

	if rQuery.Progress == nil {
		return putils.TeeErrorf("IncProgressForRRCCmd: qid=%v Progress is not initialized!", qid)
	}

	rQuery.Progress.UnitsSearched += unitsSearched
	rQuery.Progress.RecordsSearched += recordsSearched

	if rQuery.isAsync {
		wsResponse := CreateWSUpdateResponseWithProgress(qid, rQuery.QType, rQuery.Progress, rQuery.scrollFrom)
		rQuery.StateChan <- &QueryStateChanData{
			StateName:    QUERY_UPDATE,
			UpdateWSResp: wsResponse,
			Qid:          qid,
		}
	}

	return nil
}

func GetProgress(qid uint64) (structs.Progress, error) {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return structs.Progress{}, putils.TeeErrorf("GetProgress: qid %+v does not exist!", qid)
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	if rQuery.Progress == nil {
		return structs.Progress{
			RecordsSent:     0,
			TotalUnits:      0,
			UnitsSearched:   0,
			TotalRecords:    0,
			RecordsSearched: 0,
		}, nil
	}

	return structs.Progress{
		RecordsSent:     rQuery.Progress.RecordsSent,
		TotalUnits:      rQuery.Progress.TotalUnits,
		UnitsSearched:   rQuery.Progress.UnitsSearched,
		TotalRecords:    rQuery.Progress.TotalRecords,
		RecordsSearched: rQuery.Progress.RecordsSearched,
	}, nil
}

func IncRecordsSent(qid uint64, recordsSent uint64) error {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return putils.TeeErrorf("IncRecordsSent: qid %+v does not exist!", qid)
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	if rQuery.Progress == nil {
		return putils.TeeErrorf("IncRecordsSent: qid=%v Progress is not initialized!", qid)
	}

	rQuery.Progress.RecordsSent += recordsSent

	return nil
}

func CreateWSUpdateResponseWithProgress(qid uint64, qType structs.QueryType, progress *structs.Progress, scrollFrom uint64) *structs.PipeSearchWSUpdateResponse {
	completion := float64(0)
	// TODO: clean up completion percentage
	percCompleteBySearch := float64(0)
	if progress.TotalUnits > 0 {
		percCompleteBySearch = (float64(progress.UnitsSearched) * 100) / float64(progress.TotalUnits)
	}
	percCompleteByRecordsSent := (float64(progress.RecordsSent) * 100) / float64(scrollFrom+utils.QUERY_EARLY_EXIT_LIMIT)
	completion = math.Max(float64(percCompleteBySearch), percCompleteByRecordsSent)
	// TODO: fix completion percentage so that it is accurate - correctly identify UnitsSearched and TotalUnits.
	completion = math.Min(completion, 100.0)
	return &structs.PipeSearchWSUpdateResponse{
		State:               QUERY_UPDATE.String(),
		Completion:          completion,
		Qtype:               qType.String(),
		TotalEventsSearched: humanize.Comma(int64(progress.RecordsSearched)),
		TotalPossibleEvents: humanize.Comma(int64(progress.TotalRecords)),
	}
}

func InitScrollFrom(qid uint64, scrollFrom uint64) error {
	arqMapLock.RLock()
	rQuery, ok := allRunningQueries[qid]
	arqMapLock.RUnlock()
	if !ok {
		return putils.TeeErrorf("InitScrollFrom: qid %+v does not exist!", qid)
	}

	rQuery.rqsLock.Lock()
	defer rQuery.rqsLock.Unlock()
	rQuery.scrollFrom = scrollFrom

	return nil
}

func ConvertQueryCountToTotalResponse(qc *structs.QueryCount) putils.HitsCount {
	if qc == nil {
		return putils.HitsCount{Value: 0, Relation: "eq"}
	}

	if !qc.EarlyExit {
		return putils.HitsCount{Value: qc.TotalCount, Relation: "eq"}
	}

	return putils.HitsCount{Value: qc.TotalCount, Relation: qc.Op.ToString()}
}

func GetQueryStats(ctx *fasthttp.RequestCtx) {
	response := QueryStats{
		ActiveQueries:  getActiveQueriesInfo(),
		WaitingQueries: getWaitingQueriesInfo(),
	}

	ctx.SetContentType("application/json")
	if err := json.NewEncoder(ctx).Encode(response); err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		ctx.SetBodyString("Failed to encode response")
		return
	}
}

func getActiveQueriesInfo() []ActiveQueryInfo {
	arqMapLock.RLock()
	queries := make([]*RunningQueryState, 0, len(allRunningQueries))
	for _, q := range allRunningQueries {
		queries = append(queries, q)
	}
	arqMapLock.RUnlock()

	activeQueries := make([]ActiveQueryInfo, 0, len(queries))

	for _, rQuery := range queries {
		rQuery.rqsLock.Lock()
		activeQueries = append(activeQueries, ActiveQueryInfo{
			QueryText:     rQuery.queryText,
			ExecutionTime: float64(time.Since(rQuery.startTime).Milliseconds()),
		})
		rQuery.rqsLock.Unlock()
	}

	return activeQueries
}

func GetWaitingQueries() []*WaitStateData {
	waitingQueriesLock.Lock()
	queries := make([]*WaitStateData, len(waitingQueries))
	copy(queries, waitingQueries)
	waitingQueriesLock.Unlock()

	return queries
}

func GetWaitingInfoFor(queries []*WaitStateData) []WaitingQueryInfo {
	waitingQueriesInfo := make([]WaitingQueryInfo, 0, len(queries))

	for _, wQuery := range queries {
		wQuery.rQuery.rqsLock.Lock()
		waitingQueriesInfo = append(waitingQueriesInfo, WaitingQueryInfo{
			QueryText:   wQuery.rQuery.queryText,
			WaitingTime: float64(time.Since(wQuery.rQuery.startTime).Milliseconds()),
		})
		wQuery.rQuery.rqsLock.Unlock()
	}

	return waitingQueriesInfo
}

func getWaitingQueriesInfo() []WaitingQueryInfo {
	if hook := hooks.GlobalHooks.GetWaitingQueriesHook; hook != nil {
		resultAsAny, err := hook()
		if err != nil {
			log.Errorf("getWaitingQueriesInfo: error in hook: %v", err)
			return nil
		}

		result, ok := resultAsAny.([]WaitingQueryInfo)
		if !ok {
			log.Errorf("getWaitingQueriesInfo: hook returned %T instead of []*WaitStateData", resultAsAny)
			return nil
		}

		return result
	}

	return GetWaitingInfoFor(GetWaitingQueries())
}
