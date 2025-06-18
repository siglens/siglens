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

package segresults

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/dustin/go-humanize"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/aggregations"
	"github.com/siglens/siglens/pkg/segment/reader/segread"
	"github.com/siglens/siglens/pkg/segment/results/blockresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer/stats"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type EarlyExitType uint8

const EMPTY_GROUPBY_KEY = "*"

const (
	EetContSearch EarlyExitType = iota + 1
	EetMatchAllAggs
	EetEarlyExit
)

func (e EarlyExitType) String() string {
	switch e {
	case EetContSearch:
		return "Continue search"
	case EetMatchAllAggs:
		return "Match all aggs"
	case EetEarlyExit:
		return "Early exit"
	default:
		return fmt.Sprintf("Unknown early exit type %d", e)
	}
}

// Stores information received by remote nodes for a query
type remoteSearchResult struct {

	// for RRCs in BlockResults that come from remote nodes, this map stores the raw logs
	remoteLogs map[string]map[string]interface{}

	// all columns that are present in the remote logs
	remoteColumns map[string]struct{}
}

// exposes a struct to manage and maintain thread safe addition of results
type SearchResults struct {
	updateLock *sync.Mutex
	queryType  structs.QueryType
	qid        uint64
	sAggs      *structs.QueryAggregators
	sizeLimit  uint64

	resultCount  uint64 // total count of results
	EarlyExit    bool
	BlockResults *blockresults.BlockResults // stores information about the matched RRCs
	remoteInfo   *remoteSearchResult        // stores information about remote raw logs and columns

	runningSegStat         []*structs.SegStats
	runningEvalStats       map[string]interface{}
	segStatsResults        *segStatsResults
	convertedBuckets       map[string]*structs.AggregationResult
	allSSTS                map[uint32]map[string]*structs.SegStats // maps segKeyEnc to a map of segstats
	AllErrors              []error
	SegKeyToEnc            map[string]uint32
	SegEncToKey            map[uint32]string
	NextSegKeyEnc          uint32
	ColumnsOrder           map[string]int
	ProcessedRemoteRecords map[string]map[string]struct{}

	statsAreFinal bool // If true, segStatsResults and convertedBuckets must not change.
}

type segStatsResults struct {
	measureResults   map[string]sutils.CValueEnclosure // maps agg function to result
	measureFunctions []string
	groupByCols      []string
}

type RemoteStats struct {
	EvalStats map[string]EvalStatsMetaData
	SegStats  []*structs.SegStats
}

type EvalStatsMetaData struct {
	RangeStat     *structs.RangeStat
	AvgStat       *structs.AvgStat
	StrSet        map[string]struct{}
	StrList       []string
	MeasureResult interface{} // we should not use CValueEnclosure directly, because it treats interface having numbers as float64
}

type RemoteStatsJSON struct {
	EvalStats map[string]EvalStatsMetaData `json:"EvalStats"`
	SegStats  []*structs.SegStatsJSON      `json:"SegStats"`
}

func InitSearchResults(sizeLimit uint64, aggs *structs.QueryAggregators, qType structs.QueryType, qid uint64) (*SearchResults, error) {
	lock := &sync.Mutex{}
	blockResults, err := blockresults.InitBlockResults(sizeLimit, aggs, qid)
	if err != nil {
		log.Errorf("InitSearchResults: failed to initialize blockResults: %v, qid=%v", err, qid)
		return nil, err
	}

	allErrors := make([]error, 0)
	var runningSegStat []*structs.SegStats
	if aggs != nil && aggs.MeasureOperations != nil {
		runningSegStat = make([]*structs.SegStats, len(aggs.MeasureOperations))
	}
	return &SearchResults{
		queryType:    qType,
		updateLock:   lock,
		sizeLimit:    sizeLimit,
		resultCount:  uint64(0),
		BlockResults: blockResults,
		remoteInfo: &remoteSearchResult{
			remoteLogs:    make(map[string]map[string]interface{}),
			remoteColumns: make(map[string]struct{}),
		},
		qid:                    qid,
		sAggs:                  aggs,
		allSSTS:                make(map[uint32]map[string]*structs.SegStats),
		runningSegStat:         runningSegStat,
		runningEvalStats:       make(map[string]interface{}),
		AllErrors:              allErrors,
		SegKeyToEnc:            make(map[string]uint32),
		SegEncToKey:            make(map[uint32]string),
		NextSegKeyEnc:          1,
		ProcessedRemoteRecords: make(map[string]map[string]struct{}),
	}, nil
}

func (sr *SearchResults) InitSegmentStatsResults(mOps []*structs.MeasureAggregator) {
	sr.updateLock.Lock()
	mFuncs := make([]string, len(mOps))
	for i, op := range mOps {
		mFuncs[i] = op.String()
	}
	retVal := make(map[string]sutils.CValueEnclosure, len(mOps))
	sr.segStatsResults = &segStatsResults{
		measureResults:   retVal,
		measureFunctions: mFuncs,
	}
	sr.updateLock.Unlock()
}

// checks if total count has been set and if any more raw records are needed
// if retruns true, then only aggregations / sorts are needed
func (sr *SearchResults) ShouldContinueRRCSearch() bool {
	return sr.resultCount <= sr.sizeLimit
}

// Adds local results to the search results
func (sr *SearchResults) AddBlockResults(blockRes *blockresults.BlockResults) {
	sr.updateLock.Lock()
	for _, rec := range blockRes.GetResults() {
		_, removedID := sr.BlockResults.Add(rec)
		sr.removeLog(removedID)
	}
	sr.resultCount += blockRes.MatchedCount
	sr.BlockResults.MergeBuckets(blockRes)
	sr.updateLock.Unlock()
}

// returns the raw, running buckets that have been created. This is used to merge with remote results
func (sr *SearchResults) GetRunningBuckets() (*blockresults.TimeBuckets, *blockresults.GroupByBuckets) {
	return sr.BlockResults.TimeAggregation, sr.BlockResults.GroupByAggregation
}

/*
adds an entry to the remote logs

the caller is responsible for ensuring that
*/
func (sr *SearchResults) addRawLog(id string, log map[string]interface{}) {
	sr.remoteInfo.remoteLogs[id] = log
}

/*
Removes an entry from the remote logs
*/
func (sr *SearchResults) removeLog(id string) {
	if id == "" {
		return
	}
	delete(sr.remoteInfo.remoteLogs, id)
}

func (sr *SearchResults) AddSSTMap(sstMap map[string]*structs.SegStats, skEnc uint32) {
	sr.updateLock.Lock()
	sr.allSSTS[skEnc] = sstMap
	sr.updateLock.Unlock()
}

func (sr *SearchResults) AddResultCount(count uint64) {
	sr.updateLock.Lock()
	sr.resultCount += count
	sr.updateLock.Unlock()
}

// deletes segKeyEnc from the map of allSSTS and any errors associated with it
func (sr *SearchResults) GetEncodedSegStats(segKeyEnc uint32) ([]byte, error) {
	sr.updateLock.Lock()
	retVal, ok := sr.allSSTS[segKeyEnc]
	delete(sr.allSSTS, segKeyEnc)
	sr.updateLock.Unlock()
	if !ok {
		return nil, nil
	}

	allSegStatJson := make(map[string]*structs.SegStatsJSON, len(retVal))
	for k, v := range retVal {
		rawJson, err := v.ToJSON()
		if err != nil {
			log.Errorf("GetEncodedSegStats: failed to convert segstats to json, qid=%v, err: %v", sr.qid, err)
			continue
		}
		allSegStatJson[k] = rawJson
	}
	allJSON := &structs.AllSegStatsJSON{
		AllSegStats: allSegStatJson,
	}
	jsonBytes, err := json.Marshal(allJSON)
	if err != nil {
		log.Errorf("GetEncodedSegStats: failed to marshal allSegStatJson, qid=%v, err: %v", sr.qid, err)
		return nil, err
	}
	return jsonBytes, nil
}

func (sr *SearchResults) AddError(err error) {
	sr.updateLock.Lock()
	sr.AllErrors = append(sr.AllErrors, err)
	sr.updateLock.Unlock()
}

func (sr *SearchResults) UpdateNonEvalSegStats(runningSegStat *structs.SegStats, incomingSegStat *structs.SegStats, measureAgg *structs.MeasureAggregator) (*structs.SegStats, error) {
	var sstResult *sutils.NumTypeEnclosure
	var err error
	switch measureAgg.MeasureFunc {
	case sutils.Min:
		res, err := segread.GetSegMin(runningSegStat, incomingSegStat)
		if err != nil {
			return nil, fmt.Errorf("UpdateSegmentStats: error getting segment level stats for %v, err: %v, qid=%v", measureAgg.String(), err, sr.qid)
		}
		sr.segStatsResults.measureResults[measureAgg.String()] = *res
		if runningSegStat == nil {
			return incomingSegStat, nil
		}
		return runningSegStat, nil
	case sutils.Max:
		res, err := segread.GetSegMax(runningSegStat, incomingSegStat)
		if err != nil {
			return nil, fmt.Errorf("UpdateSegmentStats: error getting segment level stats for %v, err: %v, qid=%v", measureAgg.String(), err, sr.qid)
		}
		sr.segStatsResults.measureResults[measureAgg.String()] = *res
		if runningSegStat == nil {
			return incomingSegStat, nil
		}
		return runningSegStat, nil
	case sutils.EarliestTime:
		fallthrough
	case sutils.LatestTime:
		isLatest := sutils.LatestTime == measureAgg.MeasureFunc
		res, err := segread.GetSegLatestOrEarliestTs(runningSegStat, incomingSegStat, isLatest)
		if err != nil {
			return nil, fmt.Errorf("UpdateSegmentStats: error getting segment level stats for %v, err: %v, qid=%v", measureAgg.String(), err, sr.qid)
		}
		sr.segStatsResults.measureResults[measureAgg.String()] = *res
		if runningSegStat == nil {
			return incomingSegStat, nil
		}
		return runningSegStat, nil
	case sutils.Earliest:
		fallthrough
	case sutils.Latest:
		isLatest := sutils.Latest == measureAgg.MeasureFunc
		res, err := segread.GetSegLatestOrEarliestVal(runningSegStat, incomingSegStat, isLatest)
		if err != nil {
			return nil, fmt.Errorf("UpdateSegmentStats: error getting segment level stats for %v, err: %v, qi=d%v", measureAgg.String(), err, sr.qid)
		}
		sr.segStatsResults.measureResults[measureAgg.String()] = *res
		if runningSegStat == nil {
			return incomingSegStat, nil
		}
		return runningSegStat, nil
	case sutils.Range:
		res, err := segread.GetSegRange(runningSegStat, incomingSegStat)
		if err != nil {
			return nil, fmt.Errorf("UpdateSegmentStats: error getting segment level stats for %v, err: %v, qid=%v", measureAgg.String(), err, sr.qid)
		}
		sr.segStatsResults.measureResults[measureAgg.String()] = *res
		if runningSegStat == nil {
			return incomingSegStat, nil
		}
		return runningSegStat, nil
	case sutils.Cardinality:
		sstResult, err = segread.GetSegCardinality(runningSegStat, incomingSegStat)
	case sutils.Perc:
		sstResult, err = segread.GetSegPerc(runningSegStat, incomingSegStat, measureAgg.Param)
	case sutils.Count:
		sstResult, err = segread.GetSegCount(runningSegStat, incomingSegStat)
	case sutils.Sum:
		sstResult, err = segread.GetSegSum(runningSegStat, incomingSegStat)
	case sutils.Sumsq:
		sstResult, err = segread.GetSegSumsq(runningSegStat, incomingSegStat)
	case sutils.Var:
		sstResult, err = segread.GetSegVar(runningSegStat, incomingSegStat)
	case sutils.Varp:
		sstResult, err = segread.GetSegVarp(runningSegStat, incomingSegStat)
	case sutils.Stdev:
		sstResult, err = segread.GetSegStdev(runningSegStat, incomingSegStat)
	case sutils.Stdevp:
		sstResult, err = segread.GetSegStdevp(runningSegStat, incomingSegStat)
	case sutils.Avg:
		sstResult, err = segread.GetSegAvg(runningSegStat, incomingSegStat)
	case sutils.Values:
		// Use GetSegValue to process and get the segment value
		res, err := segread.GetSegValue(runningSegStat, incomingSegStat)
		if err != nil {
			return nil, fmt.Errorf("UpdateSegmentStats: error getting segment level stats for %v, err: %v, qid=%v", measureAgg.String(), err, sr.qid)
		}

		sr.segStatsResults.measureResults[measureAgg.String()] = *res

		if runningSegStat == nil {
			return incomingSegStat, nil
		}
		return runningSegStat, nil
	case sutils.List:
		res, err := segread.GetSegList(runningSegStat, incomingSegStat)
		if err != nil {
			return nil, fmt.Errorf("UpdateSegmentStats: error getting segment level stats for %v, err: %v, qid=%v", measureAgg.String(), err, sr.qid)
		}
		sr.segStatsResults.measureResults[measureAgg.String()] = *res
		if runningSegStat == nil {
			return incomingSegStat, nil
		}
		return runningSegStat, nil
	default:
		return nil, fmt.Errorf("UpdateSegmentStats: does not support using aggOps: %v, qid=%v", measureAgg.String(), sr.qid)
	}

	if err != nil {
		return nil, fmt.Errorf("UpdateSegmentStats: error getting segment level stats for %v, err: %v, qid=%v", measureAgg.String(), err, sr.qid)
	}

	enclosure, err := sstResult.ToCValueEnclosure()
	if err != nil {
		return nil, fmt.Errorf("UpdateSegmentStats: cannot convert sstResult for %v, err: %v , qid=%v", measureAgg.String(), err, sr.qid)
	}
	sr.segStatsResults.measureResults[measureAgg.String()] = *enclosure

	if runningSegStat == nil {
		return incomingSegStat, nil
	}

	return runningSegStat, nil
}

func (sr *SearchResults) UpdateSegmentStats(sstMap map[string]*structs.SegStats, measureOps []*structs.MeasureAggregator) error {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()
	for idx, measureAgg := range measureOps {
		if len(sstMap) == 0 {
			continue
		}

		aggOp := measureAgg.MeasureFunc
		aggCol := measureAgg.MeasureCol

		if aggOp != sutils.Count && aggCol == "*" {
			return fmt.Errorf("UpdateSegmentStats: aggOp: %v cannot be applied with *, qid=%v", aggOp, sr.qid)
		}
		var currSst *structs.SegStats
		currSst, ok := sstMap[aggCol]
		if !ok && measureAgg.ValueColRequest == nil {
			currSst, ok = sstMap[config.GetTimeStampKey()]
			if !ok {
				log.Debugf("UpdateSegmentStats: sstMap was nil for aggCol %v, qid=%v", aggCol, sr.qid)
				continue
			}
		}

		if measureAgg.ValueColRequest == nil {
			// If the measure is not an eval statement, then update the segment stats
			resSegStat, err := sr.UpdateNonEvalSegStats(sr.runningSegStat[idx], currSst, measureAgg)
			if err != nil {
				log.Errorf("UpdateSegmentStats: qid=%v, err: %v", sr.qid, err)
				continue
			}
			sr.runningSegStat[idx] = resSegStat
			continue
		}

		var err error
		switch aggOp {
		case sutils.Min, sutils.Max:
			err = aggregations.ComputeAggEvalForMinOrMax(measureAgg, sstMap, sr.segStatsResults.measureResults, aggOp == sutils.Min)
		case sutils.Range:
			err = aggregations.ComputeAggEvalForRange(measureAgg, sstMap, sr.segStatsResults.measureResults, sr.runningEvalStats)
		case sutils.Cardinality:
			err = aggregations.ComputeAggEvalForCardinality(measureAgg, sstMap, sr.segStatsResults.measureResults, sr.runningEvalStats)
		case sutils.Count:
			err = aggregations.ComputeAggEvalForCount(measureAgg, sstMap, sr.segStatsResults.measureResults)
		case sutils.Sum:
			err = aggregations.ComputeAggEvalForSum(measureAgg, sstMap, sr.segStatsResults.measureResults)
		case sutils.Sumsq:
			err = aggregations.ComputeAggEvalForSumsq(measureAgg, sstMap, sr.segStatsResults.measureResults)
		case sutils.Var:
			err = aggregations.ComputeAggEvalForVar(measureAgg, sstMap, sr.segStatsResults.measureResults, sr.runningEvalStats)
		case sutils.Varp:
			err = aggregations.ComputeAggEvalForVarp(measureAgg, sstMap, sr.segStatsResults.measureResults, sr.runningEvalStats)
		case sutils.Stdev:
			err = aggregations.ComputeAggEvalForStdev(measureAgg, sstMap, sr.segStatsResults.measureResults, sr.runningEvalStats)
		case sutils.Stdevp:
			err = aggregations.ComputeAggEvalForStdevp(measureAgg, sstMap, sr.segStatsResults.measureResults, sr.runningEvalStats)
		case sutils.Avg:
			err = aggregations.ComputeAggEvalForAvg(measureAgg, sstMap, sr.segStatsResults.measureResults, sr.runningEvalStats)
		case sutils.Values:
			err = aggregations.ComputeAggEvalForValues(measureAgg, sstMap, sr.segStatsResults.measureResults, sr.runningEvalStats)
		case sutils.List:
			err = aggregations.ComputeAggEvalForList(measureAgg, sstMap, sr.segStatsResults.measureResults, sr.runningEvalStats)
		case sutils.Perc:
			err = aggregations.ComputeAggEvalForPerc(measureAgg, sstMap, sr.segStatsResults.measureResults, sr.runningEvalStats)
		default:
			return fmt.Errorf("UpdateSegmentStats: does not support using aggOps: %v, qid=%v", aggOp, sr.qid)
		}
		if err != nil {
			return fmt.Errorf("UpdateSegmentStats: qid=%v, err: %v", sr.qid, err)
		}
	}

	return nil
}

func (sr *SearchResults) GetQueryCount() *structs.QueryCount {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()
	var shouldEarlyExit bool
	if sr.sAggs != nil {
		shouldEarlyExit = sr.sAggs.EarlyExit
	} else {
		shouldEarlyExit = true
	}
	qc := &structs.QueryCount{TotalCount: sr.resultCount, EarlyExit: shouldEarlyExit}
	if sr.EarlyExit {
		qc.Op = sutils.GreaterThanOrEqualTo
	} else {
		qc.Op = sutils.Equals
	}
	return qc
}

func (sr *SearchResults) GetTotalCount() uint64 {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()
	return sr.resultCount
}

func (sr *SearchResults) GetAggs() *structs.QueryAggregators {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()
	return sr.sAggs
}

// Adds remote rrc results to the search results
func (sr *SearchResults) MergeRemoteRRCResults(rrcs []*sutils.RecordResultContainer, grpByBuckets *blockresults.GroupByBucketsJSON,
	timeBuckets *blockresults.TimeBucketsJSON, allCols map[string]struct{}, rawLogs []map[string]interface{},
	remoteCount uint64, earlyExit bool) error {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()
	if len(rrcs) != len(rawLogs) {
		return fmt.Errorf("qid=%v, MergeRemoteRRCResults: rrcs and rawLogs length mismatch, len(rrcs): %v, len(rawLogs): %v", sr.qid, len(rrcs), len(rawLogs))
	}

	for cName := range allCols {
		sr.remoteInfo.remoteColumns[cName] = struct{}{}
	}

	for idx, rrc := range rrcs {
		addedRRC, removedID := sr.BlockResults.Add(rrc)
		if addedRRC {
			sr.addRawLog(rrc.SegKeyInfo.RecordId, rawLogs[idx])
		}
		sr.removeLog(removedID)
	}
	if earlyExit {
		sr.EarlyExit = true
	}
	err := sr.BlockResults.MergeRemoteBuckets(grpByBuckets, timeBuckets)
	if err != nil {
		log.Errorf("qid=%v, MergeRemoteRRCResults: Error merging remote buckets, err: %v", sr.qid, err)
		return err
	}
	sr.resultCount += remoteCount
	return nil
}

func (sr *SearchResults) AddSegmentStats(remoteStatsJSON *RemoteStatsJSON) error {
	remoteStats, err := remoteStatsJSON.ToRemoteStats()
	if err != nil {
		return fmt.Errorf("Error while converting RemoteStatsJSON to RemoteStats, qid=%v, err: %v", sr.qid, err)
	}

	return sr.MergeSegmentStats(sr.sAggs.MeasureOperations, *remoteStats)
}

// Get remote raw logs and columns based on the remoteID and all RRCs
func (sr *SearchResults) GetRemoteInfo(remoteID string, inrrcs []*sutils.RecordResultContainer, fetchAll bool) ([]map[string]interface{}, []string, error) {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()
	if sr.remoteInfo == nil {
		return nil, nil, fmt.Errorf("GetRemoteInfo: log does not have remote info, qid=%v", sr.qid)
	}
	if sr.ProcessedRemoteRecords[remoteID] == nil {
		sr.ProcessedRemoteRecords[remoteID] = make(map[string]struct{})
	}
	finalLogs := make([]map[string]interface{}, 0, len(inrrcs))
	rawLogs := sr.remoteInfo.remoteLogs
	remoteCols := sr.remoteInfo.remoteColumns
	count := 0
	for i := 0; i < len(inrrcs); i++ {
		if inrrcs[i].SegKeyInfo.IsRemote && (fetchAll || strings.HasPrefix(inrrcs[i].SegKeyInfo.RecordId, remoteID)) {
			_, isProcessed := sr.ProcessedRemoteRecords[remoteID][inrrcs[i].SegKeyInfo.RecordId]
			if isProcessed {
				continue
			}
			finalLogs = append(finalLogs, rawLogs[inrrcs[i].SegKeyInfo.RecordId])
			sr.ProcessedRemoteRecords[remoteID][inrrcs[i].SegKeyInfo.RecordId] = struct{}{}
			count++
		}
	}
	finalLogs = finalLogs[:count]

	allCols := make([]string, 0, len(remoteCols))
	idx := 0
	for col := range remoteCols {
		allCols = append(allCols, col)
		idx++
	}
	allCols = allCols[:idx]
	sort.Strings(allCols)
	return finalLogs, allCols, nil
}

func humanizeUints(v uint64) string {
	if v < 1000 {
		return strconv.FormatUint(v, 10)
	}
	parts := []string{"", "", "", "", "", "", ""}
	j := len(parts) - 1
	for v > 999 {
		parts[j] = strconv.FormatUint(v%1000, 10)
		switch len(parts[j]) {
		case 2:
			parts[j] = "0" + parts[j]
		case 1:
			parts[j] = "00" + parts[j]
		}
		v = v / 1000
		j--
	}
	parts[j] = strconv.FormatUint(v, 10)
	return strings.Join(parts[j:], ",")
}

func (sr *SearchResults) GetSegmentStatsResults(skEnc uint32, humanizeValues bool) ([]*structs.BucketHolder, []string, []string, []string, int) {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()

	if sr.segStatsResults == nil {
		return nil, nil, nil, nil, 0
	}
	delete(sr.allSSTS, skEnc)
	bucketHolder := &structs.BucketHolder{}
	bucketHolder.MeasureVal = make(map[string]interface{})
	bucketHolder.GroupByValues = []string{EMPTY_GROUPBY_KEY}
	var measureVal interface{}
	for mfName, aggVal := range sr.segStatsResults.measureResults {
		measureVal = aggVal.CVal
		switch aggVal.Dtype {
		case sutils.SS_DT_FLOAT:
			if humanizeValues {
				measureVal = humanize.CommafWithDigits(measureVal.(float64), 3)
			}
			bucketHolder.MeasureVal[mfName] = measureVal
		case sutils.SS_DT_UNSIGNED_NUM:
			if humanizeValues {
				measureVal = humanizeUints(aggVal.CVal.(uint64))
			}
			bucketHolder.MeasureVal[mfName] = measureVal
		case sutils.SS_DT_SIGNED_NUM:
			if humanizeValues {
				measureVal = humanize.Comma(aggVal.CVal.(int64))
			}
			bucketHolder.MeasureVal[mfName] = measureVal
		case sutils.SS_DT_STRING:
			bucketHolder.MeasureVal[mfName] = aggVal.CVal
		case sutils.SS_DT_STRING_SLICE:
			strVal, err := aggVal.GetString()
			if err != nil {
				log.Errorf("GetSegmentStatsResults: failed to convert string slice to string, qid: %v, err: %v", sr.qid, err)
				bucketHolder.MeasureVal[mfName] = ""
			} else {
				bucketHolder.MeasureVal[mfName] = strVal
			}
		default:
			log.Errorf("GetSegmentStatsResults: unsupported dtype: %v, qid=%v", aggVal.Dtype, sr.qid)
		}
	}
	aggMeasureResult := []*structs.BucketHolder{bucketHolder}
	return aggMeasureResult, sr.segStatsResults.measureFunctions, sr.segStatsResults.groupByCols, nil, 1
}

func (sr *SearchResults) GetSegmentStatsMeasureResults() map[string]sutils.CValueEnclosure {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()
	return sr.segStatsResults.measureResults
}

func (sr *SearchResults) GetSegmentRunningStats() []*structs.SegStats {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()
	return sr.runningSegStat
}

func (sr *SearchResults) GetGroupyByBuckets(limit int) ([]*structs.BucketHolder, []string, []string, map[string]int, int) {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()

	if sr.convertedBuckets != nil && !sr.statsAreFinal {
		sr.loadBucketsInternal()
	}

	bucketHolderArr, retMFuns, added := CreateMeasResultsFromAggResults(limit,
		sr.convertedBuckets)

	if sr.sAggs == nil || sr.sAggs.GroupByRequest == nil {
		return bucketHolderArr, retMFuns, nil, make(map[string]int), added
	} else {
		return bucketHolderArr, retMFuns, sr.sAggs.GroupByRequest.GroupByColumns, sr.ColumnsOrder, added
	}
}

// If agg.GroupByRequest.GroupByColumns == StatisticExpr.GroupByCols, which means there is only one groupby block in query
func (sr *SearchResults) IsOnlyStatisticGroupBy() bool {
	for agg := sr.sAggs; agg != nil; agg = agg.Next {
		if agg.GroupByRequest != nil && agg.GroupByRequest.GroupByColumns != nil {
			for _, groupByCol1 := range agg.GroupByRequest.GroupByColumns {
				for _, groupByCol2 := range sr.GetStatisticGroupByCols() {
					if groupByCol1 != groupByCol2 {
						return false
					}
				}
			}
			return true
		}
	}
	return false
}

func (sr *SearchResults) GetStatisticGroupByCols() []string {
	groupByCols := make([]string, 0)
	for agg := sr.sAggs; agg != nil; agg = agg.Next {
		if agg.OutputTransforms != nil && agg.OutputTransforms.LetColumns != nil && agg.OutputTransforms.LetColumns.StatisticColRequest != nil {
			groupByCols = append(agg.OutputTransforms.LetColumns.StatisticColRequest.FieldList, agg.OutputTransforms.LetColumns.StatisticColRequest.ByClause...)
			return groupByCols
		}
	}
	return groupByCols
}

// Subsequent calls may not return the same result as the previous may clean up the underlying heap used. Use GetResultsCopy to prevent this
func (sr *SearchResults) GetResults() []*sutils.RecordResultContainer {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()
	return sr.BlockResults.GetResults()
}

func (sr *SearchResults) GetResultsCopy() []*sutils.RecordResultContainer {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()
	return sr.BlockResults.GetResultsCopy()
}

func (sr *SearchResults) GetBucketResults() map[string]*structs.AggregationResult {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()

	if !sr.statsAreFinal {
		sr.loadBucketsInternal()
	}

	return sr.convertedBuckets
}

func (sr *SearchResults) SetFinalStatsFromNodeResult(nodeResult *structs.NodeResult) error {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()

	if sr.statsAreFinal {
		return fmt.Errorf("SetFinalStatsFromNodeResult: stats are already final, qid=%v", sr.qid)
	}

	sr.ColumnsOrder = nodeResult.ColumnsOrder
	if len(nodeResult.GroupByCols) > 0 {
		sr.convertedBuckets = nodeResult.Histogram
	} else {
		if length := len(nodeResult.MeasureResults); length != 1 {
			err := fmt.Errorf("SetFinalStatsFromNodeResult: unexpected MeasureResults length: %v, qid=%v",
				len(nodeResult.MeasureResults), sr.qid)
			log.Errorf("SetFinalStatsFromNodeResult: qid=%v, err: %v", sr.qid, err)
			return err
		}

		sr.segStatsResults.measureFunctions = nodeResult.MeasureFunctions
		sr.segStatsResults.measureResults = make(map[string]sutils.CValueEnclosure, len(nodeResult.MeasureFunctions))
		sr.segStatsResults.groupByCols = nil

		for _, measureFunc := range sr.segStatsResults.measureFunctions {
			value, ok := nodeResult.MeasureResults[0].MeasureVal[measureFunc]
			if !ok {
				err := fmt.Errorf("SetFinalStatsFromNodeResult: %v not found in MeasureVal, qid=%v", measureFunc, sr.qid)
				log.Errorf("SetFinalStatsFromNodeResult: qid=%v, err: %v", sr.qid, err)
				return err
			}

			// Create a CValueEnclosure for `value`.
			var valueAsEnclosure sutils.CValueEnclosure
			valueStr, ok := value.(string)
			if !ok {
				err := fmt.Errorf("SetFinalStatsFromNodeResult: unexpected type: %T, qid=%v", value, sr.qid)
				log.Errorf("SetFinalStatsFromNodeResult: qid=%v, err: %v", sr.qid, err)
				return err
			}

			// Remove any commas.
			valueStr = strings.ReplaceAll(valueStr, ",", "")

			if valueFloat, err := strconv.ParseFloat(valueStr, 64); err == nil {
				valueAsEnclosure.Dtype = sutils.SS_DT_FLOAT
				valueAsEnclosure.CVal = valueFloat
			} else if valueInt, err := strconv.ParseInt(valueStr, 10, 64); err == nil {
				valueAsEnclosure.Dtype = sutils.SS_DT_SIGNED_NUM
				valueAsEnclosure.CVal = valueInt
			} else {
				valueAsEnclosure.Dtype = sutils.SS_DT_STRING
				valueAsEnclosure.CVal = valueStr
			}

			sr.segStatsResults.measureResults[measureFunc] = valueAsEnclosure
		}
	}

	sr.statsAreFinal = true

	return nil
}

func (sr *SearchResults) GetNumBuckets() int {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()
	retVal := 0
	if sr.BlockResults == nil {
		return 0
	}
	if sr.BlockResults.TimeAggregation != nil {
		retVal += len(sr.BlockResults.TimeAggregation.AllRunningBuckets)
	}
	if sr.BlockResults.GroupByAggregation != nil {
		retVal += len(sr.BlockResults.GroupByAggregation.AllRunningBuckets)
	}
	return retVal
}

func (sr *SearchResults) loadBucketsInternal() {
	if sr.statsAreFinal {
		log.Errorf("loadBucketsInternal: cannot update convertedBuckets because stats are final, qid %v", sr.qid)
		return
	}

	retVal := make(map[string]*structs.AggregationResult)
	if sr.BlockResults.TimeAggregation != nil {
		retVal[sr.sAggs.TimeHistogram.AggName] = sr.BlockResults.GetTimeBuckets()
	}
	if sr.BlockResults.GroupByAggregation != nil {
		retVal[sr.sAggs.GroupByRequest.AggName] = sr.BlockResults.GetGroupByBuckets()
	}
	sr.convertedBuckets = retVal
}

func (sr *SearchResults) GetAllErrors() []error {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()
	return sr.AllErrors
}

// returns if the segkey needs to be searched or if we have hit an early exit
func (sr *SearchResults) ShouldSearchSegKey(tRange *dtu.TimeRange,
	snt structs.SearchNodeType, otherAggsPresent bool, timeAggs bool) EarlyExitType {

	// do we have enough RRCs?
	if sr.ShouldContinueRRCSearch() {
		return EetContSearch
	}
	if sr.queryType == structs.GroupByCmd {
		if sr.GetNumBuckets() < sr.sAggs.GroupByRequest.BucketCount {
			return EetContSearch
		} else {
			return EetEarlyExit
		}
	} else if sr.queryType != structs.RRCCmd {
		return EetContSearch
	}

	// do the RRCs we have pass the sort check?
	if sr.sAggs != nil && sr.sAggs.Sort != nil {
		var willValBeAdded bool
		if sr.sAggs.Sort.Ascending {
			willValBeAdded = sr.BlockResults.WillValueBeAdded(float64(tRange.StartEpochMs))
		} else {
			willValBeAdded = sr.BlockResults.WillValueBeAdded(float64(tRange.EndEpochMs))
		}
		if willValBeAdded {
			return EetContSearch
		}
	}

	// do we have all sorted RRCs and now need to only run date histogram?
	if snt == structs.MatchAllQuery && timeAggs && !otherAggsPresent {
		return EetMatchAllAggs
	}

	// do we have all sorted RRCs but still need raw search to complete the rest of the buckets?
	if snt != structs.MatchAllQuery && (timeAggs || otherAggsPresent) {
		return EetContSearch
	}

	// do we have all sorted RRCs with no aggs?
	if sr.sAggs == nil {
		return EetEarlyExit
	}

	// do we have all sorted RRCs but should not early exit?
	if !sr.sAggs.EarlyExit {
		return EetContSearch
	}

	// do we have all sorted RRCs and now need to run aggregations?
	if sr.sAggs.TimeHistogram != nil {
		return EetContSearch
	}

	// do we have all sorted RRCs and now need to run aggregations?
	if sr.sAggs.GroupByRequest != nil {
		return EetContSearch
	}

	return EetEarlyExit
}

// returns true in following cases:
// 1. search is not rrc
// 1. if any value in a block will be added based on highTs and lowTs
// 2. if time buckets exist
func (sr *SearchResults) ShouldSearchRange(lowTs, highTs uint64) bool {
	if sr.queryType != structs.RRCCmd {
		return true
	}

	if sr.ShouldContinueRRCSearch() {
		return true
	}
	if sr.sAggs == nil {
		return false
	}
	if !sr.sAggs.EarlyExit {
		return true
	}
	if sr.sAggs.TimeHistogram != nil {
		return true
	}

	if sr.sAggs.Sort != nil {
		if sr.sAggs.Sort.Ascending {
			return sr.BlockResults.WillValueBeAdded(float64(lowTs))
		} else {
			return sr.BlockResults.WillValueBeAdded(float64(highTs))
		}
	}
	return false
}

// sets early exit to value
func (sr *SearchResults) SetEarlyExit(exited bool) {
	sr.EarlyExit = exited
}

func (sr *SearchResults) GetAddSegEnc(sk string) uint32 {

	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()

	retval, ok := sr.SegKeyToEnc[sk]
	if ok {
		return retval
	}

	retval = sr.NextSegKeyEnc
	sr.SegEncToKey[sr.NextSegKeyEnc] = sk
	sr.SegKeyToEnc[sk] = sr.NextSegKeyEnc
	sr.NextSegKeyEnc++
	return retval
}

// helper struct to coordinate parallel segstats results
type StatsResults struct {
	rwLock  *sync.RWMutex
	ssStats map[string]*structs.SegStats // maps column name to segstats
}

func InitStatsResults() *StatsResults {
	return &StatsResults{
		rwLock:  &sync.RWMutex{},
		ssStats: make(map[string]*structs.SegStats),
	}
}

func (sr *StatsResults) MergeSegStats(m1 map[string]*structs.SegStats) {
	sr.rwLock.Lock()
	sr.ssStats = stats.MergeSegStats(sr.ssStats, m1)
	sr.rwLock.Unlock()
}

func (sr *StatsResults) GetSegStats() map[string]*structs.SegStats {
	sr.rwLock.Lock()
	retVal := sr.ssStats
	sr.rwLock.Unlock()
	return retVal
}

func CreateMeasResultsFromAggResults(limit int,
	aggRes map[string]*structs.AggregationResult) ([]*structs.BucketHolder, []string, int) {
	batchErr := utils.NewBatchError()
	defer batchErr.LogAllErrors()

	newQueryPipeline := config.IsNewQueryPipelineEnabled()

	bucketHolderArr := make([]*structs.BucketHolder, 0)
	added := int(0)
	internalMFuncs := make(map[string]bool)
	for _, agg := range aggRes {
		for _, aggVal := range agg.Results {
			measureVal := make(map[string]interface{})
			groupByValues := make([]string, 0)
			iGroupByValues := make([]sutils.CValueEnclosure, 0)
			for mName, mVal := range aggVal.StatRes {
				rawVal, err := mVal.GetValue()
				if err != nil {
					batchErr.AddError("CreateMeasResultsFromAggResults:RAW_VALUE_ERR", err)
					continue
				}
				internalMFuncs[mName] = true
				measureVal[mName] = rawVal

			}
			if added >= limit {
				break
			}

			if newQueryPipeline {
				bucketKeySlice, ok := aggVal.BucketKey.([]interface{})
				if !ok {
					batchErr.AddError("CreateMeasResultsFromAggResults:UNKNOWN_BUCKET_KEY_TYPE", fmt.Errorf("expected []interface{} got bucket Key Type as %T", aggVal.BucketKey))
					continue
				}

				if len(bucketKeySlice) == 0 {
					log.Errorf("CreateMeasResultsFromAggResults : bucketKeySlice is empty")
					continue
				}

				for _, bk := range bucketKeySlice {
					cValue := sutils.CValueEnclosure{}
					err := cValue.ConvertValue(bk)
					if err != nil {
						batchErr.AddError("CreateMeasResultsFromAggResults:CONVERT_BUCKET_KEY_ERR", err)
						cValue = sutils.CValueEnclosure{
							Dtype: sutils.SS_DT_STRING,
							CVal:  fmt.Sprintf("%+v", bk),
						}
					}
					iGroupByValues = append(iGroupByValues, cValue)
				}
			}

			switch bKey := aggVal.BucketKey.(type) {
			case float64, uint64, int64:
				bKeyConv := fmt.Sprintf("%+v", bKey)
				groupByValues = append(groupByValues, bKeyConv)
				added++
			case []string:
				groupByValues = append(groupByValues, aggVal.BucketKey.([]string)...)
				added++
			case string:
				groupByValues = append(groupByValues, bKey)
				added++
			case []interface{}:
				for _, bk := range aggVal.BucketKey.([]interface{}) {
					groupByValues = append(groupByValues, fmt.Sprintf("%+v", bk))
				}
				added++
			default:
				batchErr.AddError("CreateMeasResultsFromAggResults:UNKNOWN_BUCKET_KEY_TYPE", fmt.Errorf("got bucket Key Type as %T", bKey))
			}
			bucketHolder := &structs.BucketHolder{
				IGroupByValues: iGroupByValues,
				GroupByValues:  groupByValues,
				MeasureVal:     measureVal,
			}
			bucketHolderArr = append(bucketHolderArr, bucketHolder)
		}
	}

	retMFuns := make([]string, len(internalMFuncs))
	idx := 0
	for mName := range internalMFuncs {
		retMFuns[idx] = mName
		idx++
	}

	return bucketHolderArr, retMFuns, added
}

func (sr *SearchResults) MergeSegmentStats(measureOps []*structs.MeasureAggregator, remoteStats RemoteStats) error {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()
	for idx, measureAgg := range measureOps {
		aggOp := measureAgg.MeasureFunc
		if idx >= len(remoteStats.SegStats) {
			return fmt.Errorf("MergeSegmentStats: remoteStats.SegStats is smaller than measureOps, qid=%v", sr.qid)
		}

		if measureAgg.ValueColRequest == nil {
			remoteSegStat := remoteStats.SegStats[idx]
			if remoteSegStat == nil {
				continue // remote segment stats could be nil
			}
			resSegStat, err := sr.UpdateNonEvalSegStats(sr.runningSegStat[idx], remoteSegStat, measureAgg)
			if err != nil {
				return fmt.Errorf("MergeSegmentStats: Error while updating non eval seg stats qid=%v, err: %v", sr.qid, err)
			}
			sr.runningSegStat[idx] = resSegStat
			continue
		}

		remoteRes, exist := remoteStats.EvalStats[measureAgg.String()]
		if !exist {
			continue
		}

		// For eval statements in aggregate functions, there should be only one field for min and max
		switch aggOp {
		case sutils.Min, sutils.Max, sutils.Sum, sutils.Count:
			currRes := sr.segStatsResults.measureResults[measureAgg.String()]
			remoteResCVal := sutils.CValueEnclosure{}
			err := remoteResCVal.ConvertValue(remoteRes.MeasureResult)
			if err != nil {
				return fmt.Errorf("MergeSegmentStats: Error while converting value for %v qid=%v, err: %v", measureAgg.String(), sr.qid, err)
			}
			sr.segStatsResults.measureResults[measureAgg.String()], err = blockresults.ReduceForEval(currRes, remoteResCVal, aggOp)
			if err != nil {
				return fmt.Errorf("MergeSegmentStats: Error while merging results for %v qid=%v, err: %v", measureAgg.String(), sr.qid, err)
			}
		case sutils.Range:
			var currRangeStat *structs.RangeStat
			currVal, exist := sr.runningEvalStats[measureAgg.String()]
			if exist {
				isRangeStat := false
				currRangeStat, isRangeStat = currVal.(*structs.RangeStat)
				if !isRangeStat {
					return fmt.Errorf("MergeSegmentStats: RangeStat not found for range agg %v, qid=%v", measureAgg.String(), sr.qid)
				}
			}
			finalRangeStat := blockresults.ReduceRange(currRangeStat, remoteRes.RangeStat)
			sr.runningEvalStats[measureAgg.String()] = finalRangeStat
			if finalRangeStat != nil {
				sr.segStatsResults.measureResults[measureAgg.String()] = sutils.CValueEnclosure{
					Dtype: sutils.SS_DT_FLOAT,
					CVal:  finalRangeStat.Max - finalRangeStat.Min,
				}
			}
		case sutils.Avg:
			var currAvgStat *structs.AvgStat
			currVal, exist := sr.runningEvalStats[measureAgg.String()]
			if exist {
				isAvgStat := false
				currAvgStat, isAvgStat = currVal.(*structs.AvgStat)
				if !isAvgStat {
					return fmt.Errorf("MergeSegmentStats: AvgStat not found for avg agg %v, qid=%v", measureAgg.String(), sr.qid)
				}
			}
			finalAvgStat := blockresults.ReduceAvg(currAvgStat, remoteRes.AvgStat)
			sr.runningEvalStats[measureAgg.String()] = finalAvgStat
			if finalAvgStat != nil {
				sr.segStatsResults.measureResults[measureAgg.String()] = sutils.CValueEnclosure{
					Dtype: sutils.SS_DT_FLOAT,
					CVal:  finalAvgStat.Sum / float64(finalAvgStat.Count),
				}
			}
		case sutils.Cardinality, sutils.Values:
			currSet, exist := sr.runningEvalStats[measureAgg.String()]
			if !exist {
				currSet = make(map[string]struct{})
				sr.runningEvalStats[measureAgg.String()] = currSet
			}

			CValEnc, err := sutils.Reduce(sutils.CValueEnclosure{
				Dtype: sutils.SS_DT_STRING_SET,
				CVal:  currSet,
			},
				sutils.CValueEnclosure{
					Dtype: sutils.SS_DT_STRING_SET,
					CVal:  remoteRes.StrSet,
				},
				aggOp)

			if err != nil {
				return fmt.Errorf("MergeSegmentStats: Error while merging string sets for %v qid=%v, err: %v", measureAgg.String(), sr.qid, err)
			}
			if CValEnc.Dtype != sutils.SS_DT_STRING_SET {
				return fmt.Errorf("MergeSegmentStats: Error while merging string sets for %v qid=%v, dtype: %v", measureAgg.String(), sr.qid, CValEnc.Dtype)
			}

			sr.runningEvalStats[measureAgg.String()] = CValEnc.CVal.(map[string]struct{})

			if measureAgg.MeasureFunc == sutils.Cardinality {
				sr.segStatsResults.measureResults[measureAgg.String()] = sutils.CValueEnclosure{
					Dtype: sutils.SS_DT_SIGNED_NUM,
					CVal:  int64(len(CValEnc.CVal.(map[string]struct{}))),
				}
			} else {
				sr.segStatsResults.measureResults[measureAgg.String()] = sutils.CValueEnclosure{
					Dtype: sutils.SS_DT_STRING_SLICE,
					CVal:  utils.GetSortedStringKeys(CValEnc.CVal.(map[string]struct{})),
				}
			}
		case sutils.List:
			_, exist = sr.runningEvalStats[measureAgg.String()]
			if !exist {
				sr.runningEvalStats[measureAgg.String()] = make([]string, 0)
			}
			currList, isList := sr.runningEvalStats[measureAgg.String()].([]string)
			if !isList {
				return fmt.Errorf("MergeSegmentStats: String list not found for list agg %v, qid=%v", measureAgg.String(), sr.qid)
			}
			remoteList := remoteRes.StrList
			currList = sutils.AppendWithLimit(currList, remoteList, sutils.MAX_SPL_LIST_SIZE)

			sr.runningEvalStats[measureAgg.String()] = currList
			sr.segStatsResults.measureResults[measureAgg.String()] = sutils.CValueEnclosure{
				Dtype: sutils.SS_DT_STRING_SLICE,
				CVal:  currList,
			}
		default:
			return fmt.Errorf("MergeSegmentStats: does not support using aggOps: %v, qid=%v", aggOp, sr.qid)
		}
	}
	return nil
}

func (sr *SearchResults) GetRemoteStats() (*RemoteStats, error) {
	sr.updateLock.Lock()
	defer sr.updateLock.Unlock()

	remoteStats := &RemoteStats{
		SegStats: sr.runningSegStat,
	}

	remoteStats.EvalStats = make(map[string]EvalStatsMetaData)
	for _, measureAgg := range sr.sAggs.MeasureOperations {
		if measureAgg.ValueColRequest != nil {
			switch measureAgg.MeasureFunc {
			case sutils.Min, sutils.Max, sutils.Count, sutils.Sum:
				_, exist := sr.segStatsResults.measureResults[measureAgg.String()]
				if exist {
					remoteStats.EvalStats[measureAgg.String()] = EvalStatsMetaData{
						MeasureResult: sr.segStatsResults.measureResults[measureAgg.String()].CVal,
					}
				}
			case sutils.Range:
				metadata, exist := sr.runningEvalStats[measureAgg.String()]
				if exist {
					_, isRangeStat := metadata.(*structs.RangeStat)
					if !isRangeStat {
						return nil, fmt.Errorf("GetRemoteStats: RangeStat not found for range agg %v, qid=%v", measureAgg.String(), sr.qid)
					}
					remoteStats.EvalStats[measureAgg.String()] = EvalStatsMetaData{
						RangeStat: metadata.(*structs.RangeStat),
					}
				}
			case sutils.Avg:
				metadata, exist := sr.runningEvalStats[measureAgg.String()]
				if exist {
					_, isAvgStat := metadata.(*structs.AvgStat)
					if !isAvgStat {
						return nil, fmt.Errorf("GetRemoteStats: AvgStat not found for avg agg %v, qid=%v", measureAgg.String(), sr.qid)
					}
					remoteStats.EvalStats[measureAgg.String()] = EvalStatsMetaData{
						AvgStat: metadata.(*structs.AvgStat),
					}
				}
			case sutils.Cardinality, sutils.Values:
				metadata, exist := sr.runningEvalStats[measureAgg.String()]
				if exist {
					_, isStrSet := metadata.(map[string]struct{})
					if !isStrSet {
						return nil, fmt.Errorf("GetRemoteStats: String set not found for agg %v, qid=%v", measureAgg.String(), sr.qid)
					}
					remoteStats.EvalStats[measureAgg.String()] = EvalStatsMetaData{
						StrSet: metadata.(map[string]struct{}),
					}
				}
			case sutils.List:
				metadata, exist := sr.runningEvalStats[measureAgg.String()]
				if exist {
					_, isStrList := metadata.([]string)
					if !isStrList {
						return nil, fmt.Errorf("GetRemoteStats: String list not found for agg %v, qid=%v", measureAgg.String(), sr.qid)
					}
					remoteStats.EvalStats[measureAgg.String()] = EvalStatsMetaData{
						StrList: metadata.([]string),
					}
				}
			default:
				return nil, fmt.Errorf("GetRemoteStats: unsupported aggOps: %v, qid=%v", measureAgg.MeasureFunc, sr.qid)
			}
		}
	}

	return remoteStats, nil
}

func (rs *RemoteStats) RemoteStatsToJSON() (*RemoteStatsJSON, error) {
	var err error

	remoteStatsJson := &RemoteStatsJSON{
		EvalStats: rs.EvalStats,
	}

	remoteStatsJson.SegStats = make([]*structs.SegStatsJSON, len(rs.SegStats))
	for idx, segStat := range rs.SegStats {
		if segStat == nil {
			continue
		}
		remoteStatsJson.SegStats[idx], err = segStat.ToJSON()
		if err != nil {
			return nil, err
		}
	}

	return remoteStatsJson, nil
}

func (rj *RemoteStatsJSON) ToRemoteStats() (*RemoteStats, error) {
	var err error
	remoteStats := &RemoteStats{
		EvalStats: rj.EvalStats,
	}
	remoteStats.SegStats = make([]*structs.SegStats, len(rj.SegStats))
	for idx, segStatJSON := range rj.SegStats {
		if segStatJSON == nil {
			continue
		}
		remoteStats.SegStats[idx], err = segStatJSON.ToStats()
		if err != nil {
			return nil, err
		}
	}

	return remoteStats, nil
}
