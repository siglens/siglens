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

package summary

import (
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/segment/structs"
	uStats "github.com/siglens/siglens/pkg/usageStats"
	log "github.com/sirupsen/logrus"
)

type SearchTypeEnum int

const (
	RAW SearchTypeEnum = iota
	PQS
	STREE
	GRPC_ROTATED
	GRPC_UNROTATED
)

type QueryType int

const (
	LOGS QueryType = iota
	METRICS
)

type searchTypeSummary struct {
	numFilesSearched  uint64
	numMatched        uint64
	minPerSegment     uint64
	seenOneVal        bool
	maxPerSegment     uint64
	searchTimeHistory []time.Duration
	totalTime         time.Duration
}

type MetadataSummary struct {
	numCMIBlocksChecked uint64
	numCMIBlocksPassed  uint64
	allTimes            []time.Duration
	totalTime           time.Duration
}

type metricsQueryTypeSummary struct {
	timeGettingRotatedSearchRequests   time.Duration
	timeGettingUnrotatedSearchRequests time.Duration
	timeSearchingTagsTrees             []time.Duration
	timeLoadingTSOFiles                []time.Duration
	timeLoadingTSGFiles                []time.Duration
}

type QuerySummary struct {
	queryType                   QueryType
	updateLock                  *sync.Mutex
	qid                         uint64
	ticker                      *time.Ticker
	tickerStopChan              chan bool
	startTime                   time.Time
	rawSearchTime               time.Duration
	queryTotalTime              time.Duration
	totalNumFiles               uint64
	numRecordsSearched          uint64
	numRecordsMatched           uint64
	remoteRecordsSearched       uint64
	remoteRecordsMatched        uint64
	numMetricsSegmentsSearched  uint64
	numTSIDsMatched             uint64
	numTagsTreesSearched        uint64
	numTSOFilesLoaded           uint64
	numTSGFilesLoaded           uint64
	numSeriesSearched           uint64
	numResultSeries             uint64
	allQuerySummaries           map[SearchTypeEnum]*searchTypeSummary
	metricsQuerySummary         *metricsQueryTypeSummary
	metadataSummary             MetadataSummary
	numBuckets                  int
	remainingDistributedQueries uint64
	totalDistributedQueries     uint64
}

// InitQuerySummary returns a struct to store query level search stats.
// This function starts a ticker to log info about long running queries.
// Caller is responsible for calling LogSummary() function to stop the ticker.
func InitQuerySummary(queryType QueryType, qid uint64) *QuerySummary {
	lock := &sync.Mutex{}
	rawSearchTypeSummary := &searchTypeSummary{}
	pqsSearchTypeSummary := &searchTypeSummary{}
	agileTreeSearchTypeSummary := &searchTypeSummary{}
	allQuerySummaries := make(map[SearchTypeEnum]*searchTypeSummary)
	allQuerySummaries[RAW] = rawSearchTypeSummary
	allQuerySummaries[PQS] = pqsSearchTypeSummary
	allQuerySummaries[STREE] = agileTreeSearchTypeSummary
	allQuerySummaries[GRPC_ROTATED] = &searchTypeSummary{}
	allQuerySummaries[GRPC_UNROTATED] = &searchTypeSummary{}

	qs := &QuerySummary{
		queryType:           queryType,
		updateLock:          lock,
		qid:                 qid,
		startTime:           time.Now(),
		queryTotalTime:      time.Duration(0),
		allQuerySummaries:   allQuerySummaries,
		metricsQuerySummary: &metricsQueryTypeSummary{},
		metadataSummary: MetadataSummary{
			allTimes: make([]time.Duration, 0),
		},
		remainingDistributedQueries: 0,
	}
	qs.startTicker()
	return qs
}

func (qs *QuerySummary) startTicker() {
	qs.ticker = time.NewTicker(10 * time.Second)
	qs.tickerStopChan = make(chan bool)
	go qs.tickWatcher()
}

func (qs *QuerySummary) stopTicker() {
	if qs.ticker != nil {
		qs.ticker.Stop()
		close(qs.tickerStopChan)
	}
}

func (qs *QuerySummary) tickWatcher() {
	defer func() {
		qs.ticker.Stop()
		qs.ticker = nil
		qs.tickerStopChan = nil
	}()
	for {
		select {
		case <-qs.tickerStopChan:
			return
		case <-qs.ticker.C:
			if qs.queryType == LOGS {
				remainingDQsString := ""
				var addDistributedInfo bool
				if hook := hooks.GlobalHooks.ShouldAddDistributedInfoHook; hook != nil {
					addDistributedInfo = hook()
				}

				if addDistributedInfo {
					remainingDQsString = fmt.Sprintf(", remaining distributed queries: %v", qs.remainingDistributedQueries)
				}

				log.Infof("qid=%d, still executing. Time Elapsed (%v), files so far: PQS: %v, RAW: %v, STREE: %v%v", qs.qid, time.Since(qs.startTime),
					len(qs.allQuerySummaries[PQS].searchTimeHistory),
					len(qs.allQuerySummaries[RAW].searchTimeHistory),
					len(qs.allQuerySummaries[STREE].searchTimeHistory),
					remainingDQsString)
			} else if qs.queryType == METRICS {
				log.Infof("qid=%d, still executing. Time Elapsed (%v). So far, number of metrics segments searched=%+v, number of TSIDs searched=%+v across %+v tags trees, number of TSO files loaded=%+v, number of TSG files loaded=%+v", qs.qid, time.Since(qs.startTime), qs.getNumMetricsSegmentsSearched(), qs.getNumTSIDsMatched(), qs.getNumTagsTreesSearched(), qs.getNumTSOFilesLoaded(), qs.getNumTSGFilesLoaded())
			}
		}
	}
}

func (qs *QuerySummary) UpdateCMIResults(numBlocksChecked, passedBlocks uint64) {
	qs.metadataSummary.numCMIBlocksChecked += numBlocksChecked
	qs.metadataSummary.numCMIBlocksPassed += passedBlocks
}

func (qs *QuerySummary) IncrementNumTagsTreesSearched(record uint64) {
	qs.updateLock.Lock()
	qs.numTagsTreesSearched += record
	qs.updateLock.Unlock()
}

func (qs *QuerySummary) IncrementNumTSIDsMatched(record uint64) {
	qs.updateLock.Lock()
	qs.numTSIDsMatched += record
	qs.updateLock.Unlock()
}

func (qs *QuerySummary) IncrementNumResultSeries(record uint64) {
	qs.updateLock.Lock()
	qs.numResultSeries += record
	qs.updateLock.Unlock()
}

func (qs *QuerySummary) UpdateExtractSSRTime(t time.Duration) {
	qs.metadataSummary.allTimes = append(qs.metadataSummary.allTimes, t)
	qs.metadataSummary.totalTime += t
}

func (qs *QuerySummary) SetRRCFinishTime() {
	qs.rawSearchTime = time.Since(qs.startTime)
}

func (qs *QuerySummary) UpdateSummary(searchType SearchTypeEnum, ttime time.Duration, queryMetrics *structs.QueryProcessingMetrics) {
	qs.updateLock.Lock()
	qs.numRecordsSearched += queryMetrics.NumRecordsToRawSearch
	qs.numRecordsMatched += queryMetrics.NumRecordsMatched
	qs.totalNumFiles += 1
	qs.updateLock.Unlock()
	qs.updateSearchTime(ttime, qs.allQuerySummaries[searchType])
	qs.updateNumFilesSearched(qs.allQuerySummaries[searchType])
	qs.updateNumMatched(queryMetrics.NumRecordsMatched, qs.allQuerySummaries[searchType])
	qs.updateTotalTime(ttime, qs.allQuerySummaries[searchType])
}

func (qs *QuerySummary) UpdateRemoteSummary(searchType SearchTypeEnum, ttime time.Duration, numSearched uint64, numMatched uint64) {
	qs.updateLock.Lock()
	qs.numRecordsSearched += numSearched
	qs.numRecordsMatched += numMatched
	qs.remoteRecordsMatched += numMatched
	qs.remoteRecordsSearched += numSearched
	qs.updateLock.Unlock()
	qs.updateSearchTime(ttime, qs.allQuerySummaries[searchType])
	qs.updateNumFilesSearched(qs.allQuerySummaries[searchType])
	qs.updateTotalTime(ttime, qs.allQuerySummaries[searchType])
}

func (qs *QuerySummary) IncrementNumberDistributedQuery() {
	atomic.AddUint64(&qs.totalDistributedQueries, 1)
}

func (qs *QuerySummary) UpdateMetricsSummary(queryMetrics *structs.MetricsQueryProcessingMetrics) {
	qs.updateLock.Lock()
	qs.numTSOFilesLoaded += queryMetrics.NumTSOFilesLoaded
	qs.numTSGFilesLoaded += queryMetrics.NumTSGFilesLoaded
	qs.numSeriesSearched += queryMetrics.NumSeriesSearched
	qs.numMetricsSegmentsSearched += queryMetrics.NumMetricsSegmentsSearched
	qs.updateLock.Unlock()
}

func (qs *QuerySummary) updateSearchTime(ttime time.Duration, searchTypeSummary *searchTypeSummary) {
	qs.updateLock.Lock()
	defer qs.updateLock.Unlock()
	i := sort.Search(len(searchTypeSummary.searchTimeHistory), func(i int) bool { return searchTypeSummary.searchTimeHistory[i] >= ttime })
	searchTypeSummary.searchTimeHistory = append(searchTypeSummary.searchTimeHistory, time.Duration(0))
	copy(searchTypeSummary.searchTimeHistory[i+1:], searchTypeSummary.searchTimeHistory[i:])
	searchTypeSummary.searchTimeHistory[i] = ttime
}

func (qs *QuerySummary) updateTotalTime(ttime time.Duration, searchTypeSummary *searchTypeSummary) {
	qs.updateLock.Lock()
	defer qs.updateLock.Unlock()
	searchTypeSummary.totalTime += ttime
}

func (qs *QuerySummary) updateNumFilesSearched(searchTypeSummary *searchTypeSummary) {
	qs.updateLock.Lock()
	defer qs.updateLock.Unlock()
	searchTypeSummary.numFilesSearched += 1
}

func (qs *QuerySummary) updateNumMatched(numRecs uint64, searchTypeSummary *searchTypeSummary) {
	qs.updateLock.Lock()
	defer qs.updateLock.Unlock()
	searchTypeSummary.numMatched += numRecs
	if !searchTypeSummary.seenOneVal || searchTypeSummary.minPerSegment > numRecs {
		searchTypeSummary.minPerSegment = numRecs
	}
	if !searchTypeSummary.seenOneVal || searchTypeSummary.maxPerSegment < numRecs {
		searchTypeSummary.maxPerSegment = numRecs
	}
	searchTypeSummary.seenOneVal = true
}

func (qs *QuerySummary) UpdateQueryTotalTime(ttime time.Duration, nBuckets int) {
	qs.updateLock.Lock()
	defer qs.updateLock.Unlock()
	qs.queryTotalTime = ttime
	qs.numBuckets = nBuckets
}

func (qs *QuerySummary) UpdateTimeGettingRotatedSearchRequests(ttime time.Duration) {
	qs.updateLock.Lock()
	qs.metricsQuerySummary.timeGettingRotatedSearchRequests += ttime
	qs.updateLock.Unlock()
}

func (qs *QuerySummary) UpdateTimeGettingUnrotatedSearchRequests(ttime time.Duration) {
	qs.updateLock.Lock()
	qs.metricsQuerySummary.timeGettingUnrotatedSearchRequests += ttime
	qs.updateLock.Unlock()
}

func (qs *QuerySummary) UpdateTimeSearchingTagsTrees(ttime time.Duration) {
	qs.updateLock.Lock()
	defer qs.updateLock.Unlock()
	i := sort.Search(len(qs.metricsQuerySummary.timeSearchingTagsTrees), func(i int) bool { return qs.metricsQuerySummary.timeSearchingTagsTrees[i] >= ttime })
	qs.metricsQuerySummary.timeSearchingTagsTrees = append(qs.metricsQuerySummary.timeSearchingTagsTrees, time.Duration(0))
	copy(qs.metricsQuerySummary.timeSearchingTagsTrees[i+1:], qs.metricsQuerySummary.timeSearchingTagsTrees[i:])
	qs.metricsQuerySummary.timeSearchingTagsTrees[i] = ttime
}

func (qs *QuerySummary) UpdateTimeLoadingTSOFiles(ttime time.Duration) {
	qs.updateLock.Lock()
	defer qs.updateLock.Unlock()
	i := sort.Search(len(qs.metricsQuerySummary.timeLoadingTSOFiles), func(i int) bool { return qs.metricsQuerySummary.timeLoadingTSOFiles[i] >= ttime })
	qs.metricsQuerySummary.timeLoadingTSOFiles = append(qs.metricsQuerySummary.timeLoadingTSOFiles, time.Duration(0))
	copy(qs.metricsQuerySummary.timeLoadingTSOFiles[i+1:], qs.metricsQuerySummary.timeLoadingTSOFiles[i:])
	qs.metricsQuerySummary.timeLoadingTSOFiles[i] = ttime
}

func (qs *QuerySummary) UpdateTimeLoadingTSGFiles(ttime time.Duration) {
	qs.updateLock.Lock()
	defer qs.updateLock.Unlock()
	i := sort.Search(len(qs.metricsQuerySummary.timeLoadingTSGFiles), func(i int) bool { return qs.metricsQuerySummary.timeLoadingTSGFiles[i] >= ttime })
	qs.metricsQuerySummary.timeLoadingTSGFiles = append(qs.metricsQuerySummary.timeLoadingTSGFiles, time.Duration(0))
	copy(qs.metricsQuerySummary.timeLoadingTSGFiles[i+1:], qs.metricsQuerySummary.timeLoadingTSGFiles[i:])
	qs.metricsQuerySummary.timeLoadingTSGFiles[i] = ttime
}

func (qs *QuerySummary) getTotalTime(searchType SearchTypeEnum) time.Duration {
	qs.updateLock.Lock()
	defer qs.updateLock.Unlock()
	return qs.allQuerySummaries[searchType].totalTime
}

func (qs *QuerySummary) getQueryTotalTime() time.Duration {
	qs.updateLock.Lock()
	defer qs.updateLock.Unlock()
	return qs.queryTotalTime
}

func (qs *QuerySummary) getMinSearchTime(searchType SearchTypeEnum) float64 {
	qs.updateLock.Lock()
	defer qs.updateLock.Unlock()
	return getMinSearchTimeFromArr(qs.allQuerySummaries[searchType].searchTimeHistory)
}

func getMinSearchTimeFromArr(arr []time.Duration) float64 {
	if len(arr) > 0 {
		ret := float64(arr[0] / time.Nanosecond)
		return ret / 1000_000
	}
	return 0
}

func (qs *QuerySummary) getMaxSearchTime(searchType SearchTypeEnum) float64 {
	qs.updateLock.Lock()
	defer qs.updateLock.Unlock()
	return getMaxSearchTimeFromArr(qs.allQuerySummaries[searchType].searchTimeHistory)
}

func getMaxSearchTimeFromArr(arr []time.Duration) float64 {
	if len(arr) > 0 {
		ret := float64(arr[len(arr)-1]) / float64(time.Nanosecond)
		return ret / 1000_000
	}
	return 0
}

func getSumSearchTimeFromArr(arr []time.Duration) float64 {
	sum := float64(0)
	for _, num := range arr {
		sum += float64(num) / float64(time.Nanosecond)
	}
	return sum / 1000_000
}

func (qs *QuerySummary) getSearchTimeHistory(searchType SearchTypeEnum) []float64 {
	qs.updateLock.Lock()
	defer qs.updateLock.Unlock()
	retval := make([]float64, len(qs.allQuerySummaries[searchType].searchTimeHistory))
	for i, val := range qs.allQuerySummaries[searchType].searchTimeHistory {
		val := float64(val.Nanoseconds()) / 1000_000 // convert nano to ms
		ratio := math.Pow(10, float64(3))            // round up to 3 decimal places
		retval[i] = math.Round(val*ratio) / ratio
	}
	return retval
}

func (qs *QuerySummary) getPercentileTime(percent float64, searchType SearchTypeEnum) float64 {
	qs.updateLock.Lock()
	defer qs.updateLock.Unlock()
	return getPercentileTimeFromArr(percent, qs.allQuerySummaries[searchType].searchTimeHistory)
}

func getPercentileTimeFromArr(percent float64, arr []time.Duration) float64 {
	ret := float64(percentile(arr, percent)) / float64(time.Nanosecond)
	return ret / 1000_000
}

func (qs *QuerySummary) getNumFilesSearched(searchType SearchTypeEnum) uint64 {
	qs.updateLock.Lock()
	numFilesSearched := qs.allQuerySummaries[searchType].numFilesSearched
	qs.updateLock.Unlock()
	return numFilesSearched
}

func (qs *QuerySummary) getTotNumFilesSearched() uint64 {
	qs.updateLock.Lock()
	totalNumFiles := qs.totalNumFiles
	qs.updateLock.Unlock()
	return totalNumFiles
}

func (qs *QuerySummary) getNumBuckets() int {
	qs.updateLock.Lock()
	numBuckets := qs.numBuckets
	qs.updateLock.Unlock()
	return numBuckets
}

func (qs *QuerySummary) getNumRecordsMatched(searchType SearchTypeEnum) uint64 {
	qs.updateLock.Lock()
	numMatched := qs.allQuerySummaries[searchType].numMatched
	qs.updateLock.Unlock()
	return numMatched
}

func (qs *QuerySummary) getTotNumRecordsMatched() uint64 {
	qs.updateLock.Lock()
	numRecordsMatched := qs.numRecordsMatched
	qs.updateLock.Unlock()
	return numRecordsMatched
}

func (qs *QuerySummary) getTotNumRecordsSearched() uint64 {
	qs.updateLock.Lock()
	numRecordsSearched := qs.numRecordsSearched
	qs.updateLock.Unlock()
	return numRecordsSearched
}

func (qs *QuerySummary) getNumMetricsSegmentsSearched() uint64 {
	qs.updateLock.Lock()
	numMetricsSegmentsSearched := qs.numMetricsSegmentsSearched
	qs.updateLock.Unlock()
	return numMetricsSegmentsSearched
}

func (qs *QuerySummary) getNumTSIDsMatched() uint64 {
	qs.updateLock.Lock()
	numTSIDsMatched := qs.numTSIDsMatched
	qs.updateLock.Unlock()
	return numTSIDsMatched
}

func (qs *QuerySummary) getNumTagsTreesSearched() uint64 {
	qs.updateLock.Lock()
	numTagsTreesSearched := qs.numTagsTreesSearched
	qs.updateLock.Unlock()
	return numTagsTreesSearched
}

func (qs *QuerySummary) getNumTSOFilesLoaded() uint64 {
	qs.updateLock.Lock()
	numTSOFilesLoaded := qs.numTSOFilesLoaded
	qs.updateLock.Unlock()
	return numTSOFilesLoaded
}

func (qs *QuerySummary) getNumTSGFilesLoaded() uint64 {
	qs.updateLock.Lock()
	numTSGFilesLoaded := qs.numTSGFilesLoaded
	qs.updateLock.Unlock()
	return numTSGFilesLoaded
}

func (qs *QuerySummary) getNumSeriesSearched() uint64 {
	qs.updateLock.Lock()
	numSeriesSearched := qs.numSeriesSearched
	qs.updateLock.Unlock()
	return numSeriesSearched
}

func (qs *QuerySummary) getNumResultSeries() uint64 {
	qs.updateLock.Lock()
	numResultSeries := qs.numResultSeries
	qs.updateLock.Unlock()
	return numResultSeries
}

func (qs *QuerySummary) getNumRecordsMatchedMinMax(searchType SearchTypeEnum) (uint64, uint64) {
	qs.updateLock.Lock()
	minPerSegment, maxPerSegment := qs.allQuerySummaries[searchType].minPerSegment, qs.allQuerySummaries[searchType].maxPerSegment
	qs.updateLock.Unlock()
	return minPerSegment, maxPerSegment
}

func (qs *QuerySummary) LogSummaryAndEmitMetrics(qid uint64, pqid string, containsKibana bool, orgid uint64) {

	sort.Slice(qs.metadataSummary.allTimes, func(i, j int) bool {
		return qs.metadataSummary.allTimes[i] < qs.metadataSummary.allTimes[j]
	})

	log.Warnf("qid=%d, pqid %v, QuerySummary: Finished in  %+vms time. Total number of records searched %+v. Total number of records matched=%+v. Total number of files searched=%+v. Total number of buckets created=%+v",
		qs.qid, pqid, qs.getQueryTotalTime().Milliseconds(), qs.getTotNumRecordsSearched(), qs.getTotNumRecordsMatched(), qs.getTotNumFilesSearched(), qs.getNumBuckets())

	avgCmiTime := float64(qs.metadataSummary.totalTime.Milliseconds()) / float64(len(qs.metadataSummary.allTimes))

	log.Warnf("qid=%d, pqid %v, QuerySummary: CMI layer checked %+v total blocks, and %+v blocks passed. Total time: %+vms. min (%+vms) max (%+vms) avg (%vms) p95 (%+vms)", qs.qid, pqid,
		qs.metadataSummary.numCMIBlocksChecked,
		qs.metadataSummary.numCMIBlocksPassed,
		float64(qs.metadataSummary.totalTime)/float64(time.Millisecond),
		getMinSearchTimeFromArr(qs.metadataSummary.allTimes),
		getMaxSearchTimeFromArr(qs.metadataSummary.allTimes),
		avgCmiTime,
		getPercentileTimeFromArr(95, qs.metadataSummary.allTimes))

	log.Warnf("qid=%d, pqid %v, QuerySummary: RawSearch: Took %+vms time, after searching %+v files. RRCs were generated in %+vms", qs.qid,
		pqid, qs.getTotalTime(RAW).Milliseconds(), qs.getNumFilesSearched(RAW), qs.rawSearchTime.Milliseconds())

	if !containsKibana {
		instrumentation.SetQueryLatencyMs(int64(qs.getQueryTotalTime()/1_000_000), "pqid", pqid)
		instrumentation.SetEventsSearchedGauge(int64(qs.getTotNumRecordsSearched()))
		instrumentation.SetEventsMatchedGauge(int64(qs.getTotNumRecordsMatched()))
	}

	if len(qs.allQuerySummaries[RAW].searchTimeHistory) <= 25 {
		log.Warnf("qid=%d, pqid %v, QuerySummary: RawSearch: Search Time History across all files %vms",
			qs.qid, pqid, qs.getSearchTimeHistory(RAW))
	}

	if qs.getTotNumFilesSearched() > 0 {
		avgTime := uint64((qs.getTotalTime(RAW)).Milliseconds() / int64(qs.getTotNumFilesSearched()))
		log.Warnf("qid=%d, pqid %v, QuerySummary: RawSearch: File raw search time: min (%+vms) max (%+vms) avg (%vms) p95 (%+vms)",
			qs.qid, pqid, qs.getMinSearchTime(RAW), qs.getMaxSearchTime(RAW), avgTime,
			qs.getPercentileTime(95, RAW))

		if !containsKibana {
			instrumentation.SetSegmentLatencyMinMs(int64(qs.getMinSearchTime(RAW)), "pqid", pqid)
			instrumentation.SetSegmentLatencyMaxMs(int64(qs.getMaxSearchTime(RAW)), "pqid", pqid)
			instrumentation.SetSegmentLatencyAvgMs(int64(int64(avgTime)), "pqid", pqid)
			instrumentation.SetSegmentLatencyP95Ms(int64(qs.getPercentileTime(95, RAW)), "pqid", pqid)
		}

	}
	min, max := qs.getNumRecordsMatchedMinMax(RAW)
	log.Warnf("qid=%d, pqid %v, QuerySummary: RawSearch: Number of records matched %d, min/segment (%v) max/segment (%v)", qs.qid, pqid, qs.getNumRecordsMatched(RAW), min, max)

	if config.IsPQSEnabled() {
		log.Warnf("qid=%d, pqid %v, QuerySummary: PQS: Finished in %+vms time, after searching %+v files",
			qs.qid, pqid, qs.getTotalTime(PQS).Milliseconds(), qs.getNumFilesSearched(PQS))

		if len(qs.allQuerySummaries[PQS].searchTimeHistory) <= 25 {
			log.Warnf("qid=%d, pqid %v, QuerySummary: PQS: Search Time History across all files %vms",
				qs.qid, pqid, qs.getSearchTimeHistory(PQS))
		}
		if qs.getTotNumFilesSearched() > 0 {
			avgTime := float64((qs.getTotalTime(PQS)).Milliseconds() / int64(qs.getTotNumFilesSearched()))
			log.Warnf("qid=%d, pqid %v, QuerySummary: PQS: File raw search time: min (%+v) max (%+v) avg (%vms) p95 (%+v)",
				qs.qid, pqid, qs.getMinSearchTime(PQS),
				qs.getMaxSearchTime(PQS), avgTime, qs.getPercentileTime(95, PQS))
		}
		min, max = qs.getNumRecordsMatchedMinMax(PQS)
		log.Warnf("qid=%d, pqid %v, QuerySummary: PQS: Number of records matched %d, min/segment (%v) max/segment (%v) ",
			qs.qid, pqid, qs.getNumRecordsMatched(PQS), min, max)
	}

	if len(qs.allQuerySummaries[STREE].searchTimeHistory) > 0 {

		if len(qs.allQuerySummaries[STREE].searchTimeHistory) <= 25 {
			log.Warnf("qid=%d, pqid %v, QuerySummary: STREE: Search Time History for files %vms",
				qs.qid, pqid, qs.getSearchTimeHistory(STREE))
		}
		avgNs := qs.getTotalTime(STREE).Nanoseconds() / int64(qs.getTotNumFilesSearched())
		avgTime := float64(avgNs) / 1000_000 // nano to millis
		log.Warnf("qid=%d, pqid %v, QuerySummary: STREE: File search times: min (%+vms) max (%+vms) avg (%vms) p95 (%+vms), numFiles: %v, numRecsMatched: %v",
			qs.qid, pqid, qs.getMinSearchTime(STREE), qs.getMaxSearchTime(STREE), avgTime,
			qs.getPercentileTime(95, STREE), qs.getNumFilesSearched(STREE),
			qs.getNumRecordsMatched(STREE))
	}

	if qs.totalDistributedQueries > 0 {
		log.Warnf("qid=%d, pqid %v, QuerySummary: Distributed: Sent %d requests numRemoteMatched: %v, numRecsSearced: %v",
			qs.qid, pqid, qs.totalDistributedQueries, qs.remoteRecordsMatched, qs.remoteRecordsSearched)
		log.Warnf("qid=%d, pqid %v, QuerySummary: Unrotated.GRPC Search times: min (%+vms) max (%+vms) p95 (%+vms), numGRPCs: %v",
			qs.qid, pqid, qs.getMinSearchTime(GRPC_UNROTATED), qs.getMaxSearchTime(GRPC_UNROTATED),
			qs.getPercentileTime(95, GRPC_UNROTATED), qs.getNumFilesSearched(GRPC_UNROTATED))
		log.Warnf("qid=%d, pqid %v, QuerySummary: Rotated.GRPC Search times: min (%+vms) max (%+vms) p95 (%+vms), numGRPCs: %v",
			qs.qid, pqid, qs.getMinSearchTime(GRPC_ROTATED), qs.getMaxSearchTime(GRPC_ROTATED),
			qs.getPercentileTime(95, GRPC_ROTATED), qs.getNumFilesSearched(GRPC_ROTATED))
	}

	uStats.UpdateQueryStats(1, float64(qs.getQueryTotalTime().Milliseconds()), orgid)
	qs.stopTicker()
}

func (qs *QuerySummary) LogMetricsQuerySummary(orgid uint64) {
	log.Warnf("qid=%d, MetricsQuerySummary: Finished in %+vms time. Searched a total of %+v TSIDs. Total number of series searched=%+v. Returned number of series=%+v",
		qs.qid, time.Since(qs.startTime).Milliseconds(), qs.getNumTSIDsMatched(), qs.getNumSeriesSearched(), qs.getNumResultSeries())
	log.Warnf("qid=%d, MetricsQuerySummary: Time taken to get rotated search requests=%+vms. Time taken to get unrotated search requests=%+vms. Total number of metrics segments searched=%+v.",
		qs.qid, qs.metricsQuerySummary.timeGettingRotatedSearchRequests.Microseconds(),
		qs.metricsQuerySummary.timeGettingUnrotatedSearchRequests.Microseconds(), qs.getNumMetricsSegmentsSearched())

	avgTimeSearchingTagsTrees := getSumSearchTimeFromArr(qs.metricsQuerySummary.timeSearchingTagsTrees) / float64(qs.numTagsTreesSearched)
	avgTimeLoadingTSOFiles := getSumSearchTimeFromArr(qs.metricsQuerySummary.timeLoadingTSOFiles) / float64(qs.numTSOFilesLoaded)
	avgTimeLoadingTSGFiles := getSumSearchTimeFromArr(qs.metricsQuerySummary.timeLoadingTSGFiles) / float64(qs.numTSGFilesLoaded)

	log.Warnf("qid=%d, MetricsQuerySummary: Across %d TagsTree Files: min (%.3fms) max (%.3fms) avg (%.3fms) p95(%.3fms)", qs.qid, qs.getNumTagsTreesSearched(), getMinSearchTimeFromArr(qs.metricsQuerySummary.timeSearchingTagsTrees), getMaxSearchTimeFromArr(qs.metricsQuerySummary.timeSearchingTagsTrees), avgTimeSearchingTagsTrees, getPercentileTimeFromArr(95, qs.metricsQuerySummary.timeSearchingTagsTrees))
	log.Warnf("qid=%d, MetricsQuerySummary: Across %d TSO Files: min (%.3fms) max (%.3fms) avg (%.3fms) p95(%.3fms)", qs.qid, qs.getNumTSOFilesLoaded(), getMinSearchTimeFromArr(qs.metricsQuerySummary.timeLoadingTSOFiles), getMaxSearchTimeFromArr(qs.metricsQuerySummary.timeLoadingTSOFiles), avgTimeLoadingTSOFiles, getPercentileTimeFromArr(95, qs.metricsQuerySummary.timeLoadingTSOFiles))
	log.Warnf("qid=%d, MetricsQuerySummary: Across %d TSG Files: min (%.3fms) max (%.3fms) avg (%.3fms) p95(%.3fms)", qs.qid, qs.getNumTSGFilesLoaded(), getMinSearchTimeFromArr(qs.metricsQuerySummary.timeLoadingTSGFiles), getMaxSearchTimeFromArr(qs.metricsQuerySummary.timeLoadingTSGFiles), avgTimeLoadingTSGFiles, getPercentileTimeFromArr(95, qs.metricsQuerySummary.timeLoadingTSGFiles))

	uStats.UpdateQueryStats(1, float64(qs.getQueryTotalTime().Milliseconds()), orgid)
	qs.stopTicker()
}

func (qs *QuerySummary) UpdateRemainingDistributedQueries(remainingDistributedQueries uint64) {
	qs.updateLock.Lock()
	qs.remainingDistributedQueries = remainingDistributedQueries
	qs.updateLock.Unlock()
}

func percentile(input []time.Duration, percent float64) (percentile time.Duration) {
	length := len(input)
	if length == 0 {
		return time.Duration(0)
	}
	if length == 1 {
		return input[0]
	}
	if percent <= 0 || percent > 100 {
		return time.Duration(0)
	}
	// Multiply percent by length of input
	index := (percent / 100) * float64(len(input))
	// Check if the index is a whole number
	if index == float64(int64(index)) {
		// Convert float to int
		i := int(index)
		// Find the value at the index
		percentile = input[i-1]
	} else if index > 1 {
		// Convert float to int via truncation
		i := int(index)
		// Find the average of the index and following values
		percentile = (input[i-1] + input[i]) / 2
	} else {
		return time.Duration(0)
	}
	return percentile
}
