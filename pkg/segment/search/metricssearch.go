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

package search

import (
	"fmt"
	"sync"
	"time"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/memory/limit"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/reader/metrics/series"
	"github.com/siglens/siglens/pkg/segment/results/mresults"
	tsidtracker "github.com/siglens/siglens/pkg/segment/results/mresults/tsid"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils/semaphore"
	log "github.com/sirupsen/logrus"
)

var metricSearch *semaphore.WeightedSemaphore

func init() {
	metricSearch = semaphore.NewWeightedSemaphore(5, "metricsearch.limiter", time.Minute)
}

func RawSearchMetricsSegment(mQuery *structs.MetricsQuery, tsidInfo *tsidtracker.AllMatchedTSIDs, req *structs.MetricsSearchRequest, res *mresults.MetricsResult,
	timeRange *dtu.MetricsTimeRange, qid uint64, querySummary *summary.QuerySummary) {

	if req == nil {
		log.Errorf("qid=%d, RawSearchMetricsSegment: received a nil search request", qid)
		res.AddError(fmt.Errorf("received a nil search request"))
		return
	} else if req.Parallelism <= 0 {
		log.Errorf("qid=%d, RawSearchMetricsSegment: invalid fileParallelism of %d - must be > 0", qid, req.Parallelism)
		res.AddError(fmt.Errorf("invalid fileParallelism - must be > 0"))
		return
	}

	err := metricSearch.TryAcquireWithBackoff(1, 5, fmt.Sprintf("qid.%d", qid))
	if err != nil {
		log.Errorf("qid=%d RawSearchMetricsSegment: Failed to Acquire resources for raw search! error %+v", qid, err)
		res.AddError(err)
		return
	}
	defer metricSearch.Release(1)
	searchMemory := uint64(utils.MAX_RAW_DATAPOINTS_IN_RESULT*12 + 80)
	err = limit.RequestSearchMemory(searchMemory)
	if err != nil {
		log.Errorf("qid=%d, RawSearchMetricsSegment: Failed to acquire memory from global pool for search! Error: %v", qid, err)
		res.AddError(err)
		return
	}

	sharedBlockIterators, err := series.InitSharedTimeSeriesSegmentReader(req.MetricsKeyBaseDir, int(req.Parallelism))
	if err != nil {
		log.Errorf("qid=%d, RawSearchMetricsSegment: Error initialising a time series reader. Error: %v", qid, err)
		res.AddError(err)
		return
	}
	defer sharedBlockIterators.Close()

	blockNumChan := make(chan int, len(req.BlocksToSearch))
	for blkNum := range req.BlocksToSearch {
		blockNumChan <- int(blkNum)
	}
	close(blockNumChan)
	var wg sync.WaitGroup
	for i := 0; i < int(req.Parallelism); i++ {
		wg.Add(1)
		go blockWorker(i, sharedBlockIterators.TimeSeriesBlockReader[i], blockNumChan, tsidInfo, mQuery, timeRange, res, qid, &wg, querySummary)
	}
	wg.Wait()
}

func blockWorker(workerID int, sharedReader *series.TimeSeriesSegmentReader, blockNumChan <-chan int, tsidInfo *tsidtracker.AllMatchedTSIDs,
	mQuery *structs.MetricsQuery, timeRange *dtu.MetricsTimeRange, res *mresults.MetricsResult, qid uint64, wg *sync.WaitGroup, querySummary *summary.QuerySummary) {
	defer wg.Done()
	queryMetrics := &structs.MetricsQueryProcessingMetrics{
		UpdateLock: &sync.Mutex{},
	}
	localRes := mresults.InitMetricResults(mQuery, qid)
	for blockNum := range blockNumChan {
		tsbr, err := sharedReader.InitReaderForBlock(uint16(blockNum), queryMetrics)
		if err != nil {
			log.Errorf("qid=%d, RawSearchMetricsSegment.blockWorker: Error initialising a block reader. Error: %v", qid, err)
			res.AddError(err)
			continue
		}

		querySummary.UpdateTimeLoadingTSOFiles(queryMetrics.TimeLoadingTSOFiles)
		querySummary.UpdateTimeLoadingTSGFiles(queryMetrics.TimeLoadingTSGFiles)
		for tsid, tsGroupId := range tsidInfo.GetAllTSIDs() {
			tsitr, found, err := tsbr.GetTimeSeriesIterator(tsid)
			queryMetrics.IncrementNumSeriesSearched(1)
			if err != nil {
				log.Errorf("qid=%d, RawSearchMetricsSegment.blockWorker: Error getting the time series iterator. Error: %v", qid, err)
				res.AddError(err)
				continue
			}
			if !found {
				continue
			}
			series := mresults.InitSeriesHolder(mQuery, tsGroupId)
			for tsitr.Next() {
				ts, dp := tsitr.At()
				if !timeRange.CheckInRange(ts) {
					continue
				}
				series.AddEntry(ts, dp)
			}
			err = tsitr.Err()
			if err != nil {
				log.Errorf("RawSearchMetricsSegment.blockWorker: iterator failed %v for worker id %v", err, workerID)
				res.AddError(err)
			}
			if series.GetIdx() > 0 {
				localRes.AddSeries(series, tsid, tsGroupId)
			}
		}
	}
	err := res.Merge(localRes)
	if err != nil {
		res.AddError(err)
		log.Errorf("Failed to merge local results to global results!")
	}
	queryMetrics.IncrementNumMetricsSegmentsSearched(1)
	querySummary.UpdateMetricsSummary(queryMetrics)
}
