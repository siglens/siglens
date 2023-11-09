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

package structs

import (
	"sync"
	"sync/atomic"
	"time"
)

type QueryProcessingMetrics struct {
	NumRecordsMatched     uint64
	NumBlocksWithMatch    uint64
	NumBlocksToRawSearch  uint64
	NumBlocksInSegment    uint64
	NumRecordsToRawSearch uint64
	NumRecordsUnmatched   uint64
}

type MetricsQueryProcessingMetrics struct {
	UpdateLock                 *sync.Mutex
	NumMetricsSegmentsSearched uint64
	NumTSOFilesLoaded          uint64
	NumTSGFilesLoaded          uint64
	NumSeriesSearched          uint64
	TimeLoadingTSOFiles        time.Duration
	TimeLoadingTSGFiles        time.Duration
}

func (qm *QueryProcessingMetrics) SetNumRecordsMatched(records uint64) {
	atomic.AddUint64(&qm.NumRecordsMatched, records)
}

func (qm *QueryProcessingMetrics) SetNumRecordsUnmatched(records uint64) {
	atomic.AddUint64(&qm.NumRecordsUnmatched, records)
}

func (qm *QueryProcessingMetrics) IncrementNumBlocksWithMatch(nBlocks uint64) {
	atomic.AddUint64(&qm.NumBlocksWithMatch, nBlocks)
}

func (qm *QueryProcessingMetrics) IncrementNumRecordsWithMatch(nBlocks uint64) {
	atomic.AddUint64(&qm.NumRecordsMatched, nBlocks)
}

func (qm *QueryProcessingMetrics) IncrementNumRecordsNoMatch(nBlocks uint64) {
	atomic.AddUint64(&qm.NumRecordsUnmatched, nBlocks)
}

func (qm *QueryProcessingMetrics) IncrementNumBlocksToRawSearch(records uint64) {
	atomic.AddUint64(&qm.NumBlocksToRawSearch, records)
}

func (qm *QueryProcessingMetrics) SetNumBlocksToRawSearch(records uint64) {
	atomic.StoreUint64(&qm.NumBlocksToRawSearch, records)
}

func (qm *QueryProcessingMetrics) SetNumBlocksInSegFile(records uint64) {
	atomic.StoreUint64(&qm.NumBlocksInSegment, records)
}

func (qm *QueryProcessingMetrics) SetNumRecordsToRawSearch(records uint64) {
	atomic.StoreUint64(&qm.NumRecordsToRawSearch, records)
}

func (qm *MetricsQueryProcessingMetrics) IncrementNumMetricsSegmentsSearched(records uint64) {
	atomic.StoreUint64(&qm.NumMetricsSegmentsSearched, records)
}

func (qm *MetricsQueryProcessingMetrics) IncrementNumTSOFilesLoaded(records uint64) {
	atomic.AddUint64(&qm.NumTSOFilesLoaded, records)
}

func (qm *MetricsQueryProcessingMetrics) IncrementNumTSGFilesLoaded(records uint64) {
	atomic.AddUint64(&qm.NumTSGFilesLoaded, records)
}

func (qm *MetricsQueryProcessingMetrics) IncrementNumSeriesSearched(records uint64) {
	atomic.AddUint64(&qm.NumSeriesSearched, records)
}

func (qm *MetricsQueryProcessingMetrics) SetTimeLoadingTSOFiles(ttime time.Duration) {
	qm.UpdateLock.Lock()
	defer qm.UpdateLock.Unlock()
	qm.TimeLoadingTSOFiles = ttime
}

func (qm *MetricsQueryProcessingMetrics) SetTimeLoadingTSGFiles(ttime time.Duration) {
	qm.UpdateLock.Lock()
	defer qm.UpdateLock.Unlock()
	qm.TimeLoadingTSGFiles = ttime
}
