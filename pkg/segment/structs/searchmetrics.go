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
