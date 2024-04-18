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

import "sync/atomic"

type AllSegStoreSummary struct {
	TotalSegmentCount                  uint64
	TotalMetricsSegmentCount           uint64
	TotalTableCount                    uint64
	InMemoryCMICount                   uint64
	InMemorySearchMetadataCount        uint64
	InMemoryBlockMicroIndexSizeMB      uint64
	InMemorySsmSizeMB                  uint64
	InMemoryMetricsSearchMetadataCount uint64
	InMemoryMetricsBSumSizeMB          uint64
}

type MemoryTracker struct {
	TotalAllocatableBytes     uint64 // total bytes that can be allocated. This should not include CmiRuntimeAllocatedBytes
	CmiInMemoryAllocatedBytes uint64
	CmiRuntimeAllocatedBytes  uint64
	SegSearchRequestedBytes   uint64
	SegWriterUsageBytes       uint64
	SegStoreSummary           *AllSegStoreSummary
	SsmInMemoryAllocatedBytes uint64
	MetricsSegmentMaxSize     uint64
}

func (sum *AllSegStoreSummary) IncrementTotalSegmentCount() {
	atomic.AddUint64(&sum.TotalSegmentCount, 1)
}

func (sum *AllSegStoreSummary) SetInMemoryBlockMicroIndexCount(count uint64) {
	atomic.StoreUint64(&sum.InMemoryCMICount, count)
}

func (sum *AllSegStoreSummary) SetInMemorySearchmetadataCount(count uint64) {
	atomic.StoreUint64(&sum.InMemorySearchMetadataCount, count)
}

func (sum *AllSegStoreSummary) SetInMemoryBlockMicroIndexSizeMB(size uint64) {
	atomic.StoreUint64(&sum.InMemoryBlockMicroIndexSizeMB, size)
}

func (sum *AllSegStoreSummary) IncrementTotalTableCount() {
	atomic.AddUint64(&sum.TotalTableCount, 1)
}

func (sum *AllSegStoreSummary) DecrementTotalTableCount() {
	atomic.AddUint64(&sum.TotalTableCount, ^uint64(0))
}

func (sum *AllSegStoreSummary) DecrementTotalSegKeyCount() {
	atomic.AddUint64(&sum.TotalSegmentCount, ^uint64(0))
}

func (sum *AllSegStoreSummary) SetInMemorySsmSizeMB(val uint64) {
	atomic.StoreUint64(&sum.InMemorySsmSizeMB, val)
}

func (sum *AllSegStoreSummary) IncrementTotalMetricsSegmentCount() {
	atomic.StoreUint64(&sum.TotalMetricsSegmentCount, 1)
}

func (sum *AllSegStoreSummary) SetInMemoryMetricsSearchmetadataCount(val uint64) {
	atomic.StoreUint64(&sum.InMemoryMetricsSearchMetadataCount, val)
}

func (sum *AllSegStoreSummary) SetInMemoryMetricsSsmSizeMB(val uint64) {
	atomic.StoreUint64(&sum.InMemoryMetricsBSumSizeMB, val)
}

func (sum *AllSegStoreSummary) DecrementTotalMetricsSegmentCount() {
	if sum.TotalMetricsSegmentCount == 0 {
		return
	}
	atomic.AddUint64(&sum.TotalMetricsSegmentCount, ^uint64(0))
}
