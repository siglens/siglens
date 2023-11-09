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
