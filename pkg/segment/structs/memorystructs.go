// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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
	TotalAllocatableBytes   uint64 // total bytes that can be allocated. This should not include CmiRuntimeAllocatedBytes
	RotatedCMIBytesInMemory uint64
	SegSearchRequestedBytes uint64
	SegWriterUsageBytes     uint64
	SegStoreSummary         *AllSegStoreSummary
	MetricsSegmentMaxSize   uint64
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

func (sum *AllSegStoreSummary) GetUsedMemoryBytes() uint64 {
	if sum == nil {
		return 0
	}

	return 1e6 * (sum.InMemoryBlockMicroIndexSizeMB +
		sum.InMemorySsmSizeMB +
		sum.InMemoryMetricsBSumSizeMB)
}
