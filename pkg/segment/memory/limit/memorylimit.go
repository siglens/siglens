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

package limit

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/memory"
	segmetadata "github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	log "github.com/sirupsen/logrus"
)

const MINUTES_UPDATE_METADATA_MEM_ALLOC = 1

var SegSearchAllocatedBytes uint64 // Should not be changed after initialization

var LOG_GLOBAL_MEM_FREQUENCY = 5

func InitMemoryLimiter() {
	totalAvailableSizeBytes := config.GetTotalMemoryAvailableToUse()
	log.Infof("InitMemoryLimiter: Total available memory %+v MB", utils.ConvertUintBytesToMB(totalAvailableSizeBytes))

	memLimits := config.GetMemoryConfig()

	SegSearchAllocatedBytes = uint64(float64(totalAvailableSizeBytes*memLimits.SearchPercent) / 100)
	rotatedCMIBytes := uint64(float64(totalAvailableSizeBytes*memLimits.CMIPercent) / 100)
	metricsInMemory := uint64(float64(totalAvailableSizeBytes*memLimits.MetricsPercent) / 100)

	if config.IsDebugMode() {
		LOG_GLOBAL_MEM_FREQUENCY = 1
	}

	memory.GlobalMemoryTracker = &structs.MemoryTracker{
		TotalAllocatableBytes:   totalAvailableSizeBytes,
		RotatedCMIBytesInMemory: rotatedCMIBytes,
		SegSearchRequestedBytes: SegSearchAllocatedBytes,
		MetricsSegmentMaxSize:   metricsInMemory,

		SegWriterUsageBytes: 0,
		SegStoreSummary:     segmetadata.GlobalSegStoreSummary,
	}

	segmetadata.InitBlockMetaCheckLimiter(int64(rotatedCMIBytes))
	go rebalanceMemoryAllocationLoop()
}

func printMemoryManagerSummary() {
	numLoadedUnrotated, totalUnrotated := writer.GetUnrotatedMetadataInfo()
	unrotaedSize := writer.GetSizeOfUnrotatedMetadata()
	log.Infof("GlobalMemoryTracker: Total allocatable Memory: %+v MB", utils.ConvertUintBytesToMB(memory.GlobalMemoryTracker.TotalAllocatableBytes))
	log.Infof("GlobalMemoryTracker: segCount: %v, indexCount: %v, CmiInMemoryAllocated: %+v MB",
		memory.GlobalMemoryTracker.SegStoreSummary.TotalSegmentCount,
		memory.GlobalMemoryTracker.SegStoreSummary.TotalTableCount,
		utils.ConvertUintBytesToMB(memory.GlobalMemoryTracker.RotatedCMIBytesInMemory))

	log.Infof("GlobalMemoryTracker: AllSegReadStores has %v CMI entries in memory. This accounts for %v MB",
		memory.GlobalMemoryTracker.SegStoreSummary.InMemoryCMICount,
		memory.GlobalMemoryTracker.SegStoreSummary.InMemoryBlockMicroIndexSizeMB)

	log.Infof("GlobalMemoryTracker: AllSegReadStores %v SSM entries in memory. This accounts for %v MB",
		memory.GlobalMemoryTracker.SegStoreSummary.InMemorySearchMetadataCount,
		memory.GlobalMemoryTracker.SegStoreSummary.InMemorySsmSizeMB)

	log.Infof("GlobalMemoryTracker: MetricsMetadata has %v segments in memory. Out of which %v segment have SSMs loaded. This accounts for %v MB",
		memory.GlobalMemoryTracker.SegStoreSummary.TotalMetricsSegmentCount,
		memory.GlobalMemoryTracker.SegStoreSummary.InMemoryMetricsSearchMetadataCount,
		memory.GlobalMemoryTracker.SegStoreSummary.InMemoryMetricsBSumSizeMB)

	log.Infof("GlobalMemoryTracker: Unrotated metadata has %v total segKeys. %+v have loaded metadata in memory. This accounts for %v MB",
		totalUnrotated, numLoadedUnrotated, utils.ConvertUintBytesToMB(unrotaedSize))
	log.Infof("GlobalMemoryTracker: SegSearch has been allocated %v MB.", utils.ConvertUintBytesToMB(memory.GlobalMemoryTracker.SegSearchRequestedBytes))
	log.Infof("GlobalMemoryTracker: SegWriterUsageBytes %v MB. MetricsWriter has been allocated %v MB.",
		utils.ConvertUintBytesToMB(memory.GlobalMemoryTracker.SegWriterUsageBytes), utils.ConvertUintBytesToMB(memory.GetAvailableMetricsIngestMemory()))
}

func rebalanceMemoryAllocationLoop() {
	count := 0
	for {
		rebalanceMemoryAllocation()
		if count%LOG_GLOBAL_MEM_FREQUENCY == 0 {
			printMemoryManagerSummary()
		}
		count++
		count = count % LOG_GLOBAL_MEM_FREQUENCY
		time.Sleep(MINUTES_UPDATE_METADATA_MEM_ALLOC * time.Minute)
	}
}

/*
Main function that rebalances all memory limits with the following logic

1. Get memory that we can allocate / move around
  - memoryAvailable = TotalAvailableBytes - size of writer segstores

2. Allocate memory for segsearch. This will be the max memory of any single segment raw search we have seen
3. From available memory, use percentages to get max size of metadata
4. First, use as much metadata size as possible for unrotated data
  - if unrotated data is bigger than max metadata size, then use all metadata memory for unrotated data
  - when we remove unrotated data, we currently have no way to add it back so we will raw search the entire file

5. After allocating for unrotated data, use remaining metadata size for rotated data
  - set global var & rebalance in metadata package
*/
func rebalanceMemoryAllocation() {
	rawWriterSize := writer.GetInMemorySize()
	var memoryAvailable uint64
	if rawWriterSize > memory.GlobalMemoryTracker.TotalAllocatableBytes {
		memoryAvailable = 0
	} else {
		memoryAvailable = memory.GlobalMemoryTracker.TotalAllocatableBytes - rawWriterSize
	}

	memLimits := config.GetMemoryConfig()
	totalSsmMemory := uint64(float64(memoryAvailable*memLimits.MetadataPercent) / 100)
	segmetadata.RebalanceInMemorySsm(totalSsmMemory)

	if memory.GlobalMemoryTracker.SegSearchRequestedBytes < SegSearchAllocatedBytes {
		// reset the allocatedSegSearchBytes as we may have freed up memory
		atomic.StoreUint64(&memory.GlobalMemoryTracker.SegSearchRequestedBytes, SegSearchAllocatedBytes)
	}

	if memory.GlobalMemoryTracker.SegSearchRequestedBytes > memoryAvailable {
		memory.GlobalMemoryTracker.SegSearchRequestedBytes = memoryAvailable
		memoryAvailable = 0
	} else {
		memoryAvailable = memoryAvailable - memory.GlobalMemoryTracker.SegSearchRequestedBytes
	}

	totalCmiMemory := uint64(float64(memoryAvailable*memLimits.CMIPercent) / 100)
	unrotatedCmiMemory := writer.GetSizeOfUnrotatedMetadata()
	if unrotatedCmiMemory >= totalCmiMemory {
		unrotatedCmiMemory = writer.RebalanceUnrotatedMetadata(totalCmiMemory)
	}

	var rotatedCmiMemory uint64
	if unrotatedCmiMemory > totalCmiMemory {
		rotatedCmiMemory = 0
	} else {
		rotatedCmiMemory = totalCmiMemory - unrotatedCmiMemory
	}

	segmetadata.RebalanceInMemoryCmi(rotatedCmiMemory)
	memory.GlobalMemoryTracker.RotatedCMIBytesInMemory = rotatedCmiMemory
	memory.GlobalMemoryTracker.SegWriterUsageBytes = rawWriterSize
}

// Creates space for search by removing cmi if needed. Returns error if no space can be found.
// This function assumes only one segment is run at a time, and will not verify if >1 segment are run in parallel
func RequestSearchMemory(sLimit uint64) error {

	if sLimit <= memory.GlobalMemoryTracker.SegSearchRequestedBytes {
		return nil
	}
	atomic.StoreUint64(&memory.GlobalMemoryTracker.SegSearchRequestedBytes, sLimit)
	rebalanceMemoryAllocation()

	if sLimit <= memory.GlobalMemoryTracker.SegSearchRequestedBytes {
		return nil
	}
	// If try to rebalance and SegSearchAllocatedBytes did not change, then we could not allocate what was requested
	log.Infof("Unable to allocate memory for segsearch! Current breakdown:")
	printMemoryManagerSummary()
	return errors.New("failed to allocate resources for segment search")
}
