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

package limit

import (
	"errors"
	"sync/atomic"
	"time"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/memory"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	log "github.com/sirupsen/logrus"
)

const MINUTES_UPDATE_METADATA_MEM_ALLOC = 1

var LOG_GLOBAL_MEM_FREQUENCY = 5

func InitMemoryLimiter() {
	totalAvailableSizeBytes := config.GetTotalMemoryAvailable()
	log.Infof("InitQueryNode: Total available memory %+v MB", utils.ConvertUintBytesToMB(totalAvailableSizeBytes))

	maxBlockMetaInMemory := uint64(0)
	maxSearchAvailableSize := uint64(0)
	maxBlockMicroRuntime := uint64(0)
	maxSsmInMemory := uint64(0)
	metricsInMemory := uint64(0)

	maxSearchAvailableSize = uint64(float64(totalAvailableSizeBytes) * utils.RAW_SEARCH_MEM_PERCENT / 100)
	maxBlockMicroRuntime = uint64(float64(totalAvailableSizeBytes) * utils.MICRO_IDX_CHECK_MEM_PERCENT / 100)
	maxBlockMetaInMemory = uint64(float64(totalAvailableSizeBytes) * utils.MICRO_IDX_MEM_PERCENT / 100)
	maxSsmInMemory = uint64(float64(totalAvailableSizeBytes) * utils.SSM_MEM_PERCENT / 100)
	metricsInMemory = uint64(float64(totalAvailableSizeBytes) * utils.METRICS_MEMORY_MEM_PERCENT / 100)

	if config.IsDebugMode() {
		LOG_GLOBAL_MEM_FREQUENCY = 1
	}

	// Total available memory should not include block runtime so rebalancing is still accurate
	totalAvailableSizeBytes = totalAvailableSizeBytes - maxBlockMicroRuntime
	memory.GlobalMemoryTracker = &structs.MemoryTracker{
		TotalAllocatableBytes: totalAvailableSizeBytes,

		CmiInMemoryAllocatedBytes: maxBlockMetaInMemory,
		CmiRuntimeAllocatedBytes:  maxBlockMicroRuntime,

		SegSearchRequestedBytes: maxSearchAvailableSize,

		SegWriterUsageBytes:       0,
		SegStoreSummary:           metadata.GlobalSegStoreSummary,
		SsmInMemoryAllocatedBytes: maxSsmInMemory,

		MetricsSegmentMaxSize: metricsInMemory,
	}

	metadata.InitBlockMetaCheckLimiter(int64(maxBlockMicroRuntime))
	go rebalanceMemoryAllocationLoop()
}

func printMemoryManagerSummary() {
	numLoadedUnrotated, totalUnrotated := writer.GetUnrotatedMetadataInfo()
	unrotaedSize := writer.GetSizeOfUnrotatedMetadata()
	log.Infof("GlobalMemoryTracker: Total amount of memory available is %+v MB", utils.ConvertUintBytesToMB(memory.GlobalMemoryTracker.TotalAllocatableBytes))
	log.Infof("GlobalMemoryTracker: AllSegReadStores has %v total segment files across %v tables. Microindices have been allocated %+v MB",
		memory.GlobalMemoryTracker.SegStoreSummary.TotalSegmentCount, memory.GlobalMemoryTracker.SegStoreSummary.TotalTableCount, utils.ConvertUintBytesToMB(memory.GlobalMemoryTracker.CmiInMemoryAllocatedBytes))

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
	log.Infof("GlobalMemoryTracker: SegWriter has been allocated %v MB. MetricsWriter has been allocated %v MB.",
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
	if memoryAvailable < memory.GlobalMemoryTracker.CmiRuntimeAllocatedBytes {
		memoryAvailable = 0
	} else {
		memoryAvailable = memoryAvailable - memory.GlobalMemoryTracker.CmiRuntimeAllocatedBytes
	}

	totalSsmMemory := uint64(float64(memoryAvailable) * utils.SSM_MEM_PERCENT / 100)
	metadata.RebalanceInMemorySsm(totalSsmMemory)

	if memory.GlobalMemoryTracker.SegSearchRequestedBytes > memoryAvailable {
		memoryAvailable = 0
		memory.GlobalMemoryTracker.SegSearchRequestedBytes = memoryAvailable
	} else {
		memoryAvailable = memoryAvailable - memory.GlobalMemoryTracker.SegSearchRequestedBytes
	}

	totalMetadataMemory := uint64(float64(memoryAvailable) * utils.MICRO_IDX_MEM_PERCENT / 100)
	unrotatedMetadataMemory := writer.GetSizeOfUnrotatedMetadata()
	if unrotatedMetadataMemory >= totalMetadataMemory {
		unrotatedMetadataMemory = writer.RebalanceUnrotatedMetadata(totalMetadataMemory)
	}

	var blockMetadataMemory uint64
	if unrotatedMetadataMemory > totalMetadataMemory {
		blockMetadataMemory = 0
	} else {
		blockMetadataMemory = totalMetadataMemory - unrotatedMetadataMemory
	}

	metadata.RebalanceInMemoryCmi(blockMetadataMemory)
	memory.GlobalMemoryTracker.CmiInMemoryAllocatedBytes = blockMetadataMemory
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
