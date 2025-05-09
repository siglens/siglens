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

package metadata

import (
	"errors"
	"sort"
	"sync"
	"time"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/query/pqs"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils/semaphore"
	log "github.com/sirupsen/logrus"
)

var GlobalSegStoreSummary = &structs.AllSegStoreSummary{}

// Holder struct for all rotated Metadata that exists in the server
type allSegmentMetadata struct {

	// all SegmentMicroIndex in sorted order of descending latest time, used for global memory limiting
	allSegmentMicroIndex []*SegmentMicroIndex

	// reverse index which maps a segment key to the corresponding SegmentMicroIndex for quick access (RRC generation/search/etc.)
	segmentMetadataReverseIndex map[string]*SegmentMicroIndex

	// maps a tableName to the sorted list of SegmentMicroIndex (descending by latest time) used for initial time query filtering
	tableSortedMetadata map[string][]*SegmentMicroIndex

	// metadata update lock
	updateLock *sync.RWMutex
}

var globalMetadata *allSegmentMetadata = &allSegmentMetadata{
	allSegmentMicroIndex:        make([]*SegmentMicroIndex, 0),
	segmentMetadataReverseIndex: make(map[string]*SegmentMicroIndex),
	tableSortedMetadata:         make(map[string][]*SegmentMicroIndex),
	updateLock:                  &sync.RWMutex{},
}

var GlobalBlockMicroIndexCheckLimiter *semaphore.WeightedSemaphore

func InitBlockMetaCheckLimiter(unloadedBlockLimit int64) {
	GlobalBlockMicroIndexCheckLimiter = semaphore.NewDefaultWeightedSemaphore(unloadedBlockLimit, "GlobalBlockMicroIndexCheckLimiter")
}

func ResetGlobalMetadataForTest() {
	globalMetadata = &allSegmentMetadata{
		allSegmentMicroIndex:        make([]*SegmentMicroIndex, 0),
		segmentMetadataReverseIndex: make(map[string]*SegmentMicroIndex),
		tableSortedMetadata:         make(map[string][]*SegmentMicroIndex),
		updateLock:                  &sync.RWMutex{},
	}
}

func GetAllSegmentMicroIndexForTest() []*SegmentMicroIndex {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()

	return globalMetadata.allSegmentMicroIndex
}

func GetSegmentMetadataReverseIndexForTest() map[string]*SegmentMicroIndex {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()

	return globalMetadata.segmentMetadataReverseIndex
}

func GetTableSortedMetadata() map[string][]*SegmentMicroIndex {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()

	return globalMetadata.tableSortedMetadata
}

func ProcessSegmetaInfo(segMetaInfo *structs.SegMeta) *SegmentMicroIndex {
	pqs.AddAllPqidResults(segMetaInfo.SegmentKey, segMetaInfo.AllPQIDs)
	return InitSegmentMicroIndex(segMetaInfo, false)
}

func AddSegMetaToMetadata(segMeta *structs.SegMeta) {
	segMetaInfo := ProcessSegmetaInfo(segMeta)
	BulkAddSegmentMicroIndex([]*SegmentMicroIndex{segMetaInfo})
}

func BulkAddSegmentMicroIndex(allMetadata []*SegmentMicroIndex) {
	globalMetadata.bulkAddSegmentMicroIndex(allMetadata)
}

func DiscardUnownedSegments(ownedSegments map[string]struct{}) {
	globalMetadata.updateLock.RLock()
	segsToDelete := make(map[string]struct{})
	for segKey := range globalMetadata.segmentMetadataReverseIndex {
		if _, ok := ownedSegments[segKey]; !ok {
			segsToDelete[segKey] = struct{}{}
		}
	}
	globalMetadata.updateLock.RUnlock()

	DeleteSegmentKeys(segsToDelete)
}

func GetNumBlocksInSegment(segKey string) uint64 {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()
	if segMeta, ok := globalMetadata.segmentMetadataReverseIndex[segKey]; ok {
		return uint64(segMeta.NumBlocks)
	}
	return 0
}

func GetTotalBlocksInSegments(segKeys []string) uint64 {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()
	totalBlocks := uint64(0)
	for _, segKey := range segKeys {
		if segMeta, ok := globalMetadata.segmentMetadataReverseIndex[segKey]; ok {
			totalBlocks += uint64(segMeta.NumBlocks)
		}
	}
	return totalBlocks
}

func (hm *allSegmentMetadata) bulkAddSegmentMicroIndex(allMetadata []*SegmentMicroIndex) {
	hm.updateLock.Lock()
	defer hm.updateLock.Unlock()

	for _, newSegMeta := range allMetadata {
		if segMeta, ok := hm.segmentMetadataReverseIndex[newSegMeta.SegmentKey]; ok {
			res, err := mergeSegmentMicroIndex(segMeta, newSegMeta)
			if err != nil {
				log.Errorf("BulkAddSegmentInfo: Failed to do union for segKey=%v err=%v", segMeta.SegmentKey, err)
				continue
			}
			hm.segmentMetadataReverseIndex[newSegMeta.SegmentKey] = res
			continue
		}
		hm.allSegmentMicroIndex = append(hm.allSegmentMicroIndex, newSegMeta)
		hm.segmentMetadataReverseIndex[newSegMeta.SegmentKey] = newSegMeta
		GlobalSegStoreSummary.IncrementTotalSegmentCount()

		if _, ok := hm.tableSortedMetadata[newSegMeta.VirtualTableName]; !ok {
			GlobalSegStoreSummary.IncrementTotalTableCount()
			hm.tableSortedMetadata[newSegMeta.VirtualTableName] = make([]*SegmentMicroIndex, 0)
		}
		tableMetadata := hm.tableSortedMetadata[newSegMeta.VirtualTableName]
		tableMetadata = append(tableMetadata, newSegMeta)
		hm.tableSortedMetadata[newSegMeta.VirtualTableName] = tableMetadata
	}
	sort.Slice(hm.allSegmentMicroIndex, func(i, j int) bool {
		return hm.allSegmentMicroIndex[i].LatestEpochMS > hm.allSegmentMicroIndex[j].LatestEpochMS
	})

	for tName, tableSegmentMeta := range hm.tableSortedMetadata {
		sort.Slice(tableSegmentMeta, func(i, j int) bool {
			return tableSegmentMeta[i].LatestEpochMS > tableSegmentMeta[j].LatestEpochMS
		})
		hm.tableSortedMetadata[tName] = tableSegmentMeta
	}
}

func mergeSegmentMicroIndex(left *SegmentMicroIndex, right *SegmentMicroIndex) (*SegmentMicroIndex, error) {

	if left.SegmentKey != right.SegmentKey {
		return left, errors.New("left and right keys were not same")
	}
	if right.EarliestEpochMS != 0 && left.EarliestEpochMS == 0 {
		left.EarliestEpochMS = right.EarliestEpochMS
	}
	if right.LatestEpochMS != 0 && left.LatestEpochMS == 0 {
		left.LatestEpochMS = right.LatestEpochMS
	}
	if right.SegbaseDir != "" && left.SegbaseDir == "" {
		left.SegbaseDir = right.SegbaseDir
	}
	if len(right.ColumnNames) > 0 && len(left.ColumnNames) == 0 {
		left.ColumnNames = right.ColumnNames
	}
	if right.NumBlocks > 0 && left.NumBlocks == 0 {
		left.NumBlocks = right.NumBlocks
	}

	if right.loadedCmiSize != 0 && left.loadedCmiSize == 0 {
		left.loadedCmiSize = right.loadedCmiSize
	}

	if right.SearchMetadataSize != 0 && left.SearchMetadataSize == 0 {
		left.SearchMetadataSize = right.SearchMetadataSize
	}

	if right.RecordCount > 0 && left.RecordCount == 0 {
		left.RecordCount = right.RecordCount
	}

	return left, nil
}

func RebalanceInMemoryCmi(cmiSizeBytes uint64) {
	globalMetadata.rebalanceCmi(cmiSizeBytes)
}

// Entry point to rebalance what is loaded in memory depending on BLOCK_MICRO_MEM_SIZE
func (hm *allSegmentMetadata) rebalanceCmi(cmiSizeBytes uint64) {

	hm.updateLock.RLock()
	cmiIndex, inMemSize := hm.getCmiMaxIndicesToEvict(cmiSizeBytes)
	evicted := hm.evictCmiPastIndices(cmiIndex)

	log.Infof("rebalanceCmi: evcitCmiIndex: %v, totalSMI: %v, allocated: %+v MB, evicted: %v, allowedMB: %v",
		cmiIndex, len(hm.allSegmentMicroIndex), segutils.ConvertUintBytesToMB(inMemSize),
		evicted, segutils.ConvertUintBytesToMB(cmiSizeBytes))

	GlobalSegStoreSummary.SetInMemoryBlockMicroIndexCount(uint64(cmiIndex))
	GlobalSegStoreSummary.SetInMemoryBlockMicroIndexSizeMB(segutils.ConvertUintBytesToMB(inMemSize))
	hm.updateLock.RUnlock()
}

func (hm *allSegmentMetadata) getCmiMaxIndicesToEvict(totalMem uint64) (int, uint64) {

	numCmis := len(hm.allSegmentMicroIndex)
	totalMBAllocated := uint64(0)
	maxCmiIndex := 0

	for ; maxCmiIndex < numCmis; maxCmiIndex++ {
		smi := hm.allSegmentMicroIndex[maxCmiIndex]

		if totalMBAllocated+smi.loadedCmiSize > totalMem {
			break
		}
		totalMBAllocated += smi.loadedCmiSize
	}
	return maxCmiIndex, totalMBAllocated
}

func (hm *allSegmentMetadata) evictCmiPastIndices(cmiIndex int) int {
	var evictedCount int
	for i := cmiIndex; i < len(hm.allSegmentMicroIndex); i++ {
		smi := hm.allSegmentMicroIndex[i]
		if smi.loadedCmiSize > 0 {
			smi.clearMicroIndices()
			evictedCount++
		}
	}

	return evictedCount
}

/*
Parameters:

	idxToLoad: indices in the allSegmentMicroIndex to be loaded

Returns:

	totalSize:
	totalEntities:
*/
func (hm *allSegmentMetadata) loadParallelSsm(idxToLoad []int) (uint64, int) {

	totalSize := uint64(0)
	totalEntities := int(0)

	wg := &sync.WaitGroup{}
	var err error
	parallelism := int(config.GetParallelism())

	for i, idx := range idxToLoad {
		wg.Add(1)
		go func(myIdx int, rbufIdx int) {
			err = hm.allSegmentMicroIndex[myIdx].loadSearchMetadata()
			if err != nil {
				log.Errorf("loadParallelSsm: failed to load SSM at index %d. Error: %v", myIdx, err)
			}
			wg.Done()
		}(idx, i%parallelism)
		if i%parallelism == 0 {
			wg.Wait()
		}
	}
	wg.Wait()

	for _, idx := range idxToLoad {
		if hm.allSegmentMicroIndex[idx].loadedSearchMetadata {
			totalSize += hm.allSegmentMicroIndex[idx].SearchMetadataSize
			totalEntities += 1
		}
	}
	return totalSize, totalEntities
}

func (hm *allSegmentMetadata) deleteSegmentKey(key string) {
	hm.updateLock.Lock()
	defer hm.updateLock.Unlock()
	hm.deleteSegmentKeyWithLock(key)
}

func (hm *allSegmentMetadata) deleteTable(table string, orgid int64) {
	hm.updateLock.Lock()
	defer hm.updateLock.Unlock()

	tableSegments, ok := hm.tableSortedMetadata[table]
	if !ok {
		return
	}

	allSegKeysInTable := make(map[string]bool, len(tableSegments))
	for _, segment := range tableSegments {
		if segment.OrgId == orgid {
			allSegKeysInTable[segment.SegmentKey] = true
		}
	}
	for segKey := range allSegKeysInTable {
		hm.deleteSegmentKeyWithLock(segKey)
	}
	delete(hm.tableSortedMetadata, table)
	GlobalSegStoreSummary.DecrementTotalTableCount()
}

// internal function to delete segment key from all SiglensMetadata structs
// caller is responsible for acquiring locks
func (hm *allSegmentMetadata) deleteSegmentKeyWithLock(key string) {
	var tName string
	for i, sMetadata := range hm.allSegmentMicroIndex {
		if sMetadata.SegmentKey == key {
			hm.allSegmentMicroIndex = append(hm.allSegmentMicroIndex[:i], hm.allSegmentMicroIndex[i+1:]...)
			tName = sMetadata.VirtualTableName
			break
		}
	}
	delete(hm.segmentMetadataReverseIndex, key)
	if tName == "" {
		log.Infof("deleteSegmentKeyWithLock: key %+v not found in inmem allSegmentMicroIndex, and thats ok since Rebalance thread could have removed it", key)
		return
	}
	sortedTableSlice, ok := hm.tableSortedMetadata[tName]
	if !ok {
		return
	}

	for i, sMetadata := range sortedTableSlice {
		if sMetadata.SegmentKey == key {
			sortedTableSlice = append(sortedTableSlice[:i], sortedTableSlice[i+1:]...)
			break
		}
	}
	hm.tableSortedMetadata[tName] = sortedTableSlice
	GlobalSegStoreSummary.DecrementTotalSegKeyCount()

}

func GetMicroIndex(segKey string) (*SegmentMicroIndex, bool) {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()

	mi, ok := globalMetadata.segmentMetadataReverseIndex[segKey]
	return mi, ok

}

func getAllColumnsRecSizeWithLock(segKey string) (map[string]uint32, bool) {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()

	mi, ok := globalMetadata.segmentMetadataReverseIndex[segKey]
	if ok {
		return mi.getAllColumnsRecSize(), true
	}
	return nil, ok

}

func DeleteSegmentKey(segKey string) {
	globalMetadata.deleteSegmentKey(segKey)
}

func DeleteSegmentKeys[T any](segKeys map[string]T) {
	if len(segKeys) == 0 {
		return
	}

	globalMetadata.updateLock.Lock()
	defer globalMetadata.updateLock.Unlock()
	for segKey := range segKeys {
		globalMetadata.deleteSegmentKeyWithLock(segKey)
	}
}

func DeleteVirtualTable(vTable string, orgid int64) {
	globalMetadata.deleteTable(vTable, orgid)
}

func GetAllSegKeysForOrg(orgid int64) map[string]struct{} {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()

	segKeys := make(map[string]struct{})
	for _, smi := range globalMetadata.allSegmentMicroIndex {
		if smi.OrgId == orgid {
			segKeys[smi.SegmentKey] = struct{}{}
		}
	}
	return segKeys
}

func GetAllSegKeys() map[string]struct{} {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()

	segKeys := make(map[string]struct{})
	for _, smi := range globalMetadata.allSegmentMicroIndex {
		segKeys[smi.SegmentKey] = struct{}{}
	}
	return segKeys
}

func RebalanceInMemorySsm(ssmSizeBytes uint64) {
	logsSSM := uint64(float64(ssmSizeBytes) * segutils.METADATA_LOGS_MEM_PERCENT / 100)
	metricsSSM := uint64(float64(ssmSizeBytes) * segutils.METADATA_METRICS_MEM_PERCENT / 100)
	globalMetadata.rebalanceSsm(logsSSM)
	globalMetricsMetadata.rebalanceMetricsSsm(metricsSSM)
}

func (hm *allSegmentMetadata) rebalanceSsm(ssmSizeBytes uint64) {

	sTime := time.Now()

	hm.updateLock.RLock()
	searchIndex := hm.getSsmMaxIndicesToLoad(ssmSizeBytes)
	evicted := hm.evictSsmPastIndices(searchIndex)

	inMemSize, inMemSearchMetaCount, newloaded := hm.loadSsmUntilIndex(searchIndex)

	log.Infof("rebalanceSsm SSM, inMem: %+v SSM, allocated: %+v MB, evicted: %v, newloaded: %v, totalSsmCount: %v, allowedMB: %v, took: %vms",
		inMemSearchMetaCount, segutils.ConvertUintBytesToMB(inMemSize),
		evicted, newloaded, len(hm.allSegmentMicroIndex),
		segutils.ConvertUintBytesToMB(ssmSizeBytes),
		int(time.Since(sTime).Milliseconds()))

	GlobalSegStoreSummary.SetInMemorySearchmetadataCount(uint64(inMemSearchMetaCount))
	GlobalSegStoreSummary.SetInMemorySsmSizeMB(segutils.ConvertUintBytesToMB(inMemSize))
	hm.updateLock.RUnlock()
}

/*
Returns the max indices that should have SSM(segmentsearchmeta) loaded
*/
func (hm *allSegmentMetadata) getSsmMaxIndicesToLoad(totalMem uint64) int {

	numBlocks := len(hm.allSegmentMicroIndex)
	totalMBAllocated := uint64(0)
	maxSearchIndex := 0
	for ; maxSearchIndex < numBlocks; maxSearchIndex++ {
		searchMetadataSize := hm.allSegmentMicroIndex[maxSearchIndex].SearchMetadataSize
		if searchMetadataSize+totalMBAllocated > totalMem {
			break
		}
		totalMBAllocated += searchMetadataSize
	}

	return maxSearchIndex
}

func (hm *allSegmentMetadata) evictSsmPastIndices(searchIndex int) int {

	idxToClear := make([]int, 0)
	for i := searchIndex; i < len(hm.allSegmentMicroIndex); i++ {
		if hm.allSegmentMicroIndex[i].loadedSearchMetadata {
			idxToClear = append(idxToClear, i)
		}
	}

	if len(idxToClear) > 0 {
		for _, idx := range idxToClear {
			hm.allSegmentMicroIndex[idx].clearSearchMetadata()
		}
	}
	return len(idxToClear)
}

// Returns total in memory size in bytes, total search metadata in memory
func (hm *allSegmentMetadata) loadSsmUntilIndex(searchMetaIdx int) (uint64, int, int) {
	totalSize := uint64(0)
	totalSearchMetaCount := int(0)

	idxToLoad := make([]int, 0)
	for i := 0; i < searchMetaIdx; i++ {
		if !hm.allSegmentMicroIndex[i].loadedSearchMetadata {
			idxToLoad = append(idxToLoad, i)
		} else {
			totalSize += hm.allSegmentMicroIndex[i].SearchMetadataSize
			totalSearchMetaCount += 1
		}
	}

	if len(idxToLoad) > 0 {
		a, b := hm.loadParallelSsm(idxToLoad)
		totalSize += a
		totalSearchMetaCount += b
	}

	return totalSize, totalSearchMetaCount, len(idxToLoad)
}

func GetTotalSMICount() int64 {
	return int64(len(globalMetadata.allSegmentMicroIndex))
}

func GetNumOfSearchedRecordsRotated(segKey string) uint64 {
	globalMetadata.updateLock.RLock()
	smi, segKeyOk := globalMetadata.segmentMetadataReverseIndex[segKey]
	globalMetadata.updateLock.RUnlock()
	if !segKeyOk {
		return 0
	}
	return uint64(smi.RecordCount)
}

// returns the time range of the blocks in the segment that do not exist in spqmr
// if the timeRange is nil, no blocks were found in metadata that do not exist in spqmr
func GetTSRangeForMissingBlocks(segKey string, tRange *dtu.TimeRange, spqmr *pqmr.SegmentPQMRResults) *dtu.TimeRange {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()
	sMicroIdx, ok := globalMetadata.segmentMetadataReverseIndex[segKey]
	if !ok {
		log.Errorf("SegKey %+v does not exist in metadata yet existed for pqs!", segKey)
		return nil
	}

	if !sMicroIdx.loadedSearchMetadata {
		err := sMicroIdx.loadSearchMetadata()
		if err != nil {
			log.Errorf("Error loading search metadata: %+v", err)
			return nil
		}
		defer sMicroIdx.clearSearchMetadata()
	}

	var fRange *dtu.TimeRange
	for i, blockSummary := range sMicroIdx.BlockSummaries {
		if tRange.CheckRangeOverLap(blockSummary.LowTs, blockSummary.HighTs) && !spqmr.DoesBlockExist(uint16(i)) {

			if fRange == nil {
				fRange = &dtu.TimeRange{}
				fRange.StartEpochMs = blockSummary.LowTs
				fRange.EndEpochMs = blockSummary.HighTs
			} else {
				if blockSummary.LowTs < fRange.StartEpochMs {
					fRange.StartEpochMs = blockSummary.LowTs
				}
				if blockSummary.HighTs < fRange.EndEpochMs {
					fRange.EndEpochMs = blockSummary.HighTs
				}
			}
		}
	}
	return fRange
}

func IsUnrotatedQueryNeeded(timeRange *dtu.TimeRange, indexNames []string) bool {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()
	for _, index := range indexNames {
		if sortedTableMetadata, ok := globalMetadata.tableSortedMetadata[index]; !ok {
			return true // if table doesn't exist in segstore, assume it exists in unrotated
		} else {
			// as this list is decreasing by LatestEpochMS, we only need to check index 0
			if timeRange.EndEpochMs > sortedTableMetadata[0].LatestEpochMS {
				return true
			}
		}
	}
	return false
}

/*
	 Returns:
		 1. map of tableName -> map segKey -> segKey time range
		 2. []string of segKeys that are in the time range. that exist in the map
		 2. final matched count
		 3. total possible count
*/
func FilterSegmentsByTime(timeRange *dtu.TimeRange, indexNames []string, orgid int64) (map[string]map[string]*structs.SegmentByTimeAndColSizes, uint64, uint64) {

	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()
	timePassed := uint64(0)
	totalChecked := uint64(0)
	retVal := make(map[string]map[string]*structs.SegmentByTimeAndColSizes)
	for _, index := range indexNames {
		tableMicroIndices, ok := globalMetadata.tableSortedMetadata[index]
		if !ok {
			continue
		}

		for _, smi := range tableMicroIndices {
			if timeRange.CheckRangeOverLap(smi.EarliestEpochMS, smi.LatestEpochMS) && smi.OrgId == orgid {
				if _, ok := retVal[index]; !ok {
					retVal[index] = make(map[string]*structs.SegmentByTimeAndColSizes)
				}
				retVal[index][smi.SegmentKey] = &structs.SegmentByTimeAndColSizes{
					TimeRange: &dtu.TimeRange{
						StartEpochMs: smi.EarliestEpochMS,
						EndEpochMs:   smi.LatestEpochMS,
					},
					ConsistentCValLenMap: smi.getAllColumnsRecSize(),
					TotalRecords:         smi.getRecordCount(),
				}
				timePassed++
			}
		}
		totalChecked += uint64(len(tableMicroIndices))
	}
	return retVal, timePassed, totalChecked
}

func GetColumnsForTheIndexesByTimeRange(timeRange *dtu.TimeRange, indexNames []string, orgid int64) map[string]bool {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()
	allColumns := make(map[string]bool)
	for _, index := range indexNames {
		tableMicroIndices, ok := globalMetadata.tableSortedMetadata[index]
		if !ok {
			continue
		}

		for _, smi := range tableMicroIndices {
			if timeRange.CheckRangeOverLap(smi.EarliestEpochMS, smi.LatestEpochMS) && smi.OrgId == orgid {
				for col := range smi.ColumnNames {
					allColumns[col] = true
				}
			}
		}
	}
	return allColumns
}

func CollectColumnsForTheIndexesByTimeRange(timeRange *dtu.TimeRange,
	indexNames []string, orgid int64, resAllColumns map[string]struct{}) {

	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()
	for _, index := range indexNames {
		tableMicroIndices, ok := globalMetadata.tableSortedMetadata[index]
		if !ok {
			continue
		}

		for _, smi := range tableMicroIndices {
			if timeRange.CheckRangeOverLap(smi.EarliestEpochMS, smi.LatestEpochMS) && smi.OrgId == orgid {
				for col := range smi.ColumnNames {
					resAllColumns[col] = struct{}{}
				}
			}
		}
	}
}

func CheckAndCollectColNamesForSegKey(segKey string, resCnames map[string]struct{}) bool {

	globalMetadata.updateLock.RLock()
	segmentMetadata, segKeyOk := globalMetadata.segmentMetadataReverseIndex[segKey]
	globalMetadata.updateLock.RUnlock()
	if !segKeyOk {
		return false
	}

	segmentMetadata.CollectColumnNames(resCnames)
	return true
}

func DoesColumnExistForTable(cName string, indices []string) bool {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()
	for _, index := range indices {
		tableSMI, ok := globalMetadata.tableSortedMetadata[index]
		if !ok {
			continue
		}

		for _, smi := range tableSMI {
			_, ok := smi.ColumnNames[cName]
			if ok {
				return true
			}
		}
	}
	return false
}

func GetAllColNames(indices []string) []string {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()

	colNamesMap := make(map[string]bool)
	for _, index := range indices {
		tableSMI, ok := globalMetadata.tableSortedMetadata[index]
		if !ok {
			continue
		}
		for _, smi := range tableSMI {
			for cName := range smi.ColumnNames {
				colNamesMap[cName] = true
			}
		}
	}
	colNames := make([]string, 0, len(colNamesMap))
	for cName := range colNamesMap {
		colNames = append(colNames, cName)
	}
	return colNames
}

func GetSMIConsistentColValueLen[T any](segmap map[string]T) map[string]map[string]uint32 {
	ConsistentCValLenPerSeg := make(map[string]map[string]uint32, len(segmap))
	for segKey := range segmap {
		retval, ok := getAllColumnsRecSizeWithLock(segKey)
		if ok {
			ConsistentCValLenPerSeg[segKey] = retval
		}
	}
	return ConsistentCValLenPerSeg
}
