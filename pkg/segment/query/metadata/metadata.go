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
	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
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

func BulkAddSegmentMicroIndex(allMetadata []*SegmentMicroIndex) {
	globalMetadata.bulkAddSegmentMicroIndex(allMetadata)
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

	if right.MicroIndexSize != 0 && left.MicroIndexSize == 0 {
		left.MicroIndexSize = right.MicroIndexSize
	}

	if right.SearchMetadataSize != 0 && left.SearchMetadataSize == 0 {
		left.SearchMetadataSize = right.SearchMetadataSize
	}

	if right.RecordCount > 0 && left.RecordCount == 0 {
		left.RecordCount = right.RecordCount
	}

	return left, nil
}

func RebalanceInMemoryCmi(metadataSizeBytes uint64) {
	globalMetadata.rebalanceCmi(metadataSizeBytes)
}

// Entry point to rebalance what is loaded in memory depending on BLOCK_MICRO_MEM_SIZE
func (hm *allSegmentMetadata) rebalanceCmi(metadataSizeBytes uint64) {

	sTime := time.Now()

	hm.updateLock.RLock()
	cmiIndex := hm.getCmiMaxIndicesToLoad(metadataSizeBytes)
	evicted := hm.evictCmiPastIndices(cmiIndex)
	inMemSize, inMemCMI, newloaded := hm.loadCmiUntilIndex(cmiIndex)

	log.Infof("rebalanceCmi: CMI, inMem: %+v, allocated: %+v MB, evicted: %v, newloaded: %v, took: %vms",
		inMemCMI, utils.ConvertUintBytesToMB(inMemSize),
		evicted, newloaded, int(time.Since(sTime).Milliseconds()))
	GlobalSegStoreSummary.SetInMemoryBlockMicroIndexCount(uint64(inMemCMI))
	GlobalSegStoreSummary.SetInMemoryBlockMicroIndexSizeMB(utils.ConvertUintBytesToMB(inMemSize))
	hm.updateLock.RUnlock()
}

/*
Returns the max indices that should have metadata loaded

First value is the max index where CMIs should be loaded
*/
func (hm *allSegmentMetadata) getCmiMaxIndicesToLoad(totalMem uint64) int {
	// 1. get max index to load assuming both CMI & search metadata will be loaded
	// 2. with remaining size, load whatever search metadata that fits in memory

	numBlocks := len(hm.allSegmentMicroIndex)
	totalMBAllocated := uint64(0)
	maxCmiIndex := 0
	for ; maxCmiIndex < numBlocks; maxCmiIndex++ {
		cmiSize := hm.allSegmentMicroIndex[maxCmiIndex].MicroIndexSize + hm.allSegmentMicroIndex[maxCmiIndex].SearchMetadataSize
		if cmiSize+totalMBAllocated > totalMem {
			break
		}
		totalMBAllocated += cmiSize
	}

	return maxCmiIndex
}

func (hm *allSegmentMetadata) evictCmiPastIndices(cmiIndex int) int {
	idxToClear := make([]int, 0)
	for i := cmiIndex; i < len(hm.allSegmentMicroIndex); i++ {
		if hm.allSegmentMicroIndex[i].loadedMicroIndices {
			idxToClear = append(idxToClear, i)
		}
	}

	if len(idxToClear) > 0 {
		for _, idx := range idxToClear {
			hm.allSegmentMicroIndex[idx].clearMicroIndices()
		}
	}
	return len(idxToClear)
}

// Returns total in memory size in bytes, total cmis in memory, total search metadata in memory
func (hm *allSegmentMetadata) loadCmiUntilIndex(cmiIdx int) (uint64, int, int) {

	totalSize := uint64(0)
	totalCMICount := int(0)

	idxToLoad := make([]int, 0)

	for i := 0; i < cmiIdx; i++ {
		if !hm.allSegmentMicroIndex[i].loadedMicroIndices {
			idxToLoad = append(idxToLoad, i)
		} else {
			totalSize += hm.allSegmentMicroIndex[i].MicroIndexSize
			totalCMICount += 1
		}
	}

	if len(idxToLoad) > 0 {
		a, b := hm.loadParallel(idxToLoad, true)
		totalSize += a
		totalCMICount += b
	}

	return totalSize, totalCMICount, len(idxToLoad)
}

/*
Parameters:

	idxToLoad: indices in the allSegmentMicroIndex to be loaded
	cmi: whether to load cmi (true) or ssm (false)

Returns:

	totalSize:
	totalEntities:
*/
func (hm *allSegmentMetadata) loadParallel(idxToLoad []int, cmi bool) (uint64, int) {

	totalSize := uint64(0)
	totalEntities := int(0)

	wg := &sync.WaitGroup{}
	var err error
	var ssmBufs [][]byte
	parallelism := int(config.GetParallelism())
	if !cmi {
		ssmBufs = make([][]byte, parallelism)
		for i := 0; i < parallelism; i++ {
			ssmBufs[i] = make([]byte, 0)
		}
	}

	for i, idx := range idxToLoad {
		wg.Add(1)
		go func(myIdx int, rbufIdx int) {
			if cmi {
				pqsCols, err := querytracker.GetPersistentColumns(hm.allSegmentMicroIndex[myIdx].VirtualTableName, hm.allSegmentMicroIndex[idx].OrgId)
				if err != nil {
					log.Errorf("loadParallel: error getting persistent columns: %v", err)
				} else {
					err = hm.allSegmentMicroIndex[myIdx].loadMicroIndices(map[uint16]map[string]bool{}, true, pqsCols, true)
					if err != nil {
						log.Errorf("loadParallel: failed to load SSM at index %d. Error %v",
							myIdx, err)
					}
				}
			} else {
				ssmBufs[rbufIdx], err = hm.allSegmentMicroIndex[myIdx].LoadSearchMetadata(ssmBufs[rbufIdx])
				if err != nil {
					log.Errorf("loadParallel: failed to load SSM at index %d. Error: %v", myIdx, err)
				}
			}
			wg.Done()
		}(idx, i%parallelism)
		if i%parallelism == 0 {
			wg.Wait()
		}
	}
	wg.Wait()

	for _, idx := range idxToLoad {
		if cmi {
			if hm.allSegmentMicroIndex[idx].loadedMicroIndices {
				totalSize += hm.allSegmentMicroIndex[idx].MicroIndexSize
				totalEntities += 1
			}
		} else {
			if hm.allSegmentMicroIndex[idx].loadedSearchMetadata {
				totalSize += hm.allSegmentMicroIndex[idx].SearchMetadataSize
				totalEntities += 1
			}
		}
	}
	return totalSize, totalEntities
}

func (hm *allSegmentMetadata) deleteSegmentKey(key string) {
	hm.updateLock.Lock()
	defer hm.updateLock.Unlock()
	hm.deleteSegmentKeyInternal(key)
}

func (hm *allSegmentMetadata) deleteTable(table string, orgid uint64) {
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
		hm.deleteSegmentKeyInternal(segKey)
	}
	delete(hm.tableSortedMetadata, table)
	GlobalSegStoreSummary.DecrementTotalTableCount()
}

// internal function to delete segment key from all SiglensMetadata structs
// caller is responsible for acquiring locks
func (hm *allSegmentMetadata) deleteSegmentKeyInternal(key string) {
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
		log.Debugf("DeleteSegmentKey key %+v was not found in metadata", key)
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

func (hm *allSegmentMetadata) getMicroIndex(segKey string) (*SegmentMicroIndex, bool) {
	blockMicroIndex, ok := hm.segmentMetadataReverseIndex[segKey]
	return blockMicroIndex, ok
}

func DeleteSegmentKey(segKey string) {
	globalMetadata.deleteSegmentKey(segKey)
}

func DeleteVirtualTable(vTable string, orgid uint64) {
	globalMetadata.deleteTable(vTable, orgid)
}

/*
Internally, this will allocate 30% of the SSM size to metrics and the remaining to logs
*/
func RebalanceInMemorySsm(ssmSizeBytes uint64) {
	logsSSM := uint64(float64(ssmSizeBytes) * 0.30)
	metricsSSM := uint64(float64(ssmSizeBytes) * 0.70)
	globalMetadata.rebalanceSsm(logsSSM)
	globalMetricsMetadata.rebalanceMetricsSsm(metricsSSM)
}

func (hm *allSegmentMetadata) rebalanceSsm(ssmSizeBytes uint64) {

	sTime := time.Now()

	hm.updateLock.RLock()
	searchIndex := hm.getSsmMaxIndicesToLoad(ssmSizeBytes)
	evicted := hm.evictSsmPastIndices(searchIndex)

	inMemSize, inMemSearchMetaCount, newloaded := hm.loadSsmUntilIndex(searchIndex)

	log.Infof("rebalanceSsm SSM, inMem: %+v SSM, allocated: %+v MB, evicted: %v, newloaded: %v, took: %vms",
		inMemSearchMetaCount, utils.ConvertUintBytesToMB(inMemSize),
		evicted, newloaded, int(time.Since(sTime).Milliseconds()))

	GlobalSegStoreSummary.SetInMemorySearchmetadataCount(uint64(inMemSearchMetaCount))
	GlobalSegStoreSummary.SetInMemorySsmSizeMB(utils.ConvertUintBytesToMB(inMemSize))
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
		a, b := hm.loadParallel(idxToLoad, false)
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
		_, err := sMicroIdx.LoadSearchMetadata([]byte{})
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
func FilterSegmentsByTime(timeRange *dtu.TimeRange, indexNames []string, orgid uint64) (map[string]map[string]*dtu.TimeRange, uint64, uint64) {

	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()
	timePassed := uint64(0)
	totalChecked := uint64(0)
	retVal := make(map[string]map[string]*dtu.TimeRange)
	for _, index := range indexNames {
		tableMicroIndices, ok := globalMetadata.tableSortedMetadata[index]
		if !ok {
			continue
		}

		for _, smi := range tableMicroIndices {
			if timeRange.CheckRangeOverLap(smi.EarliestEpochMS, smi.LatestEpochMS) && smi.OrgId == orgid {
				if _, ok := retVal[index]; !ok {
					retVal[index] = make(map[string]*dtu.TimeRange)
				}
				retVal[index][smi.SegmentKey] = &dtu.TimeRange{
					StartEpochMs: smi.EarliestEpochMS,
					EndEpochMs:   smi.LatestEpochMS,
				}
				timePassed++
			}
		}
		totalChecked += uint64(len(tableMicroIndices))
	}
	return retVal, timePassed, totalChecked
}

// returns the a map with columns as keys and returns a bool if the segkey/table was found
func CheckAndGetColsForSegKey(segKey string, vtable string) (map[string]bool, bool) {

	globalMetadata.updateLock.RLock()
	segmentMetadata, segKeyOk := globalMetadata.segmentMetadataReverseIndex[segKey]
	globalMetadata.updateLock.RUnlock()
	if !segKeyOk {
		return nil, false
	}

	return segmentMetadata.getColumns(), true
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
