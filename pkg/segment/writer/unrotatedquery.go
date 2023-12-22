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

package writer

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/query/metadata/metautils"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type UnrotatedSegmentInfo struct {
	blockSummaries      []*structs.BlockSummary
	unrotatedPQSResults map[string]*pqmr.SegmentPQMRResults // maps qid to results
	blockInfo           map[uint16]*structs.BlockMetadataHolder
	allColumns          map[string]bool
	unrotatedBlockCmis  []map[string]*structs.CmiContainer
	tsRange             *dtu.TimeRange
	TableName           string
	searchMetadataSize  uint64 // size of blockSummaries & blockInfo
	cmiSize             uint64 // size of UnrotatedBlockCmis
	isCmiLoaded         bool   // is UnrotatedBlockCmis loaded?
	RecordCount         int
	orgid               uint64
}

var UnrotatedInfoLock sync.RWMutex = sync.RWMutex{}
var AllUnrotatedSegmentInfo = map[string]*UnrotatedSegmentInfo{}
var RecentlyRotatedSegmentFiles = map[string]*SegfileRotateInfo{}
var recentlyRotatedSegmentFilesLock sync.RWMutex = sync.RWMutex{}
var TotalUnrotatedMetadataSizeBytes uint64

func GetSizeOfUnrotatedMetadata() uint64 {
	return TotalUnrotatedMetadataSizeBytes
}

// Removed unrotated metadata from in memory based on the available size and return the new in memory size
// Currently, once we remove an entry we have no way of adding it back
// TODO: improve on re-loading of unrotated microindices
func RebalanceUnrotatedMetadata(totalAvailableSize uint64) uint64 {
	UnrotatedInfoLock.Lock()
	defer UnrotatedInfoLock.Unlock()
	if TotalUnrotatedMetadataSizeBytes <= totalAvailableSize {
		return TotalUnrotatedMetadataSizeBytes
	}
	sizeToRemove := TotalUnrotatedMetadataSizeBytes - totalAvailableSize
	ss := make([]*UnrotatedSegmentInfo, len(AllUnrotatedSegmentInfo))
	idx := 0
	for _, v := range AllUnrotatedSegmentInfo {
		ss[idx] = v
		idx++
	}

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].cmiSize < ss[j].cmiSize
	})
	removedSize := uint64(0)
	count := 0
	for i := 0; i < len(ss); i++ {
		if removedSize >= sizeToRemove {
			break
		}

		if ss[i].isCmiLoaded {
			removedSize += ss[i].removeInMemoryMetadata()
			count++
		}
	}
	var finalSize uint64
	if TotalUnrotatedMetadataSizeBytes > removedSize {
		finalSize = TotalUnrotatedMetadataSizeBytes - removedSize
	} else {
		finalSize = 0
	}

	atomic.StoreUint64(&TotalUnrotatedMetadataSizeBytes, finalSize)
	log.Infof("RebalanceUnrotatedMetadata: Unrotated data was allocated %v MB. Removed %+v MB of unrotated metadata after rebalance",
		segutils.ConvertUintBytesToMB(totalAvailableSize), segutils.ConvertUintBytesToMB(removedSize))
	log.Infof("RebalanceUnrotatedMetadata: Final Unrotated metadata in memory size: %v MB",
		segutils.ConvertUintBytesToMB(TotalUnrotatedMetadataSizeBytes))
	return finalSize
}

func removeSegKeyFromUnrotatedInfo(segkey string) {
	UnrotatedInfoLock.Lock()
	defer UnrotatedInfoLock.Unlock()
	var allResults *UnrotatedSegmentInfo
	var exists bool
	if allResults, exists = AllUnrotatedSegmentInfo[segkey]; !exists {
		return
	}
	delete(AllUnrotatedSegmentInfo, segkey)
	removedSize := allResults.getInMemorySize()

	if TotalUnrotatedMetadataSizeBytes > removedSize {
		atomic.AddUint64(&TotalUnrotatedMetadataSizeBytes, ^uint64(removedSize-1))
	} else {
		atomic.StoreUint64(&TotalUnrotatedMetadataSizeBytes, 0)
	}
}

func updateRecentlyRotatedSegmentFiles(segkey string, finalKey string) {
	recentlyRotatedSegmentFilesLock.Lock()
	RecentlyRotatedSegmentFiles[segkey] = &SegfileRotateInfo{
		FinalName:   finalKey,
		TimeRotated: utils.GetCurrentTimeInMs(),
	}
	recentlyRotatedSegmentFilesLock.Unlock()
}

func updateUnrotatedBlockInfo(segkey string, virtualTable string, wipBlock *WipBlock,
	blockMetadata *structs.BlockMetadataHolder, allCols map[string]bool, blockIdx uint16,
	metadataSize uint64, earliestTs uint64, latestTs uint64, recordCount int, orgid uint64) {
	UnrotatedInfoLock.Lock()
	defer UnrotatedInfoLock.Unlock()

	blkSumCpy := wipBlock.blockSummary.Copy()
	tRange := &dtu.TimeRange{StartEpochMs: earliestTs, EndEpochMs: latestTs}
	if _, ok := AllUnrotatedSegmentInfo[segkey]; !ok {
		AllUnrotatedSegmentInfo[segkey] = &UnrotatedSegmentInfo{
			blockSummaries:      make([]*structs.BlockSummary, 0),
			blockInfo:           make(map[uint16]*structs.BlockMetadataHolder),
			allColumns:          make(map[string]bool),
			unrotatedBlockCmis:  make([]map[string]*structs.CmiContainer, 0),
			unrotatedPQSResults: make(map[string]*pqmr.SegmentPQMRResults),
			TableName:           virtualTable,
			isCmiLoaded:         true, // default loading is true
			orgid:               orgid,
		}
	}
	AllUnrotatedSegmentInfo[segkey].blockSummaries = append(AllUnrotatedSegmentInfo[segkey].blockSummaries, blkSumCpy)
	AllUnrotatedSegmentInfo[segkey].blockInfo[blockIdx] = blockMetadata
	AllUnrotatedSegmentInfo[segkey].tsRange = tRange
	AllUnrotatedSegmentInfo[segkey].RecordCount = recordCount

	for col := range allCols {
		AllUnrotatedSegmentInfo[segkey].allColumns[col] = true
	}

	var pqidSize uint64
	if AllUnrotatedSegmentInfo[segkey].isCmiLoaded {
		AllUnrotatedSegmentInfo[segkey].addMicroIndicesToUnrotatedInfo(blockIdx, wipBlock.columnBlooms, wipBlock.columnRangeIndexes)
		pqidSize = AllUnrotatedSegmentInfo[segkey].addUnrotatedQIDInfo(blockIdx, wipBlock.pqMatches)
	}

	blkSumSize := blkSumCpy.GetSize()
	newSearchMetadataSize := blkSumSize + pqidSize
	AllUnrotatedSegmentInfo[segkey].searchMetadataSize += newSearchMetadataSize
	AllUnrotatedSegmentInfo[segkey].cmiSize += metadataSize

	totalSizeAdded := newSearchMetadataSize + metadataSize
	atomic.AddUint64(&TotalUnrotatedMetadataSizeBytes, totalSizeAdded)
}

func GetFileNameForRotatedSegment(seg string) (string, error) {
	recentlyRotatedSegmentFilesLock.RLock()
	defer recentlyRotatedSegmentFilesLock.RUnlock()
	newName, ok := RecentlyRotatedSegmentFiles[seg]
	if !ok {
		return "", errors.New("file was not recently rotated")
	}
	return newName.FinalName, nil
}

func IsRecentlyRotatedSegKey(key string) bool {
	recentlyRotatedSegmentFilesLock.RLock()
	_, ok := RecentlyRotatedSegmentFiles[key]
	recentlyRotatedSegmentFilesLock.RUnlock()
	return ok
}

func IsSegKeyUnrotated(key string) bool {
	UnrotatedInfoLock.RLock()
	_, ok := AllUnrotatedSegmentInfo[key]
	UnrotatedInfoLock.RUnlock()
	return ok
}

// Returns a copy of AllColumns seen for a given key from the unrotated segment infos
// If no key exists, returns an error
func CheckAndGetColsForUnrotatedSegKey(key string) (map[string]bool, bool) {
	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()
	cols, keyOk := AllUnrotatedSegmentInfo[key]
	if !keyOk {
		return nil, false
	}
	colsCopy := make(map[string]bool, len(cols.allColumns))
	for colName := range cols.allColumns {
		colsCopy[colName] = true
	}
	return colsCopy, true
}

// returns a copy of the unrotated block search info. This is to prevent concurrent modification
func GetBlockSearchInfoForKey(key string) (map[uint16]*structs.BlockMetadataHolder, error) {
	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()

	segInfo, keyOk := AllUnrotatedSegmentInfo[key]
	if !keyOk {
		return nil, errors.New("failed to get block search info for key")
	}

	retVal := make(map[uint16]*structs.BlockMetadataHolder)
	for blkNum, blkInfo := range segInfo.blockInfo {
		retVal[blkNum] = blkInfo
	}
	return retVal, nil
}

// returns the block summary for a segment key
func GetBlockSummaryForKey(key string) ([]*structs.BlockSummary, error) {
	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()

	segInfo, keyOk := AllUnrotatedSegmentInfo[key]
	if !keyOk {
		return nil, errors.New("failed to get block search info for key")
	}
	return segInfo.blockSummaries, nil
}

func (usi *UnrotatedSegmentInfo) resizeUnrotatedBlockCmis(blkNum uint16) {

	minBlocks := blkNum + 1
	if numBlks := uint16(len(usi.unrotatedBlockCmis)); minBlocks > numBlks {
		numToAdd := minBlocks - numBlks
		newSlice := make([]map[string]*structs.CmiContainer, numToAdd)
		for i := uint16(0); i < numToAdd; i++ {
			newSlice[i] = make(map[string]*structs.CmiContainer)
		}
		usi.unrotatedBlockCmis = append(usi.unrotatedBlockCmis, newSlice...)
	}
}

// add microindices to the UnrotatedBlockCmis. This function will only create copies of inputted microindices
func (usi *UnrotatedSegmentInfo) addMicroIndicesToUnrotatedInfo(blkNum uint16, columnBlooms map[string]*BloomIndex,
	columnRangeIndices map[string]*RangeIndex) {

	usi.resizeUnrotatedBlockCmis(blkNum)
	for colName, colBloom := range columnBlooms {
		if _, ok := usi.unrotatedBlockCmis[blkNum][colName]; !ok {
			usi.unrotatedBlockCmis[blkNum][colName] = &structs.CmiContainer{}
		}
		usi.unrotatedBlockCmis[blkNum][colName].CmiType = segutils.CMI_BLOOM_INDEX[0]
		usi.unrotatedBlockCmis[blkNum][colName].Bf = colBloom.Bf.Copy()
	}
	for colName, colRange := range columnRangeIndices {
		if _, ok := usi.unrotatedBlockCmis[blkNum][colName]; !ok {
			usi.unrotatedBlockCmis[blkNum][colName] = &structs.CmiContainer{}
			usi.unrotatedBlockCmis[blkNum][colName].CmiType = segutils.CMI_RANGE_INDEX[0]
		}
		newRange := colRange.copyRangeIndex()
		usi.unrotatedBlockCmis[blkNum][colName].Ranges = newRange.Ranges
	}
}

// add pqs results to UnrotatedSegmentInfo . This function will only create copies of inputted pqs results
func (usi *UnrotatedSegmentInfo) addUnrotatedQIDInfo(blkNum uint16, pqMatches map[string]*pqmr.PQMatchResults) uint64 {

	var totalSize uint64
	for qid, pqMatch := range pqMatches {
		sResults, ok := usi.unrotatedPQSResults[qid]
		if !ok {
			sResults = pqmr.InitSegmentPQMResults()
			usi.unrotatedPQSResults[qid] = sResults
		}
		newSize := sResults.CopyBlockResults(blkNum, pqMatch)
		totalSize += newSize
	}
	return totalSize
}

func (ri *RangeIndex) copyRangeIndex() *RangeIndex {
	finalRanges := make(map[string]*structs.Numbers)
	for colName, colRange := range ri.Ranges {
		finalRanges[colName] = colRange.Copy()
	}
	return &RangeIndex{Ranges: finalRanges}
}

// does CMI check on unrotated segment info for inputted request. Assumes UnrotatedInfoLock has been acquired
// returns the final blocks to search, total unrotated blocks, num filtered blocks, and errors if any
func (usi *UnrotatedSegmentInfo) DoCMICheckForUnrotated(currQuery *structs.SearchQuery, tRange *dtu.TimeRange,
	blkTracker *structs.BlockTracker, bloomWords map[string]bool, bloomOp segutils.LogicalOperator, rangeFilter map[string]string,
	rangeOp segutils.FilterOperator, isRange bool, wildcardValue bool,
	qid uint64) (map[uint16]map[string]bool, uint64, uint64, error) {

	timeFilteredBlocks := metautils.FilterBlocksByTime(usi.blockSummaries, blkTracker, tRange)
	totalPossibleBlocks := uint64(len(usi.blockSummaries))

	if len(timeFilteredBlocks) == 0 {
		return timeFilteredBlocks, totalPossibleBlocks, 0, nil
	}

	colsToCheck, wildcardColQuery := currQuery.GetAllColumnsInQuery()
	if wildcardColQuery {
		colsToCheck = usi.allColumns
	}
	var err error
	if isRange {
		err = usi.doRangeCheckForCols(timeFilteredBlocks, rangeFilter, rangeOp, colsToCheck, qid)
	} else if !wildcardValue {
		err = usi.doBloomCheckForCols(timeFilteredBlocks, bloomWords, bloomOp, colsToCheck, qid)
	}

	numFinalBlocks := uint64(len(timeFilteredBlocks))
	if err != nil {
		log.Errorf("DoCMICheckForUnrotated: failed to do cmi check for unrotated segments: %v", err)
		return timeFilteredBlocks, totalPossibleBlocks, numFinalBlocks, err
	}
	return timeFilteredBlocks, totalPossibleBlocks, numFinalBlocks, nil
}

func (usi *UnrotatedSegmentInfo) doRangeCheckForCols(timeFilteredBlocks map[uint16]map[string]bool,
	rangeFilter map[string]string, rangeOp segutils.FilterOperator,
	colsToCheck map[string]bool, qid uint64) error {

	if !usi.isCmiLoaded {
		return nil
	}
	numUnrotatedBlks := uint16(len(usi.unrotatedBlockCmis))
	for blkNum := range timeFilteredBlocks {
		if blkNum > numUnrotatedBlks {
			log.Errorf("DoRangeCheckAllCol: tried to check a block that does not exist in unrotated info. blkNum %+v, numBlocks %+v",
				blkNum, numUnrotatedBlks)
			continue
		}
		currInfo := usi.unrotatedBlockCmis[blkNum]
		var matchedBlockRange bool
		for col := range colsToCheck {
			var cmi *structs.CmiContainer
			var ok bool
			if cmi, ok = currInfo[col]; !ok {
				continue
			}
			if cmi.Ranges == nil {
				continue
			}
			matchedBlockRange = metautils.CheckRangeIndex(rangeFilter, cmi.Ranges, rangeOp, qid)
			if matchedBlockRange {
				timeFilteredBlocks[blkNum][col] = true
			}
		}
		if !matchedBlockRange {
			delete(timeFilteredBlocks, blkNum)
		}
	}
	return nil
}

func (usi *UnrotatedSegmentInfo) doBloomCheckForCols(timeFilteredBlocks map[uint16]map[string]bool,
	bloomKeys map[string]bool, bloomOp segutils.LogicalOperator,
	colsToCheck map[string]bool, qid uint64) error {

	if !usi.isCmiLoaded {
		return nil
	}
	numUnrotatedBlks := uint16(len(usi.unrotatedBlockCmis))
	for blkNum := range timeFilteredBlocks {
		if blkNum > numUnrotatedBlks {
			log.Errorf("doBloomCheckForCols: tried to check a block that does not exist in unrotated info. blkNum %+v, numBlocks %+v",
				blkNum, numUnrotatedBlks)
			continue
		}
		currInfo := usi.unrotatedBlockCmis[blkNum]
		var matchedNeedleInBlock = true
		var allEntriesMissing bool = false
		for entry := range bloomKeys {
			var atLeastOneFound bool
			for col := range colsToCheck {
				cmi, ok := currInfo[col]
				if !ok {
					continue
				}
				if cmi.Bf == nil {
					continue
				}
				needleExists := cmi.Bf.TestString(entry)
				if needleExists {
					atLeastOneFound = true
					timeFilteredBlocks[blkNum][col] = true
				}
			}
			if !atLeastOneFound && bloomOp == segutils.And {
				matchedNeedleInBlock = false
				break
			} else if atLeastOneFound && bloomOp == segutils.Or {
				allEntriesMissing = false
				matchedNeedleInBlock = true
				break
			} else if !atLeastOneFound && bloomOp == segutils.Or {
				allEntriesMissing = true
				matchedNeedleInBlock = false
			}
		}

		// Or only early exits when it sees true. If all entries are false, we need to handle it here
		if bloomOp == segutils.Or && allEntriesMissing && !matchedNeedleInBlock {
			matchedNeedleInBlock = false
		}

		if !matchedNeedleInBlock {
			delete(timeFilteredBlocks, blkNum)
		}
	}
	return nil
}

/*
For a unrotated segment info, return []blockSummaries, map[uint16]*structs.BlockMetadataHolder, and  map[string]bool (all columns)
This information will be used for unrotated queries. This will return copies of in memory metadata to avoid race conditions

# A copy needs to be returned here as usi.BlockSummaries and usi.BlockInfo may have concurrent writes

This assumes the caller has already acquired the lock on UnrotatedInfoLock
*/
func (usi *UnrotatedSegmentInfo) GetUnrotatedBlockInfoForQuery() ([]*structs.BlockSummary, map[uint16]*structs.BlockMetadataHolder, map[string]bool) {

	retBlkSum := make([]*structs.BlockSummary, len(usi.blockSummaries))
	for i := 0; i < len(usi.blockSummaries); i++ {
		retBlkSum[i] = usi.blockSummaries[i].Copy()
	}

	retBlkInfo := make(map[uint16]*structs.BlockMetadataHolder, len(usi.blockInfo))
	for k, v := range usi.blockInfo {
		retBlkInfo[k] = v
	}
	retBlkCols := make(map[string]bool, len(usi.allColumns))
	for k, v := range usi.allColumns {
		retBlkCols[k] = v
	}

	return retBlkSum, retBlkInfo, retBlkCols
}

/*
For a unrotated segment info, remove all microindices from in memory and set usi.loaded = False
Only usi.UnrotatedBlockCmis will be removed from in memory

# This function does not use locks so it is up to the caller to protect concurrent access

Returns the size removed
*/
func (usi *UnrotatedSegmentInfo) removeInMemoryMetadata() uint64 {

	if usi.isCmiLoaded {
		usi.isCmiLoaded = false
		usi.unrotatedBlockCmis = make([]map[string]*structs.CmiContainer, 0)
		return usi.cmiSize
	}
	return 0
}

/*
Returns the in memory size of a UnrotatedSegmentInfo
*/
func (usi *UnrotatedSegmentInfo) getInMemorySize() uint64 {

	size := uint64(0)
	if usi.isCmiLoaded {
		size += usi.cmiSize
	}
	size += usi.searchMetadataSize

	return size
}

/*
Returns number of loaded unrotated metadata, and total number of unrotated metadata
*/
func GetUnrotatedMetadataInfo() (uint64, uint64) {
	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()
	loaded := uint64(0)
	for _, usi := range AllUnrotatedSegmentInfo {
		if usi.isCmiLoaded {
			loaded++
		}
	}
	return loaded, uint64(len(AllUnrotatedSegmentInfo))
}

// returns map[table]->map[segKey]->timeRange that pass index & time range check, total checked, total passed
func FilterUnrotatedSegmentsInQuery(timeRange *dtu.TimeRange, indexNames []string, orgid uint64) (map[string]map[string]*dtu.TimeRange, uint64, uint64) {
	totalCount := uint64(0)
	totalChecked := uint64(0)
	retVal := make(map[string]map[string]*dtu.TimeRange)

	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()
	for segKey, usi := range AllUnrotatedSegmentInfo {
		var foundIndex bool
		for _, idxName := range indexNames {
			if idxName == usi.TableName {
				foundIndex = true
				break
			}
		}
		if !foundIndex {
			continue
		}
		totalChecked++
		if !timeRange.CheckRangeOverLap(usi.tsRange.StartEpochMs, usi.tsRange.EndEpochMs) || usi.orgid != orgid {
			continue
		}
		if _, ok := retVal[usi.TableName]; !ok {
			retVal[usi.TableName] = make(map[string]*dtu.TimeRange)
		}
		retVal[usi.TableName][segKey] = usi.tsRange
		totalCount++
	}
	return retVal, totalChecked, totalCount
}

func DoesSegKeyHavePqidResults(segKey string, pqid string) bool {
	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()
	usi, ok := AllUnrotatedSegmentInfo[segKey]
	if !ok {
		return false
	}
	if _, ok := usi.unrotatedPQSResults[pqid]; !ok {
		return false
	}
	return true
}

func GetAllPersistentQueryResults(segKey string, pqid string) (*pqmr.SegmentPQMRResults, error) {
	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()
	usi, ok := AllUnrotatedSegmentInfo[segKey]
	if !ok {
		return nil, fmt.Errorf("segkey %+v does not exist in unrotated info", segKey)
	}
	spqmr, ok := usi.unrotatedPQSResults[pqid]
	if !ok {
		return nil, fmt.Errorf("pqid %+v does not exist for segment %+v", pqid, segKey)
	}
	return spqmr, nil
}

func (usi *UnrotatedSegmentInfo) GetTimeRange() *dtu.TimeRange {
	return usi.tsRange
}

// returns the time range of the blocks in the segment that do not exist in spqmr
// if the timeRange is nil, no blocks were found in unrotated metadata that donot exist in spqmr
func GetTSRangeForMissingBlocks(segKey string, tRange *dtu.TimeRange, spqmr *pqmr.SegmentPQMRResults) *dtu.TimeRange {
	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()
	usi, ok := AllUnrotatedSegmentInfo[segKey]
	if !ok {
		log.Errorf("GetTSRangeForMissingBlocks: segKey %+v does not exist in unrotated", segKey)
		return nil
	}

	var fRange *dtu.TimeRange
	for i, blockSummary := range usi.blockSummaries {
		if tRange.CheckRangeOverLap(blockSummary.LowTs, blockSummary.HighTs) &&
			!spqmr.DoesBlockExist(uint16(i)) {
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

// returns block search info, block summaries, and any errors encountered
// block search info will be loaded for all possible columns
func GetSearchInfoForPQSQuery(key string, spqmr *pqmr.SegmentPQMRResults) (map[uint16]*structs.BlockMetadataHolder,
	[]*structs.BlockSummary, error) {
	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()

	usi, ok := AllUnrotatedSegmentInfo[key]
	if !ok {
		return nil, nil, errors.New("failed to find key in all block micro")
	}

	retSearchInfo := make(map[uint16]*structs.BlockMetadataHolder)
	for _, blkNum := range spqmr.GetAllBlocks() {
		if blkMetadata, ok := usi.blockInfo[blkNum]; ok {
			retSearchInfo[blkNum] = blkMetadata
		}
	}

	retBlkSum := make([]*structs.BlockSummary, len(usi.blockSummaries))
	copy(retBlkSum, usi.blockSummaries)

	return retSearchInfo, retBlkSum, nil
}

func GetNumOfSearchedRecordsUnRotated(segKey string) uint64 {
	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()
	usi, ok := AllUnrotatedSegmentInfo[segKey]
	if !ok {
		log.Debugf("GetNumOfSearchedRecordsUnRotated: segKey %+v does not exist in unrotated", segKey)
		return 0
	}
	return uint64(usi.RecordCount)
}
