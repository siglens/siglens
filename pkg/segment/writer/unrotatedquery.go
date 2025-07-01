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

package writer

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/query/metadata/metautils"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type UnrotatedSegmentInfo struct {
	blockSummaries      []*structs.BlockSummary
	unrotatedPQSResults map[string]*pqmr.SegmentPQMRResults // maps qid to results
	blockInfo           *structs.AllBlksMetaInfo
	allColumns          map[string]bool
	unrotatedBlockCmis  []map[string]*structs.CmiContainer
	tsRange             *dtu.TimeRange
	TableName           string
	searchMetadataSize  uint64 // size of blockSummaries & blockInfo
	cmiSize             uint64 // size of UnrotatedBlockCmis
	removedCmiSize      uint64 // size of removed CMI due to memory rebalance
	isCmiLoaded         bool   // is UnrotatedBlockCmis loaded?
	RecordCount         int
	orgid               int64
}

var UnrotatedInfoLock sync.RWMutex = sync.RWMutex{}
var AllUnrotatedSegmentInfo = map[string]*UnrotatedSegmentInfo{}
var RecentlyRotatedSegmentFiles = map[string]*SegfileRotateInfo{}
var recentlyRotatedSegmentFilesLock sync.RWMutex = sync.RWMutex{}
var TotalUnrotatedMetadataSizeBytes uint64

func GetSizeOfUnrotatedMetadata() uint64 {
	return atomic.LoadUint64(&TotalUnrotatedMetadataSizeBytes)
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
		return ss[i].cmiSize-ss[i].removedCmiSize < ss[j].cmiSize-ss[j].removedCmiSize
	})
	removedSize := uint64(0)
	count := 0
	for i := 0; i < len(ss); i++ {
		if removedSize >= sizeToRemove {
			break
		}
		size := ss[i].removeInMemoryMetadata()
		if size > 0 {
			removedSize += size
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
		sutils.ConvertUintBytesToMB(totalAvailableSize), sutils.ConvertUintBytesToMB(removedSize))
	log.Infof("RebalanceUnrotatedMetadata: Final Unrotated metadata in memory size: %v MB",
		sutils.ConvertUintBytesToMB(TotalUnrotatedMetadataSizeBytes))
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

func updateRecentlyRotatedSegmentFiles(segkey string, tableName string) {
	// TODO: Does this function do anything useful now? Can it be removed?
	recentlyRotatedSegmentFilesLock.Lock()
	RecentlyRotatedSegmentFiles[segkey] = &SegfileRotateInfo{
		FinalName:   segkey,
		TimeRotated: utils.GetCurrentTimeInMs(),
		tableName:   tableName,
	}
	recentlyRotatedSegmentFilesLock.Unlock()
}

func updateUnrotatedBlockInfo(segkey string, virtualTable string, wipBlock *WipBlock,
	bmiCnameIdxDict map[string]int, bmiColOffLen []structs.ColOffAndLen,
	allCols map[string]uint32, blockNum uint16,
	metadataSize uint64, earliestTs uint64, latestTs uint64, recordCount int, orgid int64,
	pqMatches map[string]*pqmr.PQMatchResults) {
	UnrotatedInfoLock.Lock()
	defer UnrotatedInfoLock.Unlock()

	blkSumCpy := wipBlock.blockSummary.Copy()
	tRange := &dtu.TimeRange{StartEpochMs: earliestTs, EndEpochMs: latestTs}
	var allBmi *structs.AllBlksMetaInfo
	if _, ok := AllUnrotatedSegmentInfo[segkey]; !ok {
		isCmiLoaded := true // default loading is true
		if config.IsLowMemoryModeEnabled() {
			// in low mem mode, we don't want to load up all of the cmi for all of the columns
			// for the in-process seg
			isCmiLoaded = false
		}
		allBmi = &structs.AllBlksMetaInfo{
			CnameDict: make(map[string]int),
			AllBmh:    make(map[uint16]*structs.BlockMetadataHolder),
		}
		AllUnrotatedSegmentInfo[segkey] = &UnrotatedSegmentInfo{
			blockSummaries:      make([]*structs.BlockSummary, 0),
			blockInfo:           allBmi,
			allColumns:          make(map[string]bool),
			unrotatedBlockCmis:  make([]map[string]*structs.CmiContainer, 0),
			unrotatedPQSResults: make(map[string]*pqmr.SegmentPQMRResults),
			TableName:           virtualTable,
			isCmiLoaded:         isCmiLoaded,
			orgid:               orgid,
		}
	} else {
		allBmi = AllUnrotatedSegmentInfo[segkey].blockInfo
	}
	AllUnrotatedSegmentInfo[segkey].blockSummaries = append(AllUnrotatedSegmentInfo[segkey].blockSummaries, blkSumCpy)
	AllUnrotatedSegmentInfo[segkey].tsRange = tRange
	AllUnrotatedSegmentInfo[segkey].RecordCount = recordCount

	bmh := &structs.BlockMetadataHolder{
		BlkNum:            blockNum,
		ColBlockOffAndLen: utils.ShallowCopySlice(bmiColOffLen),
	}

	allBmi.AllBmh[blockNum] = bmh
	// if new cnames got added in this block, then copy them over
	utils.MergeMapsRetainingFirst(allBmi.CnameDict, bmiCnameIdxDict)

	for col := range allCols {
		AllUnrotatedSegmentInfo[segkey].allColumns[col] = true
	}

	var pqidSize uint64
	if AllUnrotatedSegmentInfo[segkey].isCmiLoaded {
		AllUnrotatedSegmentInfo[segkey].addMicroIndicesToUnrotatedInfo(blockNum, wipBlock.columnBlooms, wipBlock.columnRangeIndexes)
		pqidSize = AllUnrotatedSegmentInfo[segkey].addUnrotatedQIDInfo(blockNum, pqMatches)
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

// If no segkey exists, returns false
func CheckAndCollectColNamesForSegKey(key string, resCnames map[string]struct{}) bool {
	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()
	cols, keyOk := AllUnrotatedSegmentInfo[key]
	if !keyOk {
		return false
	}
	for colName := range cols.allColumns {
		resCnames[colName] = struct{}{}
	}
	return true
}

// returns a copy of the unrotated block search info. This is to prevent concurrent modification
func GetBlockSearchInfoForKey(key string) (*structs.AllBlksMetaInfo, error) {
	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()

	segInfo, keyOk := AllUnrotatedSegmentInfo[key]
	if !keyOk {
		return nil, errors.New("failed to get block search info for key")
	}

	retVal := &structs.AllBlksMetaInfo{
		CnameDict: segInfo.blockInfo.CnameDict,
		AllBmh:    make(map[uint16]*structs.BlockMetadataHolder),
	}

	for blkNum, blkInfo := range segInfo.blockInfo.AllBmh {
		retVal.AllBmh[blkNum] = blkInfo
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
		usi.unrotatedBlockCmis[blkNum][colName].CmiType = sutils.CMI_BLOOM_INDEX[0]
		usi.unrotatedBlockCmis[blkNum][colName].Bf = colBloom.Bf.Copy()
	}
	for colName, colRange := range columnRangeIndices {
		if _, ok := usi.unrotatedBlockCmis[blkNum][colName]; !ok {
			usi.unrotatedBlockCmis[blkNum][colName] = &structs.CmiContainer{}
			usi.unrotatedBlockCmis[blkNum][colName].CmiType = sutils.CMI_RANGE_INDEX[0]
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
	blkTracker *structs.BlockTracker, bloomWords map[string]bool, originalBloomWords map[string]string, bloomOp sutils.LogicalOperator, rangeFilter map[string]string,
	rangeOp sutils.FilterOperator, isRange bool, wildcardValue bool,
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
		err = usi.doBloomCheckForCols(timeFilteredBlocks, bloomWords, originalBloomWords, bloomOp, colsToCheck, qid)
	}

	numFinalBlocks := uint64(len(timeFilteredBlocks))
	if err != nil {
		log.Errorf("DoCMICheckForUnrotated: failed to do cmi check for unrotated segments: %v", err)
		return timeFilteredBlocks, totalPossibleBlocks, numFinalBlocks, err
	}
	return timeFilteredBlocks, totalPossibleBlocks, numFinalBlocks, nil
}

func (usi *UnrotatedSegmentInfo) doRangeCheckForCols(timeFilteredBlocks map[uint16]map[string]bool,
	rangeFilter map[string]string, rangeOp sutils.FilterOperator,
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
		// As long as there is one column within the range, the value is equal to true
		var matchedBlockRange bool
		for col := range colsToCheck {
			cmi, ok := currInfo[col]
			if !ok || cmi == nil || cmi.Ranges == nil {
				if rangeOp == sutils.NotEquals {
					timeFilteredBlocks[blkNum][col] = true
					matchedBlockRange = true
				}
				continue
			}

			isMatched := metautils.CheckRangeIndex(rangeFilter, cmi.Ranges, rangeOp, qid)
			if isMatched {
				timeFilteredBlocks[blkNum][col] = true
			}
			matchedBlockRange = matchedBlockRange || isMatched
		}
		if !matchedBlockRange {
			delete(timeFilteredBlocks, blkNum)
		}
	}
	return nil
}

func (usi *UnrotatedSegmentInfo) doBloomCheckForCols(timeFilteredBlocks map[uint16]map[string]bool,
	bloomKeys map[string]bool, originalBloomKeys map[string]string, bloomOp sutils.LogicalOperator,
	colsToCheck map[string]bool, qid uint64) error {

	if !usi.isCmiLoaded {
		return nil
	}

	checkInOriginalKeys := len(originalBloomKeys) > 0

	numUnrotatedBlks := uint16(len(usi.unrotatedBlockCmis))
	for blkNum := range timeFilteredBlocks {
		if blkNum >= numUnrotatedBlks {
			log.Errorf("qid=%v, doBloomCheckForCols: tried to check a block that does not exist in unrotated info. blkNum %+v, numBlocks %+v",
				qid, blkNum, numUnrotatedBlks)
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
				if !needleExists && checkInOriginalKeys {
					originalEntry, ok := originalBloomKeys[entry]
					if ok {
						needleExists = cmi.Bf.TestString(originalEntry)
					}
				}
				if needleExists {
					atLeastOneFound = true
					timeFilteredBlocks[blkNum][col] = true
				}
			}
			if !atLeastOneFound && bloomOp == sutils.And {
				matchedNeedleInBlock = false
				break
			} else if atLeastOneFound && bloomOp == sutils.Or {
				allEntriesMissing = false
				matchedNeedleInBlock = true
				break
			} else if !atLeastOneFound && bloomOp == sutils.Or {
				allEntriesMissing = true
				matchedNeedleInBlock = false
			}
		}

		// Or only early exits when it sees true. If all entries are false, we need to handle it here
		if bloomOp == sutils.Or && allEntriesMissing && !matchedNeedleInBlock {
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
func (usi *UnrotatedSegmentInfo) GetUnrotatedBlockInfoForQuery() ([]*structs.BlockSummary,
	*structs.AllBlksMetaInfo, map[string]bool) {

	retBlkSum := make([]*structs.BlockSummary, len(usi.blockSummaries))
	for i := 0; i < len(usi.blockSummaries); i++ {
		retBlkSum[i] = usi.blockSummaries[i].Copy()
	}

	retBlkInfo := &structs.AllBlksMetaInfo{
		CnameDict: usi.blockInfo.CnameDict,
		AllBmh:    make(map[uint16]*structs.BlockMetadataHolder),
	}

	for k, v := range usi.blockInfo.AllBmh {
		retBlkInfo.AllBmh[k] = v
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
	usi.isCmiLoaded = false
	currentSizeToRemove := usi.cmiSize - usi.removedCmiSize
	usi.removedCmiSize += currentSizeToRemove
	usi.unrotatedBlockCmis = make([]map[string]*structs.CmiContainer, 0)
	return currentSizeToRemove
}

/*
Returns the in memory size of a UnrotatedSegmentInfo
*/
func (usi *UnrotatedSegmentInfo) getInMemorySize() uint64 {

	size := uint64(0)
	size += usi.cmiSize - usi.removedCmiSize
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
func FilterUnrotatedSegmentsInQuery(timeRange *dtu.TimeRange, indexNames []string, orgid int64) (map[string]map[string]*structs.SegmentByTimeAndColSizes, uint64, uint64) {
	totalCount := uint64(0)
	totalChecked := uint64(0)
	retVal := make(map[string]map[string]*structs.SegmentByTimeAndColSizes)

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
			retVal[usi.TableName] = make(map[string]*structs.SegmentByTimeAndColSizes)
		}
		retVal[usi.TableName][segKey] = &structs.SegmentByTimeAndColSizes{
			TimeRange:    usi.tsRange,
			TotalRecords: uint32(usi.RecordCount),
		}
		totalCount++
	}
	return retVal, totalChecked, totalCount
}

func GetUnrotatedColumnsForTheIndexesByTimeRange(timeRange *dtu.TimeRange, indexNames []string, orgid int64) map[string]bool {
	allColumns := make(map[string]bool)
	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()
	for _, usi := range AllUnrotatedSegmentInfo {
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
		if !timeRange.CheckRangeOverLap(usi.tsRange.StartEpochMs, usi.tsRange.EndEpochMs) || usi.orgid != orgid {
			continue
		}
		for col := range usi.allColumns {
			allColumns[col] = true
		}
	}
	return allColumns
}

func CollectUnrotatedColumnsForTheIndexesByTimeRange(timeRange *dtu.TimeRange,
	indexNames []string, orgid int64, resAllColumns map[string]struct{}) {

	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()
	for _, usi := range AllUnrotatedSegmentInfo {
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
		if !timeRange.CheckRangeOverLap(usi.tsRange.StartEpochMs, usi.tsRange.EndEpochMs) || usi.orgid != orgid {
			continue
		}
		for col := range usi.allColumns {
			resAllColumns[col] = struct{}{}
		}
	}
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

// returns blockNums to search, block summaries, and any errors encountered
// block search info will be loaded for all possible columns
func GetSearchInfoForPQSQuery(key string,
	spqmr *pqmr.SegmentPQMRResults) (map[uint16]struct{},
	[]*structs.BlockSummary, error) {

	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()

	usi, ok := AllUnrotatedSegmentInfo[key]
	if !ok {
		return nil, nil, errors.New("failed to find key in all block micro")
	}

	allBlocksToSearch := make(map[uint16]struct{})

	for _, blkNum := range spqmr.GetAllBlocks() {
		if _, ok := usi.blockInfo.AllBmh[blkNum]; ok {
			allBlocksToSearch[blkNum] = struct{}{}
		}
	}

	retBlkSum := make([]*structs.BlockSummary, len(usi.blockSummaries))
	copy(retBlkSum, usi.blockSummaries)

	return allBlocksToSearch, retBlkSum, nil
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

func GetIndexNamesForUnrotated() map[string]struct{} {
	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()

	retVal := make(map[string]struct{})
	for _, usi := range AllUnrotatedSegmentInfo {
		retVal[usi.TableName] = struct{}{}
	}
	return retVal
}

func GetIndexNamesForRecentlyRotated() map[string]struct{} {
	recentlyRotatedSegmentFilesLock.RLock()
	defer recentlyRotatedSegmentFilesLock.RUnlock()

	retVal := make(map[string]struct{})
	for _, rrsf := range RecentlyRotatedSegmentFiles {
		retVal[rrsf.tableName] = struct{}{}
	}
	return retVal
}
