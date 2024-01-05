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

package metadata

import (
	"errors"
	"fmt"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/query/metadata/metautils"
	pqsmeta "github.com/siglens/siglens/pkg/segment/query/pqs/meta"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/utils/semaphore"
	log "github.com/sirupsen/logrus"
)

const INITIAL_NUM_BLOCKS = 1000

var GlobalBlockMicroIndexCheckLimiter *semaphore.WeightedSemaphore

func InitBlockMetaCheckLimiter(unloadedBlockLimit int64) {
	GlobalBlockMicroIndexCheckLimiter = semaphore.NewDefaultWeightedSemaphore(unloadedBlockLimit, "GlobalBlockMicroIndexCheckLimiter")
}

// converts blocks to a search request. block summaries & column meta are not guaranteed to be in memory
// if the block summaries & column meta are not in memory, then load right before query
func convertBlocksToSearchRequest(blocksForFile map[uint16]map[string]bool, file string, indexName string,
	segMicroIdx *SegmentMicroIndex) (*structs.SegmentSearchRequest, error) {

	if len(blocksForFile) == 0 {
		return nil, errors.New("no matched blocks for search request")
	}

	searchMeta := &structs.SearchMetadataHolder{
		BlockSummariesFile: structs.GetBsuFnameFromSegKey(segMicroIdx.SegmentKey),
		SearchTotalMemory:  segMicroIdx.SearchMetadataSize,
	}
	if segMicroIdx.BlockSummaries != nil {
		searchMeta.BlockSummaries = segMicroIdx.BlockSummaries
	}

	columnCopy := segMicroIdx.getColumns()
	finalReq := &structs.SegmentSearchRequest{
		SegmentKey:         file,
		VirtualTableName:   indexName,
		SearchMetadata:     searchMeta,
		AllPossibleColumns: columnCopy,
		LatestEpochMS:      segMicroIdx.LatestEpochMS,
		CmiPassedCnames:    blocksForFile,
	}
	blockInfo := make(map[uint16]*structs.BlockMetadataHolder)
	for blockNum := range blocksForFile {
		blockInfo[blockNum] = segMicroIdx.BlockSearchInfo[blockNum]
	}
	finalReq.AllBlocksToSearch = blockInfo
	return finalReq, nil
}

// TODO: function is getting to big and has many args, needs to be refactored
// Returns all search requests,  number of blocks checked, number of blocks passed, error
func RunCmiCheck(segkey string, tableName string, timeRange *dtu.TimeRange,
	blockTracker *structs.BlockTracker, bloomKeys map[string]bool, bloomOp utils.LogicalOperator,
	rangeFilter map[string]string, rangeOp utils.FilterOperator, isRange bool, wildCardValue bool,
	currQuery *structs.SearchQuery, colsToCheck map[string]bool, wildcardCol bool,
	qid uint64, isQueryPersistent bool, pqid string) (*structs.SegmentSearchRequest, uint64, uint64, error) {

	isMatchAll := currQuery.IsMatchAll()

	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()
	segMicroIndex, exists := globalMetadata.getMicroIndex(segkey)
	if !exists {
		log.Errorf("qid=%d, Segment file %+v for table %+v does not exist in block meta, but existed in time filtering. This should not happen", qid, segkey, tableName)
		return nil, 0, 0, fmt.Errorf("segment file %+v for table %+v does not exist in block meta, but existed in time filtering. This should not happen", segkey, tableName)
	}

	totalRequestedMemory := int64(0)
	if !segMicroIndex.loadedSearchMetadata {
		currSearchMetaSize := int64(segMicroIndex.SearchMetadataSize)
		totalRequestedMemory += currSearchMetaSize
		err := GlobalBlockMicroIndexCheckLimiter.TryAcquireWithBackoff(currSearchMetaSize, 10, segkey)
		if err != nil {
			log.Errorf("qid=%d, Failed to acquire memory from global pool for search! Error: %v", qid, err)
			return nil, 0, 0, fmt.Errorf("failed to acquire memory from global pool for search! Error: %v", err)
		}
		_, err = segMicroIndex.LoadSearchMetadata([]byte{})
		if err != nil {
			log.Errorf("qid=%d, Failed to load search metadata for segKey %+v! Error: %v", qid, segMicroIndex.SegmentKey, err)
			return nil, 0, 0, fmt.Errorf("failed to acquire memory from global pool for search! Error: %v", err)
		}
	}

	totalBlockCount := uint64(len(segMicroIndex.BlockSummaries))
	timeFilteredBlocks := metautils.FilterBlocksByTime(segMicroIndex.BlockSummaries, blockTracker, timeRange)
	numBlocks := uint16(len(segMicroIndex.BlockSummaries))
	droppedBlocksDueToTime := false
	if len(timeFilteredBlocks) < int(totalBlockCount) {
		droppedBlocksDueToTime = true
	}

	var missingBlockCMI bool
	if len(timeFilteredBlocks) > 0 && !isMatchAll && !segMicroIndex.loadedMicroIndices {
		totalRequestedMemory += int64(segMicroIndex.MicroIndexSize)
		err := GlobalBlockMicroIndexCheckLimiter.TryAcquireWithBackoff(int64(segMicroIndex.MicroIndexSize), 10, segkey)
		if err != nil {
			log.Errorf("qid=%d, Failed to acquire memory from global pool for search! Error: %v", qid, err)
			return nil, 0, 0, fmt.Errorf("failed to acquire memory from global pool for search! Error: %v", err)
		}
		blkCmis, err := segMicroIndex.readCmis(timeFilteredBlocks, false, colsToCheck, wildcardCol)
		if err != nil {
			log.Errorf("qid=%d, Failed to cmi for blocks and columns. Num blocks %+v, Num columns %+v. Error: %+v",
				qid, len(timeFilteredBlocks), len(colsToCheck), err)
			missingBlockCMI = true
		} else {
			segMicroIndex.blockCmis = blkCmis
		}
	}

	if !isMatchAll && !missingBlockCMI {
		for blockToCheck := range timeFilteredBlocks {
			if blockToCheck >= numBlocks {
				log.Errorf("qid=%d, Time range passed for a block with no micro index!", qid)
				continue
			}
			if isRange {
				if wildcardCol {
					doRangeCheckAllCol(segMicroIndex, blockToCheck, rangeFilter, rangeOp, timeFilteredBlocks, qid)
				} else {
					doRangeCheckForCol(segMicroIndex, blockToCheck, rangeFilter, rangeOp, timeFilteredBlocks, colsToCheck, qid)
				}
			} else {
				negateMatch := false
				if currQuery != nil && currQuery.MatchFilter != nil && currQuery.MatchFilter.NegateMatch {
					negateMatch = true
				}
				if !wildCardValue && !negateMatch {
					if wildcardCol {
						doBloomCheckAllCol(segMicroIndex, blockToCheck, bloomKeys, bloomOp, timeFilteredBlocks)
					} else {
						doBloomCheckForCol(segMicroIndex, blockToCheck, bloomKeys, bloomOp, timeFilteredBlocks, colsToCheck)
					}
				}
			}
		}
	}

	filteredBlockCount := uint64(0)
	var finalReq *structs.SegmentSearchRequest
	var err error

	if len(timeFilteredBlocks) == 0 && !droppedBlocksDueToTime {
		if isQueryPersistent {
			go pqsmeta.AddEmptyResults(pqid, segkey, tableName)
			go writer.BackFillPQSSegmetaEntry(segkey, pqid)
		}
	}

	if len(timeFilteredBlocks) > 0 {
		finalReq, err = convertBlocksToSearchRequest(timeFilteredBlocks, segkey, tableName, segMicroIndex)
		if err == nil {
			filteredBlockCount = uint64(len(timeFilteredBlocks))
		} else {
			log.Errorf("qid=%v, runCmiCheck: failed to convert blocks, err=%v", qid, err)
		}
	}

	if !segMicroIndex.loadedMicroIndices {
		segMicroIndex.clearMicroIndices()
	}
	if !segMicroIndex.loadedSearchMetadata {
		segMicroIndex.clearSearchMetadata()
	}
	if totalRequestedMemory > 0 {
		GlobalBlockMicroIndexCheckLimiter.Release(totalRequestedMemory)
	}
	return finalReq, totalBlockCount, filteredBlockCount, err
}

func doRangeCheckAllCol(segMicroIndex *SegmentMicroIndex, blockToCheck uint16, rangeFilter map[string]string,
	rangeOp utils.FilterOperator, timeFilteredBlocks map[uint16]map[string]bool, qid uint64) {

	allCMIs, err := segMicroIndex.GetCMIsForBlock(blockToCheck)
	if err != nil {
		return
	}
	matchedAny := false
	for cname, cmi := range allCMIs {
		var matchedBlockRange bool
		if cmi.CmiType != utils.CMI_RANGE_INDEX[0] {
			continue
		}
		matchedBlockRange = metautils.CheckRangeIndex(rangeFilter, cmi.Ranges, rangeOp, qid)
		if matchedBlockRange {
			timeFilteredBlocks[blockToCheck][cname] = true
			matchedAny = true
		}
	}
	if !matchedAny {
		delete(timeFilteredBlocks, blockToCheck)
	}
}

func doRangeCheckForCol(segMicroIndex *SegmentMicroIndex, blockToCheck uint16, rangeFilter map[string]string,
	rangeOp utils.FilterOperator, timeFilteredBlocks map[uint16]map[string]bool, colsToCheck map[string]bool, qid uint64) {

	var matchedBlockRange bool
	for colName := range colsToCheck {
		colCMI, err := segMicroIndex.GetCMIForBlockAndColumn(blockToCheck, colName)
		if err != nil {
			continue
		}
		if colCMI.CmiType != utils.CMI_RANGE_INDEX[0] {
			continue
		}
		matchedBlockRange = metautils.CheckRangeIndex(rangeFilter, colCMI.Ranges, rangeOp, qid)
		if matchedBlockRange {
			timeFilteredBlocks[blockToCheck][colName] = true
		} else {
			break
		}
	}
	if !matchedBlockRange {
		delete(timeFilteredBlocks, blockToCheck)
	}
}

func doBloomCheckForCol(segMicroIndex *SegmentMicroIndex, blockToCheck uint16, bloomKeys map[string]bool,
	bloomOp utils.LogicalOperator, timeFilteredBlocks map[uint16]map[string]bool, colsToCheck map[string]bool) {

	var matchedNeedleInBlock = true
	for entry := range bloomKeys {
		var needleExists bool
		for colName := range colsToCheck {
			colCMI, err := segMicroIndex.GetCMIForBlockAndColumn(blockToCheck, colName)
			if err != nil {
				continue
			}
			if colCMI.CmiType != utils.CMI_BLOOM_INDEX[0] {
				continue
			}
			needleExists = colCMI.Bf.TestString(entry)
			if needleExists {
				timeFilteredBlocks[blockToCheck][colName] = true
				break
			}
		}
		if !needleExists && bloomOp == utils.And {
			matchedNeedleInBlock = false
			break
		} else if needleExists && bloomOp == utils.Or {
			matchedNeedleInBlock = true
			break
		}
	}
	//If no match is found removing block from incoming blocksToCheck
	if !matchedNeedleInBlock {
		delete(timeFilteredBlocks, blockToCheck)
	}
}

func doBloomCheckAllCol(segMicroIndex *SegmentMicroIndex, blockToCheck uint16, bloomKeys map[string]bool,
	bloomOp utils.LogicalOperator, timeFilteredBlocks map[uint16]map[string]bool) {

	var matchedNeedleInBlock = true
	var allEntriesMissing bool = false
	for entry := range bloomKeys {
		var needleExists bool
		allCMIs, err := segMicroIndex.GetCMIsForBlock(blockToCheck)
		if err != nil {
			needleExists = false
		} else {
			atleastOneFound := false
			for cname, cmi := range allCMIs {
				if cmi.CmiType != utils.CMI_BLOOM_INDEX[0] {
					continue
				}
				if cmi.Bf.TestString(entry) {
					timeFilteredBlocks[blockToCheck][cname] = true
					atleastOneFound = true
				}
			}
			if atleastOneFound {
				needleExists = true
			}
		}
		if !needleExists && bloomOp == utils.And {
			matchedNeedleInBlock = false
			break
		} else if needleExists && bloomOp == utils.Or {
			allEntriesMissing = false
			matchedNeedleInBlock = true
			break
		} else if !needleExists && bloomOp == utils.Or {
			allEntriesMissing = true
			matchedNeedleInBlock = false
		}
	}

	// Or only early exits when it sees true. If all entries are false, we need to handle it here
	if bloomOp == segutils.Or && allEntriesMissing && !matchedNeedleInBlock {
		matchedNeedleInBlock = false
	}

	//If no match is found, removing block from incoming blocksToCheck
	if !matchedNeedleInBlock {
		delete(timeFilteredBlocks, blockToCheck)
	}
}

func GetBlockSearchInfoForKey(key string) (map[uint16]*structs.BlockMetadataHolder, error) {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()

	segmentMeta, ok := globalMetadata.getMicroIndex(key)
	if !ok {
		return nil, errors.New("failed to find key in all block micro")
	}

	if segmentMeta.loadedSearchMetadata {
		return segmentMeta.BlockSearchInfo, nil
	}

	_, _, allBmh, err := segmentMeta.readBlockSummaries([]byte{})
	if err != nil {
		log.Errorf("GetBlockSearchInfoForKey: failed to read column block sum infos for key %s: %v", key, err)
		return nil, err
	}

	return allBmh, nil
}

func GetBlockSummariesForKey(key string) ([]*structs.BlockSummary, error) {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()

	segmentMeta, ok := globalMetadata.getMicroIndex(key)
	if !ok {
		return nil, errors.New("failed to find key in all block micro")
	}

	if segmentMeta.loadedSearchMetadata {
		return segmentMeta.BlockSummaries, nil
	}

	_, blockSum, _, err := segmentMeta.readBlockSummaries([]byte{})
	if err != nil {
		log.Errorf("GetBlockSearchInfoForKey: failed to read column block infos for key %s: %v", key, err)
		return nil, err
	}
	return blockSum, nil
}

// returns block search info, block summaries, and any errors encountered
// block search info will be loaded for all possible columns
func GetSearchInfoForPQSQuery(key string, spqmr *pqmr.SegmentPQMRResults) (map[uint16]*structs.BlockMetadataHolder,
	[]*structs.BlockSummary, error) {
	globalMetadata.updateLock.RLock()
	defer globalMetadata.updateLock.RUnlock()

	segmentMeta, ok := globalMetadata.getMicroIndex(key)
	if !ok {
		return nil, nil, errors.New("failed to find key in all block micro")
	}

	if segmentMeta.loadedSearchMetadata {
		return segmentMeta.BlockSearchInfo, segmentMeta.BlockSummaries, nil
	}

	// avoid caller having to clean up BlockSearchInfo
	_, blockSum, allBmh, err := segmentMeta.readBlockSummaries([]byte{})
	if err != nil {
		log.Errorf("GetBlockSearchInfoForKey: failed to read block infos for segKey %+v: %v", key, err)
		return nil, nil, err
	}
	retSearchInfo := make(map[uint16]*structs.BlockMetadataHolder)
	setBlocks := spqmr.GetAllBlocks()
	for _, blkNum := range setBlocks {
		if blkMetadata, ok := allBmh[blkNum]; ok {
			retSearchInfo[blkNum] = blkMetadata
		}
	}
	return retSearchInfo, blockSum, nil
}
