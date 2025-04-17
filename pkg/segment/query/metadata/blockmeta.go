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

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/query/metadata/metautils"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// converts blocks to a search request. block summaries & column meta are not guaranteed to be in memory
// if the block summaries & column meta are not in memory, then load right before query
func convertBlocksToSearchRequest(blocksForFile map[uint16]map[string]bool, file string, indexName string,
	segMicroIdx *metadata.SegmentMicroIndex) (*structs.SegmentSearchRequest, error) {

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

	columnCopy := segMicroIdx.GetColumns()
	finalReq := &structs.SegmentSearchRequest{
		SegmentKey:         file,
		VirtualTableName:   indexName,
		SearchMetadata:     searchMeta,
		AllPossibleColumns: columnCopy,
		LatestEpochMS:      segMicroIdx.LatestEpochMS,
		CmiPassedCnames:    blocksForFile,
	}

	finalReq.AllBlocksToSearch = toputils.MapToSet(blocksForFile)
	return finalReq, nil
}

// TODO: function has many args, needs to be refactored
// Returns all search requests,  number of blocks checked, number of blocks passed, error
func RunCmiCheck(segkey string, tableName string, timeRange *dtu.TimeRange,
	blockTracker *structs.BlockTracker, bloomKeys map[string]bool,
	originalBloomKeys map[string]string, bloomOp utils.LogicalOperator,
	rangeFilter map[string]string, rangeOp utils.FilterOperator, isRange bool,
	wildCardValue bool, currQuery *structs.SearchQuery,
	colsToCheck map[string]bool, wildcardCol bool,
	qid uint64, isQueryPersistent bool, pqid string,
	dualCaseCheckEnabled bool) (*structs.SegmentSearchRequest, uint64, uint64, error) {

	isMatchAll := currQuery.IsMatchAll()

	smi, err := metadata.GetLoadSsm(segkey, qid)
	if err != nil {
		return nil, 0, 0, err
	}

	smi.RLockSmi()

	totalBlockCount := uint64(len(smi.BlockSearchInfo.AllBmh))
	timeFilteredBlocks := metautils.FilterBlocksByTime(smi.BlockSummaries, blockTracker, timeRange)

	var missingBlockCMI bool
	if len(timeFilteredBlocks) > 0 && !isMatchAll && !wildCardValue {
		smi.RUnlockSmi() // release the read lock so that we can load it and it needs write access
		missingBlockCMI, err = smi.LoadCmiForSearchTime(segkey, timeFilteredBlocks, colsToCheck,
			wildcardCol, qid)
		if err != nil {
			return nil, 0, 0, err
		}
		smi.RLockSmi() // re-acquire read since it will be needed below
	}

	// TODO : we keep the cmis in mem so that the next search could use it, however
	// if an expensive search comes in, we should check here the "allowed" mem for cmi
	// and then ask the rebalance loop to release/evict some

	if !isMatchAll && !missingBlockCMI {
		doCmiChecks(smi, timeFilteredBlocks, qid, rangeFilter, rangeOp, colsToCheck,
			currQuery, isRange, wildcardCol, wildCardValue, bloomKeys, originalBloomKeys,
			bloomOp, dualCaseCheckEnabled)
	}

	filteredBlockCount := uint64(0)
	var finalReq *structs.SegmentSearchRequest

	if len(timeFilteredBlocks) > 0 {
		finalReq, err = convertBlocksToSearchRequest(timeFilteredBlocks, segkey, tableName, smi)
		if err == nil {
			filteredBlockCount = uint64(len(timeFilteredBlocks))
		} else {
			log.Errorf("qid=%v, runCmiCheck: failed to convert blocks, err=%v", qid, err)
		}
	}

	smi.RUnlockSmi()

	return finalReq, totalBlockCount, filteredBlockCount, err
}

func doCmiChecks(smi *metadata.SegmentMicroIndex, timeFilteredBlocks map[uint16]map[string]bool,
	qid uint64, rangeFilter map[string]string, rangeOp utils.FilterOperator,
	colsToCheck map[string]bool,
	currQuery *structs.SearchQuery, isRange bool, wildcardCol bool,
	wildCardValue bool, bloomKeys map[string]bool, originalBloomKeys map[string]string,
	bloomOp utils.LogicalOperator, dualCaseCheckEnabled bool) {

	for blockToCheck := range timeFilteredBlocks {
		if blockToCheck >= uint16(len(smi.BlockSummaries)) {
			log.Errorf("qid=%d, Time range passed for a block with no micro index!", qid)
			continue
		}

		if isRange {
			if wildcardCol {
				doRangeCheckAllCol(smi, blockToCheck, rangeFilter, rangeOp, timeFilteredBlocks, qid)
			} else {
				doRangeCheckForCol(smi, blockToCheck, rangeFilter, rangeOp, timeFilteredBlocks, colsToCheck, qid)
			}
		} else {
			negateMatch := false
			if currQuery != nil && currQuery.MatchFilter != nil && currQuery.MatchFilter.NegateMatch {
				negateMatch = true
			}
			if !wildCardValue && !negateMatch {
				if wildcardCol {
					doBloomCheckAllCol(smi, blockToCheck, bloomKeys, originalBloomKeys, bloomOp, timeFilteredBlocks, dualCaseCheckEnabled, qid)
				} else {
					doBloomCheckForCol(smi, blockToCheck, bloomKeys, originalBloomKeys, bloomOp, timeFilteredBlocks, colsToCheck, dualCaseCheckEnabled, qid)
				}
			}
		}
	}
}

func doRangeCheckAllCol(segMicroIndex *metadata.SegmentMicroIndex, blockToCheck uint16, rangeFilter map[string]string,
	rangeOp utils.FilterOperator, timeFilteredBlocks map[uint16]map[string]bool, qid uint64) {

	allCMIs, err := segMicroIndex.GetCMIsForBlock(blockToCheck, qid)
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

func doRangeCheckForCol(segMicroIndex *metadata.SegmentMicroIndex, blockToCheck uint16, rangeFilter map[string]string,
	rangeOp utils.FilterOperator, timeFilteredBlocks map[uint16]map[string]bool, colsToCheck map[string]bool, qid uint64) {

	var matchedBlockRange bool
	for colName := range colsToCheck {
		colCMI, err := segMicroIndex.GetCMIForBlockAndColumn(blockToCheck, colName, qid)
		if err == metadata.ErrCMIColNotFound && rangeOp == utils.NotEquals {
			matchedBlockRange = true
			timeFilteredBlocks[blockToCheck][colName] = true
			continue
		}
		if err != nil {
			log.Errorf("doRangeCheckForCol: failed to get cmi for block %d and column %s: %v", blockToCheck, colName, err)
			continue
		}
		if colCMI.CmiType != utils.CMI_RANGE_INDEX[0] {
			if rangeOp == utils.NotEquals {
				matchedBlockRange = true
				timeFilteredBlocks[blockToCheck][colName] = true
			}
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

func doBloomCheckForCol(segMicroIndex *metadata.SegmentMicroIndex, blockToCheck uint16, bloomKeys map[string]bool, originalBloomKeys map[string]string,
	bloomOp utils.LogicalOperator, timeFilteredBlocks map[uint16]map[string]bool,
	colsToCheck map[string]bool, dualCaseEnabled bool, qid uint64) {

	checkInOriginalKeys := dualCaseEnabled && len(originalBloomKeys) > 0

	var matchedNeedleInBlock = true
	for entry := range bloomKeys {
		var needleExists bool
		for colName := range colsToCheck {
			colCMI, err := segMicroIndex.GetCMIForBlockAndColumn(blockToCheck, colName, qid)
			if err != nil {
				continue
			}
			if colCMI.CmiType != utils.CMI_BLOOM_INDEX[0] {
				continue
			}
			needleExists = colCMI.Bf.TestString(entry)
			if !needleExists && checkInOriginalKeys {
				originalEntry, ok := originalBloomKeys[entry]
				if ok {
					needleExists = colCMI.Bf.TestString(originalEntry)
				}
			}
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

func doBloomCheckAllCol(segMicroIndex *metadata.SegmentMicroIndex, blockToCheck uint16, bloomKeys map[string]bool, originalBloomKeys map[string]string,
	bloomOp utils.LogicalOperator, timeFilteredBlocks map[uint16]map[string]bool,
	dualCaseCheckEnabled bool, qid uint64) {

	checkInOriginalKeys := dualCaseCheckEnabled && len(originalBloomKeys) > 0

	var matchedNeedleInBlock = true
	var allEntriesMissing bool = false
	for entry := range bloomKeys {
		var needleExists bool
		allCMIs, err := segMicroIndex.GetCMIsForBlock(blockToCheck, qid)
		if err != nil {
			needleExists = false
		} else {
			atleastOneFound := false
			for cname, cmi := range allCMIs {
				if cmi.CmiType != utils.CMI_BLOOM_INDEX[0] {
					continue
				}
				entryExists := cmi.Bf.TestString(entry)
				if !entryExists && checkInOriginalKeys {
					originalEntry, ok := originalBloomKeys[entry]
					if ok {
						entryExists = cmi.Bf.TestString(originalEntry)
					}
				}

				if entryExists {
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
