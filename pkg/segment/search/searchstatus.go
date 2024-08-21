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

package search

import (
	"errors"
	"sync"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

const PQMR_INITIAL_SIZE = 15000

// Do not modify this variable, use it only for cloning only
var pqmrAllMatchedConst *pqmr.PQMatchResults

// a helper struct to keep track of which records & blocks need to be searched
type SegmentSearchStatus struct {
	AllBlockStatus     map[uint16]*BlockSearchStatus
	numBlocksToSearch  uint16 // number of blocks to raw search (passed bloom & block time range check)
	numBlocksInSegFile uint16 // number of blocks in segment file
}

type BlockSearchStatus struct {
	allRecords    *pqmr.PQMatchResults // allrecords in block
	BlockNum      uint16               // block number of search
	numRecords    uint16               // number of records
	blockLock     *sync.RWMutex        // lock for reading/editing records
	firstSearch   bool                 // has allRecords been updated?
	hasAnyMatched bool                 // if any bit is set
}

type BlockRecordIterator struct {
	firstSearch bool // bool if first iterator
	op          utils.LogicalOperator
	AllRecords  *pqmr.PQMatchResults // allrecords in block
	AllRecLen   uint16
}

func init() {
	pqmrAllMatchedConst = pqmr.CreatePQMatchResults(PQMR_INITIAL_SIZE)
	for j := uint(0); j < PQMR_INITIAL_SIZE; j++ {
		pqmrAllMatchedConst.AddMatchedRecord(j)
	}
}

// Inits blocks & records to search based on input blkSum and tRange.
// We will generously raw search all records in a block with a HighTS and LowTs inside tRange
// It is up to the caller to call .Close()
func InitBlocksToSearch(searchReq *structs.SegmentSearchRequest, blkSum []*structs.BlockSummary, allSearchResults *segresults.SearchResults, tRange *dtu.TimeRange) *SegmentSearchStatus {

	allBlocks := make(map[uint16]*BlockSearchStatus, len(blkSum))

	blocksToSearch := uint16(0)
	for i, bSum := range blkSum {

		if tRange.CheckRangeOverLap(bSum.LowTs, bSum.HighTs) {
			currBlk := uint16(i)

			if _, shouldSearch := searchReq.AllBlocksToSearch[currBlk]; !shouldSearch {
				continue
			}

			if !allSearchResults.ShouldSearchRange(bSum.LowTs, bSum.HighTs) {
				allSearchResults.SetEarlyExit(true)
				continue
			}

			// Using clone method to set all the bits at once, instead of looping through to set each bit.
			passedRecs := pqmr.Clone(pqmrAllMatchedConst)

			// Resizing based on the recCount
			if bSum.RecCount > PQMR_INITIAL_SIZE {
				for j := uint(PQMR_INITIAL_SIZE); j < uint(bSum.RecCount); j++ {
					passedRecs.AddMatchedRecord(j)
				}
			} else {
				for j := uint(bSum.RecCount); j < PQMR_INITIAL_SIZE; j++ {
					passedRecs.ClearBit(j)
				}
			}

			allBlocks[currBlk] = &BlockSearchStatus{
				BlockNum:      currBlk,
				allRecords:    passedRecs,
				numRecords:    bSum.RecCount,
				blockLock:     &sync.RWMutex{},
				firstSearch:   true,
				hasAnyMatched: true,
			}
			blocksToSearch++
		}
	}

	return &SegmentSearchStatus{
		AllBlockStatus:     allBlocks,
		numBlocksToSearch:  blocksToSearch,
		numBlocksInSegFile: uint16(len(blkSum)),
	}
}

func (sss *SegmentSearchStatus) getTotalCounts() (uint64, uint64) {

	totalMatched := uint64(0)
	totalUnmatched := uint64(0)
	blkMatchedCount := uint64(0)
	blkUnmatchedCount := uint64(0)

	for _, blkStatus := range sss.AllBlockStatus {
		blkMatchedCount = uint64(blkStatus.allRecords.GetNumberOfSetBits())
		blkUnmatchedCount = uint64(blkStatus.numRecords) - blkMatchedCount
		totalMatched += blkMatchedCount
		totalUnmatched += blkUnmatchedCount
	}

	return totalMatched, totalUnmatched
}

func (sss *SegmentSearchStatus) Close() {
}

// if op == Or return allUnmatchedRecords
// if op == And return allMatchedRecords
// if op == Exclusion return allMatchedRecords
// if this is the first call, then return allMatchedRecords regardless (will be time filtered)
func (sss *SegmentSearchStatus) GetRecordIteratorForBlock(op utils.LogicalOperator, blkNum uint16) (*BlockRecordIterator, error) {

	blkStatus, ok := sss.AllBlockStatus[blkNum]
	if !ok {
		log.Errorf("AddTimeFilteredRecordToBlock: tried to add a record to a block that does not exist %+v", blkNum)
		return nil, errors.New("block does not exist in segment")
	}

	return blkStatus.GetRecordIteratorForBlock(op)
}

func (sss *SegmentSearchStatus) updateMatchedRecords(blkNum uint16, matchedRecs *pqmr.PQMatchResults, op utils.LogicalOperator) error {

	blkStatus, ok := sss.AllBlockStatus[blkNum]
	if !ok {
		log.Warnf("updateAndMatchedRecords: block %d does not exist in allBlockStatus!", blkNum)
		return errors.New("block does not exist in sss.allBlockStatus")
	}
	switch op {
	case utils.And:
		// new blkRecs.allMatchedRecords ==  intersection of matchedRecs and blkRecs.allMatchedRecords
		// for elements removed from blkRecs.allMatchedRecords, add to blkRecs.allUnmatchedRecords
		blkStatus.intersectMatchedRecords(matchedRecs)
	case utils.Or:
		// add all new recordNums to  sss.allBlockStatus.allMatchedRecords
		// for newly added recordNums, remove it from sss.allBlockStatus.allUnmatchedRecords
		if blkStatus.firstSearch {
			blkStatus.intersectMatchedRecords(matchedRecs)
		} else {
			blkStatus.unionMatchedRecords(matchedRecs)
		}
	case utils.Exclusion:
		// remove all recIdx from blkRecs.allMatchedRecords that exist in matchedRecs
		// for removed elements from blkRecs.allMatchedRecords, add to blkRecs.allUnmatchedRecord
		blkStatus.excludeMatchedRecords(matchedRecs)
	}
	blkStatus.firstSearch = false
	return nil
}

func (bss *BlockSearchStatus) intersectMatchedRecords(matchedRecs *pqmr.PQMatchResults) {

	bss.blockLock.Lock()
	bss.allRecords.InPlaceIntersection(matchedRecs)

	bss.hasAnyMatched = bss.allRecords.Any()

	bss.blockLock.Unlock()
}

func (bss *BlockSearchStatus) unionMatchedRecords(matchedRecs *pqmr.PQMatchResults) {

	bss.blockLock.Lock()
	bss.allRecords.InPlaceUnion(matchedRecs)

	bss.hasAnyMatched = bss.allRecords.Any()

	bss.blockLock.Unlock()
}

func (bss *BlockSearchStatus) excludeMatchedRecords(matchedRecs *pqmr.PQMatchResults) {

	bss.blockLock.Lock()
	for i := uint(0); i < uint(int(matchedRecs.GetNumberOfBits())); i++ {
		if matchedRecs.DoesRecordMatch(i) {
			if bss.allRecords.DoesRecordMatch(i) {
				bss.allRecords.ClearBit(i)
			}
		}
	}
	bss.hasAnyMatched = bss.allRecords.Any()
	bss.blockLock.Unlock()
}

func (bss *BlockSearchStatus) GetRecordIteratorForBlock(op utils.LogicalOperator) (*BlockRecordIterator, error) {
	return &BlockRecordIterator{
		firstSearch: bss.firstSearch,
		op:          op,
		AllRecords:  bss.allRecords,
		AllRecLen:   bss.numRecords,
	}, nil
}

// returns a copy of the block iterator. This should be called in during time range filtering to avoid PQMR backfilling time filtered records
func (bss *BlockSearchStatus) GetRecordIteratorCopyForBlock(op utils.LogicalOperator) (*BlockRecordIterator, error) {
	return &BlockRecordIterator{
		firstSearch: bss.firstSearch,
		op:          op,
		AllRecords:  bss.allRecords.Copy(),
		AllRecLen:   bss.numRecords,
	}, nil
}

func (bss *BlockRecordIterator) ShouldProcessRecord(idx uint) bool {
	if idx >= uint(bss.AllRecLen) {
		return false
	}
	if bss.firstSearch || bss.op == utils.And || bss.op == utils.Exclusion {
		if bss.AllRecords.DoesRecordMatch(idx) {
			return true
		}
	} else if bss.op == utils.Or {
		if !bss.AllRecords.DoesRecordMatch(idx) {
			return true
		}
	}
	return false
}

// set idx bit to 0. This function can be used to remove records that dont match timestamps
func (bss *BlockRecordIterator) UnsetRecord(idx uint) {
	if idx >= uint(bss.AllRecLen) {
		return
	}
	bss.AllRecords.ClearBit(idx)
}

// Inits blocks for aggs on input blkSum
func InitBlocksForAggsFastPath(searchReq *structs.SegmentSearchRequest,
	blkSum []*structs.BlockSummary) *SegmentSearchStatus {

	allBlocks := make(map[uint16]*BlockSearchStatus, len(searchReq.AllBlocksToSearch))

	for blkNum := range searchReq.AllBlocksToSearch {

		bSum := blkSum[blkNum]

		allBlocks[blkNum] = &BlockSearchStatus{
			BlockNum:      blkNum,
			numRecords:    bSum.RecCount,
			blockLock:     &sync.RWMutex{},
			firstSearch:   true,
			hasAnyMatched: true,
		}
	}

	return &SegmentSearchStatus{
		AllBlockStatus:     allBlocks,
		numBlocksToSearch:  uint16(len(searchReq.AllBlocksToSearch)),
		numBlocksInSegFile: uint16(len(blkSum)),
	}
}

// for fastpath, the matchedRec bitset is not used
// matchedcount is always equal to numrecs in each of the blocksearchstatus
func (sss *SegmentSearchStatus) getTotalCountsFastPath() uint64 {

	totalMatched := uint64(0)

	for _, blkStatus := range sss.AllBlockStatus {
		totalMatched += uint64(blkStatus.numRecords)
	}

	return totalMatched
}

func InitIteratorFromPQMR(pqmr *pqmr.PQMatchResults, nRecs uint) *BlockRecordIterator {
	return &BlockRecordIterator{
		firstSearch: true,
		op:          utils.And,
		AllRecords:  pqmr,
		AllRecLen:   uint16(nRecs),
	}
}
