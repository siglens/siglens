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
	"os"
	"testing"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

/*
TODO: more tests
test not first updates have different results depending on op
test combination of multiple update types
*/

func mockRecCountBlockSummaries(numBlocks uint16, numRecsPerBlock uint16) []*structs.BlockSummary {
	finalSums := make([]*structs.BlockSummary, numBlocks)
	for i := uint16(0); i < numBlocks; i++ {
		finalSums[i] = &structs.BlockSummary{
			RecCount: numRecsPerBlock,
		}
	}
	return finalSums
}

func mockSSR(numBlocks uint16) *structs.SegmentSearchRequest {
	bMeta := make(map[uint16]*structs.BlockMetadataHolder)
	for i := uint16(0); i < numBlocks; i++ {
		bMeta[i] = &structs.BlockMetadataHolder{}
	}
	return &structs.SegmentSearchRequest{
		AllBlocksToSearch: bMeta,
	}
}

func initMockSearchStatus(numBlocks uint16, numRecs uint16) *SegmentSearchStatus {
	bSum := mockRecCountBlockSummaries(numBlocks, numRecs)
	mockSSR := mockSSR(numBlocks)
	status := InitBlocksToSearch(mockSSR, bSum, &segresults.SearchResults{}, &dtu.TimeRange{StartEpochMs: 0, EndEpochMs: uint64(numRecs)})
	return status
}

func Test_InitSearchStatus(t *testing.T) {

	numBlocks := uint16(10)
	numRecs := uint16(10)
	bSum := mockRecCountBlockSummaries(numBlocks, numRecs)
	mockSSR := mockSSR(numBlocks)
	status := InitBlocksToSearch(mockSSR, bSum, &segresults.SearchResults{}, &dtu.TimeRange{StartEpochMs: 0, EndEpochMs: uint64(numRecs)})

	sumMatched, sumUnmatched := status.getTotalCounts()
	assert.Equal(t, uint64(numBlocks*numRecs), sumMatched, "expected=%v, actual=%v", numBlocks*numRecs, sumMatched)
	assert.Equal(t, sumUnmatched, uint64(0))

	bSearchHelper := structs.InitBlockSearchHelper()
	for i := uint16(0); i < numBlocks; i++ {
		bSearchHelper.ResetBlockHelper()
		for j := uint(0); j < uint(numRecs); j++ {
			bSearchHelper.AddMatchedRecord(j)
		}
		matchedRecs := bSearchHelper.GetAllMatchedRecords()
		assert.Equal(t, int(matchedRecs.GetNumberOfSetBits()), int(numRecs))
	}

	assert.Len(t, status.AllBlockStatus, int(numBlocks))
	for _, blkStatus := range status.AllBlockStatus {
		recITerator, err := blkStatus.GetRecordIteratorForBlock(utils.And)
		assert.Nil(t, err)
		assert.Equal(t, recITerator.AllRecLen, numRecs)
		for j := uint(0); j < uint(recITerator.AllRecLen); j++ {
			shoulProcess := recITerator.ShouldProcessRecord(j)
			assert.True(t, shoulProcess, j)
		}
	}
}

func Test_FirstSearchRecords(t *testing.T) {

	numBlocks := uint16(10)
	numRecs := uint16(10)
	status := initMockSearchStatus(numBlocks, numRecs)

	// initial and sets
	// type of search request should not influence the results for the first time
	for blkNum := uint16(0); blkNum < numBlocks; blkNum++ {
		recITerator, err := status.GetRecordIteratorForBlock(utils.And, blkNum)
		assert.Nil(t, err)
		assert.Equal(t, recITerator.AllRecLen, numRecs)
		for j := uint(0); j < uint(numRecs); j++ {
			readNum := recITerator.ShouldProcessRecord(j)
			assert.True(t, readNum)
		}

		recITerator, err = status.GetRecordIteratorForBlock(utils.Or, blkNum)
		assert.Nil(t, err)
		assert.Equal(t, recITerator.AllRecLen, numRecs)
		for j := uint(0); j < uint(numRecs); j++ {
			readNum := recITerator.ShouldProcessRecord(j)
			assert.True(t, readNum)
		}

		recITerator, err = status.GetRecordIteratorForBlock(utils.Exclusion, blkNum)
		assert.Nil(t, err)
		assert.Equal(t, recITerator.AllRecLen, numRecs)
		for j := uint(0); j < uint(numRecs); j++ {
			readNum := recITerator.ShouldProcessRecord(j)
			assert.True(t, readNum)
		}
	}
}

func Test_UpdateAndSearch(t *testing.T) {

	numBlocks := uint16(10)
	numRecs := uint16(10)
	status := initMockSearchStatus(numBlocks, numRecs)
	// test or/and/exclustion updates
	matched := pqmr.CreatePQMatchResults(1)
	matched.AddMatchedRecord(0)
	err := status.updateMatchedRecords(0, matched, utils.And)
	assert.Nil(t, err)
	recITerator, err := status.GetRecordIteratorForBlock(utils.And, 0)
	assert.Nil(t, err)
	processZero := recITerator.ShouldProcessRecord(0)
	assert.True(t, processZero)
	assert.Equal(t, recITerator.AllRecLen, numRecs)

	for i := uint(1); i < uint(recITerator.AllRecLen); i++ {
		process := recITerator.ShouldProcessRecord(i)
		assert.False(t, process)
	}

	blkStatus := status.AllBlockStatus[0]
	matchedRecs := blkStatus.allRecords.GetNumberOfSetBits()
	UnmatchedRecs := uint64(blkStatus.numRecords) - uint64(blkStatus.allRecords.GetNumberOfSetBits())
	assert.False(t, blkStatus.firstSearch)
	assert.Equal(t, blkStatus.numRecords, uint16(10))
	assert.Equal(t, matchedRecs, uint(1))
	assert.Equal(t, uint64(UnmatchedRecs), uint64(numRecs-1))
	log.Infof("block status after one and update %+v", blkStatus)
}

func Test_UpdateOrSearch(t *testing.T) {

	numBlocks := uint16(10)
	numRecs := uint16(10)
	status := initMockSearchStatus(numBlocks, numRecs)
	// test or/and/exclustion updates
	matched := pqmr.CreatePQMatchResults(1)
	matched.AddMatchedRecord(0)
	err := status.updateMatchedRecords(0, matched, utils.Or)
	assert.Nil(t, err)

	recITerator, err := status.GetRecordIteratorForBlock(utils.Or, 0)
	assert.Nil(t, err)
	assert.False(t, recITerator.firstSearch)
	orCount := 0

	process := recITerator.ShouldProcessRecord(0)
	assert.False(t, process)
	for i := uint(1); i < uint(recITerator.AllRecLen); i++ {
		process := recITerator.ShouldProcessRecord(i)
		assert.True(t, process)
		orCount++
	}
	assert.Equal(t, orCount, int(numRecs-1), "search all except recNum 0")

	blkStatus := status.AllBlockStatus[0]

	assert.False(t, blkStatus.firstSearch)
	assert.Equal(t, uint64(uint64(blkStatus.numRecords)-uint64(blkStatus.allRecords.GetNumberOfSetBits())), uint64(numRecs-1))
	assert.Equal(t, blkStatus.allRecords.GetNumberOfSetBits(), uint(1))

	moreOrMatch := pqmr.CreatePQMatchResults(2)
	moreOrMatch.AddMatchedRecord(1)
	moreOrMatch.AddMatchedRecord(2)

	err = status.updateMatchedRecords(0, moreOrMatch, utils.Or)
	assert.Nil(t, err)
	recITerator, err = status.GetRecordIteratorForBlock(utils.Or, 0)
	assert.Nil(t, err)
	assert.False(t, recITerator.firstSearch)
	assert.Equal(t, recITerator.AllRecLen, numRecs)

	for i := uint(0); i < uint(3); i++ {
		process := recITerator.ShouldProcessRecord(i)
		assert.False(t, process)
	}
	orCount = 0
	for i := uint(3); i < uint(numRecs); i++ {
		process := recITerator.ShouldProcessRecord(i)
		assert.True(t, process)
		orCount++
	}

	assert.Equal(t, orCount, int(numRecs-3), "search all except recNum 0,1,2")
	blkStatus = status.AllBlockStatus[0]
	matchedRecs := blkStatus.allRecords.GetNumberOfSetBits()
	unmatchedRecs := uint64(blkStatus.numRecords) - uint64(blkStatus.allRecords.GetNumberOfSetBits())
	assert.False(t, blkStatus.firstSearch)
	assert.Equal(t, uint64(unmatchedRecs), uint64(numRecs-3))
	assert.Equal(t, matchedRecs, uint(3), "matched 0,1,2")

	log.Infof("block status after one or update %+v", blkStatus)
}

func Test_UpdateExclusionSearch(t *testing.T) {

	numBlocks := uint16(10)
	numRecs := uint16(10)
	status := initMockSearchStatus(numBlocks, numRecs)
	// test or/and/exclustion updates
	matched := pqmr.CreatePQMatchResults(1)
	matched.AddMatchedRecord(0)
	err := status.updateMatchedRecords(0, matched, utils.Exclusion)
	assert.Nil(t, err)

	recITerator, err := status.GetRecordIteratorForBlock(utils.Exclusion, 0)
	assert.Nil(t, err)
	assert.False(t, recITerator.firstSearch)
	orCount := 0
	startTs := uint64(1)
	assert.Equal(t, recITerator.AllRecLen, numRecs)
	assert.False(t, recITerator.ShouldProcessRecord(0))

	for i := uint(1); i < uint(numRecs); i++ {
		readRec := recITerator.ShouldProcessRecord(i)
		log.Infof("iter next %+v %+v", i, readRec)
		assert.True(t, readRec, "i=%v", i)
		startTs++
		orCount++
	}
	assert.Equal(t, orCount, int(numRecs-1), "search all except recNum 0")

	blkStatus := status.AllBlockStatus[0]
	matchedRecs := blkStatus.allRecords.GetNumberOfSetBits()
	unmatchedRecs := uint64(blkStatus.numRecords) - uint64(blkStatus.allRecords.GetNumberOfSetBits())
	assert.False(t, blkStatus.firstSearch)
	assert.Equal(t, uint64(1), uint64(unmatchedRecs), "expected=1, unmatched=%v", unmatchedRecs)
	assert.Equal(t, matchedRecs, uint(numRecs-1))
	log.Infof("block status after one exclusion update %+v", blkStatus)
}

func Test_ReadAndWritePqmrFilesEncode(t *testing.T) {
	fname := "pqmr_encode.pqmr"
	os.Remove(fname)
	numBlocks := uint16(400)
	numRecs := uint(20_000)
	reqLen := uint64(0)
	pbset := pqmr.CreatePQMatchResults(14000)

	for recNum := uint(0); recNum < numRecs; recNum++ {
		if recNum%3 == 0 {
			pbset.AddMatchedRecord(recNum)
		}
	}

	for i := uint16(0); i < numBlocks; i++ {
		// Adding 2 bytes for blockNum and 2 bytes for blockLen
		size := 4 + pbset.GetInMemSize()
		reqLen += size
	}

	buf := make([]byte, reqLen)
	var idx uint32

	for i := uint16(0); i < numBlocks; i++ {
		packedLen, err := pbset.EncodePqmr(buf[idx:], i)
		assert.Equal(t, nil, err)
		idx += uint32(packedLen)
	}

	err := pqmr.WritePqmrToDisk(buf[0:idx], fname)
	assert.Nil(t, err)

	res, err := pqmr.ReadPqmr(&fname)
	assert.Nil(t, err)
	assert.NotNil(t, res)
	assert.Equal(t, numBlocks, res.GetNumBlocks())
	os.Remove(fname)
}
