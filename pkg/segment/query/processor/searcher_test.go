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

package processor

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

type timeRange struct {
	high, low uint64
}

func makeBlocksWithSummaryOnly(timeRanges []timeRange) []*block {
	blocks := make([]*block, len(timeRanges))
	for i, timeRange := range timeRanges {
		blocks[i] = &block{
			BlockSummary: &structs.BlockSummary{
				HighTs: timeRange.high,
				LowTs:  timeRange.low,
			},
		}
	}

	return blocks
}

func Test_sortBlocks(t *testing.T) {
	highAndLowTimestamps := []timeRange{
		{high: 100, low: 50},
		{high: 200, low: 200},
		{high: 300, low: 205},
		{high: 220, low: 80},
		{high: 120, low: 30},
	}

	// Sort most recent first.
	blocks := makeBlocksWithSummaryOnly(highAndLowTimestamps)
	sortBlocks(blocks, recentFirst)

	expectedBlocks := makeBlocksWithSummaryOnly([]timeRange{
		{high: 300, low: 205},
		{high: 220, low: 80},
		{high: 200, low: 200},
		{high: 120, low: 30},
		{high: 100, low: 50},
	})

	for i, block := range blocks {
		if block.HighTs != expectedBlocks[i].HighTs || block.LowTs != expectedBlocks[i].LowTs {
			t.Errorf("Expected %v, got %v for iter %v", expectedBlocks[i], block, i)
		}
	}

	// Sort most recent last.
	blocks = makeBlocksWithSummaryOnly(highAndLowTimestamps)
	sortBlocks(blocks, recentLast)

	expectedBlocks = makeBlocksWithSummaryOnly([]timeRange{
		{high: 120, low: 30},
		{high: 100, low: 50},
		{high: 220, low: 80},
		{high: 200, low: 200},
		{high: 300, low: 205},
	})

	for i, block := range blocks {
		if block.HighTs != expectedBlocks[i].HighTs || block.LowTs != expectedBlocks[i].LowTs {
			t.Errorf("Expected %v, got %v for iter %v", expectedBlocks[i], block, i)
		}
	}
}

func Test_getNextEndTime(t *testing.T) {
	blocksSortedHigh := makeBlocksWithSummaryOnly([]timeRange{
		{high: 40, low: 15},
		{high: 30, low: 25},
		{high: 20, low: 5},
		{high: 10, low: 8},
	})

	endTime := getNextEndTime(blocksSortedHigh, recentFirst)
	assert.Equal(t, uint64(15), endTime)

	blocksSortedLow := makeBlocksWithSummaryOnly([]timeRange{
		{high: 20, low: 5},
		{high: 10, low: 8},
		{high: 40, low: 15},
		{high: 30, low: 25},
	})

	endTime = getNextEndTime(blocksSortedLow, recentLast)
	assert.Equal(t, uint64(20), endTime)
}

func Test_getBlocksForTimeRange_recentFirst(t *testing.T) {
	blocksSortedHigh := makeBlocksWithSummaryOnly([]timeRange{
		{high: 40, low: 15},
		{high: 30, low: 25},
		{high: 20, low: 5},
		{high: 10, low: 8},
	})

	selectedBlocks, err := getBlocksForTimeRange(blocksSortedHigh, recentFirst, 25)
	assert.NoError(t, err)
	expectedBlocks := makeBlocksWithSummaryOnly([]timeRange{
		{high: 40, low: 15},
		{high: 30, low: 25},
	})

	assert.Equal(t, len(expectedBlocks), len(selectedBlocks))
	for i, block := range selectedBlocks {
		assert.Equal(t, expectedBlocks[i].HighTs, block.HighTs)
		assert.Equal(t, expectedBlocks[i].LowTs, block.LowTs)
	}
}

func Test_getBlocksForTimeRange_recentLast(t *testing.T) {
	blocksSortedLow := makeBlocksWithSummaryOnly([]timeRange{
		{high: 20, low: 5},
		{high: 10, low: 8},
		{high: 40, low: 15},
		{high: 30, low: 25},
	})

	selectedBlocks, err := getBlocksForTimeRange(blocksSortedLow, recentLast, 10)
	assert.NoError(t, err)
	expectedBlocks := makeBlocksWithSummaryOnly([]timeRange{
		{high: 20, low: 5},
		{high: 10, low: 8},
	})

	assert.Equal(t, len(expectedBlocks), len(selectedBlocks))
	for i, block := range selectedBlocks {
		assert.Equal(t, expectedBlocks[i].HighTs, block.HighTs)
		assert.Equal(t, expectedBlocks[i].LowTs, block.LowTs)
	}
}

func Test_getSSRs(t *testing.T) {
	blockMeta1 := &structs.BlockMetadataHolder{BlkNum: 1}
	blockMeta2 := &structs.BlockMetadataHolder{BlkNum: 2}
	blockMeta3 := &structs.BlockMetadataHolder{BlkNum: 3}

	ssr := &structs.SegmentSearchRequest{
		SegmentKey: "segKey",
		AllBlocksToSearch: map[uint16]*structs.BlockMetadataHolder{
			1: blockMeta1,
			2: blockMeta2,
			3: blockMeta3,
		},
		AllPossibleColumns: map[string]bool{
			"col1": true,
			"col2": true,
		},
	}

	blocks := []*block{
		{
			BlockMetadataHolder: blockMeta1,
			filename:            "file1",
			parentSSR:           ssr,
		},
		{
			BlockMetadataHolder: blockMeta2,
			filename:            "file1",
			parentSSR:           ssr,
		},
		{
			BlockMetadataHolder: blockMeta3,
			filename:            "file1",
			parentSSR:           ssr,
		},
	}

	allSegRequests, err := getSSRs(blocks)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(allSegRequests))
	assert.Equal(t, 3, len(allSegRequests["file1"].AllBlocksToSearch))
	assert.Equal(t, blockMeta1, allSegRequests["file1"].AllBlocksToSearch[1])
	assert.Equal(t, blockMeta2, allSegRequests["file1"].AllBlocksToSearch[2])
	assert.Equal(t, blockMeta3, allSegRequests["file1"].AllBlocksToSearch[3])

	// Test when blocks are from different files.
	blocks[0].filename = "file2"
	allSegRequests, err = getSSRs(blocks)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(allSegRequests))
	assert.Equal(t, 2, len(allSegRequests["file1"].AllBlocksToSearch))
	assert.Equal(t, 1, len(allSegRequests["file2"].AllBlocksToSearch))
	assert.Equal(t, blockMeta1, allSegRequests["file2"].AllBlocksToSearch[1])
	assert.Equal(t, blockMeta2, allSegRequests["file1"].AllBlocksToSearch[2])
	assert.Equal(t, blockMeta3, allSegRequests["file1"].AllBlocksToSearch[3])
	blocks[0].filename = "file1" // Reset for next test.

	// Test when blocks are from different SSRs.
	blocks[0].parentSSR = &structs.SegmentSearchRequest{}
	_, err = getSSRs(blocks)
	assert.Error(t, err)
	blocks[0].parentSSR = ssr // Reset for next test.

	// Test a subset of blocks.
	allSegRequests, err = getSSRs(blocks[:2])
	assert.NoError(t, err)
	assert.Equal(t, 1, len(allSegRequests))
	assert.Equal(t, 2, len(allSegRequests["file1"].AllBlocksToSearch))
	assert.Equal(t, blockMeta1, allSegRequests["file1"].AllBlocksToSearch[1])
	assert.Equal(t, blockMeta2, allSegRequests["file1"].AllBlocksToSearch[2])
}

func Test_getValidRRCs(t *testing.T) {
	rrcsSortedRecentFirst := []*segutils.RecordResultContainer{
		{TimeStamp: 40},
		{TimeStamp: 30},
		{TimeStamp: 20},
		{TimeStamp: 10},
	}

	actualRRCs := getValidRRCs(rrcsSortedRecentFirst, 25, recentFirst)
	assert.Equal(t, 2, len(actualRRCs))
	assert.Equal(t, uint64(40), actualRRCs[0].TimeStamp)
	assert.Equal(t, uint64(30), actualRRCs[1].TimeStamp)

	rrcsSortedRecentLast := []*segutils.RecordResultContainer{
		{TimeStamp: 10},
		{TimeStamp: 20},
		{TimeStamp: 30},
		{TimeStamp: 40},
	}

	actualRRCs = getValidRRCs(rrcsSortedRecentLast, 25, recentLast)
	assert.Equal(t, 2, len(actualRRCs))
	assert.Equal(t, uint64(10), actualRRCs[0].TimeStamp)
	assert.Equal(t, uint64(20), actualRRCs[1].TimeStamp)
}

func Test_getValidRRCs_boundaries(t *testing.T) {
	rrcsSortedRecentFirst := []*segutils.RecordResultContainer{
		{TimeStamp: 40},
		{TimeStamp: 30},
		{TimeStamp: 20},
		{TimeStamp: 10},
	}

	actualRRCs := getValidRRCs(rrcsSortedRecentFirst, 20, recentFirst)
	assert.Equal(t, 3, len(actualRRCs))
	assert.Equal(t, uint64(40), actualRRCs[0].TimeStamp)
	assert.Equal(t, uint64(30), actualRRCs[1].TimeStamp)
	assert.Equal(t, uint64(20), actualRRCs[2].TimeStamp)

	actualRRCs = getValidRRCs(rrcsSortedRecentFirst, 50, recentFirst)
	assert.Equal(t, 0, len(actualRRCs))

	actualRRCs = getValidRRCs(rrcsSortedRecentFirst, 0, recentFirst)
	assert.Equal(t, 4, len(actualRRCs))
	assert.Equal(t, uint64(40), actualRRCs[0].TimeStamp)
	assert.Equal(t, uint64(30), actualRRCs[1].TimeStamp)
	assert.Equal(t, uint64(20), actualRRCs[2].TimeStamp)
	assert.Equal(t, uint64(10), actualRRCs[3].TimeStamp)

	rrcsSortedRecentLast := []*segutils.RecordResultContainer{
		{TimeStamp: 10},
		{TimeStamp: 20},
		{TimeStamp: 30},
		{TimeStamp: 40},
	}

	actualRRCs = getValidRRCs(rrcsSortedRecentLast, 30, recentLast)
	assert.Equal(t, 3, len(actualRRCs))
	assert.Equal(t, uint64(10), actualRRCs[0].TimeStamp)
	assert.Equal(t, uint64(20), actualRRCs[1].TimeStamp)
	assert.Equal(t, uint64(30), actualRRCs[2].TimeStamp)

	actualRRCs = getValidRRCs(rrcsSortedRecentLast, 0, recentLast)
	assert.Equal(t, 0, len(actualRRCs))

	actualRRCs = getValidRRCs(rrcsSortedRecentLast, 50, recentLast)
	assert.Equal(t, 4, len(actualRRCs))
	assert.Equal(t, uint64(10), actualRRCs[0].TimeStamp)
	assert.Equal(t, uint64(20), actualRRCs[1].TimeStamp)
	assert.Equal(t, uint64(30), actualRRCs[2].TimeStamp)
	assert.Equal(t, uint64(40), actualRRCs[3].TimeStamp)
}
