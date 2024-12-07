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

	"github.com/siglens/siglens/pkg/segment/query"
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
	err := sortBlocks(blocks, recentFirst)
	assert.NoError(t, err)

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
	err = sortBlocks(blocks, recentLast)
	assert.NoError(t, err)

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

func Test_sortRRCs(t *testing.T) {
	rrcs := []*segutils.RecordResultContainer{
		{TimeStamp: 3},
		{TimeStamp: 1},
		{TimeStamp: 2},
	}

	err := sortRRCs(rrcs, recentFirst)
	assert.NoError(t, err)
	assert.Equal(t, rrcs, []*segutils.RecordResultContainer{
		{TimeStamp: 3},
		{TimeStamp: 2},
		{TimeStamp: 1},
	})

	rrcs = []*segutils.RecordResultContainer{
		{TimeStamp: 3},
		{TimeStamp: 1},
		{TimeStamp: 2},
	}

	err = sortRRCs(rrcs, recentLast)
	assert.NoError(t, err)
	assert.Equal(t, rrcs, []*segutils.RecordResultContainer{
		{TimeStamp: 1},
		{TimeStamp: 2},
		{TimeStamp: 3},
	})
}

func Test_getNextBlocks_exceedsMaxDesired(t *testing.T) {
	blocksSortedHigh := makeBlocksWithSummaryOnly([]timeRange{
		{high: 40, low: 20},
		{high: 40, low: 15},
		{high: 40, low: 25},
		{high: 30, low: 10},
	})

	desiredMaxBlocks := 1
	blocks, endTime, err := getNextBlocks(blocksSortedHigh, desiredMaxBlocks, recentFirst)
	assert.NoError(t, err)
	assert.Equal(t, uint64(30), endTime)
	assert.Equal(t, 3, len(blocks))
	assert.Equal(t, uint64(40), blocks[0].HighTs)
	assert.Equal(t, uint64(20), blocks[0].LowTs)
	assert.Equal(t, uint64(40), blocks[1].HighTs)
	assert.Equal(t, uint64(15), blocks[1].LowTs)
	assert.Equal(t, uint64(40), blocks[2].HighTs)
	assert.Equal(t, uint64(25), blocks[2].LowTs)
}

func Test_getNextBlocks_lessThanMaxDesired(t *testing.T) {
	blocksSortedHigh := makeBlocksWithSummaryOnly([]timeRange{
		{high: 40, low: 20},
		{high: 30, low: 15},
		{high: 30, low: 25},
		{high: 20, low: 10},
	})

	// Since taking the second block would require taking the third, only one
	// block can be taken.
	desiredMaxBlocks := 2
	blocks, endTime, err := getNextBlocks(blocksSortedHigh, desiredMaxBlocks, recentFirst)
	assert.NoError(t, err)
	assert.Equal(t, uint64(30), endTime)
	assert.Equal(t, 1, len(blocks))
	assert.Equal(t, uint64(40), blocks[0].HighTs)
}

func Test_getNextBlocks_recentFirst(t *testing.T) {
	blocksSortedHigh := makeBlocksWithSummaryOnly([]timeRange{
		{high: 40, low: 15},
		{high: 30, low: 25},
		{high: 20, low: 5},
		{high: 10, low: 8},
	})

	desiredMaxBlocks := 1
	blocks, endTime, err := getNextBlocks(blocksSortedHigh, desiredMaxBlocks, recentFirst)
	assert.NoError(t, err)
	assert.Equal(t, uint64(30), endTime)
	assert.Equal(t, 1, len(blocks))
	assert.Equal(t, uint64(40), blocks[0].HighTs)

	desiredMaxBlocks = 2
	blocks, endTime, err = getNextBlocks(blocksSortedHigh, desiredMaxBlocks, recentFirst)
	assert.NoError(t, err)
	assert.Equal(t, uint64(20), endTime)
	assert.Equal(t, 2, len(blocks))
	assert.Equal(t, uint64(40), blocks[0].HighTs)
	assert.Equal(t, uint64(30), blocks[1].HighTs)

	desiredMaxBlocks = 10 // More than the number of blocks.
	blocks, endTime, err = getNextBlocks(blocksSortedHigh, desiredMaxBlocks, recentFirst)
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), endTime)
	assert.Equal(t, 4, len(blocks))
	assert.Equal(t, uint64(40), blocks[0].HighTs)
	assert.Equal(t, uint64(30), blocks[1].HighTs)
	assert.Equal(t, uint64(20), blocks[2].HighTs)
	assert.Equal(t, uint64(10), blocks[3].HighTs)
}

func Test_getNextBlocks_recentLast(t *testing.T) {
	blocksSortedLow := makeBlocksWithSummaryOnly([]timeRange{
		{high: 20, low: 5},
		{high: 10, low: 8},
		{high: 40, low: 15},
		{high: 30, low: 25},
	})

	desiredMaxBlocks := 1
	blocks, endTime, err := getNextBlocks(blocksSortedLow, desiredMaxBlocks, recentLast)
	assert.NoError(t, err)
	assert.Equal(t, uint64(8), endTime)
	assert.Equal(t, 1, len(blocks))
	assert.Equal(t, uint64(5), blocks[0].LowTs)

	desiredMaxBlocks = 2
	blocks, endTime, err = getNextBlocks(blocksSortedLow, desiredMaxBlocks, recentLast)
	assert.NoError(t, err)
	assert.Equal(t, uint64(15), endTime)
	assert.Equal(t, 2, len(blocks))
	assert.Equal(t, uint64(5), blocks[0].LowTs)
	assert.Equal(t, uint64(8), blocks[1].LowTs)

	desiredMaxBlocks = 10 // More than the number of blocks.
	blocks, endTime, err = getNextBlocks(blocksSortedLow, desiredMaxBlocks, recentLast)
	assert.NoError(t, err)
	assert.Equal(t, uint64(40), endTime)
	assert.Equal(t, 4, len(blocks))
	assert.Equal(t, uint64(5), blocks[0].LowTs)
	assert.Equal(t, uint64(8), blocks[1].LowTs)
	assert.Equal(t, uint64(15), blocks[2].LowTs)
	assert.Equal(t, uint64(25), blocks[3].LowTs)
}

func Test_getNextBlocks_anyOrder(t *testing.T) {
	allBlocks := makeBlocksWithSummaryOnly([]timeRange{
		{high: 20, low: 5},
		{high: 30, low: 25},
		{high: 10, low: 8},
		{high: 40, low: 15},
	})

	desiredMaxBlocks := 1
	blocks, _, err := getNextBlocks(allBlocks, desiredMaxBlocks, anyOrder)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(blocks))
	assert.Equal(t, uint64(20), blocks[0].HighTs)

	desiredMaxBlocks = 4
	blocks, _, err = getNextBlocks(allBlocks, desiredMaxBlocks, anyOrder)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(blocks))
	assert.Equal(t, uint64(20), blocks[0].HighTs)
	assert.Equal(t, uint64(30), blocks[1].HighTs)
	assert.Equal(t, uint64(10), blocks[2].HighTs)
	assert.Equal(t, uint64(40), blocks[3].HighTs)

	desiredMaxBlocks = 10 // More than the number of blocks.
	blocks, _, err = getNextBlocks(allBlocks, desiredMaxBlocks, anyOrder)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(blocks))
	assert.Equal(t, uint64(20), blocks[0].HighTs)
	assert.Equal(t, uint64(30), blocks[1].HighTs)
	assert.Equal(t, uint64(10), blocks[2].HighTs)
	assert.Equal(t, uint64(40), blocks[3].HighTs)
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
			segkeyFname:         "file1",
			parentSSR:           ssr,
		},
		{
			BlockMetadataHolder: blockMeta2,
			segkeyFname:         "file1",
			parentSSR:           ssr,
		},
		{
			BlockMetadataHolder: blockMeta3,
			segkeyFname:         "file1",
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
	blocks[0].segkeyFname = "file2"
	allSegRequests, err = getSSRs(blocks)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(allSegRequests))
	assert.Equal(t, 2, len(allSegRequests["file1"].AllBlocksToSearch))
	assert.Equal(t, 1, len(allSegRequests["file2"].AllBlocksToSearch))
	assert.Equal(t, blockMeta1, allSegRequests["file2"].AllBlocksToSearch[1])
	assert.Equal(t, blockMeta2, allSegRequests["file1"].AllBlocksToSearch[2])
	assert.Equal(t, blockMeta3, allSegRequests["file1"].AllBlocksToSearch[3])
	blocks[0].segkeyFname = "file1" // Reset for next test.

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

	actualRRCs, err := getValidRRCs(rrcsSortedRecentFirst, 25, recentFirst)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(actualRRCs))
	assert.Equal(t, uint64(40), actualRRCs[0].TimeStamp)
	assert.Equal(t, uint64(30), actualRRCs[1].TimeStamp)

	rrcsSortedRecentLast := []*segutils.RecordResultContainer{
		{TimeStamp: 10},
		{TimeStamp: 20},
		{TimeStamp: 30},
		{TimeStamp: 40},
	}

	actualRRCs, err = getValidRRCs(rrcsSortedRecentLast, 25, recentLast)
	assert.NoError(t, err)
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

	actualRRCs, err := getValidRRCs(rrcsSortedRecentFirst, 20, recentFirst)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(actualRRCs))
	assert.Equal(t, uint64(40), actualRRCs[0].TimeStamp)
	assert.Equal(t, uint64(30), actualRRCs[1].TimeStamp)
	assert.Equal(t, uint64(20), actualRRCs[2].TimeStamp)

	actualRRCs, err = getValidRRCs(rrcsSortedRecentFirst, 50, recentFirst)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(actualRRCs))

	actualRRCs, err = getValidRRCs(rrcsSortedRecentFirst, 0, recentFirst)
	assert.NoError(t, err)
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

	actualRRCs, err = getValidRRCs(rrcsSortedRecentLast, 30, recentLast)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(actualRRCs))
	assert.Equal(t, uint64(10), actualRRCs[0].TimeStamp)
	assert.Equal(t, uint64(20), actualRRCs[1].TimeStamp)
	assert.Equal(t, uint64(30), actualRRCs[2].TimeStamp)

	actualRRCs, err = getValidRRCs(rrcsSortedRecentLast, 0, recentLast)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(actualRRCs))

	actualRRCs, err = getValidRRCs(rrcsSortedRecentLast, 50, recentLast)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(actualRRCs))
	assert.Equal(t, uint64(10), actualRRCs[0].TimeStamp)
	assert.Equal(t, uint64(20), actualRRCs[1].TimeStamp)
	assert.Equal(t, uint64(30), actualRRCs[2].TimeStamp)
	assert.Equal(t, uint64(40), actualRRCs[3].TimeStamp)
}
