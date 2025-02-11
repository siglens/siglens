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
	"io"
	"path/filepath"
	"testing"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/sortindex"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	allSegRequests, err := getSSRs(blocks, nil)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(allSegRequests))
	assert.Equal(t, 3, len(allSegRequests["file1"].AllBlocksToSearch))
	assert.Equal(t, blockMeta1, allSegRequests["file1"].AllBlocksToSearch[1])
	assert.Equal(t, blockMeta2, allSegRequests["file1"].AllBlocksToSearch[2])
	assert.Equal(t, blockMeta3, allSegRequests["file1"].AllBlocksToSearch[3])

	// Test when blocks are from different files.
	blocks[0].segkeyFname = "file2"
	allSegRequests, err = getSSRs(blocks, nil)
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
	_, err = getSSRs(blocks, nil)
	assert.Error(t, err)
	blocks[0].parentSSR = ssr // Reset for next test.

	// Test a subset of blocks.
	allSegRequests, err = getSSRs(blocks[:2], nil)
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

func writeSortIndexForTest(t *testing.T) (string, string, string) {
	t.Helper()

	tempDir := t.TempDir()
	segKey1 := filepath.Join(tempDir, "segKey1")
	segKey2 := filepath.Join(tempDir, "segKey2")

	cname := "color"
	seg1Data := map[segutils.CValueEnclosure]map[uint16][]uint16{ // value -> block number -> record numbers
		{Dtype: segutils.SS_DT_STRING, CVal: "blue"}: {
			1: {1},
			2: {2, 3},
		},
		{Dtype: segutils.SS_DT_STRING, CVal: "green"}: {
			1: {10, 11},
			2: {1},
		},
	}
	err := sortindex.WriteSortIndexMock(segKey1, cname, sortindex.SortAsAuto, seg1Data)
	assert.NoError(t, err)

	seg2Data := map[segutils.CValueEnclosure]map[uint16][]uint16{ // value -> block number -> record numbers
		{Dtype: segutils.SS_DT_STRING, CVal: "blue"}: {
			1: {1, 2},
			2: {3},
		},
		{Dtype: segutils.SS_DT_STRING, CVal: "red"}: {
			1: {10},
			2: {1, 2},
		},
	}
	err = sortindex.WriteSortIndexMock(segKey2, cname, sortindex.SortAsAuto, seg2Data)
	assert.NoError(t, err)

	return cname, segKey1, segKey2
}

func initSortIndexDataForTest(sortCname string, sortLimit uint64, numRecordsPerBatch int,
	segkeys ...string) (*Searcher, []*query.QuerySegmentRequest) {

	qsrs := make([]*query.QuerySegmentRequest, 0, len(segkeys))
	for _, segkey := range segkeys {
		qsr := &query.QuerySegmentRequest{}
		qsr.SetSegKey(segkey)
		qsr.SetTimeRange(&dtu.TimeRange{StartEpochMs: 0, EndEpochMs: 100})
		qsrs = append(qsrs, qsr)
	}

	queryInfo := &query.QueryInformation{}
	queryInfo.SetSearchNodeType(structs.MatchAllQuery)
	queryInfo.SetQueryTimeRange(&dtu.TimeRange{StartEpochMs: 0, EndEpochMs: 100})

	sortExpr := &structs.SortExpr{
		SortEles: []*structs.SortElement{
			{
				SortByAsc: true,
				Op:        "",
				Field:     sortCname,
			},
		},
		Limit: sortLimit,
	}
	searcher := &Searcher{
		sortIndexState: sortIndexState{
			numRecordsPerBatch: numRecordsPerBatch,
		},
		qid:         0,
		queryInfo:   queryInfo,
		sortExpr:    sortExpr,
		segEncToKey: utils.NewTwoWayMap[uint32, string](),
		qsrs:        qsrs,
	}

	return searcher, qsrs
}

func fetchAllFromQSRsForTest(t *testing.T, searcher *Searcher, qsrs []*query.QuerySegmentRequest) []*segutils.RecordResultContainer {
	t.Helper()

	iqr, err := searcher.fetchSortedRRCsFromQSRs()
	require.True(t, err == nil || err == io.EOF)
	require.NotNil(t, iqr)
	if err == io.EOF {
		return iqr.GetRRCs()
	}

	for {
		nextIQR, err := searcher.fetchSortedRRCsFromQSRs()
		err2 := iqr.Append(nextIQR)
		assert.NoError(t, err2)

		if err == io.EOF {
			break
		} else {
			require.NoError(t, err)
		}
	}

	return iqr.GetRRCs()
}

func Test_fetchSortedRRCsFromQSRs_multipleFetches_oneSegment(t *testing.T) {
	sortCname, segKey1, _ := writeSortIndexForTest(t)
	searcher, qsrs := initSortIndexDataForTest(sortCname, 100, 1, segKey1)
	rrcs := fetchAllFromQSRsForTest(t, searcher, qsrs)
	assert.NotNil(t, rrcs)
	assert.Equal(t, 6, len(rrcs))

	segEnc1 := searcher.getSegKeyEncoding(segKey1)
	expectedBlue := []rrcData{
		{segEnc: segEnc1, blockNum: 1, recordNum: 1},
		{segEnc: segEnc1, blockNum: 2, recordNum: 2},
		{segEnc: segEnc1, blockNum: 2, recordNum: 3},
	}
	expectedGreen := []rrcData{
		{segEnc: segEnc1, blockNum: 1, recordNum: 10},
		{segEnc: segEnc1, blockNum: 1, recordNum: 11},
		{segEnc: segEnc1, blockNum: 2, recordNum: 1},
	}

	actualRRCData := make([]rrcData, 0, len(rrcs))
	for _, rrc := range rrcs {
		actualRRCData = append(actualRRCData, extractRRCData(rrc))
	}

	assert.ElementsMatch(t, expectedBlue, actualRRCData[:3])
	assert.ElementsMatch(t, expectedGreen, actualRRCData[3:])
}

func Test_fetchSortedRRCsFromQSRs_multipleFetches_multipleSegments(t *testing.T) {
	sortCname, segKey1, segKey2 := writeSortIndexForTest(t)
	searcher, qsrs := initSortIndexDataForTest(sortCname, 100, 1, segKey1, segKey2)
	rrcs := fetchAllFromQSRsForTest(t, searcher, qsrs)
	assert.NotNil(t, rrcs)
	assert.Equal(t, 12, len(rrcs))

	segEnc1 := searcher.getSegKeyEncoding(segKey1)
	segEnc2 := searcher.getSegKeyEncoding(segKey2)
	expectedBlue := []rrcData{
		{segEnc: segEnc1, blockNum: 1, recordNum: 1},
		{segEnc: segEnc1, blockNum: 2, recordNum: 2},
		{segEnc: segEnc1, blockNum: 2, recordNum: 3},
		{segEnc: segEnc2, blockNum: 1, recordNum: 1},
		{segEnc: segEnc2, blockNum: 1, recordNum: 2},
		{segEnc: segEnc2, blockNum: 2, recordNum: 3},
	}
	expectedGreen := []rrcData{
		{segEnc: segEnc1, blockNum: 1, recordNum: 10},
		{segEnc: segEnc1, blockNum: 1, recordNum: 11},
		{segEnc: segEnc1, blockNum: 2, recordNum: 1},
	}
	expectedRed := []rrcData{
		{segEnc: segEnc2, blockNum: 1, recordNum: 10},
		{segEnc: segEnc2, blockNum: 2, recordNum: 1},
		{segEnc: segEnc2, blockNum: 2, recordNum: 2},
	}

	actualRRCData := make([]rrcData, 0, len(rrcs))
	for _, rrc := range rrcs {
		actualRRCData = append(actualRRCData, extractRRCData(rrc))
	}

	assert.ElementsMatch(t, expectedBlue, actualRRCData[:6])
	assert.ElementsMatch(t, expectedGreen, actualRRCData[6:9])
	assert.ElementsMatch(t, expectedRed, actualRRCData[9:])
}

func Test_fetchSortedRRCsFromQSRs_multipleSegments_earlyExit(t *testing.T) {
	sortCname, segKey1, segKey2 := writeSortIndexForTest(t)
	searcher, qsrs := initSortIndexDataForTest(sortCname, 6, 1, segKey1, segKey2)
	rrcs := fetchAllFromQSRsForTest(t, searcher, qsrs)
	assert.NotNil(t, rrcs)
	assert.Equal(t, 6, len(rrcs))

	segEnc1 := searcher.getSegKeyEncoding(segKey1)
	segEnc2 := searcher.getSegKeyEncoding(segKey2)
	expectedBlue := []rrcData{
		{segEnc: segEnc1, blockNum: 1, recordNum: 1},
		{segEnc: segEnc1, blockNum: 2, recordNum: 2},
		{segEnc: segEnc1, blockNum: 2, recordNum: 3},
		{segEnc: segEnc2, blockNum: 1, recordNum: 1},
		{segEnc: segEnc2, blockNum: 1, recordNum: 2},
		{segEnc: segEnc2, blockNum: 2, recordNum: 3},
	}

	actualRRCData := make([]rrcData, 0, len(rrcs))
	for _, rrc := range rrcs {
		actualRRCData = append(actualRRCData, extractRRCData(rrc))
	}

	assert.ElementsMatch(t, expectedBlue, actualRRCData)
	assert.True(t, searcher.sortIndexState.didEarlyExit)
}

type rrcData struct {
	segEnc    uint32
	blockNum  uint16
	recordNum uint16
}

func extractRRCData(rrc *segutils.RecordResultContainer) rrcData {
	return rrcData{
		segEnc:    rrc.SegKeyInfo.SegKeyEnc,
		blockNum:  rrc.BlockNum,
		recordNum: rrc.RecordNum,
	}
}
