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
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_AddSementInfo(t *testing.T) {
	ResetGlobalMetadataForTest()
	for i := uint64(0); i < 10; i++ {
		currInfo := &SegmentMicroIndex{
			SegMeta: structs.SegMeta{
				LatestEpochMS:    i,
				VirtualTableName: "test-1",
				SegmentKey:       strconv.FormatUint(i, 10),
			},
		}
		BulkAddSegmentMicroIndex([]*SegmentMicroIndex{currInfo})
	}

	for i := int64(0); i < 10; i++ {
		assert.Equal(t, uint64(i), GetSegmentMetadataReverseIndexForTest()[strconv.FormatInt(i, 10)].LatestEpochMS)
	}
	nextVal := uint64(9)
	for i := 0; i < 10; i++ {
		assert.Equal(t, GetAllSegmentMicroIndexForTest()[i].LatestEpochMS, nextVal)
		log.Infof("i %+v latest: %+v", i, GetAllSegmentMicroIndexForTest()[i].LatestEpochMS)
		nextVal--
	}
	assert.Contains(t, GetTableSortedMetadata(), "test-1")
	tableSorted := GetTableSortedMetadata()["test-1"]
	nextVal = uint64(9)
	for i := 0; i < 10; i++ {
		assert.Equal(t, tableSorted[i].LatestEpochMS, nextVal)
		log.Infof("i %+v latest: %+v", i, tableSorted[i].LatestEpochMS)
		nextVal--
	}
	isTableSorted := sort.SliceIsSorted(tableSorted, func(i, j int) bool {
		return tableSorted[i].LatestEpochMS > tableSorted[j].LatestEpochMS
	})
	assert.True(t, isTableSorted)

	isAllDataSorted := sort.SliceIsSorted(GetAllSegmentMicroIndexForTest(), func(i, j int) bool {
		return GetAllSegmentMicroIndexForTest()[i].LatestEpochMS > GetAllSegmentMicroIndexForTest()[j].LatestEpochMS
	})
	assert.True(t, isAllDataSorted)
}

func testBlockRebalance(t *testing.T, fileCount int) {
	blockSizeToLoad := GetAllSegmentMicroIndexForTest()[0].loadedCmiSize + GetAllSegmentMicroIndexForTest()[0].SearchMetadataSize
	RebalanceInMemoryCmi(blockSizeToLoad)

	loadedBlockFile := GetAllSegmentMicroIndexForTest()[0].SegmentKey
	loadedBlockLatestTime := GetAllSegmentMicroIndexForTest()[0].LatestEpochMS
	assert.Same(t, GetAllSegmentMicroIndexForTest()[0], GetSegmentMetadataReverseIndexForTest()[loadedBlockFile])

	for i := 1; i < fileCount; i++ {
		currFile := GetAllSegmentMicroIndexForTest()[i].SegmentKey
		assert.Same(t, GetAllSegmentMicroIndexForTest()[i], GetSegmentMetadataReverseIndexForTest()[currFile])
		assert.LessOrEqual(t, GetAllSegmentMicroIndexForTest()[i].LatestEpochMS, loadedBlockLatestTime, "we loaded the lastest timestamp")
	}

	// load just one more search metadata
	blockSizeToLoad += GetAllSegmentMicroIndexForTest()[1].SearchMetadataSize
	RebalanceInMemoryCmi(blockSizeToLoad)
	for i := 2; i < fileCount; i++ {
		assert.False(t, GetAllSegmentMicroIndexForTest()[i].isSearchMetadataLoaded())
		currFile := GetAllSegmentMicroIndexForTest()[i].SegmentKey
		assert.Same(t, GetAllSegmentMicroIndexForTest()[i], GetSegmentMetadataReverseIndexForTest()[currFile])
		assert.LessOrEqual(t, GetAllSegmentMicroIndexForTest()[i].LatestEpochMS, loadedBlockLatestTime, "we loaded the lastest timestamp")
	}
}

func Test_RebalanceMetadata(t *testing.T) {

	t.Cleanup(func() { os.RemoveAll("data/") })

	ResetGlobalMetadataForTest()

	fileCount := 3
	createMockMetaStore("data/", fileCount)
	assert.Len(t, GetAllSegmentMicroIndexForTest(), fileCount)
	assert.Len(t, GetSegmentMetadataReverseIndexForTest(), fileCount)

	RebalanceInMemoryCmi(0)

	testBlockRebalance(t, fileCount)
}

func createMockMetaStore(dir string, segcount int) {

	lencnames := uint8(12)
	mapCol := make(map[string]bool)
	for cidx := uint8(0); cidx < lencnames; cidx += 1 {
		currCol := fmt.Sprintf("key%v", cidx)
		mapCol[currCol] = true
	}

	for i := 0; i < segcount; i++ {
		segkey := dir + "mockquery_test_" + fmt.Sprint(i)

		allColsSizes := make(map[string]*structs.ColSizeInfo)
		for cname := range mapCol {
			allColsSizes[cname] = &structs.ColSizeInfo{CmiSize: 45, CsgSize: 9067}
		}

		sInfo := &structs.SegMeta{
			SegmentKey:       segkey,
			VirtualTableName: "evts",
			SegbaseDir:       segkey, // its actually one dir up, but for mocks its fine
			EarliestEpochMS:  0,
			LatestEpochMS:    7863564, // some random
			ColumnNames:      allColsSizes,
			NumBlocks:        12, // some random
		}
		segMetadata := InitSegmentMicroIndex(sInfo, false)
		BulkAddSegmentMicroIndex([]*SegmentMicroIndex{segMetadata})
	}
}

func Test_readEmptyColumnMicroIndices(t *testing.T) {
	ResetGlobalMetadataForTest()

	cnames := make(map[string]*structs.ColSizeInfo)
	cnames["clickid"] = &structs.ColSizeInfo{CmiSize: 0, CsgSize: 0}

	segmeta := &structs.SegMeta{
		SegmentKey:       "test-key",
		ColumnNames:      cnames,
		VirtualTableName: "test",
	}

	bMicro := InitSegmentMicroIndex(segmeta, false)

	err := bMicro.readCmis(map[uint16]map[string]bool{}, map[string]bool{})
	if err != nil {
		log.Errorf("failed to read cmi, err=%v", err)
	}
	assert.Nil(t, err)
}
