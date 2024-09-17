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
	"os"
	"sort"
	"strconv"
	"testing"

	localstorage "github.com/siglens/siglens/pkg/blob/local"
	segmetadata "github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/structs"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_AddSementInfo(t *testing.T) {
	_ = localstorage.InitLocalStorage()
	segmetadata.ResetGlobalMetadataForTest()
	for i := uint64(0); i < 10; i++ {
		currInfo := &segmetadata.SegmentMicroIndex{
			SegMeta: structs.SegMeta{
				LatestEpochMS:    i,
				VirtualTableName: "test-1",
				SegmentKey:       strconv.FormatUint(i, 10),
			},
		}
		segmetadata.BulkAddSegmentMicroIndex([]*segmetadata.SegmentMicroIndex{currInfo})
	}

	for i := int64(0); i < 10; i++ {
		assert.Equal(t, uint64(i), segmetadata.GetSegmentMetadataReverseIndexForTest()[strconv.FormatInt(i, 10)].LatestEpochMS)
	}
	nextVal := uint64(9)
	for i := 0; i < 10; i++ {
		assert.Equal(t, segmetadata.GetAllSegmentMicroIndexForTest()[i].LatestEpochMS, nextVal)
		log.Infof("i %+v latest: %+v", i, segmetadata.GetAllSegmentMicroIndexForTest()[i].LatestEpochMS)
		nextVal--
	}
	assert.Contains(t, segmetadata.GetTableSortedMetadataForTest(), "test-1")
	tableSorted := segmetadata.GetTableSortedMetadataForTest()["test-1"]
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

	isAllDataSorted := sort.SliceIsSorted(segmetadata.GetAllSegmentMicroIndexForTest(), func(i, j int) bool {
		return segmetadata.GetAllSegmentMicroIndexForTest()[i].LatestEpochMS > segmetadata.GetAllSegmentMicroIndexForTest()[j].LatestEpochMS
	})
	assert.True(t, isAllDataSorted)
}

func testBlockRebalance(t *testing.T, fileCount int) {
	blockSizeToLoad := segmetadata.GetAllSegmentMicroIndexForTest()[0].MicroIndexSize + segmetadata.GetAllSegmentMicroIndexForTest()[0].SearchMetadataSize
	segmetadata.RebalanceInMemoryCmi(blockSizeToLoad)

	assert.True(t, segmetadata.GetAllSegmentMicroIndexForTest()[0].AreMicroIndicesLoaded())
	loadedBlockFile := segmetadata.GetAllSegmentMicroIndexForTest()[0].SegmentKey
	loadedBlockLatestTime := segmetadata.GetAllSegmentMicroIndexForTest()[0].LatestEpochMS
	assert.True(t, segmetadata.GetSegmentMetadataReverseIndexForTest()[loadedBlockFile].AreMicroIndicesLoaded())
	assert.Same(t, segmetadata.GetAllSegmentMicroIndexForTest()[0], segmetadata.GetSegmentMetadataReverseIndexForTest()[loadedBlockFile])

	for i := 1; i < fileCount; i++ {
		assert.False(t, segmetadata.GetAllSegmentMicroIndexForTest()[i].AreMicroIndicesLoaded())
		currFile := segmetadata.GetAllSegmentMicroIndexForTest()[i].SegmentKey
		assert.Same(t, segmetadata.GetAllSegmentMicroIndexForTest()[i], segmetadata.GetSegmentMetadataReverseIndexForTest()[currFile])
		assert.LessOrEqual(t, segmetadata.GetAllSegmentMicroIndexForTest()[i].LatestEpochMS, loadedBlockLatestTime, "we loaded the lastest timestamp")
	}

	// load just one more search metadata
	blockSizeToLoad += segmetadata.GetAllSegmentMicroIndexForTest()[1].SearchMetadataSize
	segmetadata.RebalanceInMemoryCmi(blockSizeToLoad)
	assert.True(t, segmetadata.GetAllSegmentMicroIndexForTest()[0].AreMicroIndicesLoaded())
	assert.False(t, segmetadata.GetAllSegmentMicroIndexForTest()[1].AreMicroIndicesLoaded(), "only load search metadata for idx 1")
	for i := 2; i < fileCount; i++ {
		assert.False(t, segmetadata.GetAllSegmentMicroIndexForTest()[i].AreMicroIndicesLoaded())
		assert.False(t, segmetadata.GetAllSegmentMicroIndexForTest()[i].IsSearchMetadataLoaded())
		currFile := segmetadata.GetAllSegmentMicroIndexForTest()[i].SegmentKey
		assert.Same(t, segmetadata.GetAllSegmentMicroIndexForTest()[i], segmetadata.GetSegmentMetadataReverseIndexForTest()[currFile])
		assert.LessOrEqual(t, segmetadata.GetAllSegmentMicroIndexForTest()[i].LatestEpochMS, loadedBlockLatestTime, "we loaded the lastest timestamp")
	}
}

func Test_RebalanceMetadata(t *testing.T) {
	_ = localstorage.InitLocalStorage()
	segmetadata.ResetGlobalMetadataForTest()

	fileCount := 3
	InitMockColumnarMetadataStore("data/", fileCount, 10, 10)
	assert.Len(t, segmetadata.GetAllSegmentMicroIndexForTest(), fileCount)
	assert.Len(t, segmetadata.GetSegmentMetadataReverseIndexForTest(), fileCount)

	segmetadata.RebalanceInMemoryCmi(0)
	for i := 0; i < fileCount; i++ {
		assert.False(t, segmetadata.GetAllSegmentMicroIndexForTest()[i].AreMicroIndicesLoaded())
	}

	testBlockRebalance(t, fileCount)

	err := os.RemoveAll("data/")
	if err != nil {
		assert.FailNow(t, "failed to remove data %+v", err)
	}
}
