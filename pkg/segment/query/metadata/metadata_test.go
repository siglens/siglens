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
	"sync"
	"testing"

	localstorage "github.com/siglens/siglens/pkg/blob/local"
	"github.com/siglens/siglens/pkg/segment/structs"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_AddSementInfo(t *testing.T) {
	_ = localstorage.InitLocalStorage()
	globalMetadata = &allSegmentMetadata{
		allSegmentMicroIndex:        make([]*SegmentMicroIndex, 0),
		segmentMetadataReverseIndex: make(map[string]*SegmentMicroIndex),
		tableSortedMetadata:         make(map[string][]*SegmentMicroIndex),
		updateLock:                  &sync.RWMutex{},
	}
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
		assert.Equal(t, uint64(i), globalMetadata.segmentMetadataReverseIndex[strconv.FormatInt(i, 10)].LatestEpochMS)
	}
	nextVal := uint64(9)
	for i := 0; i < 10; i++ {
		assert.Equal(t, globalMetadata.allSegmentMicroIndex[i].LatestEpochMS, nextVal)
		log.Infof("i %+v latest: %+v", i, globalMetadata.allSegmentMicroIndex[i].LatestEpochMS)
		nextVal--
	}
	assert.Contains(t, globalMetadata.tableSortedMetadata, "test-1")
	tableSorted := globalMetadata.tableSortedMetadata["test-1"]
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

	isAllDataSorted := sort.SliceIsSorted(globalMetadata.allSegmentMicroIndex, func(i, j int) bool {
		return globalMetadata.allSegmentMicroIndex[i].LatestEpochMS > globalMetadata.allSegmentMicroIndex[j].LatestEpochMS
	})
	assert.True(t, isAllDataSorted)
}

func testBlockRebalance(t *testing.T, fileCount int) {
	blockSizeToLoad := globalMetadata.allSegmentMicroIndex[0].MicroIndexSize + globalMetadata.allSegmentMicroIndex[0].SearchMetadataSize
	RebalanceInMemoryCmi(blockSizeToLoad)

	assert.True(t, globalMetadata.allSegmentMicroIndex[0].loadedMicroIndices)
	loadedBlockFile := globalMetadata.allSegmentMicroIndex[0].SegmentKey
	loadedBlockLatestTime := globalMetadata.allSegmentMicroIndex[0].LatestEpochMS
	assert.True(t, globalMetadata.segmentMetadataReverseIndex[loadedBlockFile].loadedMicroIndices)
	assert.Same(t, globalMetadata.allSegmentMicroIndex[0], globalMetadata.segmentMetadataReverseIndex[loadedBlockFile])

	for i := 1; i < fileCount; i++ {
		assert.False(t, globalMetadata.allSegmentMicroIndex[i].loadedMicroIndices)
		currFile := globalMetadata.allSegmentMicroIndex[i].SegmentKey
		assert.Same(t, globalMetadata.allSegmentMicroIndex[i], globalMetadata.segmentMetadataReverseIndex[currFile])
		assert.LessOrEqual(t, globalMetadata.allSegmentMicroIndex[i].LatestEpochMS, loadedBlockLatestTime, "we loaded the lastest timestamp")
	}

	// load just one more search metadata
	blockSizeToLoad += globalMetadata.allSegmentMicroIndex[1].SearchMetadataSize
	RebalanceInMemoryCmi(blockSizeToLoad)
	assert.True(t, globalMetadata.allSegmentMicroIndex[0].loadedMicroIndices)
	assert.False(t, globalMetadata.allSegmentMicroIndex[1].loadedMicroIndices, "only load search metadata for idx 1")
	for i := 2; i < fileCount; i++ {
		assert.False(t, globalMetadata.allSegmentMicroIndex[i].loadedMicroIndices)
		assert.False(t, globalMetadata.allSegmentMicroIndex[i].loadedSearchMetadata)
		currFile := globalMetadata.allSegmentMicroIndex[i].SegmentKey
		assert.Same(t, globalMetadata.allSegmentMicroIndex[i], globalMetadata.segmentMetadataReverseIndex[currFile])
		assert.LessOrEqual(t, globalMetadata.allSegmentMicroIndex[i].LatestEpochMS, loadedBlockLatestTime, "we loaded the lastest timestamp")
	}
}

func Test_RebalanceMetadata(t *testing.T) {
	_ = localstorage.InitLocalStorage()
	globalMetadata = &allSegmentMetadata{
		allSegmentMicroIndex:        make([]*SegmentMicroIndex, 0),
		segmentMetadataReverseIndex: make(map[string]*SegmentMicroIndex),
		tableSortedMetadata:         make(map[string][]*SegmentMicroIndex),
		updateLock:                  &sync.RWMutex{},
	}

	fileCount := 3
	InitMockColumnarMetadataStore("data/", fileCount, 10, 10)
	assert.Len(t, globalMetadata.allSegmentMicroIndex, fileCount)
	assert.Len(t, globalMetadata.segmentMetadataReverseIndex, fileCount)

	RebalanceInMemoryCmi(0)
	for i := 0; i < fileCount; i++ {
		assert.False(t, globalMetadata.allSegmentMicroIndex[i].loadedMicroIndices)
	}

	testBlockRebalance(t, fileCount)

	err := os.RemoveAll("data/")
	if err != nil {
		assert.FailNow(t, "failed to remove data %+v", err)
	}
}
