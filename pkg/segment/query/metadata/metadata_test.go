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
