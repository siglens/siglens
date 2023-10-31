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
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_initMockMetadata(t *testing.T) {

	fileCount := 3
	InitMockColumnarMetadataStore("data/", fileCount, 10, 10)
	assert.Len(t, globalMetadata.allSegmentMicroIndex, fileCount)
	assert.Len(t, globalMetadata.segmentMetadataReverseIndex, fileCount)
	assert.Contains(t, globalMetadata.tableSortedMetadata, "evts")
	assert.Len(t, globalMetadata.tableSortedMetadata["evts"], fileCount)

	sortedByLatest := sort.SliceIsSorted(globalMetadata.allSegmentMicroIndex, func(i, j int) bool {
		return globalMetadata.allSegmentMicroIndex[i].LatestEpochMS > globalMetadata.allSegmentMicroIndex[j].LatestEpochMS
	})
	assert.True(t, sortedByLatest, "slice is sorted with most recent at the front")

	tableSorted := globalMetadata.tableSortedMetadata["evts"]
	isTableSorted := sort.SliceIsSorted(tableSorted, func(i, j int) bool {
		return tableSorted[i].LatestEpochMS > tableSorted[j].LatestEpochMS
	})
	assert.True(t, isTableSorted, "slice is sorted with most recent at the front")

	for i, rawCMI := range tableSorted {
		assert.Equal(t, rawCMI, globalMetadata.allSegmentMicroIndex[i], "bc only one table exists, these sorted slices should point to the same structs")
	}
	for _, rawCMI := range globalMetadata.allSegmentMicroIndex {
		assert.Contains(t, globalMetadata.segmentMetadataReverseIndex, rawCMI.SegmentKey, "all segkeys in allSegmentMicroIndex exist in revserse index")
	}

	duplicate := globalMetadata.tableSortedMetadata["evts"][0]
	BulkAddSegmentMicroIndex([]*SegmentMicroIndex{duplicate})
	assert.Len(t, globalMetadata.allSegmentMicroIndex, fileCount)
	assert.Len(t, globalMetadata.segmentMetadataReverseIndex, fileCount)
	assert.Contains(t, globalMetadata.tableSortedMetadata, "evts")
	assert.Len(t, globalMetadata.tableSortedMetadata["evts"], fileCount)

	err := os.RemoveAll("data/")
	if err != nil {
		log.Fatal(err)
	}
}
