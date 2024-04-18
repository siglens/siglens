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
