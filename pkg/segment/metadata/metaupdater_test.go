// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package metadata

import (
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_initMockMetadata(t *testing.T) {

	t.Cleanup(func() { os.RemoveAll("data/") })

	fileCount := 3
	createMockMetaStore("data/", fileCount)
	assert.Len(t, GetAllSegmentMicroIndexForTest(), fileCount)
	assert.Len(t, GetSegmentMetadataReverseIndexForTest(), fileCount)
	assert.Contains(t, GetTableSortedMetadata(), "evts")
	assert.Len(t, GetTableSortedMetadata()["evts"], fileCount)

	sortedByLatest := sort.SliceIsSorted(GetAllSegmentMicroIndexForTest(), func(i, j int) bool {
		return GetAllSegmentMicroIndexForTest()[i].LatestEpochMS > GetAllSegmentMicroIndexForTest()[j].LatestEpochMS
	})
	assert.True(t, sortedByLatest, "slice is sorted with most recent at the front")

	tableSorted := GetTableSortedMetadata()["evts"]
	isTableSorted := sort.SliceIsSorted(tableSorted, func(i, j int) bool {
		return tableSorted[i].LatestEpochMS > tableSorted[j].LatestEpochMS
	})
	assert.True(t, isTableSorted, "slice is sorted with most recent at the front")

	for i, rawCMI := range tableSorted {
		assert.Equal(t, rawCMI, GetAllSegmentMicroIndexForTest()[i], "because only one table exists, these sorted slices should point to the same structs")
	}
	for _, rawCMI := range GetAllSegmentMicroIndexForTest() {
		assert.Contains(t, GetSegmentMetadataReverseIndexForTest(), rawCMI.SegmentKey, "all segkeys in allSegmentMicroIndex exist in revserse index")
	}

	duplicate := GetTableSortedMetadata()["evts"][0]
	BulkAddSegmentMicroIndex([]*SegmentMicroIndex{duplicate})
	assert.Len(t, GetAllSegmentMicroIndexForTest(), fileCount)
	assert.Len(t, GetSegmentMetadataReverseIndexForTest(), fileCount)
	assert.Contains(t, GetTableSortedMetadata(), "evts")
	assert.Len(t, GetTableSortedMetadata()["evts"], fileCount)
}
