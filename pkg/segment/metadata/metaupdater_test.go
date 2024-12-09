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
