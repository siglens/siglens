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

	segmetadata "github.com/siglens/siglens/pkg/segment/metadata"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_initMockMetadata(t *testing.T) {

	fileCount := 3
	InitMockColumnarMetadataStore("data/", fileCount, 10, 10)
	assert.Len(t, segmetadata.GetAllSegmentMicroIndexForTest(), fileCount)
	assert.Len(t, segmetadata.GetSegmentMetadataReverseIndexForTest(), fileCount)
	assert.Contains(t, segmetadata.GetTableSortedMetadataForTest(), "evts")
	assert.Len(t, segmetadata.GetTableSortedMetadataForTest()["evts"], fileCount)

	sortedByLatest := sort.SliceIsSorted(segmetadata.GetAllSegmentMicroIndexForTest(), func(i, j int) bool {
		return segmetadata.GetAllSegmentMicroIndexForTest()[i].LatestEpochMS > segmetadata.GetAllSegmentMicroIndexForTest()[j].LatestEpochMS
	})
	assert.True(t, sortedByLatest, "slice is sorted with most recent at the front")

	tableSorted := segmetadata.GetTableSortedMetadataForTest()["evts"]
	isTableSorted := sort.SliceIsSorted(tableSorted, func(i, j int) bool {
		return tableSorted[i].LatestEpochMS > tableSorted[j].LatestEpochMS
	})
	assert.True(t, isTableSorted, "slice is sorted with most recent at the front")

	for i, rawCMI := range tableSorted {
		assert.Equal(t, rawCMI, segmetadata.GetAllSegmentMicroIndexForTest()[i], "bc only one table exists, these sorted slices should point to the same structs")
	}
	for _, rawCMI := range segmetadata.GetAllSegmentMicroIndexForTest() {
		assert.Contains(t, segmetadata.GetSegmentMetadataReverseIndexForTest(), rawCMI.SegmentKey, "all segkeys in allSegmentMicroIndex exist in revserse index")
	}

	duplicate := segmetadata.GetTableSortedMetadataForTest()["evts"][0]
	segmetadata.BulkAddSegmentMicroIndex([]*segmetadata.SegmentMicroIndex{duplicate})
	assert.Len(t, segmetadata.GetAllSegmentMicroIndexForTest(), fileCount)
	assert.Len(t, segmetadata.GetSegmentMetadataReverseIndexForTest(), fileCount)
	assert.Contains(t, segmetadata.GetTableSortedMetadataForTest(), "evts")
	assert.Len(t, segmetadata.GetTableSortedMetadataForTest()["evts"], fileCount)

	err := os.RemoveAll("data/")
	if err != nil {
		log.Fatal(err)
	}
}
