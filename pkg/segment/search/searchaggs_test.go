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

package search

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func getMockRecsAndFinalCols(numRecs uint64) (map[string]map[string]interface{}, map[string]bool) {
	rand.Seed(time.Now().UnixNano()) // Ensure random behavior is not repetitive.

	recs := make(map[string]map[string]interface{}, numRecs)
	finalCols := make(map[string]bool)

	// Generate records
	for i := uint64(1); i <= numRecs; i++ {
		recID := fmt.Sprintf("rec%d", i)
		recData := map[string]interface{}{
			"measure1": rand.Float64() * 100,     // Random float value
			"measure2": rand.Intn(100),           // Random integer value
			"measure3": fmt.Sprintf("info%d", i), // Incremental info string
		}
		recs[recID] = recData
	}

	// Generate final columns
	finalCols["measure1"] = true
	finalCols["measure2"] = true
	finalCols["measure3"] = true

	return recs, finalCols
}

// Tests the case where the size limit is zero and the number of segments is 2.
// Normal case where sizelimit should not be considered.
func Test_PerformMeasureAggsOnRecsSizeLimit_Zero_MultiSegments(t *testing.T) {
	numSegments := 2
	sizeLimit := 0
	recsSize := 100
	nodeResult := &structs.NodeResult{PerformAggsOnRecs: true, RecsAggsType: structs.MeasureAggsType, MeasureOperations: []*structs.MeasureAggregator{
		{
			MeasureCol:  "*",
			MeasureFunc: utils.Count,
			StrEnc:      "count(*)",
		},
	}}
	aggs := &structs.QueryAggregators{Limit: sizeLimit}

	for i := 0; i < numSegments; i++ {

		recs, finalCols := getMockRecsAndFinalCols(uint64(recsSize))

		resultMap := PerformAggsOnRecs(nodeResult, aggs, recs, finalCols, uint64(numSegments), true, 1)

		assert.Equal(t, uint64(i+1), nodeResult.RecsAggsProcessedSegments, "The processed segments count should be incremented for each Segment that is processed completely.")

		if i == numSegments-1 {
			assert.Equal(t, 1, len(resultMap), "The last Segment should return a resultMap with a single key.")
			assert.True(t, resultMap["CHECK_NEXT_AGG"], "The last Segment should return a resultMap with a key CHECK_NEXT_AGG set to true.")
			assert.Equal(t, 1, len(recs), "MeasureAggs: Should return only a single record containing result of the aggregation.")
			for _, record := range recs {
				assert.Equal(t, fmt.Sprint(recsSize*numSegments), record["count(*)"], "The count(*) value should be Equal to count of all records of all the Segments.")
			}
		} else {
			assert.Nil(t, resultMap, "The resultMap should be nil until the last segment is processed")
		}
	}
}

// Tests the case where the size limit is non-zero and the number of segments is 2.
// A case where the size limit is less than the records of all the segments.
// The size limit should be considered instead of the number of segments.
func Test_PerformMeasureAggsOnRecsSizeLimit_NonZero_LessThanSegments(t *testing.T) {
	numSegments := 2
	sizeLimit := 99
	nodeResult := &structs.NodeResult{PerformAggsOnRecs: true, RecsAggsType: structs.MeasureAggsType, MeasureOperations: []*structs.MeasureAggregator{
		{
			MeasureCol:  "*",
			MeasureFunc: utils.Count,
			StrEnc:      "count(*)",
		},
	}}
	aggs := &structs.QueryAggregators{Limit: sizeLimit}

	recs, finalCols := getMockRecsAndFinalCols(uint64(sizeLimit))

	resultMap := PerformAggsOnRecs(nodeResult, aggs, recs, finalCols, uint64(numSegments), true, 1)

	assert.Equal(t, uint64(numSegments), nodeResult.RecsAggsProcessedSegments, "The number of Segments processed should be equal to the total number of segments")

	assert.Equal(t, 1, len(resultMap), "The resultMap should have a single key")
	assert.True(t, resultMap["CHECK_NEXT_AGG"], "The resultMap should have a key CHECK_NEXT_AGG set to true")
	assert.Equal(t, 1, len(recs), "MeasureAggs: Should return only a single record containing result of the aggregation")
	for _, record := range recs {
		assert.Equal(t, fmt.Sprint(sizeLimit), record["count(*)"], "The count should be equal to the size limit")
	}
}

// Tests the case where the size limit is non-zero and the number of segments is 2.
// A case where the size limit is equal to the records of all the segments. Or It requires all the segments to be processed.
// Number of Segments should be considered.
func Test_PerformMeasureAggsOnRecsSizeLimit_NonZero_EqualToSegments(t *testing.T) {
	numSegments := 2
	recsSize := 100
	sizeLimit := 180
	nodeResult := &structs.NodeResult{PerformAggsOnRecs: true, RecsAggsType: structs.MeasureAggsType, MeasureOperations: []*structs.MeasureAggregator{
		{
			MeasureCol:  "*",
			MeasureFunc: utils.Count,
			StrEnc:      "count(*)",
		},
	}}
	aggs := &structs.QueryAggregators{Limit: sizeLimit}

	for i := 0; i < numSegments; i++ {
		recs, finalCols := getMockRecsAndFinalCols(uint64(recsSize))

		if i == numSegments-1 {
			recs, finalCols = getMockRecsAndFinalCols(uint64(sizeLimit - (recsSize * (numSegments - 1))))
		}

		resultMap := PerformAggsOnRecs(nodeResult, aggs, recs, finalCols, uint64(numSegments), true, 1)

		assert.Equal(t, uint64(i+1), nodeResult.RecsAggsProcessedSegments, "The Processed Segments count should be incremented for each Segment that is processed completely.")

		if i == numSegments-1 {
			assert.Equal(t, 1, len(resultMap), "The last Segment should return a resultMap with a single key.")
			assert.True(t, resultMap["CHECK_NEXT_AGG"], "The Last Segment should return a resultMap with a key CHECK_NEXT_AGG set to true.")
			assert.Equal(t, 1, len(recs), "MeasureAggs: Should return only a single record containing result of the aggregation.")
			for _, record := range recs {
				assert.Equal(t, fmt.Sprint(sizeLimit), record["count(*)"], "The count(*) value should be Equal to the size limit.")
			}
		} else {
			assert.Nil(t, resultMap, "The resultMap should be nil until the last segment is processed")
		}
	}
}

// Tests the case where the size limit is non-zero and the number of segments is 2.
// A case where the size limit is greater than the records of all the segments.
// Number of Segments should be considered.
func Test_PerformMeasureAggsOnRecsSizeLimit_NonZero_GreaterThanSegments(t *testing.T) {
	numSegments := 2
	recsSize := 100
	sizeLimit := 250
	nodeResult := &structs.NodeResult{PerformAggsOnRecs: true, RecsAggsType: structs.MeasureAggsType, MeasureOperations: []*structs.MeasureAggregator{
		{
			MeasureCol:  "*",
			MeasureFunc: utils.Count,
			StrEnc:      "count(*)",
		},
	}}
	aggs := &structs.QueryAggregators{Limit: sizeLimit}

	for i := 0; i < numSegments; i++ {
		recs, finalCols := getMockRecsAndFinalCols(uint64(recsSize))

		resultMap := PerformAggsOnRecs(nodeResult, aggs, recs, finalCols, uint64(numSegments), true, 1)

		assert.Equal(t, uint64(i+1), nodeResult.RecsAggsProcessedSegments, "The Processed Segments count should be incremented for each Segment that is processed completely.")

		if i == numSegments-1 {
			assert.Equal(t, 1, len(resultMap), "The Last Segment should return a resultMap with a single key.")
			assert.True(t, resultMap["CHECK_NEXT_AGG"], "The Last Segment should return a resultMap with a key CHECK_NEXT_AGG set to true.")
			assert.Equal(t, 1, len(recs), "MeasureAggs: Should return only a single record containing result of the aggregation.")
			for _, record := range recs {
				assert.Equal(t, fmt.Sprint(recsSize*numSegments), record["count(*)"], "The count(*) value should be Equal to count of all records of all the Segments.")
			}
		} else {
			assert.Nil(t, resultMap, "The resultMap should be nil until the last segment is processed")
		}
	}
}

// Tests the case where the size limit is zero and the number of segments is 2.
// Normal case where sizelimit should not be considered.
func Test_PerformGroupByRequestAggsOnRecsSizeLimit_Zero_MultiSegment(t *testing.T) {
	numSegments := 2
	sizeLimit := 0
	recsSize := 100
	nodeResult := &structs.NodeResult{
		PerformAggsOnRecs: true,
		RecsAggsType:      structs.GroupByType,
		GroupByCols:       []string{"measure3"},
		GroupByRequest: &structs.GroupByRequest{
			MeasureOperations: []*structs.MeasureAggregator{
				{
					MeasureCol:  "*",
					MeasureFunc: utils.Count,
					StrEnc:      "count(*)",
				},
			},
			GroupByColumns: []string{"measure3"},
			BucketCount:    3000,
		},
	}
	aggs := &structs.QueryAggregators{Limit: sizeLimit}

	for i := 0; i < numSegments; i++ {
		recs, finalCols := getMockRecsAndFinalCols(uint64(recsSize))

		resultMap := PerformAggsOnRecs(nodeResult, aggs, recs, finalCols, uint64(numSegments), true, 1)

		assert.Equal(t, uint64(i+1), nodeResult.RecsAggsProcessedSegments, "The processed segments count should be incremented for each Segment that is processed completely.")

		if i == numSegments-1 {
			assert.Equal(t, 1, len(resultMap), "The last Segment should return a resultMap with a single key.")
			assert.True(t, resultMap["CHECK_NEXT_AGG"], "The Last Segment should return a resultMap with a key CHECK_NEXT_AGG set to true.")
			assert.Equal(t, recsSize, len(recs), "The records size should be equal to the recsSize as each record in a segment is unique and is repeated across segments.")
			for _, record := range recs {
				assert.Equal(t, uint64(2), record["count(*)"], "The count(*) value should be Equal to 2 as each record in a segment is unique and is repeated across segments.")
			}
		} else {
			assert.Nil(t, resultMap, "The resultMap should be nil until the last segment is processed")
		}
	}
}

// Tests the case where the size limit is non-zero and the number of segments is 2.
// A case where the size limit is less than the records of all the segments.
// The size limit should be considered instead of the number of segments.
func Test_PerformGroupByRequestAggsOnRecsSizeLimit_NonZero_LessThanSegments(t *testing.T) {
	numSegments := 2
	sizeLimit := 99
	nodeResult := &structs.NodeResult{
		PerformAggsOnRecs: true,
		RecsAggsType:      structs.GroupByType,
		GroupByCols:       []string{"measure3"},
		GroupByRequest: &structs.GroupByRequest{
			MeasureOperations: []*structs.MeasureAggregator{
				{
					MeasureCol:  "*",
					MeasureFunc: utils.Count,
					StrEnc:      "count(*)",
				},
			},
			GroupByColumns: []string{"measure3"},
			BucketCount:    3000,
		},
	}
	aggs := &structs.QueryAggregators{Limit: sizeLimit}

	recs, finalCols := getMockRecsAndFinalCols(uint64(sizeLimit))

	resultMap := PerformAggsOnRecs(nodeResult, aggs, recs, finalCols, uint64(numSegments), true, 1)

	assert.Equal(t, uint64(numSegments), nodeResult.RecsAggsProcessedSegments, "The number of Segments processed should be equal to the total number of segments")

	assert.Equal(t, 1, len(resultMap), "The resultMap should have a single key")
	assert.True(t, resultMap["CHECK_NEXT_AGG"], "The resultMap should have a key CHECK_NEXT_AGG set to true")
	assert.Equal(t, sizeLimit, len(recs), "The records size should be equal to the size limit as each record is unique in a segment and only one segment is required/processed.")
	for _, record := range recs {
		assert.Equal(t, uint64(1), record["count(*)"], "The count(*) value should be Equal to 1 as each record is unique in a segment and only one segment is required/processed.")
	}
}

// Tests the case where the size limit is non-zero and the number of segments is 2.
// A case where the size limit is equal to the records of all the segments. Or It requires all the segments to be processed.
// Number of Segments should be considered.
func Test_PerformGroupByRequestAggsOnRecsSizeLimit_NonZero_EqualToSegments(t *testing.T) {
	numSegments := 2
	recsSize := 100
	sizeLimit := 180
	nodeResult := &structs.NodeResult{
		PerformAggsOnRecs: true,
		RecsAggsType:      structs.GroupByType,
		GroupByCols:       []string{"measure3"},
		GroupByRequest: &structs.GroupByRequest{
			MeasureOperations: []*structs.MeasureAggregator{
				{
					MeasureCol:  "*",
					MeasureFunc: utils.Count,
					StrEnc:      "count(*)",
				},
			},
			GroupByColumns: []string{"measure3"},
			BucketCount:    3000,
		},
	}
	aggs := &structs.QueryAggregators{Limit: sizeLimit}

	for i := 0; i < numSegments; i++ {
		recs, finalCols := getMockRecsAndFinalCols(uint64(recsSize))

		if i == numSegments-1 {
			recs, finalCols = getMockRecsAndFinalCols(uint64(sizeLimit - (recsSize * (numSegments - 1))))
		}

		resultMap := PerformAggsOnRecs(nodeResult, aggs, recs, finalCols, uint64(numSegments), true, 1)

		assert.Equal(t, uint64(i+1), nodeResult.RecsAggsProcessedSegments, "The Processed Segments count should be incremented for each Segment that is processed completely.")

		if i == numSegments-1 {
			assert.Equal(t, 1, len(resultMap), "The Last Segment should return a resultMap with a single key.")
			assert.True(t, resultMap["CHECK_NEXT_AGG"], "The Last Segment should return a resultMap with a key CHECK_NEXT_AGG set to true.")
			assert.Equal(t, recsSize, len(recs), "The records size should be equal to the recsSize as each record in a segment is unique and is repeated across segments.")
			twoCountRecs := 0
			oneCountRecs := 0
			for _, record := range recs {
				assert.True(t, (uint64(2) == record["count(*)"] || uint64(1) == record["count(*)"]), "The count should either 2 or 1 as there are two segments and the second segment has less records (resulting to 1).")
				if uint64(2) == record["count(*)"] {
					twoCountRecs++
				} else {
					oneCountRecs++
				}
			}
			assert.Equal(t, 80, twoCountRecs, "The count(*) value should be Equal to number of Segments for 80 records as the last segment there has 80 records.")
			assert.Equal(t, 20, oneCountRecs, "The count(*) value should be (num of Segments - 1) for 20 records as the each segment has 100 records and the last segment has 80 records.")
		} else {
			assert.Nil(t, resultMap, "The resultMap should be nil until the last segment is processed")
		}
	}
}

// Tests the case where the size limit is non-zero and the number of segments is 2.
// A case where the size limit is greater than the records of all the segments.
// Number of Segments should be considered.
func Test_PerformGroupByRequestAggsOnRecsSizeLimit_NonZero_GreaterThanSegments(t *testing.T) {
	numSegments := 2
	recsSize := 100
	sizeLimit := 250
	nodeResult := &structs.NodeResult{
		PerformAggsOnRecs: true,
		RecsAggsType:      structs.GroupByType,
		GroupByCols:       []string{"measure3"},
		GroupByRequest: &structs.GroupByRequest{
			MeasureOperations: []*structs.MeasureAggregator{
				{
					MeasureCol:  "*",
					MeasureFunc: utils.Count,
					StrEnc:      "count(*)",
				},
			},
			GroupByColumns: []string{"measure3"},
			BucketCount:    3000,
		},
	}
	aggs := &structs.QueryAggregators{Limit: sizeLimit}

	for i := 0; i < numSegments; i++ {
		recs, finalCols := getMockRecsAndFinalCols(uint64(recsSize))

		resultMap := PerformAggsOnRecs(nodeResult, aggs, recs, finalCols, uint64(numSegments), true, 1)

		assert.Equal(t, uint64(i+1), nodeResult.RecsAggsProcessedSegments, "The Processed Segments count should be incremented for each Segment that is processed completely.")

		if i == numSegments-1 {
			assert.Equal(t, 1, len(resultMap), "The Last Segment should return a resultMap with a single key.")
			assert.True(t, resultMap["CHECK_NEXT_AGG"], "The Last Segment should return a resultMap with a key CHECK_NEXT_AGG set to true.")
			assert.Equal(t, recsSize, len(recs), "The records size should be equal to the recsSize as each record in a segment is unique and is repeated across segments.")
			for _, record := range recs {
				assert.Equal(t, uint64(2), record["count(*)"], "The count should be equal to number of Segments as each record in a segment is unique and is repeated across segments.")
			}
		} else {
			assert.Nil(t, resultMap, "The resultMap should be nil until the last segment is processed")
		}
	}
}
