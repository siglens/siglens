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
	"strings"
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func getMockRecsAndFinalCols(numRecs uint64) (map[string]map[string]interface{}, map[string]bool) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano())) // Ensure random behavior is not repetitive.

	recs := make(map[string]map[string]interface{}, numRecs)
	finalCols := make(map[string]bool)

	// Generate records
	for i := uint64(1); i <= numRecs; i++ {
		recID := fmt.Sprintf("rec%d", i)
		recData := map[string]interface{}{
			"measure1": rng.Float64() * 100,      // Random float value
			"measure2": rng.Intn(100),            // Random integer value
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

// Tests the case where number or records exceed list size limit.
func Test_PerformMeasureAggsOnRecsSizeLimit_WithList(t *testing.T) {
	numSegments := 2
	sizeLimit := 0
	recsSize := utils.MAX_SPL_LIST_SIZE * 2
	nodeResult := &structs.NodeResult{PerformAggsOnRecs: true, RecsAggsType: structs.MeasureAggsType, MeasureOperations: []*structs.MeasureAggregator{
		{
			MeasureCol:  "measure1",
			MeasureFunc: utils.List,
			StrEnc:      "list(measure1)",
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
				len := len(strings.Split(record["list(measure1)"].(string), " "))
				assert.Equal(t, 100, len, "The list(measure1) value should be Equal to list of all records of all the Segments.")
			}
		} else {
			assert.Nil(t, resultMap, "The resultMap should be nil until the last segment is processed")
		}
	}
}

// Tests the normal list aggregation.
func Test_PerformMeasureAggsOnRecs_WithList(t *testing.T) {
	numSegments := 2
	sizeLimit := 0
	recsSize := 20
	nodeResult := &structs.NodeResult{PerformAggsOnRecs: true, RecsAggsType: structs.MeasureAggsType, MeasureOperations: []*structs.MeasureAggregator{
		{
			MeasureCol:  "measure1",
			MeasureFunc: utils.List,
			StrEnc:      "list(measure1)",
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
				len := len(strings.Split(record["list(measure1)"].(string), " "))
				assert.Equal(t, 40, len, "The list(measure1) value should be Equal to list of all records of all the Segments.")
			}
		} else {
			assert.Nil(t, resultMap, "The resultMap should be nil until the last segment is processed")
		}
	}
}

func Test_PerformGroupByAggsWithRexColumns(t *testing.T) {
	// Create a set of records that simulate the output of a rex search.
	// The "rex_field" is the field that was extracted via a rex expression.
	// We expect to see three groups: "alpha", "beta" and "gamma".
	recs := map[string]map[string]interface{}{
		"rec1": {"rex_field": "alpha", "other_field": "value1"},
		"rec2": {"rex_field": "beta", "other_field": "value2"},
		"rec3": {"rex_field": "alpha", "other_field": "value3"},
		"rec4": {"rex_field": "gamma", "other_field": "value4"},
		"rec5": {"rex_field": "beta", "other_field": "value5"},
	}
	// Final columns indicate which columns will be returned in the final aggregated result.
	finalCols := map[string]bool{
		"rex_field":   true,
		"other_field": true,
		"count(*)":    true,
	}

	// Set up the nodeResult so that aggregation is performed as a GroupBy.
	// The GroupByRequest specifies that we group by the "rex_field" column and count the records.
	nodeResult := &structs.NodeResult{
		PerformAggsOnRecs: true,
		RecsAggsType:      structs.GroupByType,
		GroupByCols:       []string{"rex_field"},
		GroupByRequest: &structs.GroupByRequest{
			GroupByColumns:   []string{"rex_field"},
			MeasureOperations: []*structs.MeasureAggregator{
				{
					MeasureCol:  "*",
					MeasureFunc: utils.Count,
					StrEnc:      "count(*)",
				},
			},
			BucketCount: 3000,
		},
	}

	// For this test we simulate a single segment that is completely processed.
	numTotalSegments := uint64(1)
	qid := uint64(1)

	// Call PerformAggsOnRecs.
	// Since RecsAggsType is GroupByType, this will call PerformGroupByRequestAggsOnRecs internally.
	resultMap := PerformAggsOnRecs(nodeResult, &structs.QueryAggregators{Limit: 0}, recs, finalCols, numTotalSegments, true, qid)

	// When the last (or only) segment is processed, a resultMap with a key "CHECK_NEXT_AGG"
	// should be returned. Also, the recs map is replaced by the aggregated result.
	assert.NotNil(t, resultMap, "Expected a non-nil resultMap on final segment processing")
	assert.True(t, resultMap["CHECK_NEXT_AGG"], "Expected CHECK_NEXT_AGG to be set to true in resultMap")

	// The aggregated results are stored in 'recs'. Because we are grouping by "rex_field",
	// we expect one aggregated record per distinct group.
	// In our test, we have three groups: "alpha", "beta", "gamma".
	assert.Equal(t, 3, len(recs), "Expected three aggregated groups in the result")

	// Verify that the count(*) aggregation is correct for each group.
	// "alpha" appears twice, "beta" appears twice, and "gamma" appears once.
	expectedCounts := map[string]int{
		"alpha": 2,
		"beta":  2,
		"gamma": 1,
	}
	for recID, record := range recs {
		groupVal, exists := record["rex_field"]
		assert.True(t, exists, "Aggregated record %s missing group-by column 'rex_field'", recID)
		countStr := fmt.Sprint(record["count(*)"])
		var count int
		_, err := fmt.Sscanf(countStr, "%d", &count)
		assert.NoError(t, err, "Failed to parse count(*) for record %s", recID)
		expected, ok := expectedCounts[fmt.Sprint(groupVal)]
		assert.True(t, ok, "Unexpected group value %v found in record %s", groupVal, recID)
		assert.Equal(t, expected, count, "Unexpected count for group %v in record %s", groupVal, recID)
	}
}