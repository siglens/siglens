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
	numSegements := 2
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

	for i := 0; i < numSegements; i++ {

		recs, finalCols := getMockRecsAndFinalCols(uint64(recsSize))

		resultMap := PerformAggsOnRecs(nodeResult, aggs, recs, finalCols, uint64(numSegements), true, 1)

		assert.Equal(t, uint64(i+1), nodeResult.RecsAggsProcessedSegments, "expected=%v, actual=%v", i+1, nodeResult.RecsAggsProcessedSegments)

		if i == numSegements-1 {
			assert.Equal(t, 1, len(resultMap), "expected=%v, actual=%v", 1, len(resultMap))
			assert.True(t, resultMap["CHECK_NEXT_AGG"], "expected=%v, actual=%v", true, resultMap["CHECK_NEXT_AGG"])
			assert.Equal(t, 1, len(recs), "expected=%v, actual=%v", 1, len(recs))
			for _, record := range recs {
				assert.Equal(t, fmt.Sprint(recsSize*numSegements), record["count(*)"], "expected=%v, actual=%v", recsSize*numSegements, recs["count(*)"])
			}
		} else {
			assert.Nil(t, resultMap, "expected=%v, actual=%v", nil, resultMap)
		}
	}
}

// Tests the case where the size limit is non-zero and the number of segments is 2.
// A case where the size limit is less than the records of all the segments.
// The size limit should be considered instead of the number of segments.
func Test_PerformMeasureAggsOnRecsSizeLimit_NonZero_LessThanSegments(t *testing.T) {
	numSegements := 2
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

	resultMap := PerformAggsOnRecs(nodeResult, aggs, recs, finalCols, uint64(numSegements), true, 1)

	assert.Equal(t, uint64(numSegements), nodeResult.RecsAggsProcessedSegments, "expected=%v, actual=%v", uint64(numSegements), nodeResult.RecsAggsProcessedSegments)

	assert.Equal(t, 1, len(resultMap), "expected=%v, actual=%v", 1, len(resultMap))
	assert.True(t, resultMap["CHECK_NEXT_AGG"], "expected=%v, actual=%v", true, resultMap["CHECK_NEXT_AGG"])
	assert.Equal(t, 1, len(recs), "expected=%v, actual=%v", 1, len(recs))
	for _, record := range recs {
		assert.Equal(t, fmt.Sprint(sizeLimit), record["count(*)"], "expected=%v, actual=%v", sizeLimit, recs["count(*)"])
	}
}

// Tests the case where the size limit is non-zero and the number of segments is 2.
// A case where the size limit is equal to the records of all the segments. Or It requires all the segments to be processed.
// Number of Segments should be considered.
func Test_PerformMeasureAggsOnRecsSizeLimit_NonZero_EqualToSegments(t *testing.T) {
	numSegements := 2
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

	for i := 0; i < numSegements; i++ {
		recs, finalCols := getMockRecsAndFinalCols(uint64(recsSize))

		if i == numSegements-1 {
			recs, finalCols = getMockRecsAndFinalCols(uint64(sizeLimit - (recsSize * (numSegements - 1))))
		}

		resultMap := PerformAggsOnRecs(nodeResult, aggs, recs, finalCols, uint64(numSegements), true, 1)

		assert.Equal(t, uint64(i+1), nodeResult.RecsAggsProcessedSegments, "expected=%v, actual=%v", uint64(numSegements), nodeResult.RecsAggsProcessedSegments)

		if i == numSegements-1 {
			assert.Equal(t, 1, len(resultMap), "expected=%v, actual=%v", 1, len(resultMap))
			assert.True(t, resultMap["CHECK_NEXT_AGG"], "expected=%v, actual=%v", true, resultMap["CHECK_NEXT_AGG"])
			assert.Equal(t, 1, len(recs), "expected=%v, actual=%v", 1, len(recs))
			for _, record := range recs {
				assert.Equal(t, fmt.Sprint(sizeLimit), record["count(*)"], "expected=%v, actual=%v", sizeLimit, recs["count(*)"])
			}
		} else {
			assert.Nil(t, resultMap, "expected=%v, actual=%v", nil, resultMap)
		}
	}
}

// Tests the case where the size limit is non-zero and the number of segments is 2.
// A case where the size limit is greater than the records of all the segments.
// Number of Segments should be considered.
func Test_PerformMeasureAggsOnRecsSizeLimit_NonZero_GreaterThanSegments(t *testing.T) {
	numSegements := 2
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

	for i := 0; i < numSegements; i++ {
		recs, finalCols := getMockRecsAndFinalCols(uint64(recsSize))

		resultMap := PerformAggsOnRecs(nodeResult, aggs, recs, finalCols, uint64(numSegements), true, 1)

		assert.Equal(t, uint64(i+1), nodeResult.RecsAggsProcessedSegments, "expected=%v, actual=%v", uint64(numSegements), nodeResult.RecsAggsProcessedSegments)

		if i == numSegements-1 {
			assert.Equal(t, 1, len(resultMap), "expected=%v, actual=%v", 1, len(resultMap))
			assert.True(t, resultMap["CHECK_NEXT_AGG"], "expected=%v, actual=%v", true, resultMap["CHECK_NEXT_AGG"])
			assert.Equal(t, 1, len(recs), "expected=%v, actual=%v", 1, len(recs))
			for _, record := range recs {
				assert.Equal(t, fmt.Sprint(recsSize*numSegements), record["count(*)"], "expected=%v, actual=%v", recsSize*numSegements, recs["count(*)"])
			}
		} else {
			assert.Nil(t, resultMap, "expected=%v, actual=%v", nil, resultMap)
		}
	}
}

// Tests the case where the size limit is zero and the number of segments is 2.
// Normal case where sizelimit should not be considered.
// func Test_PerformGroupByRequestAggsOnRecsSizeLimit_Zero_MultiSegment(t *testing.T) {
// 	numSegements := 2
// 	sizeLimit := 0
// 	recsSize := 100
// 	nodeResult := &structs.NodeResult{
// 		PerformAggsOnRecs: true,
// 		RecsAggsType: structs.GroupByType,
// 		GroupByRequest: &structs.GroupByRequest{
// 			MeasureOperations: []*structs.MeasureAggregator{
// 				{
// 					MeasureCol:  "*",
// 					MeasureFunc: utils.Count,
// 					StrEnc:      "count(*)",
// 				},
// 			},
// 			GroupByColumns: []string{"measure3"},
// 			AggName: "agg1",

// 		},
// 	}
// 	aggs := &structs.QueryAggregators{Limit: sizeLimit}
// }
