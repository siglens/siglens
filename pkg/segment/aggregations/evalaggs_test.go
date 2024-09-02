package aggregations

import (
	"fmt"
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func getDummyMeasureAggregator() *structs.MeasureAggregator {
	return &structs.MeasureAggregator{
		MeasureCol:      "vals",
		MeasureFunc:     utils.Sum,
		StrEnc:          "vals",
		ValueColRequest: nil,
	}
}

func getDummySegStats() *structs.SegStats {
	return &structs.SegStats{
		IsNumeric: true,
		Count:     3,
		Hll:       nil,
		NumStats: &structs.NumericStats{
			Min: utils.NumTypeEnclosure{
				Ntype:    utils.SS_DT_FLOAT,
				IntgrVal: 1,
				FloatVal: 1.0,
			},
			Max: utils.NumTypeEnclosure{
				Ntype:    utils.SS_DT_FLOAT,
				IntgrVal: 10,
				FloatVal: 10.0,
			},
			Sum: utils.NumTypeEnclosure{
				Ntype:    utils.SS_DT_FLOAT,
				IntgrVal: 15,
				FloatVal: 15.0,
			},
			Dtype: utils.SS_DT_FLOAT,
		},
		StringStats: &structs.StringStats{
			StrSet: map[string]struct{}{
				"1":  {},
				"4":  {},
				"10": {},
			},
			StrList: []string{"1", "4", "10"},
		},
		Records: []*utils.CValueEnclosure{
			{
				CVal:  1.0,
				Dtype: utils.SS_DT_FLOAT,
			},
			{
				CVal:  4.0,
				Dtype: utils.SS_DT_FLOAT,
			},
			{
				CVal:  10.0,
				Dtype: utils.SS_DT_FLOAT,
			},
		},
	}
}

func TestComputeAggEvalForList(t *testing.T) {
	dummyMeasureAggr := getDummyMeasureAggregator()
	tests := []struct {
		name             string
		measureAgg       *structs.MeasureAggregator
		sstMap           map[string]*structs.SegStats
		measureResults   map[string]utils.CValueEnclosure
		runningEvalStats map[string]interface{}
		expectedError    error
	}{
		{
			name:           "runningEvalStats contains measureAgg.String() but cannot be converted to []string",
			measureAgg:     dummyMeasureAggr,
			sstMap:         map[string]*structs.SegStats{dummyMeasureAggr.MeasureCol: getDummySegStats()},
			measureResults: make(map[string]utils.CValueEnclosure),
			runningEvalStats: map[string]interface{}{
				"someKey": 123, // Invalid type
			},
			expectedError: fmt.Errorf("ComputeAggEvalForList: can not convert to list for measureAgg: %v", "someKey"),
		},
		{
			name:           "fields is empty",
			measureAgg:     dummyMeasureAggr,
			sstMap:         map[string]*structs.SegStats{dummyMeasureAggr.MeasureCol: getDummySegStats()},
			measureResults: make(map[string]utils.CValueEnclosure),
			runningEvalStats: map[string]interface{}{
				"someKey": []string{},
			},
			expectedError: nil,
		},
		{
			name:           "fields is not empty but sstMap does not contain the required key",
			measureAgg:     dummyMeasureAggr,
			sstMap:         map[string]*structs.SegStats{dummyMeasureAggr.MeasureCol: getDummySegStats()},
			measureResults: make(map[string]utils.CValueEnclosure),
			runningEvalStats: map[string]interface{}{
				"someKey": []string{},
			},
			expectedError: fmt.Errorf("ComputeAggEvalForList: sstMap did not have segstats for field %v, measureAgg: %v", "someField", "someKey"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ComputeAggEvalForList(tt.measureAgg, tt.sstMap, tt.measureResults, tt.runningEvalStats)
			if tt.expectedError != nil {
				assert.EqualError(t, err, tt.expectedError.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
