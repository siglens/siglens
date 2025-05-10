package aggregations

import (
	"fmt"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	statswriter "github.com/siglens/siglens/pkg/segment/writer/stats"
	"github.com/stretchr/testify/assert"
)

func getDummyMeasureAggregator() *structs.MeasureAggregator {
	return &structs.MeasureAggregator{
		MeasureCol:      "vals",
		MeasureFunc:     sutils.Sum,
		StrEnc:          "vals",
		ValueColRequest: getDummyNumericValueExpr("vals"),
	}
}

func getDummySegStats() *structs.SegStats {
	return &structs.SegStats{
		IsNumeric: true,
		Count:     3,
		Hll:       nil,
		NumStats:  statswriter.GetDefaultNumStats(),
		StringStats: &structs.StringStats{
			StrSet: map[string]struct{}{
				"1":  {},
				"4":  {},
				"10": {},
			},
			StrList: []string{"1", "4", "10"},
		},
		Records: []*sutils.CValueEnclosure{
			{
				CVal:  1.0,
				Dtype: sutils.SS_DT_FLOAT,
			},
			{
				CVal:  4.0,
				Dtype: sutils.SS_DT_FLOAT,
			},
			{
				CVal:  10.0,
				Dtype: sutils.SS_DT_FLOAT,
			},
		},
	}
}

func getDummyNumericValueExpr(fieldName string) *structs.ValueExpr {
	leftExpr := &structs.NumericExpr{
		NumericExprMode: structs.NEMNumberField,
		IsTerminal:      true,
		ValueIsField:    true,
		Value:           fieldName,
		Op:              "",
		Left:            nil,
		Right:           nil,
	}

	rightExpr := &structs.NumericExpr{
		NumericExprMode: structs.NEMNumber,
		IsTerminal:      true,
		ValueIsField:    false,
		Value:           "2",
		Op:              "",
		Left:            nil,
		Right:           nil,
	}

	expr := &structs.NumericExpr{
		NumericExprMode: structs.NEMNumericExpr,
		IsTerminal:      false,
		ValueIsField:    false,
		Value:           "",
		Op:              "/",
		Left:            leftExpr,
		Right:           rightExpr,
	}

	return &structs.ValueExpr{
		NumericExpr:   expr,
		ValueExprMode: structs.VEMNumericExpr,
	}
}

func getDummyNumericValueExprWithoutField() *structs.ValueExpr {
	expr := &structs.NumericExpr{
		NumericExprMode: structs.NEMNumber,
		IsTerminal:      true,
		ValueIsField:    false,
		Value:           "100",
		Op:              "",
		Left:            nil,
		Right:           nil,
	}

	return &structs.ValueExpr{
		NumericExpr:   expr,
		ValueExprMode: structs.VEMNumericExpr,
	}
}

func TestComputeAggEvalForList_InvalidRunningEvalStatsConversion(t *testing.T) {
	dummyMeasureAggr := getDummyMeasureAggregator()
	runningEvalStats := map[string]interface{}{
		"vals": 123, // Invalid type, cannot convert to a list.
	}
	sstMap := map[string]*structs.SegStats{dummyMeasureAggr.MeasureCol: getDummySegStats()}
	measureResults := make(map[string]sutils.CValueEnclosure)

	expectedError := fmt.Errorf("ComputeAggEvalForList: can not convert to list for measureAgg: %v", "vals")

	err := ComputeAggEvalForList(dummyMeasureAggr, sstMap, measureResults, runningEvalStats)
	assert.EqualError(t, err, expectedError.Error())
}

func TestComputeAggEvalForList_EmptyFields(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())
	dummyMeasureAggr := getDummyMeasureAggregator()
	runningEvalStats := map[string]interface{}{
		"vals": []string{},
	}
	timestampKey := config.GetTimeStampKey()
	dummyMeasureAggr.ValueColRequest = getDummyNumericValueExprWithoutField()
	sstMap := map[string]*structs.SegStats{dummyMeasureAggr.MeasureCol: getDummySegStats(), timestampKey: getDummySegStats()}
	measureResults := make(map[string]sutils.CValueEnclosure)

	err := ComputeAggEvalForList(dummyMeasureAggr, sstMap, measureResults, runningEvalStats)
	assert.NoError(t, err)
}

func TestComputeAggEvalForList_MissingSSTMapKey(t *testing.T) {
	dummyMeasureAggr := getDummyMeasureAggregator()
	runningEvalStats := map[string]interface{}{
		"vals": []string{},
	}
	sstMap := map[string]*structs.SegStats{"otherKey": getDummySegStats()}
	measureResults := make(map[string]sutils.CValueEnclosure)

	expectedError := fmt.Errorf("ComputeAggEvalForList: sstMap did not have segstats for field %v, measureAgg: %v", "vals", "vals")

	err := ComputeAggEvalForList(dummyMeasureAggr, sstMap, measureResults, runningEvalStats)
	assert.EqualError(t, err, expectedError.Error())
}

func TestComputeAggEvalForList_CorrectInputsWithoutField(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())
	timestampKey := config.GetTimeStampKey()

	dummyMeasureAggr := getDummyMeasureAggregator()
	runningEvalStats := map[string]interface{}{
		"vals": []string{},
	}

	dummyMeasureAggr.ValueColRequest = getDummyNumericValueExprWithoutField()
	sstMap := map[string]*structs.SegStats{"vals": getDummySegStats(), timestampKey: getDummySegStats()}
	measureResults := make(map[string]sutils.CValueEnclosure)
	expected := []string{"100", "100", "100"}
	err := ComputeAggEvalForList(dummyMeasureAggr, sstMap, measureResults, runningEvalStats)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(measureResults["vals"].CVal.([]string)), "The length of the measureResults slice should be 1")
	assert.Equal(t, 3, len(runningEvalStats["vals"].([]string)), "The length of the runningEvalStats slice should be 1")
	assert.Equal(t, expected, measureResults["vals"].CVal.([]string), "The measureResults slice should be equal to the expected slice")
	assert.Equal(t, expected, runningEvalStats["vals"].([]string), "The runningEvalStats slice should be equal to the expected slice")
}

func TestComputeAggEvalForList_CorrectInputsWithField(t *testing.T) {
	dummyMeasureAggr := getDummyMeasureAggregator()
	runningEvalStats := map[string]interface{}{
		"vals": []string{},
	}
	sstMap := map[string]*structs.SegStats{"vals": getDummySegStats()}
	measureResults := make(map[string]sutils.CValueEnclosure)
	expected := []string{"0.5", "2", "5"}
	err := ComputeAggEvalForList(dummyMeasureAggr, sstMap, measureResults, runningEvalStats)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(measureResults["vals"].CVal.([]string)), "The length of the measureResults slice should be 3")
	assert.Equal(t, 3, len(runningEvalStats["vals"].([]string)), "The length of the runningEvalStats slice should be 3")
	assert.Equal(t, expected, measureResults["vals"].CVal.([]string), "The measureResults slice should be equal to the expected slice")
	assert.Equal(t, expected, runningEvalStats["vals"].([]string), "The runningEvalStats slice should be equal to the expected slice")
}

func TestComputeAggEvalForList_largeLists(t *testing.T) {
	dummyMeasureAggr := getDummyMeasureAggregator()
	runningEvalStats := map[string]interface{}{
		"vals": []string{},
	}
	list := make([]*sutils.CValueEnclosure, sutils.MAX_SPL_LIST_SIZE+100)

	for i := range list {
		list[i] = &sutils.CValueEnclosure{
			CVal:  1.0,
			Dtype: sutils.SS_DT_FLOAT,
		}
	}
	sstMap := map[string]*structs.SegStats{"vals": getDummySegStats()}
	sstMap["vals"].Records = list
	measureResults := make(map[string]sutils.CValueEnclosure)
	err := ComputeAggEvalForList(dummyMeasureAggr, sstMap, measureResults, runningEvalStats)
	assert.NoError(t, err)
	assert.Equal(t, sutils.MAX_SPL_LIST_SIZE, len(measureResults["vals"].CVal.([]string)), "The length of the measureResults slice should be MAX_SPL_LIST_SIZE")
	assert.Equal(t, sutils.MAX_SPL_LIST_SIZE, len(runningEvalStats["vals"].([]string)), "The length of the runningEvalStats slice should be MAX_SPL_LIST_SIZE")
}

func TestComputeAggEvalForList_TestUpdateWithMultipleEvals(t *testing.T) {
	dummyMeasureAggr := getDummyMeasureAggregator()
	runningEvalStats := map[string]interface{}{
		"vals": []string{},
	}
	sstMap := map[string]*structs.SegStats{"vals": getDummySegStats()}
	measureResults := make(map[string]sutils.CValueEnclosure)
	expected := []string{"0.5", "2", "5"}
	err := ComputeAggEvalForList(dummyMeasureAggr, sstMap, measureResults, runningEvalStats)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(measureResults["vals"].CVal.([]string)), "The length of the measureResults slice should be 3")
	assert.Equal(t, 3, len(runningEvalStats["vals"].([]string)), "The length of the runningEvalStats slice should be 3")
	assert.Equal(t, expected, measureResults["vals"].CVal.([]string), "The measureResults slice should be equal to the expected slice")
	assert.Equal(t, expected, runningEvalStats["vals"].([]string), "The runningEvalStats slice should be equal to the expected slice")

	expected = []string{"0.5", "2", "5", "0.5", "2", "5"}
	measureResults = make(map[string]sutils.CValueEnclosure)
	err = ComputeAggEvalForList(dummyMeasureAggr, sstMap, measureResults, runningEvalStats)
	assert.NoError(t, err)
	assert.Equal(t, 6, len(measureResults["vals"].CVal.([]string)), "The length of the measureResults slice should be 6")
	assert.Equal(t, 6, len(runningEvalStats["vals"].([]string)), "The length of the runningEvalStats slice should be 6")
	assert.Equal(t, expected, measureResults["vals"].CVal.([]string), "The measureResults slice should be equal to the expected slice")
	assert.Equal(t, expected, runningEvalStats["vals"].([]string), "The runningEvalStats slice should be equal to the expected slice")

}

func TestComputeAggEvalForValues_NoFields(t *testing.T) {
	measureAgg := getDummyMeasureAggregator()
	sstMap := map[string]*structs.SegStats{"vals": getDummySegStats()}
	measureResults := map[string]sutils.CValueEnclosure{}
	runningEvalStats := map[string]interface{}{
		"vals": map[string]struct{}{},
	}
	measureAgg.ValueColRequest = getDummyNumericValueExprWithoutField()

	err := ComputeAggEvalForValues(measureAgg, sstMap, measureResults, runningEvalStats)
	assert.NoError(t, err)

	result, ok := measureResults[measureAgg.String()]
	assert.True(t, ok)
	assert.Equal(t, sutils.SS_DT_STRING_SLICE, result.Dtype)
}

func TestComputeAggEvalForValues_MissingSSTMapKey(t *testing.T) {
	measureAgg := getDummyMeasureAggregator()
	sstMap := map[string]*structs.SegStats{"other": getDummySegStats()}
	measureResults := map[string]sutils.CValueEnclosure{}
	runningEvalStats := map[string]interface{}{
		"vals": map[string]struct{}{},
	}

	err := ComputeAggEvalForValues(measureAgg, sstMap, measureResults, runningEvalStats)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "sstMap did not have segstats")
}

func TestComputeAggEvalForValues_InvalidStrSetConversion(t *testing.T) {
	measureAgg := getDummyMeasureAggregator()
	sstMap := map[string]*structs.SegStats{"vals": getDummySegStats()}
	measureResults := map[string]sutils.CValueEnclosure{}
	runningEvalStats := map[string]interface{}{
		"vals": []string{},
	}

	err := ComputeAggEvalForValues(measureAgg, sstMap, measureResults, runningEvalStats)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "can not convert strSet")
}

func TestComputeAggEvalForValues_WithFieldsAndValidData(t *testing.T) {
	measureAgg := getDummyMeasureAggregator()
	sstMap := map[string]*structs.SegStats{"vals": getDummySegStats()}
	measureResults := map[string]sutils.CValueEnclosure{}
	runningEvalStats := map[string]interface{}{
		"vals": map[string]struct{}{},
	}

	err := ComputeAggEvalForValues(measureAgg, sstMap, measureResults, runningEvalStats)
	assert.NoError(t, err)
	expected := []string{"0.5", "2", "5"}
	result, ok := measureResults[measureAgg.String()]
	assert.True(t, ok)
	assert.Equal(t, sutils.SS_DT_STRING_SLICE, result.Dtype)

	uniqueStrings, ok := result.CVal.([]string)
	assert.True(t, ok)
	assert.Equal(t, expected, uniqueStrings)
}

func TestComputeAggEvalForValues_WithoutFieldsAndValidData(t *testing.T) {
	measureAgg := getDummyMeasureAggregator()
	sstMap := map[string]*structs.SegStats{"vals": getDummySegStats()}
	measureResults := map[string]sutils.CValueEnclosure{}
	runningEvalStats := map[string]interface{}{
		"vals": map[string]struct{}{},
	}
	measureAgg.ValueColRequest = getDummyNumericValueExprWithoutField()
	err := ComputeAggEvalForValues(measureAgg, sstMap, measureResults, runningEvalStats)
	assert.NoError(t, err)
	expected := []string{"100"}
	result, ok := measureResults[measureAgg.String()]
	assert.True(t, ok)
	assert.Equal(t, sutils.SS_DT_STRING_SLICE, result.Dtype)

	uniqueStrings, ok := result.CVal.([]string)
	assert.True(t, ok)
	assert.Equal(t, expected, uniqueStrings)
}

func TestComputeAggEvalForValues_UpdateWithMultipleEvals(t *testing.T) {
	measureAgg := getDummyMeasureAggregator()
	sstMap := map[string]*structs.SegStats{"vals": getDummySegStats()}
	measureResults := map[string]sutils.CValueEnclosure{}
	runningEvalStats := map[string]interface{}{
		"vals": map[string]struct{}{},
	}

	err := ComputeAggEvalForValues(measureAgg, sstMap, measureResults, runningEvalStats)
	assert.NoError(t, err)
	expected := []string{"0.5", "2", "5"}
	result, ok := measureResults[measureAgg.String()]
	assert.True(t, ok)
	assert.Equal(t, sutils.SS_DT_STRING_SLICE, result.Dtype)

	uniqueStrings, ok := result.CVal.([]string)
	assert.True(t, ok)
	assert.Equal(t, expected, uniqueStrings)

	err = ComputeAggEvalForValues(measureAgg, sstMap, measureResults, runningEvalStats)
	assert.NoError(t, err)
	result, ok = measureResults[measureAgg.String()]
	assert.True(t, ok)
	assert.Equal(t, sutils.SS_DT_STRING_SLICE, result.Dtype)

	uniqueStrings, ok = result.CVal.([]string)
	assert.True(t, ok)
	// expected remains the same
	assert.Equal(t, expected, uniqueStrings)
}

func TestComputeAggEvalForValues_UpdateWithMultipleEvalsWithoutField(t *testing.T) {
	measureAgg := getDummyMeasureAggregator()
	sstMap := map[string]*structs.SegStats{"vals": getDummySegStats()}
	measureResults := map[string]sutils.CValueEnclosure{}
	runningEvalStats := map[string]interface{}{
		"vals": map[string]struct{}{},
	}
	measureAgg.ValueColRequest = getDummyNumericValueExprWithoutField()
	err := ComputeAggEvalForValues(measureAgg, sstMap, measureResults, runningEvalStats)
	assert.NoError(t, err)
	expected := []string{"100"}
	result, ok := measureResults[measureAgg.String()]
	assert.True(t, ok)
	assert.Equal(t, sutils.SS_DT_STRING_SLICE, result.Dtype)

	uniqueStrings, ok := result.CVal.([]string)
	assert.True(t, ok)
	assert.Equal(t, expected, uniqueStrings)

	err = ComputeAggEvalForValues(measureAgg, sstMap, measureResults, runningEvalStats)
	assert.NoError(t, err)
	result, ok = measureResults[measureAgg.String()]
	assert.True(t, ok)
	assert.Equal(t, sutils.SS_DT_STRING_SLICE, result.Dtype)

	uniqueStrings, ok = result.CVal.([]string)
	assert.True(t, ok)
	// expected remains the same
	assert.Equal(t, expected, uniqueStrings)
}
