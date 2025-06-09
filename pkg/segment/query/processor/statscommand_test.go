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

package processor

import (
	"io"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func getTestData() map[string][]sutils.CValueEnclosure {
	knownValues := map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			{Dtype: sutils.SS_DT_STRING, CVal: "e"},
			{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		},
		"col2": {
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(1)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(2)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(3)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(4)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(5)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(6)},
		},
		"col3": {
			{Dtype: sutils.SS_DT_STRING, CVal: "z"},
			{Dtype: sutils.SS_DT_STRING, CVal: "y"},
			{Dtype: sutils.SS_DT_STRING, CVal: "x"},
			{Dtype: sutils.SS_DT_STRING, CVal: "w"},
			{Dtype: sutils.SS_DT_STRING, CVal: "v"},
			{Dtype: sutils.SS_DT_STRING, CVal: "u"},
		},
	}

	return knownValues
}

func getGroupByProcessor() *statsProcessor {
	measureOperations := []*structs.MeasureAggregator{
		{
			MeasureCol:  "col2",
			MeasureFunc: sutils.Count,
		},
		{
			MeasureCol:  "col2",
			MeasureFunc: sutils.Sum,
		},
		{
			MeasureCol:  "col2",
			MeasureFunc: sutils.Avg,
		},
	}

	groupByCols := []string{"col1", "col3"}

	processor := NewStatsProcessor(
		&structs.StatsExpr{
			GroupByRequest: &structs.GroupByRequest{
				MeasureOperations: measureOperations,
				GroupByColumns:    groupByCols,
			},
		},
	)

	return processor
}

func Test_ProcessGroupByRequest_AllColsExist(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())
	config.GetRunningConfig().UseNewPipelineConverted = true

	knownValues := getTestData()
	processor := getGroupByProcessor()

	iqr1 := iqr.NewIQR(0)

	err := iqr1.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	_, err = processor.Process(iqr1)
	assert.NoError(t, err)

	resultIqr, err := processor.Process(nil)
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, resultIqr)

	// Check the results
	actualKnownValues, err := resultIqr.ReadAllColumns()
	assert.NoError(t, err)
	assert.NotNil(t, actualKnownValues)

	expectedCountRes := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
	}

	countColName := "count(col2)"
	actualCountRes, ok := actualKnownValues[countColName]
	assert.True(t, ok)
	assert.ElementsMatch(t, expectedCountRes, actualCountRes)

	expectedSumRes := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(1)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(2)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(3)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(4)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(5)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(6)},
	}

	sumColName := "sum(col2)"
	actualSumRes, ok := actualKnownValues[sumColName]
	assert.True(t, ok)
	assert.ElementsMatch(t, expectedSumRes, actualSumRes)

	expectedAvgRes := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(1)},
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(2)},
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(3)},
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(4)},
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(5)},
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(6)},
	}

	avgColName := "avg(col2)"
	actualAvgRes, ok := actualKnownValues[avgColName]
	assert.True(t, ok)
	assert.ElementsMatch(t, expectedAvgRes, actualAvgRes)

	expectedGroupByCols := map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			{Dtype: sutils.SS_DT_STRING, CVal: "e"},
			{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		},
		"col3": {
			{Dtype: sutils.SS_DT_STRING, CVal: "z"},
			{Dtype: sutils.SS_DT_STRING, CVal: "y"},
			{Dtype: sutils.SS_DT_STRING, CVal: "x"},
			{Dtype: sutils.SS_DT_STRING, CVal: "w"},
			{Dtype: sutils.SS_DT_STRING, CVal: "v"},
			{Dtype: sutils.SS_DT_STRING, CVal: "u"},
		},
	}

	for colName, expectedValues := range expectedGroupByCols {
		actualValues, ok := actualKnownValues[colName]
		assert.True(t, ok)
		assert.ElementsMatch(t, expectedValues, actualValues)
	}
}

func Test_ProcessGroupByRequest_SomeColsMissing(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())
	config.GetRunningConfig().UseNewPipelineConverted = true

	knownValues := getTestData()
	processor := getGroupByProcessor()

	iqr1 := iqr.NewIQR(0)

	err := iqr1.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	_, err = processor.Process(iqr1)
	assert.NoError(t, err)

	// remove Col3 from the known values
	delete(knownValues, "col3")

	iqr2 := iqr.NewIQR(0)
	err = iqr2.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	_, err = processor.Process(iqr2)
	assert.NoError(t, err)

	resultIqr, err := processor.Process(nil)
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, resultIqr)

	// Check the results
	actualKnownValues, err := resultIqr.ReadAllColumns()
	assert.NoError(t, err)
	assert.NotNil(t, actualKnownValues)

	expectedCountRes := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)}, // Missing col3, so its "a~nil" repeated twice
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)}, // Missing col3, so its "b~nil" repeated twice
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
	}

	countColName := "count(col2)"
	actualCountRes, ok := actualKnownValues[countColName]
	assert.True(t, ok)
	assert.ElementsMatch(t, expectedCountRes, actualCountRes)

	expectedSumRes := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(1)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(9)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(4)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(3)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(5)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(4)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(6)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(7)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(1)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(2)},
	}

	sumColName := "sum(col2)"
	actualSumRes, ok := actualKnownValues[sumColName]
	assert.True(t, ok)
	assert.ElementsMatch(t, expectedSumRes, actualSumRes)

	expectedAvgRes := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(1)},
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(4.5)},
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(4)},
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(3)},
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(5)},
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(4)},
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(6)},
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(3.5)},
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(1)},
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(2)},
	}

	avgColName := "avg(col2)"
	actualAvgRes, ok := actualKnownValues[avgColName]
	assert.True(t, ok)
	assert.ElementsMatch(t, expectedAvgRes, actualAvgRes)

	expectedGroupByCols := map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "e"},
			{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			{Dtype: sutils.SS_DT_STRING, CVal: "e"},
			{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			{Dtype: sutils.SS_DT_STRING, CVal: "a"},
		},
		"col3": {
			{Dtype: sutils.SS_DT_STRING, CVal: "w"},
			{Dtype: sutils.SS_DT_BACKFILL, CVal: nil},
			{Dtype: sutils.SS_DT_BACKFILL, CVal: nil},
			{Dtype: sutils.SS_DT_BACKFILL, CVal: nil},
			{Dtype: sutils.SS_DT_STRING, CVal: "z"},
			{Dtype: sutils.SS_DT_STRING, CVal: "x"},
			{Dtype: sutils.SS_DT_STRING, CVal: "v"},
			{Dtype: sutils.SS_DT_STRING, CVal: "u"},
			{Dtype: sutils.SS_DT_BACKFILL, CVal: nil},
			{Dtype: sutils.SS_DT_STRING, CVal: "y"},
		},
	}

	for colName, expectedValues := range expectedGroupByCols {
		actualValues, ok := actualKnownValues[colName]
		assert.True(t, ok)
		assert.ElementsMatch(t, expectedValues, actualValues)
	}
}

func Test_ProcessGroupByRequest_MergeIqrStats(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())
	config.GetRunningConfig().UseNewPipelineConverted = true

	knownValues := getTestData()
	processor := getGroupByProcessor()
	processor.setAsIqrStatsResults = true

	iqr1 := iqr.NewIQR(0)

	err := iqr1.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	_, err = processor.Process(iqr1)
	assert.NoError(t, err)

	resultIqr1, err := processor.Process(nil)
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, resultIqr1)

	_, groupByBuckets, _ := resultIqr1.GetIQRStatsResults()
	assert.NotNil(t, groupByBuckets)

	iqr2 := iqr.NewIQR(0)
	processor = getGroupByProcessor()
	processor.setAsIqrStatsResults = true

	err = iqr2.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	_, err = processor.Process(iqr2)
	assert.NoError(t, err)

	resultIqr2, err := processor.Process(nil)
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, resultIqr2)

	_, groupByBuckets, _ = resultIqr2.GetIQRStatsResults()
	assert.NotNil(t, groupByBuckets)

	iqr3 := iqr.NewIQR(0)
	processor = getGroupByProcessor()
	processor.setAsIqrStatsResults = true

	err = iqr3.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	_, err = processor.Process(iqr3)
	assert.NoError(t, err)

	resultIqr3, err := processor.Process(nil)
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, resultIqr3)

	_, groupByBuckets, _ = resultIqr3.GetIQRStatsResults()
	assert.NotNil(t, groupByBuckets)

	iqrs := []*iqr.IQR{resultIqr1, resultIqr2, resultIqr3}

	resultIqr := iqr.NewIQR(0)
	statsExists, err := resultIqr.MergeIQRStatsResults(iqrs)
	assert.NoError(t, err)
	assert.True(t, statsExists)

	knownValues, err = resultIqr.ReadAllColumns()
	assert.NoError(t, err)

	expectedKnownValues := map[string][]sutils.CValueEnclosure{
		"count(col2)": {
			{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
			{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
			{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
			{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
			{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
			{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
		},
		"sum(col2)": {
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(3)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(6)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(9)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(12)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(15)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(18)},
		},
		"avg(col2)": {
			{Dtype: sutils.SS_DT_FLOAT, CVal: float64(1)},
			{Dtype: sutils.SS_DT_FLOAT, CVal: float64(2)},
			{Dtype: sutils.SS_DT_FLOAT, CVal: float64(3)},
			{Dtype: sutils.SS_DT_FLOAT, CVal: float64(4)},
			{Dtype: sutils.SS_DT_FLOAT, CVal: float64(5)},
			{Dtype: sutils.SS_DT_FLOAT, CVal: float64(6)},
		},
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			{Dtype: sutils.SS_DT_STRING, CVal: "e"},
			{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		},
		"col3": {
			{Dtype: sutils.SS_DT_STRING, CVal: "z"},
			{Dtype: sutils.SS_DT_STRING, CVal: "y"},
			{Dtype: sutils.SS_DT_STRING, CVal: "x"},
			{Dtype: sutils.SS_DT_STRING, CVal: "w"},
			{Dtype: sutils.SS_DT_STRING, CVal: "v"},
			{Dtype: sutils.SS_DT_STRING, CVal: "u"},
		},
	}

	columnNames := []string{"count(col2)", "sum(col2)", "avg(col2)", "col1", "col3"}
	for _, colName := range columnNames {
		actualValues, ok := knownValues[colName]
		assert.True(t, ok)
		assert.ElementsMatch(t, expectedKnownValues[colName], actualValues)
	}
}

func getStatsMeasureProcessor() *statsProcessor {
	measureOperations := []*structs.MeasureAggregator{
		{
			MeasureCol:  "col2",
			MeasureFunc: sutils.Count,
		},
		{
			MeasureCol:  "col2",
			MeasureFunc: sutils.Sum,
		},
		{
			MeasureCol:  "col2",
			MeasureFunc: sutils.Avg,
		},
	}

	processor := NewStatsProcessor(
		&structs.StatsExpr{
			MeasureOperations: measureOperations,
		},
	)

	return processor
}

func Test_ProcessSegmentStats(t *testing.T) {
	knownValues := getTestData()
	processor := getStatsMeasureProcessor()

	iqr1 := iqr.NewIQR(0)

	err := iqr1.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	_, err = processor.Process(iqr1)
	assert.NoError(t, err)

	resultIqr, err := processor.Process(nil)
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, resultIqr)

	// Check the results
	actualKnownValues, err := resultIqr.ReadAllColumns()
	assert.NoError(t, err)
	assert.NotNil(t, actualKnownValues)

	expectedCountRes := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(6)},
	}

	countColName := "count(col2)"
	actualCountRes, ok := actualKnownValues[countColName]
	assert.True(t, ok)
	assert.Equal(t, 1, len(actualCountRes))
	assert.Equal(t, expectedCountRes, actualCountRes)

	expectedSumRes := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(21)},
	}

	sumColName := "sum(col2)"
	actualSumRes, ok := actualKnownValues[sumColName]
	assert.True(t, ok)
	assert.Equal(t, 1, len(actualSumRes))
	assert.Equal(t, expectedSumRes, actualSumRes)

	expectedAvgRes := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_FLOAT, CVal: float64(3.5)},
	}

	avgColName := "avg(col2)"
	actualAvgRes, ok := actualKnownValues[avgColName]
	assert.True(t, ok)
	assert.Equal(t, 1, len(actualAvgRes))
	assert.Equal(t, expectedAvgRes, actualAvgRes)
}

func Test_ProcessSegmentStats_MergeIqrStats(t *testing.T) {
	knownValues := getTestData()
	processor := getStatsMeasureProcessor()
	processor.setAsIqrStatsResults = true

	iqr1 := iqr.NewIQR(0)

	err := iqr1.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	_, err = processor.Process(iqr1)
	assert.NoError(t, err)

	resultIqr1, err := processor.Process(nil)
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, resultIqr1)

	segStatsMap, _, _ := resultIqr1.GetIQRStatsResults()
	assert.NotNil(t, segStatsMap)
	assert.Equal(t, 1, len(segStatsMap))

	iqr2 := iqr.NewIQR(0)
	processor = getStatsMeasureProcessor()
	processor.setAsIqrStatsResults = true

	err = iqr2.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	_, err = processor.Process(iqr2)
	assert.NoError(t, err)

	resultIqr2, err := processor.Process(nil)
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, resultIqr2)

	segStatsMap, _, _ = resultIqr2.GetIQRStatsResults()
	assert.NotNil(t, segStatsMap)

	iqr3 := iqr.NewIQR(0)
	processor = getStatsMeasureProcessor()
	processor.setAsIqrStatsResults = true

	err = iqr3.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	_, err = processor.Process(iqr3)
	assert.NoError(t, err)

	resultIqr3, err := processor.Process(nil)
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, resultIqr3)

	segStatsMap, _, _ = resultIqr3.GetIQRStatsResults()
	assert.NotNil(t, segStatsMap)

	iqrs := []*iqr.IQR{resultIqr1, resultIqr2, resultIqr3}

	resultIqr := iqr.NewIQR(0)
	statsExists, err := resultIqr.MergeIQRStatsResults(iqrs)
	assert.NoError(t, err)
	assert.True(t, statsExists)

	knownValues, err = resultIqr.ReadAllColumns()
	assert.NoError(t, err)

	expectedKnownValues := map[string][]sutils.CValueEnclosure{
		"count(col2)": {
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(18)},
		},
		"sum(col2)": {
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(63)},
		},
		"avg(col2)": {
			{Dtype: sutils.SS_DT_FLOAT, CVal: float64(3.5)},
		},
	}

	assert.Equal(t, expectedKnownValues, knownValues)
}

func Test_ProcessGroupByRequestFullBuffer(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())
	config.GetRunningConfig().UseNewPipelineConverted = true

	knownValues := getTestData()
	processor := getGroupByProcessor()
	assert.Equal(t, 0, len(processor.bucketKeyWorkingBuf))

	col1Values := knownValues["col1"]
	factorSize := 5 // should be greater than len(groupByCols)

	for i := 0; i < len(col1Values); i++ {
		byteVal := make([]byte, sutils.MAX_RECORD_SIZE*factorSize)

		for j := 0; j < sutils.MAX_RECORD_SIZE; j++ {
			byteVal[j] = byte(col1Values[i].CVal.(string)[0])
		}

		knownValues["col1"][i].CVal = string(byteVal)
	}

	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	_, err = processor.Process(iqr1)
	assert.NoError(t, err)

	// value of col1 at each record is utils.MAX_RECORD_SIZE * factor bytes
	// +
	// value of col3 at each record is 1 byte
	estimatedSizeOfBuffer := (sutils.MAX_RECORD_SIZE * factorSize) + 1

	// the p.bucketKeyWorkingBuf should be at least the size of the estimated buffer
	assert.GreaterOrEqual(t, len(processor.bucketKeyWorkingBuf), estimatedSizeOfBuffer, "bucketKeyWorkingBuf size is less than estimated buffer size")
}

// Test_MergeIQRStatsResults_DifferentBuckets tests merging IQRs where each has completely different buckets.
// IQR1 has buckets for keys: "A~X", "B~Y"
// IQR2 has buckets for keys: "C~Z", "D~W"
// Expected result: merged IQR should contain all 4 buckets with their individual stats.
func Test_MergeIQRStatsResults_DifferentBuckets(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())
	config.GetRunningConfig().UseNewPipelineConverted = true

	// Create IQR1 with data that produces buckets "A~X" and "B~Y"
	iqr1 := iqr.NewIQR(0)
	iqr1Data := map[string][]sutils.CValueEnclosure{
		"category": {
			{Dtype: sutils.SS_DT_STRING, CVal: "A"},
			{Dtype: sutils.SS_DT_STRING, CVal: "B"},
		},
		"type": {
			{Dtype: sutils.SS_DT_STRING, CVal: "X"},
			{Dtype: sutils.SS_DT_STRING, CVal: "Y"},
		},
		"value": {
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(10)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(20)},
		},
	}
	err := iqr1.AppendKnownValues(iqr1Data)
	assert.NoError(t, err)

	processor1 := NewStatsProcessor(&structs.StatsExpr{
		GroupByRequest: &structs.GroupByRequest{
			GroupByColumns: []string{"category", "type"},
			MeasureOperations: []*structs.MeasureAggregator{
				{MeasureCol: "value", MeasureFunc: sutils.Count},
				{MeasureCol: "value", MeasureFunc: sutils.Sum},
			},
		},
	})
	processor1.setAsIqrStatsResults = true
	_, err = processor1.Process(iqr1)
	assert.NoError(t, err)
	resultIqr1, err := processor1.Process(nil)
	assert.Equal(t, io.EOF, err)

	// Create IQR2 with data that produces buckets "C~Z" and "D~W"
	iqr2 := iqr.NewIQR(0)
	iqr2Data := map[string][]sutils.CValueEnclosure{
		"category": {
			{Dtype: sutils.SS_DT_STRING, CVal: "C"},
			{Dtype: sutils.SS_DT_STRING, CVal: "D"},
		},
		"type": {
			{Dtype: sutils.SS_DT_STRING, CVal: "Z"},
			{Dtype: sutils.SS_DT_STRING, CVal: "W"},
		},
		"value": {
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(30)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(40)},
		},
	}
	err = iqr2.AppendKnownValues(iqr2Data)
	assert.NoError(t, err)

	processor2 := NewStatsProcessor(&structs.StatsExpr{
		GroupByRequest: &structs.GroupByRequest{
			GroupByColumns: []string{"category", "type"},
			MeasureOperations: []*structs.MeasureAggregator{
				{MeasureCol: "value", MeasureFunc: sutils.Count},
				{MeasureCol: "value", MeasureFunc: sutils.Sum},
			},
		},
	})
	processor2.setAsIqrStatsResults = true
	_, err = processor2.Process(iqr2)
	assert.NoError(t, err)
	resultIqr2, err := processor2.Process(nil)
	assert.Equal(t, io.EOF, err)

	// Merge the IQRs
	mergedIqr := iqr.NewIQR(0)
	iqrs := []*iqr.IQR{resultIqr1, resultIqr2}
	statsExists, err := mergedIqr.MergeIQRStatsResults(iqrs)
	assert.NoError(t, err)
	assert.True(t, statsExists)

	// Verify merged results contain all 4 buckets
	knownValues, err := mergedIqr.ReadAllColumns()
	assert.NoError(t, err)

	// Should have 4 rows - one for each bucket: A~X, B~Y, C~Z, D~W
	assert.Len(t, knownValues["category"], 4)
	assert.Len(t, knownValues["count(value)"], 4)
	assert.Len(t, knownValues["sum(value)"], 4)

	// Verify each bucket has count=1 and correct sum
	expectedCounts := []uint64{1, 1, 1, 1}
	expectedSums := []int64{10, 20, 30, 40}

	actualCounts := make([]uint64, len(knownValues["count(value)"]))
	actualSums := make([]int64, len(knownValues["sum(value)"]))

	for i := range knownValues["count(value)"] {
		actualCounts[i] = knownValues["count(value)"][i].CVal.(uint64)
		actualSums[i] = knownValues["sum(value)"][i].CVal.(int64)
	}

	assert.ElementsMatch(t, expectedCounts, actualCounts)
	assert.ElementsMatch(t, expectedSums, actualSums)
}

// Test_MergeIQRStatsResults_OverlappingBuckets tests merging IQRs where some buckets overlap.
// IQR1 has buckets: "A~X" (count=1, sum=10), "B~Y" (count=1, sum=20)
// IQR2 has buckets: "A~X" (count=1, sum=15), "C~Z" (count=1, sum=30)
// Expected result: "A~X" combined (count=2, sum=25), "B~Y" unchanged, "C~Z" unchanged
func Test_MergeIQRStatsResults_OverlappingBuckets(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())
	config.GetRunningConfig().UseNewPipelineConverted = true

	// Create IQR1 with "A~X"=10, "B~Y"=20
	iqr1 := iqr.NewIQR(0)
	iqr1Data := map[string][]sutils.CValueEnclosure{
		"category": {
			{Dtype: sutils.SS_DT_STRING, CVal: "A"},
			{Dtype: sutils.SS_DT_STRING, CVal: "B"},
		},
		"type": {
			{Dtype: sutils.SS_DT_STRING, CVal: "X"},
			{Dtype: sutils.SS_DT_STRING, CVal: "Y"},
		},
		"value": {
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(10)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(20)},
		},
	}
	err := iqr1.AppendKnownValues(iqr1Data)
	assert.NoError(t, err)

	processor1 := NewStatsProcessor(&structs.StatsExpr{
		GroupByRequest: &structs.GroupByRequest{
			GroupByColumns: []string{"category", "type"},
			MeasureOperations: []*structs.MeasureAggregator{
				{MeasureCol: "value", MeasureFunc: sutils.Count},
				{MeasureCol: "value", MeasureFunc: sutils.Sum},
			},
		},
	})
	processor1.setAsIqrStatsResults = true
	_, err = processor1.Process(iqr1)
	assert.NoError(t, err)
	resultIqr1, err := processor1.Process(nil)
	assert.Equal(t, io.EOF, err)

	// Create IQR2 with "A~X"=15, "C~Z"=30 (overlaps with IQR1 on "A~X")
	iqr2 := iqr.NewIQR(0)
	iqr2Data := map[string][]sutils.CValueEnclosure{
		"category": {
			{Dtype: sutils.SS_DT_STRING, CVal: "A"}, // Same as IQR1's first record
			{Dtype: sutils.SS_DT_STRING, CVal: "C"}, // New bucket
		},
		"type": {
			{Dtype: sutils.SS_DT_STRING, CVal: "X"}, // Same as IQR1's first record
			{Dtype: sutils.SS_DT_STRING, CVal: "Z"}, // New bucket
		},
		"value": {
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(15)}, // Different value for A~X
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(30)}, // New bucket value
		},
	}
	err = iqr2.AppendKnownValues(iqr2Data)
	assert.NoError(t, err)

	processor2 := NewStatsProcessor(&structs.StatsExpr{
		GroupByRequest: &structs.GroupByRequest{
			GroupByColumns: []string{"category", "type"},
			MeasureOperations: []*structs.MeasureAggregator{
				{MeasureCol: "value", MeasureFunc: sutils.Count},
				{MeasureCol: "value", MeasureFunc: sutils.Sum},
			},
		},
	})
	processor2.setAsIqrStatsResults = true
	_, err = processor2.Process(iqr2)
	assert.NoError(t, err)
	resultIqr2, err := processor2.Process(nil)
	assert.Equal(t, io.EOF, err)

	// Merge the IQRs
	mergedIqr := iqr.NewIQR(0)
	iqrs := []*iqr.IQR{resultIqr1, resultIqr2}
	statsExists, err := mergedIqr.MergeIQRStatsResults(iqrs)
	assert.NoError(t, err)
	assert.True(t, statsExists)

	// Verify merged results
	knownValues, err := mergedIqr.ReadAllColumns()
	assert.NoError(t, err)

	// Should have 3 unique buckets: A~X (merged), B~Y, C~Z
	assert.Len(t, knownValues["category"], 3)

	// Find the merged A~X bucket (should have count=2, sum=25)
	// Find the unchanged B~Y bucket (should have count=1, sum=20)
	// Find the new C~Z bucket (should have count=1, sum=30)
	foundBuckets := make(map[string]struct {
		count uint64
		sum   int64
	})

	for i := 0; i < len(knownValues["category"]); i++ {
		category := knownValues["category"][i].CVal.(string)
		typeVal := knownValues["type"][i].CVal.(string)
		count := knownValues["count(value)"][i].CVal.(uint64)
		sum := knownValues["sum(value)"][i].CVal.(int64)

		key := category + "~" + typeVal
		foundBuckets[key] = struct {
			count uint64
			sum   int64
		}{count: count, sum: sum}
	}

	// Verify expected buckets and their values
	assert.Equal(t, uint64(2), foundBuckets["A~X"].count, "A~X should have count=2 (merged)")
	assert.Equal(t, int64(25), foundBuckets["A~X"].sum, "A~X should have sum=25 (10+15)")

	assert.Equal(t, uint64(1), foundBuckets["B~Y"].count, "B~Y should have count=1")
	assert.Equal(t, int64(20), foundBuckets["B~Y"].sum, "B~Y should have sum=20")

	assert.Equal(t, uint64(1), foundBuckets["C~Z"].count, "C~Z should have count=1")
	assert.Equal(t, int64(30), foundBuckets["C~Z"].sum, "C~Z should have sum=30")
}

// Test_MergeIQRStatsResults_EmptyIQR tests merging when one IQR has no stats results.
// IQR1 has stats, IQR2 is empty.
// Expected result: should return the stats from IQR1 unchanged.
func Test_MergeIQRStatsResults_EmptyIQR(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())
	config.GetRunningConfig().UseNewPipelineConverted = true

	// Create IQR1 with data
	iqr1 := iqr.NewIQR(0)
	iqr1Data := map[string][]sutils.CValueEnclosure{
		"category": {{Dtype: sutils.SS_DT_STRING, CVal: "A"}},
		"value":    {{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(42)}},
	}
	err := iqr1.AppendKnownValues(iqr1Data)
	assert.NoError(t, err)

	processor1 := NewStatsProcessor(&structs.StatsExpr{
		GroupByRequest: &structs.GroupByRequest{
			GroupByColumns: []string{"category"},
			MeasureOperations: []*structs.MeasureAggregator{
				{MeasureCol: "value", MeasureFunc: sutils.Count},
				{MeasureCol: "value", MeasureFunc: sutils.Sum},
			},
		},
	})
	processor1.setAsIqrStatsResults = true
	_, err = processor1.Process(iqr1)
	assert.NoError(t, err)
	resultIqr1, err := processor1.Process(nil)
	assert.Equal(t, io.EOF, err)

	// Create empty IQR2
	emptyIqr := iqr.NewIQR(0)

	// Merge IQRs
	mergedIqr := iqr.NewIQR(0)
	iqrs := []*iqr.IQR{resultIqr1, emptyIqr}
	statsExists, err := mergedIqr.MergeIQRStatsResults(iqrs)
	assert.NoError(t, err)
	assert.True(t, statsExists)

	// Verify result is same as IQR1
	knownValues, err := mergedIqr.ReadAllColumns()
	assert.NoError(t, err)

	assert.Len(t, knownValues["category"], 1)
	assert.Equal(t, "A", knownValues["category"][0].CVal.(string))
	assert.Equal(t, uint64(1), knownValues["count(value)"][0].CVal.(uint64))
	assert.Equal(t, int64(42), knownValues["sum(value)"][0].CVal.(int64))
}
