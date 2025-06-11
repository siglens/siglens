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
