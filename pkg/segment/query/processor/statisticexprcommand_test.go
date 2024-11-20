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
	"math"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/reader/record"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func getMockRRCsReader() *record.MockRRCsReader {
	rrcs := []*utils.RecordResultContainer{
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 1},
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 2},
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 3},
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 4},
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 5},
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 6},
	}
	mockReader := &record.MockRRCsReader{
		RRCs: rrcs,
		FieldToValues: map[string][]utils.CValueEnclosure{
			"col1": {
				{Dtype: utils.SS_DT_STRING, CVal: "a"},
				{Dtype: utils.SS_DT_STRING, CVal: "e"},
				{Dtype: utils.SS_DT_STRING, CVal: "c"},

				{Dtype: utils.SS_DT_STRING, CVal: "e"},
				{Dtype: utils.SS_DT_STRING, CVal: "c"},
				{Dtype: utils.SS_DT_STRING, CVal: "c"},
			},
			"col2": {
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(4)},
			},
			"col3": {
				{Dtype: utils.SS_DT_STRING, CVal: "c"},
				{Dtype: utils.SS_DT_STRING, CVal: "e"},
				{Dtype: utils.SS_DT_STRING, CVal: "a"},
				{Dtype: utils.SS_DT_STRING, CVal: "e"},
				{Dtype: utils.SS_DT_STRING, CVal: "c"},
				{Dtype: utils.SS_DT_STRING, CVal: "c"},
			},
		},
	}

	return mockReader
}

func getTestStatisticExprProcessor(sfMode structs.StatisticFunctionMode, withGroupBy bool) *statisticExprProcessor {
	statisExpr := &structs.StatisticExpr{
		StatisticFunctionMode: sfMode,
		StatisticOptions: &structs.StatisticOptions{
			CountField:   "count",
			OtherStr:     "other",
			PercentField: "percent",
			ShowCount:    true,
			ShowPerc:     true,
		},
		FieldList: []string{"col1"},
		ByClause:  []string{},
	}

	groupByRequest := &structs.GroupByRequest{
		MeasureOperations: []*structs.MeasureAggregator{
			{
				MeasureCol:  "*",
				MeasureFunc: utils.Count,
			},
		},
		GroupByColumns: []string{"col1"},
	}

	if withGroupBy {
		statisExpr.ByClause = []string{"col3"}
		groupByRequest.GroupByColumns = []string{"col1", "col3"}
	}

	queryAggregators := &structs.QueryAggregators{
		StatisticExpr: statisExpr,
		StatsExpr: &structs.StatsExpr{
			GroupByRequest: groupByRequest,
		},
	}

	return NewStatisticExprProcessor(queryAggregators)
}

func Test_StatisticTopExpr_withRRCs(t *testing.T) {
	config.InitializeDefaultConfig(t.TempDir())
	config.GetRunningConfig().UseNewPipelineConverted = true

	processor := getTestStatisticExprProcessor(structs.SFMTop, false)
	assert.NotNil(t, processor)

	mockReader := getMockRRCsReader()

	iqr1 := iqr.NewIQRWithReader(0, mockReader)
	assert.NotNil(t, iqr1)

	err := iqr1.AppendRRCs(mockReader.RRCs[:3], map[uint32]string{1: "seg1"})
	assert.Nil(t, err)

	_, err = processor.Process(iqr1)
	assert.Nil(t, err)

	iqr2 := iqr.NewIQRWithReader(0, mockReader)
	assert.NotNil(t, iqr2)

	err = iqr2.AppendRRCs(mockReader.RRCs[3:], map[uint32]string{1: "seg2"})
	assert.Nil(t, err)

	_, err = processor.Process(iqr2)
	assert.Nil(t, err)

	finalIqr, err := processor.Process(nil)
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, finalIqr)

	assert.True(t, processor.hasFinalResult)
	assert.NotNil(t, processor.finalAggregationResults)

	expectedResults := map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "e"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "other"},
		},
		"count": {
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(0)},
		},
		"percent": {
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: float64(50)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: float64(33.333333333)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: float64(16.666666666667)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: float64(0)},
		},
	}

	col1Values, err := finalIqr.ReadColumn("col1")
	assert.Nil(t, err)

	countValues, err := finalIqr.ReadColumn("count")
	assert.Nil(t, err)

	percentValues, err := finalIqr.ReadColumn("percent")
	assert.Nil(t, err)

	for i := 0; i < len(col1Values); i++ {
		assert.Equal(t, expectedResults["col1"][i], col1Values[i])
		assert.Equal(t, expectedResults["count"][i], countValues[i])
		assert.LessOrEqual(t, math.Abs(expectedResults["percent"][i].CVal.(float64)-percentValues[i].CVal.(float64)), 0.001)
	}
}

func Test_StatisticRareExpr_withRRCs(t *testing.T) {
	config.InitializeDefaultConfig(t.TempDir())
	config.GetRunningConfig().UseNewPipelineConverted = true

	processor := getTestStatisticExprProcessor(structs.SFMRare, false)
	assert.NotNil(t, processor)

	mockReader := getMockRRCsReader()

	iqr1 := iqr.NewIQRWithReader(0, mockReader)
	assert.NotNil(t, iqr1)

	err := iqr1.AppendRRCs(mockReader.RRCs[:3], map[uint32]string{1: "seg1"})
	assert.Nil(t, err)

	_, err = processor.Process(iqr1)
	assert.Nil(t, err)

	iqr2 := iqr.NewIQRWithReader(0, mockReader)
	assert.NotNil(t, iqr2)

	err = iqr2.AppendRRCs(mockReader.RRCs[3:], map[uint32]string{1: "seg2"})
	assert.Nil(t, err)

	_, err = processor.Process(iqr2)
	assert.Nil(t, err)

	finalIqr, err := processor.Process(nil)
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, finalIqr)

	assert.True(t, processor.hasFinalResult)
	assert.NotNil(t, processor.finalAggregationResults)

	expectedResults := map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "e"},
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "other"},
		},
		"count": {
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(0)},
		},
		"percent": {
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: float64(16.666666666666)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: float64(33.333333333333)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: float64(50)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: float64(0)},
		},
	}

	col1Values, err := finalIqr.ReadColumn("col1")
	assert.Nil(t, err)

	countValues, err := finalIqr.ReadColumn("count")
	assert.Nil(t, err)

	percentValues, err := finalIqr.ReadColumn("percent")
	assert.Nil(t, err)

	for i := 0; i < len(col1Values); i++ {
		assert.Equal(t, expectedResults["col1"][i], col1Values[i])
		assert.Equal(t, expectedResults["count"][i], countValues[i])
		assert.LessOrEqual(t, math.Abs(expectedResults["percent"][i].CVal.(float64)-percentValues[i].CVal.(float64)), 0.001)
	}
}

func Test_StatisticExpr_withRRCs_GroupBy(t *testing.T) {
	config.InitializeDefaultConfig(t.TempDir())
	config.GetRunningConfig().UseNewPipelineConverted = true

	processor := getTestStatisticExprProcessor(structs.SFMTop, true)
	assert.NotNil(t, processor)

	mockReader := getMockRRCsReader()

	iqr1 := iqr.NewIQRWithReader(0, mockReader)
	assert.NotNil(t, iqr1)

	err := iqr1.AppendRRCs(mockReader.RRCs[:3], map[uint32]string{1: "seg1"})
	assert.Nil(t, err)

	_, err = processor.Process(iqr1)
	assert.Nil(t, err)

	iqr2 := iqr.NewIQRWithReader(0, mockReader)
	assert.NotNil(t, iqr2)

	err = iqr2.AppendRRCs(mockReader.RRCs[3:], map[uint32]string{1: "seg2"})
	assert.Nil(t, err)

	_, err = processor.Process(iqr2)
	assert.Nil(t, err)

	finalIqr, err := processor.Process(nil)
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, finalIqr)
	assert.True(t, processor.hasFinalResult)

	actualKnownValues, err := finalIqr.ReadAllColumns()
	assert.Nil(t, err)

	expectedValues := map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "e"},
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
		},
		"col3": {
			{Dtype: utils.SS_DT_STRING, CVal: "e"},
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
		},
		"count": {
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
		},
		"percent": {
			{Dtype: utils.SS_DT_FLOAT, CVal: float64(33.333)},
			{Dtype: utils.SS_DT_FLOAT, CVal: float64(33.333)},
			{Dtype: utils.SS_DT_FLOAT, CVal: float64(16.666)},
			{Dtype: utils.SS_DT_FLOAT, CVal: float64(16.666)},
		},
	}

	col1Values := actualKnownValues["col1"]
	col3Values := actualKnownValues["col3"]
	countValues := actualKnownValues["count"]
	percentValues := actualKnownValues["percent"]

	assert.ElementsMatch(t, expectedValues["col1"], col1Values)
	assert.ElementsMatch(t, expectedValues["col3"], col3Values)

	for i := 0; i < len(countValues); i++ {
		assert.Equal(t, expectedValues["count"][i], countValues[i])
		assert.LessOrEqual(t, math.Abs(expectedValues["percent"][i].CVal.(float64)-percentValues[i].CVal.(float64)), 0.001)
	}
}
