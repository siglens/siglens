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
		},
	}

	return mockReader
}

func getStatisticExprProcessor(sfMode structs.StatisticFunctionMode) *statisticExprProcessor {
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

	processor := getStatisticExprProcessor(structs.SFMTop)
	assert.NotNil(t, processor)

	mockReader := getMockRRCsReader()

	iqr1 := iqr.NewIQRWithReader(0, mockReader)
	assert.NotNil(t, iqr1)

	err := iqr1.AppendRRCs(mockReader.RRCs[:3], map[uint16]string{1: "seg1"})
	assert.Nil(t, err)

	_, err = processor.Process(iqr1)
	assert.Nil(t, err)

	iqr2 := iqr.NewIQRWithReader(0, mockReader)
	assert.NotNil(t, iqr2)

	err = iqr2.AppendRRCs(mockReader.RRCs[3:], map[uint16]string{1: "seg2"})
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

func Test_StatisticRareExpr_withRRCs_noGroupBy(t *testing.T) {
	config.InitializeDefaultConfig(t.TempDir())
	config.GetRunningConfig().UseNewPipelineConverted = true

	processor := getStatisticExprProcessor(structs.SFMRare)
	assert.NotNil(t, processor)

	mockReader := getMockRRCsReader()

	iqr1 := iqr.NewIQRWithReader(0, mockReader)
	assert.NotNil(t, iqr1)

	err := iqr1.AppendRRCs(mockReader.RRCs[:3], map[uint16]string{1: "seg1"})
	assert.Nil(t, err)

	_, err = processor.Process(iqr1)
	assert.Nil(t, err)

	iqr2 := iqr.NewIQRWithReader(0, mockReader)
	assert.NotNil(t, iqr2)

	err = iqr2.AppendRRCs(mockReader.RRCs[3:], map[uint16]string{1: "seg2"})
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
