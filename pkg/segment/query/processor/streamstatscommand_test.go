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
	"testing"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func getStreamStatsTestData() map[string][]utils.CValueEnclosure {
	return map[string][]utils.CValueEnclosure{
		"http_status": {
			{Dtype: utils.SS_DT_FLOAT, CVal: float64(200)},
			{Dtype: utils.SS_DT_FLOAT, CVal: float64(404)},
			{Dtype: utils.SS_DT_FLOAT, CVal: float64(500)},
			{Dtype: utils.SS_DT_FLOAT, CVal: float64(200)},
			{Dtype: utils.SS_DT_FLOAT, CVal: float64(403)},
			{Dtype: utils.SS_DT_FLOAT, CVal: float64(200)},
		},
		"first_name": {
			{Dtype: utils.SS_DT_STRING, CVal: "Abel"},
			{Dtype: utils.SS_DT_STRING, CVal: "Abel"},
			{Dtype: utils.SS_DT_STRING, CVal: "Afton"},
			{Dtype: utils.SS_DT_STRING, CVal: "Afton"},
			{Dtype: utils.SS_DT_STRING, CVal: "Alanis"},
			{Dtype: utils.SS_DT_STRING, CVal: "Alanis"},
		},
		"last_name": {
			{Dtype: utils.SS_DT_STRING, CVal: "White"},
			{Dtype: utils.SS_DT_STRING, CVal: "Vandervort"},
			{Dtype: utils.SS_DT_STRING, CVal: "Swaniawski"},
			{Dtype: utils.SS_DT_STRING, CVal: "Batz"},
			{Dtype: utils.SS_DT_STRING, CVal: "Legros"},
			{Dtype: utils.SS_DT_STRING, CVal: "Graham"},
		},
		"hobby": {
			{Dtype: utils.SS_DT_STRING, CVal: "reading"},
			{Dtype: utils.SS_DT_STRING, CVal: "reading"},
			{Dtype: utils.SS_DT_STRING, CVal: "gaming"},
			{Dtype: utils.SS_DT_STRING, CVal: "gaming"},
			{Dtype: utils.SS_DT_STRING, CVal: "reading"},
			{Dtype: utils.SS_DT_STRING, CVal: "gaming"},
		},
		"http_method": {
			{Dtype: utils.SS_DT_STRING, CVal: "GET"},
			{Dtype: utils.SS_DT_STRING, CVal: "POST"},
			{Dtype: utils.SS_DT_STRING, CVal: "GET"},
			{Dtype: utils.SS_DT_STRING, CVal: "GET"},
			{Dtype: utils.SS_DT_STRING, CVal: "POST"},
			{Dtype: utils.SS_DT_STRING, CVal: "DELETE"},
		},
		"latency": {
			{Dtype: utils.SS_DT_FLOAT, CVal: float64(100)},
			{Dtype: utils.SS_DT_FLOAT, CVal: float64(200)},
			{Dtype: utils.SS_DT_FLOAT, CVal: float64(150)},
			{Dtype: utils.SS_DT_FLOAT, CVal: float64(300)},
			{Dtype: utils.SS_DT_FLOAT, CVal: float64(250)},
			{Dtype: utils.SS_DT_FLOAT, CVal: float64(180)},
		},
	}
}

func Test_StreamStats_Count(t *testing.T) {
	knownValues := getStreamStatsTestData()

	measureAggs := []*structs.MeasureAggregator{
		{
			MeasureCol:  "http_status",
			MeasureFunc: utils.Count,
		},
	}

	options := &structs.StreamStatsOptions{
		Window:            3,
		MeasureOperations: measureAggs,
	}

	processor := &streamstatsProcessor{
		options: options,
	}

	inputIQR := iqr.NewIQR(0)
	err := inputIQR.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	resultIQR, err := processor.Process(inputIQR)
	assert.NoError(t, err)

	actualValues, err := resultIQR.ReadColumn("count(http_status)")
	assert.NoError(t, err)

	expectedCount := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(0)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(1)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(2)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(3)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(3)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(3)},
	}

	assert.Equal(t, expectedCount, actualValues)

	actualHttpStatus, err := resultIQR.ReadColumn("http_status")
	assert.NoError(t, err)
	assert.Equal(t, knownValues["http_status"], actualHttpStatus)
}

func Test_StreamStats_Avg(t *testing.T) {
	knownValues := getStreamStatsTestData()

	measureAggs := []*structs.MeasureAggregator{
		{
			MeasureCol:  "http_status",
			MeasureFunc: utils.Avg,
		},
	}

	options := &structs.StreamStatsOptions{
		Window:            3,
		MeasureOperations: measureAggs,
	}

	processor := &streamstatsProcessor{
		options: options,
	}

	inputIQR := iqr.NewIQR(0)
	err := inputIQR.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	resultIQR, err := processor.Process(inputIQR)
	assert.NoError(t, err)

	actualValues, err := resultIQR.ReadColumn("avg(http_status)")
	assert.NoError(t, err)

	expectedAvg := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: ""},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(200)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(302)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(368)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(368)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(367.6666666666667)},
	}

	assert.Equal(t, expectedAvg, actualValues)

	actualHttpStatus, err := resultIQR.ReadColumn("http_status")
	assert.NoError(t, err)
	assert.Equal(t, knownValues["http_status"], actualHttpStatus)
}

func Test_StreamStats_Avg_CurrentTrue(t *testing.T) {
	knownValues := getStreamStatsTestData()

	measureAggs := []*structs.MeasureAggregator{
		{
			MeasureCol:  "http_status",
			MeasureFunc: utils.Avg,
		},
	}

	options := &structs.StreamStatsOptions{
		Window:            4,
		Current:           true, // Include current record
		MeasureOperations: measureAggs,
	}

	processor := &streamstatsProcessor{
		options: options,
	}

	inputIQR := iqr.NewIQR(0)
	err := inputIQR.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	resultIQR, err := processor.Process(inputIQR)
	assert.NoError(t, err)

	actualValues, err := resultIQR.ReadColumn("avg(http_status)")
	assert.NoError(t, err)

	// For window=4 and current=true
	expectedAvg := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(200)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(302)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(368)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(326)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(376.75)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(325.75)},
	}

	assert.Equal(t, expectedAvg, actualValues)

	actualHttpStatus, err := resultIQR.ReadColumn("http_status")
	assert.NoError(t, err)
	assert.Equal(t, knownValues["http_status"], actualHttpStatus)
}

func Test_StreamStats_Sum(t *testing.T) {
	knownValues := getStreamStatsTestData()

	measureAggs := []*structs.MeasureAggregator{
		{
			MeasureCol:  "http_status",
			MeasureFunc: utils.Sum,
		},
	}

	options := &structs.StreamStatsOptions{
		Window:            3,
		MeasureOperations: measureAggs,
	}

	processor := &streamstatsProcessor{
		options: options,
	}

	inputIQR := iqr.NewIQR(0)
	err := inputIQR.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	resultIQR, err := processor.Process(inputIQR)
	assert.NoError(t, err)

	actualValues, err := resultIQR.ReadColumn("sum(http_status)")
	assert.NoError(t, err)

	expectedSum := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: ""},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(200)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(604)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(1104)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(1104)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(1103)},
	}

	assert.Equal(t, expectedSum, actualValues)

	actualHttpStatus, err := resultIQR.ReadColumn("http_status")
	assert.NoError(t, err)
	assert.Equal(t, knownValues["http_status"], actualHttpStatus)
}

func Test_StreamStats_Min(t *testing.T) {
	knownValues := getStreamStatsTestData()

	measureAggs := []*structs.MeasureAggregator{
		{
			MeasureCol:  "http_status",
			MeasureFunc: utils.Min,
		},
	}

	options := &structs.StreamStatsOptions{
		Window:            3,
		MeasureOperations: measureAggs,
	}

	processor := &streamstatsProcessor{
		options: options,
	}

	inputIQR := iqr.NewIQR(0)
	err := inputIQR.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	resultIQR, err := processor.Process(inputIQR)
	assert.NoError(t, err)

	actualValues, err := resultIQR.ReadColumn("min(http_status)")
	assert.NoError(t, err)

	expectedMin := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: ""},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(200)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(200)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(200)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(200)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(200)},
	}

	assert.Equal(t, expectedMin, actualValues)

	actualHttpStatus, err := resultIQR.ReadColumn("http_status")
	assert.NoError(t, err)
	assert.Equal(t, knownValues["http_status"], actualHttpStatus)
}

func Test_StreamStats_GlobalFalse(t *testing.T) {
	knownValues := getStreamStatsTestData()

	measureAggs := []*structs.MeasureAggregator{
		{
			MeasureCol:  "http_status",
			MeasureFunc: utils.Count,
		},
	}

	options := &structs.StreamStatsOptions{
		Window:            4,
		Global:            false, // Count resets for each bucket
		Current:           true,  // Include current record
		MeasureOperations: measureAggs,
	}

	processor := &streamstatsProcessor{
		options: options,
	}

	inputIQR := iqr.NewIQR(0)
	err := inputIQR.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	resultIQR, err := processor.Process(inputIQR)
	assert.NoError(t, err)

	actualValues, err := resultIQR.ReadColumn("count(http_status)")
	assert.NoError(t, err)

	expectedCount := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(1)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(2)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(3)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(4)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(4)},
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(4)},
	}

	assert.Equal(t, expectedCount, actualValues)

	actualHttpStatus, err := resultIQR.ReadColumn("http_status")
	assert.NoError(t, err)
	assert.Equal(t, knownValues["http_status"], actualHttpStatus)
}

func Test_StreamStats_GroupBy_Count(t *testing.T) {
	knownValues := getStreamStatsTestData()

	measureAggs := []*structs.MeasureAggregator{
		{
			MeasureCol:  "http_status",
			MeasureFunc: utils.Count,
		},
	}

	options := &structs.StreamStatsOptions{
		Window:  3,
		Global:  false,
		Current: false,
		GroupByRequest: &structs.GroupByRequest{
			GroupByColumns:    []string{"hobby"},
			MeasureOperations: measureAggs,
		},
		MeasureOperations: measureAggs,
	}

	processor := &streamstatsProcessor{
		options: options,
	}

	inputIQR := iqr.NewIQR(0)
	err := inputIQR.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	resultIQR, err := processor.Process(inputIQR)
	assert.NoError(t, err)

	actualValues, err := resultIQR.ReadColumn("count(http_status)")
	assert.NoError(t, err)

	expectedCount := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(0)}, // reading: start
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(1)}, // reading: one prev
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(0)}, // gaming: start
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(1)}, // gaming: one prev
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(2)}, // reading: two prev
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(2)}, // gaming: 2 prev records
	}

	assert.Equal(t, expectedCount, actualValues)
}

// Test 2: GroupBy with avg and latency
func Test_StreamStats_GroupBy_Avg(t *testing.T) {
	knownValues := getStreamStatsTestData()

	measureAggs := []*structs.MeasureAggregator{
		{
			MeasureCol:  "latency",
			MeasureFunc: utils.Avg,
		},
	}

	options := &structs.StreamStatsOptions{
		Window:  3,
		Global:  false,
		Current: false,
		GroupByRequest: &structs.GroupByRequest{
			GroupByColumns:    []string{"http_method"},
			MeasureOperations: measureAggs,
		},
		MeasureOperations: measureAggs,
	}

	processor := &streamstatsProcessor{
		options: options,
	}

	inputIQR := iqr.NewIQR(0)
	err := inputIQR.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	resultIQR, err := processor.Process(inputIQR)
	assert.NoError(t, err)

	actualValues, err := resultIQR.ReadColumn("avg(latency)")
	assert.NoError(t, err)

	expectedAvg := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: ""},          // GET: no prev records
		{Dtype: utils.SS_DT_STRING, CVal: ""},          // POST: no prev records
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(100)}, // GET: prev record avg
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(125)}, // GET: prev records avg
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(200)}, // POST: prev record avg
		{Dtype: utils.SS_DT_STRING, CVal: ""},          // DELETE: no prev records
	}

	assert.Equal(t, expectedAvg, actualValues)
}

func Test_StreamStats_ResetOnChange(t *testing.T) {
	knownValues := getStreamStatsTestData()

	measureAggs := []*structs.MeasureAggregator{
		{
			MeasureCol:  "http_status",
			MeasureFunc: utils.Count,
		},
	}

	options := &structs.StreamStatsOptions{
		Window:        3,
		ResetOnChange: true,
		GroupByRequest: &structs.GroupByRequest{
			GroupByColumns:    []string{"http_method"},
			MeasureOperations: measureAggs,
		},
		MeasureOperations: measureAggs,
	}

	processor := &streamstatsProcessor{
		options: options,
	}

	inputIQR := iqr.NewIQR(0)
	err := inputIQR.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	resultIQR, err := processor.Process(inputIQR)
	assert.NoError(t, err)

	actualValues, err := resultIQR.ReadColumn("count(http_status)")
	assert.NoError(t, err)

	expectedCount := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(0)}, // GET start
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(0)}, // POST - reset
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(0)}, // GET - reset
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(1)}, // GET continues
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(0)}, // POST - reset
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(0)}, // DELETE - reset
	}

	assert.Equal(t, expectedCount, actualValues)
}

func Test_StreamStats_ResetBefore(t *testing.T) {
	knownValues := getStreamStatsTestData()

	measureAggs := []*structs.MeasureAggregator{
		{
			MeasureCol:  "http_status",
			MeasureFunc: utils.Count,
		},
	}

	options := &structs.StreamStatsOptions{
		Window: 3,
		ResetBefore: &structs.BoolExpr{
			IsTerminal: true,
			LeftValue: &structs.ValueExpr{
				ValueExprMode: structs.VEMStringExpr,
				StringExpr: &structs.StringExpr{
					StringExprMode: structs.SEMField,
					FieldName:      "last_name",
				},
			},
			RightValue: &structs.ValueExpr{
				ValueExprMode: structs.VEMStringExpr,
				StringExpr: &structs.StringExpr{
					StringExprMode: structs.SEMRawString,
					RawString:      "Vandervort",
				},
			},
			ValueOp: "=",
		},
		MeasureOperations: measureAggs,
	}

	processor := &streamstatsProcessor{
		options: options,
	}

	inputIQR := iqr.NewIQR(0)
	err := inputIQR.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	resultIQR, err := processor.Process(inputIQR)
	assert.NoError(t, err)

	actualValues, err := resultIQR.ReadColumn("count(http_status)")
	assert.NoError(t, err)

	expectedCount := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(0)}, // Start (White)
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(0)}, // Reset before Vandervort
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(1)}, // Count (Swaniawski)
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(2)}, // Count (Batz)
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(3)}, // Count (Legros)
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(3)}, // Count (Graham) - window of 3 limits max count
	}

	assert.Equal(t, expectedCount, actualValues)
}

func Test_StreamStats_ResetAfter(t *testing.T) {
	knownValues := getStreamStatsTestData()

	measureAggs := []*structs.MeasureAggregator{
		{
			MeasureCol:  "http_status",
			MeasureFunc: utils.Count,
		},
	}

	options := &structs.StreamStatsOptions{
		Window: 3,
		ResetAfter: &structs.BoolExpr{
			IsTerminal: true,
			LeftValue: &structs.ValueExpr{
				ValueExprMode: structs.VEMStringExpr,
				StringExpr: &structs.StringExpr{
					StringExprMode: structs.SEMField,
					FieldName:      "last_name",
				},
			},
			RightValue: &structs.ValueExpr{
				ValueExprMode: structs.VEMStringExpr,
				StringExpr: &structs.StringExpr{
					StringExprMode: structs.SEMRawString,
					RawString:      "White", // Reset after White
				},
			},
			ValueOp: "=",
		},
		MeasureOperations: measureAggs,
	}

	processor := &streamstatsProcessor{
		options: options,
	}

	inputIQR := iqr.NewIQR(0)
	err := inputIQR.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	resultIQR, err := processor.Process(inputIQR)
	assert.NoError(t, err)

	actualValues, err := resultIQR.ReadColumn("count(http_status)")
	assert.NoError(t, err)

	// Count should reset after last_name="White"
	expectedCount := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(0)}, // Count (Vandervort)
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(0)}, // Reset after White
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(1)}, // Count (Swaniawski)
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(2)}, // Count (Batz)
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(3)}, // Count (Legros)
		{Dtype: utils.SS_DT_FLOAT, CVal: float64(3)}, // Count (Graham) - window of 3 limits max count
	}

	assert.Equal(t, expectedCount, actualValues)
}
