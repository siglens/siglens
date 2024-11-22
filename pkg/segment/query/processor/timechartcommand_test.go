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
	"time"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func getTestInput(timeNow uint64) map[string][]utils.CValueEnclosure {
	minute := uint64(time.Minute.Milliseconds())
	secondsIncrement := uint64(10 * time.Second.Milliseconds()) // 10 seconds

	knownValues := map[string][]utils.CValueEnclosure{
		"timestamp": {
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: timeNow},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: (timeNow + secondsIncrement)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: (timeNow + 2*secondsIncrement)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: (timeNow + minute)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: (timeNow + minute + secondsIncrement)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: (timeNow + 2*minute)},
		},
		"measurecol": {
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(4)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(5)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(6)},
		},
		"groupbycol": {
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
		},
	}

	return knownValues
}

func getTimechartProcessor(startTime uint64) *timechartProcessor {
	endTime := startTime + uint64(15*time.Minute.Milliseconds())

	timeHistogram := &structs.TimeBucket{
		IntervalMillis: uint64(1 * time.Minute.Milliseconds()),
		Timechart:      &structs.TimechartExpr{},
	}
	groupByRequest := &structs.GroupByRequest{
		MeasureOperations: []*structs.MeasureAggregator{
			{
				MeasureCol:  "measurecol",
				MeasureFunc: utils.Sum,
				StrEnc:      "sum_measurecol",
			},
		},
		GroupByColumns: []string{"timestamp"},
	}

	timeRange := &dtypeutils.TimeRange{
		StartEpochMs: startTime,
		EndEpochMs:   endTime,
	}

	timechartOptions := &timechartOptions{
		timeBucket:     timeHistogram,
		groupByRequest: groupByRequest,
		timeChartExpr: &structs.TimechartExpr{
			TimeHistogram: timeHistogram,
			GroupBy:       groupByRequest,
		},
		timeRange: timeRange,
		qid:       0,
	}

	return NewTimechartProcessor(timechartOptions)
}

func Test_TimechartProcessor_NoByField(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())
	config.GetRunningConfig().UseNewPipelineConverted = true

	startTime := uint64(time.Now().UnixMilli())
	processor := getTimechartProcessor(startTime)

	iqr1 := iqr.NewIQR(0)
	knownValues := getTestInput(startTime)
	err := iqr1.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	// No by field. only group on timestamp
	_, err = processor.Process(iqr1)
	assert.NoError(t, err)

	iqr1, err = processor.Process(nil)
	assert.EqualError(t, io.EOF, err.Error())
	assert.NotNil(t, iqr1)

	resultValues, err := iqr1.ReadAllColumns()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(resultValues["timestamp"]))
	assert.Equal(t, 3, len(resultValues["sum_measurecol"]))
	assert.Equal(t, startTime, resultValues["timestamp"][0].CVal)
	assert.Equal(t, startTime+uint64(time.Minute.Milliseconds()), resultValues["timestamp"][1].CVal)
	assert.Equal(t, startTime+2*uint64(time.Minute.Milliseconds()), resultValues["timestamp"][2].CVal)
	assert.Equal(t, uint64(6), resultValues["sum_measurecol"][0].CVal)
	assert.Equal(t, uint64(9), resultValues["sum_measurecol"][1].CVal)
	assert.Equal(t, uint64(6), resultValues["sum_measurecol"][2].CVal)
}

func Test_TimechartProcessor_WithByField(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())
	config.GetRunningConfig().UseNewPipelineConverted = true

	startTime := uint64(time.Now().UnixMilli())
	processor := getTimechartProcessor(startTime)
	processor.options.timeChartExpr.ByField = "groupbycol"

	iqr1 := iqr.NewIQR(0)
	knownValues := getTestInput(startTime)
	err := iqr1.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	_, err = processor.Process(iqr1)
	assert.NoError(t, err)

	iqr1, err = processor.Process(nil)
	assert.EqualError(t, io.EOF, err.Error())
	assert.NotNil(t, iqr1)

	resultValues, err := iqr1.ReadAllColumns()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(resultValues["timestamp"]))
	assert.Equal(t, 3, len(resultValues["sum_measurecol: a"]))
	assert.Equal(t, 3, len(resultValues["sum_measurecol: b"]))
	assert.Equal(t, 3, len(resultValues["sum_measurecol: c"]))

	assert.Equal(t, startTime, resultValues["timestamp"][0].CVal)
	assert.Equal(t, startTime+uint64(time.Minute.Milliseconds()), resultValues["timestamp"][1].CVal)
	assert.Equal(t, startTime+2*uint64(time.Minute.Milliseconds()), resultValues["timestamp"][2].CVal)

	assert.Equal(t, uint64(4), resultValues["sum_measurecol: a"][0].CVal)
	assert.Equal(t, int64(0), resultValues["sum_measurecol: a"][1].CVal)
	assert.Equal(t, int64(0), resultValues["sum_measurecol: a"][2].CVal)

	assert.Equal(t, uint64(2), resultValues["sum_measurecol: b"][0].CVal)
	assert.Equal(t, int64(0), resultValues["sum_measurecol: b"][1].CVal)
	assert.Equal(t, uint64(6), resultValues["sum_measurecol: b"][2].CVal)

	assert.Equal(t, int64(0), resultValues["sum_measurecol: c"][0].CVal)
	assert.Equal(t, uint64(9), resultValues["sum_measurecol: c"][1].CVal)
	assert.Equal(t, int64(0), resultValues["sum_measurecol: c"][2].CVal)
}
