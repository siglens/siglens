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

package mresults

import (
	"math"
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_applyRangeFunctionRate(t *testing.T) {
	timeSeries := map[uint32]float64{
		1000: 2.0,
		1003: 3.0,
		1008: 4.0,
		1012: 18.0,
		1020: 2.5,
		1025: 6.5,
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: segutils.Rate, TimeWindow: 10})
	assert.Nil(t, err)

	// There's six timestamps in the series, but we need two points to calculate
	// the rate, so we can't calculate it on the first point. So we should have
	// 5 elements in the result.
	assert.Len(t, res, 5)

	var val float64
	var ok bool

	val, ok = res[1003]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (3.0-2.0)/(3-0)))

	val, ok = res[1008]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (4.0-2.0)/(8-0)))

	val, ok = res[1012]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (18.0-3.0)/(12-3)))

	val, ok = res[1020]
	assert.True(t, ok)
	// Since the value here is smaller than at the last timestamp, the value was
	// reset since the last timestamp. So the increase is just this value, not
	// this value minus the previous value.
	assert.True(t, dtypeutils.AlmostEquals(val, (2.5)/(20-12)))

	val, ok = res[1025]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (6.5-2.5)/(25-20)))
}

func Test_applyRangeFunctionIRate(t *testing.T) {
	timeSeries := map[uint32]float64{
		1000: 2.0,
		1001: 3.0,
		1002: 4.0,
		1003: 0.0,
		1008: 2.5,
		1009: 1.0,
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: segutils.IRate, TimeWindow: 10})
	assert.Nil(t, err)

	// There's six timestamps in the series, but we need two points to calculate
	// the rate, so we can't calculate it on the first point. So we should have
	// 5 elements in the result.
	assert.Len(t, res, 5)

	var val float64
	var ok bool

	val, ok = res[1001]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (3.0-2.0)/(1-0)))

	val, ok = res[1002]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (4.0-3.0)/(2-1)))

	val, ok = res[1003]
	assert.True(t, ok)
	// Since the value here is smaller than at the last timestamp, the value was
	// reset since the last timestamp. So the increase is just this value, not
	// this value minus the previous value.
	assert.True(t, dtypeutils.AlmostEquals(val, 0.0))

	val, ok = res[1008]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.5/(8-3)))

	val, ok = res[1009]
	assert.True(t, ok)
	// Since the value here is smaller than at the last timestamp, the value was
	// reset since the last timestamp. So the increase is just this value, not
	// this value minus the previous value.
	assert.True(t, dtypeutils.AlmostEquals(val, 1.0/(9-8)))
}

func Test_applyRangeFunctionPredict_Linear(t *testing.T) {
	// y = 2x + 1
	timeSeries := map[uint32]float64{
		1000: 1.0,
		1001: 3.0,
		1002: 5.0,
		1003: 7.0,
	}

	result := make(map[string]map[uint32]float64)
	result["metric"] = timeSeries

	metricsResults := &MetricsResult{
		Results: result,
	}

	ans := map[uint32]float64{
		1001: 3.0 + 2*1000,
		1002: 5.0 + 2*1000,
		1003: 7.0 + 2*1000,
	}

	function := structs.Function{RangeFunction: segutils.Predict_Linear, TimeWindow: 10, ValueList: []string{"1000"}}

	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if !dtypeutils.AlmostEquals(expectedVal, val) {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}

func Test_applyRangeFunctionIncrease(t *testing.T) {
	timeSeries := map[uint32]float64{
		1000: 0.0,
		1008: 8.0,
		1010: 14.0,
		1012: 10.0,
		1020: 18.0,
	}

	timeWindow := float64(10)
	increase, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: segutils.Increase, TimeWindow: timeWindow})
	assert.Nil(t, err)

	assert.Len(t, increase, 4)

	var val float64
	var ok bool

	val, ok = increase[1008]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, timeWindow*(8.0-0.0)/(8-0)))

	val, ok = increase[1010]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, timeWindow*(14.0-0.0)/(10-0)))

	// Reset val
	val, ok = increase[1012]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, timeWindow*(10.0)/(12-10)))

	val, ok = increase[1020]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, timeWindow*(18.0-10.0)/(20-12)))
}

func Test_applyRangeFunctionDelta(t *testing.T) {
	timeSeries := map[uint32]float64{
		1000: 2.0,
		1001: 3.0,
		1002: 5.0,
		1013: 10.0,
		1018: 2.5,
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: segutils.Delta, TimeWindow: 10})
	assert.Nil(t, err)

	assert.Len(t, res, 3)

	var val float64
	var ok bool

	val, ok = res[1001]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 3.0-2.0))

	val, ok = res[1002]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 5.0-2.0))

	_, ok = res[1013]
	assert.False(t, ok)

	val, ok = res[1018]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.5-10.0))
}

func Test_applyRangeFunctionIDelta(t *testing.T) {
	timeSeries := map[uint32]float64{
		1000: 2.0,
		1001: 3.0,
		1002: 5.0,
		1013: 10.0,
		1018: 2.5,
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: segutils.IDelta, TimeWindow: 10})
	assert.Nil(t, err)

	assert.Len(t, res, 3)

	var val float64
	var ok bool

	val, ok = res[1001]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 3.0-2.0))

	val, ok = res[1002]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 5.0-3.0))

	_, ok = res[1013]
	assert.False(t, ok)

	val, ok = res[1018]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.5-10.0))
}

func Test_applyRangeFunctionChanges(t *testing.T) {
	timeSeries := map[uint32]float64{
		1000: 2.0,
		1001: 3.0,
		1002: 5.0,
		1013: 10.0,
		1018: 2.5,
		1025: 2.5,
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: segutils.Changes, TimeWindow: 10})
	assert.Nil(t, err)

	assert.Len(t, res, 6)

	var val float64
	var ok bool

	val, ok = res[1000]
	assert.True(t, ok)
	assert.Equal(t, float64(0), val)

	val, ok = res[1001]
	assert.True(t, ok)
	assert.Equal(t, float64(1), val)

	val, ok = res[1002]
	assert.True(t, ok)
	assert.Equal(t, float64(2), val)

	val, ok = res[1013]
	assert.True(t, ok)
	assert.Equal(t, float64(0), val)

	val, ok = res[1018]
	assert.True(t, ok)
	assert.Equal(t, float64(1), val)

	val, ok = res[1025]
	assert.True(t, ok)
	assert.Equal(t, float64(0), val)
}

func Test_applyRangeFunctionResets(t *testing.T) {
	timeSeries := map[uint32]float64{
		1000: 5.0,
		1001: 8.0,
		1002: 5.0,
		1008: 3.0,
		1019: 2.5,
		1025: 2.8,
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: segutils.Resets, TimeWindow: 10})
	assert.Nil(t, err)

	assert.Len(t, res, 6)

	var val float64
	var ok bool

	val, ok = res[1000]
	assert.True(t, ok)
	assert.Equal(t, float64(0), val)

	val, ok = res[1001]
	assert.True(t, ok)
	assert.Equal(t, float64(0), val)

	val, ok = res[1002]
	assert.True(t, ok)
	assert.Equal(t, float64(1), val)

	val, ok = res[1008]
	assert.True(t, ok)
	assert.Equal(t, float64(2), val)

	val, ok = res[1019]
	assert.True(t, ok)
	assert.Equal(t, float64(0), val)

	val, ok = res[1025]
	assert.True(t, ok)
	assert.Equal(t, float64(0), val)
}

func Test_applyRangeFunctionAvg(t *testing.T) {
	timeSeries := map[uint32]float64{
		1000: 2.0,
		1003: 3.0,
		1008: 4.0,
		1012: 18.0,
		1020: 2.5,
		1035: 6.5,
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: segutils.Avg_Over_Time, TimeWindow: 10})
	assert.Nil(t, err)

	assert.Len(t, res, 6)

	var val float64
	var ok bool

	val, ok = res[1000]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.0))

	val, ok = res[1003]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (3.0+2.0)/(2)))

	val, ok = res[1008]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (4.0+3.0+2.0)/(3)))

	val, ok = res[1012]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (18.0+4.0+3.0)/(3)))

	val, ok = res[1020]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (2.5+18.0)/(2)))

	val, ok = res[1035]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 6.5))
}

func Test_applyRangeFunctionMin(t *testing.T) {
	timeSeries := map[uint32]float64{
		1000: 2.0,
		1003: 3.0,
		1008: 1.0,
		1012: 18.0,
		1023: 2.5,
		1025: 6.5,
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: segutils.Min_Over_Time, TimeWindow: 10})
	assert.Nil(t, err)

	assert.Len(t, res, 6)

	var val float64
	var ok bool

	val, ok = res[1000]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.0))

	val, ok = res[1003]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.0))

	val, ok = res[1008]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 1.0))

	val, ok = res[1012]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 1.0))

	val, ok = res[1023]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.5))

	val, ok = res[1025]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.5))
}

func Test_applyRangeFunctionMax(t *testing.T) {
	timeSeries := map[uint32]float64{
		1000: 2.0,
		1003: 3.0,
		1008: 1.0,
		1012: 18.0,
		1023: 2.5,
		1025: 6.5,
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: segutils.Max_Over_Time, TimeWindow: 10})
	assert.Nil(t, err)

	assert.Len(t, res, 6)

	var val float64
	var ok bool

	val, ok = res[1000]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.0))

	val, ok = res[1003]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 3.0))

	val, ok = res[1008]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 3.0))

	val, ok = res[1012]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 18.0))

	val, ok = res[1023]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.5))

	val, ok = res[1025]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 6.5))
}

func Test_applyRangeFunctionSum(t *testing.T) {
	timeSeries := map[uint32]float64{
		1000: 2.0,
		1003: 3.0,
		1008: 4.0,
		1012: 18.0,
		1020: 2.5,
		1025: 6.5,
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: segutils.Sum_Over_Time, TimeWindow: 10})
	assert.Nil(t, err)

	assert.Len(t, res, 6)

	var val float64
	var ok bool

	val, ok = res[1000]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.0))

	val, ok = res[1003]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (3.0+2.0)))

	val, ok = res[1008]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (4.0+3.0+2.0)))

	val, ok = res[1012]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (18.0+4.0+3.0)))

	val, ok = res[1020]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (2.5+18.0)))

	val, ok = res[1025]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (6.5+2.5)))
}

func Test_applyRangeFunctionCount(t *testing.T) {
	timeSeries := map[uint32]float64{
		1000: 2.0,
		1003: 3.0,
		1008: 4.0,
		1012: 18.0,
		1020: 2.5,
		1025: 6.5,
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: segutils.Count_Over_Time, TimeWindow: 10})
	assert.Nil(t, err)

	assert.Len(t, res, 6)

	var val float64
	var ok bool

	val, ok = res[1000]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 1))

	val, ok = res[1003]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2))

	val, ok = res[1008]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 3))

	val, ok = res[1012]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 3))

	val, ok = res[1020]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2))

	val, ok = res[1025]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2))
}

func Test_applyRangeFunctionStdvarOverTime(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := map[uint32]float64{
		1000: 10,
		1001: 20,
		1002: 30,
		1013: 40,
		1018: 50,
		1019: 60,
		1020: 70,
	}

	result["metric"] = ts

	metricsResults := &MetricsResult{
		Results: result,
	}

	ans := map[uint32]float64{
		1000: 0,
		1001: 25,
		1002: 66.6666666,
		1013: 0,
		1018: 25,
		1019: 66.6666666,
		1020: 125,
	}

	function := structs.Function{RangeFunction: segutils.Stdvar_Over_Time, TimeWindow: 10}

	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if !dtypeutils.AlmostEquals(expectedVal, val) {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}

func Test_applyRangeFunctionStddevOverTime(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := map[uint32]float64{
		1000: 10,
		1001: 20,
		1002: 30,
		1013: 40,
		1018: 50,
		1019: 60,
		1020: 70,
	}

	result["metric"] = ts

	metricsResults := &MetricsResult{
		Results: result,
	}

	ans := map[uint32]float64{
		1000: 0,
		1001: math.Sqrt(25),
		1002: math.Sqrt(66.6666666),
		1013: 0,
		1018: math.Sqrt(25),
		1019: math.Sqrt(66.6666666),
		1020: math.Sqrt(125),
	}

	function := structs.Function{RangeFunction: segutils.Stddev_Over_Time, TimeWindow: 10}

	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if !dtypeutils.AlmostEquals(expectedVal, val) {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}

func Test_applyRangeFunctionQuantileOverTime(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := map[uint32]float64{
		1000: 0.1,
		1001: 0.2,
		1002: 0.3,
		1013: 0.4,
		1018: 0.5,
		1019: 0.6,
		1020: 0.7,
	}

	result["metric"] = ts

	metricsResults := &MetricsResult{
		Results: result,
	}

	ans := map[uint32]float64{
		1000: 0.09,
		1001: 0.19,
		1002: 0.28,
		1013: 0.36,
		1018: 0.49,
		1019: 0.58,
		1020: 0.67,
	}

	function := structs.Function{RangeFunction: segutils.Quantile_Over_Time, ValueList: []string{"0.9"}, TimeWindow: 10}

	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if !dtypeutils.AlmostEquals(expectedVal, val) {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}

func Test_applyRangeFunctionLastOverTime(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := map[uint32]float64{
		1000: 0.1,
		1001: 0.2,
		1002: 0.3,
		1013: 0.4,
		1018: 0.5,
		1019: 0.6,
		1020: 0.7,
	}

	result["metric"] = ts

	metricsResults := &MetricsResult{
		Results: result,
	}

	ans := map[uint32]float64{
		1000: 0.1,
		1001: 0.2,
		1002: 0.3,
		1013: 0.4,
		1018: 0.5,
		1019: 0.6,
		1020: 0.7,
	}

	function := structs.Function{RangeFunction: segutils.Last_Over_Time, TimeWindow: 10}

	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if !dtypeutils.AlmostEquals(expectedVal, val) {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}

func Test_applyRangeFunctionPresentOverTime(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := map[uint32]float64{
		1000: 0.1,
		1001: 0.2,
		1002: 0.3,
		1013: 0.4,
		1018: 0.5,
		1019: 0.6,
		1020: 0.7,
	}

	result["metric"] = ts

	metricsResults := &MetricsResult{
		Results: result,
	}

	ans := map[uint32]float64{
		1000: 1,
		1001: 1,
		1002: 1,
		1013: 1,
		1018: 1,
		1019: 1,
		1020: 1,
	}

	function := structs.Function{RangeFunction: segutils.Present_Over_Time, TimeWindow: 10}

	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if !dtypeutils.AlmostEquals(expectedVal, val) {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}

func Test_reduceEntries(t *testing.T) {
	entries := []Entry{
		Entry{downsampledTime: 0, dpVal: 4.3},
		Entry{downsampledTime: 1, dpVal: 5.0},
		Entry{downsampledTime: 2, dpVal: 1.7},
		Entry{downsampledTime: 8, dpVal: 5.0},
		Entry{downsampledTime: 5, dpVal: 0.0},
	}

	var functionConstant float64 // Only needed for quantile.
	var val float64
	var err error

	val, err = reduceEntries(entries, segutils.Sum, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(16.0, val))

	val, err = reduceEntries(entries, segutils.Max, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(5.0, val))

	val, err = reduceEntries(entries, segutils.Min, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(0.0, val))

	functionConstant = 0.5 // The median should be exactly 4.3
	val, err = reduceEntries(entries, segutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(4.3, val))

	functionConstant = 0.0 // The 0th percentile should be the min element
	val, err = reduceEntries(entries, segutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(0.0, val))

	functionConstant = 1.0 // The 100th percentile should be the max element
	val, err = reduceEntries(entries, segutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(5.0, val))

	// Since there are 5 elements, there are 4 buckets. So the 37.5th percentile
	// should be directly between sorted elements at index 1 and 2. Those
	// elements are 1.7 and 4.3, so the value should be 3.0.
	functionConstant = 0.375
	val, err = reduceEntries(entries, segutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(3.0, val))

	// Each quantile bucket has size 0.25, so at 0.25 * 1.25 = 0.25 + 0.0625 =
	// 0.3125, the quantile should be a quarter way between the elements at
	// indices 1 and 2. So 1.7 * 0.75 + 4.3 * 0.25 = 2.35.
	functionConstant = 0.3125
	val, err = reduceEntries(entries, segutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(2.35, val))

	// Avg is not implemented yet, so this should error.
	_, err = reduceEntries(entries, segutils.Avg, functionConstant)
	assert.NotNil(t, err)

	// Cardinality is not implemented yet, so this should error.
	_, err = reduceEntries(entries, segutils.Cardinality, functionConstant)
	assert.NotNil(t, err)
}

func Test_reduceRunningEntries(t *testing.T) {
	entries := []RunningEntry{
		RunningEntry{downsampledTime: 0, runningVal: 4.3, runningCount: 1},
		RunningEntry{downsampledTime: 1, runningVal: 5.0, runningCount: 1},
		RunningEntry{downsampledTime: 2, runningVal: 1.7, runningCount: 1},
		RunningEntry{downsampledTime: 8, runningVal: 5.0, runningCount: 1},
		RunningEntry{downsampledTime: 5, runningVal: 0.0, runningCount: 1},
	}

	var functionConstant float64 // Only needed for quantile.
	var val float64
	var err error

	val, err = reduceRunningEntries(entries, segutils.Avg, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(16.0/5, val))

	val, err = reduceRunningEntries(entries, segutils.Sum, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(16.0, val))

	val, err = reduceRunningEntries(entries, segutils.Max, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(5.0, val))

	val, err = reduceRunningEntries(entries, segutils.Min, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(0.0, val))

	functionConstant = 0.5 // The median should be exactly 4.3
	val, err = reduceRunningEntries(entries, segutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(4.3, val))

	functionConstant = 0.0 // The 0th percentile should be the min element
	val, err = reduceRunningEntries(entries, segutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(0.0, val))

	functionConstant = 1.0 // The 100th percentile should be the max element
	val, err = reduceRunningEntries(entries, segutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(5.0, val))

	// Since there are 5 elements, there are 4 buckets. So the 37.5th percentile
	// should be directly between sorted elements at index 1 and 2. Those
	// elements are 1.7 and 4.3, so the value should be 3.0.
	functionConstant = 0.375
	val, err = reduceRunningEntries(entries, segutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(3.0, val))

	// Each quantile bucket has size 0.25, so at 0.25 * 1.25 = 0.25 + 0.0625 =
	// 0.3125, the quantile should be a quarter way between the elements at
	// indices 1 and 2. So 1.7 * 0.75 + 4.3 * 0.25 = 2.35.
	functionConstant = 0.3125
	val, err = reduceRunningEntries(entries, segutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(2.35, val))

	// Cardinality is not implemented yet, so this should error.
	_, err = reduceRunningEntries(entries, segutils.Cardinality, functionConstant)
	assert.NotNil(t, err)
}

func Test_applyMathFunctionAbs(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1714880880] = -3
	ts[1714880881] = 2
	ts[1714880891] = -1

	result["metric"] = ts

	metricsResults := &MetricsResult{
		Results: result,
	}

	function := structs.Function{MathFunction: segutils.Abs}

	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			preVal, exists := ts[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != math.Abs(preVal) {
				t.Errorf("Expected value should be %v, but got %v", math.Abs(preVal), val)
			}
		}
	}

}

func Test_applyMathFunctionSqrt(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1714880880] = 9
	ts[1714880882] = 7744
	ts[1714880883] = 10000
	ts[1714880884] = 9801

	result["metric"] = ts
	ans := make(map[uint32]float64)
	ans[1714880880] = 3
	ans[1714880882] = 88
	ans[1714880883] = 100
	ans[1714880884] = 99

	metricsResults := &MetricsResult{
		Results: result,
	}

	function := structs.Function{MathFunction: segutils.Sqrt}
	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {

			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}

	ts[3] = -1
	result["metric"] = ts
	metricsResults.Results = result
	err = metricsResults.ApplyFunctionsToResults(8, function)
	assert.NotNil(t, err)
}

func Test_applyMathFunctionFloor(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1] = -0.255
	ts[2] = 0.6
	ts[3] = 11.2465

	result["metric"] = ts
	ans := make(map[uint32]float64)
	ans[1] = -1
	ans[2] = 0
	ans[3] = 11

	metricsResults := &MetricsResult{
		Results: result,
	}

	function := structs.Function{MathFunction: segutils.Floor, ValueList: []string{""}}
	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {

			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}

func Test_applyMathFunctionCeil(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1] = -0.255
	ts[2] = 0.6
	ts[3] = 11.2465

	result["metric"] = ts
	ans := make(map[uint32]float64)
	ans[1] = 0
	ans[2] = 1
	ans[3] = 12

	metricsResults := &MetricsResult{
		Results: result,
	}

	function := structs.Function{MathFunction: segutils.Ceil, ValueList: []string{""}}
	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {

			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}

}

func Test_applyMathFunctionRoundWithoutPrecision(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1] = -0.255
	ts[2] = 0.6
	ts[3] = 11.6465

	result["metric"] = ts
	ans := make(map[uint32]float64)
	ans[1] = 0
	ans[2] = 1
	ans[3] = 12

	metricsResults := &MetricsResult{
		Results: result,
	}

	function := structs.Function{MathFunction: segutils.Round, ValueList: []string{""}}
	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {

			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}

func Test_applyMathFunctionRoundWithPrecision1(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1] = -0.255
	ts[2] = 0.6
	ts[3] = 11.6465

	result["metric"] = ts
	ans := make(map[uint32]float64)
	ans[1] = -0.3
	ans[2] = 0.6
	ans[3] = 11.7

	metricsResults := &MetricsResult{
		Results: result,
	}

	function := structs.Function{MathFunction: segutils.Round, ValueList: []string{"0.3"}}
	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}

}

func Test_applyMathFunctionRoundWithPrecision2(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	result["metric"] = ts

	ans := make(map[uint32]float64)
	ans[1] = -0.5
	ans[2] = 0.5
	ans[3] = 11.5

	metricsResults := &MetricsResult{
		Results: result,
	}

	function := structs.Function{MathFunction: segutils.Round, ValueList: []string{"1 /2"}}
	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}

func Test_applyMathFunctionExp(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1714880880] = 0
	ts[1714880882] = 1
	ts[1714880883] = 2
	ts[1714880884] = 3

	result["metric"] = ts
	ans := make(map[uint32]float64)
	ans[1714880880] = 1
	ans[1714880882] = 2.718281828459045
	ans[1714880883] = 7.38905609893065
	ans[1714880884] = 20.085536923187668

	metricsResults := &MetricsResult{
		Results: result,
	}

	function := structs.Function{MathFunction: segutils.Exp}
	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {

			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}

func Test_applyMathFunctionLog2(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1] = 2
	ts[2] = 8

	result["metric"] = ts
	ans := make(map[uint32]float64)
	ans[1] = 1
	ans[2] = 3

	metricsResults := &MetricsResult{
		Results: result,
	}

	function := structs.Function{MathFunction: segutils.Log2}
	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}

	ts[3] = -1
	result["metric"] = ts
	metricsResults.Results = result
	err = metricsResults.ApplyFunctionsToResults(8, function)
	assert.NotNil(t, err)
}

func Test_applyMathFunctionLog10(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1] = 10
	ts[2] = 10000

	result["metric"] = ts
	ans := make(map[uint32]float64)
	ans[1] = 1
	ans[2] = 4

	metricsResults := &MetricsResult{
		Results: result,
	}

	function := structs.Function{MathFunction: segutils.Log10}
	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}

	ts[3] = -1
	result["metric"] = ts
	metricsResults.Results = result
	err = metricsResults.ApplyFunctionsToResults(8, function)
	assert.NotNil(t, err)
}

func Test_applyMathFunctionLn(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1] = math.E
	ts[2] = 7.38905609893065

	result["metric"] = ts
	ans := make(map[uint32]float64)
	ans[1] = 1
	ans[2] = 2

	metricsResults := &MetricsResult{
		Results: result,
	}

	function := structs.Function{MathFunction: segutils.Ln}
	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}

	ts[3] = -1
	result["metric"] = ts
	metricsResults.Results = result
	err = metricsResults.ApplyFunctionsToResults(8, function)
	assert.NotNil(t, err)
}

func Test_applyMathFunctionSgn(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1001] = 10
	ts[1002] = 0
	ts[1003] = -99.51

	result["metric"] = ts
	ans := make(map[uint32]float64)
	ans[1001] = 1
	ans[1002] = 0
	ans[1003] = -1

	metricsResults := &MetricsResult{
		Results: result,
	}

	function := structs.Function{MathFunction: segutils.Sgn}
	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}

func Test_applyMathFunctionDeg(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1001] = 10
	ts[1002] = 2.5
	ts[1003] = -99.51

	result["metric"] = ts
	ans := make(map[uint32]float64)
	ans[1001] = 10 * 180 / math.Pi
	ans[1002] = 2.5 * 180 / math.Pi
	ans[1003] = -99.51 * 180 / math.Pi

	metricsResults := &MetricsResult{
		Results: result,
	}

	function := structs.Function{MathFunction: segutils.Deg}
	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}

func Test_applyMathFunctionRad(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1001] = 10
	ts[1002] = 2.5
	ts[1003] = -99.51

	result["metric"] = ts
	ans := make(map[uint32]float64)
	ans[1001] = 10 * math.Pi / 180
	ans[1002] = 2.5 * math.Pi / 180
	ans[1003] = -99.51 * math.Pi / 180

	metricsResults := &MetricsResult{
		Results: result,
	}

	function := structs.Function{MathFunction: segutils.Rad}
	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}

func Test_applyTrigonometricFunctionCos(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Cos, segutils.Cos, false)
}

func Test_applyTrigonometricFunctionCosh(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Cosh, segutils.Cosh, false)
}

func Test_applyTrigonometricFunctionSin(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Sin, segutils.Sin, false)
}

func Test_applyTrigonometricFunctionSinh(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Sinh, segutils.Sinh, false)
}

func Test_applyTrigonometricFunctionTan(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Tan, segutils.Tan, false)
}

func Test_applyTrigonometricFunctionTanh(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Tanh, segutils.Tanh, false)
}

func Test_applyTrigonometricFunctionAsinh(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Asinh, segutils.Asinh, false)
}

func Test_applyTrigonometricFunctionAtan(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Atan, segutils.Atan, false)
}

func Test_applyTrigonometricFunctionAcos(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Acos, segutils.Acos, true)
}

func Test_applyTrigonometricFunctionAsin(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Asin, segutils.Asin, true)
}

func Test_applyTrigonometricFunctionAtanh(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Atanh, segutils.Atanh, true)
}

func Test_applyTrigonometricFunctionAcosh(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Acosh, segutils.Acosh, true)
}

func runTrigonometricFunctionTest(t *testing.T, mathFunc float64Func, mathFunction utils.MathFunctions, testError bool) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)

	// Define initial values based on whether we're testing an error case
	if mathFunction == utils.Acosh {
		ts[1] = 1.255
		ts[2] = 6
		ts[3] = 2.465
	} else if testError {
		ts[1] = 0.255
		ts[2] = 0.6
		ts[3] = -0.2465
	} else {
		ts[1] = -0.255
		ts[2] = 0.6
		ts[3] = 11.2465
	}

	result["metric"] = ts
	ans := make(map[uint32]float64)
	for key, val := range ts {
		ans[key] = mathFunc(val)
	}

	metricsResults := &MetricsResult{
		Results: result,
	}

	function := structs.Function{MathFunction: mathFunction}
	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {

			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}

	if testError {
		// Modify values to trigger error
		ts[3] = -10.2465
		err = metricsResults.ApplyFunctionsToResults(8, function)
		assert.NotNil(t, err)
	} else {
		// Add specific test for acosh case where valid input should be > 1
		if mathFunction == utils.Acosh {
			ts[3] = 0.2465
			err = metricsResults.ApplyFunctionsToResults(8, function)
			assert.NotNil(t, err)
		}
	}
}

func Test_applyMathFunctionClamp(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1714880880] = -30.2
	ts[1714880881] = 22
	ts[1714880891] = -10
	ts[1714880892] = 5.5

	result["metric"] = ts

	metricsResults := &MetricsResult{
		Results: result,
	}

	ans := make(map[uint32]float64)
	ans[1714880880] = 1
	ans[1714880881] = 10
	ans[1714880891] = 1
	ans[1714880892] = 5.5

	function := structs.Function{MathFunction: segutils.Clamp, ValueList: []string{"1", "10"}}

	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}

func Test_applyMathFunctionClampMin(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1714880880] = -30.2
	ts[1714880881] = 22
	ts[1714880891] = -10
	ts[1714880892] = 5.5

	result["metric"] = ts

	metricsResults := &MetricsResult{
		Results: result,
	}

	ans := make(map[uint32]float64)
	ans[1714880880] = 1
	ans[1714880881] = 22
	ans[1714880891] = 1
	ans[1714880892] = 5.5

	function := structs.Function{MathFunction: segutils.Clamp_Min, ValueList: []string{"1"}}

	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}

func Test_applyMathFunctionClampMax(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1714880880] = -30.2
	ts[1714880881] = 22
	ts[1714880891] = -10
	ts[1714880892] = 5.5

	result["metric"] = ts

	metricsResults := &MetricsResult{
		Results: result,
	}

	ans := make(map[uint32]float64)
	ans[1714880880] = -30.2
	ans[1714880881] = 4
	ans[1714880891] = -10
	ans[1714880892] = 4

	function := structs.Function{MathFunction: segutils.Clamp_Max, ValueList: []string{"4"}}

	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}

func Test_applyMathFunctionTimestamp(t *testing.T) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)
	ts[1714880880] = -30.2
	ts[1714880881] = 22
	ts[1714880891] = -10
	ts[1714880892] = 5.5

	result["metric"] = ts

	metricsResults := &MetricsResult{
		Results: result,
	}

	ans := make(map[uint32]float64)
	ans[1714880880] = 1714880880
	ans[1714880881] = 1714880881
	ans[1714880891] = 1714880891
	ans[1714880892] = 1714880892

	function := structs.Function{MathFunction: segutils.Timestamp}

	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if val != expectedVal {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}
func runTimeFunctionTest(t *testing.T, timeFunction utils.TimeFunctions, expectedCalculation func(time.Time) float64) {
	allMetricsData := make(map[string]map[uint32]float64)
	allDPs := make(map[uint32]float64)

	dpTs := uint32(time.Date(2024, 1, 1, 23, 0, 0, 0, time.UTC).Unix())
	allDPs[dpTs] = -30.2
	allDPs[dpTs+1] = 22
	allDPs[dpTs+11] = -10
	allDPs[dpTs+12] = 5.5

	allMetricsData["metric"] = allDPs

	metricsResults := &MetricsResult{
		Results: allMetricsData,
	}

	expectedResults := make(map[uint32]float64)
	for dpTs := range allDPs {
		expectedResults[dpTs] = expectedCalculation(time.Unix(int64(dpTs), 0).UTC())
	}

	function := structs.Function{TimeFunction: timeFunction}

	err := metricsResults.ApplyFunctionsToResults(8, function)
	assert.Nil(t, err)
	for metric, timeSeries := range metricsResults.Results {
		for dpTs, actualValue := range timeSeries {
			expectedValue, exists := expectedResults[dpTs]
			if !exists {
				t.Errorf("Unexpected timestamp: %v in metric: %v", dpTs, metric)
			}

			if actualValue != expectedValue {
				t.Errorf("For timestamp: %v in metric: %v, expected value: %v, but got: %v", dpTs, metric, expectedValue, actualValue)
			}
		}
	}
}

func Test_applyTimeFunctionHour(t *testing.T) {
	runTimeFunctionTest(t, segutils.Hour, func(t time.Time) float64 { return float64(t.Hour()) })
}

func Test_applyTimeFunctionMinute(t *testing.T) {
	runTimeFunctionTest(t, segutils.Minute, func(t time.Time) float64 { return float64(t.Minute()) })
}

func Test_applyTimeFunctionMonth(t *testing.T) {
	runTimeFunctionTest(t, segutils.Month, func(t time.Time) float64 { return float64(t.Month()) })
}

func Test_applyTimeFunctionYear(t *testing.T) {
	runTimeFunctionTest(t, segutils.Year, func(t time.Time) float64 { return float64(t.Year()) })
}

func Test_applyTimeFunctionDayOfMonth(t *testing.T) {
	runTimeFunctionTest(t, segutils.DayOfMonth, func(t time.Time) float64 { return float64(t.Day()) })
}

func Test_applyTimeFunctionDayOfWeek(t *testing.T) {
	runTimeFunctionTest(t, segutils.DayOfWeek, func(t time.Time) float64 { return float64(t.Weekday()) })
}

func Test_applyTimeFunctionDayOfYear(t *testing.T) {
	runTimeFunctionTest(t, segutils.DayOfYear, func(t time.Time) float64 { return float64(t.YearDay()) })
}

func Test_applyTimeFunctionDaysInMonth(t *testing.T) {
	runTimeFunctionTest(t, segutils.DaysInMonth, func(t time.Time) float64 {
		return float64(time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day())
	})
}
