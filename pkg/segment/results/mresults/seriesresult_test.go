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
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func Test_applyRangeFunctionRate(t *testing.T) {
	timeSeries := map[uint32]float64{
		980:  0.0,
		990:  1.0,
		1000: 2.0,
		1003: 3.0,
		1008: 4.0,
		1012: 18.0,
		1020: 2.5, // Counter reset
		1025: 6.5,
		1030: 8.5,
		1035: 10.5,
	}

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1050),
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: sutils.Rate, TimeWindow: 10, Step: 5}, timeRange)
	assert.Nil(t, err)

	/**
	  There windows for the evluation would be
		1. 990 - 1000 => Since both start and end data points exist, there will not extrapolation and no counter resets
				   => (2.0 - 1.0) / 10s = 0.1
		2. 995 - 1005 => Since the end and start time, does not exist, extrapolation for these points will be done.
		3. 1000 - 1010 => The end data point will be extrapolated
		4. 1005 - 1015 => Both data points will be extrapolated
		5. 1010 - 1020 => 1015 data point will be extrapolated and the counter reset at 1020 will be handled
		6. 1015 - 1025 => The start data point will be extrapolated and the counter reset at 1020 will be handled
		7. 1020 - 1030 => Same as 1st window
		8. 1025 - 1035 => Same as 1st window
		9. 1030 - 1040 => The last data point will be extrapolated
		10. 1035 - 1045 => Since there is only one data point, no rate will be calculated
		11. 1040 - 1050 => Since there are no data points, no rate will be calculated

		**** Handling of counter resets:
		for the window 1010 - 1020, the counter reset at 1020 will be handled by the following calculation
		initial dx = 2.5 - 18.0 = -15.5 (without extrapolation)
		And then from the first value of the sample window, the values will be checked and if the value is less than the previous value,
		the dx now will be adjusted by adding the previous value.
			=> prev value = 18.0
			=> current value = 2.5
			current value < prev value => dx = -15.5 + 18.0 = 2.5


		**** And then for extrapolation, the limit will be 1.1 * average duration of the sample window
		=> totalSampleDuartion = 1020 - 1012 = 8
		=> averageDurationBetweenSamples = totalSampleDuartion / totalSamplesMinusOne = 8 / 1 = 8
		=> extrapolationLimit =  1.1 * averageDurationBetweenSamples = 1.1 * 8 = 8.8

		=> durationToStart = The time difference between the first sample and the start of the window = 1012 - 1010 = 2
		=> durationToEnd = The time difference between the end of the window and the last sample = 1020 - 1020 = 0

		**	if durationToStart >= extrapolationLimit, then the durationToStart will become (average duration of the sample window / 2).
			We do this way, because Prometheus assumes that the series does not cover the whole range and they still extrapolate but not all the way to boundaries,
			but only half way. Which is their guess for where the series might have started or ended.
		** 	The same logic applies for durationToEnd.
		** If not, these durationToStart and durationToEnd will remain the same.

		And also to not extrapolate too much into the past where the series does not even exist, we estimate the time to zero or start value.
		=> estimatedZeroTime = totalSampleDuartion * (firstSample / dx) = 8 * (18.0 / 2.5) = 57.6
		=> Since estimatedZeroTime > durationToStart, the durationToStart will remain the same. Otherwise, it will be set to estimatedZeroTime.


		Since durationToStart < extrapolationLimit and durationToEnd < extrapolationLimit, the durationToStart and durationToEnd will remain the same.

		=> extrapolationDuration = totalSampleDuartion + durationToStart + durationToEnd = 8 + 2 + 0 = 10
		=> extrapolatedRate = (dx * (extrapolationDuration / totalSampleDuration)) / timewindow = (2.5 * (10 / 8)) / 10 = 0.3125

		If we apply similar logic for all other windows, we will get the following results:
		map[1000:0.1 1005:0.21666666666666665 1010:0.25 1015:2.85 1020:0.3125 1025:0.65 1030:0.6 1035:0.4]
	*/

	assert.Len(t, res, 9)

	var val float64
	var ok bool

	val, ok = res[1000]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 0.1))

	val, ok = res[1005]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 0.21666666666666665))

	val, ok = res[1010]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 0.25))

	val, ok = res[1015]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.85))

	val, ok = res[1020]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 0.3125))

	val, ok = res[1025]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 0.65))

	val, ok = res[1030]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 0.6))

	val, ok = res[1035]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 0.4))

	val, ok = res[1040]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 0.4))
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

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1009),
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: sutils.IRate, TimeWindow: 10}, timeRange)
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

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1003),
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

	function := structs.Function{RangeFunction: sutils.Predict_Linear, TimeWindow: 10, ValueList: []string{"1000"}}

	err := metricsResults.ApplyFunctionsToResults(8, function, timeRange)
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
		980:  0.0,
		990:  1.0,
		1000: 2.0,
		1003: 3.0,
		1008: 4.0,
		1012: 18.0,
		1020: 2.5,
		1025: 6.5,
		1030: 8.5,
		1035: 10.5,
	}

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1035),
	}

	timeWindow := float64(10)
	increase, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: sutils.Increase, TimeWindow: timeWindow, Step: 5}, timeRange)
	assert.Nil(t, err)

	// Check the rate function Test for the explanation of the results
	// For increase, the result will not be divided by the time window.
	// map[1000:1 1005:2.1666666666666665 1010:2.5 1015:28.5 1020:3.125 1025:6.5 1030:6 1035:4]

	assert.Len(t, increase, 8)

	var val float64
	var ok bool

	val, ok = increase[1000]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 1))

	val, ok = increase[1005]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.1666666666666665))

	val, ok = increase[1010]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.5))

	val, ok = increase[1015]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 28.5))

	val, ok = increase[1020]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 3.125))

	val, ok = increase[1025]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 6.5))

	val, ok = increase[1030]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 6))

	val, ok = increase[1035]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 4))
}

func Test_applyRangeFunctionDelta(t *testing.T) {
	timeSeries := map[uint32]float64{
		1000: 2.0,
		1001: 3.0,
		1002: 5.0,
		1013: 10.0,
		1018: 2.5,
	}

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1025),
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: sutils.Delta, TimeWindow: 10}, timeRange)
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

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1025),
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: sutils.IDelta, TimeWindow: 10}, timeRange)
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

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1025),
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: sutils.Changes, TimeWindow: 10}, timeRange)
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

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1025),
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: sutils.Resets, TimeWindow: 10}, timeRange)
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
		990:  1.0,
		1000: 2.0,
		1003: 3.0,
		1008: 4.0,
		1012: 18.0,
		1020: 2.5,
		1035: 6.5,
	}

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1035),
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: sutils.Avg_Over_Time, TimeWindow: 20, Step: 15}, timeRange)
	assert.Nil(t, err)

	/**
	 * The result will be:
	 Evalution starts at 1000 and evaluates at every 15 seconds with 20 seconds lookback at each evaluation
	 => 1000: 1000-20 = 980 => 1.0 + 2.0 = 3.0 => (1.0 + 2.0) / 2 = 1.5
	 => 1015: 1015-20 = 995 => (2.0 + 3.0 + 4.0 + 18.0) / 4 = 6.75
	 => 1030: 1030-20 = 1010 => (18.0 + 2.5) / 2 = 10.25
	**/

	assert.Len(t, res, 3)

	var val float64
	var ok bool

	val, ok = res[1000]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 1.5))

	val, ok = res[1015]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (2.0+3.0+4.0+18.0)/(4)))

	val, ok = res[1030]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (18.0+2.5)/(2)))
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

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1035),
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: sutils.Min_Over_Time, TimeWindow: 10, Step: 10}, timeRange)
	assert.Nil(t, err)

	/**
	 * The result will be:
	 Evalution starts at 1000 and evaluates at every 10 seconds with 10 seconds lookback at each evaluation
	 => 1000: 1000-10 = 990 => 2.0
	 => 1010: 1010-10 = 1000 => 1.0
	 => 1020: 1020-10 = 1010 => 18.0
	 => 1030: 1030-10 = 1020 => 2.5
	**/

	assert.Len(t, res, 4)

	var val float64
	var ok bool

	val, ok = res[1000]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.0))

	val, ok = res[1010]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 1.0))

	val, ok = res[1020]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 18.0))

	val, ok = res[1030]
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

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1035),
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: sutils.Max_Over_Time, TimeWindow: 10, Step: 10}, timeRange)
	assert.Nil(t, err)

	/**
	 * The result will be:
	 Evalution starts at 1000 and evaluates at every 10 seconds with 10 seconds lookback at each evaluation
	 => 1000: 1000-10 = 990 => 2.0
	 => 1010: 1010-10 = 1000 => 3.0
	 => 1020: 1020-10 = 1010 => 18.0
	 => 1030: 1030-10 = 1020 => 6.5
	**/

	assert.Len(t, res, 4)

	var val float64
	var ok bool

	val, ok = res[1000]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.0))

	val, ok = res[1010]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 3.0))

	val, ok = res[1020]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 18.0))

	val, ok = res[1030]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 6.5))
}

func Test_applyRangeFunctionSum(t *testing.T) {
	timeSeries := map[uint32]float64{
		980:  1.0,
		990:  1.0,
		1000: 2.0,
		1003: 3.0,
		1008: 4.0,
		1012: 18.0,
		1020: 2.5,
		1025: 6.5,
		1030: 1.5,
		1035: 2.5,
	}

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1035),
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: sutils.Sum_Over_Time, TimeWindow: 20, Step: 15}, timeRange)
	assert.Nil(t, err)

	/**
	 * The result will be:
	 Evalution starts at 1000 and evaluates at every 15 seconds with 20 seconds lookback at each evaluation
	 => 1000: 1000-20 = 980 => 1.0 + 2.0 = 3.0
	 => 1015: 1015-20 = 995 => 2.0 + 3.0 + 4.0 + 18.0 = 27.0
	 => 1030: 1030-20 = 1010 => 18.0 + 2.5 + 6.5 + 1.5 = 28.5
	**/

	assert.Len(t, res, 3)

	var val float64
	var ok bool

	val, ok = res[1000]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 3.0))

	val, ok = res[1015]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 27.0))

	val, ok = res[1030]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 28.5))
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

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1025),
	}

	res, err := ApplyRangeFunction(timeSeries, structs.Function{RangeFunction: sutils.Count_Over_Time, TimeWindow: 10, Step: 10}, timeRange)
	assert.Nil(t, err)

	/**
	 * The result will be:
	 Evalution starts at 1000 and evaluates at every 10 seconds with 10 seconds lookback at each evaluation
	 => 1000: 1000-10 = 990 => 1
	 => 1010: 1010-10 = 1000 => 2
	 => 1020: 1020-10 = 1010 => 2
	**/

	assert.Len(t, res, 3)

	var val float64
	var ok bool

	val, ok = res[1000]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 1))

	val, ok = res[1010]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2))

	val, ok = res[1020]
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

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1025),
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

	function := structs.Function{RangeFunction: sutils.Stdvar_Over_Time, TimeWindow: 10}

	err := metricsResults.ApplyFunctionsToResults(8, function, timeRange)
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

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1025),
	}

	function := structs.Function{RangeFunction: sutils.Stddev_Over_Time, TimeWindow: 10}

	err := metricsResults.ApplyFunctionsToResults(8, function, timeRange)
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

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1025),
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

	function := structs.Function{RangeFunction: sutils.Quantile_Over_Time, ValueList: []string{"0.9"}, TimeWindow: 10}

	err := metricsResults.ApplyFunctionsToResults(8, function, timeRange)
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

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1025),
	}

	result["metric"] = ts

	metricsResults := &MetricsResult{
		Results: result,
	}

	ans := map[uint32]float64{
		1000: 0.1,
		1010: 0.3,
		1020: 0.7,
	}

	function := structs.Function{RangeFunction: sutils.Last_Over_Time, TimeWindow: 10, Step: 10}

	err := metricsResults.ApplyFunctionsToResults(8, function, timeRange)
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

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1025),
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

	function := structs.Function{RangeFunction: sutils.Present_Over_Time, TimeWindow: 10}

	err := metricsResults.ApplyFunctionsToResults(8, function, timeRange)
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

	val, err = reduceEntries(entries, sutils.Sum, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(16.0, val))

	val, err = reduceEntries(entries, sutils.Max, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(5.0, val))

	val, err = reduceEntries(entries, sutils.Min, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(0.0, val))

	functionConstant = 0.5 // The median should be exactly 4.3
	val, err = reduceEntries(entries, sutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(4.3, val))

	functionConstant = 0.0 // The 0th percentile should be the min element
	val, err = reduceEntries(entries, sutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(0.0, val))

	functionConstant = 1.0 // The 100th percentile should be the max element
	val, err = reduceEntries(entries, sutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(5.0, val))

	// Since there are 5 elements, there are 4 buckets. So the 37.5th percentile
	// should be directly between sorted elements at index 1 and 2. Those
	// elements are 1.7 and 4.3, so the value should be 3.0.
	functionConstant = 0.375
	val, err = reduceEntries(entries, sutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(3.0, val))

	// Each quantile bucket has size 0.25, so at 0.25 * 1.25 = 0.25 + 0.0625 =
	// 0.3125, the quantile should be a quarter way between the elements at
	// indices 1 and 2. So 1.7 * 0.75 + 4.3 * 0.25 = 2.35.
	functionConstant = 0.3125
	val, err = reduceEntries(entries, sutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(2.35, val))

	// Avg is not implemented yet, so this should error.
	_, err = reduceEntries(entries, sutils.Avg, functionConstant)
	assert.NotNil(t, err)

	// Cardinality is not implemented yet, so this should error.
	_, err = reduceEntries(entries, sutils.Cardinality, functionConstant)
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

	val, err = reduceRunningEntries(entries, sutils.Avg, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(16.0/5, val))

	val, err = reduceRunningEntries(entries, sutils.Sum, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(16.0, val))

	val, err = reduceRunningEntries(entries, sutils.Max, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(5.0, val))

	val, err = reduceRunningEntries(entries, sutils.Min, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(0.0, val))

	functionConstant = 0.5 // The median should be exactly 4.3
	val, err = reduceRunningEntries(entries, sutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(4.3, val))

	functionConstant = 0.0 // The 0th percentile should be the min element
	val, err = reduceRunningEntries(entries, sutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(0.0, val))

	functionConstant = 1.0 // The 100th percentile should be the max element
	val, err = reduceRunningEntries(entries, sutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(5.0, val))

	// Since there are 5 elements, there are 4 buckets. So the 37.5th percentile
	// should be directly between sorted elements at index 1 and 2. Those
	// elements are 1.7 and 4.3, so the value should be 3.0.
	functionConstant = 0.375
	val, err = reduceRunningEntries(entries, sutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(3.0, val))

	// Each quantile bucket has size 0.25, so at 0.25 * 1.25 = 0.25 + 0.0625 =
	// 0.3125, the quantile should be a quarter way between the elements at
	// indices 1 and 2. So 1.7 * 0.75 + 4.3 * 0.25 = 2.35.
	functionConstant = 0.3125
	val, err = reduceRunningEntries(entries, sutils.Quantile, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(2.35, val))

	// Cardinality is not implemented yet, so this should error.
	_, err = reduceRunningEntries(entries, sutils.Cardinality, functionConstant)
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

	function := structs.Function{MathFunction: sutils.Abs}

	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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

	function := structs.Function{MathFunction: sutils.Sqrt}
	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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
	err = metricsResults.ApplyFunctionsToResults(8, function, nil)
	assert.Nil(t, err)
	assert.Equal(t, fmt.Sprint(math.NaN()), fmt.Sprint(metricsResults.Results["metric"][3]))
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

	function := structs.Function{MathFunction: sutils.Floor, ValueList: []string{""}}
	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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

	function := structs.Function{MathFunction: sutils.Ceil, ValueList: []string{""}}
	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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

	function := structs.Function{MathFunction: sutils.Round, ValueList: []string{""}}
	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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

	function := structs.Function{MathFunction: sutils.Round, ValueList: []string{"0.3"}}
	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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

	function := structs.Function{MathFunction: sutils.Round, ValueList: []string{"1 /2"}}
	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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

	function := structs.Function{MathFunction: sutils.Exp}
	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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

	function := structs.Function{MathFunction: sutils.Log2}
	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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
	err = metricsResults.ApplyFunctionsToResults(8, function, nil)
	assert.Nil(t, err)
	assert.Equal(t, fmt.Sprint(math.NaN()), fmt.Sprint(metricsResults.Results["metric"][3]))
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

	function := structs.Function{MathFunction: sutils.Log10}
	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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
	err = metricsResults.ApplyFunctionsToResults(8, function, nil)
	assert.Nil(t, err)
	assert.Equal(t, fmt.Sprint(math.NaN()), fmt.Sprint(metricsResults.Results["metric"][3]))
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

	function := structs.Function{MathFunction: sutils.Ln}
	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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
	err = metricsResults.ApplyFunctionsToResults(8, function, nil)
	assert.Nil(t, err)
	assert.Equal(t, fmt.Sprint(math.NaN()), fmt.Sprint(metricsResults.Results["metric"][3]))
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

	function := structs.Function{MathFunction: sutils.Sgn}
	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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

	function := structs.Function{MathFunction: sutils.Deg}
	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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

	function := structs.Function{MathFunction: sutils.Rad}
	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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
	runTrigonometricFunctionTest(t, math.Cos, sutils.Cos, false)
}

func Test_applyTrigonometricFunctionCosh(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Cosh, sutils.Cosh, false)
}

func Test_applyTrigonometricFunctionSin(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Sin, sutils.Sin, false)
}

func Test_applyTrigonometricFunctionSinh(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Sinh, sutils.Sinh, false)
}

func Test_applyTrigonometricFunctionTan(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Tan, sutils.Tan, false)
}

func Test_applyTrigonometricFunctionTanh(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Tanh, sutils.Tanh, false)
}

func Test_applyTrigonometricFunctionAsinh(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Asinh, sutils.Asinh, false)
}

func Test_applyTrigonometricFunctionAtan(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Atan, sutils.Atan, false)
}

func Test_applyTrigonometricFunctionAcos(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Acos, sutils.Acos, true)
}

func Test_applyTrigonometricFunctionAsin(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Asin, sutils.Asin, true)
}

func Test_applyTrigonometricFunctionAtanh(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Atanh, sutils.Atanh, true)
}

func Test_applyTrigonometricFunctionAcosh(t *testing.T) {
	runTrigonometricFunctionTest(t, math.Acosh, sutils.Acosh, true)
}

func runTrigonometricFunctionTest(t *testing.T, mathFunc float64Func, mathFunction sutils.MathFunctions, testError bool) {
	result := make(map[string]map[uint32]float64)
	ts := make(map[uint32]float64)

	// Define initial values based on whether we're testing an error case
	if mathFunction == sutils.Acosh {
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
	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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
		err = metricsResults.ApplyFunctionsToResults(8, function, nil)
		assert.NotNil(t, err)
	} else {
		// Add specific test for acosh case where valid input should be > 1
		if mathFunction == sutils.Acosh {
			ts[3] = 0.2465
			err = metricsResults.ApplyFunctionsToResults(8, function, nil)
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

	function := structs.Function{MathFunction: sutils.Clamp, ValueList: []string{"1", "10"}}

	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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

	function := structs.Function{MathFunction: sutils.Clamp_Min, ValueList: []string{"1"}}

	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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

	function := structs.Function{MathFunction: sutils.Clamp_Max, ValueList: []string{"4"}}

	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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

	function := structs.Function{MathFunction: sutils.Timestamp}

	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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
func runTimeFunctionTest(t *testing.T, timeFunction sutils.TimeFunctions, expectedCalculation func(time.Time) float64) {
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

	err := metricsResults.ApplyFunctionsToResults(8, function, nil)
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
	runTimeFunctionTest(t, sutils.Hour, func(t time.Time) float64 { return float64(t.Hour()) })
}

func Test_applyTimeFunctionMinute(t *testing.T) {
	runTimeFunctionTest(t, sutils.Minute, func(t time.Time) float64 { return float64(t.Minute()) })
}

func Test_applyTimeFunctionMonth(t *testing.T) {
	runTimeFunctionTest(t, sutils.Month, func(t time.Time) float64 { return float64(t.Month()) })
}

func Test_applyTimeFunctionYear(t *testing.T) {
	runTimeFunctionTest(t, sutils.Year, func(t time.Time) float64 { return float64(t.Year()) })
}

func Test_applyTimeFunctionDayOfMonth(t *testing.T) {
	runTimeFunctionTest(t, sutils.DayOfMonth, func(t time.Time) float64 { return float64(t.Day()) })
}

func Test_applyTimeFunctionDayOfWeek(t *testing.T) {
	runTimeFunctionTest(t, sutils.DayOfWeek, func(t time.Time) float64 { return float64(t.Weekday()) })
}

func Test_applyTimeFunctionDayOfYear(t *testing.T) {
	runTimeFunctionTest(t, sutils.DayOfYear, func(t time.Time) float64 { return float64(t.YearDay()) })
}

func Test_applyTimeFunctionDaysInMonth(t *testing.T) {
	runTimeFunctionTest(t, sutils.DaysInMonth, func(t time.Time) float64 {
		return float64(time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day())
	})
}

func Test_applyRangeFunctionMADOverTime(t *testing.T) {

	result := make(map[string]map[uint32]float64)
	ts := map[uint32]float64{
		1000: 10,
		1001: 20.5,
		1002: 30.25,
		1013: 40,
		1018: 50.75,
		1019: 60,
		1020: 70.5,
	}

	result["metric"] = ts

	metricsResults := &MetricsResult{
		Results: result,
	}

	ans := map[uint32]float64{
		1000: 0,
		1001: 5.25,
		1002: 9.75,
		1013: 0,
		1018: 5.375,
		1019: 9.25,
		1020: 9.875,
	}

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(1000),
		EndEpochSec:   uint32(1025),
	}

	function := structs.Function{RangeFunction: sutils.Mad_Over_Time, TimeWindow: 10}

	err := metricsResults.ApplyFunctionsToResults(8, function, timeRange)
	assert.Nil(t, err)
	for _, timeSeries := range metricsResults.Results {
		for key, val := range timeSeries {
			expectedVal, exists := ans[key]
			if !exists {
				t.Errorf("Should not have this key: %v", key)
			}

			if !dtypeutils.AlmostEquals(expectedVal, val) {
				t.Errorf("Expected value should be %v, but got %v for timestamp %v", expectedVal, val, key)
			}
		}
	}
}

func Test_applyLabelReplace(t *testing.T) {
	initSeriesId := `process_runtime_go_goroutines{job:product-catalog,`

	labelFunctionExpr := &structs.LabelFunctionExpr{
		FunctionType:     sutils.LabelReplace,
		DestinationLabel: "newLabel",
		SourceLabel:      "job",
		Replacement: &structs.LabelReplacementKey{
			KeyType:      structs.NameBased,
			NameBasedVal: "name",
		},
	}

	rawRegex := "(?P<name>.*)-(?P<version>.*)"

	labelFunctionExpr.GobRegexp = &utils.GobbableRegex{}
	err := labelFunctionExpr.GobRegexp.SetRegex(rawRegex)
	assert.Nil(t, err)

	expectedSeriesId := `process_runtime_go_goroutines{job:product-catalog,newLabel:product,`

	seriesId, err := applyLabelReplace(initSeriesId, labelFunctionExpr)
	assert.Nil(t, err)
	assert.Equal(t, expectedSeriesId, seriesId)

	rawRegex = "(.*)-.*"
	err = labelFunctionExpr.GobRegexp.SetRegex(rawRegex)
	assert.Nil(t, err)
	labelFunctionExpr.DestinationLabel = "job"
	labelFunctionExpr.Replacement.KeyType = structs.IndexBased
	labelFunctionExpr.Replacement.IndexBasedVal = 1

	expectedSeriesId = `process_runtime_go_goroutines{job:product,`

	seriesId, err = applyLabelReplace(initSeriesId, labelFunctionExpr)
	assert.Nil(t, err)
	assert.Equal(t, expectedSeriesId, seriesId)
}
