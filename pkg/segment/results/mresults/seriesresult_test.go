/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mresults

import (
	"testing"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_applyRangeFunctionRate(t *testing.T) {
	timeSeries := map[uint32]float64{
		0: 2.0,
		1: 3.0,
		2: 4.0,
		3: 0.0,
		8: 2.5,
		9: 1.0,
	}

	rate, err := ApplyRangeFunction(timeSeries, segutils.Rate)
	assert.Nil(t, err)

	// There's six timestamps in the series, but we need two points to calculate
	// the rate, so we can't calculate it on the first point. So we should have
	// 5 elements in the result.
	assert.Len(t, rate, 5)

	var val float64
	var ok bool

	val, ok = rate[1]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (3.0-2.0)/(1-0)))

	val, ok = rate[2]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, (4.0-3.0)/(2-1)))

	val, ok = rate[3]
	assert.True(t, ok)
	// Since the value here is smaller than at the last timestamp, the value was
	// reset since the last timestamp. So the increase is just this value, not
	// this value minus the previous value.
	assert.True(t, dtypeutils.AlmostEquals(val, 0.0))

	val, ok = rate[8]
	assert.True(t, ok)
	assert.True(t, dtypeutils.AlmostEquals(val, 2.5/(8-3)))

	val, ok = rate[9]
	assert.True(t, ok)
	// Since the value here is smaller than at the last timestamp, the value was
	// reset since the last timestamp. So the increase is just this value, not
	// this value minus the previous value.
	assert.True(t, dtypeutils.AlmostEquals(val, 1.0/(9-8)))
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

	val, err = reduceEntries(entries, segutils.Count, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(5.0, val))

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

	val, err = reduceRunningEntries(entries, segutils.Count, functionConstant)
	assert.Nil(t, err)
	assert.True(t, dtypeutils.AlmostEquals(5.0, val))

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
