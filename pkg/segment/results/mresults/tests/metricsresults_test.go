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

package tests

import (
	"testing"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment"
	mresults "github.com/siglens/siglens/pkg/segment/results/mresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/bytebufferpool"
)

func Test_GetResults_AggFn_Sum(t *testing.T) {
	mQuery := &structs.MetricsQuery{
		MetricName: "test.metric.0",
		Aggregator: structs.Aggregation{AggregatorFunction: utils.Sum},
		Downsampler: structs.Downsampler{
			Interval:   3,
			Unit:       "h",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Sum},
		},
		TagsFilters: []*structs.TagsFilter{
			{
				TagKey:      "color",
				RawTagValue: "yellow`",
			},
		},
	}
	qid := uint64(0)
	metricsResults := mresults.InitMetricResults(mQuery, qid)
	assert.NotNil(t, metricsResults)

	var tsGroupId *bytebufferpool.ByteBuffer = bytebufferpool.Get()
	defer bytebufferpool.Put(tsGroupId)
	_, err := tsGroupId.Write([]byte("color:yellow"))
	assert.NoError(t, err)
	series := mresults.InitSeriesHolder(mQuery, tsGroupId)

	// they should all downsample to 0
	sum := float64(0)
	for i := 0; i < 10; i++ {
		sum += float64(i)
		series.AddEntry(uint32(i), float64(i))
	}

	assert.Equal(t, series.GetIdx(), 10)
	tsid := uint64(100)
	metricsResults.AddSeries(series, tsid, tsGroupId)
	assert.Len(t, metricsResults.AllSeries, 1)
	assert.Len(t, metricsResults.DsResults, 0)
	assert.Len(t, metricsResults.Results, 0)
	assert.Equal(t, metricsResults.State, mresults.SERIES_READING)

	metricsResults.DownsampleResults(mQuery.Downsampler, 1)
	assert.Equal(t, metricsResults.State, mresults.DOWNSAMPLING)
	assert.Len(t, metricsResults.AllSeries, 0)
	assert.Len(t, metricsResults.DsResults, 1)
	assert.Len(t, metricsResults.Results, 0)
	assert.Contains(t, metricsResults.DsResults, tsGroupId.String())

	errors := metricsResults.AggregateResults(1)
	assert.Nil(t, errors)
	assert.Len(t, metricsResults.AllSeries, 0)
	assert.Len(t, metricsResults.DsResults, 0)
	assert.Len(t, metricsResults.Results, 1)
	assert.Contains(t, metricsResults.Results, tsGroupId.String())
	retVal := metricsResults.Results[tsGroupId.String()]
	assert.Len(t, retVal, 1)
	assert.Contains(t, retVal, uint32(0))
	assert.Equal(t, retVal[0], sum)

	mQResponse, err := metricsResults.GetOTSDBResults(mQuery)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(mQResponse))
	assert.Equal(t, 1, len(mQResponse[0].Dps))
	for _, val := range mQResponse[0].Dps {
		assert.Equal(t, sum, val)
	}
}

func Test_GetResults_AggFn_Avg(t *testing.T) {
	mQuery := &structs.MetricsQuery{
		MetricName: "test.metric.0",
		Aggregator: structs.Aggregation{AggregatorFunction: utils.Avg},
		Downsampler: structs.Downsampler{
			Interval:   1,
			Unit:       "h",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Sum},
		},
		TagsFilters: []*structs.TagsFilter{
			{
				TagKey:      "color",
				RawTagValue: "yellow`",
			},
		},
	}
	qid := uint64(0)
	metricsResults := mresults.InitMetricResults(mQuery, qid)
	assert.NotNil(t, metricsResults)

	var tsGroupId *bytebufferpool.ByteBuffer = bytebufferpool.Get()
	defer bytebufferpool.Put(tsGroupId)
	_, err := tsGroupId.Write([]byte("color:yellow"))
	assert.NoError(t, err)
	series := mresults.InitSeriesHolder(mQuery, tsGroupId)

	// they should all downsample to i
	avg := float64(0)
	for i := 0; i < 10; i++ {
		avg += float64(i)
		series.AddEntry(uint32(i), float64(i))
	}
	finalAvg := avg // because we have 1 series, with a 1h-sum:avg, the avg does nothing

	assert.Equal(t, series.GetIdx(), 10)
	tsid := uint64(100)
	metricsResults.AddSeries(series, tsid, tsGroupId)
	assert.Len(t, metricsResults.AllSeries, 1)
	assert.Len(t, metricsResults.DsResults, 0)
	assert.Len(t, metricsResults.Results, 0)
	assert.Equal(t, metricsResults.State, mresults.SERIES_READING)

	metricsResults.DownsampleResults(mQuery.Downsampler, 1)
	assert.Equal(t, metricsResults.State, mresults.DOWNSAMPLING)
	assert.Len(t, metricsResults.AllSeries, 0)
	assert.Len(t, metricsResults.DsResults, 1)
	assert.Len(t, metricsResults.Results, 0)
	assert.Contains(t, metricsResults.DsResults, tsGroupId.String())

	errors := metricsResults.AggregateResults(1)
	assert.Nil(t, errors)

	assert.Len(t, metricsResults.AllSeries, 0)
	assert.Len(t, metricsResults.DsResults, 0)
	assert.Len(t, metricsResults.Results, 1)
	assert.Contains(t, metricsResults.Results, tsGroupId.String())
	retVal := metricsResults.Results[tsGroupId.String()]
	assert.Len(t, retVal, 1)
	assert.Contains(t, retVal, uint32(0))
	assert.Equal(t, retVal[0], finalAvg)

	mQResponse, err := metricsResults.GetOTSDBResults(mQuery)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(mQResponse))
	assert.Equal(t, 1, len(mQResponse[0].Dps))
	for _, val := range mQResponse[0].Dps {
		assert.Equal(t, finalAvg, val)
	}
}

func Test_GetResults_AggFn_Multiple(t *testing.T) {
	var numSeries int = 5
	mQuery := &structs.MetricsQuery{
		MetricName: "test.metric.0",
		Aggregator: structs.Aggregation{AggregatorFunction: utils.Avg},
		Downsampler: structs.Downsampler{
			Interval:   2,
			Unit:       "s",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Sum},
		},
		TagsFilters: []*structs.TagsFilter{
			{
				TagKey:      "color",
				RawTagValue: "yellow`",
			},
		},
	}

	qid := uint64(0)
	metricsResults := mresults.InitMetricResults(mQuery, qid)
	assert.NotNil(t, metricsResults)
	dsSec := mQuery.Downsampler.GetIntervalTimeInSeconds()

	grpId := []byte("yellow`")
	for i := 0; i < numSeries; i++ {
		var tsGroupId *bytebufferpool.ByteBuffer = bytebufferpool.Get()
		defer bytebufferpool.Put(tsGroupId)
		_, err := tsGroupId.Write(grpId)
		assert.NoError(t, err)
		series := mresults.InitSeriesHolder(mQuery, tsGroupId)
		for i := 0; i < 10; i++ {
			series.AddEntry(uint32(i), float64(i))
		}
		metricsResults.AddSeries(series, uint64(i), tsGroupId)
	}
	assert.Len(t, metricsResults.AllSeries, numSeries)
	assert.Len(t, metricsResults.DsResults, 0)
	assert.Len(t, metricsResults.Results, 0)
	assert.Equal(t, metricsResults.State, mresults.SERIES_READING)

	metricsResults.DownsampleResults(mQuery.Downsampler, 1)
	assert.Equal(t, metricsResults.State, mresults.DOWNSAMPLING)
	assert.Len(t, metricsResults.AllSeries, 0)
	assert.Len(t, metricsResults.DsResults, 1)
	assert.Len(t, metricsResults.Results, 0)
	assert.Contains(t, metricsResults.DsResults, string(grpId))

	errors := metricsResults.AggregateResults(1)
	assert.Nil(t, errors)
	assert.Len(t, metricsResults.AllSeries, 0)
	assert.Len(t, metricsResults.DsResults, 0)
	assert.Len(t, metricsResults.Results, 1)
	assert.Contains(t, metricsResults.Results, "yellow`")
	retVal := metricsResults.Results["yellow`"]
	for i := uint32(0); i < 10; i++ {
		newTime := ((i / dsSec) * dsSec)
		assert.Contains(t, retVal, newTime)
	}
}

func Test_GetResults_AggFn_Quantile(t *testing.T) {
	mQuery := &structs.MetricsQuery{
		MetricName: "test.metric.0",
		Aggregator: structs.Aggregation{AggregatorFunction: utils.Quantile, FuncConstant: 0.5},
		Downsampler: structs.Downsampler{
			Interval:   3,
			Unit:       "h",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Quantile, FuncConstant: 0.5},
		},
		TagsFilters: []*structs.TagsFilter{
			{
				TagKey:      "color",
				RawTagValue: "yellow`",
			},
		},
	}
	qid := uint64(0)
	metricsResults := mresults.InitMetricResults(mQuery, qid)
	assert.NotNil(t, metricsResults)

	var tsGroupId *bytebufferpool.ByteBuffer = bytebufferpool.Get()
	defer bytebufferpool.Put(tsGroupId)
	_, err := tsGroupId.Write([]byte("color:yellow"))
	assert.NoError(t, err)
	series := mresults.InitSeriesHolder(mQuery, tsGroupId)

	// they should all downsample to 0
	sum := float64(0)
	for i := 0; i < 10; i++ {
		sum += float64(i)
		series.AddEntry(uint32(i), float64(i))
	}

	assert.Equal(t, series.GetIdx(), 10)
	tsid := uint64(100)
	metricsResults.AddSeries(series, tsid, tsGroupId)
	assert.Len(t, metricsResults.AllSeries, 1)
	assert.Len(t, metricsResults.DsResults, 0)
	assert.Len(t, metricsResults.Results, 0)
	assert.Equal(t, metricsResults.State, mresults.SERIES_READING)

	metricsResults.DownsampleResults(mQuery.Downsampler, 1)
	assert.Equal(t, metricsResults.State, mresults.DOWNSAMPLING)
	assert.Len(t, metricsResults.AllSeries, 0)
	assert.Len(t, metricsResults.DsResults, 1)
	assert.Len(t, metricsResults.Results, 0)
	assert.Contains(t, metricsResults.DsResults, tsGroupId.String())

	errors := metricsResults.AggregateResults(1)
	assert.Nil(t, errors)
	assert.Len(t, metricsResults.AllSeries, 0)
	assert.Len(t, metricsResults.DsResults, 0)
	assert.Len(t, metricsResults.Results, 1)
	assert.Contains(t, metricsResults.Results, tsGroupId.String())
	retVal := metricsResults.Results[tsGroupId.String()]
	assert.Len(t, retVal, 1)
	assert.Contains(t, retVal, uint32(0))
	assert.Equal(t, retVal[0], float64(4.5))

	mQResponse, err := metricsResults.GetOTSDBResults(mQuery)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(mQResponse))
	assert.Equal(t, 1, len(mQResponse[0].Dps))
	for _, val := range mQResponse[0].Dps {
		assert.Equal(t, float64(4.5), val)
	}
}

func Test_GetResults_AggFn_QuantileFloatIndex(t *testing.T) {
	mQuery := &structs.MetricsQuery{
		MetricName: "test.metric.0",
		Aggregator: structs.Aggregation{AggregatorFunction: utils.Quantile, FuncConstant: 0.3},
		Downsampler: structs.Downsampler{
			Interval:   3,
			Unit:       "h",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Quantile, FuncConstant: 0.3},
		},
		TagsFilters: []*structs.TagsFilter{
			{
				TagKey:      "color",
				RawTagValue: "yellow`",
			},
		},
	}
	qid := uint64(0)
	metricsResults := mresults.InitMetricResults(mQuery, qid)
	assert.NotNil(t, metricsResults)

	var tsGroupId *bytebufferpool.ByteBuffer = bytebufferpool.Get()
	defer bytebufferpool.Put(tsGroupId)
	_, err := tsGroupId.Write([]byte("color:yellow`"))
	assert.NoError(t, err)
	series := mresults.InitSeriesHolder(mQuery, tsGroupId)

	// they should all downsample to 0
	sum := float64(0)
	for i := 0; i < 10; i++ {
		sum += float64(i)
		series.AddEntry(uint32(i), float64(i))
	}

	assert.Equal(t, series.GetIdx(), 10)
	tsid := uint64(100)
	metricsResults.AddSeries(series, tsid, tsGroupId)
	assert.Len(t, metricsResults.AllSeries, 1)
	assert.Len(t, metricsResults.DsResults, 0)
	assert.Len(t, metricsResults.Results, 0)
	assert.Equal(t, metricsResults.State, mresults.SERIES_READING)

	metricsResults.DownsampleResults(mQuery.Downsampler, 1)
	assert.Equal(t, metricsResults.State, mresults.DOWNSAMPLING)
	assert.Len(t, metricsResults.AllSeries, 0)
	assert.Len(t, metricsResults.DsResults, 1)
	assert.Len(t, metricsResults.Results, 0)
	assert.Contains(t, metricsResults.DsResults, tsGroupId.String())

	errors := metricsResults.AggregateResults(1)
	assert.Nil(t, errors)
	assert.Len(t, metricsResults.AllSeries, 0)
	assert.Len(t, metricsResults.DsResults, 0)
	assert.Len(t, metricsResults.Results, 1)
	assert.Contains(t, metricsResults.Results, tsGroupId.String())
	retVal := metricsResults.Results[tsGroupId.String()]
	assert.Len(t, retVal, 1)
	assert.Contains(t, retVal, uint32(0))
	assert.True(t, dtypeutils.AlmostEquals(retVal[0], float64(2.7)))

	mQResponse, err := metricsResults.GetOTSDBResults(mQuery)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(mQResponse))
	assert.Equal(t, 1, len(mQResponse[0].Dps))
	for _, val := range mQResponse[0].Dps {
		assert.True(t, dtypeutils.AlmostEquals(val, float64(2.7)))
	}
}

func TestCalculateInterval(t *testing.T) {
	var steps = []uint32{1, 5, 10, 20, 60, 120, 300, 600, 1200, 3600, 7200, 14400, 28800, 57600, 115200, 230400, 460800, 921600}
	var timerangeSeconds = []uint32{
		360,       // 6 minutes
		1800,      // 30 minutes
		3600,      // 1 hour
		7200,      // 2 hours
		21600,     // 6 hours
		43200,     // 12 hours
		108000,    // 30 hours
		216000,    // 60 hours
		432000,    // 5 days
		1296000,   // 15 days
		2592000,   // 30 days
		5184000,   // 60 days
		10368000,  // 120 days
		20736000,  // 240 days
		41472000,  // 480 days
		82944000,  // 960 days
		165888000, // 1920 days
		315360000, // 10 years
		315360001, // 10 years + 1 second
	}

	for i, tr := range timerangeSeconds {
		expectedInterval := uint32(0)
		if i < len(steps) {
			expectedInterval = steps[i]
		}
		actualInterval, err := mresults.CalculateInterval(tr)
		if timerangeSeconds[i] > 315360000 { // 10 years in seconds
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, expectedInterval, actualInterval)
		}
	}
}

func Test_GetResults_Modulo(t *testing.T) {
	test_GetResults_Ops(t,
		map[uint32]float64{
			0:    52.5,
			3600: 23,
			7200: -7.5,
		},
		map[uint32]float64{
			0:    2.5,
			3600: 3,
			7200: -2.5,
		},
		[]structs.QueryArithmetic{
			{
				LHS:        1,
				ConstantOp: true,
				Operation:  utils.LetModulo,
				Constant:   5,
			},
		},
		structs.Downsampler{
			Interval:   1,
			Unit:       "h",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Avg},
		},
	)
}

func Test_GetResults_Addition(t *testing.T) {
	test_GetResults_Ops(t,
		map[uint32]float64{
			0:    10,
			3600: 20,
			7200: 30,
		},
		map[uint32]float64{
			0:    15,
			3600: 25,
			7200: 35,
		},
		[]structs.QueryArithmetic{
			{
				LHS:        1,
				ConstantOp: true,
				Operation:  utils.LetAdd,
				Constant:   5,
			},
		},
		structs.Downsampler{
			Interval:   1,
			Unit:       "h",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Avg},
		},
	)
}

func Test_GetResults_Subtraction(t *testing.T) {
	test_GetResults_Ops(t,
		map[uint32]float64{
			0:    10,
			3600: 20,
			7200: 30,
		},
		map[uint32]float64{
			0:    5,
			3600: 15,
			7200: 25,
		},
		[]structs.QueryArithmetic{
			{
				LHS:        1,
				ConstantOp: true,
				Operation:  utils.LetSubtract,
				Constant:   5,
			},
		},
		structs.Downsampler{
			Interval:   1,
			Unit:       "h",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Avg},
		},
	)
}

func Test_GetResults_Multiplication(t *testing.T) {
	test_GetResults_Ops(t,
		map[uint32]float64{
			0:    2,
			3600: 3,
			7200: 4,
		},
		map[uint32]float64{
			0:    10,
			3600: 15,
			7200: 20,
		},
		[]structs.QueryArithmetic{
			{
				LHS:        1,
				ConstantOp: true,
				Operation:  utils.LetMultiply,
				Constant:   5,
			},
		},
		structs.Downsampler{
			Interval:   1,
			Unit:       "h",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Avg},
		},
	)
}

func Test_GetResults_Division(t *testing.T) {
	test_GetResults_Ops(t,
		map[uint32]float64{
			0:    10,
			3600: 20,
			7200: 30,
		},
		map[uint32]float64{
			0:    2,
			3600: 4,
			7200: 6,
		},
		[]structs.QueryArithmetic{
			{
				LHS:        1,
				ConstantOp: true,
				Operation:  utils.LetDivide,
				Constant:   5,
			},
		},
		structs.Downsampler{
			Interval:   1,
			Unit:       "h",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Avg},
		},
	)
}

func Test_GetResults_Power(t *testing.T) {
	test_GetResults_Ops(t,
		map[uint32]float64{
			0:    2,
			7200: -10,
		},
		map[uint32]float64{
			0:    4,
			7200: 100,
		},
		[]structs.QueryArithmetic{
			{
				LHS:        1,
				ConstantOp: true,
				Operation:  utils.LetPower,
				Constant:   2,
			},
		},
		structs.Downsampler{
			Interval:   1,
			Unit:       "h",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Avg},
		},
	)
}

func Test_GetResults_GreaterThan(t *testing.T) {
	test_GetResults_Ops(t,
		map[uint32]float64{
			0:     45,
			10800: 1045,
			21600: 1046,
		},
		map[uint32]float64{
			21600: 1046,
		},
		[]structs.QueryArithmetic{
			{
				LHS:        1,
				ConstantOp: true,
				Operation:  utils.LetGreaterThan,
				Constant:   1045,
			},
		},
		structs.Downsampler{
			Interval:   3,
			Unit:       "h",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Avg},
		},
	)
}

func Test_GetResults_GreaterThanOrEqualTo(t *testing.T) {
	test_GetResults_Ops(t,
		map[uint32]float64{
			0:    45,
			3600: 1045,
			7200: 100,
		},
		map[uint32]float64{
			3600: 1045,
			7200: 100,
		},
		[]structs.QueryArithmetic{
			{
				LHS:        1,
				ConstantOp: true,
				Operation:  utils.LetGreaterThanOrEqualTo,
				Constant:   100,
			},
		},
		structs.Downsampler{
			Interval:   1,
			Unit:       "h",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Avg},
		},
	)
}

func Test_GetResults_LessThan(t *testing.T) {
	test_GetResults_Ops(t,
		map[uint32]float64{
			0:     45,
			3600:  1045,
			10800: 1044,
		},
		map[uint32]float64{
			0:     45,
			10800: 1044,
		},
		[]structs.QueryArithmetic{
			{
				LHS:        1,
				ConstantOp: true,
				Operation:  utils.LetLessThan,
				Constant:   1045,
			},
		},
		structs.Downsampler{
			Interval:   1,
			Unit:       "h",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Avg},
		},
	)
}

func Test_GetResults_LessThanOrEqualTo(t *testing.T) {
	test_GetResults_Ops(t,
		map[uint32]float64{
			0:     45,
			3600:  1046,
			10800: 1045,
		},
		map[uint32]float64{
			0:     45,
			10800: 1045,
		},
		[]structs.QueryArithmetic{
			{
				LHS:        1,
				ConstantOp: true,
				Operation:  utils.LetLessThanOrEqualTo,
				Constant:   1045,
			},
		},
		structs.Downsampler{
			Interval:   1,
			Unit:       "h",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Avg},
		},
	)
}

func Test_GetResults_Equals(t *testing.T) {
	test_GetResults_Ops(t,
		map[uint32]float64{
			0:     45,
			7200:  1045,
			14400: 666,
		},
		map[uint32]float64{
			0: 45,
		},
		[]structs.QueryArithmetic{
			{
				LHS:        1,
				ConstantOp: true,
				Operation:  utils.LetEquals,
				Constant:   45,
			},
		},
		structs.Downsampler{
			Interval:   2,
			Unit:       "h",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Avg},
		},
	)
}

func Test_GetResults_NotEquals(t *testing.T) {
	test_GetResults_Ops(t,
		map[uint32]float64{
			0:     45,
			7200:  1045,
			14400: 666,
		},
		map[uint32]float64{
			7200:  1045,
			14400: 666,
		},
		[]structs.QueryArithmetic{
			{
				LHS:        1,
				ConstantOp: true,
				Operation:  utils.LetNotEquals,
				Constant:   45,
			},
		},
		structs.Downsampler{
			Interval:   2,
			Unit:       "h",
			CFlag:      false,
			Aggregator: structs.Aggregation{AggregatorFunction: utils.Avg},
		},
	)
}

func test_GetResults_Ops(t *testing.T, initialEntries map[uint32]float64, ansMap map[uint32]float64, queryOps []structs.QueryArithmetic, downsampler structs.Downsampler) {
	mQuery := &structs.MetricsQuery{
		MetricName:  "test.metric.0",
		HashedMName: 1,
		Downsampler: downsampler,
	}
	qid := uint64(0)
	metricsResults := mresults.InitMetricResults(mQuery, qid)
	assert.NotNil(t, metricsResults)

	var tsGroupId *bytebufferpool.ByteBuffer = bytebufferpool.Get()
	defer bytebufferpool.Put(tsGroupId)
	_, err := tsGroupId.Write([]byte("color:yellow"))
	assert.NoError(t, err)
	series := mresults.InitSeriesHolder(mQuery, tsGroupId)

	for timestamp, val := range initialEntries {
		series.AddEntry(timestamp, val)
	}

	metricsResults.AddSeries(series, uint64(100), tsGroupId)
	metricsResults.DownsampleResults(mQuery.Downsampler, 1)

	errors := metricsResults.AggregateResults(1)
	assert.Nil(t, errors)

	res := segment.HelperQueryArithmeticAndLogical(queryOps, map[uint64]*mresults.MetricsResult{
		1: metricsResults,
	})
	assert.Len(t, res.Results, 1)
	for _, resMap := range res.Results {
		assert.Equal(t, len(ansMap), len(resMap))
		for timestamp, val := range resMap {
			expectedVal, exists := ansMap[timestamp]
			if !exists {
				t.Errorf("Should not have this key: %v", timestamp)
			}

			if expectedVal != val {
				t.Errorf("Expected value should be %v, but got %v", expectedVal, val)
			}
		}
	}
}
