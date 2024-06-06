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
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nethruster/go-fraction"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/bytebufferpool"
)

/*
	Defines functions used to store and merge series results with metrics results
*/

type Series struct {
	idx       int // entries[:idx] is guaranteed to have valid results
	len       int // the number of available elements. Once idx==len, entries needs to be resized
	entries   []Entry
	dsSeconds uint32
	sorted    bool
	grpID     *bytebufferpool.ByteBuffer

	// If the original Downsampler Aggregator is Avg, the convertedDownsampleAggFn is set to Sum; otherwise, it is set to the original Downsampler Aggregator.
	convertedDownsampleAggFn utils.AggregateFunctions
	aggregationConstant      float64
}

type DownsampleSeries struct {
	idx    int // runningEntries[:idx] is guaranteed to have valid results
	len    int // denotes the number of available elements. When idx==len, the underlying slice needs to be resized
	sorted bool

	// original Downsampler Aggregator which comes in with metricsQuery
	downsampleAggFn     utils.AggregateFunctions
	aggregationConstant float64
	runningEntries      []RunningEntry
	grpID               *bytebufferpool.ByteBuffer
}

type RunningEntry struct {
	runningCount    uint64
	runningVal      float64
	downsampledTime uint32
}

type Entry struct {
	downsampledTime uint32
	dpVal           float64
}

var initial_len = 10
var extend_capacity = 50

/*
Allocates a series from the pool and returns.

The allocated series should be returned to the pools via (mr *MetricsResults).DownsampleResults()
If the original Downsampler Aggregator is Avg, the convertedDownsampleAggFn is set to Sum; otherwise, it is set to the original Downsampler Aggregator.
*/
func InitSeriesHolder(mQuery *structs.MetricsQuery, tsGroupId *bytebufferpool.ByteBuffer) *Series {
	// have some info about downsample
	ds := mQuery.Downsampler
	downsampleAggFn := mQuery.Downsampler.Aggregator.AggregatorFunction
	convertedDownsampleAggFn := mQuery.Downsampler.Aggregator.AggregatorFunction
	if downsampleAggFn == utils.Avg {
		convertedDownsampleAggFn = utils.Sum
	}
	aggregationConstant := mQuery.Aggregator.FuncConstant

	retVal := make([]Entry, initial_len, extend_capacity)
	return &Series{
		idx:                      0,
		len:                      initial_len,
		entries:                  retVal,
		dsSeconds:                ds.GetIntervalTimeInSeconds(),
		sorted:                   false,
		convertedDownsampleAggFn: convertedDownsampleAggFn,
		aggregationConstant:      aggregationConstant,
		grpID:                    tsGroupId,
	}
}

func InitSeriesHolderForTags(mQuery *structs.MetricsQuery, tsGroupId *bytebufferpool.ByteBuffer) *Series {
	return &Series{
		grpID: tsGroupId,
	}
}

func (s *Series) GetIdx() int {
	return s.idx
}

func (s *Series) AddEntry(ts uint32, dp float64) {
	s.entries[s.idx].downsampledTime = (ts / s.dsSeconds) * s.dsSeconds
	s.entries[s.idx].dpVal = dp
	s.idx++
	if s.idx >= s.len {
		if cap(s.entries)-len(s.entries) > 0 {
			s.entries = s.entries[:cap(s.entries)]
			s.len = cap(s.entries)
		} else {
			newBuf := make([]Entry, extend_capacity)
			s.entries = append(s.entries, newBuf...)
			s.len += extend_capacity
		}
	}
}

func (s *Series) sortEntries() {
	if s.sorted {
		return
	}

	s.entries = s.entries[:s.idx]
	sort.Slice(s.entries, func(i, j int) bool {
		return s.entries[i].downsampledTime < s.entries[j].downsampledTime
	})
	s.sorted = true
}

func (s *Series) Merge(toJoin *Series) {
	toJoinEntries := toJoin.entries[:toJoin.idx]
	s.entries = s.entries[:s.idx]
	s.len = s.idx
	s.entries = append(s.entries, toJoinEntries...)
	s.idx += toJoin.idx
	s.sorted = false
	s.len += toJoin.idx
}

func (s *Series) Downsample(downsampler structs.Downsampler) (*DownsampleSeries, error) {
	// get downsampled series
	s.sortEntries()
	ds := initDownsampleSeries(downsampler.Aggregator)
	for i := 0; i < s.idx; i++ {
		currDSTime := s.entries[i].downsampledTime
		maxJ := sort.Search(len(s.entries), func(j int) bool {
			return s.entries[j].downsampledTime > currDSTime
		})
		retVal, err := reduceEntries(s.entries[i:maxJ], s.convertedDownsampleAggFn, s.aggregationConstant)
		if err != nil {
			log.Errorf("Downsample: failed to reduce entries: %v by using this operator: %v, err: %v", s.entries[i:maxJ], s.convertedDownsampleAggFn, err)
			return nil, err
		}
		ds.Add(retVal, s.entries[i].downsampledTime, uint64(maxJ-i))
		i = maxJ - 1
	}
	ds.grpID = s.grpID
	return ds, nil
}

func initDownsampleSeries(agg structs.Aggregation) *DownsampleSeries {

	runningEntries := make([]RunningEntry, initial_len, extend_capacity)
	return &DownsampleSeries{
		idx:                 0,
		len:                 initial_len,
		runningEntries:      runningEntries,
		downsampleAggFn:     agg.AggregatorFunction,
		aggregationConstant: agg.FuncConstant,
		sorted:              false,
	}
}

func (dss *DownsampleSeries) Add(retVal float64, ts uint32, count uint64) {
	dss.runningEntries[dss.idx].runningCount = count
	dss.runningEntries[dss.idx].runningVal = retVal
	dss.runningEntries[dss.idx].downsampledTime = ts
	dss.idx++
	if dss.idx >= dss.len {
		if cap(dss.runningEntries)-len(dss.runningEntries) > 0 {
			dss.runningEntries = dss.runningEntries[:cap(dss.runningEntries)]
			dss.len = cap(dss.runningEntries)
		} else {
			newBuf := make([]RunningEntry, extend_capacity)
			dss.runningEntries = append(dss.runningEntries, newBuf...)
			dss.len += extend_capacity
		}
	}
	dss.sorted = false
}

/*
Merge takes the first toJoin.idx elements of the incoming running entires and adds them to the current entries
*/
func (dss *DownsampleSeries) Merge(toJoin *DownsampleSeries) {
	toJoinEntries := toJoin.runningEntries[:toJoin.idx]
	dss.runningEntries = dss.runningEntries[:dss.idx]
	dss.len = dss.idx
	dss.runningEntries = append(dss.runningEntries, toJoinEntries...)
	dss.idx += toJoin.idx
	dss.len += toJoin.idx
	dss.sorted = false
}

func (dss *DownsampleSeries) AggregateFromSingleTimeseries() (map[uint32]float64, error) {
	// dss has a list of RunningEntry that caputre downsampled time per tsid
	// many tsids will exist but they will share the grpID
	dss.sortEntries()
	retVal := make(map[uint32]float64)
	for i := 0; i < dss.idx; i++ {
		// find the first index where the downsampled time is greater than the current buckets time
		currDSTime := dss.runningEntries[i].downsampledTime
		maxJ := sort.Search(len(dss.runningEntries), func(j int) bool {
			return dss.runningEntries[j].downsampledTime > currDSTime
		})
		currVal, err := reduceRunningEntries(dss.runningEntries[i:maxJ], dss.downsampleAggFn, dss.aggregationConstant)
		if err != nil {
			log.Errorf("Aggregate: failed to reduce entries: %v by using this operator: %v, err: %v", dss.runningEntries[i:maxJ], dss.downsampleAggFn, err)
			return nil, err
		}
		retVal[dss.runningEntries[i].downsampledTime] = currVal
		i = maxJ - 1
	}
	return retVal, nil
}

func ApplyAggregationFromSingleTimeseries(entries []RunningEntry, aggregation structs.Aggregation) (float64, error) {
	return reduceRunningEntries(entries, aggregation.AggregatorFunction, aggregation.FuncConstant)
}

func ApplyFunction(ts map[uint32]float64, function structs.Function) (map[uint32]float64, error) {
	if function.RangeFunction > 0 {
		return ApplyRangeFunction(ts, function)
	}

	if function.MathFunction > 0 {
		return ApplyMathFunction(ts, function)
	}

	if function.TimeFunction > 0 {
		return ApplyTimeFunction(ts, function)
	}

	return ts, nil
}

func ApplyMathFunction(ts map[uint32]float64, function structs.Function) (map[uint32]float64, error) {
	var err error
	switch function.MathFunction {
	case segutils.Abs:
		evaluate(ts, math.Abs)
	case segutils.Sqrt:
		err = applyFuncToNonNegativeValues(ts, math.Sqrt)
	case segutils.Ceil:
		evaluate(ts, math.Ceil)
	case segutils.Floor:
		evaluate(ts, math.Floor)
	case segutils.Round:
		if len(function.ValueList) > 0 && len(function.ValueList[0]) > 0 {
			err = evaluateRoundWithPrecision(ts, function.ValueList[0])
		} else {
			evaluate(ts, math.Round)
		}
	case segutils.Exp:
		evaluate(ts, math.Exp)
	case segutils.Ln:
		err = applyFuncToNonNegativeValues(ts, math.Log)
	case segutils.Log2:
		err = applyFuncToNonNegativeValues(ts, math.Log2)
	case segutils.Log10:
		err = applyFuncToNonNegativeValues(ts, math.Log10)
	case segutils.Sgn:
		evaluate(ts, calculateSgn)
	case segutils.Deg:
		evaluate(ts, func(val float64) float64 {
			return val * 180 / math.Pi
		})
	case segutils.Rad:
		evaluate(ts, func(val float64) float64 {
			return val * math.Pi / 180
		})

	case segutils.Acos:
		err = evaluateWithErr(ts, func(val float64) (float64, error) {
			if val < -1 || val > 1 {
				return val, fmt.Errorf("evaluateWithErr: acos evaluate values in the range [-1,1], but got input value: %v", val)
			}
			return math.Acos(val), nil
		})
	case segutils.Acosh:
		err = evaluateWithErr(ts, func(val float64) (float64, error) {
			if val < 1 {
				return val, fmt.Errorf("evaluateWithErr: acosh evaluate values in the range [1,+Inf], but got input value: %v", val)
			}
			return math.Acosh(val), nil
		})
	case segutils.Asin:
		err = evaluateWithErr(ts, func(val float64) (float64, error) {
			if val < -1 || val > 1 {
				return val, fmt.Errorf("evaluateWithErr: asin evaluate values in the range [-1,1], but got input value: %v", val)
			}
			return math.Asin(val), nil
		})
	case segutils.Asinh:
		evaluate(ts, math.Asinh)
	case segutils.Atan:
		evaluate(ts, math.Atan)
	case segutils.Atanh:
		err = evaluateWithErr(ts, func(val float64) (float64, error) {
			if val <= -1 || val >= 1 {
				return val, fmt.Errorf("evaluateWithErr: atanh evaluate values in the range [-1,1], but got input value: %v", val)
			}
			return math.Atanh(val), nil
		})
	case segutils.Cos:
		evaluate(ts, math.Cos)
	case segutils.Cosh:
		evaluate(ts, math.Cosh)
	case segutils.Sin:
		evaluate(ts, math.Sin)
	case segutils.Sinh:
		evaluate(ts, math.Sinh)
	case segutils.Tan:
		evaluate(ts, math.Tan)
	case segutils.Tanh:
		evaluate(ts, math.Tanh)
	case segutils.Clamp:
		if len(function.ValueList) != 2 {
			return ts, fmt.Errorf("ApplyMathFunction: clamp has incorrect parameters: %v", function.ValueList)
		}
		minVal, err1 := strconv.ParseFloat(function.ValueList[0], 64)
		maxVal, err2 := strconv.ParseFloat(function.ValueList[1], 64)
		if err1 != nil || err2 != nil {
			return ts, fmt.Errorf("ApplyMathFunction: clamp has incorrect parameters: %v", function.ValueList)
		}
		if minVal > maxVal {
			return make(map[uint32]float64), nil
		}
		evaluateClamp(ts, minVal, maxVal)
	case segutils.Clamp_Max:
		if len(function.ValueList) != 1 {
			return ts, fmt.Errorf("ApplyMathFunction: clamp_max has incorrect parameters: %v", function.ValueList)
		}
		maxVal, err := strconv.ParseFloat(function.ValueList[0], 64)
		if err != nil {
			return ts, fmt.Errorf("ApplyMathFunction: clamp_max has incorrect parameters: %v", function.ValueList)
		}
		evaluateClamp(ts, -1.7976931348623157e+308, maxVal)
	case segutils.Clamp_Min:
		if len(function.ValueList) != 1 {
			return ts, fmt.Errorf("ApplyMathFunction: clamp_min has incorrect parameters: %v", function.ValueList)
		}
		minVal, err := strconv.ParseFloat(function.ValueList[0], 64)
		if err != nil {
			return ts, fmt.Errorf("ApplyMathFunction: clamp_min has incorrect parameters: %v", function.ValueList)
		}
		evaluateClamp(ts, minVal, math.MaxFloat64)
	case segutils.Timestamp:
		for timestamp := range ts {
			ts[timestamp] = float64(timestamp)
		}
	default:
		return ts, fmt.Errorf("ApplyMathFunction: unsupported function type %v", function)
	}

	if err != nil {
		return ts, fmt.Errorf("ApplyMathFunction: %v", err)
	}

	return ts, nil
}

func ApplyRangeFunction(ts map[uint32]float64, function structs.Function) (map[uint32]float64, error) {

	if len(ts) == 0 {
		return ts, nil
	}

	// Convert ts to a sorted list of Entry's
	sortedTimeSeries := make([]Entry, 0, len(ts))
	for time, value := range ts {
		entry := Entry{
			downsampledTime: time,
			dpVal:           value,
		}
		sortedTimeSeries = append(sortedTimeSeries, entry)
	}

	sort.Slice(sortedTimeSeries, func(i int, k int) bool {
		return sortedTimeSeries[i].downsampledTime < sortedTimeSeries[k].downsampledTime
	})

	timeWindow := uint32(function.TimeWindow)
	if sortedTimeSeries[0].downsampledTime <= timeWindow {
		return ts, fmt.Errorf("ApplyRangeFunction: time window is too large: %v", timeWindow)
	}

	// ts is a time series mapping timestamps to values
	switch function.RangeFunction {
	case segutils.Derivative:
		// Use those points which within the time window to calculate the derivative
		for i := 1; i < len(sortedTimeSeries); i++ {
			timeWindowStartTime := sortedTimeSeries[i].downsampledTime - timeWindow
			preIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
				return sortedTimeSeries[j].downsampledTime >= timeWindowStartTime
			})

			if i <= preIndex { // Can not find the second point within the time window
				delete(ts, sortedTimeSeries[i].downsampledTime)
				continue
			}

			timestamp := sortedTimeSeries[i].downsampledTime
			slope := getSlopeByLinearRegression(sortedTimeSeries, preIndex, i)
			ts[timestamp] = slope
		}
		// derivtives at edges do not exist
		delete(ts, sortedTimeSeries[0].downsampledTime)
		return ts, nil
	case segutils.Predict_Linear:
		if len(function.ValueList) != 1 {
			return ts, fmt.Errorf("ApplyRangeFunction: predict_linear has incorrect parameters: %v", function.ValueList)
		}

		floatVal, err := strconv.ParseFloat(function.ValueList[0], 64)
		if err != nil {
			return ts, fmt.Errorf("ApplyRangeFunction: predict_linear has incorrect parameters: %v", function.ValueList)
		}

		predictDuration := uint32(floatVal)

		for i := 1; i < len(sortedTimeSeries); i++ {
			timeWindowStartTime := sortedTimeSeries[i].downsampledTime - timeWindow
			preIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
				return sortedTimeSeries[j].downsampledTime >= timeWindowStartTime
			})

			if i <= preIndex { // Can not find the second point within the time window
				delete(ts, sortedTimeSeries[i].downsampledTime)
				continue
			}

			timestamp := sortedTimeSeries[i].downsampledTime

			slope := getSlopeByLinearRegression(sortedTimeSeries, preIndex, i)
			ts[timestamp] = sortedTimeSeries[i].dpVal + float64(predictDuration)*slope
		}
		delete(ts, sortedTimeSeries[0].downsampledTime)
		return ts, nil
	case segutils.Rate:
		return evaluateRate(sortedTimeSeries, ts, timeWindow), nil
	case segutils.IRate:
		// Calculate the instant rate (per-second rate) for each timestamp, based on the last two data points within the timewindow
		// If the previous point is outside the time window, we still need to use it to calculate the current point's rate, unless its value is greater than the value of the current point
		var dx, dt float64
		for i := 1; i < len(sortedTimeSeries); i++ {
			timeDff := sortedTimeSeries[i].downsampledTime - sortedTimeSeries[i-1].downsampledTime
			if timeDff > timeWindow {
				delete(ts, sortedTimeSeries[i].downsampledTime)
				continue
			}

			// Calculate the time difference between consecutive data points
			dt = float64(timeDff)
			curVal := sortedTimeSeries[i].dpVal
			prevVal := sortedTimeSeries[i-1].dpVal

			if curVal > prevVal {
				dx = curVal - prevVal
			} else {
				// This metric was reset.
				dx = curVal
			}

			ts[sortedTimeSeries[i].downsampledTime] = dx / dt
		}

		// Rate at edge does not exist.
		delete(ts, sortedTimeSeries[0].downsampledTime)
		return ts, nil
	case segutils.Increase:
		// Increase is extrapolated to cover the full time range as specified in the range vector selector. (increse = avg rate * timewindow)
		ts := evaluateRate(sortedTimeSeries, ts, timeWindow)
		for key, rateVal := range ts {
			ts[key] = rateVal * float64(timeWindow)
		}
		return ts, nil
	case segutils.Delta:
		// Calculates the difference between the first and last value of each time series element within the timewindow
		for i := 1; i < len(sortedTimeSeries); i++ {
			timeWindowStartTime := sortedTimeSeries[i].downsampledTime - timeWindow
			preIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
				return sortedTimeSeries[j].downsampledTime >= timeWindowStartTime
			})

			if i <= preIndex { // Can not find the second point within the time window
				delete(ts, sortedTimeSeries[i].downsampledTime)
				continue
			}
			ts[sortedTimeSeries[i].downsampledTime] = sortedTimeSeries[i].dpVal - sortedTimeSeries[preIndex].dpVal
		}

		// Delta at left edge does not exist.
		delete(ts, sortedTimeSeries[0].downsampledTime)
		return ts, nil
	case segutils.IDelta:
		// Calculates the instant delta for each timestamp, based on the last two data points within the time window
		for i := 1; i < len(sortedTimeSeries); i++ {
			timeDff := sortedTimeSeries[i].downsampledTime - sortedTimeSeries[i-1].downsampledTime
			if timeDff > timeWindow {
				delete(ts, sortedTimeSeries[i].downsampledTime)
				continue
			}
			ts[sortedTimeSeries[i].downsampledTime] = sortedTimeSeries[i].dpVal - sortedTimeSeries[i-1].dpVal
		}
		// IDelta at left edge does not exist.
		delete(ts, sortedTimeSeries[0].downsampledTime)
		return ts, nil
	case segutils.Changes:
		// Calculates the number of times its value has changed within the provided time window
		prefixSum := make([]float64, len(sortedTimeSeries))
		prefixSum[0] = 0
		for i := 1; i < len(sortedTimeSeries); i++ {
			prefixSum[i] = prefixSum[i-1]
			if sortedTimeSeries[i].dpVal != sortedTimeSeries[i-1].dpVal {
				prefixSum[i]++
			}
		}

		ts[sortedTimeSeries[0].downsampledTime] = 0

		for i := 1; i < len(sortedTimeSeries); i++ {
			timeWindowStartTime := sortedTimeSeries[i].downsampledTime - timeWindow
			preIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
				return sortedTimeSeries[j].downsampledTime >= timeWindowStartTime
			})

			if i <= preIndex { // Can not find the second point within the time window
				ts[sortedTimeSeries[i].downsampledTime] = 0
				continue
			}

			ts[sortedTimeSeries[i].downsampledTime] = prefixSum[i] - prefixSum[preIndex]
		}
		return ts, nil
	case segutils.Resets:
		// Any decrease in the value between two consecutive float samples is interpreted as a counter reset.
		prefixSum := make([]float64, len(sortedTimeSeries))
		prefixSum[0] = 0

		for i := 1; i < len(sortedTimeSeries); i++ {
			prefixSum[i] = prefixSum[i-1]
			if sortedTimeSeries[i].dpVal < sortedTimeSeries[i-1].dpVal {
				prefixSum[i]++
			}
		}

		ts[sortedTimeSeries[0].downsampledTime] = 0

		for i := 1; i < len(sortedTimeSeries); i++ {
			timeWindowStartTime := sortedTimeSeries[i].downsampledTime - timeWindow
			preIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
				return sortedTimeSeries[j].downsampledTime >= timeWindowStartTime
			})

			if i <= preIndex { // Can not find the second point within the time window
				ts[sortedTimeSeries[i].downsampledTime] = 0
				continue
			}

			ts[sortedTimeSeries[i].downsampledTime] = prefixSum[i] - prefixSum[preIndex]
		}
		return ts, nil
	case segutils.Avg_Over_Time:
		prefixSum := make([]float64, len(sortedTimeSeries)+1)
		prefixSum[1] = sortedTimeSeries[0].dpVal
		for i := 1; i < len(sortedTimeSeries); i++ {
			prefixSum[i+1] = (prefixSum[i] + sortedTimeSeries[i].dpVal)
		}

		for i := 1; i < len(sortedTimeSeries); i++ {
			timeWindowStartTime := sortedTimeSeries[i].downsampledTime - timeWindow
			preIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
				return sortedTimeSeries[j].downsampledTime >= timeWindowStartTime
			})

			if i <= preIndex { // Can not find the second point within the time window
				continue
			}

			ts[sortedTimeSeries[i].downsampledTime] = (prefixSum[i+1] - prefixSum[preIndex]) / float64(i-preIndex+1)
		}
		return ts, nil
	case segutils.Min_Over_Time:
		for i := 1; i < len(sortedTimeSeries); i++ {
			timeWindowStartTime := sortedTimeSeries[i].downsampledTime - timeWindow
			preIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
				return sortedTimeSeries[j].downsampledTime >= timeWindowStartTime
			})

			min := math.MaxFloat64
			for j := preIndex; j <= i; j++ {
				min = math.Min(min, sortedTimeSeries[j].dpVal)
			}

			ts[sortedTimeSeries[i].downsampledTime] = min
		}
		return ts, nil
	case segutils.Max_Over_Time:
		for i := 1; i < len(sortedTimeSeries); i++ {
			timeWindowStartTime := sortedTimeSeries[i].downsampledTime - timeWindow
			preIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
				return sortedTimeSeries[j].downsampledTime >= timeWindowStartTime
			})

			max := -1.7976931348623157e+308
			for j := preIndex; j <= i; j++ {
				max = math.Max(max, sortedTimeSeries[j].dpVal)
			}

			ts[sortedTimeSeries[i].downsampledTime] = max
		}
		return ts, nil
	case segutils.Sum_Over_Time:
		prefixSum := make([]float64, len(sortedTimeSeries)+1)
		prefixSum[1] = sortedTimeSeries[0].dpVal
		for i := 1; i < len(sortedTimeSeries); i++ {
			prefixSum[i+1] = (prefixSum[i] + sortedTimeSeries[i].dpVal)
		}

		for i := 1; i < len(sortedTimeSeries); i++ {
			timeWindowStartTime := sortedTimeSeries[i].downsampledTime - timeWindow
			preIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
				return sortedTimeSeries[j].downsampledTime >= timeWindowStartTime
			})

			if i <= preIndex { // Can not find the second point within the time window
				continue
			}

			ts[sortedTimeSeries[i].downsampledTime] = prefixSum[i+1] - prefixSum[preIndex]
		}
		return ts, nil
	case segutils.Count_Over_Time:
		ts[sortedTimeSeries[0].downsampledTime] = 1
		for i := 1; i < len(sortedTimeSeries); i++ {
			timeWindowStartTime := sortedTimeSeries[i].downsampledTime - timeWindow
			preIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
				return sortedTimeSeries[j].downsampledTime >= timeWindowStartTime
			})

			if i <= preIndex { // Can not find the second point within the time window
				ts[sortedTimeSeries[i].downsampledTime] = 1
				continue
			}

			ts[sortedTimeSeries[i].downsampledTime] = float64(i - preIndex + 1)
		}
		return ts, nil
	case segutils.Stdvar_Over_Time:
		return evaluateStandardVariance(sortedTimeSeries, ts, timeWindow), nil
	case segutils.Stddev_Over_Time:
		ts = evaluateStandardVariance(sortedTimeSeries, ts, timeWindow)
		for key, val := range ts {
			ts[key] = math.Sqrt(val)
		}
		return ts, nil
	case segutils.Last_Over_Time:
		// If we take the very last sample from every element of a range vector, the resulting vector will be identical to a regular instant vector query.
		return ts, nil
	case segutils.Present_Over_Time:
		for key := range ts {
			ts[key] = 1
		}
		return ts, nil
	case segutils.Quantile_Over_Time:
		if len(function.ValueList) != 1 {
			return ts, fmt.Errorf("ApplyMathFunction: quantile_over_time has incorrect parameters: %v", function.ValueList)
		}
		quantile, err := strconv.ParseFloat(function.ValueList[0], 64)
		if err != nil {
			return ts, fmt.Errorf("ApplyMathFunction: quantile_over_time has incorrect parameters: %v, params can not convert to a float: %v", function.ValueList, err)
		}

		ts[sortedTimeSeries[0].downsampledTime] = sortedTimeSeries[0].dpVal * quantile
		for i := 1; i < len(sortedTimeSeries); i++ {
			timeWindowStartTime := sortedTimeSeries[i].downsampledTime - timeWindow
			preIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
				return sortedTimeSeries[j].downsampledTime >= timeWindowStartTime
			})

			if i <= preIndex { // Can not find the second point within the time window
				ts[sortedTimeSeries[i].downsampledTime] = sortedTimeSeries[i].dpVal * quantile
				continue
			}
			// Linear interpolation calculation: P = φ * (N - 1), V = V1 + (P - floor(P)) * (V2 - V1)
			p := quantile * float64(i-preIndex)
			index1 := int(math.Floor(float64(preIndex) + p))
			index2 := int(math.Ceil(float64(preIndex) + p))
			val := sortedTimeSeries[index1].dpVal + (p-math.Floor(p))*(sortedTimeSeries[index2].dpVal-sortedTimeSeries[index1].dpVal)
			ts[sortedTimeSeries[i].downsampledTime] = val
		}
		return ts, nil
	default:
		return ts, fmt.Errorf("ApplyRangeFunction: Unknown function type: %v", function.RangeFunction)
	}
}

func (dss *DownsampleSeries) sortEntries() {
	if dss.sorted {
		return
	}
	dss.runningEntries = dss.runningEntries[:dss.idx]
	sort.Slice(dss.runningEntries, func(i, j int) bool {
		return dss.runningEntries[i].downsampledTime < dss.runningEntries[j].downsampledTime
	})
	dss.sorted = true
}

func reduceEntries(entries []Entry, fn utils.AggregateFunctions, fnConstant float64) (float64, error) {
	var ret float64
	switch fn {
	case utils.Sum:
		for i := range entries {
			ret += entries[i].dpVal
		}
	case utils.Min:
		for i := range entries {
			if i == 0 || entries[i].dpVal < ret {
				ret = entries[i].dpVal
			}
		}
	case utils.Max:
		for i := range entries {
			if i == 0 || entries[i].dpVal > ret {
				ret = entries[i].dpVal
			}
		}
	case utils.Count:
		// Count is to calculate the number of time series, we do not care about the entry value
	case utils.Quantile: //valid range for fnConstant is 0 <= fnConstant <= 1
		// TODO: calculate the quantile without needing to sort the elements.

		entriesCopy := make([]Entry, len(entries))
		copy(entriesCopy, entries)
		sort.Slice(entriesCopy, func(i, k int) bool {
			return entriesCopy[i].dpVal < entriesCopy[k].dpVal
		})

		index := fnConstant * float64(len(entriesCopy)-1)

		// Check for special cases when quantile position doesn't fall on an exact index
		if index != float64(int(index)) && int(index)+1 < len(entriesCopy) {
			// Calculate the weight for interpolation
			fraction := index - float64(int(index))

			dpVal1 := entriesCopy[int(index)].dpVal
			dpVal2 := entriesCopy[int(index)+1].dpVal

			ret = dpVal1 + fraction*(dpVal2-dpVal1)
		} else {
			ret = entriesCopy[int(index)].dpVal
		}
	default:
		err := fmt.Errorf("reduceEntries: unsupported AggregateFunction: %v", fn)
		log.Errorf("%v", err)
		return 0.0, err
	}

	return ret, nil
}

func reduceRunningEntries(entries []RunningEntry, fn utils.AggregateFunctions, fnConstant float64) (float64, error) {
	var ret float64
	switch fn {
	case utils.Avg:
		count := uint64(0)
		for i := range entries {
			ret += entries[i].runningVal
			count += entries[i].runningCount
		}
		ret = ret / float64(count)
	case utils.Sum:
		for i := range entries {
			ret += entries[i].runningVal
		}
	case utils.Min:
		for i := range entries {
			if i == 0 || entries[i].runningVal < ret {
				ret = entries[i].runningVal
			}
		}
	case utils.Max:
		for i := range entries {
			if i == 0 || entries[i].runningVal > ret {
				ret = entries[i].runningVal
			}
		}
	case utils.Count:
		// Count is to calculate the number of time series, we do not care about the entry value
	case utils.Quantile: //valid range for fnConstant is 0 <= fnConstant <= 1
		// TODO: calculate the quantile without needing to sort the elements.

		entriesCopy := make([]RunningEntry, len(entries))
		copy(entriesCopy, entries)
		sort.Slice(entriesCopy, func(i, k int) bool {
			return entriesCopy[i].runningVal < entriesCopy[k].runningVal
		})

		index := fnConstant * float64(len(entriesCopy)-1)
		// Check for special cases when quantile position doesn't fall on an exact index
		if index != float64(int(index)) && int(index)+1 < len(entriesCopy) {
			// Calculate the weight for interpolation
			fraction := index - float64(int(index))

			dpVal1 := entriesCopy[int(index)].runningVal
			dpVal2 := entriesCopy[int(index)+1].runningVal

			ret = dpVal1 + fraction*(dpVal2-dpVal1)
		} else {
			ret = entriesCopy[int(index)].runningVal
		}
	default:
		err := fmt.Errorf("reduceRunningEntries: unsupported AggregateFunction: %v", fn)
		log.Errorf("%v", err)
		return 0.0, err
	}

	return ret, nil
}

type float64Func func(float64) float64
type float64FuncWithErr func(float64) (float64, error)

func evaluate(ts map[uint32]float64, mathFunc float64Func) {
	for key, val := range ts {
		ts[key] = mathFunc(val)
	}
}

type timeFunc func(time.Time) float64

func evaluateTimeFunc(allDPs map[uint32]float64, timeFunc timeFunc) {
	for dpTs := range allDPs {
		t := time.Unix(int64(dpTs), 0).UTC()
		allDPs[dpTs] = timeFunc(t)
	}
}

func ApplyTimeFunction(allDPs map[uint32]float64, function structs.Function) (map[uint32]float64, error) {
	switch function.TimeFunction {
	case segutils.Hour:
		evaluateTimeFunc(allDPs, func(t time.Time) float64 {
			return float64(t.Hour())
		})
	case segutils.Minute:
		evaluateTimeFunc(allDPs, func(t time.Time) float64 {
			return float64(t.Minute())
		})
	case segutils.Month:
		evaluateTimeFunc(allDPs, func(t time.Time) float64 {
			return float64(t.Month())
		})
	case segutils.Year:
		evaluateTimeFunc(allDPs, func(t time.Time) float64 {
			return float64(t.Year())
		})
	case segutils.DayOfMonth:
		evaluateTimeFunc(allDPs, func(t time.Time) float64 {
			return float64(t.Day())
		})
	case segutils.DayOfWeek:
		evaluateTimeFunc(allDPs, func(t time.Time) float64 {
			return float64(t.Weekday())
		})
	case segutils.DayOfYear:
		evaluateTimeFunc(allDPs, func(t time.Time) float64 {
			return float64(t.YearDay())
		})
	case segutils.DaysInMonth:
		evaluateTimeFunc(allDPs, func(t time.Time) float64 {
			return float64(time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day())
		})
	default:
		return allDPs, fmt.Errorf("ApplyTimeFunction: unsupported function type %v", function)
	}

	return allDPs, nil
}

func evaluateWithErr(ts map[uint32]float64, mathFunc float64FuncWithErr) error {
	for key, val := range ts {
		resVal, err := mathFunc(val)
		if err != nil {
			return err
		}
		ts[key] = resVal
	}
	return nil
}

func applyFuncToNonNegativeValues(ts map[uint32]float64, mathFunc float64Func) error {
	for key, val := range ts {
		if val < 0 {
			return fmt.Errorf("applyFuncToNonNegativeValues: negative param not allowed: %v", val)
		}
		ts[key] = mathFunc(val)
	}
	return nil
}

func calculateSgn(val float64) float64 {
	if val > 0 {
		return 1
	} else if val < 0 {
		return -1
	} else {
		return 0
	}
}

func evaluateRoundWithPrecision(ts map[uint32]float64, toNearestStr string) error {
	toNearestStr = strings.ReplaceAll(toNearestStr, " ", "")
	toNearest, err := convertStrToFloat64(toNearestStr)
	if err != nil {
		return fmt.Errorf("evaluateRoundWithPrecision: can not convert toNearest param: %v to a float, err: %v", toNearestStr, err)
	}

	for key, val := range ts {
		ts[key] = roundToNearest(val, toNearest)
	}

	return nil
}

func roundToNearest(val float64, toNearest float64) float64 {
	if toNearest == 0 {
		return val
	}

	factor := math.Pow(10, 14)
	rounded := math.Round(val/toNearest) * toNearest

	// Correct precision errors by rounding to the 14th decimal place
	return math.Round(rounded*factor) / factor
}

// Str could be a fraction/num with decimal
// Try to convert the string into a fraction first, then try to convert it into a number after failing
func convertStrToFloat64(toNearestStr string) (float64, error) {
	regex, err := regexp.Compile(`^(\d+)/(\d+)$`)
	if err != nil {
		return 0, fmt.Errorf("convertStrToFloat64: There are some errors in the pattern: %v", err)
	}

	matches := regex.FindStringSubmatch(toNearestStr)
	if matches != nil {
		numerator, _ := strconv.Atoi(matches[1])
		denominator, _ := strconv.Atoi(matches[2])

		frac, err := fraction.New(numerator, denominator)
		if err != nil {
			return 0, fmt.Errorf("convertStrToFloat64: Can not convert fraction: %v to a float64 value: %v", toNearestStr, err)
		}

		return frac.Float64(), nil
	} else {
		float64Val, err := strconv.ParseFloat(toNearestStr, 64)
		if err != nil {
			return 0, fmt.Errorf("convertStrToFloat64: Can not convert toNearestStr: %v to a float64 value: %v", toNearestStr, err)
		}
		if float64Val == 0 {
			return 0, fmt.Errorf("toNearest value cannot be zero")
		}

		return float64Val, nil
	}
}

// Calculate the average rate (per-second rate) for each timestamp. E.g: to determine the rate for the current point with its timestamp,
// find the earliest point within the time window: [timestamp - time window, timestamp]
// Then, calculate the rate between that point and the current point
func evaluateRate(sortedTimeSeries []Entry, ts map[uint32]float64, timeWindow uint32) map[uint32]float64 {
	var dx, dt float64
	resetIndex := -1
	for i := 1; i < len(sortedTimeSeries); i++ {
		timeWindowStartTime := sortedTimeSeries[i].downsampledTime - timeWindow
		preIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
			return sortedTimeSeries[j].downsampledTime >= timeWindowStartTime
		})

		if i <= preIndex { // Can not find the second point within the time window
			delete(ts, sortedTimeSeries[i].downsampledTime)
			continue
		}

		if sortedTimeSeries[i].dpVal < sortedTimeSeries[i-1].dpVal {
			// This metric was reset.
			dx = sortedTimeSeries[i].dpVal
			dt = float64(sortedTimeSeries[i].downsampledTime - sortedTimeSeries[i-1].downsampledTime)
			ts[sortedTimeSeries[i].downsampledTime] = dx / dt
			resetIndex = i
			continue
		}

		if resetIndex > preIndex {
			preIndex = resetIndex
		}

		// Calculate the time difference between consecutive data points
		dx = sortedTimeSeries[i].dpVal - sortedTimeSeries[preIndex].dpVal
		dt = float64(sortedTimeSeries[i].downsampledTime - sortedTimeSeries[preIndex].downsampledTime)
		ts[sortedTimeSeries[i].downsampledTime] = dx / dt
	}

	// Rate at edge does not exist.
	delete(ts, sortedTimeSeries[0].downsampledTime)
	return ts
}

func evaluateClamp(ts map[uint32]float64, minVal float64, maxVal float64) {

	for key, val := range ts {
		if val < minVal {
			ts[key] = minVal
			continue
		}

		if val > maxVal {
			ts[key] = maxVal
		}
	}
}

func evaluateStandardVariance(sortedTimeSeries []Entry, ts map[uint32]float64, timeWindow uint32) map[uint32]float64 {
	prefixSum := make([]float64, len(sortedTimeSeries)+1)
	prefixSum[1] = sortedTimeSeries[0].dpVal
	for i := 1; i < len(sortedTimeSeries); i++ {
		prefixSum[i+1] = (prefixSum[i] + sortedTimeSeries[i].dpVal)
	}

	ts[sortedTimeSeries[0].downsampledTime] = 0
	for i := 1; i < len(sortedTimeSeries); i++ {
		timeWindowStartTime := sortedTimeSeries[i].downsampledTime - timeWindow
		preIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
			return sortedTimeSeries[j].downsampledTime >= timeWindowStartTime
		})

		if i <= preIndex { // Can not find the second point within the time window
			ts[sortedTimeSeries[i].downsampledTime] = 0
			continue
		}

		avgVal := (prefixSum[i+1] - prefixSum[preIndex]) / float64(i-preIndex+1)
		sumValSquare := 0.0
		for j := preIndex; j <= i; j++ {
			sumValSquare += (sortedTimeSeries[j].dpVal - avgVal) * (sortedTimeSeries[j].dpVal - avgVal)
		}

		ts[sortedTimeSeries[i].downsampledTime] = sumValSquare / float64(i-preIndex+1)
	}
	return ts
}

func getSlopeByLinearRegression(sortedTimeSeries []Entry, preIndex int, i int) float64 {
	// Find neighboring data points for linear regression
	var x []float64
	var y []float64

	// Collect data points for linear regression
	for j := preIndex; j <= i; j++ {
		x = append(x, float64(sortedTimeSeries[j].downsampledTime))
		y = append(y, sortedTimeSeries[j].dpVal)
	}

	var sumX, sumY, sumXY, sumX2 float64
	for k := 0; k < len(x); k++ {
		sumX += x[k]
		sumY += y[k]
		sumXY += x[k] * y[k]
		sumX2 += x[k] * x[k]
	}
	n := float64(len(x))
	slope := (n*sumXY - sumX*sumY) / (n*sumX2 - sumX*sumX)
	return slope
}
