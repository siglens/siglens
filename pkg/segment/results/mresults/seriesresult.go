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
	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
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
	convertedDownsampleAggFn sutils.AggregateFunctions
	aggregationConstant      float64
}

type DownsampleSeries struct {
	idx    int // runningEntries[:idx] is guaranteed to have valid results
	len    int // denotes the number of available elements. When idx==len, the underlying slice needs to be resized
	sorted bool

	// original Downsampler Aggregator which comes in with metricsQuery
	downsampleAggFn     sutils.AggregateFunctions
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
	if downsampleAggFn == sutils.Avg {
		convertedDownsampleAggFn = sutils.Sum
	}
	aggregationConstant := mQuery.FirstAggregator.FuncConstant

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

func (s *Series) Merge(toJoin *Series) error {
	toJoinEntries := toJoin.entries[:toJoin.idx]
	s.entries = s.entries[:s.idx]
	s.len = s.idx
	s.entries = append(s.entries, toJoinEntries...)
	s.idx += toJoin.idx
	s.sorted = false
	s.len += toJoin.idx

	return nil
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

func ApplyFunction(seriesId string, ts map[uint32]float64, function structs.Function, timeRange *dtypeutils.MetricsTimeRange) (string, map[uint32]float64, error) {
	if function.RangeFunction > 0 {
		ts, err := ApplyRangeFunction(ts, function, timeRange)
		return seriesId, ts, err
	}

	if function.MathFunction > 0 {
		ts, err := ApplyMathFunction(ts, function)
		return seriesId, ts, err
	}

	if function.TimeFunction > 0 {
		ts, err := ApplyTimeFunction(ts, function)
		return seriesId, ts, err
	}

	switch function.FunctionType {
	case structs.LabelFunction:
		seriesId, err := ApplyLabelFunction(seriesId, function.LabelFunction)
		return seriesId, ts, err
	// TODO: Implement other function types and remove the above if blocks
	default:
		return seriesId, ts, nil
	}
}

func ApplyLabelFunction(seriesId string, labelFunction *structs.LabelFunctionExpr) (string, error) {
	switch labelFunction.FunctionType {
	case sutils.LabelJoin:
		return seriesId, fmt.Errorf("ApplyLabelFunction: label_join is not supported")
	case sutils.LabelReplace:
		return applyLabelReplace(seriesId, labelFunction)
	}

	return seriesId, nil
}

func applyLabelReplace(seriesId string, labelFunction *structs.LabelFunctionExpr) (string, error) {
	if labelFunction == nil {
		return seriesId, fmt.Errorf("applyLabelReplace: labelFunction is nil")
	}

	if labelFunction.SourceLabel == "" {
		seriesId = fmt.Sprintf("%s,%s:%s", seriesId, labelFunction.DestinationLabel, labelFunction.Replacement.NameBasedVal)
		return seriesId, nil
	}

	_, values := ExtractGroupByFieldsFromSeriesId(seriesId, []string{labelFunction.SourceLabel})
	if len(values) == 0 {
		return seriesId, nil
	}

	keyToValuesMap, extractedValuesSlice, err := structs.MatchAndExtractGroups(values[0], labelFunction.GobRegexp.GetCompiledRegex())
	if err != nil {
		// If there are no matches, return the original seriesId
		return seriesId, nil
	}

	var replacementValue string

	switch labelFunction.Replacement.KeyType {
	case structs.IndexBased:
		if len(extractedValuesSlice) <= labelFunction.Replacement.IndexBasedVal {
			return seriesId, nil
		}

		replacementValue = extractedValuesSlice[labelFunction.Replacement.IndexBasedVal]
	case structs.NameBased:
		if _, ok := keyToValuesMap[labelFunction.Replacement.NameBasedVal]; !ok {
			return seriesId, nil
		}

		replacementValue = keyToValuesMap[labelFunction.Replacement.NameBasedVal]
	default:
		return seriesId, fmt.Errorf("applyLabelReplace: unsupported key type %v", labelFunction.Replacement.KeyType)
	}

	pattern := fmt.Sprintf(`\b%s:[^,]*`, labelFunction.DestinationLabel)
	replacement := fmt.Sprintf("%s:%s", labelFunction.DestinationLabel, replacementValue)

	// Use regex to replace the entire "key:value" pair
	re, err := regexp.Compile(pattern)
	if err != nil {
		return seriesId, fmt.Errorf("applyLabelReplace:  Error compiling regex pattern with destination label. Err=%v", err)
	}
	if re.MatchString(seriesId) {
		seriesId = re.ReplaceAllString(seriesId, replacement)
	} else {
		// If the key is not found, append it
		seriesId = fmt.Sprintf("%s%s,", seriesId, replacement)
	}

	return seriesId, nil
}

func ApplyMathFunction(ts map[uint32]float64, function structs.Function) (map[uint32]float64, error) {
	var err error
	switch function.MathFunction {
	case sutils.Abs:
		evaluate(ts, math.Abs)
	case sutils.Sqrt:
		applyFuncToNonNegativeValues(ts, math.Sqrt)
	case sutils.Ceil:
		evaluate(ts, math.Ceil)
	case sutils.Floor:
		evaluate(ts, math.Floor)
	case sutils.Round:
		if len(function.ValueList) > 0 && len(function.ValueList[0]) > 0 {
			err = evaluateRoundWithPrecision(ts, function.ValueList[0])
		} else {
			evaluate(ts, math.Round)
		}
	case sutils.Exp:
		evaluate(ts, math.Exp)
	case sutils.Ln:
		applyFuncToNonNegativeValues(ts, math.Log)
	case sutils.Log2:
		applyFuncToNonNegativeValues(ts, math.Log2)
	case sutils.Log10:
		applyFuncToNonNegativeValues(ts, math.Log10)
	case sutils.Sgn:
		evaluate(ts, calculateSgn)
	case sutils.Deg:
		evaluate(ts, func(val float64) float64 {
			return val * 180 / math.Pi
		})
	case sutils.Rad:
		evaluate(ts, func(val float64) float64 {
			return val * math.Pi / 180
		})

	case sutils.Acos:
		err = evaluateWithErr(ts, func(val float64) (float64, error) {
			if val < -1 || val > 1 {
				return val, fmt.Errorf("evaluateWithErr: acos evaluate values in the range [-1,1], but got input value: %v", val)
			}
			return math.Acos(val), nil
		})
	case sutils.Acosh:
		err = evaluateWithErr(ts, func(val float64) (float64, error) {
			if val < 1 {
				return val, fmt.Errorf("evaluateWithErr: acosh evaluate values in the range [1,+Inf], but got input value: %v", val)
			}
			return math.Acosh(val), nil
		})
	case sutils.Asin:
		err = evaluateWithErr(ts, func(val float64) (float64, error) {
			if val < -1 || val > 1 {
				return val, fmt.Errorf("evaluateWithErr: asin evaluate values in the range [-1,1], but got input value: %v", val)
			}
			return math.Asin(val), nil
		})
	case sutils.Asinh:
		evaluate(ts, math.Asinh)
	case sutils.Atan:
		evaluate(ts, math.Atan)
	case sutils.Atanh:
		err = evaluateWithErr(ts, func(val float64) (float64, error) {
			if val <= -1 || val >= 1 {
				return val, fmt.Errorf("evaluateWithErr: atanh evaluate values in the range [-1,1], but got input value: %v", val)
			}
			return math.Atanh(val), nil
		})
	case sutils.Cos:
		evaluate(ts, math.Cos)
	case sutils.Cosh:
		evaluate(ts, math.Cosh)
	case sutils.Sin:
		evaluate(ts, math.Sin)
	case sutils.Sinh:
		evaluate(ts, math.Sinh)
	case sutils.Tan:
		evaluate(ts, math.Tan)
	case sutils.Tanh:
		evaluate(ts, math.Tanh)
	case sutils.Clamp:
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
	case sutils.Clamp_Max:
		if len(function.ValueList) != 1 {
			return ts, fmt.Errorf("ApplyMathFunction: clamp_max has incorrect parameters: %v", function.ValueList)
		}
		maxVal, err := strconv.ParseFloat(function.ValueList[0], 64)
		if err != nil {
			return ts, fmt.Errorf("ApplyMathFunction: clamp_max has incorrect parameters: %v", function.ValueList)
		}
		evaluateClamp(ts, -1.7976931348623157e+308, maxVal)
	case sutils.Clamp_Min:
		if len(function.ValueList) != 1 {
			return ts, fmt.Errorf("ApplyMathFunction: clamp_min has incorrect parameters: %v", function.ValueList)
		}
		minVal, err := strconv.ParseFloat(function.ValueList[0], 64)
		if err != nil {
			return ts, fmt.Errorf("ApplyMathFunction: clamp_min has incorrect parameters: %v", function.ValueList)
		}
		evaluateClamp(ts, minVal, math.MaxFloat64)
	case sutils.Timestamp:
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

func ApplyRangeFunction(ts map[uint32]float64, function structs.Function, timeRange *dtypeutils.MetricsTimeRange) (map[uint32]float64, error) {

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

		// delete the entry so that only the values added after processing the range function are left
		delete(ts, time)
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
	case sutils.Derivative:
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
	case sutils.Predict_Linear:
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
	case sutils.Rate:
		return evaluateRate(sortedTimeSeries, ts, timeRange, function)
	case sutils.IRate:
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
	case sutils.Increase:
		// Increase is extrapolated to cover the full time range as specified in the range vector selector. (increse = avg rate * timewindow)
		return evaluateRate(sortedTimeSeries, ts, timeRange, function)
	case sutils.Delta:
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
	case sutils.IDelta:
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
	case sutils.Changes:
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
	case sutils.Resets:
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
	case sutils.Avg_Over_Time, sutils.Min_Over_Time, sutils.Max_Over_Time, sutils.Sum_Over_Time, sutils.Count_Over_Time, sutils.Last_Over_Time:
		return evaluateAggregationOverTime(sortedTimeSeries, ts, function, timeRange)
	case sutils.Stdvar_Over_Time:
		return evaluateStandardVariance(sortedTimeSeries, ts, timeWindow), nil
	case sutils.Stddev_Over_Time:
		ts = evaluateStandardVariance(sortedTimeSeries, ts, timeWindow)
		for key, val := range ts {
			ts[key] = math.Sqrt(val)
		}
		return ts, nil
	case sutils.Mad_Over_Time:
		return evaluateMADOverTime(sortedTimeSeries, ts, timeWindow), nil
	case sutils.Present_Over_Time:
		for key := range ts {
			ts[key] = 1
		}
		return ts, nil
	case sutils.Quantile_Over_Time:
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

			if index2 >= len(sortedTimeSeries) || index1 >= len(sortedTimeSeries) || index1 < 0 || index2 < 0 {
				log.Errorf("ApplyRangeFunction: index out of range: index1=%v, index2=%v, preIndex=%v, i=%v. len(sortedTimeSeries)=%v", index1, index2, preIndex, i, len(sortedTimeSeries))
				continue
			}

			val := sortedTimeSeries[index1].dpVal + (p-math.Floor(p))*(sortedTimeSeries[index2].dpVal-sortedTimeSeries[index1].dpVal)
			ts[sortedTimeSeries[i].downsampledTime] = val
		}
		return ts, nil
	default:
		return ts, fmt.Errorf("ApplyRangeFunction: Unknown function type: %v", function.RangeFunction)
	}
}

func evaluateAggregationOverTime(sortedTimeSeries []Entry, ts map[uint32]float64, function structs.Function,
	timeRange *dtypeutils.MetricsTimeRange) (map[uint32]float64, error) {

	if len(sortedTimeSeries) == 0 {
		return ts, nil
	}

	timeWindow := uint32(function.TimeWindow)
	step := uint32(function.Step)
	nextEvaluationTime := timeRange.StartEpochSec

	var prefixSum []float64

	if function.RangeFunction == sutils.Sum_Over_Time || function.RangeFunction == sutils.Avg_Over_Time {
		prefixSum = make([]float64, len(sortedTimeSeries)+1)
		prefixSum[1] = sortedTimeSeries[0].dpVal
		for i := 1; i < len(sortedTimeSeries); i++ {
			prefixSum[i+1] = (prefixSum[i] + sortedTimeSeries[i].dpVal)
		}
	}

	for nextEvaluationTime <= timeRange.EndEpochSec {
		timeWindowStartTime := nextEvaluationTime - timeWindow

		// In Prometheus, the time window is left-open and right-closed, meaning
		// that the start time is exclusive and the end time is inclusive.
		// refer to: https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors

		// Find index of the first point within the time window using binary search (Exclusive)
		preIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
			return sortedTimeSeries[j].downsampledTime > timeWindowStartTime
		})

		// Find index of the last point within the time window using binary search (Inclusive)
		lastIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
			return sortedTimeSeries[j].downsampledTime > nextEvaluationTime
		}) - 1

		if lastIndex < preIndex {
			ts[nextEvaluationTime] = 0
			nextEvaluationTime += step
			continue
		}

		switch function.RangeFunction {
		case sutils.Count_Over_Time:
			ts[nextEvaluationTime] = float64(lastIndex - preIndex + 1)
		case sutils.Sum_Over_Time:
			ts[nextEvaluationTime] = prefixSum[lastIndex+1] - prefixSum[preIndex]
		case sutils.Avg_Over_Time:
			ts[nextEvaluationTime] = (prefixSum[lastIndex+1] - prefixSum[preIndex]) / float64(lastIndex-preIndex+1)
		case sutils.Min_Over_Time:
			min := math.MaxFloat64
			for j := preIndex; j <= lastIndex; j++ {
				min = math.Min(min, sortedTimeSeries[j].dpVal)
			}
			ts[nextEvaluationTime] = min
		case sutils.Max_Over_Time:
			max := -math.MaxFloat64
			for j := preIndex; j <= lastIndex; j++ {
				max = math.Max(max, sortedTimeSeries[j].dpVal)
			}
			ts[nextEvaluationTime] = max
		case sutils.Last_Over_Time:
			// the most recent point value in the specified interval
			ts[nextEvaluationTime] = sortedTimeSeries[lastIndex].dpVal
		default:
			return ts, fmt.Errorf("evaluateAggregationOverTime: unsupported function type %v", function.RangeFunction)
		}

		nextEvaluationTime += step
	}

	return ts, nil
}

/*
* Median Absolute Deviation Over Time
1. Calculate the median absolute deviation of all points in the specified interval
2. Calculate the absolute difference between each point and obtained median in that interval
3. Calculate median of these obtained values
*
*/
func evaluateMADOverTime(sortedTimeSeries []Entry, ts map[uint32]float64, timeWindow uint32) map[uint32]float64 {
	for i := range sortedTimeSeries {
		// Define the start time of the window for the current entry
		timeWindowStartTime := sortedTimeSeries[i].downsampledTime
		if timeWindow < sortedTimeSeries[i].downsampledTime {
			timeWindowStartTime -= timeWindow
		} else {
			// Case where the window calculation might underflow
			ts[sortedTimeSeries[i].downsampledTime] = 0
			continue
		}
		// Index of the first entry within the time window
		preIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
			return sortedTimeSeries[j].downsampledTime >= timeWindowStartTime
		})

		if preIndex >= i { // Check if there is no previous data point within the window
			ts[sortedTimeSeries[i].downsampledTime] = 0
			continue
		}

		// All values within the time window
		values := make([]float64, 0, i-preIndex+1)
		for j := preIndex; j <= i; j++ {
			values = append(values, sortedTimeSeries[j].dpVal)
		}

		// Median of these values
		median := computeMedian(values)
		// Absolute deviations from the median
		deviations := make([]float64, len(values))
		for k, v := range values {
			deviations[k] = math.Abs(v - median)
		}

		// Median of the absolute deviations
		mad := computeMedian(deviations)
		// Store the MAD in the result map
		ts[sortedTimeSeries[i].downsampledTime] = mad
	}
	return ts
}

// Median Calculation
func computeMedian(values []float64) float64 {
	sort.Float64s(values)
	n := len(values)
	if n%2 == 0 {
		return (values[n/2-1] + values[n/2]) / 2
	}
	return values[n/2]
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

func reduceEntries(entries []Entry, fn sutils.AggregateFunctions, fnConstant float64) (float64, error) {
	var ret float64
	switch fn {
	case sutils.Sum:
		for i := range entries {
			ret += entries[i].dpVal
		}
	case sutils.BottomK:
		fallthrough
	case sutils.Min:
		for i := range entries {
			if i == 0 || entries[i].dpVal < ret {
				ret = entries[i].dpVal
			}
		}
	case sutils.TopK:
		fallthrough
	case sutils.Max:
		for i := range entries {
			if i == 0 || entries[i].dpVal > ret {
				ret = entries[i].dpVal
			}
		}
	case sutils.Count:
		// Count is to calculate the number of time series, we do not care about the entry value
	case sutils.Quantile: //valid range for fnConstant is 0 <= fnConstant <= 1
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
	case sutils.Stddev:
		fallthrough
	case sutils.Stdvar:
		sum := 0.0
		for i := range entries {
			sum += entries[i].dpVal
		}
		ret = sum / float64(len(entries))
	case sutils.Group:
		ret = 1
	default:
		err := fmt.Errorf("reduceEntries: unsupported AggregateFunction: %v", fn)
		log.Errorf("%v", err)
		return 0.0, err
	}

	return ret, nil
}

func reduceRunningEntries(entries []RunningEntry, fn sutils.AggregateFunctions, fnConstant float64) (float64, error) {
	var ret float64
	switch fn {
	case sutils.Avg:
		count := uint64(0)
		for i := range entries {
			ret += entries[i].runningVal
			count += entries[i].runningCount
		}
		ret = ret / float64(count)
	case sutils.Sum:
		for i := range entries {
			ret += entries[i].runningVal
		}
	case sutils.Min:
		for i := range entries {
			if i == 0 || entries[i].runningVal < ret {
				ret = entries[i].runningVal
			}
		}
	case sutils.Max:
		for i := range entries {
			if i == 0 || entries[i].runningVal > ret {
				ret = entries[i].runningVal
			}
		}
	case sutils.Count:
		// Count is to calculate the number of time series, we do not care about the entry value
	case sutils.Quantile: //valid range for fnConstant is 0 <= fnConstant <= 1
		// TODO: calculate the quantile without needing to sort the elements.

		entriesCopy := make([]RunningEntry, len(entries))
		copy(entriesCopy, entries)
		sort.Slice(entriesCopy, func(i, k int) bool {
			return entriesCopy[i].runningVal < entriesCopy[k].runningVal
		})

		index := fnConstant * float64(len(entriesCopy)-1)
		// Check for special cases when quantile position doesn't fall on an exact index
		if index >= 0 && index != float64(int(index)) && int(index)+1 < len(entriesCopy) {
			// Calculate the weight for interpolation
			fraction := index - float64(int(index))

			dpVal1 := entriesCopy[int(index)].runningVal
			dpVal2 := entriesCopy[int(index)+1].runningVal

			ret = dpVal1 + fraction*(dpVal2-dpVal1)
		} else if index >= 0 && int(index) < len(entriesCopy) {
			ret = entriesCopy[int(index)].runningVal
		} else {
			log.Errorf("reduceRunningEntries: invalid index: %v, len(entriesCopy): %v", index, len(entriesCopy))
		}
	case sutils.Group:
		ret = 1
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
	case sutils.Hour:
		evaluateTimeFunc(allDPs, func(t time.Time) float64 {
			return float64(t.Hour())
		})
	case sutils.Minute:
		evaluateTimeFunc(allDPs, func(t time.Time) float64 {
			return float64(t.Minute())
		})
	case sutils.Month:
		evaluateTimeFunc(allDPs, func(t time.Time) float64 {
			return float64(t.Month())
		})
	case sutils.Year:
		evaluateTimeFunc(allDPs, func(t time.Time) float64 {
			return float64(t.Year())
		})
	case sutils.DayOfMonth:
		evaluateTimeFunc(allDPs, func(t time.Time) float64 {
			return float64(t.Day())
		})
	case sutils.DayOfWeek:
		evaluateTimeFunc(allDPs, func(t time.Time) float64 {
			return float64(t.Weekday())
		})
	case sutils.DayOfYear:
		evaluateTimeFunc(allDPs, func(t time.Time) float64 {
			return float64(t.YearDay())
		})
	case sutils.DaysInMonth:
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

func applyFuncToNonNegativeValues(ts map[uint32]float64, mathFunc float64Func) {
	for key, val := range ts {
		if val < 0 {
			ts[key] = math.NaN()
			continue
		}
		ts[key] = mathFunc(val)
	}
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

// Computes the per-second rate for each step within the time window.
// The rate is derived from the first and last samples in the window.
// If the counter resets, adjustments are made to maintain accuracy.
// The rate is then normalized over the specified time window.
// This function is inspired by Prometheus' `extrapolatedRate` function,
// adapting the extrapolation logic to ensure Prometheus compatibility.
// Source: https://github.com/prometheus/prometheus/blob/main/promql/functions.go#L72
// Explanation about the extrapolation logic: https://promlabs.com/blog/2021/01/29/how-exactly-does-promql-calculate-rates/
func evaluateRate(sortedTimeSeries []Entry, ts map[uint32]float64, timeRange *dtypeutils.MetricsTimeRange, function structs.Function) (map[uint32]float64, error) {
	if len(sortedTimeSeries) == 0 {
		return ts, nil
	}

	var isRate bool

	switch function.RangeFunction {
	case sutils.Rate:
		isRate = true
	case sutils.Increase:
		isRate = false
	default:
		return ts, fmt.Errorf("evaluateRate: unsupported function type %v", function.RangeFunction)
	}

	var dx float64

	timeWindow := uint32(function.TimeWindow)
	step := uint32(function.Step)
	nextEvaluationTime := timeRange.StartEpochSec

	for nextEvaluationTime <= timeRange.EndEpochSec {
		windowStartTime := nextEvaluationTime - timeWindow

		// Find the first sample in the time window using binary search (inclusive)
		preIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
			return sortedTimeSeries[j].downsampledTime >= windowStartTime
		})

		// Find the last sample (including `nextEvaluationTime`)
		lastIndex := sort.Search(len(sortedTimeSeries), func(j int) bool {
			return sortedTimeSeries[j].downsampledTime > nextEvaluationTime
		}) - 1

		// If there are not atleast two samples within the time window, continue to the next evaluation time
		if lastIndex < preIndex || lastIndex-preIndex < 1 {
			nextEvaluationTime += step
			continue
		}

		firstSample := sortedTimeSeries[preIndex]
		lastSample := sortedTimeSeries[lastIndex]

		// Compute the value difference between the first and last sample
		dx = lastSample.dpVal - firstSample.dpVal
		prevValue := firstSample.dpVal
		for i := preIndex + 1; i <= lastIndex; i++ {
			if sortedTimeSeries[i].dpVal < prevValue {
				// Counter reset detected, adjust the value difference
				dx += prevValue
			}
			prevValue = sortedTimeSeries[i].dpVal
		}

		// Calculate the time between the first and last sample
		totalSampleDuration := float64(lastSample.downsampledTime - firstSample.downsampledTime)
		avgDurationBetweenSamples := totalSampleDuration / float64(lastIndex-preIndex)

		// The extrapolation limit is set to 10% above the avgDurationBetweenSamples
		// This is taken from Prometheus' extrapolatedRate function
		extrapolationDurationLimit := avgDurationBetweenSamples * 1.1

		// Adjust durationToStart based on sample distribution
		durationToStart := float64(firstSample.downsampledTime - windowStartTime)
		if durationToStart >= extrapolationDurationLimit {
			durationToStart = avgDurationBetweenSamples / 2
		}

		if dx > 0 && firstSample.dpVal >= 0 {
			// Ensure counter extrapolation does not assume negative values.
			// If the counter is increasing (dx > 0) and starts from a non-negative value (firstSample.dpVal >= 0),
			// we estimate when the counter would have been zero using linear interpolation.
			// If this estimated time to zero is shorter than durationToStart, we update durationToStart.
			// This prevents excessive extrapolation into the past where the counter may not have existed yet.
			estimatedZeroTime := totalSampleDuration * (firstSample.dpVal / dx)
			if estimatedZeroTime < durationToStart {
				durationToStart = estimatedZeroTime
			}
		}

		durationToEnd := float64(nextEvaluationTime - lastSample.downsampledTime)
		if durationToEnd >= extrapolationDurationLimit {
			durationToEnd = avgDurationBetweenSamples / 2
		}

		totalExtrapolatedDuration := totalSampleDuration + durationToStart + durationToEnd

		// Calculate the extrapolated rate
		finalDx := dx * (totalExtrapolatedDuration / totalSampleDuration)
		if isRate {
			finalDx /= float64(timeWindow)
		}

		ts[nextEvaluationTime] = finalDx
		nextEvaluationTime += step
	}

	return ts, nil
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
