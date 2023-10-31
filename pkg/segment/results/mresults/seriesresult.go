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
	"fmt"
	"sort"

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

	// if original Downsampler Aggregator is `Avg`, convertedDownsampleAggFn is equal to `Sum` else equal to original Downsampler Aggregator
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
			log.Errorf("Downsample: failed to reduce entries: %v", err)
			return nil, err
		}
		ds.Add(retVal, s.entries[i].downsampledTime, uint64(maxJ-i))
		i = maxJ - 1
	}
	ds.grpID = s.grpID
	return ds, nil
}

func initDownsampleSeries(agg structs.Aggreation) *DownsampleSeries {

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

func (dss *DownsampleSeries) Aggregate() (map[uint32]float64, error) {
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
			log.Errorf("Aggregate: failed to reduce running entries: %v", err)
			return nil, err
		}
		retVal[dss.runningEntries[i].downsampledTime] = currVal
		i = maxJ - 1
	}
	return retVal, nil
}

func ApplyRangeFunction(ts map[uint32]float64, function segutils.RangeFunctions) (map[uint32]float64, error) {
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

	// ts is a time series mapping timestamps to values
	switch function {
	case segutils.Derivative:
		// Calculate the derivative at each timestamp and store it in the resulting map
		var timestamps []uint32
		var values []float64

		for timestamp, value := range ts {
			timestamps = append(timestamps, timestamp)
			values = append(values, value)
		}

		for i := 0; i < len(timestamps); i++ {
			timestamp := timestamps[i]

			// Find neighboring data points for linear regression
			var x []float64
			var y []float64

			// Collect data points for linear regression
			for j := i - 1; j <= i+1; j++ {
				if j >= 0 && j < len(timestamps) {
					x = append(x, float64(timestamps[j]))
					y = append(y, values[j])
				}
			}

			if len(x) < 2 {
				log.Errorf("ApplyFunctions: %v does not have enough sample points", function)
				continue
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
			ts[timestamp] = slope
		}
		// derivtives at edges do not exist
		delete(ts, timestamps[len(timestamps)-1])
		delete(ts, timestamps[0])
		return ts, nil
	case segutils.Rate:
		// Calculate the rate (per-second rate) for each timestamp and store it in the resulting map
		if len(sortedTimeSeries) == 0 {
			return nil, nil
		}

		var dx, dt float64
		prevVal := sortedTimeSeries[0].dpVal
		for i := 1; i < len(sortedTimeSeries); i++ {
			// Calculate the time difference between consecutive data points
			dt = float64(sortedTimeSeries[i].downsampledTime - sortedTimeSeries[i-1].downsampledTime)
			curVal := sortedTimeSeries[i].dpVal

			if curVal > prevVal {
				dx = curVal - prevVal
			} else {
				// This metric was reset.
				dx = curVal
			}

			ts[sortedTimeSeries[i].downsampledTime] = dx / dt
			prevVal = curVal
		}

		// Rate at edge does not exist.
		delete(ts, sortedTimeSeries[0].downsampledTime)
		return ts, nil
	default:
		return ts, fmt.Errorf("ApplyFunctions: Unknown function type")
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
		ret += float64(len(entries))
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
		for i := range entries {
			ret += entries[i].runningVal
		}
		ret = ret / float64(len(entries))
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
		ret += float64(len(entries))
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
