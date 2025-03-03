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

package metrics

import (
	"sort"

	"github.com/siglens/siglens/pkg/utils"
)

type epoch uint32

type timeseries interface {
	AtOrBefore(timestamp epoch) (float64, bool)
	Iterator() utils.Iterator[entry]
	Range(start epoch, end epoch, mode RangeMode) timeseries
}

type rangeIterable interface {
	rangeIterator(start epoch, end epoch, mode RangeMode) utils.Iterator[entry]
}

type rangeIterableSeries interface {
	rangeIterable
	timeseries
}

type entry struct {
	timestamp epoch
	value     float64
}

type lookupSeries struct {
	values []entry
}

func (t *lookupSeries) AtOrBefore(timestamp epoch) (float64, bool) {
	i := sort.Search(len(t.values), func(k int) bool {
		return t.values[k].timestamp > timestamp
	})

	if i > 0 {
		return t.values[i-1].value, true
	}

	return 0, false
}

func (t *lookupSeries) Iterator() utils.Iterator[entry] {
	return utils.NewIterator(t.values)
}

func (t *lookupSeries) Range(start epoch, end epoch, mode RangeMode) timeseries {
	return &rangeSeries{
		series: t,
		start:  start,
		end:    end,
		mode:   mode,
	}
}

func (t *lookupSeries) rangeIterator(start epoch, end epoch, mode RangeMode) utils.Iterator[entry] {
	switch mode {
	case PromQl3Range:
		startIndex := sort.Search(len(t.values), func(i int) bool {
			return t.values[i].timestamp > start
		})
		endIndex := sort.Search(len(t.values), func(i int) bool {
			return t.values[i].timestamp > end
		})

		if startIndex >= endIndex {
			return utils.NewIterator([]entry{})
		}

		return utils.NewIterator(t.values[startIndex:endIndex])
	}

	return utils.NewIterator([]entry{})
}

type generatedSeries struct {
	timestamps []epoch
	valueAt    func(epoch) float64
}

func (g *generatedSeries) AtOrBefore(timestamp epoch) (float64, bool) {
	if len(g.timestamps) == 0 || timestamp < g.timestamps[0] {
		return 0, false
	}

	return g.valueAt(timestamp), true
}

func (g *generatedSeries) Iterator() utils.Iterator[entry] {
	return &generatedIterator{
		series: g,
		index:  0,
	}
}

type generatedIterator struct {
	series *generatedSeries
	index  int
}

func (gi *generatedIterator) Next() (entry, bool) {
	if gi.index >= len(gi.series.timestamps) {
		return entry{}, false
	}

	value := entry{
		timestamp: gi.series.timestamps[gi.index],
		value:     gi.series.valueAt(gi.series.timestamps[gi.index]),
	}

	gi.index++

	return value, true
}

func (g *generatedSeries) Range(start epoch, end epoch, mode RangeMode) timeseries {
	switch mode {
	case PromQl3Range:
		startIndex := sort.Search(len(g.timestamps), func(i int) bool {
			return g.timestamps[i] > start
		})
		endIndex := sort.Search(len(g.timestamps), func(i int) bool {
			return g.timestamps[i] > end
		})

		values := make([]entry, 0)
		for i := startIndex; i < endIndex; i++ {
			values = append(values, entry{timestamp: g.timestamps[i], value: g.valueAt(g.timestamps[i])})
		}

		return &lookupSeries{values: values}
	}

	return nil
}

// When getting the value at time T, and T is outside the range, no value is returned.
type rangeSeries struct {
	series rangeIterableSeries
	start  epoch
	end    epoch
	mode   RangeMode
}

type RangeMode int

const (
	// Start is exclusive; end is inclusive.
	// See https://github.com/prometheus/prometheus/issues/13213
	PromQl3Range RangeMode = iota + 1
)

func (r *rangeSeries) AtOrBefore(timestamp epoch) (float64, bool) {
	switch r.mode {
	case PromQl3Range:
		if timestamp <= r.start || timestamp > r.end {
			return 0, false
		}

		return r.series.AtOrBefore(timestamp)
	}

	return 0, false
}

func (r *rangeSeries) Iterator() utils.Iterator[entry] {
	return r.series.rangeIterator(r.start, r.end, r.mode)
}

func (r *rangeSeries) Range(start epoch, end epoch, mode RangeMode) timeseries {
	if mode != r.mode {
		return nil
	}

	switch r.mode {
	case PromQl3Range:
		start := max(r.start, start)
		end := min(r.end, end)
		return &rangeSeries{
			series: r.series,
			start:  start,
			end:    end,
			mode:   r.mode,
		}
	}

	return nil
}

type aggSeries struct {
	allSeries  []timeseries
	aggregator func([]float64) float64

	isEvaluated bool
	result      timeseries
}

func (a *aggSeries) AtOrBefore(timestamp epoch) (float64, bool) {
	if !a.isEvaluated {
		a.result = a.evaluate()
		a.isEvaluated = true
	}

	return a.result.AtOrBefore(timestamp)
}

func (a *aggSeries) Iterator() utils.Iterator[entry] {
	if !a.isEvaluated {
		a.result = a.evaluate()
		a.isEvaluated = true
	}

	return a.result.Iterator()
}

func (a *aggSeries) Range(start epoch, end epoch, mode RangeMode) timeseries {
	if !a.isEvaluated {
		a.result = a.evaluate()
		a.isEvaluated = true
	}

	return a.result.Range(start, end, mode)
}

func (a *aggSeries) evaluate() timeseries {
	if len(a.allSeries) == 0 {
		return &lookupSeries{values: []entry{}}
	}

	allIters := make([]utils.Iterator[entry], 0, len(a.allSeries))
	for _, series := range a.allSeries {
		allIters = append(allIters, series.Iterator())
	}

	// Keep track of the current value for each iterator
	currentValues := make([]entry, len(allIters))
	hasValue := make([]bool, len(allIters))

	// Initialize values from all iterators
	for i, iter := range allIters {
		if value, ok := iter.Next(); ok {
			currentValues[i] = value
			hasValue[i] = true
		}
	}

	result := make([]entry, 0)

	for {
		// Find the earliest timestamp among all current values
		var minTimestamp epoch
		minFound := false

		for i, has := range hasValue {
			if has && (!minFound || currentValues[i].timestamp < minTimestamp) {
				minTimestamp = currentValues[i].timestamp
				minFound = true
			}
		}

		if !minFound {
			// No more values in any iterator
			break
		}

		// Collect all values that have the minimum timestamp
		valuesToAggregate := make([]float64, 0, len(allIters))

		for i, has := range hasValue {
			if has && currentValues[i].timestamp == minTimestamp {
				valuesToAggregate = append(valuesToAggregate, currentValues[i].value)

				// Advance this iterator since we've used its value
				if value, ok := allIters[i].Next(); ok {
					currentValues[i] = value
				} else {
					hasValue[i] = false
				}
			}
		}

		// Aggregate and add to result
		if len(valuesToAggregate) > 0 {
			aggregatedValue := a.aggregator(valuesToAggregate)
			result = append(result, entry{timestamp: minTimestamp, value: aggregatedValue})
		}
	}

	return &lookupSeries{values: result}
}

type downsampler struct {
	timeseries timeseries
	aggregator func([]float64) float64
	interval   epoch
}

func (d *downsampler) Evaluate() timeseries {
	iterator := d.timeseries.Iterator()

	firstEntry, ok := iterator.Next()
	if !ok {
		return &lookupSeries{}
	}

	currentBucket := d.snapToInterval(firstEntry.timestamp)
	currentValues := []float64{firstEntry.value}

	finalEntries := make([]entry, 0)

	for {
		firstEntry, ok = iterator.Next()
		if !ok {
			break
		}

		thisBucket := d.snapToInterval(firstEntry.timestamp)
		if thisBucket == currentBucket {
			currentValues = append(currentValues, firstEntry.value)
			continue
		}

		// Close the current bucket.
		if len(currentValues) > 0 {
			value := d.aggregator(currentValues)
			finalEntries = append(finalEntries, entry{timestamp: currentBucket, value: value})
		}

		currentBucket = d.snapToInterval(firstEntry.timestamp)
		currentValues = []float64{firstEntry.value}
	}

	// Close the last bucket.
	if len(currentValues) > 0 {
		value := d.aggregator(currentValues)
		finalEntries = append(finalEntries, entry{timestamp: currentBucket, value: value})
	}

	return &lookupSeries{values: finalEntries}
}

func (d *downsampler) snapToInterval(timestamp epoch) epoch {
	return timestamp - timestamp%d.interval
}
