// Copyright (c) 2021-2025 SigScalr, Inc.
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
	log "github.com/sirupsen/logrus"
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

	log.Errorf("lookupSeries.rangeIterator: unsupported mode %v", mode)
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

	log.Errorf("generatedSeries.Range: unsupported mode %v", mode)
	return &lookupSeries{}
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

	log.Errorf("rangeSeries.AtOrBefore: unsupported mode %v", r.mode)
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

	log.Errorf("rangeSeries.Range: unsupported mode %v", r.mode)
	return &lookupSeries{}
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

type valueMappingSeries struct {
	series  timeseries
	mapping func(float64) float64
}

func (v *valueMappingSeries) AtOrBefore(timestamp epoch) (float64, bool) {
	value, ok := v.series.AtOrBefore(timestamp)
	if !ok {
		return 0, false
	}

	return v.mapping(value), true
}

func (v *valueMappingSeries) Iterator() utils.Iterator[entry] {
	return &valueMappingIterator{
		series: v.series.Iterator(),
		mapper: v.mapping,
	}
}

type valueMappingIterator struct {
	series utils.Iterator[entry]
	mapper func(float64) float64
}

func (v *valueMappingIterator) Next() (entry, bool) {
	value, ok := v.series.Next()
	if !ok {
		return entry{}, false
	}

	return entry{timestamp: value.timestamp, value: v.mapper(value.value)}, true
}

func (v *valueMappingSeries) Range(start epoch, end epoch, mode RangeMode) timeseries {
	return &valueMappingSeries{
		series:  v.series.Range(start, end, mode),
		mapping: v.mapping,
	}
}

// This constructs a new series by taking an existing series, sliding a window
// over it, and performing some operation on the values in each window.
type windowMappingSeries struct {
	timeseries timeseries
	aggregator func([]float64) float64
	windowSize epoch
	stepSize   epoch // How far to slide the window each time
	endTime    epoch // The final window ends here (inclusive)
	mode       RangeMode

	isEvaluated bool
	result      timeseries
}

func (w *windowMappingSeries) AtOrBefore(timestamp epoch) (float64, bool) {
	if !w.isEvaluated {
		w.result = w.evaluate()
		w.isEvaluated = true
	}

	return w.result.AtOrBefore(timestamp)
}

func (w *windowMappingSeries) Iterator() utils.Iterator[entry] {
	if !w.isEvaluated {
		w.result = w.evaluate()
		w.isEvaluated = true
	}

	return w.result.Iterator()
}

func (w *windowMappingSeries) Range(start epoch, end epoch, mode RangeMode) timeseries {
	if !w.isEvaluated {
		w.result = w.evaluate()
		w.isEvaluated = true
	}

	return w.result.Range(start, end, mode)
}

func (w *windowMappingSeries) evaluate() timeseries {
	if w.stepSize == 0 || w.windowSize == 0 {
		return &lookupSeries{}
	}

	// Get all values from the input series
	iterator := w.timeseries.Iterator()
	allValues := make([]entry, 0)

	for {
		e, ok := iterator.Next()
		if !ok {
			break
		}
		allValues = append(allValues, e)
	}

	if len(allValues) == 0 {
		return &lookupSeries{}
	}

	finalValues := make([]entry, 0)

	switch w.mode {
	case PromQl3Range:
		startTimestamp := allValues[0].timestamp
		breakNextIter := false
		for windowEnd := w.endTime; windowEnd >= startTimestamp && !breakNextIter; windowEnd -= w.stepSize {
			windowStart := windowEnd - w.windowSize
			if w.windowSize > windowEnd {
				// Handle underflow
				windowStart = 0
			}

			if w.stepSize > windowEnd {
				breakNextIter = true
			}

			subSeries := w.timeseries.Range(windowStart, windowEnd, PromQl3Range)
			subIterator := subSeries.Iterator()
			valuesInWindow := make([]float64, 0)
			for {
				point, ok := subIterator.Next()
				if !ok {
					break
				}

				valuesInWindow = append(valuesInWindow, point.value)
			}

			if len(valuesInWindow) > 0 {
				aggregatedValue := w.aggregator(valuesInWindow)
				finalValues = append(finalValues, entry{
					timestamp: windowEnd,
					value:     aggregatedValue,
				})
			}
		}

		// Reverse the finalValues since we built it in reverse order.
		for i := 0; i < len(finalValues)/2; i++ {
			k := len(finalValues) - 1 - i
			finalValues[i], finalValues[k] = finalValues[k], finalValues[i]
		}

		return &lookupSeries{values: finalValues}
	}

	log.Errorf("windowMappingSeries.evaluate: unsupported mode %v", w.mode)
	return &lookupSeries{}
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
