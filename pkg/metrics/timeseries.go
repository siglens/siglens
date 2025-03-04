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
	"fmt"
	"sort"

	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type Epoch uint32

type timeseries interface {
	AtOrBefore(timestamp Epoch) (float64, bool)
	Iterator() utils.Iterator[Entry]
	Range(start Epoch, end Epoch, mode RangeMode) timeseries
}

type rangeIterable interface {
	rangeIterator(start Epoch, end Epoch, mode RangeMode) utils.Iterator[Entry]
}

type rangeIterableSeries interface {
	rangeIterable
	timeseries
}

type Entry struct {
	Timestamp Epoch
	Value     float64
}

type lookupSeries struct {
	values []Entry
}

func NewLookupSeries(values []Entry) timeseries {
	return &lookupSeries{values: values}
}

func (t *lookupSeries) AtOrBefore(timestamp Epoch) (float64, bool) {
	i := sort.Search(len(t.values), func(k int) bool {
		return t.values[k].Timestamp > timestamp
	})

	if i > 0 {
		return t.values[i-1].Value, true
	}

	return 0, false
}

func (t *lookupSeries) Iterator() utils.Iterator[Entry] {
	return utils.NewIterator(t.values)
}

func (t *lookupSeries) Range(start Epoch, end Epoch, mode RangeMode) timeseries {
	return &rangeSeries{
		series: t,
		start:  start,
		end:    end,
		mode:   mode,
	}
}

func (t *lookupSeries) rangeIterator(start Epoch, end Epoch, mode RangeMode) utils.Iterator[Entry] {
	switch mode {
	case PromQl3Range:
		startIndex := sort.Search(len(t.values), func(i int) bool {
			return t.values[i].Timestamp > start
		})
		endIndex := sort.Search(len(t.values), func(i int) bool {
			return t.values[i].Timestamp > end
		})

		if startIndex >= endIndex {
			return utils.NewIterator([]Entry{})
		}

		return utils.NewIterator(t.values[startIndex:endIndex])
	}

	log.Errorf("lookupSeries.rangeIterator: unsupported mode %v", mode)
	return utils.NewIterator([]Entry{})
}

type generatedSeries struct {
	timestamps []Epoch
	valueAt    func(Epoch) float64
}

func NewGeneratedSeries(timestamps []Epoch, valueAt func(Epoch) float64) (timeseries, error) {
	if valueAt == nil {
		return nil, fmt.Errorf("nil valueAt function")
	}

	return &generatedSeries{
		timestamps: timestamps,
		valueAt:    valueAt,
	}, nil
}

func (g *generatedSeries) AtOrBefore(timestamp Epoch) (float64, bool) {
	if len(g.timestamps) == 0 || timestamp < g.timestamps[0] {
		return 0, false
	}

	return g.valueAt(timestamp), true
}

func (g *generatedSeries) Iterator() utils.Iterator[Entry] {
	return &generatedIterator{
		series: g,
		index:  0,
	}
}

type generatedIterator struct {
	series *generatedSeries
	index  int
}

func (gi *generatedIterator) Next() (Entry, bool) {
	if gi.index >= len(gi.series.timestamps) {
		return Entry{}, false
	}

	value := Entry{
		Timestamp: gi.series.timestamps[gi.index],
		Value:     gi.series.valueAt(gi.series.timestamps[gi.index]),
	}

	gi.index++

	return value, true
}

func (g *generatedSeries) Range(start Epoch, end Epoch, mode RangeMode) timeseries {
	switch mode {
	case PromQl3Range:
		startIndex := sort.Search(len(g.timestamps), func(i int) bool {
			return g.timestamps[i] > start
		})
		endIndex := sort.Search(len(g.timestamps), func(i int) bool {
			return g.timestamps[i] > end
		})

		values := make([]Entry, 0)
		for i := startIndex; i < endIndex; i++ {
			values = append(values, Entry{Timestamp: g.timestamps[i], Value: g.valueAt(g.timestamps[i])})
		}

		return &lookupSeries{values: values}
	}

	log.Errorf("generatedSeries.Range: unsupported mode %v", mode)
	return &lookupSeries{}
}

// When getting the value at time T, and T is outside the range, no value is returned.
type rangeSeries struct {
	series rangeIterableSeries
	start  Epoch
	end    Epoch
	mode   RangeMode
}

type RangeMode int

const (
	// Start is exclusive; end is inclusive.
	// See https://github.com/prometheus/prometheus/issues/13213
	PromQl3Range RangeMode = iota + 1
)

func isValidRangeMode(mode RangeMode) bool {
	switch mode {
	case PromQl3Range:
		return true
	}

	return false
}

func NewRangeSeries(series rangeIterableSeries, start Epoch, end Epoch,
	mode RangeMode) (timeseries, error) {

	if start > end {
		return nil, fmt.Errorf("start %v is after end %v", start, end)
	}

	if !isValidRangeMode(mode) {
		return nil, fmt.Errorf("invalid range mode %v", mode)
	}

	return &rangeSeries{
		series: series,
		start:  start,
		end:    end,
		mode:   mode,
	}, nil
}

func (r *rangeSeries) AtOrBefore(timestamp Epoch) (float64, bool) {
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

func (r *rangeSeries) Iterator() utils.Iterator[Entry] {
	return r.series.rangeIterator(r.start, r.end, r.mode)
}

func (r *rangeSeries) Range(start Epoch, end Epoch, mode RangeMode) timeseries {
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

func NewAggSeries(allSeries []timeseries, aggregator func([]float64) float64) (timeseries, error) {
	if aggregator == nil {
		return nil, fmt.Errorf("nil aggregator")
	}

	return &aggSeries{
		allSeries:  allSeries,
		aggregator: aggregator,
	}, nil
}

func (a *aggSeries) AtOrBefore(timestamp Epoch) (float64, bool) {
	if !a.isEvaluated {
		a.result = a.evaluate()
		a.isEvaluated = true
	}

	return a.result.AtOrBefore(timestamp)
}

func (a *aggSeries) Iterator() utils.Iterator[Entry] {
	if !a.isEvaluated {
		a.result = a.evaluate()
		a.isEvaluated = true
	}

	return a.result.Iterator()
}

func (a *aggSeries) Range(start Epoch, end Epoch, mode RangeMode) timeseries {
	if !a.isEvaluated {
		a.result = a.evaluate()
		a.isEvaluated = true
	}

	return a.result.Range(start, end, mode)
}

func (a *aggSeries) evaluate() timeseries {
	if len(a.allSeries) == 0 {
		return &lookupSeries{values: []Entry{}}
	}

	allIters := make([]utils.Iterator[Entry], 0, len(a.allSeries))
	for _, series := range a.allSeries {
		allIters = append(allIters, series.Iterator())
	}

	// Keep track of the current value for each iterator
	currentValues := make([]Entry, len(allIters))
	hasValue := make([]bool, len(allIters))

	// Initialize values from all iterators
	for i, iter := range allIters {
		if value, ok := iter.Next(); ok {
			currentValues[i] = value
			hasValue[i] = true
		}
	}

	result := make([]Entry, 0)

	for {
		// Find the earliest timestamp among all current values
		var minTimestamp Epoch
		minFound := false

		for i, has := range hasValue {
			if has && (!minFound || currentValues[i].Timestamp < minTimestamp) {
				minTimestamp = currentValues[i].Timestamp
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
			if has && currentValues[i].Timestamp == minTimestamp {
				valuesToAggregate = append(valuesToAggregate, currentValues[i].Value)

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
			result = append(result, Entry{Timestamp: minTimestamp, Value: aggregatedValue})
		}
	}

	return &lookupSeries{values: result}
}

type valueMappingSeries struct {
	series  timeseries
	mapping func(float64) float64
}

func NewValueMappingSeries(series timeseries, mapping func(float64) float64) (timeseries, error) {
	if mapping == nil {
		return nil, fmt.Errorf("nil mapping function")
	}

	return &valueMappingSeries{
		series:  series,
		mapping: mapping,
	}, nil
}

func (v *valueMappingSeries) AtOrBefore(timestamp Epoch) (float64, bool) {
	value, ok := v.series.AtOrBefore(timestamp)
	if !ok {
		return 0, false
	}

	return v.mapping(value), true
}

func (v *valueMappingSeries) Iterator() utils.Iterator[Entry] {
	return &valueMappingIterator{
		series: v.series.Iterator(),
		mapper: v.mapping,
	}
}

type valueMappingIterator struct {
	series utils.Iterator[Entry]
	mapper func(float64) float64
}

func (v *valueMappingIterator) Next() (Entry, bool) {
	value, ok := v.series.Next()
	if !ok {
		return Entry{}, false
	}

	return Entry{Timestamp: value.Timestamp, Value: v.mapper(value.Value)}, true
}

func (v *valueMappingSeries) Range(start Epoch, end Epoch, mode RangeMode) timeseries {
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
	windowSize Epoch
	stepSize   Epoch // How far to slide the window each time
	endTime    Epoch // The final window ends here (inclusive)
	mode       RangeMode

	isEvaluated bool
	result      timeseries
}

func NewWindowMappingSeries(timeseries timeseries, aggregator func([]float64) float64,
	windowSize Epoch, stepSize Epoch, endTime Epoch, mode RangeMode) (timeseries, error) {

	if aggregator == nil {
		return nil, fmt.Errorf("nil aggregator")
	}
	if windowSize <= 0 {
		return nil, fmt.Errorf("non-positive window size %v", windowSize)
	}
	if stepSize <= 0 {
		return nil, fmt.Errorf("non-positive step size %v", stepSize)
	}
	if !isValidRangeMode(mode) {
		return nil, fmt.Errorf("invalid mode %v", mode)
	}

	return &windowMappingSeries{
		timeseries: timeseries,
		aggregator: aggregator,
		windowSize: windowSize,
		stepSize:   stepSize,
		endTime:    endTime,
		mode:       mode,
	}, nil
}

func (w *windowMappingSeries) AtOrBefore(timestamp Epoch) (float64, bool) {
	if !w.isEvaluated {
		w.result = w.evaluate()
		w.isEvaluated = true
	}

	return w.result.AtOrBefore(timestamp)
}

func (w *windowMappingSeries) Iterator() utils.Iterator[Entry] {
	if !w.isEvaluated {
		w.result = w.evaluate()
		w.isEvaluated = true
	}

	return w.result.Iterator()
}

func (w *windowMappingSeries) Range(start Epoch, end Epoch, mode RangeMode) timeseries {
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
	allValues := make([]Entry, 0)

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

	finalValues := make([]Entry, 0)

	switch w.mode {
	case PromQl3Range:
		startTimestamp := allValues[0].Timestamp
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

				valuesInWindow = append(valuesInWindow, point.Value)
			}

			if len(valuesInWindow) > 0 {
				aggregatedValue := w.aggregator(valuesInWindow)
				finalValues = append(finalValues, Entry{
					Timestamp: windowEnd,
					Value:     aggregatedValue,
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
	interval   Epoch
}

func (d *downsampler) Evaluate() timeseries {
	iterator := d.timeseries.Iterator()

	firstEntry, ok := iterator.Next()
	if !ok {
		return &lookupSeries{}
	}

	currentBucket := d.snapToInterval(firstEntry.Timestamp)
	currentValues := []float64{firstEntry.Value}

	finalEntries := make([]Entry, 0)

	for {
		firstEntry, ok = iterator.Next()
		if !ok {
			break
		}

		thisBucket := d.snapToInterval(firstEntry.Timestamp)
		if thisBucket == currentBucket {
			currentValues = append(currentValues, firstEntry.Value)
			continue
		}

		// Close the current bucket.
		if len(currentValues) > 0 {
			value := d.aggregator(currentValues)
			finalEntries = append(finalEntries, Entry{Timestamp: currentBucket, Value: value})
		}

		currentBucket = d.snapToInterval(firstEntry.Timestamp)
		currentValues = []float64{firstEntry.Value}
	}

	// Close the last bucket.
	if len(currentValues) > 0 {
		value := d.aggregator(currentValues)
		finalEntries = append(finalEntries, Entry{Timestamp: currentBucket, Value: value})
	}

	return &lookupSeries{values: finalEntries}
}

func (d *downsampler) snapToInterval(timestamp Epoch) Epoch {
	return timestamp - timestamp%d.interval
}
