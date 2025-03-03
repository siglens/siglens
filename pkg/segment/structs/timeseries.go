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

package structs

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

type normalTimeseries struct {
	values []entry
}

func (t *normalTimeseries) AtOrBefore(timestamp epoch) (float64, bool) {
	i := sort.Search(len(t.values), func(k int) bool {
		return t.values[k].timestamp > timestamp
	})

	if i > 0 {
		return t.values[i-1].value, true
	}

	return 0, false
}

func (t *normalTimeseries) Iterator() utils.Iterator[entry] {
	return utils.NewIterator(t.values)
}

func (t *normalTimeseries) Range(start epoch, end epoch, mode RangeMode) timeseries {
	return &rangeSeries{
		series: t,
		start:  start,
		end:    end,
		mode:   mode,
	}
}

func (t *normalTimeseries) rangeIterator(start epoch, end epoch, mode RangeMode) utils.Iterator[entry] {
	switch mode {
	case PromQl3Range:
		startIndex := sort.Search(len(t.values), func(i int) bool {
			return t.values[i].timestamp > start
		})
		endIndex := sort.Search(len(t.values), func(i int) bool {
			return t.values[i].timestamp > end
		})
		return utils.NewIterator(t.values[startIndex:endIndex])
	}

	return utils.NewIterator([]entry{})
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

type timeBasedSeries struct {
	timestamps []epoch
	valueAt    func(epoch) float64
}

func (t *timeBasedSeries) AtOrBefore(timestamp epoch) (float64, bool) {
	if len(t.timestamps) == 0 || timestamp < t.timestamps[0] {
		return 0, false
	}

	return t.valueAt(timestamp), true
}

func (t *timeBasedSeries) Iterator() utils.Iterator[entry] {
	return &timeBasedIterator{
		series: t,
		index:  0,
	}
}

type timeBasedIterator struct {
	series *timeBasedSeries
	index  int
}

func (t *timeBasedIterator) Next() (entry, bool) {
	if t.index >= len(t.series.timestamps) {
		return entry{}, false
	}

	value := entry{
		timestamp: t.series.timestamps[t.index],
		value:     t.series.valueAt(t.series.timestamps[t.index]),
	}

	t.index++

	return value, true
}

func (t *timeBasedSeries) Range(startExclusive, endInclusive epoch) timeseries {
	startIndex := sort.Search(len(t.timestamps), func(i int) bool {
		return t.timestamps[i] > startExclusive
	})
	endIndex := sort.Search(len(t.timestamps), func(i int) bool {
		return t.timestamps[i] > endInclusive
	})

	values := make([]entry, 0)
	for i := startIndex; i < endIndex; i++ {
		values = append(values, entry{timestamp: t.timestamps[i], value: t.valueAt(t.timestamps[i])})
	}

	return &normalTimeseries{values: values}
}

type downsampler struct {
	timeseries timeseries
	aggregator func([]float64) float64
	interval   epoch
}

func (d *downsampler) Evaluate() timeseries {
	// TODO
	return nil
}

func (d *downsampler) snapToInterval(timestamp epoch) epoch {
	return timestamp - timestamp%d.interval
}
