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
	"testing"

	"github.com/siglens/siglens/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func Test_implementsSeries(t *testing.T) {
	var _ timeseries = &lookupSeries{}
	var _ timeseries = &generatedSeries{}
	var _ timeseries = &rangeSeries{}
}

func Test_lookupSeries(t *testing.T) {
	series := &lookupSeries{
		values: []Entry{
			{Timestamp: 1, Value: 101},
			{Timestamp: 2, Value: 102},
		},
	}

	t.Run("AtOrBefore", func(t *testing.T) {
		assertAtOrBefore(t, series, 0, 0.0, false)
		assertAtOrBefore(t, series, 1, 101.0, true)
		assertAtOrBefore(t, series, 2, 102.0, true)
		assertAtOrBefore(t, series, 100, 102.0, true)
	})

	t.Run("Iterator", func(t *testing.T) {
		assertEqualIterators(t, utils.NewIterator([]Entry{
			{Timestamp: 1, Value: 101},
			{Timestamp: 2, Value: 102},
		}), series.Iterator())
	})
}

func Test_generatedSeries(t *testing.T) {
	series := &generatedSeries{
		timestamps: []Epoch{1, 2},
		valueAt: func(timestamp Epoch) float64 {
			return float64(timestamp) + 100
		},
	}

	t.Run("AtOrBefore", func(t *testing.T) {
		assertAtOrBefore(t, series, 0, 0.0, false)
		assertAtOrBefore(t, series, 1, 101.0, true)
		assertAtOrBefore(t, series, 2, 102.0, true)
		assertAtOrBefore(t, series, 3, 103.0, true)
	})

	t.Run("Iterator", func(t *testing.T) {
		assertEqualIterators(t, utils.NewIterator([]Entry{
			{Timestamp: 1, Value: 101},
			{Timestamp: 2, Value: 102},
		}), series.Iterator())
	})
}

func Test_generatedSeries_empty(t *testing.T) {
	series := &generatedSeries{
		timestamps: []Epoch{},
		valueAt: func(timestamp Epoch) float64 {
			return float64(timestamp) + 100
		},
	}

	t.Run("AtOrBefore", func(t *testing.T) {
		assertAtOrBefore(t, series, 0, 0.0, false)
	})

	t.Run("Iterator", func(t *testing.T) {
		iter := series.Iterator()

		_, ok := iter.Next()
		assert.False(t, ok)
	})
}

func Test_rangeSeries(t *testing.T) {
	lookupSeries := &lookupSeries{
		values: []Entry{
			{Timestamp: 1, Value: 101},
			{Timestamp: 2, Value: 102},
			{Timestamp: 3, Value: 103},
		},
	}

	t.Run("AtOrBefore", func(t *testing.T) {
		rangeSeries := lookupSeries.Range(1, 3, PromQl3Range)
		assertAtOrBefore(t, rangeSeries, 0, 0.0, false)
		assertAtOrBefore(t, rangeSeries, 1, 0.0, false)
		assertAtOrBefore(t, rangeSeries, 2, 102.0, true)
		assertAtOrBefore(t, rangeSeries, 3, 103.0, true)

		// Test outside the range.
		assertAtOrBefore(t, lookupSeries, 4, 103.0, true)
		assertAtOrBefore(t, rangeSeries, 4, 0.0, false)
	})

	t.Run("Iterator", func(t *testing.T) {
		assertEqualIterators(t, utils.NewIterator([]Entry{
			{Timestamp: 2, Value: 102},
			{Timestamp: 3, Value: 103},
		}), lookupSeries.Range(1, 3, PromQl3Range).Iterator())

		assertEqualIterators(t, utils.NewIterator([]Entry{
			{Timestamp: 2, Value: 102},
		}), lookupSeries.Range(1, 3, PromQl3Range).Range(0, 2, PromQl3Range).Iterator())

		assertEqualIterators(t, utils.NewIterator([]Entry{}),
			lookupSeries.Range(1, 1, PromQl3Range).Iterator(),
		)

		assertEqualIterators(t, utils.NewIterator([]Entry{}),
			lookupSeries.Range(5, 1, PromQl3Range).Iterator(),
		)

		assertEqualIterators(t, utils.NewIterator([]Entry{}),
			lookupSeries.Range(2, 3, PromQl3Range).Range(0, 1, PromQl3Range).Iterator(),
		)
	})
}

func Test_aggSeries(t *testing.T) {
	series1 := &lookupSeries{
		values: []Entry{
			{Timestamp: 1, Value: 101},
			{Timestamp: 2, Value: 102},
			{Timestamp: 4, Value: 104},
		},
	}

	series2 := &lookupSeries{
		values: []Entry{
			{Timestamp: 2, Value: 202},
			{Timestamp: 3, Value: 203},
			{Timestamp: 4, Value: 204},
			{Timestamp: 5, Value: 205},
		},
	}

	result := &aggSeries{
		allSeries: []timeseries{series1, series2},
		aggregator: func(values []float64) float64 {
			sum := 0.0
			for _, v := range values {
				sum += v
			}
			return sum
		},
	}

	t.Run("AtOrBefore", func(t *testing.T) {
		assertAtOrBefore(t, result, 0, 0.0, false)
		assertAtOrBefore(t, result, 1, 101.0, true)
		assertAtOrBefore(t, result, 2, 304.0, true)
		assertAtOrBefore(t, result, 3, 203.0, true)
		assertAtOrBefore(t, result, 4, 308.0, true)
		assertAtOrBefore(t, result, 5, 205.0, true)
		assertAtOrBefore(t, result, 100, 205.0, true)
	})

	t.Run("Iterator", func(t *testing.T) {
		assertEqualIterators(t, utils.NewIterator([]Entry{
			{Timestamp: 1, Value: 101},
			{Timestamp: 2, Value: 304},
			{Timestamp: 3, Value: 203},
			{Timestamp: 4, Value: 308},
			{Timestamp: 5, Value: 205},
		}), result.Iterator())
	})
}

func Test_valueMappingSeries(t *testing.T) {
	series := &lookupSeries{
		values: []Entry{
			{Timestamp: 1, Value: 3},
			{Timestamp: 2, Value: 4},
		},
	}

	mapping := func(value float64) float64 {
		return value * value
	}

	mappedSeries := &valueMappingSeries{
		series:  series,
		mapping: mapping,
	}

	t.Run("AtOrBefore", func(t *testing.T) {
		assertAtOrBefore(t, mappedSeries, 0, 0.0, false)
		assertAtOrBefore(t, mappedSeries, 1, 9.0, true)
		assertAtOrBefore(t, mappedSeries, 2, 16.0, true)
		assertAtOrBefore(t, mappedSeries, 100, 16.0, true)
	})

	t.Run("Iterator", func(t *testing.T) {
		assertEqualIterators(t, utils.NewIterator([]Entry{
			{Timestamp: 1, Value: 9},
			{Timestamp: 2, Value: 16},
		}), mappedSeries.Iterator())
	})
}

func Test_Downsample(t *testing.T) {
	series := &lookupSeries{
		values: []Entry{
			{Timestamp: 1, Value: 101},
			{Timestamp: 2, Value: 102},
			{Timestamp: 30, Value: 130},
		},
	}

	downsampler := &downsampler{
		timeseries: series,
		aggregator: func(values []float64) float64 {
			sum := 0.0
			for _, v := range values {
				sum += v
			}
			return sum / float64(len(values))
		},
		interval: 10,
	}

	downsampled := downsampler.Evaluate()
	t.Run("AtOrBefore", func(t *testing.T) {
		assertAtOrBefore(t, downsampled, 0, 101.5, true)
		assertAtOrBefore(t, downsampled, 10, 101.5, true)
		assertAtOrBefore(t, downsampled, 20, 101.5, true)
		assertAtOrBefore(t, downsampled, 30, 130.0, true)
		assertAtOrBefore(t, downsampled, 100, 130.0, true)
	})

	t.Run("Iterator", func(t *testing.T) {
		assertEqualIterators(t, utils.NewIterator([]Entry{
			{Timestamp: 0, Value: 101.5},
			{Timestamp: 30, Value: 130.0},
		}), downsampled.Iterator())
	})
}

func Test_WindowMappingSeries_SmallStep(t *testing.T) {
	baseSeries := &lookupSeries{
		values: []Entry{
			{Timestamp: 1, Value: 101},
			{Timestamp: 2, Value: 102},
			{Timestamp: 3, Value: 103},
			{Timestamp: 4, Value: 104},
			{Timestamp: 5, Value: 105},
			{Timestamp: 6, Value: 106},
		},
	}

	series := &windowMappingSeries{
		timeseries: baseSeries,
		aggregator: avg,
		windowSize: 3,
		stepSize:   2,
		endTime:    7,
		mode:       PromQl3Range,
	}

	t.Run("AtOrBefore", func(t *testing.T) {
		assertAtOrBefore(t, series, 0, 0.0, false)
		assertAtOrBefore(t, series, 1, 101.0, true)
		assertAtOrBefore(t, series, 2, 101.0, true)
		assertAtOrBefore(t, series, 3, 102.0, true)
		assertAtOrBefore(t, series, 4, 102.0, true)
		assertAtOrBefore(t, series, 5, 104.0, true)
		assertAtOrBefore(t, series, 6, 104.0, true)
		assertAtOrBefore(t, series, 7, 105.5, true)
		assertAtOrBefore(t, series, 100, 105.5, true)
	})

	t.Run("Iterator", func(t *testing.T) {
		assertEqualIterators(t, utils.NewIterator([]Entry{
			{Timestamp: 1, Value: 101},   // (101 + 102) / 2
			{Timestamp: 3, Value: 102.0}, // (101 + 102 + 103) / 3
			{Timestamp: 5, Value: 104.0}, // (103 + 104 + 105) / 3
			{Timestamp: 7, Value: 105.5}, // (105 + 106) / 2
		}), series.Iterator())
	})
}

func Test_WindowMappingSeries_LargeStep(t *testing.T) {
	baseSeries := &lookupSeries{
		values: []Entry{
			{Timestamp: 1, Value: 101},
			{Timestamp: 2, Value: 102},
			{Timestamp: 3, Value: 103},
			{Timestamp: 4, Value: 104},
			{Timestamp: 5, Value: 105},
			{Timestamp: 6, Value: 106},
		},
	}

	series := &windowMappingSeries{
		timeseries: baseSeries,
		aggregator: avg,
		windowSize: 3,
		stepSize:   4,
		endTime:    7,
		mode:       PromQl3Range,
	}

	t.Run("AtOrBefore", func(t *testing.T) {
		assertAtOrBefore(t, series, 0, 0.0, false)
		assertAtOrBefore(t, series, 1, 0.0, false)
		assertAtOrBefore(t, series, 2, 0.0, false)
		assertAtOrBefore(t, series, 3, 102.0, true)
		assertAtOrBefore(t, series, 4, 102.0, true)
		assertAtOrBefore(t, series, 5, 102.0, true)
		assertAtOrBefore(t, series, 6, 102.0, true)
		assertAtOrBefore(t, series, 7, 105.5, true)
		assertAtOrBefore(t, series, 100, 105.5, true)
	})

	t.Run("Iterator", func(t *testing.T) {
		assertEqualIterators(t, utils.NewIterator([]Entry{
			{Timestamp: 3, Value: 102.0}, // (101 + 102 + 103) / 3
			{Timestamp: 7, Value: 105.5}, // (105 + 106) / 2
		}), series.Iterator())
	})
}

func assertAtOrBefore(t *testing.T, series timeseries, timestamp Epoch, expectedValue float64, expectedOk bool) {
	t.Helper()

	value, ok := series.AtOrBefore(timestamp)
	assert.Equal(t, expectedOk, ok)
	assert.Equal(t, expectedValue, value)
}

func assertEqualIterators[T any](t *testing.T, expected utils.Iterator[T], actual utils.Iterator[T]) {
	t.Helper()

	for {
		expectedValue, expectedOk := expected.Next()
		actualValue, actualOk := actual.Next()

		assert.Equal(t, expectedOk, actualOk)
		if !expectedOk {
			break
		}

		assert.Equal(t, expectedValue, actualValue)
	}
}

func avg(values []float64) float64 {
	sum := 0.0
	for _, v := range values {
		sum += v
	}

	return sum / float64(len(values))
}
