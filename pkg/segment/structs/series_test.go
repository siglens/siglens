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
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_implementsSeries(t *testing.T) {
	var _ timeseries = &normalTimeseries{}
}

func Test_normalTimeseries(t *testing.T) {
	series := &normalTimeseries{
		values: []entry{
			{timestamp: 1, value: 101},
			{timestamp: 2, value: 102},
		},
	}

	t.Run("GetTimestamps", func(t *testing.T) {
		timestamps := series.GetTimestamps()
		assert.Equal(t, []epoch{1, 2}, timestamps)
	})

	t.Run("AtOrBefore", func(t *testing.T) {
		value, ok := series.AtOrBefore(0)
		assert.False(t, ok)

		value, ok = series.AtOrBefore(1)
		assert.True(t, ok)
		assert.Equal(t, 101.0, value)

		value, ok = series.AtOrBefore(2)
		assert.True(t, ok)
		assert.Equal(t, 102.0, value)

		value, ok = series.AtOrBefore(100)
		assert.True(t, ok)
		assert.Equal(t, 102.0, value)
	})
}

func Test_windowedTimeseries(t *testing.T) {
	innerSeries := &normalTimeseries{
		values: []entry{
			{timestamp: 1, value: 101},
			{timestamp: 2, value: 102},
			{timestamp: 3, value: 103},
		},
	}

	series := RestrictRange(innerSeries, 1, 2)
	_, ok := series.(*windowedTimeseries)
	assert.True(t, ok)

	t.Run("GetTimestamps", func(t *testing.T) {
		timestamps := series.GetTimestamps()
		assert.Equal(t, []epoch{2}, timestamps)
	})

	t.Run("AtOrBefore", func(t *testing.T) {
		value, ok := series.AtOrBefore(0)
		assert.False(t, ok)

		value, ok = series.AtOrBefore(1)
		assert.False(t, ok)

		value, ok = series.AtOrBefore(2)
		assert.True(t, ok)
		assert.Equal(t, 102.0, value)

		value, ok = series.AtOrBefore(3)
		assert.True(t, ok)
		assert.Equal(t, 102.0, value)
	})
}
