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

package mresults

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_histogramQuantile(t *testing.T) {
	t.Run("badQuantile", func(t *testing.T) {
		value, err := histogramQuantile(-0.1, []histogramBin{})
		assert.NoError(t, err)
		assert.True(t, math.IsInf(value, -1), "expected -Inf; got %f", value)

		value, err = histogramQuantile(1.1, []histogramBin{})
		assert.NoError(t, err)
		assert.True(t, math.IsInf(value, 1), "expected Inf; got %f", value)

		value, err = histogramQuantile(math.NaN(), []histogramBin{})
		assert.NoError(t, err)
		assert.True(t, math.IsNaN(value), "expected NaN; got %f", value)
	})

	t.Run("insufficientData", func(t *testing.T) {
		bins := []histogramBin{}
		value, err := histogramQuantile(0.5, bins)
		assert.NoError(t, err)
		assert.True(t, math.IsNaN(value), "expected NaN; got %f", value)

		bins = []histogramBin{{upperBound: 1, count: 1}}
		value, err = histogramQuantile(0.5, bins)
		assert.NoError(t, err)
		assert.True(t, math.IsNaN(value), "expected NaN; got %f", value)

		bins = []histogramBin{{upperBound: 1, count: 0}, {upperBound: math.Inf(1), count: 0}}
		value, err = histogramQuantile(0.5, bins)
		assert.NoError(t, err)
		assert.True(t, math.IsNaN(value), "expected NaN; got %f", value)
	})

	t.Run("badHighestBin", func(t *testing.T) {
		bins := []histogramBin{{upperBound: 1, count: 1}, {upperBound: 10, count: 1}}
		value, err := histogramQuantile(0.5, bins)
		assert.NoError(t, err)
		assert.True(t, math.IsNaN(value), "expected NaN; got %f", value)
	})

	t.Run("monotonicIncreasing", func(t *testing.T) {
		bins := []histogramBin{{upperBound: 1, count: 4}, {upperBound: math.Inf(1), count: 3}}
		_, err := histogramQuantile(0.5, bins)
		assert.Error(t, err)

		// There should be a little room for floating point errors.
		bins = []histogramBin{{upperBound: 1, count: 2e12}, {upperBound: math.Inf(1), count: 2e12 - 1}}
		_, err = histogramQuantile(0.5, bins)
		assert.NoError(t, err)
	})

	t.Run("quantileInMiddle", func(t *testing.T) {
		bins := []histogramBin{
			{upperBound: 10, count: 250},
			{upperBound: 20, count: 500},
			{upperBound: 30, count: 750},
			{upperBound: math.Inf(1), count: 1000},
		}

		value, err := histogramQuantile(0.5, bins)
		assert.NoError(t, err)
		assert.Equal(t, 20.0, value)

		value, err = histogramQuantile(0.6, bins)
		assert.NoError(t, err)
		assert.Equal(t, 24.0, value) // 50th is 20; 75th is 30; so 60th is (20 + (2/5) * 10) = 24
	})
}
