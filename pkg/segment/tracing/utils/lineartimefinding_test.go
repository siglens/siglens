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

package utils

import (
	"math"
	"testing"
)

func TestFindPercentileData_uint64(t *testing.T) {
	testCases := []struct {
		arr        []uint64
		percentile int
		expected   float64
	}{
		{
			arr:        []uint64{44, 11, 22},
			percentile: 66,
			expected:   29.04,
		},
		{
			arr:        []uint64{44, 11, 22},
			percentile: 50,
			expected:   22,
		},
		{
			arr:        []uint64{20, 50, 40, 30, 10},
			percentile: 35,
			expected:   24,
		},
		{
			arr:        []uint64{20, 50, 40, 30, 10},
			percentile: 95,
			expected:   48,
		},
		{
			arr:        []uint64{25, 75, 0, 50, 100},
			percentile: 25,
			expected:   25,
		},
	}

	precision := 8
	scale := math.Pow10(precision)

	for _, tc := range testCases {
		result := FindPercentileData(tc.arr, tc.percentile)
		result = math.Round(result*scale) / scale
		if result != tc.expected {
			t.Errorf("Expected %d percentile to be %f, but got %f", tc.percentile, tc.expected, result)
		}
	}
}

func TestFindPercentileData_float64(t *testing.T) {
	testCases := []struct {
		arr        []float64
		percentile int
		expected   float64
	}{
		{
			arr:        []float64{9.1, 6.8, 4.7, 7.5, 8.7},
			percentile: 0,
			expected:   4.7,
		},
		{
			arr:        []float64{-1.7, -9.5, 9.1, -3.2, -0.9},
			percentile: 50,
			expected:   -1.7,
		},
		{
			arr:        []float64{8.8, 1.5, 6.9, 5.4, 9.4, 5.4},
			percentile: 50,
			expected:   6.15,
		},
		{
			arr:        []float64{8.8, 1.5, 6.9, 5.4, 9.4, 5.4},
			percentile: 30,
			expected:   5.4,
		},
		{
			arr:        []float64{-30.6, -63.4, -75.0, -57.1, 68.5, 38.9},
			percentile: 61,
			expected:   -27.125,
		},
	}

	precision := 8
	scale := math.Pow10(precision)

	for _, tc := range testCases {
		result := FindPercentileData(tc.arr, tc.percentile)
		result = math.Round(result*scale) / scale
		if result != tc.expected {
			t.Errorf("Expected %d percentile to be %f, but got %f", tc.percentile, tc.expected, result)
		}
	}
}
