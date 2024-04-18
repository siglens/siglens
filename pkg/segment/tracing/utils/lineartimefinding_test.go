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
	"testing"
)

func TestFindPercentileData(t *testing.T) {
	testCases := []struct {
		arr        []uint64
		percentile int
		expected   uint64
	}{
		{
			arr:        []uint64{44, 11, 22},
			percentile: 66,
			expected:   22,
		},
		{
			arr:        []uint64{44, 11, 22},
			percentile: 67,
			expected:   44,
		},
		{
			arr:        []uint64{20, 50, 40, 30, 10},
			percentile: 35,
			expected:   20,
		},
		{
			arr:        []uint64{20, 50, 40, 30, 10},
			percentile: 95,
			expected:   50,
		},
	}

	for _, tc := range testCases {
		result := FindPercentileData(tc.arr, tc.percentile)
		if result != tc.expected {
			t.Errorf("Expected %d percentile to be %d, but got %d", tc.percentile, tc.expected, result)
		}
	}
}
