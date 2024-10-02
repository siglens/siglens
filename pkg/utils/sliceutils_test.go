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

	"github.com/stretchr/testify/assert"
)

func Test_ResizeSlice(t *testing.T) {
	originalSlice := []int{1, 2, 3, 4, 5}

	newSlice := ResizeSlice(originalSlice, 3)
	assert.Len(t, newSlice, 3)
	assert.Equal(t, newSlice, []int{1, 2, 3})

	newSlice = ResizeSlice(originalSlice, 10)
	assert.Len(t, newSlice, 10)
	assert.Equal(t, newSlice[:5], originalSlice)
}

func Test_ConvertSliceToMap_EmptySlice(t *testing.T) {
	emptySlice := []string{}
	result := ConvertSliceToMap(emptySlice, func(s string) string {
		return s
	})

	assert.Len(t, result, 0)
}

func Test_ConvertSliceToMap(t *testing.T) {
	slice := []string{"a", "b", "c", "d"}
	result := ConvertSliceToMap(slice, func(s string) string {
		return s
	})

	assert.Len(t, result, 4)
	assert.Equal(t, result["a"], []string{"a"})
	assert.Equal(t, result["b"], []string{"b"})
	assert.Equal(t, result["c"], []string{"c"})
	assert.Equal(t, result["d"], []string{"d"})
}

func Test_ConvertSliceToMapWithTransform(t *testing.T) {
	slice := []int{1, 2, 3, 20, 42, 100, 47}
	result := ConvertSliceToMap(slice, func(i int) int {
		return i / 10
	})

	assert.Len(t, result, 4)
	assert.Equal(t, result[0], []int{1, 2, 3})
	assert.Equal(t, result[2], []int{20})
	assert.Equal(t, result[4], []int{42, 47})
	assert.Equal(t, result[10], []int{100})
}

func Test_BatchProcess(t *testing.T) {
	batchingFunc := func(x int) int {
		return x / 10
	}
	operation := func(slice []int) []int {
		result := make([]int, 0, len(slice))
		for _, i := range slice {
			result = append(result, i+len(slice))
		}

		return result
	}

	input := []int{1, 2, 3, 20, 42, 100, 47}
	expected := []int{4, 5, 6, 21, 44, 101, 49}
	actual := BatchProcess(input, batchingFunc, NewUnsetOption[func(int, int) bool](), operation)
	assert.Equal(t, expected, actual)
}

func Test_BatchProcess_BatchOrderMatters(t *testing.T) {
	batchingFunc := func(x int) int {
		return x / 10
	}
	batchOrderingFunc := NewOptionWithValue(func(a, b int) bool {
		return a > b
	})
	actualBatchSizes := make([]int, 0)
	operation := func(slice []int) []int {
		result := make([]int, 0, len(slice))
		for _, i := range slice {
			result = append(result, i+len(slice))
		}

		actualBatchSizes = append(actualBatchSizes, len(slice))

		return result
	}

	input := []int{1, 2, 3, 20, 42, 100, 47}
	expected := []int{4, 5, 6, 21, 44, 101, 49}
	expectedBatchSizes := []int{1, 2, 1, 3} // Batches should be 100s, 40s, 20s, 0s
	actual := BatchProcess(input, batchingFunc, batchOrderingFunc, operation)
	assert.Equal(t, expected, actual)
	assert.Equal(t, expectedBatchSizes, actualBatchSizes)
}

func Test_SortThenProcessThenUnsort(t *testing.T) {
	slice := []int{1, 2, 3, 20, 42, 100, 47}
	less := func(a, b int) bool {
		return a < b
	}
	actualReceivedOrder := make([]int, 0)
	operation := func(slice []int) []int {
		result := make([]int, 0, len(slice))
		for _, i := range slice {
			result = append(result, i+10)
			actualReceivedOrder = append(actualReceivedOrder, i)
		}

		return result
	}

	expected := []int{11, 12, 13, 30, 52, 110, 57}
	expectedReceivedOrder := []int{1, 2, 3, 20, 42, 47, 100}
	actual := SortThenProcessThenUnsort(slice, less, operation)
	assert.Equal(t, expected, actual)
	assert.Equal(t, expectedReceivedOrder, actualReceivedOrder)
}

func Test_RemoveElements(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5}
	idxsToRemove := map[int]struct{}{
		1: {},
		3: {},
	}

	newSlice := RemoveElements(slice, idxsToRemove)
	assert.Len(t, newSlice, 3)
	assert.Equal(t, newSlice, []int{1, 3, 5})

	newSlice = RemoveElements(newSlice, idxsToRemove)
	assert.Len(t, newSlice, 2)
	assert.Equal(t, newSlice, []int{1, 5})
}
