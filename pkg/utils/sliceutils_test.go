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
	"sync"
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

func Test_ResizeSliceWithDefault(t *testing.T) {
	originalSlice := []int{1, 2, 3, 4, 5}

	newSlice := ResizeSliceWithDefault(originalSlice, 3, 42)
	assert.Len(t, newSlice, 3)
	assert.Equal(t, newSlice, []int{1, 2, 3})

	newSlice = ResizeSliceWithDefault(originalSlice, 10, 42)
	assert.Len(t, newSlice, 10)
	assert.Equal(t, newSlice[:5], originalSlice)
	assert.Equal(t, newSlice[5:], []int{42, 42, 42, 42, 42})
}

func Test_GrowSliceInChunks(t *testing.T) {
	slice, err := GrowSliceInChunks(make([]int, 10), 5, 5)
	assert.NoError(t, err)
	assert.Len(t, slice, 10)

	slice, err = GrowSliceInChunks(make([]int, 10), 15, 5)
	assert.NoError(t, err)
	assert.Len(t, slice, 15)

	slice, err = GrowSliceInChunks(make([]int, 10), 15, 3)
	assert.NoError(t, err)
	assert.Len(t, slice, 16)

	_, err = GrowSliceInChunks(make([]int, 10), 15, -1)
	assert.Error(t, err)

	_, err = GrowSliceInChunks(make([]int, 10), -1, 3)
	assert.Error(t, err)
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
	batchOrderingFunc := NewOptionWithValue(func(a, b int) bool {
		return a > b
	})
	actualBatchSizes := make([]int, 0)
	operation := func(slice []int) ([]int, error) {
		result := make([]int, 0, len(slice))
		for _, i := range slice {
			result = append(result, i+len(slice))
		}

		actualBatchSizes = append(actualBatchSizes, len(slice))

		return result, nil
	}

	input := []int{1, 2, 3, 20, 42, 100, 47}
	expected := []int{4, 5, 6, 21, 44, 101, 49}
	expectedBatchSizes := []int{1, 2, 1, 3} // Batches should be 100s, 40s, 20s, 0s
	actual, err := BatchProcess(input, batchingFunc, batchOrderingFunc, operation, 1)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
	assert.Equal(t, expectedBatchSizes, actualBatchSizes)
}

func Test_BatchProcessToMap(t *testing.T) {
	batchingFunc := func(x int) int {
		return x / 10
	}
	batchOrderingFunc := NewOptionWithValue(func(a, b int) bool {
		return a > b
	})
	actualBatchSizes := make([]int, 0)
	operation := func(slice []int) map[string][]int {
		result := make(map[string][]int)
		for _, i := range slice {
			result["normal"] = append(result["normal"], i)
			result["double"] = append(result["double"], i*2)
		}

		actualBatchSizes = append(actualBatchSizes, len(slice))

		return result
	}

	input := []int{1, 2, 3, 20, 42, 100, 47}
	expected := map[string][]int{
		"normal": {1, 2, 3, 20, 42, 100, 47},
		"double": {2, 4, 6, 40, 84, 200, 94},
	}
	expectedBatchSizes := []int{1, 2, 1, 3} // Batches should be 100s, 40s, 20s, 0s
	actual := BatchProcessToMap(input, batchingFunc, batchOrderingFunc, operation)
	assert.Equal(t, expected, actual)
	assert.Equal(t, expectedBatchSizes, actualBatchSizes)
}

func Test_SortThenProcessThenUnsort(t *testing.T) {
	slice := []int{1, 2, 3, 20, 42, 100, 47}
	less := func(a, b int) bool {
		return a < b
	}
	actualReceivedOrder := make([]int, 0)
	operation := func(slice []int) ([]int, error) {
		result := make([]int, 0, len(slice))
		for _, i := range slice {
			result = append(result, i+10)
			actualReceivedOrder = append(actualReceivedOrder, i)
		}

		return result, nil
	}

	expected := []int{11, 12, 13, 30, 52, 110, 57}
	expectedReceivedOrder := []int{1, 2, 3, 20, 42, 47, 100}
	actual, err := SortThenProcessThenUnsort(slice, less, operation)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual)
	assert.Equal(t, expectedReceivedOrder, actualReceivedOrder)
}

func Test_Unsort(t *testing.T) {
	t.Run("1-cycle", func(t *testing.T) {
		sorter := sortable[string]{
			order: []int{0, 1, 2, 3, 4, 5},
		}
		actual, err := Unsort(sorter, []string{"a", "b", "c", "d", "e", "f"})
		expected := []string{"a", "b", "c", "d", "e", "f"}
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("2-cycle", func(t *testing.T) {
		sorter := sortable[string]{
			order: []int{5, 4, 3, 2, 1, 0},
		}
		actual, err := Unsort(sorter, []string{"a", "b", "c", "d", "e", "f"})
		expected := []string{"f", "e", "d", "c", "b", "a"}
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})

	t.Run("6-cycle", func(t *testing.T) {
		sorter := sortable[string]{
			order: []int{2, 0, 1, 4, 3, 5},
		}

		actual, err := Unsort(sorter, []string{"a", "b", "c", "d", "e", "f"})
		expected := []string{"b", "c", "a", "e", "d", "f"}
		assert.NoError(t, err)
		assert.Equal(t, expected, actual)
	})
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

func Test_RemoveSortedIndices_valid(t *testing.T) {
	slice := []int{3, 2, 1}
	slice, err := RemoveSortedIndices(slice, []int{0})
	assert.NoError(t, err)
	assert.Equal(t, []int{2, 1}, slice)

	slice = []int{3, 2, 1}
	slice, err = RemoveSortedIndices(slice, []int{2})
	assert.NoError(t, err)
	assert.Equal(t, []int{3, 2}, slice)

	slice = []int{5, 4, 3, 2, 1}
	slice, err = RemoveSortedIndices(slice, []int{0, 2, 3})
	assert.NoError(t, err)
	assert.Equal(t, []int{4, 1}, slice)

	slice = []int{3, 2, 1}
	slice, err = RemoveSortedIndices(slice, []int{0, 1, 2})
	assert.NoError(t, err)
	assert.Len(t, slice, 0)

	slice = []int{3, 2, 1}
	slice, err = RemoveSortedIndices(slice, []int{})
	assert.NoError(t, err)
	assert.Equal(t, []int{3, 2, 1}, slice)
}

func Test_RemoveSortedIndices_invalid(t *testing.T) {
	_, err := RemoveSortedIndices([]int{1, 2, 3}, []int{2, 0})
	assert.Error(t, err)

	_, err = RemoveSortedIndices([]int{1, 2, 3}, []int{3})
	assert.Error(t, err)

	_, err = RemoveSortedIndices([]int{1, 2, 3}, []int{-1})
	assert.Error(t, err)

	_, err = RemoveSortedIndices([]int{1, 2, 3}, []int{1, 1})
	assert.Error(t, err)
}

func Test_IndexOfMin(t *testing.T) {
	slice := []int{5, 3, 1, 4, 2}
	less := func(a, b int) bool {
		return a < b
	}

	assert.Equal(t, 2, IndexOfMin(slice, less))

	slice = []int{1, 1, 0, 0}
	index := IndexOfMin(slice, less)
	assert.True(t, index == 2 || index == 3)
}

func Test_MergeSortedSlices(t *testing.T) {
	less := func(a, b int) bool {
		return a < b
	}

	slice1 := []int{1, 3, 5, 7}
	slice2 := []int{2, 4, 6, 8}
	slice3 := []int{0, 9, 10, 11}

	expected := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	actual := MergeSortedSlices(less, slice1, slice2, slice3)
	assert.Equal(t, expected, actual)
}

func Test_MergeSortedSlices_someEmpty(t *testing.T) {
	less := func(a, b int) bool {
		return a < b
	}

	slice1 := []int{}
	slice2 := []int{2, 3, 5}
	slice3 := []int{1, 4, 6}

	expected := []int{1, 2, 3, 4, 5, 6}
	actual := MergeSortedSlices(less, slice1, slice2, slice3)
	assert.Equal(t, expected, actual)
}

func Test_MergeSortedSlices_allEmpty(t *testing.T) {
	less := func(a, b int) bool {
		return a < b
	}

	slice1 := []int{}
	slice2 := []int{}
	slice3 := []int{}

	expected := []int{}
	actual := MergeSortedSlices(less, slice1, slice2, slice3)
	assert.Equal(t, expected, actual)
}

func Test_SelectFromSlice(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5}
	evens := SelectFromSlice(slice, func(i int) bool {
		return i%2 == 0
	})

	assert.Equal(t, []int{2, 4}, evens)
}

func Test_ReverseSlice(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5}
	ReverseSlice(slice)
	assert.Equal(t, []int{5, 4, 3, 2, 1}, slice)

	slice2 := []string{"a", "b", "c", "d"}
	ReverseSlice(slice2)
	assert.Equal(t, []string{"d", "c", "b", "a"}, slice2)
}

func Test_ShallowCopySlice(t *testing.T) {
	slice := []int{1, 2, 3, 4, 5}
	newSlice := ShallowCopySlice(slice)

	assert.Len(t, newSlice, len(slice))
	assert.Equal(t, []int{1, 2, 3, 4, 5}, newSlice)

	// modify original slice and make sure new one is unchanged
	slice[0] = 299
	slice[3] = 456
	assert.Equal(t, []int{1, 2, 3, 4, 5}, newSlice)

	// modify new slice and make sure original one is unchanged
	newSlice[4] = 456
	assert.Equal(t, 5, slice[4])
}

func Test_ProcessWithParallelism(t *testing.T) {
	slice := []int{1, 3, 5, 7, 9, 11, 13, 15}
	results := make([]int, 0, len(slice))
	lock := &sync.Mutex{}

	operation := func(i int) error {
		lock.Lock()
		results = append(results, i*2)
		lock.Unlock()

		return nil
	}

	err := ProcessWithParallelism(3, slice, operation)
	assert.NoError(t, err)
	assert.Len(t, results, len(slice))
	for _, original := range slice {
		assert.Contains(t, results, original*2)
	}
}

func Test_Insert(t *testing.T) {
	assert.Equal(t, []int{5, 100, 200}, Insert([]int{100, 200}, 0, 5))
	assert.Equal(t, []int{100, 5, 200}, Insert([]int{100, 200}, 1, 5))
	assert.Equal(t, []int{100, 200, 5}, Insert([]int{100, 200}, 2, 5))
}
