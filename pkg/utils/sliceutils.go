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
	"reflect"
	"sort"

	log "github.com/sirupsen/logrus"
)

func SliceContainsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}

	return false
}

func SliceContainsInt(slice []int, x int) bool {
	for _, v := range slice {
		if v == x {
			return true
		}
	}

	return false
}

func SelectIndicesFromSlice(slice []string, indices []int) []string {
	var result []string
	for _, v := range indices {
		if v < 0 || v >= len(slice) {
			log.Errorf("SelectIndicesFromSlice: index %d out of range for slice of length %v", v, len(slice))
			continue
		}

		result = append(result, slice[v])
	}

	return result
}

func ResizeSlice[T any](slice []T, newLength int) []T {
	slice = slice[:cap(slice)]

	if len(slice) >= newLength {
		return slice[:newLength]
	}

	return append(slice, make([]T, newLength-len(slice))...)
}

func IsArrayOrSlice(val interface{}) (bool, reflect.Value, string) {
	v := reflect.ValueOf(val)
	switch v.Kind() {
	case reflect.Array:
		return true, v, "array"
	case reflect.Slice:
		return true, v, "slice"
	default:
		return false, v, "neither"
	}
}

func CompareStringSlices(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func ConvertSliceToMap[K comparable, V any](slice []V, keyFunc func(V) K) map[K][]V {
	result := make(map[K][]V)
	for _, v := range slice {
		key := keyFunc(v)
		result[key] = append(result[key], v)
	}

	return result
}

type orderedItems[T any] struct {
	items []T
	order []int
}

// Sometimes we want to performn an operation on each item of a slice, but for
// performance reasons, it's better to do that operation on batches of the data
// based on some property of each item. This is a util do to that.
//
// The output order is the same as the input order.
func BatchProcess[T any, K comparable, R any](slice []T, batchBy func(T) K,
	batchKeyLess Option[func(K, K) bool], operation func([]T) []R) []R {

	// Batch the items, but track their original order.
	batches := make(map[K]*orderedItems[T])
	for i, item := range slice {
		batchKey := batchBy(item)
		batch, ok := batches[batchKey]
		if !ok {
			batch = &orderedItems[T]{
				items: make([]T, 0),
				order: make([]int, 0),
			}

			batches[batchKey] = batch
		}

		batch.items = append(batch.items, item)
		batch.order = append(batch.order, i)
	}

	batchKeys := GetKeysOfMap(batches)
	if less, ok := batchKeyLess.Get(); ok {
		sort.Slice(batchKeys, func(i, k int) bool {
			return less(batchKeys[i], batchKeys[k])
		})
	}

	results := make([]R, len(slice))
	for _, key := range batchKeys {
		batch := batches[key]
		batchResults := operation(batch.items)
		for i, result := range batchResults {
			results[batch.order[i]] = result
		}
	}

	return results
}

type sortable[T any] struct {
	items []T
	order []int
	less  func(T, T) bool
}

func newSortable[T any](items []T, less func(T, T) bool) sortable[T] {
	order := make([]int, len(items))
	for i := range items {
		order[i] = i
	}

	return sortable[T]{items, order, less}
}

func (s sortable[T]) Len() int {
	return len(s.items)
}

func (s sortable[T]) Less(i, j int) bool {
	return s.less(s.items[i], s.items[j])
}

func (s sortable[T]) Swap(i, j int) {
	s.items[i], s.items[j] = s.items[j], s.items[i]
	s.order[i], s.order[j] = s.order[j], s.order[i]
}

func SortThenProcessThenUnsort[T any, R any](slice []T, less func(T, T) bool, operation func([]T) []R) []R {
	sortableItems := newSortable(slice, less)
	sort.Sort(sortableItems)
	sortedResults := operation(sortableItems.items)

	// Now unsort to get the original order.
	results := make([]R, len(slice))
	for i, result := range sortedResults {
		results[sortableItems.order[i]] = result
	}

	return results
}

// idxsToRemove should contain only valid indexes in the array
func RemoveElements[T any, T2 any](arr []T, idxsToRemove map[int]T2) []T {

	newArr := make([]T, 0)
	for idx, element := range arr {
		_, exists := idxsToRemove[idx]
		if !exists {
			newArr = append(newArr, element)
		}
	}

	return newArr
}
