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
	"fmt"
	"reflect"
	"sort"
	"sync"

	log "github.com/sirupsen/logrus"
)

func SliceHas[T comparable](slice []T, item T) bool {
	for _, v := range slice {
		if v == item {
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

func ResizeSliceWithDefault[T any](slice []T, newLength int, defaultValue T) []T {
	oldLength := len(slice)

	newSlice := ResizeSlice(slice, newLength)

	for i := oldLength; i < newLength; i++ {
		newSlice[i] = defaultValue
	}

	return newSlice
}

func GrowSliceInChunks[T any](slice []T, minSize int, chunkSize int) ([]T, error) {
	if minSize < 0 {
		return nil, TeeErrorf("GrowSliceInChunks: minSize must be non-negative; found %v", minSize)
	}
	if chunkSize <= 0 {
		return nil, TeeErrorf("GrowSliceInChunks: chunkSize must be positive; found %v", chunkSize)
	}

	numChunksToAdd := (minSize - len(slice) + chunkSize - 1) / chunkSize
	if numChunksToAdd <= 0 {
		return slice, nil
	}

	return append(slice, make([]T, numChunksToAdd*chunkSize)...), nil
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

func ReverseSlice[V any](slice []V) {
	for i := 0; i < len(slice)/2; i++ {
		j := len(slice) - i - 1
		slice[i], slice[j] = slice[j], slice[i]
	}
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
	batchKeyLess Option[func(K, K) bool], operation func([]T) ([]R, error), maxParallelism int) ([]R, error) {

	if maxParallelism < 1 {
		maxParallelism = 1
	}

	// Batch the items, but track their original order.
	// First find the number of items in each batch, so we can allocate the
	// slices with the correct size.
	keyToNumItems := make(map[K]int)
	for _, item := range slice {
		keyToNumItems[batchBy(item)] += 1
	}

	// Now allocate and fill the batches.
	batches := make(map[K]*orderedItems[T])
	for batchKey, numItems := range keyToNumItems {
		batches[batchKey] = &orderedItems[T]{
			items: make([]T, 0, numItems),
			order: make([]int, 0, numItems),
		}
	}
	for i, item := range slice {
		batchKey := batchBy(item)
		batch := batches[batchKey]
		batch.items = append(batch.items, item)
		batch.order = append(batch.order, i)
	}

	batchKeys := GetKeysOfMap(batches)
	if less, ok := batchKeyLess.Get(); ok {
		sort.Slice(batchKeys, func(i, k int) bool {
			return less(batchKeys[i], batchKeys[k])
		})
	}

	wg := sync.WaitGroup{}
	var finalErr error
	results := make([]R, len(slice))
	for i, key := range batchKeys {
		wg.Add(1)
		go func(key K) {
			defer wg.Done()

			batch := batches[key]
			batchResults, err := operation(batch.items)
			if err != nil {
				finalErr = err
				return
			}
			for i, result := range batchResults {
				results[batch.order[i]] = result
			}
		}(key)

		if (i+1)%maxParallelism == 0 {
			wg.Wait()
		}
	}

	wg.Wait()

	return results, finalErr
}

// This is similar to BatchProcess, but instead of returning a slice of
// results, each batch gets processed to a map[MK][]R, and then we want to
// combine the batch results into final results. For each slice in the map, the
// order of the results is the same as the order of the input slice.
//
// T: Type of the input
// BK: Batch key type
// MK: Map key type
// R: Result type
func BatchProcessToMap[T any, BK comparable, MK comparable, R any](slice []T,
	batchBy func(T) BK, batchKeyLess Option[func(BK, BK) bool],
	operation func([]T) map[MK][]R) map[MK][]R {

	// Batch the items, but track their original order.
	batches := make(map[BK]*orderedItems[T])
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

	results := make(map[MK][]R, 0)
	for _, batchKey := range batchKeys {
		batch := batches[batchKey]
		batchResults := operation(batch.items)

		for mapKey, mapValues := range batchResults {
			if _, ok := results[mapKey]; !ok {
				results[mapKey] = make([]R, len(slice))
			}

			for i, mapValue := range mapValues {
				results[mapKey][batch.order[i]] = mapValue
			}
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

func Unsort[T, R any](sorter sortable[T], results []R) ([]R, error) {
	if len(sorter.order) != len(results) {
		return nil, fmt.Errorf("Unsort: inputs have different lenghts; %d and %d",
			len(sorter.order), len(results))
	}

	visited := make([]bool, len(results))
	for i := 0; i < len(results); i++ {
		if visited[i] {
			continue
		}

		if sorter.order[i] == i {
			visited[i] = true
			continue
		}

		// Process a cycle starting at position i
		temp := results[i] // Save the starting element
		curr := i
		for !visited[curr] {
			visited[curr] = true
			next := sorter.order[curr] // Where current element should go
			if next == i {
				results[curr] = temp // Complete the cycle
				break
			} else {
				results[curr] = results[next] // Move next element here
				curr = next                   // Follow the chain
			}
		}
	}

	return results, nil
}

func SortThenProcessThenUnsort[T any, R any](slice []T, less func(T, T) bool,
	operation func([]T) ([]R, error)) ([]R, error) {
	sortableItems := newSortable(slice, less)
	sort.Sort(sortableItems)
	sortedResults, err := operation(sortableItems.items)
	if err != nil {
		return nil, err
	}

	// Now unsort to get the original order.
	return Unsort(sortableItems, sortedResults)
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

// The indicesToRemove must be sorted in increasing order.
// Note: if this returns an error, the slice may be partially modified.
func RemoveSortedIndices[T any](slice []T, indicesToRemove []int) ([]T, error) {
	// Validate the indices.
	prevIndex := -1
	for _, index := range indicesToRemove {
		if index < 0 || index >= len(slice) {
			return nil, fmt.Errorf("RemoveSortedIndices: index %v out of range for slice of length %v",
				index, len(slice))
		}

		if index <= prevIndex {
			return nil, fmt.Errorf("RemoveSortedIndices: indicesToRemove must be increasing; found %v after %v",
				index, prevIndex)
		}

		prevIndex = index
	}

	numRemoved := 0
	for i := 0; i < len(slice); i++ {
		if numRemoved < len(indicesToRemove) && i == indicesToRemove[numRemoved] {
			numRemoved++
			continue
		}

		slice[i-numRemoved] = slice[i]
	}

	return slice[:len(slice)-len(indicesToRemove)], nil
}

func IndexOfMin[T any](arr []T, less func(T, T) bool) int {
	result := 0
	for i := 1; i < len(arr); i++ {
		if less(arr[i], arr[result]) {
			result = i
		}
	}

	return result
}

// All input slices must already be sorted by the `less` function.
func MergeSortedSlices[T any](less func(T, T) bool, slices ...[]T) []T {
	remainingSlices := make([][]T, 0, len(slices))
	nextIndices := make([]int, 0, len(slices))
	for _, slice := range slices {
		if len(slice) > 0 {
			remainingSlices = append(remainingSlices, slice)
			nextIndices = append(nextIndices, 0)
		}
	}

	totalLen := 0
	for _, slice := range slices {
		totalLen += len(slice)
	}
	result := make([]T, 0, totalLen)

	for len(remainingSlices) > 0 {
		// Find the slice with the next smallest element.
		minValue := remainingSlices[0][nextIndices[0]]
		indexOfMinSlice := 0
		for i, slice := range remainingSlices {
			if less(slice[nextIndices[i]], minValue) {
				minValue = slice[nextIndices[i]]
				indexOfMinSlice = i
			}
		}

		result = append(result, minValue)

		// Move to the next element in the selected slice.
		nextIndices[indexOfMinSlice]++
		if nextIndices[indexOfMinSlice] >= len(remainingSlices[indexOfMinSlice]) {
			// This slice is exhausted.
			remainingSlices = append(remainingSlices[:indexOfMinSlice], remainingSlices[indexOfMinSlice+1:]...)
			nextIndices = append(nextIndices[:indexOfMinSlice], nextIndices[indexOfMinSlice+1:]...)
		}
	}

	return result
}

func SelectFromSlice[T any](slice []T, shouldKeep func(T) bool) []T {
	result := make([]T, 0, len(slice))
	for _, item := range slice {
		if shouldKeep(item) {
			result = append(result, item)
		}
	}

	return result
}

func NormalizeSlice[T any](slice []T) []T {
	if slice == nil {
		return []T{}
	}

	return slice
}

func ShallowCopySlice[T any](src []T) []T {
	if src == nil {
		return nil
	}
	dst := make([]T, len(src))
	copy(dst, src)
	return dst
}

// If there's multiple errors, one of them is returned, but it's not guaranteed
// which one.
func ProcessWithParallelism[T any](parallelism int, items []T, processor func(T) error) error {
	var finalErr error
	waitGroup := sync.WaitGroup{}
	itemChan := make(chan T)

	// Add the items to a channel.
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		for _, item := range items {
			itemChan <- item
		}
		close(itemChan)
	}()

	// Process the items.
	waitGroup.Add(parallelism)
	for i := 0; i < parallelism; i++ {
		go func() {
			defer waitGroup.Done()
			for item := range itemChan {
				if err := processor(item); err != nil {
					finalErr = err
				}
			}
		}()
	}
	waitGroup.Wait()

	return finalErr
}

func Transform[T any, R any](slice []T, transform func(T) R) []R {
	result := make([]R, len(slice))
	for i, item := range slice {
		result[i] = transform(item)
	}

	return result
}

func Insert[T any](slice []T, index int, value T) []T {
	slice = append(slice, value)         // Increment length
	copy(slice[index+1:], slice[index:]) // Shift elements to the right
	slice[index] = value
	return slice
}
