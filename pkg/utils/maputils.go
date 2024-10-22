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
	"sort"
)

// If there are duplicate keys, values from the second map will overwrite those
// from the first map.
func MergeMaps[K comparable, V any](map1, map2 map[K]V) map[K]V {
	result := make(map[K]V)

	for k, v := range map1 {
		result[k] = v
	}

	for k, v := range map2 {
		result[k] = v
	}

	return result
}

func MapsConflict[K comparable, V comparable](map1 map[K]V, map2 map[K]V) bool {
	for key, v1 := range map1 {
		if v2, ok := map2[key]; ok && v1 != v2 {
			return true
		}
	}

	return false
}

func MapToSet[K comparable, V any](m map[K]V) map[K]struct{} {
	set := make(map[K]struct{}, len(m))

	for key := range m {
		set[key] = struct{}{}
	}

	return set
}

func SetToMap[K comparable, V any](s map[K]struct{}, defaultVal V) map[K]V {
	m := make(map[K]V, len(s))

	for key := range s {
		m[key] = defaultVal
	}

	return m
}

// Appends the Second map to the First Map. If there are duplicate keys, the value from the first map will be retained.
// The First Map will be modified in place and will have the values from the Second Map appended to it.
func MergeMapsRetainingFirst[K comparable, V any](firstMap map[K]V, secondMap map[K]V) {
	for k, v := range secondMap {
		if _, ok := firstMap[k]; !ok {
			firstMap[k] = v
		}
	}
}

// Appends the Second map to the First Map.
// The slice values from the Second Map will be appended to the slice values of the First Map.
// If the First Map does not have a key from the Second Map, the key will be added to the First Map
// And the slice with givem will be backfilled with the backFillValue.
func MergeMapSlicesWithBackfill[K comparable, V any](map1 map[K][]V, map2 map[K][]V, backFillValue V, size int) map[K][]V {
	for k, v := range map2 {
		v1, ok := map1[k]
		if !ok {
			v1 = ResizeSliceWithDefault(v1, size, backFillValue)
		}
		map1[k] = append(v1, v...)
	}

	return map1
}

func CreateRecord(columnNames []string, record []string) (map[string]interface{}, error) {
	if len(columnNames) != len(record) {
		return nil, fmt.Errorf("CreateRecord: Column and record lengths are not equal")
	}
	recordMap := make(map[string]interface{})
	for i, col := range columnNames {
		recordMap[col] = record[i]
	}
	return recordMap, nil
}

// SetDifference returns the added and removed elements between two sets.
func SetDifference[K comparable, T1, T2 any](newSet map[K]T2, oldSet map[K]T1) ([]K, []K) {
	var added, removed []K

	for key := range oldSet {
		if _, exists := newSet[key]; !exists {
			removed = append(removed, key)
		}
	}

	for key := range newSet {
		if _, exists := oldSet[key]; !exists {
			added = append(added, key)
		}
	}

	return added, removed
}

func RemoveEntriesFromMap[K comparable, T any](map1 map[K]T, keysToRemove []K) {
	for _, key := range keysToRemove {
		delete(map1, key)
	}
}
func AddMapKeysToSet[K comparable, V any](set map[K]struct{}, source map[K]V) {
	for k := range source {
		set[k] = struct{}{}
	}
}

func AddSliceToSet[K comparable](set map[K]struct{}, source []K) {
	for _, k := range source {
		set[k] = struct{}{}
	}
}

func AddToSet[K comparable](set map[K]struct{}, key K) {
	set[key] = struct{}{}
}

// IntersectionWithFirstMapValues returns a map containing the intersection of the keys of the two maps.
// The values of the first map are retained.
func IntersectionWithFirstMapValues[K comparable, V1 any, V2 any](map1 map[K]V1, map2 map[K]V2) map[K]V1 {
	intersection := make(map[K]V1)

	for k, v := range map1 {
		if _, exists := map2[k]; exists {
			intersection[k] = v
		}
	}

	return intersection
}

func GetKeysOfMap[K comparable, T any](map1 map[K]T) []K {
	keys := make([]K, 0, len(map1))
	for k := range map1 {
		keys = append(keys, k)
	}

	return keys
}

func GetSortedStringKeys[T any](map1 map[string]T) []string {
	keys := make([]string, 0)
	for key := range map1 {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return keys
}

func TransposeMapOfSlices[K comparable, V any](m map[K][]V) []map[K]V {
	result := make([]map[K]V, 0)

	for key, slice := range m {
		for i, v := range slice {
			if i >= len(result) {
				result = append(result, make(map[K]V))
			}

			result[i][key] = v
		}
	}

	return result
}

type TwoWayMap[T1, T2 comparable] struct {
	normal  map[T1]T2
	reverse map[T2]T1
}

func NewTwoWayMap[T1, T2 comparable]() *TwoWayMap[T1, T2] {
	return &TwoWayMap[T1, T2]{
		normal:  make(map[T1]T2),
		reverse: make(map[T2]T1),
	}
}

func (twm *TwoWayMap[T1, T2]) Set(key T1, value T2) {
	twm.normal[key] = value
	twm.reverse[value] = key
}

func (twm *TwoWayMap[T1, T2]) Get(key T1) (T2, bool) {
	value, exists := twm.normal[key]
	return value, exists
}

func (twm *TwoWayMap[T1, T2]) GetReverse(key T2) (T1, bool) {
	value, exists := twm.reverse[key]
	return value, exists
}

func (twm *TwoWayMap[T1, T2]) Conflicts(other map[T1]T2) bool {
	return MapsConflict(twm.normal, other)
}

// Do not modify the returned map. Modifying it will break the two-way mapping.
func (twm *TwoWayMap[T1, T2]) GetMapForReading() map[T1]T2 {
	return twm.normal
}

func (twm *TwoWayMap[T1, T2]) Len() int {
	return len(twm.normal)
}
