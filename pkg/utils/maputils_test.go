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
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MapsConflict(t *testing.T) {
	map1 := map[string]int{
		"key1": 1,
		"key2": 2,
	}

	assert.False(t, MapsConflict(map1, map1))
	assert.False(t, MapsConflict(map1, map[string]int{}))
	assert.False(t, MapsConflict(map1, nil))
	assert.False(t, MapsConflict(map[string]int{}, map1))
	assert.False(t, MapsConflict(nil, map1))
	assert.False(t, MapsConflict(map1, map[string]int{"key1": 1}))
	assert.False(t, MapsConflict(map1, map[string]int{"key3": 3}))
	assert.False(t, MapsConflict(map1, map[string]int{"key5": 1}))
	assert.True(t, MapsConflict(map1, map[string]int{"key1": 2}))
}

func Test_MapToSet(t *testing.T) {
	map1 := map[string]string{}
	assert.Equal(t, 0, len(MapToSet(map1)))

	map2 := map[string]int{
		"key1": 1,
		"key2": 1,
		"key3": 5,
	}

	set := MapToSet(map2)
	assert.Equal(t, 3, len(set))

	_, ok := set["key1"]
	assert.True(t, ok)
	_, ok = set["key2"]
	assert.True(t, ok)
	_, ok = set["key3"]
	assert.True(t, ok)
}

func Test_SetToMap(t *testing.T) {
	set := map[string]struct{}{}
	assert.Equal(t, 0, len(SetToMap(set, 1)))

	set["key1"] = struct{}{}
	set["key2"] = struct{}{}
	set["key3"] = struct{}{}

	m := SetToMap(set, true)
	assert.Equal(t, 3, len(m))

	assert.Equal(t, true, m["key1"])
	assert.Equal(t, true, m["key2"])
	assert.Equal(t, true, m["key3"])
}

func Test_ConvertToSetFromMap(t *testing.T) {
	set := make(map[string]struct{})
	sourceMap := map[string]int{
		"a": 1,
		"b": 2,
		"c": 3,
	}

	AddMapKeysToSet(set, sourceMap)

	expectedSet := map[string]struct{}{
		"a": {},
		"b": {},
		"c": {},
	}

	if !reflect.DeepEqual(set, expectedSet) {
		t.Errorf("Expected %v, got %v", expectedSet, set)
	}
}

func Test_ConvertToSetFromSlice(t *testing.T) {
	set := make(map[string]struct{})
	sourceSlice := []string{"x", "y", "z"}

	AddSliceToSet(set, sourceSlice)

	expectedSet := map[string]struct{}{
		"x": {},
		"y": {},
		"z": {},
	}

	if !reflect.DeepEqual(set, expectedSet) {
		t.Errorf("Expected %v, got %v", expectedSet, set)
	}
}

// Test case: Both maps have common keys
func Test_IntersectionWithCommonKeys(t *testing.T) {
	map1 := map[string]int{
		"a": 1,
		"b": 2,
		"c": 3,
	}
	map2 := map[string]bool{
		"b": true,
		"c": false,
		"d": true,
	}

	expected := map[string]int{
		"b": 2,
		"c": 3,
	}

	result := IntersectionWithFirstMapValues(map1, map2)

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

// Test case: No common keys between the maps
func Test_IntersectionWithNoCommonKeys(t *testing.T) {
	map1 := map[string]int{
		"a": 1,
		"b": 2,
	}
	map2 := map[string]bool{
		"c": true,
		"d": false,
	}

	expected := map[string]int{}

	result := IntersectionWithFirstMapValues(map1, map2)

	if !reflect.DeepEqual(result, expected) {
		t.Errorf("Expected %v, got %v", expected, result)
	}
}

func Test_SetDifference(t *testing.T) {
	set1 := map[string]struct{}{
		"key1": {},
		"key2": {},
	}

	set2 := map[string]struct{}{}

	addedEntries, removedEntries := SetDifference(set1, set2)
	assert.Equal(t, 2, len(addedEntries))
	assert.Equal(t, 0, len(removedEntries))
	assert.ElementsMatch(t, []string{"key1", "key2"}, addedEntries)

	set1["key3"] = struct{}{}
	set2["key1"] = struct{}{}
	set2["key2"] = struct{}{}

	addedEntries, removedEntries = SetDifference(set1, set2)
	assert.Equal(t, 1, len(addedEntries))
	assert.Equal(t, 0, len(removedEntries))
	assert.ElementsMatch(t, []string{"key3"}, addedEntries)

	addedEntries, removedEntries = SetDifference(set2, set1)
	assert.Equal(t, 0, len(addedEntries))
	assert.Equal(t, 1, len(removedEntries))
	assert.ElementsMatch(t, []string{"key3"}, removedEntries)

	set1 = map[string]struct{}{}
	addedEntries, removedEntries = SetDifference(set1, set2)
	assert.Equal(t, 0, len(addedEntries))
	assert.Equal(t, 2, len(removedEntries))
	assert.ElementsMatch(t, []string{"key1", "key2"}, removedEntries)
}

func Test_RemoveEntriesFromMap(t *testing.T) {
	map1 := map[string]int{
		"key1": 1,
		"key2": 2,
		"key3": 3,
	}

	RemoveEntriesFromMap(map1, []string{"key1", "key2"})
	assert.Equal(t, 1, len(map1))
	_, exists := map1["key3"]
	assert.True(t, exists)

	map2 := map[string]struct{}{
		"key1": {},
		"key2": {},
		"key3": {},
	}

	RemoveEntriesFromMap(map2, []string{"key1"})
	assert.Equal(t, 2, len(map2))
	_, exists = map2["key2"]
	assert.True(t, exists)
	_, exists = map2["key3"]
	assert.True(t, exists)

	RemoveEntriesFromMap(map2, []string{"key2", "key3"})
	assert.Equal(t, 0, len(map2))
}

func Test_GetKeysOfMap(t *testing.T) {
	map1 := map[string]int{
		"z": 1,
		"a": 2,
		"b": 3,
	}

	keys := GetKeysOfMap(map1)

	assert.Equal(t, 3, len(keys))
	assert.ElementsMatch(t, []string{"z", "a", "b"}, keys)

	delete(map1, "a")
	keys = GetKeysOfMap(map1)

	assert.Equal(t, 2, len(keys))
	assert.ElementsMatch(t, []string{"z", "b"}, keys)

	map2 := map[int]string{
		1: "abc",
		2: "def",
		3: "ghi",
		4: "jkl",
	}

	keys2 := GetKeysOfMap(map2)
	assert.Equal(t, 4, len(keys2))
	assert.ElementsMatch(t, []int{1, 2, 3, 4}, keys2)

	delete(map2, 3)
	keys2 = GetKeysOfMap(map2)
	assert.Equal(t, 3, len(keys2))
	assert.ElementsMatch(t, []int{1, 2, 4}, keys2)

	for _, key := range keys2 {
		delete(map2, key)
	}

	keys2 = GetKeysOfMap(map2)
	assert.Equal(t, 0, len(keys2))
}

func Test_GetSortedStringKeys(t *testing.T) {
	map1 := map[string]bool{
		"key1": false,
		"key3": true,
		"key2": false,
	}
	expected := []string{"key1", "key2", "key3"}
	assert.Equal(t, expected, GetSortedStringKeys(map1))

	map2 := map[string]struct{}{
		"def": {},
		"abc": {},
		"ghi": {},
	}

	expected = []string{"abc", "def", "ghi"}
	assert.Equal(t, expected, GetSortedStringKeys(map2))
}

func Test_TransposeMapOfSlices(t *testing.T) {
	m := map[string][]int{
		"a": {1, 2, 3},
		"b": {1, 42, 100},
	}

	result := TransposeMapOfSlices(m)
	expected := []map[string]int{
		{"a": 1, "b": 1},
		{"a": 2, "b": 42},
		{"a": 3, "b": 100},
	}

	assert.Equal(t, len(expected), len(result))
	for i := range expected {
		assert.Equal(t, expected[i], result[i])
	}
}

func Test_MapLessThan(t *testing.T) {
	assert.True(t, MapLessThan(map[string]int{"a": 1}, map[string]int{"b": 2}))
	assert.True(t, MapLessThan(map[string]int{"a": 1}, map[string]int{"a": 2}))
	assert.False(t, MapLessThan(map[string]int{"a": 2}, map[string]int{"a": 1}))
	assert.False(t, MapLessThan(map[string]int{"a": 1}, map[string]int{"a": 1}))
	assert.True(t, MapLessThan(map[string]int{"a": 1, "b": 2}, map[string]int{"a": 1, "b": 3}))
	assert.True(t, MapLessThan(map[string]int{"a": 1, "b": 2}, map[string]int{"a": 1, "c": 0}))
}

func Test_TwoWayMap(t *testing.T) {
	twm := NewTwoWayMap[string, int]()
	twm.Set("key1", 1)

	value, ok := twm.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, 1, value)

	_, ok = twm.Get("key2")
	assert.False(t, ok)

	key, ok := twm.GetReverse(1)
	assert.True(t, ok)
	assert.Equal(t, "key1", key)

	_, ok = twm.GetReverse(2)
	assert.False(t, ok)
}

func Test_MergeMapSlicesWithBackfill(t *testing.T) {
	tests := []struct {
		name           string
		map1           map[string][]int
		map2           map[string][]int
		backFillValue  int
		size           int
		expectedResult map[string][]int
	}{
		{
			name: "Basic merge with no missing keys",
			map1: map[string][]int{
				"a": {1, 2},
				"b": {3, 4},
			},
			map2: map[string][]int{
				"a": {5, 6},
				"b": {7, 8},
			},
			backFillValue: 0,
			size:          2,
			expectedResult: map[string][]int{
				"a": {1, 2, 5, 6},
				"b": {3, 4, 7, 8},
			},
		},
		{
			name: "Adding new keys with backfilling",
			map1: map[string][]int{
				"a": {1, 2},
			},
			map2: map[string][]int{
				"b": {7, 8},
			},
			backFillValue: 0,
			size:          2,
			expectedResult: map[string][]int{
				"a": {1, 2},
				"b": {0, 0, 7, 8},
			},
		},
		{
			name: "Empty map1, backfill with default values",
			map1: map[string][]int{},
			map2: map[string][]int{
				"c": {9, 10},
			},
			backFillValue: 0,
			size:          3,
			expectedResult: map[string][]int{
				"c": {0, 0, 0, 9, 10},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := MergeMapSlicesWithBackfill(tt.map1, tt.map2, tt.backFillValue, tt.size)
			if !reflect.DeepEqual(result, tt.expectedResult) {
				t.Errorf("Test %s failed: got %v, expected %v", tt.name, result, tt.expectedResult)
			}
		})
	}
}

func TestGetOrCreateNestedMap(t *testing.T) {
	m := make(map[string]map[string]int)

	// Test when key1 does not exist
	innerMap := GetOrCreateNestedMap(m, "key1")
	assert.NotNil(t, innerMap)
	assert.Equal(t, 0, len(innerMap))

	// Test if key1 now exists and inner map is accessible
	innerMap["key2"] = 42
	newInnerMap := GetOrCreateNestedMap(m, "key1")
	assert.Equal(t, 42, newInnerMap["key2"])
}

func TestGetEntryFromNestedMap(t *testing.T) {
	m := make(map[string]map[string]int)

	// Test when outer map key1 does not exist
	value, exists := GetEntryFromNestedMap(m, "key1", "key2")
	assert.False(t, exists, "Value should not exist for a non-existent outer key")
	assert.Equal(t, 0, value, "Returned value should be the zero value of the type")

	// Test when key1 exists but key2 does not
	m["key1"] = map[string]int{"key3": 100}
	value, exists = GetEntryFromNestedMap(m, "key1", "key2")
	assert.False(t, exists, "Value should not exist for a non-existent inner key")
	assert.Equal(t, 0, value, "Returned value should be the zero value of the type")

	// Test when both key1 and key2 exist
	value, exists = GetEntryFromNestedMap(m, "key1", "key3")
	assert.True(t, exists, "Value should exist when both outer and inner keys exist")
	assert.Equal(t, 100, value, "The correct value should be returned")
}

func TestRemoveKeyFromNestedMap(t *testing.T) {
	m := make(map[string]map[string]bool)

	// Test removing key from an empty map (no-op)
	RemoveKeyFromNestedMap(m, "key1", "key2")
	assert.Equal(t, 0, len(m))

	// Test removing an existing inner key and leaving the outer map intact
	m["key1"] = map[string]bool{"key2": true, "key3": true}
	RemoveKeyFromNestedMap(m, "key1", "key2")
	assert.Equal(t, 1, len(m["key1"]), "One key should remain in the inner map after removing one key")
	assert.Equal(t, true, m["key1"]["key3"], "The remaining key should still exist in the map")

	// Test removing the last inner key and removing the outer map key
	RemoveKeyFromNestedMap(m, "key1", "key3")
	assert.Equal(t, 0, len(m), "Outer map key should be removed when the inner map becomes empty")
}
