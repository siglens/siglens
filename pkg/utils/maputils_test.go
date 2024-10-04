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
