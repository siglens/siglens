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

func Test_ConvertToSetFromMap(t *testing.T) {
	set := make(map[string]struct{})
	sourceMap := map[string]int{
		"a": 1,
		"b": 2,
		"c": 3,
	}

	ConvertToSetFromMap(set, sourceMap)

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

	ConvertToSetFromSlice(set, sourceSlice)

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
	sort.Strings(removedEntries)
	assert.True(t, CompareStringSlices([]string{"key1", "key2"}, addedEntries))

	set1["key3"] = struct{}{}
	set2["key1"] = struct{}{}
	set2["key2"] = struct{}{}

	addedEntries, removedEntries = SetDifference(set1, set2)
	assert.Equal(t, 1, len(addedEntries))
	assert.Equal(t, 0, len(removedEntries))
	assert.True(t, CompareStringSlices([]string{"key3"}, addedEntries))

	addedEntries, removedEntries = SetDifference(set2, set1)
	assert.Equal(t, 0, len(addedEntries))
	assert.Equal(t, 1, len(removedEntries))
	assert.True(t, CompareStringSlices([]string{"key3"}, removedEntries))

	set1 = map[string]struct{}{}
	addedEntries, removedEntries = SetDifference(set1, set2)
	assert.Equal(t, 0, len(addedEntries))
	assert.Equal(t, 2, len(removedEntries))
	sort.Strings(removedEntries)
	assert.True(t, CompareStringSlices([]string{"key1", "key2"}, removedEntries))
}
