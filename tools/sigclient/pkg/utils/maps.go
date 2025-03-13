// Copyright (c) 2021-2025 SigScalr, Inc.
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

func EqualMaps[K comparable, V comparable](map1 map[K]V, map2 map[K]V) bool {
	if len(map1) != len(map2) {
		return false
	}

	for key, v1 := range map1 {
		if v2, ok := map2[key]; !ok || v1 != v2 {
			return false
		}
	}

	return true
}

// Checks if set1 is a subset of set2.
func IsSubset[K comparable](set1 map[K]struct{}, set2 map[K]struct{}) bool {
	for key := range set1 {
		if _, ok := set2[key]; !ok {
			return false
		}
	}

	return true
}

func EqualSets[K comparable](set1 map[K]struct{}, set2 map[K]struct{}) bool {
	return IsSubset(set1, set2) && IsSubset(set2, set1)
}
