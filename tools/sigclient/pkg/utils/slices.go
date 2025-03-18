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

type equalFunc[T any] func(T, T) bool

// Returns true if all items are present in the slice. The items don't need to
// be contiguous or in order.
func SliceContainsItems[T any](slice []T, items []T, equal equalFunc[T]) bool {
	for i, item := range items {
		requiredCount := 1
		for _, other := range items[i+1:] {
			if equal(item, other) {
				requiredCount++
			}
		}

		foundCount := 0
		for _, s := range slice {
			if equal(s, item) {
				foundCount++
				if foundCount >= requiredCount {
					break
				}
			}
		}

		if foundCount < requiredCount {
			return false
		}
	}

	return true
}

func IsPermutation[T any](slice1, slice2 []T, equal equalFunc[T]) bool {
	if len(slice1) != len(slice2) {
		return false
	}

	return SliceContainsItems(slice1, slice2, equal) && SliceContainsItems(slice2, slice1, equal)
}
