// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0
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
