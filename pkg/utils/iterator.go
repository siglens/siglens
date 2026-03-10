// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

type Iterator[T any] interface {
	Next() (T, bool)
}

type iterator[T any] struct {
	slice []T
	index int
}

func NewIterator[T any](slice []T) *iterator[T] {
	return &iterator[T]{slice: slice}
}

func (it *iterator[T]) Next() (T, bool) {
	if it.index >= len(it.slice) {
		var defaultValue T
		return defaultValue, false
	}

	value := it.slice[it.index]
	it.index++

	return value, true
}
