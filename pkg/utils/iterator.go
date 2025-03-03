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
