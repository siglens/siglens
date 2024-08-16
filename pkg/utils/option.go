// Copyright (c) 2021-2024 SigScalr, Inc.
//
// # This file is part of SigLens Observability Solution
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
)

type Option[T any] struct {
	value    T
	hasValue bool // This will never be true if `value` is nil.
}

func NewUnsetOption[T any]() Option[T] {
	return Option[T]{hasValue: false}
}

func NewOptionWithValue[T any](value T) Option[T] {
	option := NewUnsetOption[T]()
	option.Set(value)

	return option
}

func (o *Option[T]) Set(value T) {
	if o == nil {
		return
	}

	if isNil(value) {
		o.Clear()
		return
	}

	o.value = value
	o.hasValue = true
}

func (o *Option[T]) Clear() {
	if o == nil {
		return
	}

	var defaultValue T
	o.value = defaultValue
	o.hasValue = false
}

func (o *Option[T]) Get() (T, bool) {
	if o == nil {
		var defaultValue T
		return defaultValue, false
	}

	return o.value, o.hasValue
}

func isNil(value interface{}) bool {
	if value == nil {
		return true
	}

	reflectValue := reflect.ValueOf(value)
	switch reflectValue.Kind() {
	case reflect.Ptr, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return reflectValue.IsNil()
	default:
		return false
	}
}
