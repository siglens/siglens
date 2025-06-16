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
	"bytes"
	"encoding/gob"
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

// Implement the GobEncoder interface.
func (o Option[T]) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	if err := encoder.Encode(o.hasValue); err != nil {
		return nil, err
	}

	if o.hasValue {
		if err := encoder.Encode(o.value); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// Implement the GobDecoder interface.
func (o *Option[T]) GobDecode(data []byte) error {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)

	if err := decoder.Decode(&o.hasValue); err != nil {
		return err
	}

	if o.hasValue {
		if err := decoder.Decode(&o.value); err != nil {
			return err
		}
	} else {
		var defaultValue T
		o.value = defaultValue
	}

	return nil
}

func EqualOptions[T comparable](a, b Option[T]) bool {
	val1, ok1 := a.Get()
	val2, ok2 := b.Get()

	if !ok1 && !ok2 {
		return true
	}

	if !ok1 || !ok2 {
		return false
	}

	return val1 == val2
}
