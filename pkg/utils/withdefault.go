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
	"gopkg.in/yaml.v3"
)

type WithDefault[T comparable] struct {
	value        T
	defaultValue T
	isSet        bool
}

func DefaultValue[T comparable](defaultValue T) WithDefault[T] {
	return WithDefault[T]{defaultValue: defaultValue}
}

func (w *WithDefault[T]) Set(value T) {
	w.value = value
	w.isSet = true
}

func (w WithDefault[T]) With(value T) WithDefault[T] {
	return WithDefault[T]{
		value:        value,
		defaultValue: w.defaultValue,
		isSet:        true,
	}
}

func (w *WithDefault[T]) Value() T {
	if w.isSet {
		return w.value
	}

	return w.defaultValue
}

func (w WithDefault[T]) Equals(other WithDefault[T]) bool {
	return w.Value() == other.Value()
}

func (w *WithDefault[T]) UnmarshalYAML(value *yaml.Node) error {
	if value.Tag == "!!null" || value.Value == "" {
		w.isSet = false
	} else {
		var val T
		if err := value.Decode(&val); err != nil {
			w.isSet = false
			return nil
		}
		w.value = val
		w.isSet = true
	}

	return nil
}
