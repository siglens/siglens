// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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
