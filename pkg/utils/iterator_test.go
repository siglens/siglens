// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Iterator_empty(t *testing.T) {
	iter := NewIterator([]int{})

	_, ok := iter.Next()
	assert.False(t, ok)
}

func Test_Iterator(t *testing.T) {
	slice := []int{42, 100}
	iter := NewIterator(slice)

	value, ok := iter.Next()
	assert.True(t, ok)
	assert.Equal(t, 42, value)

	value, ok = iter.Next()
	assert.True(t, ok)
	assert.Equal(t, 100, value)

	_, ok = iter.Next()
	assert.False(t, ok)
}
