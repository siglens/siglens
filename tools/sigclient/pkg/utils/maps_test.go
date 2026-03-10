// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_EqualMaps(t *testing.T) {
	assert.True(t, EqualMaps(map[string]int{}, map[string]int{}))
	assert.True(t, EqualMaps(map[string]int{"key1": 1}, map[string]int{"key1": 1}))
	assert.True(t, EqualMaps(map[string]int{"key1": 1, "key2": 2}, map[string]int{"key2": 2, "key1": 1}))
	assert.False(t, EqualMaps(map[string]int{"key1": 1}, map[string]int{"key2": 2}))
	assert.False(t, EqualMaps(map[string]int{"key1": 1}, map[string]int{"key1": 2}))
	assert.False(t, EqualMaps(map[string]int{"key1": 2}, map[string]int{"key2": 2}))
	assert.False(t, EqualMaps(map[string]int{"key1": 1}, map[string]int{"key1": 1, "key2": 2}))
	assert.False(t, EqualMaps(map[string]int{"key1": 1, "key2": 2}, map[string]int{"key1": 1}))

	assert.True(t, EqualMaps(map[string]interface{}{"key1": 1}, map[string]interface{}{"key1": 1}))
	assert.False(t, EqualMaps(map[string]interface{}{"key1": int(1)}, map[string]interface{}{"key1": float64(1)}))
}
