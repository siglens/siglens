// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SliceContainsItems(t *testing.T) {
	equal := func(a, b int) bool { return a == b }
	assert.True(t, SliceContainsItems([]int{1, 2, 3}, []int{}, equal))
	assert.True(t, SliceContainsItems([]int{1, 2, 3}, []int{1}, equal))
	assert.True(t, SliceContainsItems([]int{1, 2, 3}, []int{3, 2, 1}, equal))
	assert.False(t, SliceContainsItems([]int{1, 2, 3}, []int{4}, equal))
	assert.False(t, SliceContainsItems([]int{1, 2, 3}, []int{1, 1}, equal))
}

func Test_IsPermutation(t *testing.T) {
	equal := func(a, b int) bool { return a == b }
	assert.True(t, IsPermutation([]int{}, []int{}, equal))
	assert.True(t, IsPermutation([]int{1}, []int{1}, equal))
	assert.True(t, IsPermutation([]int{1, 2}, []int{2, 1}, equal))
	assert.True(t, IsPermutation([]int{1, 2, 3}, []int{2, 1, 3}, equal))
	assert.False(t, IsPermutation([]int{1, 2, 3}, []int{1, 2}, equal))
	assert.False(t, IsPermutation([]int{1, 2, 3}, []int{1, 2, 4}, equal))
	assert.False(t, IsPermutation([]int{1, 1, 2}, []int{1, 2, 2}, equal))
}
