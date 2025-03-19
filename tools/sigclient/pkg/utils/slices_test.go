// Copyright (c) 2021-2025 SigScalr, Inc.
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
