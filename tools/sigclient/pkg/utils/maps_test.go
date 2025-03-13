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

func Test_IsSubset(t *testing.T) {
	assert.True(t, IsSubset(map[string]struct{}{}, map[string]struct{}{}))
	assert.True(t, IsSubset(map[string]struct{}{}, map[string]struct{}{"key1": {}}))
	assert.True(t, IsSubset(map[string]struct{}{"key1": {}}, map[string]struct{}{"key1": {}}))
	assert.False(t, IsSubset(map[string]struct{}{"key1": {}}, map[string]struct{}{"key2": {}}))
	assert.False(t, IsSubset(map[string]struct{}{"key1": {}, "key2": {}}, map[string]struct{}{"key1": {}}))
}

func Test_EqualSets(t *testing.T) {
	assert.True(t, EqualSets(map[string]struct{}{}, map[string]struct{}{}))
	assert.True(t, EqualSets(map[string]struct{}{"key1": {}}, map[string]struct{}{"key1": {}}))
	assert.True(t, EqualSets(map[string]struct{}{"key1": {}, "key2": {}}, map[string]struct{}{"key2": {}, "key1": {}}))
	assert.False(t, EqualSets(map[string]struct{}{"key1": {}}, map[string]struct{}{"key2": {}}))
	assert.False(t, EqualSets(map[string]struct{}{"key1": {}}, map[string]struct{}{"key1": {}, "key2": {}}))
	assert.False(t, EqualSets(map[string]struct{}{"key1": {}, "key2": {}}, map[string]struct{}{"key1": {}}))
}
