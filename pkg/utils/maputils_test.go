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

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_MapToSet(t *testing.T) {
	map1 := map[string]string{}
	assert.Equal(t, 0, len(MapToSet(map1)))

	map2 := map[string]int{
		"key1": 1,
		"key2": 1,
		"key3": 5,
	}

	set := MapToSet(map2)
	assert.Equal(t, 3, len(set))

	_, ok := set["key1"]
	assert.True(t, ok)
	_, ok = set["key2"]
	assert.True(t, ok)
	_, ok = set["key3"]
	assert.True(t, ok)
}
