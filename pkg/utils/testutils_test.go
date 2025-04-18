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

func Test_EquivalentJson(t *testing.T) {
	// Dictionary permutation is ok.
	assert.True(t, EquivalentJson(
		`{"a":1,"b":2,"c":3}`,
		`{"b":2,"c":3,"a":1}`,
	))

	// Nested dictionary permutation is ok.
	assert.True(t, EquivalentJson(
		`{"a":1,"b":{"x":2,"y":3},"c":3}`,
		`{"b":{"y":3,"x":2},"c":3,"a":1}`,
	))

	// Missing key is not ok.
	assert.False(t, EquivalentJson(
		`{"a":1,"b":2,"c":3}`,
		`{"b":2,"c":3}`,
	))

	// Missing array element is not ok.
	assert.False(t, EquivalentJson(
		`{"a":1,"b":[2,3],"c":3}`,
		`{"a":1,"b":[3],"c":3}`,
	))

	// Array permutation is not ok.
	assert.False(t, EquivalentJson(
		`{"a":1,"b":[2,3],"c":3}`,
		`{"a":1,"b":[3,2],"c":3}`,
	))
}
