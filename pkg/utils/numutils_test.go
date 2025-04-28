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

func TestFastParseFloat(t *testing.T) {
	tests := []struct {
		input    []byte
		expected float64
		hasError bool
	}{
		{[]byte("123"), 123.0, false},
		{[]byte("123.456"), 123.456, false},
		{[]byte("0.789"), 0.789, false},
		{[]byte("-42.1"), -42.1, false},
		{[]byte("-0.001"), -0.001, false},
		{[]byte("0"), 0, false},
		{[]byte(""), 0, true},
		{[]byte("abc"), 0, true},
		{[]byte("12abc"), 0, true},
		{[]byte("12.3.4"), 0, true},
		{[]byte("1.2.3"), 0, true},
	}

	for _, tt := range tests {
		result, err := FastParseFloat(tt.input)
		if tt.hasError {
			assert.Error(t, err, "input: %s", tt.input)
		} else {
			assert.NoError(t, err, "input: %s", tt.input)
			assert.InDelta(t, tt.expected, result, 0.0001, "input: %s", tt.input)
		}
	}
}
