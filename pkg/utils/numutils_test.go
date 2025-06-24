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
		{[]byte("-456"), -456.0, false},
		{[]byte("1.23e3"), 1230.0, false},
		{[]byte("1.23E3"), 1230.0, false},
		{[]byte("1e2"), 100.0, false},
		{[]byte("1e0"), 1.0, false},
		{[]byte("1.5e-2"), 0.015, false},
		{[]byte("-2.5e+2"), -250.0, false},
		{[]byte("0.0001e4"), 1.0, false},
		{[]byte("42e0"), 42.0, false},
		{[]byte("000123.4500"), 123.45, false},

		// Invalid cases
		{[]byte(""), 0.0, true},
		{[]byte("abc"), 0.0, true},
		{[]byte("12abc"), 0.0, true},
		{[]byte("12.3.4"), 0.0, true},
		{[]byte("1.2.3"), 0.0, true},
		{[]byte("1e-"), 0.0, true},
		{[]byte("1e+"), 0.0, true},
		{[]byte("1.23e+"), 0.0, true},
		{[]byte("NaN"), 0.0, true},
		{[]byte("Infinity"), 0.0, true},
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

func Test_HumanizeUints(t *testing.T) {
	testCases := map[uint64]string{
		0:       "0",
		100:     "100",
		1000:    "1,000",
		9999:    "9,999",
		10000:   "10,000",
		999999:  "999,999",
		1000000: "1,000,000",
		// padding
		1001:     "1,001",
		12000345: "12,000,345",
		1002003:  "1,002,003",
		//
		18446744073709551615: "18,446,744,073,709,551,615",
		// different part lengths
		123456789012345678: "123,456,789,012,345,678",
		12345678901234567:  "12,345,678,901,234,567",
		1234567890123456:   "1,234,567,890,123,456",
		//
	}

	for test, answer := range testCases {
		humanizedVal := HumanizeUints(test)
		assert.Equal(t, answer, humanizedVal)
	}
}
