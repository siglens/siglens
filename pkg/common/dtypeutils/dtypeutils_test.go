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

package dtypeutils

import (
	"fmt"
	"math"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_convertExpToType(t *testing.T) {

	_, err := ConvertExpToType(float64(123.123), uint8(12))
	assert.NotNil(t, err, "test cannot convert 123.123 to uint")

	val, err := ConvertExpToType(float64(10), uint8(12))
	assert.Nil(t, err) // can convert 123 to uint
	assert.Equal(t, val, uint8(10), "test conversion of 123 to uint")

}

// TODO: test cases for all arithmetic functions
// TODO: test cases for regex & string searching
func Test_divide(t *testing.T) {

	_, err := Divide(uint32(10), uint32(0))
	//errstrng := "cannot divide by zero"
	//assert.EqualError(t, err, errstrng)
	assert.NotNil(t, err)

	val, err := Divide(float64(math.MaxFloat64-2), float64(0.5))
	assert.NotNil(t, err)
	assert.Equal(t, val, float64(math.Inf(1)))

	val, err = Divide(float64(math.MaxFloat64-2), float64(-0.5))
	assert.NotNil(t, err)
	assert.Equal(t, val, float64(math.Inf(-1)))

	val, err = Divide(int64(64), int64(4))
	assert.Nil(t, err)
	assert.Equal(t, val, int64(16))

}

func Test_modulo(t *testing.T) {

	_, err := Modulo(int8(10), int8(0))
	assert.NotNil(t, err)

	val, err := Modulo(uint8(25), uint8(2))
	assert.Nil(t, err)
	assert.Equal(t, val, uint8(1))
}

func Test_multiply(t *testing.T) {

	val, err := Multiply(uint8(254), uint8(2))
	assert.Equal(t, uint16(508), val)
	assert.Nil(t, err)
	val, err = Multiply(uint16(65532), uint16(3))
	assert.Equal(t, uint32(196596), val)
	assert.Nil(t, err)
	val, err = Multiply(uint32(4294967295), uint32(2))
	assert.Equal(t, uint64(8589934590), val)
	assert.Nil(t, err)
	val, err = Multiply(uint64(18446744073709551615), uint64(2))
	assert.Equal(t, uint64(18446744073709551614), val)
	assert.Nil(t, err)
	val, err = Multiply(int8(127), int8(2))
	assert.Equal(t, int16(254), val)
	assert.Nil(t, err)
	val, err = Multiply(int8(-128), int8(2))
	assert.Equal(t, int16(-256), val)
	assert.Nil(t, err)
	val, err = Multiply(int16(32767), int16(3))
	assert.Equal(t, int32(98301), val)
	assert.Nil(t, err)
	val, err = Multiply(int16(-32768), int16(3))
	assert.Equal(t, int32(-98304), val)
	assert.Nil(t, err)
	val, err = Multiply(int32(2147483647), int32(3))
	assert.Equal(t, int64(6442450941), val)
	assert.Nil(t, err)
	val, err = Multiply(int32(-2147483648), int32(2))
	assert.Equal(t, int64(-4294967296), val)
	assert.Nil(t, err)
	val, err = Multiply(int64(9223372036854775807), int64(2))
	assert.Equal(t, int64(-2), val)
	assert.NotNil(t, err)
	val, err = Multiply(int64(-9223372036854775808), int64(3))
	assert.Equal(t, int64(-9223372036854775808), val)
	assert.NotNil(t, err)
	val, err = Multiply(float64(1.7e+308), float64(2))
	assert.Equal(t, float64(math.Inf(1)), val)
	assert.NotNil(t, err)
	val, err = Multiply(float64(-1.7e+308), float64(3))
	assert.Equal(t, (float64(math.Inf(-1))), val)
	assert.NotNil(t, err)
	val, err = Multiply(int8(10), int8(2))
	assert.Nil(t, err)
	assert.Equal(t, val, int16(20))
	val, err = Multiply(uint8(255), uint8(0))
	assert.Nil(t, err)
	assert.Equal(t, uint16(0), val)
	val, err = Multiply(uint16(65535), uint16(0))
	assert.Nil(t, err)
	assert.Equal(t, uint32(0), val)
	val, err = Multiply(uint32(4294967295), uint32(0))
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), val)
	val, err = Multiply(uint64(18446744073709551615), uint64(0))
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), val)
}

func Test_add(t *testing.T) {
	val, err := Add(uint8(255), uint8(255))
	assert.Equal(t, uint8(254), val)
	assert.NotNil(t, err)
	val, err = Add(uint16(65535), uint16(255))
	assert.Equal(t, uint16(254), val)
	assert.NotNil(t, err)
	val, err = Add(uint32(4294967295), uint32(250))
	assert.Equal(t, uint32(249), val)
	assert.NotNil(t, err)
	val, err = Add(uint64(18446744073709551615), uint64(250))
	assert.Equal(t, uint64(249), val)
	assert.NotNil(t, err)
	val, err = Add(int8(127), int8(127))
	assert.Equal(t, int8(-2), val)
	assert.NotNil(t, err)
	val, err = Add(int8(-128), int8(-128))
	assert.Equal(t, int8(0), val)
	assert.NotNil(t, err)
	val, err = Add(int16(32767), int16(10))
	assert.Equal(t, int16(-32759), val)
	assert.NotNil(t, err)
	val, err = Add(int16(-32768), int16(-10))
	assert.Equal(t, int16(32758), val)
	assert.NotNil(t, err)
	val, err = Add(int32(2147483647), int32(1))
	assert.Equal(t, int32(-2147483648), val)
	assert.NotNil(t, err)
	val, err = Add(int32(-2147483648), int32(-10))
	assert.Equal(t, int32(2147483638), val)
	assert.NotNil(t, err)
	val, err = Add(int64(9223372036854775807), int64(1))
	assert.Equal(t, int64(-9223372036854775808), val)
	assert.NotNil(t, err)
	val, err = Add(int64(-9223372036854775808), int64(-1))
	assert.Equal(t, int64(9223372036854775807), val)
	assert.NotNil(t, err)
	val, err = Add(float64(1.7e+308), float64(1.7e+308))
	assert.Equal(t, float64(math.Inf(1)), val)
	assert.NotNil(t, err)
	val, err = Add(float64(-1.7e+308), float64(-1.7e+308))
	assert.Equal(t, float64(float64(math.Inf(-1))), val)
	assert.NotNil(t, err)
	val, err = Add(int8(10), int8(2))
	assert.Nil(t, err)
	assert.Equal(t, val, int8(12))
}
func Test_subtract(t *testing.T) {
	val, err := Subtract(uint8(0), uint8(255))
	assert.Equal(t, uint8(1), val)
	assert.NotNil(t, err)
	val, err = Subtract(uint16(10), uint16(65535))
	assert.Equal(t, uint16(11), val)
	assert.NotNil(t, err)
	val, err = Subtract(uint32(25), uint32(4294967295))
	assert.Equal(t, uint32(26), val)
	assert.NotNil(t, err)
	val, err = Subtract(uint64(25), uint64(18446744073709551615))
	assert.Equal(t, uint64(26), val)
	assert.NotNil(t, err)
	val, err = Subtract(int8(127), int8(-10))
	assert.Equal(t, int8(-119), val)
	assert.NotNil(t, err)
	val, err = Subtract(int8(-128), int8(10))
	assert.Equal(t, int8(118), val)
	assert.NotNil(t, err)
	val, err = Subtract(int16(32767), int16(-10))
	assert.Equal(t, int16(-32759), val)
	assert.NotNil(t, err)
	val, err = Subtract(int16(-32768), int16(10))
	assert.Equal(t, int16(32758), val)
	assert.NotNil(t, err)
	val, err = Subtract(int32(2147483647), int32(-10))
	assert.Equal(t, int32(-2147483639), val)
	assert.NotNil(t, err)
	val, err = Subtract(int32(-2147483648), int32(10))
	assert.Equal(t, int32(2147483638), val)
	assert.NotNil(t, err)
	val, err = Subtract(int64(9223372036854775807), int64(-10))
	assert.Equal(t, int64(-9223372036854775799), val)
	assert.NotNil(t, err)
	val, err = Subtract(int64(-9223372036854775808), int64(10))
	assert.Equal(t, int64(9223372036854775798), val)
	assert.NotNil(t, err)
	val, err = Subtract(float64(1.7e+308), float64(-1.7e+308))
	assert.Equal(t, float64(float64(math.Inf(1))), val)
	assert.NotNil(t, err)
	val, err = Subtract(float64(-1.7e+308), float64(1.7e+308))
	assert.Equal(t, float64(float64(math.Inf(-1))), val)
	assert.NotNil(t, err)
	val, err = Subtract(int8(10), int8(2))
	assert.Nil(t, err)
	assert.Equal(t, val, int8(8))
}

func Test_SPLtoRegex(t *testing.T) {

	type testCase struct {
		splPattern      string
		caseInsensitive bool
		isTerm          bool
		acceptStrings   []string
		rejectStrings   []string
	}

	testCases := []testCase{
		// case-sensitive, non-TERM()
		{
			splPattern:      `foo`,
			caseInsensitive: false,
			isTerm:          false,
			acceptStrings:   []string{"foo"},
			rejectStrings:   []string{"Foo", "fOo", "FOO", "bar", "foo bar"},
		},
		{
			splPattern:      `foo*`,
			caseInsensitive: false,
			isTerm:          false,
			acceptStrings:   []string{"foo", "foot", "football", "foot-ball123"},
			rejectStrings:   []string{"fOo", "FOO", "bar", "Football", "afoo", "afoot"},
		},

		// case-sensitive, TERM()
		{
			splPattern:      `foo`,
			caseInsensitive: false,
			isTerm:          true,
			acceptStrings:   []string{"foo", "foo bar", "a[foo)", "foo123;foo", "<foo%21"},
			rejectStrings:   []string{"Foo", "fOo", "FOO", "bar"},
		},
		{
			splPattern:      `foo*`,
			caseInsensitive: false,
			isTerm:          true,
			acceptStrings:   []string{"foo", "foot", "football", "foot-ball123", "foo bar", "a[foo)", "foo123;foo", "<foo%21"},
			rejectStrings:   []string{"Foo", "fOo", "FOO", "bar", "Football"},
		},
		{
			splPattern:      `an Apple`,
			caseInsensitive: true,
			isTerm:          true,
			acceptStrings:   []string{"an apple", "An Apple", "aN aPPle", "an APPLE a day", "i EAT an Apple every day", " an apple"},
			rejectStrings:   []string{"ban apples", "Van Apple", "a-n aPPle", "an apple_", "an appLeT"},
		},

		// case-insensitive, non-TERM()
		{
			splPattern:      `foo`,
			caseInsensitive: true,
			isTerm:          false,
			acceptStrings:   []string{"foo", "Foo", "FOo"},
			rejectStrings:   []string{"foo bar", "foot", "bar"},
		},
		{
			splPattern:      `foo*`,
			caseInsensitive: true,
			isTerm:          false,
			acceptStrings:   []string{"foo", "Foo", "FOo", "foobar", "foot", "foot-ball123"},
			rejectStrings:   []string{"bar", "Goo", "bar foo"},
		},

		// case-insensitive, TERM()
		{
			splPattern:      `foo`,
			caseInsensitive: true,
			isTerm:          true,
			acceptStrings:   []string{"foo", "foo bar", "a[foo)", "foo123;foo", "<foo%21", "Foo", "fOo", "FOO"},
			rejectStrings:   []string{"foobar", "bar"},
		},
		{
			splPattern:      `foo*`,
			caseInsensitive: true,
			isTerm:          true,
			acceptStrings:   []string{"foo", "Foo", "FOo", "food", "foo&$^!@#", "Football"},
			rejectStrings:   []string{"bar", "floo", "afoo", "a_foo"},
		},
		{
			splPattern:      `an apple`,
			caseInsensitive: true,
			isTerm:          true,
			acceptStrings:   []string{"an Apple", "i EAT an AppLe every day", " an Apple"},
			rejectStrings:   []string{"ban apples", "Van Apple", "a-n aPPle", "an apple_", "an appLeT"},
		},
	}

	for _, testCase := range testCases {
		regexStr := SPLToRegex(testCase.splPattern, testCase.caseInsensitive, testCase.isTerm)
		regexAutomaton, err := regexp.Compile(regexStr)
		assert.NoError(t, err)

		for _, acceptedStr := range testCase.acceptStrings {
			assert.True(t, regexAutomaton.MatchString(acceptedStr), fmt.Sprintf("string %s with regex %s", acceptedStr, regexAutomaton.String()))
		}
		for _, rejectedStr := range testCase.rejectStrings {
			assert.False(t, regexAutomaton.MatchString(rejectedStr), fmt.Sprintf("string %s with regex %s", rejectedStr, regexAutomaton.String()))
		}
	}
}
