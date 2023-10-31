/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dtypeutils

import (
	"math"
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
