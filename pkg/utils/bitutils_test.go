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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_Int64Conversion(t *testing.T) {
	testValues := []int64{0, 42, -42, math.MaxInt64, math.MinInt64}

	for _, value := range testValues {
		bytes := Int64ToBytesLittleEndian(value)
		result := BytesToInt64LittleEndian(bytes)
		assert.Equal(t, value, result, "Int64 conversion failed for %d", value)
	}
}

func Test_Uint64Conversion(t *testing.T) {
	testValues := []uint64{0, 42, math.MaxUint64}

	for _, value := range testValues {
		bytes := Uint64ToBytesLittleEndian(value)
		result := BytesToUint64LittleEndian(bytes)
		assert.Equal(t, value, result, "Uint64 conversion failed for %d", value)
	}
}

func Test_Float64Conversion(t *testing.T) {
	testValues := []float64{0, 3.14159, -2.71828, math.MaxFloat64, math.SmallestNonzeroFloat64}

	for _, value := range testValues {
		bytes := Float64ToBytesLittleEndian(value)
		result := BytesToFloat64LittleEndian(bytes)
		assert.Equal(t, value, result, "Float64 conversion failed for %f", value)
	}
}

func Test_Int32Conversion(t *testing.T) {
	testValues := []int32{0, 42, -42, math.MaxInt32, math.MinInt32}

	for _, value := range testValues {
		bytes := Int32ToBytesLittleEndian(value)
		result := BytesToInt32LittleEndian(bytes)
		assert.Equal(t, value, result, "Int32 conversion failed for %d", value)
	}
}

func Test_Uint32Conversion(t *testing.T) {
	testValues := []uint32{0, 42, math.MaxUint32}

	for _, value := range testValues {
		bytes := Uint32ToBytesLittleEndian(value)
		result := BytesToUint32LittleEndian(bytes)
		assert.Equal(t, value, result, "Uint32 conversion failed for %d", value)
	}
}

func Test_Int16Conversion(t *testing.T) {
	testValues := []int16{0, 42, -42, math.MaxInt16, math.MinInt16}

	for _, value := range testValues {
		bytes := Int16ToBytesLittleEndian(value)
		result := BytesToInt16LittleEndian(bytes)
		assert.Equal(t, value, result, "Int16 conversion failed for %d", value)
	}
}

func Test_Uint16Conversion(t *testing.T) {
	testValues := []uint16{0, 42, math.MaxUint16}

	for _, value := range testValues {
		bytes := Uint16ToBytesLittleEndian(value)
		result := BytesToUint16LittleEndian(bytes)
		assert.Equal(t, value, result, "Uint16 conversion failed for %d", value)
	}
}

func Test_InplaceInt64(t *testing.T) {
	testValues := []int64{0, 42, -42, math.MaxInt64, math.MinInt64}
	buf := make([]byte, 8)

	for _, value := range testValues {
		Int64ToBytesLittleEndianInplace(value, buf)
		result := BytesToInt64LittleEndian(buf)
		assert.Equal(t, value, result, "Int64 conversion failed for %d", value)
	}
}

func Test_InplaceUint64(t *testing.T) {
	testValues := []uint64{0, 42, math.MaxUint64}
	buf := make([]byte, 8)

	for _, value := range testValues {
		Uint64ToBytesLittleEndianInplace(value, buf)
		result := BytesToUint64LittleEndian(buf)
		assert.Equal(t, value, result, "Uint64 conversion failed for %d", value)
	}
}

func Test_InplaceFloat64(t *testing.T) {
	testValues := []float64{0, 3.14159, -2.71828, math.MaxFloat64, math.SmallestNonzeroFloat64}
	buf := make([]byte, 8)

	for _, value := range testValues {
		Float64ToBytesLittleEndianInplace(value, buf)
		result := BytesToFloat64LittleEndian(buf)
		assert.Equal(t, value, result, "Float64 conversion failed for %f", value)
	}
}

func Test_InplaceInt16(t *testing.T) {
	testValues := []int16{0, 42, -42, math.MaxInt16, math.MinInt16}
	buf := make([]byte, 2)

	for _, value := range testValues {
		Int16ToBytesLittleEndianInplace(value, buf)
		result := BytesToInt16LittleEndian(buf)
		assert.Equal(t, value, result, "Int16 conversion failed for %d", value)
	}
}

func Test_InplaceInt32(t *testing.T) {
	testValues := []int32{0, 42, -42, math.MaxInt32, math.MinInt32}
	buf := make([]byte, 4)

	for _, value := range testValues {
		Int32ToBytesLittleEndianInplace(value, buf)
		result := BytesToInt32LittleEndian(buf)
		assert.Equal(t, value, result, "Int32 conversion failed for %d", value)
	}
}

func Test_InplaceUint16(t *testing.T) {
	testValues := []uint16{0, 42, math.MaxUint16}
	buf := make([]byte, 2)

	for _, value := range testValues {
		Uint16ToBytesLittleEndianInplace(value, buf)
		result := BytesToUint16LittleEndian(buf)
		assert.Equal(t, value, result, "Uint16 conversion failed for %d", value)
	}
}

func Test_InplaceUint32(t *testing.T) {
	testValues := []uint32{0, 42, math.MaxUint32}
	buf := make([]byte, 4)

	for _, value := range testValues {
		Uint32ToBytesLittleEndianInplace(value, buf)
		result := BytesToUint32LittleEndian(buf)
		assert.Equal(t, value, result, "Uint32 conversion failed for %d", value)
	}
}
