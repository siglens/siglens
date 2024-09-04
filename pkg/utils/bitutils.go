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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"

	log "github.com/sirupsen/logrus"
)

const UINT8_MAX = 255
const UINT16_MAX = 65_535
const UINT32_MAX = 4_294_967_295

func BoolToBytesLittleEndian(b bool) []byte {
	if b {
		return []byte{1}
	} else {
		return []byte{0}
	}
}

func BytesToBoolLittleEndian(bytes []byte) bool {
	return bytes[0] == []byte{1}[0]
}

/*
This function converts the float64 to bytes in place. It is the responsibility
of the caller to make sure buf is atleast 8 bytes, else this func will crash
*/
func Float64ToBytesLittleEndianInplace(val float64, buf []byte) {

	// Convert float64 to uint64 representation
	bits := math.Float64bits(val)

	// Write the uint64 value into the byte slice in little-endian order
	buf[0] = byte(bits)
	buf[1] = byte(bits >> 8)
	buf[2] = byte(bits >> 16)
	buf[3] = byte(bits >> 24)
	buf[4] = byte(bits >> 32)
	buf[5] = byte(bits >> 40)
	buf[6] = byte(bits >> 48)
	buf[7] = byte(bits >> 56)
}

func Float64ToBytesLittleEndian(val float64) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, val)
	if err != nil {
		log.Errorf("Float64ToBytesLittleEndian: binary.Write failed, val: %v, err: %v", val, err)
	}
	return buf.Bytes()
}

func BytesToFloat64LittleEndian(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	return math.Float64frombits(bits)
}

func Uint64ToBytesLittleEndian(val uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, val)
	return b
}

func Uint32ToBytesLittleEndian(val uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, val)
	return b
}

func Uint16ToBytesLittleEndian(val uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, val)
	return b
}

func BytesToUint64LittleEndian(bytes []byte) uint64 {
	return binary.LittleEndian.Uint64(bytes)
}
func BytesToUint32LittleEndian(bytes []byte) uint32 {
	return binary.LittleEndian.Uint32(bytes)
}
func BytesToUint16LittleEndian(bytes []byte) uint16 {
	return binary.LittleEndian.Uint16(bytes)
}

func Int16ToBytesLittleEndian(signedval int16) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, signedval)
	if err != nil {
		log.Errorf("Int16ToBytesLittleEndian: binary.Write failed: val: %v, err: %v\n", signedval, err)
	}
	return buf.Bytes()
}

func Int32ToBytesLittleEndian(signedval int32) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, signedval)
	if err != nil {
		log.Errorf("Int32ToBytesLittleEndian: binary.Write failed: val: %v, err: %v\n", signedval, err)
	}
	return buf.Bytes()
}

func Int64ToBytesLittleEndian(signedval int64) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, signedval)
	if err != nil {
		log.Errorf("Int64ToBytesLittleEndian: binary.Write failed: val: %v, err: %v\n", signedval, err)
	}
	return buf.Bytes()
}

/*
This function converts the int64 to bytes in place. It is the responsibility
of the caller to make sure buf is atleast 8 bytes, else this func will crash
*/
func Int64ToBytesLittleEndianInplace(signedval int64, buf []byte) {
	buf[0] = byte(signedval)
	buf[1] = byte(signedval >> 8)
	buf[2] = byte(signedval >> 16)
	buf[3] = byte(signedval >> 24)
	buf[4] = byte(signedval >> 32)
	buf[5] = byte(signedval >> 40)
	buf[6] = byte(signedval >> 48)
	buf[7] = byte(signedval >> 56)
}

func BytesToInt64LittleEndian(bytes []byte) int64 {
	return int64(binary.LittleEndian.Uint64(bytes))
}
func BytesToInt32LittleEndian(bytes []byte) int32 {
	return int32(binary.LittleEndian.Uint32(bytes))
}
func BytesToInt16LittleEndian(bytes []byte) int16 {
	return int16(binary.LittleEndian.Uint16(bytes))
}

func UInt64ToStringBytes(val uint64) string {
	const unit = 1000
	if val < unit {
		return fmt.Sprintf("%db", val)
	}
	div, exp := int64(unit), 0
	for n := val / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cb", float64(val)/float64(div), "kmgtpe"[exp])
}

func BinarySearchUint16(needle uint16, haystack []uint16) bool {

	low := 0
	high := len(haystack) - 1

	for low <= high {
		median := (low + high) / 2

		if haystack[median] < needle {
			low = median + 1
		} else {
			high = median - 1
		}
	}

	if low == len(haystack) || haystack[low] != needle {
		return false
	}

	return true
}

// todo write a optimized version of this and replace all invocations of this func
func SearchStr(needle string, haystack []string) bool {

	for _, h := range haystack {
		if needle == h {
			return true
		}
	}
	return false
}

// returns string using unsafe. This is zero-copy and uses unsafe
func UnsafeByteSliceToString(haystack []byte) string {
	return *(*string)(unsafe.Pointer(&haystack))
}
