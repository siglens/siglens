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
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
	"unsafe"

	jp "github.com/buger/jsonparser"
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

func Float64ToBytesLittleEndian(val float64) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, val)
	if err != nil {
		log.Error("binary.Write failed:", err)
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
		log.Errorf("binary.Write failed:%v\n", err)
	}
	return buf.Bytes()
}

func Int32ToBytesLittleEndian(signedval int32) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, signedval)
	if err != nil {
		log.Errorf("binary.Write failed:%v\n", err)
	}
	return buf.Bytes()
}

func Int64ToBytesLittleEndian(signedval int64) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, signedval)
	if err != nil {
		log.Errorf("binary.Write failed:%v\n", err)
	}
	return buf.Bytes()
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

func IsTimeInMilli(tval uint64) bool {
	if tval >= 99999999999 {
		return true
	} else {
		return false
	}
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
func GetCurrentTimeInMs() uint64 {
	return uint64(time.Now().UTC().UnixNano()) / uint64(time.Millisecond)
}

// This function will extract the timestamp from the raw body. This will assume the timestamp key exists at the root level
func ExtractTimeStamp(raw []byte, timestampKey *string) uint64 {
	rawVal, dType, _, err := jp.Get(raw, *timestampKey)
	if err != nil {
		// timestamp key does not exist in doc
		return 0
	}
	switch dType {
	case jp.String:
		tsStr, err := jp.ParseString(rawVal)
		if err != nil {
			log.Errorf("Failed to parse timestamp of raw string val: %v. Error: %v", rawVal, err)
			return 0
		}
		ts_millis, err := convertTimestampToMillis(tsStr)
		if err != nil {
			ts_millis = GetCurrentTimeInMs()
			log.Errorf("ExtractTimeStamp: Setting timestamp to current time in milli sec as parsing timestamp failed, err = %v", err)
		}
		return ts_millis
	case jp.Number:
		var ts_millis uint64
		val, err := jp.ParseInt(rawVal)
		if err != nil {
			val, err := jp.ParseFloat(rawVal)
			if err != nil {
				log.Errorf("Failed to parse timestamp of float val: %v. Error: %v", rawVal, err)
				return 0
			}
			ts_millis = uint64(val)
		} else {
			ts_millis = uint64(val)
		}

		if !IsTimeInMilli(ts_millis) {
			ts_millis *= 1000
		}
		return ts_millis
	default:
		return 0
	}
}

func convertTimestampToMillis(value string) (uint64, error) {
	parsed_value, err := strconv.ParseUint(string(value), 10, 64)
	if err == nil {
		if !IsTimeInMilli(parsed_value) {
			parsed_value *= 1000
		}
		return parsed_value, nil
	}

	timeFormats := []string{"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.999Z",
		"2006-01-02T15:04:05.999-07:00"}

	for _, timeFormat := range timeFormats {
		parsed_value, err := time.Parse(timeFormat, value)
		if err != nil {
			continue
		}
		return uint64(parsed_value.UTC().UnixNano() / 1000000), nil
	}
	return 0, errors.New("couldn't find matching time format")
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
