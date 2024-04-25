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
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"unsafe"
)

type TimeRange struct {
	StartEpochMs uint64
	EndEpochMs   uint64
}

type MetricsTimeRange struct {
	StartEpochSec uint32
	EndEpochSec   uint32
}

// returns true if [earliest_ts, latest_ts] overlaps with tsVal
func (tsVal *TimeRange) CheckRangeOverLap(earliest_ts, latest_ts uint64) bool {

	if (earliest_ts >= tsVal.StartEpochMs && earliest_ts <= tsVal.EndEpochMs) ||
		(latest_ts >= tsVal.StartEpochMs && latest_ts <= tsVal.EndEpochMs) ||
		(earliest_ts <= tsVal.StartEpochMs && latest_ts >= tsVal.EndEpochMs) {
		return true
	}
	return false
}

// returns true if [earliest_ts, latest_ts] overlaps with tsVal
func (tsVal *MetricsTimeRange) CheckRangeOverLap(earliest_ts, latest_ts uint32) bool {

	if (earliest_ts >= tsVal.StartEpochSec && earliest_ts <= tsVal.EndEpochSec) ||
		(latest_ts >= tsVal.StartEpochSec && latest_ts <= tsVal.EndEpochSec) ||
		(earliest_ts <= tsVal.StartEpochSec && latest_ts >= tsVal.EndEpochSec) {
		return true
	}
	return false
}

// returns true if [lowTs, highTs] is fully enclosed within timeRange
func (tr *TimeRange) AreTimesFullyEnclosed(lowTs, highTs uint64) bool {

	if lowTs >= tr.StartEpochMs && lowTs <= tr.EndEpochMs &&
		highTs >= tr.StartEpochMs && highTs <= tr.EndEpochMs {
		return true
	}
	return false
}

func (tsVal *TimeRange) CheckInRange(timeStamp uint64) bool {

	if tsVal.StartEpochMs <= timeStamp && timeStamp <= tsVal.EndEpochMs {
		return true
	}
	return false
}

func (tsVal *MetricsTimeRange) CheckInRange(timeStamp uint32) bool {

	if tsVal.StartEpochSec <= timeStamp && timeStamp <= tsVal.EndEpochSec {
		return true
	}
	return false
}

func ConvertToUInt(exp interface{}, bytes int) (uint64, error) {
	str := fmt.Sprint(exp)
	return strconv.ParseUint(str, 10, bytes)
}

func ConvertToInt(exp interface{}, bytes int) (int64, error) {
	str := fmt.Sprint(exp)
	return strconv.ParseInt(str, 10, bytes)
}

func ConvertToFloat(exp interface{}, bytes int) (float64, error) {
	str := fmt.Sprint(exp)
	return strconv.ParseFloat(str, bytes)
}

func ConvertToFloatAndReturnString(exp interface{}, bytes int) (float64, string, error) {
	str := fmt.Sprint(exp)
	floatExp, err := strconv.ParseFloat(str, bytes)
	if err != nil {
		return 0, "", err
	}
	return floatExp, str, nil
}

func ConvertExpToType(valueToConvert interface{}, knownType interface{}) (interface{}, error) {

	switch knownType.(type) {
	case uint8:
		retVal, ok := valueToConvert.(uint8)
		if !ok {
			retVal, err := ConvertToUInt(valueToConvert, 8)
			return uint8(retVal), err
		} else {
			return retVal, nil
		}
	case uint16:
		retVal, ok := valueToConvert.(uint16)
		if !ok {
			retVal, err := ConvertToUInt(valueToConvert, 16)
			return uint16(retVal), err
		} else {
			return retVal, nil
		}
	case uint32:
		retVal, ok := valueToConvert.(uint32)
		if !ok {
			retVal, err := ConvertToUInt(valueToConvert, 32)
			return uint32(retVal), err
		} else {
			return retVal, nil
		}
	case uint64:
		retVal, ok := valueToConvert.(uint64)
		if !ok {
			return ConvertToUInt(valueToConvert, 64)
		} else {
			return retVal, nil
		}
	case int8:
		retVal, ok := valueToConvert.(int8)
		if !ok {
			retVal, err := ConvertToInt(valueToConvert, 8)
			return int8(retVal), err
		} else {
			return retVal, nil
		}
	case int16:
		retVal, ok := valueToConvert.(int16)
		if !ok {
			retVal, err := ConvertToInt(valueToConvert, 16)
			return int16(retVal), err
		} else {
			return retVal, nil
		}
	case int32:
		retVal, ok := valueToConvert.(int32)
		if !ok {
			retVal, err := ConvertToInt(valueToConvert, 32)
			return int32(retVal), err
		} else {
			return retVal, nil
		}
	case int64:
		retVal, ok := valueToConvert.(int64)
		if !ok {
			return ConvertToInt(valueToConvert, 64)
		} else {
			return retVal, nil
		}
	case float64:
		retVal, ok := valueToConvert.(float64)
		if !ok {
			return ConvertToFloat(valueToConvert, 64)
		} else {
			return retVal, nil
		}
	case bool:
		retVal, ok := valueToConvert.(bool)
		if !ok {
			retVal, err := ConvertToInt(valueToConvert, 8)

			if err != nil {
				return int8(0), err
			}
			number := int8(retVal)

			if number == int8(0) {
				return false, nil
			} else {
				return true, nil
			}
		} else {
			return retVal, nil
		}
	case string:
		retVal, ok := valueToConvert.(string)
		if !ok {
			return fmt.Sprint(valueToConvert), nil
		} else {
			return retVal, nil
		}
	}
	return nil, errors.New("invalid conversion type")
}

// TODO: add logic for overflow/underflow cases
func Multiply(left interface{}, right interface{}) (interface{}, error) {

	switch left.(type) {
	case uint8:
		a := left.(uint8)
		b := right.(uint8)
		c := uint16(a) * uint16(b)
		return c, nil

	case uint16:
		a := left.(uint16)
		b := right.(uint16)
		c := uint32(a) * uint32(b)
		return c, nil

	case uint32:
		a := left.(uint32)
		b := right.(uint32)
		c := uint64(a) * uint64(b)
		return c, nil

	case uint64:
		a := left.(uint64)
		b := right.(uint64)
		c := a * b
		return c, nil

	case int8:
		a := left.(int8)
		b := right.(int8)
		c := int16(a) * int16(b)
		return c, nil

	case int16:
		a := left.(int16)
		b := right.(int16)
		c := int32(a) * int32(b)
		return c, nil

	case int32:
		a := left.(int32)
		b := right.(int32)
		c := int64(a) * int64(b)
		return c, nil

	case int64:
		a := left.(int64)
		b := right.(int64)
		c := a * b
		if a == 0 || b == 0 {
			return c, nil
		}

		if (c < 0) == ((a < 0) != (b < 0)) {
			if c/b == a {
				return c, nil
			}
		}
		return c, errors.New("Overflow")
	case float64:
		c := left.(float64) * right.(float64)
		if c != float64(math.Inf(1)) && c != float64(math.Inf(-1)) {
			return c, nil
		}
		return c, errors.New("Overflow")
	}

	return "", errors.New("invalid type for multiply")

}

// TODO: add logic for overflow/underflow cases
func Add(left interface{}, right interface{}) (interface{}, error) {

	switch left.(type) {
	case uint8:
		c := left.(uint8) + right.(uint8)
		if (c > left.(uint8)) == (right.(uint8) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")
	case uint16:
		c := left.(uint16) + right.(uint16)
		if (c > left.(uint16)) == (right.(uint16) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")
	case uint32:
		c := left.(uint32) + right.(uint32)
		if (c > left.(uint32)) == (right.(uint32) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")
	case uint64:
		c := left.(uint64) + right.(uint64)
		if (c > left.(uint64)) == (right.(uint64) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")
	case int8:
		c := left.(int8) + right.(int8)
		if (c > left.(int8)) == (right.(int8) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")
	case int16:
		c := left.(int16) + right.(int16)
		if (c > left.(int16)) == (right.(int16) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")
	case int32:
		c := left.(int32) + right.(int32)
		if (c > left.(int32)) == (right.(int32) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")
	case int64:
		c := left.(int64) + right.(int64)
		if (c > left.(int64)) == (right.(int64) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")
	case int:
		c := left.(int) + right.(int)
		if (c > left.(int)) == (right.(int) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")
	case float64:
		c := left.(float64) + right.(float64)
		if c != float64(math.Inf(1)) && c != float64(math.Inf(-1)) {
			return c, nil
		}
		return c, errors.New("Overflow")
	}

	return "", errors.New("invalid type for addition")
}

// TODO: add logic for overflow/underflow cases
func Subtract(left interface{}, right interface{}) (interface{}, error) {

	switch left.(type) {
	case uint8:
		c := left.(uint8) - right.(uint8)
		if (c < left.(uint8)) == (right.(uint8) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")

	case uint16:
		c := left.(uint16) - right.(uint16)
		if (c < left.(uint16)) == (right.(uint16) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")
	case uint32:
		c := left.(uint32) - right.(uint32)
		if (c < left.(uint32)) == (right.(uint32) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")
	case uint64:
		c := left.(uint64) - right.(uint64)
		if (c < left.(uint64)) == (right.(uint64) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")
	case int8:
		c := left.(int8) - right.(int8)
		if (c < left.(int8)) == (right.(int8) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")
	case int16:
		c := left.(int16) - right.(int16)
		if (c < left.(int16)) == (right.(int16) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")
	case int32:
		c := left.(int32) - right.(int32)
		if (c < left.(int32)) == (right.(int32) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")
	case int64:
		c := left.(int64) - right.(int64)
		if (c < left.(int64)) == (right.(int64) > 0) {
			return c, nil
		}
		return c, errors.New("Overflow")
	case float64:
		c := left.(float64) - right.(float64)
		if c != float64(math.Inf(1)) && c != float64(math.Inf(-1)) {
			return c, nil
		}
		return c, errors.New("Overflow")
	}

	return "", errors.New("invalid type for subtraction")
}

// TODO: add logic for overflow/underflow cases and divide by 0 verification
func Divide(left interface{}, right interface{}) (interface{}, error) {

	switch left.(type) {
	case uint8:
		if right.(uint8) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		return left.(uint8) / right.(uint8), nil
	case uint16:
		if right.(uint16) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		return left.(uint16) / right.(uint16), nil
	case uint32:
		if right.(uint32) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		return left.(uint32) / right.(uint32), nil
	case uint64:
		if right.(uint64) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		return left.(uint64) / right.(uint64), nil
	case int8:
		if right.(int8) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		return left.(int8) / right.(int8), nil
	case int16:
		if right.(int16) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		return left.(int16) / right.(int16), nil
	case int32:
		if right.(int32) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		return left.(int32) / right.(int32), nil
	case int64:
		if right.(int64) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		return left.(int64) / right.(int64), nil
	case float64:
		if right.(float64) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		c := left.(float64) / right.(float64)
		if c != float64(math.Inf(1)) && c != float64(math.Inf(-1)) {
			return c, nil
		}
		return c, errors.New("Overflow")
	}

	return "", errors.New("invalid type for divide")

}

// TODO: divide by 0 verification
func Modulo(left interface{}, right interface{}) (interface{}, error) {

	switch left.(type) {
	case uint8:
		if right.(uint8) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		return left.(uint8) % right.(uint8), nil
	case uint16:
		if right.(uint16) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		return left.(uint16) % right.(uint16), nil
	case uint32:
		if right.(uint32) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		return left.(uint32) % right.(uint32), nil
	case uint64:
		if right.(uint64) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		return left.(uint64) % right.(uint64), nil
	case int8:
		if right.(int8) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		return left.(int8) % right.(int8), nil
	case int16:
		if right.(int16) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		return left.(int16) % right.(int16), nil
	case int32:
		if right.(int32) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		return left.(int32) % right.(int32), nil
	case int64:
		if right.(int64) == 0 {
			return nil, errors.New("cannot divide by zero")
		}
		return left.(int64) % right.(int64), nil
	}

	return "", errors.New("invalid type for modulus")
}

func BitwiseAnd(left interface{}, right interface{}) (interface{}, error) {

	switch left.(type) {
	case uint8:
		return left.(uint8) & right.(uint8), nil
	case uint16:
		return left.(uint16) & right.(uint16), nil
	case uint32:
		return left.(uint32) & right.(uint32), nil
	case uint64:
		return left.(uint64) & right.(uint64), nil
	case int8:
		return left.(int8) & right.(int8), nil
	case int16:
		return left.(int16) & right.(int16), nil
	case int32:
		return left.(int32) & right.(int32), nil
	case int64:
		return left.(int64) & right.(int64), nil
	}

	return "", errors.New("invalid type for bitwise and")
}

func BitwiseOr(left interface{}, right interface{}) (interface{}, error) {

	switch left.(type) {
	case uint8:
		return left.(uint8) | right.(uint8), nil
	case uint16:
		return left.(uint16) | right.(uint16), nil
	case uint32:
		return left.(uint32) | right.(uint32), nil
	case uint64:
		return left.(uint64) | right.(uint64), nil
	case int8:
		return left.(int8) | right.(int8), nil
	case int16:
		return left.(int16) | right.(int16), nil
	case int32:
		return left.(int32) | right.(int32), nil
	case int64:
		return left.(int64) | right.(int64), nil
	}

	return "", errors.New("invalid type for bitwise or")
}

func BitwiseXOr(left interface{}, right interface{}) (interface{}, error) {

	switch left.(type) {
	case uint8:
		return left.(uint8) ^ right.(uint8), nil
	case uint16:
		return left.(uint16) ^ right.(uint16), nil
	case uint32:
		return left.(uint32) ^ right.(uint32), nil
	case uint64:
		return left.(uint64) ^ right.(uint64), nil
	case int8:
		return left.(int8) ^ right.(int8), nil
	case int16:
		return left.(int16) ^ right.(int16), nil
	case int32:
		return left.(int32) ^ right.(int32), nil
	case int64:
		return left.(int64) ^ right.(int64), nil
	}

	return "", errors.New("invalid type for bitwise xor")
}

// todo: better wildcard comparison
func ReplaceWildcardStarWithRegex(input string) string {
	var result strings.Builder
	for i, literal := range strings.Split(input, "*") {

		// Replace * with .*
		if i > 0 {
			result.WriteString(".*")
		}

		// Quote any regular expression meta characters in the
		// literal text.
		result.WriteString(regexp.QuoteMeta(literal))
	}
	return result.String()
}

func AlmostEquals(left, right float64) bool {
	tolerance := 0.000001
	if difference := math.Abs(left - right); difference < tolerance {
		return true
	} else {
		return false
	}
}

func ConvertToSameType(leftType, rightType interface{}) (interface{}, interface{}, error) {

	if fmt.Sprintf("%T", leftType) == fmt.Sprintf("%T", rightType) {
		return leftType, rightType, nil
	}

	if unsafe.Sizeof(leftType) > unsafe.Sizeof(rightType) {
		rightType, err := ConvertExpToType(rightType, leftType)
		return leftType, rightType, err
	} else {
		leftType, err := ConvertExpToType(leftType, rightType)
		return rightType, leftType, err
	}
}

type AccessLogData struct {
	TimeStamp   string
	UserName    string
	URI         string
	RequestBody string
	StatusCode  int
	Duration    int64
}
