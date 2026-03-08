// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"fmt"
	"strconv"
	"strings"
)

func GetNumberTypeAndVal(numstr string) (SS_IntUintFloatTypes, int64, uint64, float64) {
	firstchar := numstr[0]
	var numType SS_IntUintFloatTypes
	var intVal int64
	var uintVal uint64
	var fltVal float64
	var ok bool

	//ToDo : Assume float values are specified using decimal point and do not contain e/E
	//if strings.Contains(strings.ToLower(numstr), "e") {
	//  numstr = convertFloatToInt(numstr)
	//}
	if firstchar == '-' {
		if numType, intVal, ok = getIntTypeAndVal(numstr); ok {
			//fmt.Println("signed int", numType, intVal, numstr)
			return numType, intVal, uintVal, fltVal
		} else if numType, fltVal, ok = getFloatTypeAndVal(numstr); ok {
			//fmt.Println("Float", numType, fltVal, numstr)
			return numType, intVal, uintVal, fltVal
		}
	} else {
		if numType, uintVal, ok = getUintTypeAndVal(numstr); ok {
			//fmt.Println("Unsigned int", numType, uintVal, numstr)
			return numType, intVal, uintVal, fltVal
		} else if numType, fltVal, ok = getFloatTypeAndVal(numstr); ok {
			//fmt.Println("Float", numType, fltVal, numstr)
			return numType, intVal, uintVal, fltVal
		}
	}
	return numType, intVal, uintVal, fltVal
}
func getFloatTypeAndVal(strnum string) (SS_IntUintFloatTypes, float64, bool) {
	if fltval, err := strconv.ParseFloat(strnum, 64); err == nil {
		//fmt.Printf("packRecord: got float=%v\n", valconv)
		if fltval == 0 {
			return SS_UINT8, fltval, true
		} else {
			return SS_FLOAT64, fltval, true
		}
	} else {
		return -1, 0, false
	}
}

func getUintTypeAndVal(strnum string) (SS_IntUintFloatTypes, uint64, bool) {
	if bigval, err := strconv.ParseUint(strnum, 10, 64); err == nil {
		switch {
		case bigval == 0:
			return SS_UINT8, bigval, true
		case bigval <= 255:
			return SS_UINT8, bigval, true
		case bigval <= 65535:
			return SS_UINT16, bigval, true
		case bigval <= 4294967295:
			return SS_UINT32, bigval, true
		default:
			return SS_UINT64, bigval, true
		}
	}
	return -1, 0, false
}

func getIntTypeAndVal(strnum string) (SS_IntUintFloatTypes, int64, bool) {
	if bigval, err := strconv.ParseInt(strnum, 10, 64); err == nil {
		switch {
		case bigval == 0:
			return SS_UINT8, bigval, true
		case bigval >= -128 && bigval <= 127:
			return SS_INT8, bigval, true
		case bigval >= -32768 && bigval <= 32767:
			return SS_INT16, bigval, true
		case bigval >= -2147483648 && bigval <= 2147483647:
			return SS_INT32, bigval, true
		default:
			return SS_INT64, bigval, true
		}
	}
	return -1, 0, false
}

func ParseHumanizedValueToFloat(humanizedValue interface{}) (float64, error) {
	humanizedValueStr := fmt.Sprintf("%v", humanizedValue)

	if humanizedValueStr == "" {
		return 0, fmt.Errorf("ParseHumanizedValueToFloat: humanizedValue is empty")
	}

	cleanedStr := strings.ReplaceAll(humanizedValueStr, ",", "")

	floatVal, err := strconv.ParseFloat(cleanedStr, 64)
	if err != nil {
		return 0, fmt.Errorf("ParseHumanizedValueToFloat: error parsing float value. Error: %v", err)
	}

	return floatVal, nil
}
