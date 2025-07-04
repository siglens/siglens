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
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func IsNumeric(exp interface{}) bool {
	str := fmt.Sprint(exp)
	_, err := strconv.ParseFloat(str, 64)
	return err == nil

}

func GetCurrentTimeMillis() uint64 {
	return uint64(time.Now().UTC().UnixNano()) / uint64(time.Millisecond)
}

func GetLiteralFromString(identifier string) (v interface{}) {
	numericVal := strings.Replace(identifier, ",", "", -1)
	pInt, err := strconv.ParseInt(numericVal, 10, 64)
	if err == nil {
		return pInt
	}
	f, err := strconv.ParseFloat(numericVal, 64)
	if err == nil {
		return f
	}
	pBool, err := strconv.ParseBool(identifier)
	if err == nil {
		return pBool
	}
	return identifier
}

func CreateDtypeEnclosure(inVal interface{}, qid uint64) (*DtypeEnclosure, error) {
	var dte DtypeEnclosure

	if inVal == nil {
		dte.Dtype = SS_DT_BACKFILL
		return &dte, nil
	}

	//todo check for float convert errors and return them
	switch inVal := inVal.(type) {
	case string:
		dte.Dtype = SS_DT_STRING
		dte.StringVal = inVal

		if strings.Contains(inVal, "*") {
			rawRegex := dtu.ReplaceWildcardStarWithRegex(inVal)
			compiledRegex, err := regexp.Compile(rawRegex)
			if err != nil {
				log.Errorf("CreateDtypeEnclosure: Failed to compile regex for %s. This may cause search failures. Err: %v", rawRegex, err)
			}
			dte.SetRegexp(compiledRegex)
		}
	case []string:
		dte.Dtype = SS_DT_STRING_SLICE
		dte.StringSliceVal = inVal
		dte.StringVal = fmt.Sprintf("%v", inVal)
	case *regexp.Regexp:
		if inVal == nil {
			return nil, errors.New("CreateDtypeEnclosure: inVal is nil Regexp")
		}
		dte.Dtype = SS_DT_STRING
		dte.StringVal = inVal.String()
		dte.SetRegexp(inVal)
	case bool:
		dte.Dtype = SS_DT_BOOL
		bVal := inVal
		if bVal {
			dte.BoolVal = 1
		} else {
			dte.BoolVal = 0
		}
		dte.StringVal = fmt.Sprint(inVal)
	case uint8:
		dte.Dtype = SS_DT_UNSIGNED_NUM
		dte.UnsignedVal = uint64(inVal)
		dte.SignedVal = int64(inVal)
		dte.FloatVal, dte.StringVal, _ = dtu.ConvertToFloatAndReturnString(dte.UnsignedVal, 64)
	case uint16:
		dte.Dtype = SS_DT_UNSIGNED_NUM
		dte.UnsignedVal = uint64(inVal)
		dte.SignedVal = int64(inVal)
		dte.FloatVal, dte.StringVal, _ = dtu.ConvertToFloatAndReturnString(dte.UnsignedVal, 64)
	case uint32:
		dte.Dtype = SS_DT_UNSIGNED_NUM
		dte.UnsignedVal = uint64(inVal)
		dte.SignedVal = int64(inVal)
		dte.FloatVal, dte.StringVal, _ = dtu.ConvertToFloatAndReturnString(dte.UnsignedVal, 64)
	case uint:
		dte.Dtype = SS_DT_UNSIGNED_NUM
		dte.UnsignedVal = uint64(inVal)
		dte.SignedVal = int64(inVal)
		dte.FloatVal, dte.StringVal, _ = dtu.ConvertToFloatAndReturnString(dte.UnsignedVal, 64)
	case uint64:
		dte.Dtype = SS_DT_UNSIGNED_NUM
		dte.UnsignedVal = uint64(inVal)
		dte.SignedVal = int64(inVal)
		dte.FloatVal, dte.StringVal, _ = dtu.ConvertToFloatAndReturnString(dte.UnsignedVal, 64)
	case int8:
		dte.Dtype = SS_DT_SIGNED_NUM
		dte.SignedVal = int64(inVal)
		dte.UnsignedVal = uint64(dte.SignedVal)
		dte.FloatVal, dte.StringVal, _ = dtu.ConvertToFloatAndReturnString(dte.SignedVal, 64)
	case int16:
		dte.Dtype = SS_DT_SIGNED_NUM
		dte.SignedVal = int64(inVal)
		dte.UnsignedVal = uint64(dte.SignedVal)
		dte.FloatVal, dte.StringVal, _ = dtu.ConvertToFloatAndReturnString(dte.SignedVal, 64)
	case int32:
		dte.Dtype = SS_DT_SIGNED_NUM
		dte.SignedVal = int64(inVal)
		dte.UnsignedVal = uint64(dte.SignedVal)
		dte.FloatVal, dte.StringVal, _ = dtu.ConvertToFloatAndReturnString(dte.SignedVal, 64)
	case int:
		dte.Dtype = SS_DT_SIGNED_NUM
		dte.SignedVal = int64(inVal)
		dte.UnsignedVal = uint64(dte.SignedVal)
		dte.FloatVal, dte.StringVal, _ = dtu.ConvertToFloatAndReturnString(dte.SignedVal, 64)
	case int64:
		dte.Dtype = SS_DT_SIGNED_NUM
		dte.SignedVal = int64(inVal)
		dte.UnsignedVal = uint64(dte.SignedVal)
		dte.FloatVal, dte.StringVal, _ = dtu.ConvertToFloatAndReturnString(dte.SignedVal, 64)
	case float64:
		dte.Dtype = SS_DT_FLOAT
		dte.FloatVal = inVal
		dte.StringVal = fmt.Sprint(inVal)
	case json.Number:
		enclosureFromJsonNumber(inVal, &dte)
	default:
		log.Errorf("qid=%d, CreateDtypeEnclosure: could not convert unknown type=%T", qid, inVal)
		return &dte, errors.New("could not convert unknown type")
	}

	return &dte, nil
}

func (dte *DtypeEnclosure) UpdateRegexp(caseInsensitive bool, isTerm bool) {
	if dte == nil {
		return
	}

	if dte.Dtype != SS_DT_STRING {
		return
	}

	if strings.Contains(dte.StringVal, "*") || isTerm {

		rawRegex := dtu.SPLToRegex(dte.StringVal, caseInsensitive, isTerm)

		compiledRegex, err := regexp.Compile(rawRegex)
		if err != nil {
			log.Errorf("UpdateRegexp: Failed to compile regex for %s. This may cause search failures. Err: %v", rawRegex, err)
		}
		dte.SetRegexp(compiledRegex)
	}
}

func enclosureFromJsonNumber(num json.Number, dte *DtypeEnclosure) {

	numstr := string(num)
	dte.StringVal = numstr
	numType, intVal, uintVal, fltVal := GetNumberTypeAndVal(numstr)

	switch numType {
	case SS_INT8, SS_INT16, SS_INT32, SS_INT64:
		dte.Dtype = SS_DT_SIGNED_NUM
		dte.SignedVal = intVal
		dte.UnsignedVal = uint64(intVal)
		dte.FloatVal = float64(intVal)
	case SS_UINT8, SS_UINT16, SS_UINT32, SS_UINT64:
		dte.Dtype = SS_DT_UNSIGNED_NUM
		dte.UnsignedVal = uintVal
		dte.SignedVal = int64(uintVal)
		dte.FloatVal = float64(uintVal)
	case SS_FLOAT64:
		dte.Dtype = SS_DT_FLOAT
		dte.SignedVal = int64(fltVal)
		dte.UnsignedVal = uint64(fltVal)
		dte.FloatVal = fltVal
	}
}

func BytesToMiB[T ~uint64 | ~float64](bytes T) T {
	return bytes / 1048576
}

// converts the input byte slice to a string representation of all read values
// returns array of strings with groupBy values
func ConvertGroupByKey(rec []byte) ([]string, error) {
	resultArr, err := ConvertGroupByKeyFromBytes(rec)
	if err != nil {
		return nil, err
	}

	strArr := make([]string, len(resultArr))

	for i, v := range resultArr {
		switch v := v.(type) {
		case []byte:
			strArr[i] = string(v)
		case string:
			strArr[i] = v
		default:
			strArr[i] = fmt.Sprintf("%v", v)
		}
	}

	return strArr, nil
}

func ConvertGroupByKeyFromBytes(rec []byte) ([]interface{}, error) {
	var resultArr []interface{}
	idx := 0
	for idx < len(rec) {
		switch rec[idx] {
		case VALTYPE_ENC_SMALL_STRING[0]:
			idx += 1
			len := int(utils.BytesToUint16LittleEndian(rec[idx:]))
			idx += 2
			resultArr = append(resultArr, string(rec[idx:idx+len]))
			idx += len
		case VALTYPE_ENC_BOOL[0]:
			resultArr = append(resultArr, rec[idx+1] != 0)
			idx += 2
		case VALTYPE_ENC_INT8[0]:
			resultArr = append(resultArr, int8(rec[idx+1]))
			idx += 2
		case VALTYPE_ENC_INT16[0]:
			resultArr = append(resultArr, utils.BytesToInt16LittleEndian(rec[idx+1:]))
			idx += 3
		case VALTYPE_ENC_INT32[0]:
			resultArr = append(resultArr, utils.BytesToInt32LittleEndian(rec[idx+1:]))
			idx += 5
		case VALTYPE_ENC_INT64[0]:
			resultArr = append(resultArr, utils.BytesToInt64LittleEndian(rec[idx+1:]))
			idx += 9
		case VALTYPE_ENC_UINT8[0]:
			resultArr = append(resultArr, uint8(rec[idx+1]))
			idx += 2
		case VALTYPE_ENC_UINT16[0]:
			resultArr = append(resultArr, utils.BytesToUint16LittleEndian(rec[idx+1:]))
			idx += 3
		case VALTYPE_ENC_UINT32[0]:
			resultArr = append(resultArr, utils.BytesToUint32LittleEndian(rec[idx+1:]))
			idx += 5
		case VALTYPE_ENC_UINT64[0]:
			resultArr = append(resultArr, utils.BytesToUint64LittleEndian(rec[idx+1:]))
			idx += 9
		case VALTYPE_ENC_FLOAT64[0]:
			resultArr = append(resultArr, utils.BytesToFloat64LittleEndian(rec[idx+1:]))
			idx += 9
		case VALTYPE_ENC_BACKFILL[0]:
			resultArr = append(resultArr, nil)
			idx += 1
		default:
			log.Errorf("ConvertRowEncodingToInterface: don't know how to convert type=%v, idx: %v", rec[idx], idx)
			return nil, fmt.Errorf("ConvertRowEncodingToInterface: don't know how to convert type=%v, idx: %v",
				rec[idx], idx)
		}
	}
	return resultArr, nil
}

// IsNumTypeAgg checks if aggregate function requires numeric type data
func IsNumTypeAgg(fun AggregateFunctions) bool {
	switch fun {
	case Avg, Min, Max, Sum, Range:
		return true
	default:
		return false
	}
}
