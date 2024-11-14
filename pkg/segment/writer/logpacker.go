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

package writer

import (
	"bytes"
	"fmt"

	jp "github.com/buger/jsonparser"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func ParseRawJsonObject(currKey string, data []byte, tsKey *string,
	jsParsingStackbuf []byte, ple *ParsedLogEvent) error {

	handler := func(key []byte, value []byte, valueType jp.ValueType, off int) error {
		// Maybe push some state onto a stack here?
		var finalKey string
		var err error

		if currKey == "" {
			finalKey = string(key)
		} else {
			finalKey = fmt.Sprintf("%s.%s", currKey, key)
		}
		switch valueType {
		case jp.Object:
			err = ParseRawJsonObject(finalKey, value, tsKey, jsParsingStackbuf, ple)
			if err != nil {
				return fmt.Errorf("parseRawJsonObject: obj currKey: %v, err: %v", currKey, err)
			}
		case jp.Array:
			err = parseNonJaegerRawJsonArray(finalKey, value, tsKey, jsParsingStackbuf, ple)
			if err != nil {
				return fmt.Errorf("parseRawJsonObject: arr currKey: %v, err: %v", currKey, err)
			}
		case jp.String:
			// We are performing a shallow copy on this unescaped value in ple, which can result in values being overwritten later as the buffer is reused.
			// Pass nil instead of the buffer so that jp.Unescape allocates a new buffer for each value.
			firstBackslash := bytes.IndexByte(value, '\\')
			if firstBackslash != -1 {
				valUnescaped, err := jp.Unescape(value, nil)
				if err != nil {
					return fmt.Errorf("parseRawJsonObject: failed to unescape currKey: %v, err: %v",
						currKey, err)
				}
				parseSingleString(finalKey, tsKey, valUnescaped, ple)
			} else {
				parseSingleString(finalKey, tsKey, value, ple)
			}
		case jp.Number:
			numVal, err := jp.ParseInt(value)
			if err != nil {
				fltVal, err := jp.ParseFloat(value)
				if err != nil {
					return fmt.Errorf("parseRawJsonObject: flt currKey: %v, err: %v", currKey, err)
				}
				parseSingleNumber(finalKey, fltVal, tsKey, value, ple)
				return nil
			}
			parseSingleNumber(finalKey, numVal, tsKey, value, ple)
		case jp.Boolean:
			boolVal, err := jp.ParseBoolean(value)
			if err != nil {
				return fmt.Errorf("parseRawJsonObject: bool currKey: %v, err: %v", currKey, err)
			}
			parseSingleBool(finalKey, boolVal, tsKey, ple)
		case jp.Null:
			parseSingleNull(finalKey, tsKey, ple)
		default:
			return fmt.Errorf("parseRawJsonObject: currKey: %v, received unknown type of %+s", currKey, valueType)
		}
		return nil
	}
	err := jp.ObjectEach(data, handler)
	return err
}

func parseNonJaegerRawJsonArray(currKey string, data []byte, tsKey *string,
	jsParsingStackbuf []byte, ple *ParsedLogEvent) error {

	i := 0
	var finalErr error
	_, aErr := jp.ArrayEach(data, func(value []byte, valueType jp.ValueType, offset int, err error) {
		var finalKey string
		var encErr error
		if currKey == "" {
			finalKey = fmt.Sprintf("%d", i)
		} else {
			finalKey = fmt.Sprintf("%s.%d", currKey, i)
		}
		i++
		switch valueType {
		case jp.Object:
			encErr = ParseRawJsonObject(finalKey, value, tsKey, jsParsingStackbuf, ple)
			if encErr != nil {
				finalErr = encErr
				return
			}
		case jp.Array:
			encErr = parseNonJaegerRawJsonArray(finalKey, value, tsKey, jsParsingStackbuf, ple)
			if encErr != nil {
				finalErr = encErr
				return
			}
		case jp.String:
			// We are performing a shallow copy on this unescaped value in ple, which can result in values being overwritten later as the buffer is reused.
			// Pass nil instead of the buffer so that jp.Unescape allocates a new buffer for each value.
			firstBackslash := bytes.IndexByte(value, '\\')
			if firstBackslash != -1 {
				valUnescaped, encErr := jp.Unescape(value, nil)
				if err != nil {
					finalErr = encErr
					return
				}
				parseSingleString(finalKey, tsKey, valUnescaped, ple)
			} else {
				parseSingleString(finalKey, tsKey, value, ple)
			}
		case jp.Number:
			numVal, encErr := jp.ParseInt(value)
			if encErr != nil {
				fltVal, encErr := jp.ParseFloat(value)
				if encErr != nil {
					finalErr = encErr
					return
				}
				parseSingleNumber(finalKey, fltVal, tsKey, value, ple)
				return
			}
			parseSingleNumber(finalKey, numVal, tsKey, value, ple)
		case jp.Boolean:
			boolVal, encErr := jp.ParseBoolean(value)
			if encErr != nil {
				finalErr = encErr
				return
			}
			parseSingleBool(finalKey, boolVal, tsKey, ple)
		case jp.Null:
			parseSingleNull(finalKey, tsKey, ple)
		default:
			finalErr = fmt.Errorf("received unknown type of %+s", valueType)
			return
		}
	})
	if aErr != nil {
		finalErr = aErr
	}
	return finalErr
}

func parseSingleString(key string, tsKey *string, valBytes []byte, ple *ParsedLogEvent) {

	if key == *tsKey {
		return
	}

	ple.MakeSpaceForNewColumn()

	ple.allCnames[ple.numCols] = key

	cbufidx := 0
	copy(ple.allCvalsTypeLen[ple.numCols][cbufidx:], VALTYPE_ENC_SMALL_STRING[:])
	cbufidx += 1
	n := uint16(len(valBytes))
	utils.Uint16ToBytesLittleEndianInplace(n, ple.allCvalsTypeLen[ple.numCols][cbufidx:])
	ple.allCvals[ple.numCols] = valBytes

	ple.numCols++
}

func parseSingleBool(key string, val bool, tsKey *string, ple *ParsedLogEvent) {
	if key == *tsKey {
		return
	}

	ple.MakeSpaceForNewColumn()

	ple.allCnames[ple.numCols] = key
	copy(ple.allCvalsTypeLen[ple.numCols][0:], VALTYPE_ENC_BOOL[:])
	ple.allCvals[ple.numCols] = utils.BoolToBytesLittleEndian(val)
	ple.numCols++
}

func parseSingleNull(key string, tsKey *string, ple *ParsedLogEvent) {
	if key == *tsKey {
		return
	}

	ple.MakeSpaceForNewColumn()

	ple.allCnames[ple.numCols] = key
	copy(ple.allCvalsTypeLen[ple.numCols][0:], VALTYPE_ENC_BACKFILL[:])
	ple.numCols++
}

func parseSingleNumber(key string, value interface{}, tsKey *string, valBytes []byte, ple *ParsedLogEvent) {
	if key == *tsKey {
		return
	}

	ple.MakeSpaceForNewColumn()

	switch cval := value.(type) {
	case float64:
		parsedEncJsonNumber(key, SS_FLOAT64, FPARM_INT64, FPARM_UINT64, cval, ple,
			0)
	case int64:
		parsedEncJsonNumber(key, SS_INT64, cval, FPARM_UINT64, FPARM_FLOAT64, ple,
			0)
	default:
		log.Errorf("parseSingleNumber: Tried to encode a non int/float value! value=%+v", cval)
		return
	}

	ple.allCnames[ple.numCols] = key
	ple.numCols++

}

func parsedEncJsonNumber(key string, numType SS_IntUintFloatTypes, intVal int64, uintVal uint64,
	fltVal float64, ple *ParsedLogEvent, idx uint32) uint32 {

	var valSize uint32

	switch numType {
	case SS_INT64:
		copy(ple.allCvalsTypeLen[ple.numCols][0:], VALTYPE_ENC_INT64[:])
		utils.Int64ToBytesLittleEndianInplace(intVal, ple.allCvalsTypeLen[ple.numCols][1:])
		valSize = 1 + 8
	case SS_UINT64:
		copy(ple.allCvalsTypeLen[ple.numCols][0:], VALTYPE_ENC_UINT64[:])
		utils.Uint64ToBytesLittleEndianInplace(uintVal, ple.allCvalsTypeLen[ple.numCols][1:])
		valSize = 1 + 8
	case SS_FLOAT64:
		copy(ple.allCvalsTypeLen[ple.numCols][0:], VALTYPE_ENC_FLOAT64[:])
		utils.Float64ToBytesLittleEndianInplace(fltVal, ple.allCvalsTypeLen[ple.numCols][1:])
		valSize = 1 + 8
	default:
		log.Errorf("parsedEncJsonNumber: unknown numType: %v", numType)
	}

	return valSize
}
