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

package writer

import (
	"bytes"
	"errors"
	"fmt"

	. "github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/utils"

	log "github.com/sirupsen/logrus"
)

func ApplySearchToMatchFilterRawCsg(match *MatchFilter, col []byte) (bool, error) {

	if len(match.MatchWords) == 0 {
		return true, nil
	}

	if len(col) == 0 {
		return false, errors.New("column does not exist")
	}

	if col[0] != VALTYPE_ENC_SMALL_STRING[0] {
		return false, nil
	}

	idx := uint16(1) // for encoding type
	// next 2 bytes tell us the len of column
	clen := utils.BytesToUint16LittleEndian(col[idx : idx+COL_OFF_BYTE_SIZE])
	idx += COL_OFF_BYTE_SIZE

	// todo MatchWords struct can store bytes
	if match.MatchOperator == And {
		var foundQword bool = true
		if match.MatchType == MATCH_PHRASE {
			if match.Regexp != nil {
				foundQword = match.Regexp.Match(col[idx : idx+clen])
			} else {
				foundQword = utils.IsSubWordPresent(col[idx:idx+clen], match.MatchPhrase)
			}
		} else {
			for _, qword := range match.MatchWords {
				foundQword = utils.IsSubWordPresent(col[idx:idx+clen], []byte(qword))
				if !foundQword {
					break
				}
			}
		}
		return foundQword, nil
	}

	if match.MatchOperator == Or {
		var foundQword bool
		for _, qword := range match.MatchWords {
			foundQword = utils.IsSubWordPresent(col[idx:idx+clen], []byte(qword))
			if foundQword {
				return true, nil
			}
		}
		return false, nil
	}

	return false, nil
}

func ApplySearchToDictArrayFilter(col []byte, qValDte *DtypeEnclosure, rec []byte, fop FilterOperator, isRegexSearch bool,
	holderDte *DtypeEnclosure) (bool, error) {
	if qValDte == nil {
		return false, nil
	}

	if len(rec) == 0 || rec[0] != VALTYPE_DICT_ARRAY[0] {
		return false, nil
	} else if rec[0] == VALTYPE_DICT_ARRAY[0] {
		//loop over the dict arrray till we reach the end
		totalLen := utils.BytesToInt16LittleEndian(rec[1:])
		idx := uint16(3)
		var keyEquals, valEquals bool
		var err error
		for idx < uint16(totalLen) {
			strlen := utils.BytesToUint16LittleEndian(rec[idx : idx+2])
			idx += 2
			if int(strlen) == len(col) {
				keyEquals = bytes.Equal(rec[idx:idx+strlen], col)
			}
			idx += strlen
			if !keyEquals {
				switch rec[idx] {
				case VALTYPE_ENC_SMALL_STRING[0]:
					// one byte for type & two for reclen
					strlen := utils.BytesToUint16LittleEndian(rec[idx+1 : idx+3])
					idx += 3 + strlen
				case VALTYPE_ENC_BOOL[0]:
					strlen := utils.BytesToUint16LittleEndian(rec[idx+1 : idx+3])
					idx += 3 + strlen
				case VALTYPE_ENC_INT64[0], VALTYPE_ENC_FLOAT64[0]:
					strlen := utils.BytesToUint16LittleEndian(rec[idx+1 : idx+3])
					idx += 3 + strlen
				default:
					log.Errorf("ApplySearchToDictArrayFilter:SS_DT_ARRAY_DICT unknown type=%v\n", rec[idx])
					return false, errors.New("invalid rec type")
				}
				continue
			}
			switch rec[idx] {
			case VALTYPE_ENC_SMALL_STRING[0]:
				// one byte for type & two for reclen
				strlen := utils.BytesToUint16LittleEndian(rec[idx+1 : idx+3])
				idx += 3
				valEquals = bytes.Equal(rec[idx:idx+strlen], qValDte.StringValBytes)
				idx += strlen
			case VALTYPE_ENC_BOOL[0]:
				// valEquals, err = fopOnBool(rec[idx:], qValDte, fop)
				strlen := utils.BytesToUint16LittleEndian(rec[idx+1 : idx+3])
				idx += 3
				valEquals = bytes.Equal(rec[idx:idx+strlen], qValDte.StringValBytes)
				idx += strlen
			case VALTYPE_ENC_INT64[0]:
				strlen := utils.BytesToUint16LittleEndian(rec[idx+1 : idx+3])
				idx += 3
				valEquals = bytes.Equal(rec[idx:idx+strlen], qValDte.StringValBytes)
				idx += strlen
			case VALTYPE_ENC_FLOAT64[0]:
				strlen := utils.BytesToUint16LittleEndian(rec[idx+1 : idx+3])
				idx += 3
				valEquals = bytes.Equal(rec[idx:idx+strlen], qValDte.StringValBytes)
				idx += strlen
			default:
				log.Errorf("ApplySearchToDictArrayFilter:SS_DT_ARRAY_DICT unknown type=%v\n", rec[idx])
				return false, errors.New("invalid rec type")
			}
			if keyEquals && valEquals {
				return true, nil
			}
		}
		return keyEquals && valEquals, err
	}
	return false, nil
}

func ApplySearchToExpressionFilterSimpleCsg(qValDte *DtypeEnclosure, fop FilterOperator,
	col []byte, isRegexSearch bool, holderDte *DtypeEnclosure) (bool, error) {

	holderDte.Reset()

	return filterOpOnDataType(col, qValDte, fop, isRegexSearch, holderDte)
}

func isValTypeEncANumber(valTypeEnc byte) bool {
	switch valTypeEnc {
	case VALTYPE_ENC_INT8[0], VALTYPE_ENC_INT16[0], VALTYPE_ENC_INT32[0], VALTYPE_ENC_INT64[0],
		VALTYPE_ENC_UINT8[0], VALTYPE_ENC_UINT16[0], VALTYPE_ENC_UINT32[0], VALTYPE_ENC_UINT64[0],
		VALTYPE_ENC_FLOAT64[0]:
		return true
	}
	return false
}

func filterOpOnDataType(rec []byte, qValDte *DtypeEnclosure, fop FilterOperator,
	isRegexSearch bool, recDte *DtypeEnclosure) (bool, error) {

	if qValDte == nil {
		return false, nil
	}
	switch qValDte.Dtype {
	case SS_DT_STRING:
		if len(rec) == 0 || rec[0] != VALTYPE_ENC_SMALL_STRING[0] {
			// if we are doing a regex search on a number, we need to convert the number to string
			if isRegexSearch && isValTypeEncANumber(rec[0]) {
				return filterOpOnRecNumberEncType(rec, qValDte, fop, isRegexSearch, recDte)
			}
			return false, nil
		}
		return fopOnString(rec, qValDte, fop, isRegexSearch)
	case SS_DT_BOOL:
		if len(rec) == 0 || rec[0] != VALTYPE_ENC_BOOL[0] {
			return false, nil
		}
		return fopOnBool(rec, qValDte, fop)
	case SS_DT_SIGNED_NUM, SS_DT_UNSIGNED_NUM, SS_DT_FLOAT:
		return fopOnNumber(rec, qValDte, recDte, fop)
	case SS_DT_BACKFILL:
		return false, nil
	default:
		return false, errors.New("filterOpOnDataType:could not complete op")
	}
}

func filterOpOnRecNumberEncType(rec []byte, qValDte *DtypeEnclosure, fop FilterOperator,
	isRegexSearch bool, recDte *DtypeEnclosure) (bool, error) {

	if qValDte == nil || !isRegexSearch {
		return false, nil
	}

	validNumberType, err := getNumberRecDte(rec, recDte)
	if !validNumberType {
		return false, err
	}

	regexp := qValDte.GetRegexp()
	if regexp == nil {
		return false, errors.New("qValDte had nil regexp compilation")
	}

	var recValString string

	if recDte.Dtype == SS_DT_FLOAT {
		recValString = fmt.Sprintf("%f", recDte.FloatVal)
	} else if recDte.Dtype == SS_DT_UNSIGNED_NUM {
		recValString = fmt.Sprintf("%d", recDte.UnsignedVal)
	} else if recDte.Dtype == SS_DT_SIGNED_NUM {
		recValString = fmt.Sprintf("%d", recDte.SignedVal)
	} else {
		return false, errors.New("filterOpOnRecNumberEncType: unknown dtype")
	}

	if fop == Equals {
		return regexp.Match([]byte(recValString)), nil
	} else if fop == NotEquals {
		return !regexp.Match([]byte(recValString)), nil
	} else {
		return false, nil
	}

}

func fopOnString(rec []byte, qValDte *DtypeEnclosure, fop FilterOperator,
	isRegexSearch bool) (bool, error) {

	var sOff uint16 = 3
	switch fop {
	case Equals:
		if isRegexSearch {
			regexp := qValDte.GetRegexp()
			if regexp == nil {
				return false, errors.New("qValDte had nil regexp compilation")
			}
			return regexp.Match(rec[sOff:]), nil
		}
		if len(rec[sOff:]) != len(qValDte.StringVal) {
			return false, nil
		}
		return bytes.Equal(rec[sOff:], qValDte.StringValBytes), nil
	case NotEquals:
		if isRegexSearch {
			regexp := qValDte.GetRegexp()
			if regexp == nil {
				return false, errors.New("qValDte had nil regexp compilation")
			}
			return !regexp.Match(rec[sOff:]), nil
		}
		return !bytes.Equal(rec[sOff:], qValDte.StringValBytes), nil
	}
	return false, nil
}

func fopOnBool(rec []byte, qValDte *DtypeEnclosure, fop FilterOperator) (bool, error) {

	switch fop {
	case Equals:
		return rec[1] == qValDte.BoolVal, nil
	case NotEquals:
		return rec[1] != qValDte.BoolVal, nil
	}
	return false, nil
}

func getNumberRecDte(rec []byte, recDte *DtypeEnclosure) (bool, error) {
	if len(rec) == 0 {
		return false, nil
	}
	// first find recDte's Dtype and typecast it
	switch rec[0] {
	case VALTYPE_ENC_BACKFILL[0]:
		return false, nil
	case VALTYPE_ENC_BOOL[0]:
		return false, nil
	case VALTYPE_ENC_SMALL_STRING[0]:
		return false, nil
	case VALTYPE_ENC_INT8[0]:
		recDte.Dtype = SS_DT_SIGNED_NUM
		recDte.SignedVal = int64(rec[1])
	case VALTYPE_ENC_INT16[0]:
		recDte.Dtype = SS_DT_SIGNED_NUM
		recDte.SignedVal = int64(utils.BytesToInt16LittleEndian(rec[1:3]))
	case VALTYPE_ENC_INT32[0]:
		recDte.Dtype = SS_DT_SIGNED_NUM
		recDte.SignedVal = int64(utils.BytesToInt32LittleEndian(rec[1:5]))
	case VALTYPE_ENC_INT64[0]:
		recDte.Dtype = SS_DT_SIGNED_NUM
		recDte.SignedVal = utils.BytesToInt64LittleEndian(rec[1:9])
	case VALTYPE_ENC_UINT8[0]:
		recDte.Dtype = SS_DT_UNSIGNED_NUM
		recDte.UnsignedVal = uint64(rec[1])
	case VALTYPE_ENC_UINT16[0]:
		recDte.Dtype = SS_DT_UNSIGNED_NUM
		recDte.UnsignedVal = uint64(utils.BytesToUint16LittleEndian(rec[1:3]))
	case VALTYPE_ENC_UINT32[0]:
		recDte.Dtype = SS_DT_UNSIGNED_NUM
		recDte.UnsignedVal = uint64(utils.BytesToUint32LittleEndian(rec[1:5]))
	case VALTYPE_ENC_UINT64[0]:
		recDte.Dtype = SS_DT_UNSIGNED_NUM
		recDte.UnsignedVal = utils.BytesToUint64LittleEndian(rec[1:9])
	case VALTYPE_ENC_FLOAT64[0]:
		recDte.Dtype = SS_DT_FLOAT
		recDte.FloatVal = utils.BytesToFloat64LittleEndian(rec[1:9])
	case VALTYPE_DICT_ARRAY[0], VALTYPE_RAW_JSON[0]:
		return false, nil
	default:
		log.Errorf("fopOnNumber: dont know how to convert type=%v", rec[0])
		return false, errors.New("fopOnNumber: invalid rec type")
	}
	return true, nil
}

func fopOnNumber(rec []byte, qValDte *DtypeEnclosure,
	recDte *DtypeEnclosure, op FilterOperator) (bool, error) {

	validNumberType, err := getNumberRecDte(rec, recDte)
	if !validNumberType {
		return false, err
	}

	// now create a float (highest level for rec, only if we need to based on query
	if qValDte.Dtype == SS_DT_FLOAT && recDte.Dtype != SS_DT_FLOAT {
		// todo need to check err
		recDte.FloatVal, _ = dtu.ConvertToFloat(recDte.UnsignedVal, 64)
	}

	return compareNumberDte(recDte, qValDte, op)

}

/*
We never convert any qValDte params, caller's responsibility to store
all possible values in a heierarichal order.
We will only convert the recDte (stored val) to appropriate formats as needed
*/
func compareNumberDte(recDte *DtypeEnclosure, qValDte *DtypeEnclosure, op FilterOperator) (bool, error) {

	switch recDte.Dtype {
	case SS_DT_FLOAT:
		switch op {
		case Equals:
			return dtu.AlmostEquals(recDte.FloatVal, qValDte.FloatVal), nil
		case NotEquals:
			return !dtu.AlmostEquals(recDte.FloatVal, qValDte.FloatVal), nil
		case LessThan:
			return recDte.FloatVal < qValDte.FloatVal, nil
		case LessThanOrEqualTo:
			return recDte.FloatVal <= qValDte.FloatVal, nil
		case GreaterThan:
			return recDte.FloatVal > qValDte.FloatVal, nil
		case GreaterThanOrEqualTo:
			return recDte.FloatVal >= qValDte.FloatVal, nil
		}
	case SS_DT_UNSIGNED_NUM:
		switch op {
		case Equals:
			return recDte.UnsignedVal == qValDte.UnsignedVal, nil
		case NotEquals:
			return recDte.UnsignedVal != qValDte.UnsignedVal, nil
		case LessThan:
			//todo rec is unsigned but if qVal is signed and is negative num we need to handle that case
			return recDte.UnsignedVal < qValDte.UnsignedVal, nil
		case LessThanOrEqualTo:
			return recDte.UnsignedVal <= qValDte.UnsignedVal, nil
		case GreaterThan:
			return recDte.UnsignedVal > qValDte.UnsignedVal, nil
		case GreaterThanOrEqualTo:
			return recDte.UnsignedVal >= qValDte.UnsignedVal, nil
		}
	case SS_DT_SIGNED_NUM:
		switch op {
		case Equals:
			return recDte.SignedVal == qValDte.SignedVal, nil
		case NotEquals:
			return recDte.SignedVal != qValDte.SignedVal, nil
		case LessThan:
			return recDte.SignedVal < qValDte.SignedVal, nil
		case LessThanOrEqualTo:
			return recDte.SignedVal <= qValDte.SignedVal, nil
		case GreaterThan:
			return recDte.SignedVal > qValDte.SignedVal, nil
		case GreaterThanOrEqualTo:
			return recDte.SignedVal >= qValDte.SignedVal, nil
		}
	}
	log.Errorf("CompareNumbers: unknown op=%v or recDte=%v, qValDte=%v", op, recDte, qValDte)
	return false, errors.New("unknown op or dtype")
}
