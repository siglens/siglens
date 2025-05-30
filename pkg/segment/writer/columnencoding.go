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
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func EncodeDictionaryColumn(columnValueMap map[sutils.CValueDictEnclosure][]uint16, colRis map[string]*RangeIndex, recNum uint16) ([]byte, uint32) {
	columnValueSummary := make([]byte, sutils.WIP_SIZE)
	var idx uint32 = 0

	noOfColumnValues := uint16(len(columnValueMap))
	utils.Uint16ToBytesLittleEndianInplace(noOfColumnValues, columnValueSummary[idx:])
	idx += 2

	for key, val := range columnValueMap {
		switch key.Dtype {
		case sutils.SS_DT_STRING:
			columnValueSummary[idx] = byte(sutils.SS_DT_STRING)
			idx += 1
			colValue, err := key.GetValue()
			if err != nil {
				log.Errorf("EncodeDictionaryColumn: Failed to get value of %v; err: %v", key, err)
				continue
			}
			n := uint16(len(colValue.(string)))

			utils.Uint16ToBytesLittleEndianInplace(n, columnValueSummary[idx:])
			idx += 2

			copy(columnValueSummary[idx:], colValue.(string))
			idx += uint32(n)

		case sutils.SS_DT_BOOL:
			columnValueSummary[idx] = byte(sutils.SS_DT_BOOL)
			idx += 1
			colValue, err := key.GetValue()
			if err != nil {
				log.Errorf("EncodeDictionaryColumn: Failed to get value of %v; err: %v", key, err)
				continue
			}

			copy(columnValueSummary[idx:], utils.BoolToBytesLittleEndian(colValue.(bool)))
			idx += 1

		case sutils.SS_DT_UNSIGNED_NUM:
			columnValueSummary[idx] = byte(sutils.SS_DT_UNSIGNED_NUM)
			idx += 1
			colValue, err := key.GetValue()
			if err != nil {
				log.Errorf("EncodeDictionaryColumn: Failed to get value of %v; err: %v", key, err)
				continue
			}

			utils.Uint64ToBytesLittleEndianInplace(colValue.(uint64), columnValueSummary[idx:])
			idx += 8

		case sutils.SS_DT_SIGNED_NUM:
			columnValueSummary[idx] = byte(sutils.SS_DT_SIGNED_NUM)
			idx += 1
			colValue, err := key.GetValue()
			if err != nil {
				log.Errorf("EncodeDictionaryColumn: Failed to get value of %v; err: %v", key, err)
				continue
			}

			utils.Int64ToBytesLittleEndianInplace(colValue.(int64), columnValueSummary[idx:])
			idx += 8

		case sutils.SS_DT_FLOAT:
			columnValueSummary[idx] = byte(sutils.SS_DT_FLOAT)
			idx += 1
			colValue, err := key.GetValue()
			if err != nil {
				log.Errorf("EncodeDictionaryColumn: Failed to get value of %v; err: %v", key, err)
				continue
			}

			utils.Float64ToBytesLittleEndianInplace(colValue.(float64), columnValueSummary[idx:])
			idx += 8
		case sutils.SS_DT_USIGNED_32_NUM:
			columnValueSummary[idx] = byte(sutils.SS_DT_USIGNED_32_NUM)
			idx += 1
			colValue, err := key.GetValue()
			if err != nil {
				log.Errorf("EncodeDictionaryColumn: Failed to get value of %v; err: %v", key, err)
				continue
			}

			utils.Uint32ToBytesLittleEndianInplace(colValue.(uint32), columnValueSummary[idx:])
			idx += 4
		case sutils.SS_DT_SIGNED_32_NUM:
			columnValueSummary[idx] = byte(sutils.SS_DT_SIGNED_32_NUM)
			idx += 1
			colValue, err := key.GetValue()
			if err != nil {
				log.Errorf("EncodeDictionaryColumn: Failed to get value of %v; err: %v", key, err)
				continue
			}

			utils.Int32ToBytesLittleEndianInplace(colValue.(int32), columnValueSummary[idx:])
			idx += 4
		case sutils.SS_DT_USIGNED_16_NUM:
			columnValueSummary[idx] = byte(sutils.SS_DT_SIGNED_32_NUM)
			idx += 1
			colValue, err := key.GetValue()
			if err != nil {
				log.Errorf("EncodeDictionaryColumn: Failed to get value of %v; err: %v", key, err)
				continue
			}

			utils.Uint16ToBytesLittleEndianInplace(colValue.(uint16), columnValueSummary[idx:])
			idx += 2
		case sutils.SS_DT_SIGNED_16_NUM:
			columnValueSummary[idx] = byte(sutils.SS_DT_SIGNED_32_NUM)
			idx += 1
			colValue, err := key.GetValue()
			if err != nil {
				log.Errorf("EncodeDictionaryColumn: Failed to get value of %v; err: %v", key, err)
				continue
			}

			utils.Int16ToBytesLittleEndianInplace(colValue.(int16), columnValueSummary[idx:idx+2])
			idx += 2
		case sutils.SS_DT_USIGNED_8_NUM:
			columnValueSummary[idx] = byte(sutils.SS_DT_SIGNED_8_NUM)
			idx += 1
			colValue, err := key.GetValue()
			if err != nil {
				log.Errorf("EncodeDictionaryColumn: Failed to get value of %v; err: %v", key, err)
				continue
			}

			copy(columnValueSummary[idx:], []byte{colValue.(uint8)})
			idx += 1
		case sutils.SS_DT_SIGNED_8_NUM:
			columnValueSummary[idx] = byte(sutils.SS_DT_SIGNED_8_NUM)
			idx += 1
			colValue, err := key.GetValue()
			if err != nil {
				log.Errorf("EncodeDictionaryColumn: Failed to get value of %v; err: %v", key, err)
				continue
			}

			copy(columnValueSummary[idx:], []byte{byte(colValue.(int8))})
			idx += 1
		default:
			log.Errorf("EncodeDictionaryColumn: Unsupported data type: %v", key.Dtype)
		}
		utils.Uint16ToBytesLittleEndianInplace(uint16(len(val)), columnValueSummary[idx:])
		idx += 2

		for _, value := range val {
			utils.Uint16ToBytesLittleEndianInplace(value, columnValueSummary[idx:])
			idx += 2
		}
	}

	compressed := encoder.EncodeAll(columnValueSummary[0:idx], make([]byte, 0, idx))
	return compressed, idx
}

func DecodeDictionaryColumn(encodedBytes []byte) map[sutils.CValueDictEnclosure][]uint16 {

	encodedBytes, err := decoder.DecodeAll(encodedBytes, make([]byte, 0, len(encodedBytes)))

	if err != nil {
		log.Errorf("DecodeDictionaryColumn: Failed to decompress, error: %+v", err)
	}

	columnValueMap := make(map[sutils.CValueDictEnclosure][]uint16)
	var idx uint32 = 0

	noOfColumnValues := utils.BytesToUint16LittleEndian(encodedBytes[0:2])
	idx += 2

	for noOfColumnValues > 0 {
		var colCVEnclosure sutils.CValueDictEnclosure
		colCVEnclosure.Dtype = sutils.SS_DTYPE(encodedBytes[idx])
		idx += 1
		switch colCVEnclosure.Dtype {
		case sutils.SS_DT_STRING:
			strLen := uint32(utils.BytesToUint16LittleEndian(encodedBytes[idx:(idx + 2)]))
			idx += 2
			colCVEnclosure.CValString = string(encodedBytes[idx:(idx + strLen)])
			idx += strLen
		case sutils.SS_DT_BOOL:
			colCVEnclosure.CValBool = utils.BytesToBoolLittleEndian([]byte{encodedBytes[idx]})
			idx += 1
		case sutils.SS_DT_UNSIGNED_NUM:
			colCVEnclosure.CValUInt64 = utils.BytesToUint64LittleEndian(encodedBytes[idx:(idx + 8)])
			idx += 8
		case sutils.SS_DT_SIGNED_NUM:
			colCVEnclosure.CValInt64 = utils.BytesToInt64LittleEndian(encodedBytes[idx:(idx + 8)])
			idx += 8
		case sutils.SS_DT_FLOAT:
			colCVEnclosure.CValFloat64 = utils.BytesToFloat64LittleEndian(encodedBytes[idx:(idx + 8)])
			idx += 8
		case sutils.SS_DT_USIGNED_32_NUM:
			colCVEnclosure.CValUInt32 = utils.BytesToUint32LittleEndian(encodedBytes[idx:(idx + 4)])
			idx += 4
		case sutils.SS_DT_SIGNED_32_NUM:
			colCVEnclosure.CValInt32 = utils.BytesToInt32LittleEndian(encodedBytes[idx:(idx + 4)])
			idx += 4
		case sutils.SS_DT_USIGNED_16_NUM:
			colCVEnclosure.CValUInt16 = utils.BytesToUint16LittleEndian(encodedBytes[idx:(idx + 2)])
			idx += 2
		case sutils.SS_DT_SIGNED_16_NUM:
			colCVEnclosure.CValInt16 = utils.BytesToInt16LittleEndian(encodedBytes[idx:(idx + 2)])
			idx += 2
		case sutils.SS_DT_USIGNED_8_NUM:
			colCVEnclosure.CValUInt = encodedBytes[idx+1]
			idx += 1
		case sutils.SS_DT_SIGNED_8_NUM:
			colCVEnclosure.CValInt = int8(encodedBytes[idx+1])
			idx += 1
		default:
			log.Errorf("DecodeDictionaryColumn: Unsupported data type: %v", colCVEnclosure.Dtype)
		}

		valuesLen := utils.BytesToUint16LittleEndian(encodedBytes[idx:(idx + 2)])
		idx += 2

		valSlice := make([]uint16, valuesLen)
		for id := 0; valuesLen > 0; id++ {
			valSlice[id] = utils.BytesToUint16LittleEndian(encodedBytes[idx:(idx + 2)])
			idx += 2
			valuesLen -= 1
		}

		columnValueMap[colCVEnclosure] = valSlice
		noOfColumnValues -= 1
	}

	return columnValueMap
}
