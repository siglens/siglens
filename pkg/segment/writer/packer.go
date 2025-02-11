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
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	jp "github.com/buger/jsonparser"
	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/blob/ssutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	. "github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	statswriter "github.com/siglens/siglens/pkg/segment/writer/stats"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	bbp "github.com/valyala/bytebufferpool"
)

var wipCardLimit uint16 = 501

const MaxDeEntries = 1002 // this should be atleast 2x of wipCardLimit

const FPARM_INT64 = int64(0)
const FPARM_UINT64 = uint64(0)
const FPARM_FLOAT64 = float64(0)

/*
	   Each column stored in its own columnar file
	   Each column file format:
		  [ValType-1 1B] [OptionalStringVal-Len-1 2B] [ActualValue-1]
		  [ValType-2 1B] [OptionalStringVal-Len-2 2B] [ActualValue-2]


	   This function should not be called by itself, must be called via locks

	   This function assumes that the record_json has been flattened

	   foundColsInRecord is a map[string]bool of all columns in the WIPBlock. New columns will be added to this map
	   The values of this map will be set to false before returning for subsequent calls. This lets us re-use the same map across WIPBlock

	   returns :
		  1) Max index amongst the columns
		  3) bool if this record matched the column conditions in PQColTracker
		  3) error
*/
func (ss *SegStore) EncodeColumns(rawData []byte, recordTime uint64, tsKey *string,
	signalType segutils.SIGNAL_TYPE,
	cnameCacheByteHashToStr map[uint64]string,
	jsParsingStackbuf []byte) (bool, error) {

	var matchedCol = false

	ss.encodeTime(recordTime, tsKey)
	var err error
	matchedCol, err = ss.encodeRawJsonObject("", rawData, tsKey, matchedCol,
		signalType, cnameCacheByteHashToStr, jsParsingStackbuf)
	if err != nil {
		log.Errorf("Failed to encode json object! err: %+v", err)
		return matchedCol, err
	}

	for colName, foundCol := range ss.wipBlock.columnsInBlock {
		if foundCol {
			ss.wipBlock.columnsInBlock[colName] = false
			continue
		}
		colWip, ok := ss.wipBlock.colWips[colName]
		if !ok {
			log.Errorf("EncodeColumns: tried to add a backfill for a column with no colWip! %v. This should not happen", colName)
			continue
		}
		colWip.cstartidx = colWip.cbufidx
		colWip.cbuf.Append(VALTYPE_ENC_BACKFILL[:])
		colWip.cbufidx += 1
		ss.updateColValueSizeInAllSeenColumns(colName, 1)
		// also do backfill dictEnc for this recnum
		ss.checkAddDictEnc(colWip, VALTYPE_ENC_BACKFILL[:], ss.wipBlock.blockSummary.RecCount,
			colWip.cbufidx-1, true)
	}

	return matchedCol, nil
}

func (ss *SegStore) encodeRawJsonObject(currKey string, data []byte, tsKey *string,
	matchedCol bool, signalType segutils.SIGNAL_TYPE,
	cnameCacheByteHashToStr map[uint64]string, jsParsingStackbuf []byte) (bool, error) {

	handler := func(key []byte, value []byte, valueType jp.ValueType, off int) error {
		// Maybe push some state onto a stack here?
		var finalKey string
		var err error

		if currKey == "" {
			cnameHash := xxhash.Sum64(key)
			cnameVal, ok := cnameCacheByteHashToStr[cnameHash]
			if ok {
				finalKey = cnameVal
			} else {
				finalKey = string(key)
				cnameCacheByteHashToStr[cnameHash] = finalKey
			}
		} else {
			finalKey = fmt.Sprintf("%s.%s", currKey, key)
		}
		switch valueType {
		case jp.Object:
			matchedCol, err = ss.encodeRawJsonObject(finalKey, value, tsKey,
				matchedCol, signalType, cnameCacheByteHashToStr, jsParsingStackbuf)
			if err != nil {
				return fmt.Errorf("encodeRawJsonObject: obj currKey: %v, err: %v", currKey, err)
			}
		case jp.Array:
			if signalType == SIGNAL_JAEGER_TRACES {

				matchedCol, err = ss.encodeRawJsonArray(finalKey, value, tsKey, matchedCol, signalType)
			} else {
				matchedCol, err = ss.encodeNonJaegerRawJsonArray(finalKey, value, tsKey, matchedCol, signalType, cnameCacheByteHashToStr, jsParsingStackbuf)
			}
			if err != nil {
				return fmt.Errorf("encodeRawJsonObject: arr currKey: %v, err: %v", currKey, err)
			}
		case jp.String:

			valUnescaped, err := jp.Unescape(value, jsParsingStackbuf[:])
			if err != nil {
				return fmt.Errorf("encodeRawJsonObject: failed to unescape currKey: %v, err: %v",
					currKey, err)
			}

			matchedCol = ss.encodeSingleString(finalKey, tsKey, matchedCol, valUnescaped)
		case jp.Number:
			numVal, err := jp.ParseInt(value)
			if err != nil {
				fltVal, err := jp.ParseFloat(value)
				if err != nil {
					return fmt.Errorf("encodeRawJsonObject: flt currKey: %v, err: %v", currKey, err)
				}
				matchedCol = ss.encodeSingleNumber(finalKey, fltVal, tsKey,
					matchedCol, value)
				return nil
			}
			matchedCol = ss.encodeSingleNumber(finalKey, numVal, tsKey,
				matchedCol, value)
		case jp.Boolean:
			boolVal, err := jp.ParseBoolean(value)
			if err != nil {
				return fmt.Errorf("encodeRawJsonObject: bool currKey: %v, err: %v", currKey, err)
			}
			matchedCol = ss.encodeSingleBool(finalKey, boolVal, tsKey, matchedCol)
		case jp.Null:
			matchedCol = ss.encodeSingleNull(finalKey, tsKey, matchedCol)
		default:
			return fmt.Errorf("currKey: %v, received unknown type of %+s", currKey, valueType)
		}
		return nil
	}
	err := jp.ObjectEach(data, handler)
	return matchedCol, err
}

func (ss *SegStore) encodeRawJsonArray(currKey string, data []byte, tsKey *string,
	matchedCol bool, signalType segutils.SIGNAL_TYPE) (bool, error) {
	var encErr error
	if signalType == SIGNAL_JAEGER_TRACES {
		if currKey != "references" && currKey != "logs" {
			matchedCol, encErr = ss.encodeSingleDictArray(currKey, data, tsKey, matchedCol, signalType)
			if encErr != nil {
				log.Infof("encodeRawJsonArray error %s", encErr)
				return matchedCol, encErr
			}
		} else {
			matchedCol, encErr = ss.encodeSingleRawBuffer(currKey, data, tsKey, matchedCol, signalType)
			if encErr != nil {
				return matchedCol, encErr
			}
		}
	}
	return matchedCol, nil
}

func (ss *SegStore) encodeNonJaegerRawJsonArray(currKey string, data []byte, tsKey *string,
	matchedCol bool, signalType segutils.SIGNAL_TYPE,
	cnameCacheByteHashToStr map[uint64]string,
	jsParsingStackbuf []byte) (bool, error) {

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
			matchedCol, encErr = ss.encodeRawJsonObject(finalKey, value, tsKey, matchedCol, signalType, cnameCacheByteHashToStr, jsParsingStackbuf)
			if encErr != nil {
				finalErr = encErr
				return
			}
		case jp.Array:
			matchedCol, encErr = ss.encodeNonJaegerRawJsonArray(finalKey, value, tsKey, matchedCol, signalType, cnameCacheByteHashToStr, jsParsingStackbuf)
			if encErr != nil {
				finalErr = encErr
				return
			}
		case jp.String:

			valUnescaped, encErr := jp.Unescape(value, jsParsingStackbuf[:])
			if err != nil {
				finalErr = encErr
				return
			}
			matchedCol = ss.encodeSingleString(finalKey, tsKey, matchedCol, valUnescaped)
		case jp.Number:
			numVal, encErr := jp.ParseInt(value)
			if encErr != nil {
				fltVal, encErr := jp.ParseFloat(value)
				if encErr != nil {
					finalErr = encErr
					return
				}
				matchedCol = ss.encodeSingleNumber(finalKey, fltVal, tsKey,
					matchedCol, value)
				return
			}
			matchedCol = ss.encodeSingleNumber(finalKey, numVal, tsKey,
				matchedCol, value)
		case jp.Boolean:
			boolVal, encErr := jp.ParseBoolean(value)
			if encErr != nil {
				finalErr = encErr
				return
			}
			matchedCol = ss.encodeSingleBool(finalKey, boolVal, tsKey, matchedCol)
		case jp.Null:
			matchedCol = ss.encodeSingleNull(finalKey, tsKey, matchedCol)
		default:
			finalErr = fmt.Errorf("received unknown type of %+s", valueType)
			return
		}
	})
	if aErr != nil {
		finalErr = aErr
	}
	return matchedCol, finalErr
}

func (ss *SegStore) encodeSingleDictArray(arraykey string, data []byte,
	tsKey *string, matchedCol bool, signalType segutils.SIGNAL_TYPE) (bool, error) {
	if arraykey == *tsKey {
		return matchedCol, nil
	}
	var finalErr error
	var colWip *ColWip
	colWip, _, matchedCol = ss.initAndBackFillColumn(arraykey, SS_DT_ARRAY_DICT, matchedCol)
	colBlooms := ss.wipBlock.columnBlooms
	var bi *BloomIndex
	var ok bool
	bi, ok = colBlooms[arraykey]
	if !ok {
		bi = &BloomIndex{}
		bi.uniqueWordCount = 0
		bCount := getBlockBloomSize(bi)
		bi.Bf = bloom.NewWithEstimates(uint(bCount), BLOOM_COLL_PROBABILITY)
		colBlooms[arraykey] = bi
	}
	s := colWip.cbufidx
	colWip.cbuf.Append(VALTYPE_DICT_ARRAY[:])
	colWip.cbufidx += 1
	colWip.cbuf.AppendUint16LittleEndian(0) // Placeholder for encoding length of array
	colWip.cbufidx += 2
	_, aErr := jp.ArrayEach(data, func(value []byte, valueType jp.ValueType, offset int, err error) {
		switch valueType {
		case jp.Object:
			keyName, keyType, keyVal, err := getNestedDictEntries(value)
			if err != nil {
				log.Errorf("getNestedDictEntries error %+v", err)
				return
			}
			if len(keyName) == 0 || keyType == "" || len(keyVal) == 0 {
				err = fmt.Errorf("encodeSingleDictArray: Jaeger tags array should have key/value/type values")
				log.Error(err)
				return
			}
			//encode and copy keyName
			n := uint16(len(keyName))
			colWip.cbuf.AppendUint16LittleEndian(n)
			colWip.cbufidx += 2
			colWip.cbuf.Append(keyName)
			colWip.cbufidx += uint32(n)

			keyValLen := uint16(len(keyVal))
			keyValLenBytes := utils.Uint16ToBytesLittleEndian(keyValLen)

			//check key type
			//based on that encode key value
			switch keyType {
			case "string":
				colWip.cbuf.Append(VALTYPE_ENC_SMALL_STRING[:])
				colWip.cbufidx += 1
				colWip.cbuf.Append(keyValLenBytes)
				colWip.cbufidx += 2
				colWip.cbuf.Append(keyVal)
				colWip.cbufidx += uint32(keyValLen)
			case "bool":
				colWip.cbuf.Append(VALTYPE_ENC_BOOL[:])
				colWip.cbufidx += 1
				colWip.cbuf.Append(keyValLenBytes)
				colWip.cbufidx += 2
				colWip.cbuf.Append(keyVal)
				colWip.cbufidx += uint32(keyValLen)
			case "int64":
				colWip.cbuf.Append(VALTYPE_ENC_INT64[:])
				colWip.cbufidx += 1
				colWip.cbuf.Append(keyValLenBytes)
				colWip.cbufidx += 2
				colWip.cbuf.Append(keyVal)
				colWip.cbufidx += uint32(keyValLen)
			case "float64":
				colWip.cbuf.Append(segutils.VALTYPE_ENC_FLOAT64[:])
				colWip.cbufidx += 1
				colWip.cbuf.Append(keyValLenBytes)
				colWip.cbufidx += 2
				colWip.cbuf.Append(keyVal)
				colWip.cbufidx += uint32(keyValLen)
			default:
				finalErr = fmt.Errorf("encodeSingleDictArray : received unknown key  %+s", keyType)
			}
			keyNameStr := string(keyName)
			if bi != nil {
				bi.uniqueWordCount += addToBlockBloomBothCases(bi.Bf, keyName)
				bi.uniqueWordCount += addToBlockBloomBothCases(bi.Bf, keyVal)
			}
			// get the copied key value bytes from the ColWip buffer,
			// As the keyVal bytes are converted to lower case while adding to Bloom above.
			keyValBytes := colWip.cbuf.Slice(int(colWip.cbufidx-uint32(keyValLen)), colWip.cbuf.Len())
			addSegStatsStrIngestion(ss.AllSst, keyNameStr, keyValBytes)
		default:
			finalErr = fmt.Errorf("encodeSingleDictArray : received unknown type of %+s", valueType)
			return
		}
	})
	bytes := [2]byte{}
	utils.Uint16ToBytesLittleEndianInplace(uint16(colWip.cbufidx-s-3), bytes[:])
	err := colWip.cbuf.WriteAt(bytes[:], int(s+1))
	if err != nil {
		log.Errorf("encodeSingleDictArray: failed to copy length of array to buffer; err=%v", err)
		return matchedCol, err
	}

	if aErr != nil {
		finalErr = aErr
	}
	ss.updateColValueSizeInAllSeenColumns(arraykey, uint32(colWip.cbufidx-s))
	return matchedCol, finalErr
}

func getNestedDictEntries(data []byte) ([]byte, string, []byte, error) {
	var nkey, nvalue []byte
	var ntype string

	handler := func(key []byte, value []byte, valueType jp.ValueType, off int) error {
		switch string(key) {
		case "key":
			if valueType != jp.String {
				err := fmt.Errorf("getNestedDictEntries key should be of type string , found type %+v", valueType)
				return err
			}
			nkey = value
		case "type":
			ntype = string(value)
		case "value":
			nvalue = value
		default:
			err := fmt.Errorf("getNestedDictEntries: received unknown key of %+s", key)
			return err
		}
		return nil
	}
	err := jp.ObjectEach(data, handler)
	return nkey, ntype, nvalue, err

}

func (ss *SegStore) encodeSingleRawBuffer(key string, value []byte,
	tsKey *string, matchedCol bool, signalType segutils.SIGNAL_TYPE) (bool, error) {
	if key == *tsKey {
		return matchedCol, nil
	}
	var colWip *ColWip
	colWip, _, matchedCol = ss.initAndBackFillColumn(key, SS_DT_STRING, matchedCol)
	colBlooms := ss.wipBlock.columnBlooms
	var bi *BloomIndex
	var ok bool
	if key != "_type" && key != "_index" && key != "tags" {
		_, ok = colBlooms[key]
		if !ok {
			bi = &BloomIndex{}
			bi.uniqueWordCount = 0
			bi.Bf = bloom.NewWithEstimates(uint(BLOCK_BLOOM_SIZE), BLOOM_COLL_PROBABILITY)
			colBlooms[key] = bi
		}
	}
	//[utils.VALTYPE_RAW_JSON][raw-byte-len][raw-byte]
	colWip.cbuf.Append(VALTYPE_RAW_JSON[:])
	colWip.cbufidx += 1
	n := uint16(len(value))
	colWip.cbuf.AppendUint16LittleEndian(n)
	colWip.cbufidx += 2
	colWip.cbuf.Append(value)
	colWip.cbufidx += uint32(n)
	ss.updateColValueSizeInAllSeenColumns(key, uint32(3+n))

	return matchedCol, nil
}

func (ss *SegStore) encodeSingleString(key string,
	tsKey *string, matchedCol bool, valBytes []byte) bool {
	if key == *tsKey {
		return matchedCol
	}
	var colWip *ColWip
	var recNum uint16
	colWip, recNum, matchedCol = ss.initAndBackFillColumn(key, SS_DT_STRING, matchedCol)
	colBlooms := ss.wipBlock.columnBlooms
	if key != "_type" && key != "_index" {
		_, ok := colBlooms[key]
		if !ok {
			bi := &BloomIndex{}
			bi.uniqueWordCount = 0
			bi.Bf = bloom.NewWithEstimates(uint(BLOCK_BLOOM_SIZE), BLOOM_COLL_PROBABILITY)
			colBlooms[key] = bi
		}
	}
	s := colWip.cbufidx
	colWip.WriteSingleStringBytes(valBytes)
	recLen := colWip.cbufidx - s
	ss.updateColValueSizeInAllSeenColumns(key, recLen)

	if !ss.skipDe {
		ss.checkAddDictEnc(colWip, colWip.cbuf.Slice(int(s), int(colWip.cbufidx)), recNum, s, false)
	}
	valueLen := uint32(len(valBytes))
	addSegStatsStrIngestion(ss.AllSst, key, colWip.cbuf.Slice(int(colWip.cbufidx-valueLen), int(colWip.cbufidx)))
	return matchedCol
}

func (ss *SegStore) encodeSingleBool(key string, val bool,
	tsKey *string, matchedCol bool) bool {
	if key == *tsKey {
		return matchedCol
	}
	var colWip *ColWip
	colBlooms := ss.wipBlock.columnBlooms
	colWip, _, matchedCol = ss.initAndBackFillColumn(key, SS_DT_BOOL, matchedCol)

	// todo for bools, we really don't have to do BI, they will get encoded as DictEnc
	_, ok := colBlooms[key]
	if !ok {
		bi := &BloomIndex{}
		bi.uniqueWordCount = 0
		bi.Bf = bloom.NewWithEstimates(uint(BLOCK_BLOOM_SIZE), BLOOM_COLL_PROBABILITY)
		colBlooms[key] = bi
	}
	colWip.cbuf.Append(VALTYPE_ENC_BOOL[:])
	colWip.cbufidx += 1
	colWip.cbuf.Append(utils.BoolToBytesLittleEndian(val))
	colWip.cbufidx += 1
	ss.updateColValueSizeInAllSeenColumns(key, 2)

	return matchedCol
}

func (ss *SegStore) encodeSingleNull(key string,
	tsKey *string, matchedCol bool) bool {
	if key == *tsKey {
		return matchedCol
	}
	var colWip *ColWip
	colWip, _, matchedCol = ss.initAndBackFillColumn(key, SS_DT_BACKFILL, matchedCol)
	colWip.cbuf.Append(VALTYPE_ENC_BACKFILL[:])
	colWip.cbufidx += 1
	ss.updateColValueSizeInAllSeenColumns(key, 1)
	return matchedCol
}

func (ss *SegStore) encodeSingleNumber(key string, value interface{},
	tsKey *string, matchedCol bool, valBytes []byte) bool {
	if key == *tsKey {
		return matchedCol
	}
	var colWip *ColWip
	var recNum uint16
	var numType SS_DTYPE

	switch value.(type) {
	case float64, json.Number:
		numType = SS_DT_FLOAT
	case int64, int32, int16, int8, int:
		numType = SS_DT_SIGNED_NUM
	case uint64, uint32, uint16, uint8, uint:
		numType = SS_DT_UNSIGNED_NUM
	default:
		numType = SS_DT_BACKFILL
	}

	colWip, recNum, matchedCol = ss.initAndBackFillColumn(key, numType, matchedCol)
	colRis := ss.wipBlock.columnRangeIndexes
	segstats := ss.AllSst
	retLen := ss.encSingleNumber(key, value, colWip.cbuf, colWip.cbufidx, colRis, recNum, segstats,
		ss.wipBlock.bb, colWip, valBytes)
	colWip.cbufidx += retLen
	ss.updateColValueSizeInAllSeenColumns(key, retLen)

	return matchedCol
}

func (ss *SegStore) initAndBackFillColumn(key string, ssType SS_DTYPE,
	matchedCol bool) (*ColWip, uint16, bool) {
	allColWip := ss.wipBlock.colWips
	colBlooms := ss.wipBlock.columnBlooms
	colRis := ss.wipBlock.columnRangeIndexes
	allColsInBlock := ss.wipBlock.columnsInBlock
	recNum := ss.wipBlock.blockSummary.RecCount

	colWip, ok := allColWip[key]
	if !ok {
		colWip = InitColWip(ss.SegmentKey, key)
		allColWip[key] = colWip
	}
	_, ok = allColsInBlock[key]
	if !ok {
		if recNum != 0 {
			log.Debugf("EncodeColumns: newColumn=%v showed up in the middle, backfilling it now", key)
			ss.backFillPastRecords(key, ssType, recNum, colBlooms, colRis, colWip)
		}
	}
	allColsInBlock[key] = true
	matchedCol = matchedCol || ss.pqTracker.isColumnInPQuery(key)
	colWip.cstartidx = colWip.cbufidx
	return colWip, recNum, matchedCol
}

func initMicroIndices(key string, ssType SS_DTYPE, colBlooms map[string]*BloomIndex,
	colRis map[string]*RangeIndex) error {

	switch ssType {
	case SS_DT_STRING:
		bi := &BloomIndex{}
		bi.uniqueWordCount = 0
		bi.Bf = bloom.NewWithEstimates(uint(BLOCK_BLOOM_SIZE), BLOOM_COLL_PROBABILITY)
		colBlooms[key] = bi
	case SS_DT_SIGNED_NUM, SS_DT_UNSIGNED_NUM, SS_DT_FLOAT:
		ri := &RangeIndex{}
		ri.Ranges = make(map[string]*Numbers)
		colRis[key] = ri
	case SS_DT_BOOL:
		// todo kunal, for bool type we need to keep a inverted index
		bi := &BloomIndex{}
		bi.uniqueWordCount = 0
		bi.Bf = bloom.NewWithEstimates(uint(BLOCK_BLOOM_SIZE), BLOOM_COLL_PROBABILITY)
		colBlooms[key] = bi
	default:
		return fmt.Errorf("initMicroIndices: unknown ssType: %v", ssType)
	}

	return nil
}

func (ss *SegStore) backFillPastRecords(key string, ssType SS_DTYPE, recNum uint16, colBlooms map[string]*BloomIndex,
	colRis map[string]*RangeIndex, colWip *ColWip) uint32 {

	err := initMicroIndices(key, ssType, colBlooms, colRis)
	if err != nil {
		ss.StoreSegmentError(err.Error(), log.ErrorLevel, err)
	}

	packedLen := uint32(0)

	recArr := make([]uint16, recNum)

	for i := uint16(0); i < recNum; i++ {
		// only the type will be saved when we are backfilling
		colWip.cbuf.Append(VALTYPE_ENC_BACKFILL[:])
		colWip.cbufidx += 1
		recArr[i] = i
	}

	packedLen += uint32(recNum)

	// we will also init dictEnc for backfilled recnums

	colWip.deData.deMap[string(VALTYPE_ENC_BACKFILL[:])] = recArr
	colWip.deData.deCount++

	return packedLen
}

func (ss *SegStore) encSingleNumber(key string, val interface{}, wipbuf *utils.Buffer, idx uint32,
	colRis map[string]*RangeIndex, wRecNum uint16,
	segstats map[string]*SegStats, bb *bbp.ByteBuffer, colWip *ColWip,
	valBytes []byte) uint32 {

	ri, ok := colRis[key]
	if !ok {
		ri = &RangeIndex{}
		ri.Ranges = make(map[string]*Numbers)
		colRis[key] = ri
	}

	switch cval := val.(type) {
	case float64:
		addSegStatsNums(segstats, key, SS_FLOAT64, FPARM_INT64, FPARM_UINT64, cval,
			valBytes)
		valSize := encJsonNumber(key, SS_FLOAT64, FPARM_INT64, FPARM_UINT64, cval, wipbuf,
			idx, ri.Ranges)
		ss.checkAddDictEnc(colWip, wipbuf.Slice(int(idx), int(idx+valSize)), wRecNum, idx, false)
		return valSize
	case int64:
		addSegStatsNums(segstats, key, SS_INT64, cval, FPARM_UINT64, FPARM_FLOAT64,
			valBytes)

		valSize := encJsonNumber(key, SS_INT64, cval, FPARM_UINT64, FPARM_FLOAT64, wipbuf,
			idx, ri.Ranges)
		ss.checkAddDictEnc(colWip, wipbuf.Slice(int(idx), int(idx+valSize)), wRecNum, idx, false)
		return valSize

	default:
		log.Errorf("encSingleNumber: Tried to encode a non int/float value! value=%+v", cval)
	}
	return 0
}

func encJsonNumber(key string, numType SS_IntUintFloatTypes, intVal int64, uintVal uint64,
	fltVal float64, wipbuf *utils.Buffer, idx uint32, blockRangeIndex map[string]*Numbers) uint32 {

	var valSize uint32

	switch numType {
	case SS_INT64:
		wipbuf.Append(VALTYPE_ENC_INT64[:])
		wipbuf.AppendInt64LittleEndian(intVal)
		valSize = 1 + 8
	case SS_UINT64:
		wipbuf.Append(VALTYPE_ENC_UINT64[:])
		wipbuf.AppendUint64LittleEndian(uintVal)
		valSize = 1 + 8
	case SS_FLOAT64:
		wipbuf.Append(VALTYPE_ENC_FLOAT64[:])
		wipbuf.AppendFloat64LittleEndian(fltVal)
		valSize = 1 + 8
	default:
		log.Errorf("encJsonNumber: unknown numType: %v", numType)
	}

	if blockRangeIndex != nil {
		updateRangeIndex(key, blockRangeIndex, numType, intVal, uintVal, fltVal)
	}

	return valSize
}

func (ss *SegStore) updateColValueSizeInAllSeenColumns(colName string, size uint32) {
	currentSize, ok := ss.AllSeenColumnSizes[colName]
	if !ok {
		if ss.RecordCount > 0 {
			// column appearing first time in the middle of the wip, so past recNums will be filled with BackFill_CVAL_TYpe, so mark this as inconsistent
			ss.AllSeenColumnSizes[colName] = INCONSISTENT_CVAL_SIZE
		} else {
			ss.AllSeenColumnSizes[colName] = size
		}

		return
	}

	if currentSize == INCONSISTENT_CVAL_SIZE {
		return
	}

	if currentSize != size {
		ss.AllSeenColumnSizes[colName] = INCONSISTENT_CVAL_SIZE
	}
}

/*
   Caller of this function can confidently cast the CValEncoslure.CVal to one of the foll types:
	 bool       (if CValEncoslure.Dtype = SS_DT_BOOL)
	 uint64     (if CValEncoslure.Dtype = SS_DT_UNSIGNED_NUM)
	 int64      (if CValEncoslure.Dtype = SS_DT_SIGNED_NUM)
	 float64    (if CValEncoslure.Dtype = SS_DT_FLOAT)
	 string     (if CValEncoslure.Dtype = SS_DT_STRING)
	 array      (if CValEncoslure.Dtype = SS_DT_ARRAY_DICT)
*/
/*
parameters:
   rec: byte slice
   qid
   CValEncoslure: Cval encoding of this col entry
returns:
   uint16: len of this entry inside that was inside the byte slice
   error:
*/
func GetCvalFromRec(rec []byte, qid uint64, retVal *CValueEnclosure) (uint16, error) {

	if len(rec) == 0 {
		return 0, errors.New("column value is empty")
	}

	var endIdx uint16
	switch rec[0] {

	case VALTYPE_ENC_SMALL_STRING[0]:
		retVal.Dtype = SS_DT_STRING
		// one byte for type & two for reclen

		strlen := utils.BytesToUint16LittleEndian(rec[1:3])
		endIdx = strlen + 3
		retVal.CVal = string(rec[3:endIdx])
	case VALTYPE_ENC_BOOL[0]:
		retVal.Dtype = SS_DT_BOOL
		if rec[1] == 0 {
			retVal.CVal = false
		} else {
			retVal.CVal = true
		}
		endIdx = 2
	case VALTYPE_ENC_INT8[0]:
		retVal.Dtype = SS_DT_SIGNED_NUM
		retVal.CVal = int64(int8(rec[1:][0]))
		endIdx = 2
	case VALTYPE_ENC_INT16[0]:
		retVal.Dtype = SS_DT_SIGNED_NUM
		retVal.CVal = int64(utils.BytesToInt16LittleEndian(rec[1:]))
		endIdx = 3
	case VALTYPE_ENC_INT32[0]:
		retVal.Dtype = SS_DT_SIGNED_NUM
		retVal.CVal = int64(utils.BytesToInt32LittleEndian(rec[1:]))
		endIdx = 5
	case VALTYPE_ENC_INT64[0]:
		retVal.Dtype = SS_DT_SIGNED_NUM
		retVal.CVal = utils.BytesToInt64LittleEndian(rec[1:])
		endIdx = 9
	case VALTYPE_ENC_UINT8[0]:
		retVal.Dtype = SS_DT_UNSIGNED_NUM
		retVal.CVal = uint64((rec[1:])[0])
		endIdx = 2
	case VALTYPE_ENC_UINT16[0]:
		retVal.Dtype = SS_DT_UNSIGNED_NUM
		retVal.CVal = uint64(utils.BytesToUint16LittleEndian(rec[1:]))
		endIdx = 3
	case VALTYPE_ENC_UINT32[0]:
		retVal.Dtype = SS_DT_UNSIGNED_NUM
		retVal.CVal = uint64(utils.BytesToUint32LittleEndian(rec[1:]))
		endIdx = 5
	case VALTYPE_ENC_UINT64[0]:
		retVal.Dtype = SS_DT_UNSIGNED_NUM
		retVal.CVal = utils.BytesToUint64LittleEndian(rec[1:])
		endIdx = 9
	case VALTYPE_ENC_FLOAT64[0]:
		retVal.Dtype = SS_DT_FLOAT
		retVal.CVal = utils.BytesToFloat64LittleEndian(rec[1:])
		endIdx = 9
	case VALTYPE_ENC_BACKFILL[0]:
		retVal.Dtype = SS_DT_BACKFILL
		retVal.CVal = nil
		endIdx = 1
	case VALTYPE_RAW_JSON[0]:
		retVal.Dtype = SS_DT_RAW_JSON
		strlen := utils.BytesToUint16LittleEndian(rec[1:3])
		endIdx = strlen + 3
		data := rec[3:endIdx]
		entries := make([]interface{}, 0)
		err := json.Unmarshal(data, &entries)
		if err != nil {
			log.Errorf("GetCvalFromRec: Error unmarshalling VALTYPE_RAW_JSON = %v", err)
			return 0, err
		}
		retVal.CVal = entries
	case VALTYPE_DICT_ARRAY[0]:
		retVal.Dtype = SS_DT_ARRAY_DICT
		// one byte for type & two for reclen
		totalLen := utils.BytesToInt16LittleEndian(rec[1:])
		idx := uint16(3)
		cValArray := make([]map[string]interface{}, 0)
		for idx < uint16(totalLen) {
			cVal := make(map[string]interface{})
			strlen := utils.BytesToUint16LittleEndian(rec[idx : idx+2])
			idx += 2
			keyVal := string(rec[idx : idx+strlen])
			idx += strlen

			cVal["key"] = keyVal
			switch rec[idx] {
			case VALTYPE_ENC_SMALL_STRING[0]:
				cVal["type"] = "string"
				// one byte for type & two for reclen
				strlen := utils.BytesToUint16LittleEndian(rec[idx+1 : idx+3])
				idx += 3
				cVal["value"] = string(rec[idx : idx+strlen])
				idx += strlen
			case VALTYPE_ENC_BOOL[0]:
				cVal["type"] = "bool"
				strlen := utils.BytesToUint16LittleEndian(rec[idx+1 : idx+3])
				idx += 3
				cVal["value"] = string(rec[idx : idx+strlen])
				idx += strlen
			case VALTYPE_ENC_INT64[0]:
				cVal["type"] = "int64"
				strlen := utils.BytesToUint16LittleEndian(rec[idx+1 : idx+3])
				idx += 3
				cVal["value"] = string(rec[idx : idx+strlen])
				idx += strlen
			case VALTYPE_ENC_FLOAT64[0]:
				cVal["type"] = "float64"
				strlen := utils.BytesToUint16LittleEndian(rec[idx+1 : idx+3])
				idx += 3
				cVal["value"] = string(rec[idx : idx+strlen])
				idx += strlen
			default:
				log.Errorf("qid=%d, GetCvalFromRec:SS_DT_ARRAY_DICT unknown type=%v\n", qid, rec[idx])
				return endIdx, errors.New("invalid rec type")
			}
			cValArray = append(cValArray, cVal)
		}
		retVal.CVal = cValArray
		endIdx = uint16(totalLen)

	default:
		log.Errorf("qid=%d, GetCvalFromRec: dont know how to convert type=%v\n", qid, rec[0])
		return endIdx, errors.New("invalid rec type")
	}

	return endIdx, nil
}

func GetNumValFromRec(rec *utils.Buffer, offset int, qid uint64, retVal *Number) (uint16, error) {

	retVal.SetInvalidType()

	if rec.Len() == 0 {
		return 0, errors.New("column value is empty")
	}

	var endIdx uint16
	typeByte, err := rec.At(offset)
	if err != nil {
		log.Errorf("GetNumValFromRec: Error reading type byte; err=%v", err)
		return 0, err
	}

	switch typeByte {
	case VALTYPE_ENC_SMALL_STRING[0]:
		strlen := utils.BytesToUint16LittleEndian(rec.Slice(offset+1, offset+3))
		endIdx = strlen + 3
	case VALTYPE_ENC_BOOL[0]:
		endIdx = 2
	case VALTYPE_ENC_INT8[0]:
		value, err := rec.At(offset + 1)
		if err != nil {
			log.Errorf("GetNumValFromRec: Error reading int8 value; err=%v", err)
			return 0, err
		}
		retVal.SetInt64(int64(int8(value)))
		endIdx = 2
	case VALTYPE_ENC_INT16[0]:
		retVal.SetInt64(int64(utils.BytesToInt16LittleEndian(rec.Slice(offset+1, offset+3))))
		endIdx = 3
	case VALTYPE_ENC_INT32[0]:
		retVal.SetInt64(int64(utils.BytesToInt32LittleEndian(rec.Slice(offset+1, offset+5))))
		endIdx = 5
	case VALTYPE_ENC_INT64[0]:
		retVal.SetInt64(utils.BytesToInt64LittleEndian(rec.Slice(offset+1, offset+9)))
		endIdx = 9
	case VALTYPE_ENC_UINT8[0]:
		value, err := rec.At(offset + 1)
		if err != nil {
			log.Errorf("GetNumValFromRec: Error reading uint8 value; err=%v", err)
			return 0, err
		}
		retVal.SetInt64(int64(value))
		endIdx = 2
	case VALTYPE_ENC_UINT16[0]:
		retVal.SetInt64(int64(utils.BytesToUint16LittleEndian(rec.Slice(offset+1, offset+3))))
		endIdx = 3
	case VALTYPE_ENC_UINT32[0]:
		retVal.SetInt64(int64(utils.BytesToUint32LittleEndian(rec.Slice(offset+1, offset+5))))
		endIdx = 5
	case VALTYPE_ENC_UINT64[0]:
		retVal.SetInt64(int64(utils.BytesToUint64LittleEndian(rec.Slice(offset+1, offset+9))))
		endIdx = 9
	case VALTYPE_ENC_FLOAT64[0]:
		retVal.SetFloat64(utils.BytesToFloat64LittleEndian(rec.Slice(offset+1, offset+9)))
		endIdx = 9
	case VALTYPE_ENC_BACKFILL[0]:
		retVal.SetBackfillType()
		endIdx = 1
	case VALTYPE_RAW_JSON[0]:
		strlen := utils.BytesToUint16LittleEndian(rec.Slice(offset+1, offset+3))
		endIdx = strlen + 3
	default:
		log.Errorf("qid=%d, GetNumValFromRec: dont know how to convert type=%v\n", qid, typeByte)
		return endIdx, errors.New("invalid rec type")
	}
	return endIdx, nil
}

func GetMockSegBaseDirAndKeyForTest(dataDir string, indexName string) (string, string, error) {
	// segBaseDir format: data Dir / host / final / indexName / stream-id / suffix
	segBaseDir := fmt.Sprintf("%smock-host.test/final/%s/stream-1/0/", dataDir, indexName)
	err := os.MkdirAll(segBaseDir, 0755)
	if err != nil {
		return "", "", err
	}
	segKey := segBaseDir + "0"
	return segBaseDir, segKey, nil
}

func WriteMockColSegFile(segBaseDir string, segkey string, numBlocks int, entryCount int) ([]map[string]*BloomIndex,
	[]*BlockSummary, []map[string]*RangeIndex, map[string]bool, map[uint16]*BlockMetadataHolder,
	map[string]*ColSizeInfo) {

	allBlockBlooms := make([]map[string]*BloomIndex, numBlocks)
	allBlockRangeIdx := make([]map[string]*RangeIndex, numBlocks)
	allBlockSummaries := make([]*BlockSummary, numBlocks)
	allBlockOffsets := make(map[uint16]*BlockMetadataHolder)
	segstats := make(map[string]*SegStats)
	lencnames := uint8(12)
	cnames := make([]string, lencnames)
	mapCol := make(map[string]bool)
	for cidx := uint8(0); cidx < lencnames; cidx += 1 {
		currCol := fmt.Sprintf("key%v", cidx)
		cnames[cidx] = currCol
		mapCol[currCol] = true
	}

	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [64]byte

	compWorkBuf := make([]byte, WIP_SIZE)
	tsKey := config.GetTimeStampKey()
	allCols := make(map[string]uint32)
	// set up entries
	for j := 0; j < numBlocks; j++ {
		currBlockUint := uint16(j)
		columnBlooms := make(map[string]*BloomIndex)
		columnRangeIndexes := make(map[string]*RangeIndex)
		colWips := make(map[string]*ColWip)
		wipBlock := WipBlock{
			columnBlooms:       columnBlooms,
			columnRangeIndexes: columnRangeIndexes,
			colWips:            colWips,
			columnsInBlock:     mapCol,
			tomRollup:          make(map[uint64]*RolledRecs),
			tohRollup:          make(map[uint64]*RolledRecs),
			todRollup:          make(map[uint64]*RolledRecs),
			bb:                 bbp.Get(),
			blockTs:            make([]uint64, 0),
		}
		segStore := NewSegStore(0)
		segStore.wipBlock = wipBlock
		segStore.SegmentKey = segkey
		segStore.AllSeenColumnSizes = allCols
		segStore.pqTracker = initPQTracker()
		segStore.AllSst = segstats
		segStore.numBlocks = currBlockUint

		for i := 0; i < entryCount; i++ {
			entry := make(map[string]interface{})
			entry[cnames[0]] = "match words 123 abc"
			entry[cnames[1]] = "value1"
			entry[cnames[2]] = i
			entry[cnames[3]] = (i%2 == 0)
			entry[cnames[4]] = strconv.FormatUint(uint64(i)*2, 10)
			entry[cnames[5]] = "batch-" + fmt.Sprint(j) + "-" + utils.GetRandomString(10, utils.AlphaNumeric)
			entry[cnames[6]] = (i * 2)
			entry[cnames[7]] = "batch-" + fmt.Sprint(j)
			entry[cnames[8]] = j
			entry[cnames[9]] = rand.Float64()
			entry[cnames[10]] = segkey
			entry[cnames[11]] = "record-batch-" + fmt.Sprint(i%2)

			timestp := uint64(i) + 1 // dont start with 0 as timestamp
			raw, _ := json.Marshal(entry)
			_, err := segStore.EncodeColumns(raw, timestp, &tsKey, SIGNAL_EVENTS,
				cnameCacheByteHashToStr, jsParsingStackbuf[:])
			if err != nil {
				log.Errorf("WriteMockColSegFile: error packing entry: %s", err)
			}
			segStore.wipBlock.blockSummary.RecCount += 1
		}

		allBlockBlooms[j] = segStore.wipBlock.columnBlooms
		allBlockSummaries[j] = &segStore.wipBlock.blockSummary
		allBlockRangeIdx[j] = segStore.wipBlock.columnRangeIndexes
		allBlockOffsets[currBlockUint] = &BlockMetadataHolder{
			ColumnBlockOffset: make(map[string]int64),
			ColumnBlockLen:    make(map[string]uint32),
		}
		for cname, colWip := range segStore.wipBlock.colWips {
			csgFname := fmt.Sprintf("%v_%v.csg", segkey, xxhash.Sum64String(cname))
			var encType []byte
			if cname == config.GetTimeStampKey() {
				encType, _ = segStore.wipBlock.encodeTimestamps()
			} else {
				encType = ZSTD_COMLUNAR_BLOCK
			}

			err := segStore.writeToBloom(encType, compWorkBuf[:cap(compWorkBuf)], cname, colWip)
			if err != nil {
				log.Fatalf("WriteMockColSegFile: failed to writeToBloom colsegfilename=%v, err=%v", colWip.csgFname, err)
			}

			blkLen, blkOffset, err := writeWip(colWip, encType, compWorkBuf)
			if err != nil {
				log.Errorf("WriteMockColSegFile: failed to write colsegfilename=%v, err=%v", csgFname, err)
			}
			allBlockOffsets[currBlockUint].ColumnBlockLen[cname] = blkLen
			allBlockOffsets[currBlockUint].ColumnBlockOffset[cname] = blkOffset
		}
	}

	allColsSizes := make(map[string]*ColSizeInfo)
	for cname := range mapCol {
		fnamecmi := fmt.Sprintf("%v_%v.csg", segkey, xxhash.Sum64String(cname))
		cmiSize, _ := ssutils.GetFileSizeFromDisk(fnamecmi)
		fnamecsg := fmt.Sprintf("%v_%v.csg", segkey, xxhash.Sum64String(cname))
		csgSize, _ := ssutils.GetFileSizeFromDisk(fnamecsg)
		allColsSizes[cname] = &ColSizeInfo{CmiSize: cmiSize, CsgSize: csgSize}
	}

	return allBlockBlooms, allBlockSummaries, allBlockRangeIdx, mapCol, allBlockOffsets, allColsSizes
}

func WriteMockTraceFile(segkey string, numBlocks int, entryCount int) ([]map[string]*BloomIndex,
	[]*BlockSummary, []map[string]*RangeIndex, map[string]bool, map[uint16]*BlockMetadataHolder) {

	allBlockBlooms := make([]map[string]*BloomIndex, numBlocks)
	allBlockRangeIdx := make([]map[string]*RangeIndex, numBlocks)
	allBlockSummaries := make([]*BlockSummary, numBlocks)
	allBlockOffsets := make(map[uint16]*BlockMetadataHolder)

	segstats := make(map[string]*SegStats)

	mapCol := make(map[string]bool)
	mapCol["tags"] = true
	mapCol["startTimeMillis"] = true
	mapCol["timestamp"] = true

	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [64]byte

	compWorkBuf := make([]byte, WIP_SIZE)

	tsKey := config.GetTimeStampKey()
	allCols := make(map[string]uint32)
	// set up entries
	for j := 0; j < numBlocks; j++ {
		currBlockUint := uint16(j)
		columnBlooms := make(map[string]*BloomIndex)
		columnRangeIndexes := make(map[string]*RangeIndex)
		colWips := make(map[string]*ColWip)
		wipBlock := WipBlock{
			columnBlooms:       columnBlooms,
			columnRangeIndexes: columnRangeIndexes,
			colWips:            colWips,
			columnsInBlock:     mapCol,
			tomRollup:          make(map[uint64]*RolledRecs),
			tohRollup:          make(map[uint64]*RolledRecs),
			todRollup:          make(map[uint64]*RolledRecs),
			bb:                 bbp.Get(),
			blockTs:            make([]uint64, 0),
		}
		segStore := NewSegStore(0)
		segStore.wipBlock = wipBlock
		segStore.SegmentKey = segkey
		segStore.AllSeenColumnSizes = allCols
		segStore.pqTracker = initPQTracker()
		segStore.AllSst = segstats
		segStore.numBlocks = currBlockUint

		entries := []struct {
			entry []byte
		}{

			{
				[]byte(`{"tags": [
				{
					"key": "sampler.type",
					"type": "string",
					"value": "const"
				},
				{
					"key": "sampler.param",
					"type": "bool",
					"value": "true"
				},
				{
					"key": "http.status_code",
					"type": "int64",
					"value": "200"
				},
				{
					"key": "component",
					"type": "string",
					"value": "gRPC"
				},
				{
					"key": "retry_no",
					"type": "int64",
					"value": "1"
				}

				],

			}`,
				)},
		}

		entry := entries[0].entry
		timestp := uint64(2) + 1 // dont start with 0 as timestamp
		_, err := segStore.EncodeColumns(entry, timestp, &tsKey, SIGNAL_JAEGER_TRACES,
			cnameCacheByteHashToStr, jsParsingStackbuf[:])
		if err != nil {
			log.Errorf("WriteMockTraceFile: error packing entry: %s", err)
		}
		segStore.wipBlock.blockSummary.RecCount += 1

		allBlockBlooms[j] = segStore.wipBlock.columnBlooms
		allBlockSummaries[j] = &segStore.wipBlock.blockSummary
		allBlockRangeIdx[j] = segStore.wipBlock.columnRangeIndexes
		allBlockOffsets[currBlockUint] = &BlockMetadataHolder{
			ColumnBlockOffset: make(map[string]int64),
			ColumnBlockLen:    make(map[string]uint32),
		}
		for cname, colWip := range segStore.wipBlock.colWips {
			csgFname := fmt.Sprintf("%v_%v.csg", segkey, xxhash.Sum64String(cname))
			var encType []byte
			if cname == config.GetTimeStampKey() {
				encType, _ = segStore.wipBlock.encodeTimestamps()
			} else {
				encType = ZSTD_COMLUNAR_BLOCK
			}
			blkLen, blkOffset, err := writeWip(colWip, encType, compWorkBuf)
			if err != nil {
				log.Errorf("WriteMockTraceFile: failed to write tracer filename=%v, err=%v", csgFname, err)
			}
			allBlockOffsets[currBlockUint].ColumnBlockLen[cname] = blkLen
			allBlockOffsets[currBlockUint].ColumnBlockOffset[cname] = blkOffset
		}
	}
	return allBlockBlooms, allBlockSummaries, allBlockRangeIdx, mapCol, allBlockOffsets
}

func WriteMockMetricsSegment(forceRotate bool, entryCount int) ([]*metrics.MetricsSegment, error) {

	timestamp := uint64(time.Now().Unix() - 24*3600)
	metric := []string{"test.metric.0", "test.metric.1", "test.metric.2", "test.metric.3"}
	car_type := []string{"Passenger car light", "Passenger car compact", "Passenger car heavy", "Passenger car mini", "Passenger car medium", "Pickup truck", "Van"}
	color := []string{"olive", "green", "maroon", "lime", "yellow", "white", "purple", "navy", "aqua"}
	group := []string{"group 0", "group 1"}
	fuel_type := []string{"Electric", "Diesel", "Gasoline", "CNG", "Ethanol", "Methanol"}
	model := []string{"C55 Amg", "325i", "Ranger Pickup 2wd", "Sts", "Pacifica 2wd", "Trailblazer 2wd", "E320 Cdi"}
	metrics.InitMetricsSegStore()
	for i := 0; i < entryCount; i++ {
		entry := make(map[string]interface{})
		entry["metric"] = metric[rand.Intn(len(metric))]
		entry["tags"] = map[string]string{
			"car_type":  car_type[rand.Intn(len(car_type))],
			"color":     color[rand.Intn(len(color))],
			"group":     group[rand.Intn(len(group))],
			"fuel_type": fuel_type[rand.Intn(len(fuel_type))],
			"model":     model[rand.Intn(len(model))],
		}
		entry["timestamp"] = timestamp + uint64(i)
		entry["value"] = rand.Intn(500)
		rawJson, _ := json.Marshal(entry)
		err := AddTimeSeriesEntryToInMemBuf(rawJson, SIGNAL_METRICS_OTSDB, 0)
		if err != nil {
			log.Errorf("WriteMockMetricsSegment: error adding time series entry to in memory buffer: %s", err)
			return nil, err
		}
	}
	retVal := make([]*metrics.MetricsSegment, len(metrics.GetAllMetricsSegments()))

	for idx, mSeg := range metrics.GetAllMetricsSegments() {
		err := mSeg.CheckAndRotate(forceRotate)
		if err != nil {
			log.Errorf("WriteMockMetricsSegment: unable to force rotate: %s", err)
			return nil, err
		}
		retVal[idx] = mSeg
	}

	return retVal, nil
}

/*
[BlockRangeIndexLen 4B]  [rangeKeyData-1] [rangeKeyData-2]....

** rangeKeyData **
[RangeKeyLen 2B] [ActualRangeKey xxBytes] [RangeNumType 1B] [MinNumValue 8B] [MaxNumValue 8B]
*/

func EncodeRIBlock(blockRangeIndex map[string]*Numbers, blkNum uint16) (uint32, []byte, error) {
	var idx uint32

	idx += uint32(RI_BLK_LEN_SIZE)

	// 255 for key + 1 (type) + 8 (MinVal) + 8 (MaxVal)
	riSizeEstimate := (255 + 17) * len(blockRangeIndex)
	blkRIBuf := make([]byte, riSizeEstimate)

	// copy the blockNum
	utils.Uint16ToBytesLittleEndianInplace(blkNum, blkRIBuf[idx:])
	idx += 2

	copy(blkRIBuf[idx:], CMI_RANGE_INDEX)
	idx += 1 // for CMI type

	for key, item := range blockRangeIndex {
		if len(blkRIBuf) < int(idx) {
			newSlice := make([]byte, riSizeEstimate)
			blkRIBuf = append(blkRIBuf, newSlice...)
		}
		utils.Uint16ToBytesLittleEndianInplace(uint16(len(key)), blkRIBuf[idx:])
		idx += 2
		n := copy(blkRIBuf[idx:], key)
		idx += uint32(n)
		switch item.NumType {
		case RNT_UNSIGNED_INT:
			copy(blkRIBuf[idx:], VALTYPE_ENC_RNT_UNSIGNED_INT[:])
			idx += 1
			utils.Uint64ToBytesLittleEndianInplace(item.Min_uint64, blkRIBuf[idx:])
			idx += 8
			utils.Uint64ToBytesLittleEndianInplace(item.Max_uint64, blkRIBuf[idx:])
			idx += 8
		case RNT_SIGNED_INT:
			copy(blkRIBuf[idx:], VALTYPE_ENC_RNT_SIGNED_INT[:])
			idx += 1
			utils.Int64ToBytesLittleEndianInplace(item.Min_int64, blkRIBuf[idx:])
			idx += 8
			utils.Int64ToBytesLittleEndianInplace(item.Max_int64, blkRIBuf[idx:])
			idx += 8
		case RNT_FLOAT64:
			copy(blkRIBuf[idx:], VALTYPE_ENC_RNT_FLOAT64[:])
			idx += 1
			utils.Float64ToBytesLittleEndianInplace(item.Min_float64, blkRIBuf[idx:])
			idx += 8
			utils.Float64ToBytesLittleEndianInplace(item.Max_float64, blkRIBuf[idx:])
			idx += 8
		}
	}
	// copy the recordlen at the start of the buf
	utils.Uint32ToBytesLittleEndianInplace(uint32(idx-RI_BLK_LEN_SIZE), blkRIBuf[0:])
	// log.Infof("EncodeRIBlock EncodeRIBlock=%v", blkRIBuf[:idx])
	return idx, blkRIBuf, nil
}

func (ss *SegStore) encodeTime(recordTimeMS uint64, tsKey *string) {
	allColWip := ss.wipBlock.colWips
	allColsInBlock := ss.wipBlock.columnsInBlock
	tsWip, ok := allColWip[*tsKey]
	if !ok {
		tsWip = InitColWip(ss.SegmentKey, *tsKey)
		allColWip[*tsKey] = tsWip
		ss.AllSeenColumnSizes[*tsKey] = INCONSISTENT_CVAL_SIZE
	}
	// we will never need to backfill a ts key
	allColsInBlock[*tsKey] = true
	if int(ss.wipBlock.blockSummary.RecCount) >= len(ss.wipBlock.blockTs) {
		newslice := make([]uint64, WIP_NUM_RECS)
		ss.wipBlock.blockTs = append(ss.wipBlock.blockTs, newslice...)
	}
	ss.wipBlock.blockTs[ss.wipBlock.blockSummary.RecCount] = recordTimeMS
	tsWip.cbufidx = 1 // just so the flush/append gets called

	// calculate rollups
	tom := (recordTimeMS / MS_IN_MIN) * MS_IN_MIN
	toh := (recordTimeMS / MS_IN_HOUR) * MS_IN_HOUR
	tod := (recordTimeMS / MS_IN_DAY) * MS_IN_DAY
	ss.wipBlock.adjustEarliestLatestTimes(recordTimeMS)
	addRollup(ss.wipBlock.tomRollup, tom, ss.wipBlock.blockSummary.RecCount)
	addRollup(ss.wipBlock.tohRollup, toh, ss.wipBlock.blockSummary.RecCount)
	addRollup(ss.wipBlock.todRollup, tod, ss.wipBlock.blockSummary.RecCount)

}

func addRollup(rrmap map[uint64]*RolledRecs, rolledTs uint64, lastRecNum uint16) {

	var rr *RolledRecs
	var ok bool
	rr, ok = rrmap[rolledTs]
	if !ok {
		mr := pqmr.CreatePQMatchResults(WIP_NUM_RECS)
		rr = &RolledRecs{MatchedRes: mr}
		rrmap[rolledTs] = rr
	}
	rr.MatchedRes.AddMatchedRecord(uint(lastRecNum))
	rr.lastRecNum = lastRecNum
}

func WriteMockTsRollup(t *testing.T, segkey string) error {

	ss := NewSegStore(0)
	ss.SegmentKey = segkey
	ss.suffix = 1

	wipBlock := createMockTsRollupWipBlock(t, segkey)
	ss.wipBlock = *wipBlock
	err := ss.writeWipTsRollups("timestamp")
	return err
}

func createMockTsRollupWipBlock(t *testing.T, segkey string) *WipBlock {

	config.InitializeTestingConfig(t.TempDir())
	defer os.RemoveAll(config.GetDataPath()) // we just create a suffix file during segstore creation

	lencnames := uint8(2)
	cnames := make([]string, lencnames)
	for cidx := uint8(0); cidx < lencnames; cidx += 1 {
		currCol := fmt.Sprintf("fortscheckkey-%v", cidx)
		cnames[cidx] = currCol
	}
	sId := "ts-rollup"
	segstore, err := getOrCreateSegStore(sId, "test", 0)
	if err != nil {
		log.Errorf("createMockTsRollupWipBlock, getSegstore err=%v", err)
		return nil
	}
	tsKey := config.GetTimeStampKey()
	entryCount := 1000

	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [64]byte

	startTs := uint64(1652222966645) // Tuesday, May 10, 2022 22:49:26.645
	tsincr := uint64(7200)           // so that we have 2 hours, 2 days, and > 2mins buckets

	runningTs := startTs
	for i := 0; i < entryCount; i++ {
		//		t.Logf("TestTimestampEncoding: ts=%v", runningTs)
		record_json := make(map[string]interface{})
		record_json[cnames[0]] = "value1"
		record_json[cnames[1]] = json.Number(fmt.Sprint(i))
		rawJson, _ := json.Marshal(record_json)
		_, err := segstore.EncodeColumns(rawJson, runningTs, &tsKey, SIGNAL_EVENTS,
			cnameCacheByteHashToStr, jsParsingStackbuf[:])
		if err != nil {
			log.Errorf("Error:WriteMockColSegFile: error packing entry: %s", err)
		}
		segstore.wipBlock.blockSummary.RecCount += 1
		segstore.adjustEarliestLatestTimes(runningTs)
		runningTs += tsincr
	}

	return &segstore.wipBlock
}

// EncodeBlocksum: format as below
/*
   [SummaryLen 4B] [blkNum 2B] [highTs 8B] [lowTs 8B] [recCount 2B] [numColumns 2B] [ColumnBlkInfo]

   ColumnBlkInfo : ...
   [cnameLen 2B] [COlName xxB] [blkOff 8B] [blkLen 4B]...

*/

func EncodeBlocksum(bmh *BlockMetadataHolder, bsum *BlockSummary,
	blockSummBuf []byte, blkNum uint16) (uint32, []byte, error) {

	var idx uint32

	//check if blockSummBuf is enough to pack blocksummary data
	// Each BlockSummary entry = xx bytes
	// summLen *4 bytes) + blkNum 2 bytes + bsum.HighTs(8 bytes) + bsum.LowTs(8 bytes) + bsum.RecCoun(2 bytes)
	// + N * [ 2 (cnamelen) +  (actualCnamLen) + 8 (blkOff) + 4 (blkLen)]

	clen := 0
	numCols := uint16(0)
	for cname := range bmh.ColumnBlockOffset {
		clen += len(cname)
		numCols++
	}
	// summLen + blkNum + highTs + lowTs + recCount + numCols + totalCnamesLen + N * (cnameLenHolder + blkOff + blkLen)
	requiredLen := 4 + 2 + 8 + 8 + 2 + 2 + clen + len(bmh.ColumnBlockOffset)*(2+8+4)
	blockSummBuf = utils.ResizeSlice(blockSummBuf, requiredLen)

	// reserve first 4 bytes for BLOCK_SUMMARY_LEN.
	idx += 4

	utils.Uint16ToBytesLittleEndianInplace(blkNum, blockSummBuf[idx:])
	idx += 2
	utils.Uint64ToBytesLittleEndianInplace(bsum.HighTs, blockSummBuf[idx:])
	idx += 8
	utils.Uint64ToBytesLittleEndianInplace(bsum.LowTs, blockSummBuf[idx:])
	idx += 8
	utils.Uint16ToBytesLittleEndianInplace(bsum.RecCount, blockSummBuf[idx:])
	idx += 2
	utils.Uint16ToBytesLittleEndianInplace(numCols, blockSummBuf[idx:])
	idx += 2

	for cname, cOff := range bmh.ColumnBlockOffset {
		utils.Uint16ToBytesLittleEndianInplace(uint16(len(cname)), blockSummBuf[idx:])
		idx += 2
		copy(blockSummBuf[idx:], cname)
		idx += uint32(len(cname))
		utils.Int64ToBytesLittleEndianInplace(cOff, blockSummBuf[idx:])
		idx += 8
		utils.Uint32ToBytesLittleEndianInplace(bmh.ColumnBlockLen[cname], blockSummBuf[idx:])
		idx += 4
	}

	// copy the summlen at the start of the buf
	utils.Uint32ToBytesLittleEndianInplace(uint32(idx), blockSummBuf[0:])

	return idx, blockSummBuf, nil
}

func WriteMockBlockSummary(file string, blockSums []*BlockSummary,
	allBmh map[uint16]*BlockMetadataHolder) {
	fd, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("WriteMockBlockSummary: open failed blockSummaryFname=%v, err=%v", file, err)
		return
	}

	defer fd.Close()

	for blkNum, block := range blockSums {
		blkSumBuf := make([]byte, BLOCK_SUMMARY_SIZE)
		packedLen, _, err := EncodeBlocksum(allBmh[uint16(blkNum)], block, blkSumBuf[0:], uint16(blkNum))

		if err != nil {
			log.Errorf("WriteMockBlockSummary: EncodeBlocksum: Failed to encode blocksummary=%+v, err=%v", block, err)
			return
		}
		if _, err := fd.Write(blkSumBuf[:packedLen]); err != nil {
			log.Errorf("WriteMockBlockSummary:  write failed blockSummaryFname=%v, err=%v", file, err)
			return
		}
	}
	err = fd.Sync()
	if err != nil {
		log.Fatal(err)
	}
}

func (ss *SegStore) checkAddDictEnc(colWip *ColWip, cval []byte, recNum uint16, cbufIdx uint32,
	isBackfill bool) {

	if colWip.deData.deCount < wipCardLimit {
		var recs []uint16
		var ok bool
		if isBackfill {
			recs, ok = colWip.deData.deMap[STR_VALTYPE_ENC_BACKFILL]
		} else {
			recs, ok = colWip.deData.deMap[string(cval)]
		}
		if !ok {
			recs = make([]uint16, 0)
			colWip.deData.deCount++
		}
		recs = append(recs, recNum)
		if isBackfill {
			colWip.deData.deMap[STR_VALTYPE_ENC_BACKFILL] = recs
		} else {
			colWip.deData.deMap[string(cval)] = recs
		}
	}
}

func SetCardinalityLimit(val uint16) {
	wipCardLimit = val
}

/*
		Packing format for dictionary encoding
		[NumDictWords 2B] [dEntry1 XX] [dEntry2 XX] ...

	   dEntry1 -- format
	   [word1Len 2B] [ActualWord] [numRecs 2B] [recNum1 2B][recNum2 2B]....
*/
func PackDictEnc(colWip *ColWip) {

	localIdx := 0
	colWip.dePackingBuf.Reset()

	// copy num of dict words
	colWip.dePackingBuf.AppendUint16LittleEndian(colWip.deData.deCount)
	localIdx += 2

	for dword, recNumsArr := range colWip.deData.deMap {

		// copy the actual dict word , the TLV is packed inside the dword
		colWip.dePackingBuf.Append([]byte(dword))

		localIdx += len(dword)

		// copy num of records, by finding how many bits are set
		numRecs := uint16(len(recNumsArr))
		colWip.dePackingBuf.AppendUint16LittleEndian(numRecs)
		localIdx += 2

		for i := uint16(0); i < numRecs; i++ {
			// copy the recNum
			colWip.dePackingBuf.AppendUint16LittleEndian(recNumsArr[i])
			localIdx += 2
		}
	}
	colWip.cbuf.Reset()
	colWip.cbuf.Append(colWip.dePackingBuf.Slice(0, localIdx))
	colWip.cbufidx = uint32(localIdx)
}

func addSegStatsStrIngestion(segstats map[string]*SegStats, cname string, valBytes []byte) {

	var stats *SegStats
	var ok bool
	stats, ok = segstats[cname]
	if !ok {
		stats = &SegStats{
			IsNumeric: false,
			Count:     0,
		}
		stats.CreateNewHll()

		segstats[cname] = stats
	}

	floatVal, err := strconv.ParseFloat(string(valBytes), 64)
	if err == nil {
		if !stats.IsNumeric {
			stats.IsNumeric = true
		}
		addSegStatsNums(segstats, cname, SS_FLOAT64, 0, 0, floatVal, valBytes)
		return
	}

	UpdateMinMax(stats, segutils.CValueEnclosure{
		CVal:  string(valBytes),
		Dtype: SS_DT_STRING,
	})

	stats.Count++
	stats.InsertIntoHll(valBytes)
}

func addSegStatsBool(segstats map[string]*SegStats, cname string, valBytes []byte) {

	var stats *SegStats
	var ok bool
	stats, ok = segstats[cname]
	if !ok {
		stats = &SegStats{
			IsNumeric: false,
			Count:     0,
		}
		stats.CreateNewHll()

		segstats[cname] = stats
	}
	stats.Count++
	stats.InsertIntoHll(valBytes)
}

func addSegStatsNums(segstats map[string]*SegStats, cname string,
	inNumType SS_IntUintFloatTypes, intVal int64, uintVal uint64,
	fltVal float64, valBytes []byte) {

	var stats *SegStats
	var ok bool
	stats, ok = segstats[cname]
	if !ok {
		stats = &SegStats{
			IsNumeric: true,
			Count:     0,
			NumStats:  statswriter.GetDefaultNumStats(),
		}
		stats.CreateNewHll()
		segstats[cname] = stats
	}

	// prior entries were non numeric, so we should init NumStats, but keep the hll and count vars
	if stats.NumStats == nil {
		stats.NumStats = statswriter.GetDefaultNumStats()
		stats.IsNumeric = true
	}

	stats.InsertIntoHll(valBytes)
	processStats(stats, inNumType, intVal, uintVal, fltVal)
}

func processStats(stats *SegStats, inNumType SS_IntUintFloatTypes, intVal int64,
	uintVal uint64, fltVal float64) {

	stats.Count++
	stats.NumStats.NumericCount++

	var inIntgrVal int64
	switch inNumType {
	case SS_UINT8, SS_UINT16, SS_UINT32, SS_UINT64:
		inIntgrVal = int64(uintVal)
	case SS_INT8, SS_INT16, SS_INT32, SS_INT64:
		inIntgrVal = intVal
	case SS_FLOAT64:
		// Do nothing. We'll handle this later.
	}

	// we just use the Min stats for stored val comparison but apply the same
	// logic to max and sum
	switch inNumType {
	case SS_FLOAT64:
		UpdateMinMax(stats, CValueEnclosure{Dtype: SS_DT_FLOAT, CVal: fltVal})
		if stats.NumStats.Sum.Ntype == SS_DT_FLOAT {
			// incoming float, stored is float, simple sum
			stats.NumStats.Sum.FloatVal = stats.NumStats.Sum.FloatVal + fltVal
		} else {
			// incoming float, stored is non-float, upgrade it
			stats.NumStats.Sum.FloatVal = float64(stats.NumStats.Sum.IntgrVal) + fltVal
			stats.NumStats.Sum.Ntype = SS_DT_FLOAT
		}
	// incoming is NON-float
	default:
		UpdateMinMax(stats, CValueEnclosure{Dtype: SS_DT_SIGNED_NUM, CVal: inIntgrVal})
		if stats.NumStats.Sum.Ntype == SS_DT_FLOAT {
			// incoming non-float, stored is float, cast it
			stats.NumStats.Sum.FloatVal = stats.NumStats.Sum.FloatVal + float64(inIntgrVal)
		} else {
			// incoming non-float, stored is non-float, simple sum
			stats.NumStats.Sum.IntgrVal = stats.NumStats.Sum.IntgrVal + inIntgrVal
		}
	}
}

func getColByteSlice(rec *utils.Buffer, offset int, qid uint64) ([]byte, uint16, error) {

	if rec.Len() == 0 {
		return []byte{}, 0, errors.New("column value is empty")
	}

	var endIdx uint16
	typeByte, err := rec.At(offset)
	if err != nil {
		log.Errorf("qid=%d, getColByteSlice: failed to get type byte: %s", qid, err)
		return []byte{}, 0, err
	}

	switch typeByte {
	case VALTYPE_ENC_SMALL_STRING[0]:
		strlen := utils.BytesToUint16LittleEndian(rec.Slice(offset+1, offset+3))
		endIdx = strlen + 3
	case VALTYPE_ENC_BOOL[0], VALTYPE_ENC_INT8[0], VALTYPE_ENC_UINT8[0]:
		endIdx = 2
	case VALTYPE_ENC_INT16[0], VALTYPE_ENC_UINT16[0]:
		endIdx = 3
	case VALTYPE_ENC_INT32[0], VALTYPE_ENC_UINT32[0]:
		endIdx = 5
	case VALTYPE_ENC_INT64[0], VALTYPE_ENC_UINT64[0], VALTYPE_ENC_FLOAT64[0]:
		endIdx = 9
	case VALTYPE_ENC_BACKFILL[0]:
		endIdx = 1
	default:
		log.Errorf("qid=%d, getColByteSlice: dont know how to convert type=%v\n", qid, typeByte)
		return []byte{}, endIdx, errors.New("invalid rec type")
	}

	return rec.Slice(offset, offset+int(endIdx)), endIdx, nil
}

func (colWip *ColWip) CopyWipForTestOnly(cbuf []byte, cbufIdx uint32) {
	colWip.cbuf.Reset()
	colWip.cbuf.Append(cbuf[:cbufIdx])
	colWip.cbufidx = cbufIdx
}
