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
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/axiomhq/hyperloglog"
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
	"github.com/siglens/siglens/pkg/segment/writer/stats"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	bbp "github.com/valyala/bytebufferpool"
)

var wipCardLimit uint16 = 1001

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
	signalType segutils.SIGNAL_TYPE) (uint32, bool, error) {

	var maxIdx uint32 = 0
	var matchedCol = false

	ss.encodeTime(recordTime, tsKey)
	var err error
	maxIdx, matchedCol, err = ss.encodeRawJsonObject("", rawData, maxIdx, tsKey, matchedCol, signalType)
	if err != nil {
		log.Errorf("Failed to encode json object! err: %+v", err)
		return maxIdx, matchedCol, err
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
		copy(colWip.cbuf[colWip.cbufidx:], VALTYPE_ENC_BACKFILL[:])
		colWip.cbufidx += 1
		// also do backfill dictEnc for this recnum
		checkAddDictEnc(colWip, VALTYPE_ENC_BACKFILL[:], ss.wipBlock.blockSummary.RecCount)
	}

	return maxIdx, matchedCol, nil
}

func (ss *SegStore) encodeRawJsonObject(currKey string, data []byte, maxIdx uint32, tsKey *string,
	matchedCol bool, signalType segutils.SIGNAL_TYPE) (uint32, bool, error) {
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
			maxIdx, matchedCol, err = ss.encodeRawJsonObject(finalKey, value, maxIdx, tsKey, matchedCol, signalType)
			if err != nil {
				return fmt.Errorf("encodeRawJsonObject: obj currKey: %v, err: %v", currKey, err)
			}
		case jp.Array:
			if signalType == SIGNAL_JAEGER_TRACES {

				maxIdx, matchedCol, err = ss.encodeRawJsonArray(finalKey, value, maxIdx, tsKey, matchedCol, signalType)
			} else {
				maxIdx, matchedCol, err = ss.encodeNonJaegerRawJsonArray(finalKey, value, maxIdx, tsKey, matchedCol, signalType)
			}
			if err != nil {
				return fmt.Errorf("encodeRawJsonObject: arr currKey: %v, err: %v", currKey, err)
			}
		case jp.String:
			strVal, err := jp.ParseString(value)
			if err != nil {
				return fmt.Errorf("encodeRawJsonObject: str currKey: %v, err: %v", currKey, err)
			}
			maxIdx, matchedCol, err = ss.encodeSingleString(finalKey, strVal, maxIdx, tsKey, matchedCol)
			if err != nil {
				return fmt.Errorf("encodeRawJsonObject: singstr currKey: %v, err: %v", currKey, err)
			}
		case jp.Number:
			numVal, err := jp.ParseInt(value)
			if err != nil {
				fltVal, err := jp.ParseFloat(value)
				if err != nil {
					return fmt.Errorf("encodeRawJsonObject: flt currKey: %v, err: %v", currKey, err)
				}
				maxIdx, matchedCol, _ = ss.encodeSingleNumber(finalKey, fltVal, maxIdx, tsKey, matchedCol)
				return nil
			}
			maxIdx, matchedCol, _ = ss.encodeSingleNumber(finalKey, numVal, maxIdx, tsKey, matchedCol)
		case jp.Boolean:
			boolVal, err := jp.ParseBoolean(value)
			if err != nil {
				return fmt.Errorf("encodeRawJsonObject: bool currKey: %v, err: %v", currKey, err)
			}
			maxIdx, matchedCol, err = ss.encodeSingleBool(finalKey, boolVal, maxIdx, tsKey, matchedCol)
			if err != nil {
				return fmt.Errorf("encodeRawJsonObject: singbool currKey: %v, err: %v", currKey, err)
			}
		case jp.Null:
			maxIdx, matchedCol, err = ss.encodeSingleNull(finalKey, maxIdx, tsKey, matchedCol)
			if err != nil {
				return fmt.Errorf("encodeRawJsonObject: singnull currKey: %v, err: %v", currKey, err)
			}
		default:
			return fmt.Errorf("currKey: %v, received unknown type of %+s", currKey, valueType)
		}
		return nil
	}
	err := jp.ObjectEach(data, handler)
	return maxIdx, matchedCol, err
}

func (ss *SegStore) encodeRawJsonArray(currKey string, data []byte, maxIdx uint32, tsKey *string,
	matchedCol bool, signalType segutils.SIGNAL_TYPE) (uint32, bool, error) {
	var encErr error
	if signalType == SIGNAL_JAEGER_TRACES {
		if currKey != "references" && currKey != "logs" {
			maxIdx, matchedCol, encErr = ss.encodeSingleDictArray(currKey, data, maxIdx, tsKey, matchedCol, signalType)
			if encErr != nil {
				log.Infof("encodeRawJsonArray error %s", encErr)
				return maxIdx, matchedCol, encErr
			}
		} else {
			maxIdx, matchedCol, encErr = ss.encodeSingleRawBuffer(currKey, data, maxIdx, tsKey, matchedCol, signalType)
			if encErr != nil {
				return maxIdx, matchedCol, encErr
			}
		}
	}
	return maxIdx, matchedCol, nil
}

func (ss *SegStore) encodeNonJaegerRawJsonArray(currKey string, data []byte, maxIdx uint32, tsKey *string,
	matchedCol bool, signalType segutils.SIGNAL_TYPE) (uint32, bool, error) {
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
			maxIdx, matchedCol, encErr = ss.encodeRawJsonObject(finalKey, value, maxIdx, tsKey, matchedCol, signalType)
			if encErr != nil {
				finalErr = encErr
				return
			}
		case jp.Array:
			maxIdx, matchedCol, encErr = ss.encodeNonJaegerRawJsonArray(finalKey, value, maxIdx, tsKey, matchedCol, signalType)
			if encErr != nil {
				finalErr = encErr
				return
			}
		case jp.String:
			strVal, encErr := jp.ParseString(value)
			if encErr != nil {
				finalErr = encErr
				return
			}
			maxIdx, matchedCol, encErr = ss.encodeSingleString(finalKey, strVal, maxIdx, tsKey, matchedCol)
			if encErr != nil {
				finalErr = encErr
				return
			}
		case jp.Number:
			numVal, encErr := jp.ParseInt(value)
			if encErr != nil {
				fltVal, encErr := jp.ParseFloat(value)
				if encErr != nil {
					finalErr = encErr
					return
				}
				maxIdx, matchedCol, _ = ss.encodeSingleNumber(finalKey, fltVal, maxIdx, tsKey, matchedCol)
				return
			}
			maxIdx, matchedCol, _ = ss.encodeSingleNumber(finalKey, numVal, maxIdx, tsKey, matchedCol)
		case jp.Boolean:
			boolVal, encErr := jp.ParseBoolean(value)
			if encErr != nil {
				finalErr = encErr
				return
			}
			maxIdx, matchedCol, encErr = ss.encodeSingleBool(finalKey, boolVal, maxIdx, tsKey, matchedCol)
			if encErr != nil {
				finalErr = encErr
				return
			}
		case jp.Null:
			maxIdx, matchedCol, encErr = ss.encodeSingleNull(finalKey, maxIdx, tsKey, matchedCol)
			if encErr != nil {
				finalErr = encErr
				return
			}
		default:
			finalErr = fmt.Errorf("received unknown type of %+s", valueType)
			return
		}
	})
	if aErr != nil {
		finalErr = aErr
	}
	return maxIdx, matchedCol, finalErr
}

func (ss *SegStore) encodeSingleDictArray(arraykey string, data []byte, maxIdx uint32,
	tsKey *string, matchedCol bool, signalType segutils.SIGNAL_TYPE) (uint32, bool, error) {
	if arraykey == *tsKey {
		return maxIdx, matchedCol, nil
	}
	var finalErr error
	var colWip *ColWip
	colWip, _, matchedCol = ss.initAndBackFillColumn(arraykey, data, matchedCol)
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
	copy(colWip.cbuf[colWip.cbufidx:], VALTYPE_DICT_ARRAY[:])
	colWip.cbufidx += 1
	copy(colWip.cbuf[colWip.cbufidx:], utils.Uint16ToBytesLittleEndian(0)) //placeholder for encoding length of array
	colWip.cbufidx += 2
	_, aErr := jp.ArrayEach(data, func(value []byte, valueType jp.ValueType, offset int, err error) {
		switch valueType {
		case jp.Object:
			keyName, keyType, keyVal, err := getNestedDictEntries(value)
			if err != nil {
				log.Errorf("getNestedDictEntries error %+v", err)
				return
			}
			if keyName == "" || keyType == "" || keyVal == "" {
				err = fmt.Errorf("encodeSingleDictArray: Jaeger tags array should have key/value/type values")
				log.Error(err)
				return
			}
			//encode and copy keyName
			n := uint16(len(keyName))
			copy(colWip.cbuf[colWip.cbufidx:], utils.Uint16ToBytesLittleEndian(n))
			colWip.cbufidx += 2
			copy(colWip.cbuf[colWip.cbufidx:], keyName)
			colWip.cbufidx += uint32(n)
			//check key type
			//based on that encode key value
			switch keyType {
			case "string":
				copy(colWip.cbuf[colWip.cbufidx:], VALTYPE_ENC_SMALL_STRING[:])
				colWip.cbufidx += 1
				n := uint16(len(keyVal))
				copy(colWip.cbuf[colWip.cbufidx:], utils.Uint16ToBytesLittleEndian(n))
				colWip.cbufidx += 2
				copy(colWip.cbuf[colWip.cbufidx:], keyVal)
				colWip.cbufidx += uint32(n)
			case "bool":
				copy(colWip.cbuf[colWip.cbufidx:], VALTYPE_ENC_BOOL[:])
				colWip.cbufidx += 1
				n := uint16(len(keyVal))
				copy(colWip.cbuf[colWip.cbufidx:], utils.Uint16ToBytesLittleEndian(n))
				colWip.cbufidx += 2
				copy(colWip.cbuf[colWip.cbufidx:], keyVal)
				colWip.cbufidx += uint32(n)
			case "int64":
				copy(colWip.cbuf[colWip.cbufidx:], VALTYPE_ENC_INT64[:])
				colWip.cbufidx += 1
				n := uint16(len(keyVal))
				copy(colWip.cbuf[colWip.cbufidx:], utils.Uint16ToBytesLittleEndian(n))
				colWip.cbufidx += 2
				copy(colWip.cbuf[colWip.cbufidx:], keyVal)
				colWip.cbufidx += uint32(n)
			case "float64":
				copy(colWip.cbuf[colWip.cbufidx:], segutils.VALTYPE_ENC_FLOAT64[:])
				colWip.cbufidx += 1
				n := uint16(len(keyVal))
				copy(colWip.cbuf[colWip.cbufidx:], utils.Uint16ToBytesLittleEndian(n))
				colWip.cbufidx += 2
				copy(colWip.cbuf[colWip.cbufidx:], keyVal)
				colWip.cbufidx += uint32(n)
			default:
				finalErr = fmt.Errorf("encodeSingleDictArray : received unknown key  %+s", keyType)
			}
			if bi != nil {
				bi.uniqueWordCount += addToBlockBloom(bi.Bf, []byte(keyName))
				bi.uniqueWordCount += addToBlockBloom(bi.Bf, []byte(keyVal))
			}
			stats.AddSegStatsStr(ss.AllSst, keyName, keyVal, ss.wipBlock.bb, nil, false)
			if colWip.cbufidx > maxIdx {
				maxIdx = colWip.cbufidx
			}
		default:
			finalErr = fmt.Errorf("encodeSingleDictArray : received unknown type of %+s", valueType)
			return
		}
	})
	copy(colWip.cbuf[s+1:], utils.Uint16ToBytesLittleEndian(uint16(colWip.cbufidx-s-3)))
	if aErr != nil {
		finalErr = aErr
	}
	return maxIdx, matchedCol, finalErr
}

func getNestedDictEntries(data []byte) (string, string, string, error) {
	var nkey, ntype, nvalue string

	handler := func(key []byte, value []byte, valueType jp.ValueType, off int) error {
		switch string(key) {
		case "key":
			if valueType != jp.String {
				err := fmt.Errorf("getNestedDictEntries key should be of type string , found type %+v", valueType)
				return err
			}
			nkey = string(value)
		case "type":
			ntype = string(value)
		case "value":
			nvalue = string(value)
		default:
			err := fmt.Errorf("getNestedDictEntries: received unknown key of %+s", key)
			return err
		}
		return nil
	}
	err := jp.ObjectEach(data, handler)
	return nkey, ntype, nvalue, err

}

func (ss *SegStore) encodeSingleRawBuffer(key string, value []byte, maxIdx uint32,
	tsKey *string, matchedCol bool, signalType segutils.SIGNAL_TYPE) (uint32, bool, error) {
	if key == *tsKey {
		return maxIdx, matchedCol, nil
	}
	var colWip *ColWip
	colWip, _, matchedCol = ss.initAndBackFillColumn(key, value, matchedCol)
	colBlooms := ss.wipBlock.columnBlooms
	var bi *BloomIndex
	var ok bool
	if key != "_type" && key != "_index" && key != "tags" {
		_, ok = colBlooms[key]
		if !ok {
			bi = &BloomIndex{}
			bi.uniqueWordCount = 0
			bCount := getBlockBloomSize(bi)
			bi.Bf = bloom.NewWithEstimates(uint(bCount), BLOOM_COLL_PROBABILITY)
			colBlooms[key] = bi
		}
	}
	//[utils.VALTYPE_RAW_JSON][raw-byte-len][raw-byte]
	copy(colWip.cbuf[colWip.cbufidx:], VALTYPE_RAW_JSON[:])
	colWip.cbufidx += 1
	n := uint16(len(value))
	copy(colWip.cbuf[colWip.cbufidx:], utils.Uint16ToBytesLittleEndian(n))
	colWip.cbufidx += 2
	copy(colWip.cbuf[colWip.cbufidx:], value)
	colWip.cbufidx += uint32(n)

	if colWip.cbufidx > maxIdx {
		maxIdx = colWip.cbufidx
	}
	return maxIdx, matchedCol, nil
}

func (ss *SegStore) encodeSingleString(key string, value string, maxIdx uint32,
	tsKey *string, matchedCol bool) (uint32, bool, error) {
	if key == *tsKey {
		return maxIdx, matchedCol, nil
	}
	var colWip *ColWip
	var recNum uint16
	colWip, recNum, matchedCol = ss.initAndBackFillColumn(key, value, matchedCol)
	colBlooms := ss.wipBlock.columnBlooms
	var bi *BloomIndex
	var ok bool
	if key != "_type" && key != "_index" {
		bi, ok = colBlooms[key]
		if !ok {
			bi = &BloomIndex{}
			bi.uniqueWordCount = 0
			bCount := getBlockBloomSize(bi)
			bi.Bf = bloom.NewWithEstimates(uint(bCount), BLOOM_COLL_PROBABILITY)
			colBlooms[key] = bi
		}
	}
	s := colWip.cbufidx
	colWip.WriteSingleString(value)

	if bi != nil {
		bi.uniqueWordCount += addToBlockBloom(bi.Bf, []byte(value))
	}
	if !ss.skipDe {
		checkAddDictEnc(colWip, colWip.cbuf[s:colWip.cbufidx], recNum)
	}
	stats.AddSegStatsStr(ss.AllSst, key, value, ss.wipBlock.bb, nil, false)
	if colWip.cbufidx > maxIdx {
		maxIdx = colWip.cbufidx
	}
	return maxIdx, matchedCol, nil
}

func (ss *SegStore) encodeSingleBool(key string, val bool, maxIdx uint32,
	tsKey *string, matchedCol bool) (uint32, bool, error) {
	if key == *tsKey {
		return maxIdx, matchedCol, nil
	}
	var colWip *ColWip
	colBlooms := ss.wipBlock.columnBlooms
	colWip, _, matchedCol = ss.initAndBackFillColumn(key, val, matchedCol)
	var bi *BloomIndex
	var ok bool

	bi, ok = colBlooms[key]
	if !ok {
		bi = &BloomIndex{}
		bi.uniqueWordCount = 0
		bCount := 10
		bi.Bf = bloom.NewWithEstimates(uint(bCount), BLOOM_COLL_PROBABILITY)
		colBlooms[key] = bi
	}
	copy(colWip.cbuf[colWip.cbufidx:], VALTYPE_ENC_BOOL[:])
	colWip.cbufidx += 1
	copy(colWip.cbuf[colWip.cbufidx:], utils.BoolToBytesLittleEndian(val))
	colWip.cbufidx += 1

	if bi != nil {
		bi.uniqueWordCount += addToBlockBloom(bi.Bf, []byte(strconv.FormatBool(val)))
	}
	if colWip.cbufidx > maxIdx {
		maxIdx = colWip.cbufidx
	}
	return maxIdx, matchedCol, nil
}

func (ss *SegStore) encodeSingleNull(key string, maxIdx uint32,
	tsKey *string, matchedCol bool) (uint32, bool, error) {
	if key == *tsKey {
		return maxIdx, matchedCol, nil
	}
	var colWip *ColWip
	colWip, _, matchedCol = ss.initAndBackFillColumn(key, nil, matchedCol)
	copy(colWip.cbuf[colWip.cbufidx:], VALTYPE_ENC_BACKFILL[:])
	colWip.cbufidx += 1
	if colWip.cbufidx > maxIdx {
		maxIdx = colWip.cbufidx
	}
	return maxIdx, matchedCol, nil
}

func (ss *SegStore) encodeSingleNumber(key string, value interface{}, maxIdx uint32,
	tsKey *string, matchedCol bool) (uint32, bool, error) {
	if key == *tsKey {
		return maxIdx, matchedCol, nil
	}
	var colWip *ColWip
	var recNum uint16
	colWip, recNum, matchedCol = ss.initAndBackFillColumn(key, value, matchedCol)
	colRis := ss.wipBlock.columnRangeIndexes
	segstats := ss.AllSst
	retLen := encSingleNumber(key, value, colWip.cbuf[:], colWip.cbufidx, colRis, recNum, segstats,
		ss.wipBlock.bb, colWip)
	colWip.cbufidx += retLen

	if colWip.cbufidx > maxIdx {
		maxIdx = colWip.cbufidx
	}
	return maxIdx, matchedCol, nil
}

func (ss *SegStore) initAndBackFillColumn(key string, value interface{}, matchedCol bool) (*ColWip, uint16, bool) {
	allColWip := ss.wipBlock.colWips
	colBlooms := ss.wipBlock.columnBlooms
	colRis := ss.wipBlock.columnRangeIndexes
	allColsInBlock := ss.wipBlock.columnsInBlock
	recNum := ss.wipBlock.blockSummary.RecCount

	colWip, ok := allColWip[key]
	if !ok {
		colWip = InitColWip(ss.SegmentKey, key)
		allColWip[key] = colWip
		ss.AllSeenColumns[key] = true
	}
	_, ok = allColsInBlock[key]
	if !ok {
		if recNum != 0 {
			log.Debugf("EncodeColumns: newColumn=%v showed up in the middle, backfilling it now", key)
			backFillPastRecords(key, value, recNum, colBlooms, colRis, colWip)
		}
	}
	allColsInBlock[key] = true
	matchedCol = matchedCol || ss.pqTracker.isColumnInPQuery(key)
	colWip.cstartidx = colWip.cbufidx
	return colWip, recNum, matchedCol
}

func initMicroIndices(key string, val interface{}, colBlooms map[string]*BloomIndex,
	colRis map[string]*RangeIndex) {
	switch val.(type) {
	case string:
		bi := &BloomIndex{}
		bi.uniqueWordCount = 0
		bCount := getBlockBloomSize(bi)
		bi.Bf = bloom.NewWithEstimates(uint(bCount), BLOOM_COLL_PROBABILITY)
		colBlooms[key] = bi

	case float64, int64, uint64, json.Number:
		ri := &RangeIndex{}
		ri.Ranges = make(map[string]*Numbers, BLOCK_RI_MAP_SIZE)
		colRis[key] = ri

	case bool:
		// todo kunal, for bool type we need to keep a inverted index
		bi := &BloomIndex{}
		bi.uniqueWordCount = 0
		bCount := 10
		bi.Bf = bloom.NewWithEstimates(uint(bCount), BLOOM_COLL_PROBABILITY)
		colBlooms[key] = bi
	}
}

func backFillPastRecords(key string, val interface{}, recNum uint16, colBlooms map[string]*BloomIndex,
	colRis map[string]*RangeIndex, colWip *ColWip) uint32 {
	initMicroIndices(key, val, colBlooms, colRis)
	packedLen := uint32(0)

	recArr := make([]uint16, recNum)
	for i := uint16(0); i < recNum; i++ {
		// only the type will be saved when we are backfilling
		copy(colWip.cbuf[colWip.cbufidx:], VALTYPE_ENC_BACKFILL[:])
		colWip.cbufidx += 1
		packedLen += 1
		recArr[i] = i
	}
	// we will also init dictEnc for backfilled recnums
	colWip.deMap[string(VALTYPE_ENC_BACKFILL[:])] = recArr
	colWip.deCount++
	return packedLen
}

func encSingleNumber(key string, val interface{}, wipbuf []byte, idx uint32,
	colRis map[string]*RangeIndex, wRecNum uint16,
	segstats map[string]*SegStats, bb *bbp.ByteBuffer, colWip *ColWip) uint32 {

	ri, ok := colRis[key]
	if !ok {
		ri = &RangeIndex{}
		ri.Ranges = make(map[string]*Numbers, BLOCK_RI_MAP_SIZE)
		colRis[key] = ri
	}

	switch cval := val.(type) {
	case float64:
		addSegStatsNums(segstats, key, SS_FLOAT64, FPARM_INT64, FPARM_UINT64, cval,
			fmt.Sprintf("%v", cval), bb)
		valSize := encJsonNumber(key, SS_FLOAT64, FPARM_INT64, FPARM_UINT64, cval, wipbuf[:],
			idx, ri.Ranges)
		checkAddDictEnc(colWip, wipbuf[idx:idx+valSize], wRecNum)
		return valSize
	case int64:
		addSegStatsNums(segstats, key, SS_INT64, cval, FPARM_UINT64, FPARM_FLOAT64,
			fmt.Sprintf("%v", cval), bb)

		valSize := encJsonNumber(key, SS_INT64, cval, FPARM_UINT64, FPARM_FLOAT64, wipbuf[:],
			idx, ri.Ranges)
		checkAddDictEnc(colWip, wipbuf[idx:idx+valSize], wRecNum)
		return valSize

	default:
		log.Errorf("encSingleNumber: Tried to encode a non int/float value! value=%+v", cval)
	}
	return 0
}

func encJsonNumber(key string, numType SS_IntUintFloatTypes, intVal int64, uintVal uint64,
	fltVal float64, wipbuf []byte, idx uint32, blockRangeIndex map[string]*Numbers) uint32 {

	var valSize uint32

	switch numType {
	case SS_INT64:
		copy(wipbuf[idx:], VALTYPE_ENC_INT64[:])
		copy(wipbuf[idx+1:], utils.Int64ToBytesLittleEndian(int64(intVal)))
		valSize = 1 + 8
	case SS_UINT64:
		copy(wipbuf[idx:], VALTYPE_ENC_UINT64[:])
		copy(wipbuf[idx+1:], utils.Uint64ToBytesLittleEndian(uintVal))
		valSize = 1 + 8
	case SS_FLOAT64:
		copy(wipbuf[idx:], VALTYPE_ENC_FLOAT64[:])
		copy(wipbuf[idx+1:], utils.Float64ToBytesLittleEndian(fltVal))
		valSize = 1 + 8
	default:
		log.Errorf("encJsonNumber: unknown numType: %v", numType)
	}

	if blockRangeIndex != nil {
		updateRangeIndex(key, blockRangeIndex, numType, intVal, uintVal, fltVal)
	}

	return valSize
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
returns:
   CValEncoslure: Cval encoding of this col entry
   uint16: len of this entry inside that was inside the byte slice
   error:
*/
func GetCvalFromRec(rec []byte, qid uint64) (CValueEnclosure, uint16, error) {

	if len(rec) == 0 {
		return CValueEnclosure{}, 0, errors.New("column value is empty")
	}

	var retVal CValueEnclosure
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
			return CValueEnclosure{}, 0, err
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
				return retVal, endIdx, errors.New("invalid rec type")
			}
			cValArray = append(cValArray, cVal)
		}
		retVal.CVal = cValArray
		endIdx = uint16(totalLen)

	default:
		log.Errorf("qid=%d, GetCvalFromRec: dont know how to convert type=%v\n", qid, rec[0])
		return retVal, endIdx, errors.New("invalid rec type")
	}

	return retVal, endIdx, nil
}

func WriteMockColSegFile(segkey string, numBlocks int, entryCount int) ([]map[string]*BloomIndex,
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

	tsKey := config.GetTimeStampKey()
	allCols := make(map[string]bool)
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
			pqMatches:          make(map[string]*pqmr.PQMatchResults),
			columnsInBlock:     mapCol,
			tomRollup:          make(map[uint64]*RolledRecs),
			tohRollup:          make(map[uint64]*RolledRecs),
			todRollup:          make(map[uint64]*RolledRecs),
			bb:                 bbp.Get(),
			blockTs:            make([]uint64, 0),
		}
		segStore := &SegStore{
			wipBlock:       wipBlock,
			SegmentKey:     segkey,
			AllSeenColumns: allCols,
			pqTracker:      initPQTracker(),
			AllSst:         segstats,
			numBlocks:      currBlockUint,
		}
		for i := 0; i < entryCount; i++ {
			entry := make(map[string]interface{})
			entry[cnames[0]] = "match words 123 abc"
			entry[cnames[1]] = "value1"
			entry[cnames[2]] = i
			entry[cnames[3]] = (i%2 == 0)
			entry[cnames[4]] = strconv.FormatUint(uint64(i)*2, 10)
			entry[cnames[5]] = "batch-" + fmt.Sprint(j) + "-" + utils.RandomStringWithCharset(10)
			entry[cnames[6]] = (i * 2)
			entry[cnames[7]] = "batch-" + fmt.Sprint(j)
			entry[cnames[8]] = j
			entry[cnames[9]] = rand.Float64()
			entry[cnames[10]] = segkey
			entry[cnames[11]] = "record-batch-" + fmt.Sprint(i%2)

			timestp := uint64(i) + 1 // dont start with 0 as timestamp
			raw, _ := json.Marshal(entry)
			_, _, err := segStore.EncodeColumns(raw, timestp, &tsKey, SIGNAL_EVENTS)
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
			blkLen, blkOffset, err := writeWip(colWip, encType)
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

	tsKey := config.GetTimeStampKey()
	allCols := make(map[string]bool)
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
			pqMatches:          make(map[string]*pqmr.PQMatchResults),
			columnsInBlock:     mapCol,
			tomRollup:          make(map[uint64]*RolledRecs),
			tohRollup:          make(map[uint64]*RolledRecs),
			todRollup:          make(map[uint64]*RolledRecs),
			bb:                 bbp.Get(),
			blockTs:            make([]uint64, 0),
		}
		segStore := &SegStore{
			wipBlock:       wipBlock,
			SegmentKey:     segkey,
			AllSeenColumns: allCols,
			pqTracker:      initPQTracker(),
			AllSst:         segstats,
			numBlocks:      currBlockUint,
		}
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
		_, _, err := segStore.EncodeColumns(entry, timestp, &tsKey, SIGNAL_JAEGER_TRACES)
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
			blkLen, blkOffset, err := writeWip(colWip, encType)
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

	blkRIBuf := make([]byte, RI_SIZE)

	// copy the blockNum
	copy(blkRIBuf[idx:], utils.Uint16ToBytesLittleEndian(blkNum))
	idx += 2

	copy(blkRIBuf[idx:], CMI_RANGE_INDEX)
	idx += 1 // for CMI type

	for key, item := range blockRangeIndex {
		if len(blkRIBuf) < int(idx) {
			newSlice := make([]byte, RI_SIZE)
			blkRIBuf = append(blkRIBuf, newSlice...)
		}
		copy(blkRIBuf[idx:], utils.Uint16ToBytesLittleEndian(uint16(len(key))))
		idx += 2
		n := copy(blkRIBuf[idx:], key)
		idx += uint32(n)
		switch item.NumType {
		case RNT_UNSIGNED_INT:
			copy(blkRIBuf[idx:], VALTYPE_ENC_RNT_UNSIGNED_INT[:])
			idx += 1
			copy(blkRIBuf[idx:], utils.Uint64ToBytesLittleEndian(item.Min_uint64))
			idx += 8
			copy(blkRIBuf[idx:], utils.Uint64ToBytesLittleEndian(item.Max_uint64))
			idx += 8
		case RNT_SIGNED_INT:
			copy(blkRIBuf[idx:], VALTYPE_ENC_RNT_SIGNED_INT[:])
			idx += 1
			copy(blkRIBuf[idx:], utils.Int64ToBytesLittleEndian(item.Min_int64))
			idx += 8
			copy(blkRIBuf[idx:], utils.Int64ToBytesLittleEndian(item.Max_int64))
			idx += 8
		case RNT_FLOAT64:
			copy(blkRIBuf[idx:], VALTYPE_ENC_RNT_FLOAT64[:])
			idx += 1
			copy(blkRIBuf[idx:], utils.Float64ToBytesLittleEndian(item.Min_float64))
			idx += 8
			copy(blkRIBuf[idx:], utils.Float64ToBytesLittleEndian(item.Max_float64))
			idx += 8
		}
	}
	// copy the recordlen at the start of the buf
	copy(blkRIBuf[0:], utils.Uint32ToBytesLittleEndian(uint32(idx-RI_BLK_LEN_SIZE)))
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
		ss.AllSeenColumns[*tsKey] = true
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

func WriteMockTsRollup(segkey string) error {

	ss := &SegStore{suffix: 1, lock: sync.Mutex{}, SegmentKey: segkey}

	wipBlock := createMockTsRollupWipBlock(segkey)
	ss.wipBlock = *wipBlock
	err := ss.writeWipTsRollups("timestamp")
	return err
}

func createMockTsRollupWipBlock(segkey string) *WipBlock {

	config.InitializeTestingConfig()
	defer os.RemoveAll(config.GetDataPath()) // we just create a suffix file during segstore creation

	cTime := uint64(time.Now().UnixMilli())
	lencnames := uint8(2)
	cnames := make([]string, lencnames)
	for cidx := uint8(0); cidx < lencnames; cidx += 1 {
		currCol := fmt.Sprintf("fortscheckkey-%v", cidx)
		cnames[cidx] = currCol
	}
	sId := "ts-rollup"
	segstore, err := getSegStore(sId, cTime, "test", 0)
	if err != nil {
		log.Errorf("createMockTsRollupWipBlock, getSegstore err=%v", err)
		return nil
	}
	tsKey := config.GetTimeStampKey()
	entryCount := 1000

	startTs := uint64(1652222966645) // Tuesday, May 10, 2022 22:49:26.645
	tsincr := uint64(7200)           // so that we have 2 hours, 2 days, and > 2mins buckets

	runningTs := startTs
	for i := 0; i < entryCount; i++ {
		//		t.Logf("TestTimestampEncoding: ts=%v", runningTs)
		record_json := make(map[string]interface{})
		record_json[cnames[0]] = "value1"
		record_json[cnames[1]] = json.Number(fmt.Sprint(i))
		rawJson, _ := json.Marshal(record_json)
		_, _, err := segstore.EncodeColumns(rawJson, runningTs, &tsKey, SIGNAL_EVENTS)
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

	if len(blockSummBuf) < requiredLen {
		newSlice := make([]byte, requiredLen-len(blockSummBuf))
		blockSummBuf = append(blockSummBuf, newSlice...)
	}

	// reserve first 4 bytes for BLOCK_SUMMARY_LEN.
	idx += 4

	copy(blockSummBuf[idx:], utils.Uint16ToBytesLittleEndian(blkNum))
	idx += 2
	copy(blockSummBuf[idx:], utils.Uint64ToBytesLittleEndian(bsum.HighTs))
	idx += 8
	copy(blockSummBuf[idx:], utils.Uint64ToBytesLittleEndian(bsum.LowTs))
	idx += 8
	copy(blockSummBuf[idx:], utils.Uint16ToBytesLittleEndian(bsum.RecCount))
	idx += 2
	copy(blockSummBuf[idx:], utils.Uint16ToBytesLittleEndian(numCols))
	idx += 2

	for cname, cOff := range bmh.ColumnBlockOffset {
		copy(blockSummBuf[idx:], utils.Uint16ToBytesLittleEndian(uint16(len(cname))))
		idx += 2
		copy(blockSummBuf[idx:], cname)
		idx += uint32(len(cname))
		copy(blockSummBuf[idx:], utils.Int64ToBytesLittleEndian(cOff))
		idx += 8
		copy(blockSummBuf[idx:], utils.Uint32ToBytesLittleEndian(bmh.ColumnBlockLen[cname]))
		idx += 4
	}

	// copy the summlen at the start of the buf
	copy(blockSummBuf[0:], utils.Uint32ToBytesLittleEndian(uint32(idx)))

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

func checkAddDictEnc(colWip *ColWip, cval []byte, recNum uint16) {
	if colWip.deCount < wipCardLimit {
		recs, ok := colWip.deMap[string(cval)]
		if !ok {
			recs = make([]uint16, 0)
			colWip.deCount += 1
		}
		recs = append(recs, recNum)
		colWip.deMap[string(cval)] = recs
		// todo we optimize this code, by pre-allocing a fixed length of recs, keep an idx, then add it to recs
		// advantages: 1) we avoid extending the array. 2) we avoid inserting in the map on every rec
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

	colWip.cbufidx = 0
	// reuse the existing cbuf
	// copy num of dict words
	copy(colWip.cbuf[colWip.cbufidx:], utils.Uint16ToBytesLittleEndian(colWip.deCount))
	colWip.cbufidx += 2

	for dword, recNumsArr := range colWip.deMap {

		// copy the actual dict word , the TLV is packed inside the dword
		copy(colWip.cbuf[colWip.cbufidx:], []byte(dword))
		colWip.cbufidx += uint32(len(dword))

		// copy num of records
		numRecs := uint16(len(recNumsArr))
		copy(colWip.cbuf[colWip.cbufidx:], utils.Uint16ToBytesLittleEndian(numRecs))
		colWip.cbufidx += 2

		for i := uint16(0); i < numRecs; i++ {
			// copy the recNum
			copy(colWip.cbuf[colWip.cbufidx:], utils.Uint16ToBytesLittleEndian(recNumsArr[i]))
			colWip.cbufidx += 2
		}
	}
}

func addSegStatsStr(segstats map[string]*SegStats, cname string, strVal string,
	bb *bbp.ByteBuffer) {

	var stats *SegStats
	var ok bool
	stats, ok = segstats[cname]
	if !ok {
		stats = &SegStats{
			IsNumeric: false,
			Count:     0,
			Hll:       hyperloglog.New16()}

		segstats[cname] = stats
	}

	stats.Count++
	bb.Reset()
	_, _ = bb.WriteString(strVal)
	stats.Hll.Insert(bb.B)
}

func addSegStatsNums(segstats map[string]*SegStats, cname string,
	inNumType SS_IntUintFloatTypes, intVal int64, uintVal uint64,
	fltVal float64, numstr string, bb *bbp.ByteBuffer) {

	var stats *SegStats
	var ok bool
	stats, ok = segstats[cname]
	if !ok {
		numStats := &NumericStats{
			Min: NumTypeEnclosure{Ntype: SS_DT_SIGNED_NUM,
				IntgrVal: math.MaxInt64,
				FloatVal: math.MaxFloat64,
			},
			Max: NumTypeEnclosure{Ntype: SS_DT_SIGNED_NUM,
				IntgrVal: math.MinInt64,
				FloatVal: math.SmallestNonzeroFloat64,
			},
			Sum: NumTypeEnclosure{Ntype: SS_DT_SIGNED_NUM,
				IntgrVal: 0,
				FloatVal: 0},
		}
		stats = &SegStats{
			IsNumeric: true,
			Count:     0,
			Hll:       hyperloglog.New16(),
			NumStats:  numStats,
		}
		segstats[cname] = stats
	}

	// prior entries were non numeric, so we should init NumStats, but keep the hll and count vars
	if stats.NumStats == nil {
		numStats := &NumericStats{
			Min: NumTypeEnclosure{Ntype: SS_DT_SIGNED_NUM,
				IntgrVal: math.MaxInt64,
				FloatVal: math.MaxFloat64,
			},
			Max: NumTypeEnclosure{Ntype: SS_DT_SIGNED_NUM,
				IntgrVal: math.MinInt64,
				FloatVal: math.SmallestNonzeroFloat64,
			},
			Sum: NumTypeEnclosure{Ntype: SS_DT_SIGNED_NUM,
				IntgrVal: 0,
				FloatVal: 0},
		}
		stats.NumStats = numStats
		stats.IsNumeric = true // TODO: what if we have a mix of numeric and non-numeric
	}

	bb.Reset()
	_, _ = bb.WriteString(numstr)
	stats.Hll.Insert(bb.B)
	processStats(stats, inNumType, intVal, uintVal, fltVal)
}

func processStats(stats *SegStats, inNumType SS_IntUintFloatTypes, intVal int64,
	uintVal uint64, fltVal float64) {

	stats.Count++

	var inIntgrVal int64
	switch inNumType {
	case SS_UINT8, SS_UINT16, SS_UINT32, SS_UINT64:
		inIntgrVal = int64(uintVal)
	case SS_INT8, SS_INT16, SS_INT32, SS_INT64:
		inIntgrVal = intVal
	}

	// we just use the Min stats for stored val comparison but apply the same
	// logic to max and sum
	switch inNumType {
	case SS_FLOAT64:
		if stats.NumStats.Min.Ntype == SS_DT_FLOAT {
			// incoming float, stored is float, simple min
			stats.NumStats.Min.FloatVal = math.Min(stats.NumStats.Min.FloatVal, fltVal)
			stats.NumStats.Max.FloatVal = math.Max(stats.NumStats.Max.FloatVal, fltVal)
			stats.NumStats.Sum.FloatVal = stats.NumStats.Sum.FloatVal + fltVal
		} else {
			// incoming float, stored is non-float, upgrade it
			stats.NumStats.Min.FloatVal = math.Min(float64(stats.NumStats.Min.IntgrVal), fltVal)
			stats.NumStats.Min.Ntype = SS_DT_FLOAT

			stats.NumStats.Max.FloatVal = math.Max(float64(stats.NumStats.Max.IntgrVal), fltVal)
			stats.NumStats.Max.Ntype = SS_DT_FLOAT

			stats.NumStats.Sum.FloatVal = float64(stats.NumStats.Sum.IntgrVal) + fltVal
			stats.NumStats.Sum.Ntype = SS_DT_FLOAT
		}
	// incoming is NON-float
	default:
		if stats.NumStats.Min.Ntype == SS_DT_FLOAT {
			// incoming non-float, stored is float, cast it
			stats.NumStats.Min.FloatVal = math.Min(stats.NumStats.Min.FloatVal, float64(inIntgrVal))
			stats.NumStats.Max.FloatVal = math.Max(stats.NumStats.Max.FloatVal, float64(inIntgrVal))
			stats.NumStats.Sum.FloatVal = stats.NumStats.Sum.FloatVal + float64(inIntgrVal)
		} else {
			// incoming non-float, stored is non-float, simple min
			stats.NumStats.Min.IntgrVal = utils.MinInt64(stats.NumStats.Min.IntgrVal, inIntgrVal)
			stats.NumStats.Max.IntgrVal = utils.MaxInt64(stats.NumStats.Max.IntgrVal, inIntgrVal)
			stats.NumStats.Sum.IntgrVal = stats.NumStats.Sum.IntgrVal + inIntgrVal
		}
	}

}

func getColByteSlice(rec []byte, qid uint64) ([]byte, uint16, error) {

	if len(rec) == 0 {
		return []byte{}, 0, errors.New("column value is empty")
	}

	var endIdx uint16
	switch rec[0] {

	case VALTYPE_ENC_SMALL_STRING[0]:
		strlen := utils.BytesToUint16LittleEndian(rec[1:3])
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
		log.Errorf("qid=%d, getColByteSlice: dont know how to convert type=%v\n", qid, rec[0])
		return []byte{}, endIdx, errors.New("invalid rec type")
	}

	return rec[0:endIdx], endIdx, nil
}
