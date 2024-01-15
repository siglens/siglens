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

package utils

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash"
)

// ValType : One-byte that encodes data type of the value field
// 0000 0001 ==> Bool
// 0000 0010 ==> String
// 0000 0011 ==> Uint8
// 0000 0100 ==> Uint16
// 0000 0101 ==> Uint32
// 0000 0110 ==> Uint64
// 0000 0111 ==> int8
// 0000 1000 ==> int16
// 0000 1001 ==> int32
// 0000 1010 ==> int64
// 0000 1011 ==> Float64

// GLOBAL Defs
// proportion of available to allocate for specific uses
const MICRO_IDX_MEM_PERCENT = 35 // percent allocated for both rotated & unrotated metadata (cmi/searchmetadata)
const SSM_MEM_PERCENT = 20
const MICRO_IDX_CHECK_MEM_PERCENT = 5 // percent allocated for runtime checking & loading of cmis
const BUFFER_MEM_PERCENT = 5
const RAW_SEARCH_MEM_PERCENT = 15 // minimum percent allocated for segsearch
const METRICS_MEMORY_MEM_PERCENT = 20

// percent allocated for segmentsearchmeta (blocksummaries, blocklen/off)

const BLOCK_MICRO_MULTINODE_MEM_PERCENT = 80
const BLOCK_MICRO_CHECK_MULTINODE_MEM_PERCENT = 15
const RAW_SEARCH_MULTINODE_MEM_PERCENT = 95
const MULTINODE_SSM_MEM_PERCENT = 20

// if you change this size, adjust the block bloom size
const WIP_SIZE = 2_000_000
const PQMR_SIZE uint = 4000 // init size of pqs bitset
const WIP_NUM_RECS = 4000
const BLOOM_SIZE_HISTORY = 5 // number of entries to analyze to get next block's bloom size
const BLOCK_BLOOM_SIZE = 100 // the default should be on the smaller side. Let dynamic bloom sizing fix the optimal one
const BLOCK_RI_MAP_SIZE = 100

var MAX_BYTES_METRICS_BLOCK uint64 = 1e+8         // 100MB
var METRICS_SEARCH_ALLOCATE_BLOCK uint64 = 1.5e+8 // 150MB
var MAX_BYTES_METRICS_SEGMENT uint64 = 1e+10      // 10GB
var MAX_ACTIVE_SERIES_PER_SEGMENT = 10_000_000

const MAX_RAW_DATAPOINTS_IN_RESULT = 5_000_000

// leave some room for column name/value meta
// since we use 2 bytes for record len, columnname-len, we can accommodate 65535
const MAX_RECORD_SIZE = 63_000
const MAX_RECS_PER_WIP = 65_534
const BLOOM_COLL_PROBABILITY = 0.001
const RECORDLEN_BYTE_SIZE = 2

const LEN_BLOCK_CMI_SIZE = 4
const LEN_BLKNUM_CMI_SIZE = 2

const LEN_PQMR_BLK_SIZE = 2

const BLOCK_SUMMARY_SIZE = 50_000
const BLOCK_SUMMARY_LEN_SIZE = 4
const BLOCK_SUMMARY_TS_SIZE = 8
const BLOCK_SUMMARY_REC_CNT_SIZE = 2

const RI_SIZE = 2_000_000
const RI_BLK_LEN_SIZE = 4

const FILE_READ_BUFFER_SIZE = 100_000
const DEFAULT_TIME_SLICE_SIZE = 10_000

const COL_OFF_BYTE_SIZE = 2

const NUMCOLS_OFF_START = RECORDLEN_BYTE_SIZE
const NUMCOLS_OFF_END = NUMCOLS_OFF_START + 2

const COL_BLK_OFF_START = NUMCOLS_OFF_END

const BLOCK_BLOOM_SEPARATOR = ":"

const MS_IN_MIN = 60_000     // 60 * 1000
const MS_IN_HOUR = 3_600_000 // 60 * 60 * 1000
const MS_IN_DAY = 86_400_000 // 24 * 60 * 60 * 1000

var BYTE_SPACE = []byte(" ")
var BYTE_SPACE_LEN = len(BYTE_SPACE)
var BYTE_EMPTY_STRING = []byte("")

var VALTYPE_ENC_BOOL = []byte{0x01}
var VALTYPE_ENC_SMALL_STRING = []byte{0x02}
var VALTYPE_ENC_UINT8 = []byte{0x03}
var VALTYPE_ENC_UINT16 = []byte{0x04}
var VALTYPE_ENC_UINT32 = []byte{0x05}
var VALTYPE_ENC_UINT64 = []byte{0x06}
var VALTYPE_ENC_INT8 = []byte{0x07}
var VALTYPE_ENC_INT16 = []byte{0x08}
var VALTYPE_ENC_INT32 = []byte{0x09}
var VALTYPE_ENC_INT64 = []byte{0x10}
var VALTYPE_ENC_FLOAT64 = []byte{0x11}
var VALTYPE_ENC_LARGE_STRING = []byte{0x12}
var VALTYPE_ENC_BACKFILL = []byte{0x13}
var VALTYPE_DICT_ARRAY = []byte{0x14}
var VALTYPE_RAW_JSON = []byte{0x15}

var VERSION_TAGSTREE = []byte{0x01}
var VERSION_TSOFILE = []byte{0x01}
var VERSION_TSGFILE = []byte{0x01}
var VERSION_MBLOCKSUMMARY = []byte{0x01}

type SS_DTYPE uint8

const (
	SS_INVALID SS_DTYPE = iota
	SS_DT_BOOL
	SS_DT_SIGNED_NUM
	SS_DT_UNSIGNED_NUM
	SS_DT_FLOAT
	SS_DT_STRING
	SS_DT_STRING_SET
	SS_DT_BACKFILL
	SS_DT_SIGNED_32_NUM
	SS_DT_USIGNED_32_NUM
	SS_DT_SIGNED_16_NUM
	SS_DT_USIGNED_16_NUM
	SS_DT_SIGNED_8_NUM
	SS_DT_USIGNED_8_NUM
	SS_DT_ARRAY_DICT
	SS_DT_RAW_JSON
)

const STALE_RECENTLY_ROTATED_ENTRY_MS = 60_000             // one minute
const SEGMENT_ROTATE_DURATION_SECONDS = 15 * 60            // 15 mins
var UPLOAD_INGESTNODE_DIR = time.Duration(1 * time.Minute) // one minute
const SEGMENT_ROTATE_SLEEP_DURATION_SECONDS = 60           // 1 min

var QUERY_EARLY_EXIT_LIMIT = uint64(10_000)
var QUERY_MAX_BUCKETS = uint64(10_000)

var ZSTD_COMLUNAR_BLOCK = []byte{0}
var ZSTD_DICTIONARY_BLOCK = []byte{1}
var TIMESTAMP_TOPDIFF_VARENC = []byte{2}
var STAR_TREE_BLOCK = []byte{3}

type SS_IntUintFloatTypes int

const (
	SS_UINT8 SS_IntUintFloatTypes = iota
	SS_UINT16
	SS_UINT32
	SS_UINT64
	SS_INT8
	SS_INT16
	SS_INT32
	SS_INT64
	SS_FLOAT64
)

type RangeNumType uint8

// If you add new datatype under RangeNumType please add corresponding encoding VALTYPE_ENC_RNT_* in the following block
const (
	RNT_UNSIGNED_INT RangeNumType = iota
	RNT_SIGNED_INT
	RNT_FLOAT64
)

var VALTYPE_ENC_RNT_UNSIGNED_INT = []byte{0x00}
var VALTYPE_ENC_RNT_SIGNED_INT = []byte{0x01}
var VALTYPE_ENC_RNT_FLOAT64 = []byte{0x02}

type FilterOperator int

const (
	Equals FilterOperator = iota
	NotEquals
	LessThan
	LessThanOrEqualTo
	GreaterThan
	GreaterThanOrEqualTo
	// Between - on the query parser to break down
	// In - on the query parser to break down
	IsNull
	IsNotNull
)

func (e FilterOperator) ToString() string {
	switch e {
	case Equals:
		return "eq"
	case NotEquals:
		return "neq"
	case LessThan:
		return "lt"
	case GreaterThan:
		return "gt"
	case LessThanOrEqualTo:
		return "lte"
	case GreaterThanOrEqualTo:
		return "gte"
	default:
		return fmt.Sprintf("%d", int(e))
	}
}

var STAR_BYTE = []byte("*")

// Maps a filter to its equivalent if left and right were swapped
// If a range filter is given as left op right, what is the right op if it swaps to right op* left
var ReflectFilterOperator = map[FilterOperator]FilterOperator{
	Equals:               Equals,
	NotEquals:            NotEquals,
	LessThan:             GreaterThan,
	LessThanOrEqualTo:    GreaterThanOrEqualTo,
	GreaterThan:          LessThan,
	GreaterThanOrEqualTo: LessThanOrEqualTo,
	IsNull:               IsNull,
	IsNotNull:            IsNotNull,
}

type ArithmeticOperator int

const (
	Add ArithmeticOperator = iota
	Subtract
	Divide
	Multiply
	Modulo
	BitwiseAnd
	BitwiseOr
	BitwiseExclusiveOr
)

type LogicalAndArithmeticOperator int

const (
	LetAdd LogicalAndArithmeticOperator = iota
	LetSubtract
	LetDivide
	LetMultiply
	LetModulo
	LetEquals
	LetNotEquals
	LetLessThan
	LetLessThanOrEqualTo
	LetGreaterThan
	LetGreaterThanOrEqualTo
)

type AggregateFunctions int

const (
	Count AggregateFunctions = iota + 1
	Avg
	Min
	Max
	Range
	Sum
	Cardinality
	Quantile
	Values
)

type MathFunctions int

const (
	Round MathFunctions = iota + 1
	Ceil
	Abs
	Sqrt
	Exp
)

type RangeFunctions int

const (
	Derivative RangeFunctions = iota + 1
	Rate
)

// For columns used by aggs with eval statements, we should keep their raw values because we need to evaluate them
// For columns only used by aggs without eval statements, we should not keep their raw values because it is a waste of performance
// If we only use two modes. Later occurring aggs will overwrite earlier occurring aggs' usage status. E.g. stats dc(eval(lower(state))), dc(state)
type AggColUsageMode int

const (
	NoEvalUsage   AggColUsageMode = iota // NoEvalUsage indicates that the column will be used by an aggregator without an eval function
	WithEvalUsage                        // WithEvalUsage indicates that the column will be used by an aggregator with an eval function
	BothUsage                            // BothUsage indicates that the column will be used by both types of aggregators simultaneously
)

func (e AggregateFunctions) String() string {
	switch e {
	case Count:
		return "count"
	case Avg:
		return "avg"
	case Min:
		return "min"
	case Max:
		return "max"
	case Range:
		return "range"
	case Sum:
		return "sum"
	case Cardinality:
		return "cardinality"
	case Values:
		return "values"
	default:
		return fmt.Sprintf("%d", int(e))
	}
}

type TagOperator int

const (
	Equal TagOperator = iota
	NotEqual
	Regex
	NegRegex
)

func (e TagOperator) String() string {
	switch e {
	case Equal:
		return "eq"
	case NotEqual:
		return "neq"
	default:
		return fmt.Sprintf("%d", int(e))
	}
}

type LogicalOperator int

const (
	Or LogicalOperator = iota
	And
	Exclusion
)

// every time you change this struct remember to adjust CreateDtypeEnclosure and ResetDtypeEnclosure
type DtypeEnclosure struct {
	Dtype          SS_DTYPE
	BoolVal        uint8
	UnsignedVal    uint64
	SignedVal      int64
	FloatVal       float64
	StringVal      string
	StringValBytes []byte         // byte slice representation of StringVal
	rexpCompiled   *regexp.Regexp //  should be unexported to allow for gob encoding
}

func (dte *DtypeEnclosure) SetRegexp(exp *regexp.Regexp) {
	dte.rexpCompiled = exp
}

func (dte *DtypeEnclosure) GetRegexp() *regexp.Regexp {
	return dte.rexpCompiled
}

// used for numeric calcs and promotions
type NumTypeEnclosure struct {
	Ntype    SS_DTYPE `json:"ntype,omitempty"`
	IntgrVal int64    `json:"intgrVal,omitempty"`
	FloatVal float64  `json:"floatVal,omitempty"`
}

func (nte *NumTypeEnclosure) ToCValueEnclosure() (*CValueEnclosure, error) {
	switch nte.Ntype {
	case SS_DT_FLOAT:
		return &CValueEnclosure{
			Dtype: SS_DT_FLOAT,
			CVal:  nte.FloatVal,
		}, nil
	case SS_DT_SIGNED_NUM, SS_DT_SIGNED_32_NUM, SS_DT_SIGNED_16_NUM, SS_DT_SIGNED_8_NUM:
		return &CValueEnclosure{
			Dtype: nte.Ntype,
			CVal:  nte.IntgrVal,
		}, nil
	default:
		return nil, fmt.Errorf("ToCValueEnclosure: unexpected Ntype: %v", nte.Ntype)
	}
}

var CMI_BLOOM_INDEX = []byte{0x01}
var CMI_RANGE_INDEX = []byte{0x02}
var CMI_INVERTED_INDEX = []byte{0x03}

func (dte *DtypeEnclosure) AddStringAsByteSlice() {
	switch dte.Dtype {
	case SS_DT_STRING:
		dte.StringValBytes = []byte(dte.StringVal)
	}
}

func (dte *DtypeEnclosure) IsNumeric() bool {
	switch dte.Dtype {
	case SS_DT_BOOL:
		return false
	case SS_DT_STRING:
		return false
	default:
		return true
	}
}

func (dte *DtypeEnclosure) IsString() bool {
	switch dte.Dtype {
	case SS_DT_STRING:
		return true
	default:
		return false
	}
}

func (dte *DtypeEnclosure) Reset() {
	dte.Dtype = 0
	dte.BoolVal = 0
	dte.UnsignedVal = 0
	dte.SignedVal = 0
	dte.FloatVal = 0
	dte.StringVal = ""
	dte.rexpCompiled = nil
}

func (dte *DtypeEnclosure) IsFullWildcard() bool {
	switch dte.Dtype {
	case SS_DT_STRING:
		if dte.StringVal == "*" {
			return true
		}
		return dte.rexpCompiled != nil && dte.rexpCompiled.String() == ".*"
	default:
		return false
	}
}

func (dte *DtypeEnclosure) IsRegex() bool {
	switch dte.Dtype {
	case SS_DT_STRING:
		if strings.Contains(dte.StringVal, "*") {
			return true
		}
		return dte.GetRegexp() != nil
	default:
		return false
	}
}

func (dte *DtypeEnclosure) GetValue() (interface{}, error) {
	switch dte.Dtype {
	case SS_DT_STRING:
		return dte.StringVal, nil
	case SS_DT_BOOL:
		return dte.BoolVal, nil
	case SS_DT_UNSIGNED_NUM:
		return dte.UnsignedVal, nil
	case SS_DT_SIGNED_NUM:
		return dte.SignedVal, nil
	case SS_DT_FLOAT:
		return dte.FloatVal, nil
	case SS_DT_BACKFILL:
		return nil, nil
	default:
		return nil, fmt.Errorf("GetValue: unsupported Dtype: %v", dte.Dtype)
	}
}

type CValueEnclosure struct {
	Dtype SS_DTYPE
	CVal  interface{}
}

// resets the CValueEnclosure to the given value
func (e *CValueEnclosure) ConvertValue(val interface{}) error {
	switch val := val.(type) {
	case string:
		e.Dtype = SS_DT_STRING
		e.CVal = val
	case bool:
		e.Dtype = SS_DT_BOOL
		e.CVal = val
	case uint64:
		e.Dtype = SS_DT_UNSIGNED_NUM
		e.CVal = val
	case int64:
		e.Dtype = SS_DT_SIGNED_NUM
		e.CVal = val
	case float64:
		e.Dtype = SS_DT_FLOAT
		e.CVal = val
	case nil:
		e.Dtype = SS_DT_BACKFILL
		e.CVal = nil
	default:
		return fmt.Errorf("ConvertValue: unsupported type: %T", val)
	}
	return nil
}

func (e *CValueEnclosure) GetValue() (interface{}, error) {
	switch e.Dtype {
	case SS_DT_STRING_SET:
		return e.CVal.(map[string]struct{}), nil
	case SS_DT_STRING:
		return e.CVal.(string), nil
	case SS_DT_BOOL:
		return e.CVal.(bool), nil
	case SS_DT_UNSIGNED_NUM:
		return e.CVal.(uint64), nil
	case SS_DT_SIGNED_NUM:
		return e.CVal.(int64), nil
	case SS_DT_FLOAT:
		return e.CVal.(float64), nil
	case SS_DT_BACKFILL:
		return nil, nil
	default:
		return nil, fmt.Errorf("GetValue: unsupported Dtype: %v", e.Dtype)
	}
}

func (e *CValueEnclosure) GetString() (string, error) {
	switch e.Dtype {
	case SS_DT_STRING:
		return e.CVal.(string), nil
	case SS_DT_BOOL:
		return strconv.FormatBool(e.CVal.(bool)), nil
	case SS_DT_UNSIGNED_NUM:
		return strconv.FormatUint(e.CVal.(uint64), 10), nil
	case SS_DT_SIGNED_NUM:
		return strconv.FormatInt(e.CVal.(int64), 10), nil
	case SS_DT_FLOAT:
		return fmt.Sprintf("%f", e.CVal.(float64)), nil
	default:
		return "", errors.New("CValueEnclosure GetString: unsupported Dtype")
	}
}

func (e *CValueEnclosure) GetFloatValue() (float64, error) {
	switch e.Dtype {
	case SS_DT_STRING, SS_DT_BOOL:
		return 0, errors.New("CValueEnclosure GetFloatValue: cannot convert to float")
	case SS_DT_UNSIGNED_NUM:
		return float64(e.CVal.(uint64)), nil
	case SS_DT_SIGNED_NUM:
		return float64(e.CVal.(int64)), nil
	case SS_DT_FLOAT:
		return e.CVal.(float64), nil
	default:
		return 0, errors.New("CValueEnclosure GetFloatValue: unsupported Dtype")
	}
}

/*
Returns a uint64 representation of the value

if its a number, casts to uint64
if its a string, xxhashed and returns it
*/
func (e *CValueEnclosure) GetUIntValue() (uint64, error) {
	switch e.Dtype {
	case SS_DT_STRING:
		return xxhash.Sum64String(e.CVal.(string)), nil
	case SS_DT_BACKFILL:
		return 0, nil
	case SS_DT_BOOL:
		if e.CVal.(bool) {
			return 1, nil
		} else {
			return 0, nil
		}
	// Treat it as a Uint64; using it as a 4-byte object, not a number
	case SS_DT_UNSIGNED_NUM:
		return e.CVal.(uint64), nil
	case SS_DT_FLOAT:
		return uint64(e.CVal.(float64)), nil
	case SS_DT_SIGNED_NUM:
		return uint64(e.CVal.(int64)), nil
	default:
		return 0, errors.New("CValueEnclosure GetUIntValue: unsupported Dtype")
	}
}

type CValueDictEnclosure struct {
	Dtype       SS_DTYPE
	CValString  string
	CValBool    bool
	CValUInt64  uint64
	CValInt64   int64
	CValFloat64 float64
	CValUInt32  uint32
	CValInt32   int32
	CValUInt16  uint16
	CValInt16   int16
	CValUInt    uint8
	CValInt     int8
}

func (e *CValueDictEnclosure) GetValue() (interface{}, error) {
	switch e.Dtype {
	case SS_DT_STRING:
		return e.CValString, nil
	case SS_DT_BOOL:
		return e.CValBool, nil
	case SS_DT_UNSIGNED_NUM:
		return e.CValUInt64, nil
	case SS_DT_SIGNED_NUM:
		return e.CValInt64, nil
	case SS_DT_FLOAT:
		return e.CValFloat64, nil
	case SS_DT_USIGNED_32_NUM:
		return e.CValUInt32, nil
	case SS_DT_SIGNED_32_NUM:
		return e.CValInt32, nil
	case SS_DT_USIGNED_16_NUM:
		return e.CValUInt16, nil
	case SS_DT_SIGNED_16_NUM:
		return e.CValInt16, nil
	case SS_DT_USIGNED_8_NUM:
		return e.CValUInt, nil
	case SS_DT_SIGNED_8_NUM:
		return e.CValInt, nil
	default:
		return nil, errors.New("CValueDictEnclosure GetValue: unsupported Dtype")
	}
}

// Information about the segment key for a record
// Stores if the RRC came from a remote node
type SegKeyInfo struct {
	// Encoded segment key
	SegKeyEnc uint16
	// If the RRC came from a remote node
	IsRemote bool
	// if IsRemote, Record will be initialized to a string of the form <<node_id>>-<<segkey>>-<<block_num>>-<<record_num>>
	RecordId string
}

type RecordResultContainer struct {
	SegKeyInfo       SegKeyInfo // Information about the segment key for a record (remote or not)
	BlockNum         uint16     // Block number of the record
	RecordNum        uint16     // Record number of the record
	SortColumnValue  float64    // Sort column value of the record
	TimeStamp        uint64     // Timestamp of the record
	VirtualTableName string     // Table name of the record
}

type BlkRecIdxContainer struct {
	BlkRecIndexes    map[uint16]map[uint16]uint64
	VirtualTableName string
}

func ConvertOperatorToString(op LogicalOperator) string {
	switch op {
	case And:
		return "AND"
	case Or:
		return "OR"
	case Exclusion:
		return "EXCLUSION"
	default:
		return ""
	}
}

type SIGNAL_TYPE uint8

const (
	SIGNAL_METRICS_OTSDB      = 1
	SIGNAL_EVENTS             = 2
	SIGNAL_JAEGER_TRACES      = 3
	SIGNAL_METRICS_INFLUX     = 4
	SIGNAL_METRICS_PROMETHEUS = 5
)

type RR_ENC_TYPE uint8

const (
	RR_ENC_UINT64 = 1
	RR_ENC_BITSET = 2
)
