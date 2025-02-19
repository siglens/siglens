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
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
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

// How the metadata memory is split. These should sum to 100.
const METADATA_LOGS_MEM_PERCENT = 70
const METADATA_METRICS_MEM_PERCENT = 30

// if you change this size, adjust the block bloom size
const WIP_SIZE = 2_000_000

const PQMR_SIZE uint = 4000 // init size of pqs bitset
const WIP_NUM_RECS = 4000
const BLOOM_SIZE_HISTORY = 5 // number of entries to analyze to get next block's bloom size
const BLOCK_BLOOM_SIZE = 10
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

// Splunk limits the number of values returned by stat list to 100 values.
// We can use similar limit for stat list
// https://docs.splunk.com/Documentation/SplunkCloud/9.1.2312/SearchReference/Multivaluefunctions
const MAX_SPL_LIST_SIZE = 100

var BYTE_SPACE = []byte(" ")
var BYTE_SPACE_LEN = len(BYTE_SPACE)
var BYTE_EMPTY_STRING = []byte("")

var BYTE_TILDE = []byte("~")
var BYTE_TILDE_LEN = len(BYTE_TILDE)

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
var STR_VALTYPE_ENC_BACKFILL = string([]byte{0x13})
var VALTYPE_DICT_ARRAY = []byte{0x14}
var VALTYPE_RAW_JSON = []byte{0x15}

var VERSION_TAGSTREE = []byte{0x01}
var VERSION_TSOFILE = []byte{0x01}
var VERSION_TSGFILE = []byte{0x01}
var VERSION_MBLOCKSUMMARY = []byte{0x01}

var VERSION_SEGSTATS = []byte{2} // version of the Segment Stats file.
var VERSION_SEGSTATS_LEGACY = []byte{1}

var VERSION_SEGSTATS_BUF_V4 = []byte{4} // current version of the single column Seg Stats in a Segment
// deprecated versions
var VERSION_SEGSTATS_BUF_V1 = []byte{1}
var VERSION_SEGSTATS_BUF_V2 = []byte{2}
var VERSION_SEGSTATS_BUF_V3 = []byte{3}

const INCONSISTENT_CVAL_SIZE uint32 = math.MaxUint32

const MAX_SIMILAR_ERRORS_TO_LOG = 5 // max number of similar errors to log: This is used to avoid flooding the logs with similar errors

type T_SegReaderId = uint16
type T_SegEncoding = uint32

type SS_DTYPE uint8

const (
	SS_INVALID SS_DTYPE = iota
	SS_DT_BOOL
	SS_DT_SIGNED_NUM
	SS_DT_UNSIGNED_NUM
	SS_DT_FLOAT
	SS_DT_STRING
	SS_DT_STRING_SLICE
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

func ValTypeToSSDType(valtype byte) SS_DTYPE {
	switch valtype {
	case VALTYPE_ENC_BOOL[0]:
		return SS_DT_BOOL
	case VALTYPE_ENC_UINT8[0]:
		return SS_DT_USIGNED_8_NUM
	case VALTYPE_ENC_UINT16[0]:
		return SS_DT_USIGNED_16_NUM
	case VALTYPE_ENC_UINT32[0]:
		return SS_DT_USIGNED_32_NUM
	case VALTYPE_ENC_UINT64[0]:
		return SS_DT_UNSIGNED_NUM
	case VALTYPE_ENC_INT8[0]:
		return SS_DT_SIGNED_8_NUM
	case VALTYPE_ENC_INT16[0]:
		return SS_DT_SIGNED_16_NUM
	case VALTYPE_ENC_INT32[0]:
		return SS_DT_SIGNED_32_NUM
	case VALTYPE_ENC_INT64[0]:
		return SS_DT_SIGNED_NUM
	case VALTYPE_ENC_FLOAT64[0]:
		return SS_DT_FLOAT
	case VALTYPE_ENC_SMALL_STRING[0], VALTYPE_ENC_LARGE_STRING[0]:
		return SS_DT_STRING
	case VALTYPE_ENC_BACKFILL[0]:
		return SS_DT_BACKFILL
	case VALTYPE_DICT_ARRAY[0]:
		return SS_DT_ARRAY_DICT
	case VALTYPE_RAW_JSON[0]:
		return SS_DT_RAW_JSON
	default:
		log.Errorf("ValTypeToSSDType: invalid valtype: %v", valtype)
		return SS_INVALID
	}
}

const STALE_RECENTLY_ROTATED_ENTRY_MS = 60_000             // one minute
const SEGMENT_ROTATE_DURATION_SECONDS = 15 * 60            // 15 mins
var UPLOAD_INGESTNODE_DIR = time.Duration(1 * time.Minute) // one minute
const SEGMENT_ROTATE_SLEEP_DURATION_SECONDS = 120

const QUERY_EARLY_EXIT_LIMIT = uint64(100)
const QUERY_MAX_BUCKETS = uint64(10_000)

var ZSTD_COMLUNAR_BLOCK = []byte{0}
var ZSTD_DICTIONARY_BLOCK = []byte{1}
var TIMESTAMP_TOPDIFF_VARENC = []byte{2}
var VERSION_STAR_TREE_BLOCK = []byte{4}
var VERSION_STAR_TREE_BLOCK_LEGACY = []byte{3}

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
	LetPower
	LetEquals
	LetNotEquals
	LetLessThan
	LetLessThanOrEqualTo
	LetGreaterThan
	LetGreaterThanOrEqualTo
	LetAnd
	LetOr
	LetUnless
)

type AggregateFunctions int

const (
	Invalid AggregateFunctions = iota
	Count
	Avg
	Min
	Max
	Range
	Sum
	Cardinality
	Quantile
	TopK
	BottomK
	Stddev
	Stdvar
	Group
	Values
	List
	Estdc
	EstdcError
	ExactPerc
	Median
	Mode
	Perc
	UpperPerc
	Stdev
	Stdevp
	Sumsq
	Var
	Varp
	First
	Last
	Earliest
	EarliestTime
	Latest
	LatestTime
	StatsRate
)

type MathFunctions int

const (
	Round MathFunctions = iota + 1
	Ceil
	Floor
	Abs
	Sqrt
	Exp
	Ln
	Log2
	Log10
	Sgn
	Deg
	Rad
	Clamp
	Clamp_Max
	Clamp_Min
	Timestamp
	Acos
	Acosh
	Asin
	Asinh
	Atan
	Atanh
	Cos
	Cosh
	Sin
	Sinh
	Tan
	Tanh
)

type TimeFunctions float64

const (
	Hour TimeFunctions = iota + 1
	Minute
	Month
	Year
	DayOfMonth
	DayOfWeek
	DayOfYear
	DaysInMonth
)

type RangeFunctions int

const (
	Derivative RangeFunctions = iota + 1
	Predict_Linear
	Rate
	IRate
	Increase
	Delta
	IDelta
	Avg_Over_Time
	Min_Over_Time
	Max_Over_Time
	Sum_Over_Time
	Count_Over_Time
	Stdvar_Over_Time
	Stddev_Over_Time
	Mad_Over_Time
	Last_Over_Time
	Present_Over_Time
	Quantile_Over_Time
	Changes
	Resets
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
	case Quantile:
		return "quantile"
	case TopK:
		return "topk"
	case BottomK:
		return "bottomk"
	case Stddev:
		return "stddev"
	case Stdvar:
		return "stdvar"
	case Group:
		return "group"
	case Values:
		return "values"
	case List:
		return "list"
	case Estdc:
		return "estdc"
	case EstdcError:
		return "estdc_error"
	case ExactPerc:
		return "exactperc"
	case Median:
		return "median"
	case Mode:
		return "mode"
	case Perc:
		return "perc"
	case UpperPerc:
		return "upperperc"
	case Stdev:
		return "stdev"
	case Stdevp:
		return "stdevp"
	case Sumsq:
		return "sumsq"
	case Var:
		return "var"
	case Varp:
		return "varp"
	case First:
		return "first"
	case Last:
		return "last"
	case Earliest:
		return "earliest"
	case EarliestTime:
		return "earliest_time"
	case Latest:
		return "latest"
	case LatestTime:
		return "latest_time"
	case StatsRate:
		return "rate"
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
	StringValBytes []byte   // byte slice representation of StringVal
	StringSliceVal []string // used for array dict
	RexpCompiled   *regexp.Regexp
}

func (dte *DtypeEnclosure) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	for i, v := range []interface{}{dte.Dtype, dte.BoolVal, dte.UnsignedVal, dte.SignedVal, dte.FloatVal, dte.StringVal, dte.StringValBytes} {
		err := encoder.Encode(v)
		if err != nil {
			log.Errorf("DtypeEnclosure.GobEncode: error encoding %v in iteration %d: %v", v, i, err)
			return nil, err
		}
	}

	hasRegexp := dte.RexpCompiled != nil
	err := encoder.Encode(hasRegexp)
	if err != nil {
		log.Errorf("DtypeEnclosure.GobEncode: error encoding hasRegexp: %v", err)
		return nil, err
	}

	if hasRegexp {
		err := encoder.Encode(dte.RexpCompiled.String())
		if err != nil {
			log.Errorf("DtypeEnclosure.GobEncode: error encoding RexpCompiled: %v", err)
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (dte *DtypeEnclosure) GobDecode(data []byte) error {
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)

	for i, v := range []interface{}{&dte.Dtype, &dte.BoolVal, &dte.UnsignedVal, &dte.SignedVal, &dte.FloatVal, &dte.StringVal, &dte.StringValBytes} {
		err := decoder.Decode(v)
		if err != nil {
			log.Errorf("DtypeEnclosure.GobDecode: error decoding %v in iteration %d: %v", v, i, err)
			return err
		}
	}

	var hasRegexp bool
	err := decoder.Decode(&hasRegexp)
	if err != nil {
		log.Errorf("DtypeEnclosure.GobDecode: error decoding hasRegexp: %v", err)
		return err
	}

	if hasRegexp {
		var rexp string
		err := decoder.Decode(&rexp)
		if err != nil {
			log.Errorf("DtypeEnclosure.GobDecode: error decoding rexp: %v", err)
			return err
		}

		dte.RexpCompiled, err = regexp.Compile(rexp)
		if err != nil {
			log.Errorf("DtypeEnclosure.GobDecode: error compiling rexp %v: %v", rexp, err)
			return err
		}
	}

	return nil
}

func (dte *DtypeEnclosure) SetRegexp(exp *regexp.Regexp) {
	dte.RexpCompiled = exp
}

func (dte *DtypeEnclosure) GetRegexp() *regexp.Regexp {
	return dte.RexpCompiled
}

// used for numeric calcs and promotions
type NumTypeEnclosure struct {
	Ntype    SS_DTYPE `json:"ntype,omitempty"`
	IntgrVal int64    `json:"intgrVal,omitempty"`
	FloatVal float64  `json:"floatVal,omitempty"`
}

func (nte *NumTypeEnclosure) ToCValueEnclosure() (*CValueEnclosure, error) {
	if nte == nil {
		return nil, fmt.Errorf("ToCValueEnclosure: numTypeEnclosure is nil")
	}
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

func (nte *NumTypeEnclosure) Reset() {
	nte.Ntype = SS_INVALID
	nte.IntgrVal = 0
	nte.FloatVal = 0
}

func (cval *CValueEnclosure) ToNumber(number *Number) error {

	number.SetInvalidType()
	if cval == nil {
		return fmt.Errorf("ToNumber: cval is nil")
	}

	switch cval.Dtype {
	case SS_DT_FLOAT:
		val, ok := cval.CVal.(float64)
		if !ok {
			return fmt.Errorf("ToNumber: unexpected Dtype: %v", cval.Dtype)
		}
		number.SetFloat64(val)
	case SS_DT_SIGNED_NUM:
		val, ok := cval.CVal.(int64)
		if !ok {
			return fmt.Errorf("ToNumber: unexpected Dtype: %v", cval.Dtype)
		}
		number.SetInt64(int64(val))
	case SS_DT_UNSIGNED_NUM:
		val, ok := cval.CVal.(int64)
		if !ok {
			return fmt.Errorf("ToNumber: unexpected Dtype: %v", cval.Dtype)
		}
		number.SetInt64(val)
	case SS_DT_BACKFILL:
		number.SetBackfillType()
		return nil
	default:
		return fmt.Errorf("ToNumber: unexpected Dtype: %v", cval.Dtype)
	}

	return nil
}

func (cval *CValueEnclosure) ToNumType(res *NumTypeEnclosure) error {
	if cval == nil {
		return fmt.Errorf("ToNumType: cval is nil")
	}
	switch cval.Dtype {
	case SS_DT_FLOAT:
		res.Ntype = SS_DT_FLOAT
		res.FloatVal = cval.CVal.(float64)
		return nil
	case SS_DT_SIGNED_NUM:
		res.Ntype = SS_DT_SIGNED_NUM
		res.IntgrVal = cval.CVal.(int64)
		return nil
	case SS_DT_UNSIGNED_NUM:
		res.Ntype = SS_DT_UNSIGNED_NUM
		res.IntgrVal = int64(cval.CVal.(uint64))
		return nil
	case SS_DT_BACKFILL:
		res.Ntype = SS_DT_BACKFILL
		res.Ntype = SS_DT_BACKFILL
		return nil
	default:
		return fmt.Errorf("ToNumType: unexpected Ntype: %v", cval.Dtype)
	}
}

var CMI_BLOOM_INDEX = []byte{0x01}
var CMI_RANGE_INDEX = []byte{0x02}
var CMI_INVERTED_INDEX = []byte{0x03}

func (dte *DtypeEnclosure) AddStringAsByteSlice() {
	switch dte.Dtype {
	case SS_DT_STRING:
		dte.StringValBytes = []byte(dte.StringVal)
	default:
		// This function is only for string dtype. And converts only string to byte slice.
		// We do not want to log error for other dtypes. So we will just log debug message.
		log.Debugf("AddStringAsByteSlice: unsupported Dtype: %v", dte.Dtype)
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

func (dte *DtypeEnclosure) IsBool() bool {
	switch dte.Dtype {
	case SS_DT_BOOL:
		return true
	default:
		return false
	}
}

func (dte *DtypeEnclosure) IsFloat() bool {
	switch dte.Dtype {
	case SS_DT_FLOAT:
		return true
	default:
		return false
	}
}

func (dte *DtypeEnclosure) IsInt() bool {
	return dte.IsNumeric() && !dte.IsFloat()
}

func IsBoolean(str string) bool {
	lowerStr := strings.ToLower(str)
	return lowerStr == "true" || lowerStr == "false"
}

func (dte *DtypeEnclosure) Reset() {
	dte.Dtype = 0
	dte.BoolVal = 0
	dte.UnsignedVal = 0
	dte.SignedVal = 0
	dte.FloatVal = 0
	dte.StringVal = ""
	dte.RexpCompiled = nil
}

func (dte *DtypeEnclosure) IsFullWildcard() bool {
	switch dte.Dtype {
	case SS_DT_STRING:
		if dte.StringVal == "*" {
			return true
		}
		return dte.RexpCompiled != nil && dte.RexpCompiled.String() == ".*"
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

func (e *CValueEnclosure) Equal(other *CValueEnclosure) bool {
	if e.Dtype != other.Dtype {
		return false
	}

	switch e.Dtype {
	case SS_DT_STRING:
		return e.CVal.(string) == other.CVal.(string)
	case SS_DT_BOOL:
		return e.CVal.(bool) == other.CVal.(bool)
	case SS_DT_UNSIGNED_NUM:
		return e.CVal.(uint64) == other.CVal.(uint64)
	case SS_DT_SIGNED_NUM:
		return e.CVal.(int64) == other.CVal.(int64)
	case SS_DT_FLOAT:
		return math.Abs(e.CVal.(float64)-other.CVal.(float64)) < 1e-6
	case SS_DT_BACKFILL:
		return true
	default:
		log.Errorf("CValueEnclosure.Equal: unsupported Dtype: %v", e.Dtype)
		return false
	}
}

func (e *CValueEnclosure) Hash() uint64 {
	bytes := make([]byte, 0)
	bytes = append(bytes, byte(e.Dtype))

	switch e.Dtype {
	case SS_DT_STRING:
		bytes = append(bytes, []byte(e.CVal.(string))...)
	case SS_DT_BOOL:
		bytes = append(bytes, []byte(strconv.FormatBool(e.CVal.(bool)))...)
	case SS_DT_UNSIGNED_NUM:
		bytes = append(bytes, []byte(strconv.FormatUint(e.CVal.(uint64), 10))...)
	case SS_DT_SIGNED_NUM:
		bytes = append(bytes, []byte(strconv.FormatInt(e.CVal.(int64), 10))...)
	case SS_DT_FLOAT:
		bytes = append(bytes, []byte(strconv.FormatFloat(e.CVal.(float64), 'f', -1, 64))...)
	case SS_DT_BACKFILL:
		// Do nothing.
	default:
		log.Errorf("CValueEnclosure.Hash: unsupported Dtype: %v", e.Dtype)
		return 0
	}

	return xxhash.Sum64(bytes)
}

// resets the CValueEnclosure to the given value
func (e *CValueEnclosure) ConvertValue(val interface{}) error {
	switch val := val.(type) {
	case string:
		e.Dtype = SS_DT_STRING
		e.CVal = val
	case []string:
		e.Dtype = SS_DT_STRING_SLICE
		e.CVal = val
	case bool:
		e.Dtype = SS_DT_BOOL
		e.CVal = val
	case uint8:
		e.Dtype = SS_DT_UNSIGNED_NUM
		e.CVal = uint64(val)
	case uint16:
		e.Dtype = SS_DT_UNSIGNED_NUM
		e.CVal = uint64(val)
	case uint32:
		e.Dtype = SS_DT_UNSIGNED_NUM
		e.CVal = uint64(val)
	case uint64:
		e.Dtype = SS_DT_UNSIGNED_NUM
		e.CVal = val
	case uint:
		e.Dtype = SS_DT_UNSIGNED_NUM
		e.CVal = uint64(val)
	case int8:
		e.Dtype = SS_DT_SIGNED_NUM
		e.CVal = int64(val)
	case int16:
		e.Dtype = SS_DT_SIGNED_NUM
		e.CVal = int64(val)
	case int32:
		e.Dtype = SS_DT_SIGNED_NUM
		e.CVal = int64(val)
	case int64:
		e.Dtype = SS_DT_SIGNED_NUM
		e.CVal = val
	case int:
		e.Dtype = SS_DT_SIGNED_NUM
		e.CVal = int64(val)
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
	case SS_DT_STRING_SLICE:
		return e.CVal.([]string), nil
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

// TODO: After evaluation is fixed, merge GetString and GetValueAsString
func (e *CValueEnclosure) GetString() (string, error) {
	switch e.Dtype {
	case SS_DT_STRING:
		return e.CVal.(string), nil
	case SS_DT_STRING_SLICE:
		return fmt.Sprintf("%v", e.CVal.([]string)), nil
	case SS_DT_BOOL:
		return strconv.FormatBool(e.CVal.(bool)), nil
	case SS_DT_UNSIGNED_NUM:
		return strconv.FormatUint(e.CVal.(uint64), 10), nil
	case SS_DT_SIGNED_NUM:
		return strconv.FormatInt(e.CVal.(int64), 10), nil
	case SS_DT_FLOAT:
		return fmt.Sprintf("%f", e.CVal.(float64)), nil
	case SS_DT_BACKFILL:
		return "", toputils.NewErrorWithCode(toputils.NIL_VALUE_ERR, fmt.Errorf("CValueEnclosure GetString: nil value"))
	default:
		return "", fmt.Errorf("CValueEnclosure GetString: unsupported Dtype: %v", e.Dtype)
	}
}

func (e *CValueEnclosure) GetValueAsString() (string, error) {
	switch e.Dtype {
	case SS_DT_STRING:
		return e.CVal.(string), nil
	case SS_DT_STRING_SLICE:
		return fmt.Sprintf("%v", e.CVal.([]string)), nil
	case SS_DT_STRING_SET:
		return fmt.Sprintf("%v", e.CVal.(map[string]struct{})), nil
	case SS_DT_BOOL:
		return strconv.FormatBool(e.CVal.(bool)), nil
	case SS_DT_UNSIGNED_NUM:
		return strconv.FormatUint(e.CVal.(uint64), 10), nil
	case SS_DT_SIGNED_NUM:
		return strconv.FormatInt(e.CVal.(int64), 10), nil
	case SS_DT_FLOAT:
		return fmt.Sprintf("%f", e.CVal.(float64)), nil
	case SS_DT_BACKFILL:
		return "", nil
	default:
		return "", fmt.Errorf("CValueEnclosure.GetValueAsString: unsupported Dtype: %v", e.Dtype)
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
	case SS_DT_BACKFILL:
		return 0, toputils.NewErrorWithCode(toputils.NIL_VALUE_ERR, fmt.Errorf("CValueEnclosure GetFloatValue: nil value"))
	default:
		return 0, errors.New("CValueEnclosure GetFloatValue: unsupported Dtype")
	}
}

func (e *CValueEnclosure) GetFloatValueIfPossible() (float64, bool) {
	switch e.Dtype {
	case SS_DT_STRING:
		strVal := e.CVal.(string)
		if !toputils.MightBeFloat(strVal) {
			return 0, false
		}

		floatVal, err := strconv.ParseFloat(strVal, 64)
		if err != nil {
			return 0, false
		}
		return floatVal, true
	case SS_DT_UNSIGNED_NUM:
		return float64(e.CVal.(uint64)), true
	case SS_DT_SIGNED_NUM:
		return float64(e.CVal.(int64)), true
	case SS_DT_FLOAT:
		return e.CVal.(float64), true
	default:
		return 0, false
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

/*
Returns a int64 representation of the value

if its a number, casts to int64
if its a string, try to parse as int64, if fails, xxhashes and returns it
*/
func (e *CValueEnclosure) GetIntValue() (int64, error) {
	switch e.Dtype {
	case SS_DT_STRING:
		int64Val, err := strconv.ParseInt(e.CVal.(string), 10, 64)
		if err != nil {
			int64Val = int64(xxhash.Sum64String(e.CVal.(string)))
		}
		return int64Val, nil
	case SS_DT_BACKFILL:
		return 0, nil
	case SS_DT_BOOL:
		if e.CVal.(bool) {
			return 1, nil
		} else {
			return 0, nil
		}
	case SS_DT_UNSIGNED_NUM:
		return int64(e.CVal.(uint64)), nil
	case SS_DT_SIGNED_NUM:
		return e.CVal.(int64), nil
	case SS_DT_FLOAT:
		return int64(e.CVal.(float64)), nil
	default:
		return 0, fmt.Errorf("CValueEnclosure GetIntValue: unsupported Dtype: %v", e.Dtype)
	}
}

func (e *CValueEnclosure) getWriteTotalBytesSize() int {
	size := 0
	switch e.Dtype {
	case SS_DT_BOOL:
		size += 1 // for the type
		size += 1 // for the value
	case SS_DT_UNSIGNED_NUM:
		size += 1 // for the type
		size += 8 // for the value
	case SS_DT_SIGNED_NUM:
		size += 1 // for the type
		size += 8 // for the value
	case SS_DT_FLOAT:
		size += 1 // for the type
		size += 8 // for the value
	case SS_DT_STRING:
		size += 1 // for the type
		strLen := len(e.CVal.(string))
		sizeOfStrLen := 2    // 2 bytes
		size += sizeOfStrLen // for the length of the string
		size += strLen       // for the string
	case SS_DT_BACKFILL:
		size += 1 // for the type
	default:
		str := fmt.Sprintf("%v", e.CVal)
		strLen := len(str)
		if strLen <= math.MaxUint16 {
			size += 1 // for the type
			sizeOfStrLen := 2
			size += sizeOfStrLen // for the length of the string
		} else {
			size += 1 // for the type
			sizeOfStrLen := 4
			size += sizeOfStrLen // for the length of the string
		}
		size += strLen // for the string
	}

	return size
}

// WriteToBytesWithType writes the CValueEnclosure to a byte slice with the type
// The byte slice is resized if required
func (e *CValueEnclosure) WriteToBytesWithType(buf []byte, bufIdx int) ([]byte, int) {
	requiredSize := e.getWriteTotalBytesSize()
	availableSize := len(buf) - bufIdx

	// Resize the buffer if required
	if requiredSize > availableSize {
		buf = toputils.ResizeSlice(buf, len(buf)+requiredSize+MAX_RECORD_SIZE)
	}

	switch e.Dtype {
	case SS_DT_BOOL:
		copy(buf[bufIdx:], VALTYPE_ENC_BOOL)
		bufIdx += 1
		if e.CVal.(bool) {
			buf[bufIdx] = 1
		} else {
			buf[bufIdx] = 0
		}
		bufIdx += 1
	case SS_DT_UNSIGNED_NUM:
		copy(buf[bufIdx:], VALTYPE_ENC_UINT64)
		bufIdx += 1
		toputils.Uint64ToBytesLittleEndianInplace(e.CVal.(uint64), buf[bufIdx:bufIdx+8])
		bufIdx += 8
	case SS_DT_SIGNED_NUM:
		copy(buf[bufIdx:], VALTYPE_ENC_INT64)
		bufIdx += 1
		toputils.Int64ToBytesLittleEndianInplace(e.CVal.(int64), buf[bufIdx:bufIdx+8])
		bufIdx += 8
	case SS_DT_FLOAT:
		copy(buf[bufIdx:], VALTYPE_ENC_FLOAT64)
		bufIdx += 1
		toputils.Float64ToBytesLittleEndianInplace(e.CVal.(float64), buf[bufIdx:bufIdx+8])
		bufIdx += 8
	case SS_DT_STRING:
		copy(buf[bufIdx:], VALTYPE_ENC_SMALL_STRING)
		bufIdx += 1
		strBytes := []byte(e.CVal.(string))
		strLen := len(strBytes)
		copy(buf[bufIdx:], toputils.Uint16ToBytesLittleEndian(uint16(strLen)))
		bufIdx += 2
		copy(buf[bufIdx:], strBytes)
		bufIdx += strLen
	case SS_DT_BACKFILL:
		copy(buf[bufIdx:], VALTYPE_ENC_BACKFILL)
		bufIdx += 1
	default:
		str := fmt.Sprintf("%v", e.CVal)
		strBytes := []byte(str)
		strLen := len(strBytes)
		if strLen <= 255 {
			copy(buf[bufIdx:], VALTYPE_ENC_SMALL_STRING)
			bufIdx += 1
			copy(buf[bufIdx:], toputils.Uint16ToBytesLittleEndian(uint16(strLen)))
			bufIdx += 2
		} else {
			copy(buf[bufIdx:], VALTYPE_ENC_LARGE_STRING)
			bufIdx += 1
			copy(buf[bufIdx:], toputils.Uint32ToBytesLittleEndian(uint32(strLen)))
			bufIdx += 4
		}
		copy(buf[bufIdx:], strBytes)
		bufIdx += strLen
	}

	return buf, bufIdx
}

// TODO: remove the duplication with WriteToBytesWithType
func (e *CValueEnclosure) WriteBytes(writer io.Writer) (int, error) {
	var typeErr, valErr error
	size := 1 // The Dtype byte

	switch e.Dtype {
	case SS_DT_BOOL:
		size += 1
		_, typeErr = writer.Write(VALTYPE_ENC_BOOL)
		value, ok := e.CVal.(bool)
		if !ok {
			return 0, fmt.Errorf("WriteBytes: error converting value %v to bool", e.CVal)
		}

		if value {
			_, valErr = writer.Write([]byte{1})
		} else {
			_, valErr = writer.Write([]byte{0})
		}
	case SS_DT_UNSIGNED_NUM:
		size += 8
		_, typeErr = writer.Write(VALTYPE_ENC_UINT64)
		if value, ok := e.CVal.(uint64); ok {
			_, valErr = writer.Write(toputils.Uint64ToBytesLittleEndian(value))
		} else {
			return 0, fmt.Errorf("WriteBytes: error converting value %v to uint64", e.CVal)
		}
	case SS_DT_SIGNED_NUM:
		size += 8
		_, typeErr = writer.Write(VALTYPE_ENC_INT64)
		if value, ok := e.CVal.(int64); ok {
			_, valErr = writer.Write(toputils.Int64ToBytesLittleEndian(value))
		} else {
			return 0, fmt.Errorf("WriteBytes: error converting value %v to int64", e.CVal)
		}
	case SS_DT_FLOAT:
		size += 8
		_, typeErr = writer.Write(VALTYPE_ENC_FLOAT64)
		if value, ok := e.CVal.(float64); ok {
			_, valErr = writer.Write(toputils.Float64ToBytesLittleEndian(value))
		} else {
			return 0, fmt.Errorf("WriteBytes: error converting value %v to float64", e.CVal)
		}
	case SS_DT_STRING:
		size += 2
		_, typeErr = writer.Write(VALTYPE_ENC_SMALL_STRING)
		value, ok := e.CVal.(string)
		if !ok {
			return 0, fmt.Errorf("WriteBytes: error converting value %v to string", e.CVal)
		}
		strBytes := []byte(value)
		size += len(strBytes)

		_, err := writer.Write(toputils.Uint16ToBytesLittleEndian(uint16(len(strBytes))))
		if err != nil {
			return 0, fmt.Errorf("WriteBytes: error writing string length: %v", err)
		}

		_, valErr = writer.Write(strBytes)
	case SS_DT_BACKFILL:
		_, typeErr = writer.Write(VALTYPE_ENC_BACKFILL)
	default:
		return 0, fmt.Errorf("WriteBytes: unsupported Dtype: %v", e.Dtype)
	}

	if typeErr != nil {
		return 0, fmt.Errorf("WriteBytes: error writing type: %v", typeErr)
	}
	if valErr != nil {
		return 0, fmt.Errorf("WriteBytes: error writing value: %v", valErr)
	}

	return size, nil
}

// Returns the number of bytes read
func (e *CValueEnclosure) FromBytes(buf []byte) (int, error) {
	if len(buf) == 0 {
		return 0, errors.New("CVal.FromBytes: empty buffer")
	}

	valtype := buf[0]
	idx := 1

	switch valtype {
	case VALTYPE_ENC_BOOL[0]:
		if len(buf) < idx+1 {
			return 0, errors.New("CVal.FromBytes: not enough bytes for bool")
		}
		boolVal := buf[idx]
		idx += 1
		e.CVal = (boolVal == 1)
		e.Dtype = SS_DT_BOOL
	case VALTYPE_ENC_UINT64[0]:
		if len(buf) < idx+8 {
			return 0, errors.New("CVal.FromBytes: not enough bytes for uint64")
		}
		uint64Val := toputils.BytesToUint64LittleEndian(buf[idx : idx+8])
		idx += 8
		e.CVal = uint64Val
		e.Dtype = SS_DT_UNSIGNED_NUM
	case VALTYPE_ENC_INT64[0]:
		if len(buf) < idx+8 {
			return 0, errors.New("CVal.FromBytes: not enough bytes for int64")
		}
		int64Val := toputils.BytesToInt64LittleEndian(buf[idx : idx+8])
		idx += 8
		e.CVal = int64Val
		e.Dtype = SS_DT_SIGNED_NUM
	case VALTYPE_ENC_FLOAT64[0]:
		if len(buf) < idx+8 {
			return 0, errors.New("CVal.FromBytes: not enough bytes for float64")
		}
		float64Val := toputils.BytesToFloat64LittleEndian(buf[idx : idx+8])
		idx += 8
		e.CVal = float64Val
		e.Dtype = SS_DT_FLOAT
	case VALTYPE_ENC_SMALL_STRING[0]:
		if len(buf) < idx+2 {
			return 0, errors.New("CVal.FromBytes: not enough bytes for string length")
		}
		strLen := int(toputils.BytesToUint16LittleEndian(buf[idx : idx+2]))
		idx += 2
		if len(buf) < idx+strLen {
			return 0, errors.New("CVal.FromBytes: not enough bytes for string")
		}
		strVal := string(buf[idx : idx+strLen])
		idx += strLen
		e.CVal = strVal
		e.Dtype = SS_DT_STRING
	case VALTYPE_ENC_BACKFILL[0]:
		e.CVal = nil
		e.Dtype = SS_DT_BACKFILL
	default:
		return 0, fmt.Errorf("CVal.FromBytes: unsupported Dtype: %v", valtype)
	}

	return idx, nil
}

// TODO: remove the duplication with FromBytes
func (e *CValueEnclosure) FromReader(reader io.Reader) (int, error) {
	var encoding byte
	err := binary.Read(reader, binary.LittleEndian, &encoding)
	if err != nil {
		return 0, fmt.Errorf("CVal.FromReader: failed reading DType: %v", err)
	}

	idx := 1
	switch encoding {
	case VALTYPE_ENC_BOOL[0]:
		var boolVal byte
		err = binary.Read(reader, binary.LittleEndian, &boolVal)
		if err != nil {
			return 0, fmt.Errorf("CVal.FromReader: failed reading bool value: %v", err)
		}

		e.CVal = (boolVal == 1)
		e.Dtype = SS_DT_BOOL
		idx += 1
	case VALTYPE_ENC_UINT64[0]:
		var uint64Val uint64
		err = binary.Read(reader, binary.LittleEndian, &uint64Val)
		if err != nil {
			return 0, fmt.Errorf("CVal.FromReader: failed reading uint64 value: %v", err)
		}

		e.CVal = uint64Val
		e.Dtype = SS_DT_UNSIGNED_NUM
		idx += 8
	case VALTYPE_ENC_INT64[0]:
		var int64Val int64
		err = binary.Read(reader, binary.LittleEndian, &int64Val)
		if err != nil {
			return 0, fmt.Errorf("CVal.FromReader: failed reading int64 value: %v", err)
		}

		e.CVal = int64Val
		e.Dtype = SS_DT_SIGNED_NUM
		idx += 8
	case VALTYPE_ENC_FLOAT64[0]:
		var float64Val float64
		err = binary.Read(reader, binary.LittleEndian, &float64Val)
		if err != nil {
			return 0, fmt.Errorf("CVal.FromReader: failed reading float64 value: %v", err)
		}

		e.CVal = float64Val
		e.Dtype = SS_DT_FLOAT
		idx += 8
	case VALTYPE_ENC_SMALL_STRING[0]:
		// Read len of value
		var valueLen uint16
		err = binary.Read(reader, binary.LittleEndian, &valueLen)
		if err != nil {
			return 0, fmt.Errorf("CVal.FromReader: failed reading len of value: %v", err)
		}

		valueBytes := make([]byte, valueLen)
		_, err = reader.Read(valueBytes)
		if err != nil {
			return 0, fmt.Errorf("CVal.FromReader: failed reading value: %v", err)
		}

		e.CVal = string(valueBytes)
		e.Dtype = SS_DT_STRING
		idx += 2 + int(valueLen)
	case VALTYPE_ENC_BACKFILL[0]:
		e.Dtype = SS_DT_BACKFILL
		e.CVal = nil
	default:
		return idx, fmt.Errorf("CVal.FromReader: invalid DType: %v", encoding)
	}

	return idx, nil
}

func (e *CValueEnclosure) IsNull() bool {
	return e.Dtype == SS_DT_BACKFILL || e.Dtype == SS_INVALID || e.CVal == nil
}

func (e *CValueEnclosure) IsString() bool {
	return e.Dtype == SS_DT_STRING
}

func (e *CValueEnclosure) IsBool() bool {
	return e.Dtype == SS_DT_BOOL
}

func (e *CValueEnclosure) IsNumeric() bool {
	return e.Dtype == SS_DT_FLOAT || e.Dtype == SS_DT_SIGNED_NUM || e.Dtype == SS_DT_UNSIGNED_NUM
}

func (e *CValueEnclosure) IsFloat() bool {
	return e.Dtype == SS_DT_FLOAT
}

func (e *CValueEnclosure) IsInt() bool {
	return e.Dtype == SS_DT_SIGNED_NUM || e.Dtype == SS_DT_UNSIGNED_NUM
}

func (e *CValueEnclosure) IsSignedInt() bool {
	return e.Dtype == SS_DT_SIGNED_NUM
}

func (e *CValueEnclosure) IsUnsignedInt() bool {
	return e.Dtype == SS_DT_UNSIGNED_NUM
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
	SegKeyEnc uint32
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
	SIGNAL_METRICS_OTLP       = 6
)

type RR_ENC_TYPE uint8

const (
	RR_ENC_UINT64 = 1
	RR_ENC_BITSET = 2
)

func GobEncodeCValueEnclosureMap(m map[string][]CValueEnclosure) ([]byte, error) {
	gob.Register(map[string][]CValueEnclosure{})
	gob.Register(CValueEnclosure{})

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	err := encoder.Encode(m)
	if err != nil {
		return nil, fmt.Errorf("GobEncodeCValueEnclosureMap: error encoding map: %v", err)
	}

	return buf.Bytes(), nil
}

func GobDecodeCValueEnclosureMap(data []byte, m *map[string][]CValueEnclosure) error {
	if len(data) == 0 {
		return nil
	}

	if m == nil {
		return fmt.Errorf("GobDecodeCValueEnclosureMap: map is nil")
	}

	gob.Register(map[string][]CValueEnclosure{})
	gob.Register(CValueEnclosure{})

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	err := dec.Decode(m)
	if err != nil {
		return fmt.Errorf("GobDecodeCValueEnclosureMap: error decoding map: %v", err)
	}

	return nil
}
