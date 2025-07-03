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

package structs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beevik/etree"
	"github.com/dustin/go-humanize"
	log "github.com/sirupsen/logrus"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
)

// These structs are used to organize boolean, string, and numeric expressions.
// The expressions can contain fields. To evaluate one of these expressions,
// use GetFields() to find all the fields used directly or indirectly by the
// expression, then create a fieldToValue map specifying the value for each of
// these fields, and call Evaluate(fieldToValue).
type BoolExpr struct {
	IsTerminal bool

	// Only used when IsTerminal is true.
	LeftValue  *ValueExpr
	RightValue *ValueExpr
	ValueOp    string       // Only = or != for strings; can also be <, <=, >, >= for numbers.
	ValueList  []*ValueExpr //Use for in(<value>, <list>)

	// Only used when IsTerminal is false. For a unary BoolOp, RightExpr should be nil.
	LeftBool  *BoolExpr
	RightBool *BoolExpr
	BoolOp    BoolOperator
}

type EvalExpr struct {
	ValueExpr *ValueExpr
	FieldName string
}

type RenameExpr struct {
	RenameExprMode  RenameExprMode
	OriginalPattern string
	NewPattern      string
}

type RenameExp struct {
	RenameExprMode RenameExprMode
	RenameColumns  map[string]string
}

type RegexExpr struct {
	Op        string // must be "=" or "!="
	Field     string
	RawRegex  string
	GobRegexp *utils.GobbableRegex
}

type RexExpr struct {
	Pattern     string
	FieldName   string
	RexColNames []string
}

type StatisticExpr struct {
	StatisticFunctionMode StatisticFunctionMode
	Limit                 string
	StatisticOptions      *StatisticOptions
	FieldList             []string //Must have FieldList
	ByClause              []string
	AggregationResult     map[string]*AggregationResult
	ExprSplitDone         bool
}

type StatisticOptions struct {
	CountField   string
	OtherStr     string
	PercentField string
	ShowCount    bool
	ShowPerc     bool
	UseOther     bool
}

type DedupExpr struct {
	Limit              uint64
	FieldList          []string // Must have FieldList
	DedupOptions       *DedupOptions
	DedupSortEles      []*SortElement
	DedupSortAscending []int // Derived from DedupSortEles.SortByAsc values.

	// TODO: remove the below fields after we move to the new query pipeline.

	// DedupCombinations maps combinations to a map mapping the record index
	// (of all included records for this combination) to the sort values for
	// that record. For example, if Limit is 3, each inner map will have at
	// most 3 entries and will store the index and sort values of the top 3
	// records for that combination.
	DedupCombinations map[string]map[int][]SortValue
	PrevCombination   string

	DedupRecords          map[string]map[string]interface{}
	NumProcessedSegments  uint64
	processedSegmentsLock sync.Mutex
}

type DedupOptions struct {
	Consecutive bool
	KeepEmpty   bool
	KeepEvents  bool
}

type SortExpr struct {
	SortEles      []*SortElement
	SortAscending []int
	Limit         uint64

	// The following will be deprecated when we move to the new query pipeline.
	SortRecords           map[string]map[string]interface{}
	NumProcessedSegments  uint64
	processedSegmentsLock sync.Mutex
}

func (e *SortExpr) Equal(other *SortExpr) bool {
	if e == nil || other == nil {
		return e == other
	}

	if len(e.SortEles) != len(other.SortEles) || len(e.SortAscending) != len(other.SortAscending) {
		return false
	}

	for i, sortEle := range e.SortEles {
		if sortEle.Field != other.SortEles[i].Field || sortEle.Op != other.SortEles[i].Op ||
			sortEle.SortByAsc != other.SortEles[i].SortByAsc {
			return false
		}
	}

	for i, asc := range e.SortAscending {
		if asc != other.SortAscending[i] {
			return false
		}
	}

	return e.Limit == other.Limit
}

func (e *SortExpr) ShallowCopy() *SortExpr {
	return &SortExpr{
		SortEles:      e.SortEles,
		SortAscending: e.SortAscending,
		Limit:         e.Limit,

		// These aren't used.
		SortRecords:           nil,
		NumProcessedSegments:  0,
		processedSegmentsLock: sync.Mutex{},
	}
}

type SortElement struct {
	SortByAsc bool
	Op        string
	Field     string
}

type SortValue struct {
	Val         string
	InterpretAs string // Should be "ip", "num", "str", "auto", or ""
}

// See ValueExprMode type definition for which fields are valid for each mode.
type ValueExpr struct {
	ValueExprMode ValueExprMode

	FloatValue     float64
	NumericExpr    *NumericExpr
	StringExpr     *StringExpr
	ConditionExpr  *ConditionExpr
	BooleanExpr    *BoolExpr
	MultiValueExpr *MultiValueExpr
}

type ConcatExpr struct {
	Atoms []*ConcatAtom
}

type ConcatAtom struct {
	IsField  bool
	Value    string
	TextExpr *TextExpr
}

type MultiValueExpr struct {
	MultiValueExprMode   MultiValueExprMode
	Op                   string
	Condition            *BoolExpr // To filter out values that do not meet the criteria within a multivalue field
	NumericExprParams    []*NumericExpr
	StringExprParams     []*StringExpr
	MultiValueExprParams []*MultiValueExpr
	ValueExprParams      []*ValueExpr
	InferTypes           bool // To specify that the mv_to_json_array function should attempt to infer JSON data types when it converts field values into array elements.
	FieldName            string
}

type NumericExpr struct {
	NumericExprMode NumericExprMode

	IsTerminal bool

	// Only used when IsTerminal is true.
	ValueIsField bool
	Value        string

	Fields            []string
	IsFieldsPopulated bool

	// Only used when IsTerminal is false.
	Op           string // Including arithmetic, mathematical and text functions ops
	Left         *NumericExpr
	Right        *NumericExpr
	Val          *StringExpr
	RelativeTime sutils.RelativeTimeExpr
}

type StringExpr struct {
	StringExprMode StringExprMode
	RawString      string      // only used when mode is RawString
	StringList     []string    // only used when mode is RawStringList
	FieldName      string      // only used when mode is Field
	ConcatExpr     *ConcatExpr // only used when mode is Concat
	TextExpr       *TextExpr   // only used when mode is TextExpr
	FieldList      []string    // only used when you want fields in the string from the parser
}

type TextExpr struct {
	IsTerminal     bool
	Op             string //lower, ltrim, rtrim
	Param          *StringExpr
	StrToRemove    string
	Delimiter      *StringExpr
	MultiValueExpr *MultiValueExpr
	ValueList      []*StringExpr
	StartIndex     *NumericExpr
	LengthExpr     *NumericExpr
	Val            *ValueExpr
	Cluster        *Cluster   // generates a cluster label
	SPathExpr      *SPathExpr // To extract information from the structured data formats XML and JSON.
	Regex          *utils.GobbableRegex
}

type ConditionExpr struct {
	Op                  string //if, case, coalesce
	BoolExpr            *BoolExpr
	TrueValue           *ValueExpr //if bool expr is true, take this value
	FalseValue          *ValueExpr
	ConditionValuePairs []*ConditionValuePair
	ValueList           []*ValueExpr
}

type ConditionValuePair struct {
	Condition *BoolExpr
	Value     *ValueExpr
}

type TimechartExpr struct {
	TimeHistogram *TimeBucket
	GroupBy       *GroupByRequest
	TcOptions     *TcOptions
	BinOptions    *BinOptions
	SingleAgg     *SingleAgg
	ByField       string // group by this field inside each time range bucket (timechart)
	LimitExpr     *LimitExpr
}

type LimitExpr struct {
	IsTop          bool // True: keeps the N highest scoring distinct values of the split-by field
	Num            int
	LimitScoreMode LimitScoreMode
}

type SingleAgg struct {
	MeasureOperations []*MeasureAggregator
	//Split By clause
}

type TcOptions struct {
	BinOptions *BinOptions
	UseNull    bool
	UseOther   bool
	NullStr    string
	OtherStr   string
}

type BinCmdOptions struct {
	BinSpanOptions       *BinSpanOptions
	MinSpan              *BinSpanLength
	MaxBins              uint64
	Start                *float64
	End                  *float64
	AlignTime            *uint64
	Field                string
	NewFieldName         utils.Option[string]
	Records              map[string]map[string]interface{}
	RecordIndex          map[int]map[string]int
	NumProcessedSegments uint64
}

type BinSpanLength struct {
	Num       float64
	TimeScale sutils.TimeUnit
}

type BinSpanOptions struct {
	BinSpanLength *BinSpanLength
	LogSpan       *LogSpan
}

type BinOptions struct {
	SpanOptions *SpanOptions
}

type SpanOptions struct {
	DefaultSettings bool
	SpanLength      *SpanLength
}

type LogSpan struct {
	Coefficient float64
	Base        float64
}

type SpanLength struct {
	Num       int
	TimeScalr sutils.TimeUnit
}

type SplitByClause struct {
	Field     string
	TcOptions *TcOptions
	// Where clause: to be finished
}

type Cluster struct {
	Field     string
	Threshold float64
	Match     string // termlist, termset, ngramset
	Delims    string
}

// This structure is used to store values which are not within limit. And These values will be merged into the 'other' category.
type TMLimitResult struct {
	ValIsInLimit     map[string]bool
	GroupValScoreMap map[string]*sutils.CValueEnclosure
	Hll              *utils.GobbableHll
	TDigest          *utils.GobbableTDigest
	StrSet           map[string]struct{}
	OtherCValArr     []*sutils.CValueEnclosure
}

// To extract information from the structured data formats XML and JSON.
type SPathExpr struct {
	InputColName    string // default is set to _raw
	Path            string // the path to the field from which the values need to be extracted.
	IsPathFieldName bool   // If true, the path is the field name and the value is the field value
	OutputColName   string // the name of the column in the output table to which the extracted values will be written. By Default it is set the same as the path.
}

type SigfigInfo struct {
	Value        float64 // result of mathematical operation
	SigFigs      int
	DecimalPlace int
}

type RunningLatestOrEarliestVal struct {
	Value     sutils.CValueEnclosure
	Timestamp uint64
}

type BoolOperator uint8

const (
	BoolOpNot BoolOperator = iota
	BoolOpAnd
	BoolOpOr
)

type StatisticFunctionMode uint8

const (
	SFMTop = iota
	SFMRare
)

type RenameExprMode uint8

const (
	REMPhrase   = iota //Rename with a phrase
	REMRegex           //Rename fields with similar names using a wildcard
	REMOverride        //Rename to a existing field
)

type LimitScoreMode uint8

const (
	LSMBySum  = iota // If only a single aggregation is specified, the score is based on the sum of the values in the aggregation
	LSMByFreq        // Otherwise the score is based on the frequency of the by field's val
)

type ValueExprMode uint8

const (
	VEMNumericExpr    = iota // Only NumricExpr is valid
	VEMStringExpr            // Only StringExpr is valid
	VEMConditionExpr         // Only ConditionExpr is valud
	VEMBooleanExpr           // Only BooleanExpr is valid
	VEMMultiValueExpr        // Only MultiValueExpr is valid
)

type StringExprMode uint8

const (
	SEMRawString     = iota // only used when mode is RawString
	SEMRawStringList        // only used when mode is RawStringList
	SEMField                // only used when mode is Field
	SEMConcatExpr           // only used when mode is Concat
	SEMTextExpr             // only used when mode is TextExpr
	SEMFieldList            // only used when mode is FieldList
)

type NumericExprMode uint8

const (
	NEMNumber      = iota // only used when mode is a Number
	NEMLenString          // only used when mode is a str (used for len())
	NEMNumberField        // only used when mode is Field (Field can be evaluated to a float)
	NEMLenField           // only used when mode is Field (Field can not be evaluated to a float, used for len())
	NEMNumericExpr        // only used when mode is a NumericExpr
)

type MultiValueExprMode uint8

const (
	MVEMMultiValueExpr = iota // only used when mode is a MultiValueExpr
	MVEMField
)

var timeFormatReplacements = []struct {
	key string
	val string
}{
	{"%d", "02"},
	{"%m", "01"},
	{"%Y", "2006"},
	{"%H", "15"},
	{"%I", "03"},
	{"%p", "PM"},
	{"%M", "04"},
	{"%S", "05"},
	{"%b", "Jan"},
	{"%B", "January"},
	{"%y", "06"},
	{"%e", "2"},
	{"%a", "Mon"},
	{"%A", "Monday"},
	{"%w", "Monday"},
	{"%j", "002"},
	{"%U", "00"},
	{"%W", "00"},
	{"%V", "00"},
	{"%z", "-0700"},
	{"%Z", "MST"},
	{"%c", "Mon Jan  2 15:04:05 2006"},
	{"%x", "01/02/06"},
	{"%X", "15:04:05"},
	{"%%", "%"},
}

var ErrFloatMissingField = fmt.Errorf("Missing field")
var ErrFloatFieldNull = fmt.Errorf("field was null")

var ErrWithCodeConversionErr = utils.NewErrorWithCode(utils.CONVERSION_ERR,
	sutils.ErrFloatConversionFailed)

var ErrWithCodeFloatMissingField = utils.NewErrorWithCode(utils.NIL_VALUE_ERR,
	ErrFloatMissingField)

var ErrWithCodeFieldNull = utils.NewErrorWithCode(utils.NIL_VALUE_ERR,
	ErrFloatFieldNull)

func (self *DedupExpr) AcquireProcessedSegmentsLock() {
	self.processedSegmentsLock.Lock()
}

func (self *DedupExpr) ReleaseProcessedSegmentsLock() {
	self.processedSegmentsLock.Unlock()
}

func (self *DedupExpr) GetFields() []string {
	if self == nil {
		return nil
	}
	return append(self.FieldList, GetFieldsFromSortElements(self.DedupSortEles)...)
}

func (self *SortExpr) GetFields() []string {
	if self == nil {
		return nil
	}
	return GetFieldsFromSortElements(self.SortEles)
}

func GetFieldsFromSortElements(sortEles []*SortElement) []string {
	fields := []string{}
	for _, sortEle := range sortEles {
		fields = append(fields, sortEle.Field)
	}
	return fields
}

func (self *SortExpr) AcquireProcessedSegmentsLock() {
	self.processedSegmentsLock.Lock()
}

func (self *SortExpr) ReleaseProcessedSegmentsLock() {
	self.processedSegmentsLock.Unlock()
}

func findNullFields(fields []string, fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	nullFields := []string{}
	for _, field := range fields {
		val, exists := fieldToValue[field]
		if !exists {
			return []string{}, fmt.Errorf("findNullFields: Expression has a field for which value is not present")
		}
		if val.Dtype == sutils.SS_DT_BACKFILL {
			nullFields = append(nullFields, field)
		}
	}

	return nullFields, nil
}

func (self *BoolExpr) GetNullFields(fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func (self *NumericExpr) GetNullFields(fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func (self *StringExpr) GetNullFields(fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func (self *RenameExpr) GetNullFields(fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func (self *ConcatExpr) GetNullFields(fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func (self *TextExpr) GetNullFields(fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func (self *ValueExpr) GetNullFields(fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func (self *ConditionExpr) GetNullFields(fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func (self *RexExpr) GetNullFields(fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func checkStringInFields(searchStr string, fieldToValue map[string]sutils.CValueEnclosure) (bool, error) {
	for _, fieldCValue := range fieldToValue {
		stringValue, err := fieldCValue.GetString()
		if err != nil {
			return false, fmt.Errorf("checkStringInFields: Cannot convert field value: %v to string", fieldCValue)
		}
		match, err := filepath.Match(searchStr, stringValue)
		if err == nil && match {
			return true, nil
		}
	}

	return false, nil
}

func handleSearchMatch(self *BoolExpr, searchStr string, fieldToValue map[string]sutils.CValueEnclosure) (bool, error) {

	kvPairs := strings.Fields(searchStr)
	nullMap := make(map[string]bool)

	nullFields, err := self.GetNullFields(fieldToValue)
	fields := self.GetFields()
	// in case of single search this error is bound to happen because of *
	// so we are ignoring this error in this particular scenario
	if err != nil && !(len(fields) == 1 && fields[0] == "*") {
		return false, fmt.Errorf("handleSearchMatch: Error getting null fields: %v", err)
	}
	for _, nullField := range nullFields {
		nullMap[nullField] = true
	}

	for _, kvPair := range kvPairs {
		parts := strings.Split(kvPair, "=")
		if len(parts) == 1 && len(kvPairs) == 1 {
			// check if string is present any field
			return checkStringInFields(searchStr, fieldToValue)
		}
		if len(parts) != 2 {
			return false, fmt.Errorf("handleSearchMatch: Invalid Syntax")
		}

		// key does not exists
		fieldVal, exist := fieldToValue[parts[0]]
		if !exist {
			return false, nil
		}
		// key has NULl value
		_, exist = nullMap[parts[0]]
		if exist {
			return false, nil
		}

		val, err := fieldVal.GetString()
		if err != nil {
			return false, fmt.Errorf("handleSearchMatch: Cannot convert fieldVal: %v to string", fieldVal)
		}

		match, err := filepath.Match(parts[1], val)
		if err != nil || !match {
			return false, nil
		}
	}

	return true, nil
}

func validateBoolExprError(err error, errInfo string) (*sutils.CValueEnclosure, error) {
	if utils.IsNilValueError(err) {
		return &sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_BACKFILL,
			CVal:  nil,
		}, nil
	} else if utils.IsNonNilValueError(err) {
		return nil, utils.WrapErrorf(err, "%v. Error: %v", errInfo, err)
	}

	return nil, nil
}

func getBoolCValueEnclosure(boolVal bool) *sutils.CValueEnclosure {
	return &sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_BOOL,
		CVal:  boolVal,
	}
}

// Evaluate this BoolExpr to a boolean, replacing each field in the expression
// with the value specified by fieldToValue. if the field is not present in the fieldToValue map
// then NULL is returned.
func (self *BoolExpr) evaluateToCValueEnclosure(fieldToValue map[string]sutils.CValueEnclosure) (*sutils.CValueEnclosure, error) {

	if self.IsTerminal {
		switch self.ValueOp {
		case "in":
			inFlag, err := isInValueList(fieldToValue, self.LeftValue, self.ValueList)
			if err != nil {
				return validateBoolExprError(err, "BoolExpr.Evaluate: can not evaluate Eval In function")
			}
			return getBoolCValueEnclosure(inFlag), nil
		case "isbool":
			val, err := self.LeftValue.EvaluateToString(fieldToValue)
			if err != nil {
				return validateBoolExprError(err, "BoolExpr.Evaluate: 'isbool' can not evaluate to String")
			}
			isBool := strings.ToLower(val) == "true" || strings.ToLower(val) == "false" || val == "0" || val == "1"
			return getBoolCValueEnclosure(isBool), nil

		case "isint":
			val, err := self.LeftValue.EvaluateToString(fieldToValue)
			if err != nil {
				return validateBoolExprError(err, "BoolExpr.Evaluate: 'isint' can not evaluate to String")
			}

			_, parseErr := strconv.Atoi(val)
			return getBoolCValueEnclosure(parseErr == nil), nil
		case "isnum":
			val, err := self.LeftValue.EvaluateToString(fieldToValue)
			if err != nil {
				return validateBoolExprError(err, "BoolExpr.Evaluate: 'isnum' can not evaluate to String")
			}

			_, parseErr := utils.FastParseFloat([]byte(val))
			return getBoolCValueEnclosure(parseErr == nil), nil
		case "isstr":
			_, floatErr := self.LeftValue.EvaluateToFloat(fieldToValue)

			if floatErr == nil {
				return getBoolCValueEnclosure(false), nil
			}

			_, strErr := self.LeftValue.EvaluateToString(fieldToValue)
			if strErr != nil {
				return validateBoolExprError(strErr, "BoolExpr.Evaluate: 'isstr' can not evaluate to String")
			}

			return getBoolCValueEnclosure(strErr == nil), nil
		case "isnull":
			// Get the fields associated with this expression
			fields := self.GetFields()
			if len(fields) == 0 {
				return nil, fmt.Errorf("BoolExpr.Evaluate: No fields found for isnull operation")
			}

			// Check the first field's value in the fieldToValue map
			value, exists := fieldToValue[fields[0]]
			if !exists {
				return getBoolCValueEnclosure(true), nil
			}
			// Check if the value's Dtype is SS_DT_BACKFILL
			if value.Dtype == sutils.SS_DT_BACKFILL {
				return getBoolCValueEnclosure(true), nil
			}
			return getBoolCValueEnclosure(false), nil
		case "like":
			leftStr, errLeftStr := self.LeftValue.EvaluateToString(fieldToValue)
			if errLeftStr != nil {
				return validateBoolExprError(errLeftStr, "BoolExpr.Evaluate: error evaluating left side of LIKE to string")
			}

			rightStr, errRightStr := self.RightValue.EvaluateToString(fieldToValue)
			if errRightStr != nil {
				return validateBoolExprError(errRightStr, "BoolExpr.Evaluate: error evaluating right side of LIKE to string")
			}

			regexPattern := strings.Replace(strings.Replace(regexp.QuoteMeta(rightStr), "%", ".*", -1), "_", ".", -1)
			matched, err := regexp.MatchString("^"+regexPattern+"$", leftStr)
			if err != nil {
				return nil, utils.WrapErrorf(err, "BoolExpr.Evaluate: regex error in LIKE operation pattern: %v, string: %v, err: %v", regexPattern, leftStr, err)
			}
			return getBoolCValueEnclosure(matched), nil
		case "match":
			leftStr, errLeftStr := self.LeftValue.EvaluateToString(fieldToValue)
			if errLeftStr != nil {
				return validateBoolExprError(errLeftStr, "BoolExpr.Evaluate: error evaluating left side of MATCH to string")
			}

			rightStr, errRightStr := self.RightValue.EvaluateToString(fieldToValue)
			if errRightStr != nil {
				return validateBoolExprError(errRightStr, "BoolExpr.Evaluate: error evaluating right side of MATCH to string")
			}

			matched, err := regexp.MatchString(rightStr, leftStr)
			if err != nil {
				return nil, utils.WrapErrorf(err, "BoolExpr.Evaluate: regex error in MATCH operation leftString %v, rightString %v, err: %v", leftStr, rightStr, err)
			}
			return getBoolCValueEnclosure(matched), nil

		case "cidrmatch":
			cidrStr, errCidr := self.LeftValue.EvaluateToString(fieldToValue)
			if errCidr != nil {
				return validateBoolExprError(errCidr, "BoolExpr.Evaluate: error evaluating left side of CIDR to string")
			}

			ipStr, errIp := self.RightValue.EvaluateToString(fieldToValue)
			if errIp != nil {
				return validateBoolExprError(errIp, "BoolExpr.Evaluate: error evaluating right side of CIDR to string")
			}

			match, err := isIPInCIDR(cidrStr, ipStr)
			if err != nil {
				return nil, utils.WrapErrorf(err, "BoolExpr.Evaluate: 'cidrmatch' error in matching is IP in CIDR: cidr: %v, ip: %v, err: %v", cidrStr, ipStr, err)
			}
			return getBoolCValueEnclosure(match), nil
		case "isnotnull":
			fields := self.GetFields()
			if len(fields) == 0 {
				return nil, fmt.Errorf("BoolExpr.Evaluate: No fields found for isnotnull operation")
			}

			value, exists := fieldToValue[fields[0]]
			if !exists {
				return getBoolCValueEnclosure(false), nil
			}
			if !value.IsNull() {
				return getBoolCValueEnclosure(true), nil
			}
			return getBoolCValueEnclosure(false), nil
		case "searchmatch":
			searchStr, err := self.LeftValue.EvaluateToString(fieldToValue)
			if err != nil {
				return validateBoolExprError(err, "BoolExpr.Evaluate: error evaluating searchmatch string to string")
			}
			resultBool, err := handleSearchMatch(self, searchStr, fieldToValue)
			return getBoolCValueEnclosure(resultBool), err
		}

		leftVal, errLeft := self.LeftValue.EvaluateValueExpr(fieldToValue)
		rightVal, errRight := self.RightValue.EvaluateValueExpr(fieldToValue)
		isLeftValNull := utils.IsNilValueError(errLeft) || leftVal == nil
		isRightValNull := utils.IsNilValueError(errRight) || rightVal == nil

		if (errLeft != nil && !isLeftValNull) || (errRight != nil && !isRightValNull) {
			return nil, fmt.Errorf("BoolExpr.Evaluate: error evaluating ValueExprs, errLeft: %v, errRight: %v", errLeft, errRight)
		}

		if isLeftValNull || isRightValNull {
			// If any of the values are NULL, then the result cannot be determined.
			// Return NULL.
			return &sutils.CValueEnclosure{Dtype: sutils.SS_DT_BACKFILL, CVal: nil}, nil
		}

		switch self.ValueOp {
		case "=", "!=":
			convertedLeftVal, convertedRightVal := dtypeutils.ConvertToSameType(leftVal, rightVal)
			if self.ValueOp == "=" {
				return getBoolCValueEnclosure(convertedLeftVal == convertedRightVal), nil
			} else {
				return getBoolCValueEnclosure(convertedLeftVal != convertedRightVal), nil
			}
		case "<", ">", "<=", ">=":
			return getBoolCValueEnclosure(dtypeutils.CompareValues(leftVal, rightVal, self.ValueOp)), nil
		}

		return getBoolCValueEnclosure(false), fmt.Errorf("BoolExpr.Evaluate: invalid ValueOp %v for strings", self.ValueOp)
	} else { // IsTerminal is false
		return nil, fmt.Errorf("BoolExpr.Evaluate: non-terminal BoolExprs are not supported")
	}
}

func GetBoolResult(leftVal bool, rightVal bool, Op BoolOperator) (bool, error) {
	switch Op {
	case BoolOpNot:
		return !leftVal, nil
	case BoolOpAnd:
		return leftVal && rightVal, nil
	case BoolOpOr:
		return leftVal || rightVal, nil
	default:
		return false, fmt.Errorf("GetBoolResult: invalid BoolOp: %v", Op)
	}
}

// Evaluate this BoolExpr to a boolean, replacing each field in the expression
// with the value specified by fieldToValue. If the field is not present in the fieldToValue map
// then false is returned.
func (self *BoolExpr) Evaluate(fieldToValue map[string]sutils.CValueEnclosure) (bool, error) {
	if self.IsTerminal {
		cValueEnclosure, err := self.evaluateToCValueEnclosure(fieldToValue)
		if err != nil {
			return false, err
		}

		if cValueEnclosure.Dtype == sutils.SS_DT_BACKFILL {
			return false, nil
		} else if cValueEnclosure.Dtype != sutils.SS_DT_BOOL {
			return false, fmt.Errorf("BoolExpr.Evaluate: result is not a boolean")
		}

		return cValueEnclosure.CVal.(bool), nil
	} else {
		// Non-terminal BoolExprs
		left, err := self.LeftBool.Evaluate(fieldToValue)
		if err != nil {
			return false, err
		}

		// Short-circuit logic
		if self.BoolOp == BoolOpAnd && !left {
			return false, nil
		}
		if self.BoolOp == BoolOpOr && left {
			return true, nil
		}

		var right bool
		if self.RightBool != nil {
			right, err = self.RightBool.Evaluate(fieldToValue)
			if err != nil {
				return false, err
			}
		}

		boolResult, err := GetBoolResult(left, right, self.BoolOp)
		if err != nil {
			return false, fmt.Errorf("BoolExpr.Evaluate: error evaluating BoolExpr: %v", err)
		}

		return boolResult, nil
	}
}

// Evaluate this BoolExpr to a boolean, replacing each field in the expression
// with the value specified by fieldToValue.
// Will be evaluated to either true, false, or NULL.
func (self *BoolExpr) EvaluateWithNull(fieldToValue map[string]sutils.CValueEnclosure) (sutils.CValueEnclosure, error) {
	if self.IsTerminal {
		cValEnc, err := self.evaluateToCValueEnclosure(fieldToValue)
		if err != nil {
			return sutils.CValueEnclosure{}, fmt.Errorf("BoolExpr.EvaluateWithNull: error evaluating terminal BoolExpr: %v", err)
		}
		return *cValEnc, nil
	} else {
		left, err := self.LeftBool.EvaluateWithNull(fieldToValue)
		if err != nil {
			return sutils.CValueEnclosure{}, fmt.Errorf("BoolExpr.EvaluateWithNull: error evaluating left BoolExpr: %v", err)
		}

		if left.IsNull() {
			return left, nil
		}

		right := sutils.CValueEnclosure{CVal: false}
		if self.RightBool != nil {
			right, err = self.RightBool.EvaluateWithNull(fieldToValue)
			if err != nil {
				return sutils.CValueEnclosure{}, fmt.Errorf("BoolExpr.EvaluateWithNull: error evaluating right BoolExpr: %v", err)
			}

			if right.IsNull() {
				return right, nil
			}
		}

		boolResult, err := GetBoolResult(left.CVal.(bool), right.CVal.(bool), self.BoolOp)
		if err != nil {
			return sutils.CValueEnclosure{}, fmt.Errorf("BoolExpr.EvaluateWithNull: error evaluating BoolExpr: %v", err)
		}

		return sutils.CValueEnclosure{Dtype: sutils.SS_DT_BOOL, CVal: boolResult}, nil
	}
}

// This evaluation is specific for inputlookup command.
// Only =, != , <, >, <=, >= operators are supported for strings and numbers.
// Strings and numbers are compared as strings.
// Strings are compared lexicographically and are case-insensitive.
// Wildcards can be used to match a string with a pattern. Only * is supported.
func (self *BoolExpr) EvaluateForInputLookup(fieldToValue map[string]sutils.CValueEnclosure) (bool, error) {
	if self.IsTerminal {
		leftStr, errLeftStr := self.LeftValue.EvaluateToString(fieldToValue)
		rightStr, errRightStr := self.RightValue.EvaluateToString(fieldToValue)
		leftFloat, errLeftFloat := self.LeftValue.EvaluateToFloat(fieldToValue)
		rightFloat, errRightFloat := self.RightValue.EvaluateToFloat(fieldToValue)

		if errLeftFloat == nil && errRightFloat == nil {
			switch self.ValueOp {
			case "=":
				return leftFloat == rightFloat, nil
			case "!=":
				return leftFloat != rightFloat, nil
			case "<":
				return leftFloat < rightFloat, nil
			case ">":
				return leftFloat > rightFloat, nil
			case "<=":
				return leftFloat <= rightFloat, nil
			case ">=":
				return leftFloat >= rightFloat, nil
			default:
				return false, fmt.Errorf("BoolExpr.EvaluateForInputLookup: invalid ValueOp %v for floats", self.ValueOp)
			}
		} else if errLeftStr == nil && errRightStr == nil {
			leftStr = strings.ToLower(leftStr)
			rightStr = strings.ToLower(rightStr)
			pattern := dtypeutils.ReplaceWildcardStarWithRegex(rightStr)
			compiledRegex, err := regexp.Compile(pattern)
			if err != nil {
				return false, fmt.Errorf("Error compiling regular expression, err:%v", err)
			}
			match := compiledRegex.MatchString(leftStr)

			switch self.ValueOp {
			case "=":
				return match, nil
			case "!=":
				return !match, nil
			case "<":
				return (leftStr < rightStr && !match), nil
			case ">":
				return (leftStr > rightStr && !match), nil
			case "<=":
				return (leftStr <= rightStr || match), nil
			case ">=":
				return (leftStr >= rightStr || match), nil
			default:
				return false, fmt.Errorf("BoolExpr.EvaluateForInputLookup: invalid ValueOp %v for strings", self.ValueOp)
			}
		} else {
			if errLeftStr != nil && errLeftFloat != nil {
				if utils.IsNilValueError(errLeftStr) || utils.IsNilValueError(errLeftFloat) {
					return false, nil
				}
				return false, fmt.Errorf("BoolExpr.EvaluateForInputLookup: left cannot be evaluated to a string or float")
			}
			if errRightStr != nil && errRightFloat != nil {
				if utils.IsNilValueError(errRightStr) || utils.IsNilValueError(errRightFloat) {
					return false, nil
				}
				return false, fmt.Errorf("BoolExpr.EvaluateForInputLookup: right cannot be evaluated to a string or float")
			}
			return false, fmt.Errorf("BoolExpr.EvaluateForInputLookup: left and right ValueExpr have different types")
		}
	} else {
		left, err := self.LeftBool.EvaluateForInputLookup(fieldToValue)
		if err != nil {
			return false, err
		}

		switch self.BoolOp {
		case BoolOpNot:
			return !left, nil

		case BoolOpAnd:
			if !left {
				return false, nil // short-circuit
			}
			if self.RightBool != nil {
				right, err := self.RightBool.EvaluateForInputLookup(fieldToValue)
				if err != nil {
					return false, err
				}
				return left && right, nil
			}
			return false, fmt.Errorf("BoolExpr.EvaluateForInputLookup: missing RightBool for AND")

		case BoolOpOr:
			if left {
				return true, nil // short-circuit
			}
			if self.RightBool != nil {
				right, err := self.RightBool.EvaluateForInputLookup(fieldToValue)
				if err != nil {
					return false, err
				}
				return left || right, nil
			}
			return false, fmt.Errorf("BoolExpr.EvaluateForInputLookup: missing RightBool for OR")

		default:
			return false, fmt.Errorf("BoolExpr.EvaluateForInputLookup: invalid BoolOp: %v", self.BoolOp)
		}
	}
}

func isIPInCIDR(cidrStr, ipStr string) (bool, error) {
	_, cidrNet, err := net.ParseCIDR(cidrStr)
	if err != nil {
		return false, err
	}
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false, fmt.Errorf("isIPInCIDR: invalid IP address: %v", ipStr)
	}

	return cidrNet.Contains(ip), nil
}

func isInValueList(fieldToValue map[string]sutils.CValueEnclosure, value *ValueExpr, valueList []*ValueExpr) (bool, error) {
	valueStr, err := value.EvaluateToString(fieldToValue)
	if utils.IsNonNilValueError(err) {
		return false, utils.WrapErrorf(err, "isInValueList: can not evaluate to String: %v", err)
	}

	for _, atom := range valueList {
		atomStr, err := atom.EvaluateToString(fieldToValue)
		if utils.IsNonNilValueError(err) {
			continue
		}

		if atomStr == valueStr {
			return true, nil
		}
	}

	return false, nil
}

func (self *BoolExpr) GetFields() []string {
	if self == nil {
		return nil
	}
	if self.IsTerminal {
		fields := make([]string, 0)

		if self.RightValue != nil {
			fields = append(fields, self.LeftValue.GetFields()...)
			fields = append(fields, self.RightValue.GetFields()...)
		} else {
			fields = append(fields, self.LeftValue.GetFields()...)
		}

		//Append fields from the InExpr value list
		for _, ValueExpr := range self.ValueList {
			fields = append(fields, ValueExpr.GetFields()...)
		}
		return fields
	} else {
		// When IsTerminal is false, LeftBool must not be nil, but RightBool will be
		// nil if BoolOp is a unary operation.
		if self.RightBool == nil {
			return self.LeftBool.GetFields()
		}

		return append(self.LeftBool.GetFields(), self.RightBool.GetFields()...)
	}
}

func (self *ValueExpr) EvaluateToMultiValue(fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	switch self.ValueExprMode {
	case VEMMultiValueExpr:
		return self.MultiValueExpr.Evaluate(fieldToValue)
	default:
		return []string{}, fmt.Errorf("ValueExpr.EvaluateToMultiValue: cannot evaluate to multivalue")
	}
}

func handleSplit(self *MultiValueExpr, fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	if len(self.StringExprParams) != 2 {
		return []string{}, fmt.Errorf("MultiValueExpr.Evaluate: split requires two arguments")
	}
	cellValueStr, err := self.StringExprParams[0].Evaluate(fieldToValue)
	if utils.IsNilValueError(err) {
		return nil, err
	} else if err != nil {
		return nil, fmt.Errorf("MultiValueExpr.Evaluate: cannot evaluate input value as a string: %v", err)
	}

	delimiterStr, err := self.StringExprParams[1].Evaluate(fieldToValue)
	if err != nil {
		return []string{}, fmt.Errorf("MultiValueExpr.Evaluate: cannot evaluate delimiter as a string: %v", err)
	}
	stringsList := strings.Split(cellValueStr, delimiterStr)

	return stringsList, nil
}

func extractInnerJSONObj(outerJSON, path string) []string {

	var outerObj interface{}

	err := json.Unmarshal([]byte(outerJSON), &outerObj)
	if err != nil {
		return nil
	}

	// This is an invalid path; we check for this here since this would cause issues later on
	if strings.Contains(path, ".{") {
		return nil
	}

	// we convert path into a format that is easier to work with
	// e.g. "field1.field2.array{0}{1}.field3" -> "field1.field2.array.{0}.{1}.field3" -> ["field1", "field2", "array", "{0}", "{1}", "field3"]
	newPath := strings.ReplaceAll(path, "{", ".{")
	if newPath[0] == '.' {
		newPath = newPath[1:]
	}
	parts := strings.Split(newPath, ".")

	return extractInnerJSONObjUsingParts(outerObj, parts)
}

// helper function for extractInnerJSONObj
func extractInnerJSONObjUsingParts(currentObj interface{}, parts []string) []string {

	if len(parts) == 0 {
		// this means that we are at the end of the path, so we return the current object

		// splunk requires that we specify the indices of the array
		// e.g. field1.array1{0}
		// For accessing all records in array1, use field1.array1{} or field1.array1{}{}, etc.
		// Do not use field1.array1
		switch currentObj := currentObj.(type) {
		case []interface{}:
			return nil
		case string:
			return []string{currentObj} // this is handled separately because we don't want extra quotes
		case map[string]interface{}, bool, float64, nil:
			jsonBytes, err := json.Marshal(currentObj)
			if err != nil {
				return nil
			}

			// single-element array containing the only data
			return []string{string(jsonBytes)}
		default:
			// should not happen
			log.Errorf("extractInnerJSONObjUsingParts: unexpected type for array element: %T", currentObj)
			return nil
		}
	}

	firstPart := parts[0]

	if len(firstPart) >= 2 && firstPart[0] == '{' && firstPart[len(firstPart)-1] == '}' {
		// part is an array index, e.g. "{5}", "{}"

		switch currentObj := currentObj.(type) {
		case []interface{}:
			if firstPart == "{}" {
				var result []string
				for _, subObj := range currentObj {
					// query each json object in the array
					matched := extractInnerJSONObjUsingParts(subObj, parts[1:])
					if matched != nil {
						result = append(result, matched...)
					}
				}
				if len(result) == 0 {
					return nil
				}
				return result
			}

			// firstPart = {<index>}
			index, err := strconv.Atoi(firstPart[1 : len(firstPart)-1]) // remove the curly braces and convert to int
			if err != nil {
				return nil
			}

			if index < 0 || index >= len(currentObj) { // index out of bounds
				return nil
			}

			// recursively query inner JSON object
			return extractInnerJSONObjUsingParts(currentObj[index], parts[1:])
		default:
			return nil
		}
	} else {
		// part is a field name
		switch value := currentObj.(type) {
		case map[string]interface{}:
			nestedObject, ok := value[firstPart]

			if !ok { // key doesn't exist
				return nil
			}

			return extractInnerJSONObjUsingParts(nestedObject, parts[1:])
		default:
			return nil
		}
	}
}

func extractInnerXMLObj(inputStr, path string) []string {

	doc := etree.NewDocument()
	err := doc.ReadFromString(inputStr)
	if err != nil {
		return nil
	}

	// This is an invalid path; we check for this here since this would cause issues later on
	if strings.Contains(path, ".{") {
		return nil
	}

	// we convert path into a format that is easier to work with
	// e.g. "field1.field2.array{0}.field3" -> "field1.field2.array.{0}.field3" -> ["field1", "field2", "array", "{0}", "field3"]
	newPath := strings.ReplaceAll(path, "{", ".{")
	if newPath[0] == '.' {
		newPath = newPath[1:]
	}
	parts := strings.Split(newPath, ".")

	return extractInnerXMLObjUsingParts(&doc.Element, parts)
}

// helper function for extractValueFromJSON
func extractInnerXMLObjUsingParts(value *etree.Element, parts []string) []string {

	if len(parts) == 0 {
		var sb strings.Builder
		children := value.ChildElements()
		numChildren := len(children)
		if numChildren == 0 {
			// <name>Alice</name> -> Alice
			text := value.Text()
			if text == "" {
				return nil
			}
			sb.WriteString(text)
			return []string{sb.String()}
		} else {
			// <root>a<tag/>b<tag2/>c<tag3/>d</root> -> ["a", "b", "c", "d", "<tag/>b<tag2/>c<tag3/>"]
			// see evaluationstructs_test.go: Test_Spath_XML_Unclear for more examples
			var result []string

			text := value.Text()
			if text != "" {
				result = append(result, text)
			}

			for i, child := range children {
				// add any textual data to the result array
				text := child.Tail()
				if text != "" {
					result = append(result, text)
				}

				// add inner tags to the last element of the result array
				child.WriteTo(&sb, &etree.WriteSettings{})
				if i != numChildren-1 {
					// for every inner tag except the last one, add the text immediately after it
					// to the last element of the result array
					sb.WriteString(child.Tail())
				}
			}

			// construct the last element (which consists of all inner tags and any text data between them)
			// and append it to the result array
			// sb.String() is not empty since numChildren > 0
			result = append(result, sb.String())
			return result
		}
	}

	firstPart := parts[0]

	if len(firstPart) >= 3 && firstPart[0] == '{' && firstPart[1] == '@' && firstPart[len(firstPart)-1] == '}' {
		// firstPart is an attribute name, e.g. "{@attr1}"

		attr := value.SelectAttr(firstPart[2 : len(firstPart)-1])
		if attr == nil {
			return nil
		}
		return []string{attr.Value}
	}

	// zero, one or more of the sub-objects with the tag stored in firstPart
	subObjArr := value.SelectElements(firstPart)

	if len(parts) >= 2 && len(parts[1]) > 2 && parts[1][0] == '{' && parts[1][1] != '@' && parts[1][len(parts[1])-1] == '}' {
		// parts[0] = "array_name"
		// parts[1] = "{index}"

		index, err := strconv.Atoi(parts[1][1 : len(parts[1])-1]) // remove the curly braces and convert to int
		if err != nil {
			return nil
		}

		index-- // converting from 1-indexing (XML) to 0-indexing (Go)
		if index < 0 || index >= len(subObjArr) {
			return nil
		}
		return extractInnerXMLObjUsingParts(subObjArr[index], parts[2:])
	} else if len(subObjArr) == 0 {
		return nil
	} else if len(subObjArr) == 1 {
		return extractInnerXMLObjUsingParts(subObjArr[0], parts[1:])
	} else {
		var result []string
		for _, subObj := range subObjArr {
			matched := extractInnerXMLObjUsingParts(subObj, parts[1:])
			if matched != nil {
				result = append(result, matched...)
			}
		}
		if len(result) == 0 {
			return nil
		}
		return result
	}
}

func handleSpath(self *MultiValueExpr, fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	// eval spath(value, path)
	if len(self.StringExprParams) != 2 {
		return nil, fmt.Errorf("MultiValueExpr.Evaluate: spath requires two arguments")
	}
	valueStr, err := self.StringExprParams[0].Evaluate(fieldToValue)
	if utils.IsNilValueError(err) {
		return nil, err
	} else if err != nil {
		return nil, fmt.Errorf("MultiValueExpr.Evaluate: cannot evaluate input value as a string: %v", err)
	}

	// Spath only considers the first 5000 characters in the value and attempts to extract the value
	// even though this may result in an invalid JSON / XML, in which case it fails silently
	// https://docs.splunk.com/Documentation/SplunkCloud/latest/SearchReference/Spath#Overriding_the_spath_extraction_character_limit
	if len(valueStr) > 5000 {
		valueStr = valueStr[:5000]
	}

	pathStr, err := self.StringExprParams[1].Evaluate(fieldToValue)
	if err != nil {
		return nil, fmt.Errorf("MultiValueExpr.Evaluate: cannot evaluate path as a string: %v", err)
	}

	extractedValue := extractInnerJSONObj(valueStr, pathStr)
	if extractedValue != nil {
		return extractedValue, nil
	}

	extractedValue = extractInnerXMLObj(valueStr, pathStr)
	if extractedValue != nil {
		return extractedValue, nil
	}

	// If both JSON and XML value extraction fail, spath returns nil
	return nil, nil
}

func handleMVSort(self *MultiValueExpr, fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	if self.MultiValueExprParams == nil || len(self.MultiValueExprParams) != 1 || self.MultiValueExprParams[0] == nil {
		return []string{}, fmt.Errorf("handleMVSort: mvsort requires one multiValueExpr argument")
	}
	mvSlice, err := self.MultiValueExprParams[0].Evaluate(fieldToValue)
	if utils.IsNilValueError(err) {
		return nil, err
	} else if err != nil {
		return []string{}, fmt.Errorf("handleMVSort: %v", err)
	}
	// does lexicograrphical sorting => for numbers checks the first digit
	// => 123456 < 2
	sort.Strings(mvSlice)
	return mvSlice, nil
}

func handleMVZip(self *MultiValueExpr, fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	if self.MultiValueExprParams == nil || len(self.MultiValueExprParams) != 2 {
		return []string{}, fmt.Errorf("handleMVZip: mvzip requires two multiValueExpr argument")
	}

	var delimiter string
	if self.StringExprParams == nil || len(self.StringExprParams) != 1 {
		delimiter = ","
	} else {
		// delimiter must be enclosed in quotation marks
		if len(self.StringExprParams[0].RawString) != 0 {
			delimiter = self.StringExprParams[0].RawString
		} else {
			delimiter = ","
		}
	}

	mvLeft, err := self.MultiValueExprParams[0].Evaluate(fieldToValue)
	if utils.IsNilValueError(err) {
		mvLeft = append(mvLeft, "")
	} else if err != nil {
		return []string{}, fmt.Errorf("handleMVZip: %v", err)
	}
	mvRight, err := self.MultiValueExprParams[1].Evaluate(fieldToValue)
	if utils.IsNilValueError(err) {
		mvRight = append(mvRight, "")
	} else if err != nil {
		return []string{}, fmt.Errorf("handleMVZip: %v", err)
	}

	// mvzip is simmilar to python zip => the resulting array size will be = to the shortest array
	minLen := min(len(mvLeft), len(mvRight))
	resultSlice := make([]string, minLen)
	for i := 0; i < minLen; i++ {
		resultSlice[i] = mvLeft[i] + delimiter + mvRight[i]
	}

	return resultSlice, nil
}

func handleMVToJsonArray(self *MultiValueExpr, fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	if self.MultiValueExprParams == nil || len(self.MultiValueExprParams) != 1 || self.MultiValueExprParams[0] == nil {
		return []string{}, fmt.Errorf("handleMVToJsonArray: mv_to_json_array requires one multiValueExpr argument")
	}
	mvSlice, err := self.MultiValueExprParams[0].Evaluate(fieldToValue)
	if utils.IsNilValueError(err) {
		return nil, err
	} else if err != nil {
		return []string{}, fmt.Errorf("handleMVToJsonArray: %v", err)
	}

	resultArr := make([]any, len(mvSlice))
	if self.InferTypes {
		for idx, val := range mvSlice {
			switch val {
			case "true":
				resultArr[idx] = true
				continue
			case "false":
				resultArr[idx] = false
				continue
			case "null":
				resultArr[idx] = nil
				continue
			default:
				// Do Nothing. Handled below
			}

			if num, err := utils.FastParseFloat([]byte(mvSlice[idx])); err == nil {
				resultArr[idx] = num
				continue
			}

			// parser automatically removes extra quotes
			if len(mvSlice[idx]) != 0 {
				resultArr[idx] = mvSlice[idx]
			} else {
				resultArr[idx] = nil
			}
		}
	}

	var jsonBytes []byte
	if resultArr[0] != nil {
		jsonBytes, err = json.Marshal(resultArr)
	} else {
		jsonBytes, err = json.Marshal(mvSlice)
	}
	if err != nil {
		return []string{}, fmt.Errorf("handleMVToJsonArray: error marshaling multivalue field %v; err: %v", mvSlice, err)
	}
	return []string{string(jsonBytes)}, nil

}

func handleMVDedup(self *MultiValueExpr, fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	if self.MultiValueExprParams == nil || len(self.MultiValueExprParams) != 1 || self.MultiValueExprParams[0] == nil {
		return []string{}, fmt.Errorf("handleMVDedup: mvdedup requires one multiValueExpr argument")
	}
	mvSlice, err := self.MultiValueExprParams[0].Evaluate(fieldToValue)
	if utils.IsNilValueError(err) {
		return nil, err
	} else if err != nil {
		return []string{}, fmt.Errorf("handleMVDedup: %v", err)
	}

	seen := make(map[string]string, len(mvSlice))
	dedupedSlice := []string{}
	for _, val := range mvSlice {
		if _, ok := seen[val]; ok {
			continue
		}
		seen[val] = ""
		dedupedSlice = append(dedupedSlice, val)
	}

	return dedupedSlice, nil
}

func handleMVAppend(self *MultiValueExpr, fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	if self.ValueExprParams == nil || len(self.ValueExprParams) < 1 || self.ValueExprParams[0] == nil {
		return []string{}, fmt.Errorf("handleMVAppend: mvappend requires atleast one argument")
	}

	finalMVSlice := []string{}
	for _, param := range self.ValueExprParams {
		switch param.ValueExprMode {
		case VEMMultiValueExpr:
			mvSlice, err := param.MultiValueExpr.Evaluate(fieldToValue)
			if utils.IsNilValueError(err) {
				return nil, err
			} else if err != nil {
				return []string{}, fmt.Errorf("handleMVAppend: %v", err)
			}
			finalMVSlice = append(finalMVSlice, mvSlice...)
		case VEMStringExpr:
			if len(param.StringExpr.RawString) != 0 {
				finalMVSlice = append(finalMVSlice, param.StringExpr.RawString)
			} else {
				result, err := param.StringExpr.ConcatExpr.Evaluate(fieldToValue)
				if utils.IsNilValueError(err) {
					return nil, err
				} else if err != nil {
					return []string{}, fmt.Errorf("handleMVAppend: %v", err)
				}
				finalMVSlice = append(finalMVSlice, result)
			}
		}
	}
	return finalMVSlice, nil
}

func (self *BoolExpr) ExtractSingleField() string {

	fields := self.GetFields()
	if len(fields) == 0 {
		return ""
	}

	uniqueFields := make(map[string]struct{})
	for _, field := range fields {
		uniqueFields[field] = struct{}{}
	}

	if len(uniqueFields) == 1 {
		for field := range uniqueFields {
			return field
		}
	}

	return ""
}

func handleMVFilter(self *MultiValueExpr, fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	if self.Condition == nil {
		return []string{}, fmt.Errorf("handleMVFilter: missing predicate condition")
	}

	targetField := self.Condition.ExtractSingleField()
	if targetField == "" {
		return []string{}, fmt.Errorf("handleMVFilter: unable to extract field from condition")
	}

	mvRaw, ok := fieldToValue[targetField]
	if !ok || mvRaw.Dtype != sutils.SS_DT_STRING_SLICE {
		return []string{}, fmt.Errorf("handleMVFilter: field %s is not a multivalue field", targetField)
	}

	var mvSlice []string
	if mvRaw.CVal != nil {
		mvSlice = mvRaw.CVal.([]string)
	} else {
		return []string{}, nil
	}

	result := []string{}

	for _, val := range mvSlice {
		tempFieldToValue := map[string]sutils.CValueEnclosure{
			targetField: {
				Dtype: sutils.SS_DT_STRING,
				CVal:  val,
			},
		}

		ok, err := self.Condition.Evaluate(tempFieldToValue)
		if err != nil {
			return []string{}, fmt.Errorf("handleMVFilter: condition evaluation failed: %v", err)
		}

		if ok {
			result = append(result, val)
		}
	}

	return result, nil
}

func handleMVIndex(self *MultiValueExpr, fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	if self.MultiValueExprParams == nil || len(self.MultiValueExprParams) != 1 || self.MultiValueExprParams[0] == nil {
		return []string{}, fmt.Errorf("MultiValueExpr.Evaluate: mvindex requires one multiValueExpr argument")
	}
	mvSlice, err := self.MultiValueExprParams[0].Evaluate(fieldToValue)
	if utils.IsNilValueError(err) {
		return nil, err
	} else if err != nil {
		return []string{}, fmt.Errorf("TextExpr.EvaluateText: %v", err)
	}

	if self.NumericExprParams == nil || len(self.NumericExprParams) == 0 || self.NumericExprParams[0] == nil {
		return []string{}, fmt.Errorf("TextExpr.EvaluateText: self.NumericExprParams is required but is nil or empty")
	}

	startIndex, err := strconv.Atoi(self.NumericExprParams[0].Value)
	if err != nil {
		return []string{}, fmt.Errorf("TextExpr.EvaluateText: failed to parse startIndex: %v", err)
	}

	if startIndex < 0 {
		startIndex += len(mvSlice)
	}
	// If endIndex is not provided, use startIndex as endIndex to fetch single value
	endIndex := startIndex
	if len(self.NumericExprParams) == 2 && self.NumericExprParams[1] != nil {
		endIndex, err = strconv.Atoi(self.NumericExprParams[1].Value)
		if err != nil {
			return []string{}, fmt.Errorf("TextExpr.EvaluateText: failed to parse endIndex: %v", err)
		}
		if endIndex < 0 {
			endIndex += len(mvSlice)
		}
	}

	// Check for index out of bounds
	if startIndex > endIndex || startIndex < 0 || endIndex < 0 || endIndex >= len(mvSlice) || startIndex >= len(mvSlice) {
		return []string{}, nil
	}

	return mvSlice[startIndex : endIndex+1], nil
}

func (self *MultiValueExpr) Evaluate(fieldToValue map[string]sutils.CValueEnclosure) ([]string, error) {
	if self == nil {
		return []string{}, fmt.Errorf("MultiValueExpr.Evaluate: self is nil")
	}
	if self.MultiValueExprMode == MVEMField {
		fieldValue, exists := fieldToValue[self.FieldName]
		if !exists {
			return nil, utils.NewErrorWithCode(utils.NIL_VALUE_ERR, fmt.Errorf("MultiValueExpr.Evaluate: field %v not found", self.FieldName))
		}
		if fieldValue.CVal == nil {
			return nil, utils.NewErrorWithCode(utils.NIL_VALUE_ERR, fmt.Errorf("MultiValueExpr.Evaluate: field %v is nil", self.FieldName))
		}
		if fieldValue.Dtype != sutils.SS_DT_STRING_SLICE {
			value := fmt.Sprintf("%v", fieldValue.CVal)
			return []string{value}, nil
		}

		return fieldValue.CVal.([]string), nil
	}

	switch self.Op {
	case "split":
		return handleSplit(self, fieldToValue)
	case "spath":
		return handleSpath(self, fieldToValue)
	case "mvindex":
		return handleMVIndex(self, fieldToValue)
	case "mvsort":
		return handleMVSort(self, fieldToValue)
	case "mvzip":
		return handleMVZip(self, fieldToValue)
	case "mv_to_json_array":
		return handleMVToJsonArray(self, fieldToValue)
	case "mvdedup":
		return handleMVDedup(self, fieldToValue)
	case "mvappend":
		return handleMVAppend(self, fieldToValue)
	case "mvfilter":
		return handleMVFilter(self, fieldToValue)
	default:
		return []string{}, fmt.Errorf("MultiValueExpr.Evaluate: invalid Op %v", self.Op)
	}
}

// Try evaluating this ValueExpr to a string value, replacing each field in the
// expression with the value specified by fieldToValue. Each field listed by
// GetFields() must be in fieldToValue.
//
// A ValueExpr can be evaluated to a string or float, so if this fails you may
// want to call ValueExpr.EvaluateToFloat().
func (self *ValueExpr) EvaluateToString(fieldToValue map[string]sutils.CValueEnclosure) (string, error) {
	switch self.ValueExprMode {
	case VEMStringExpr:
		str, err := self.StringExpr.Evaluate(fieldToValue)
		if err != nil {
			return "", utils.WrapErrorf(err, "ValueExpr.EvaluateToString: cannot evaluate to string %v", err)
		}
		return str, nil
	//In this case, field name will be recognized as part of NumericExpr at first. It it can not be converted to float64, it should be evaluated as a str
	case VEMNumericExpr:
		floatValue, err := self.NumericExpr.Evaluate(fieldToValue)
		if err != nil {
			//Because parsing is successful and it can not evaluate as a float in here,
			//There is one possibility: field name may not be float
			if !self.NumericExpr.IsTerminal {
				// But this is not a terminal case, so we just return 0
				return "0", err
			}

			str, err := getValueAsString(fieldToValue, self.NumericExpr.Value)

			if err == nil {
				return str, nil
			} else if utils.IsNilValueError(err) {
				return "", nil
			}

			return "0", fmt.Errorf("ValueExpr.EvaluateToString: cannot evaluate to float64 or string: %v", err)
		}
		return strconv.FormatFloat(floatValue, 'f', -1, 64), nil
	case VEMConditionExpr:
		val, err := self.ConditionExpr.EvaluateCondition(fieldToValue)
		if err != nil {
			return "", utils.WrapErrorf(err, "ValueExpr.EvaluateToString: cannot evaluate Condition Expr to string %v", err)
		}
		str := fmt.Sprintf("%v", val)
		return str, nil
	case VEMBooleanExpr:
		boolResult, err := self.BooleanExpr.Evaluate(fieldToValue)
		if err != nil {
			return "", utils.WrapErrorf(err, "ValueExpr.EvaluateToString: cannot evaluate Boolean Expr to string %v", err)
		}
		return strconv.FormatBool(boolResult), nil
	case VEMMultiValueExpr:
		mvSlice, err := self.MultiValueExpr.Evaluate(fieldToValue)
		if err != nil {
			return "", utils.WrapErrorf(err, "ValueExpr.EvaluateToString: cannot evaluate to string %v", err)
		}
		if len(mvSlice) == 0 {
			return "", nil
		} else if len(mvSlice) == 1 {
			return mvSlice[0], nil
		}

		CVal := sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING_SLICE, CVal: mvSlice}
		return CVal.GetString()
	default:
		return "", fmt.Errorf("ValueExpr.EvaluateToString: cannot evaluate to string, not a valid ValueExprMode")
	}
}

func (self *StringExpr) Evaluate(fieldToValue map[string]sutils.CValueEnclosure) (string, error) {
	switch self.StringExprMode {
	case SEMRawString:
		return self.RawString, nil
	case SEMField:
		floatValue, err := getValueAsFloat(fieldToValue, self.FieldName)
		if err == nil {
			return strconv.FormatFloat(floatValue, 'f', -1, 64), nil
		}

		str, err := getValueAsString(fieldToValue, self.FieldName)
		if err == nil {
			return str, nil
		}

		if utils.IsNilValueError(err) {
			return "", err
		}
		return "", fmt.Errorf("StringExpr.Evaluate: cannot evaluate to field")
	case SEMConcatExpr:
		return self.ConcatExpr.Evaluate(fieldToValue)
	case SEMTextExpr:
		return self.TextExpr.EvaluateText(fieldToValue)
	case SEMFieldList:
		return self.RawString, nil
	default:
		return "", fmt.Errorf("StringExpr.Evaluate: cannot evaluate to string")
	}
}

// Try evaluating this ValueExpr to a float value, replacing each field in the
// expression with the value specified by fieldToValue. Each field listed by
// GetFields() must be in fieldToValue.
//
// A ValueExpr can be evaluated to a string or float, so if this fails you may
// want to call ValueExpr.EvaluateToString().
func (self *ValueExpr) EvaluateToFloat(fieldToValue map[string]sutils.CValueEnclosure) (float64, error) {
	switch self.ValueExprMode {
	case VEMNumericExpr:
		return self.NumericExpr.Evaluate(fieldToValue)
	default:
		return 0, fmt.Errorf("ValueExpr.EvaluateToFloat: cannot evaluate to float")
	}
}

func (valueExpr *ValueExpr) EvaluateToNumber(fieldToValue map[string]sutils.CValueEnclosure) (interface{}, error) {
	floatValue, err := valueExpr.EvaluateToFloat(fieldToValue)
	if err != nil {
		return 0, err
	}

	// Check if the float value can be represented as an integer
	int64Value := int64(floatValue)
	if floatValue == float64(int64Value) {
		return int64Value, nil
	}
	return floatValue, nil
}

// TODO: Migrate so that every Evaluation Expression returns an utils.CValueEnclosure
func (valueExpr *ValueExpr) EvaluateValueExpr(fieldToValue map[string]sutils.CValueEnclosure) (interface{}, error) {
	var value interface{}
	var err error
	switch valueExpr.ValueExprMode {
	case VEMConditionExpr:
		value, err = valueExpr.ConditionExpr.EvaluateCondition(fieldToValue)
	case VEMStringExpr:
		value, err = valueExpr.EvaluateToString(fieldToValue)
	case VEMNumericExpr:
		value, err = valueExpr.EvaluateValueExprToNumberOrString(fieldToValue)
	case VEMBooleanExpr:
		value, err = valueExpr.BooleanExpr.Evaluate(fieldToValue)
	case VEMMultiValueExpr:
		value, err = valueExpr.EvaluateToMultiValue(fieldToValue)
	default:
		return nil, fmt.Errorf("EvaluateValueExpr: unknown value expr mode %v", valueExpr.ValueExprMode)
	}

	if utils.IsNilValueError(err) {
		return nil, nil
	} else if err != nil {
		return nil, utils.WrapErrorf(err, "EvaluateValueExpr: failed to evaluate value expr, err: %v", err)
	}

	return value, nil
}

// This function will first try to evaluate the ValueExpr to a Number (float64 or int64). If that
// fails, it will try to evaluate it to a string. If that fails, it will return
// an error.
func (expr *ValueExpr) EvaluateValueExprToNumberOrString(fieldToValue map[string]sutils.CValueEnclosure) (interface{}, error) {

	switch expr.ValueExprMode {
	case VEMNumericExpr, VEMStringExpr:
		// Nothing to do
	default:
		return nil, fmt.Errorf("ValueExpr.EvaluateValueExprToNumberOrString: invalid ValueExprMode %v", expr.ValueExprMode)
	}

	value, numErr := expr.EvaluateToNumber(fieldToValue)
	if numErr == nil {
		return value, nil
	} else if utils.IsNilValueError(numErr) {
		return nil, numErr
	}

	valueStr, strErr := expr.EvaluateToString(fieldToValue)
	if strErr == nil {
		return valueStr, nil
	}

	if utils.IsConversionError(numErr) && expr.ValueExprMode == VEMNumericExpr {
		return int64(0), nil
	}

	return nil, utils.WrapErrorf(strErr, "ValueExpr.EvaluateValueExprToNumberOrString: failed to evaluate to number or string. numErr: %v, strErr: %v", numErr, strErr)
}

func (self *ValueExpr) GetFields() []string {
	if self == nil {
		return nil
	}
	switch self.ValueExprMode {
	case VEMNumericExpr:
		return self.NumericExpr.GetFields()
	case VEMStringExpr:
		return self.StringExpr.GetFields()
	case VEMConditionExpr:
		return self.ConditionExpr.GetFields()
	case VEMBooleanExpr:
		return self.BooleanExpr.GetFields()
	case VEMMultiValueExpr:
		return self.MultiValueExpr.GetFields()
	default:
		return []string{}
	}
}

func (self *MultiValueExpr) GetFields() []string {
	if self == nil {
		return nil
	}
	fields := make([]string, 0)
	for _, stringExpr := range self.StringExprParams {
		fields = append(fields, stringExpr.GetFields()...)
	}
	for _, numericExpr := range self.NumericExprParams {
		fields = append(fields, numericExpr.GetFields()...)
	}
	for _, multiValueExpr := range self.MultiValueExprParams {
		fields = append(fields, multiValueExpr.GetFields()...)
	}
	for _, valueExpr := range self.ValueExprParams {
		fields = append(fields, valueExpr.GetFields()...)
	}
	if self.Condition != nil {
		fields = append(fields, self.Condition.GetFields()...)
	}
	if self.MultiValueExprMode == MVEMField {
		fields = append(fields, self.FieldName)
	}

	return fields
}

func (self *RexExpr) GetFields() []string {
	if self == nil {
		return nil
	}
	var fields []string
	fields = append(fields, self.FieldName)
	return fields
}

func (self *RenameExpr) GetFields() []string {
	if self == nil {
		return nil
	}
	fields := make([]string, 0)

	switch self.RenameExprMode {
	case REMPhrase:
		fallthrough
	case REMOverride:
		fields = append(fields, self.OriginalPattern)
		return fields
	default:
		return []string{}
	}
}

func (self *StringExpr) GetFields() []string {
	if self == nil {
		return nil
	}
	switch self.StringExprMode {
	case SEMConcatExpr:
		return self.ConcatExpr.GetFields()
	case SEMTextExpr:
		return self.TextExpr.GetFields()
	case SEMField:
		return []string{self.FieldName}
	case SEMFieldList:
		return self.FieldList
	default:
		return []string{}
	}
}

// Concatenate all the atoms in this ConcatExpr, replacing all fields with the
// values specified by fieldToValue. Each field listed by GetFields() must be in
// fieldToValue.
func (self *ConcatExpr) Evaluate(fieldToValue map[string]sutils.CValueEnclosure) (string, error) {
	result := ""
	for _, atom := range self.Atoms {
		if atom.IsField {
			value, err := getValueAsString(fieldToValue, atom.Value)
			if err != nil {
				return "", err
			} else if utils.IsNilValueError(err) {
				return "", err
			}

			result += value
		} else if atom.TextExpr != nil {
			value, err := atom.TextExpr.EvaluateText(fieldToValue)
			if err != nil {
				return "", err
			}
			result += value
		} else {
			result += atom.Value
		}
	}

	return result, nil
}

func (self *ConcatExpr) GetFields() []string {
	if self == nil {
		return nil
	}
	fields := make([]string, 0)
	for _, atom := range self.Atoms {
		if atom.IsField {
			fields = append(fields, atom.Value)
		} else if atom.TextExpr != nil {
			textFields := atom.TextExpr.GetFields()
			if len(textFields) > 0 {
				fields = append(fields, textFields...)
			}
		}
	}

	return fields
}

func GetBucketKey(BucketKey interface{}, keyIndex int) string {
	switch bucketKey := BucketKey.(type) {
	case []interface{}:
		return fmt.Sprintf("%v", bucketKey[keyIndex])
	case []string:
		return bucketKey[keyIndex]
	case string:
		return bucketKey
	default:
		return ""
	}
}

func (self *StatisticExpr) OverrideGroupByCol(bucketResult *BucketResult, resTotal uint64) error {

	var cellValue interface{}
	cellValueStr := ""
	for keyIndex, groupByCol := range bucketResult.GroupByKeys {
		if !self.StatisticOptions.ShowCount || !self.StatisticOptions.ShowPerc || (self.StatisticOptions.CountField != groupByCol && self.StatisticOptions.PercentField != groupByCol) {
			continue
		}

		if self.StatisticOptions.ShowCount && self.StatisticOptions.CountField == groupByCol {
			cellValue = bucketResult.ElemCount
			cellValueStr = strconv.FormatUint(bucketResult.ElemCount, 10)
		}

		if self.StatisticOptions.ShowPerc && self.StatisticOptions.PercentField == groupByCol {
			percent := float64(bucketResult.ElemCount) / float64(resTotal) * 100
			cellValue = float64(math.Round(percent*1e6) / 1e6)
			cellValueStr = fmt.Sprintf("%.6f", percent)
		}

		// Set the appropriate element of BucketKey to cellValueStr.
		switch bucketKey := bucketResult.BucketKey.(type) {
		case []interface{}:
			bucketKey[keyIndex] = cellValue
			bucketResult.BucketKey = bucketKey
		case []string:
			bucketKey[keyIndex] = cellValueStr
			bucketResult.BucketKey = bucketKey
		case string:
			if keyIndex != 0 {
				return fmt.Errorf("StatisticExpr.OverrideGroupByCol: expected keyIndex to be 0, not %v", keyIndex)
			}
			bucketResult.BucketKey = cellValueStr
		default:
			return fmt.Errorf("StatisticExpr.OverrideGroupByCol: bucket key has unexpected type: %T", bucketKey)
		}
	}
	return nil
}

func (self *StatisticExpr) SetCountToStatRes(statRes map[string]sutils.CValueEnclosure, elemCount uint64) {
	statRes[self.StatisticOptions.CountField] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_UNSIGNED_NUM,
		CVal:  elemCount,
	}
}

func (self *StatisticExpr) SetPercToStatRes(statRes map[string]sutils.CValueEnclosure, elemCount uint64, resTotal uint64) {
	percent := float64(elemCount) / float64(resTotal) * 100
	statRes[self.StatisticOptions.PercentField] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_FLOAT,
		CVal:  float64(math.Round(percent*1e6) / 1e6),
	}
}

func (self *StatisticExpr) sortByBucketKey(a, b *BucketResult, fieldList []string, fieldToGroupByKeyIndex map[string]int) bool {

	for _, field := range fieldList {
		keyIndex := fieldToGroupByKeyIndex[field]
		if GetBucketKey(a.BucketKey, keyIndex) < GetBucketKey(b.BucketKey, keyIndex) {
			return false
		} else if GetBucketKey(a.BucketKey, keyIndex) > GetBucketKey(b.BucketKey, keyIndex) {
			return true
		}
	}
	return true
}

func (self *StatisticExpr) SortBucketResult(results *[]*BucketResult) error {

	//GroupByKeys -> BucketKey
	fieldToGroupByKeyIndex := make(map[string]int, len(self.FieldList))

	//If use the limit option, only the last limit lexicographical of the <field-list> is returned in the search results
	//Therefore, we should sort the bucket by lexicographical value and retain a limited number of values
	if len(self.Limit) > 0 {

		for index, groupByKey := range (*results)[0].GroupByKeys {
			for _, field := range self.FieldList {
				if field == groupByKey {
					fieldToGroupByKeyIndex[field] = index
				}
			}
		}

		//Moving the if statement outside of the sorting process can reduce the number of conditional checks
		//Sort results based on the lexicographical value of their field list
		switch self.StatisticFunctionMode {
		case SFMTop:
			sort.Slice(*results, func(index1, index2 int) bool {
				return self.sortByBucketKey((*results)[index1], (*results)[index2], self.FieldList, fieldToGroupByKeyIndex)
			})
		case SFMRare:
			sort.Slice(*results, func(index1, index2 int) bool {
				return !self.sortByBucketKey((*results)[index1], (*results)[index2], self.FieldList, fieldToGroupByKeyIndex)
			})
		}

		limit, err := strconv.Atoi(self.Limit)
		if err != nil {
			return fmt.Errorf("StatisticExpr.SortBucketResult: cannot convert %v to int", self.Limit)
		}

		// Only return unique limit field combinations
		// Since the results are already in order, and there is no Set in Go, we can use a string arr to record the previous field combinations
		// If the current field combination is different from the previous one, it means we have finished processing data for one field combination (we need to process limit combinations in total, limit is a number)
		uniqueFieldsCombination := make([]string, len(self.FieldList))
		combinationCount := 0
		newResults := make([]*BucketResult, 0)
		for _, bucketResult := range *results {
			combinationExist := true
			for index, fieldName := range self.FieldList {
				keyIndex := fieldToGroupByKeyIndex[fieldName]
				val := GetBucketKey(bucketResult.BucketKey, keyIndex)
				if uniqueFieldsCombination[index] != val {
					uniqueFieldsCombination[index] = val
					combinationExist = false
				}

				statEnclosure, exists := bucketResult.StatRes[fieldName]
				statVal, err := statEnclosure.GetString()
				if exists && err == nil && uniqueFieldsCombination[index] != statVal {
					uniqueFieldsCombination[index] = val
					combinationExist = false
				}
			}

			// If there is a stats groupby block before statistic groupby block. E.g. ... | stats count BY http_status, gender | rare 1 http_status,
			// In this case, each http_status will be divided by two genders, so we should merge them into one row here
			//Fields combination does not exist
			if !combinationExist {
				combinationCount++
				if combinationCount > limit {
					*results = newResults
					return nil
				}
				newResults = append(newResults, bucketResult)
			} else {
				newResults[combinationCount-1].ElemCount += bucketResult.ElemCount
			}
		}

	} else { //No limit option, sort results by its values frequency
		switch self.StatisticFunctionMode {
		case SFMTop:
			sort.Slice(*results, func(index1, index2 int) bool {
				return (*results)[index1].ElemCount > (*results)[index2].ElemCount
			})
		case SFMRare:
			sort.Slice(*results, func(index1, index2 int) bool {
				return (*results)[index1].ElemCount < (*results)[index2].ElemCount
			})
		}
	}

	return nil
}

// Only display fields which in StatisticExpr
func (self *StatisticExpr) RemoveFieldsNotInExprForBucketRes(bucketResult *BucketResult) error {
	groupByCols := self.GetFields()
	groupByKeys := make([]string, 0)
	bucketKey := make([]string, 0)
	iBucketKey := make([]interface{}, 0)

	switch bucketResult.BucketKey.(type) {
	case []interface{}:
		bucketKeySlice := bucketResult.BucketKey.([]interface{})
		for _, field := range groupByCols {
			for rowIndex, groupByCol := range bucketResult.GroupByKeys {
				if field == groupByCol {
					groupByKeys = append(groupByKeys, field)
					iBucketKey = append(iBucketKey, bucketKeySlice[rowIndex])
					break
				}
				//Can not find field in GroupByCol, so it may in the StatRes
				val, exists := bucketResult.StatRes[field]
				if exists {
					groupByKeys = append(groupByKeys, field)
					iBucketKey = append(iBucketKey, val)
					delete(bucketResult.StatRes, field)
				}
			}
		}
		bucketResult.BucketKey = iBucketKey
	case []string:
		bucketKeyStrs := bucketResult.BucketKey.([]string)

		for _, field := range groupByCols {
			for rowIndex, groupByCol := range bucketResult.GroupByKeys {
				if field == groupByCol {
					groupByKeys = append(groupByKeys, field)
					bucketKey = append(bucketKey, bucketKeyStrs[rowIndex])
					break
				}
				//Can not find field in GroupByCol, so it may in the StatRes
				val, exists := bucketResult.StatRes[field]
				if exists {
					valStr, _ := val.GetString()
					groupByKeys = append(groupByKeys, field)
					bucketKey = append(bucketKey, valStr)
					delete(bucketResult.StatRes, field)
				}
			}
		}
		bucketResult.BucketKey = bucketKey
	case string:
		if len(groupByCols) == 0 {
			bucketResult.BucketKey = nil
		} else {
			groupByKeys = groupByCols
			// The GroupByCols of the Statistic block increase, so the new columns must come from the Stats function
			if len(groupByCols) > 1 {
				newBucketKey := make([]string, 0)
				for i := 0; i < len(groupByCols); i++ {
					val, exists := bucketResult.StatRes[groupByCols[i]]
					if exists {
						str, err := val.GetString()
						if err != nil {
							return fmt.Errorf("StatisticExpr.RemoveFieldsNotInExpr: %v", err)
						}
						newBucketKey = append(newBucketKey, str)
					} else {
						newBucketKey = append(newBucketKey, bucketResult.BucketKey.(string))
					}
				}
				bucketResult.BucketKey = newBucketKey
			}
		}
	default:
		return fmt.Errorf("StatisticExpr.RemoveFieldsNotInExpr: bucket key has unexpected type: %T", bucketKey)
	}

	// Remove unused func in stats res
	for statColName := range bucketResult.StatRes {
		statColInGroupByCols := false
		for _, groupByCol := range groupByCols {
			if groupByCol == statColName {
				statColInGroupByCols = true
				break
			}
		}
		if !statColInGroupByCols {
			delete(bucketResult.StatRes, statColName)
		}
	}

	bucketResult.GroupByKeys = groupByKeys
	return nil
}

func (self *StatisticExpr) GetFields() []string {
	if self == nil {
		return nil
	}
	return append(self.FieldList, self.ByClause...)
}

func (self *RenameExpr) Evaluate(fieldToValue map[string]sutils.CValueEnclosure, fieldName string) (string, error) {
	return getValueAsString(fieldToValue, fieldName)
}

func (self *RexExpr) Evaluate(fieldToValue map[string]sutils.CValueEnclosure, rexExp *regexp.Regexp) (map[string]string, error) {

	fieldValue, err := getValueAsString(fieldToValue, self.FieldName)
	if utils.IsNilValueError(err) {
		return nil, err
	} else if err != nil {
		return nil, fmt.Errorf("RexExpr.Evaluate: %v", err)
	}

	return MatchAndExtractNamedGroups(fieldValue, rexExp)
}

func MatchAndExtractNamedGroups(str string, rexExp *regexp.Regexp) (map[string]string, error) {
	match := rexExp.FindStringSubmatch(str)
	if len(match) == 0 {
		return nil, fmt.Errorf("MatchAndExtractNamedGroups: no str in field match the pattern")
	}

	names := rexExp.SubexpNames()
	if len(names) == 0 {
		return nil, fmt.Errorf("MatchAndExtractNamedGroups: no field create from the pattern")
	}

	result := make(map[string]string, len(names))
	for i, name := range names {
		if i != 0 && name != "" {
			result[name] = match[i]
		}
	}

	return result, nil
}

func MatchAndPopulateNamedGroups(str string, rexExp *regexp.Regexp,
	newColValues map[string][]sutils.CValueEnclosure, idx int, numItems int) error {
	match := rexExp.FindStringSubmatch(str)
	if len(match) == 0 {
		return fmt.Errorf("MatchAndPopulateNamedGroups: no str in field match the pattern")
	}
	names := rexExp.SubexpNames()
	if len(names) == 0 {
		return fmt.Errorf("MatchAndPopulateNamedGroups: no field create from the pattern")
	}

	for i, name := range names {
		if newColValues[name] == nil {
			newColValues[name] = make([]sutils.CValueEnclosure, numItems)
		}
		if i != 0 && name != "" {
			newColValues[name][idx].Dtype = sutils.SS_DT_STRING
			newColValues[name][idx].CVal = match[i]
		}
	}

	return nil
}

func MatchAndExtractGroups(str string, rexExp *regexp.Regexp) (map[string]string, []string, error) {
	match := rexExp.FindStringSubmatch(str)
	if len(match) == 0 {
		return nil, nil, fmt.Errorf("MatchAndExtractGroups: no str in field match the pattern")
	}

	keyToValuesMap := make(map[string]string)
	for i, name := range rexExp.SubexpNames() {
		if i != 0 && name != "" {
			keyToValuesMap[name] = match[i]
		}
	}

	return keyToValuesMap, match, nil
}

// Check if colName match the OriginalPattern
func (self *RenameExpr) CheckIfMatch(colName string) bool {
	regexPattern := `\b` + strings.ReplaceAll(self.OriginalPattern, "*", "(.*)") + `\b`
	regex, err := regexp.Compile(regexPattern)
	if err != nil {
		return false
	}

	matchingParts := regex.FindStringSubmatch(colName)
	return len(matchingParts) != 0
}

// Check if colName matches the specified pattern and replace wildcards to generate a new colName.
func (self *RenameExpr) ProcessRenameRegexExpression(colName string) (string, error) {

	originalPattern := self.OriginalPattern
	newPattern := self.NewPattern

	regexPattern := `\b` + strings.ReplaceAll(originalPattern, "*", "(.*)") + `\b`
	regex, err := regexp.Compile(regexPattern)
	if err != nil {
		return "", fmt.Errorf("RenameExpr.ProcessRenameRegexExpression: There are some errors in the pattern: %v, err: %v", regexPattern, err)
	}

	matchingParts := regex.FindStringSubmatch(colName)
	if len(matchingParts) == 0 {
		return "", nil
	}

	result := newPattern
	for _, match := range matchingParts[1:] {
		result = strings.Replace(result, "*", match, 1)
	}

	return result, nil
}

func ProcessRenameRegexExp(origPattern string, newPattern string, colName string) (string, error) {

	regexPattern := `\b` + strings.ReplaceAll(origPattern, "*", "(.*)") + `\b`
	regex, err := regexp.Compile(regexPattern)
	if err != nil {
		return "", fmt.Errorf("ProcessRenameRegexExp: There are some errors in the pattern: %v, err: %v", regexPattern, err)
	}

	matchingParts := regex.FindStringSubmatch(colName)
	if len(matchingParts) == 0 {
		return "", nil
	}

	result := newPattern
	for _, match := range matchingParts[1:] {
		result = strings.Replace(result, "*", match, 1)
	}

	return result, nil
}

func (self *RenameExpr) RemoveColsByIndex(strs []string, indexToRemove []int) []string {
	results := make([]string, 0)

	for index, val := range strs {
		shouldRemove := false
		for _, delIndex := range indexToRemove {
			if delIndex == index {
				shouldRemove = true
				break
			}
		}
		if shouldRemove {
			continue
		}
		results = append(results, val)
	}
	return results
}

func (self *RenameExpr) RemoveBucketResGroupByColumnsByIndex(bucketResult *BucketResult, indexToRemove []int) {

	if len(indexToRemove) == 0 {
		return
	}

	bucketResult.GroupByKeys = self.RemoveColsByIndex(bucketResult.GroupByKeys, indexToRemove)

	switch bucketKey := bucketResult.BucketKey.(type) {
	case []string:
		bucketResult.BucketKey = self.RemoveColsByIndex(bucketKey, indexToRemove)
	case string:
		bucketResult.BucketKey = nil
	}

}

// Remove unused GroupByVals in Bucket Holder
func (self *RenameExpr) RemoveBucketHolderGroupByColumnsByIndex(bucketHolder *BucketHolder, groupByCols []string, indexToRemove []int) {

	if len(indexToRemove) == 0 {
		return
	}

	groupByVals := make([]string, 0)
	for index := range groupByCols {
		shouldRemove := false
		for _, delIndex := range indexToRemove {
			if delIndex == index {
				shouldRemove = true
				break
			}
		}
		if shouldRemove {
			continue
		}
		groupByVals = append(groupByVals, bucketHolder.GroupByValues[index])
	}

	bucketHolder.GroupByValues = groupByVals

}

// For Rename or top/rare block, we may need to delete some groupby columns while processing them
func RemoveUnusedGroupByCols(aggs *QueryAggregators, aggGroupByCols []string) []string {
	for agg := aggs; agg != nil; agg = agg.Next {
		// Rename block
		aggGroupByCols = GetRenameGroupByCols(aggGroupByCols, agg)
		// Statistic block: to be finished
	}
	return aggGroupByCols
}

// Rename field A to field B. If A and B are groupby columns, field B should be removed from groupby columns, and rename A to B
func GetRenameGroupByCols(aggGroupByCols []string, agg *QueryAggregators) []string {
	if agg.OutputTransforms != nil && agg.OutputTransforms.LetColumns != nil && agg.OutputTransforms.LetColumns.RenameColRequest != nil {

		// Except for regex, other RenameExprModes will only rename one column
		renameIndex := -1
		indexToRemove := make([]int, 0)

		for index, groupByCol := range aggGroupByCols {
			switch agg.OutputTransforms.LetColumns.RenameColRequest.RenameExprMode {
			case REMPhrase:
				fallthrough
			case REMOverride:

				if groupByCol == agg.OutputTransforms.LetColumns.RenameColRequest.OriginalPattern {
					renameIndex = index
				}
				if groupByCol == agg.OutputTransforms.LetColumns.RenameColRequest.NewPattern {
					indexToRemove = append(indexToRemove, index)
				}

			case REMRegex:
				newColName, err := agg.OutputTransforms.LetColumns.RenameColRequest.ProcessRenameRegexExpression(groupByCol)
				if err != nil {
					return []string{}
				}
				if len(newColName) == 0 {
					continue
				}
				for i, colName := range aggGroupByCols {
					if colName == newColName {
						indexToRemove = append(indexToRemove, i)
						break
					}
				}
				aggGroupByCols[index] = newColName
			}
		}
		if renameIndex != -1 {
			aggGroupByCols[renameIndex] = agg.OutputTransforms.LetColumns.RenameColRequest.NewPattern
		}
		aggGroupByCols = agg.OutputTransforms.LetColumns.RenameColRequest.RemoveColsByIndex(aggGroupByCols, indexToRemove)
	}
	return aggGroupByCols
}

func handleNoArgFunction(op string) (float64, error) {
	switch op {
	case "now":
		return float64(time.Now().Unix()), nil
	case "random":
		return float64(rand.Int31()), nil
	case "pi":
		return math.Pi, nil
	case "time":
		return float64(time.Now().UnixMilli()), nil
	default:
		log.Errorf("handleNoArgFunction: Unsupported no argument function: %v", op)
		return 0, fmt.Errorf("handleNoArgFunction: Unsupported no argument function: %v", op)
	}
}

func handleComparisonAndConditionalFunctions(self *ConditionExpr, fieldToValue map[string]sutils.CValueEnclosure, functionName string) (interface{}, error) {
	switch functionName {
	case "validate":
		for _, cvPair := range self.ConditionValuePairs {
			res, err := cvPair.Condition.Evaluate(fieldToValue)
			if err != nil {
				nullFields, nullFieldsErr := cvPair.Condition.GetNullFields(fieldToValue)
				if nullFieldsErr != nil {
					return "", fmt.Errorf("handleComparisonAndConditionalFunctions: Error while getting null fields, err: %v fieldToValue: %v", nullFieldsErr, fieldToValue)
				}
				if len(nullFields) > 0 {
					continue
				}
				return "", utils.WrapErrorf(err, "handleComparisonAndConditionalFunctions: Error while evaluating condition, err: %v fieldToValue: %v", err, fieldToValue)
			}
			if !res {
				val, err := cvPair.Value.EvaluateValueExpr(fieldToValue)
				if err != nil {
					return "", utils.WrapErrorf(err, "handleComparisonAndConditionalFunctions: Error while evaluating value, err: %v fieldToValue: %v", err, fieldToValue)
				}
				return val, nil
			}
		}
		return "", nil
	default:
		return "", fmt.Errorf("handleComparisonAndConditionalFunctions: Unknown function name: %s", functionName)
	}
}

func applySigfigToRes(fltVal float64, sigfigs int) float64 {
	strArr := strings.Split(strconv.FormatFloat(fltVal, 'f', -1, 64), ".")
	var lenDecPart int = 0
	if len(strArr) > 1 {
		lenDecPart = len(strArr[1])
	}
	lenMantPart := len(strArr[0])
	totalResLen := lenDecPart + lenMantPart
	if totalResLen < sigfigs {
		return fltVal
	}
	if strArr[0] == "0" {
		offset := 0
		for _, char := range strArr[1] {
			if char != '0' {
				break
			}
			offset++
		}
		return round(fltVal, sigfigs+offset)
	}
	return round(fltVal, sigfigs-lenMantPart)
}

func (self *NumericExpr) getSigDecimals(strVal string, fltVal float64) int {
	if len(strVal) == 0 {
		strVal = strconv.FormatFloat(fltVal, 'f', -1, 64)
	}
	clnArr := strings.Split(strings.Split(strVal, "e")[0], ".")
	if len(clnArr) > 1 {
		return len(clnArr[1])
	} else {
		return 0
	}
}

func (self *NumericExpr) getSigFigs(strVal string, fltVal float64) int {
	if len(strVal) == 0 {
		strVal = strconv.FormatFloat(fltVal, 'f', -1, 64)
	}
	strVal = strings.Split(strVal, "e")[0]
	sigfigs := 0
	if strings.Contains(strVal, ".") {
		valArr := strings.Split(strVal, ".")
		mantPart := valArr[0]
		decPart := valArr[1]
		mantPartNum, err := utils.FastParseFloat([]byte(mantPart))
		if err == nil {
			if mantPartNum == 0 {
				// all leading 0s are not significant
				sigfigs += len(strings.TrimLeft(decPart, "0"))
			} else {
				sigfigs += len(mantPart) + len(decPart)
			}
		}
	} else {
		// all trailing 0s are not significant
		sigfigs += len(strings.TrimRight(strVal, "0"))
	}
	return sigfigs
}

func splPrintfHumanizeHelper(verbCounter int, value any, formatValues []any, buffer *[]byte, verb byte, parseInt func(string) (int64, error), parseFloat func(string) (float64, error)) {
	var strVal string
	formatValues[verbCounter] = value
	numFlagsWConsumeArgs := 0
	lastPercentPos := -1
	for i := len(*buffer) - 1; i >= 0; i-- {
		if (*buffer)[i] == '%' {
			lastPercentPos = i
			break
		} else if (*buffer)[i] == '*' {
			numFlagsWConsumeArgs++
		}
	}
	//  only use the last verb for formatting. i.e the buffer from the lastPercentPos
	formattedVal := fmt.Sprintf(string(append((*buffer)[lastPercentPos:], verb)), formatValues[(verbCounter-numFlagsWConsumeArgs):verbCounter+1]...)
	// can't use precomputed hasDecimal since formatting may remove the decimal
	hasDecimal := strings.Contains(formattedVal, ".")
	// remove spaces, leading zeros, signs from the left.
	// remove spaces and trailing zeros from the right
	// we add back these removed values at the end
	trimmedVal := strings.TrimSpace(formattedVal)
	start := strings.Index(formattedVal, trimmedVal)
	end := start + len(trimmedVal)
	if trimmedVal[0] == '+' || trimmedVal[0] == '-' {
		trimmedVal = trimmedVal[1:]
		start += 1
	}
	for i := 0; i < len(trimmedVal); i++ {
		if trimmedVal[i] != '0' || (trimmedVal[i] == '.' && hasDecimal) {
			break
		}
		start++
	}
	if hasDecimal {
		for i := len(trimmedVal) - 1; i >= 0; i-- {
			if trimmedVal[i] != '0' || trimmedVal[i] == '.' {
				break
			}
			end--
		}
	}
	removedLeft := formattedVal[:start]
	removedRight := formattedVal[end:]
	switch value.(type) {
	case int64:
		intVal, err := parseInt(trimmedVal)
		if err != nil {
			log.Errorf("splPrintfToGoPrintfConverter: Unable to parse int value; val: %v", formattedVal)
		} else {
			strVal = humanize.Comma(intVal)
		}
	case float64:
		fltVal, err := parseFloat(trimmedVal)
		if err != nil {
			log.Errorf("splPrintfToGoPrintfConverter: Unable to parse float value; val: %v", formattedVal)
		} else {
			strVal = humanize.Commaf(fltVal)
		}
	case uint64:
		uintVal, err := strconv.ParseUint(trimmedVal, 10, 64)
		if err != nil {
			log.Errorf("splPrintfToGoPrintfConverter: Unable to parse uint value; val: %v", formattedVal)
		} else {
			strVal = utils.HumanizeUints(uintVal)
		}
	case string:
		strVal = formattedVal
	default:
		log.Errorf("splPrintfToGoPrintfConverter: Unable to determine dtype when adding val to buffer; val: %v", formattedVal)
	}
	strVal = removedLeft + strVal + removedRight
	formatValues[verbCounter] = strVal
	if numFlagsWConsumeArgs != 0 {
		for i := verbCounter - numFlagsWConsumeArgs; i < verbCounter; i++ {
			formatValues[i] = nil
		}
	}
	// Clear any formatting flags from the previous '%' token.
	// For example, consider the format string "%.5g": the rounding has already been applied earlier
	// when we initially processed this format using sprintf.
	//
	// If we now substitute the formatted value 's' directly into the format string, like replacing %g with %s,
	// the format string becomes "%.5s". This causes the final sprintf call to re-apply formatting (like rounding or truncation),
	// which is incorrect because the value has already been rounded once.

	if lastPercentPos != -1 {
		*buffer = (*buffer)[:lastPercentPos]
	}
	*buffer = append(*buffer, '%', 's')
}

func splPrintfToGoPrintfConverter(buffer *[]byte, originalVerb byte, formatValues []any, valStr string, verbCounter int, humanizeVal bool) error {
	var splToGoFmt = map[byte]byte{
		'a': 'x',
		'A': 'X',
		'i': 'd',
		'z': 's',
		'u': '-',
		'x': '-',
		'X': '-',
		'o': '-',
		'p': '-',
	}

	parseInt := func(s string) (int64, error) {
		return strconv.ParseInt(s, 10, 64)
	}
	parseFloat := func(s string) (float64, error) {
		return utils.FastParseFloat([]byte(s))
	}

	addToBuffer := func(buffer *[]byte, verb byte, value any, formatValues []any, humanizeVal bool, verbCounter int) {
		if humanizeVal {
			splPrintfHumanizeHelper(verbCounter, value, formatValues, buffer, verb, parseInt, parseFloat)
		} else {
			formatValues[verbCounter] = value
			*buffer = append(*buffer, verb)
		}
	}

	hasDecimal := strings.Contains(valStr, ".")

	var verb byte
	if val, ok := splToGoFmt[originalVerb]; ok {
		verb = val
	} else {
		verb = originalVerb
	}
	var INVALID_INT_DTYPE_PRINTF_ERR = fmt.Errorf("splPrintfToGoPrintfConverter: printf - %v format specifier requires an integer value; received: %v", verb, valStr)
	var INVALID_FLOAT_DTYPE_PRINTF_ERR = fmt.Errorf("splPrintfToGoPrintfConverter: printf - %v format specifier requires a float value; received: %v", verb, valStr)
	var INVALID_DTYPE_PRINTF_ERR = fmt.Errorf("splPrintfToGoPrintfConverter: printf - %v format specifier requires either an integer or a float value; received: %v", verb, valStr)

	switch verb {
	// only accepts integers
	case 'b', 'c', 'd', 'q':
		if !hasDecimal {
			if val, err := parseInt(valStr); err == nil {
				addToBuffer(buffer, verb, val, formatValues, humanizeVal, verbCounter)
				// c in go doesn't accept a string value. Therefore we handle it manually
			} else if verb == 'c' {
				if len(valStr) > 0 {
					var first rune
					for _, r := range valStr {
						first = r
						break
					}
					addToBuffer(buffer, 'c', first, formatValues, humanizeVal, verbCounter)
				} else {
					return fmt.Errorf("splPrintfToGoPrintfConverter: printf - %v format requires a non empty string", verb)
				}
			} else {
				return INVALID_INT_DTYPE_PRINTF_ERR
			}
		} else {
			return INVALID_INT_DTYPE_PRINTF_ERR
		}
	// only accepts floats
	case 'e', 'E', 'f', 'F', 'g', 'G':
		// according to splunks example, there is no strict checking
		// => 123 can be treated as 123.0
		val, err := parseFloat(valStr)
		if err != nil {
			return INVALID_FLOAT_DTYPE_PRINTF_ERR
		}
		addToBuffer(buffer, verb, val, formatValues, humanizeVal, verbCounter)
	// works for both integer and floats
	case 'X', 'x':
		if hasDecimal {
			val, err := parseFloat(valStr)
			if err != nil {
				return INVALID_DTYPE_PRINTF_ERR
			}
			addToBuffer(buffer, verb, val, formatValues, humanizeVal, verbCounter)
		} else {
			val, err := parseInt(valStr)
			if err != nil {
				return INVALID_DTYPE_PRINTF_ERR
			}
			addToBuffer(buffer, verb, val, formatValues, humanizeVal, verbCounter)
		}
	// these are verbs which don't have an equivalent in go. Therefore we handle it manually
	case '-':
		switch originalVerb {
		// x, o, u - return unsigned values
		case 'x', 'o', 'u', 'X', 'p':
			if hasDecimal && originalVerb != 'o' {
				if originalVerb == 'u' {
					originalVerb = 'f'
				} else if originalVerb == 'p' {
					originalVerb = 'X'
				}
				val, err := parseFloat(valStr)
				if err != nil {
					return INVALID_DTYPE_PRINTF_ERR
				}
				if val < 0 {
					val = val * -1
				}
				addToBuffer(buffer, originalVerb, val, formatValues, humanizeVal, verbCounter)
			} else {
				if originalVerb == 'u' {
					originalVerb = 'd'
				} else if originalVerb == 'o' {
					if hasDecimal {
						valStr = strings.Split(valStr, ".")[0]
					}
				} else if originalVerb == 'p' {
					originalVerb = 'X'
				}
				val, err := parseInt(valStr)
				if err != nil {
					return INVALID_DTYPE_PRINTF_ERR
				}
				if val < 0 {
					val = val * -1
				}
				addToBuffer(buffer, originalVerb, val, formatValues, humanizeVal, verbCounter)
			}
		default:
			return INVALID_DTYPE_PRINTF_ERR
		}
	// rest are only string formatters which require a string argument
	default:
		addToBuffer(buffer, verb, valStr, formatValues, humanizeVal, verbCounter)
	}
	return nil
}

func splPrintfHandleArgs(verbCounter int, fieldToValue map[string]sutils.CValueEnclosure, self *TextExpr) (string, error) {
	var valStr string
	var err error
	if verbCounter < len(self.ValueList) {
		fieldName := self.ValueList[verbCounter].FieldName
		if fieldName != "" {
			fltVal, err := handleNoArgFunction(fieldName)
			if err != nil {
				if val, ok := fieldToValue[fieldName]; ok {
					switch val.Dtype {
					case sutils.SS_DT_STRING:
						valStr = val.CVal.(string)
					case sutils.SS_DT_SIGNED_NUM:
						valStr = strconv.FormatInt(val.CVal.(int64), 10)
					case sutils.SS_DT_UNSIGNED_NUM:
						valStr = strconv.FormatUint(val.CVal.(uint64), 10)
					case sutils.SS_DT_FLOAT:
						valStr = strconv.FormatFloat(val.CVal.(float64), 'f', -1, 64)
					default:
						return "", fmt.Errorf("splPrintfHandleArgs: Unable to determine dtype for value in fieldToValueMap; val: %v", val)
					}
				}
			} else {
				valStr = strconv.FormatFloat(fltVal, 'f', -1, 64)
			}
		} else {
			if len(self.ValueList[verbCounter].RawString) == 0 {
				valStr, err = self.ValueList[verbCounter].ConcatExpr.Evaluate(fieldToValue)
			} else {
				valStr = self.ValueList[verbCounter].RawString
			}
			if err != nil {
				return "", fmt.Errorf("splPrintfHandleArgs: error while evaluating ConcatExpr")
			}
		}
	} else {
		return "", fmt.Errorf("splPrintfHandleArgs: printf didn't receive enough arguments to format")
	}
	return valStr, nil
}

func (self *NumericExpr) evaluateWithSigfig(expr *NumericExpr, fieldToValue map[string]sutils.CValueEnclosure, sigfigArr *[]SigfigInfo) (float64, string, error) {
	if expr.IsTerminal {
		value, err := expr.Evaluate(fieldToValue)
		if err != nil {
			return 0, "", err
		}
		return value, "", nil
	} else {
		var leftInfo, rightInfo SigfigInfo
		if expr.Left != nil {
			leftValue, _, err := self.evaluateWithSigfig(expr.Left, fieldToValue, sigfigArr)
			if err != nil {
				return 0, "", err
			}
			leftInfo = SigfigInfo{
				Value:        leftValue,
				SigFigs:      self.getSigFigs(expr.Left.Value, leftValue),
				DecimalPlace: self.getSigDecimals(expr.Left.Value, leftValue),
			}
		}

		if expr.Right != nil {
			rightValue, _, err := self.evaluateWithSigfig(expr.Right, fieldToValue, sigfigArr)
			if err != nil {
				return 0, "", err
			}
			rightInfo = SigfigInfo{
				Value:        rightValue,
				SigFigs:      self.getSigFigs(expr.Right.Value, rightValue),
				DecimalPlace: self.getSigDecimals(expr.Right.Value, rightValue),
			}
		}

		resultInfo := SigfigInfo{
			SigFigs:      min(leftInfo.SigFigs, rightInfo.SigFigs),
			DecimalPlace: min(leftInfo.DecimalPlace, rightInfo.DecimalPlace),
		}
		switch expr.Op {
		case "+", "-":
			var result float64
			if expr.Op == "+" {
				result = leftInfo.Value + rightInfo.Value
			} else {
				result = leftInfo.Value - rightInfo.Value
			}
			resultInfo.Value = result
			// convert to sigfigs -> usefull if the last operations is either * or /
			resArr := strings.Split(strconv.FormatFloat(result, 'f', -1, 64), ".")
			// required due to floating point error
			var resStr string
			if len(resArr) > 1 {
				resStr = resArr[0] + "." + resArr[1][:resultInfo.DecimalPlace]
			} else {
				resStr = resArr[0]
			}
			resultInfo.SigFigs = expr.getSigFigs(resStr, 0.0)
			*sigfigArr = append(*sigfigArr, resultInfo)
			return result, expr.Op, nil
		case "*", "/":
			var result float64
			if expr.Op == "*" {
				result = leftInfo.Value * rightInfo.Value
			} else {
				result = leftInfo.Value / rightInfo.Value
			}
			resultInfo.Value = result
			// convert to decimal places -> usefull if the last operations is either + or -
			tempRes := applySigfigToRes(result, resultInfo.SigFigs)
			tempResStr := strconv.FormatFloat(tempRes, 'f', -1, 64)
			tempResArr := strings.Split(tempResStr, ".")
			if len(tempResArr) > 1 {
				resultInfo.DecimalPlace = len(tempResArr[1])
			} else {
				resultInfo.DecimalPlace = 0
			}
			*sigfigArr = append(*sigfigArr, resultInfo)
			return result, expr.Op, nil
		default:
			result, err := expr.Evaluate(fieldToValue)
			return result, "", err
		}
	}
}

// Evaluate this NumericExpr to a float, replacing each field in the expression
// with the value specified by fieldToValue. Each field listed by GetFields()
// must be in fieldToValue.
func (self *NumericExpr) Evaluate(fieldToValue map[string]sutils.CValueEnclosure) (float64, error) {
	if self.IsTerminal {
		if self.ValueIsField {
			switch self.NumericExprMode {
			case NEMNumberField:
				return getValueAsFloat(fieldToValue, self.Value)
			case NEMLenField:
				_, ok := fieldToValue[self.Value].CVal.(string)
				if !ok {
					return 0, fmt.Errorf("NumericExpr.Evaluate: cannot convert %v to string", self.Value)
				}
				return float64(len(fieldToValue[self.Value].CVal.(string))), nil
			}
		} else {
			switch self.NumericExprMode {
			case NEMNumber:
				if self.Op != "" {
					if self.Value != "" {
						return 0, fmt.Errorf("NumericExpr.Evaluate: Error calling no argument function: %v, value: %v", self.Op, self.Value)
					}
					return handleNoArgFunction(self.Op)
				}

				value, err := strconv.ParseFloat(self.Value, 64)
				if err != nil {
					return 0, utils.NewErrorWithCode(utils.CONVERSION_ERR, fmt.Errorf("NumericExpr.Evaluate: cannot convert %v to float", self.Value))
				}
				return value, nil
			case NEMLenString:
				value := float64(len(self.Value))
				return value, nil
			}
		}
		return 0, fmt.Errorf("NumericExpr.Evaluate: cannot convert %v to float", self.Value)
	} else {

		left := float64(0)
		var err error
		if self.Left != nil {
			left, err = self.Left.Evaluate(fieldToValue)
			if err != nil {
				return 0, err
			}
		}

		var right float64
		if self.Right != nil {
			right, err = self.Right.Evaluate(fieldToValue)
			if err != nil {
				return 0, err
			}
		}

		switch self.Op {
		case "+":
			return left + right, nil
		case "-":
			return left - right, nil
		case "*":
			return left * right, nil
		case "/":
			return left / right, nil
		case "%":
			return math.Mod(left, right), nil
		case "abs":
			return math.Abs(left), nil
		case "ceil":
			return math.Ceil(left), nil
		case "sigfig":
			var sigfigArr []SigfigInfo
			result, op, err := self.evaluateWithSigfig(self.Left, fieldToValue, &sigfigArr)
			if err != nil {
				return -1, fmt.Errorf("NumericExpr.Evaluate: error while evaluating sigfig; err: %v", err)
			}
			switch op {
			case "+", "-":
				minDecPlace := math.MaxInt
				for idx := range sigfigArr {
					minDecPlace = min(minDecPlace, sigfigArr[idx].DecimalPlace)
				}
				return round(result, minDecPlace), nil
			case "*", "/":
				minSigFig := math.MaxInt
				for idx := range sigfigArr {
					minSigFig = min(minSigFig, sigfigArr[idx].SigFigs)
				}
				return applySigfigToRes(result, minSigFig), nil
			default:
				return result, nil
			}
		case "acosh":
			if left < 1 {
				return -1, fmt.Errorf("NumericExpr.Evaluate: acosh requires values >= 1, got: %v", left)
			}
			return math.Acosh(left), nil
		case "acos":
			if left < -1 || left > 1 {
				return -1, fmt.Errorf("NumericExpr.Evaluate: acos requires values between -1 and 1, got: %v", left)
			}
			return math.Acos(left), nil
		case "asin":
			if left < -1 || left > 1 {
				return -1, fmt.Errorf("NumericExpr.Evaluate: asin requires values between -1 and 1, got: %v", left)
			}
			return math.Asin(left), nil
		case "asinh":
			return math.Asinh(left), nil
		case "atan":
			return math.Atan(left), nil
		case "atanh":
			if left <= -1 || left >= 1 {
				return -1, fmt.Errorf("NumericExpr.Evaluate: atanh requires values between -1 and 1 exclusive, got: %v", left)
			}
			return math.Atanh(left), nil
		case "cos":
			return math.Cos(left), nil
		case "cosh":
			return math.Cosh(left), nil
		case "sin":
			return math.Sin(left), nil
		case "sinh":
			return math.Sinh(left), nil
		case "tan":
			// Check for points where cos(x) = 0, which would cause tan to be undefined.
			// These are points (pi/2 + k*pi) where k is an integer.
			// To check for this, see if left modulo pi is pi/2 (or very close due to floating point precision).
			halfPi := math.Pi / 2
			mod := math.Mod(left, math.Pi)
			if math.Abs(mod-halfPi) < 0.0000001 || math.Abs(mod+halfPi) < 0.0000001 {
				return -1, fmt.Errorf("NumericExpr.Evaluate: tan is undefined at pi/2 + k*pi, got: %v", left)
			}
			return math.Tan(left), nil
		case "tanh":
			return math.Tanh(left), nil
		case "atan2":
			if self.Left == nil || self.Right == nil {
				return -1, fmt.Errorf("NumericExpr.Evaluate: atan2 requires two values, got: left=%v, right=%v", self.Left, self.Right)
			}
			return math.Atan2(left, right), nil
		case "hypot":
			if self.Left == nil || self.Right == nil {
				return -1, fmt.Errorf("NumericExpr.Evaluate: hypot requires two values, got: left=%v, right=%v", self.Left, self.Right)
			}
			return math.Hypot(left, right), nil
		case "log":
			switch {
			case left <= 0:
				return -1, fmt.Errorf("NumericExpr.Evaluate: Non-positive values cannot be used for logarithm: %v", left)
			case right < 0, right == 1:
				return -1, fmt.Errorf("NumericExpr.Evaluate: Invalid base for logarithm: %v", right)
			case right == 0:
				right = 10
			}
			return math.Log(left) / math.Log(right), nil
		case "ln":
			if left < 0 {
				return -1, fmt.Errorf("NumericExpr.Evaluate: Negative values cannot be used for natural logarithm: %v", left)
			}
			return math.Log(left), nil
		case "floor":
			return math.Floor(left), nil
		case "pow":
			return math.Pow(left, right), nil
		case "round":
			if self.Right != nil {
				return round(left, int(right)), nil
			} else {
				return math.Round(left), nil
			}
		case "sqrt":
			if left < 0 {
				return -1, fmt.Errorf("NumericExpr.Evaluate: Negative values cannot be converted to square roots: %v", left)
			}
			return math.Sqrt(left), nil
		case "len":
			return left, nil
		case "exact":
			result, err := self.Left.Evaluate(fieldToValue)
			if err != nil {
				return 0, err
			}
			return result, nil
		case "exp":
			exp, err := self.Left.Evaluate(fieldToValue)
			if err != nil {
				return 0, err
			}
			return math.Exp(exp), nil
		case "tonumber":
			if self.Val == nil {
				return 0, fmt.Errorf("NumericExpr.Evaluate: tonumber operation requires a string expression")
			}
			strValue, err := self.Val.Evaluate(fieldToValue)
			if err != nil {
				return 0, utils.WrapErrorf(err, "NumericExpr.Evaluate: Error in tonumber operation: %v", err)
			}
			base := 10
			if self.Right != nil {
				baseValue, err := self.Right.Evaluate(fieldToValue)
				if err != nil {
					return 0, err
				}
				base = int(baseValue)
				if base < 2 || base > 36 {
					return 0, fmt.Errorf("NumericExpr.Evaluate: Invalid base for tonumber: %v", base)
				}
			}
			number, err := strconv.ParseInt(strValue, base, 64)
			if err != nil {
				// DO NOT return an error if the string is not a number
				// just return 0
				return 0, nil
			}
			return float64(number), nil
		case "relative_time":
			if self.Left == nil {
				return 0, fmt.Errorf("NumericExpr.Evaluate: relative_time operation requires a non-nil left operand")
			}

			var epochTime int64
			var err error
			if left >= 0 {
				epochTime = int64(left)
			} else {
				return 0, fmt.Errorf("NumericExpr.Evaluate: relative_time operation requires a valid timestamp")
			}

			relTime, err := sutils.CalculateAdjustedTimeForRelativeTimeCommand(self.RelativeTime, time.Unix(epochTime, 0))
			if err != nil {
				return 0, fmt.Errorf("NumericExpr.Evaluate: error calculating relative time: %v", err)
			}

			return float64(relTime), nil
		default:
			return 0, fmt.Errorf("NumericExpr.Evaluate: unexpected operation: %v", self.Op)
		}
	}
}

func handleTrimFunctions(op string, value string, trim_chars string) string {
	if trim_chars == "" {
		trim_chars = "\t "
	}
	switch op {
	case "ltrim":
		return strings.TrimLeft(value, trim_chars)
	case "rtrim":
		return strings.TrimRight(value, trim_chars)
	case "trim":
		return strings.Trim(value, trim_chars)
	default:
		return value
	}
}

// formatTime formats a time.Time object into a string based on the provided format string, using mappings from Go's time package and strftime.net.
func formatTime(t time.Time, format string) string {
	preReplacements := map[string]string{
		"%e": "_2",
		"%a": "Mon",
		"%A": "Monday",
		"%d": "02",
		"%b": "Jan",
		"%B": "January",
		"%m": "01",
		"%y": "06",
		"%Y": "2006",
		"%H": "15",
		"%I": "03",
		"%p": "PM",
		"%M": "04",
		"%S": "05",
		"%f": ".000000",
		"%z": "-0700",
		"%Z": "MST",
		"%c": "Mon Jan 2 15:04:05 2006",
		"%x": "01/02/06",
		"%X": "15:04:05",
		"%%": "%",
		"%k": "_15",
		"%T": "15:04:05",
		"%F": "2006-01-02", // The ISO 8601 date format
	}
	for k, v := range preReplacements {
		format = strings.ReplaceAll(format, k, v)
	}

	timeStr := t.Format(format)

	_, week := t.ISOWeek()
	_, offset := t.Zone()
	offsetHours := offset / 3600
	offsetMinutes := (offset % 3600) / 60
	formattedOffset := fmt.Sprintf("%+03d:%02d", offsetHours, offsetMinutes)
	postReplacements := map[string]string{
		"%w":  strconv.Itoa(int(t.Weekday())),                         // weekday as a decimal number
		"%j":  strconv.Itoa(t.YearDay()),                              // day of the year as a decimal number
		"%U":  strconv.Itoa(t.YearDay() / 7),                          // week number of the year (Sunday as the first day of the week)
		"%W":  strconv.Itoa((int(t.Weekday()) - 1 + t.YearDay()) / 7), // week number of the year (Monday as the first day of the week)
		"%V":  strconv.Itoa(week),                                     // ISO week number
		"%+":  t.Format("Mon Jan 2 15:04:05 MST 2006"),                // date and time with timezone
		"%N":  fmt.Sprintf("%09d", t.Nanosecond()),                    // nanoseconds
		"%Q":  strconv.Itoa(t.Nanosecond() / 1e6),                     // milliseconds
		"%Ez": formattedOffset,                                        // timezone offset
		"%s":  strconv.FormatInt(t.Unix(), 10),                        // Unix Epoch Time timestamp
	}
	for k, v := range postReplacements {
		timeStr = strings.ReplaceAll(timeStr, k, v)
	}

	return timeStr
}

// parseTime parses a string into a time.Time object based on the provided format string, using mappings for Go's time package.
func parseTime(dateStr, format string) (time.Time, error) {

	for _, r := range timeFormatReplacements {
		format = strings.ReplaceAll(format, r.key, r.val)
	}

	// Check if format contains only time components (%H, %I, %M, %S, %p) and no date components (%d, %m, %Y, etc.)
	if !strings.Contains(format, "2006") && !strings.Contains(format, "01") && !strings.Contains(format, "02") {
		// Prepend a default date if only time is present
		dateStr = "1970-01-01 " + dateStr
		format = "2006-01-02 " + format
	}

	return time.Parse(format, dateStr)
}

func handlePrintf(self *TextExpr, fieldToValue map[string]sutils.CValueEnclosure) (string, error) {
	if self.Param == nil {
		return "", fmt.Errorf("handlePrintf: invalid format specifier")
	}

	if len(self.Param.RawString) == 0 {
		if len(self.ValueList) != 0 {
			return "", fmt.Errorf("handlePrintf: format specifier empty, but values still provided")
		} else {
			return "", nil
		}
	}
	format := self.Param.RawString
	var formatResult []byte
	var pointerToResBuff = &formatResult
	var formatValues []any = make([]any, len(self.ValueList))
	formatEnd := len(format)
	humanizeRes := false
	verbCounter := 0

	for i := 0; i < formatEnd; i++ {
		lasti := i
		for i < formatEnd && format[i] != '%' {
			i++
		}
		if i > lasti {
			*pointerToResBuff = append(*pointerToResBuff, format[lasti:i]...)
		}
		if i >= formatEnd {
			break
		}

		// check if the verb is %%, and continue since %% doesn't consume any arguments
		lookAhead := i + 1
		if lookAhead == formatEnd {
			return "", fmt.Errorf("handlePrintf: unexpected end of format string, string has trailing '%%'")
		}
		if format[lookAhead] == '%' {
			*pointerToResBuff = append(*pointerToResBuff, format[i])
			*pointerToResBuff = append(*pointerToResBuff, format[lookAhead])
			i++
			continue
		}

		// skip to the verbs (while checking for flags not handled by golang)
		for ; i < len(format); i++ {
			if !((format[i] >= 'A' && format[i] <= 'Z') || (format[i] >= 'a' && format[i] <= 'z')) {
				if format[i] == '\'' {
					humanizeRes = true
					// don't write to buffer
					continue
				}
				// asterixis consumes one argument
				if format[i] == '*' {
					valStr, err := splPrintfHandleArgs(verbCounter, fieldToValue, self)
					if err != nil {
						return "", err
					}
					valInt, err := strconv.ParseInt(valStr, 10, 64)
					if err != nil {
						return "", fmt.Errorf("handlePrintf: got invalid dtype for * replacement; got: %v", valStr)
					}
					formatValues[verbCounter] = valInt
					verbCounter++
				}
				*pointerToResBuff = append(*pointerToResBuff, format[i])
			} else {
				break
			}
		}

		valStr, err := splPrintfHandleArgs(verbCounter, fieldToValue, self)
		if err != nil {
			return "", err
		}
		err = splPrintfToGoPrintfConverter(pointerToResBuff, format[i], formatValues, valStr, verbCounter, humanizeRes)
		if err != nil {
			return "", err
		}
		verbCounter++
		humanizeRes = false
	}
	cleanedFormattedArr := formatValues[:0]
	for _, v := range formatValues {
		if v != nil {
			cleanedFormattedArr = append(cleanedFormattedArr, v)
		}
	}
	val := fmt.Sprintf(string(formatResult), cleanedFormattedArr...)
	return val, nil
}

func (self *TextExpr) EvaluateText(fieldToValue map[string]sutils.CValueEnclosure) (string, error) {
	// Todo: implement the processing logic for these functions:
	switch self.Op {
	case "strftime":
		timestamp, err := self.Val.EvaluateToFloat(fieldToValue)
		if err != nil {
			return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: cannot evaluate timestamp: %v", err)
		}
		timestampInSeconds := timestamp / 1000
		t := time.Unix(int64(timestampInSeconds), 0) // time.Unix(sec int64, nsec int64) -> expects seconds and nanoseconds. Since strftime expects seconds, we pass 0 for nanoseconds.

		timeStr := formatTime(t, self.Param.RawString)
		return timeStr, nil
	case "strptime":
		dateStr, err := self.Val.EvaluateToString(fieldToValue)
		if err != nil {
			return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: cannot evaluate date string: %v", err)
		}
		t, err := parseTime(dateStr, self.Param.RawString)
		if err != nil {
			return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: cannot parse date string: %v", err)
		}
		return strconv.FormatInt(t.Unix(), 10), nil
	case "ipmask":
		mask := net.ParseIP(self.Param.RawString).To4()
		ip := net.ParseIP(self.Val.StringExpr.RawString).To4()
		if mask == nil || ip == nil {
			return "", fmt.Errorf("TextExpr.EvaluateText: invalid mask or IP address for 'ipmask' operation")
		}
		if len(ip) != len(mask) {
			return "", fmt.Errorf("TextExpr.EvaluateText: IP address and mask are of different lengths")
		}
		for i := range ip {
			ip[i] &= mask[i]
		}
		return ip.String(), nil
	case "printf":
		formattedString, err := handlePrintf(self, fieldToValue)
		if err != nil {
			return "", err
		}
		return formattedString, nil
	case "replace":
		if len(self.ValueList) < 2 {
			return "", fmt.Errorf("TextExpr.EvaluateText: 'replace' operation requires a regex and a replacement")
		}

		regexStr, err := self.ValueList[0].Evaluate(fieldToValue)
		if err != nil {
			return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: cannot evaluate regex as a string: %v", err)
		}
		replacementStr, err := self.ValueList[1].Evaluate(fieldToValue)
		if err != nil {
			return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: cannot evaluate replacement as a string: %v", err)
		}
		baseStr, err := self.Val.EvaluateValueExprAsString(fieldToValue)
		if err != nil {
			return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: cannot evaluate base string, err: %v", err)
		}
		regex, err := regexp.Compile(regexStr)
		if err != nil {
			return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: failed to compile regex '%s': %v", regexStr, err)
		}
		return regex.ReplaceAllString(baseStr, replacementStr), nil
	case "mvjoin":
		mvSlice, err := self.MultiValueExpr.Evaluate(fieldToValue)
		if err != nil {
			return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: Error while evaluating MultiValueExpr, err: %v", err)
		}

		if self.Delimiter == nil {
			return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: Delimiter is nil")
		}
		delimiter := self.Delimiter.RawString

		return strings.Join(mvSlice, delimiter), nil
	case "mvcount":
		mvSlice, err := self.MultiValueExpr.Evaluate(fieldToValue)
		if err != nil {
			return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: Error while evaluating MultiValueExpr, err: %v", err)
		}

		return strconv.Itoa(len(mvSlice)), nil
	case "mvfind":
		mvSlice, err := self.MultiValueExpr.Evaluate(fieldToValue)
		if err != nil {
			return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: Error while evaluating MultiValueExpr, err: %v", err)
		}
		compiledRegex := self.Regex.GetCompiledRegex()

		// Check if compiledRegex is nil
		if compiledRegex == nil {
			return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: compiled regex is nil")
		}

		for index, value := range mvSlice {
			if compiledRegex.MatchString(value) {
				return strconv.Itoa(index), nil
			}
		}

		// If no match is found
		return "", nil
	case "mvappend":
		fallthrough
	case "mvdedup":
		fallthrough
	case "mvfilter":
		fallthrough
	case "mvmap":
		fallthrough
	case "mvrange":
		fallthrough
	case "mvsort":
		fallthrough
	case "mvzip":
		fallthrough
	case "mv_to_json_array":
		fallthrough
	case "cluster":
		fallthrough
	case "getfields":
		fallthrough
	case "typeof":
		if self.Val.NumericExpr != nil && self.Val.NumericExpr.ValueIsField {
			val, ok := fieldToValue[self.Val.NumericExpr.Value]
			if !ok {
				return "Invalid", nil
			}
			switch val.Dtype {
			case sutils.SS_DT_BOOL:
				return "Boolean", nil
			case sutils.SS_DT_SIGNED_NUM, sutils.SS_DT_UNSIGNED_NUM, sutils.SS_DT_FLOAT,
				sutils.SS_DT_SIGNED_32_NUM, sutils.SS_DT_USIGNED_32_NUM,
				sutils.SS_DT_SIGNED_16_NUM, sutils.SS_DT_USIGNED_16_NUM,
				sutils.SS_DT_SIGNED_8_NUM, sutils.SS_DT_USIGNED_8_NUM:
				return "Number", nil
			case sutils.SS_DT_STRING, sutils.SS_DT_STRING_SET, sutils.SS_DT_RAW_JSON:
				return "String", nil
			case sutils.SS_DT_BACKFILL:
				return "Null", nil
			default:
				return "Invalid", nil
			}
		} else {
			// Handle raw values directly based on expression type
			if self.Val.NumericExpr != nil {
				return "Number", nil
			} else if self.Val.StringExpr != nil {
				if sutils.IsBoolean(self.Val.StringExpr.RawString) {
					return "Boolean", nil
				}
				return "String", nil
			} else {
				return "Invalid", nil
			}
		}
	}
	if self.Op == "max" {
		if len(self.ValueList) == 0 {
			return "", fmt.Errorf("TextExpr.EvaluateText: no values provided for 'max' operation")
		}
		maxString := ""
		for _, expr := range self.ValueList {
			result, err := expr.Evaluate(fieldToValue)
			if utils.IsNonNilValueError(err) {
				return "", err
			}
			if result > maxString {
				maxString = result
			}
		}
		return maxString, nil

	} else if self.Op == "min" {
		if len(self.ValueList) == 0 {
			return "", fmt.Errorf("TextExpr.EvaluateText: no values provided for 'min' operation")
		}
		minString := ""
		for _, expr := range self.ValueList {
			result, err := expr.Evaluate(fieldToValue)
			if utils.IsNonNilValueError(err) {
				return "", err
			}
			if minString == "" || result < minString {
				minString = result
			}
		}
		return minString, nil

	} else if self.Op == "tostring" {
		valueStr, err := self.Val.EvaluateToString(fieldToValue)
		if err != nil {
			return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: failed to evaluate value for 'tostring' operation: %v", err)
		}
		if self.Param != nil {
			formatStr, err := self.Param.Evaluate(fieldToValue)
			if err != nil {
				return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: failed to evaluate format for 'tostring' operation: %v", err)
			}
			switch formatStr {
			case "hex":
				num, convErr := strconv.Atoi(valueStr)
				if convErr != nil {
					return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: failed to convert value '%s' to integer for hex formatting: %v", valueStr, convErr)
				}
				return fmt.Sprintf("%#x", num), nil
			case "commas":
				num, convErr := strconv.ParseFloat(valueStr, 64)
				if convErr != nil {
					return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: failed to convert value '%s' to float for comma formatting: %v", valueStr, convErr)
				}
				roundedNum := math.Round(num*100) / 100
				formattedNum := humanize.CommafWithDigits(roundedNum, 2)
				return formattedNum, nil
			case "duration":
				num, convErr := strconv.Atoi(valueStr)
				if convErr != nil {
					return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: failed to convert value '%s' to seconds for duration formatting: %v", valueStr, convErr)
				}
				hours := num / 3600
				minutes := (num % 3600) / 60
				seconds := num % 60
				return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds), nil
			default:
				return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: unsupported format '%s' for tostring operation", formatStr)
			}
		} else {
			return valueStr, nil
		}
	}
	cellValueStr, err := self.Param.Evaluate(fieldToValue)
	if err != nil {
		return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: can not evaluate text as a str: %v", err)
	}

	switch self.Op {
	case "lower":
		return strings.ToLower(cellValueStr), nil
	case "upper":
		return strings.ToUpper(cellValueStr), nil
	case "ltrim", "rtrim", "trim":
		return handleTrimFunctions(self.Op, cellValueStr, self.StrToRemove), nil
	case "urldecode":
		decodedStr, decodeErr := url.QueryUnescape(cellValueStr)
		if decodeErr != nil {
			return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: failed to decode URL: %v", decodeErr)
		}
		return decodedStr, nil
	case "substr":
		baseString, err := self.Param.Evaluate(fieldToValue)
		if err != nil {
			return "", err
		}
		startIndexFloat, err := self.StartIndex.Evaluate(fieldToValue)
		if err != nil {
			return "", err
		}
		startIndex := int(startIndexFloat)
		if startIndex > 0 {
			startIndex = startIndex - 1
		}
		if startIndex < 0 {
			startIndex = len(baseString) + startIndex
		}
		if startIndex < 0 || startIndex >= len(baseString) {
			return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: 'substr' start index is out of range")
		}
		substrLength := len(baseString) - startIndex
		if self.LengthExpr != nil {
			lengthFloat, err := self.LengthExpr.Evaluate(fieldToValue)
			if err != nil {
				return "", err
			}
			substrLength = int(lengthFloat)
			if substrLength < 0 || startIndex+substrLength > len(baseString) {
				return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: 'substr' length leads to out of range substring")
			}
		}
		endIndex := startIndex + substrLength
		if endIndex > len(baseString) {
			endIndex = len(baseString)
		}
		return baseString[startIndex:endIndex], nil

	default:
		return "", utils.WrapErrorf(err, "TextExpr.EvaluateText: unexpected operation: %v", self.Op)
	}
}

// In this case, if we can not evaluate numeric expr to a float, we should evaluate it as a str
func (self *ValueExpr) EvaluateValueExprAsString(fieldToValue map[string]sutils.CValueEnclosure) (string, error) {
	var str string
	var err error
	switch self.ValueExprMode {
	case VEMNumericExpr:
		floatValue, err := self.EvaluateToFloat(fieldToValue)
		str = fmt.Sprintf("%v", floatValue)
		if err != nil {
			str, err = self.EvaluateToString(fieldToValue)
			if err != nil {
				return "", utils.WrapErrorf(err, "ValueExpr.EvaluateValueExprAsString: can not evaluate VEMNumericExpr to string: %v", err)
			}
		}
	case VEMStringExpr:
		str, err = self.EvaluateToString(fieldToValue)
		if err != nil {
			return "", utils.WrapErrorf(err, "ValueExpr.EvaluateValueExprAsString: can not evaluate VEMStringExpr to string: %v", err)
		}
	}
	return str, nil
}

func handleCaseFunction(self *ConditionExpr, fieldToValue map[string]sutils.CValueEnclosure) (interface{}, error) {

	for _, cvPair := range self.ConditionValuePairs {
		res, err := cvPair.Condition.Evaluate(fieldToValue)
		if err != nil {
			nullFields, err2 := cvPair.Condition.GetNullFields(fieldToValue)
			if err2 == nil && len(nullFields) > 0 {
				continue
			}
			return "", utils.WrapErrorf(err, "handleCaseFunction: Error while evaluating condition, err: %v", err)
		}
		if res {
			val, err := cvPair.Value.EvaluateValueExpr(fieldToValue)
			if err != nil {
				return "", utils.WrapErrorf(err, "handleCaseFunction: Error while evaluating value, err: %v", err)
			}
			return val, nil
		}
	}

	return "", nil
}

func handleCoalesceFunction(self *ConditionExpr, fieldToValue map[string]sutils.CValueEnclosure) (interface{}, error) {
	for _, valueExpr := range self.ValueList {
		nullFields, err := valueExpr.GetNullFields(fieldToValue)
		if err != nil || len(nullFields) > 0 {
			continue
		}

		val, err := valueExpr.EvaluateValueExpr(fieldToValue)
		if err != nil {
			return "", utils.WrapErrorf(err, "handleCoalesceFunction: Error while evaluating value, err: %v", err)
		}
		return val, nil
	}

	return "", nil
}

func handleNullIfFunction(expr *ConditionExpr, fieldToValue map[string]sutils.CValueEnclosure) (interface{}, error) {
	if len(expr.ValueList) != 2 {
		return nil, fmt.Errorf("handleNullIfFunction: nullif requires exactly two arguments")
	}

	value1, err := expr.ValueList[0].EvaluateValueExpr(fieldToValue)
	if err != nil {
		return nil, utils.WrapErrorf(err, "handleNullIfFunction: Error while evaluating value1, err: %v", err)
	}

	value2, err := expr.ValueList[1].EvaluateValueExpr(fieldToValue)
	if err != nil {
		return nil, utils.WrapErrorf(err, "handleNullIfFunction: Error while evaluating value2, err: %v", err)
	}

	if value1 == value2 {
		return nil, nil
	}

	return value1, nil
}

// Field may come from BoolExpr or ValueExpr
func (expr *ConditionExpr) EvaluateCondition(fieldToValue map[string]sutils.CValueEnclosure) (interface{}, error) {

	switch expr.Op {
	case "if":
		predicateFlag, err := expr.BoolExpr.Evaluate(fieldToValue)
		if err != nil {
			return "", utils.WrapErrorf(err, "ConditionExpr.EvaluateCondition cannot evaluate BoolExpr: %v", err)
		}

		var trueValue interface{}
		var falseValue interface{}

		if predicateFlag {
			trueValue, err = expr.TrueValue.EvaluateValueExpr(fieldToValue)
			if err != nil {
				return "", utils.WrapErrorf(err, "ConditionExpr.EvaluateCondition: can not evaluate trueValue. ValueExpr: %v", err)
			}

			return trueValue, nil
		} else {
			falseValue, err = expr.FalseValue.EvaluateValueExpr(fieldToValue)
			if err != nil {
				return "", utils.WrapErrorf(err, "ConditionExpr.EvaluateCondition: can not evaluate falseValue. ValueExpr: %v", err)
			}

			return falseValue, nil
		}
	case "validate":
		return handleComparisonAndConditionalFunctions(expr, fieldToValue, expr.Op)
	case "case":
		return handleCaseFunction(expr, fieldToValue)
	case "coalesce":
		return handleCoalesceFunction(expr, fieldToValue)
	case "nullif":
		return handleNullIfFunction(expr, fieldToValue)
	case "null":
		return nil, nil
	default:
		return "", fmt.Errorf("ConditionExpr.EvaluateCondition: unsupported operation: %v", expr.Op)
	}

}

func (self *TextExpr) GetFields() []string {
	if self == nil {
		return nil
	}
	fields := make([]string, 0)
	if self.IsTerminal || (self.Op != "max" && self.Op != "min" && self.Op != "printf") {
		if self.Param != nil {
			fields = append(fields, self.Param.GetFields()...)
		}
		if self.Val != nil {
			fields = append(fields, self.Val.GetFields()...)
		}
		if self.Delimiter != nil {
			fields = append(fields, self.Delimiter.GetFields()...)
		}
		if self.StartIndex != nil {
			fields = append(fields, self.StartIndex.GetFields()...)
		}
		if self.LengthExpr != nil {
			fields = append(fields, self.LengthExpr.GetFields()...)
		}
		if self.MultiValueExpr != nil {
			fields = append(fields, self.MultiValueExpr.GetFields()...)
		}
		return fields
	}
	for _, expr := range self.ValueList {
		fields = append(fields, expr.GetFields()...)
	}
	return fields

}

// Append all the fields in ConditionExpr
func (self *ConditionExpr) GetFields() []string {
	if self == nil {
		return nil
	}
	fields := make([]string, 0)
	if self.BoolExpr != nil {
		fields = append(fields, self.BoolExpr.GetFields()...)
	}
	if self.TrueValue != nil {
		fields = append(fields, self.TrueValue.GetFields()...)
	}
	if self.FalseValue != nil {
		fields = append(fields, self.FalseValue.GetFields()...)
	}
	for _, pair := range self.ConditionValuePairs {
		fields = append(fields, pair.Condition.GetFields()...)
	}
	for _, valueExpr := range self.ValueList {
		fields = append(fields, valueExpr.GetFields()...)
	}
	return fields
}

// Specifying a value and a precision
func round(number float64, precision int) float64 {
	scale := math.Pow10(precision)
	return math.Round(number*scale) / scale
}

func (self *NumericExpr) GetFields() []string {
	if self == nil {
		return nil
	}

	if self.IsFieldsPopulated {
		return self.Fields
	}

	fields := make([]string, 0)
	if self.Val != nil {
		self.Fields = append(fields, self.Val.GetFields()...)
		self.IsFieldsPopulated = true
		return self.Fields
	}
	if self.IsTerminal {
		if self.Op == "now" {
			self.IsFieldsPopulated = true
			self.Fields = fields
			return self.Fields
		}
		if self.ValueIsField {
			self.IsFieldsPopulated = true
			self.Fields = []string{self.Value}
			return self.Fields
		} else {
			self.IsFieldsPopulated = true
			self.Fields = []string{}
			return self.Fields
		}
	} else if self.Right != nil {
		self.IsFieldsPopulated = true
		self.Fields = append(self.Left.GetFields(), self.Right.GetFields()...)
		return self.Fields
	} else {
		self.IsFieldsPopulated = true
		self.Fields = self.Left.GetFields()
		return self.Fields
	}
}

func getValueAsString(fieldToValue map[string]sutils.CValueEnclosure, field string) (string, error) {
	enclosure, ok := fieldToValue[field]
	if !ok {
		return "", utils.NewErrorWithCode(utils.NIL_VALUE_ERR, fmt.Errorf("getValueAsString: Missing field %v", field))
	}

	return enclosure.GetString()
}

func getValueAsFloat(fieldToValue map[string]sutils.CValueEnclosure, field string) (float64, error) {
	enclosure, ok := fieldToValue[field]
	if !ok {
		return 0, ErrWithCodeFloatMissingField
	}

	if enclosure.IsNull() {
		return 0, ErrWithCodeFieldNull
	}

	if value, err := enclosure.GetFloatValue(); err == nil {
		return value, nil
	}

	// Check if the string value is a number.
	if enclosure.Dtype == sutils.SS_DT_STRING {

		value, err := utils.FastParseFloat([]byte(enclosure.CVal.(string)))
		if err == nil {
			return value, nil
		}
	}

	return 0, ErrWithCodeConversionErr
}

func (self *SortValue) Compare(other *SortValue) (int, error) {
	switch self.InterpretAs {
	case "ip":
		selfIP := net.ParseIP(self.Val)
		otherIP := net.ParseIP(other.Val)
		if selfIP == nil || otherIP == nil {
			return 0, fmt.Errorf("SortValue.Compare: cannot parse IP address selfIp: %v, otherIp: %v", self.Val, other.Val)
		}
		return bytes.Compare(selfIP, otherIP), nil
	case "num":
		selfFloat, selfErr := strconv.ParseFloat(self.Val, 64)
		otherFloat, otherErr := strconv.ParseFloat(other.Val, 64)
		if selfErr != nil || otherErr != nil {
			return 0, fmt.Errorf("SortValue.Compare: cannot parse %v and %v as float", self.Val, other.Val)
		}

		if selfFloat == otherFloat {
			return 0, nil
		} else if selfFloat < otherFloat {
			return -1, nil
		} else {
			return 1, nil
		}
	case "str":
		return strings.Compare(self.Val, other.Val), nil
	case "auto", "":
		selfFloat, selfErr := strconv.ParseFloat(self.Val, 64)
		otherFloat, otherErr := strconv.ParseFloat(other.Val, 64)
		if selfErr == nil && otherErr == nil {
			if selfFloat == otherFloat {
				return 0, nil
			} else if selfFloat < otherFloat {
				return -1, nil
			} else {
				return 1, nil
			}
		}

		selfIp := net.ParseIP(self.Val)
		otherIp := net.ParseIP(other.Val)
		if selfIp != nil && otherIp != nil {
			return bytes.Compare(selfIp, otherIp), nil
		}

		return strings.Compare(self.Val, other.Val), nil
	default:
		return 0, fmt.Errorf("SortValue.Compare: invalid InterpretAs value: %v", self.InterpretAs)
	}
}

// The `ascending` slice should have the same length as `a` and `b`. Moreover,
// each element of `ascending` should be either +1 or -1; +1 means higher
// values get sorted higher, and -1 means lower values get sorted higher.
func CompareSortValueSlices(a []SortValue, b []SortValue, ascending []int) (int, error) {
	if len(a) != len(b) || len(a) != len(ascending) {
		return 0, fmt.Errorf("CompareSortValueSlices: slices have different lengths")
	}

	for i := 0; i < len(a); i++ {
		comp, err := a[i].Compare(&b[i])
		if err != nil {
			return 0, fmt.Errorf("CompareSortValueSlices err: %v", err)
		}

		if comp != 0 {
			return comp * ascending[i], nil
		}
	}

	return 0, nil
}

func GetDefaultTimechartSpanOptions(startEpoch, endEpoch uint64, qid uint64) (*SpanOptions, error) {
	if startEpoch == 0 || endEpoch == 0 {
		err := fmt.Errorf("GetDefaultTimechartSpanOptions: startEpoch/endEpoch is not set. Given startEpoch=%v, endEpoch=%v", startEpoch, endEpoch)
		return nil, err
	}

	duration := endEpoch - startEpoch

	// 15 minutes
	if duration <= 15*60*1000 {
		return &SpanOptions{SpanLength: &SpanLength{Num: 10, TimeScalr: sutils.TMSecond}}, nil
	} else if duration <= 60*60*1000 {
		return &SpanOptions{SpanLength: &SpanLength{Num: 1, TimeScalr: sutils.TMMinute}}, nil
	} else if duration <= 4*60*60*1000 {
		return &SpanOptions{SpanLength: &SpanLength{Num: 5, TimeScalr: sutils.TMMinute}}, nil
	} else if duration <= 24*60*60*1000 {
		return &SpanOptions{SpanLength: &SpanLength{Num: 30, TimeScalr: sutils.TMMinute}}, nil
	} else if duration <= 7*24*60*60*1000 {
		return &SpanOptions{SpanLength: &SpanLength{Num: 1, TimeScalr: sutils.TMHour}}, nil
	} else if duration <= 180*24*60*60*1000 {
		return &SpanOptions{SpanLength: &SpanLength{Num: 1, TimeScalr: sutils.TMDay}}, nil
	} else {
		return &SpanOptions{SpanLength: &SpanLength{Num: 1, TimeScalr: sutils.TMMonth}}, nil
	}
}
