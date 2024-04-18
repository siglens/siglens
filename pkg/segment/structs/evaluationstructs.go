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
	"fmt"
	"math"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/axiomhq/hyperloglog"
	"github.com/dustin/go-humanize"

	"github.com/siglens/siglens/pkg/segment/utils"
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

type RenameExpr struct {
	RenameExprMode  RenameExprMode
	OriginalPattern string
	NewPattern      string
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
	SortEles []*SortElement
	Limit    uint64

	SortAscending         []int
	SortRecords           map[string]map[string]interface{}
	NumProcessedSegments  uint64
	processedSegmentsLock sync.Mutex
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

	FloatValue    float64
	NumericExpr   *NumericExpr
	StringExpr    *StringExpr
	ConditionExpr *ConditionExpr
	BooleanExpr   *BoolExpr
}

type ConcatExpr struct {
	Atoms []*ConcatAtom
}

type ConcatAtom struct {
	IsField  bool
	Value    string
	TextExpr *TextExpr
}

type NumericExpr struct {
	NumericExprMode NumericExprMode

	IsTerminal bool

	// Only used when IsTerminal is true.
	ValueIsField bool
	Value        string

	// Only used when IsTerminal is false.
	Op    string // Either +, -, /, *, abs, ceil, round, sqrt, len
	Left  *NumericExpr
	Right *NumericExpr
	Val   *StringExpr
}

type StringExpr struct {
	StringExprMode StringExprMode
	RawString      string      // only used when mode is RawString
	FieldName      string      // only used when mode is Field
	ConcatExpr     *ConcatExpr // only used when mode is Concat
	TextExpr       *TextExpr   // only used when mode is TextExpr
}

type TextExpr struct {
	IsTerminal   bool
	Op           string //lower, ltrim, rtrim
	Value        *StringExpr
	StrToRemove  string
	Delimiter    *StringExpr
	MaxMinValues []*StringExpr
	StartIndex   *NumericExpr
	LengthExpr   *NumericExpr
	Val          *ValueExpr
	Format       *StringExpr
}

type ConditionExpr struct {
	Op         string //if
	BoolExpr   *BoolExpr
	TrueValue  *ValueExpr //if bool expr is true, take this value
	FalseValue *ValueExpr
}

type TimechartExpr struct {
	TcOptions  *TcOptions
	BinOptions *BinOptions
	SingleAgg  *SingleAgg
	ByField    string // group by this field inside each time range bucket (timechart)
	LimitExpr  *LimitExpr
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

type BinOptions struct {
	SpanOptions *SpanOptions
}

type SpanOptions struct {
	DefaultSettings bool
	SpanLength      *SpanLength
}

type SpanLength struct {
	Num       int
	TimeScalr utils.TimeUnit
}

type SplitByClause struct {
	Field     string
	TcOptions *TcOptions
	// Where clause: to be finished
}

// This structure is used to store values which are not within limit. And These values will be merged into the 'other' category.
type TMLimitResult struct {
	ValIsInLimit     map[string]bool
	GroupValScoreMap map[string]*utils.CValueEnclosure
	Hll              *hyperloglog.Sketch
	StrSet           map[string]struct{}
	OtherCValArr     []*utils.CValueEnclosure
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
	VEMNumericExpr   = iota // Only NumricExpr is valid
	VEMStringExpr           // Only StringExpr is valid
	VEMConditionExpr        // Only ConditionExpr is valud
	VEMBooleanExpr          // Only BooleanExpr is valid
)

type StringExprMode uint8

const (
	SEMRawString  = iota // only used when mode is RawString
	SEMField             // only used when mode is Field
	SEMConcatExpr        // only used when mode is Concat
	SEMTextExpr          // only used when mode is TextExpr
)

type NumericExprMode uint8

const (
	NEMNumber      = iota // only used when mode is a Number
	NEMLenString          // only used when mode is a str (used for len())
	NEMNumberField        // only used when mode is Field (Field can be evaluated to a float)
	NEMLenField           // only used when mode is Field (Field can not be evaluated to a float, used for len())
	NEMNumericExpr        // only used when mode is a NumericExpr
)

func (self *DedupExpr) AcquireProcessedSegmentsLock() {
	self.processedSegmentsLock.Lock()
}

func (self *DedupExpr) ReleaseProcessedSegmentsLock() {
	self.processedSegmentsLock.Unlock()
}

func (self *SortExpr) AcquireProcessedSegmentsLock() {
	self.processedSegmentsLock.Lock()
}

func (self *SortExpr) ReleaseProcessedSegmentsLock() {
	self.processedSegmentsLock.Unlock()
}

// Evaluate this BoolExpr to a boolean, replacing each field in the expression
// with the value specified by fieldToValue. Each field listed by GetFields()
// must be in fieldToValue.
func (self *BoolExpr) Evaluate(fieldToValue map[string]utils.CValueEnclosure) (bool, error) {
	if self.IsTerminal {
		if self.ValueOp == "in" {
			inFlag, err := isInValueList(fieldToValue, self.LeftValue, self.ValueList)
			if err != nil {
				return false, fmt.Errorf("BoolExpr.Evaluate: can not evaluate Eval In function: %v", err)
			}
			return inFlag, err
		} else if self.ValueOp == "isbool" {
			val, err := self.LeftValue.EvaluateToString(fieldToValue)
			if err != nil {
				return false, fmt.Errorf("isbool: can not evaluate to String: %v", err)
			}
			isBool := strings.ToLower(val) == "true" || strings.ToLower(val) == "false" || val == "0" || val == "1"
			return isBool, nil

		} else if self.ValueOp == "isint" {
			val, err := self.LeftValue.EvaluateToString(fieldToValue)
			if err != nil {
				return false, err
			}

			_, parseErr := strconv.Atoi(val)
			return parseErr == nil, nil
		} else if self.ValueOp == "isstr" {
			_, floatErr := self.LeftValue.EvaluateToFloat(fieldToValue)

			if floatErr == nil {
				return false, nil
			}

			_, strErr := self.LeftValue.EvaluateToString(fieldToValue)
			return strErr == nil, nil
		} else if self.ValueOp == "isnull" {
			// Get the fields associated with this expression
			fields := self.GetFields()
			if len(fields) == 0 {
				return false, fmt.Errorf("BoolExpr.Evaluate: No fields found for isnull operation")
			}

			// Check the first field's value in the fieldToValue map
			value, exists := fieldToValue[fields[0]]
			if !exists {
				return false, fmt.Errorf("BoolExpr.Evaluate: Field '%s' not found in data", fields[0])
			}
			// Check if the value's Dtype is SS_DT_BACKFILL
			if value.Dtype == utils.SS_DT_BACKFILL {
				return true, nil
			}
			return false, nil
		} else if self.ValueOp == "like" {
			leftStr, errLeftStr := self.LeftValue.EvaluateToString(fieldToValue)
			if errLeftStr != nil {
				return false, fmt.Errorf("BoolExpr.Evaluate: error evaluating left side of LIKE to string: %v", errLeftStr)
			}

			rightStr, errRightStr := self.RightValue.EvaluateToString(fieldToValue)
			if errRightStr != nil {
				return false, fmt.Errorf("BoolExpr.Evaluate: error evaluating right side of LIKE to string: %v", errRightStr)
			}

			regexPattern := strings.Replace(strings.Replace(regexp.QuoteMeta(rightStr), "%", ".*", -1), "_", ".", -1)
			matched, err := regexp.MatchString("^"+regexPattern+"$", leftStr)
			if err != nil {
				return false, fmt.Errorf("BoolExpr.Evaluate: regex error in LIKE operation: %v", err)
			}
			return matched, nil
		} else if self.ValueOp == "match" {
			leftStr, errLeftStr := self.LeftValue.EvaluateToString(fieldToValue)
			if errLeftStr != nil {
				return false, fmt.Errorf("BoolExpr.Evaluate: error evaluating left side of MATCH to string: %v", errLeftStr)
			}

			rightStr, errRightStr := self.RightValue.EvaluateToString(fieldToValue)
			if errRightStr != nil {
				return false, fmt.Errorf("BoolExpr.Evaluate: error evaluating right side of MATCH to string: %v", errRightStr)
			}

			matched, err := regexp.MatchString(rightStr, leftStr)
			if err != nil {
				return false, fmt.Errorf("BoolExpr.Evaluate: regex error in MATCH operation: %v", err)
			}
			return matched, nil

		} else if self.ValueOp == "cidrmatch" {
			cidrStr, errCidr := self.LeftValue.EvaluateToString(fieldToValue)
			ipStr, errIp := self.RightValue.EvaluateToString(fieldToValue)
			if errCidr != nil || errIp != nil {
				return false, fmt.Errorf("cidrmatch: error evaluating arguments: %v, %v", errCidr, errIp)
			}

			match, err := isIPInCIDR(cidrStr, ipStr)
			if err != nil {
				return false, fmt.Errorf("cidrmatch: error in matching CIDR: %v", err)
			}
			return match, nil
		}

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
				return false, fmt.Errorf("BoolExpr.Evaluate: invalid ValueOp %v for floats", self.ValueOp)
			}
		} else if errLeftStr == nil && errRightStr == nil {
			switch self.ValueOp {
			case "=":
				return leftStr == rightStr, nil
			case "!=":

				return leftStr != rightStr, nil
			default:
				return false, fmt.Errorf("BoolExpr.Evaluate: invalid ValueOp %v for strings", self.ValueOp)
			}
		} else {
			if errLeftStr != nil && errLeftFloat != nil {
				return false, fmt.Errorf("BoolExpr.Evaluate: left cannot be evaluated to a string or float")
			}
			if errRightStr != nil && errRightFloat != nil {
				return false, fmt.Errorf("BoolExpr.Evaluate: right cannot be evaluated to a string or float")
			}

			return false, fmt.Errorf("BoolExpr.Evaluate: left and right ValueExpr have different types")
		}
	} else { // IsTerminal is false
		left, err := self.LeftBool.Evaluate(fieldToValue)
		if err != nil {
			return false, err
		}

		var right bool
		if self.RightBool != nil {
			var err error
			right, err = self.RightBool.Evaluate(fieldToValue)
			if err != nil {
				return false, err
			}
		}

		switch self.BoolOp {
		case BoolOpNot:
			return !left, nil
		case BoolOpAnd:
			return left && right, nil
		case BoolOpOr:
			return left || right, nil
		default:
			return false, fmt.Errorf("invalid BoolOp: %v", self.BoolOp)
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
		return false, fmt.Errorf("invalid IP address")
	}

	return cidrNet.Contains(ip), nil
}

func isInValueList(fieldToValue map[string]utils.CValueEnclosure, value *ValueExpr, valueList []*ValueExpr) (bool, error) {
	valueStr, err := value.EvaluateToString(fieldToValue)
	if err != nil {
		return false, fmt.Errorf("isInValueList: can not evaluate to String: %v", err)
	}

	for _, atom := range valueList {
		atomStr, err := atom.EvaluateToString(fieldToValue)
		if err != nil {
			return false, fmt.Errorf("isInValueList: can not evaluate to String: %v", err)
		}

		if atomStr == valueStr {
			return true, nil
		}
	}

	return false, nil
}

func (self *BoolExpr) GetFields() []string {
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

// Try evaluating this ValueExpr to a string value, replacing each field in the
// expression with the value specified by fieldToValue. Each field listed by
// GetFields() must be in fieldToValue.
//
// A ValueExpr can be evaluated to a string or float, so if this fails you may
// want to call ValueExpr.EvaluateToFloat().
func (self *ValueExpr) EvaluateToString(fieldToValue map[string]utils.CValueEnclosure) (string, error) {
	switch self.ValueExprMode {
	case VEMStringExpr:
		str, err := self.StringExpr.Evaluate(fieldToValue)
		if err != nil {
			return "", fmt.Errorf("ValueExpr.EvaluateToString: cannot evaluate to string %v", err)
		}
		return str, nil
	//In this case, field name will be recognized as part of NumericExpr at first. It it can not be converted to float64, it should be evaluated as a str
	case VEMNumericExpr:
		floatValue, err := self.NumericExpr.Evaluate(fieldToValue)
		if err != nil {
			//Because parsing is successful and it can not evaluate as a float in here,
			//There is one possibility: field name may not be float
			str, err := getValueAsString(fieldToValue, self.NumericExpr.Value)

			if err == nil {
				return str, nil
			}

			return "", fmt.Errorf("ValueExpr.EvaluateToString: cannot evaluate to float64 or string: %v", err)
		}
		return strconv.FormatFloat(floatValue, 'f', -1, 64), nil
	case VEMConditionExpr:
		str, err := self.ConditionExpr.EvaluateCondition(fieldToValue)
		if err != nil {
			return "", fmt.Errorf("ValueExpr.EvaluateToString: cannot evaluate to string %v", err)
		}
		return str, nil
	case VEMBooleanExpr:
		boolResult, err := self.BooleanExpr.Evaluate(fieldToValue)
		if err != nil {
			return "", err
		}
		return strconv.FormatBool(boolResult), nil
	default:
		return "", fmt.Errorf("ValueExpr.EvaluateToString: cannot evaluate to string")
	}
}

func (self *StringExpr) Evaluate(fieldToValue map[string]utils.CValueEnclosure) (string, error) {
	switch self.StringExprMode {
	case SEMRawString:
		return self.RawString, nil
	case SEMField:
		if floatValue, err := getValueAsFloat(fieldToValue, self.FieldName); err == nil {
			return strconv.FormatFloat(floatValue, 'f', -1, 64), nil
		}

		if str, err := getValueAsString(fieldToValue, self.FieldName); err == nil {
			return str, nil
		}
		return "", fmt.Errorf("StringExpr.Evaluate: cannot evaluate to field")
	case SEMConcatExpr:
		return self.ConcatExpr.Evaluate(fieldToValue)
	case SEMTextExpr:
		return self.TextExpr.EvaluateText(fieldToValue)
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
func (self *ValueExpr) EvaluateToFloat(fieldToValue map[string]utils.CValueEnclosure) (float64, error) {
	switch self.ValueExprMode {
	case VEMNumericExpr:
		return self.NumericExpr.Evaluate(fieldToValue)
	default:
		return 0, fmt.Errorf("ValueExpr.EvaluateToFloat: cannot evaluate to float")
	}
}

func (self *ValueExpr) GetFields() []string {
	switch self.ValueExprMode {
	case VEMNumericExpr:
		return self.NumericExpr.GetFields()
	case VEMStringExpr:
		return self.StringExpr.GetFields()
	case VEMConditionExpr:
		return self.ConditionExpr.GetFields()
	case VEMBooleanExpr:
		return self.BooleanExpr.GetFields()
	default:
		return []string{}
	}
}

func (self *RexExpr) GetFields() []string {
	var fields []string
	fields = append(fields, self.FieldName)
	return fields
}

func (self *RenameExpr) GetFields() []string {
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
	switch self.StringExprMode {
	case SEMConcatExpr:
		return self.ConcatExpr.GetFields()
	case SEMTextExpr:
		return self.TextExpr.GetFields()
	case SEMField:
		return []string{self.FieldName}
	default:
		return []string{}
	}
}

// Concatenate all the atoms in this ConcatExpr, replacing all fields with the
// values specified by fieldToValue. Each field listed by GetFields() must be in
// fieldToValue.
func (self *ConcatExpr) Evaluate(fieldToValue map[string]utils.CValueEnclosure) (string, error) {
	result := ""
	for _, atom := range self.Atoms {
		if atom.IsField {
			value, err := getValueAsString(fieldToValue, atom.Value)
			if err != nil {
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
	case []string:
		return bucketKey[keyIndex]
	case string:
		return bucketKey
	default:
		return ""
	}
}

func (self *StatisticExpr) OverrideGroupByCol(bucketResult *BucketResult, resTotal uint64) error {

	cellValueStr := ""
	for keyIndex, groupByCol := range bucketResult.GroupByKeys {
		if !self.StatisticOptions.ShowCount || !self.StatisticOptions.ShowPerc || (self.StatisticOptions.CountField != groupByCol && self.StatisticOptions.PercentField != groupByCol) {
			continue
		}

		if self.StatisticOptions.ShowCount && self.StatisticOptions.CountField == groupByCol {
			cellValueStr = strconv.FormatUint(bucketResult.ElemCount, 10)
		}

		if self.StatisticOptions.ShowPerc && self.StatisticOptions.PercentField == groupByCol {
			percent := float64(bucketResult.ElemCount) / float64(resTotal) * 100
			cellValueStr = fmt.Sprintf("%.6f", percent)
		}

		// Set the appropriate element of BucketKey to cellValueStr.
		switch bucketKey := bucketResult.BucketKey.(type) {
		case []string:
			bucketKey[keyIndex] = cellValueStr
			bucketResult.BucketKey = bucketKey
		case string:
			if keyIndex != 0 {
				return fmt.Errorf("OverrideGroupByCol: expected keyIndex to be 0, not %v", keyIndex)
			}
			bucketResult.BucketKey = cellValueStr
		default:
			return fmt.Errorf("OverrideGroupByCol: bucket key has unexpected type: %T", bucketKey)
		}
	}
	return nil
}

func (self *StatisticExpr) SetCountToStatRes(statRes map[string]utils.CValueEnclosure, elemCount uint64) {
	statRes[self.StatisticOptions.CountField] = utils.CValueEnclosure{
		Dtype: utils.SS_DT_UNSIGNED_NUM,
		CVal:  elemCount,
	}
}

func (self *StatisticExpr) SetPercToStatRes(statRes map[string]utils.CValueEnclosure, elemCount uint64, resTotal uint64) {
	percent := float64(elemCount) / float64(resTotal) * 100
	statRes[self.StatisticOptions.PercentField] = utils.CValueEnclosure{
		Dtype: utils.SS_DT_STRING,
		CVal:  fmt.Sprintf("%.6f", percent),
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
			return fmt.Errorf("SortBucketResult: cannot convert %v to int", self.Limit)
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
	groupByCols := self.GetGroupByCols()
	groupByKeys := make([]string, 0)
	bucketKey := make([]string, 0)
	switch bucketResult.BucketKey.(type) {
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
							return fmt.Errorf("RemoveFieldsNotInExpr: %v", err)
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
		return fmt.Errorf("RemoveFieldsNotInExpr: bucket key has unexpected type: %T", bucketKey)
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

func (self *StatisticExpr) GetGroupByCols() []string {
	return append(self.FieldList, self.ByClause...)
}

func (self *RenameExpr) Evaluate(fieldToValue map[string]utils.CValueEnclosure, fieldName string) (string, error) {
	return getValueAsString(fieldToValue, fieldName)
}

func (self *RexExpr) Evaluate(fieldToValue map[string]utils.CValueEnclosure, rexExp *regexp.Regexp) (map[string]string, error) {

	fieldValue, err := getValueAsString(fieldToValue, self.FieldName)
	if err != nil {
		return nil, fmt.Errorf("RexExpr.Evaluate: %v", err)
	}

	return MatchAndExtractGroups(fieldValue, rexExp)
}

func MatchAndExtractGroups(str string, rexExp *regexp.Regexp) (map[string]string, error) {
	match := rexExp.FindStringSubmatch(str)
	if len(match) == 0 {
		return nil, fmt.Errorf("MatchAndExtractGroups: no str in field match the pattern")
	}
	if len(rexExp.SubexpNames()) == 0 {
		return nil, fmt.Errorf("MatchAndExtractGroups: no field create from the pattern")
	}

	result := make(map[string]string)
	for i, name := range rexExp.SubexpNames() {
		if i != 0 && name != "" {
			result[name] = match[i]
		}
	}

	return result, nil
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
		return "", fmt.Errorf("ProcessRenameRegexExpression: There are some errors in the pattern: %v", err)
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

// Evaluate this NumericExpr to a float, replacing each field in the expression
// with the value specified by fieldToValue. Each field listed by GetFields()
// must be in fieldToValue.
func (self *NumericExpr) Evaluate(fieldToValue map[string]utils.CValueEnclosure) (float64, error) {
	if self.Op == "now" {
		timestamp := time.Now().Unix()
		return float64(timestamp), nil
	}
	if self.IsTerminal {
		if self.ValueIsField {
			switch self.NumericExprMode {
			case NEMNumberField:
				return getValueAsFloat(fieldToValue, self.Value)
			case NEMLenField:
				return float64(len(fieldToValue[self.Value].CVal.(string))), nil
			}
		} else {
			switch self.NumericExprMode {
			case NEMNumber:
				value, err := strconv.ParseFloat(self.Value, 64)
				if err != nil {
					return 0, fmt.Errorf("NumericExpr.Evaluate: cannot convert %v to float", self.Value)
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
		case "abs":
			return math.Abs(left), nil
		case "ceil":
			return math.Ceil(left), nil
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
				return 0, fmt.Errorf("NumericExpr.Evaluate: Error in tonumber operation: %v", err)
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
				return 0, fmt.Errorf("NumericExpr.Evaluate: cannot convert '%v' to number with base %d", strValue, base)
			}
			return float64(number), nil

		default:
			return 0, fmt.Errorf("NumericExpr.Evaluate: unexpected operation: %v", self.Op)
		}
	}
}

func (self *TextExpr) EvaluateText(fieldToValue map[string]utils.CValueEnclosure) (string, error) {
	if self.Op == "max" {
		if len(self.MaxMinValues) == 0 {
			return "", fmt.Errorf("TextExpr.Evaluate: no values provided for 'max' operation")
		}
		maxString := ""
		for _, expr := range self.MaxMinValues {
			result, err := expr.Evaluate(fieldToValue)
			if err != nil {
				return "", err
			}
			if result > maxString {
				maxString = result
			}
		}
		return maxString, nil

	} else if self.Op == "min" {
		if len(self.MaxMinValues) == 0 {
			return "", fmt.Errorf("TextExpr.Evaluate: no values provided for 'min' operation")
		}
		minString := ""
		for _, expr := range self.MaxMinValues {
			result, err := expr.Evaluate(fieldToValue)
			if err != nil {
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
			return "", fmt.Errorf("TextExpr.Evaluate: failed to evaluate value for 'tostring' operation: %v", err)
		}
		if self.Format != nil {
			formatStr, err := self.Format.Evaluate(fieldToValue)
			if err != nil {
				return "", fmt.Errorf("TextExpr.Evaluate: failed to evaluate format for 'tostring' operation: %v", err)
			}
			switch formatStr {
			case "hex":
				num, convErr := strconv.Atoi(valueStr)
				if convErr != nil {
					return "", fmt.Errorf("TextExpr.Evaluate: failed to convert value '%s' to integer for hex formatting: %v", valueStr, convErr)
				}
				return fmt.Sprintf("%#x", num), nil
			case "commas":
				num, convErr := strconv.ParseFloat(valueStr, 64)
				if convErr != nil {
					return "", fmt.Errorf("TextExpr.Evaluate: failed to convert value '%s' to float for comma formatting: %v", valueStr, convErr)
				}
				roundedNum := math.Round(num*100) / 100
				formattedNum := humanize.CommafWithDigits(roundedNum, 2)
				return formattedNum, nil
			case "duration":
				num, convErr := strconv.Atoi(valueStr)
				if convErr != nil {
					return "", fmt.Errorf("TextExpr.Evaluate: failed to convert value '%s' to seconds for duration formatting: %v", valueStr, convErr)
				}
				hours := num / 3600
				minutes := (num % 3600) / 60
				seconds := num % 60
				return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds), nil
			default:
				return "", fmt.Errorf("TextExpr.Evaluate: unsupported format '%s' for tostring operation", formatStr)
			}
		} else {
			return valueStr, nil
		}
	}
	cellValueStr, err := self.Value.Evaluate(fieldToValue)
	if err != nil {
		return "", fmt.Errorf("TextExpr.Evaluate: can not evaluate text as a str: %v", err)
	}

	switch self.Op {
	case "lower":
		return strings.ToLower(cellValueStr), nil
	case "ltrim":
		return strings.TrimLeft(cellValueStr, self.StrToRemove), nil
	case "rtrim":
		return strings.TrimRight(cellValueStr, self.StrToRemove), nil
	case "urldecode":
		decodedStr, decodeErr := url.QueryUnescape(cellValueStr)
		if decodeErr != nil {
			return "", fmt.Errorf("TextExpr.Evaluate: failed to decode URL: %v", decodeErr)
		}
		return decodedStr, nil

	case "split":
		delimiterStr, err := self.Delimiter.Evaluate(fieldToValue)
		if err != nil {
			return "", fmt.Errorf("TextExpr.Evaluate: cannot evaluate delimiter as a string: %v", err)
		}

		return strings.Join(strings.Split(cellValueStr, delimiterStr), "&nbsp"), nil

	case "substr":
		baseString, err := self.Value.Evaluate(fieldToValue)
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
			return "", fmt.Errorf("substr: start index is out of range")
		}
		substrLength := len(baseString) - startIndex
		if self.LengthExpr != nil {
			lengthFloat, err := self.LengthExpr.Evaluate(fieldToValue)
			if err != nil {
				return "", err
			}
			substrLength = int(lengthFloat)
			if substrLength < 0 || startIndex+substrLength > len(baseString) {
				return "", fmt.Errorf("substr: length leads to out of range substring")
			}
		}
		endIndex := startIndex + substrLength
		if endIndex > len(baseString) {
			endIndex = len(baseString)
		}
		return baseString[startIndex:endIndex], nil

	default:
		return "", fmt.Errorf("TextExpr.Evaluate: unexpected operation: %v", self.Op)
	}
}

// In this case, if we can not evaluate numeric expr to a float, we should evaluate it as a str
func (self *ValueExpr) EvaluateValueExprAsString(fieldToValue map[string]utils.CValueEnclosure) (string, error) {
	var str string
	var err error
	switch self.ValueExprMode {
	case VEMNumericExpr:
		floatValue, err := self.EvaluateToFloat(fieldToValue)
		str = fmt.Sprintf("%v", floatValue)
		if err != nil {
			str, err = self.EvaluateToString(fieldToValue)
			if err != nil {
				return "", fmt.Errorf("ConditionExpr.Evaluate: can not evaluate to a ValueExpr: %v", err)
			}
		}
	case VEMStringExpr:
		str, err = self.EvaluateToString(fieldToValue)
		if err != nil {
			return "", fmt.Errorf("ConditionExpr.Evaluate: can not evaluate to a ValueExpr: %v", err)
		}
	}
	return str, nil
}

// Field may come from BoolExpr or ValueExpr
func (self *ConditionExpr) EvaluateCondition(fieldToValue map[string]utils.CValueEnclosure) (string, error) {
	predicateFlag, err := self.BoolExpr.Evaluate(fieldToValue)
	if err != nil {
		return "", fmt.Errorf("ConditionExpr.Evaluate: %v", err)
	}

	trueValue, err := self.TrueValue.EvaluateValueExprAsString(fieldToValue)
	if err != nil {
		return "", fmt.Errorf("ConditionExpr.Evaluate: can not evaluate trueValue to a ValueExpr: %v", err)
	}
	falseValue, err := self.FalseValue.EvaluateValueExprAsString(fieldToValue)
	if err != nil {
		return "", fmt.Errorf("ConditionExpr.Evaluate: can not evaluate falseValue to a ValueExpr: %v", err)
	}

	switch self.Op {
	case "if":
		if predicateFlag {
			return trueValue, nil
		} else {
			return falseValue, nil
		}
	default:
		return "", fmt.Errorf("ConditionExpr.Evaluate: unexpected operation: %v", self.Op)
	}

}

func (self *TextExpr) GetFields() []string {
	fields := make([]string, 0)
	if self.IsTerminal || (self.Op != "max" && self.Op != "min") {
		if self.Value != nil {
			fields = append(fields, self.Value.GetFields()...)
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
		if self.Format != nil {
			fields = append(fields, self.Format.GetFields()...)
		}
		return fields
	}
	for _, expr := range self.MaxMinValues {
		fields = append(fields, expr.GetFields()...)
	}
	return fields

}

// Append all the fields in ConditionExpr
func (self *ConditionExpr) GetFields() []string {
	fields := make([]string, 0)
	fields = append(fields, self.BoolExpr.GetFields()...)
	fields = append(fields, self.TrueValue.GetFields()...)
	fields = append(fields, self.FalseValue.GetFields()...)
	return fields
}

// Specifying a value and a precision
func round(number float64, precision int) float64 {
	scale := math.Pow10(precision)
	return math.Round(number*scale) / scale
}

func (self *NumericExpr) GetFields() []string {
	fields := make([]string, 0)
	if self.Val != nil {
		return append(fields, self.Val.GetFields()...)
	}
	if self.IsTerminal {
		if self.Op == "now" {
			return fields
		}
		if self.ValueIsField {
			return []string{self.Value}
		} else {
			return []string{}
		}
	} else if self.Right != nil {
		return append(self.Left.GetFields(), self.Right.GetFields()...)
	} else {
		return self.Left.GetFields()
	}
}

func getValueAsString(fieldToValue map[string]utils.CValueEnclosure, field string) (string, error) {
	enclosure, ok := fieldToValue[field]
	if !ok {
		return "", fmt.Errorf("Missing field %v", field)
	}

	return enclosure.GetString()
}

func getValueAsFloat(fieldToValue map[string]utils.CValueEnclosure, field string) (float64, error) {
	enclosure, ok := fieldToValue[field]
	if !ok {
		return 0, fmt.Errorf("Missing field %v", field)
	}

	if value, err := enclosure.GetFloatValue(); err == nil {
		return value, nil
	}

	// Check if the string value is a number.
	if enclosure.Dtype == utils.SS_DT_STRING {
		if value, err := strconv.ParseFloat(enclosure.CVal.(string), 64); err == nil {
			return value, nil
		}
	}

	return 0, fmt.Errorf("Cannot convert CValueEnclosure %v to float", enclosure)
}

func (self *SortValue) Compare(other *SortValue) (int, error) {
	switch self.InterpretAs {
	case "ip":
		selfIP := net.ParseIP(self.Val)
		otherIP := net.ParseIP(other.Val)
		if selfIP == nil || otherIP == nil {
			return 0, fmt.Errorf("SortValue.Compare: cannot parse IP address")
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
			return 0, fmt.Errorf("CompareSortValueSlices: %v", err)
		}

		if comp != 0 {
			return comp * ascending[i], nil
		}
	}

	return 0, nil
}
