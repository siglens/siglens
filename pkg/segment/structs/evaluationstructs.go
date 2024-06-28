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
	"math/rand"
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
	log "github.com/sirupsen/logrus"

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
	Op    string // Including arithmetic, mathematical and text functions ops
	Left  *NumericExpr
	Right *NumericExpr
	Val   *StringExpr
}

type StringExpr struct {
	StringExprMode StringExprMode
	RawString      string      // only used when mode is RawString
	StringList     []string    // only used when mode is RawStringList
	FieldName      string      // only used when mode is Field
	ConcatExpr     *ConcatExpr // only used when mode is Concat
	TextExpr       *TextExpr   // only used when mode is TextExpr
}

type TextExpr struct {
	IsTerminal  bool
	Op          string //lower, ltrim, rtrim
	Param       *StringExpr
	StrToRemove string
	Delimiter   *StringExpr
	ValueList   []*StringExpr
	StartIndex  *NumericExpr
	EndIndex    *NumericExpr
	LengthExpr  *NumericExpr
	Val         *ValueExpr
	Condition   *BoolExpr  // To filter out values that do not meet the criteria within a multivalue field
	InferTypes  bool       // To specify that the mv_to_json_array function should attempt to infer JSON data types when it converts field values into array elements.
	Cluster     *Cluster   // generates a cluster label
	SPathExpr   *SPathExpr // To extract information from the structured data formats XML and JSON.
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

type Cluster struct {
	Field     string
	Threshold float64
	Match     string // termlist, termset, ngramset
	Delims    string
}

// This structure is used to store values which are not within limit. And These values will be merged into the 'other' category.
type TMLimitResult struct {
	ValIsInLimit     map[string]bool
	GroupValScoreMap map[string]*utils.CValueEnclosure
	Hll              *hyperloglog.Sketch
	StrSet           map[string]struct{}
	OtherCValArr     []*utils.CValueEnclosure
}

// To extract information from the structured data formats XML and JSON.
type SPathExpr struct {
	InputColName    string // default is set to _raw
	Path            string // the path to the field from which the values need to be extracted.
	IsPathFieldName bool   // If true, the path is the field name and the value is the field value
	OutputColName   string // the name of the column in the output table to which the extracted values will be written. By Default it is set the same as the path.
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
	SEMRawString     = iota // only used when mode is RawString
	SEMRawStringList        // only used when mode is RawStringList
	SEMField                // only used when mode is Field
	SEMConcatExpr           // only used when mode is Concat
	SEMTextExpr             // only used when mode is TextExpr
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

func findNullFields(fields []string, fieldToValue map[string]utils.CValueEnclosure) ([]string, error) {
	nullFields := []string{}
	for _, field := range fields {
		val, exists := fieldToValue[field]
		if !exists {
			return []string{}, fmt.Errorf("findNullFields: Expression has a field for which value is not present")
		}
		if val.Dtype == utils.SS_DT_BACKFILL {
			nullFields = append(nullFields, field)
		}
	}

	return nullFields, nil
}

func (self *BoolExpr) GetNullFields(fieldToValue map[string]utils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func (self *NumericExpr) GetNullFields(fieldToValue map[string]utils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func (self *StringExpr) GetNullFields(fieldToValue map[string]utils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func (self *RenameExpr) GetNullFields(fieldToValue map[string]utils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func (self *ConcatExpr) GetNullFields(fieldToValue map[string]utils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func (self *TextExpr) GetNullFields(fieldToValue map[string]utils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func (self *ValueExpr) GetNullFields(fieldToValue map[string]utils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func (self *ConditionExpr) GetNullFields(fieldToValue map[string]utils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

func (self *RexExpr) GetNullFields(fieldToValue map[string]utils.CValueEnclosure) ([]string, error) {
	return findNullFields(self.GetFields(), fieldToValue)
}

// Evaluate this BoolExpr to a boolean, replacing each field in the expression
// with the value specified by fieldToValue. Each field listed by GetFields()
// must be in fieldToValue.
func (self *BoolExpr) Evaluate(fieldToValue map[string]utils.CValueEnclosure) (bool, error) {
	if self.IsTerminal {
		switch self.ValueOp {
		case "in":
			inFlag, err := isInValueList(fieldToValue, self.LeftValue, self.ValueList)
			if err != nil {
				return false, fmt.Errorf("BoolExpr.Evaluate: can not evaluate Eval In function: %v", err)
			}
			return inFlag, err
		case "isbool":
			val, err := self.LeftValue.EvaluateToString(fieldToValue)
			if err != nil {
				return false, fmt.Errorf("BoolExpr.Evaluate: 'isbool' can not evaluate to String: %v", err)
			}
			isBool := strings.ToLower(val) == "true" || strings.ToLower(val) == "false" || val == "0" || val == "1"
			return isBool, nil

		case "isint":
			val, err := self.LeftValue.EvaluateToString(fieldToValue)
			if err != nil {
				return false, err
			}

			_, parseErr := strconv.Atoi(val)
			return parseErr == nil, nil
		case "isnum":
			val, err := self.LeftValue.EvaluateToString(fieldToValue)
			if err != nil {
				return false, err
			}

			_, parseErr := strconv.ParseFloat(val, 64)
			return parseErr == nil, nil
		case "isstr":
			_, floatErr := self.LeftValue.EvaluateToFloat(fieldToValue)

			if floatErr == nil {
				return false, nil
			}

			_, strErr := self.LeftValue.EvaluateToString(fieldToValue)
			return strErr == nil, nil
		case "isnull":
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
		case "like":
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
				return false, fmt.Errorf("BoolExpr.Evaluate: regex error in LIKE operation pattern: %v, string: %v, err: %v", regexPattern, leftStr, err)
			}
			return matched, nil
		case "match":
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
				return false, fmt.Errorf("BoolExpr.Evaluate: regex error in MATCH operation leftString %v, rightString %v, err: %v", leftStr, rightStr, err)
			}
			return matched, nil

		case "cidrmatch":
			cidrStr, errCidr := self.LeftValue.EvaluateToString(fieldToValue)
			ipStr, errIp := self.RightValue.EvaluateToString(fieldToValue)
			if errCidr != nil || errIp != nil {
				return false, fmt.Errorf("BoolExpr.Evaluate: 'cidrmatch' error evaluating arguments errCidr: %v, errIp: %v", errCidr, errIp)
			}

			match, err := isIPInCIDR(cidrStr, ipStr)
			if err != nil {
				return false, fmt.Errorf("BoolExpr.Evaluate: 'cidrmatch' error in matching is IP in CIDR: cidr: %v, ip: %v, err: %v", cidrStr, ipStr, err)
			}
			return match, nil
		case "isnotnull":
			fields := self.GetFields()
			if len(fields) == 0 {
				return false, fmt.Errorf("BoolExpr.Evaluate: No fields found for isnotnull operation")
			}

			value, exists := fieldToValue[fields[0]]
			if !exists {
				return false, fmt.Errorf("BoolExpr.Evaluate: Field '%s' not found in data", fields[0])
			}
			if value.Dtype != utils.SS_DT_BACKFILL {
				return true, nil
			}
			return false, nil
		case "searchmatch":
			return false, fmt.Errorf("BoolExpr.Evaluate: does not support using this operator: %v", self.ValueOp)
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
			return false, fmt.Errorf("BoolExpr.Evaluate: invalid BoolOp: %v", self.BoolOp)
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

func isInValueList(fieldToValue map[string]utils.CValueEnclosure, value *ValueExpr, valueList []*ValueExpr) (bool, error) {
	valueStr, err := value.EvaluateToString(fieldToValue)
	if err != nil {
		return false, fmt.Errorf("isInValueList: can not evaluate to String: %v", err)
	}

	for _, atom := range valueList {
		atomStr, err := atom.EvaluateToString(fieldToValue)
		if err != nil {
			return false, fmt.Errorf("isInValueList: can not evaluate to string: %v", err)
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
		return "", fmt.Errorf("ValueExpr.EvaluateToString: cannot evaluate to string, not a valid ValueExprMode")
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
				return fmt.Errorf("StatisticExpr.OverrideGroupByCol: expected keyIndex to be 0, not %v", keyIndex)
			}
			bucketResult.BucketKey = cellValueStr
		default:
			return fmt.Errorf("StatisticExpr.OverrideGroupByCol: bucket key has unexpected type: %T", bucketKey)
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
	default:
		log.Errorf("handleNoArgFunction: Unsupported no argument function: %v", op)
		return 0, fmt.Errorf("handleNoArgFunction: Unsupported no argument function: %v", op)
	}
}

func handleComparisonAndConditionalFunctions(self *ConditionExpr, fieldToValue map[string]utils.CValueEnclosure, functionName string) (string, error) {
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
				return "", fmt.Errorf("handleComparisonAndConditionalFunctions: Error while evaluating condition, err: %v fieldToValue: %v", err, fieldToValue)
			}
			if !res {
				val, err := cvPair.Value.EvaluateValueExprAsString(fieldToValue)
				if err != nil {
					return "", fmt.Errorf("handleComparisonAndConditionalFunctions: Error while evaluating value, err: %v fieldToValue: %v", err, fieldToValue)
				}
				return val, nil
			}
		}
		return "", nil
	default:
		return "", fmt.Errorf("handleComparisonAndConditionalFunctions: Unknown function name: %s", functionName)
	}
}

// Evaluate this NumericExpr to a float, replacing each field in the expression
// with the value specified by fieldToValue. Each field listed by GetFields()
// must be in fieldToValue.
func (self *NumericExpr) Evaluate(fieldToValue map[string]utils.CValueEnclosure) (float64, error) {
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
				if self.Op != "" {
					if self.Value != "" {
						return 0, fmt.Errorf("NumericExpr.Evaluate: Error calling no argument function: %v, value: %v", self.Op, self.Value)
					}
					return handleNoArgFunction(self.Op)
				}

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
	replacements := map[string]string{
		"%d": "02",
		"%m": "01",
		"%Y": "2006",
		"%H": "15",
		"%I": "03",
		"%p": "PM",
		"%M": "04",
		"%S": "05",
		"%b": "Jan",
		"%B": "January",
		"%y": "06",
		"%e": "2",
		"%a": "Mon",
		"%A": "Monday",
		"%w": "Monday",
		"%j": "002",
		"%U": "00",
		"%W": "00",
		"%V": "00",
		"%z": "-0700",
		"%Z": "MST",
		"%c": "Mon Jan  2 15:04:05 2006",
		"%x": "01/02/06",
		"%X": "15:04:05",
		"%%": "%",
	}
	for k, v := range replacements {
		format = strings.ReplaceAll(format, k, v)
	}

	// Check if format contains only time components (%H, %I, %M, %S, %p) and no date components (%d, %m, %Y, etc.)
	if !strings.Contains(format, "2006") && !strings.Contains(format, "01") && !strings.Contains(format, "02") {
		// Prepend a default date if only time is present
		dateStr = "1970-01-01 " + dateStr
		format = "2006-01-02 " + format
	}

	return time.Parse(format, dateStr)
}
func (self *TextExpr) EvaluateText(fieldToValue map[string]utils.CValueEnclosure) (string, error) {
	// Todo: implement the processing logic for these functions:
	switch self.Op {
	case "strftime":
		timestamp, err := self.Val.EvaluateToFloat(fieldToValue)
		if err != nil {
			return "", fmt.Errorf("TextExpr.EvaluateText: cannot evaluate timestamp: %v", err)
		}
		timestampInSeconds := timestamp / 1000
		t := time.Unix(int64(timestampInSeconds), 0) // time.Unix(sec int64, nsec int64) -> expects seconds and nanoseconds. Since strftime expects seconds, we pass 0 for nanoseconds.

		timeStr := formatTime(t, self.Param.RawString)
		return timeStr, nil
	case "strptime":
		dateStr, err := self.Val.EvaluateToString(fieldToValue)
		if err != nil {
			return "", fmt.Errorf("TextExpr.EvaluateText: cannot evaluate date string: %v", err)
		}
		t, err := parseTime(dateStr, self.Param.RawString)
		if err != nil {
			return "", fmt.Errorf("TextExpr.EvaluateText: cannot parse date string: %v", err)
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
	case "replace":
		if len(self.ValueList) < 2 {
			return "", fmt.Errorf("TextExpr.EvaluateText: 'replace' operation requires a regex and a replacement")
		}

		regexStr, err := self.ValueList[0].Evaluate(fieldToValue)
		if err != nil {
			return "", fmt.Errorf("TextExpr.EvaluateText: cannot evaluate regex as a string: %v", err)
		}
		replacementStr, err := self.ValueList[1].Evaluate(fieldToValue)
		if err != nil {
			return "", fmt.Errorf("TextExpr.EvaluateText: cannot evaluate replacement as a string: %v", err)
		}
		baseValue, exists := fieldToValue[self.Val.NumericExpr.Value]
		if !exists {
			return "", fmt.Errorf("TextExpr.EvaluateText: field '%s' not found in data", self.Val.NumericExpr.Value)
		}
		baseStr, ok := baseValue.CVal.(string)
		if !ok {
			return "", fmt.Errorf("TextExpr.EvaluateText: expected baseValue.CVal to be a string, got %T with value %v", baseValue.CVal, baseValue.CVal)
		}
		regex, err := regexp.Compile(regexStr)
		if err != nil {
			return "", fmt.Errorf("TextExpr.EvaluateText: failed to compile regex '%s': %v", regexStr, err)
		}
		return regex.ReplaceAllString(baseStr, replacementStr), nil
	case "mvappend":
		fallthrough
	case "mvcount":
		fallthrough
	case "mvdedup":
		fallthrough
	case "mvfilter":
		fallthrough
	case "mvfind":
		fallthrough
	case "mvindex":
		fallthrough
	case "mvjoin":
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
		var val utils.CValueEnclosure
		var ok bool

		if self.Val.NumericExpr == nil {
			val.Dtype = utils.SS_INVALID
		} else {
			val, ok = fieldToValue[self.Val.NumericExpr.Value] // The parser always sets the fieldName in NumericExpr even if it's a string
			if !ok {
				val.Dtype = utils.SS_INVALID
			}
		}

		switch val.Dtype {
		case utils.SS_INVALID:
			valueExpr, err := self.Val.EvaluateValueExprAsString(fieldToValue)
			if err != nil {
				return "Invalid", nil
			}
			if isNumber(valueExpr) {
				return "Number", nil
			}
			if isBoolean(valueExpr) {
				return "Boolean", nil
			}
			return "String", nil
		case utils.SS_DT_BOOL:
			return "Boolean", nil
		case utils.SS_DT_SIGNED_NUM, utils.SS_DT_UNSIGNED_NUM, utils.SS_DT_FLOAT,
			utils.SS_DT_SIGNED_32_NUM, utils.SS_DT_USIGNED_32_NUM,
			utils.SS_DT_SIGNED_16_NUM, utils.SS_DT_USIGNED_16_NUM,
			utils.SS_DT_SIGNED_8_NUM, utils.SS_DT_USIGNED_8_NUM:
			return "Number", nil
		case utils.SS_DT_STRING, utils.SS_DT_STRING_SET, utils.SS_DT_RAW_JSON:
			return "String", nil
		case utils.SS_DT_BACKFILL:
			return "Null", nil
		default:
			return "Invalid", nil
		}
	}
	if self.Op == "max" {
		if len(self.ValueList) == 0 {
			return "", fmt.Errorf("TextExpr.EvaluateText: no values provided for 'max' operation")
		}
		maxString := ""
		for _, expr := range self.ValueList {
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
		if len(self.ValueList) == 0 {
			return "", fmt.Errorf("TextExpr.EvaluateText: no values provided for 'min' operation")
		}
		minString := ""
		for _, expr := range self.ValueList {
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
			return "", fmt.Errorf("TextExpr.EvaluateText: failed to evaluate value for 'tostring' operation: %v", err)
		}
		if self.Param != nil {
			formatStr, err := self.Param.Evaluate(fieldToValue)
			if err != nil {
				return "", fmt.Errorf("TextExpr.EvaluateText: failed to evaluate format for 'tostring' operation: %v", err)
			}
			switch formatStr {
			case "hex":
				num, convErr := strconv.Atoi(valueStr)
				if convErr != nil {
					return "", fmt.Errorf("TextExpr.EvaluateText: failed to convert value '%s' to integer for hex formatting: %v", valueStr, convErr)
				}
				return fmt.Sprintf("%#x", num), nil
			case "commas":
				num, convErr := strconv.ParseFloat(valueStr, 64)
				if convErr != nil {
					return "", fmt.Errorf("TextExpr.EvaluateText: failed to convert value '%s' to float for comma formatting: %v", valueStr, convErr)
				}
				roundedNum := math.Round(num*100) / 100
				formattedNum := humanize.CommafWithDigits(roundedNum, 2)
				return formattedNum, nil
			case "duration":
				num, convErr := strconv.Atoi(valueStr)
				if convErr != nil {
					return "", fmt.Errorf("TextExpr.EvaluateText: failed to convert value '%s' to seconds for duration formatting: %v", valueStr, convErr)
				}
				hours := num / 3600
				minutes := (num % 3600) / 60
				seconds := num % 60
				return fmt.Sprintf("%02d:%02d:%02d", hours, minutes, seconds), nil
			default:
				return "", fmt.Errorf("TextExpr.EvaluateText: unsupported format '%s' for tostring operation", formatStr)
			}
		} else {
			return valueStr, nil
		}
	}
	cellValueStr, err := self.Param.Evaluate(fieldToValue)
	if err != nil {
		return "", fmt.Errorf("TextExpr.EvaluateText: can not evaluate text as a str: %v", err)
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
			return "", fmt.Errorf("TextExpr.EvaluateText: failed to decode URL: %v", decodeErr)
		}
		return decodedStr, nil

	case "split":
		delimiterStr, err := self.Delimiter.Evaluate(fieldToValue)
		if err != nil {
			return "", fmt.Errorf("TextExpr.EvaluateText: cannot evaluate delimiter as a string: %v", err)
		}

		return strings.Join(strings.Split(cellValueStr, delimiterStr), "&nbsp"), nil

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
			return "", fmt.Errorf("TextExpr.EvaluateText: 'substr' start index is out of range")
		}
		substrLength := len(baseString) - startIndex
		if self.LengthExpr != nil {
			lengthFloat, err := self.LengthExpr.Evaluate(fieldToValue)
			if err != nil {
				return "", err
			}
			substrLength = int(lengthFloat)
			if substrLength < 0 || startIndex+substrLength > len(baseString) {
				return "", fmt.Errorf("TextExpr.EvaluateText: 'substr' length leads to out of range substring")
			}
		}
		endIndex := startIndex + substrLength
		if endIndex > len(baseString) {
			endIndex = len(baseString)
		}
		return baseString[startIndex:endIndex], nil

	default:
		return "", fmt.Errorf("TextExpr.EvaluateText: unexpected operation: %v", self.Op)
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
				return "", fmt.Errorf("ValueExpr.EvaluateValueExprAsString: can not evaluate VEMNumericExpr to string: %v", err)
			}
		}
	case VEMStringExpr:
		str, err = self.EvaluateToString(fieldToValue)
		if err != nil {
			return "", fmt.Errorf("ValueExpr.EvaluateValueExprAsString: can not evaluate VEMStringExpr to string: %v", err)
		}
	}
	return str, nil
}

func isNumber(str string) bool {
	_, err := strconv.ParseFloat(str, 64)
	return err == nil
}

func isBoolean(str string) bool {
	lowerStr := strings.ToLower(str)
	return lowerStr == "true" || lowerStr == "false"
}

func handleCaseFunction(self *ConditionExpr, fieldToValue map[string]utils.CValueEnclosure) (string, error) {

	for _, cvPair := range self.ConditionValuePairs {
		res, err := cvPair.Condition.Evaluate(fieldToValue)
		if err != nil {
			nullFields, err2 := cvPair.Condition.GetNullFields(fieldToValue)
			if err2 == nil && len(nullFields) > 0 {
				continue
			}
			return "", fmt.Errorf("handleCaseFunction: Error while evaluating condition, err: %v", err)
		}
		if res {
			val, err := cvPair.Value.EvaluateValueExprAsString(fieldToValue)
			if err != nil {
				return "", fmt.Errorf("handleCaseFunction: Error while evaluating value, err: %v", err)
			}
			return val, nil
		}
	}

	return "", nil
}

func handleCoalesceFunction(self *ConditionExpr, fieldToValue map[string]utils.CValueEnclosure) (string, error) {
	for _, valueExpr := range self.ValueList {
		nullFields, err := valueExpr.GetNullFields(fieldToValue)
		if err != nil || len(nullFields) > 0 {
			continue
		}

		val, err := valueExpr.EvaluateValueExprAsString(fieldToValue)
		if err != nil {
			return "", fmt.Errorf("handleCoalesceFunction: Error while evaluating value, err: %v", err)
		}
		return val, nil
	}

	return "", nil
}

// Field may come from BoolExpr or ValueExpr
func (self *ConditionExpr) EvaluateCondition(fieldToValue map[string]utils.CValueEnclosure) (string, error) {

	switch self.Op {
	case "if":
		predicateFlag, err := self.BoolExpr.Evaluate(fieldToValue)
		if err != nil {
			return "", fmt.Errorf("ConditionExpr.EvaluateCondition cannot evaluate BoolExpr: %v", err)
		}

		trueValue, err := self.TrueValue.EvaluateValueExprAsString(fieldToValue)
		if err != nil {
			return "", fmt.Errorf("ConditionExpr.EvaluateCondition: can not evaluate trueValue to a ValueExpr: %v", err)
		}
		falseValue, err := self.FalseValue.EvaluateValueExprAsString(fieldToValue)
		if err != nil {
			return "", fmt.Errorf("ConditionExpr.EvaluateCondition: can not evaluate falseValue to a ValueExpr: %v", err)
		}
		if predicateFlag {
			return trueValue, nil
		} else {
			return falseValue, nil
		}
	case "validate":
		return handleComparisonAndConditionalFunctions(self, fieldToValue, self.Op)
	case "case":
		return handleCaseFunction(self, fieldToValue)
	case "coalesce":
		return handleCoalesceFunction(self, fieldToValue)
	default:
		return "", fmt.Errorf("ConditionExpr.EvaluateCondition: unsupported operation: %v", self.Op)
	}

}

func (self *TextExpr) GetFields() []string {
	fields := make([]string, 0)
	if self.IsTerminal || (self.Op != "max" && self.Op != "min") {
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
		return fields
	}
	for _, expr := range self.ValueList {
		fields = append(fields, expr.GetFields()...)
	}
	return fields

}

// Append all the fields in ConditionExpr
func (self *ConditionExpr) GetFields() []string {
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
		return "", fmt.Errorf("getValueAsString: Missing field %v", field)
	}

	return enclosure.GetString()
}

func getValueAsFloat(fieldToValue map[string]utils.CValueEnclosure, field string) (float64, error) {
	enclosure, ok := fieldToValue[field]
	if !ok {
		return 0, fmt.Errorf("getValueAsFloat: Missing field %v", field)
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

	return 0, fmt.Errorf("getValueAsFloat: Cannot convert CValueEnclosure %v to float", enclosure)
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
