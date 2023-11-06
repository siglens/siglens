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

package structs

import (
	"fmt"
	"math"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"

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

// See ValueExprMode type definition for which fields are valid for each mode.
type ValueExpr struct {
	ValueExprMode ValueExprMode

	FloatValue    float64
	NumericExpr   *NumericExpr
	StringExpr    *StringExpr
	ConditionExpr *ConditionExpr
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
}

type ConditionExpr struct {
	Op         string //if
	BoolExpr   *BoolExpr
	TrueValue  *ValueExpr //if bool expr is true, take this value
	FalseValue *ValueExpr
}

type BoolOperator uint8

const (
	BoolOpNot BoolOperator = iota
	BoolOpAnd
	BoolOpOr
)

type RenameExprMode uint8

const (
	REMPhrase   = iota //Rename with a phrase
	REMRegex           //Rename fields with similar names using a wildcard
	REMOverride        //Rename to a existing field
)

type ValueExprMode uint8

const (
	VEMNumericExpr   = iota // Only NumricExpr is valid
	VEMStringExpr           // Only StringExpr is valid
	VEMConditionExpr        // Only ConditionExpr is valud
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
	if self.IsTerminal || (self.Op != "max" && self.Op != "min") {
		return self.Value.GetFields()
	}
	var fields []string
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
	if self.IsTerminal {
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
