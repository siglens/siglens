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
	"testing"
	"time"

	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func Test_ConcatExpr(t *testing.T) {
	expr := &ConcatExpr{
		Atoms: []*ConcatAtom{
			{IsField: true, Value: "FieldWithStrings"},
			{IsField: false, Value: " and "},
			{IsField: true, Value: "FieldWithNumbers"},
		},
	}

	// Test GetFields()
	assert.Equal(t, expr.GetFields(), []string{"FieldWithStrings", "FieldWithNumbers"})

	// Test Evaluate()
	fieldToValue := make(map[string]sutils.CValueEnclosure)
	fieldToValue["FieldWithStrings"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "testing",
	}
	fieldToValue["FieldWithNumbers"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_SIGNED_NUM,
		CVal:  int64(123),
	}

	value, err := expr.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, "testing and 123")

	// When fieldToValue is missing fields, Evaluate() should error.
	delete(fieldToValue, "FieldWithNumbers")
	_, err = expr.Evaluate(fieldToValue)
	assert.NotNil(t, err)
}

func Test_NumericExpr(t *testing.T) {
	exprA := &NumericExpr{
		IsTerminal:   true,
		ValueIsField: false,
		Value:        "5",
	}
	exprB := &NumericExpr{
		IsTerminal:      true,
		ValueIsField:    true,
		Value:           "Max",
		NumericExprMode: NEMNumberField,
	}
	exprC := &NumericExpr{
		IsTerminal: false,
		Op:         "-",
		Left:       exprB,
		Right:      exprA,
	}
	exprD := &NumericExpr{
		IsTerminal:      true,
		ValueIsField:    true,
		Value:           "Min",
		NumericExprMode: NEMNumberField,
	}
	exprE := &NumericExpr{
		IsTerminal: false,
		Op:         "/",
		Left:       exprC,
		Right:      exprD,
	}

	// Test GetFields()
	assert.Equal(t, exprA.GetFields(), []string{})

	assert.Equal(t, exprB.GetFields(), []string{"Max"})
	assert.Equal(t, exprC.GetFields(), []string{"Max"})
	assert.Equal(t, exprD.GetFields(), []string{"Min"})

	eFields := exprE.GetFields()
	assert.True(t, utils.SliceHas(eFields, "Min"))

	assert.True(t, utils.SliceHas(eFields, "Max"))

	// Test Evaluate()
	fieldToValue := make(map[string]sutils.CValueEnclosure)
	fieldToValue["Min"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_FLOAT,
		CVal:  float64(12),
	}

	fieldToValue["Max"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_SIGNED_NUM,
		CVal:  int64(62),
	}

	value, err := exprA.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, float64(5))

	value, err = exprB.Evaluate(fieldToValue)

	assert.Nil(t, err)
	assert.Equal(t, value, float64(62))

	value, err = exprC.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, float64(62-5))

	value, err = exprD.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, float64(12))

	value, err = exprE.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, float64(62-5)/12)

	// When fieldToValue is missing fields, Evaluate() should error.
	delete(fieldToValue, "Max")
	_, err = exprE.Evaluate(fieldToValue)
	assert.NotNil(t, err)

	multiplierExpr := &NumericExpr{
		IsTerminal:   true,
		ValueIsField: false,
		Value:        "3.14",
	}
	httpStatusExpr := &NumericExpr{
		IsTerminal:      true,
		ValueIsField:    true,
		Value:           "http_status",
		NumericExprMode: NEMNumberField,
	}
	productExpr := &NumericExpr{
		IsTerminal: false,
		Op:         "*",
		Left:       multiplierExpr,
		Right:      httpStatusExpr,
	}
	exactExpr := &NumericExpr{
		IsTerminal: false,
		Op:         "exact",
		Left:       productExpr,
	}
	assert.Equal(t, exactExpr.GetFields(), []string{"http_status"})

	fieldToValue["http_status"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_SIGNED_NUM,
		CVal:  int64(200),
	}

	value, err = exactExpr.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, float64(628))

	expExpr := &NumericExpr{
		IsTerminal: false,
		Op:         "exp",
		Left: &NumericExpr{
			IsTerminal:   true,
			ValueIsField: false,
			Value:        "3",
		},
	}

	assert.Equal(t, expExpr.GetFields(), []string{})

	value, err = expExpr.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, 20.085536923187668)

	nowExpr := &NumericExpr{
		NumericExprMode: NEMNumber,
		IsTerminal:      true,
		Op:              "now",
	}
	assert.Equal(t, nowExpr.GetFields(), []string{})

	// Test Evaluate()
	fieldToValue = make(map[string]sutils.CValueEnclosure)

	value, err = nowExpr.Evaluate(fieldToValue)
	assert.Nil(t, err)
	currentTimestamp := time.Now().Unix()

	assert.InDelta(t, currentTimestamp, value, 1, "The evaluated timestamp is not within the expected range")

	strToNumber :=
		&NumericExpr{
			NumericExprMode: NEMNumericExpr,
			IsTerminal:      false,
			Op:              "tonumber",
			Right: &NumericExpr{
				NumericExprMode: NEMNumber,
				IsTerminal:      true,
				ValueIsField:    false,
				Value:           "16",
			},
			Val: &StringExpr{
				StringExprMode: SEMRawString,
				RawString:      "0A4",
			},
		}
	assert.Equal(t, strToNumber.GetFields(), []string{})

	value, err = strToNumber.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, float64(164))

}

func Test_NumericExpr_Mod(t *testing.T) {
	numericExpr := &NumericExpr{
		IsTerminal:      true,
		ValueIsField:    true,
		Value:           "number",
		NumericExprMode: NEMNumberField,
	}

	numericExpr2 := &NumericExpr{
		IsTerminal:   true,
		ValueIsField: false,
		Value:        "5",
	}

	numericExpr3 := &NumericExpr{
		IsTerminal: false,
		Op:         "%",
		Left:       numericExpr,
		Right:      numericExpr2,
	}

	values := []float64{10, 12, 23, 39, 91}
	expectedValues := []float64{0, 2, 3, 4, 1}

	fieldToValue := make(map[string]sutils.CValueEnclosure)
	for i, value := range values {
		fieldToValue["number"] = sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_FLOAT,
			CVal:  value,
		}

		result, err := numericExpr3.Evaluate(fieldToValue)
		assert.Nil(t, err)
		assert.Equal(t, expectedValues[i], result)
	}
}

func Test_ValueExpr(t *testing.T) {
	numericExpr := &NumericExpr{
		IsTerminal: false,
		Op:         "-",
		Left: &NumericExpr{
			IsTerminal:      true,
			ValueIsField:    true,
			Value:           "Max",
			NumericExprMode: NEMNumberField,
		},
		Right: &NumericExpr{
			IsTerminal:   true,
			ValueIsField: false,
			Value:        "5",
		},
	}

	concatExpr := &ConcatExpr{
		Atoms: []*ConcatAtom{
			{IsField: true, Value: "FieldWithStrings"},
			{IsField: false, Value: " and "},
			{IsField: true, Value: "FieldWithNumbers"},
		},
	}

	valueExprA := &ValueExpr{
		ValueExprMode: VEMNumericExpr,
		NumericExpr:   numericExpr,
	}

	valueExprB := &ValueExpr{
		ValueExprMode: VEMStringExpr,
		StringExpr: &StringExpr{
			StringExprMode: SEMConcatExpr,
			ConcatExpr:     concatExpr,
		},
	}

	valueExprC := &ValueExpr{
		ValueExprMode: VEMStringExpr,
		StringExpr: &StringExpr{
			StringExprMode: SEMConcatExpr,
			ConcatExpr: &ConcatExpr{
				Atoms: []*ConcatAtom{
					{Value: "hello"},
				},
			},
		},
	}
	valueExprD := &ValueExpr{
		ValueExprMode: VEMNumericExpr,
		NumericExpr: &NumericExpr{

			IsTerminal:   true,
			ValueIsField: false,
			Value:        "99",
		},
	}

	valueExprE := &ValueExpr{
		ValueExprMode: VEMNumericExpr,
		NumericExpr: &NumericExpr{
			IsTerminal:   true,
			ValueIsField: true,

			Value:           "Seconds",
			NumericExprMode: NEMNumberField,
		},
	}

	// Test GetFields()
	assert.Equal(t, valueExprA.GetFields(), []string{"Max"})
	assert.Equal(t, valueExprB.GetFields(), []string{"FieldWithStrings", "FieldWithNumbers"})
	assert.Equal(t, valueExprC.GetFields(), []string{})
	assert.Equal(t, valueExprD.GetFields(), []string{})
	assert.Equal(t, valueExprE.GetFields(), []string{"Seconds"})

	// Test Evaluate()
	fieldToValue := make(map[string]sutils.CValueEnclosure)
	fieldToValue["Max"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_FLOAT,
		CVal:  float64(62),
	}
	fieldToValue["Seconds"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_SIGNED_NUM,
		CVal:  int64(42),
	}
	fieldToValue["FieldWithStrings"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "testing",
	}
	fieldToValue["FieldWithNumbers"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_SIGNED_NUM,
		CVal:  int64(123),
	}

	var valueFloat float64
	var valueStr string
	var err error
	valueFloat, err = valueExprA.EvaluateToFloat(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, valueFloat, float64(62-5))
	valueStr, err = valueExprA.EvaluateToString(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, valueStr, "57")

	_, err = valueExprB.EvaluateToFloat(fieldToValue)
	assert.NotNil(t, err)
	valueStr, err = valueExprB.EvaluateToString(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, valueStr, "testing and 123")

	_, err = valueExprC.EvaluateToFloat(fieldToValue)
	assert.NotNil(t, err)
	valueStr, err = valueExprC.EvaluateToString(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, valueStr, "hello")

	valueFloat, err = valueExprD.EvaluateToFloat(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, valueFloat, float64(99))
	valueStr, err = valueExprD.EvaluateToString(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, valueStr, "99")

	valueFloat, err = valueExprE.EvaluateToFloat(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, valueFloat, float64(42))
	valueStr, err = valueExprE.EvaluateToString(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, valueStr, "42")

	// When fieldToValue is missing fields, Evaluate() should error.
	fieldToValue = make(map[string]sutils.CValueEnclosure)
	_, err = valueExprA.EvaluateToFloat(fieldToValue)
	assert.NotNil(t, err)
	_, err = valueExprA.EvaluateToString(fieldToValue)
	assert.NotNil(t, err)

	_, err = valueExprB.EvaluateToFloat(fieldToValue)
	assert.NotNil(t, err)
	_, err = valueExprA.EvaluateToString(fieldToValue)
	assert.NotNil(t, err)

	_, err = valueExprE.EvaluateToFloat(fieldToValue)
	assert.NotNil(t, err)
	_, err = valueExprA.EvaluateToString(fieldToValue)
	assert.NotNil(t, err)
}

func Test_BoolExpr(t *testing.T) {
	valueExprA := &ValueExpr{
		ValueExprMode: VEMNumericExpr,
		NumericExpr: &NumericExpr{
			IsTerminal: false,
			Op:         "-",
			Left: &NumericExpr{
				IsTerminal:      true,
				ValueIsField:    true,
				Value:           "Max",
				NumericExprMode: NEMNumberField,
			},
			Right: &NumericExpr{
				IsTerminal:   true,
				ValueIsField: false,
				Value:        "5",
			},
		},
	}

	valueExprB := &ValueExpr{
		ValueExprMode: VEMStringExpr,
		StringExpr: &StringExpr{
			StringExprMode: SEMConcatExpr,
			ConcatExpr: &ConcatExpr{
				Atoms: []*ConcatAtom{
					{IsField: true, Value: "FieldWithStrings"},
					{IsField: false, Value: " and "},
					{IsField: true, Value: "FieldWithNumbers"},
				},
			},
		},
	}

	valueExprC := &ValueExpr{
		ValueExprMode: VEMStringExpr,
		StringExpr: &StringExpr{
			StringExprMode: SEMConcatExpr,
			ConcatExpr: &ConcatExpr{

				Atoms: []*ConcatAtom{
					{Value: "hello"},
				},
			},
		},
	}

	valueExprD := &ValueExpr{
		ValueExprMode: VEMNumericExpr,
		NumericExpr: &NumericExpr{

			IsTerminal:   true,
			ValueIsField: false,
			Value:        "99",
		},
	}

	boolExprA := &BoolExpr{
		IsTerminal: true,
		ValueOp:    "<",
		LeftValue:  valueExprA,
		RightValue: valueExprD,
	}

	boolExprB := &BoolExpr{
		IsTerminal: true,
		ValueOp:    "=",
		LeftValue:  valueExprB,
		RightValue: valueExprC,
	}

	boolExprC := &BoolExpr{
		IsTerminal: false,
		BoolOp:     BoolOpOr,
		LeftBool:   boolExprA,
		RightBool:  boolExprB,
	}

	boolExprD := &BoolExpr{
		IsTerminal: false,
		BoolOp:     BoolOpAnd,
		LeftBool:   boolExprA,
		RightBool:  boolExprB,
	}

	boolExprE := &BoolExpr{
		IsTerminal: false,
		BoolOp:     BoolOpNot,
		LeftBool:   boolExprA,
	}

	// Test GetFields()
	assert.Equal(t, boolExprA.GetFields(), []string{"Max"})
	assert.Equal(t, boolExprB.GetFields(), []string{"FieldWithStrings", "FieldWithNumbers"})

	cFields := boolExprC.GetFields()
	assert.True(t, utils.SliceHas(cFields, "Max"))
	assert.True(t, utils.SliceHas(cFields, "FieldWithStrings"))
	assert.True(t, utils.SliceHas(cFields, "FieldWithNumbers"))

	dFields := boolExprD.GetFields()
	assert.True(t, utils.SliceHas(dFields, "Max"))
	assert.True(t, utils.SliceHas(dFields, "FieldWithStrings"))
	assert.True(t, utils.SliceHas(dFields, "FieldWithNumbers"))

	assert.Equal(t, boolExprE.GetFields(), []string{"Max"})

	// Test Evaluate()
	fieldToValue := make(map[string]sutils.CValueEnclosure)
	fieldToValue["Max"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_FLOAT,
		CVal:  float64(62),
	}
	fieldToValue["FieldWithStrings"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "testing",
	}
	fieldToValue["FieldWithNumbers"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_SIGNED_NUM,
		CVal:  int64(123),
	}

	value, err := boolExprA.Evaluate(fieldToValue)

	assert.Nil(t, err)
	assert.Equal(t, value, true)

	value, err = boolExprB.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, false)

	value, err = boolExprC.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, true)

	value, err = boolExprD.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, false)

	value, err = boolExprE.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, false)

	// In a terminal node, the left and right ValueExpr must both be strings or both be floats.
	boolExprBadValueExprTypes := &BoolExpr{
		IsTerminal: true,
		ValueOp:    "!=",
		LeftValue:  valueExprA, // Evaluates to float
		RightValue: valueExprB, // Evaluates to string
	}

	value, err = boolExprBadValueExprTypes.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, true)

	// In a terminal node with string ValueExprs, the ValueOp cannot be an inequality.
	boolExprBadOpForStringValues := &BoolExpr{
		IsTerminal: true,
		ValueOp:    "<",
		LeftValue:  valueExprB, // Evaluates to string
		RightValue: valueExprC, // Evaluates to string
	}

	value, err = boolExprBadOpForStringValues.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, false)

	// When fieldToValue is missing fields, Evaluate() should error.
	delete(fieldToValue, "Max")
	delete(fieldToValue, "FieldWithNumbers")
	value, err = boolExprA.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, false)

	value, err = boolExprB.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, false)

	value, err = boolExprC.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, false)

	value, err = boolExprD.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, false)

	value, err = boolExprE.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, true)
}

func EvaluateForInputLookup_Helper(t *testing.T, boolExpr *BoolExpr, colValues []string, expectedOutput []bool, valueOps []string, colName string) {
	fieldToValue := make(map[string]sutils.CValueEnclosure)

	for i, valueOp := range valueOps {
		boolExpr.ValueOp = valueOp
		fieldToValue[colName] = sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_STRING,
			CVal:  colValues[i],
		}
		value, err := boolExpr.EvaluateForInputLookup(fieldToValue)
		assert.Nil(t, err)
		assert.Equal(t, expectedOutput[i], value)
	}
}

func Test_EvaluateForInputLookup(t *testing.T) {
	valueExprA := &ValueExpr{
		ValueExprMode: VEMNumericExpr,
		NumericExpr: &NumericExpr{
			IsTerminal:      true,
			ValueIsField:    true,
			Value:           "Test",
			NumericExprMode: NEMNumberField,
		},
	}

	valueExprB := &ValueExpr{
		ValueExprMode: VEMStringExpr,
		StringExpr: &StringExpr{
			StringExprMode: SEMRawString,
			RawString:      "test*",
		},
	}

	valueExprC := &ValueExpr{
		ValueExprMode: VEMNumericExpr,
		NumericExpr: &NumericExpr{
			IsTerminal:   true,
			ValueIsField: false,
			Value:        "100",
		},
	}

	valueExprD := &ValueExpr{
		ValueExprMode: VEMNumericExpr,
		NumericExpr: &NumericExpr{
			IsTerminal:      true,
			ValueIsField:    true,
			Value:           "Check",
			NumericExprMode: NEMNumberField,
		},
	}

	boolExprStr := &BoolExpr{
		IsTerminal: true,
		LeftValue:  valueExprA,
		RightValue: valueExprB,
	}

	boolExprNum := &BoolExpr{
		IsTerminal: true,
		LeftValue:  valueExprA,
		RightValue: valueExprC,
	}

	boolExprNum2 := &BoolExpr{
		IsTerminal: true,
		LeftValue:  valueExprD,
		RightValue: valueExprC,
	}

	fieldToValue := make(map[string]sutils.CValueEnclosure)
	valueOps := []string{"=", "!=", ">", "<", ">=", "<="}

	// Test String Comparisons
	colStrValues := []string{"Testing", "test", "xyz", "tester", "sun", "Test"}
	expectedOutput := []bool{true, false, true, false, false, true}
	EvaluateForInputLookup_Helper(t, boolExprStr, colStrValues, expectedOutput, valueOps, "Test")

	// Test Numeric Comparisons
	colNumValues := []string{"100", "100", "101", "0", "99", "100"}
	expectedOutput = []bool{true, false, true, true, false, true}
	EvaluateForInputLookup_Helper(t, boolExprNum, colNumValues, expectedOutput, valueOps, "Test")

	// Test String and Numeric Comparisons
	colValues := []string{"Testing", "100", "sun", "12", "-10", "-3"}
	expectedOutput = []bool{true, true, false, true, false, true}
	EvaluateForInputLookup_Helper(t, boolExprStr, colValues, expectedOutput, valueOps, "Test")

	// Test Invalid ValueOp
	boolExprStr.ValueOp = "invalid"
	_, err := boolExprStr.EvaluateForInputLookup(fieldToValue)
	assert.NotNil(t, err)

	// Test AND
	boolExprAnd := &BoolExpr{
		IsTerminal: false,
		BoolOp:     BoolOpAnd,
		LeftBool:   boolExprStr,
		RightBool:  boolExprNum2,
	}
	expectedOutput = []bool{true, false, true, false, false, true}
	for i, valueOp := range valueOps {
		boolExprStr.ValueOp = valueOp
		fieldToValue["Test"] = sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_STRING,
			CVal:  colStrValues[i],
		}
		boolExprNum2.ValueOp = valueOp
		fieldToValue["Check"] = sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_STRING,
			CVal:  colNumValues[i],
		}
		value, err := boolExprAnd.EvaluateForInputLookup(fieldToValue)
		assert.Nil(t, err)
		assert.Equal(t, expectedOutput[i], value)
	}

	// Test OR
	boolExprOr := &BoolExpr{
		IsTerminal: false,
		BoolOp:     BoolOpOr,
		LeftBool:   boolExprStr,
		RightBool:  boolExprNum2,
	}
	expectedOutput = []bool{true, false, true, true, false, true}
	for i, valueOp := range valueOps {
		boolExprStr.ValueOp = valueOp
		fieldToValue["Test"] = sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_STRING,
			CVal:  colStrValues[i],
		}
		boolExprNum2.ValueOp = valueOp
		fieldToValue["Check"] = sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_STRING,
			CVal:  colNumValues[i],
		}
		value, err := boolExprOr.EvaluateForInputLookup(fieldToValue)
		assert.Nil(t, err)
		assert.Equal(t, expectedOutput[i], value)
	}

	// Test NOT
	boolExprNot := &BoolExpr{
		IsTerminal: false,
		BoolOp:     BoolOpNot,
		LeftBool:   boolExprStr,
	}
	expectedOutput = []bool{false, true, false, true, true, false}

	for i, valueOp := range valueOps {
		boolExprStr.ValueOp = valueOp
		fieldToValue["Test"] = sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_STRING,
			CVal:  colStrValues[i],
		}
		value, err := boolExprNot.EvaluateForInputLookup(fieldToValue)
		assert.Nil(t, err)
		assert.Equal(t, expectedOutput[i], value)
	}
}

func Test_ConditionExpr(t *testing.T) {

	boolExpr :=
		&BoolExpr{
			IsTerminal: true,
			LeftValue: &ValueExpr{
				ValueExprMode: VEMNumericExpr,
				NumericExpr: &NumericExpr{
					NumericExprMode: NEMNumberField,
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "state",
				},
			},
			ValueOp: "in",
			ValueList: []*ValueExpr{
				&ValueExpr{
					ValueExprMode: VEMStringExpr,
					StringExpr: &StringExpr{
						StringExprMode: SEMConcatExpr,
						ConcatExpr: &ConcatExpr{
							Atoms: []*ConcatAtom{
								{IsField: false, Value: "Mary"},
								{IsField: false, Value: "land"},
							},
						},
					},
				},
				&ValueExpr{
					ValueExprMode: VEMStringExpr,
					StringExpr: &StringExpr{
						StringExprMode: SEMRawString,
						RawString:      "Hawaii",
					},
				},
				&ValueExpr{
					ValueExprMode: VEMNumericExpr,
					NumericExpr: &NumericExpr{
						NumericExprMode: NEMNumericExpr,
						Op:              "+",
						Left: &NumericExpr{
							NumericExprMode: NEMNumber,
							IsTerminal:      true,
							ValueIsField:    false,
							Value:           "99",
						},
						Right: &NumericExpr{
							NumericExprMode: NEMNumber,
							IsTerminal:      true,
							ValueIsField:    false,
							Value:           "1",
						},
					},
				},
			},
		}

	conditionExpr := &ConditionExpr{
		Op:       "if",
		BoolExpr: boolExpr,
		TrueValue: &ValueExpr{
			ValueExprMode: VEMNumericExpr,
			NumericExpr: &NumericExpr{
				NumericExprMode: NEMNumberField,
				IsTerminal:      true,
				ValueIsField:    true,
				Value:           "state",
			},
		},
		FalseValue: &ValueExpr{
			ValueExprMode: VEMStringExpr,
			StringExpr: &StringExpr{
				StringExprMode: SEMRawString,
				RawString:      "Error",
			},
		},
	}

	// Test GetFields()
	assert.Equal(t, boolExpr.GetFields(), []string{"state"})

	// Test Evaluate()
	fieldToValue := make(map[string]sutils.CValueEnclosure)
	fieldToValue["state"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "Maryland",
	}

	value, err := boolExpr.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, true)

	fieldToValue["state"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "MarylandTest",
	}

	value, err = boolExpr.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, false)

	fieldToValue["state"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "Hawaii",
	}

	//If true, it should return true value: state
	str, err := conditionExpr.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "Hawaii")

	fieldToValue["state"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "NewYork",
	}

	//If false, it should return false value: "Error"
	str, err = conditionExpr.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "Error")

	fieldToValue["state"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "Maryland",
	}

	str, err = conditionExpr.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "Maryland")

	boolExpr.LeftValue.NumericExpr.Value = "101"
	boolExpr.LeftValue.NumericExpr.ValueIsField = false
	boolExpr.LeftValue.NumericExpr.NumericExprMode = NEMNumber
	value, err = boolExpr.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, false)

	boolExpr.LeftValue.NumericExpr.Value = "100"
	value, err = boolExpr.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, true)

	isStr :=
		&BoolExpr{
			IsTerminal: true,
			LeftValue: &ValueExpr{
				ValueExprMode: VEMNumericExpr,
				NumericExpr: &NumericExpr{
					NumericExprMode: NEMNumberField,
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "country",
				},
			},
			ValueOp: "isstr",
		}
	isStrIf :=
		&ConditionExpr{
			Op:       "if",
			BoolExpr: boolExpr,
			TrueValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "This is a string",
				},
			},
			FalseValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "This is not a string",
				},
			},
		}

	assert.Equal(t, isStr.GetFields(), []string{"country"})
	fieldToValue["country"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "Spain",
	}

	str, err = isStrIf.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "This is a string")

	isNotStr :=
		&BoolExpr{
			IsTerminal: true,
			LeftValue: &ValueExpr{
				ValueExprMode: VEMNumericExpr,
				NumericExpr: &NumericExpr{
					NumericExprMode: NEMNumberField,
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "longitude",
				},
			},
			ValueOp: "isstr",
		}
	isNotStrIf :=
		&ConditionExpr{
			Op:       "if",
			BoolExpr: isNotStr,
			TrueValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "This is a string",
				},
			},
			FalseValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "This is not a string",
				},
			},
		}

	assert.Equal(t, isNotStr.GetFields(), []string{"longitude"})
	fieldToValue["longitude"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_FLOAT,
		CVal:  float64(99.619024),
	}

	str, err = isNotStrIf.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "This is not a string")

	isInt :=
		&BoolExpr{
			IsTerminal: true,
			LeftValue: &ValueExpr{
				ValueExprMode: VEMNumericExpr,
				NumericExpr: &NumericExpr{
					NumericExprMode: NEMNumberField,
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "http_status",
				},
			},
			ValueOp: "isint",
		}
	isIntIf :=
		&ConditionExpr{
			Op:       "if",
			BoolExpr: isInt,
			TrueValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "This is an integer",
				},
			},
			FalseValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "This is not an integer",
				},
			},
		}

	assert.Equal(t, isInt.GetFields(), []string{"http_status"})
	fieldToValue["http_status"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_SIGNED_NUM,
		CVal:  int64(500),
	}

	str, err = isIntIf.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "This is an integer")

	isNotInt :=
		&BoolExpr{
			IsTerminal: true,
			LeftValue: &ValueExpr{
				ValueExprMode: VEMNumericExpr,
				NumericExpr: &NumericExpr{
					NumericExprMode: NEMNumberField,
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "longitude",
				},
			},
			ValueOp: "isint",
		}
	isNotIntIf :=
		&ConditionExpr{
			Op:       "if",
			BoolExpr: isNotInt,
			TrueValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "This is an integer",
				},
			},
			FalseValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "This is not an integer",
				},
			},
		}

	assert.Equal(t, isNotInt.GetFields(), []string{"longitude"})
	fieldToValue["longitude"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_FLOAT,
		CVal:  float64(99.619024),
	}

	str, err = isNotIntIf.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "This is not an integer")

	isNotBool :=
		&BoolExpr{
			IsTerminal: true,
			LeftValue: &ValueExpr{
				ValueExprMode: VEMNumericExpr,
				NumericExpr: &NumericExpr{
					NumericExprMode: NEMNumberField,
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "city",
				},
			},
			ValueOp: "isbool",
		}
	isNotBoolIf :=
		&ConditionExpr{
			Op:       "if",
			BoolExpr: isNotBool,
			TrueValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "This is a boolean value",
				},
			},
			FalseValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "This is not a boolean value",
				},
			},
		}

	assert.Equal(t, isNotBool.GetFields(), []string{"city"})
	fieldToValue["city"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "Boston",
	}

	str, err = isNotBoolIf.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "This is not a boolean value")

	isBool :=
		&BoolExpr{
			IsTerminal: true,
			LeftValue: &ValueExpr{
				ValueExprMode: VEMNumericExpr,
				NumericExpr: &NumericExpr{
					NumericExprMode: NEMNumber,
					IsTerminal:      true,
					ValueIsField:    false,
					Value:           "true",
				},
			},
			ValueOp: "isbool",
		}
	isBoolIf :=
		&ConditionExpr{
			Op:       "if",
			BoolExpr: isBool,
			TrueValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "This is a boolean value",
				},
			},
			FalseValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "This is not a boolean value",
				},
			},
		}

	assert.Equal(t, isBool.GetFields(), []string{})
	fieldToValue["true"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "true",
	}

	str, err = isBoolIf.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "This is a boolean value")

	IsNotNull :=
		&BoolExpr{
			IsTerminal: true,
			LeftValue: &ValueExpr{
				ValueExprMode: VEMNumericExpr,
				NumericExpr: &NumericExpr{
					NumericExprMode: NEMNumberField,
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "state",
				},
			},
			ValueOp: "isbool",
		}
	isNotNullIf :=
		&ConditionExpr{
			Op:       "if",
			BoolExpr: IsNotNull,
			TrueValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "This is a null value",
				},
			},
			FalseValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "This is not a null value",
				},
			},
		}

	assert.Equal(t, IsNotNull.GetFields(), []string{"state"})
	fieldToValue["state"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "Florida",
	}

	str, err = isNotNullIf.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "This is not a null value")

	cidrBoolExpr :=
		&BoolExpr{
			IsTerminal: true,
			LeftValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "192.0.2.0/24",
				},
			},
			RightValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "192.0.2.5",
				},
			},
			ValueOp: "cidrmatch",
		}
	cidrConditionExpr :=
		&ConditionExpr{
			Op:       "if",
			BoolExpr: cidrBoolExpr,
			TrueValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "local",
				},
			},
			FalseValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "not local",
				},
			},
		}

	assert.Equal(t, cidrBoolExpr.GetFields(), []string{})

	str, err = cidrConditionExpr.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "local")

	notCidrMatchBoolExpr :=
		&BoolExpr{
			IsTerminal: true,
			LeftValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "192.0.2.0/24",
				},
			},
			RightValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "192.0.3.1",
				},
			},
			ValueOp: "cidrmatch",
		}
	notCidrMatchConditionExpr :=
		&ConditionExpr{
			Op:       "if",
			BoolExpr: notCidrMatchBoolExpr,
			TrueValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "local",
				},
			},
			FalseValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "not local",
				},
			},
		}
	assert.Equal(t, notCidrMatchBoolExpr.GetFields(), []string{})

	str, err = notCidrMatchConditionExpr.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "not local")

	likeBoolExpr :=
		&BoolExpr{
			IsTerminal: true,
			LeftValue: &ValueExpr{
				ValueExprMode: VEMNumericExpr,
				NumericExpr: &NumericExpr{
					NumericExprMode: NEMNumberField,
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "http_status",
				},
			},
			RightValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "4%",
				},
			},
			ValueOp: "like",
		}
	likeConditionExpr :=
		&ConditionExpr{
			Op:       "if",
			BoolExpr: likeBoolExpr,
			TrueValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "True",
				},
			},
			FalseValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "False",
				},
			},
		}

	assert.Equal(t, likeBoolExpr.GetFields(), []string{"http_status"})
	fieldToValue["http_status"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_SIGNED_NUM,
		CVal:  int64(400),
	}

	str, err = likeConditionExpr.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "True")

	notLikeBoolExpr :=
		&BoolExpr{
			IsTerminal: true,
			LeftValue: &ValueExpr{
				ValueExprMode: VEMNumericExpr,
				NumericExpr: &NumericExpr{
					NumericExprMode: NEMNumberField,
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "http_status",
				},
			},
			RightValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "4%",
				},
			},
			ValueOp: "like",
		}
	notLikeConditionExpr :=
		&ConditionExpr{
			Op:       "if",
			BoolExpr: likeBoolExpr,
			TrueValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "True",
				},
			},
			FalseValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "False",
				},
			},
		}

	assert.Equal(t, notLikeBoolExpr.GetFields(), []string{"http_status"})
	fieldToValue["http_status"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_SIGNED_NUM,
		CVal:  int64(200),
	}

	str, err = notLikeConditionExpr.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "False")

	matchBoolExpr :=
		&BoolExpr{
			IsTerminal: true,
			LeftValue: &ValueExpr{
				ValueExprMode: VEMNumericExpr,
				NumericExpr: &NumericExpr{
					NumericExprMode: NEMNumberField,
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "country",
				},
			},
			RightValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "^Sa",
				},
			},
			ValueOp: "match",
		}
	matchConditionExpr :=
		&ConditionExpr{
			Op:       "if",
			BoolExpr: matchBoolExpr,
			TrueValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "yes",
				},
			},
			FalseValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "no",
				},
			},
		}

	assert.Equal(t, matchBoolExpr.GetFields(), []string{"country"})
	fieldToValue["country"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "Saudi Arabia",
	}

	str, err = matchConditionExpr.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "yes")

	notMatchBoolExpr :=
		&BoolExpr{
			IsTerminal: true,
			LeftValue: &ValueExpr{
				ValueExprMode: VEMNumericExpr,
				NumericExpr: &NumericExpr{
					NumericExprMode: NEMNumberField,
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "country",
				},
			},
			RightValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "^Sa",
				},
			},
			ValueOp: "match",
		}
	notMatchConditionExpr :=
		&ConditionExpr{
			Op:       "if",
			BoolExpr: notMatchBoolExpr,
			TrueValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "yes",
				},
			},
			FalseValue: &ValueExpr{
				ValueExprMode: VEMStringExpr,
				StringExpr: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "no",
				},
			},
		}

	assert.Equal(t, notMatchBoolExpr.GetFields(), []string{"country"})
	fieldToValue["country"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "Jersey",
	}

	str, err = notMatchConditionExpr.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "no")

}

func Test_StringExpr(t *testing.T) {
	strExpr :=
		&StringExpr{
			StringExprMode: SEMTextExpr,
			TextExpr: &TextExpr{
				IsTerminal: false,
				Op:         "urldecode",
				Param: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "http%3A%2F%2Fwww.splunk.com%2Fdownload%3Fr%3Dheader",
				},
			},
		}
	assert.Equal(t, strExpr.GetFields(), []string{})

	// Test Evaluate()
	fieldToValue := make(map[string]sutils.CValueEnclosure)

	value, err := strExpr.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, "http://www.splunk.com/download?r=header")

	strMax :=
		&StringExpr{
			StringExprMode: SEMTextExpr,
			TextExpr: &TextExpr{
				IsTerminal: false,
				Op:         "max",
				ValueList: []*StringExpr{
					{
						StringExprMode: SEMConcatExpr,
						ConcatExpr: &ConcatExpr{
							Atoms: []*ConcatAtom{
								{IsField: false, Value: "1"},
							},
						},
					},

					{
						StringExprMode: SEMConcatExpr,
						ConcatExpr: &ConcatExpr{
							Atoms: []*ConcatAtom{
								{IsField: false, Value: "3"},
							},
						},
					},

					{
						StringExprMode: SEMConcatExpr,
						ConcatExpr: &ConcatExpr{
							Atoms: []*ConcatAtom{
								{IsField: false, Value: "450"},
							},
						},
					},
					{

						StringExprMode: SEMField,
						FieldName:      "http_status",
					},
				},
			},
		}
	assert.Equal(t, strMax.GetFields(), []string{"http_status"})

	// Test Evaluate()
	fieldToValue["http_status"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "200",
	}

	value, err = strMax.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, "450")

	strMin :=
		&StringExpr{
			StringExprMode: SEMTextExpr,
			TextExpr: &TextExpr{
				IsTerminal: false,
				Op:         "min",
				ValueList: []*StringExpr{
					{
						StringExprMode: SEMConcatExpr,
						ConcatExpr: &ConcatExpr{
							Atoms: []*ConcatAtom{
								{IsField: false, Value: "1"},
							},
						},
					},

					{
						StringExprMode: SEMConcatExpr,
						ConcatExpr: &ConcatExpr{
							Atoms: []*ConcatAtom{
								{IsField: false, Value: "3"},
							},
						},
					},

					{
						StringExprMode: SEMConcatExpr,
						ConcatExpr: &ConcatExpr{
							Atoms: []*ConcatAtom{
								{IsField: false, Value: "450"},
							},
						},
					},
					{

						StringExprMode: SEMField,
						FieldName:      "http_status",
					},
				},
			},
		}
	assert.Equal(t, strMin.GetFields(), []string{"http_status"})

	// Test Evaluate()
	fieldToValue["http_status"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "200",
	}

	value, err = strMin.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, "1")

	strSubStr := &StringExpr{
		StringExprMode: SEMConcatExpr,
		ConcatExpr: &ConcatExpr{
			Atoms: []*ConcatAtom{
				{
					IsField: false,
					TextExpr: &TextExpr{
						IsTerminal: false,
						Op:         "substr",
						Param: &StringExpr{
							StringExprMode: SEMRawString,
							RawString:      "splendid",
						},
						StartIndex: &NumericExpr{
							NumericExprMode: NEMNumber,
							IsTerminal:      true,
							ValueIsField:    false,
							Value:           "1",
						},
						LengthExpr: &NumericExpr{
							NumericExprMode: NEMNumber,
							IsTerminal:      true,
							ValueIsField:    false,
							Value:           "3",
						},
					},
				},
				{
					IsField: false,
					TextExpr: &TextExpr{
						IsTerminal: false,
						Op:         "substr",
						Param: &StringExpr{
							StringExprMode: SEMRawString,
							RawString:      "chunk",
						},
						StartIndex: &NumericExpr{
							NumericExprMode: NEMNumber,
							IsTerminal:      true,
							ValueIsField:    false,
							Value:           "-3",
						},
					},
				},
			},
		},
	}
	assert.Equal(t, strSubStr.GetFields(), []string{})

	value, err = strSubStr.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, "splunk")

	strToStringBool :=
		&StringExpr{
			StringExprMode: SEMTextExpr,
			TextExpr: &TextExpr{
				IsTerminal: false,
				Op:         "tostring",
				Val: &ValueExpr{
					ValueExprMode: VEMBooleanExpr,
					BooleanExpr: &BoolExpr{
						IsTerminal: true,
						LeftValue: &ValueExpr{
							ValueExprMode: VEMNumericExpr,
							NumericExpr: &NumericExpr{
								NumericExprMode: NEMNumber,
								IsTerminal:      true,
								ValueIsField:    false,
								Value:           "2",
							},
						},
						RightValue: &ValueExpr{
							ValueExprMode: VEMNumericExpr,
							NumericExpr: &NumericExpr{
								NumericExprMode: NEMNumber,
								IsTerminal:      true,
								ValueIsField:    false,
								Value:           "1",
							},
						},
						ValueOp: ">",
					},
				},
			},
		}
	assert.Equal(t, strToStringBool.GetFields(), []string{})

	value, err = strToStringBool.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, "true")

	strToStringHex :=
		&StringExpr{
			StringExprMode: SEMTextExpr,
			TextExpr: &TextExpr{
				IsTerminal: false,
				Op:         "tostring",
				Val: &ValueExpr{
					ValueExprMode: VEMNumericExpr,
					NumericExpr: &NumericExpr{
						NumericExprMode: NEMNumber,
						IsTerminal:      true,
						ValueIsField:    false,
						Value:           "15",
					},
				},
				Param: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "hex",
				},
			},
		}
	assert.Equal(t, strToStringHex.GetFields(), []string{})

	value, err = strToStringHex.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, "0xf")

	strToStringCommas :=
		&StringExpr{
			StringExprMode: SEMTextExpr,
			TextExpr: &TextExpr{
				IsTerminal: false,
				Op:         "tostring",
				Val: &ValueExpr{
					ValueExprMode: VEMNumericExpr,
					NumericExpr: &NumericExpr{
						NumericExprMode: NEMNumber,
						IsTerminal:      true,
						ValueIsField:    false,
						Value:           "12345.6789",
					},
				},
				Param: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "commas",
				},
			},
		}
	assert.Equal(t, strToStringCommas.GetFields(), []string{})

	value, err = strToStringCommas.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, "12,345.68")

	strToStringDuration :=
		&StringExpr{
			StringExprMode: SEMTextExpr,
			TextExpr: &TextExpr{
				IsTerminal: false,
				Op:         "tostring",
				Val: &ValueExpr{
					ValueExprMode: VEMNumericExpr,
					NumericExpr: &NumericExpr{
						NumericExprMode: NEMNumber,
						IsTerminal:      true,
						ValueIsField:    false,
						Value:           "615",
					},
				},
				Param: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "duration",
				},
			},
		}
	assert.Equal(t, strToStringDuration.GetFields(), []string{})

	value, err = strToStringDuration.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, "00:10:15")

}

func Test_RenameExpr(t *testing.T) {
	renameToPhrase := &RenameExpr{
		RenameExprMode:  REMPhrase,
		OriginalPattern: "city",
		NewPattern:      "test",
	}

	renameRegex := &RenameExpr{
		RenameExprMode:  REMRegex,
		OriginalPattern: "app*",
		NewPattern:      "start*end",
	}

	renameToExistingField := &RenameExpr{
		RenameExprMode:  REMOverride,
		OriginalPattern: "http_status",
		NewPattern:      "",
	}

	assert.Equal(t, []string{"city"}, renameToPhrase.GetFields())
	assert.Equal(t, []string{}, renameRegex.GetFields())
	assert.Equal(t, []string{"http_status"}, renameToExistingField.GetFields())

	fieldToValue := make(map[string]sutils.CValueEnclosure)
	fieldToValue["city"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "Boston",
	}
	fieldToValue["http_status"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "200",
	}

	val, err := renameToPhrase.Evaluate(fieldToValue, renameToPhrase.GetFields()[0])
	assert.Nil(t, err)
	assert.Equal(t, "Boston", val)

	val, err = renameToExistingField.Evaluate(fieldToValue, renameToExistingField.GetFields()[0])
	assert.Nil(t, err)
	assert.Equal(t, "200", val)

	// Test Process Rename Regex logic
	// No match column
	newCol, err := renameRegex.ProcessRenameRegexExpression("http_status")
	assert.Nil(t, err)
	assert.Equal(t, "", newCol)

	newCol, err = renameRegex.ProcessRenameRegexExpression("app_name")
	assert.Nil(t, err)
	assert.Equal(t, "start_nameend", newCol)

	// Multiple wildcards
	renameRegex.OriginalPattern = "ht*_*ta*"
	renameRegex.NewPattern = "first*second*third*end"
	newCol, err = renameRegex.ProcessRenameRegexExpression("http_status")

	assert.Nil(t, err)
	assert.Equal(t, newCol, "firsttpsecondsthirdtusend")

	// Wrong Pattern
	renameRegex.OriginalPattern = "[abc"
	renameRegex.NewPattern = "first*second*third*end"
	_, err = renameRegex.ProcessRenameRegexExpression("ddd")

	assert.NotNil(t, err)

	// Test Remove unused GroupByCols by index
	bucketResult := &BucketResult{
		GroupByKeys: []string{"http_status", "http_method", "city", "state", "gender", "app_name"},
		BucketKey:   []string{"200", "POST", "Boston", "MA", "Male", "sig"},
	}

	renameRegex.RemoveBucketResGroupByColumnsByIndex(bucketResult, []int{3, 1, 4})
	assert.Equal(t, []string{"http_status", "city", "app_name"}, bucketResult.GroupByKeys)
	assert.Equal(t, []string{"200", "Boston", "sig"}, bucketResult.BucketKey.([]string))

	bucketHolder := &BucketHolder{
		GroupByValues: []string{"200", "POST", "Boston", "MA", "Male", "sig"},
	}

	renameRegex.RemoveBucketHolderGroupByColumnsByIndex(bucketHolder, []string{"http_status", "http_method", "city", "state", "gender", "app_name"}, []int{5, 2})
	assert.Equal(t, []string{"200", "POST", "MA", "Male"}, bucketHolder.GroupByValues)
}

func Test_StatisticExpr(t *testing.T) {

	statisticExpr := &StatisticExpr{
		StatisticFunctionMode: SFMRare,
		Limit:                 "2",
		StatisticOptions: &StatisticOptions{
			CountField:   "app_name",
			OtherStr:     "other",
			PercentField: "http_method",
			ShowCount:    true,
			ShowPerc:     true,
			UseOther:     true,
		},
		FieldList: []string{"http_method", "weekday"},
		ByClause:  []string{"app_name"},
	}

	assert.Equal(t, []string{"http_method", "weekday", "app_name"}, statisticExpr.GetFields())

	bucketResult := &BucketResult{
		ElemCount:   333,
		GroupByKeys: []string{"http_method", "weekday", "app_name"},
		BucketKey:   []string{"PUT", "Sunday", "sig"},
	}

	err := statisticExpr.OverrideGroupByCol(bucketResult, 666)
	assert.Nil(t, err)
	assert.Equal(t, []string{"50.000000", "Sunday", "333"}, bucketResult.BucketKey.([]string))

	bucketResult1 := &BucketResult{
		ElemCount:   333,
		GroupByKeys: []string{"http_method", "http_status", "weekday", "state", "gender", "app_name"},
		BucketKey:   []string{"POST", "404", "Sunday", "MA", "Male", "sig"},
	}

	bucketResult2 := &BucketResult{
		ElemCount:   111,
		GroupByKeys: []string{"http_method", "http_status", "weekday", "state", "gender", "app_name"},
		BucketKey:   []string{"Get", "200", "Tuesday", "LA", "Male", "test"},
	}

	bucketResult3 := &BucketResult{
		ElemCount:   222,
		GroupByKeys: []string{"http_method", "http_status", "weekday", "state", "gender", "app_name"},
		BucketKey:   []string{"PUT", "501", "Monday", "NH", "Femali", "sig_test"},
	}

	// Test Sorting func. If use the limit option, only the last limit lexicographical of the <field-list> is returned in the search results
	results := append(make([]*BucketResult, 0), bucketResult1, bucketResult2, bucketResult3)
	err = statisticExpr.SortBucketResult(&results)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(results))
	assert.Equal(t, bucketResult2, results[0])
	assert.Equal(t, bucketResult1, results[1])
}

func TestFormatTime(t *testing.T) {
	cases := []struct {
		time     time.Time
		format   string
		expected string
	}{
		{
			time:     time.Date(2023, 3, 14, 1, 59, 26, 0, time.UTC),
			format:   "%Y-%m-%d %H:%M:%S",
			expected: "2023-03-14 01:59:26",
		},
		{
			time:     time.Date(2020, 12, 31, 23, 59, 59, 999999000, time.UTC),
			format:   "%Y-%m-%d %I:%M:%S %p %f",
			expected: "2020-12-31 11:59:59 PM .999999",
		},
		{
			time:     time.Date(1999, 1, 1, 15, 0, 0, 0, time.UTC),
			format:   "%A, %B %d, %Y",
			expected: "Friday, January 01, 1999",
		},
		{
			time:     time.Date(2023, 3, 14, 1, 59, 26, 0, time.UTC),
			format:   "%F %T",
			expected: "2023-03-14 01:59:26",
		},
		{
			time:     time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
			format:   "%s",
			expected: "0",
		},
		{
			time:     time.Date(2022, 2, 28, 12, 34, 56, 789000000, time.Local),
			format:   "%c",
			expected: "Mon Feb 28 12:34:56 2022",
		},
		{
			time:     time.Date(2021, 10, 10, 10, 10, 10, 10000000, time.UTC),
			format:   "%x %X %N %Q",
			expected: "10/10/21 10:10:10 010000000 10",
		},
		{
			time:     time.Date(1998, 5, 17, 0, 0, 0, 0, time.UTC),
			format:   "%V",
			expected: "20",
		},
		{
			time:     time.Date(2000, 1, 2, 3, 4, 5, 678000000, time.UTC),
			format:   "%Y-%m-%d %H:%M:%S%f",
			expected: "2000-01-02 03:04:05.678000",
		},
		{
			time:     time.Date(2000, 12, 25, 23, 59, 59, 999000000, time.UTC),
			format:   "%c",
			expected: "Mon Dec 25 23:59:59 2000",
		},
	}

	for _, c := range cases {
		formattedTime := formatTime(c.time, c.format)
		if formattedTime != c.expected {
			t.Errorf("Test failed for time: %v, format: %s, expected: %s, got: %s",
				c.time, c.format, c.expected, formattedTime)
		}
	}
}

func TestParseTime(t *testing.T) {
	tests := []struct {
		dateStr     string
		format      string
		expected    time.Time
		shouldError bool
	}{
		{
			dateStr:     "01-02-2006 15:04:05",
			format:      "%d-%m-%Y %H:%M:%S",
			expected:    time.Date(2006, time.February, 1, 15, 4, 5, 0, time.UTC),
			shouldError: false,
		},
		{
			dateStr:     "01:08 PM",
			format:      "%I:%M %p",
			expected:    time.Date(1970, time.January, 1, 13, 8, 0, 0, time.UTC),
			shouldError: false,
		},
		{
			dateStr:     "31/12/99",
			format:      "%d/%m/%y",
			expected:    time.Date(1999, time.December, 31, 0, 0, 0, 0, time.UTC),
			shouldError: false,
		},
		{
			dateStr:     "Monday, 01-January-06 15:04",
			format:      "%A, %d-%B-%y %H:%M",
			expected:    time.Date(2006, time.January, 1, 15, 4, 0, 0, time.UTC),
			shouldError: false,
		},
		{
			dateStr:     "invalid date",
			format:      "%d-%m-%Y",
			expected:    time.Time{},
			shouldError: true,
		},
	}

	for _, test := range tests {
		got, err := parseTime(test.dateStr, test.format)
		if test.shouldError && err == nil {
			t.Errorf("parseTime(%q, %q) expected an error, but got nil", test.dateStr, test.format)
		} else if !test.shouldError && err != nil {
			t.Errorf("parseTime(%q, %q) unexpected error: %v", test.dateStr, test.format, err)
		} else if !test.shouldError && !got.Equal(test.expected) {
			t.Errorf("parseTime(%q, %q) = %v, want %v", test.dateStr, test.format, got, test.expected)
		}
	}
}

func Test_MultiValueExpr(t *testing.T) {
	mvExpr := &MultiValueExpr{
		MultiValueExprMode: MVEMMultiValueExpr,
		Op:                 "split",
		StringExprParams: []*StringExpr{
			{
				StringExprMode: SEMField,
				FieldName:      "test_field",
			},
			{
				StringExprMode: SEMRawString,
				RawString:      ":",
			},
		},
	}
	assert.Equal(t, mvExpr.GetFields(), []string{"test_field"})

	fieldToValue := make(map[string]sutils.CValueEnclosure)
	fieldToValue["test_field"] = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  "a:dc:b2c:123",
	}

	value, err := mvExpr.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.True(t, utils.CompareStringSlices(value, []string{"a", "dc", "b2c", "123"}))

	strExpr := &StringExpr{
		StringExprMode: SEMTextExpr,
		TextExpr: &TextExpr{
			Op:             "mvjoin",
			MultiValueExpr: mvExpr,
			Delimiter: &StringExpr{
				StringExprMode: SEMRawString,
				RawString:      "?",
			},
		},
	}
	assert.Equal(t, strExpr.GetFields(), []string{"test_field"})

	joinedStr, err := strExpr.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, "a?dc?b2c?123", joinedStr)

	strExpr2 := &StringExpr{
		StringExprMode: SEMTextExpr,
		TextExpr: &TextExpr{
			Op:             "mvcount",
			MultiValueExpr: mvExpr,
		},
	}
	assert.Equal(t, strExpr.GetFields(), []string{"test_field"})

	countVal, err := strExpr2.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, "4", countVal)

	gobRegex := utils.GobbableRegex{}
	err = gobRegex.SetRegex("b2*")
	assert.Nil(t, err)

	strExpr3 := &StringExpr{
		StringExprMode: SEMTextExpr,
		TextExpr: &TextExpr{
			Op:             "mvfind",
			MultiValueExpr: mvExpr,
			Regex:          &gobRegex,
		},
	}
	assert.Equal(t, strExpr3.GetFields(), []string{"test_field"})

	foundVal, err := strExpr3.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, "2", foundVal)

	err = gobRegex.SetRegex("b3c")
	assert.Nil(t, err)
	strExpr3.TextExpr.Regex = &gobRegex

	foundVal, err = strExpr3.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, "", foundVal)

	mvExpr2 := &MultiValueExpr{
		MultiValueExprMode: MVEMMultiValueExpr,
		Op:                 "mvindex",
		MultiValueExprParams: []*MultiValueExpr{
			mvExpr,
		},
		NumericExprParams: []*NumericExpr{
			{
				NumericExprMode: NEMNumber,
				IsTerminal:      true,
				Value:           "1",
			},
		},
	}
	assert.Equal(t, mvExpr2.GetFields(), []string{"test_field"})

	value, err = mvExpr2.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.True(t, utils.CompareStringSlices(value, []string{"dc"}))

	mvExpr2.NumericExprParams = append(mvExpr2.NumericExprParams, &NumericExpr{
		NumericExprMode: NEMNumber,
		IsTerminal:      true,
		Value:           "2",
	})

	value, err = mvExpr2.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.True(t, utils.CompareStringSlices(value, []string{"dc", "b2c"}))
}

func Test_GetDefaultTimechartSpanOptions(t *testing.T) {
	type args struct {
		startEpoch uint64
		endEpoch   uint64
		qid        uint64
	}
	tests := []struct {
		name    string
		args    args
		want    *SpanOptions
		wantErr bool
	}{
		{"startEpoch = 0 should be error", args{0, 1, 1}, nil, true},
		{"endEpoch = 0 should be error", args{1, 0, 1}, nil, true},
		{"<15*60*1000 should be TMSecond with Num = 10",
			args{1, 5*60*1000 + 1, 1},
			&SpanOptions{SpanLength: &SpanLength{Num: 10, TimeScalr: sutils.TMSecond}, DefaultSettings: false},
			false},
		{"15*60*1000 should be TMSecond with Num = 10",
			args{1, 15*60*1000 + 1, 1},
			&SpanOptions{SpanLength: &SpanLength{Num: 10, TimeScalr: sutils.TMSecond}, DefaultSettings: false},
			false},
		{"<60*60*1000 should be TMMinute with Num = 1",
			args{1, 30*60*1000 + 1, 1},
			&SpanOptions{SpanLength: &SpanLength{Num: 1, TimeScalr: sutils.TMMinute}, DefaultSettings: false},
			false},
		{"60*60*1000 should be TMMinute with Num = 1",
			args{1, 60*60*1000 + 1, 1},
			&SpanOptions{SpanLength: &SpanLength{Num: 1, TimeScalr: sutils.TMMinute}, DefaultSettings: false},
			false},
		{"<4*60*60*1000 should be TMMinute with Num = 5",
			args{1, 2*60*60*1000 + 1, 1},
			&SpanOptions{SpanLength: &SpanLength{Num: 5, TimeScalr: sutils.TMMinute}, DefaultSettings: false},
			false},
		{"4*60*60*1000 should be TMMinute with Num = 5",
			args{1, 4*60*60*1000 + 1, 1},
			&SpanOptions{SpanLength: &SpanLength{Num: 5, TimeScalr: sutils.TMMinute}, DefaultSettings: false},
			false},
		{"<24*60*60*1000 should be TMMinute with Num = 30",
			args{1, 20*60*60*1000 + 1, 1},
			&SpanOptions{SpanLength: &SpanLength{Num: 30, TimeScalr: sutils.TMMinute}, DefaultSettings: false},
			false},
		{"24*60*60*1000 should be TMMinute with Num = 30",
			args{1, 24*60*60*1000 + 1, 1},
			&SpanOptions{SpanLength: &SpanLength{Num: 30, TimeScalr: sutils.TMMinute}, DefaultSettings: false},
			false},
		{"<7*24*60*60*1000 should be TMHour with Num = 1",
			args{1, 6*24*60*60*1000 + 1, 1},
			&SpanOptions{SpanLength: &SpanLength{Num: 1, TimeScalr: sutils.TMHour}, DefaultSettings: false},
			false},
		{"7*24*60*60*1000 should be TMHour with Num = 1",
			args{1, 7*24*60*60*1000 + 1, 1},
			&SpanOptions{SpanLength: &SpanLength{Num: 1, TimeScalr: sutils.TMHour}, DefaultSettings: false},
			false},
		{"<180*24*60*60*1000 should be TMDay with Num = 1",
			args{1, 179*24*60*60*1000 + 1, 1},
			&SpanOptions{SpanLength: &SpanLength{Num: 1, TimeScalr: sutils.TMDay}, DefaultSettings: false},
			false},
		{"180*24*60*60*1000 should be TMDay with Num = 1",
			args{1, 180*24*60*60*1000 + 1, 1},
			&SpanOptions{SpanLength: &SpanLength{Num: 1, TimeScalr: sutils.TMDay}, DefaultSettings: false},
			false},
		{">180*24*60*60*1000 should be TMDay with Num = 1",
			args{1, 181*24*60*60*1000 + 1, 1},
			&SpanOptions{SpanLength: &SpanLength{Num: 1, TimeScalr: sutils.TMMonth}, DefaultSettings: false},
			false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetDefaultTimechartSpanOptions(tt.args.startEpoch, tt.args.endEpoch, tt.args.qid)
			assert.Equal(t, err != nil, tt.wantErr)
			assert.Equal(t, got, tt.want)
		})
	}
}

func Test_Spath_JSON(t *testing.T) {

	type args struct {
		data          string
		path          string
		expectedValue []string
	}

	var testCases = []args{
		{
			data:          `{"name": "John", "age": 30, "city": "New York"}`,
			path:          `city`,
			expectedValue: []string{`New York`},
		},
		{
			data:          `{"name": "John", "age": 30, "city": "New York"}`,
			path:          `address.city`,
			expectedValue: nil,
		},
		{
			data:          `[{"name": "John", "age": 30, "city": "New York"}, {"name": "Alice", "age": 25, "city": "Los Angeles"}]`,
			path:          `{0}.city`,
			expectedValue: []string{`New York`},
		},
		{
			data:          `[{"name": "John", "age": 30, "city": "New York"}, {"name": "Alice", "age": 25, "city": "Los Angeles"}]`,
			path:          `{}.city`,
			expectedValue: []string{`New York`, `Los Angeles`},
		},
		{
			// checking all types
			// the array [1, 2] is absent in expectedValue; the path should be {}.data{} to access the array
			data:          `[{"data": {"field": "value"}}, {"data": true}, {"data": false}, {"data": null}, {"data": [1, 2]}, {"data": 3}, {"data": 3.5}, {"data": "four"}]`,
			path:          `{}.data`,
			expectedValue: []string{`{"field":"value"}`, `true`, `false`, `null`, `3`, `3.5`, `four`},
		},
		{
			data:          `{"people": [{"pets": ["Cat", "Dog"]}, {"pets": ["Goldfish", "Parrot"]}, {"name": "Bob"}]}`,
			path:          `people{}.pets{}`,
			expectedValue: []string{`Cat`, `Dog`, `Goldfish`, `Parrot`},
		},
		{
			// the JSON module we're using produces a string with the fields sorted lexicographically
			data:          `[{"name": "John", "age": 30, "city": "New York"}, {"name": "Alice", "age": 25, "city": "Los Angeles"}]`,
			path:          `{}`,
			expectedValue: []string{`{"age":30,"city":"New York","name":"John"}`, `{"age":25,"city":"Los Angeles","name":"Alice"}`},
		},
	}

	for _, testCase := range testCases {
		actualValue := extractInnerJSONObj(testCase.data, testCase.path)
		assert.Equal(t, testCase.expectedValue, actualValue)
	}
}

func Test_Spath_XML(t *testing.T) {

	type args struct {
		data          string
		path          string
		expectedValue []string
	}

	var testCases = []args{
		{
			data:          `<person><name>John</name><age>30</age><city>New York</city></person>`,
			path:          `person.city`,
			expectedValue: []string{`New York`},
		},
		{
			data:          `<person><name>John</name><age>30</age><city>New York</city></person>`,
			path:          `person.address.city`,
			expectedValue: nil,
		},
		{
			data:          `<people><person><name>John</name><age>30</age><city>New York</city></person><person><name>Alice</name><age>25</age><city>Los Angeles</city></person></people>`,
			path:          `people.person{1}.city`, // XML uses 1-indexing
			expectedValue: []string{`New York`},
		},
		{
			data:          `<people><person><name>John</name><age>30</age><city>New York</city></person><person><name>Alice</name><age>25</age><city>Los Angeles</city></person></people>`,
			path:          `people.person.city`,
			expectedValue: []string{`New York`, `Los Angeles`},
		},
		{
			data:          `<people><person><pet>Cat</pet><pet>Dog</pet></person><person><pet>Hamster</pet><pet>Goldfish</pet></person></people>`,
			path:          `people.person.pet`,
			expectedValue: []string{`Cat`, `Dog`, `Hamster`, `Goldfish`},
		},
		{
			data:          `<people><person><pet legs="4">Cat</pet><pet legs="4">Dog</pet></person><person><pet>Goldfish</pet><pet legs="2.0">Parrot</pet></person></people>`,
			path:          `people.person.pet{@legs}`,
			expectedValue: []string{`4`, `4`, `2.0`},
		},
		{
			data:          `<people><person></person></people>`,
			path:          `people.person`,
			expectedValue: nil,
		},
	}

	for _, testCase := range testCases {
		actualValue := extractInnerXMLObj(testCase.data, testCase.path)
		assert.Equal(t, testCase.expectedValue, actualValue)
	}
}

func Test_Spath_XML_Unclear(t *testing.T) {
	// this function contains some cases where we're not sure what the result should be
	// but we return the same result that Splunk returns

	type args struct {
		data          string
		path          string
		expectedValue []string
	}

	var testCases = []args{
		{
			data:          `<root>a<tag/>b<tag2/>c<tag3/>d</root>`,
			path:          `root`,
			expectedValue: []string{`a`, `b`, `c`, `d`, `<tag/>b<tag2/>c<tag3/>`},
		},
		{
			data:          `<root><tag/>b<tag2/>c<tag3/></root>`,
			path:          `root`,
			expectedValue: []string{`b`, `c`, `<tag/>b<tag2/>c<tag3/>`},
		},
		{
			data:          `<root>a<tag/><tag2/><tag3/>d</root>`,
			path:          `root`,
			expectedValue: []string{`a`, `d`, `<tag/><tag2/><tag3/>`},
		},
		{
			data:          `<root><tag/><tag2/><tag3/></root>`,
			path:          `root`,
			expectedValue: []string{`<tag/><tag2/><tag3/>`},
		},
		{
			data:          `<root>pre<tag/>mid<tag/>post</root>`,
			path:          `root`,
			expectedValue: []string{`pre`, `mid`, `post`, `<tag/>mid<tag/>`},
		},
		{
			data:          `<root><a>1<b/>2<c/>3</a>4</root>`,
			path:          `root.a`,
			expectedValue: []string{`1`, `2`, `3`, `<b/>2<c/>`},
		},
		{
			data:          `<root><a>1<b/>2<c/>3</a>4</root>`,
			path:          `root`,
			expectedValue: []string{`4`, `<a>1<b/>2<c/>3</a>`},
		},
		{
			data:          `<root>start<item id="x"/>mid<item id="y"/>stop</root>`,
			path:          `root`,
			expectedValue: []string{`start`, `mid`, `stop`, `<item id="x"/>mid<item id="y"/>`},
		},
	}

	for _, testCase := range testCases {
		actualValue := extractInnerXMLObj(testCase.data, testCase.path)
		assert.Equal(t, testCase.expectedValue, actualValue)
	}
}
