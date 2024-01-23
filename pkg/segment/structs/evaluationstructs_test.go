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
	"testing"
	"time"

	segutils "github.com/siglens/siglens/pkg/segment/utils"
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
	fieldToValue := make(map[string]segutils.CValueEnclosure)
	fieldToValue["FieldWithStrings"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
		CVal:  "testing",
	}
	fieldToValue["FieldWithNumbers"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_SIGNED_NUM,
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
	assert.True(t, utils.SliceContainsString(eFields, "Min"))

	assert.True(t, utils.SliceContainsString(eFields, "Max"))

	// Test Evaluate()
	fieldToValue := make(map[string]segutils.CValueEnclosure)
	fieldToValue["Min"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_FLOAT,
		CVal:  float64(12),
	}

	fieldToValue["Max"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_SIGNED_NUM,
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

	fieldToValue["http_status"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_SIGNED_NUM,
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
	fieldToValue = make(map[string]segutils.CValueEnclosure)

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
	fieldToValue := make(map[string]segutils.CValueEnclosure)
	fieldToValue["Max"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_FLOAT,
		CVal:  float64(62),
	}
	fieldToValue["Seconds"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_SIGNED_NUM,
		CVal:  int64(42),
	}
	fieldToValue["FieldWithStrings"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
		CVal:  "testing",
	}
	fieldToValue["FieldWithNumbers"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_SIGNED_NUM,
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
	fieldToValue = make(map[string]segutils.CValueEnclosure)
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
	assert.True(t, utils.SliceContainsString(cFields, "Max"))
	assert.True(t, utils.SliceContainsString(cFields, "FieldWithStrings"))
	assert.True(t, utils.SliceContainsString(cFields, "FieldWithNumbers"))

	dFields := boolExprD.GetFields()
	assert.True(t, utils.SliceContainsString(dFields, "Max"))
	assert.True(t, utils.SliceContainsString(dFields, "FieldWithStrings"))
	assert.True(t, utils.SliceContainsString(dFields, "FieldWithNumbers"))

	assert.Equal(t, boolExprE.GetFields(), []string{"Max"})

	// Test Evaluate()
	fieldToValue := make(map[string]segutils.CValueEnclosure)
	fieldToValue["Max"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_FLOAT,
		CVal:  float64(62),
	}
	fieldToValue["FieldWithStrings"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
		CVal:  "testing",
	}
	fieldToValue["FieldWithNumbers"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_SIGNED_NUM,
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

	_, err = boolExprBadOpForStringValues.Evaluate(fieldToValue)
	assert.NotNil(t, err)

	// When fieldToValue is missing fields, Evaluate() should error.
	delete(fieldToValue, "Max")
	delete(fieldToValue, "FieldWithNumbers")
	_, err = boolExprA.Evaluate(fieldToValue)
	assert.NotNil(t, err)

	_, err = boolExprB.Evaluate(fieldToValue)
	assert.NotNil(t, err)

	_, err = boolExprC.Evaluate(fieldToValue)
	assert.NotNil(t, err)

	_, err = boolExprD.Evaluate(fieldToValue)
	assert.NotNil(t, err)

	_, err = boolExprE.Evaluate(fieldToValue)
	assert.NotNil(t, err)
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
	fieldToValue := make(map[string]segutils.CValueEnclosure)
	fieldToValue["state"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
		CVal:  "Maryland",
	}

	value, err := boolExpr.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, true)

	fieldToValue["state"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
		CVal:  "MarylandTest",
	}

	value, err = boolExpr.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, false)

	fieldToValue["state"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
		CVal:  "Hawaii",
	}

	//If true, it should return true value: state
	str, err := conditionExpr.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "Hawaii")

	fieldToValue["state"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
		CVal:  "NewYork",
	}

	//If false, it should return false value: "Error"
	str, err = conditionExpr.EvaluateCondition(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, str, "Error")

	fieldToValue["state"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
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
	fieldToValue["country"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
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
	fieldToValue["longitude"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_FLOAT,
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
	fieldToValue["http_status"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_SIGNED_NUM,
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
	fieldToValue["longitude"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_FLOAT,
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
	fieldToValue["city"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
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
	fieldToValue["true"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
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
	fieldToValue["state"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
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
	fieldToValue["http_status"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_SIGNED_NUM,
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
	fieldToValue["http_status"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_SIGNED_NUM,
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
	fieldToValue["country"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
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
	fieldToValue["country"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
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
				Value: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "http%3A%2F%2Fwww.splunk.com%2Fdownload%3Fr%3Dheader",
				},
			},
		}
	assert.Equal(t, strExpr.GetFields(), []string{})

	// Test Evaluate()
	fieldToValue := make(map[string]segutils.CValueEnclosure)

	value, err := strExpr.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, "http://www.splunk.com/download?r=header")

	strExpr1 :=
		&StringExpr{
			StringExprMode: SEMTextExpr,
			TextExpr: &TextExpr{
				IsTerminal: false,
				Op:         "split",
				Value: &StringExpr{
					StringExprMode: SEMField,
					FieldName:      "ident",
				},
				Delimiter: &StringExpr{
					StringExprMode: SEMRawString,
					RawString:      "-",
				},
			},
		}
	assert.Equal(t, strExpr1.GetFields(), []string{"ident"})

	// Test Evaluate()
	fieldToValue["ident"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
		CVal:  "a111d29d-dd70-48b2-8987-a807b4b8bbae",
	}

	value, err = strExpr1.Evaluate(fieldToValue)
	assert.Nil(t, err)
	assert.Equal(t, value, "a111d29d&nbspdd70&nbsp48b2&nbsp8987&nbspa807b4b8bbae")

	strMax :=
		&StringExpr{
			StringExprMode: SEMTextExpr,
			TextExpr: &TextExpr{
				IsTerminal: false,
				Op:         "max",
				MaxMinValues: []*StringExpr{
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
	fieldToValue["http_status"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
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
				MaxMinValues: []*StringExpr{
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
	fieldToValue["http_status"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
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
						Value: &StringExpr{
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
						Value: &StringExpr{
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
				Format: &StringExpr{
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
				Format: &StringExpr{
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
				Format: &StringExpr{
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

	fieldToValue := make(map[string]segutils.CValueEnclosure)
	fieldToValue["city"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
		CVal:  "Boston",
	}
	fieldToValue["http_status"] = segutils.CValueEnclosure{
		Dtype: segutils.SS_DT_STRING,
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

	assert.Equal(t, []string{"http_method", "weekday", "app_name"}, statisticExpr.GetGroupByCols())

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
