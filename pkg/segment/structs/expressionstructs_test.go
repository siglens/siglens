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

// TODO: uncomment after zero-copy complex relation
// func Test_evaluateExpression(t *testing.T) {

// 	literalEnc, err := CreateDtypeEnclosure(0.6)
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	simpleExpression := &Expression{
// 		LeftInput:    &ExpressionInput{ColumnValue: literalEnc},
// 		ExpressionOp: Add,
// 		RightInput:   &ExpressionInput{ColumnName: "col1"},
// 	}

// 	valueMap := make([]*RecordEntry, 0)

// 	entry1 := &RecordEntry{
// 		ColumnName:      []byte("col1"),
// 		ColumnValueType: VALTYPE_ENC_FLOAT64[0],
// 		ColumnValue:     utils.Float64ToBytesLittleEndian(float64(0.5)),
// 	}
// 	valueMap = append(valueMap, entry1)

// 	retVal, valid := simpleExpression.Evaluate(valueMap)
// 	assert.Equal(t, retVal.(float64), float64(1.1), "test substitution of map values for similar types")
// 	assert.Nil(t, valid)

// 	valueMap[0].ColumnValue = []byte("invalid")
// 	valueMap[0].ColumnValueType = VALTYPE_ENC_SMALL_STRING[0]
// 	_, inValid := simpleExpression.Evaluate(valueMap) // will fail as string addition is not supported
// 	assert.NotNil(t, inValid, "test invalid operation for converted datatype")

// 	valueMap[0].ColumnValue = utils.Uint64ToBytesLittleEndian(10)
// 	valueMap[0].ColumnValueType = VALTYPE_ENC_UINT64[0]
// 	res, inValid := simpleExpression.Evaluate(valueMap) // will fail as 0.6 can't get converted to uint
// 	assert.Nil(t, inValid)
// 	assert.Equal(t, 10.6, res)

// 	missingColumn := &Expression{
// 		LeftInput:    &ExpressionInput{ColumnValue: literalEnc},
// 		ExpressionOp: Add,
// 		RightInput:   &ExpressionInput{ColumnName: "col2"},
// 	}
// 	_, missing := missingColumn.Evaluate(valueMap)
// 	assert.NotNil(t, missing, "cannot evaluate expression if column does not exist in map")
// }
