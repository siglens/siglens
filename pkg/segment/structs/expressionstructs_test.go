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
