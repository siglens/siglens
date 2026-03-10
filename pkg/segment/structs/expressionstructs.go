// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package structs

import (
	"github.com/siglens/siglens/pkg/config"
	. "github.com/siglens/siglens/pkg/segment/utils"
)

// only one field will be non-nil
// literal can either be a string or a json.Number
type ExpressionInput struct {
	ColumnValue         *DtypeEnclosure // column value: "0", "abc", "abcd*", "0.213". This value will be normalized to Lower Case if the search is case insensitive.
	OriginalColumnValue *DtypeEnclosure // original column value. Similar to Column Value, but is only created when the search is case insensitive
	ColumnName          string          // column name for expression: "col1", "col2", ... "colN"
}

// expressions are used for SegReaders to parse and search segment files
// If expressionOp == nil, only leftInput is not nil
// else, only one of left/right ExpressionInput literal or columnName will be non empty
//   - right expressionInput may not exist if op only needs one input (i.e. NOT_NULL)
//
// i.e field2 * 0.2, is Expression{leftInput=ExpressionInput{columnName=field2}, op=Multiply, rightInput=ExpressionInput{literal=0.2}}
// for just literal  39, Expression{leftInput=ExpressionInput{literal=39}}
type Expression struct {
	LeftInput    *ExpressionInput   // left expression input for operator
	ExpressionOp ArithmeticOperator // operator, used if complex expression that relates keys
	RightInput   *ExpressionInput   // right expression input for operator
}

func (exp *Expression) IsTimeExpression() bool {
	if exp.LeftInput != nil && len(exp.LeftInput.ColumnName) > 0 {
		if exp.LeftInput.ColumnName == "*" {
			return true
		}
		return exp.LeftInput.ColumnName == config.GetTimeStampKey()
	}
	if exp.RightInput != nil && len(exp.RightInput.ColumnName) > 0 {
		if exp.RightInput.ColumnName == "*" {
			return true
		}
		return exp.RightInput.ColumnName == config.GetTimeStampKey()
	}
	return false
}
