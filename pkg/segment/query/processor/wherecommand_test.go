// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func getWhereCommandProcessorForTest() *whereProcessor {
	return &whereProcessor{}
}

func getTestKnownValues() map[string][]sutils.CValueEnclosure {
	return map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		},
		"col2": {
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(1)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(2)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(3)},
		},
	}
}

func Test_WhereCommandProcess_NoResults(t *testing.T) {
	processor := getWhereCommandProcessorForTest()
	boolExpr := &structs.BoolExpr{
		IsTerminal: true,
		LeftValue: &structs.ValueExpr{
			ValueExprMode: structs.VEMNumericExpr,
			NumericExpr: &structs.NumericExpr{
				NumericExprMode: structs.NEMNumberField,
				IsTerminal:      true,
				ValueIsField:    true,
				Value:           "col2",
			},
		},
		RightValue: &structs.ValueExpr{
			ValueExprMode: structs.VEMNumericExpr,
			NumericExpr: &structs.NumericExpr{
				NumericExprMode: structs.NEMNumericExpr,
				IsTerminal:      false,
				Op:              "+",
				Left: &structs.NumericExpr{
					NumericExprMode: structs.NEMNumberField,
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "col1",
				},
				Right: &structs.NumericExpr{
					NumericExprMode: structs.NEMNumber,
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "1",
				},
			},
		},
		ValueOp: "=",
	}

	processor.options = boolExpr

	iqr := iqr.NewIQR(0)
	err := iqr.AppendKnownValues(getTestKnownValues())
	assert.NoError(t, err)

	iqr, err = processor.Process(iqr)
	assert.NoError(t, err)

	col1Values, err := iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(col1Values))
}
