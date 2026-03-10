// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func Test_EvalCommand(t *testing.T) {
	// SPL command: | eval evalField = if(state in("Massa"."chusetts", "RAW_STRING", 99+1), numField, country)
	boolExpr := &structs.BoolExpr{
		IsTerminal: true,
		LeftValue: &structs.ValueExpr{
			ValueExprMode: structs.VEMNumericExpr,
			NumericExpr: &structs.NumericExpr{
				NumericExprMode: structs.NEMNumberField,
				IsTerminal:      true,
				ValueIsField:    true,
				Value:           "state",
			},
		},
		ValueOp: "in",
		ValueList: []*structs.ValueExpr{
			&structs.ValueExpr{
				ValueExprMode: structs.VEMStringExpr,
				StringExpr: &structs.StringExpr{
					StringExprMode: structs.SEMConcatExpr,
					ConcatExpr: &structs.ConcatExpr{
						Atoms: []*structs.ConcatAtom{
							{IsField: false, Value: "Massa"},
							{IsField: false, Value: "chusetts"},
						},
					},
				},
			},
			&structs.ValueExpr{
				ValueExprMode: structs.VEMStringExpr,
				StringExpr: &structs.StringExpr{
					StringExprMode: structs.SEMRawString,
					RawString:      "RAW_STRING",
				},
			},
			&structs.ValueExpr{
				ValueExprMode: structs.VEMNumericExpr,
				NumericExpr: &structs.NumericExpr{
					NumericExprMode: structs.NEMNumericExpr,
					Op:              "+",
					Left: &structs.NumericExpr{
						NumericExprMode: structs.NEMNumber,
						IsTerminal:      true,
						ValueIsField:    false,
						Value:           "99",
					},
					Right: &structs.NumericExpr{
						NumericExprMode: structs.NEMNumber,
						IsTerminal:      true,
						ValueIsField:    false,
						Value:           "1",
					},
				},
			},
		},
	}

	evalProcessor := &evalProcessor{
		options: &structs.EvalExpr{
			ValueExpr: &structs.ValueExpr{
				ValueExprMode: structs.VEMConditionExpr,
				ConditionExpr: &structs.ConditionExpr{
					Op:       "if",
					BoolExpr: boolExpr,
					TrueValue: &structs.ValueExpr{
						ValueExprMode: structs.VEMNumericExpr,
						NumericExpr: &structs.NumericExpr{
							NumericExprMode: structs.NEMNumberField,
							IsTerminal:      true,
							ValueIsField:    true,
							Value:           "numField",
						},
					},
					FalseValue: &structs.ValueExpr{
						ValueExprMode: structs.VEMNumericExpr,
						NumericExpr: &structs.NumericExpr{
							NumericExprMode: structs.NEMNumberField,
							IsTerminal:      true,
							ValueIsField:    true,
							Value:           "country",
						},
					},
				},
			},
			FieldName: "evalField",
		},
	}

	inputIqr := iqr.NewIQR(0)
	knownValues := map[string][]sutils.CValueEnclosure{
		"state": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "Massachusetts"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "California"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "New York"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "RAW_STRING"},
		},
		"numField": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(1)},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(2)},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(3)},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(4)},
		},
		"country": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "USA"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "India"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "China"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "Japan"},
		},
	}

	expected := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(1)},
		{Dtype: sutils.SS_DT_STRING, CVal: "India"},
		{Dtype: sutils.SS_DT_STRING, CVal: "China"},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(4)},
	}

	err := inputIqr.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	assert.Equal(t, 4, inputIqr.NumberOfRecords())
	cols, err := inputIqr.GetColumns()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"state", "numField", "country"}, utils.GetKeysOfMap(cols))

	iqr, err := evalProcessor.Process(inputIqr)
	assert.NoError(t, err)

	assert.Equal(t, 4, iqr.NumberOfRecords())
	cols, err = iqr.GetColumns()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"state", "numField", "country", "evalField"}, utils.GetKeysOfMap(cols))

	records, err := iqr.ReadColumn("evalField")
	assert.NoError(t, err)
	assert.Equal(t, 4, len(records))
	assert.Equal(t, expected, records)
}
