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

package processor

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
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
	knownValues := map[string][]utils.CValueEnclosure{
		"state": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "Massachusetts"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "California"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "New York"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "RAW_STRING"},
		},
		"numField": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(1)},
			utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(2)},
			utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(3)},
			utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(4)},
		},
		"country": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "USA"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "India"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "China"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "Japan"},
		},
	}

	expected := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(1)},
		{Dtype: utils.SS_DT_STRING, CVal: "India"},
		{Dtype: utils.SS_DT_STRING, CVal: "China"},
		{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(4)},
	}

	err := inputIqr.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	assert.Equal(t, 4, inputIqr.NumberOfRecords())
	cols, err := inputIqr.GetColumns()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"state", "numField", "country"}, toputils.GetKeysOfMap(cols))

	iqr, err := evalProcessor.Process(inputIqr)
	assert.NoError(t, err)

	assert.Equal(t, 4, iqr.NumberOfRecords())
	cols, err = iqr.GetColumns()
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"state", "numField", "country", "evalField"}, toputils.GetKeysOfMap(cols))

	records, err := iqr.ReadColumn("evalField")
	assert.NoError(t, err)
	assert.Equal(t, 4, len(records))
	assert.Equal(t, expected, records)
}
