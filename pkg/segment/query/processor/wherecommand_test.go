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
	"github.com/stretchr/testify/assert"
)

func getWhereCommandProcessorForTest() *whereProcessor {
	return &whereProcessor{}
}

func getTestKnownValues() map[string][]utils.CValueEnclosure {
	return map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
		},
		"col2": {
			{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(1)},
			{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(2)},
			{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(3)},
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
