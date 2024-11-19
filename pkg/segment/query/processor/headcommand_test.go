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
	"io"
	"math"
	"testing"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

var boolExpr = &structs.BoolExpr{
	LeftValue: &structs.ValueExpr{
		ValueExprMode: structs.VEMNumericExpr,
		NumericExpr: &structs.NumericExpr{
			NumericExprMode: structs.NEMNumberField,
			Value:           "gender",
			IsTerminal:      true,
			ValueIsField:    true,
		},
	},
	RightValue: &structs.ValueExpr{
		ValueExprMode: structs.VEMStringExpr,
		StringExpr: &structs.StringExpr{
			StringExprMode: structs.SEMRawString,
			RawString:      "male",
		},
	},
	ValueOp:    "=",
	IsTerminal: true,
}

func Test_Head_WithLimit(t *testing.T) {
	const headLimit = 2
	dp := NewHeadDP(&structs.HeadExpr{MaxRows: headLimit})
	stream := &mockStreamer{
		allRecords: map[string][]utils.CValueEnclosure{
			"col1": {
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			},
		},
		qid: 0,
	}

	dp.streams = append(dp.streams, &CachedStream{stream, nil, false})

	totalFetched := 0
	numFetches := 0
	for {
		iqr, err := dp.Fetch()
		if err != io.EOF {
			assert.NoError(t, err)
		}

		totalFetched += iqr.NumberOfRecords()
		if err == io.EOF {
			break
		}

		numFetches++
		if numFetches > headLimit {
			t.Fatalf("Number of fetches exceeded head limit")
		}
	}

	assert.Equal(t, headLimit, totalFetched)
}

func Test_Head_Expr_Basic(t *testing.T) {
	knownValues := map[string][]utils.CValueEnclosure{
		"gender": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "male"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "male"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "male"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "female"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "male"},
		},
		"ident": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "d"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "e"},
		},
	}
	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	headProcessor1 := &headProcessor{
		options: &structs.HeadExpr{
			BoolExpr: boolExpr,
			MaxRows:  math.MaxUint64,
		},
	}

	iqr1, err = headProcessor1.Process(iqr1)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 3, iqr1.NumberOfRecords())

	columnValues, err := iqr1.ReadAllColumns()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(columnValues))
	assert.Equal(t, knownValues["gender"][:3], columnValues["gender"])
	assert.Equal(t, knownValues["ident"][:3], columnValues["ident"])

	iqr2 := iqr.NewIQR(0)
	err = iqr2.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	headProcessor2 := &headProcessor{
		options: &structs.HeadExpr{
			BoolExpr: boolExpr,
			MaxRows:  2,
		},
	}

	iqr2, err = headProcessor2.Process(iqr2)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 2, iqr2.NumberOfRecords())

	columnValues, err = iqr2.ReadAllColumns()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(columnValues))
	assert.Equal(t, knownValues["gender"][:2], columnValues["gender"])
	assert.Equal(t, knownValues["ident"][:2], columnValues["ident"])
}

func Test_Head_Expr_Null(t *testing.T) {
	knownValues := map[string][]utils.CValueEnclosure{
		"gender": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "male"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "male"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "female"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "male"},
		},
		"ident": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "d"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "e"},
		},
	}
	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	headProcessor1 := &headProcessor{
		options: &structs.HeadExpr{
			BoolExpr: boolExpr,
			Null:     true,
			MaxRows:  math.MaxUint64,
		},
	}

	iqr1, err = headProcessor1.Process(iqr1)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 3, iqr1.NumberOfRecords())

	columnValues, err := iqr1.ReadAllColumns()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(columnValues))
	assert.Equal(t, knownValues["gender"][:3], columnValues["gender"])
	assert.Equal(t, knownValues["ident"][:3], columnValues["ident"])
}

func Test_Head_Expr_Keeplast(t *testing.T) {
	knownValues := map[string][]utils.CValueEnclosure{
		"gender": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "male"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "female"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "male"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "female"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "male"},
		},
		"ident": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "d"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "e"},
		},
	}
	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	headProcessor1 := &headProcessor{
		options: &structs.HeadExpr{
			BoolExpr: boolExpr,
			Keeplast: true,
			MaxRows:  math.MaxUint64,
		},
	}

	iqr1, err = headProcessor1.Process(iqr1)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 2, iqr1.NumberOfRecords())

	columnValues, err := iqr1.ReadAllColumns()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(columnValues))
	assert.Equal(t, knownValues["gender"][:2], columnValues["gender"])
	assert.Equal(t, knownValues["ident"][:2], columnValues["ident"])
}

func Test_Head_Expr_Multiple(t *testing.T) {
	knownValues := map[string][]utils.CValueEnclosure{
		"gender": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "male"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "male"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "male"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "female"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "female"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "male"},
		},
		"ident": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "d"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "e"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "f"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "g"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "h"},
		},
	}
	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	headProcessor1 := &headProcessor{
		options: &structs.HeadExpr{
			BoolExpr: boolExpr,
			Null:     true,
			Keeplast: true,
			MaxRows:  math.MaxUint64,
		},
	}

	iqr1, err = headProcessor1.Process(iqr1)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 5, iqr1.NumberOfRecords())

	columnValues, err := iqr1.ReadAllColumns()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(columnValues))
	assert.Equal(t, knownValues["gender"][:5], columnValues["gender"])
	assert.Equal(t, knownValues["ident"][:5], columnValues["ident"])

	headProcessor2 := &headProcessor{
		options: &structs.HeadExpr{
			BoolExpr: boolExpr,
			Null:     true,
			Keeplast: true,
			MaxRows:  3,
		},
	}

	iqr2 := iqr.NewIQR(0)
	err = iqr2.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	iqr2, err = headProcessor2.Process(iqr2)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 3, iqr2.NumberOfRecords())

	columnValues, err = iqr2.ReadAllColumns()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(columnValues))
	assert.Equal(t, knownValues["gender"][:3], columnValues["gender"])
	assert.Equal(t, knownValues["ident"][:3], columnValues["ident"])
}

func Test_Head_Expr_NonExistentCol(t *testing.T) {
	knownValues := map[string][]utils.CValueEnclosure{
		"ident": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "d"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "e"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "f"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "g"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "h"},
		},
	}
	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	headProcessor1 := &headProcessor{
		options: &structs.HeadExpr{
			BoolExpr: boolExpr,
			Null:     true,
			Keeplast: true,
			MaxRows:  3,
		},
	}

	iqr1, err = headProcessor1.Process(iqr1)
	assert.Equal(t, io.EOF, err)
	assert.Equal(t, 3, iqr1.NumberOfRecords())

	columnValues, err := iqr1.ReadAllColumns()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(columnValues))
	assert.Equal(t, knownValues["ident"][:3], columnValues["ident"])
}
