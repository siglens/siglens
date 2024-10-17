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
	"testing"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_SortCommand_simple(t *testing.T) {
	sorter := &sortProcessor{
		options: &structs.SortExpr{
			SortEles: []*structs.SortElement{
				{Field: "col1", SortByAsc: true, Op: ""},
			},
			Limit: 4,
		},
	}

	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
			{Dtype: utils.SS_DT_STRING, CVal: "e"},
			{Dtype: utils.SS_DT_STRING, CVal: "d"},
			{Dtype: utils.SS_DT_STRING, CVal: "f"},
		},
	})
	assert.NoError(t, err)

	_, err = sorter.Process(iqr1)
	assert.NoError(t, err)

	expected := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "a"},
		{Dtype: utils.SS_DT_STRING, CVal: "b"},
		{Dtype: utils.SS_DT_STRING, CVal: "c"},
		{Dtype: utils.SS_DT_STRING, CVal: "d"},
	}

	actualCol1, err := sorter.resultsSoFar.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actualCol1)

	// Add more records that should replace some existing ones.
	iqr2 := iqr.NewIQR(0)
	err = iqr2.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "g"},
			{Dtype: utils.SS_DT_STRING, CVal: "apple"},
			{Dtype: utils.SS_DT_STRING, CVal: "banana"},
			{Dtype: utils.SS_DT_STRING, CVal: "h"},
		},
	})
	assert.NoError(t, err)

	_, err = sorter.Process(iqr2)
	assert.NoError(t, err)

	expected = []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "a"},
		{Dtype: utils.SS_DT_STRING, CVal: "apple"},
		{Dtype: utils.SS_DT_STRING, CVal: "b"},
		{Dtype: utils.SS_DT_STRING, CVal: "banana"},
	}

	actualCol1, err = sorter.resultsSoFar.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actualCol1)

	// Get the final results.
	finalIQR, err := sorter.Process(nil)
	assert.Equal(t, io.EOF, err)

	actualCol1, err = finalIQR.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actualCol1)
}
