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
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func Test_MVExpand_noLimit(t *testing.T) {
	mvexpand := &mvexpandProcessor{
		options: &structs.MultiValueColLetRequest{
			Command: "mvexpand",
			ColName: "col1",
			Limit:   utils.NewUnsetOption[int64](),
		},
	}
	iqr := iqr.NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING_SLICE, CVal: []string{"a", "b", "c"}},
			{Dtype: sutils.SS_DT_STRING_SLICE, CVal: []string{"d", "e"}},
		},
		"col2": {
			{Dtype: sutils.SS_DT_STRING, CVal: "red"},
			{Dtype: sutils.SS_DT_STRING, CVal: "blue"},
		},
	})
	assert.NoError(t, err)

	iqr, err = mvexpand.Process(iqr)
	assert.NoError(t, err)

	expectedCol1 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "a"},
		{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		{Dtype: sutils.SS_DT_STRING, CVal: "c"},
		{Dtype: sutils.SS_DT_STRING, CVal: "d"},
		{Dtype: sutils.SS_DT_STRING, CVal: "e"},
	}
	expectedCol2 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "red"},
		{Dtype: sutils.SS_DT_STRING, CVal: "red"},
		{Dtype: sutils.SS_DT_STRING, CVal: "red"},
		{Dtype: sutils.SS_DT_STRING, CVal: "blue"},
		{Dtype: sutils.SS_DT_STRING, CVal: "blue"},
	}

	actualCol1, err := iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol1, actualCol1)

	actualCol2, err := iqr.ReadColumn("col2")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol2, actualCol2)
}

func Test_MVExpand_withLimit(t *testing.T) {
	mvexpand := &mvexpandProcessor{
		options: &structs.MultiValueColLetRequest{
			Command: "mvexpand",
			ColName: "col1",
			Limit:   utils.NewOptionWithValue[int64](2),
		},
	}
	iqr := iqr.NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING_SLICE, CVal: []string{"a", "b", "c"}},
			{Dtype: sutils.SS_DT_STRING_SLICE, CVal: []string{"d", "e"}},
		},
		"col2": {
			{Dtype: sutils.SS_DT_STRING, CVal: "red"},
			{Dtype: sutils.SS_DT_STRING, CVal: "blue"},
		},
	})
	assert.NoError(t, err)

	iqr, err = mvexpand.Process(iqr)
	assert.NoError(t, err)

	expectedCol1 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "a"},
		{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		{Dtype: sutils.SS_DT_STRING, CVal: "d"},
		{Dtype: sutils.SS_DT_STRING, CVal: "e"},
	}
	expectedCol2 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "red"},
		{Dtype: sutils.SS_DT_STRING, CVal: "red"},
		{Dtype: sutils.SS_DT_STRING, CVal: "blue"},
		{Dtype: sutils.SS_DT_STRING, CVal: "blue"},
	}

	actualCol1, err := iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol1, actualCol1)

	actualCol2, err := iqr.ReadColumn("col2")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol2, actualCol2)
}
