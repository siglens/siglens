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

func Test_MakeMV_simple(t *testing.T) {
	makemv := &makemvProcessor{
		options: &structs.MultiValueColLetRequest{
			Command:         "makemv",
			ColName:         "col1",
			DelimiterString: ",",
			IsRegex:         false,
			AllowEmpty:      false,
			Limit:           10,
		},
	}
	iqr := iqr.NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "a,b,c"},
			{Dtype: utils.SS_DT_STRING, CVal: "d,e,f"},
		},
	})

	iqr, err = makemv.Process(iqr)
	assert.NoError(t, err)

	expected := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING_SLICE, CVal: []string{"a", "b", "c"}},
		{Dtype: utils.SS_DT_STRING_SLICE, CVal: []string{"d", "e", "f"}},
	}

	actual, err := iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}
