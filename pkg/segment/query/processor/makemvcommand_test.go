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
	segutils "github.com/siglens/siglens/pkg/segment/utils"
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
			Setsv:           false,
		},
	}
	iqr := iqr.NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]segutils.CValueEnclosure{
		"col1": {
			{Dtype: segutils.SS_DT_STRING, CVal: "a,b,c"},
			{Dtype: segutils.SS_DT_STRING, CVal: "d,e,f"},
		},
	})
	assert.NoError(t, err)

	iqr, err = makemv.Process(iqr)
	assert.NoError(t, err)

	expected := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING_SLICE, CVal: []string{"a", "b", "c"}},
		{Dtype: segutils.SS_DT_STRING_SLICE, CVal: []string{"d", "e", "f"}},
	}

	actual, err := iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func Test_MakeMV_multicharDelimeter(t *testing.T) {
	makemv := &makemvProcessor{
		options: &structs.MultiValueColLetRequest{
			Command:         "makemv",
			ColName:         "col1",
			DelimiterString: "|foo|",
			IsRegex:         false,
			AllowEmpty:      false,
			Setsv:           false,
		},
	}

	iqr := iqr.NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]segutils.CValueEnclosure{
		"col1": {
			{Dtype: segutils.SS_DT_STRING, CVal: "a|foo|b|foo|c"},
			{Dtype: segutils.SS_DT_STRING, CVal: "d|foo|e|foo|f"},
		},
	})
	assert.NoError(t, err)

	iqr, err = makemv.Process(iqr)
	assert.NoError(t, err)

	expected := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING_SLICE, CVal: []string{"a", "b", "c"}},
		{Dtype: segutils.SS_DT_STRING_SLICE, CVal: []string{"d", "e", "f"}},
	}

	actual, err := iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func Test_MakeMV_overlappingDelimeter(t *testing.T) {
	makemv := &makemvProcessor{
		options: &structs.MultiValueColLetRequest{
			Command:         "makemv",
			ColName:         "col1",
			DelimiterString: "||",
			IsRegex:         false,
			AllowEmpty:      false,
			Setsv:           false,
		},
	}

	iqr := iqr.NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]segutils.CValueEnclosure{
		"col1": {
			{Dtype: segutils.SS_DT_STRING, CVal: "a|||b||||c"}, // Three and four consecutive pipes
		},
	})
	assert.NoError(t, err)

	iqr, err = makemv.Process(iqr)
	assert.NoError(t, err)

	expected := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING_SLICE, CVal: []string{"a", "|b", "c"}},
	}

	actual, err := iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func Test_MakeMV_allowEmpty(t *testing.T) {
	makemv := &makemvProcessor{
		options: &structs.MultiValueColLetRequest{
			Command:         "makemv",
			ColName:         "col1",
			DelimiterString: ",",
			IsRegex:         false,
			AllowEmpty:      true,
			Setsv:           false,
		},
	}

	iqr := iqr.NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]segutils.CValueEnclosure{
		"col1": {
			{Dtype: segutils.SS_DT_STRING, CVal: "a,,b,"},
			{Dtype: segutils.SS_DT_STRING, CVal: ",c,d,e"},
		},
	})
	assert.NoError(t, err)

	iqr, err = makemv.Process(iqr)
	assert.NoError(t, err)

	expected := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING_SLICE, CVal: []string{"a", "", "b", ""}},
		{Dtype: segutils.SS_DT_STRING_SLICE, CVal: []string{"", "c", "d", "e"}},
	}

	actual, err := iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func Test_MakeMV_regex(t *testing.T) {
	makemv := &makemvProcessor{
		options: &structs.MultiValueColLetRequest{
			Command:         "makemv",
			ColName:         "col1",
			DelimiterString: `([a-zA-Z]+)`,
			IsRegex:         true,
			AllowEmpty:      true,
			Setsv:           false,
		},
	}

	iqr := iqr.NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]segutils.CValueEnclosure{
		"col1": {
			{Dtype: segutils.SS_DT_STRING, CVal: "123abc456def"},
			{Dtype: segutils.SS_DT_STRING, CVal: "99XYZ88ABC"},
		},
	})
	assert.NoError(t, err)

	iqr, err = makemv.Process(iqr)
	assert.NoError(t, err)

	expected := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING_SLICE, CVal: []string{"abc", "def"}},
		{Dtype: segutils.SS_DT_STRING_SLICE, CVal: []string{"XYZ", "ABC"}},
	}

	actual, err := iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func Test_MakeMV_setSV(t *testing.T) {
	makemv := &makemvProcessor{
		options: &structs.MultiValueColLetRequest{
			Command:         "makemv",
			ColName:         "col1",
			DelimiterString: ",",
			IsRegex:         false,
			AllowEmpty:      false,
			Setsv:           true,
		},
	}

	iqr := iqr.NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]segutils.CValueEnclosure{
		"col1": {
			{Dtype: segutils.SS_DT_STRING, CVal: "apple,banana,cherry"},
			{Dtype: segutils.SS_DT_STRING, CVal: "dog,cat,fish"},
		},
	})
	assert.NoError(t, err)

	iqr, err = makemv.Process(iqr)
	assert.NoError(t, err)

	expected := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "apple banana cherry"},
		{Dtype: segutils.SS_DT_STRING, CVal: "dog cat fish"},
	}

	actual, err := iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}
