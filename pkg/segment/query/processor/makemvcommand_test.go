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
	err := iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "a,b,c"},
			{Dtype: sutils.SS_DT_STRING, CVal: "d,e,f"},
		},
	})
	assert.NoError(t, err)

	iqr, err = makemv.Process(iqr)
	assert.NoError(t, err)

	expected := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING_SLICE, CVal: []string{"a", "b", "c"}},
		{Dtype: sutils.SS_DT_STRING_SLICE, CVal: []string{"d", "e", "f"}},
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
	err := iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "a|foo|b|foo|c"},
			{Dtype: sutils.SS_DT_STRING, CVal: "d|foo|e|foo|f"},
		},
	})
	assert.NoError(t, err)

	iqr, err = makemv.Process(iqr)
	assert.NoError(t, err)

	expected := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING_SLICE, CVal: []string{"a", "b", "c"}},
		{Dtype: sutils.SS_DT_STRING_SLICE, CVal: []string{"d", "e", "f"}},
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
	err := iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "a|||b||||c"}, // Three and four consecutive pipes
		},
	})
	assert.NoError(t, err)

	iqr, err = makemv.Process(iqr)
	assert.NoError(t, err)

	expected := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING_SLICE, CVal: []string{"a", "|b", "c"}},
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
	err := iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "a,,b,"},
			{Dtype: sutils.SS_DT_STRING, CVal: ",c,d,e"},
		},
	})
	assert.NoError(t, err)

	iqr, err = makemv.Process(iqr)
	assert.NoError(t, err)

	expected := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING_SLICE, CVal: []string{"a", "", "b", ""}},
		{Dtype: sutils.SS_DT_STRING_SLICE, CVal: []string{"", "c", "d", "e"}},
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
	err := iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "123abc456def"},
			{Dtype: sutils.SS_DT_STRING, CVal: "99XYZ88ABC"},
		},
	})
	assert.NoError(t, err)

	iqr, err = makemv.Process(iqr)
	assert.NoError(t, err)

	expected := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING_SLICE, CVal: []string{"abc", "def"}},
		{Dtype: sutils.SS_DT_STRING_SLICE, CVal: []string{"XYZ", "ABC"}},
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
	err := iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "apple,banana,cherry"},
			{Dtype: sutils.SS_DT_STRING, CVal: "dog,cat,fish"},
		},
	})
	assert.NoError(t, err)

	iqr, err = makemv.Process(iqr)
	assert.NoError(t, err)

	expected := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "apple banana cherry"},
		{Dtype: sutils.SS_DT_STRING, CVal: "dog cat fish"},
	}

	actual, err := iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}
