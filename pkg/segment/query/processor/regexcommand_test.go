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

func getTestValues() map[string][]sutils.CValueEnclosure {
	values := map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "Boston"},
			{Dtype: sutils.SS_DT_STRING, CVal: "New York"},
			{Dtype: sutils.SS_DT_STRING, CVal: "Bos___some_on"},
			{Dtype: sutils.SS_DT_BACKFILL, CVal: nil},
		},
		"col2": {
			{Dtype: sutils.SS_DT_STRING, CVal: "anything"},
			{Dtype: sutils.SS_DT_STRING, CVal: "New Jersey"},
			{Dtype: sutils.SS_DT_STRING, CVal: "Nothing"},
			{Dtype: sutils.SS_DT_BACKFILL, CVal: nil},
		},
		"col3": {
			{Dtype: sutils.SS_DT_STRING, CVal: "anything"},
			{Dtype: sutils.SS_DT_STRING, CVal: "New Jersey"},
			{Dtype: sutils.SS_DT_STRING, CVal: "Nothing"},
			{Dtype: sutils.SS_DT_STRING, CVal: "Boston"},
		},
	}

	return values
}

func Test_processRegexOnAllColumns_KeepMatch(t *testing.T) {
	pattern := "^Bos.*on$"
	gobRegex := &utils.GobbableRegex{}
	err := gobRegex.SetRegex(pattern)
	assert.Nil(t, err)

	regexProcessor := &regexProcessor{
		options: &structs.RegexExpr{
			Op:        "=",
			Field:     "*",
			RawRegex:  pattern,
			GobRegexp: gobRegex,
		},
	}

	values := getTestValues()

	iqr1 := iqr.NewIQR(0)
	err = iqr1.AppendKnownValues(values)
	assert.NoError(t, err)

	_, err = regexProcessor.Process(iqr1)
	assert.NoError(t, err)

	expectedCol1 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "Boston"},
		{Dtype: sutils.SS_DT_STRING, CVal: "Bos___some_on"},
		{Dtype: sutils.SS_DT_BACKFILL, CVal: nil},
	}
	expectedCol2 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "anything"},
		{Dtype: sutils.SS_DT_STRING, CVal: "Nothing"},
		{Dtype: sutils.SS_DT_BACKFILL, CVal: nil},
	}

	expectedCol3 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "anything"},
		{Dtype: sutils.SS_DT_STRING, CVal: "Nothing"},
		{Dtype: sutils.SS_DT_STRING, CVal: "Boston"},
	}

	actualCol1, err := iqr1.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol1, actualCol1)

	actualCol2, err := iqr1.ReadColumn("col2")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol2, actualCol2)

	actualCol3, err := iqr1.ReadColumn("col3")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol3, actualCol3)
}

func Test_processRegexOnAllColumns_DiscardMatch(t *testing.T) {
	pattern := "^Bos.*on$"
	gobRegex := &utils.GobbableRegex{}
	err := gobRegex.SetRegex(pattern)
	assert.Nil(t, err)

	regexProcessor := &regexProcessor{
		options: &structs.RegexExpr{
			Op:        "!=",
			Field:     "*",
			RawRegex:  pattern,
			GobRegexp: gobRegex,
		},
	}

	values := getTestValues()

	iqr1 := iqr.NewIQR(0)
	err = iqr1.AppendKnownValues(values)
	assert.NoError(t, err)

	_, err = regexProcessor.Process(iqr1)
	assert.NoError(t, err)

	expectedCol1 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "New York"},
	}

	expectedCol2 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "New Jersey"},
	}

	expectedCol3 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "New Jersey"},
	}

	actualCol1, err := iqr1.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol1, actualCol1)

	actualCol2, err := iqr1.ReadColumn("col2")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol2, actualCol2)

	actualCol3, err := iqr1.ReadColumn("col3")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol3, actualCol3)
}

func Test_processRegexOnSingleColumns_KeepMatch(t *testing.T) {
	pattern := "^Bos.*on$"
	gobRegex := &utils.GobbableRegex{}
	err := gobRegex.SetRegex(pattern)
	assert.Nil(t, err)

	regexProcessor := &regexProcessor{
		options: &structs.RegexExpr{
			Op:        "=",
			Field:     "col1",
			RawRegex:  pattern,
			GobRegexp: gobRegex,
		},
	}

	values := getTestValues()

	iqr1 := iqr.NewIQR(0)
	err = iqr1.AppendKnownValues(values)
	assert.NoError(t, err)

	_, err = regexProcessor.Process(iqr1)
	assert.NoError(t, err)

	expectedCol1 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "Boston"},
		{Dtype: sutils.SS_DT_STRING, CVal: "Bos___some_on"},
	}
	expectedCol2 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "anything"},
		{Dtype: sutils.SS_DT_STRING, CVal: "Nothing"},
	}
	expectedCol3 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "anything"},
		{Dtype: sutils.SS_DT_STRING, CVal: "Nothing"},
	}

	actualCol1, err := iqr1.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol1, actualCol1)

	actualCol2, err := iqr1.ReadColumn("col2")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol2, actualCol2)

	actualCol3, err := iqr1.ReadColumn("col3")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol3, actualCol3)
}

func Test_processRegexOnSingleColumns_DiscardMatch(t *testing.T) {
	pattern := "^Bos.*on$"
	gobRegex := &utils.GobbableRegex{}
	err := gobRegex.SetRegex(pattern)
	assert.Nil(t, err)

	regexProcessor := &regexProcessor{
		options: &structs.RegexExpr{
			Op:        "!=",
			Field:     "col1",
			RawRegex:  pattern,
			GobRegexp: gobRegex,
		},
	}

	values := getTestValues()

	iqr1 := iqr.NewIQR(0)
	err = iqr1.AppendKnownValues(values)
	assert.NoError(t, err)

	_, err = regexProcessor.Process(iqr1)
	assert.NoError(t, err)

	expectedCol1 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "New York"},
		{Dtype: sutils.SS_DT_BACKFILL, CVal: nil},
	}
	expectedCol2 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "New Jersey"},
		{Dtype: sutils.SS_DT_BACKFILL, CVal: nil},
	}
	expectedCol3 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "New Jersey"},
		{Dtype: sutils.SS_DT_STRING, CVal: "Boston"},
	}

	actualCol1, err := iqr1.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol1, actualCol1)

	actualCol2, err := iqr1.ReadColumn("col2")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol2, actualCol2)

	actualCol3, err := iqr1.ReadColumn("col3")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol3, actualCol3)
}
