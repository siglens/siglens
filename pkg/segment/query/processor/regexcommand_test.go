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

func getTestValues() map[string][]utils.CValueEnclosure {
	values := map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "Boston"},
			{Dtype: utils.SS_DT_STRING, CVal: "New York"},
			{Dtype: utils.SS_DT_STRING, CVal: "Bos___some_on"},
			{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
		},
		"col2": {
			{Dtype: utils.SS_DT_STRING, CVal: "anything"},
			{Dtype: utils.SS_DT_STRING, CVal: "New Jersey"},
			{Dtype: utils.SS_DT_STRING, CVal: "Nothing"},
			{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
		},
		"col3": {
			{Dtype: utils.SS_DT_STRING, CVal: "anything"},
			{Dtype: utils.SS_DT_STRING, CVal: "New Jersey"},
			{Dtype: utils.SS_DT_STRING, CVal: "Nothing"},
			{Dtype: utils.SS_DT_STRING, CVal: "Boston"},
		},
	}

	return values
}

func Test_processRegexOnAllColumns_KeepMatch(t *testing.T) {
	pattern := "^Bos.*on$"
	gobRegex := &toputils.GobbableRegex{}
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

	expectedCol1 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "Boston"},
		{Dtype: utils.SS_DT_STRING, CVal: "Bos___some_on"},
		{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
	}
	expectedCol2 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "anything"},
		{Dtype: utils.SS_DT_STRING, CVal: "Nothing"},
		{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
	}

	expectedCol3 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "anything"},
		{Dtype: utils.SS_DT_STRING, CVal: "Nothing"},
		{Dtype: utils.SS_DT_STRING, CVal: "Boston"},
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
	gobRegex := &toputils.GobbableRegex{}
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

	expectedCol1 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "New York"},
	}

	expectedCol2 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "New Jersey"},
	}

	expectedCol3 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "New Jersey"},
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
	gobRegex := &toputils.GobbableRegex{}
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

	expectedCol1 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "Boston"},
		{Dtype: utils.SS_DT_STRING, CVal: "Bos___some_on"},
	}
	expectedCol2 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "anything"},
		{Dtype: utils.SS_DT_STRING, CVal: "Nothing"},
	}
	expectedCol3 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "anything"},
		{Dtype: utils.SS_DT_STRING, CVal: "Nothing"},
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
	gobRegex := &toputils.GobbableRegex{}
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

	expectedCol1 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "New York"},
		{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
	}
	expectedCol2 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "New Jersey"},
		{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
	}
	expectedCol3 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "New Jersey"},
		{Dtype: utils.SS_DT_STRING, CVal: "Boston"},
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
