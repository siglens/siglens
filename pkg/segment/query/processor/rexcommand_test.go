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
	"strings"
	"testing"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

// Convert a Perl-style regex pattern to RE2-style regex pattern.
func convertPerlToRE2(pattern string) string {
	pattern = strings.Replace(pattern, "(?<", "(?P<", -1)
	return pattern
}

func Test_RexCommand_ValidColumn(t *testing.T) {
	iqr := iqr.NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "apple-123"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "banana-456"},
		},
	})
	assert.NoError(t, err)

	rex := &rexProcessor{
		options: &structs.RexExpr{
			FieldName:   "col1",
			Pattern:     convertPerlToRE2(`(?<item>.+)-(?<number>.+)`),
			RexColNames: []string{"item", "number"},
		},
	}

	iqr, err = rex.Process(iqr)
	assert.NoError(t, err)

	values, err := iqr.ReadColumn("item")
	assert.NoError(t, err)
	assert.Equal(t, []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "apple"},
		{Dtype: sutils.SS_DT_STRING, CVal: "banana"},
	}, values)

	values, err = iqr.ReadColumn("number")
	assert.NoError(t, err)
	assert.Equal(t, []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "123"},
		{Dtype: sutils.SS_DT_STRING, CVal: "456"},
	}, values)

	values, err = iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "apple-123"},
		{Dtype: sutils.SS_DT_STRING, CVal: "banana-456"},
	}, values)
}

func Test_RexCommand_InvalidColumn(t *testing.T) {
	iqr := iqr.NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "apple-123"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "banana-456"},
		},
	})
	assert.NoError(t, err)

	rex := &rexProcessor{
		options: &structs.RexExpr{
			FieldName:   "col2",
			Pattern:     convertPerlToRE2(`(?<item>.+)-(?<number>.+)`),
			RexColNames: []string{"item", "number"},
		},
	}

	iqr, err = rex.Process(iqr)
	assert.NoError(t, err)

	values, _ := iqr.ReadColumn("item")
	assert.Nil(t, values)

	values, _ = iqr.ReadColumn("number")
	assert.Nil(t, values)

	values, err = iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "apple-123"},
		{Dtype: sutils.SS_DT_STRING, CVal: "banana-456"},
	}, values)
}

func Test_RexCommand_InvalidPattern(t *testing.T) {
	iqr := iqr.NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "apple-123"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "banana-456"},
		},
	})
	assert.NoError(t, err)

	rex := &rexProcessor{
		options: &structs.RexExpr{
			FieldName:   "col1",
			Pattern:     convertPerlToRE2(`(?<item>.+)-(?<number>`),
			RexColNames: []string{"item", "number"},
		},
	}

	_, err = rex.Process(iqr)
	assert.Error(t, err)
}

func Test_RexCommand_WriteOverExistingColumn(t *testing.T) {
	iqr := iqr.NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "apple-123"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "banana-456"},
		},
		"item": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "pear"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "mandarin"},
		},
	})
	assert.NoError(t, err)

	rex := &rexProcessor{
		options: &structs.RexExpr{
			FieldName:   "col1",
			Pattern:     convertPerlToRE2(`(?<item>.+)-(?<number>.+)`),
			RexColNames: []string{"item", "number"},
		},
	}

	iqr, err = rex.Process(iqr)
	assert.NoError(t, err)

	values, err := iqr.ReadColumn("item")
	assert.NoError(t, err)
	assert.Equal(t, []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "apple"},
		{Dtype: sutils.SS_DT_STRING, CVal: "banana"},
	}, values)

	values, err = iqr.ReadColumn("number")
	assert.NoError(t, err)
	assert.Equal(t, []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "123"},
		{Dtype: sutils.SS_DT_STRING, CVal: "456"},
	}, values)

	values, err = iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "apple-123"},
		{Dtype: sutils.SS_DT_STRING, CVal: "banana-456"},
	}, values)
}
