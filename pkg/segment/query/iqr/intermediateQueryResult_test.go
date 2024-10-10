// Copyright (c) 2021-2024 SigScalr, Inc.
//
// # This file is part of SigLens Observability Solution
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

package iqr

import (
	"testing"

	"github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_initIQR(t *testing.T) {
	iqr := NewIQR(0)
	err := iqr.validate()
	assert.NoError(t, err)

	iqr = &IQR{}
	err = iqr.validate()
	assert.Error(t, err)
}

func Test_AppendRRCs(t *testing.T) {
	iqr := NewIQR(0)
	segKeyInfo1 := utils.SegKeyInfo{
		SegKeyEnc: 1,
	}
	encodingToSegKey := map[uint16]string{1: "segKey1"}
	rrcs := []*utils.RecordResultContainer{
		{SegKeyInfo: segKeyInfo1, BlockNum: 1, RecordNum: 1},
		{SegKeyInfo: segKeyInfo1, BlockNum: 1, RecordNum: 2},
		{SegKeyInfo: segKeyInfo1, BlockNum: 1, RecordNum: 3},
	}

	err := iqr.AppendRRCs(rrcs, encodingToSegKey)
	assert.NoError(t, err)
	assert.Equal(t, withRRCs, iqr.mode)
	assert.Equal(t, rrcs, iqr.rrcs)
	assert.Equal(t, encodingToSegKey, iqr.encodingToSegKey)
}

func Test_AppendKnownValues_OnEmptyIQR(t *testing.T) {
	iqr := NewIQR(0)

	knownValues1 := map[string][]utils.CValueEnclosure{
		"col1": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
		},
	}
	err := iqr.AppendKnownValues(knownValues1)
	assert.NoError(t, err)
	assert.Equal(t, knownValues1, iqr.knownValues)
	assert.Equal(t, withoutRRCs, iqr.mode)

	// A different column with a different number of records should fail.
	knownValues2 := map[string][]utils.CValueEnclosure{
		"col2": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
		},
	}
	err = iqr.AppendKnownValues(knownValues2)
	assert.Error(t, err)

	// A different column with the same number of records should succeed.
	knownValues3 := map[string][]utils.CValueEnclosure{
		"col2": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "x"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "y"},
		},
	}
	err = iqr.AppendKnownValues(knownValues3)
	assert.NoError(t, err)
	assert.Equal(t, toputils.MergeMaps(knownValues1, knownValues3), iqr.knownValues)
	assert.Equal(t, withoutRRCs, iqr.mode)
}

func Test_AsResult(t *testing.T) {
	iqr := NewIQR(0)
	knownValues := map[string][]utils.CValueEnclosure{
		"col1": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
		},
	}
	err := iqr.AppendKnownValues(knownValues)
	require.NoError(t, err)

	expectedResult := &pipesearch.PipeSearchResponseOuter{
		Hits: pipesearch.PipeSearchResponse{
			TotalMatched: 2,
			Hits: []map[string]interface{}{
				{"col1": "a"},
				{"col1": "b"},
			},
		},
		AllPossibleColumns: []string{"col1"},
		Errors:             nil,
		Qtype:              "logs-query",
		CanScrollMore:      false,
		ColumnsOrder:       []string{"col1"},
	}
	result, err := iqr.AsResult()
	assert.NoError(t, err)
	assert.Equal(t, expectedResult, result)
}

func Test_mergeMetadata(t *testing.T) {
	iqr1 := NewIQR(0)
	iqr2 := NewIQR(0)

	// Disjoint encodings.
	iqr1.encodingToSegKey = map[uint16]string{1: "segKey1"}
	iqr2.encodingToSegKey = map[uint16]string{2: "segKey2"}

	iqr, err := mergeMetadata([]*IQR{iqr1, iqr2})
	assert.NoError(t, err)
	assert.Equal(t, map[uint16]string{1: "segKey1", 2: "segKey2"}, iqr.encodingToSegKey)

	// Overlapping encodings.
	iqr1.encodingToSegKey = map[uint16]string{1: "segKey1", 2: "segKey2"}
	iqr2.encodingToSegKey = map[uint16]string{2: "segKey2", 3: "segKey3"}

	iqr, err = mergeMetadata([]*IQR{iqr1, iqr2})
	assert.NoError(t, err)
	assert.Equal(t, map[uint16]string{1: "segKey1", 2: "segKey2", 3: "segKey3"}, iqr.encodingToSegKey)

	// Inconsistent encodings.
	iqr1.encodingToSegKey = map[uint16]string{1: "segKey1", 2: "segKey2"}
	iqr2.encodingToSegKey = map[uint16]string{2: "segKey100", 3: "segKey3"}

	_, err = mergeMetadata([]*IQR{iqr1, iqr2})
	assert.Error(t, err)
}

func Test_mergeMetadata_differentQids(t *testing.T) {
	iqr1 := NewIQR(0)
	iqr2 := NewIQR(1)

	_, err := mergeMetadata([]*IQR{iqr1, iqr2})
	assert.Error(t, err)
}

func Test_Append(t *testing.T) {
	iqr1 := NewIQR(0)
	iqr2 := NewIQR(0)

	err := iqr1.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
		},
		"col2": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "y"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "z"},
		},
	})
	assert.NoError(t, err)

	err = iqr2.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "d"},
		},
		"col3": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "foo"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "bar"},
		},
	})
	assert.NoError(t, err)

	err = iqr1.Append(iqr2)
	assert.NoError(t, err)

	expected := map[string][]utils.CValueEnclosure{
		"col1": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "d"},
		},
		"col2": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "y"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "z"},
			*backfillCVal,
			*backfillCVal,
		},
		"col3": {
			*backfillCVal,
			*backfillCVal,
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "foo"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "bar"},
		},
	}

	assert.Equal(t, len(expected), len(iqr1.knownValues))
	for cname, expectedValues := range expected {
		if _, ok := iqr1.knownValues[cname]; !ok {
			assert.Fail(t, "missing column %v", cname)
		}

		assert.Equal(t, expectedValues, iqr1.knownValues[cname],
			"actual=%v, expected=%v, cname=%v", iqr1.knownValues[cname],
			expectedValues, cname)
	}
}

func Test_MergeIQRs(t *testing.T) {
	iqr1 := NewIQR(0)
	iqr2 := NewIQR(0)
	iqr3 := NewIQR(0)

	err := iqr1.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "e"},
		},
	})
	assert.NoError(t, err)

	err = iqr2.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "f"},
		},
	})
	assert.NoError(t, err)

	err = iqr3.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "d"},
		},
	})
	assert.NoError(t, err)

	less := func(a, b *Record) bool {
		aVal, err := a.ReadColumn("col1")
		assert.NoError(t, err)

		bVal, err := b.ReadColumn("col1")
		assert.NoError(t, err)

		return aVal.CVal.(string) < bVal.CVal.(string)
	}

	mergedIqr, firstExhaustedIndex, err := MergeIQRs([]*IQR{iqr1, iqr2, iqr3}, less)
	assert.NoError(t, err)
	assert.Equal(t, 2, firstExhaustedIndex)
	assert.Equal(t, 4, mergedIqr.NumberOfRecords())
	assert.Equal(t, map[string][]utils.CValueEnclosure{
		"col1": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "d"},
		},
	}, mergedIqr.knownValues)

	// The merged records should have been discarded from the input IQRs.
	assert.Equal(t, 1, iqr1.NumberOfRecords())
	assert.Equal(t, 1, iqr2.NumberOfRecords())
	assert.Equal(t, 0, iqr3.NumberOfRecords())
}

func Test_DiscardAfter(t *testing.T) {
	iqr := NewIQR(0)
	segKeyInfo1 := utils.SegKeyInfo{
		SegKeyEnc: 1,
	}
	encodingToSegKey := map[uint16]string{1: "segKey1"}
	rrcs := []*utils.RecordResultContainer{
		{SegKeyInfo: segKeyInfo1, BlockNum: 1, RecordNum: 1},
		{SegKeyInfo: segKeyInfo1, BlockNum: 1, RecordNum: 2},
		{SegKeyInfo: segKeyInfo1, BlockNum: 1, RecordNum: 3},
	}

	err := iqr.AppendRRCs(rrcs, encodingToSegKey)
	assert.NoError(t, err)

	assert.Equal(t, 3, iqr.NumberOfRecords())
	err = iqr.DiscardAfter(3)
	assert.NoError(t, err)
	assert.Equal(t, 3, iqr.NumberOfRecords())

	err = iqr.DiscardAfter(1)
	assert.NoError(t, err)
	assert.Equal(t, 1, iqr.NumberOfRecords())
}
