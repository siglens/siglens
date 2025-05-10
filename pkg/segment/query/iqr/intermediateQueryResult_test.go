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
	"sort"
	"testing"

	"github.com/siglens/siglens/pkg/segment/reader/record"
	"github.com/siglens/siglens/pkg/segment/results/blockresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
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
	segKeyInfo1 := sutils.SegKeyInfo{
		SegKeyEnc: 1,
	}
	encodingToSegKey := map[uint32]string{1: "segKey1"}
	rrcs := []*sutils.RecordResultContainer{
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

	knownValues1 := map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		},
	}
	err := iqr.AppendKnownValues(knownValues1)
	assert.NoError(t, err)
	assert.Equal(t, knownValues1, iqr.knownValues)
	assert.Equal(t, withoutRRCs, iqr.mode)

	// A different column with a different number of records should fail.
	knownValues2 := map[string][]sutils.CValueEnclosure{
		"col2": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
		},
	}
	err = iqr.AppendKnownValues(knownValues2)
	assert.Error(t, err)

	// A different column with the same number of records should succeed.
	knownValues3 := map[string][]sutils.CValueEnclosure{
		"col2": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "x"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "y"},
		},
	}
	err = iqr.AppendKnownValues(knownValues3)
	assert.NoError(t, err)
	assert.Equal(t, utils.MergeMaps(knownValues1, knownValues3), iqr.knownValues)
	assert.Equal(t, withoutRRCs, iqr.mode)
}

func Test_AsResult(t *testing.T) {
	iqr := NewIQR(0)
	knownValues := map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		},
	}
	err := iqr.AppendKnownValues(knownValues)
	require.NoError(t, err)

	expectedResult := &structs.PipeSearchResponseOuter{
		Hits: structs.PipeSearchResponse{
			TotalMatched: utils.HitsCount{Value: 2, Relation: "eq"},
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
	result, err := iqr.AsResult(structs.RRCCmd, false, false)
	assert.NoError(t, err)
	assert.Equal(t, expectedResult, result)
}

func Test_mergeMetadata(t *testing.T) {
	iqr1 := NewIQR(0)
	iqr2 := NewIQR(0)

	// Disjoint encodings.
	iqr1.encodingToSegKey = map[uint32]string{1: "segKey1"}
	iqr2.encodingToSegKey = map[uint32]string{2: "segKey2"}

	iqr, err := mergeMetadata([]*IQR{iqr1, iqr2}, false)
	assert.NoError(t, err)
	assert.Equal(t, map[uint32]string{1: "segKey1", 2: "segKey2"}, iqr.encodingToSegKey)

	// Overlapping encodings.
	iqr1.encodingToSegKey = map[uint32]string{1: "segKey1", 2: "segKey2"}
	iqr2.encodingToSegKey = map[uint32]string{2: "segKey2", 3: "segKey3"}

	iqr, err = mergeMetadata([]*IQR{iqr1, iqr2}, false)
	assert.NoError(t, err)
	assert.Equal(t, map[uint32]string{1: "segKey1", 2: "segKey2", 3: "segKey3"}, iqr.encodingToSegKey)

	// Inconsistent encodings.
	iqr1.encodingToSegKey = map[uint32]string{1: "segKey1", 2: "segKey2"}
	iqr2.encodingToSegKey = map[uint32]string{2: "segKey100", 3: "segKey3"}

	_, err = mergeMetadata([]*IQR{iqr1, iqr2}, false)
	assert.Error(t, err)
}

func Test_mergeMetadata_modes(t *testing.T) {
	iqr1 := NewIQR(0)
	iqr2 := NewIQR(0)

	// Incompatible modes.
	iqr1.mode = withRRCs
	iqr2.mode = withoutRRCs
	_, err := mergeMetadata([]*IQR{iqr1, iqr2}, false)
	assert.Error(t, err)

	// Same modes.
	iqr1.mode = withRRCs
	iqr2.mode = withRRCs
	iqr, err := mergeMetadata([]*IQR{iqr1, iqr2}, false)
	assert.NoError(t, err)
	assert.Equal(t, withRRCs, iqr.mode)

	// First is unset.
	iqr1.mode = notSet
	iqr2.mode = withoutRRCs
	iqr, err = mergeMetadata([]*IQR{iqr1, iqr2}, false)
	assert.NoError(t, err)
	assert.Equal(t, withoutRRCs, iqr.mode)

	// Second is unset.
	iqr1.mode = withoutRRCs
	iqr2.mode = notSet
	iqr, err = mergeMetadata([]*IQR{iqr1, iqr2}, false)
	assert.NoError(t, err)
	assert.Equal(t, withoutRRCs, iqr.mode)
}

func Test_mergeMetadata_differentQids(t *testing.T) {
	iqr1 := NewIQR(0)
	iqr2 := NewIQR(1)

	_, err := mergeMetadata([]*IQR{iqr1, iqr2}, false)
	assert.Error(t, err)
}

func Test_Append(t *testing.T) {
	iqr1 := NewIQR(0)
	iqr2 := NewIQR(0)

	err := iqr1.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		},
		"col2": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "y"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "z"},
		},
	})
	assert.NoError(t, err)

	err = iqr2.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "d"},
		},
		"col3": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "foo"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "bar"},
		},
	})
	assert.NoError(t, err)

	err = iqr1.Append(iqr2)
	assert.NoError(t, err)

	expected := map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "d"},
		},
		"col2": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "y"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "z"},
			*backfillCVal,
			*backfillCVal,
		},
		"col3": {
			*backfillCVal,
			*backfillCVal,
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "foo"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "bar"},
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

func Test_Append_mergeMetaData(t *testing.T) {
	iqr1 := NewIQR(0)
	iqr2 := NewIQR(0)

	iqr1.encodingToSegKey = map[uint32]string{1: "segKey1"}
	iqr2.encodingToSegKey = map[uint32]string{2: "segKey2"}

	err := iqr1.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
		},
	})
	assert.NoError(t, err)

	err = iqr2.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col2": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		},
	})
	assert.NoError(t, err)

	iqr1.deletedColumns = map[string]struct{}{"col10": {}}
	iqr2.deletedColumns = map[string]struct{}{"col11": {}}
	iqr1.columnIndex = map[string]int{"col1": 0}
	iqr2.columnIndex = map[string]int{"col2": 1}

	err = iqr1.Append(iqr2)
	assert.NoError(t, err)
	assert.Equal(t, iqr1.mode, withoutRRCs)
	assert.Equal(t, map[uint32]string{1: "segKey1", 2: "segKey2"}, iqr1.encodingToSegKey)
	assert.Equal(t, map[string]struct{}{"col10": {}, "col11": {}}, iqr1.deletedColumns)
	assert.Equal(t, map[string]int{"col1": 0, "col2": 1}, iqr1.columnIndex)
}

func Test_Append_withRRCs(t *testing.T) {
	iqr := NewIQR(0)
	segKeyInfo1 := sutils.SegKeyInfo{
		SegKeyEnc: 1,
	}
	encodingToSegKey := map[uint32]string{1: "segKey1"}
	rrcs := []*sutils.RecordResultContainer{
		{SegKeyInfo: segKeyInfo1, BlockNum: 1, RecordNum: 1},
		{SegKeyInfo: segKeyInfo1, BlockNum: 1, RecordNum: 2},
		{SegKeyInfo: segKeyInfo1, BlockNum: 1, RecordNum: 3},
	}
	err := iqr.AppendRRCs(rrcs, encodingToSegKey)
	assert.NoError(t, err)

	otherIqr := NewIQR(0)
	segKeyInfo2 := sutils.SegKeyInfo{
		SegKeyEnc: 2,
	}
	encodingToSegKey2 := map[uint32]string{2: "segKey2"}
	rrcs2 := []*sutils.RecordResultContainer{
		{SegKeyInfo: segKeyInfo2, BlockNum: 1, RecordNum: 1},
		{SegKeyInfo: segKeyInfo2, BlockNum: 1, RecordNum: 2},
		{SegKeyInfo: segKeyInfo2, BlockNum: 1, RecordNum: 3},
	}
	err = otherIqr.AppendRRCs(rrcs2, encodingToSegKey2)
	assert.NoError(t, err)

	err = otherIqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c"},
		},
	})
	assert.NoError(t, err)

	err = iqr.Append(otherIqr)
	assert.NoError(t, err)
	assert.NoError(t, iqr.validate())
	assert.Equal(t, withRRCs, iqr.mode)
	assert.Equal(t, 6, iqr.NumberOfRecords())
	assert.Equal(t, append(rrcs, rrcs2...), iqr.rrcs)
}

func setupTestIQRsWithRRCs(t *testing.T) (*IQR, *IQR) {
	allRRCs := []*sutils.RecordResultContainer{
		{SegKeyInfo: sutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 1},
		{SegKeyInfo: sutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 2},
		{SegKeyInfo: sutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 3},
		{SegKeyInfo: sutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 4},
	}
	mockReader := &record.MockRRCsReader{
		RRCs: allRRCs,
		FieldToValues: map[string][]sutils.CValueEnclosure{
			"col1": {
				{Dtype: sutils.SS_DT_STRING, CVal: "a"},
				{Dtype: sutils.SS_DT_STRING, CVal: "b"},
				{Dtype: sutils.SS_DT_STRING, CVal: "c"},
				{Dtype: sutils.SS_DT_STRING, CVal: "d"},
			},
		},
	}

	iqr1 := NewIQR(0)
	iqr1.reader = NewIQRReader(mockReader)
	err := iqr1.AppendRRCs(allRRCs[:2], map[uint32]string{1: "segKey1"})
	assert.NoError(t, err)

	iqr2 := NewIQR(0)
	iqr2.reader = NewIQRReader(mockReader)
	err = iqr2.AppendRRCs(allRRCs[2:], map[uint32]string{1: "segKey1"})
	assert.NoError(t, err)

	return iqr1, iqr2
}

func Test_Append_withRRCs_firstHasKnownValues(t *testing.T) {
	iqr1, iqr2 := setupTestIQRsWithRRCs(t)

	// Read from one IQR but not the other.
	values, err := iqr1.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "a"},
		{Dtype: sutils.SS_DT_STRING, CVal: "b"},
	}, values)

	_, ok := iqr1.knownValues["col1"]
	assert.True(t, ok)
	_, ok = iqr2.knownValues["col1"]
	assert.False(t, ok)

	// Even though they have different knownValues, we should be able to append
	// them and read the correct values.
	err = iqr1.Append(iqr2)
	assert.NoError(t, err)
	values, err = iqr1.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "a"},
		{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		{Dtype: sutils.SS_DT_STRING, CVal: "c"},
		{Dtype: sutils.SS_DT_STRING, CVal: "d"},
	}, values)
}

// Like Test_Append_withRRCs_firstHasKnownValues, but the second IQR has some
// known values read from the RRCs and the first IQR does not.
func Test_Append_withRRCs_secondHasKnownValues(t *testing.T) {
	iqr1, iqr2 := setupTestIQRsWithRRCs(t)

	// Read from one IQR but not the other.
	values, err := iqr2.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "c"},
		{Dtype: sutils.SS_DT_STRING, CVal: "d"},
	}, values)

	_, ok := iqr1.knownValues["col1"]
	assert.False(t, ok)
	_, ok = iqr2.knownValues["col1"]
	assert.True(t, ok)

	// Even though they have different knownValues, we should be able to append
	// them and read the correct values.
	err = iqr1.Append(iqr2)
	assert.NoError(t, err)
	values, err = iqr1.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "a"},
		{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		{Dtype: sutils.SS_DT_STRING, CVal: "c"},
		{Dtype: sutils.SS_DT_STRING, CVal: "d"},
	}, values)
}

func Test_Sort(t *testing.T) {
	iqr := NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "d"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "e"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "f"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
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

	err = iqr.Sort([]string{"col1", "col2"}, less, 100)
	assert.NoError(t, err)

	expected := map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "d"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "e"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "f"},
		},
	}

	assert.Equal(t, len(expected), len(iqr.knownValues))
	for cname, expectedValues := range expected {
		if _, ok := iqr.knownValues[cname]; !ok {
			assert.Fail(t, "missing column %v", cname)
		}

		assert.Equal(t, expectedValues, iqr.knownValues[cname],
			"actual=%v, expected=%v, cname=%v", iqr.knownValues[cname],
			expectedValues, cname)
	}
}

func Test_Sort_multipleColumns(t *testing.T) {
	rrcs := []*sutils.RecordResultContainer{
		{SegKeyInfo: sutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 1},
		{SegKeyInfo: sutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 2},
		{SegKeyInfo: sutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 3},
		{SegKeyInfo: sutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 4},
	}
	mockReader := &record.MockRRCsReader{
		RRCs: rrcs,
		FieldToValues: map[string][]sutils.CValueEnclosure{
			"col1": {
				{Dtype: sutils.SS_DT_STRING, CVal: "a"},
				{Dtype: sutils.SS_DT_STRING, CVal: "b"},
				{Dtype: sutils.SS_DT_STRING, CVal: "b"},
				{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			},
			"col2": {
				{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
				{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
				{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
				{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
			},
		},
	}

	iqr := NewIQR(0)
	iqr.reader = NewIQRReader(mockReader)
	err := iqr.AppendRRCs(rrcs, map[uint32]string{1: "segKey1"})
	assert.NoError(t, err)

	less := func(a, b *Record) bool {
		aVal1, err := a.ReadColumn("col1")
		assert.NoError(t, err)

		bVal1, err := b.ReadColumn("col1")
		assert.NoError(t, err)

		if aVal1.CVal.(string) != bVal1.CVal.(string) {
			return aVal1.CVal.(string) < bVal1.CVal.(string)
		}

		aVal2, err := a.ReadColumn("col2")
		assert.NoError(t, err)

		bVal2, err := b.ReadColumn("col2")
		assert.NoError(t, err)

		return aVal2.CVal.(uint64) < bVal2.CVal.(uint64)
	}

	err = iqr.Sort([]string{"col1", "col2"}, less, 100)
	assert.NoError(t, err)
	values, err := iqr.ReadAllColumns()
	assert.NoError(t, err)
	assert.Equal(t, map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		},
		"col2": {
			{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
			{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
			{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
			{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
		},
	}, values)
}

func Test_ReverseRecords(t *testing.T) {
	iqr := NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "d"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "e"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "f"},
		},
	})
	assert.NoError(t, err)

	err = iqr.ReverseRecords()
	assert.NoError(t, err)

	expected := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "f"},
		{Dtype: sutils.SS_DT_STRING, CVal: "e"},
		{Dtype: sutils.SS_DT_STRING, CVal: "d"},
		{Dtype: sutils.SS_DT_STRING, CVal: "c"},
		{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		{Dtype: sutils.SS_DT_STRING, CVal: "a"},
	}

	col1, err := iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, col1)
}

func Test_MergeIQRs(t *testing.T) {
	iqr1 := NewIQR(0)
	iqr2 := NewIQR(0)
	iqr3 := NewIQR(0)

	err := iqr1.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "e"},
		},
	})
	assert.NoError(t, err)

	err = iqr2.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "f"},
		},
	})
	assert.NoError(t, err)

	err = iqr3.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "d"},
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
	assert.Equal(t, map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "d"},
		},
	}, mergedIqr.knownValues)

	// The merged records should have been discarded from the input IQRs.
	assert.Equal(t, 1, iqr1.NumberOfRecords())
	assert.Equal(t, 1, iqr2.NumberOfRecords())
	assert.Equal(t, 0, iqr3.NumberOfRecords())
}

func Test_MergeIQR_withNotSetIQRs(t *testing.T) {
	less := func(a, b *Record) bool {
		aVal, err := a.ReadColumn("col1")
		assert.NoError(t, err)

		bVal, err := b.ReadColumn("col1")
		assert.NoError(t, err)

		return aVal.CVal.(string) < bVal.CVal.(string)
	}

	_, firstExhaustedIndex, err := MergeIQRs([]*IQR{NewIQR(0), NewIQR(0), NewIQR(0)}, less)
	assert.NoError(t, err)
	assert.Equal(t, 0, firstExhaustedIndex)

	iqr1 := NewIQR(0)
	err = iqr1.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "e"},
		},
	})

	assert.NoError(t, err)
	_, firstExhausted, err := MergeIQRs([]*IQR{NewIQR(0), iqr1}, less)
	assert.NoError(t, err)
	assert.Equal(t, 0, firstExhausted)
}

func Test_Mode_AfterAppendRRC(t *testing.T) {
	iqr := NewIQR(0)
	assert.Equal(t, notSet, iqr.mode)

	encodingToSegKey := map[uint32]string{1: "segKey1"}

	err := iqr.AppendRRCs([]*sutils.RecordResultContainer{}, encodingToSegKey)
	assert.NoError(t, err)
	assert.Equal(t, withRRCs, iqr.mode)
}

func Test_Discard(t *testing.T) {
	knownValues := map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "d"},
		},
	}
	iqr := NewIQR(0)
	err := iqr.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	err = iqr.Discard(3)
	assert.NoError(t, err)
	assert.Equal(t, 1, iqr.NumberOfRecords())

	values, err := iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, knownValues["col1"][3:], values)

	err = iqr.Discard(0)
	assert.NoError(t, err)
	assert.Equal(t, 1, iqr.NumberOfRecords())

	values, err = iqr.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, knownValues["col1"][3:], values)

	err = iqr.Discard(1)
	assert.NoError(t, err)
	assert.Equal(t, 0, iqr.NumberOfRecords())

	err = iqr.Discard(1)
	assert.Error(t, err)
}

func Test_DiscardAfter(t *testing.T) {
	iqr := NewIQR(0)
	segKeyInfo1 := sutils.SegKeyInfo{
		SegKeyEnc: 1,
	}
	encodingToSegKey := map[uint32]string{1: "segKey1"}
	rrcs := []*sutils.RecordResultContainer{
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

func getTestKnownValues() map[string][]sutils.CValueEnclosure {
	return map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a1"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a2"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a3"},
		},
		"col2": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b1"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b2"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b3"},
		},
	}
}

func Test_MergeWithoutRRCIQRIntoRRCIQR(t *testing.T) {
	rrcIqr := NewIQR(0)
	segKeyInfo1 := sutils.SegKeyInfo{
		SegKeyEnc: 1,
	}
	encodingToSegKey := map[uint32]string{1: "segKey1"}
	rrcs := []*sutils.RecordResultContainer{
		{SegKeyInfo: segKeyInfo1, BlockNum: 1, RecordNum: 1},
		{SegKeyInfo: segKeyInfo1, BlockNum: 1, RecordNum: 2},
		{SegKeyInfo: segKeyInfo1, BlockNum: 1, RecordNum: 3},
	}

	err := rrcIqr.AppendRRCs(rrcs, encodingToSegKey)
	assert.NoError(t, err)

	withoutRRCIqr := NewIQR(0)
	knownValues := getTestKnownValues()
	err = withoutRRCIqr.AppendKnownValues(knownValues)
	assert.NoError(t, err)
	numGeneratedCol := withoutRRCIqr.NumberOfRecords()
	assert.Equal(t, 3, numGeneratedCol)

	err = rrcIqr.Append(withoutRRCIqr)
	assert.Nil(t, err)
	assert.Equal(t, withRRCs, rrcIqr.mode)
	assert.Equal(t, 6, rrcIqr.NumberOfRecords())

	for col, values := range rrcIqr.knownValues {
		for i, value := range values {
			if i < numGeneratedCol {
				assert.True(t, value.IsNull())
			} else {
				assert.Equal(t, knownValues[col][i-numGeneratedCol], value)
			}
		}
	}

	withoutRRCIqr.knownValues["col1"] = withoutRRCIqr.knownValues["col1"][1:]
	err = rrcIqr.Append(withoutRRCIqr)
	assert.Error(t, err)
}

func Test_AppendKnownValuesWithIncorrectColValues(t *testing.T) {
	knownValues := getTestKnownValues()

	knownValues["col1"] = knownValues["col1"][1:]
	newIQR := NewIQR(0)
	err := newIQR.AppendKnownValues(knownValues)
	assert.Error(t, err)
}

func Test_getRRCIQR(t *testing.T) {
	knownValues := getTestKnownValues()
	withoutRRCIqr := NewIQR(0)
	err := withoutRRCIqr.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	convertedRRCIQR, err := withoutRRCIqr.getRRCIQR()
	assert.NoError(t, err)
	assert.Equal(t, withRRCs, convertedRRCIQR.mode)
	assert.Equal(t, 3, len(convertedRRCIQR.rrcs))
	assert.Equal(t, knownValues, convertedRRCIQR.knownValues)
	assert.Equal(t, 3, convertedRRCIQR.NumberOfRecords())
}

func test_Rename(t *testing.T, iqr *IQR, oldNames []string, newName string, expectedValue []sutils.CValueEnclosure) {
	for _, oldName := range oldNames {
		values, err := iqr.ReadColumn(oldName)
		assert.NoError(t, err)
		assert.Nil(t, values)
	}
	values, err := iqr.ReadColumn(newName)
	assert.NoError(t, err)
	assert.Equal(t, expectedValue, values)
}

func Test_RenameColumn(t *testing.T) {
	iqr := NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
		},
	})
	assert.NoError(t, err)

	err = iqr.RenameColumn("col1", "newCol1")
	assert.NoError(t, err)
	test_Rename(t, iqr, []string{"col1"}, "newCol1", []sutils.CValueEnclosure{{Dtype: sutils.SS_DT_STRING, CVal: "a"}})

	// Rename the renamed column.
	err = iqr.RenameColumn("newCol1", "superNewCol1")
	assert.NoError(t, err)
	test_Rename(t, iqr, []string{"col1", "newCol1"}, "superNewCol1", []sutils.CValueEnclosure{{Dtype: sutils.SS_DT_STRING, CVal: "a"}})
}

func Test_RenameColumn_GrpBy_MeasureCol(t *testing.T) {
	iqr := NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
		},
		"col2": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		},
		"col3": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c"},
		},
	})
	iqr.measureColumns = []string{"col2", "col1"}
	iqr.groupbyColumns = []string{"col3"}
	assert.NoError(t, err)

	// Rename measure column
	err = iqr.RenameColumn("col1", "newCol1")
	assert.NoError(t, err)
	test_Rename(t, iqr, []string{"col1"}, "newCol1", []sutils.CValueEnclosure{{Dtype: sutils.SS_DT_STRING, CVal: "a"}})

	err = iqr.RenameColumn("col2", "newCol2")
	assert.NoError(t, err)
	test_Rename(t, iqr, []string{"col2"}, "newCol2", []sutils.CValueEnclosure{{Dtype: sutils.SS_DT_STRING, CVal: "b"}})

	// Rename group by Column
	err = iqr.RenameColumn("col3", "newCol3")
	assert.NoError(t, err)
	test_Rename(t, iqr, []string{"col3"}, "newCol3", []sutils.CValueEnclosure{{Dtype: sutils.SS_DT_STRING, CVal: "c"}})

	assert.Equal(t, []string{"newCol2", "newCol1"}, iqr.measureColumns)
	assert.Equal(t, []string{"newCol3"}, iqr.groupbyColumns)
}

func Test_RenameMultiple(t *testing.T) {
	iqr1 := NewIQR(0)

	err := iqr1.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
		},
		"col2": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		},
		"col3": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c"},
		},
		"col4": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "d"},
		},
	})
	iqr1.measureColumns = []string{"col1"}
	iqr1.groupbyColumns = []string{"col3"}
	assert.NoError(t, err)

	// Rename measure column
	err = iqr1.RenameColumn("col1", "newCol1")
	assert.NoError(t, err)
	err = iqr1.RenameColumn("newCol1", "superNewCol1")
	assert.NoError(t, err)
	test_Rename(t, iqr1, []string{"col1", "newCol1"}, "superNewCol1", []sutils.CValueEnclosure{{Dtype: sutils.SS_DT_STRING, CVal: "a"}})

	// Rename group by Column
	err = iqr1.RenameColumn("col3", "newCol3")
	assert.NoError(t, err)
	test_Rename(t, iqr1, []string{"col3"}, "newCol3", []sutils.CValueEnclosure{{Dtype: sutils.SS_DT_STRING, CVal: "c"}})

	assert.Equal(t, []string{"superNewCol1"}, iqr1.measureColumns)
	assert.Equal(t, []string{"newCol3"}, iqr1.groupbyColumns)

	// Rename RRC column
	err = iqr1.RenameColumn("rrcCol", "col2")
	assert.NoError(t, err)
	_, exist := iqr1.knownValues["col2"]
	assert.False(t, exist)
	assert.Equal(t, "col2", iqr1.renamedColumns["rrcCol"])

	// Rename knownValue column over RRC column
	err = iqr1.RenameColumn("col4", "col2")
	assert.NoError(t, err)
	test_Rename(t, iqr1, []string{"col4"}, "col2", []sutils.CValueEnclosure{{Dtype: sutils.SS_DT_STRING, CVal: "d"}})
}

func Test_GetColumnsOrder(t *testing.T) {
	iqr := NewIQR(0)
	knownValues := map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
		},
		"col2": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		},
		"col22": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c"},
		},
		"col3": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "d"},
		},
		"col0": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "e"},
		},
		"col23": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "e"},
		},
	}
	err := iqr.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	iqr.AddColumnIndex(map[string]int{
		"col1":  0,
		"col2":  1,
		"col22": 1,
		"col3":  2,
	})

	assert.Equal(t, []string{"col1", "col2", "col22", "col3", "col0", "col23"}, iqr.GetColumnsOrder(utils.GetKeysOfMap(knownValues)))
}

func getTestValuesForGroupBy() ([]*structs.BucketHolder, []string, []string) {
	groupByCols := []string{"val1", "val2"}
	measureFuncs := []string{"count", "sum(x)", "avg(y)"}

	bucketHolderSlice := []*structs.BucketHolder{
		{
			GroupByValues: []string{"a", "b"},
			IGroupByValues: []sutils.CValueEnclosure{
				{CVal: "a", Dtype: sutils.SS_DT_STRING},
				{CVal: "b", Dtype: sutils.SS_DT_STRING},
			},
			MeasureVal: map[string]interface{}{
				"count":  int64(10),
				"sum(x)": int64(100),
				"avg(y)": int64(10),
			},
		},
		{
			GroupByValues: []string{"a", "c"},
			IGroupByValues: []sutils.CValueEnclosure{
				{CVal: "a", Dtype: sutils.SS_DT_STRING},
				{CVal: "c", Dtype: sutils.SS_DT_STRING},
			},
			MeasureVal: map[string]interface{}{
				"count":  int64(20),
				"sum(x)": int64(200),
				"avg(y)": int64(20),
			},
		},
		{
			GroupByValues: []string{"d", "e"},
			IGroupByValues: []sutils.CValueEnclosure{
				{CVal: "d", Dtype: sutils.SS_DT_STRING},
				{CVal: "e", Dtype: sutils.SS_DT_STRING},
			},
			MeasureVal: map[string]interface{}{
				"count":  int64(30),
				"sum(x)": int64(300),
				"avg(y)": int64(30),
			},
		},
	}

	return bucketHolderSlice, groupByCols, measureFuncs
}

func getTestValuesForSegmentStats() ([]*structs.BucketHolder, []string, []string) {
	groupByCols := []string{}
	measureFuncs := []string{"count", "sum(x)", "avg(y)"}

	bucketHolderSlice := []*structs.BucketHolder{
		{
			GroupByValues: []string{},
			MeasureVal: map[string]interface{}{
				"count":  int64(10),
				"sum(x)": int64(100),
				"avg(y)": int64(10),
			},
		},
	}

	return bucketHolderSlice, groupByCols, measureFuncs
}

func Test_CreateRRCStatsResults_GroupBy(t *testing.T) {
	bucketHolderSlice, groupByCols, measureFuncs := getTestValuesForGroupBy()

	bucketCount := len(bucketHolderSlice)

	iqr := NewIQR(0)
	err := iqr.CreateStatsResults(bucketHolderSlice, measureFuncs, groupByCols, bucketCount)
	assert.NoError(t, err)
	assert.Equal(t, withoutRRCs, iqr.mode)

	expected := map[string][]sutils.CValueEnclosure{
		"val1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "d"},
		},
		"val2": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "e"},
		},
		"count": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(10)},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(20)},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(30)},
		},
		"sum(x)": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(100)},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(200)},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(300)},
		},
		"avg(y)": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(10)},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(20)},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(30)},
		},
	}

	assert.Equal(t, len(expected), len(iqr.knownValues))
	assert.Equal(t, groupByCols, iqr.groupbyColumns)
	assert.Equal(t, measureFuncs, iqr.measureColumns)

	for cname, expectedValues := range expected {
		values, err := iqr.ReadColumn(cname)
		assert.NoError(t, err)

		assert.Equal(t, expectedValues, values, "cname=%v", cname)
	}
}

func Test_CreateRRCStatsResults_SegmentStats(t *testing.T) {
	bucketHolderSlice, groupByCols, measureFuncs := getTestValuesForSegmentStats()
	bucketCount := len(bucketHolderSlice)

	iqr := NewIQR(0)
	err := iqr.CreateStatsResults(bucketHolderSlice, measureFuncs, groupByCols, bucketCount)
	assert.NoError(t, err)
	assert.Equal(t, withoutRRCs, iqr.mode)

	expected := map[string][]sutils.CValueEnclosure{
		"count": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(10)},
		},
		"sum(x)": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(100)},
		},
		"avg(y)": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(10)},
		},
	}

	assert.Equal(t, len(expected), len(iqr.knownValues))
	assert.Equal(t, groupByCols, iqr.groupbyColumns)
	assert.Equal(t, measureFuncs, iqr.measureColumns)

	for cname, expectedValues := range expected {
		values, err := iqr.ReadColumn(cname)
		assert.NoError(t, err)

		assert.Equal(t, expectedValues, values, "cname=%v", cname)
	}
}

func Test_getFinalStatsResults(t *testing.T) {
	bucketHolderSlice, groupByCols, measureFuncs := getTestValuesForGroupBy()
	bucketCount := len(bucketHolderSlice)

	iqr := NewIQR(0)
	err := iqr.CreateStatsResults(bucketHolderSlice, measureFuncs, groupByCols, bucketCount)
	assert.NoError(t, err)

	actualBucketHolderSlice, actualGroupByCols, actualMeasureFuncs, actualBucketCount, err := iqr.getFinalStatsResults()
	assert.NoError(t, err)
	assert.Equal(t, bucketCount, actualBucketCount)
	assert.Equal(t, groupByCols, actualGroupByCols)
	assert.ElementsMatch(t, measureFuncs, actualMeasureFuncs)

	for i, expectedBucketHolder := range bucketHolderSlice {
		actualBucketHolder := actualBucketHolderSlice[i]
		assert.Equal(t, expectedBucketHolder, actualBucketHolder, "i=%v", i)
	}

	bucketHolderSlice, groupByCols, measureFuncs = getTestValuesForSegmentStats()
	bucketCount = len(bucketHolderSlice)

	iqr = NewIQR(0)
	err = iqr.CreateStatsResults(bucketHolderSlice, measureFuncs, groupByCols, bucketCount)
	assert.NoError(t, err)

	actualBucketHolderSlice, actualGroupByCols, actualMeasureFuncs, actualBucketCount, err = iqr.getFinalStatsResults()
	assert.NoError(t, err)
	assert.Equal(t, bucketCount, actualBucketCount)
	assert.Equal(t, groupByCols, actualGroupByCols)
	assert.ElementsMatch(t, measureFuncs, actualMeasureFuncs)

	for i, expectedBucketHolder := range bucketHolderSlice {
		actualBucketHolder := actualBucketHolderSlice[i]
		if len(expectedBucketHolder.GroupByValues) == 0 {
			expectedBucketHolder.GroupByValues = []string{"*"}
			expectedBucketHolder.IGroupByValues = []sutils.CValueEnclosure{{CVal: "*", Dtype: sutils.SS_DT_STRING}}
		}
		assert.Equal(t, expectedBucketHolder, actualBucketHolder, "i=%v", i)
	}
}

func Test_ReadColumnsWithBackfill(t *testing.T) {
	iqr := NewIQR(0)
	knownValues := map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a1"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b1"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c1"},
		},
		"col2": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a2"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b2"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c2"},
		},
		"col3": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a3"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b3"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c3"},
		},
	}
	err := iqr.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	columnValues, err := iqr.ReadColumnsWithBackfill([]string{"col1", "col2"})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(columnValues))
	assert.Equal(t, knownValues["col1"], columnValues["col1"])
	assert.Equal(t, knownValues["col2"], columnValues["col2"])

	columnValues, err = iqr.ReadColumnsWithBackfill([]string{"col3", "col4"})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(columnValues))
	assert.Equal(t, knownValues["col3"], columnValues["col3"])
	expectedBackfilledCol := []sutils.CValueEnclosure{}
	for i := 0; i < 3; i++ {
		expectedBackfilledCol = append(expectedBackfilledCol, *backfillCVal)
	}
	assert.Equal(t, expectedBackfilledCol, columnValues["col4"])
}

func Test_IQRBytesEncodeDecode(t *testing.T) {
	iqr := NewIQR(0)
	knownValues := map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a1"},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b1"},
		},
		"col2": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(1)},
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(2)},
		},
	}

	rrcs := []*sutils.RecordResultContainer{
		{
			SegKeyInfo:       sutils.SegKeyInfo{SegKeyEnc: 1, RecordId: "record1"},
			BlockNum:         1,
			RecordNum:        1,
			SortColumnValue:  float64(1),
			TimeStamp:        uint64(1),
			VirtualTableName: "vt1",
		},
		{
			SegKeyInfo:       sutils.SegKeyInfo{SegKeyEnc: 2, RecordId: "record2"},
			BlockNum:         2,
			RecordNum:        2,
			SortColumnValue:  float64(2),
			TimeStamp:        uint64(2),
			VirtualTableName: "vt2",
		},
	}

	segStatsMap := make(map[string]*structs.SegStats)
	segStatsMap["segKey1"] = &structs.SegStats{
		IsNumeric: true,
		Count:     10,
		Min:       sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(1)},
		Max:       sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(10)},
		Hll:       structs.CreateNewHll(),
		NumStats: &structs.NumericStats{
			Sum: sutils.NumTypeEnclosure{
				Ntype:    sutils.SS_DT_SIGNED_NUM,
				IntgrVal: int64(55),
				FloatVal: float64(55),
			},
		},
		StringStats: &structs.StringStats{
			StrSet: map[string]struct{}{
				"a": {},
				"b": {},
				"c": {},
			},
			StrList: []string{"a", "b", "c"},
		},
	}

	allRunningBuckets := blockresults.GetRunningBucketResultsSliceForTest()

	timeBuckets := &blockresults.TimeBuckets{
		AllRunningBuckets: allRunningBuckets,
		UnsignedBucketIdx: map[uint64]int{1: 0, 2: 1},
	}

	groupByBuckets := &blockresults.GroupByBuckets{
		AllRunningBuckets: allRunningBuckets,
		StringBucketIdx:   map[string]int{"a": 0, "b": 1, "c": 2},
		GroupByColValCnt:  map[string]int{"a": 1, "b": 1, "c": 1},
	}

	statsRes := &IQRStatsResults{
		aggs:           &structs.QueryAggregators{},
		segStatsMap:    segStatsMap,
		timeBuckets:    timeBuckets,
		groupByBuckets: groupByBuckets,
	}

	iqr.encodingToSegKey = map[uint32]string{1: "segKey1", 2: "segKey2"}
	iqr.rrcs = rrcs
	iqr.knownValues = knownValues
	iqr.mode = withRRCs
	iqr.measureColumns = []string{"col2"}
	iqr.groupbyColumns = []string{"col1"}
	iqr.deletedColumns = map[string]struct{}{"col2": {}}
	iqr.renamedColumns = map[string]string{"col1": "newCol1"}
	iqr.columnIndex = map[string]int{"col1": 0, "col2": 1}
	iqr.statsResults = statsRes

	bytes, err := iqr.GobEncode()
	assert.NoError(t, err)

	decodedIQR := NewIQRWithReader(0, &record.RRCsReader{})

	err = decodedIQR.GobDecode(bytes)
	assert.NoError(t, err)

	assert.Equal(t, iqr, decodedIQR)

	statsRes.timeBuckets = nil
	iqr.statsResults = statsRes

	bytes, err = iqr.GobEncode()
	assert.NoError(t, err)

	decodedIQR = NewIQRWithReader(0, &record.RRCsReader{})

	err = decodedIQR.GobDecode(bytes)
	assert.NoError(t, err)

	assert.Equal(t, iqr, decodedIQR)
}

func Test_RemovesEmptyColumns(t *testing.T) {
	iqr := NewIQR(0)
	iqr.mode = withoutRRCs

	knownValues := map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "value1"},
			{Dtype: sutils.SS_DT_STRING, CVal: "value2"},
		},
		"empty_col": {
			{Dtype: sutils.SS_DT_STRING, CVal: ""},
			{Dtype: sutils.SS_DT_STRING, CVal: ""},
		},
	}
	err := iqr.AppendKnownValues(knownValues)
	require.NoError(t, err)

	expected := &structs.PipeSearchResponseOuter{
		Hits: structs.PipeSearchResponse{
			TotalMatched: utils.HitsCount{Value: 2, Relation: "eq"},
			Hits: []map[string]interface{}{
				{"col1": "value1"},
				{"col1": "value2"},
			},
		},
		AllPossibleColumns: []string{"col1"},
		Errors:             nil,
		Qtype:              "logs-query",
		CanScrollMore:      false,
		ColumnsOrder:       []string{"col1"},
	}

	result, err := iqr.AsResult(structs.RRCCmd, false, true)
	require.NoError(t, err)
	assert.Equal(t, expected, result)
}

func Test__RemoveEmptyRecords(t *testing.T) {
	iqr := NewIQR(0)
	iqr.mode = withoutRRCs

	knownValues := map[string][]sutils.CValueEnclosure{}
	err := iqr.AppendKnownValues(knownValues)
	require.NoError(t, err)

	expected := &structs.PipeSearchResponseOuter{
		Hits: structs.PipeSearchResponse{
			TotalMatched: utils.HitsCount{Value: 0, Relation: "eq"},
			Hits:         []map[string]interface{}{},
		},
		AllPossibleColumns: []string{},
		Errors:             nil,
		Qtype:              "logs-query",
		CanScrollMore:      false,
		ColumnsOrder:       []string{},
	}

	result, err := iqr.AsResult(structs.RRCCmd, false, true)
	require.NoError(t, err)
	assert.Equal(t, expected, result)
}

func Test_RemoveOneEmptyRecord(t *testing.T) {
	iqr := NewIQR(0)
	iqr.mode = withoutRRCs

	knownValues := map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "value1"},
			{Dtype: sutils.SS_DT_STRING, CVal: ""},
		},
		"col2": {
			{Dtype: sutils.SS_DT_STRING, CVal: "value2"},
			{Dtype: sutils.SS_DT_STRING, CVal: ""},
		},
	}

	err := iqr.AppendKnownValues(knownValues)
	require.NoError(t, err)

	expected := &structs.PipeSearchResponseOuter{
		Hits: structs.PipeSearchResponse{
			TotalMatched: utils.HitsCount{Value: 1, Relation: "eq"},
			Hits: []map[string]interface{}{
				{
					"col1": "value1",
					"col2": "value2",
				},
			},
		},
		AllPossibleColumns: []string{"col1", "col2"},
		Errors:             nil,
		Qtype:              "logs-query",
		CanScrollMore:      false,
		ColumnsOrder:       []string{"col1", "col2"},
	}

	result, err := iqr.AsResult(structs.RRCCmd, false, true)
	sort.Strings(result.AllPossibleColumns)
	require.NoError(t, err)
	assert.Equal(t, expected, result)
}
