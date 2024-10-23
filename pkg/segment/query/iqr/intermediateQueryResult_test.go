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

	"github.com/siglens/siglens/pkg/segment/structs"
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

	expectedResult := &structs.PipeSearchResponseOuter{
		Hits: structs.PipeSearchResponse{
			TotalMatched: toputils.HitsCount{Value: 2, Relation: "eq"},
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
	result, err := iqr.AsResult(structs.RRCCmd)
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

func Test_mergeMetadata_modes(t *testing.T) {
	iqr1 := NewIQR(0)
	iqr2 := NewIQR(0)

	// Incompatible modes.
	iqr1.mode = withRRCs
	iqr2.mode = withoutRRCs
	_, err := mergeMetadata([]*IQR{iqr1, iqr2})
	assert.Error(t, err)

	// Same modes.
	iqr1.mode = withRRCs
	iqr2.mode = withRRCs
	iqr, err := mergeMetadata([]*IQR{iqr1, iqr2})
	assert.NoError(t, err)
	assert.Equal(t, withRRCs, iqr.mode)

	// First is unset.
	iqr1.mode = notSet
	iqr2.mode = withoutRRCs
	iqr, err = mergeMetadata([]*IQR{iqr1, iqr2})
	assert.NoError(t, err)
	assert.Equal(t, withoutRRCs, iqr.mode)

	// Second is unset.
	iqr1.mode = withoutRRCs
	iqr2.mode = notSet
	iqr, err = mergeMetadata([]*IQR{iqr1, iqr2})
	assert.NoError(t, err)
	assert.Equal(t, withoutRRCs, iqr.mode)
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

func Test_Append_withRRCs(t *testing.T) {
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

	otherIqr := NewIQR(0)
	segKeyInfo2 := utils.SegKeyInfo{
		SegKeyEnc: 2,
	}
	encodingToSegKey2 := map[uint16]string{2: "segKey2"}
	rrcs2 := []*utils.RecordResultContainer{
		{SegKeyInfo: segKeyInfo2, BlockNum: 1, RecordNum: 1},
		{SegKeyInfo: segKeyInfo2, BlockNum: 1, RecordNum: 2},
		{SegKeyInfo: segKeyInfo2, BlockNum: 1, RecordNum: 3},
	}
	err = otherIqr.AppendRRCs(rrcs2, encodingToSegKey2)
	assert.NoError(t, err)

	err = otherIqr.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
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

func Test_Sort(t *testing.T) {
	iqr := NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "d"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "e"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "f"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
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

	err = iqr.Sort(less)
	assert.NoError(t, err)

	expected := map[string][]utils.CValueEnclosure{
		"col1": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "d"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "e"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "f"},
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

func Test_RenameColumn(t *testing.T) {
	iqr := NewIQR(0)
	err := iqr.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
		},
	})
	assert.NoError(t, err)

	err = iqr.RenameColumn("col1", "newCol1")
	assert.NoError(t, err)
	_, err = iqr.ReadColumn("col1")
	assert.Error(t, err)
	values, err := iqr.ReadColumn("newCol1")
	assert.NoError(t, err)
	assert.Equal(t, []utils.CValueEnclosure{{Dtype: utils.SS_DT_STRING, CVal: "a"}}, values)

	// Rename the renamed column.
	err = iqr.RenameColumn("newCol1", "superNewCol1")
	assert.NoError(t, err)
	_, err = iqr.ReadColumn("col1")
	assert.Error(t, err)
	_, err = iqr.ReadColumn("newCol1")
	assert.Error(t, err)
	values, err = iqr.ReadColumn("superNewCol1")
	assert.NoError(t, err)
	assert.Equal(t, []utils.CValueEnclosure{{Dtype: utils.SS_DT_STRING, CVal: "a"}}, values)
}

func Test_GetColumnsOrder(t *testing.T) {
	iqr := NewIQR(0)
	knownValues := map[string][]utils.CValueEnclosure{
		"col1": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
		},
		"col2": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
		},
		"col22": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
		},
		"col3": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "d"},
		},
		"col0": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "e"},
		},
		"col23": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "e"},
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

	assert.Equal(t, []string{"col1", "col2", "col22", "col3", "col0", "col23"}, iqr.GetColumnsOrder(toputils.GetKeysOfMap(knownValues)))
}

func getTestValuesForGroupBy() ([]*structs.BucketHolder, []string, []string) {
	groupByCols := []string{"val1", "val2"}
	measureFuncs := []string{"count", "sum(x)", "avg(y)"}

	bucketHolderSlice := []*structs.BucketHolder{
		&structs.BucketHolder{
			GroupByValues: []string{"a", "b"},
			MeasureVal: map[string]interface{}{
				"count":  int64(10),
				"sum(x)": int64(100),
				"avg(y)": int64(10),
			},
		},
		&structs.BucketHolder{
			GroupByValues: []string{"a", "c"},
			MeasureVal: map[string]interface{}{
				"count":  int64(20),
				"sum(x)": int64(200),
				"avg(y)": int64(20),
			},
		},
		&structs.BucketHolder{
			GroupByValues: []string{"d", "e"},
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
		&structs.BucketHolder{
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

func Test_AppendRRCStatsResults_GroupBy(t *testing.T) {
	bucketHolderSlice, groupByCols, measureFuncs := getTestValuesForGroupBy()

	bucketCount := len(bucketHolderSlice)

	iqr := NewIQR(0)
	err := iqr.AppendStatsResults(bucketHolderSlice, measureFuncs, groupByCols, bucketCount)
	assert.NoError(t, err)
	assert.Equal(t, withoutRRCs, iqr.mode)

	expected := map[string][]utils.CValueEnclosure{
		"val1": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "d"},
		},
		"val2": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "e"},
		},
		"count": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(10)},
			utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(20)},
			utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(30)},
		},
		"sum(x)": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(100)},
			utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(200)},
			utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(300)},
		},
		"avg(y)": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(10)},
			utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(20)},
			utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(30)},
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

func Test_AppendRRCStatsResults_SegmentStats(t *testing.T) {
	bucketHolderSlice, groupByCols, measureFuncs := getTestValuesForSegmentStats()
	bucketCount := len(bucketHolderSlice)

	iqr := NewIQR(0)
	err := iqr.AppendStatsResults(bucketHolderSlice, measureFuncs, groupByCols, bucketCount)
	assert.NoError(t, err)
	assert.Equal(t, withoutRRCs, iqr.mode)

	expected := map[string][]utils.CValueEnclosure{
		"count": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(10)},
		},
		"sum(x)": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(100)},
		},
		"avg(y)": {
			utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(10)},
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
	err := iqr.AppendStatsResults(bucketHolderSlice, measureFuncs, groupByCols, bucketCount)
	assert.NoError(t, err)

	actualBucketHolderSlice, actualGroupByCols, actualMeasureFuncs, actualBucketCount, err := iqr.getFinalStatsResults()
	assert.NoError(t, err)
	assert.Equal(t, bucketCount, actualBucketCount)
	assert.Equal(t, groupByCols, actualGroupByCols)
	assert.Equal(t, measureFuncs, actualMeasureFuncs)

	for i, expectedBucketHolder := range bucketHolderSlice {
		actualBucketHolder := actualBucketHolderSlice[i]
		assert.Equal(t, expectedBucketHolder, actualBucketHolder, "i=%v", i)
	}

	bucketHolderSlice, groupByCols, measureFuncs = getTestValuesForSegmentStats()
	bucketCount = len(bucketHolderSlice)

	iqr = NewIQR(0)
	err = iqr.AppendStatsResults(bucketHolderSlice, measureFuncs, groupByCols, bucketCount)
	assert.NoError(t, err)

	actualBucketHolderSlice, actualGroupByCols, actualMeasureFuncs, actualBucketCount, err = iqr.getFinalStatsResults()
	assert.NoError(t, err)
	assert.Equal(t, bucketCount, actualBucketCount)
	assert.Equal(t, groupByCols, actualGroupByCols)
	assert.Equal(t, measureFuncs, actualMeasureFuncs)

	for i, expectedBucketHolder := range bucketHolderSlice {
		actualBucketHolder := actualBucketHolderSlice[i]
		assert.Equal(t, expectedBucketHolder, actualBucketHolder, "i=%v", i)
	}
}
