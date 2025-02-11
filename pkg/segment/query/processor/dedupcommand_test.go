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

func makeDedup(t *testing.T, limit uint64, fieldList []string, consecutive bool,
	keepEmpty bool, keepEvents bool) *DataProcessor {

	options := &structs.DedupExpr{
		Limit:     limit,
		FieldList: fieldList,
		DedupOptions: &structs.DedupOptions{
			KeepEmpty:   keepEmpty,
			Consecutive: consecutive,
			KeepEvents:  keepEvents,
		},
	}

	dataProcessor := NewDedupDP(options)
	assert.NotNil(t, dataProcessor)
	return dataProcessor
}

func Test_Dedup_consecutive(t *testing.T) {
	dataProcessor := makeDedup(t, 1, []string{"col1"}, true, false, false)

	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
		},
	})
	assert.NoError(t, err)

	result1, err := dataProcessor.processor.Process(iqr1)
	assert.NoError(t, err)
	assert.NotNil(t, result1)

	expected := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "a"},
		{Dtype: utils.SS_DT_STRING, CVal: "b"},
	}
	actual, err := result1.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)

	iqr2 := iqr.NewIQR(0)
	err = iqr2.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
		},
	})
	assert.NoError(t, err)

	result2, err := dataProcessor.processor.Process(iqr2)
	assert.NoError(t, err)
	assert.NotNil(t, result2)

	// The last batch ended with a "b", so the "b" at the start of this batch
	// should be dropped.
	expected = []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "c"},
	}
	actual, err = result2.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func Test_Dedup_keepEmpty(t *testing.T) {
	dataProcessor := makeDedup(t, 1, []string{"col1"}, true, true, false)

	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
			{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
		},
	})
	assert.NoError(t, err)

	result1, err := dataProcessor.processor.Process(iqr1)
	assert.NoError(t, err)
	assert.NotNil(t, result1)

	expected := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "a"},
		{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
		{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
	}
	actual, err := result1.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func Test_Dedup_dropEmpty(t *testing.T) {
	dataProcessor := makeDedup(t, 1, []string{"col1"}, true, false, false)

	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
			{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
		},
	})
	assert.NoError(t, err)

	result1, err := dataProcessor.processor.Process(iqr1)
	assert.NoError(t, err)
	assert.NotNil(t, result1)

	expected := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "a"},
	}
	actual, err := result1.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func Test_Dedup_keepEvents(t *testing.T) {
	dataProcessor := makeDedup(t, 1, []string{"col1"}, true, false, true)

	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
		},
	})
	assert.NoError(t, err)

	result1, err := dataProcessor.processor.Process(iqr1)
	assert.NoError(t, err)
	assert.NotNil(t, result1)

	expected := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "a"},
		{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
		{Dtype: utils.SS_DT_STRING, CVal: "b"},
		{Dtype: utils.SS_DT_STRING, CVal: "a"},
		{Dtype: utils.SS_DT_BACKFILL, CVal: nil},
	}
	actual, err := result1.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func Test_Dedup_nonconsecutive(t *testing.T) {
	dataProcessor := makeDedup(t, 1, []string{"col1"}, false, false, false)

	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
		},
	})
	assert.NoError(t, err)

	result1, err := dataProcessor.processor.Process(iqr1)
	assert.NoError(t, err)
	assert.NotNil(t, result1)

	expected := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "a"},
		{Dtype: utils.SS_DT_STRING, CVal: "b"},
	}
	actual, err := result1.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)

	iqr2 := iqr.NewIQR(0)
	err = iqr2.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
		},
	})
	assert.NoError(t, err)

	result2, err := dataProcessor.processor.Process(iqr2)
	assert.NoError(t, err)
	assert.NotNil(t, result2)

	// The previous batch already had "a" and "b", so they should be dropped.
	expected = []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "c"},
	}
	actual, err = result2.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func Test_Dedup_withLimit(t *testing.T) {
	dataProcessor := makeDedup(t, 2, []string{"col1"}, false, false, false)

	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
		},
	})
	assert.NoError(t, err)

	result1, err := dataProcessor.processor.Process(iqr1)
	assert.NoError(t, err)
	assert.NotNil(t, result1)

	expected := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "a"},
		{Dtype: utils.SS_DT_STRING, CVal: "a"},
		{Dtype: utils.SS_DT_STRING, CVal: "b"},
	}
	actual, err := result1.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)

	iqr2 := iqr.NewIQR(0)
	err = iqr2.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
		},
	})
	assert.NoError(t, err)

	result2, err := dataProcessor.processor.Process(iqr2)
	assert.NoError(t, err)
	assert.NotNil(t, result2)

	expected = []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "c"},
		{Dtype: utils.SS_DT_STRING, CVal: "b"},
		{Dtype: utils.SS_DT_STRING, CVal: "c"},
	}
	actual, err = result2.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func Test_Dedup_multipleCols(t *testing.T) {
	dataProcessor := makeDedup(t, 1, []string{"col1", "col2"}, false, false, false)

	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
		},
		"col2": {
			{Dtype: utils.SS_DT_STRING, CVal: "x"},
			{Dtype: utils.SS_DT_STRING, CVal: "y"},
			{Dtype: utils.SS_DT_STRING, CVal: "z"},
			{Dtype: utils.SS_DT_STRING, CVal: "x"},
		},
	})
	assert.NoError(t, err)

	result1, err := dataProcessor.processor.Process(iqr1)
	assert.NoError(t, err)
	assert.NotNil(t, result1)

	expectedCol1 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "a"},
		{Dtype: utils.SS_DT_STRING, CVal: "a"},
		{Dtype: utils.SS_DT_STRING, CVal: "b"},
	}
	expectedCol2 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "x"},
		{Dtype: utils.SS_DT_STRING, CVal: "y"},
		{Dtype: utils.SS_DT_STRING, CVal: "z"},
	}

	actualCol1, err := result1.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol1, actualCol1)

	actualCol2, err := result1.ReadColumn("col2")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol2, actualCol2)
}

func Test_Dedup_WithSort(t *testing.T) {
	stream := &mockStreamer{
		allRecords: map[string][]utils.CValueEnclosure{
			"col1": {
				{Dtype: utils.SS_DT_STRING, CVal: "a"},
				{Dtype: utils.SS_DT_STRING, CVal: "a"},
				{Dtype: utils.SS_DT_STRING, CVal: "a"},
				{Dtype: utils.SS_DT_STRING, CVal: "b"},
				{Dtype: utils.SS_DT_STRING, CVal: "b"},
				{Dtype: utils.SS_DT_STRING, CVal: "b"},
				{Dtype: utils.SS_DT_STRING, CVal: "c"},
				{Dtype: utils.SS_DT_STRING, CVal: "c"},
			},
			"col2": {
				{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(0)},
				{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(1)},
				{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(3)},
				{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(2)},
				{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(4)},
				{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(5)},
				{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(6)},
				{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(7)},
			},
		},
		qid: 0,
	}

	sortProcessor := NewSortDP(&structs.SortExpr{
		SortEles: []*structs.SortElement{
			{Field: "col2", SortByAsc: true, Op: "num"},
		},
		Limit: 1000,
	})
	sortProcessor.streams = []*CachedStream{{stream, nil, false}}

	dedupProcessor := NewDedupDP(&structs.DedupExpr{
		DedupOptions: &structs.DedupOptions{
			Consecutive: true,
		},
		FieldList: []string{"col1"},
	})

	dedupProcessor.streams = []*CachedStream{{sortProcessor, nil, false}}

	result, err := dedupProcessor.Fetch()
	assert.NoError(t, err)
	assert.NotNil(t, result)

	expectedCol1 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "a"},
		{Dtype: utils.SS_DT_STRING, CVal: "b"},
		{Dtype: utils.SS_DT_STRING, CVal: "a"},
		{Dtype: utils.SS_DT_STRING, CVal: "b"},
		{Dtype: utils.SS_DT_STRING, CVal: "c"},
	}

	expectedCol2 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(0)},
		{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(2)},
		{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(3)},
		{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(4)},
		{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(6)},
	}

	actualCol1, err := result.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol1, actualCol1)

	actualCol2, err := result.ReadColumn("col2")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol2, actualCol2)
}
