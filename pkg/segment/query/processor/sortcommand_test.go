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
	"io"
	"testing"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/reader/record"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_SortCommand_simple(t *testing.T) {
	sorter := &sortProcessor{
		options: &structs.SortExpr{
			SortEles: []*structs.SortElement{
				{Field: "col1", SortByAsc: true, Op: ""},
			},
			Limit: 4,
		},
	}

	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(map[string][]segutils.CValueEnclosure{
		"col1": {
			{Dtype: segutils.SS_DT_STRING, CVal: "c"},
			{Dtype: segutils.SS_DT_STRING, CVal: "a"},
			{Dtype: segutils.SS_DT_STRING, CVal: "b"},
			{Dtype: segutils.SS_DT_STRING, CVal: "e"},
			{Dtype: segutils.SS_DT_STRING, CVal: "d"},
			{Dtype: segutils.SS_DT_STRING, CVal: "f"},
		},
	})
	assert.NoError(t, err)

	_, err = sorter.Process(iqr1)
	assert.NoError(t, err)

	expected := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "a"},
		{Dtype: segutils.SS_DT_STRING, CVal: "b"},
		{Dtype: segutils.SS_DT_STRING, CVal: "c"},
		{Dtype: segutils.SS_DT_STRING, CVal: "d"},
	}

	actualCol1, err := sorter.resultsSoFar.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actualCol1)

	// Add more records that should replace some existing ones.
	iqr2 := iqr.NewIQR(0)
	err = iqr2.AppendKnownValues(map[string][]segutils.CValueEnclosure{
		"col1": {
			{Dtype: segutils.SS_DT_STRING, CVal: "g"},
			{Dtype: segutils.SS_DT_STRING, CVal: "apple"},
			{Dtype: segutils.SS_DT_STRING, CVal: "banana"},
			{Dtype: segutils.SS_DT_STRING, CVal: "h"},
		},
	})
	assert.NoError(t, err)

	_, err = sorter.Process(iqr2)
	assert.NoError(t, err)

	expected = []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "a"},
		{Dtype: segutils.SS_DT_STRING, CVal: "apple"},
		{Dtype: segutils.SS_DT_STRING, CVal: "b"},
		{Dtype: segutils.SS_DT_STRING, CVal: "banana"},
	}

	actualCol1, err = sorter.resultsSoFar.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actualCol1)

	// Get the final results.
	finalIQR, err := sorter.Process(nil)
	assert.Equal(t, io.EOF, err)

	actualCol1, err = finalIQR.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actualCol1)
}

func Test_SortCommand_withTieBreakers(t *testing.T) {
	sorter := &sortProcessor{
		options: &structs.SortExpr{
			SortEles: []*structs.SortElement{
				{Field: "col1", SortByAsc: true, Op: ""},
				{Field: "col2", SortByAsc: false, Op: "num"},
			},
			Limit: 4,
		},
	}

	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(map[string][]segutils.CValueEnclosure{
		"col1": {
			{Dtype: segutils.SS_DT_STRING, CVal: "a"},
			{Dtype: segutils.SS_DT_STRING, CVal: "b"},
		},
		"col2": {
			{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(1)},
			{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(2)},
		},
	})
	assert.NoError(t, err)

	_, err = sorter.Process(iqr1)
	assert.NoError(t, err)

	expectedCol1 := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "a"},
		{Dtype: segutils.SS_DT_STRING, CVal: "b"},
	}
	expectedCol2 := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(1)},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(2)},
	}

	actualCol1, err := sorter.resultsSoFar.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol1, actualCol1)

	actualCol2, err := sorter.resultsSoFar.ReadColumn("col2")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol2, actualCol2)

	// Add more records.
	iqr2 := iqr.NewIQR(0)
	err = iqr2.AppendKnownValues(map[string][]segutils.CValueEnclosure{
		"col1": {
			{Dtype: segutils.SS_DT_STRING, CVal: "a"},
			{Dtype: segutils.SS_DT_STRING, CVal: "b"},
		},
		"col2": {
			{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(3)},
			{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(0)},
		},
	})
	assert.NoError(t, err)

	_, err = sorter.Process(iqr2)
	assert.NoError(t, err)

	expectedCol1 = []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "a"},
		{Dtype: segutils.SS_DT_STRING, CVal: "a"},
		{Dtype: segutils.SS_DT_STRING, CVal: "b"},
		{Dtype: segutils.SS_DT_STRING, CVal: "b"},
	}
	expectedCol2 = []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(3)},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(1)},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(2)},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(0)},
	}

	actualCol1, err = sorter.resultsSoFar.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol1, actualCol1)

	actualCol2, err = sorter.resultsSoFar.ReadColumn("col2")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol2, actualCol2)

	// Get the final results.
	finalIQR, err := sorter.Process(nil)
	assert.Equal(t, io.EOF, err)

	actualCol1, err = finalIQR.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol1, actualCol1)

	actualCol2, err = finalIQR.ReadColumn("col2")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol2, actualCol2)
}

func Test_SortCommand_withRRCs(t *testing.T) {
	sorter := &sortProcessor{
		options: &structs.SortExpr{
			SortEles: []*structs.SortElement{
				{Field: "col1", SortByAsc: true, Op: ""},
				{Field: "col2", SortByAsc: true, Op: "num"},
			},
			Limit: 100,
		},
	}

	rrcs := []*segutils.RecordResultContainer{
		{SegKeyInfo: segutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 1},
		{SegKeyInfo: segutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 2},
		{SegKeyInfo: segutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 3},
		{SegKeyInfo: segutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 4},
		{SegKeyInfo: segutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 5},
		{SegKeyInfo: segutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 6},
	}
	mockReader := &record.MockRRCsReader{
		RRCs: rrcs,
		FieldToValues: map[string][]segutils.CValueEnclosure{
			"col1": {
				{Dtype: segutils.SS_DT_STRING, CVal: "a"},
				{Dtype: segutils.SS_DT_STRING, CVal: "e"},
				{Dtype: segutils.SS_DT_STRING, CVal: "c"},

				{Dtype: segutils.SS_DT_STRING, CVal: "z"},
				{Dtype: segutils.SS_DT_STRING, CVal: "b"},
				{Dtype: segutils.SS_DT_STRING, CVal: "c"},
			},
			"col2": {
				{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
				{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
				{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
				{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
				{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
				{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(4)},
			},
		},
	}

	iqr1 := iqr.NewIQRWithReader(0, mockReader)
	iqr2 := iqr.NewIQRWithReader(0, mockReader)

	err := iqr1.AppendRRCs(rrcs[:3], map[uint32]string{1: "segKey1"})
	assert.NoError(t, err)

	err = iqr2.AppendRRCs(rrcs[3:], map[uint32]string{1: "segKey1"})
	assert.NoError(t, err)

	_, err = sorter.Process(iqr1)
	assert.NoError(t, err)
	_, err = sorter.Process(iqr2)
	assert.NoError(t, err)
	result, err := sorter.Process(nil)
	assert.Equal(t, io.EOF, err)

	expectedCol1 := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "a"},
		{Dtype: segutils.SS_DT_STRING, CVal: "b"},
		{Dtype: segutils.SS_DT_STRING, CVal: "c"},
		{Dtype: segutils.SS_DT_STRING, CVal: "c"},
		{Dtype: segutils.SS_DT_STRING, CVal: "e"},
		{Dtype: segutils.SS_DT_STRING, CVal: "z"},
	}
	expectedCol2 := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(4)},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
	}

	actualCol1, err := result.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol1, actualCol1)

	actualCol2, err := result.ReadColumn("col2")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol2, actualCol2)
}

func getKnownValuesForSortTest() map[string][]segutils.CValueEnclosure {
	return map[string][]segutils.CValueEnclosure{
		"col1": {
			{Dtype: segutils.SS_DT_STRING, CVal: "z"},
			{Dtype: segutils.SS_DT_STRING, CVal: "a"},
			{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
			{Dtype: segutils.SS_DT_FLOAT, CVal: float64(3)},
			{Dtype: segutils.SS_DT_STRING, CVal: "2"},
			{Dtype: segutils.SS_DT_STRING, CVal: "1"},
			{Dtype: segutils.SS_DT_STRING, CVal: "15"},
			{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
			{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(5)},
			{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(4)},
			{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(16)},
			{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(21)},
			{Dtype: segutils.SS_DT_BOOL, CVal: true},
			{Dtype: segutils.SS_DT_BOOL, CVal: false},
			{Dtype: segutils.SS_DT_BOOL, CVal: false},
			{Dtype: segutils.SS_DT_BOOL, CVal: true},
			{Dtype: segutils.SS_DT_STRING, CVal: "A"},
			{Dtype: segutils.SS_DT_STRING, CVal: "Z"},
		},
	}
}

func TestSortMultipleDataTypes(t *testing.T) {

	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(getKnownValuesForSortTest())
	assert.NoError(t, err)

	// Test with ascending order.
	sorter := &sortProcessor{
		options: &structs.SortExpr{
			SortEles: []*structs.SortElement{
				{Field: "col1", SortByAsc: true, Op: ""},
			},
			Limit: 100,
		},
	}

	expected := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "1"},
		{Dtype: segutils.SS_DT_STRING, CVal: "2"},
		{Dtype: segutils.SS_DT_FLOAT, CVal: float64(3)},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(4)},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(5)},
		{Dtype: segutils.SS_DT_STRING, CVal: "15"},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(16)},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(21)},
		{Dtype: segutils.SS_DT_STRING, CVal: "A"},
		{Dtype: segutils.SS_DT_STRING, CVal: "Z"},
		{Dtype: segutils.SS_DT_STRING, CVal: "a"},
		{Dtype: segutils.SS_DT_BOOL, CVal: false},
		{Dtype: segutils.SS_DT_BOOL, CVal: false},
		{Dtype: segutils.SS_DT_BOOL, CVal: true},
		{Dtype: segutils.SS_DT_BOOL, CVal: true},
		{Dtype: segutils.SS_DT_STRING, CVal: "z"},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
	}

	_, err = sorter.Process(iqr1)
	assert.NoError(t, err)
	result, err := sorter.Process(nil)
	assert.Equal(t, io.EOF, err)

	actualCol1, err := result.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actualCol1)

	// Test with descending order.
	sorter = &sortProcessor{
		options: &structs.SortExpr{
			SortEles: []*structs.SortElement{
				{Field: "col1", SortByAsc: false, Op: ""},
			},
			Limit: 100,
		},
	}

	iqr2 := iqr.NewIQR(0)
	err = iqr2.AppendKnownValues(getKnownValuesForSortTest())
	assert.NoError(t, err)

	_, err = sorter.Process(iqr2)
	assert.NoError(t, err)

	result, err = sorter.Process(nil)
	assert.Equal(t, io.EOF, err)

	expected = []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "z"},
		{Dtype: segutils.SS_DT_BOOL, CVal: true},
		{Dtype: segutils.SS_DT_BOOL, CVal: true},
		{Dtype: segutils.SS_DT_BOOL, CVal: false},
		{Dtype: segutils.SS_DT_BOOL, CVal: false},
		{Dtype: segutils.SS_DT_STRING, CVal: "a"},
		{Dtype: segutils.SS_DT_STRING, CVal: "Z"},
		{Dtype: segutils.SS_DT_STRING, CVal: "A"},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(21)},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(16)},
		{Dtype: segutils.SS_DT_STRING, CVal: "15"},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(5)},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(4)},
		{Dtype: segutils.SS_DT_FLOAT, CVal: float64(3)},
		{Dtype: segutils.SS_DT_STRING, CVal: "2"},
		{Dtype: segutils.SS_DT_STRING, CVal: "1"},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
	}

	actualCol1, err = result.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actualCol1)
}

func TestSortMultipleDataTypesWithStrOperator(t *testing.T) {
	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(getKnownValuesForSortTest())
	assert.NoError(t, err)

	// Test with ascending order.
	sorter := &sortProcessor{
		options: &structs.SortExpr{
			SortEles: []*structs.SortElement{
				{Field: "col1", SortByAsc: true, Op: "str"},
			},
			Limit: 100,
		},
	}

	expected := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "1"},
		{Dtype: segutils.SS_DT_STRING, CVal: "15"},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(16)},
		{Dtype: segutils.SS_DT_STRING, CVal: "2"},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(21)},
		{Dtype: segutils.SS_DT_FLOAT, CVal: float64(3)},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(4)},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(5)},
		{Dtype: segutils.SS_DT_STRING, CVal: "A"},
		{Dtype: segutils.SS_DT_STRING, CVal: "Z"},
		{Dtype: segutils.SS_DT_STRING, CVal: "a"},
		{Dtype: segutils.SS_DT_BOOL, CVal: false},
		{Dtype: segutils.SS_DT_BOOL, CVal: false},
		{Dtype: segutils.SS_DT_BOOL, CVal: true},
		{Dtype: segutils.SS_DT_BOOL, CVal: true},
		{Dtype: segutils.SS_DT_STRING, CVal: "z"},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
	}

	_, err = sorter.Process(iqr1)
	assert.NoError(t, err)
	result, err := sorter.Process(nil)
	assert.Equal(t, io.EOF, err)

	actualCol1, err := result.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actualCol1)

	// Test with descending order.
	sorter = &sortProcessor{
		options: &structs.SortExpr{
			SortEles: []*structs.SortElement{
				{Field: "col1", SortByAsc: false, Op: "str"},
			},
			Limit: 100,
		},
	}

	iqr2 := iqr.NewIQR(0)
	err = iqr2.AppendKnownValues(getKnownValuesForSortTest())
	assert.NoError(t, err)

	_, err = sorter.Process(iqr2)
	assert.NoError(t, err)

	result, err = sorter.Process(nil)
	assert.Equal(t, io.EOF, err)

	expected = []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "z"},
		{Dtype: segutils.SS_DT_BOOL, CVal: true},
		{Dtype: segutils.SS_DT_BOOL, CVal: true},
		{Dtype: segutils.SS_DT_BOOL, CVal: false},
		{Dtype: segutils.SS_DT_BOOL, CVal: false},
		{Dtype: segutils.SS_DT_STRING, CVal: "a"},
		{Dtype: segutils.SS_DT_STRING, CVal: "Z"},
		{Dtype: segutils.SS_DT_STRING, CVal: "A"},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(5)},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(4)},
		{Dtype: segutils.SS_DT_FLOAT, CVal: float64(3)},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(21)},
		{Dtype: segutils.SS_DT_STRING, CVal: "2"},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(16)},
		{Dtype: segutils.SS_DT_STRING, CVal: "15"},
		{Dtype: segutils.SS_DT_STRING, CVal: "1"},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
	}

	actualCol1, err = result.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actualCol1)
}

func TestSortMultipleDataTypesWithNumOperator(t *testing.T) {

	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(getKnownValuesForSortTest())
	assert.NoError(t, err)

	// Test with ascending order.
	sorter := &sortProcessor{
		options: &structs.SortExpr{
			SortEles: []*structs.SortElement{
				{Field: "col1", SortByAsc: true, Op: "num"},
			},
			Limit: 100,
		},
	}

	expected := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "1"},
		{Dtype: segutils.SS_DT_STRING, CVal: "2"},
		{Dtype: segutils.SS_DT_FLOAT, CVal: float64(3)},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(4)},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(5)},
		{Dtype: segutils.SS_DT_STRING, CVal: "15"},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(16)},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(21)},
		{Dtype: segutils.SS_DT_STRING, CVal: "A"},
		{Dtype: segutils.SS_DT_STRING, CVal: "Z"},
		{Dtype: segutils.SS_DT_STRING, CVal: "a"},
		{Dtype: segutils.SS_DT_BOOL, CVal: false},
		{Dtype: segutils.SS_DT_BOOL, CVal: false},
		{Dtype: segutils.SS_DT_BOOL, CVal: true},
		{Dtype: segutils.SS_DT_BOOL, CVal: true},
		{Dtype: segutils.SS_DT_STRING, CVal: "z"},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
	}

	_, err = sorter.Process(iqr1)
	assert.NoError(t, err)
	result, err := sorter.Process(nil)
	assert.Equal(t, io.EOF, err)

	actualCol1, err := result.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actualCol1)

	// Test with descending order.
	sorter = &sortProcessor{
		options: &structs.SortExpr{
			SortEles: []*structs.SortElement{
				{Field: "col1", SortByAsc: false, Op: "num"},
			},
			Limit: 100,
		},
	}

	iqr2 := iqr.NewIQR(0)
	err = iqr2.AppendKnownValues(getKnownValuesForSortTest())
	assert.NoError(t, err)

	_, err = sorter.Process(iqr2)
	assert.NoError(t, err)

	result, err = sorter.Process(nil)
	assert.Equal(t, io.EOF, err)

	expected = []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "z"},
		{Dtype: segutils.SS_DT_BOOL, CVal: true},
		{Dtype: segutils.SS_DT_BOOL, CVal: true},
		{Dtype: segutils.SS_DT_BOOL, CVal: false},
		{Dtype: segutils.SS_DT_BOOL, CVal: false},
		{Dtype: segutils.SS_DT_STRING, CVal: "a"},
		{Dtype: segutils.SS_DT_STRING, CVal: "Z"},
		{Dtype: segutils.SS_DT_STRING, CVal: "A"},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(21)},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(16)},
		{Dtype: segutils.SS_DT_STRING, CVal: "15"},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(5)},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(4)},
		{Dtype: segutils.SS_DT_FLOAT, CVal: float64(3)},
		{Dtype: segutils.SS_DT_STRING, CVal: "2"},
		{Dtype: segutils.SS_DT_STRING, CVal: "1"},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
	}

	actualCol1, err = result.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expected, actualCol1)
}
