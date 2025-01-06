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

package sortindex

import (
	"path/filepath"
	"testing"

	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
)

func writeTestData(t *testing.T) (string, string) {
	t.Helper()

	data := map[segutils.CValueEnclosure]map[uint16][]uint16{
		{Dtype: segutils.SS_DT_STRING, CVal: "apple"}: {
			1: {1, 2},
			2: {100, 42},
		},
		{Dtype: segutils.SS_DT_STRING, CVal: "zebra"}: {
			1: {7},
		},
		{Dtype: segutils.SS_DT_STRING, CVal: "banana"}: {
			2: {13, 2, 7},
		},
	}

	segkey := filepath.Join(t.TempDir(), "test-segkey")
	cname := "col1"
	err := writeSortIndex(segkey, cname, SortAsString, data)
	assert.NoError(t, err)

	return segkey, cname
}

func readAndAssert(t *testing.T, segkey, cname string, sortMode SortMode, reverse bool,
	maxRecords int, checkpoint *Checkpoint, expected []Line) *Checkpoint {

	t.Helper()
	readFullLine := false
	actual, checkpoint, err := ReadSortIndex(segkey, cname, sortMode, reverse, maxRecords, readFullLine, checkpoint)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)

	return checkpoint
}

func Test_writeAndRead(t *testing.T) {
	segkey, cname := writeTestData(t)

	_ = readAndAssert(t, segkey, cname, SortAsString, false, 100, nil, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "apple"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1, 2}},
			{BlockNum: 2, RecNums: []uint16{42, 100}},
		}},
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "banana"}, Blocks: []Block{
			{BlockNum: 2, RecNums: []uint16{2, 7, 13}},
		}},
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "zebra"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{7}},
		}},
	})

	_ = readAndAssert(t, segkey, cname, SortAsString, false, 3, nil, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "apple"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1, 2}},
			{BlockNum: 2, RecNums: []uint16{42}},
		}},
	})
}

func Test_readFromCheckpointAtStartOfLine(t *testing.T) {
	segkey, cname := writeTestData(t)

	checkpoint := readAndAssert(t, segkey, cname, SortAsString, false, 4, nil, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "apple"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1, 2}},
			{BlockNum: 2, RecNums: []uint16{42, 100}},
		}},
	})

	_ = readAndAssert(t, segkey, cname, SortAsString, false, 4, checkpoint, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "banana"}, Blocks: []Block{
			{BlockNum: 2, RecNums: []uint16{2, 7, 13}},
		}},
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "zebra"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{7}},
		}},
	})
}

func Test_readFromCheckpointInMiddleOfLine(t *testing.T) {
	segkey, cname := writeTestData(t)

	checkpoint := readAndAssert(t, segkey, cname, SortAsString, false, 2, nil, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "apple"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1, 2}},
		}},
	})

	_ = readAndAssert(t, segkey, cname, SortAsString, false, 2, checkpoint, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "apple"}, Blocks: []Block{
			{BlockNum: 2, RecNums: []uint16{42, 100}},
		}},
	})
}

func Test_readFromCheckpointInMiddleOfBlock(t *testing.T) {
	segkey, cname := writeTestData(t)

	checkpoint := readAndAssert(t, segkey, cname, SortAsString, false, 1, nil, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "apple"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1}},
		}},
	})

	_ = readAndAssert(t, segkey, cname, SortAsString, false, 1, checkpoint, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "apple"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{2}},
		}},
	})
}

func Test_sort(t *testing.T) {
	enclosures := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "10"},
		{Dtype: segutils.SS_DT_STRING, CVal: "5"},
		{Dtype: segutils.SS_DT_STRING, CVal: "apple"},
		{Dtype: segutils.SS_DT_STRING, CVal: "zebra"},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
		{Dtype: segutils.SS_DT_BOOL, CVal: true},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(8)},
	}

	err := sortEnclosures(enclosures, SortAsString)
	assert.NoError(t, err)
	assert.Equal(t, []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "10"},
		{Dtype: segutils.SS_DT_STRING, CVal: "5"},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(8)},
		{Dtype: segutils.SS_DT_STRING, CVal: "apple"},
		{Dtype: segutils.SS_DT_BOOL, CVal: true},
		{Dtype: segutils.SS_DT_STRING, CVal: "zebra"},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
	}, enclosures)

	rand.Seed(42)
	rand.Shuffle(len(enclosures), func(i, j int) {
		enclosures[i], enclosures[j] = enclosures[j], enclosures[i]
	})

	err = sortEnclosures(enclosures, SortAsNumeric)
	assert.NoError(t, err)
	assert.Equal(t, []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "5"},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(8)},
		{Dtype: segutils.SS_DT_STRING, CVal: "10"},
		{Dtype: segutils.SS_DT_STRING, CVal: "apple"},
		{Dtype: segutils.SS_DT_BOOL, CVal: true},
		{Dtype: segutils.SS_DT_STRING, CVal: "zebra"},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
	}, enclosures)

	rand.Seed(42)
	rand.Shuffle(len(enclosures), func(i, j int) {
		enclosures[i], enclosures[j] = enclosures[j], enclosures[i]
	})

	err = sortEnclosures(enclosures, SortAsAuto)
	assert.NoError(t, err)
	assert.Equal(t, []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "5"},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(8)},
		{Dtype: segutils.SS_DT_STRING, CVal: "10"},
		{Dtype: segutils.SS_DT_STRING, CVal: "apple"},
		{Dtype: segutils.SS_DT_BOOL, CVal: true},
		{Dtype: segutils.SS_DT_STRING, CVal: "zebra"},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil},
	}, enclosures)
}

func Test_readReverse(t *testing.T) {
	segkey, cname := writeTestData(t)

	_ = readAndAssert(t, segkey, cname, SortAsString, true, 100, nil, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "zebra"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{7}},
		}},
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "banana"}, Blocks: []Block{
			{BlockNum: 2, RecNums: []uint16{2, 7, 13}},
		}},
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "apple"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1, 2}},
			{BlockNum: 2, RecNums: []uint16{42, 100}},
		}},
	})

	_ = readAndAssert(t, segkey, cname, SortAsString, true, 1, nil, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "zebra"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{7}},
		}},
	})
}

func Test_readReverseFromEndOfLineCheckpoint(t *testing.T) {
	segkey, cname := writeTestData(t)

	checkpoint := readAndAssert(t, segkey, cname, SortAsString, true, 4, nil, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "zebra"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{7}},
		}},
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "banana"}, Blocks: []Block{
			{BlockNum: 2, RecNums: []uint16{2, 7, 13}},
		}},
	})

	_ = readAndAssert(t, segkey, cname, SortAsString, true, 4, checkpoint, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "apple"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1, 2}},
			{BlockNum: 2, RecNums: []uint16{42, 100}},
		}},
	})
}

func Test_readReverseFromEndOfBlockCheckpoint(t *testing.T) {
	segkey, cname := writeTestData(t)

	checkpoint := readAndAssert(t, segkey, cname, SortAsString, true, 6, nil, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "zebra"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{7}},
		}},
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "banana"}, Blocks: []Block{
			{BlockNum: 2, RecNums: []uint16{2, 7, 13}},
		}},
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "apple"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1, 2}},
		}},
	})

	_ = readAndAssert(t, segkey, cname, SortAsString, true, 2, checkpoint, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "apple"}, Blocks: []Block{
			{BlockNum: 2, RecNums: []uint16{42, 100}},
		}},
	})
}

func Test_readReverseFromMiddleOfBlockCheckpoint(t *testing.T) {
	segkey, cname := writeTestData(t)

	checkpoint := readAndAssert(t, segkey, cname, SortAsString, true, 2, nil, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "zebra"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{7}},
		}},
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "banana"}, Blocks: []Block{
			{BlockNum: 2, RecNums: []uint16{2}},
		}},
	})

	checkpoint = readAndAssert(t, segkey, cname, SortAsString, true, 2, checkpoint, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "banana"}, Blocks: []Block{
			{BlockNum: 2, RecNums: []uint16{7, 13}},
		}},
	})

	checkpoint = readAndAssert(t, segkey, cname, SortAsString, true, 1, checkpoint, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "apple"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1}},
		}},
	})

	_ = readAndAssert(t, segkey, cname, SortAsString, true, 3, checkpoint, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "apple"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{2}},
			{BlockNum: 2, RecNums: []uint16{42, 100}},
		}},
	})
}

func Test_readMultipleTypes(t *testing.T) {
	data := map[segutils.CValueEnclosure]map[uint16][]uint16{
		{Dtype: segutils.SS_DT_STRING, CVal: "apple"}:          {1: {1, 2}},
		{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(-42)}:   {1: {3, 4}},
		{Dtype: segutils.SS_DT_FLOAT, CVal: float64(123.456)}:  {2: {5, 6}},
		{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(42)}: {2: {7, 8}},
		{Dtype: segutils.SS_DT_BOOL, CVal: true}:               {3: {9, 10}},
		{Dtype: segutils.SS_DT_BACKFILL, CVal: nil}:            {3: {11, 12}},
	}

	segkey := filepath.Join(t.TempDir(), "test-segkey")
	cname := "col1"
	err := writeSortIndex(segkey, cname, SortAsNumeric, data)
	assert.NoError(t, err)

	_ = readAndAssert(t, segkey, cname, SortAsNumeric, false, 100, nil, []Line{
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_SIGNED_NUM, CVal: int64(-42)}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{3, 4}},
		}},
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_UNSIGNED_NUM, CVal: uint64(42)}, Blocks: []Block{
			{BlockNum: 2, RecNums: []uint16{7, 8}},
		}},
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_FLOAT, CVal: float64(123.456)}, Blocks: []Block{
			{BlockNum: 2, RecNums: []uint16{5, 6}},
		}},
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: "apple"}, Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1, 2}},
		}},
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_BOOL, CVal: true}, Blocks: []Block{
			{BlockNum: 3, RecNums: []uint16{9, 10}},
		}},
		{Value: segutils.CValueEnclosure{Dtype: segutils.SS_DT_BACKFILL, CVal: nil}, Blocks: []Block{
			{BlockNum: 3, RecNums: []uint16{11, 12}},
		}},
	})
}
