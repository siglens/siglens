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

	"github.com/stretchr/testify/assert"
)

func writeTestData(t *testing.T) (string, string) {
	t.Helper()

	data := map[string]map[uint16][]uint16{
		"apple": {
			1: {1, 2},
			2: {100, 42},
		},
		"zebra": {
			1: {7},
		},
		"banana": {
			2: {13, 2, 7},
		},
	}

	segkey := filepath.Join(t.TempDir(), "test-segkey")
	cname := "col1"
	err := writeSortIndex(segkey, cname, data)
	assert.NoError(t, err)

	return segkey, cname
}

func readAndAssert(t *testing.T, segkey, cname string, maxRecords int, checkpoint *Checkpoint,
	expected []Line) *Checkpoint {

	t.Helper()
	actual, checkpoint, err := ReadSortIndex(segkey, cname, maxRecords, checkpoint)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)

	return checkpoint
}

func Test_writeAndRead(t *testing.T) {
	segkey, cname := writeTestData(t)

	_ = readAndAssert(t, segkey, cname, 100, nil, []Line{
		{Value: "apple", Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1, 2}},
			{BlockNum: 2, RecNums: []uint16{42, 100}},
		}},
		{Value: "banana", Blocks: []Block{
			{BlockNum: 2, RecNums: []uint16{2, 7, 13}},
		}},
		{Value: "zebra", Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{7}},
		}},
	})

	_ = readAndAssert(t, segkey, cname, 3, nil, []Line{
		{Value: "apple", Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1, 2}},
			{BlockNum: 2, RecNums: []uint16{42}},
		}},
	})
}

func Test_readFromCheckpointAtStartOfLine(t *testing.T) {
	segkey, cname := writeTestData(t)

	checkpoint := readAndAssert(t, segkey, cname, 4, nil, []Line{
		{Value: "apple", Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1, 2}},
			{BlockNum: 2, RecNums: []uint16{42, 100}},
		}},
	})

	_ = readAndAssert(t, segkey, cname, 4, checkpoint, []Line{
		{Value: "banana", Blocks: []Block{
			{BlockNum: 2, RecNums: []uint16{2, 7, 13}},
		}},
		{Value: "zebra", Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{7}},
		}},
	})
}

func Test_readFromCheckpointInMiddleOfLine(t *testing.T) {
	segkey, cname := writeTestData(t)

	checkpoint := readAndAssert(t, segkey, cname, 2, nil, []Line{
		{Value: "apple", Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1, 2}},
		}},
	})

	_ = readAndAssert(t, segkey, cname, 2, checkpoint, []Line{
		{Value: "apple", Blocks: []Block{
			{BlockNum: 2, RecNums: []uint16{42, 100}},
		}},
	})
}

func Test_readFromCheckpointInMiddleOfLine_test2(t *testing.T) {
	data := map[string]map[uint16][]uint16{
		"blue": {
			1: {1},
			2: {2, 3},
		},
		"green": {
			1: {10, 11},
			2: {1},
		},
	}

	segkey := filepath.Join(t.TempDir(), "test-segkey")
	cname := "col1"
	err := writeSortIndex(segkey, cname, data)
	assert.NoError(t, err)

	checkpoint := readAndAssert(t, segkey, cname, 1, nil, []Line{
		{Value: "blue", Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1}},
		}},
	})

	_ = readAndAssert(t, segkey, cname, 1, checkpoint, []Line{
		{Value: "blue", Blocks: []Block{
			{BlockNum: 2, RecNums: []uint16{2}},
		}},
	})
}
