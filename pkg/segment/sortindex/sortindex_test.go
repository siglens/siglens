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

func Test_writeAndRead(t *testing.T) {
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

	maxRecords := 100
	expected := []Line{
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
	}

	actual, _, err := ReadSortIndex(segkey, cname, maxRecords, nil)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)

	// Test with maxRecords
	maxRecords = 3
	expected = []Line{
		{Value: "apple", Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1, 2}},
			{BlockNum: 2, RecNums: []uint16{42}},
		}},
	}

	actual, _, err = ReadSortIndex(segkey, cname, maxRecords, nil)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func Test_read_fromCheckpoint(t *testing.T) {
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

	maxRecords := 4
	expected := []Line{
		{Value: "apple", Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{1, 2}},
			{BlockNum: 2, RecNums: []uint16{42, 100}},
		}},
	}

	actual, checkpoint, err := ReadSortIndex(segkey, cname, maxRecords, nil)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)

	maxRecords = 4
	expected = []Line{
		{Value: "banana", Blocks: []Block{
			{BlockNum: 2, RecNums: []uint16{2, 7, 13}},
		}},
		{Value: "zebra", Blocks: []Block{
			{BlockNum: 1, RecNums: []uint16{7}},
		}},
	}

	t.Logf("using checkkpoint: %v", checkpoint)

	actual, checkpoint, err = ReadSortIndex(segkey, cname, maxRecords, checkpoint)
	assert.NoError(t, err)
	assert.Equal(t, expected, actual)
}
