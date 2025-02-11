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
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_TailCommand_Simple(t *testing.T) {
	tail := &tailProcessor{
		options: &structs.TailExpr{
			TailRows: 4,
		},
	}

	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
		},
		"col2": {
			{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(1)},
			{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(2)},
			{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(3)},
		},
	})
	assert.NoError(t, err)

	expectedCol1 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "b"},
		{Dtype: utils.SS_DT_STRING, CVal: "a"},
		{Dtype: utils.SS_DT_STRING, CVal: "c"},
	}

	expectedCol2 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(3)},
		{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(2)},
		{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(1)},
	}

	for {
		iqr1, err = tail.Process(iqr1)
		if iqr1 != nil {
			assert.Equal(t, io.EOF, err)
			break
		}
		assert.NoError(t, err)
	}

	col1, err := iqr1.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol1, col1)

	col2, err := iqr1.ReadColumn("col2")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol2, col2)
}

func Test_TailCommand_Discard(t *testing.T) {
	tail := &tailProcessor{
		options: &structs.TailExpr{
			TailRows: 4,
		},
	}

	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
			{Dtype: utils.SS_DT_STRING, CVal: "e"},
			{Dtype: utils.SS_DT_STRING, CVal: "d"},
			{Dtype: utils.SS_DT_STRING, CVal: "f"},
		},
		"col2": {
			{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(1)},
			{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(2)},
			{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(3)},
			{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(4)},
			{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(5)},
			{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(6)},
		},
	})
	assert.NoError(t, err)

	expectedCol1 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "f"},
		{Dtype: utils.SS_DT_STRING, CVal: "d"},
		{Dtype: utils.SS_DT_STRING, CVal: "e"},
		{Dtype: utils.SS_DT_STRING, CVal: "b"},
	}

	expectedCol2 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(6)},
		{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(5)},
		{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(4)},
		{Dtype: utils.SS_DT_SIGNED_NUM, CVal: int64(3)},
	}

	for {
		iqr1, err = tail.Process(iqr1)
		if iqr1 != nil {
			assert.Equal(t, io.EOF, err)
			break
		}
		assert.NoError(t, err)
	}

	col1, err := iqr1.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol1, col1)

	col2, err := iqr1.ReadColumn("col2")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol2, col2)
}

func Test_TailCommand_WithRRC(t *testing.T) {
	tail := &tailProcessor{
		options: &structs.TailExpr{
			TailRows: 3,
		},
	}

	rrcs := []*utils.RecordResultContainer{
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 1},
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 2},
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 3},
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 4},
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 5},
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 6},
	}
	mockReader := &record.MockRRCsReader{
		RRCs: rrcs,
		FieldToValues: map[string][]utils.CValueEnclosure{
			"col1": {
				{Dtype: utils.SS_DT_STRING, CVal: "a"},
				{Dtype: utils.SS_DT_STRING, CVal: "b"},
				{Dtype: utils.SS_DT_STRING, CVal: "c"},
				{Dtype: utils.SS_DT_STRING, CVal: "d"},
				{Dtype: utils.SS_DT_STRING, CVal: "e"},
				{Dtype: utils.SS_DT_STRING, CVal: "f"},
			},
			"col2": {
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(4)},
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(5)},
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(6)},
			},
		},
	}

	iqr1 := iqr.NewIQRWithReader(0, mockReader)
	iqr2 := iqr.NewIQRWithReader(0, mockReader)
	iqr3 := iqr.NewIQRWithReader(0, mockReader)

	err := iqr1.AppendRRCs(rrcs[:2], map[uint32]string{1: "segKey1"})
	assert.NoError(t, err)

	err = iqr2.AppendRRCs(rrcs[2:4], map[uint32]string{1: "segKey1"})
	assert.NoError(t, err)

	err = iqr3.AppendRRCs(rrcs[4:], map[uint32]string{1: "segKey1"})
	assert.NoError(t, err)

	_, err = tail.Process(iqr1)
	assert.NoError(t, err)
	_, err = tail.Process(iqr2)
	assert.NoError(t, err)
	_, err = tail.Process(iqr3)
	assert.NoError(t, err)

	result, err := tail.Process(nil)
	assert.Equal(t, io.EOF, err)

	expectedCol1 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "f"},
		{Dtype: utils.SS_DT_STRING, CVal: "e"},
		{Dtype: utils.SS_DT_STRING, CVal: "d"},
	}

	expectedCol2 := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(6)},
		{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(5)},
		{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(4)},
	}

	col1, err := result.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol1, col1)

	col2, err := result.ReadColumn("col2")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol2, col2)
}
