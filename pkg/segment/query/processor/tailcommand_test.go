// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"io"
	"testing"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/reader/record"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_TailCommand_Simple(t *testing.T) {
	tail := &tailProcessor{
		options: &structs.TailExpr{
			TailRows: 4,
		},
	}

	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		},
		"col2": {
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(1)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(2)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(3)},
		},
	})
	assert.NoError(t, err)

	expectedCol1 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "b"},
		{Dtype: sutils.SS_DT_STRING, CVal: "a"},
		{Dtype: sutils.SS_DT_STRING, CVal: "c"},
	}

	expectedCol2 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(3)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(2)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(1)},
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
	err := iqr1.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			{Dtype: sutils.SS_DT_STRING, CVal: "a"},
			{Dtype: sutils.SS_DT_STRING, CVal: "b"},
			{Dtype: sutils.SS_DT_STRING, CVal: "e"},
			{Dtype: sutils.SS_DT_STRING, CVal: "d"},
			{Dtype: sutils.SS_DT_STRING, CVal: "f"},
		},
		"col2": {
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(1)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(2)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(3)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(4)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(5)},
			{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(6)},
		},
	})
	assert.NoError(t, err)

	expectedCol1 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "f"},
		{Dtype: sutils.SS_DT_STRING, CVal: "d"},
		{Dtype: sutils.SS_DT_STRING, CVal: "e"},
		{Dtype: sutils.SS_DT_STRING, CVal: "b"},
	}

	expectedCol2 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(6)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(5)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(4)},
		{Dtype: sutils.SS_DT_SIGNED_NUM, CVal: int64(3)},
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

	rrcs := []*sutils.RecordResultContainer{
		{SegKeyInfo: sutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 1},
		{SegKeyInfo: sutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 2},
		{SegKeyInfo: sutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 3},
		{SegKeyInfo: sutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 4},
		{SegKeyInfo: sutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 5},
		{SegKeyInfo: sutils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 6},
	}
	mockReader := &record.MockRRCsReader{
		RRCs: rrcs,
		FieldToValues: map[string][]sutils.CValueEnclosure{
			"col1": {
				{Dtype: sutils.SS_DT_STRING, CVal: "a"},
				{Dtype: sutils.SS_DT_STRING, CVal: "b"},
				{Dtype: sutils.SS_DT_STRING, CVal: "c"},
				{Dtype: sutils.SS_DT_STRING, CVal: "d"},
				{Dtype: sutils.SS_DT_STRING, CVal: "e"},
				{Dtype: sutils.SS_DT_STRING, CVal: "f"},
			},
			"col2": {
				{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
				{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
				{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
				{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(4)},
				{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(5)},
				{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(6)},
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

	expectedCol1 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "f"},
		{Dtype: sutils.SS_DT_STRING, CVal: "e"},
		{Dtype: sutils.SS_DT_STRING, CVal: "d"},
	}

	expectedCol2 := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(6)},
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(5)},
		{Dtype: sutils.SS_DT_UNSIGNED_NUM, CVal: uint64(4)},
	}

	col1, err := result.ReadColumn("col1")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol1, col1)

	col2, err := result.ReadColumn("col2")
	assert.NoError(t, err)
	assert.Equal(t, expectedCol2, col2)
}
