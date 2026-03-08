// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package record

import (
	"testing"

	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

var mockRRCs = []*sutils.RecordResultContainer{
	{BlockNum: 1, RecordNum: 1},
	{BlockNum: 1, RecordNum: 2},
	{BlockNum: 1, RecordNum: 3},
	{BlockNum: 2, RecordNum: 1},
	{BlockNum: 2, RecordNum: 2},
	{BlockNum: 2, RecordNum: 3},
}

func Test_GetColumnNames(t *testing.T) {
	mocker := &MockRRCsReader{
		FieldToValues: map[string][]sutils.CValueEnclosure{
			"col1": {},
			"col2": {},
		},
	}

	expected := map[string]struct{}{
		"col1": {},
		"col2": {},
	}

	columns, err := mocker.GetColsForSegKey("segKey", "vTable")
	assert.NoError(t, err)
	assert.Equal(t, expected, columns)
}

func Test_ReadOneColumn(t *testing.T) {
	rrcs := mockRRCs[:4]
	mocker := &MockRRCsReader{
		RRCs: rrcs,
		FieldToValues: map[string][]sutils.CValueEnclosure{
			"col1": {
				{Dtype: sutils.SS_DT_STRING, CVal: "val1"},
				{Dtype: sutils.SS_DT_STRING, CVal: "val2"},
				{Dtype: sutils.SS_DT_STRING, CVal: "val3"},
				{Dtype: sutils.SS_DT_STRING, CVal: "val4"},
			},
		},
	}

	expected := []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "val1"},
		{Dtype: sutils.SS_DT_STRING, CVal: "val2"},
		{Dtype: sutils.SS_DT_STRING, CVal: "val3"},
		{Dtype: sutils.SS_DT_STRING, CVal: "val4"},
	}

	values, err := mocker.ReadColForRRCs("segKey", rrcs, "col1", 0, false)
	assert.NoError(t, err)
	assert.Equal(t, expected, values)

	// Test reading a non-existent column.
	values, err = mocker.ReadColForRRCs("segKey", rrcs, "col2", 0, false)
	assert.NoError(t, err)
	assert.Nil(t, values)

	// Test reading RRCs in a different order.
	rrcs = []*sutils.RecordResultContainer{
		mockRRCs[3],
		mockRRCs[1],
		mockRRCs[0],
		mockRRCs[2],
	}

	expected = []sutils.CValueEnclosure{
		{Dtype: sutils.SS_DT_STRING, CVal: "val4"},
		{Dtype: sutils.SS_DT_STRING, CVal: "val2"},
		{Dtype: sutils.SS_DT_STRING, CVal: "val1"},
		{Dtype: sutils.SS_DT_STRING, CVal: "val3"},
	}

	values, err = mocker.ReadColForRRCs("segKey", rrcs, "col1", 0, false)
	assert.NoError(t, err)
	assert.Equal(t, expected, values)
}

func Test_ReadAllColumns(t *testing.T) {
	rrcs := mockRRCs[:2]
	mocker := &MockRRCsReader{
		RRCs: rrcs,
		FieldToValues: map[string][]sutils.CValueEnclosure{
			"col1": {
				{Dtype: sutils.SS_DT_STRING, CVal: "val1"},
				{Dtype: sutils.SS_DT_STRING, CVal: "val2"},
			},
			"col2": {
				{Dtype: sutils.SS_DT_STRING, CVal: "apple"},
				{Dtype: sutils.SS_DT_STRING, CVal: "banana"},
			},
		},
	}

	expected := map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "val1"},
			{Dtype: sutils.SS_DT_STRING, CVal: "val2"},
		},
		"col2": {
			{Dtype: sutils.SS_DT_STRING, CVal: "apple"},
			{Dtype: sutils.SS_DT_STRING, CVal: "banana"},
		},
	}

	ignoredCols := map[string]struct{}{}
	results, err := mocker.ReadAllColsForRRCs("segKey", "vTable", mockRRCs[:2], 0, ignoredCols)
	assert.NoError(t, err)
	assert.Equal(t, expected, results)

	// Test ignoring a column.
	ignoredCols = map[string]struct{}{
		"col1": {},
	}
	expected = map[string][]sutils.CValueEnclosure{
		"col2": {
			{Dtype: sutils.SS_DT_STRING, CVal: "apple"},
			{Dtype: sutils.SS_DT_STRING, CVal: "banana"},
		},
	}
	results, err = mocker.ReadAllColsForRRCs("segKey", "vTable", mockRRCs[:2], 0, ignoredCols)
	assert.NoError(t, err)
	assert.Equal(t, expected, results)

	// Test reading RRCs in a different order.
	rrcs = []*sutils.RecordResultContainer{
		mockRRCs[1],
		mockRRCs[0],
	}
	ignoredCols = map[string]struct{}{}
	expected = map[string][]sutils.CValueEnclosure{
		"col1": {
			{Dtype: sutils.SS_DT_STRING, CVal: "val2"},
			{Dtype: sutils.SS_DT_STRING, CVal: "val1"},
		},
		"col2": {
			{Dtype: sutils.SS_DT_STRING, CVal: "banana"},
			{Dtype: sutils.SS_DT_STRING, CVal: "apple"},
		},
	}
	results, err = mocker.ReadAllColsForRRCs("segKey", "vTable", rrcs, 0, ignoredCols)
	assert.NoError(t, err)
	assert.Equal(t, expected, results)
}
