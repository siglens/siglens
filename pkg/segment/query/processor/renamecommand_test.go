// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func test_Rename(t *testing.T, iqr *iqr.IQR, oldNames []string, newName string, expectedValue []sutils.CValueEnclosure) {
	for _, oldName := range oldNames {
		values, err := iqr.ReadColumn(oldName)
		assert.NoError(t, err)
		assert.Nil(t, values)
	}
	values, err := iqr.ReadColumn(newName)
	assert.NoError(t, err)
	assert.Equal(t, expectedValue, values)
}

func Test_Rename_Override(t *testing.T) {
	iqr1 := iqr.NewIQR(0)
	err := iqr1.AppendKnownValues(map[string][]sutils.CValueEnclosure{
		"col1": {
			sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
		},
	})
	assert.NoError(t, err)

	processor := &renameProcessor{
		options: &structs.RenameExp{
			RenameExprMode: structs.REMOverride,
			RenameColumns: map[string]string{
				"col1": "newCol1",
			},
		},
	}

	iqr1, err = processor.Process(iqr1)
	assert.NoError(t, err)
	test_Rename(t, iqr1, []string{"col1"}, "newCol1", []sutils.CValueEnclosure{{Dtype: sutils.SS_DT_STRING, CVal: "a"}})

	processor2 := &renameProcessor{
		options: &structs.RenameExp{
			RenameExprMode: structs.REMOverride,
			RenameColumns: map[string]string{
				"newCol1": "superNewCol1",
			},
		},
	}

	iqr1, err = processor2.Process(iqr1)
	assert.NoError(t, err)
	test_Rename(t, iqr1, []string{"col1", "newCol1"}, "superNewCol1", []sutils.CValueEnclosure{{Dtype: sutils.SS_DT_STRING, CVal: "a"}})
}

func Test_Rename_Regex(t *testing.T) {
	iqr1 := iqr.NewIQR(0)
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
	})
	assert.NoError(t, err)

	processor := &renameProcessor{
		options: &structs.RenameExp{
			RenameExprMode: structs.REMRegex,
			RenameColumns: map[string]string{
				"col*": "newCol*",
			},
		},
	}

	iqr1, err = processor.Process(iqr1)
	assert.NoError(t, err)
	test_Rename(t, iqr1, []string{"col1"}, "newCol1", []sutils.CValueEnclosure{{Dtype: sutils.SS_DT_STRING, CVal: "a"}})
	test_Rename(t, iqr1, []string{"col2"}, "newCol2", []sutils.CValueEnclosure{{Dtype: sutils.SS_DT_STRING, CVal: "b"}})
	test_Rename(t, iqr1, []string{"col3"}, "newCol3", []sutils.CValueEnclosure{{Dtype: sutils.SS_DT_STRING, CVal: "c"}})

	processor2 := &renameProcessor{
		options: &structs.RenameExp{
			RenameExprMode: structs.REMRegex,
			RenameColumns: map[string]string{
				"newCol2*": "superNewCol*",
			},
		},
	}

	iqr1, err = processor2.Process(iqr1)
	assert.NoError(t, err)
	test_Rename(t, iqr1, []string{}, "newCol1", []sutils.CValueEnclosure{{Dtype: sutils.SS_DT_STRING, CVal: "a"}})
	test_Rename(t, iqr1, []string{"newCol2"}, "superNewCol", []sutils.CValueEnclosure{{Dtype: sutils.SS_DT_STRING, CVal: "b"}})
	test_Rename(t, iqr1, []string{}, "newCol3", []sutils.CValueEnclosure{{Dtype: sutils.SS_DT_STRING, CVal: "c"}})
}
