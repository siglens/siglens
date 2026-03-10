// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"fmt"
	"testing"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func getColValues(col string, num int) []sutils.CValueEnclosure {
	values := make([]sutils.CValueEnclosure, num)
	for i := 0; i < num; i++ {
		values[i] = sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: col + fmt.Sprintf("%v", i)}
	}
	return values
}

func getKnownValues(cols []string, numRecords int) map[string][]sutils.CValueEnclosure {
	knownValues := map[string][]sutils.CValueEnclosure{}
	for _, col := range cols {
		knownValues[col] = getColValues(col, numRecords)
	}
	return knownValues
}

func validateCols(t *testing.T, iqr *iqr.IQR, expectedCols []string, numRecords int, knownValues map[string][]sutils.CValueEnclosure) {
	cols, err := iqr.ReadAllColumns()
	assert.NoError(t, err)
	assert.Equal(t, numRecords, iqr.NumberOfRecords())
	assert.ElementsMatch(t, expectedCols, utils.GetKeysOfMap(cols))
	for _, col := range expectedCols {
		assert.Equal(t, knownValues[col], cols[col])
	}
	colOrder := iqr.GetColumnsOrder(utils.GetKeysOfMap(cols))
	assert.Equal(t, expectedCols, colOrder)
}

func Test_FieldsCommand_Include(t *testing.T) {
	fieldsProcessor := &fieldsProcessor{&structs.ColumnsRequest{
		IncludeColumns: []string{"col2", "col1", "col4"},
	}}
	inputIqr := iqr.NewIQR(0)
	numRecords := 3
	colsAvailable := []string{"col1", "col2", "col22", "col3"}
	knownValues := getKnownValues(colsAvailable, numRecords)

	err := inputIqr.AppendKnownValues(knownValues)
	assert.NoError(t, err)

	expectedCols := []string{"col1", "col2", "col22", "col3"}
	validateCols(t, inputIqr, expectedCols, numRecords, knownValues)

	iqr, err := fieldsProcessor.Process(inputIqr)
	assert.NoError(t, err)
	expectedCols = []string{"col2", "col1"}
	validateCols(t, iqr, expectedCols, numRecords, knownValues)
}

func Test_FieldsCommand_Exclude(t *testing.T) {
	fieldsProcessor := &fieldsProcessor{&structs.ColumnsRequest{
		ExcludeColumns: []string{"col2*"},
	}}
	inputIqr := iqr.NewIQR(0)
	numRecords := 5
	colsAvailable := []string{"col1", "col2", "col22", "col3"}
	knownValues := getKnownValues(colsAvailable, numRecords)

	err := inputIqr.AppendKnownValues(knownValues)
	assert.NoError(t, err)
	expectedCols := []string{"col1", "col2", "col22", "col3"}
	validateCols(t, inputIqr, expectedCols, numRecords, knownValues)

	iqr, err := fieldsProcessor.Process(inputIqr)
	assert.NoError(t, err)
	expectedCols = []string{"col1", "col3"}
	validateCols(t, iqr, expectedCols, numRecords, knownValues)
}

func Test_FieldsCommand_Multiple(t *testing.T) {
	fieldsProcessor1 := &fieldsProcessor{&structs.ColumnsRequest{
		IncludeColumns: []string{"col2*", "col1*", "col4"},
	}}
	inputIqr := iqr.NewIQR(0)
	numRecords := 3
	colsAvailable := []string{"col1", "col11", "col2", "col22", "col23", "col3", "col4", "col5"}
	knownValues := getKnownValues(colsAvailable, numRecords)

	err := inputIqr.AppendKnownValues(knownValues)
	assert.NoError(t, err)
	expectedCols := []string{"col1", "col11", "col2", "col22", "col23", "col3", "col4", "col5"}
	validateCols(t, inputIqr, expectedCols, numRecords, knownValues)

	iqr, err := fieldsProcessor1.Process(inputIqr)
	assert.NoError(t, err)
	expectedCols = []string{"col2", "col22", "col23", "col1", "col11", "col4"}
	validateCols(t, iqr, expectedCols, numRecords, knownValues)

	fieldsProcessor2 := &fieldsProcessor{&structs.ColumnsRequest{
		ExcludeColumns: []string{"col2*", "col3"},
	}}
	iqr, err = fieldsProcessor2.Process(iqr)
	assert.NoError(t, err)
	expectedCols = []string{"col1", "col11", "col4"}
	validateCols(t, iqr, expectedCols, numRecords, knownValues)

	fieldsProcessor3 := &fieldsProcessor{&structs.ColumnsRequest{
		IncludeColumns: []string{"col2*", "col4", "col1*"},
	}}

	iqr, err = fieldsProcessor3.Process(iqr)
	assert.NoError(t, err)
	expectedCols = []string{"col4", "col1", "col11"}
	validateCols(t, iqr, expectedCols, numRecords, knownValues)
}
