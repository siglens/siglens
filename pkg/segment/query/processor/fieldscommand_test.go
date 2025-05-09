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
	"fmt"
	"testing"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func getColValues(col string, num int) []segutils.CValueEnclosure {
	values := make([]segutils.CValueEnclosure, num)
	for i := 0; i < num; i++ {
		values[i] = segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: col + fmt.Sprintf("%v", i)}
	}
	return values
}

func getKnownValues(cols []string, numRecords int) map[string][]segutils.CValueEnclosure {
	knownValues := map[string][]segutils.CValueEnclosure{}
	for _, col := range cols {
		knownValues[col] = getColValues(col, numRecords)
	}
	return knownValues
}

func validateCols(t *testing.T, iqr *iqr.IQR, expectedCols []string, numRecords int, knownValues map[string][]segutils.CValueEnclosure) {
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
