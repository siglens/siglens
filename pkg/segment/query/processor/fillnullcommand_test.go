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

func insertColumnsWithSomeNulls(t *testing.T, iqr *iqr.IQR, fillValue segutils.CValueEnclosure, columnsToInsert []string, valuesCountToInsert int) (
	map[string][]segutils.CValueEnclosure, map[string][]segutils.CValueEnclosure) {
	knownValuesToInsert := make(map[string][]segutils.CValueEnclosure)
	knownValuesToExpect := make(map[string][]segutils.CValueEnclosure)

	var err error

	for _, column := range columnsToInsert {
		valuesToInsert := make([]segutils.CValueEnclosure, valuesCountToInsert)
		valuesToExpect := make([]segutils.CValueEnclosure, valuesCountToInsert)
		for i := 0; i < valuesCountToInsert; i++ {
			insertNull := i%2 == 0

			cValueToInsert := segutils.CValueEnclosure{}
			var cValueToExpect segutils.CValueEnclosure
			if insertNull {
				err = cValueToInsert.ConvertValue(nil)
				assert.NoError(t, err)
				cValueToExpect = fillValue
			} else {
				err = cValueToInsert.ConvertValue(fmt.Sprintf("value-%v", i))
				assert.NoError(t, err)
				cValueToExpect = cValueToInsert
			}

			valuesToInsert[i] = cValueToInsert
			valuesToExpect[i] = cValueToExpect
		}

		knownValuesToInsert[column] = valuesToInsert
		knownValuesToExpect[column] = valuesToExpect
	}

	err = iqr.AppendKnownValues(knownValuesToInsert)
	assert.NoError(t, err)

	return knownValuesToInsert, knownValuesToExpect
}

func Test_fillNullCommandWithFields(t *testing.T) {
	columnsToInsert := []string{"column1", "column2", "column3"}
	valuesCountToInsert := 10

	fillValue := "fill-value"
	fillCValue := segutils.CValueEnclosure{}
	err := fillCValue.ConvertValue(fillValue)
	assert.NoError(t, err)

	fillNullProcessor := &fillnullProcessor{
		options: &structs.FillNullExpr{
			FieldList: columnsToInsert,
			Value:     fillValue,
		},
	}

	iqr1 := iqr.NewIQR(0)

	_, knownValuesToExpect := insertColumnsWithSomeNulls(t, iqr1, fillCValue, columnsToInsert, valuesCountToInsert)

	iqr1, err = fillNullProcessor.Process(iqr1)
	assert.NoError(t, err)

	for _, column := range columnsToInsert {
		values, err := iqr1.ReadColumn(column)
		assert.NoError(t, err)
		assert.Equal(t, knownValuesToExpect[column], values)
	}

	// Will not ask to fillnull for this column
	newColName := "column4"
	columnsToInsert = append(columnsToInsert, newColName)

	iqr2 := iqr.NewIQR(0)

	knownValuesInserted, knownValuesToExpect := insertColumnsWithSomeNulls(t, iqr2, fillCValue, columnsToInsert, valuesCountToInsert)

	iqr2, err = fillNullProcessor.Process(iqr2)
	assert.NoError(t, err)

	for _, column := range columnsToInsert {
		values, err := iqr2.ReadColumn(column)
		assert.NoError(t, err)

		if column == newColName {
			assert.Equal(t, knownValuesInserted[column], values)
			continue
		}

		assert.Equal(t, knownValuesToExpect[column], values)
	}
}

func Test_fillNullCommandWithNoFields(t *testing.T) {
	columnsToInsert := []string{"column1", "column2", "column3"}
	valuesCountToInsert := 10

	fillValue := "fill-value"
	fillCValue := segutils.CValueEnclosure{}
	err := fillCValue.ConvertValue(fillValue)
	assert.NoError(t, err)

	fillNullProcessor := &fillnullProcessor{
		options: &structs.FillNullExpr{
			Value: fillValue,
		},
	}

	// first Pass

	iqr1 := iqr.NewIQR(0)

	_, knownValuesToExpect1 := insertColumnsWithSomeNulls(t, iqr1, fillCValue, columnsToInsert, valuesCountToInsert)

	iqr1, err = fillNullProcessor.Process(iqr1)
	assert.NoError(t, err)

	iqr2 := iqr.NewIQR(0)

	newColName := "column4"
	columnsToInsert = append(columnsToInsert, newColName)

	_, knownValuesToExpect2 := insertColumnsWithSomeNulls(t, iqr2, fillCValue, columnsToInsert, valuesCountToInsert)

	iqr2, err = fillNullProcessor.Process(iqr2)
	assert.NoError(t, err)

	knownValuesToExpect := utils.MergeMapSlicesWithBackfill(knownValuesToExpect1, knownValuesToExpect2, fillCValue, valuesCountToInsert)

	// second pass

	// Call rewind to initiate the second pass
	fillNullProcessor.Rewind()

	iqr1, err = fillNullProcessor.Process(iqr1)
	assert.NoError(t, err)

	iqr2, err = fillNullProcessor.Process(iqr2)
	assert.NoError(t, err)

	err = iqr1.Append(iqr2)
	assert.NoError(t, err)

	for _, column := range columnsToInsert {
		values, err := iqr1.ReadColumn(column)
		assert.NoError(t, err)
		assert.Equal(t, knownValuesToExpect[column], values)
	}
}
