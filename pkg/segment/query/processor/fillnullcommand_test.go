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
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func insertColumnsWithSomeNulls(iqr *iqr.IQR, fillValue utils.CValueEnclosure, columnsToInsert []string, valuesCountToInsert int) (
	map[string][]utils.CValueEnclosure, map[string][]utils.CValueEnclosure, error) {
	knownValuesToInsert := make(map[string][]utils.CValueEnclosure)
	knownValuesToExpect := make(map[string][]utils.CValueEnclosure)

	for _, column := range columnsToInsert {
		valuesToInsert := make([]utils.CValueEnclosure, valuesCountToInsert)
		valuesToExpect := make([]utils.CValueEnclosure, valuesCountToInsert)
		for i := 0; i < valuesCountToInsert; i++ {
			insertNull := i%2 == 0

			cValueToInsert := utils.CValueEnclosure{}
			cValueToExpect := utils.CValueEnclosure{}
			if insertNull {
				cValueToInsert.ConvertValue(nil)
				cValueToExpect = fillValue
			} else {
				cValueToInsert.ConvertValue(fmt.Sprintf("value-%v", i))
				cValueToExpect = cValueToInsert
			}

			valuesToInsert[i] = cValueToInsert
			valuesToExpect[i] = cValueToExpect
		}

		knownValuesToInsert[column] = valuesToInsert
		knownValuesToExpect[column] = valuesToExpect
	}

	err := iqr.AppendKnownValues(knownValuesToInsert)

	return knownValuesToInsert, knownValuesToExpect, err
}

func Test_fillNullCommandWithFields(t *testing.T) {
	columnsToInsert := []string{"column1", "column2", "column3"}
	valuesCountToInsert := 10

	fillValue := "fill-value"
	fillCValue := utils.CValueEnclosure{}
	fillCValue.ConvertValue(fillValue)

	fillNullProcessor := &fillnullProcessor{
		options: &structs.FillNullExpr{
			FieldList: columnsToInsert,
			Value:     fillValue,
		},
	}

	iqr1 := iqr.NewIQR(0)

	_, knownValuesToExpect, err := insertColumnsWithSomeNulls(iqr1, fillCValue, columnsToInsert, valuesCountToInsert)
	assert.NoError(t, err)

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

	knownValuesInserted, knownValuesToExpect, err := insertColumnsWithSomeNulls(iqr2, fillCValue, columnsToInsert, valuesCountToInsert)
	assert.NoError(t, err)

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
	fillCValue := utils.CValueEnclosure{}
	fillCValue.ConvertValue(fillValue)

	fillNullProcessor := &fillnullProcessor{
		options: &structs.FillNullExpr{
			Value: fillValue,
		},
	}

	// first Pass

	iqr1 := iqr.NewIQR(0)

	_, knownValuesToExpect1, err := insertColumnsWithSomeNulls(iqr1, fillCValue, columnsToInsert, valuesCountToInsert)
	assert.NoError(t, err)

	iqr1, err = fillNullProcessor.Process(iqr1)
	assert.NoError(t, err)

	iqr2 := iqr.NewIQR(0)

	newColName := "column4"
	columnsToInsert = append(columnsToInsert, newColName)

	_, knownValuesToExpect2, err := insertColumnsWithSomeNulls(iqr2, fillCValue, columnsToInsert, valuesCountToInsert)
	assert.NoError(t, err)

	iqr2, err = fillNullProcessor.Process(iqr2)
	assert.NoError(t, err)

	knownValuesToExpect := toputils.MergeMapSlicesWithBackfill(knownValuesToExpect1, knownValuesToExpect2, fillCValue, valuesCountToInsert)

	// second pass

	// Call rewind to initiate the second pass
	fillNullProcessor.Rewind()

	iqr1, err = fillNullProcessor.Process(iqr1)
	assert.NoError(t, err)

	iqr2, err = fillNullProcessor.Process(iqr2)
	assert.NoError(t, err)

	iqr1.Append(iqr2)

	for _, column := range columnsToInsert {
		values, err := iqr1.ReadColumn(column)
		assert.NoError(t, err)
		assert.Equal(t, knownValuesToExpect[column], values)
	}
}
