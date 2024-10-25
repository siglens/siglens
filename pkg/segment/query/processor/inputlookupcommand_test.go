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
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func initTestConfig(t *testing.T) error {
	runningConfig := config.GetTestConfig(t.TempDir())
	runningConfig.DataPath = "inputlookup-data/"
	runningConfig.SSInstanceName = "test"
	config.SetConfig(runningConfig)

	return config.InitDerivedConfig("test")
}

func prepareData(data []string, fileName string) error {

	err := os.MkdirAll(config.GetLookupPath(), os.ModePerm)
	if err != nil {
		return err
	}

	fd, err := os.Create(filepath.Join(config.GetLookupPath(), fileName))
	if err != nil {
		return err
	}
	defer fd.Close()

	for _, d := range data {
		_, err = fd.WriteString(d)
		if err != nil {
			return err
		}
	}

	return nil
}

func Test_InputLookup(t *testing.T) {

	err := initTestConfig(t)
	assert.Nil(t, err)

	data := []string{
		"a,b,c\n",
		"1,2,3\n",
		"4,5,6\n",
		"7,8,9\n",
	}

	csvFile := "test.csv"

	err = prepareData(data, csvFile)
	assert.Nil(t, err)

	dp := NewInputLookupDP(&structs.InputLookup{
		IsFirstCommand: true,
		Filename:       csvFile,
		Start:          1,
		Max:            3,
	},
	)

	iqr, err := dp.Fetch()
	assert.Nil(t, err)
	assert.NotNil(t, iqr)
	assert.Equal(t, 2, iqr.NumberOfRecords())

	col_a, err := iqr.ReadColumn("a")
	assert.Nil(t, err)
	expected := []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "4"},
		{Dtype: utils.SS_DT_STRING, CVal: "7"},
	}
	assert.Equal(t, expected, col_a)

	col_b, err := iqr.ReadColumn("b")
	assert.Nil(t, err)
	expected = []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "5"},
		{Dtype: utils.SS_DT_STRING, CVal: "8"},
	}
	assert.Equal(t, expected, col_b)

	col_c, err := iqr.ReadColumn("c")
	assert.Nil(t, err)
	expected = []utils.CValueEnclosure{
		{Dtype: utils.SS_DT_STRING, CVal: "6"},
		{Dtype: utils.SS_DT_STRING, CVal: "9"},
	}
	assert.Equal(t, expected, col_c)

	iqr, err = dp.Fetch()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, iqr)

	os.RemoveAll(config.GetDataPath())
}

func getExpectedColumnValues(val string, start int, count int) []utils.CValueEnclosure {
	var expected []utils.CValueEnclosure
	for i := 0; i < count; i++ {
		colValue := fmt.Sprintf("%v%v", val, start+i)
		expected = append(expected, utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: colValue})
	}
	return expected
}

func validate_MultiFetch(t *testing.T, dp *DataProcessor, expectedCols map[string]struct{}, start []int, numRecords []int, numFetches int) {
	fetch := 0
	for {
		iqr, err := dp.Fetch()
		if fetch == numFetches {
			assert.Equal(t, io.EOF, err)
			break
		} else {
			assert.Nil(t, err)
		}
		assert.NotNil(t, iqr)
		assert.Equal(t, numRecords[fetch], iqr.NumberOfRecords())
		actualCols, err := iqr.GetColumns()
		assert.Nil(t, err)
		assert.Equal(t, expectedCols, actualCols)

		// Column values from 2 to 101 would be read
		for col := range expectedCols {
			colValues, err := iqr.ReadColumn(col)
			assert.Nil(t, err)
			assert.Equal(t, getExpectedColumnValues(col, start[fetch], numRecords[fetch]), colValues)
		}
		fetch++
	}

	assert.Equal(t, numFetches, fetch)
}

func Test_InputLookupMultipleFetchAndRewind(t *testing.T) {

	err := initTestConfig(t)
	assert.Nil(t, err)

	data := []string{"a,b,c\n"}
	for i := 0; i < 110; i++ {
		idx := fmt.Sprintf("%v", i)
		row := "a" + idx + ",b" + idx + ",c" + idx + "\n"
		data = append(data, row)
	}

	csvFile := "test.csv"

	err = prepareData(data, csvFile)
	assert.Nil(t, err)

	dp := NewInputLookupDP(&structs.InputLookup{
		IsFirstCommand: true,
		Filename:       csvFile,
		Start:          2,
		Max:            10000,
	},
	)

	expectedCols := map[string]struct{}{
		"a": {},
		"b": {},
		"c": {},
	}
	start := []int{2, 102}
	numRecords := []int{100, 8}

	validate_MultiFetch(t, dp, expectedCols, start, numRecords, 2)

	// Rewind to the start
	dp.Rewind()

	validate_MultiFetch(t, dp, expectedCols, start, numRecords, 2)

	os.RemoveAll(config.GetDataPath())
}
