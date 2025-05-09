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
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func initTestConfig(t *testing.T) error {
	runningConfig := config.GetTestConfig(t.TempDir())
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
		"a1,b2,c3\n",
		"a4,b5,c6\n",
		"a7,b8,c9\n",
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
	expected := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "a4"},
		{Dtype: segutils.SS_DT_STRING, CVal: "a7"},
	}
	assert.Equal(t, expected, col_a)

	col_b, err := iqr.ReadColumn("b")
	assert.Nil(t, err)
	expected = []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "b5"},
		{Dtype: segutils.SS_DT_STRING, CVal: "b8"},
	}
	assert.Equal(t, expected, col_b)

	col_c, err := iqr.ReadColumn("c")
	assert.Nil(t, err)
	expected = []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "c6"},
		{Dtype: segutils.SS_DT_STRING, CVal: "c9"},
	}
	assert.Equal(t, expected, col_c)

	iqr, err = dp.Fetch()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, iqr)

	os.RemoveAll(config.GetDataPath())
}

func getExpectedColumnValues(val string, start int, count int) []segutils.CValueEnclosure {
	var expected []segutils.CValueEnclosure
	for i := 0; i < count; i++ {
		colValue := fmt.Sprintf("%v%v", val, start+i)
		expected = append(expected, segutils.CValueEnclosure{Dtype: segutils.SS_DT_STRING, CVal: colValue})
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
		row := fmt.Sprintf("a%v,b%v,c%v\n", idx, idx, idx)
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

func Test_MultipleInputlookups(t *testing.T) {

	err := initTestConfig(t)
	assert.Nil(t, err)

	data1 := []string{
		"a,b,c\n",
		"a1,b2,c3\n",
		"a4,b5,c6\n",
		"a7,b8,c9\n",
	}

	csvFile1 := "test1.csv"

	err = prepareData(data1, csvFile1)
	assert.Nil(t, err)

	data2 := []string{
		"a,b,c\n",
		"10,20,30\n",
		"40,50,60\n",
		"70,80,90\n",
	}

	csvFile2 := "test2.csv"

	err = prepareData(data2, csvFile2)
	assert.Nil(t, err)

	dp1 := NewInputLookupDP(&structs.InputLookup{
		IsFirstCommand: true,
		Filename:       csvFile1,
		Max:            2,
	},
	)

	dp2 := NewInputLookupDP(&structs.InputLookup{
		Filename: csvFile2,
		Start:    1,
		Max:      3,
	},
	)
	dp2.streams = append(dp2.streams, NewCachedStream(dp1))

	expectedCols := map[string]struct{}{
		"a": {},
		"b": {},
		"c": {},
	}

	finalRes := iqr.NewIQR(0)

	for {
		iqr, err := dp2.Fetch()
		if err == io.EOF {
			break
		}
		assert.Nil(t, err)
		assert.NotNil(t, iqr)
		err = finalRes.Append(iqr)
		assert.Nil(t, err)
	}

	assert.Equal(t, 4, finalRes.NumberOfRecords())
	actualCols, err := finalRes.GetColumns()
	assert.Nil(t, err)
	assert.Equal(t, expectedCols, actualCols)

	col_a, err := finalRes.ReadColumn("a")
	assert.Nil(t, err)
	expected := []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "a1"},
		{Dtype: segutils.SS_DT_STRING, CVal: "a4"},
		{Dtype: segutils.SS_DT_FLOAT, CVal: float64(40)},
		{Dtype: segutils.SS_DT_FLOAT, CVal: float64(70)},
	}
	assert.Equal(t, expected, col_a)

	col_b, err := finalRes.ReadColumn("b")
	assert.Nil(t, err)
	expected = []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "b2"},
		{Dtype: segutils.SS_DT_STRING, CVal: "b5"},
		{Dtype: segutils.SS_DT_FLOAT, CVal: float64(50)},
		{Dtype: segutils.SS_DT_FLOAT, CVal: float64(80)},
	}
	assert.Equal(t, expected, col_b)

	col_c, err := finalRes.ReadColumn("c")
	assert.Nil(t, err)
	expected = []segutils.CValueEnclosure{
		{Dtype: segutils.SS_DT_STRING, CVal: "c3"},
		{Dtype: segutils.SS_DT_STRING, CVal: "c6"},
		{Dtype: segutils.SS_DT_FLOAT, CVal: float64(60)},
		{Dtype: segutils.SS_DT_FLOAT, CVal: float64(90)},
	}
	assert.Equal(t, expected, col_c)

	os.RemoveAll(config.GetDataPath())
}
