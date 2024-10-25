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

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func getLookupPath() string {
	return "lookups"
}

func prepareData(data []string, fileName string) error {

	err := os.MkdirAll(getLookupPath(), os.ModePerm)
	if err != nil {
		return err
	}

	fd, err := os.Create(filepath.Join(getLookupPath(), fileName))
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

	data := []string{
		"a,b,c\n",
		"1,2,3\n",
		"4,5,6\n",
		"7,8,9\n",
	}

	csvFile := "test.csv"

	err := prepareData(data, csvFile)
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

	os.RemoveAll(getLookupPath())
}

// TODO: Add more tests for different cases
func Test_InputLookupMultiple(t *testing.T) {

	data := []string{"a,b,c\n"}
	for i := 0; i < 110; i++ {
		idx := fmt.Sprintf("%v", i)
		row := "a"+idx + ",b"+idx + ",c"+idx + "\n"
		data = append(data, row)
	}

	csvFile := "test.csv"

	err := prepareData(data, csvFile)
	assert.Nil(t, err)

	dp := NewInputLookupDP(&structs.InputLookup{
		IsFirstCommand: true,
		Filename:       csvFile,
		Start:          1,
		Max:            10000,
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

	os.RemoveAll(getLookupPath())
}