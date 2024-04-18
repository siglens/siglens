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

package logql

import (
	"os"
	"testing"

	"github.com/siglens/siglens/pkg/ast"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/stretchr/testify/assert"
)

func Test_ParseStream(t *testing.T) {
	json_body := []byte(`{something="another"}`)
	res, err := Parse("", json_body)
	queryJson := res.(ast.QueryStruct).SearchFilter
	assert.Nil(t, err)
	assert.NotNil(t, queryJson)
	assert.Equal(t, queryJson.Comparison.Field, "something")
	assert.Equal(t, queryJson.Comparison.Values, "\"another\"")
	assert.Equal(t, queryJson.Comparison.Op, "=")

	json_body = []byte(`{something="another", another="thing"}`)
	res, err = Parse("", json_body)
	queryJson = res.(ast.QueryStruct).SearchFilter
	assert.Nil(t, err)
	assert.NotNil(t, queryJson)
	assert.Equal(t, queryJson.Left.Comparison.Field, "something")
	assert.Equal(t, queryJson.Left.Comparison.Values, "\"another\"")
	assert.Equal(t, queryJson.Right.Comparison.Field, "another")
	assert.Equal(t, queryJson.Right.Comparison.Values, "\"thing\"")
	assert.Equal(t, queryJson.NodeType, ast.NodeAnd)
}

func Test_ParseFile(t *testing.T) {
	tempFile, err := os.CreateTemp("", "test.log")
	assert.Nil(t, err)
	defer os.Remove(tempFile.Name())

	testContent := []byte(`{something="another"} | another >= thing`)
	_, err = tempFile.Write(testContent)
	assert.Nil(t, err)

	res, err := ParseFile(tempFile.Name())
	assert.Nil(t, err)
	queryJson := res.(ast.QueryStruct).SearchFilter
	assert.Equal(t, queryJson.Left.Comparison.Field, "something")
	assert.Equal(t, queryJson.Left.Comparison.Values, "\"another\"")
	assert.Equal(t, queryJson.Right.Comparison.Field, "another")
	assert.Equal(t, queryJson.Right.Comparison.Values, "thing")
}

func Test_ParseLabelFilter(t *testing.T) {
	json_body := []byte(`{something="another"} | another >= thing`)
	res, err := Parse("", json_body)
	queryJson := res.(ast.QueryStruct).SearchFilter
	assert.Nil(t, err)
	assert.Equal(t, queryJson.Left.Comparison.Field, "something")
	assert.Equal(t, queryJson.Left.Comparison.Values, "\"another\"")
	assert.Equal(t, queryJson.Right.Comparison.Field, "another")
	assert.Equal(t, queryJson.Right.Comparison.Values, "thing")
}

func Test_ParseLogFilter(t *testing.T) {
	json_body := []byte(`{gender="female",city="Fresno"} != "batch-212"`)
	res, err := Parse("", json_body)
	queryJson := res.(ast.QueryStruct).SearchFilter
	assert.Nil(t, err)
	assert.Equal(t, queryJson.Right.Comparison.Values, ast.GrepValue{Field: "\"batch-212\""})
	assert.Equal(t, queryJson.Left.Right.Comparison.Values, "\"Fresno\"")
	assert.Equal(t, queryJson.Left.Left.Comparison.Values, "\"female\"")
}

func Test_ParseLogAndLabelFilter(t *testing.T) {
	json_body := []byte(`{gender="female",city="Fresno"} |= "batch-212" | another >= thing`)
	res, err := Parse("", json_body)
	queryJson := res.(ast.QueryStruct).SearchFilter
	assert.Nil(t, err)
	assert.Equal(t, queryJson.Right.Right.Comparison.Values, "thing")
	assert.Equal(t, queryJson.Right.Left.Comparison.Values, ast.GrepValue{Field: "\"batch-212\""})
	assert.Equal(t, queryJson.Left.Right.Comparison.Values, "\"Fresno\"")
	assert.Equal(t, queryJson.Left.Left.Comparison.Values, "\"female\"")
}

func Test_ParseJSONKeyword(t *testing.T) {
	json_body := []byte(`{gender="female",city="Fresno"} | json city_life="city", single_gender="gender[0]"`)
	res, err := Parse("", json_body)
	queryJson := res.(ast.QueryStruct).SearchFilter
	pipeCommands := res.(ast.QueryStruct).PipeCommands
	testOutputColumns := append(make([]string, 0), "city", "gender")
	testRenameColumns := make(map[string]string)
	testRenameColumns["city"] = "city_life"
	testIncludeValues := append(make([]*structs.IncludeValue, 0), &structs.IncludeValue{Index: 0, ColName: "gender", Label: "single_gender"})
	assert.Nil(t, err)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeColumns, testOutputColumns)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.RenameColumns, testRenameColumns)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeValues, testIncludeValues)
	assert.Equal(t, queryJson.Left.Comparison.Values, "\"female\"")
	assert.Equal(t, queryJson.Right.Comparison.Values, "\"Fresno\"")

	json_body = []byte(`{gender="female"} | json `)
	res, err = Parse("", json_body)
	pipeCommands = res.(ast.QueryStruct).PipeCommands
	queryJson = res.(ast.QueryStruct).SearchFilter
	assert.Nil(t, err)
	assert.Equal(t, queryJson.Comparison.Values, "\"female\"")
	assert.Nil(t, pipeCommands)
}

func Test_ParseJSONKeywordAndFilters(t *testing.T) {
	json_body := []byte(`{gender="female",city="Fresno"} | json city_life="city", single_gender="gender[0]" |= "batch-212"`)
	res, err := Parse("", json_body)
	queryJson := res.(ast.QueryStruct).SearchFilter
	pipeCommands := res.(ast.QueryStruct).PipeCommands
	testOutputColumns := append(make([]string, 0), "city", "gender")
	testRenameColumns := make(map[string]string)
	testRenameColumns["city"] = "city_life"
	testIncludeValues := append(make([]*structs.IncludeValue, 0), &structs.IncludeValue{Index: 0, ColName: "gender", Label: "single_gender"})
	assert.Nil(t, err)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeColumns, testOutputColumns)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.RenameColumns, testRenameColumns)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeValues, testIncludeValues)
	assert.Equal(t, queryJson.Left.Left.Comparison.Values, "\"female\"")
	assert.Equal(t, queryJson.Left.Right.Comparison.Values, "\"Fresno\"")
	assert.Equal(t, queryJson.Right.Comparison.Values, ast.GrepValue{Field: "\"batch-212\""})

	json_body = []byte(`{gender="female",city="Fresno"} | json city_life="city", single_gender="gender[0]" |= "batch-212" | another >= thing`)
	res, err = Parse("", json_body)
	queryJson = res.(ast.QueryStruct).SearchFilter
	pipeCommands = res.(ast.QueryStruct).PipeCommands
	assert.Nil(t, err)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeColumns, testOutputColumns)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.RenameColumns, testRenameColumns)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeValues, testIncludeValues)
	assert.Equal(t, queryJson.Left.Left.Comparison.Values, "\"female\"")
	assert.Equal(t, queryJson.Left.Right.Comparison.Values, "\"Fresno\"")
	assert.Equal(t, queryJson.Right.Left.Comparison.Values, ast.GrepValue{Field: "\"batch-212\""})
	assert.Equal(t, queryJson.Right.Right.Comparison.Values, "thing")

}

func Test_RangeMetrics(t *testing.T) {
	json_body := []byte(`count_over_time({gender="male"}[90d])`)
	res, err := Parse("", json_body)
	queryJson := res.(ast.QueryStruct).SearchFilter
	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.Nil(t, err)
	assert.Equal(t, pipeCommands.GroupByRequest.GroupByColumns, []string{"*"})
	assert.Equal(t, queryJson.Comparison.Field, "gender")
	assert.Equal(t, queryJson.Comparison.Values, "\"male\"")
	assert.Equal(t, queryJson.Comparison.Op, "=")
}
