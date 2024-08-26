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

package tests

import (
	"testing"

	"github.com/siglens/siglens/pkg/ast"
	"github.com/siglens/siglens/pkg/ast/logql"
	"github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/stretchr/testify/assert"
)

func Test_ParseStream(t *testing.T) {
	astNode := &structs.ASTNode{}
	json_body := []byte(`{something="another"}`)
	res, err := logql.Parse("", json_body)
	queryJson := res.(ast.QueryStruct).SearchFilter
	assert.Nil(t, err)
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0, false)
	assert.Nil(t, err)
	assert.NotNil(t, queryJson)
	assert.Equal(t, queryJson.Comparison.Field, "something")
	assert.Equal(t, queryJson.Comparison.Values, "\"another\"")
	assert.Equal(t, queryJson.Comparison.Op, "=")

	json_body = []byte(`{something="another", another="thing"}`)
	res, err = logql.Parse("", json_body)
	queryJson = res.(ast.QueryStruct).SearchFilter
	assert.Nil(t, err)
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0, false)
	assert.Nil(t, err)
	assert.NotNil(t, queryJson)
	assert.Equal(t, queryJson.Left.Comparison.Field, "something")
	assert.Equal(t, queryJson.Left.Comparison.Values, "\"another\"")
	assert.Equal(t, queryJson.Right.Comparison.Field, "another")
	assert.Equal(t, queryJson.Right.Comparison.Values, "\"thing\"")
	assert.Equal(t, queryJson.NodeType, ast.NodeAnd)
}

func Test_ParseLabelFilter(t *testing.T) {
	astNode := &structs.ASTNode{}
	json_body := []byte(`{something="another"} | another >= thing`)
	res, err := logql.Parse("", json_body)
	queryJson := res.(ast.QueryStruct).SearchFilter
	assert.Nil(t, err)
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, queryJson.Left.Comparison.Field, "something")
	assert.Equal(t, queryJson.Left.Comparison.Values, "\"another\"")
	assert.Equal(t, queryJson.Right.Comparison.Field, "another")
	assert.Equal(t, queryJson.Right.Comparison.Values, "thing")
}

func Test_ParseLogFilter(t *testing.T) {
	astNode := &structs.ASTNode{}
	json_body := []byte(`{gender="female",city="Fresno"} != "batch-212"`)
	res, err := logql.Parse("", json_body)
	queryJson := res.(ast.QueryStruct).SearchFilter
	assert.Nil(t, err)
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, queryJson.Right.Comparison.Values, ast.GrepValue{Field: "\"batch-212\""})
	assert.Equal(t, queryJson.Left.Right.Comparison.Values, "\"Fresno\"")
	assert.Equal(t, queryJson.Left.Left.Comparison.Values, "\"female\"")
}

func Test_ParseLogAndLabelFilter(t *testing.T) {
	astNode := &structs.ASTNode{}
	json_body := []byte(`{gender="female",city="Fresno"} |= "batch-212" | another >= thing`)
	res, err := logql.Parse("", json_body)
	assert.Nil(t, err)
	queryJson := res.(ast.QueryStruct).SearchFilter
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, queryJson.Right.Right.Comparison.Values, "thing")
	assert.Equal(t, queryJson.Right.Left.Comparison.Values, ast.GrepValue{Field: "\"batch-212\""})
	assert.Equal(t, queryJson.Left.Right.Comparison.Values, "\"Fresno\"")
	assert.Equal(t, queryJson.Left.Left.Comparison.Values, "\"female\"")
}

func Test_ParseLogfmtKeyword(t *testing.T) {
	astNode := &structs.ASTNode{}
	json_body := []byte(`{gender="female",city="Fresno"} | logfmt city_life="city", single_gender="gender", host`)
	res, err := logql.Parse("", json_body)
	assert.Nil(t, err)
	queryJson := res.(ast.QueryStruct).SearchFilter
	pipeCommands := res.(ast.QueryStruct).PipeCommands
	testIncludeValues := append(make([]*structs.IncludeValue, 0), &structs.IncludeValue{ColName: "city", Label: "city_life"}, &structs.IncludeValue{ColName: "gender", Label: "single_gender"}, &structs.IncludeValue{ColName: "host", Label: "host"})
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeValues, testIncludeValues)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.Logfmt, true)
	assert.Equal(t, queryJson.Left.Comparison.Values, "\"female\"")
	assert.Equal(t, queryJson.Right.Comparison.Values, "\"Fresno\"")

	json_body = []byte(`{gender="female"} | logfmt `)
	res, err = logql.Parse("", json_body)
	assert.Nil(t, err)
	pipeCommands = res.(ast.QueryStruct).PipeCommands
	queryJson = res.(ast.QueryStruct).SearchFilter
	astNode = &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.Logfmt, true)
	assert.Equal(t, len(pipeCommands.OutputTransforms.OutputColumns.IncludeValues), 0)
	assert.Equal(t, queryJson.Comparison.Values, "\"female\"")
}

func Test_ParseJSONKeyword(t *testing.T) {
	astNode := &structs.ASTNode{}
	json_body := []byte(`{gender="female",city="Fresno"} | json city_life="city", single_gender="gender[0]"`)
	res, err := logql.Parse("", json_body)
	assert.Nil(t, err)
	queryJson := res.(ast.QueryStruct).SearchFilter
	pipeCommands := res.(ast.QueryStruct).PipeCommands
	testOutputColumns := append(make([]string, 0), "city", "gender")
	testRenameColumns := make(map[string]string)
	testRenameColumns["city"] = "city_life"
	testIncludeValues := append(make([]*structs.IncludeValue, 0), &structs.IncludeValue{Index: 0, ColName: "gender", Label: "single_gender"})
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeColumns, testOutputColumns)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.RenameColumns, testRenameColumns)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeValues, testIncludeValues)
	assert.Equal(t, queryJson.Left.Comparison.Values, "\"female\"")
	assert.Equal(t, queryJson.Right.Comparison.Values, "\"Fresno\"")

	json_body = []byte(`{gender="female"} | json `)
	res, err = logql.Parse("", json_body)
	assert.Nil(t, err)
	pipeCommands = res.(ast.QueryStruct).PipeCommands
	queryJson = res.(ast.QueryStruct).SearchFilter
	astNode = &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, queryJson.Comparison.Values, "\"female\"")
	assert.Nil(t, pipeCommands)
}

func Test_ParseJSONKeywordAndFilters(t *testing.T) {
	astNode := &structs.ASTNode{}
	json_body := []byte(`{gender="female",city="Fresno"} | json city_life="city", single_gender="gender[0]" |= "batch-212"`)
	res, err := logql.Parse("", json_body)
	assert.Nil(t, err)
	queryJson := res.(ast.QueryStruct).SearchFilter
	pipeCommands := res.(ast.QueryStruct).PipeCommands
	testOutputColumns := append(make([]string, 0), "city", "gender")
	testRenameColumns := make(map[string]string)
	testRenameColumns["city"] = "city_life"
	testIncludeValues := append(make([]*structs.IncludeValue, 0), &structs.IncludeValue{Index: 0, ColName: "gender", Label: "single_gender"})
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeColumns, testOutputColumns)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.RenameColumns, testRenameColumns)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeValues, testIncludeValues)
	assert.Equal(t, queryJson.Left.Left.Comparison.Values, "\"female\"")
	assert.Equal(t, queryJson.Left.Right.Comparison.Values, "\"Fresno\"")
	assert.Equal(t, queryJson.Right.Comparison.Values, ast.GrepValue{Field: "\"batch-212\""})

	json_body = []byte(`{gender="female",city="Fresno"} | json city_life="city", single_gender="gender[0]" |= "batch-212" | another >= thing`)
	res, err = logql.Parse("", json_body)
	assert.Nil(t, err)
	queryJson = res.(ast.QueryStruct).SearchFilter
	pipeCommands = res.(ast.QueryStruct).PipeCommands
	astNode = &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0, false)
	assert.Nil(t, err)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeColumns, testOutputColumns)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.RenameColumns, testRenameColumns)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeValues, testIncludeValues)
	assert.Equal(t, queryJson.Left.Left.Comparison.Values, "\"female\"")
	assert.Equal(t, queryJson.Left.Right.Comparison.Values, "\"Fresno\"")
	assert.Equal(t, queryJson.Right.Left.Comparison.Values, ast.GrepValue{Field: "\"batch-212\""})
	assert.Equal(t, queryJson.Right.Right.Comparison.Values, "thing")

}
