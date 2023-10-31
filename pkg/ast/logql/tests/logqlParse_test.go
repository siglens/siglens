/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, queryJson)
	assert.Equal(t, queryJson.Comparison.Field, "something")
	assert.Equal(t, queryJson.Comparison.Values, "\"another\"")
	assert.Equal(t, queryJson.Comparison.Op, "=")

	json_body = []byte(`{something="another", another="thing"}`)
	res, err = logql.Parse("", json_body)
	queryJson = res.(ast.QueryStruct).SearchFilter
	assert.Nil(t, err)
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0)
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
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0)
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
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0)
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
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0)
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
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0)
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
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0)
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
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0)
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
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0)
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
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0)
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
	err = pipesearch.SearchQueryToASTnode(queryJson, astNode, 0)
	assert.Nil(t, err)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeColumns, testOutputColumns)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.RenameColumns, testRenameColumns)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeValues, testIncludeValues)
	assert.Equal(t, queryJson.Left.Left.Comparison.Values, "\"female\"")
	assert.Equal(t, queryJson.Left.Right.Comparison.Values, "\"Fresno\"")
	assert.Equal(t, queryJson.Right.Left.Comparison.Values, ast.GrepValue{Field: "\"batch-212\""})
	assert.Equal(t, queryJson.Right.Right.Comparison.Values, "thing")

}
