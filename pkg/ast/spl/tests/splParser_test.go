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
	"encoding/json"
	"io"
	"os"
	"regexp"
	"testing"

	"github.com/siglens/siglens/pkg/ast"
	"github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/ast/spl"
	segquery "github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// Helper functions

func extractMatchFilter(t *testing.T, node *ast.Node) *structs.MatchFilter {
	astNode := &structs.ASTNode{}
	err := pipesearch.SearchQueryToASTnode(node, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition)

	criteria := astNode.AndFilterCondition.FilterCriteria
	assert.NotNil(t, criteria)
	assert.Equal(t, 1, len(criteria))
	assert.Nil(t, criteria[0].ExpressionFilter)

	matchFilter := criteria[0].MatchFilter
	assert.NotNil(t, matchFilter)

	return matchFilter
}

func extractExpressionFilter(t *testing.T, node *ast.Node) *structs.ExpressionFilter {
	astNode := &structs.ASTNode{}
	err := pipesearch.SearchQueryToASTnode(node, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition)

	criteria := astNode.AndFilterCondition.FilterCriteria
	assert.NotNil(t, criteria)
	assert.Equal(t, 1, len(criteria))
	assert.Nil(t, criteria[0].MatchFilter)

	expressionFilter := criteria[0].ExpressionFilter
	assert.NotNil(t, expressionFilter)

	return expressionFilter
}

// Initial setup.
func TestMain(m *testing.M) {
	// Suppress log output.
	log.SetOutput(io.Discard)

	// Run the tests.
	os.Exit(m.Run())
}

// Tests

func Test_searchQuotedStringNoBreakers(t *testing.T) {
	query := []byte(`search "abc"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Field, "*")
	assert.Equal(t, filterNode.Comparison.Values, `"abc"`)

	matchFilter := extractMatchFilter(t, filterNode)
	assert.Equal(t, structs.MATCH_PHRASE, matchFilter.MatchType)

	// Note: the double quotes got stripped off in ast.ProcessSingleFilter(), so
	// it's just abc and not "abc"
	assert.Equal(t, []byte(`abc`), matchFilter.MatchPhrase)
}

func Test_searchQuotedStringMinorBreakers(t *testing.T) {
	query := []byte(`search "abc./\\:=@#$%-_DEF"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Field, "*")
	assert.Equal(t, filterNode.Comparison.Values, `"abc./\\:=@#$%-_DEF"`)

	matchFilter := extractMatchFilter(t, filterNode)
	assert.Equal(t, structs.MATCH_PHRASE, matchFilter.MatchType)
	assert.Equal(t, []byte(`abc./\\:=@#$%-_DEF`), matchFilter.MatchPhrase)
}

func Test_searchQuotedStringMajorBreakers(t *testing.T) {
	query := []byte(`search "abc DEF < > [ ] ( ) { } ! ? ; , ' &"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Field, "*")
	assert.Equal(t, filterNode.Comparison.Values, `"abc DEF < > [ ] ( ) { } ! ? ; , ' &"`)

	matchFilter := extractMatchFilter(t, filterNode)
	assert.Equal(t, structs.MATCH_PHRASE, matchFilter.MatchType)
	assert.Equal(t, []byte(`abc DEF < > [ ] ( ) { } ! ? ; , ' &`), matchFilter.MatchPhrase)
}

func Test_impliedSearchCommand(t *testing.T) {
	query := []byte(`"apple"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Field, "*")
	assert.Equal(t, filterNode.Comparison.Values, `"apple"`)

	matchFilter := extractMatchFilter(t, filterNode)
	assert.Equal(t, structs.MATCH_PHRASE, matchFilter.MatchType)
	assert.Equal(t, []byte(`apple`), matchFilter.MatchPhrase)
}

func Test_searchUnquotedStringNoBreakers(t *testing.T) {
	query := []byte(`search abc`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Field, "*")
	assert.Equal(t, filterNode.Comparison.Values, `"abc"`)

	matchFilter := extractMatchFilter(t, filterNode)
	assert.Equal(t, structs.MATCH_PHRASE, matchFilter.MatchType)
	assert.Equal(t, []byte(`abc`), matchFilter.MatchPhrase)
}

func Test_searchUnquotedStringMinorBreakers(t *testing.T) {
	query := []byte(`search "abc./\\:=@#$%-_DEF"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Field, "*")
	assert.Equal(t, filterNode.Comparison.Values, `"abc./\\:=@#$%-_DEF"`)

	matchFilter := extractMatchFilter(t, filterNode)
	assert.Equal(t, structs.MATCH_PHRASE, matchFilter.MatchType)
	assert.Equal(t, []byte(`abc./\\:=@#$%-_DEF`), matchFilter.MatchPhrase)
}

func Test_searchUnquotedStringMajorBreakerAtStart(t *testing.T) {
	query := []byte(`search &abcDEF`)
	res, err := spl.Parse("", query)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func Test_searchUnquotedStringMajorBreakerInMiddle(t *testing.T) {
	query := []byte(`search abc(DEF`)
	res, err := spl.Parse("", query)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func Test_searchUnquotedStringMajorBreakerAtEnd(t *testing.T) {
	query := []byte(`search &abcDEF%26`)
	res, err := spl.Parse("", query)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func Test_searchFieldEqualToQuotedString(t *testing.T) {
	query := []byte(`search status="ok"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "status", filterNode.Comparison.Field)
	assert.Equal(t, `"ok"`, filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "status", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.Equals, expressionFilter.FilterOperator)
	assert.Equal(t, "ok", expressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal)
}

func Test_searchFieldNotEqualToQuotedString(t *testing.T) {
	query := []byte(`search status!="ok"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "!=", filterNode.Comparison.Op)
	assert.Equal(t, "status", filterNode.Comparison.Field)
	assert.Equal(t, `"ok"`, filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "status", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.NotEquals, expressionFilter.FilterOperator)
	assert.Equal(t, "ok", expressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal)
}

func Test_searchFieldLessThanQuotedString(t *testing.T) {
	query := []byte(`search status<"ok"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func Test_searchFieldLessThanOrEqualToQuotedString(t *testing.T) {
	query := []byte(`search status<="ok"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func Test_searchFieldGreaterThanThanQuotedString(t *testing.T) {
	query := []byte(`search status>"ok"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func Test_searchFieldGreaterThanOrEqualToQuotedString(t *testing.T) {
	query := []byte(`search status>="ok"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func Test_searchFieldEqualToBooleanTrue(t *testing.T) {
	query := []byte(`search status=true`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "status", filterNode.Comparison.Field)
	assert.Equal(t, true, filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "status", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.Equals, expressionFilter.FilterOperator)
	assert.Equal(t, uint8(1), expressionFilter.RightInput.Expression.LeftInput.ColumnValue.BoolVal)
}

func Test_searchFieldEqualToBooleanFalse(t *testing.T) {
	query := []byte(`search status=false`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "status", filterNode.Comparison.Field)
	assert.Equal(t, false, filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "status", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.Equals, expressionFilter.FilterOperator)
	assert.Equal(t, uint8(0), expressionFilter.RightInput.Expression.LeftInput.ColumnValue.BoolVal)
}

func Test_searchFieldEqualToUnquotedString(t *testing.T) {
	query := []byte(`search status=ok`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "status", filterNode.Comparison.Field)
	assert.Equal(t, `"ok"`, filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "status", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.Equals, expressionFilter.FilterOperator)
	assert.Equal(t, "ok", expressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal)
}

func Test_searchFieldNotEqualToUnquotedString(t *testing.T) {
	query := []byte(`search status!=ok`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "!=", filterNode.Comparison.Op)
	assert.Equal(t, "status", filterNode.Comparison.Field)
	assert.Equal(t, `"ok"`, filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "status", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.NotEquals, expressionFilter.FilterOperator)
	assert.Equal(t, "ok", expressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal)
}

func Test_searchFieldLessThanUnquotedString(t *testing.T) {
	query := []byte(`search status<ok`)
	res, err := spl.Parse("", query)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func Test_searchFieldLessThanOrEqualToUnquotedString(t *testing.T) {
	query := []byte(`search status<=ok`)
	res, err := spl.Parse("", query)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func Test_searchFieldGreaterThanThanUnquotedString(t *testing.T) {
	query := []byte(`search status>ok`)
	res, err := spl.Parse("", query)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func Test_searchFieldGreaterThanOrEqualToUnquotedString(t *testing.T) {
	query := []byte(`search status>=ok`)
	res, err := spl.Parse("", query)
	assert.Nil(t, res)
	assert.NotNil(t, err)
}

func Test_searchInteger(t *testing.T) {
	query := []byte(`search 123`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "*", filterNode.Comparison.Field)
	assert.Equal(t, json.Number("123"), filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "*", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.Equals, expressionFilter.FilterOperator)
	dtype, err := utils.CreateDtypeEnclosure(json.Number("123"), 0 /* qid */)
	assert.Nil(t, err)
	assert.Equal(t, dtype, expressionFilter.RightInput.Expression.LeftInput.ColumnValue)
}

func Test_searchIntegerLeadingZeros(t *testing.T) {
	query := []byte(`search 00123`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "*", filterNode.Comparison.Field)
	assert.Equal(t, json.Number("00123"), filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "*", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.Equals, expressionFilter.FilterOperator)
	dtype, err := utils.CreateDtypeEnclosure(json.Number("00123"), 0 /* qid */)
	assert.Nil(t, err)
	assert.Equal(t, dtype, expressionFilter.RightInput.Expression.LeftInput.ColumnValue)
}

func Test_searchIntegerLeadingPlusSign(t *testing.T) {
	query := []byte(`search +123`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "*", filterNode.Comparison.Field)
	assert.Equal(t, json.Number("+123"), filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "*", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.Equals, expressionFilter.FilterOperator)
	dtype, err := utils.CreateDtypeEnclosure(json.Number("+123"), 0 /* qid */)
	assert.Nil(t, err)
	assert.Equal(t, dtype, expressionFilter.RightInput.Expression.LeftInput.ColumnValue)
}

func Test_searchNegativeInteger(t *testing.T) {
	query := []byte(`search -123`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "*", filterNode.Comparison.Field)
	assert.Equal(t, json.Number("-123"), filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "*", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.Equals, expressionFilter.FilterOperator)
	dtype, err := utils.CreateDtypeEnclosure(json.Number("-123"), 0 /* qid */)
	assert.Nil(t, err)
	assert.Equal(t, dtype, expressionFilter.RightInput.Expression.LeftInput.ColumnValue)
}

func Test_searchFloat(t *testing.T) {
	query := []byte(`search 123.5`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "*", filterNode.Comparison.Field)
	assert.Equal(t, json.Number("123.5"), filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "*", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.Equals, expressionFilter.FilterOperator)
	dtype, err := utils.CreateDtypeEnclosure(json.Number("123.5"), 0 /* qid */)
	assert.Nil(t, err)
	assert.Equal(t, dtype, expressionFilter.RightInput.Expression.LeftInput.ColumnValue)
}

func Test_searchFloatLeadingDecimal(t *testing.T) {
	query := []byte(`search .375`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "*", filterNode.Comparison.Field)
	assert.Equal(t, json.Number(".375"), filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "*", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.Equals, expressionFilter.FilterOperator)
	dtype, err := utils.CreateDtypeEnclosure(json.Number(".375"), 0 /* qid */)
	assert.Nil(t, err)
	assert.Equal(t, dtype, expressionFilter.RightInput.Expression.LeftInput.ColumnValue)
}

func Test_searchFloatLeadingPlusSign(t *testing.T) {
	query := []byte(`search +0.375`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "*", filterNode.Comparison.Field)
	assert.Equal(t, json.Number("+0.375"), filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "*", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.Equals, expressionFilter.FilterOperator)
	dtype, err := utils.CreateDtypeEnclosure(json.Number("+0.375"), 0 /* qid */)
	assert.Nil(t, err)
	assert.Equal(t, dtype, expressionFilter.RightInput.Expression.LeftInput.ColumnValue)
}

func Test_searchFieldEqualToNumber(t *testing.T) {
	query := []byte(`search status=400`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "status", filterNode.Comparison.Field)
	assert.Equal(t, json.Number("400"), filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "status", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.Equals, expressionFilter.FilterOperator)
	dtype, err := utils.CreateDtypeEnclosure(json.Number("400"), 0 /* qid */)
	assert.Nil(t, err)
	assert.Equal(t, dtype, expressionFilter.RightInput.Expression.LeftInput.ColumnValue)
}

func Test_searchFieldNotEqualToNumber(t *testing.T) {
	query := []byte(`search status!=400`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "!=", filterNode.Comparison.Op)
	assert.Equal(t, "status", filterNode.Comparison.Field)
	assert.Equal(t, json.Number("400"), filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "status", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.NotEquals, expressionFilter.FilterOperator)
	dtype, err := utils.CreateDtypeEnclosure(json.Number("400"), 0 /* qid */)
	assert.Nil(t, err)
	assert.Equal(t, dtype, expressionFilter.RightInput.Expression.LeftInput.ColumnValue)
}

func Test_searchFieldLessThanNumber(t *testing.T) {
	query := []byte(`search latency<1000`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "<", filterNode.Comparison.Op)
	assert.Equal(t, "latency", filterNode.Comparison.Field)
	assert.Equal(t, json.Number("1000"), filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "latency", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.LessThan, expressionFilter.FilterOperator)
	dtype, err := utils.CreateDtypeEnclosure(json.Number("1000"), 0 /* qid */)
	assert.Nil(t, err)
	assert.Equal(t, dtype, expressionFilter.RightInput.Expression.LeftInput.ColumnValue)
}

func Test_searchFieldLessThanOrEqualToNumber(t *testing.T) {
	query := []byte(`search latency<=0.5`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "<=", filterNode.Comparison.Op)
	assert.Equal(t, "latency", filterNode.Comparison.Field)
	assert.Equal(t, json.Number("0.5"), filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "latency", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.LessThanOrEqualTo, expressionFilter.FilterOperator)
	dtype, err := utils.CreateDtypeEnclosure(json.Number("0.5"), 0 /* qid */)
	assert.Nil(t, err)
	assert.Equal(t, dtype, expressionFilter.RightInput.Expression.LeftInput.ColumnValue)
}

func Test_searchFieldGreaterThanNumber(t *testing.T) {
	query := []byte(`search latency>3.175`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, ">", filterNode.Comparison.Op)
	assert.Equal(t, "latency", filterNode.Comparison.Field)
	assert.Equal(t, json.Number("3.175"), filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "latency", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.GreaterThan, expressionFilter.FilterOperator)
	dtype, err := utils.CreateDtypeEnclosure(json.Number("3.175"), 0 /* qid */)
	assert.Nil(t, err)
	assert.Equal(t, dtype, expressionFilter.RightInput.Expression.LeftInput.ColumnValue)
}

func Test_searchFieldGreaterThanOrEqualToNumber(t *testing.T) {
	query := []byte(`search latency>=1200`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, ">=", filterNode.Comparison.Op)
	assert.Equal(t, "latency", filterNode.Comparison.Field)
	assert.Equal(t, json.Number("1200"), filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "latency", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.GreaterThanOrEqualTo, expressionFilter.FilterOperator)
	dtype, err := utils.CreateDtypeEnclosure(json.Number("1200"), 0 /* qid */)
	assert.Nil(t, err)
	assert.Equal(t, dtype, expressionFilter.RightInput.Expression.LeftInput.ColumnValue)
}

func Test_searchSimpleAND(t *testing.T) {
	query := []byte(`search status=ok AND latency<1000`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.NodeType, ast.NodeAnd)

	assert.Equal(t, filterNode.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Comparison.Field, "status")
	assert.Equal(t, filterNode.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Comparison.Values, `"ok"`)

	assert.Equal(t, filterNode.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Comparison.Field, "latency")
	assert.Equal(t, filterNode.Right.Comparison.Op, "<")
	assert.Equal(t, filterNode.Right.Comparison.Values, json.Number("1000"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "status")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "ok")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "latency")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.LessThan)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1000))
}

func Test_searchChainedAND(t *testing.T) {
	query := []byte(`search A=1 AND B=2 AND C=3`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, ast.NodeAnd, filterNode.NodeType)
	assert.Equal(t, ast.NodeAnd, filterNode.Left.NodeType)

	assert.Equal(t, filterNode.Left.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Left.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Right.Comparison.Field, "B")
	assert.Equal(t, filterNode.Left.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Right.Comparison.Values, json.Number("2"))

	assert.Equal(t, filterNode.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Comparison.Field, "C")
	assert.Equal(t, filterNode.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Comparison.Values, json.Number("3"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "C")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(3))
	assert.Len(t, astNode.AndFilterCondition.NestedNodes, 1)
	assert.Len(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "B")
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(2))
}

func Test_searchSimpleOR(t *testing.T) {
	query := []byte(`search status=ok OR latency<1000`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.NodeType, ast.NodeOr)

	assert.Equal(t, filterNode.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Comparison.Field, "status")
	assert.Equal(t, filterNode.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Comparison.Values, `"ok"`)

	assert.Equal(t, filterNode.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Comparison.Field, "latency")
	assert.Equal(t, filterNode.Right.Comparison.Op, "<")
	assert.Equal(t, filterNode.Right.Comparison.Values, json.Number("1000"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.OrFilterCondition.FilterCriteria)
	assert.Len(t, astNode.OrFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "status")
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "ok")
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "latency")
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.LessThan)
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1000))
}

func Test_searchChainedOR(t *testing.T) {
	query := []byte(`search A=1 OR B=2 OR C=3`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, ast.NodeOr, filterNode.NodeType)
	assert.Equal(t, ast.NodeOr, filterNode.Left.NodeType)

	assert.Equal(t, filterNode.Left.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Left.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Right.Comparison.Field, "B")
	assert.Equal(t, filterNode.Left.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Right.Comparison.Values, json.Number("2"))

	assert.Equal(t, filterNode.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Comparison.Field, "C")
	assert.Equal(t, filterNode.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Comparison.Values, json.Number("3"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.OrFilterCondition.FilterCriteria)
	assert.Len(t, astNode.OrFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "C")
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(3))
	assert.Len(t, astNode.OrFilterCondition.NestedNodes, 1)
	assert.Len(t, astNode.OrFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "B")
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(2))
}

func Test_searchANDThenOR(t *testing.T) {
	// This should be parsed as `A=1 AND (B=2 OR C=3)`.
	query := []byte(`search A=1 AND B=2 OR C=3`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.NodeType, ast.NodeAnd)
	assert.Equal(t, filterNode.Right.NodeType, ast.NodeOr)

	assert.Equal(t, filterNode.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Right.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Left.Comparison.Field, "B")
	assert.Equal(t, filterNode.Right.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Left.Comparison.Values, json.Number("2"))

	assert.Equal(t, filterNode.Right.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Right.Comparison.Field, "C")
	assert.Equal(t, filterNode.Right.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Right.Comparison.Values, json.Number("3"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
	assert.Len(t, astNode.AndFilterCondition.NestedNodes, 1)
	assert.Len(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "B")
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(2))
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "C")
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(3))
}

func Test_searchORThenAND(t *testing.T) {
	// This should be parsed as `(A=1 OR B=2) AND C=3`.
	query := []byte(`search A=1 OR B=2 AND C=3`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.NodeType, ast.NodeAnd)
	assert.Equal(t, filterNode.Left.NodeType, ast.NodeOr)

	assert.Equal(t, filterNode.Left.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Left.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Right.Comparison.Field, "B")
	assert.Equal(t, filterNode.Left.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Right.Comparison.Values, json.Number("2"))

	assert.Equal(t, filterNode.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Comparison.Field, "C")
	assert.Equal(t, filterNode.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Comparison.Values, json.Number("3"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "C")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(3))
	assert.Len(t, astNode.AndFilterCondition.NestedNodes, 1)
	assert.Len(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "B")
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(2))
}

func Test_SimpleParentheses(t *testing.T) {
	query := []byte(`search (status="ok")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "status", filterNode.Comparison.Field)
	assert.Equal(t, `"ok"`, filterNode.Comparison.Values)

	expressionFilter := extractExpressionFilter(t, filterNode)
	assert.Equal(t, "status", expressionFilter.LeftInput.Expression.LeftInput.ColumnName)
	assert.Equal(t, utils.Equals, expressionFilter.FilterOperator)
	assert.Equal(t, "ok", expressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal)
}

func Test_searchParenthesesToChangePrecedence(t *testing.T) {
	query := []byte(`search A=1 OR (B=2 AND C=3)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.NodeType, ast.NodeOr)
	assert.Equal(t, filterNode.Right.NodeType, ast.NodeAnd)

	assert.Equal(t, filterNode.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Right.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Left.Comparison.Field, "B")
	assert.Equal(t, filterNode.Right.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Left.Comparison.Values, json.Number("2"))

	assert.Equal(t, filterNode.Right.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Right.Comparison.Field, "C")
	assert.Equal(t, filterNode.Right.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Right.Comparison.Values, json.Number("3"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.OrFilterCondition.FilterCriteria)
	assert.Len(t, astNode.OrFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
	assert.Len(t, astNode.OrFilterCondition.NestedNodes, 1)
	assert.Len(t, astNode.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "B")
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(2))
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "C")
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(3))
}

func Test_searchImplicitAND(t *testing.T) {
	query := []byte(`search status=ok latency<1000`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.NodeType, ast.NodeAnd)

	assert.Equal(t, filterNode.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Comparison.Field, "status")
	assert.Equal(t, filterNode.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Comparison.Values, `"ok"`)

	assert.Equal(t, filterNode.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Comparison.Field, "latency")
	assert.Equal(t, filterNode.Right.Comparison.Op, "<")
	assert.Equal(t, filterNode.Right.Comparison.Values, json.Number("1000"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "status")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "ok")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "latency")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.LessThan)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1000))
}

func Test_searchChainedImplicitAND(t *testing.T) {
	query := []byte(`search A=1 B=2 C=3`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, ast.NodeAnd, filterNode.NodeType)
	assert.Equal(t, ast.NodeAnd, filterNode.Left.NodeType)

	assert.Equal(t, filterNode.Left.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Left.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Right.Comparison.Field, "B")
	assert.Equal(t, filterNode.Left.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Right.Comparison.Values, json.Number("2"))

	assert.Equal(t, filterNode.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Comparison.Field, "C")
	assert.Equal(t, filterNode.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Comparison.Values, json.Number("3"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "C")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(3))
	assert.Len(t, astNode.AndFilterCondition.NestedNodes, 1)
	assert.Len(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "B")
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(2))
}

func Test_searchMixedImplicitAndExplicitAND(t *testing.T) {
	query := []byte(`search A=1 AND B=2 C=3`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, ast.NodeAnd, filterNode.NodeType)
	assert.Equal(t, ast.NodeAnd, filterNode.Left.NodeType)

	assert.Equal(t, filterNode.Left.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Left.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Right.Comparison.Field, "B")
	assert.Equal(t, filterNode.Left.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Right.Comparison.Values, json.Number("2"))

	assert.Equal(t, filterNode.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Comparison.Field, "C")
	assert.Equal(t, filterNode.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Comparison.Values, json.Number("3"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "C")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(3))
	assert.Len(t, astNode.AndFilterCondition.NestedNodes, 1)
	assert.Len(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "B")
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(2))
}

func Test_searchSimpleNOTEquals(t *testing.T) {
	query := []byte(`search NOT status=200`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)

	// NOT is handled internally with De Morgan's law.
	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "status")
	assert.Equal(t, filterNode.Comparison.Op, "!=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("200"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "status")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.NotEquals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(200))
}

func Test_searchSimpleNOTNotEquals(t *testing.T) {
	query := []byte(`search NOT status!=200`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)

	// NOT is handled internally with De Morgan's law.
	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "status")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("200"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "status")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(200))
}

func Test_searchSimpleNOTLessThan(t *testing.T) {
	query := []byte(`search NOT status<200`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)

	// NOT is handled internally with De Morgan's law.
	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "status")
	assert.Equal(t, filterNode.Comparison.Op, ">=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("200"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "status")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.GreaterThanOrEqualTo)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(200))
}

func Test_searchSimpleNOTGreaterThan(t *testing.T) {
	query := []byte(`search NOT status>200`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)

	// NOT is handled internally with De Morgan's law.
	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "status")
	assert.Equal(t, filterNode.Comparison.Op, "<=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("200"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "status")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.LessThanOrEqualTo)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(200))
}

func Test_searchSimpleNOTLessThanOrEqual(t *testing.T) {
	query := []byte(`search NOT status<=200`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)

	// NOT is handled internally with De Morgan's law.
	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "status")
	assert.Equal(t, filterNode.Comparison.Op, ">")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("200"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "status")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.GreaterThan)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(200))
}

func Test_searchSimpleNOTGreaterThanOrEqual(t *testing.T) {
	query := []byte(`search NOT status>=200`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)

	// NOT is handled internally with De Morgan's law.
	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "status")
	assert.Equal(t, filterNode.Comparison.Op, "<")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("200"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "status")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.LessThan)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(200))
}

func Test_searchCancelingNots(t *testing.T) {
	query := []byte(`search NOT NOT (NOT (NOT status=200))`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)

	// NOT is handled internally with De Morgan's law.
	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "status")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("200"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "status")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(200))
}

func Test_searchCompoundNOT(t *testing.T) {
	query := []byte(`search NOT (status=ok OR (A>1 AND NOT A>=10))`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	// NOT is handled internally with De Morgan's law.
	assert.Equal(t, filterNode.NodeType, ast.NodeAnd)
	assert.Equal(t, filterNode.Right.NodeType, ast.NodeOr)

	assert.Equal(t, filterNode.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Comparison.Field, "status")
	assert.Equal(t, filterNode.Left.Comparison.Op, "!=")
	assert.Equal(t, filterNode.Left.Comparison.Values, `"ok"`)

	assert.Equal(t, filterNode.Right.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Right.Left.Comparison.Op, "<=")
	assert.Equal(t, filterNode.Right.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Right.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Right.Comparison.Field, "A")
	assert.Equal(t, filterNode.Right.Right.Comparison.Op, ">=")
	assert.Equal(t, filterNode.Right.Right.Comparison.Values, json.Number("10"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.Len(t, astNode.AndFilterCondition.NestedNodes, 1)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "status")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.NotEquals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "ok")
	assert.Len(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.LessThanOrEqualTo)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.GreaterThanOrEqualTo)
	assert.Equal(t, astNode.AndFilterCondition.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(10))
}

func Test_searchQuotedWildcard(t *testing.T) {
	query := []byte(`search day="T*day"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "day")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, `"T*day"`)

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "day")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "T*day")
}

func Test_searchUnquotedWildcard(t *testing.T) {
	query := []byte(`search day=T*day`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "day")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, `"T*day"`)

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "day")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "T*day")
}

func Test_searchNumberedWildcardBecomesString(t *testing.T) {
	query := []byte(`search status_code=50*`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "status_code")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, `"50*"`)

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "status_code")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "50*")
}

func Test_chainedSearch(t *testing.T) {
	// This should be equivalent to `search A=1 AND B=2`
	query := []byte(`A=1 | search B=2`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, ast.NodeAnd, filterNode.NodeType)

	assert.Equal(t, filterNode.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Comparison.Field, "B")
	assert.Equal(t, filterNode.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Comparison.Values, json.Number("2"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "B")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(2))
}

func Test_manyChainedSearch(t *testing.T) {
	// This should be equivalent to `search A=1 AND (B=2 AND (C=3 AND D=4))`
	query := []byte(`search A=1 | search B=2 | search C=3 | search D=4`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, ast.NodeAnd, filterNode.NodeType)
	assert.Equal(t, ast.NodeAnd, filterNode.Right.NodeType)
	assert.Equal(t, ast.NodeAnd, filterNode.Right.Right.NodeType)

	assert.Equal(t, filterNode.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Right.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Left.Comparison.Field, "B")
	assert.Equal(t, filterNode.Right.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Left.Comparison.Values, json.Number("2"))

	assert.Equal(t, filterNode.Right.Right.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Right.Left.Comparison.Field, "C")
	assert.Equal(t, filterNode.Right.Right.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Right.Left.Comparison.Values, json.Number("3"))

	assert.Equal(t, filterNode.Right.Right.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Right.Right.Comparison.Field, "D")
	assert.Equal(t, filterNode.Right.Right.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Right.Right.Comparison.Values, json.Number("4"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Len(t, astNode.AndFilterCondition.NestedNodes, 1)
	andFilter := astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition
	assert.Len(t, andFilter.FilterCriteria, 1)
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "B")
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(2))

	assert.Len(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.NestedNodes, 1)
	andFilter = astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.NestedNodes[0].AndFilterCondition
	assert.Len(t, andFilter.FilterCriteria, 2)
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "C")
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(3))
	assert.Equal(t, andFilter.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "D")
	assert.Equal(t, andFilter.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(4))
}

func Test_manyChainedSearchOptionalPipeSpacing(t *testing.T) {
	// This should be equivalent to `search A=1 AND (B=2 AND (C=3 AND D=4))`
	query := []byte(`search A=1| search B=apple|search C=3 |search D=4`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, ast.NodeAnd, filterNode.NodeType)
	assert.Equal(t, ast.NodeAnd, filterNode.Right.NodeType)
	assert.Equal(t, ast.NodeAnd, filterNode.Right.Right.NodeType)

	assert.Equal(t, filterNode.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Right.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Left.Comparison.Field, "B")
	assert.Equal(t, filterNode.Right.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Left.Comparison.Values, `"apple"`)

	assert.Equal(t, filterNode.Right.Right.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Right.Left.Comparison.Field, "C")
	assert.Equal(t, filterNode.Right.Right.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Right.Left.Comparison.Values, json.Number("3"))

	assert.Equal(t, filterNode.Right.Right.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Right.Right.Comparison.Field, "D")
	assert.Equal(t, filterNode.Right.Right.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Right.Right.Comparison.Values, json.Number("4"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Len(t, astNode.AndFilterCondition.NestedNodes, 1)
	andFilter := astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition
	assert.Len(t, andFilter.FilterCriteria, 1)
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "B")
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "apple")

	assert.Len(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.NestedNodes, 1)
	andFilter = astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.NestedNodes[0].AndFilterCondition
	assert.Len(t, andFilter.FilterCriteria, 2)
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "C")
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(3))
	assert.Equal(t, andFilter.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "D")
	assert.Equal(t, andFilter.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(4))
}

func Test_manyChainedCompoundSearch(t *testing.T) {
	// This should be equivalent to `search A=1 AND ((B=2 AND C=3) AND ((D=4 OR E=5) AND F=6))`
	query := []byte(`search A=1 | search B=2 C=3 | search D=4 OR E=5 | search F=6`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, ast.NodeAnd, filterNode.NodeType)
	assert.Equal(t, ast.NodeAnd, filterNode.Right.NodeType)
	assert.Equal(t, ast.NodeAnd, filterNode.Right.Left.NodeType)
	assert.Equal(t, ast.NodeAnd, filterNode.Right.Right.NodeType)
	assert.Equal(t, ast.NodeOr, filterNode.Right.Right.Left.NodeType)

	assert.Equal(t, filterNode.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Right.Left.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Left.Left.Comparison.Field, "B")
	assert.Equal(t, filterNode.Right.Left.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Left.Left.Comparison.Values, json.Number("2"))

	assert.Equal(t, filterNode.Right.Left.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Left.Right.Comparison.Field, "C")
	assert.Equal(t, filterNode.Right.Left.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Left.Right.Comparison.Values, json.Number("3"))

	assert.Equal(t, filterNode.Right.Right.Left.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Right.Left.Left.Comparison.Field, "D")
	assert.Equal(t, filterNode.Right.Right.Left.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Right.Left.Left.Comparison.Values, json.Number("4"))

	assert.Equal(t, filterNode.Right.Right.Left.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Right.Left.Right.Comparison.Field, "E")
	assert.Equal(t, filterNode.Right.Right.Left.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Right.Left.Right.Comparison.Values, json.Number("5"))

	assert.Equal(t, filterNode.Right.Right.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Right.Right.Comparison.Field, "F")
	assert.Equal(t, filterNode.Right.Right.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Right.Right.Comparison.Values, json.Number("6"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	andFilter := astNode.AndFilterCondition
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Len(t, astNode.AndFilterCondition.NestedNodes, 1)
	assert.Len(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.NestedNodes, 2)
	andFilter = astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.NestedNodes[0].AndFilterCondition
	assert.Len(t, andFilter.FilterCriteria, 2)
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "B")
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(2))
	assert.Equal(t, andFilter.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "C")
	assert.Equal(t, andFilter.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(3))

	andFilter = astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.NestedNodes[1].AndFilterCondition
	assert.Len(t, andFilter.NestedNodes, 1)
	assert.Equal(t, andFilter.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "D")
	assert.Equal(t, andFilter.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(4))
	assert.Equal(t, andFilter.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "E")
	assert.Equal(t, andFilter.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(5))
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "F")
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(6))
}

func Test_searchBlockWithoutUsingSearchKeyword(t *testing.T) {
	// This should be equivalent to `search A=1 AND ((B=2 AND C=3) AND ((D=4 OR E=5) AND F=6))`
	query := []byte(`search A=1 | B=2 C=3 | search D=4 OR E=5 | F=6`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, ast.NodeAnd, filterNode.NodeType)
	assert.Equal(t, ast.NodeAnd, filterNode.Right.NodeType)
	assert.Equal(t, ast.NodeAnd, filterNode.Right.Left.NodeType)
	assert.Equal(t, ast.NodeAnd, filterNode.Right.Right.NodeType)
	assert.Equal(t, ast.NodeOr, filterNode.Right.Right.Left.NodeType)

	assert.Equal(t, filterNode.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Right.Left.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Left.Left.Comparison.Field, "B")
	assert.Equal(t, filterNode.Right.Left.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Left.Left.Comparison.Values, json.Number("2"))

	assert.Equal(t, filterNode.Right.Left.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Left.Right.Comparison.Field, "C")
	assert.Equal(t, filterNode.Right.Left.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Left.Right.Comparison.Values, json.Number("3"))

	assert.Equal(t, filterNode.Right.Right.Left.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Right.Left.Left.Comparison.Field, "D")
	assert.Equal(t, filterNode.Right.Right.Left.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Right.Left.Left.Comparison.Values, json.Number("4"))

	assert.Equal(t, filterNode.Right.Right.Left.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Right.Left.Right.Comparison.Field, "E")
	assert.Equal(t, filterNode.Right.Right.Left.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Right.Left.Right.Comparison.Values, json.Number("5"))

	assert.Equal(t, filterNode.Right.Right.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Right.Right.Comparison.Field, "F")
	assert.Equal(t, filterNode.Right.Right.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Right.Right.Comparison.Values, json.Number("6"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	andFilter := astNode.AndFilterCondition
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Len(t, astNode.AndFilterCondition.NestedNodes, 1)
	assert.Len(t, astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.NestedNodes, 2)
	andFilter = astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.NestedNodes[0].AndFilterCondition
	assert.Len(t, andFilter.FilterCriteria, 2)
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "B")
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(2))
	assert.Equal(t, andFilter.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "C")
	assert.Equal(t, andFilter.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(3))

	andFilter = astNode.AndFilterCondition.NestedNodes[0].AndFilterCondition.NestedNodes[1].AndFilterCondition
	assert.Len(t, andFilter.NestedNodes, 1)
	assert.Equal(t, andFilter.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "D")
	assert.Equal(t, andFilter.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.NestedNodes[0].OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(4))
	assert.Equal(t, andFilter.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "E")
	assert.Equal(t, andFilter.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.NestedNodes[0].OrFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(5))
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "F")
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, andFilter.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(6))
}

func Test_regexSingleColumnEquals(t *testing.T) {
	query := []byte(`A=1 | regex B="^\d$"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, ast.NodeAnd, filterNode.NodeType)

	assert.Equal(t, filterNode.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Comparison.Field, "B")
	assert.Equal(t, filterNode.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Comparison.Values, `^\d$`)
	assert.Equal(t, filterNode.Right.Comparison.ValueIsRegex, true)

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "B")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, `^\d$`)

	compiledRegex, err := regexp.Compile(`^\d$`)
	assert.Nil(t, err)
	assert.NotNil(t, compiledRegex)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.GetRegexp(), compiledRegex)
}

func Test_regexSingleColumnNotEquals(t *testing.T) {
	query := []byte(`A=1 | regex B!="^\d$"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, ast.NodeAnd, filterNode.NodeType)

	assert.Equal(t, filterNode.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Comparison.Field, "B")
	assert.Equal(t, filterNode.Right.Comparison.Op, "!=")
	assert.Equal(t, filterNode.Right.Comparison.Values, `^\d$`)
	assert.Equal(t, filterNode.Right.Comparison.ValueIsRegex, true)

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "B")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.NotEquals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, `^\d$`)

	compiledRegex, err := regexp.Compile(`^\d$`)
	assert.Nil(t, err)
	assert.NotNil(t, compiledRegex)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.GetRegexp(), compiledRegex)
}

func Test_regexRequiresQuotes(t *testing.T) {
	query := []byte(`A=1 | regex B=^\d$`)
	res, err := spl.Parse("", query)
	assert.NotNil(t, err)
	assert.Nil(t, res)
}

func Test_regexAnyColumn(t *testing.T) {
	query := []byte(`A=1 | regex "^\d$"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, ast.NodeAnd, filterNode.NodeType)

	assert.Equal(t, filterNode.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Comparison.Field, "*")
	assert.Equal(t, filterNode.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Comparison.Values, `^\d$`)
	assert.Equal(t, filterNode.Right.Comparison.ValueIsRegex, true)

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "*")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, `^\d$`)

	compiledRegex, err := regexp.Compile(`^\d$`)
	assert.Nil(t, err)
	assert.NotNil(t, compiledRegex)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.GetRegexp(), compiledRegex)
}

func Test_aggCountWithField(t *testing.T) {
	query := []byte(`search A=1 | stats count(city)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, pipeCommands.MeasureOperations, 1)
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureCol, "city")
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureFunc, utils.Count)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, aggregator.MeasureOperations, 1)
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureCol, "city")
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureFunc, utils.Count)
}

func Test_aggCountWithoutField(t *testing.T) {
	query := []byte(`search A=1 | stats count`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, pipeCommands.MeasureOperations, 1)
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureCol, "*")
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureFunc, utils.Count)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, aggregator.MeasureOperations, 1)
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureCol, "*")
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureFunc, utils.Count)
}

func Test_aggCountAlias(t *testing.T) {
	query := []byte(`search A=1 | stats c(city)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, pipeCommands.MeasureOperations, 1)
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureCol, "city")
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureFunc, utils.Count)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, aggregator.MeasureOperations, 1)
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureCol, "city")
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureFunc, utils.Count)
}

func Test_aggDistinctCount(t *testing.T) {
	query := []byte(`search A=1 | stats distinct_count(city)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, pipeCommands.MeasureOperations, 1)
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureCol, "city")
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureFunc, utils.Cardinality)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, aggregator.MeasureOperations, 1)
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureCol, "city")
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureFunc, utils.Cardinality)
}

func Test_aggDistinctCountAlias(t *testing.T) {
	query := []byte(`search A=1 | stats dc(city)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, pipeCommands.MeasureOperations, 1)
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureCol, "city")
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureFunc, utils.Cardinality)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, aggregator.MeasureOperations, 1)
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureCol, "city")
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureFunc, utils.Cardinality)
}

func Test_aggAvg(t *testing.T) {
	query := []byte(`search A=1 | stats avg(latency)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, pipeCommands.MeasureOperations, 1)
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureFunc, utils.Avg)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, aggregator.MeasureOperations, 1)
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureFunc, utils.Avg)
}

func Test_aggMin(t *testing.T) {
	query := []byte(`search A=1 | stats min(latency)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, pipeCommands.MeasureOperations, 1)
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureFunc, utils.Min)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, aggregator.MeasureOperations, 1)
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureFunc, utils.Min)
}

func Test_aggMax(t *testing.T) {
	query := []byte(`search A=1 | stats max(latency)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, pipeCommands.MeasureOperations, 1)
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureFunc, utils.Max)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, aggregator.MeasureOperations, 1)
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureFunc, utils.Max)
}

func Test_aggRange(t *testing.T) {
	query := []byte(`search A=1 | stats range(latency)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, pipeCommands.MeasureOperations, 1)
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureFunc, utils.Range)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, aggregator.MeasureOperations, 1)
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureFunc, utils.Range)
}

func Test_aggValues(t *testing.T) {
	query := []byte(`search A=1 | stats values(latency)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, pipeCommands.MeasureOperations, 1)
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureFunc, utils.Values)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, aggregator.MeasureOperations, 1)
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureFunc, utils.Values)
}

func Test_aggSum(t *testing.T) {
	query := []byte(`search A=1 | stats sum(latency)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, pipeCommands.MeasureOperations, 1)
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureFunc, utils.Sum)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, aggregator.MeasureOperations, 1)
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureFunc, utils.Sum)
}

func Test_groupbyOneField(t *testing.T) {
	query := []byte(`search A=1 | stats avg(latency) BY http_status`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.GroupByType)
	assert.Len(t, pipeCommands.GroupByRequest.MeasureOperations, 1)
	assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[0].MeasureFunc, utils.Avg)
	assert.Len(t, pipeCommands.GroupByRequest.GroupByColumns, 1)
	assert.Equal(t, pipeCommands.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, pipeCommands.BucketLimit, segquery.MAX_GRP_BUCKS)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.GroupByType)
	assert.Len(t, aggregator.GroupByRequest.MeasureOperations, 1)
	assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[0].MeasureFunc, utils.Avg)
	assert.Len(t, aggregator.GroupByRequest.GroupByColumns, 1)
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.BucketLimit, segquery.MAX_GRP_BUCKS)
}

func Test_groupbyManyFields(t *testing.T) {
	query := []byte(`search A=1 | stats avg(latency) BY http_status, weekday, city`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.GroupByType)
	assert.Len(t, pipeCommands.GroupByRequest.MeasureOperations, 1)
	assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[0].MeasureFunc, utils.Avg)
	assert.Len(t, pipeCommands.GroupByRequest.GroupByColumns, 3)
	assert.Equal(t, pipeCommands.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, pipeCommands.GroupByRequest.GroupByColumns[1], "weekday")
	assert.Equal(t, pipeCommands.GroupByRequest.GroupByColumns[2], "city")
	assert.Equal(t, pipeCommands.BucketLimit, segquery.MAX_GRP_BUCKS)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.GroupByType)
	assert.Len(t, aggregator.GroupByRequest.MeasureOperations, 1)
	assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[0].MeasureFunc, utils.Avg)
	assert.Len(t, aggregator.GroupByRequest.GroupByColumns, 3)
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[1], "weekday")
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[2], "city")
	assert.Equal(t, aggregator.BucketLimit, segquery.MAX_GRP_BUCKS)
}

func Test_timechartHasGroupby(t *testing.T) {
	queries := []string{
		`search A=1 | timechart span=1d avg(latency), sum(latitude) BY http_status limit=bottom2`,
		`search A=1 | timechart avg(latency), sum(latitude) BY http_status span=1d limit=bottom2`,
	}

	for _, queryStr := range queries {
		query := []byte(queryStr)
		res, err := spl.Parse("", query)
		assert.Nil(t, err)
		filterNode := res.(ast.QueryStruct).SearchFilter
		assert.NotNil(t, filterNode)

		assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
		assert.Equal(t, filterNode.Comparison.Field, "A")
		assert.Equal(t, filterNode.Comparison.Op, "=")
		assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

		pipeCommands := res.(ast.QueryStruct).PipeCommands
		assert.NotNil(t, pipeCommands)
		assert.Equal(t, pipeCommands.PipeCommandType, structs.GroupByType)
		assert.Len(t, pipeCommands.GroupByRequest.MeasureOperations, 2)
		assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[0].MeasureCol, "latency")
		assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[0].MeasureFunc, utils.Avg)
		assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[1].MeasureCol, "latitude")
		assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[1].MeasureFunc, utils.Sum)
		assert.Len(t, pipeCommands.GroupByRequest.GroupByColumns, 1)
		assert.Equal(t, pipeCommands.GroupByRequest.GroupByColumns[0], "timestamp")
		assert.Equal(t, pipeCommands.BucketLimit, segquery.MAX_GRP_BUCKS)
		// Timechart
		assert.NotNil(t, pipeCommands.TimeHistogram)
		assert.NotNil(t, pipeCommands.TimeHistogram.Timechart)
		assert.Equal(t, uint64(86_400_000), pipeCommands.TimeHistogram.IntervalMillis)
		assert.Equal(t, "http_status", pipeCommands.TimeHistogram.Timechart.ByField)
		assert.NotNil(t, pipeCommands.TimeHistogram.Timechart.LimitExpr)
		assert.False(t, pipeCommands.TimeHistogram.Timechart.LimitExpr.IsTop)
		assert.Equal(t, 2, pipeCommands.TimeHistogram.Timechart.LimitExpr.Num)
		assert.Equal(t, structs.LSMByFreq, int(pipeCommands.TimeHistogram.Timechart.LimitExpr.LimitScoreMode))

		astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
		assert.Nil(t, err)
		assert.NotNil(t, astNode)
		assert.NotNil(t, aggregator)

		assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
		assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
		assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
		assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

		assert.Equal(t, aggregator.PipeCommandType, structs.GroupByType)
		assert.Len(t, aggregator.GroupByRequest.MeasureOperations, 2)
		assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[0].MeasureCol, "latency")
		assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[0].MeasureFunc, utils.Avg)
		assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[1].MeasureCol, "latitude")
		assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[1].MeasureFunc, utils.Sum)
		assert.Len(t, aggregator.GroupByRequest.GroupByColumns, 1)
		assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "timestamp")
		assert.Equal(t, aggregator.BucketLimit, segquery.MAX_GRP_BUCKS)
		// Timechart
		assert.NotNil(t, aggregator.TimeHistogram)
		assert.NotNil(t, aggregator.TimeHistogram.Timechart)
		assert.Equal(t, uint64(86_400_000), aggregator.TimeHistogram.IntervalMillis)
		assert.Equal(t, "http_status", aggregator.TimeHistogram.Timechart.ByField)
		assert.NotNil(t, aggregator.TimeHistogram.Timechart.LimitExpr)
		assert.False(t, aggregator.TimeHistogram.Timechart.LimitExpr.IsTop)
		assert.Equal(t, 2, aggregator.TimeHistogram.Timechart.LimitExpr.Num)
		assert.Equal(t, structs.LSMByFreq, int(aggregator.TimeHistogram.Timechart.LimitExpr.LimitScoreMode))
	}
}

func Test_timechartWithoutGroupby(t *testing.T) {
	queries := []string{
		`search A=1 | timechart span=1hr min(latency), range(longitude)`,
		`search A=1 | timechart min(latency), range(longitude) span=1hr`,
	}

	for _, queryStr := range queries {
		query := []byte(queryStr)
		res, err := spl.Parse("", query)
		assert.Nil(t, err)
		filterNode := res.(ast.QueryStruct).SearchFilter
		assert.NotNil(t, filterNode)

		assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
		assert.Equal(t, filterNode.Comparison.Field, "A")
		assert.Equal(t, filterNode.Comparison.Op, "=")
		assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

		pipeCommands := res.(ast.QueryStruct).PipeCommands
		assert.NotNil(t, pipeCommands)
		assert.Equal(t, pipeCommands.PipeCommandType, structs.GroupByType)
		assert.Len(t, pipeCommands.GroupByRequest.MeasureOperations, 2)
		assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[0].MeasureCol, "latency")
		assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[0].MeasureFunc, utils.Min)
		assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[1].MeasureCol, "longitude")
		assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[1].MeasureFunc, utils.Range)
		assert.Len(t, pipeCommands.GroupByRequest.GroupByColumns, 1)
		assert.Equal(t, pipeCommands.GroupByRequest.GroupByColumns[0], "timestamp")
		assert.Equal(t, pipeCommands.BucketLimit, segquery.MAX_GRP_BUCKS)
		// Timechart
		assert.NotNil(t, pipeCommands.TimeHistogram)
		assert.NotNil(t, pipeCommands.TimeHistogram.Timechart)
		assert.Equal(t, uint64(3_600_000), pipeCommands.TimeHistogram.IntervalMillis)
		assert.Equal(t, "", pipeCommands.TimeHistogram.Timechart.ByField)
		assert.Nil(t, pipeCommands.TimeHistogram.Timechart.LimitExpr)

		astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
		assert.Nil(t, err)
		assert.NotNil(t, astNode)
		assert.NotNil(t, aggregator)

		assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
		assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
		assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
		assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

		assert.Equal(t, aggregator.PipeCommandType, structs.GroupByType)
		assert.Len(t, aggregator.GroupByRequest.MeasureOperations, 2)
		assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[0].MeasureCol, "latency")
		assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[0].MeasureFunc, utils.Min)
		assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[1].MeasureCol, "longitude")
		assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[1].MeasureFunc, utils.Range)
		assert.Len(t, aggregator.GroupByRequest.GroupByColumns, 1)
		assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "timestamp")
		assert.Equal(t, aggregator.BucketLimit, segquery.MAX_GRP_BUCKS)
		// Timechart
		assert.NotNil(t, aggregator.TimeHistogram)
		assert.NotNil(t, aggregator.TimeHistogram.Timechart)
		assert.Equal(t, uint64(3_600_000), aggregator.TimeHistogram.IntervalMillis)
		assert.Equal(t, "", aggregator.TimeHistogram.Timechart.ByField)
		assert.Nil(t, aggregator.TimeHistogram.Timechart.LimitExpr)
	}
}

func Test_timechartWithoutGroupBy(t *testing.T) {
	queries := []string{
		`search A=1 | timechart span=1d avg(latency), sum(latitude) BY http_status`,
		`search A=1 | timechart avg(latency), sum(latitude) BY http_status span=1d`,
	}
	for _, queryStr := range queries {
		query := []byte(queryStr)
		res, err := spl.Parse("", query)
		assert.Nil(t, err)
		filterNode := res.(ast.QueryStruct).SearchFilter
		assert.NotNil(t, filterNode)

		assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
		assert.Equal(t, filterNode.Comparison.Field, "A")
		assert.Equal(t, filterNode.Comparison.Op, "=")
		assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

		pipeCommands := res.(ast.QueryStruct).PipeCommands
		assert.NotNil(t, pipeCommands)
		assert.Equal(t, pipeCommands.PipeCommandType, structs.GroupByType)
		assert.Len(t, pipeCommands.GroupByRequest.MeasureOperations, 2)
		assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[0].MeasureCol, "latency")
		assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[0].MeasureFunc, utils.Avg)
		assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[1].MeasureCol, "latitude")
		assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[1].MeasureFunc, utils.Sum)
		assert.Len(t, pipeCommands.GroupByRequest.GroupByColumns, 1)
		assert.Equal(t, pipeCommands.GroupByRequest.GroupByColumns[0], "timestamp")
		assert.Equal(t, pipeCommands.BucketLimit, segquery.MAX_GRP_BUCKS)
		// Timechart
		assert.NotNil(t, pipeCommands.TimeHistogram)
		assert.NotNil(t, pipeCommands.TimeHistogram.Timechart)
		assert.Equal(t, "http_status", pipeCommands.TimeHistogram.Timechart.ByField)

		astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
		assert.Nil(t, err)
		assert.NotNil(t, astNode)
		assert.NotNil(t, aggregator)

		assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
		assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
		assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
		assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

		assert.Equal(t, aggregator.PipeCommandType, structs.GroupByType)
		assert.Len(t, aggregator.GroupByRequest.MeasureOperations, 2)
		assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[0].MeasureCol, "latency")
		assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[0].MeasureFunc, utils.Avg)
		assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[1].MeasureCol, "latitude")
		assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[1].MeasureFunc, utils.Sum)
		assert.Len(t, aggregator.GroupByRequest.GroupByColumns, 1)
		assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "timestamp")
		assert.Equal(t, aggregator.BucketLimit, segquery.MAX_GRP_BUCKS)
		// Timechart
		assert.NotNil(t, aggregator.TimeHistogram)
		assert.NotNil(t, aggregator.TimeHistogram.Timechart)
		assert.Equal(t, "http_status", pipeCommands.TimeHistogram.Timechart.ByField)
	}
}

func Test_TimechartSpanArgWithoutGroupBy(t *testing.T) {
	queries := []string{
		`search A=1 | timechart span=1m avg(latency)`,
		`search A=1 | timechart avg(latency) span=1m`,
	}

	for _, queryStr := range queries {
		query := []byte(queryStr)
		res, err := spl.Parse("", query)
		assert.Nil(t, err)

		astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")

		assert.Nil(t, err)
		assert.NotNil(t, astNode)
		assert.NotNil(t, aggregator)

		assert.Equal(t, uint64(60_000), aggregator.TimeHistogram.IntervalMillis)

		pipeCommands := res.(ast.QueryStruct).PipeCommands
		assert.NotNil(t, pipeCommands)
		assert.Equal(t, pipeCommands.PipeCommandType, structs.GroupByType)
		assert.Len(t, pipeCommands.GroupByRequest.MeasureOperations, 1)
		assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[0].MeasureCol, "latency")
		assert.Equal(t, pipeCommands.GroupByRequest.MeasureOperations[0].MeasureFunc, utils.Avg)
		assert.Len(t, pipeCommands.GroupByRequest.GroupByColumns, 1)
		assert.Equal(t, pipeCommands.GroupByRequest.GroupByColumns[0], "timestamp")
		assert.Equal(t, pipeCommands.BucketLimit, segquery.MAX_GRP_BUCKS)
	}
}

func Test_aggHasEvalFuncWithoutGroupBy(t *testing.T) {
	query := []byte(`city=Boston | stats max(latitude), range(eval(latitude >= 0))`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, ast.NodeTerminal, filterNode.NodeType)
	assert.Equal(t, "city", filterNode.Comparison.Field)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "\"Boston\"", filterNode.Comparison.Values)

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, pipeCommands.MeasureOperations, 2)
	assert.Equal(t, "latitude", pipeCommands.MeasureOperations[0].MeasureCol)
	assert.Equal(t, utils.Max, pipeCommands.MeasureOperations[0].MeasureFunc)

	assert.Equal(t, "range(eval(latitude >= 0))", pipeCommands.MeasureOperations[1].StrEnc)
	assert.Equal(t, utils.Range, pipeCommands.MeasureOperations[1].MeasureFunc)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest)
	assert.Equal(t, structs.VEMBooleanExpr, int(pipeCommands.MeasureOperations[1].ValueColRequest.ValueExprMode))
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.LeftValue)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.RightValue)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.LeftValue.NumericExpr)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.RightValue.NumericExpr)
	assert.Equal(t, "latitude", pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.LeftValue.NumericExpr.Value)
	assert.Equal(t, "0", pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.RightValue.NumericExpr.Value)
}

func Test_aggHasEvalFuncWithGroupBy(t *testing.T) {
	query := []byte(`* | stats count(eval(http_status >= 100)), values(eval(if(len(state) > 5, job_title, city))) BY state`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, ast.NodeTerminal, filterNode.NodeType)
	assert.Equal(t, "*", filterNode.Comparison.Field)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "\"*\"", filterNode.Comparison.Values)

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.GroupByType)
	assert.NotNil(t, pipeCommands.GroupByRequest)
	assert.Len(t, pipeCommands.GroupByRequest.MeasureOperations, 2)

	assert.Equal(t, "count(eval(http_status >= 100))", pipeCommands.GroupByRequest.MeasureOperations[0].StrEnc)
	assert.Equal(t, utils.Count, pipeCommands.GroupByRequest.MeasureOperations[0].MeasureFunc)
	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[0].ValueColRequest)
	assert.Equal(t, structs.VEMBooleanExpr, int(pipeCommands.GroupByRequest.MeasureOperations[0].ValueColRequest.ValueExprMode))
	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[0].ValueColRequest.BooleanExpr)
	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[0].ValueColRequest.BooleanExpr.LeftValue)
	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[0].ValueColRequest.BooleanExpr.RightValue)
	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[0].ValueColRequest.BooleanExpr.LeftValue.NumericExpr)
	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[0].ValueColRequest.BooleanExpr.RightValue.NumericExpr)
	assert.Equal(t, "http_status", pipeCommands.GroupByRequest.MeasureOperations[0].ValueColRequest.BooleanExpr.LeftValue.NumericExpr.Value)
	assert.Equal(t, "100", pipeCommands.GroupByRequest.MeasureOperations[0].ValueColRequest.BooleanExpr.RightValue.NumericExpr.Value)

	assert.Equal(t, "values(eval(if(len(state) > 5, job_title, city)))", pipeCommands.GroupByRequest.MeasureOperations[1].StrEnc)
	assert.Equal(t, utils.Values, pipeCommands.GroupByRequest.MeasureOperations[1].MeasureFunc)
	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest)
	assert.Equal(t, structs.VEMConditionExpr, int(pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest.ValueExprMode))
	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest.ConditionExpr)
	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest.ConditionExpr.BoolExpr)
	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest.ConditionExpr.BoolExpr.LeftValue)
	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest.ConditionExpr.BoolExpr.RightValue)
	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr)
	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest.ConditionExpr.BoolExpr.RightValue.NumericExpr)
	assert.Equal(t, "len", pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.Op)
	assert.Equal(t, "state", pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.Left.Value)
	assert.Equal(t, "5", pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest.ConditionExpr.BoolExpr.RightValue.NumericExpr.Value)

	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest.ConditionExpr.TrueValue)
	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest.ConditionExpr.FalseValue)
	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest.ConditionExpr.TrueValue.NumericExpr)
	assert.NotNil(t, pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest.ConditionExpr.FalseValue.NumericExpr)
	assert.Equal(t, "job_title", pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest.ConditionExpr.TrueValue.NumericExpr.Value)
	assert.Equal(t, "city", pipeCommands.GroupByRequest.MeasureOperations[1].ValueColRequest.ConditionExpr.FalseValue.NumericExpr.Value)
}

func Test_groupbyFieldWithWildcard(t *testing.T) {
	query := []byte(`search A=1 | stats avg(latency) BY http*`)
	_, err := spl.Parse("", query)
	assert.NotNil(t, err)
}

func Test_fieldSelectImplicitPlus(t *testing.T) {
	query := []byte(`search A=1 | fields weekday`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.OutputTransformType)
	assert.Len(t, pipeCommands.OutputTransforms.OutputColumns.ExcludeColumns, 0)
	assert.Len(t, pipeCommands.OutputTransforms.OutputColumns.IncludeColumns, 1)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeColumns[0], "weekday")

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.Len(t, aggregator.OutputTransforms.OutputColumns.ExcludeColumns, 0)
	assert.Len(t, aggregator.OutputTransforms.OutputColumns.IncludeColumns, 1)
	assert.Equal(t, aggregator.OutputTransforms.OutputColumns.IncludeColumns[0], "weekday")
}

func Test_fieldSelectExplicitPlus(t *testing.T) {
	query := []byte(`search A=1 | fields + weekday`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.OutputTransformType)
	assert.Len(t, pipeCommands.OutputTransforms.OutputColumns.ExcludeColumns, 0)
	assert.Len(t, pipeCommands.OutputTransforms.OutputColumns.IncludeColumns, 1)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeColumns[0], "weekday")

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.Len(t, aggregator.OutputTransforms.OutputColumns.ExcludeColumns, 0)
	assert.Len(t, aggregator.OutputTransforms.OutputColumns.IncludeColumns, 1)
	assert.Equal(t, aggregator.OutputTransforms.OutputColumns.IncludeColumns[0], "weekday")
}

func Test_fieldSelectMinus(t *testing.T) {
	query := []byte(`search A=1 | fields - weekday`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.OutputTransformType)
	assert.Len(t, pipeCommands.OutputTransforms.OutputColumns.IncludeColumns, 0)
	assert.Len(t, pipeCommands.OutputTransforms.OutputColumns.ExcludeColumns, 1)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.ExcludeColumns[0], "weekday")

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.Len(t, aggregator.OutputTransforms.OutputColumns.IncludeColumns, 0)
	assert.Len(t, aggregator.OutputTransforms.OutputColumns.ExcludeColumns, 1)
	assert.Equal(t, aggregator.OutputTransforms.OutputColumns.ExcludeColumns[0], "weekday")
}

func Test_fieldSelectManyFields(t *testing.T) {
	query := []byte(`search A=1 | fields weekday, latency, city`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.OutputTransformType)
	assert.Len(t, pipeCommands.OutputTransforms.OutputColumns.ExcludeColumns, 0)
	assert.Len(t, pipeCommands.OutputTransforms.OutputColumns.IncludeColumns, 3)
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeColumns[0], "weekday")
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeColumns[1], "latency")
	assert.Equal(t, pipeCommands.OutputTransforms.OutputColumns.IncludeColumns[2], "city")

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.Len(t, aggregator.OutputTransforms.OutputColumns.ExcludeColumns, 0)
	assert.Len(t, aggregator.OutputTransforms.OutputColumns.IncludeColumns, 3)
	assert.Equal(t, aggregator.OutputTransforms.OutputColumns.IncludeColumns[0], "weekday")
	assert.Equal(t, aggregator.OutputTransforms.OutputColumns.IncludeColumns[1], "latency")
	assert.Equal(t, aggregator.OutputTransforms.OutputColumns.IncludeColumns[2], "city")
}

func Test_commentAtStart(t *testing.T) {
	query := []byte("```Hello, world!``` A=1")
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.NestedNodes, 0)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
}

func Test_commentInMiddle(t *testing.T) {
	query := []byte("search A=1 ```Hello, world!``` OR C=3")
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, ast.NodeOr, filterNode.NodeType)

	assert.Equal(t, filterNode.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Comparison.Field, "C")
	assert.Equal(t, filterNode.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Comparison.Values, json.Number("3"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.OrFilterCondition.FilterCriteria)
	assert.Len(t, astNode.OrFilterCondition.NestedNodes, 0)
	assert.Len(t, astNode.OrFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "C")
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(3))
}

func Test_commentAtEnd(t *testing.T) {
	query := []byte("A=1 ```| search B=2```")
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.NestedNodes, 0)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
}

func Test_commentContainingQuotes(t *testing.T) {
	query := []byte("A=1 ```| search B=\"2\"```")
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.NestedNodes, 0)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
}

func Test_commentContainingBackticks(t *testing.T) {
	query := []byte("A=1 ```one ` and two `` still inside comment```")
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.NestedNodes, 0)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
}

func Test_commentInsideQuotes(t *testing.T) {
	query := []byte("A=\"Hello, ```this is not commented out``` world!\"")
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, "\"Hello, ```this is not commented out``` world!\"")

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.AndFilterCondition.FilterCriteria)
	assert.Len(t, astNode.AndFilterCondition.NestedNodes, 0)
	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.StringVal, "Hello, ```this is not commented out``` world!")
}

func Test_renameOneAggField(t *testing.T) {
	query := []byte(`search A=1 | stats avg(latency) AS Average`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, aggregator.MeasureOperations, 1)
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureFunc, utils.Avg)

	renameAgg := aggregator.Next
	assert.NotNil(t, renameAgg)
	assert.Equal(t, renameAgg.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, renameAgg.OutputTransforms)
	assert.NotNil(t, renameAgg.OutputTransforms.OutputColumns)
	assert.Len(t, renameAgg.OutputTransforms.OutputColumns.RenameColumns, 0)
	assert.Len(t, renameAgg.OutputTransforms.OutputColumns.RenameAggregationColumns, 1)
	assert.Equal(t, renameAgg.OutputTransforms.OutputColumns.RenameAggregationColumns["avg(latency)"], "Average")
}

func Test_renameManyAggFields(t *testing.T) {
	query := []byte(`search A=1 | stats avg(latency) AS Average, count AS Count, min(latency)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, aggregator.MeasureOperations, 3)
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureFunc, utils.Avg)
	assert.Equal(t, aggregator.MeasureOperations[1].MeasureCol, "*")
	assert.Equal(t, aggregator.MeasureOperations[1].MeasureFunc, utils.Count)
	assert.Equal(t, aggregator.MeasureOperations[2].MeasureCol, "latency")
	assert.Equal(t, aggregator.MeasureOperations[2].MeasureFunc, utils.Min)

	renameAgg := aggregator.Next
	assert.NotNil(t, renameAgg)
	assert.Equal(t, renameAgg.PipeCommandType, structs.OutputTransformType)
	assert.Len(t, renameAgg.OutputTransforms.OutputColumns.RenameColumns, 0)
	assert.Len(t, renameAgg.OutputTransforms.OutputColumns.RenameAggregationColumns, 2)
	assert.Equal(t, renameAgg.OutputTransforms.OutputColumns.RenameAggregationColumns["avg(latency)"], "Average")
	assert.Equal(t, renameAgg.OutputTransforms.OutputColumns.RenameAggregationColumns["count(*)"], "Count")
}

func Test_renameFieldsWithGroupby(t *testing.T) {
	query := []byte(`search A=1 | stats avg(latency) AS Average, count BY weekday, city`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, filterNode.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Comparison.Field, "A")
	assert.Equal(t, filterNode.Comparison.Op, "=")
	assert.Equal(t, filterNode.Comparison.Values, json.Number("1"))

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)

	assert.Len(t, astNode.AndFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))

	assert.Equal(t, aggregator.PipeCommandType, structs.GroupByType)
	assert.Len(t, aggregator.GroupByRequest.MeasureOperations, 2)
	assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[0].MeasureCol, "latency")
	assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[0].MeasureFunc, utils.Avg)
	assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[1].MeasureCol, "*")
	assert.Equal(t, aggregator.GroupByRequest.MeasureOperations[1].MeasureFunc, utils.Count)
	assert.Len(t, aggregator.GroupByRequest.GroupByColumns, 2)
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "weekday")
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[1], "city")
	assert.Equal(t, aggregator.BucketLimit, segquery.MAX_GRP_BUCKS)

	renameAgg := aggregator.Next
	assert.NotNil(t, renameAgg)
	assert.Equal(t, renameAgg.PipeCommandType, structs.OutputTransformType)
	assert.Len(t, renameAgg.OutputTransforms.OutputColumns.RenameColumns, 0)
	assert.Len(t, renameAgg.OutputTransforms.OutputColumns.RenameAggregationColumns, 1)
	assert.Equal(t, renameAgg.OutputTransforms.OutputColumns.RenameAggregationColumns["avg(latency)"], "Average")
}

func Test_rexBlockNewFieldWithoutGroupBy(t *testing.T) {
	query := []byte(`city=Boston | rex field=app_version "(?<first>\d+)\.(?<second>\d+)\.(?<third>\d+)"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)
	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.RexColRequest)
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.RexColRequest.Pattern, "(?P<first>\\d+)\\.(?P<second>\\d+)\\.(?P<third>\\d+)")
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.RexColRequest.FieldName, "app_version")
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.RexColRequest.RexColNames, []string{"first", "second", "third"})
}

func Test_rexBlockOverideExistingFieldWithoutGroupBy(t *testing.T) {
	query := []byte(`city=Boston | rex field=app_version "(?<app_version>\d+)\.(?<city>\d+)\.(?<third>\d+)"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)
	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.RexColRequest)
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.RexColRequest.Pattern, "(?P<app_version>\\d+)\\.(?P<city>\\d+)\\.(?P<third>\\d+)")
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.RexColRequest.FieldName, "app_version")
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.RexColRequest.RexColNames, []string{"app_version", "city", "third"})
}

func Test_rexBlockNewFieldWithGroupBy(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY user_email | rex field=user_email "(?<name>.+)@(?<provider>.+)"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.GroupByRequest)
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns, []string{"user_email"})
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.RexColRequest)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.RexColRequest.Pattern, "(?P<name>.+)@(?P<provider>.+)")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.RexColRequest.FieldName, "user_email")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.RexColRequest.RexColNames, []string{"name", "provider"})
}

func Test_rexBlockOverideExistingFieldWithGroupBy(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status, weekday | rex field=http_status "(?<http_status>\d)(?<weekday>\d)(?<third>\d)"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.GroupByRequest)
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns, []string{"http_status", "weekday"})
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.RexColRequest)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.RexColRequest.Pattern, "(?P<http_status>\\d)(?P<weekday>\\d)(?P<third>\\d)")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.RexColRequest.FieldName, "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.RexColRequest.RexColNames, []string{"http_status", "weekday", "third"})
}

func Test_statisticBlockWithoutStatsGroupBy(t *testing.T) {
	query := []byte(`city=Boston | rare 3 http_method, gender by country, http_status useother=true otherstr=testOther percentfield=http_method countfield=gender showperc=false`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.GroupByRequest)
	assert.Equal(t, []string{"http_method", "gender", "country", "http_status"}, aggregator.GroupByRequest.GroupByColumns)
	assert.NotNil(t, aggregator.Next)
	assert.Equal(t, structs.OutputTransformType, aggregator.Next.PipeCommandType)
	assert.NotNil(t, aggregator.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.OutputTransforms.LetColumns.StatisticColRequest)
	assert.Equal(t, structs.SFMRare, int(aggregator.Next.OutputTransforms.LetColumns.StatisticColRequest.StatisticFunctionMode))

	assert.Equal(t, "3", aggregator.Next.OutputTransforms.LetColumns.StatisticColRequest.Limit)
	assert.Equal(t, "gender", aggregator.Next.OutputTransforms.LetColumns.StatisticColRequest.StatisticOptions.CountField)
	assert.Equal(t, "testOther", aggregator.Next.OutputTransforms.LetColumns.StatisticColRequest.StatisticOptions.OtherStr)
	assert.Equal(t, "http_method", aggregator.Next.OutputTransforms.LetColumns.StatisticColRequest.StatisticOptions.PercentField)
	assert.Equal(t, true, aggregator.Next.OutputTransforms.LetColumns.StatisticColRequest.StatisticOptions.ShowCount)
	assert.Equal(t, false, aggregator.Next.OutputTransforms.LetColumns.StatisticColRequest.StatisticOptions.ShowPerc)
	assert.Equal(t, true, aggregator.Next.OutputTransforms.LetColumns.StatisticColRequest.StatisticOptions.UseOther)

	assert.Equal(t, []string{"http_method", "gender"}, aggregator.Next.OutputTransforms.LetColumns.StatisticColRequest.FieldList)
	assert.Equal(t, []string{"country", "http_status"}, aggregator.Next.OutputTransforms.LetColumns.StatisticColRequest.ByClause)
}

func Test_statisticBlockWithStatsGroupBy(t *testing.T) {
	query := []byte(`city=Boston | stats count AS gg BY http_status, weekday, gender, state | top 2 gg, state, http_status useother=true countfield=true percentfield=weekday`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.GroupByType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.GroupByRequest)
	assert.Equal(t, []string{"http_status", "weekday", "gender", "state"}, aggregator.GroupByRequest.GroupByColumns)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.NotNil(t, aggregator.Next.Next.Next)
	assert.NotNil(t, aggregator.Next.Next.GroupByRequest)
	assert.Equal(t, structs.GroupByType, aggregator.Next.Next.PipeCommandType)
	assert.Equal(t, []string{"gg", "state", "http_status"}, aggregator.Next.Next.GroupByRequest.GroupByColumns)
	assert.Equal(t, structs.OutputTransformType, aggregator.Next.Next.Next.PipeCommandType)
	assert.NotNil(t, aggregator.Next.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns)

	assert.NotNil(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.StatisticColRequest)
	assert.Equal(t, structs.SFMTop, int(aggregator.Next.Next.Next.OutputTransforms.LetColumns.StatisticColRequest.StatisticFunctionMode))
	assert.Equal(t, "2", aggregator.Next.Next.Next.OutputTransforms.LetColumns.StatisticColRequest.Limit)
	assert.Equal(t, "true", aggregator.Next.Next.Next.OutputTransforms.LetColumns.StatisticColRequest.StatisticOptions.CountField)
	assert.Equal(t, "other", aggregator.Next.Next.Next.OutputTransforms.LetColumns.StatisticColRequest.StatisticOptions.OtherStr)
	assert.Equal(t, "weekday", aggregator.Next.Next.Next.OutputTransforms.LetColumns.StatisticColRequest.StatisticOptions.PercentField)
	assert.Equal(t, true, aggregator.Next.Next.Next.OutputTransforms.LetColumns.StatisticColRequest.StatisticOptions.ShowCount)
	assert.Equal(t, true, aggregator.Next.Next.Next.OutputTransforms.LetColumns.StatisticColRequest.StatisticOptions.ShowPerc)
	assert.Equal(t, true, aggregator.Next.Next.Next.OutputTransforms.LetColumns.StatisticColRequest.StatisticOptions.UseOther)

	assert.Equal(t, []string{"gg", "state", "http_status"}, aggregator.Next.Next.Next.OutputTransforms.LetColumns.StatisticColRequest.FieldList)
	assert.Equal(t, []string(nil), aggregator.Next.Next.Next.OutputTransforms.LetColumns.StatisticColRequest.ByClause)
}

func Test_renameBlockPhrase(t *testing.T) {
	query := []byte(`city=Boston | fields city, country | rename city AS "test"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.Equal(t, aggregator.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.OutputTransforms.LetColumns.RenameColRequest)
	assert.Equal(t, structs.REMPhrase, int(aggregator.Next.OutputTransforms.LetColumns.RenameColRequest.RenameExprMode))
	assert.Equal(t, "city", aggregator.Next.OutputTransforms.LetColumns.RenameColRequest.OriginalPattern)
	assert.Equal(t, "test", aggregator.Next.OutputTransforms.LetColumns.RenameColRequest.NewPattern)
}

func Test_renameBlockRegex(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status, http_method | rename ht*_* AS start*mid*end`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.RenameColRequest)
	assert.Equal(t, structs.REMRegex, int(aggregator.Next.Next.OutputTransforms.LetColumns.RenameColRequest.RenameExprMode))
	assert.Equal(t, "ht*_*", aggregator.Next.Next.OutputTransforms.LetColumns.RenameColRequest.OriginalPattern)
	assert.Equal(t, "start*mid*end", aggregator.Next.Next.OutputTransforms.LetColumns.RenameColRequest.NewPattern)
}

func Test_renameOverrideExistingField(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status, http_method | rename http_status AS Count`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.RenameColRequest)
	assert.Equal(t, structs.REMOverride, int(aggregator.Next.Next.OutputTransforms.LetColumns.RenameColRequest.RenameExprMode))
	assert.Equal(t, "http_status", aggregator.Next.Next.OutputTransforms.LetColumns.RenameColRequest.OriginalPattern)
	assert.Equal(t, "Count", aggregator.Next.Next.OutputTransforms.LetColumns.RenameColRequest.NewPattern)
}

func Test_evalNewField(t *testing.T) {
	query := []byte(`search A=1 | stats max(latency) AS Max | eval MaxSeconds=Max . " seconds"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)

	// Second agg is for renaming max(latency) to Max, the third is for eval.
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "MaxSeconds")
	assert.Len(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms, 2)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].IsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].Value, "Max")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].Value, " seconds")
}

func Test_evalReplaceField(t *testing.T) {
	query := []byte(`search A=1 | stats max(latency) AS Max | eval Max=Max . " seconds"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)

	// Second agg is for renaming max(latency) to Max, the third is for eval.
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "Max")
	assert.Len(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms, 2)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].IsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].Value, "Max")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].Value, " seconds")
}

func Test_evalAfterGroupBy(t *testing.T) {
	query := []byte(`search A=1 | stats max(latency) AS Max BY weekday | eval Max=Max . " seconds on " . weekday`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)

	// Second agg is for renaming max(latency) to Max, the third is for eval.
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "Max")
	assert.Len(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms, 3)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].IsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].Value, "Max")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].Value, " seconds on ")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].IsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].Value, "weekday")
}

func Test_evalReplaceGroupByCol(t *testing.T) {
	query := []byte(`search A=1 | stats max(latency) AS Max BY weekday | eval weekday=weekday . ": " . Max . " seconds"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)

	// Second agg is for renaming max(latency) to Max, the third is for eval.
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "weekday")
	assert.Len(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms, 4)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].IsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].Value, "weekday")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].Value, ": ")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].IsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].Value, "Max")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[3].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[3].Value, " seconds")
}

func Test_evalFunctionsToNumber(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval result=tonumber("0A4",16)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMNumericExpr)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.NumericExprMode), structs.NEMNumericExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "tonumber")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.NumericExprMode), structs.NEMNumber)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.Value, "16")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Val)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Val.StringExprMode), structs.SEMRawString)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Val.RawString, "0A4")

}

func Test_evalFunctionsAbs(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval myField=abs(http_status - 100)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "myField")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "abs")
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right, err)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Op, "-")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.Value, "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Value, "100")
}

func Test_evalFunctionsCeil(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY weekday | eval ceil=ceil(Count + 0.2)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "weekday")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "ceil")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal, false)
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right, err)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Op, "+")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.Value, "Count")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Value, "0.2")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "ceil")
}

func Test_evalFunctionsRound(t *testing.T) {
	query := []byte(`city=Detroit | stats count AS Count BY latitude | where latitude > 89.6 | eval round=round(latitude)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.LeftValue.NumericExpr.Value, "latitude")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.LeftValue.NumericExpr.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.RightValue.NumericExpr.Value, "89.6")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.RightValue.NumericExpr.ValueIsField, false)

	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "round")
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Value, "latitude")
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal, true)
	assert.Nil(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right)
}

func Test_evalFunctionsRoundPrecision(t *testing.T) {
	query := []byte(`city=Detroit | stats count AS Count BY latitude | where latitude > 89.6 | eval round=round(latitude, 3)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.LeftValue.NumericExpr.Value, "latitude")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.LeftValue.NumericExpr.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.RightValue.NumericExpr.Value, "89.6")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.RightValue.NumericExpr.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "round")
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Value, "latitude")
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.Value, "3")
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.IsTerminal, true)
}

func Test_evalFunctionsSqrt(t *testing.T) {
	query := []byte(`city=Columbus | stats count AS Count BY http_status | eval sqrt=sqrt(http_status + 200)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "sqrt")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "sqrt")
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right, err)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Op, "+")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.Value, "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Value, "200")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.ValueIsField, false)
}

func Test_evalFunctionsLen(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY app_name | eval len=len(app_name) | where len > 22`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "len")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "len")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.NumericExprMode), structs.NEMLenField)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Value, "app_name")
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right)
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left)
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right)
}

func Test_evalFunctionsLower(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval myField="Test concat:" . lower(state) . "  end"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "myField")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMStringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].Value, "Test concat:")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.Op, "lower")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.Value.StringExprMode), structs.SEMField)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.Value.FieldName, "state")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].Value, "  end")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].IsField, false)
}

func Test_evalFunctionsLtrim(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval myField=ltrim(state, "Ma") . " test end"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "myField")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMStringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.Op, "ltrim")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.StrToRemove, "Ma")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.Value.FieldName, "state")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.Value.StringExprMode), structs.SEMField)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].Value, " test end")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, false)
}

func Test_evalFunctionsRtrim(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval myField=state . " start:" . rtrim(state, "nd")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "myField")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMStringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].Value, "state")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].IsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].Value, " start:")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].TextExpr.Op, "rtrim")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].TextExpr.Value.FieldName, "state")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].TextExpr.Value.StringExprMode), structs.SEMField)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].TextExpr.StrToRemove, "nd")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].IsField, false)
}

func Test_evalFunctionsIf(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval myField=if(http_status > 400, http_status, "Error")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "myField")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMConditionExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.Op, "if")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.Value, "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.RightValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.RightValue.NumericExpr.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.RightValue.NumericExpr.Value, "400")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.NumericExpr.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.NumericExpr.Value, "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.FalseValue.StringExpr.RawString, "Error")
}

func Test_evalFunctionsIn(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | where http_status in(404, 301, "abc")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.FilterRows)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.IsTerminal, true)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.FilterRows.LeftValue)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.FilterRows.LeftValue.NumericExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.LeftValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.LeftValue.NumericExpr.ValueIsField, true)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.FilterRows.LeftValue.NumericExpr.NumericExprMode), structs.NEMNumberField)
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.FilterRows.RightValue)

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.ValueOp, "in")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.FilterRows.ValueList)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.ValueList[0].NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.ValueList[0].NumericExpr.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.ValueList[0].NumericExpr.Value, "404")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.ValueList[1].NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.ValueList[1].NumericExpr.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.ValueList[1].NumericExpr.Value, "301")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.FilterRows.ValueList[2].StringExpr.RawString, "abc")
}

func Test_evalFunctionsIfAndIn(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval myField=if(in(state, "Mary" . "land", "Hawaii", 99 + 1), state, "Error")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "myField")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMConditionExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.Op, "if")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueOp, "in")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.Value, "state")
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.RightValue)

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueList[0].StringExpr.ConcatExpr.Atoms[0].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueList[0].StringExpr.ConcatExpr.Atoms[0].Value, "Mary")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueList[0].StringExpr.ConcatExpr.Atoms[1].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueList[0].StringExpr.ConcatExpr.Atoms[1].Value, "land")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueList[1].StringExpr.RawString, "Hawaii")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueList[2].NumericExpr.Op, "+")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueList[2].NumericExpr.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueList[2].NumericExpr.Left.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueList[2].NumericExpr.Left.Value, "99")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueList[2].NumericExpr.Right.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueList[2].NumericExpr.Right.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueList[2].NumericExpr.Right.Value, "1")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.NumericExpr.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.NumericExpr.Value, "state")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.FalseValue.StringExpr.RawString, "Error")
}

func Test_evalFunctionsCidrmatch(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval result=if(cidrmatch("192.0.2.0/24", "192.0.2.5"), "local", "not local")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMConditionExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.Op, "if")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueOp, "cidrmatch")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.StringExpr.RawString, "192.0.2.0/24")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.RightValue.StringExpr.RawString, "192.0.2.5")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.StringExpr.RawString, "local")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.FalseValue.StringExpr.RawString, "not local")

}
func Test_evalFunctionsIfAndIsString(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY country | eval result=if(isstr(country), "This is a string", "This is not a string")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMConditionExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.Op, "if")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueOp, "isstr")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.Value, "country")
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.RightValue)

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.StringExpr.RawString, "This is a string")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.FalseValue.StringExpr.RawString, "This is not a string")

}

func Test_evalFunctionsIfAndIsInt(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval result=if(isint(http_status), "This is an integer", "This is not an integer")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMConditionExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.Op, "if")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueOp, "isint")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.Value, "http_status")
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.RightValue)

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.StringExpr.RawString, "This is an integer")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.FalseValue.StringExpr.RawString, "This is not an integer")

}
func Test_evalFunctionsIfAndIsBool(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY city | eval result=if(isbool(city), "This is a boolean value", "This is not a boolean value")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMConditionExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.Op, "if")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueOp, "isbool")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.Value, "city")
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.RightValue)

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.StringExpr.RawString, "This is a boolean value")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.FalseValue.StringExpr.RawString, "This is not a boolean value")

}

func Test_evalFunctionsIfAndIsNull(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval result=if(isnull(state), "This is a null value", "This is not a null value")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMConditionExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.Op, "if")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueOp, "isnull")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.Value, "state")
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.RightValue)

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.StringExpr.RawString, "This is a null value")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.FalseValue.StringExpr.RawString, "This is not a null value")

}

func Test_evalFunctionsLike(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval result=if(like(http_status, "4%"), "True", "False")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMConditionExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.Op, "if")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueOp, "like")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.Value, "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.RightValue.StringExpr.RawString, "4%")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.StringExpr.RawString, "True")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.FalseValue.StringExpr.RawString, "False")

}

func Test_evalFunctionsMatch(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY country | eval result=if(match(country, "^Sa"), "yes", "no")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMConditionExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.Op, "if")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueOp, "match")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.Value, "country")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.RightValue.StringExpr.RawString, "^Sa")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.StringExpr.RawString, "yes")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.FalseValue.StringExpr.RawString, "no")

}

func Test_evalFunctionsUrldecode(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval result=urldecode("http%3A%2F%2Fwww.splunk.com%2Fdownload%3Fr%3Dheader")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMStringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "urldecode")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Value)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Value.RawString, "http%3A%2F%2Fwww.splunk.com%2Fdownload%3Fr%3Dheader")

}

func Test_evalFunctionsSplit(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY ident | eval result=split(ident,"-")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMStringExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "split")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Value)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Value.StringExprMode), structs.SEMField)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Value.FieldName, "ident")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Delimiter)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Delimiter.RawString, "-")

}

func Test_evalFunctionsNow(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY ident | eval result=now()`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMNumericExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.NumericExprMode), structs.NEMNumber)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "now")

}

func Test_evalFunctionsMax(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval result=max(1, 3, 450, http_status)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMStringExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "max")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MaxMinValues)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MaxMinValues[0].ConcatExpr.Atoms[0].IsField, false)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MaxMinValues[0].ConcatExpr.Atoms[0].Value, "1")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MaxMinValues[1].ConcatExpr.Atoms[0].IsField, false)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MaxMinValues[1].ConcatExpr.Atoms[0].Value, "3")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MaxMinValues[2].ConcatExpr.Atoms[0].IsField, false)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MaxMinValues[2].ConcatExpr.Atoms[0].Value, "450")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MaxMinValues[3].FieldName, "http_status")
}
func Test_evalFunctionsMin(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval result=min(1, 3, 450, http_status)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMStringExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "min")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MaxMinValues)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MaxMinValues[0].ConcatExpr.Atoms[0].IsField, false)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MaxMinValues[0].ConcatExpr.Atoms[0].Value, "1")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MaxMinValues[1].ConcatExpr.Atoms[0].IsField, false)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MaxMinValues[1].ConcatExpr.Atoms[0].Value, "3")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MaxMinValues[2].ConcatExpr.Atoms[0].IsField, false)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MaxMinValues[2].ConcatExpr.Atoms[0].Value, "450")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MaxMinValues[3].FieldName, "http_status")
}

func Test_evalFunctionsSubstr(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval result=substr("splendid", 1, 3) . substr("chunk", -3)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMStringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.Op, "substr")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.Value)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.Value.StringExprMode), structs.SEMRawString)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.Value.RawString, "splendid")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.StartIndex)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.StartIndex.NumericExprMode), structs.NEMNumber)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.StartIndex.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.StartIndex.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.StartIndex.Value, "1")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.LengthExpr)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.LengthExpr.NumericExprMode), structs.NEMNumber)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.LengthExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.LengthExpr.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.LengthExpr.Value, "3")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.Op, "substr")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.Value)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.Value.StringExprMode), structs.SEMRawString)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.Value.RawString, "chunk")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.StartIndex)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.StartIndex.NumericExprMode), structs.NEMNumber)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.StartIndex.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.StartIndex.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.StartIndex.Value, "-3")
}

func Test_evalFunctionsToStringBooleanValue(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval result=tostring((2 > 1))`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMStringExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "tostring")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.ValueExprMode), structs.VEMBooleanExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.BooleanExpr.ValueOp, ">")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.BooleanExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.BooleanExpr.LeftValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.BooleanExpr.LeftValue.NumericExpr.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.BooleanExpr.LeftValue.NumericExpr.Value, "2")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.BooleanExpr.RightValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.BooleanExpr.RightValue.NumericExpr.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.BooleanExpr.RightValue.NumericExpr.Value, "1")
}

func Test_evalFunctionsToStringHex(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval result=tostring(15,"hex")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMStringExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "tostring")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.ValueExprMode), structs.VEMNumericExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.Value, "15")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Format.StringExprMode), structs.SEMRawString)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Format.RawString, "hex")
}

func Test_evalFunctionsToStringCommas(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval result=tostring(12345.6789,"commas")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMStringExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "tostring")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.ValueExprMode), structs.VEMNumericExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.Value, "12345.6789")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Format.StringExprMode), structs.SEMRawString)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Format.RawString, "commas")
}

func Test_evalFunctionsToStringDuration(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval result=tostring(615,"duration")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMStringExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "tostring")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.ValueExprMode), structs.VEMNumericExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.Value, "615")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Format.StringExprMode), structs.SEMRawString)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Format.RawString, "duration")
}

func Test_evalFunctionsExact(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval result=exact(3.14 * http_status)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "exact")
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right, err)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Op, "*")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.Value, "3.14")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Value, "http_status")
}

func Test_evalFunctionsExp(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval result=exp(3)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "result")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "exp")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Value, "3")
}

func Test_ChainedEval(t *testing.T) {
	query := []byte(`search A=1 | stats max(latency) AS Max | eval Max=Max . " seconds" | eval Max="Max Latency: " . Max`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)

	// Second agg is for renaming max(latency) to Max, the third is for the first eval.
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "Max")
	assert.Len(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms, 2)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].IsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].Value, "Max")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].Value, " seconds")

	// The fourth agg is for the second eval.
	assert.NotNil(t, aggregator.Next.Next.Next)
	assert.Equal(t, aggregator.Next.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.NewColName, "Max")
	assert.Len(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms, 2)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].IsField, false)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].Value, "Max Latency: ")
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, true)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].Value, "Max")
}

func Test_evalWithMultipleElements(t *testing.T) {
	query := []byte(`search A=1 | stats max(latency) AS Max | eval Max=Max . " seconds", Max="Max Latency: " . Max`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)

	// Second agg is for renaming max(latency) to Max, the third is for the first statement in eval.
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "Max")
	assert.Len(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms, 2)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].IsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].Value, "Max")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].Value, " seconds")

	// The fourth agg is for the second statement in eval.
	assert.NotNil(t, aggregator.Next.Next.Next)
	assert.Equal(t, aggregator.Next.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.NewColName, "Max")
	assert.Len(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms, 2)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].IsField, false)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].Value, "Max Latency: ")
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, true)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].Value, "Max")
}

func Test_evalWithMultipleSpaces(t *testing.T) {
	query := []byte(`search      A    =   1  |   stats   max(  latency  )   AS   Max | eval Max  =  Max   .  " seconds", Max="Max Latency: " . Max`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)

	// Second agg is for renaming max(latency) to Max, the third is for the first statement in eval.
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "Max")
	assert.Len(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms, 2)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].IsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].Value, "Max")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].Value, " seconds")

	// The fourth agg is for the second statement in eval.
	assert.NotNil(t, aggregator.Next.Next.Next)
	assert.Equal(t, aggregator.Next.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.NewColName, "Max")
	assert.Len(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms, 2)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].IsField, false)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].Value, "Max Latency: ")
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, true)
	assert.Equal(t, aggregator.Next.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].Value, "Max")
}

func Test_evalWithMultipleSpaces2(t *testing.T) {
	query := []byte(`search   A   =   1   OR  ( B =  2  AND   C =  3)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter

	assert.NotNil(t, filterNode)
	assert.Equal(t, filterNode.NodeType, ast.NodeOr)
	assert.Equal(t, filterNode.Right.NodeType, ast.NodeAnd)

	assert.Equal(t, filterNode.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Left.Comparison.Field, "A")
	assert.Equal(t, filterNode.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Left.Comparison.Values, json.Number("1"))

	assert.Equal(t, filterNode.Right.Left.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Left.Comparison.Field, "B")
	assert.Equal(t, filterNode.Right.Left.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Left.Comparison.Values, json.Number("2"))

	assert.Equal(t, filterNode.Right.Right.NodeType, ast.NodeTerminal)
	assert.Equal(t, filterNode.Right.Right.Comparison.Field, "C")
	assert.Equal(t, filterNode.Right.Right.Comparison.Op, "=")
	assert.Equal(t, filterNode.Right.Right.Comparison.Values, json.Number("3"))

	astNode := &structs.ASTNode{}
	err = pipesearch.SearchQueryToASTnode(filterNode, astNode, 0)
	assert.Nil(t, err)
	assert.NotNil(t, astNode.OrFilterCondition.FilterCriteria)
	assert.Len(t, astNode.OrFilterCondition.FilterCriteria, 1)
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "A")
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.OrFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(1))
	assert.Len(t, astNode.OrFilterCondition.NestedNodes, 1)
	assert.Len(t, astNode.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria, 2)
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "B")
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[0].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(2))
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.LeftInput.Expression.LeftInput.ColumnName, "C")
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.FilterOperator, utils.Equals)
	assert.Equal(t, astNode.OrFilterCondition.NestedNodes[0].AndFilterCondition.FilterCriteria[1].ExpressionFilter.RightInput.Expression.LeftInput.ColumnValue.UnsignedVal, uint64(3))
}

func Test_SimpleNumericEval(t *testing.T) {
	query := []byte(`search A=1 | stats count AS Count | eval Thousands=Count / 1000`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)

	// Second agg is for renaming count(*) to Count, the third is for eval.
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "Thousands")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "/")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Value, "Count")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.Value, "1000")
}

func Test_NumericEvalLeftAssociative(t *testing.T) {
	query := []byte(`search A=1 | stats count AS Count | eval Custom=100 - Count - 40.5`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)

	// Second agg is for renaming count(*) to Count, the third is for eval.
	// The node structure should be:
	//         Minus
	//         /   \
	//      Minus  40.5
	//      /   \
	//    100  Count
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "Custom")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr)

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "-")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Op, "-")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.Value, "100")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Value, "Count")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.Value, "40.5")
}

func Test_NumericEvalOrderOfOperations(t *testing.T) {
	query := []byte(`search A=1 | stats count AS Count | eval Custom=100 - 17 * 22 / 5 + 11`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)

	// Second agg is for renaming count(*) to Count, the third is for eval.
	// The eval should be parsed as (100 - ((17 * 22) / 5)) + 11, which is:
	//         Plus
	//         /   \
	//      Minus   11
	//      /   \
	//    100  Divide
	//         /    \
	//      Times    5
	//      /   \
	//     17   22
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "Custom")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr)

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "+")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Op, "-")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.Value, "100")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Op, "/")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Left.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Left.Op, "*")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Left.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Left.Left.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Left.Left.Value, "17")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Left.Right.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Left.Right.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Left.Right.Value, "22")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Right.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Right.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Right.Value, "5")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.Value, "11")
}

func Test_NumericEvalParentheses(t *testing.T) {
	query := []byte(`search A=1 | stats count AS Count | eval Custom=22 * (100 - 17)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)

	// Second agg is for renaming count(*) to Count, the third is for eval.
	// The node structure should be:
	//      Times
	//      /   \
	//    22   Minus
	//         /   \
	//       100   17
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "Custom")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr)

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "*")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Value, "22")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.Op, "-")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.Left.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.Left.Value, "100")

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.Right.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.Right.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.Right.Value, "17")
}

func Test_WhereNumeric(t *testing.T) {
	query := []byte(`search A=1 | stats count AS Count | where 22 * (100 - 17) >= Count / 50`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)

	// Second agg is for renaming count(*) to Count, the third is for eval.
	// The node structure should be:
	//             BoolExpr
	//            /   |    \
	//    ValueExpr   >=   ValueExpr
	//        |                |
	//      Times             Div
	//      /   \            /   \
	//    22   Minus      Count   50
	//         /   \
	//       100   17
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)

	filterRows := aggregator.Next.Next.OutputTransforms.FilterRows
	assert.NotNil(t, filterRows)
	assert.Equal(t, filterRows.IsTerminal, true)
	assert.Equal(t, filterRows.ValueOp, ">=")

	assert.Equal(t, int(filterRows.LeftValue.ValueExprMode), structs.VEMNumericExpr)
	assert.NotNil(t, filterRows.LeftValue.NumericExpr)

	assert.Equal(t, filterRows.LeftValue.NumericExpr.IsTerminal, false)
	assert.Equal(t, filterRows.LeftValue.NumericExpr.Op, "*")

	assert.Equal(t, filterRows.LeftValue.NumericExpr.Left.IsTerminal, true)
	assert.Equal(t, filterRows.LeftValue.NumericExpr.Left.ValueIsField, false)
	assert.Equal(t, filterRows.LeftValue.NumericExpr.Left.Value, "22")

	assert.Equal(t, filterRows.LeftValue.NumericExpr.Right.IsTerminal, false)
	assert.Equal(t, filterRows.LeftValue.NumericExpr.Right.Op, "-")

	assert.Equal(t, filterRows.LeftValue.NumericExpr.Right.Left.IsTerminal, true)
	assert.Equal(t, filterRows.LeftValue.NumericExpr.Right.Left.ValueIsField, false)
	assert.Equal(t, filterRows.LeftValue.NumericExpr.Right.Left.Value, "100")

	assert.Equal(t, filterRows.LeftValue.NumericExpr.Right.Right.IsTerminal, true)
	assert.Equal(t, filterRows.LeftValue.NumericExpr.Right.Right.ValueIsField, false)
	assert.Equal(t, filterRows.LeftValue.NumericExpr.Right.Right.Value, "17")

	assert.Equal(t, int(filterRows.RightValue.ValueExprMode), structs.VEMNumericExpr)
	assert.NotNil(t, filterRows.RightValue.NumericExpr)

	assert.Equal(t, filterRows.RightValue.NumericExpr.IsTerminal, false)
	assert.Equal(t, filterRows.RightValue.NumericExpr.Op, "/")

	assert.Equal(t, filterRows.RightValue.NumericExpr.Left.IsTerminal, true)
	assert.Equal(t, filterRows.RightValue.NumericExpr.Left.ValueIsField, true)
	assert.Equal(t, filterRows.RightValue.NumericExpr.Left.Value, "Count")

	assert.Equal(t, filterRows.RightValue.NumericExpr.Right.IsTerminal, true)
	assert.Equal(t, filterRows.RightValue.NumericExpr.Right.ValueIsField, false)
	assert.Equal(t, filterRows.RightValue.NumericExpr.Right.Value, "50")
}

func Test_WhereConcat(t *testing.T) {
	query := []byte(`search A=1 | stats count AS Count BY weekday | where Count = 10 . " items"`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)

	// Second agg is for renaming count(*) to Count, the third is for eval.
	// The node structure should be:
	//             BoolExpr
	//            /   |    \
	//    ValueExpr   =   ValueExpr
	//        |               |
	//      Count         ConcatExpr
	//                      /   \
	//                    10   " items"
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)

	filterRows := aggregator.Next.Next.OutputTransforms.FilterRows
	assert.NotNil(t, filterRows)
	assert.Equal(t, filterRows.IsTerminal, true)
	assert.Equal(t, filterRows.ValueOp, "=")

	assert.Equal(t, int(filterRows.LeftValue.ValueExprMode), structs.VEMNumericExpr)
	assert.Equal(t, filterRows.LeftValue.NumericExpr.Value, "Count")
	assert.Equal(t, filterRows.LeftValue.NumericExpr.ValueIsField, true)

	assert.Equal(t, int(filterRows.RightValue.ValueExprMode), structs.VEMStringExpr)
	assert.NotNil(t, filterRows.RightValue.StringExpr.ConcatExpr)

	assert.Len(t, filterRows.RightValue.StringExpr.ConcatExpr.Atoms, 2)
	assert.Equal(t, filterRows.RightValue.StringExpr.ConcatExpr.Atoms[0].IsField, false)
	assert.Equal(t, filterRows.RightValue.StringExpr.ConcatExpr.Atoms[0].Value, "10")
	assert.Equal(t, filterRows.RightValue.StringExpr.ConcatExpr.Atoms[1].IsField, false)
	assert.Equal(t, filterRows.RightValue.StringExpr.ConcatExpr.Atoms[1].Value, " items")
}

func Test_WhereBooleanOrderOfOperations(t *testing.T) {
	query := []byte(`search A=1 | stats count AS Count | where Count > 1 OR Count > 2 AND NOT (Count > 3) OR Count > 4`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)

	// Second agg is for renaming count(*) to Count, the third is for eval.
	// This should be parsed as: (Count > 1 OR (Count > 2 AND NOT (Count > 3))) OR Count > 4
	// The node structure should be:
	//                          BoolExpr
	//                        /     |    \
	//                BoolExpr     OR     BoolExpr
	//               /   |    \           /   |   \
	//       BoolExpr   OR    BoolExpr  Count >    4
	//       /   |   \        /   |   \
	//    Count  >    1  BoolExpr AND  BoolExpr
	//                  /   |   \      /      |
	//               Count  >    2  BoolExp  Not
	//                             /   |   \
	//                          Count  >    3

	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)

	filterRows := aggregator.Next.Next.OutputTransforms.FilterRows
	assert.NotNil(t, filterRows)
	assert.Equal(t, filterRows.IsTerminal, false)
	assert.Equal(t, filterRows.BoolOp, structs.BoolOpOr)

	assert.Equal(t, filterRows.LeftBool.IsTerminal, false)
	assert.Equal(t, filterRows.LeftBool.BoolOp, structs.BoolOpOr)

	assert.Equal(t, filterRows.LeftBool.LeftBool.IsTerminal, true)
	assert.Equal(t, filterRows.LeftBool.LeftBool.RightValue.NumericExpr.Value, "1")
	assert.Equal(t, filterRows.LeftBool.LeftBool.RightValue.NumericExpr.ValueIsField, false)

	assert.Equal(t, filterRows.LeftBool.RightBool.IsTerminal, false)
	assert.Equal(t, filterRows.LeftBool.RightBool.BoolOp, structs.BoolOpAnd)

	assert.Equal(t, filterRows.LeftBool.RightBool.LeftBool.IsTerminal, true)
	assert.Equal(t, filterRows.LeftBool.RightBool.LeftBool.RightValue.NumericExpr.Value, "2")
	assert.Equal(t, filterRows.LeftBool.RightBool.LeftBool.RightValue.NumericExpr.ValueIsField, false)

	assert.Equal(t, filterRows.LeftBool.RightBool.RightBool.IsTerminal, false)
	assert.Equal(t, filterRows.LeftBool.RightBool.RightBool.BoolOp, structs.BoolOpNot)

	assert.Equal(t, filterRows.LeftBool.RightBool.RightBool.LeftBool.IsTerminal, true)
	assert.Equal(t, filterRows.LeftBool.RightBool.RightBool.LeftBool.RightValue.NumericExpr.Value, "3")
	assert.Equal(t, filterRows.LeftBool.RightBool.RightBool.LeftBool.RightValue.NumericExpr.ValueIsField, false)

	assert.Equal(t, filterRows.RightBool.IsTerminal, true)
	assert.Equal(t, filterRows.RightBool.RightValue.NumericExpr.Value, "4")
	assert.Equal(t, filterRows.RightBool.RightValue.NumericExpr.ValueIsField, false)

}

func Test_WhereBoolean(t *testing.T) {
	query := []byte(`search A=1 | stats count AS Count, min(latency) AS Min, max(latency) AS Max | where Count > 100 OR (Max > 1000 AND NOT ((Max - Min) / 2 <= 600))`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)

	// Second agg is for renaming, the third is for eval.
	// The node structure should be:
	//                  BoolExpr
	//                /     |    \
	//        BoolExpr     OR     BoolExpr
	//        /  |  \            /    |   \
	//    Count  >  100   BoolExpr   And   BoolExpr
	//                    /  |  \          /      |
	//                  Max  >  1000   BoolExpr  Not
	//                                /   |   \
	//                       ValueExpr   <=   ValueExpr
	//                           |                |
	//                      NumericExpr          600
	//                          |
	//                         Div
	//                        /   \
	//                     Minus   2
	//                    /     \
	//                  Max     Min

	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)

	filterRows := aggregator.Next.Next.OutputTransforms.FilterRows
	assert.NotNil(t, filterRows)
	assert.Equal(t, filterRows.IsTerminal, false)
	assert.Equal(t, filterRows.BoolOp, structs.BoolOpOr)

	assert.Equal(t, filterRows.LeftBool.IsTerminal, true)
	assert.Equal(t, int(filterRows.LeftBool.LeftValue.ValueExprMode), structs.VEMNumericExpr)
	assert.Equal(t, filterRows.LeftBool.LeftValue.NumericExpr.Value, "Count")
	assert.Equal(t, filterRows.LeftBool.LeftValue.NumericExpr.ValueIsField, true)

	assert.Equal(t, filterRows.LeftBool.ValueOp, ">")
	assert.Equal(t, int(filterRows.LeftBool.RightValue.ValueExprMode), structs.VEMNumericExpr)
	assert.NotNil(t, filterRows.LeftBool.RightValue.NumericExpr)
	assert.Equal(t, filterRows.LeftBool.RightValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, filterRows.LeftBool.RightValue.NumericExpr.Value, "100")
	assert.Equal(t, filterRows.LeftBool.RightValue.NumericExpr.ValueIsField, false)

	assert.Equal(t, filterRows.RightBool.IsTerminal, false)
	assert.Equal(t, filterRows.RightBool.BoolOp, structs.BoolOpAnd)

	assert.Equal(t, filterRows.RightBool.LeftBool.IsTerminal, true)
	assert.Equal(t, int(filterRows.RightBool.LeftBool.LeftValue.ValueExprMode), structs.VEMNumericExpr)
	assert.Equal(t, filterRows.RightBool.LeftBool.LeftValue.NumericExpr.Value, "Max")
	assert.Equal(t, filterRows.RightBool.LeftBool.LeftValue.NumericExpr.ValueIsField, true)

	assert.Equal(t, filterRows.RightBool.LeftBool.ValueOp, ">")
	assert.Equal(t, int(filterRows.RightBool.LeftBool.RightValue.ValueExprMode), structs.VEMNumericExpr)
	assert.NotNil(t, filterRows.RightBool.LeftBool.RightValue.NumericExpr)
	assert.Equal(t, filterRows.RightBool.LeftBool.RightValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, filterRows.RightBool.LeftBool.RightValue.NumericExpr.Value, "1000")
	assert.Equal(t, filterRows.RightBool.LeftBool.RightValue.NumericExpr.ValueIsField, false)

	assert.Equal(t, filterRows.RightBool.RightBool.IsTerminal, false)
	assert.Equal(t, filterRows.RightBool.RightBool.BoolOp, structs.BoolOpNot)
	assert.Nil(t, filterRows.RightBool.RightBool.RightBool)

	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.IsTerminal, true)
	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.ValueOp, "<=")

	assert.Equal(t, int(filterRows.RightBool.RightBool.LeftBool.LeftValue.ValueExprMode), structs.VEMNumericExpr)
	assert.NotNil(t, filterRows.RightBool.RightBool.LeftBool.LeftValue.NumericExpr)
	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.LeftValue.NumericExpr.IsTerminal, false)
	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.LeftValue.NumericExpr.Op, "/")
	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.LeftValue.NumericExpr.Left.IsTerminal, false)
	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.LeftValue.NumericExpr.Left.Op, "-")
	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.LeftValue.NumericExpr.Left.Left.IsTerminal, true)
	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.LeftValue.NumericExpr.Left.Left.Value, "Max")
	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.LeftValue.NumericExpr.Left.Left.ValueIsField, true)
	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.LeftValue.NumericExpr.Left.Right.Value, "Min")
	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.LeftValue.NumericExpr.Left.Right.ValueIsField, true)
	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.LeftValue.NumericExpr.Right.IsTerminal, true)
	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.LeftValue.NumericExpr.Right.Value, "2")
	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.LeftValue.NumericExpr.Right.ValueIsField, false)

	assert.Equal(t, int(filterRows.RightBool.RightBool.LeftBool.RightValue.ValueExprMode), structs.VEMNumericExpr)
	assert.NotNil(t, filterRows.RightBool.RightBool.LeftBool.RightValue.NumericExpr)
	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.RightValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.RightValue.NumericExpr.Value, "600")
	assert.Equal(t, filterRows.RightBool.RightBool.LeftBool.RightValue.NumericExpr.ValueIsField, false)
}

func Test_head(t *testing.T) {
	query := []byte(`A=1 | head`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)

	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.Equal(t, aggregator.OutputTransforms.MaxRows, uint64(10)) // This is the SPL default when no value is given.
}

func Test_headWithNumber(t *testing.T) {
	query := []byte(`A=1 | head 22`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)

	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.Equal(t, aggregator.OutputTransforms.MaxRows, uint64(22))
}

// SPL allows "limit=" right before the number.
func Test_headWithLimitKeyword(t *testing.T) {
	query := []byte(`A=1 | head limit=15`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)

	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.Equal(t, aggregator.OutputTransforms.MaxRows, uint64(15))
}

func Test_dedupOneField(t *testing.T) {
	query := []byte(`A=1 | dedup state`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)

	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.DedupColRequest)

	dedupExpr := aggregator.OutputTransforms.LetColumns.DedupColRequest
	assert.Equal(t, dedupExpr.Limit, uint64(1))
	assert.Equal(t, dedupExpr.FieldList, []string{"state"})
	assert.NotNil(t, dedupExpr.DedupOptions)
	assert.Equal(t, dedupExpr.DedupOptions.Consecutive, false)
	assert.Equal(t, dedupExpr.DedupOptions.KeepEmpty, false)
	assert.Equal(t, dedupExpr.DedupOptions.KeepEvents, false)
	assert.Len(t, dedupExpr.DedupSortEles, 0)
}

func Test_dedupMultipleFields(t *testing.T) {
	query := []byte(`A=1 | dedup state weekday http_status`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)

	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.DedupColRequest)

	dedupExpr := aggregator.OutputTransforms.LetColumns.DedupColRequest
	assert.Equal(t, dedupExpr.Limit, uint64(1))
	assert.Equal(t, dedupExpr.FieldList, []string{"state", "weekday", "http_status"})
	assert.NotNil(t, dedupExpr.DedupOptions)
	assert.Equal(t, dedupExpr.DedupOptions.Consecutive, false)
	assert.Equal(t, dedupExpr.DedupOptions.KeepEmpty, false)
	assert.Equal(t, dedupExpr.DedupOptions.KeepEvents, false)
	assert.Len(t, dedupExpr.DedupSortEles, 0)
}

func Test_dedupWithLimit(t *testing.T) {
	query := []byte(`A=1 | dedup 4 state weekday http_status`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)

	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.DedupColRequest)

	dedupExpr := aggregator.OutputTransforms.LetColumns.DedupColRequest
	assert.Equal(t, dedupExpr.Limit, uint64(4))
	assert.Equal(t, dedupExpr.FieldList, []string{"state", "weekday", "http_status"})
	assert.NotNil(t, dedupExpr.DedupOptions)
	assert.Equal(t, dedupExpr.DedupOptions.Consecutive, false)
	assert.Equal(t, dedupExpr.DedupOptions.KeepEmpty, false)
	assert.Equal(t, dedupExpr.DedupOptions.KeepEvents, false)
	assert.Len(t, dedupExpr.DedupSortEles, 0)
}

func Test_dedupWithOptionsBeforeFieldList(t *testing.T) {
	query := []byte(`A=1 | dedup keepevents=true keepempty=false consecutive=true state weekday http_status `)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)

	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.DedupColRequest)

	dedupExpr := aggregator.OutputTransforms.LetColumns.DedupColRequest
	assert.Equal(t, dedupExpr.Limit, uint64(1))
	assert.Equal(t, dedupExpr.FieldList, []string{"state", "weekday", "http_status"})
	assert.NotNil(t, dedupExpr.DedupOptions)
	assert.Equal(t, dedupExpr.DedupOptions.Consecutive, true)
	assert.Equal(t, dedupExpr.DedupOptions.KeepEmpty, false)
	assert.Equal(t, dedupExpr.DedupOptions.KeepEvents, true)
	assert.Len(t, dedupExpr.DedupSortEles, 0)
}

func Test_dedupWithOptionsAfterFieldList(t *testing.T) {
	query := []byte(`A=1 | dedup state weekday http_status keepevents=true keepempty=true consecutive=false`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)

	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.DedupColRequest)

	dedupExpr := aggregator.OutputTransforms.LetColumns.DedupColRequest
	assert.Equal(t, dedupExpr.Limit, uint64(1))
	assert.Equal(t, dedupExpr.FieldList, []string{"state", "weekday", "http_status"})
	assert.NotNil(t, dedupExpr.DedupOptions)
	assert.Equal(t, dedupExpr.DedupOptions.Consecutive, false)
	assert.Equal(t, dedupExpr.DedupOptions.KeepEmpty, true)
	assert.Equal(t, dedupExpr.DedupOptions.KeepEvents, true)
	assert.Len(t, dedupExpr.DedupSortEles, 0)
}

func Test_dedupWithSortBy(t *testing.T) {
	query := []byte(`A=1 | dedup state weekday http_status sortby +weekday -state`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)

	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.DedupColRequest)

	dedupExpr := aggregator.OutputTransforms.LetColumns.DedupColRequest
	assert.Equal(t, dedupExpr.Limit, uint64(1))
	assert.Equal(t, dedupExpr.FieldList, []string{"state", "weekday", "http_status"})
	assert.NotNil(t, dedupExpr.DedupOptions)
	assert.Equal(t, dedupExpr.DedupOptions.Consecutive, false)
	assert.Equal(t, dedupExpr.DedupOptions.KeepEmpty, false)
	assert.Equal(t, dedupExpr.DedupOptions.KeepEvents, false)
	assert.Len(t, dedupExpr.DedupSortEles, 2)
	assert.Equal(t, dedupExpr.DedupSortEles[0].SortByAsc, true)
	assert.Equal(t, dedupExpr.DedupSortEles[0].Op, "")
	assert.Equal(t, dedupExpr.DedupSortEles[0].Field, "weekday")
	assert.Equal(t, dedupExpr.DedupSortEles[1].SortByAsc, false)
	assert.Equal(t, dedupExpr.DedupSortEles[1].Op, "")
	assert.Equal(t, dedupExpr.DedupSortEles[1].Field, "state")
}

func Test_sortWithOneField(t *testing.T) {
	query := []byte(`A=1 | sort auto(city)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)

	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.SortColRequest)

	sortExpr := aggregator.OutputTransforms.LetColumns.SortColRequest
	assert.Len(t, sortExpr.SortEles, 1)
	assert.Equal(t, []int{1}, sortExpr.SortAscending)
	assert.Equal(t, "city", sortExpr.SortEles[0].Field)
	assert.Equal(t, "auto", sortExpr.SortEles[0].Op)
	assert.Equal(t, true, sortExpr.SortEles[0].SortByAsc)
}

func Test_sortWithMultipleFields(t *testing.T) {
	query := []byte(`A=1 | sort str(app_name), -city, num(latency)`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)

	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.SortColRequest)

	sortExpr := aggregator.OutputTransforms.LetColumns.SortColRequest
	assert.Len(t, sortExpr.SortEles, 3)
	assert.Equal(t, []int{1, -1, 1}, sortExpr.SortAscending)
	assert.Equal(t, "app_name", sortExpr.SortEles[0].Field)
	assert.Equal(t, "str", sortExpr.SortEles[0].Op)
	assert.Equal(t, true, sortExpr.SortEles[0].SortByAsc)

	assert.Equal(t, "city", sortExpr.SortEles[1].Field)
	assert.Equal(t, "", sortExpr.SortEles[1].Op)
	assert.Equal(t, false, sortExpr.SortEles[1].SortByAsc)

	assert.Equal(t, "latency", sortExpr.SortEles[2].Field)
	assert.Equal(t, "num", sortExpr.SortEles[2].Op)
	assert.Equal(t, true, sortExpr.SortEles[2].SortByAsc)
}

// SPL Transaction command.
func Test_TransactionRequestWithFields(t *testing.T) {
	query := []byte(`A=1 | transaction A`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.TransactionArguments)

	transactionRequest := aggregator.TransactionArguments
	assert.Equal(t, aggregator.PipeCommandType, structs.TransactionType)
	assert.Equal(t, transactionRequest.Fields, []string{"A"})

	query = []byte(`A=1 | transaction A B C`)
	res, err = spl.Parse("", query)
	assert.Nil(t, err)
	filterNode = res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err = pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.TransactionArguments)

	transactionRequest = aggregator.TransactionArguments
	assert.Equal(t, aggregator.PipeCommandType, structs.TransactionType)
	assert.Equal(t, transactionRequest.Fields, []string{"A", "B", "C"})
}

func Test_TransactionRequestWithStartsAndEndsWith(t *testing.T) {
	query1 := []byte(`A=1 | transaction A B C startswith="foo" endswith="bar"`)
	query1Res := &structs.TransactionArguments{
		Fields:     []string{"A", "B", "C"},
		StartsWith: &structs.FilterStringExpr{StringValue: "foo"},
		EndsWith:   &structs.FilterStringExpr{StringValue: "bar"},
	}

	query2 := []byte(`A=1 | transaction endswith="bar" startswith="foo"`)
	query2Res := &structs.TransactionArguments{
		Fields:     []string(nil),
		StartsWith: &structs.FilterStringExpr{StringValue: "foo"},
		EndsWith:   &structs.FilterStringExpr{StringValue: "bar"},
	}

	query3 := []byte(`A=1 | transaction startswith="foo" endswith="bar"`)
	query3Res := &structs.TransactionArguments{
		Fields:     []string(nil),
		StartsWith: &structs.FilterStringExpr{StringValue: "foo"},
		EndsWith:   &structs.FilterStringExpr{StringValue: "bar"},
	}

	query4 := []byte(`A=1 | transaction endswith="bar"`)
	query4Res := &structs.TransactionArguments{
		Fields:     []string(nil),
		StartsWith: nil,
		EndsWith:   &structs.FilterStringExpr{StringValue: "bar"},
	}

	query5 := []byte(`A=1 | transaction startswith="foo"`)
	query5Res := &structs.TransactionArguments{
		Fields:     []string(nil),
		StartsWith: &structs.FilterStringExpr{StringValue: "foo"},
		EndsWith:   nil,
	}

	query6 := []byte(`A=1 | transaction startswith="foo" endswith="bar" A B C`)
	query6Res := &structs.TransactionArguments{
		Fields:     []string{"A", "B", "C"},
		StartsWith: &structs.FilterStringExpr{StringValue: "foo"},
		EndsWith:   &structs.FilterStringExpr{StringValue: "bar"},
	}

	queries := [][]byte{query1, query2, query3, query4, query5, query6}
	results := []*structs.TransactionArguments{query1Res, query2Res, query3Res, query4Res, query5Res, query6Res}

	for ind, query := range queries {
		res, err := spl.Parse("", query)
		assert.Nil(t, err)
		filterNode := res.(ast.QueryStruct).SearchFilter
		assert.NotNil(t, filterNode)

		astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
		assert.Nil(t, err)
		assert.NotNil(t, astNode)
		assert.NotNil(t, aggregator)
		assert.NotNil(t, aggregator.TransactionArguments)

		transactionRequest := aggregator.TransactionArguments
		assert.Equal(t, aggregator.PipeCommandType, structs.TransactionType)
		assert.Equal(t, transactionRequest.Fields, results[ind].Fields)
		assert.Equal(t, transactionRequest.StartsWith, results[ind].StartsWith)
		assert.Equal(t, transactionRequest.EndsWith, results[ind].EndsWith)
	}
}

func Test_TransactionRequestWithFilterStringExpr(t *testing.T) {
	// CASE 1: Fields + StartsWith is Eval + EndsWith is TransactionQueryString With only OR
	query1 := []byte(`A=1 | transaction A B C startswith=eval(duration > 10) endswith=("foo" OR "bar2")`)
	query1Res := &structs.TransactionArguments{
		Fields: []string{"A", "B", "C"},
		StartsWith: &structs.FilterStringExpr{
			EvalBoolExpr: &structs.BoolExpr{
				IsTerminal: true,
				LeftValue: &structs.ValueExpr{
					NumericExpr: &structs.NumericExpr{
						IsTerminal:      true,
						NumericExprMode: structs.NEMNumberField,
						ValueIsField:    true,
						Value:           "duration",
					},
				},
				RightValue: &structs.ValueExpr{
					NumericExpr: &structs.NumericExpr{
						IsTerminal:      true,
						NumericExprMode: structs.NEMNumber,
						ValueIsField:    false,
						Value:           "10",
					},
				},
				ValueOp: ">",
			},
		},
		EndsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				OrFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("foo"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("foo"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("bar2"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("bar2"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
					},
				},
			},
		},
	}

	// CASE 2: Fields + StartsWith is searchTerm (String) + EndsWith is TransactionQueryString With OR & AND
	query2 := []byte(`A=1 | transaction A B C startswith=status="ok" endswith=("foo" OR "foo1" AND "bar")`)
	query2Res := &structs.TransactionArguments{
		Fields: []string{"A", "B", "C"},
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							ExpressionFilter: &structs.ExpressionFilter{
								LeftInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: nil,
											ColumnName:  "status",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								FilterOperator: utils.Equals,
								RightInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: &utils.DtypeEnclosure{
												Dtype:     utils.SS_DT_STRING,
												StringVal: "ok",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("bar"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("bar"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
					},
					NestedNodes: []*structs.ASTNode{
						{
							OrFilterCondition: &structs.Condition{
								FilterCriteria: []*structs.FilterCriteria{
									{
										MatchFilter: &structs.MatchFilter{
											MatchColumn: "*",
											MatchWords: [][]byte{
												[]byte("foo"),
											},
											MatchOperator: utils.And,
											MatchPhrase:   []byte("foo"),
											MatchType:     structs.MATCH_PHRASE,
										},
									},
									{
										MatchFilter: &structs.MatchFilter{
											MatchColumn: "*",
											MatchWords: [][]byte{
												[]byte("foo1"),
											},
											MatchOperator: utils.And,
											MatchPhrase:   []byte("foo1"),
											MatchType:     structs.MATCH_PHRASE,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// CASE 3: Fields + StartWith is searchTerm (Number) + endswith is Eval
	query3 := []byte(`A=1 | transaction A B C startswith=duration>10 endswith=eval(status<400)`)
	query3Res := &structs.TransactionArguments{
		Fields: []string{"A", "B", "C"},
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							ExpressionFilter: &structs.ExpressionFilter{
								LeftInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: nil,
											ColumnName:  "duration",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								FilterOperator: utils.GreaterThan,
								RightInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: &utils.DtypeEnclosure{
												Dtype:       utils.SS_DT_UNSIGNED_NUM,
												UnsignedVal: uint64(10),
												SignedVal:   int64(10),
												FloatVal:    float64(10),
												StringVal:   "10",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{
			EvalBoolExpr: &structs.BoolExpr{
				IsTerminal: true,
				LeftValue: &structs.ValueExpr{
					NumericExpr: &structs.NumericExpr{
						IsTerminal:      true,
						NumericExprMode: structs.NEMNumberField,
						ValueIsField:    true,
						Value:           "status",
					},
				},
				RightValue: &structs.ValueExpr{
					NumericExpr: &structs.NumericExpr{
						IsTerminal:      true,
						NumericExprMode: structs.NEMNumber,
						ValueIsField:    false,
						Value:           "400",
					},
				},
				ValueOp: "<",
			},
		},
	}

	// CASE 4: Fields + StartWith is searchTerm (String) + endswith is String Value
	query4 := []byte(`A=1 | transaction A B C startswith=status="Ok" endswith="foo"`)
	query4Res := &structs.TransactionArguments{
		Fields: []string{"A", "B", "C"},
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							ExpressionFilter: &structs.ExpressionFilter{
								LeftInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: nil,
											ColumnName:  "status",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								FilterOperator: utils.Equals,
								RightInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: &utils.DtypeEnclosure{
												Dtype:     utils.SS_DT_STRING,
												StringVal: "Ok",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{
			StringValue: "foo",
		},
	}

	// CASE 5: Fields + StartWith is String Search Expression + endswith is String Value
	query5 := []byte(`A=1 | transaction A B C startswith="status=300 OR status=bar" endswith="bar"`)
	query5Res := &structs.TransactionArguments{
		Fields: []string{"A", "B", "C"},
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				OrFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							ExpressionFilter: &structs.ExpressionFilter{
								LeftInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: nil,
											ColumnName:  "status",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								RightInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: &utils.DtypeEnclosure{
												Dtype:       utils.SS_DT_UNSIGNED_NUM,
												StringVal:   "300",
												UnsignedVal: uint64(300),
												SignedVal:   int64(300),
												FloatVal:    float64(300),
											},
											ColumnName: "",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								FilterOperator: utils.Equals,
							},
						},
						{
							ExpressionFilter: &structs.ExpressionFilter{
								LeftInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: nil,
											ColumnName:  "status",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								RightInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: &utils.DtypeEnclosure{
												Dtype:     utils.SS_DT_STRING,
												StringVal: "bar",
											},
											ColumnName: "",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								FilterOperator: utils.Equals,
							},
						},
					},
					NestedNodes: nil,
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{
			StringValue: "bar",
		},
	}

	// CASE 6: Fields + StartWith is String Search Expression + endswith is Eval
	query6 := []byte(`A=1 | transaction A B C startswith="status=foo OR status=bar AND action=login" endswith=eval(status<400)`)
	query6Res := &structs.TransactionArguments{
		Fields: []string{"A", "B", "C"},
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							ExpressionFilter: &structs.ExpressionFilter{
								LeftInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: nil,
											ColumnName:  "action",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								RightInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: &utils.DtypeEnclosure{
												Dtype:     utils.SS_DT_STRING,
												StringVal: "login",
											},
											ColumnName: "",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								FilterOperator: utils.Equals,
							},
						},
					},
					NestedNodes: []*structs.ASTNode{
						{
							OrFilterCondition: &structs.Condition{
								FilterCriteria: []*structs.FilterCriteria{
									{
										ExpressionFilter: &structs.ExpressionFilter{
											LeftInput: &structs.FilterInput{
												Expression: &structs.Expression{
													LeftInput: &structs.ExpressionInput{
														ColumnValue: nil,
														ColumnName:  "status",
													},
													ExpressionOp: utils.Add,
													RightInput:   nil,
												},
											},
											RightInput: &structs.FilterInput{
												Expression: &structs.Expression{
													LeftInput: &structs.ExpressionInput{
														ColumnValue: &utils.DtypeEnclosure{
															Dtype:     utils.SS_DT_STRING,
															StringVal: "foo",
														},
														ColumnName: "",
													},
													ExpressionOp: utils.Add,
													RightInput:   nil,
												},
											},
											FilterOperator: utils.Equals,
										},
									},
									{
										ExpressionFilter: &structs.ExpressionFilter{
											LeftInput: &structs.FilterInput{
												Expression: &structs.Expression{
													LeftInput: &structs.ExpressionInput{
														ColumnValue: nil,
														ColumnName:  "status",
													},
													ExpressionOp: utils.Add,
													RightInput:   nil,
												},
											},
											RightInput: &structs.FilterInput{
												Expression: &structs.Expression{
													LeftInput: &structs.ExpressionInput{
														ColumnValue: &utils.DtypeEnclosure{
															Dtype:     utils.SS_DT_STRING,
															StringVal: "bar",
														},
														ColumnName: "",
													},
													ExpressionOp: utils.Add,
													RightInput:   nil,
												},
											},
											FilterOperator: utils.Equals,
										},
									},
								},
								NestedNodes: nil,
							},
						},
					},
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{
			EvalBoolExpr: &structs.BoolExpr{
				IsTerminal: true,
				LeftValue: &structs.ValueExpr{
					NumericExpr: &structs.NumericExpr{
						IsTerminal:      true,
						NumericExprMode: structs.NEMNumberField,
						ValueIsField:    true,
						Value:           "status",
					},
				},
				RightValue: &structs.ValueExpr{
					NumericExpr: &structs.NumericExpr{
						IsTerminal:      true,
						NumericExprMode: structs.NEMNumber,
						ValueIsField:    false,
						Value:           "400",
					},
				},
				ValueOp: "<",
			},
		},
	}

	// CASE 7: Fileds + StartWith is Search Term (With Number) + endsWith is String Search Expression
	query7 := []byte(`A=1 | transaction A B C startswith=(status>300 OR status=201) endswith="status=foo OR status=bar AND action=login"`)
	query7Res := &structs.TransactionArguments{
		Fields: []string{"A", "B", "C"},
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				OrFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							ExpressionFilter: &structs.ExpressionFilter{
								LeftInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: nil,
											ColumnName:  "status",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								FilterOperator: utils.GreaterThan,
								RightInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: &utils.DtypeEnclosure{
												Dtype:       utils.SS_DT_UNSIGNED_NUM,
												UnsignedVal: uint64(300),
												SignedVal:   int64(300),
												FloatVal:    float64(300),
												StringVal:   "300",
											},
										},
									},
								},
							},
						},
						{
							ExpressionFilter: &structs.ExpressionFilter{
								LeftInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: nil,
											ColumnName:  "status",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								FilterOperator: utils.Equals,
								RightInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: &utils.DtypeEnclosure{
												Dtype:       utils.SS_DT_UNSIGNED_NUM,
												UnsignedVal: uint64(201),
												SignedVal:   int64(201),
												FloatVal:    float64(201),
												StringVal:   "201",
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							ExpressionFilter: &structs.ExpressionFilter{
								LeftInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: nil,
											ColumnName:  "action",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								RightInput: &structs.FilterInput{
									Expression: &structs.Expression{
										LeftInput: &structs.ExpressionInput{
											ColumnValue: &utils.DtypeEnclosure{
												Dtype:     utils.SS_DT_STRING,
												StringVal: "login",
											},
											ColumnName: "",
										},
										ExpressionOp: utils.Add,
										RightInput:   nil,
									},
								},
								FilterOperator: utils.Equals,
							},
						},
					},
					NestedNodes: []*structs.ASTNode{
						{
							OrFilterCondition: &structs.Condition{
								FilterCriteria: []*structs.FilterCriteria{
									{
										ExpressionFilter: &structs.ExpressionFilter{
											LeftInput: &structs.FilterInput{
												Expression: &structs.Expression{
													LeftInput: &structs.ExpressionInput{
														ColumnValue: nil,
														ColumnName:  "status",
													},
													ExpressionOp: utils.Add,
													RightInput:   nil,
												},
											},
											RightInput: &structs.FilterInput{
												Expression: &structs.Expression{
													LeftInput: &structs.ExpressionInput{
														ColumnValue: &utils.DtypeEnclosure{
															Dtype:     utils.SS_DT_STRING,
															StringVal: "foo",
														},
														ColumnName: "",
													},
													ExpressionOp: utils.Add,
													RightInput:   nil,
												},
											},
											FilterOperator: utils.Equals,
										},
									},
									{
										ExpressionFilter: &structs.ExpressionFilter{
											LeftInput: &structs.FilterInput{
												Expression: &structs.Expression{
													LeftInput: &structs.ExpressionInput{
														ColumnValue: nil,
														ColumnName:  "status",
													},
													ExpressionOp: utils.Add,
													RightInput:   nil,
												},
											},
											RightInput: &structs.FilterInput{
												Expression: &structs.Expression{
													LeftInput: &structs.ExpressionInput{
														ColumnValue: &utils.DtypeEnclosure{
															Dtype:     utils.SS_DT_STRING,
															StringVal: "bar",
														},
														ColumnName: "",
													},
													ExpressionOp: utils.Add,
													RightInput:   nil,
												},
											},
											FilterOperator: utils.Equals,
										},
									},
								},
								NestedNodes: nil,
							},
						},
					},
				},
			},
		},
	}

	// CASE 8: Fields + StartsWith=OR String Clauses + EndsWith=OR Clauses
	query8 := []byte(`A=1 | transaction A B C startswith=("GET" OR "POST1") endswith=("DELETE" OR "POST2")`)
	query8Res := &structs.TransactionArguments{
		Fields: []string{"A", "B", "C"},
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				OrFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("GET"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("GET"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("POST1"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("POST1"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
					},
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				OrFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("DELETE"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("DELETE"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("POST2"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("POST2"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
					},
				},
			},
		},
	}

	// CASE 9: Fields + StartsWith=AND String Clauses + EndsWith=Single String Clause
	query9 := []byte(`A=1 | transaction A B C startswith=("GET" AND "POST1") endswith=("DELETE")`)
	query9Res := &structs.TransactionArguments{
		Fields: []string{"A", "B", "C"},
		StartsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("GET"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("GET"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("POST1"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("POST1"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
					},
				},
			},
		},
		EndsWith: &structs.FilterStringExpr{
			SearchNode: &structs.ASTNode{
				AndFilterCondition: &structs.Condition{
					FilterCriteria: []*structs.FilterCriteria{
						{
							MatchFilter: &structs.MatchFilter{
								MatchColumn: "*",
								MatchWords: [][]byte{
									[]byte("DELETE"),
								},
								MatchOperator: utils.And,
								MatchPhrase:   []byte("DELETE"),
								MatchType:     structs.MATCH_PHRASE,
							},
						},
					},
				},
			},
		},
	}

	queries := [][]byte{query1, query2, query3, query4, query5, query6, query7, query8, query9}
	results := []*structs.TransactionArguments{query1Res, query2Res, query3Res, query4Res, query5Res, query6Res, query7Res, query8Res, query9Res}

	for ind, query := range queries {
		res, err := spl.Parse("", query)
		assert.Nil(t, err)
		filterNode := res.(ast.QueryStruct).SearchFilter
		assert.NotNil(t, filterNode)

		astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
		assert.Nil(t, err)
		assert.NotNil(t, astNode)
		assert.NotNil(t, aggregator)
		assert.NotNil(t, aggregator.TransactionArguments)

		transactionRequest := aggregator.TransactionArguments
		assert.Equal(t, structs.TransactionType, aggregator.PipeCommandType)
		assert.Equal(t, results[ind].Fields, transactionRequest.Fields)
		assert.Equal(t, results[ind].StartsWith, transactionRequest.StartsWith)
		assert.Equal(t, results[ind].EndsWith, transactionRequest.EndsWith)
	}
}
