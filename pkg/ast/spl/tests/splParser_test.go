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
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/ast"
	"github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/ast/spl"
	segquery "github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	putils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// Helper functions

func parseWithoutError(t *testing.T, query string) (*structs.ASTNode, *structs.QueryAggregators) {
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)

	return astNode, aggregator
}

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

func Test_ParseTimechart(t *testing.T) {
	query := `search A=1 | timechart span=1y avg(latency)`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
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

func Test_EvalTrigAndHyperbolicfunctions(t *testing.T) {
	testTrigAndHyperbolicfunctions(t, "acos")
	testTrigAndHyperbolicfunctions(t, "acosh")
	testTrigAndHyperbolicfunctions(t, "asin")
	testTrigAndHyperbolicfunctions(t, "asinh")
	testTrigAndHyperbolicfunctions(t, "atan")
	testTrigAndHyperbolicfunctions(t, "atanh")
	testTrigAndHyperbolicfunctions(t, "cos")
	testTrigAndHyperbolicfunctions(t, "cosh")
	testTrigAndHyperbolicfunctions(t, "sin")
	testTrigAndHyperbolicfunctions(t, "sinh")
	testTrigAndHyperbolicfunctions(t, "tan")
	testTrigAndHyperbolicfunctions(t, "tanh")
}

func testTrigAndHyperbolicfunctions(t *testing.T, op string) {
	query := []byte("city=Columbus | stats count AS Count BY http_status | eval newField=" + op + "(1)")
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, op)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Value, "1")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.ValueIsField, false)
}

func Test_EvalAtan2(t *testing.T) {
	query := []byte(`city=Columbus | stats count AS Count BY http_status | eval newField=atan2(0.5, 0.75)`)
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "atan2")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Value, "0.5")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.Value, "0.75")
	assert.True(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.IsTerminal)
}

func Test_EvalHypot(t *testing.T) {
	query := []byte(`city=Columbus | stats count AS Count BY http_status | eval newField=hypot(3, 4)`)
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "hypot")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Value, "3")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.Value, "4")
	assert.True(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.IsTerminal)
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
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.Param.StringExprMode), structs.SEMField)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.Param.FieldName, "state")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].Value, "  end")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].IsField, false)
}

func Test_evalFunctionsUpper(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval myField="Test concat:" . upper(state) . "  end"`)
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.Op, "upper")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.Param.StringExprMode), structs.SEMField)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.Param.FieldName, "state")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].Value, "  end")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].IsField, false)
}

func Test_evalFunctionsTrim(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval myField=trim(state, "Ma") . " test end"`)
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.Op, "trim")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.StrToRemove, "Ma")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.Param.FieldName, "state")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.Param.StringExprMode), structs.SEMField)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].Value, " test end")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, false)
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.Param.FieldName, "state")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.Param.StringExprMode), structs.SEMField)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].IsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].Value, " test end")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].IsField, false)
}

func Test_evalFunctionsReplace(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval myField=replace(date, "^(\d{1,2})/(\d{1,2})/", "\2/\1/")`)
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
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "replace")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.Value, "date")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[0].RawString, `^(\d{1,2})/(\d{1,2})/`)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[1].RawString, `\2/\1/`)
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].TextExpr.Param.FieldName, "state")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].TextExpr.Param.StringExprMode), structs.SEMField)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].TextExpr.StrToRemove, "nd")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[2].IsField, false)
}

func Test_evalFunctionsSpath(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval myField=spath(_raw, "vendorProductSet.product.desc.locDesc")`)
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
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "spath")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.InputColName, "_raw")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.Path, `vendorProductSet.product.desc.locDesc`)
	assert.False(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.IsPathFieldName)
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

func Test_evalFunctionsIfAndIsNum(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY country | eval result=if(isnum(http_status), "This is a number", "This is not a number")`)
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueOp, "isnum")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.Value, "http_status")
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.RightValue)

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.StringExpr.RawString, "This is a number")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.FalseValue.StringExpr.RawString, "This is not a number")

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

func Test_evalFunctionsIfAndIsNotNull(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval result=if(isnotnull(state), "yes", "no")`)
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueOp, "isnotnull")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.Value, "state")
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.RightValue)

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.StringExpr.RawString, "yes")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.FalseValue.StringExpr.RawString, "no")
}

func Test_evalFunctionsIfAndSearchMatch(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval result=if(searchmatch("x=hi y=*"), "yes", "no")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)
	expectedFields := []string{"x", "y"}

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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueOp, "searchmatch")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.StringExpr.RawString, "x=hi y=*")
	assert.True(t, putils.CompareStringSlices(expectedFields, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.StringExpr.FieldList))
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.RightValue)

	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.StringExpr.RawString, "yes")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.FalseValue.StringExpr.RawString, "no")

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
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.RawString, "http%3A%2F%2Fwww.splunk.com%2Fdownload%3Fr%3Dheader")

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
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMMultiValueExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.Op, "split")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.StringExprParams)
	assert.Equal(t, len(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.StringExprParams), 2)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.StringExprParams[0])
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.StringExprParams[1])
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.StringExprParams[0].StringExprMode), structs.SEMField)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.StringExprParams[0].FieldName, "ident")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.StringExprParams[1])
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.StringExprParams[1].RawString, "-")
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

func Test_evalFunctionsTime(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY ident | eval result=time()`)
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "time")
}

func Test_evalFunctionsRelativeTime(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY ident | eval result=relative_time(now(), "-1d@d")`)
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
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.NumericExprMode), structs.NEMNumericExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "relative_time")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Op, "now")
	assert.True(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.RelativeTime.Offset, int64(-1))
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.RelativeTime.TimeUnit, utils.TMDay)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.RelativeTime.Snap, fmt.Sprintf("%d", utils.TMDay))
}

func Test_evalFunctionsStrfTime(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY ident | eval result=strftime(timeField, "%Y-%m-%dT%H:%M:%S.%Q")`)
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
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "strftime")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.Value, "timeField")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.RawString, "%Y-%m-%dT%H:%M:%S.%Q")
}

func Test_evalFunctionsStrptime(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY ident | eval result=strptime(timeStr, "%H:%M")`)
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
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "strptime")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.Value, "timeStr")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.RawString, "%H:%M")
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
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[0].ConcatExpr.Atoms[0].IsField, false)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[0].ConcatExpr.Atoms[0].Value, "1")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[1].ConcatExpr.Atoms[0].IsField, false)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[1].ConcatExpr.Atoms[0].Value, "3")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[2].ConcatExpr.Atoms[0].IsField, false)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[2].ConcatExpr.Atoms[0].Value, "450")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[3].FieldName, "http_status")
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
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[0].ConcatExpr.Atoms[0].IsField, false)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[0].ConcatExpr.Atoms[0].Value, "1")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[1].ConcatExpr.Atoms[0].IsField, false)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[1].ConcatExpr.Atoms[0].Value, "3")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[2].ConcatExpr.Atoms[0].IsField, false)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[2].ConcatExpr.Atoms[0].Value, "450")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[3].FieldName, "http_status")
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
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.Param)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.Param.StringExprMode), structs.SEMRawString)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[0].TextExpr.Param.RawString, "splendid")
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
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.Param)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.Param.StringExprMode), structs.SEMRawString)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.ConcatExpr.Atoms[1].TextExpr.Param.RawString, "chunk")
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
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.StringExprMode), structs.SEMRawString)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.RawString, "hex")
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
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.StringExprMode), structs.SEMRawString)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.RawString, "commas")
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
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.StringExprMode), structs.SEMRawString)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.RawString, "duration")
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

func Test_evalFunctionsCase(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=case(http_status = 200, "OK")`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.Op, "case")
	assert.Len(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.ConditionValuePairs, 1)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.ConditionValuePairs[0].Condition.ValueOp, "=")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.ConditionValuePairs[0].Condition.LeftValue.NumericExpr.Value, "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.ConditionValuePairs[0].Condition.RightValue.NumericExpr.Value, "200")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsValidate(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=validate(http_status = 200, "OK")`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.Op, "validate")
	assert.Len(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.ConditionValuePairs, 1)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.ConditionValuePairs[0].Condition.ValueOp, "=")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.ConditionValuePairs[0].Condition.LeftValue.NumericExpr.Value, "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.ConditionValuePairs[0].Condition.RightValue.NumericExpr.Value, "200")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsCoalesce(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=coalesce(city, "usa")`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.Op, "coalesce")
	assert.Len(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.ValueList, 2)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.ValueList[0].NumericExpr.Value, "city")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.ValueList[1].StringExpr.RawString, "usa")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsNullIf(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=nullif(http_status, newField)`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.Op, "nullif")
	assert.Len(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.ValueList, 2)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.ValueList[0].NumericExpr.Value, "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.ValueList[1].NumericExpr.Value, "newField")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsNull(t *testing.T) {
	query := []byte(`city=Boston | eval newField=null()`)
	_, err := spl.Parse("", query)
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.NewColName, "newField")
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.Op, "null")
}

func Test_evalFunctionsNullInAIf(t *testing.T) {
	query := []byte(`city=Boston | eval newField=if(http_status = 200, null(), "OK")`)
	_, err := spl.Parse("", query)
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.NewColName, "newField")
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr)
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.Op, "if")
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue)
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.ConditionExpr.Op, "null")
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.FalseValue)
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.FalseValue.StringExpr.RawString, "OK")
}

func Test_evalFunctionsIpMask(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval result=ipmask("255.255.255.0", clientip)`)
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "ipmask")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.ValueExprMode), structs.VEMNumericExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.IsTerminal, true)
	assert.True(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.ValueIsField)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.Value, "clientip")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.StringExprMode), structs.SEMRawString)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.RawString, "255.255.255.0")
}

func Test_evalFunctionsObjectToArray(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval result=object_to_array(statePop,"state", "population")`)
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "object_to_array")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.ValueExprMode), structs.VEMNumericExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.IsTerminal, true)
	assert.True(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.ValueIsField)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.Value, "statePop")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.StringExprMode), structs.SEMRawStringList)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.StringList, []string{"\"state\"", "\"population\""})
}

func Test_evalFunctionsPrintf(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval result=printf("%c,%c","abc","Foo")`)
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "printf")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.StringExprMode), structs.SEMRawString)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.RawString, "%c,%c")
	assert.Len(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList, 2)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[0].RawString, "abc")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[1].RawString, "Foo")
}

func Test_evalFunctionsToJson(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY state | eval result=tojson(true())`)
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "tojson")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.StringExprMode), structs.SEMRawString)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.RawString, "true")
}

func Test_evalFunctionsPi(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=pi()`)
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "pi")
}

func Test_evalFunctionsFloor(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY weekday | eval floor=floor(Count + 0.2)`)
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "floor")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal, false)
	assert.Nil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right, err)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Op, "+")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Left.Value, "Count")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.Value, "0.2")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Right.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "floor")
}

func Test_evalFunctionsLn(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=ln(http_status)`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "ln")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Value, "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsLog(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=log(http_status, 2)`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "log")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Value, "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.Value, "2")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsPower(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=pow(http_status, 2)`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "pow")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Value, "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.ValueIsField, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right.Value, "2")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsSigfig(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=sigfig(http_status)`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "sigfig")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, false)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.ValueIsField, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left.Value, "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsRandom(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=random()`)
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
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op, "random")
}

func Test_evalFunctionsMvAppend(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=mvappend("abc", http_status)`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.Op, "mvappend")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprMode), structs.MVEMMultiValueExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams)
	assert.Len(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams, 2)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams[0])
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams[0].ValueExprMode), structs.VEMStringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams[0].StringExpr)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams[0].StringExpr.StringExprMode), int(structs.SEMRawString))
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams[0].StringExpr.RawString), "abc")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams[1])
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams[1].ValueExprMode), structs.VEMMultiValueExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams[1].MultiValueExpr)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams[1].MultiValueExpr.MultiValueExprMode), structs.MVEMField)
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams[1].MultiValueExpr.FieldName), "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsMvCount(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=mvcount(http_status)`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "mvcount")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MultiValueExpr)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MultiValueExpr.MultiValueExprMode), structs.MVEMField)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MultiValueExpr.FieldName, "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsMvDedup(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=mvdedup(http_status)`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.Op, "mvdedup")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprMode), int(structs.MVEMMultiValueExpr))
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams)
	assert.Equal(t, len(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams), 1)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0])
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0].MultiValueExprMode), structs.MVEMField)
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0].FieldName), "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsMvFilter(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=mvfilter(http_status > 300)`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.Op, "mvfilter")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprMode), int(structs.MVEMMultiValueExpr))
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.Condition)
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.Condition.LeftValue.NumericExpr.Value), "http_status")
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.Condition.ValueOp), ">")
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.Condition.RightValue.NumericExpr.Value), "300")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsMvFind(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=mvfind(http_status, "err\d+")`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)
	compiledRegex, _ := regexp.Compile(`err\d+`)
	assert.Nil(t, err)
	assert.NotNil(t, compiledRegex)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.NotNil(t, aggregator.Next.Next)
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "mvfind")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MultiValueExpr)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MultiValueExpr.MultiValueExprMode), structs.MVEMField)
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MultiValueExpr.FieldName), "http_status")
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Regex.GetCompiledRegex()), compiledRegex)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsMvIndex(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=mvindex(http_status, 1)`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.Op, "mvindex")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprMode), int(structs.MVEMMultiValueExpr))
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams)
	assert.Equal(t, len(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams), 1)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0])
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0].MultiValueExprMode), structs.MVEMField)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.NumericExprParams)
	assert.Equal(t, len(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.NumericExprParams), 1)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.NumericExprParams[0].IsTerminal, true)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.NumericExprParams[0].Value, "1")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsMvJoin(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=mvjoin(http_status, ";")`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "mvjoin")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MultiValueExpr)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MultiValueExpr.MultiValueExprMode), structs.MVEMField)
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.MultiValueExpr.FieldName), "http_status")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Delimiter.StringExprMode), int(structs.SEMRawString))
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Delimiter.RawString), ";")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsMvMap(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=mvmap(http_status, http_status * 10)`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.Op, "mvmap")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprMode), int(structs.MVEMMultiValueExpr))
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams)
	assert.Equal(t, len(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams), 1)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0])
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0].MultiValueExprMode), structs.MVEMField)
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0].FieldName), "http_status")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams)
	assert.Equal(t, len(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams), 1)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams[0])
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams[0].NumericExpr)
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams[0].NumericExpr.Op), "*")
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams[0].NumericExpr.Left.Value), "http_status")
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.ValueExprParams[0].NumericExpr.Right.Value), "10")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsMvRange(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=mvrange(1514834731, 1524134919, "7d")`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.Op, "mvrange")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprMode), int(structs.MVEMMultiValueExpr))
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.NumericExprParams)
	assert.Equal(t, len(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.NumericExprParams), 2)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.NumericExprParams[0])
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.NumericExprParams[0].NumericExprMode), int(structs.NEMNumber))
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.NumericExprParams[0].Value), "1514834731")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.NumericExprParams[1])
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.NumericExprParams[1].NumericExprMode), int(structs.NEMNumber))
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.NumericExprParams[1].Value), "1524134919")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.StringExprParams)
	assert.Equal(t, len(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.StringExprParams), 1)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.StringExprParams[0])
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.StringExprParams[0].StringExprMode), int(structs.SEMRawString))
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.StringExprParams[0].RawString), "7d")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsMvSort(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=mvsort(http_status)`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.Op, "mvsort")
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprMode), int(structs.MVEMMultiValueExpr))
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams)
	assert.Equal(t, len(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams), 1)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0])
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0].MultiValueExprMode), structs.MVEMField)
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0].FieldName), "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsMvZip(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=mvzip(mvleft, mvright)`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprMode), structs.MVEMMultiValueExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.Op, "mvzip")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams)
	assert.Equal(t, len(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams), 2)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0])
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0].MultiValueExprMode), structs.MVEMField)
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0].FieldName), "mvleft")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[1])
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[1].MultiValueExprMode), structs.MVEMField)
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[1].FieldName), "mvright")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsMvToJsonArray(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=mv_to_json_array(http_status, true())`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr)
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprMode), structs.MVEMMultiValueExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.Op, "mv_to_json_array")
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams)
	assert.Equal(t, len(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams), 1)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0])
	assert.Equal(t, int(aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0].MultiValueExprMode), structs.MVEMField)
	assert.Equal(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.MultiValueExprParams[0].FieldName), "http_status")
	assert.True(t, (aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.MultiValueExpr.InferTypes))
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsCluster(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=cluster(http_status, threshold:0.5, match:termset, delims:";")`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "cluster")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Cluster.Field, "http_status")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Cluster.Threshold, 0.5)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Cluster.Match, "termset")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Cluster.Delims, "\";\"")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsGetFields(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=getfields("status_*_*")`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "getfields")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Param.RawString, "status_*_*")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
}

func Test_evalFunctionsTypeOf(t *testing.T) {
	query := []byte(`city=Boston | stats count AS Count BY http_status | eval newField=typeof("abc")`)
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
	assert.Equal(t, aggregator.GroupByRequest.GroupByColumns[0], "http_status")
	assert.Equal(t, aggregator.Next.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.NotNil(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "typeof")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.StringExpr.RawString, "abc")
	assert.Equal(t, aggregator.Next.Next.OutputTransforms.LetColumns.NewColName, "newField")
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

func Test_multilineQuery(t *testing.T) {
	query := []byte(`A=1
	|
	regex
	B="^\d$"

	`)
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

func Test_MakeMVWithOnlyField(t *testing.T) {
	query := `* | makemv senders`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest)
	assert.Equal(t, "makemv", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.Command)
	assert.Equal(t, "senders", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.ColName)
	assert.Equal(t, " ", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.DelimiterString)
}

func Test_MakeMVWithFieldAndDelimiter(t *testing.T) {
	query := `* | makemv delim="," senders`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest)
	assert.Equal(t, "makemv", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.Command)
	assert.Equal(t, "senders", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.ColName)
	assert.Equal(t, ",", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.DelimiterString)
	assert.False(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest.IsRegex)
}

func Test_MakeMVWithFieldAndDelimiterAndSetSv(t *testing.T) {
	query := `* | makemv setsv=true delim="," senders`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest)
	assert.Equal(t, "makemv", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.Command)
	assert.Equal(t, "senders", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.ColName)
	assert.Equal(t, ",", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.DelimiterString)
	assert.False(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest.IsRegex)
	assert.True(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest.Setsv)
}

func Test_MakeMVWithFieldAndRegexDelimiterAndSetSv(t *testing.T) {
	query := `* | makemv tokenizer="([^,]+),?" setsv=true senders`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest)
	assert.Equal(t, "makemv", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.Command)
	assert.Equal(t, "senders", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.ColName)
	assert.Equal(t, "([^,]+),?", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.DelimiterString)
	assert.True(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest.IsRegex)
	assert.True(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest.Setsv)
}

func Test_MakeMVWithFieldAndDelimiterPlusAllowEmpty(t *testing.T) {
	query := `* | makemv delim="," allowempty=true senders`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest)
	assert.Equal(t, "makemv", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.Command)
	assert.Equal(t, "senders", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.ColName)
	assert.Equal(t, ",", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.DelimiterString)
	assert.False(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest.IsRegex)
	assert.True(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest.AllowEmpty)

	query = `* | makemv allowempty=true setsv=true delim="," senders`
	res, err = spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode = res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err = pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest)
	assert.Equal(t, "makemv", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.Command)
	assert.Equal(t, "senders", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.ColName)
	assert.Equal(t, ",", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.DelimiterString)
	assert.False(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest.IsRegex)
	assert.True(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest.AllowEmpty)
	assert.True(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest.Setsv)
}

func Test_SPath_Input_Output_dataPath(t *testing.T) {
	query := `* | spath input=rawjson output=user user.name`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, structs.ValueExprMode(structs.VEMStringExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.Equal(t, structs.StringExprMode(structs.SEMTextExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.StringExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, "spath", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr)
	assert.Equal(t, "rawjson", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.InputColName)
	assert.Equal(t, "user", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.OutputColName)
	assert.Equal(t, "user.name", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.Path)
	assert.Equal(t, "user", aggregator.OutputTransforms.LetColumns.NewColName)
}

func Test_SPath_Input_Output_Path(t *testing.T) {
	query := `* | spath input=rawjson output=user path="user.name"`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, structs.ValueExprMode(structs.VEMStringExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.Equal(t, structs.StringExprMode(structs.SEMTextExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.StringExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, "spath", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr)
	assert.Equal(t, "rawjson", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.InputColName)
	assert.Equal(t, "user", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.OutputColName)
	assert.Equal(t, "user.name", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.Path)
	assert.Equal(t, "user", aggregator.OutputTransforms.LetColumns.NewColName)

	query = `* | spath input=rawjson output=user path = "user.name"`
	res, err = spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode = res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err = pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, structs.ValueExprMode(structs.VEMStringExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.Equal(t, structs.StringExprMode(structs.SEMTextExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.StringExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, "spath", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr)
	assert.Equal(t, "rawjson", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.InputColName)
	assert.Equal(t, "user", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.OutputColName)
	assert.Equal(t, "user.name", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.Path)
	assert.Equal(t, "user", aggregator.OutputTransforms.LetColumns.NewColName)
}

func Test_SPath_NoArgs(t *testing.T) {
	query := `* | spath`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, structs.ValueExprMode(structs.VEMStringExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.Equal(t, structs.StringExprMode(structs.SEMTextExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.StringExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, "spath", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr)
	assert.Equal(t, "_raw", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.InputColName)
}

func Test_SPath_Input_Path(t *testing.T) {
	query := `* | spath input=rawjson path="user.name"`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, structs.ValueExprMode(structs.VEMStringExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.Equal(t, structs.StringExprMode(structs.SEMTextExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.StringExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, "spath", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr)
	assert.Equal(t, "rawjson", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.InputColName)
	assert.Equal(t, "user.name", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.Path)
}

func Test_SPath_Output_dataPath(t *testing.T) {
	query := `* | spath output=user user.name`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, structs.ValueExprMode(structs.VEMStringExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.Equal(t, structs.StringExprMode(structs.SEMTextExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.StringExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, "spath", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr)
	assert.Equal(t, "_raw", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.InputColName)
	assert.Equal(t, "user", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.OutputColName)
	assert.Equal(t, "user.name", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.Path)
	assert.Equal(t, "user", aggregator.OutputTransforms.LetColumns.NewColName)
}

func Test_SPath_Output_Path_With_WildCards_Quoted(t *testing.T) {
	query := `* | spath output=myfield path="vendorProductSet.product{}.locDesc"`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, structs.ValueExprMode(structs.VEMStringExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.Equal(t, structs.StringExprMode(structs.SEMTextExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.StringExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, "spath", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr)
	assert.Equal(t, "_raw", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.InputColName)
	assert.Equal(t, "myfield", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.OutputColName)
	assert.Equal(t, "vendorProductSet.product{}.locDesc", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.Path)
	assert.Equal(t, "myfield", aggregator.OutputTransforms.LetColumns.NewColName)
}

func Test_SPath_Output_dataPath_With_WildCards_Unquoted_v1(t *testing.T) {
	query := `* | spath output=myfield vendorProductSet.product{1}.locDesc`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, structs.ValueExprMode(structs.VEMStringExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.Equal(t, structs.StringExprMode(structs.SEMTextExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.StringExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, "spath", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr)
	assert.Equal(t, "_raw", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.InputColName)
	assert.Equal(t, "myfield", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.OutputColName)
	assert.Equal(t, "vendorProductSet.product{1}.locDesc", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.Path)
	assert.Equal(t, "myfield", aggregator.OutputTransforms.LetColumns.NewColName)
}

func Test_SPath_Output_dataPath_With_WildCards_Unquoted_v2(t *testing.T) {
	query := `* | spath output=myfield vendorProductSet.product{@year}.locDesc`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, structs.ValueExprMode(structs.VEMStringExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.Equal(t, structs.StringExprMode(structs.SEMTextExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.StringExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, "spath", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr)
	assert.Equal(t, "_raw", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.InputColName)
	assert.Equal(t, "myfield", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.OutputColName)
	assert.Equal(t, "vendorProductSet.product{@year}.locDesc", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.Path)
	assert.Equal(t, "myfield", aggregator.OutputTransforms.LetColumns.NewColName)
}

func Test_SPath_In_Eval_PathIsFieldName(t *testing.T) {
	query := `* | eval locDesc=spath(_raw, locDesc)`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, structs.ValueExprMode(structs.VEMStringExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.Equal(t, structs.StringExprMode(structs.SEMTextExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.StringExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, "spath", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr)
	assert.Equal(t, "_raw", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.InputColName)
	assert.Equal(t, "locDesc", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.Path)
	assert.True(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.IsPathFieldName)
	assert.Equal(t, "locDesc", aggregator.OutputTransforms.LetColumns.NewColName)
}

func Test_SPath_In_Eval_IsPath(t *testing.T) {
	query := `* | eval hashtags=spath(_raw, "entities.hashtags")`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, structs.ValueExprMode(structs.VEMStringExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.Equal(t, structs.StringExprMode(structs.SEMTextExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.StringExprMode)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, "spath", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr)
	assert.Equal(t, "_raw", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.InputColName)
	assert.Equal(t, "entities.hashtags", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.Path)
	assert.False(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.IsPathFieldName)
	assert.Equal(t, "hashtags", aggregator.OutputTransforms.LetColumns.NewColName)
}

func Test_Spath_Extract_PathTest(t *testing.T) {
	queries := []string{
		`* | spath pathApple`,
		`* | spath path= pathApple`,
		`* | spath path = pathApple`,
		`* | spath path =pathApple`,
	}

	for _, query := range queries {
		res, err := spl.Parse("", []byte(query))
		assert.Nil(t, err)
		filterNode := res.(ast.QueryStruct).SearchFilter
		assert.NotNil(t, filterNode)

		astNpde, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
		assert.Nil(t, err)
		assert.NotNil(t, astNpde)
		assert.NotNil(t, aggregator)
		assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
		assert.NotNil(t, aggregator.OutputTransforms)
		assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
		assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest)
		assert.Equal(t, structs.ValueExprMode(structs.VEMStringExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode)
		assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
		assert.Equal(t, structs.StringExprMode(structs.SEMTextExpr), aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.StringExprMode)
		assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
		assert.Equal(t, "spath", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op)
		assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr)
		assert.Equal(t, "_raw", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.InputColName)
		assert.Equal(t, "pathApple", aggregator.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.SPathExpr.Path)
	}
}

func Test_Format_cmd_No_Argumenst(t *testing.T) {
	query := `* | format`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNpde, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNpde)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "search", aggregator.OutputTransforms.LetColumns.NewColName)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.FormatResults)
	assert.Equal(t, "OR", aggregator.OutputTransforms.LetColumns.FormatResults.MVSeparator)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.LetColumns.FormatResults.MaxResults)
	assert.Equal(t, "NOT()", aggregator.OutputTransforms.LetColumns.FormatResults.EmptyString)

	rowColOptions := aggregator.OutputTransforms.LetColumns.FormatResults.RowColOptions

	assert.NotNil(t, rowColOptions)
	assert.Equal(t, "(", rowColOptions.RowPrefix)
	assert.Equal(t, "(", rowColOptions.ColumnPrefix)
	assert.Equal(t, "AND", rowColOptions.ColumnSeparator)
	assert.Equal(t, ")", rowColOptions.ColumnEnd)
	assert.Equal(t, "OR", rowColOptions.RowSeparator)
	assert.Equal(t, ")", rowColOptions.RowEnd)
}

func Test_Format_cmd_Custom_Row_Col_Options(t *testing.T) {
	query := `* | format "[" "[" "&&" "]" "||" "]"`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "search", aggregator.OutputTransforms.LetColumns.NewColName)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.FormatResults)
	assert.Equal(t, "OR", aggregator.OutputTransforms.LetColumns.FormatResults.MVSeparator)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.LetColumns.FormatResults.MaxResults)
	assert.Equal(t, "NOT()", aggregator.OutputTransforms.LetColumns.FormatResults.EmptyString)

	rowColOptions := aggregator.OutputTransforms.LetColumns.FormatResults.RowColOptions

	assert.NotNil(t, rowColOptions)
	assert.Equal(t, "[", rowColOptions.RowPrefix)
	assert.Equal(t, "[", rowColOptions.ColumnPrefix)
	assert.Equal(t, "&&", rowColOptions.ColumnSeparator)
	assert.Equal(t, "]", rowColOptions.ColumnEnd)
	assert.Equal(t, "||", rowColOptions.RowSeparator)
	assert.Equal(t, "]", rowColOptions.RowEnd)
}

func Test_Format_cmd_MVSeparator_And_Custom_Row_Col_Options(t *testing.T) {
	query := `* | format mvsep="mvseparator" "{" "[" "AND" "]" "AND" "}"`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "search", aggregator.OutputTransforms.LetColumns.NewColName)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.FormatResults)
	assert.Equal(t, "mvseparator", aggregator.OutputTransforms.LetColumns.FormatResults.MVSeparator)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.LetColumns.FormatResults.MaxResults)
	assert.Equal(t, "NOT()", aggregator.OutputTransforms.LetColumns.FormatResults.EmptyString)

	rowColOptions := aggregator.OutputTransforms.LetColumns.FormatResults.RowColOptions

	assert.NotNil(t, rowColOptions)
	assert.Equal(t, "{", rowColOptions.RowPrefix)
	assert.Equal(t, "[", rowColOptions.ColumnPrefix)
	assert.Equal(t, "AND", rowColOptions.ColumnSeparator)
	assert.Equal(t, "]", rowColOptions.ColumnEnd)
	assert.Equal(t, "AND", rowColOptions.RowSeparator)
	assert.Equal(t, "}", rowColOptions.RowEnd)
}

func Test_Format_cmd_EmptyStr_Option(t *testing.T) {
	query := `* | format emptystr="Error Found"`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "search", aggregator.OutputTransforms.LetColumns.NewColName)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.FormatResults)
	assert.Equal(t, "OR", aggregator.OutputTransforms.LetColumns.FormatResults.MVSeparator)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.LetColumns.FormatResults.MaxResults)
	assert.Equal(t, "Error Found", aggregator.OutputTransforms.LetColumns.FormatResults.EmptyString)

	rowColOptions := aggregator.OutputTransforms.LetColumns.FormatResults.RowColOptions

	assert.NotNil(t, rowColOptions)
	assert.Equal(t, "(", rowColOptions.RowPrefix)
	assert.Equal(t, "(", rowColOptions.ColumnPrefix)
	assert.Equal(t, "AND", rowColOptions.ColumnSeparator)
	assert.Equal(t, ")", rowColOptions.ColumnEnd)
	assert.Equal(t, "OR", rowColOptions.RowSeparator)
	assert.Equal(t, ")", rowColOptions.RowEnd)
}

func Test_Format_cmd_MaxResults(t *testing.T) {
	query := `* | format maxresults=5`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "search", aggregator.OutputTransforms.LetColumns.NewColName)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.FormatResults)
	assert.Equal(t, "OR", aggregator.OutputTransforms.LetColumns.FormatResults.MVSeparator)
	assert.Equal(t, uint64(5), aggregator.OutputTransforms.LetColumns.FormatResults.MaxResults)
	assert.Equal(t, "NOT()", aggregator.OutputTransforms.LetColumns.FormatResults.EmptyString)

	rowColOptions := aggregator.OutputTransforms.LetColumns.FormatResults.RowColOptions

	assert.NotNil(t, rowColOptions)
	assert.Equal(t, "(", rowColOptions.RowPrefix)
	assert.Equal(t, "(", rowColOptions.ColumnPrefix)
	assert.Equal(t, "AND", rowColOptions.ColumnSeparator)
	assert.Equal(t, ")", rowColOptions.ColumnEnd)
	assert.Equal(t, "OR", rowColOptions.RowSeparator)
	assert.Equal(t, ")", rowColOptions.RowEnd)
}

func Test_Format_cmd_Custom_MVSeparator(t *testing.T) {
	query := `* | format mvsep=";"`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "search", aggregator.OutputTransforms.LetColumns.NewColName)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.FormatResults)
	assert.Equal(t, ";", aggregator.OutputTransforms.LetColumns.FormatResults.MVSeparator)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.LetColumns.FormatResults.MaxResults)
	assert.Equal(t, "NOT()", aggregator.OutputTransforms.LetColumns.FormatResults.EmptyString)

	rowColOptions := aggregator.OutputTransforms.LetColumns.FormatResults.RowColOptions

	assert.NotNil(t, rowColOptions)
	assert.Equal(t, "(", rowColOptions.RowPrefix)
	assert.Equal(t, "(", rowColOptions.ColumnPrefix)
	assert.Equal(t, "AND", rowColOptions.ColumnSeparator)
	assert.Equal(t, ")", rowColOptions.ColumnEnd)
	assert.Equal(t, "OR", rowColOptions.RowSeparator)
	assert.Equal(t, ")", rowColOptions.RowEnd)
}

func Test_Format_cmd_Custom_EmptyStr_MaxResults(t *testing.T) {
	query := `* | format emptystr="No Results" maxresults=3`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "search", aggregator.OutputTransforms.LetColumns.NewColName)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.FormatResults)
	assert.Equal(t, "OR", aggregator.OutputTransforms.LetColumns.FormatResults.MVSeparator)
	assert.Equal(t, uint64(3), aggregator.OutputTransforms.LetColumns.FormatResults.MaxResults)
	assert.Equal(t, "No Results", aggregator.OutputTransforms.LetColumns.FormatResults.EmptyString)

	rowColOptions := aggregator.OutputTransforms.LetColumns.FormatResults.RowColOptions

	assert.NotNil(t, rowColOptions)
	assert.Equal(t, "(", rowColOptions.RowPrefix)
	assert.Equal(t, "(", rowColOptions.ColumnPrefix)
	assert.Equal(t, "AND", rowColOptions.ColumnSeparator)
	assert.Equal(t, ")", rowColOptions.ColumnEnd)
	assert.Equal(t, "OR", rowColOptions.RowSeparator)
	assert.Equal(t, ")", rowColOptions.RowEnd)
}

func Test_Format_cmd_All_Arguments(t *testing.T) {
	query := `* | format mvsep="|" maxresults=10 "[" "{" "&&" "}" "||" "]" emptystr="Empty"`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "search", aggregator.OutputTransforms.LetColumns.NewColName)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.FormatResults)
	assert.Equal(t, "|", aggregator.OutputTransforms.LetColumns.FormatResults.MVSeparator)
	assert.Equal(t, uint64(10), aggregator.OutputTransforms.LetColumns.FormatResults.MaxResults)
	assert.Equal(t, "Empty", aggregator.OutputTransforms.LetColumns.FormatResults.EmptyString)

	rowColOptions := aggregator.OutputTransforms.LetColumns.FormatResults.RowColOptions

	assert.NotNil(t, rowColOptions)
	assert.Equal(t, "[", rowColOptions.RowPrefix)
	assert.Equal(t, "{", rowColOptions.ColumnPrefix)
	assert.Equal(t, "&&", rowColOptions.ColumnSeparator)
	assert.Equal(t, "}", rowColOptions.ColumnEnd)
	assert.Equal(t, "||", rowColOptions.RowSeparator)
	assert.Equal(t, "]", rowColOptions.RowEnd)
}

func Test_Format_cmd_Incomplete_RowCol_Options(t *testing.T) {
	query := `* | format "[" "[" "&&"`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_CalculateRelativeTime_1(t *testing.T) {
	currTime := time.Date(2024, time.June, 5, 13, 37, 5, 0, time.Local)
	tm := ast.TimeModifier{
		RelativeTime: ast.RelativeTimeModifier{
			Snap: "w0",
		},
	}
	epoch, err := spl.CalculateRelativeTime(tm, currTime)
	assert.Nil(t, err)
	// Sunday
	expectedEpoch := time.Date(2024, time.June, 2, 0, 0, 0, 0, time.Local).UnixMilli()
	assert.Equal(t, expectedEpoch, epoch)
}

func Test_CalculateRelativeTime_2(t *testing.T) {
	currTime := time.Date(2024, time.June, 5, 13, 37, 5, 0, time.Local)
	tm := ast.TimeModifier{
		RelativeTime: ast.RelativeTimeModifier{
			RelativeTimeOffset: ast.RelativeTimeOffset{
				Offset:   -1,
				TimeUnit: utils.TMHour,
			},
			Snap: strconv.Itoa(int(utils.TMHour)),
		},
	}
	epoch, err := spl.CalculateRelativeTime(tm, currTime)
	assert.Nil(t, err)

	expectedEpoch := time.Date(2024, time.June, 5, 12, 0, 0, 0, time.Local).UnixMilli()
	assert.Equal(t, expectedEpoch, epoch)
}

func Test_CalculateRelativeTime_3(t *testing.T) {
	currTime := time.Date(2024, time.June, 5, 13, 37, 5, 0, time.Local)
	tm := ast.TimeModifier{
		RelativeTime: ast.RelativeTimeModifier{
			RelativeTimeOffset: ast.RelativeTimeOffset{
				Offset:   -24,
				TimeUnit: utils.TMHour,
			},
		},
	}
	epoch, err := spl.CalculateRelativeTime(tm, currTime)
	assert.Nil(t, err)

	expectedEpoch := time.Date(2024, time.June, 4, 13, 37, 5, 0, time.Local).UnixMilli()
	assert.Equal(t, expectedEpoch, epoch)
}

func Test_CalculateRelativeTime_4(t *testing.T) {
	currTime := time.Date(2024, time.June, 5, 13, 37, 5, 0, time.Local)
	tm := ast.TimeModifier{
		RelativeTime: ast.RelativeTimeModifier{
			RelativeTimeOffset: ast.RelativeTimeOffset{
				Offset:   -1,
				TimeUnit: utils.TMYear,
			},
			Snap: "w0",
		},
	}

	epoch, err := spl.CalculateRelativeTime(tm, currTime)
	assert.Nil(t, err)
	// Sunday
	expectedEpoch := time.Date(2023, time.June, 4, 0, 0, 0, 0, time.Local).UnixMilli()
	assert.Equal(t, expectedEpoch, epoch)
}

func Test_CalculateRelativeTime_5(t *testing.T) {
	currTime := time.Date(2024, time.June, 5, 13, 37, 5, 0, time.Local)
	tm := ast.TimeModifier{
		RelativeTime: ast.RelativeTimeModifier{
			Snap: "w3",
		},
	}
	epoch, err := spl.CalculateRelativeTime(tm, currTime)
	assert.Nil(t, err)
	// Wednesday
	expectedEpoch := time.Date(2024, time.June, 5, 0, 0, 0, 0, time.Local).UnixMilli()
	assert.Equal(t, expectedEpoch, epoch)
}

func Test_CalculateRelativeTime_6(t *testing.T) {
	currTime := time.Date(2024, time.June, 5, 13, 37, 5, 0, time.Local)
	tm := ast.TimeModifier{
		RelativeTime: ast.RelativeTimeModifier{
			RelativeTimeOffset: ast.RelativeTimeOffset{
				Offset:   -7,
				TimeUnit: utils.TMDay,
			},
			Snap: strconv.Itoa(int(utils.TMMinute)),
		},
	}
	epoch, err := spl.CalculateRelativeTime(tm, currTime)
	assert.Nil(t, err)

	expectedEpoch := time.Date(2024, time.May, 29, 13, 37, 0, 0, time.Local).UnixMilli()
	assert.Equal(t, expectedEpoch, epoch)
}

func Test_CalculateRelativeTime_7(t *testing.T) {
	currTime := time.Date(2024, time.June, 5, 13, 37, 5, 0, time.Local)
	tm := ast.TimeModifier{
		RelativeTime: ast.RelativeTimeModifier{
			RelativeTimeOffset: ast.RelativeTimeOffset{
				Offset:   -1,
				TimeUnit: utils.TMYear,
			},
			Snap: strconv.Itoa(int(utils.TMYear)),
		},
	}
	epoch, err := spl.CalculateRelativeTime(tm, currTime)
	assert.Nil(t, err)

	expectedEpoch := time.Date(2023, time.January, 1, 0, 0, 0, 0, time.Local).UnixMilli()
	assert.Equal(t, expectedEpoch, epoch)
}

func Test_CalculateRelativeTime_8(t *testing.T) {
	currTime := time.Date(2024, time.June, 5, 13, 37, 5, 0, time.Local)
	tm := ast.TimeModifier{
		RelativeTime: ast.RelativeTimeModifier{
			Snap: strconv.Itoa(int(utils.TMQuarter)),
		},
	}
	epoch, err := spl.CalculateRelativeTime(tm, currTime)
	assert.Nil(t, err)

	expectedEpoch := time.Date(2024, time.April, 1, 0, 0, 0, 0, time.Local).UnixMilli()
	assert.Equal(t, expectedEpoch, epoch)
}

func Test_CalculateRelativeTime_9(t *testing.T) {
	currTime := time.Date(2024, time.June, 5, 13, 37, 5, 0, time.Local)
	tm := ast.TimeModifier{
		RelativeTime: ast.RelativeTimeModifier{
			RelativeTimeOffset: ast.RelativeTimeOffset{
				Offset:   -1,
				TimeUnit: utils.TMWeek,
			},
			Snap: strconv.Itoa(int(utils.TMMonth)),
		},
	}
	epoch, err := spl.CalculateRelativeTime(tm, currTime)
	assert.Nil(t, err)

	expectedEpoch := time.Date(2024, time.May, 1, 0, 0, 0, 0, time.Local).UnixMilli()
	assert.Equal(t, expectedEpoch, epoch)
}

func Test_CalculateRelativeTime_10(t *testing.T) {
	currTime := time.Date(2024, time.June, 5, 13, 37, 5, 0, time.Local)
	tm := ast.TimeModifier{
		RelativeTime: ast.RelativeTimeModifier{
			RelativeTimeOffset: ast.RelativeTimeOffset{
				Offset:   -1,
				TimeUnit: utils.TMQuarter,
			},
			Snap: strconv.Itoa(int(utils.TMMonth)),
		},
	}
	epoch, err := spl.CalculateRelativeTime(tm, currTime)
	assert.Nil(t, err)

	expectedEpoch := time.Date(2024, time.February, 1, 0, 0, 0, 0, time.Local).UnixMilli()
	assert.Equal(t, expectedEpoch, epoch)
}

func Test_CalculateRelativeTime_11(t *testing.T) {
	currTime := time.Date(2024, time.June, 5, 13, 37, 5, 0, time.Local)
	tm := ast.TimeModifier{
		AbsoluteTime: "06/19/2024:18:55:00",
	}
	epoch, err := spl.CalculateRelativeTime(tm, currTime)
	assert.Nil(t, err)

	expectedEpoch := time.Date(2024, time.June, 19, 18, 55, 0, 0, time.Local).UnixMilli()
	assert.Equal(t, expectedEpoch, epoch)
}

func Test_CalculateRelativeTime_12(t *testing.T) {
	currTime := time.Date(2024, time.June, 5, 13, 37, 5, 0, time.Local)
	tm := ast.TimeModifier{
		AbsoluteTime: "1",
	}
	epoch, err := spl.CalculateRelativeTime(tm, currTime)
	assert.Nil(t, err)
	// Epoch 1: January 1, 1970 12:00:01 AM UTC
	assert.Equal(t, int64(1), epoch)
}

func Test_ParseRelativeTimeModifier_1(t *testing.T) {
	query := `* | earliest=+1d@w3 latest=+1d@d`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")

	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, astNode.TimeRange)
}

func Test_ParseRelativeTimeModifier_2(t *testing.T) {
	query := `* | earliest=+1d@w3 latest=+1d@d`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")

	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, astNode.TimeRange)
}

func Test_ParseRelativeTimeModifier_3(t *testing.T) {
	query := `* | earliest=-1d@w4 latest=+1mon`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")

	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, astNode.TimeRange)

	expEndTime := time.Now().AddDate(0, 1, 0).UnixMilli()
	endTimeDiff := expEndTime - int64(astNode.TimeRange.EndEpochMs)
	assert.True(t, endTimeDiff <= int64(1000))
}

func Test_ParseRelativeTimeModifier_4(t *testing.T) {
	query := `* | earliest=06/19/2024:18:55:00 latest=+24h`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")

	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, astNode.TimeRange)

	expStartTime := time.Date(2024, time.June, 19, 18, 55, 0, 0, time.Local).UnixMilli()
	assert.Equal(t, expStartTime, int64(astNode.TimeRange.StartEpochMs))

	expEndTime := time.Now().Add(24 * time.Hour).UnixMilli()
	endTimeDiff := expEndTime - int64(astNode.TimeRange.EndEpochMs)
	assert.True(t, endTimeDiff <= int64(1000))
}

func Test_ParseRelativeTimeModifier_5(t *testing.T) {
	query := `* | earliest=06/19/2024:18:55:00 latest=06/20/2024:18:55:00`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")

	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, astNode.TimeRange)

	expStartTime := time.Date(2024, time.June, 19, 18, 55, 0, 0, time.Local).UnixMilli()
	assert.Equal(t, expStartTime, int64(astNode.TimeRange.StartEpochMs))

	expEndTime := time.Date(2024, time.June, 20, 18, 55, 0, 0, time.Local).UnixMilli()
	assert.Equal(t, expEndTime, int64(astNode.TimeRange.EndEpochMs))
}

func Test_ParseRelativeTimeModifier_6(t *testing.T) {
	query := `* | earliest=-month@year latest=-2days@minute`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")

	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, astNode.TimeRange)
}

func Test_ParseRelativeTimeModifier_7(t *testing.T) {
	query := `* | latest=-2days@minute`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_ParseRelativeTimeModifier_8(t *testing.T) {
	query := `* | earliest=-60m`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")

	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, astNode.TimeRange)

	expStartTime := time.Now().Add(-60 * time.Minute).UnixMilli()
	timeDiff := expStartTime - int64(astNode.TimeRange.StartEpochMs)
	assert.True(t, timeDiff <= int64(1000))

	expEndTime := time.Now().UnixMilli()
	endTimeDiff := expEndTime - int64(astNode.TimeRange.EndEpochMs)
	assert.True(t, endTimeDiff <= int64(1000))
}

func Test_ParseRelativeTimeModifier_9(t *testing.T) {
	query := `address = "4852 Lake Ridge port, Santa Ana, Nebraska 13548" | earliest=-week@mon latest=@s`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")

	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, astNode.TimeRange)
}

func Test_ParseRelativeTimeModifier_10(t *testing.T) {
	query := `city = Boston | earliest=@w5 latest=@w0`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")

	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, astNode.TimeRange)
}

func Test_ParseRelativeTimeModifier_11(t *testing.T) {
	query := `* | earliest=-1y latest=-1quarter`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")

	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, astNode.TimeRange)

	expStartTime := time.Now().AddDate(-1, 0, 0).UnixMilli()
	timeDiff := expStartTime - int64(astNode.TimeRange.StartEpochMs)
	assert.True(t, timeDiff <= int64(1000))

	expEndTime := time.Now().AddDate(0, -4, 0).UnixMilli()
	endTimeDiff := expEndTime - int64(astNode.TimeRange.EndEpochMs)
	assert.True(t, endTimeDiff <= int64(1000))
}

func Test_ParseRelativeTimeModifier_12(t *testing.T) {
	query := `* | earliest=-2mon latest=-1day`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")

	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, astNode.TimeRange)

	expStartTime := time.Now().AddDate(0, -2, 0).UnixMilli()
	timeDiff := expStartTime - int64(astNode.TimeRange.StartEpochMs)
	assert.True(t, timeDiff <= int64(1000))

	expEndTime := time.Now().AddDate(0, 0, -1).UnixMilli()
	endTimeDiff := expEndTime - int64(astNode.TimeRange.EndEpochMs)
	assert.True(t, endTimeDiff <= int64(1000))
}

func Test_ParseRelativeTimeModifier_13(t *testing.T) {
	query := `* | earliest=1ms`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_EventCount_Defaults(t *testing.T) {
	query := `* | eventcount`
	performEventCountTest(t, query, []string{"*"}, true, false, true)
}

func Test_EventCount_IndexSpecified(t *testing.T) {
	query := `* | eventcount index=my_index`
	performEventCountTest(t, query, []string{"my_index"}, true, false, true)
}

func Test_EventCount_SummarizeFalse(t *testing.T) {
	query := `* | eventcount summarize=false`
	performEventCountTest(t, query, []string{"*"}, false, false, true)
}

func Test_EventCount_ReportSizeTrue(t *testing.T) {
	query := `* | eventcount report_size=true`
	performEventCountTest(t, query, []string{"*"}, true, true, true)
}

func Test_EventCount_ListVixFalse(t *testing.T) {
	query := `* | eventcount list_vix=false`
	performEventCountTest(t, query, []string{"*"}, true, false, false)
}

func Test_EventCount_Combination1(t *testing.T) {
	query := `* | eventcount index=my_index summarize=false report_size=true`
	performEventCountTest(t, query, []string{"my_index"}, false, true, true)
}

func Test_EventCount_Combination2(t *testing.T) {
	query := `* | eventcount index=my_index summarize=true report_size=false list_vix=false`
	performEventCountTest(t, query, []string{"my_index"}, true, false, false)
}

func Test_EventCount_Combination3(t *testing.T) {
	query := `* | eventcount report_size=true index=my_index summarize=false list_vix=false`
	performEventCountTest(t, query, []string{"my_index"}, false, true, false)
}

func Test_EventCount_Combination4(t *testing.T) {
	query := `* | eventcount list_vix=false report_size=true index=my_index summarize=false`
	performEventCountTest(t, query, []string{"my_index"}, false, true, false)
}

func Test_EventCount_Combination5(t *testing.T) {
	query := `* | eventcount summarize=false list_vix=false report_size=true index=my_index`
	performEventCountTest(t, query, []string{"my_index"}, false, true, false)
}

func Test_EventCount_MultipleIndices1(t *testing.T) {
	query := `* | eventcount index=my_index index=my_index2 list_vix=false`
	performEventCountTest(t, query, []string{"my_index", "my_index2"}, true, false, false)
}

func Test_EventCount_MultipleIndices2(t *testing.T) {
	query := `* | eventcount index=my_index index=my_index2 summarize=false index=ind-0`
	performEventCountTest(t, query, []string{"my_index", "my_index2", "ind-0"}, false, false, true)
}

// This helper function encapsulates the common test logic for eventcount command
func performEventCountTest(t *testing.T, query string, expectedIndices []string, expectedSummarize bool, expectedReportSize bool, expectedListVix bool) {
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.EventCountRequest)
	assert.Equal(t, expectedIndices, aggregator.OutputTransforms.LetColumns.EventCountRequest.Indices)
	assert.Equal(t, expectedSummarize, aggregator.OutputTransforms.LetColumns.EventCountRequest.Summarize)
	assert.Equal(t, expectedReportSize, aggregator.OutputTransforms.LetColumns.EventCountRequest.ReportSize)
	assert.Equal(t, expectedListVix, aggregator.OutputTransforms.LetColumns.EventCountRequest.ListVix)
}

func performSearchMatchCheck(t *testing.T, query string, expectedFields []string, searchStr string) {
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, aggregator.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.NewColName, "n")
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMConditionExpr)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr)
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.Op, "if")
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr)
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.ValueOp, "searchmatch")
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.IsTerminal, true)
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.StringExpr.RawString, searchStr)
	assert.True(t, putils.CompareStringSlices(expectedFields, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.LeftValue.StringExpr.FieldList))
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.BoolExpr.RightValue)

	assert.Equal(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.TrueValue.StringExpr.RawString, "yes")
	assert.Equal(t, aggregator.OutputTransforms.LetColumns.ValueColRequest.ConditionExpr.FalseValue.StringExpr.RawString, "no")
}

func Test_SearchMatch_1(t *testing.T) {
	query := `* | eval n = if(searchmatch("city=abc"), "yes", "no")`
	expectedFields := []string{"city"}
	rawStr := "city=abc"
	performSearchMatchCheck(t, query, expectedFields, rawStr)
}

func Test_SearchMatch_2(t *testing.T) {
	query := `app_name = "Bracecould" | eval n = if(searchmatch("city=abc address=*"), "yes", "no")`
	expectedFields := []string{"city", "address"}
	rawStr := "city=abc address=*"
	performSearchMatchCheck(t, query, expectedFields, rawStr)
}

func Test_SearchMatch_3(t *testing.T) {
	query := `city=Boston | eval n = if(searchmatch("x=abc address=123"), "yes", "no")`
	expectedFields := []string{"x", "address"}
	rawStr := "x=abc address=123"
	performSearchMatchCheck(t, query, expectedFields, rawStr)
}

func Test_SearchMatch_4(t *testing.T) {
	query := `* | eval n = if(searchmatch("*"), "yes", "no")`
	expectedFields := []string{"*"}
	rawStr := "*"
	performSearchMatchCheck(t, query, expectedFields, rawStr)
}

func Test_SearchMatch_5(t *testing.T) {
	query := `city=Boston | eval n = if(searchmatch("  *  "), "yes", "no")`
	expectedFields := []string{"*"}
	rawStr := "  *  "
	performSearchMatchCheck(t, query, expectedFields, rawStr)
}

func Test_SearchMatch_6(t *testing.T) {
	query := `* | eval n = if(searchmatch("abc"), "yes", "no")`
	expectedFields := []string{"*"}
	rawStr := "abc"
	performSearchMatchCheck(t, query, expectedFields, rawStr)
}

func Test_SearchMatch_7(t *testing.T) {
	query := `* | eval n = if(searchmatch("first_name=A* last_name=B?"), "yes", "no")`
	expectedFields := []string{"first_name", "last_name"}
	rawStr := "first_name=A* last_name=B?"
	performSearchMatchCheck(t, query, expectedFields, rawStr)
}

func Test_SearchMatch_8(t *testing.T) {
	query := `* | eval n = if(searchmatch("a b"), "yes", "no")`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_SearchMatch_9(t *testing.T) {
	query := `* | eval n = if(searchmatch("   "), "yes", "no")`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_SearchMatch_10(t *testing.T) {
	query := `* | eval n = if(searchmatch(""), "yes", "no")`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_SearchMatch_11(t *testing.T) {
	query := `* | eval n = if(searchmatch(123), "yes", "no")`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_tail(t *testing.T) {
	query := []byte(`city=Boston | tail`)
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
	assert.NotNil(t, aggregator.OutputTransforms.TailRequest)
	assert.Equal(t, aggregator.OutputTransforms.TailRequest.TailRows, uint64(10)) // This is the SPL default when no value is given.
}

func Test_tailWithSort(t *testing.T) {
	query := []byte(`city=Boston | sort batch | tail`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.Nil(t, aggregator.Next.Next)

	assert.Equal(t, aggregator.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.OutputTransforms.TailRequest)
	assert.Equal(t, aggregator.Next.OutputTransforms.TailRequest.TailRows, uint64(10)) // This is the SPL default when no value is given.
}

func Test_tailWithNumber(t *testing.T) {
	query := []byte(`city=Boston | tail 7`)
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
	assert.NotNil(t, aggregator.OutputTransforms.TailRequest)
	assert.Equal(t, aggregator.OutputTransforms.TailRequest.TailRows, uint64(7))
}

func Test_tailWithSortAndNumber(t *testing.T) {
	query := []byte(`* | sort batch | tail 20`)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.Nil(t, aggregator.Next.Next)

	assert.Equal(t, aggregator.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.OutputTransforms.TailRequest)
	assert.Equal(t, aggregator.Next.OutputTransforms.TailRequest.TailRows, uint64(20)) // This is the SPL default when no value is given.
}

func Test_Head1(t *testing.T) {
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
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest)
	assert.Equal(t, uint64(10), aggregator.OutputTransforms.HeadRequest.MaxRows)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.RowsAdded)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Null)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Keeplast)
	assert.Nil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr)
}

func Test_Head2(t *testing.T) {
	query := []byte(`A=1 | head 11`)
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
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest)
	assert.Equal(t, uint64(11), aggregator.OutputTransforms.HeadRequest.MaxRows)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.RowsAdded)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Null)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Keeplast)
	assert.Nil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr)
}

func Test_Head3(t *testing.T) {
	query := []byte(`A=1 | head a=b`)
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
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.MaxRows)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.RowsAdded)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Null)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Keeplast)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr)
	assert.Equal(t, "a", aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr.IsTerminal)
	assert.Equal(t, "=", aggregator.OutputTransforms.HeadRequest.BoolExpr.ValueOp)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr)
	assert.Equal(t, "b", aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr.IsTerminal)
}

func Test_Head4(t *testing.T) {
	query := []byte(`A=1 | head (a=b)`)
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
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.MaxRows)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.RowsAdded)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Null)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Keeplast)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr)
	assert.Equal(t, "a", aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr.IsTerminal)
	assert.Equal(t, "=", aggregator.OutputTransforms.HeadRequest.BoolExpr.ValueOp)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr)
	assert.Equal(t, "b", aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr.IsTerminal)
}

func Test_Head5(t *testing.T) {
	query := []byte(`A=1 | head keeplast=true`)
	_, err := spl.Parse("", query)
	assert.NotNil(t, err)
}

func Test_Head6(t *testing.T) {
	query := []byte(`A=1 | head null=true`)
	_, err := spl.Parse("", query)
	assert.NotNil(t, err)
}

func Test_Head7(t *testing.T) {
	query := []byte(`A=1 | head keeplast=true null=true`)
	_, err := spl.Parse("", query)
	assert.NotNil(t, err)
}

func Test_Head8(t *testing.T) {
	query := []byte(`A=1 | head 10 a=b`)
	_, err := spl.Parse("", query)
	assert.NotNil(t, err)
}

func Test_Head9(t *testing.T) {
	query := []byte(`A=1 | head a>b keeplast=true null=true`)
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
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.MaxRows)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.RowsAdded)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.Null)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.Keeplast)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr)
	assert.Equal(t, "a", aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr.IsTerminal)
	assert.Equal(t, ">", aggregator.OutputTransforms.HeadRequest.BoolExpr.ValueOp)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr)
	assert.Equal(t, "b", aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr.IsTerminal)
}

func Test_Head10(t *testing.T) {
	query := []byte(`A=1 | head (a<c) keeplast=true null=false`)
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
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.MaxRows)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.RowsAdded)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Null)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.Keeplast)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr)
	assert.Equal(t, "a", aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr.IsTerminal)
	assert.Equal(t, "<", aggregator.OutputTransforms.HeadRequest.BoolExpr.ValueOp)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr)
	assert.Equal(t, "c", aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr.IsTerminal)
}

func Test_Head11(t *testing.T) {
	query := []byte(`A=1 | head limit=12`)
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
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest)
	assert.Equal(t, uint64(12), aggregator.OutputTransforms.HeadRequest.MaxRows)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.RowsAdded)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Null)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Keeplast)
	assert.Nil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr)
}

func Test_Head12(t *testing.T) {
	query := []byte(`A=1 | head limit=200 a=1`)
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
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest)
	assert.Equal(t, uint64(200), aggregator.OutputTransforms.HeadRequest.MaxRows)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.RowsAdded)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Null)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Keeplast)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr)
	assert.Equal(t, "a", aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr.IsTerminal)
	assert.Equal(t, "=", aggregator.OutputTransforms.HeadRequest.BoolExpr.ValueOp)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr)
	assert.Equal(t, "1", aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr.IsTerminal)
}

func Test_Head13(t *testing.T) {
	query := []byte(`A=1 | head a=1 limit=50 keeplast=true`)
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
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest)
	assert.Equal(t, uint64(50), aggregator.OutputTransforms.HeadRequest.MaxRows)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.RowsAdded)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Null)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.Keeplast)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr)
	assert.Equal(t, "a", aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr.IsTerminal)
	assert.Equal(t, "=", aggregator.OutputTransforms.HeadRequest.BoolExpr.ValueOp)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr)
	assert.Equal(t, "1", aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue.NumericExpr.IsTerminal)
}

func Test_Head14(t *testing.T) {
	query := []byte(`A=1 | head limit=3 isnull(col) null=true`)
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
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest)
	assert.Equal(t, uint64(3), aggregator.OutputTransforms.HeadRequest.MaxRows)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.RowsAdded)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.Null)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Keeplast)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr)
	assert.Equal(t, "col", aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue.NumericExpr.IsTerminal)
	assert.Equal(t, "isnull", aggregator.OutputTransforms.HeadRequest.BoolExpr.ValueOp)
	assert.Nil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue)
}

func Test_Head15(t *testing.T) {
	query := []byte(`A=1 | head limit=5 a=1 OR b>2 AND c<=3 keeplast=true`)
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
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest)
	assert.Equal(t, uint64(5), aggregator.OutputTransforms.HeadRequest.MaxRows)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.RowsAdded)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Null)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.Keeplast)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr)
	assert.Nil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue)
	assert.Nil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool)
	assert.Equal(t, structs.BoolOpOr, aggregator.OutputTransforms.HeadRequest.BoolExpr.BoolOp)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.LeftValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.LeftValue.NumericExpr)
	assert.Equal(t, "a", aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, "=", aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.ValueOp)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.RightValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.RightValue.NumericExpr)
	assert.Equal(t, "1", aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.RightValue.NumericExpr.IsTerminal)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool)
	assert.Nil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.LeftValue)
	assert.Nil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.RightValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.LeftBool)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.RightBool)

	assert.Equal(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.BoolOp, structs.BoolOpAnd)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.LeftBool.LeftValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.LeftBool.LeftValue.NumericExpr)
	assert.Equal(t, "b", aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.LeftBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.LeftBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, ">", aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.LeftBool.ValueOp)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.LeftBool.RightValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.LeftBool.RightValue.NumericExpr)
	assert.Equal(t, "2", aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.LeftBool.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.LeftBool.RightValue.NumericExpr.IsTerminal)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.RightBool.LeftValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.RightBool.LeftValue.NumericExpr)
	assert.Equal(t, "c", aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.RightBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.RightBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, "<=", aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.RightBool.ValueOp)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.RightBool.RightValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.RightBool.RightValue.NumericExpr)
	assert.Equal(t, "3", aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.RightBool.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.RightBool.RightValue.NumericExpr.IsTerminal)
}

func Test_Head16(t *testing.T) {
	query := []byte(`A=1 | head col="abc" OR isbool(mycol) keeplast=true`)
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
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.MaxRows)
	assert.Equal(t, uint64(0), aggregator.OutputTransforms.HeadRequest.RowsAdded)
	assert.Equal(t, false, aggregator.OutputTransforms.HeadRequest.Null)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.Keeplast)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr)
	assert.Nil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftValue)
	assert.Nil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightValue)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool)
	assert.Equal(t, structs.BoolOpOr, aggregator.OutputTransforms.HeadRequest.BoolExpr.BoolOp)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.LeftValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.LeftValue.NumericExpr)
	assert.Equal(t, "col", aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, "=", aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.ValueOp)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.RightValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.RightValue.StringExpr)
	assert.Equal(t, "abc", aggregator.OutputTransforms.HeadRequest.BoolExpr.LeftBool.RightValue.StringExpr.RawString)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool)
	assert.NotNil(t, "isbool", aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.ValueOp)

	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.LeftValue)
	assert.NotNil(t, aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.LeftValue.NumericExpr)
	assert.Equal(t, "mycol", aggregator.OutputTransforms.HeadRequest.BoolExpr.RightBool.LeftValue.NumericExpr.Value)
}

func Test_Bin(t *testing.T) {
	query := `* | bin span=1h timestamp AS tmpStamp`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "tmpStamp", aggregator.OutputTransforms.LetColumns.NewColName)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength)
	assert.Equal(t, float64(1), aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength.Num)
	assert.Equal(t, utils.TMHour, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength.TimeScale)

	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.Start)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.End)
	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.BinRequest.Field)
	assert.Equal(t, uint64(100), aggregator.OutputTransforms.LetColumns.BinRequest.MaxBins)
}

func Test_Bin2(t *testing.T) {
	query := `* | bin minspan=1mon timestamp`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.NewColName)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan)
	assert.Equal(t, float64(1), aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan.Num)
	assert.Equal(t, utils.TMMonth, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan.TimeScale)

	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.Start)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.End)
	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.BinRequest.Field)
	assert.Equal(t, uint64(100), aggregator.OutputTransforms.LetColumns.BinRequest.MaxBins)
}

func Test_Bin3(t *testing.T) {
	query := `* | bin bins=3 timestamp`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.NewColName)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.Start)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.End)

	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.BinRequest.Field)
	assert.Equal(t, uint64(3), aggregator.OutputTransforms.LetColumns.BinRequest.MaxBins)
}

func Test_Bin4(t *testing.T) {
	query := `* | bin start=123 timestamp`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.NewColName)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.End)

	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.BinRequest.Field)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.Start)
	assert.Equal(t, float64(123), *aggregator.OutputTransforms.LetColumns.BinRequest.Start)
	assert.Equal(t, uint64(100), aggregator.OutputTransforms.LetColumns.BinRequest.MaxBins)
}

func Test_Bin5(t *testing.T) {
	query := `* | bin end=123 timestamp`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.NewColName)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.Start)

	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.BinRequest.Field)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.End)
	assert.Equal(t, float64(123), *aggregator.OutputTransforms.LetColumns.BinRequest.End)
	assert.Equal(t, uint64(100), aggregator.OutputTransforms.LetColumns.BinRequest.MaxBins)
}

func Test_Bin6(t *testing.T) {
	query := `* | bin span=123log456.123 timestamp`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.NewColName)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.Start)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.End)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.LogSpan)
	assert.Equal(t, float64(123), aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.LogSpan.Coefficient)
	assert.Equal(t, float64(456.123), aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.LogSpan.Base)

	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.BinRequest.Field)
	assert.Equal(t, uint64(100), aggregator.OutputTransforms.LetColumns.BinRequest.MaxBins)
}

func Test_Bin7(t *testing.T) {
	query := `* | bin span=123.456 minspan=100 bins=4 start=-123.456 end=456.789 aligntime=123456789 timestamp as timeStmp`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "timeStmp", aggregator.OutputTransforms.LetColumns.NewColName)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan)
	assert.Equal(t, float64(100), aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan.Num)
	assert.Equal(t, 0, int(aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan.TimeScale))

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength)
	assert.Equal(t, float64(123.456), aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength.Num)
	assert.Equal(t, utils.TMInvalid, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength.TimeScale)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.LogSpan)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.Start)
	assert.Equal(t, float64(-123.456), *aggregator.OutputTransforms.LetColumns.BinRequest.Start)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.End)
	assert.Equal(t, float64(456.789), *aggregator.OutputTransforms.LetColumns.BinRequest.End)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.AlignTime)
	assert.Equal(t, uint64(123456789), *aggregator.OutputTransforms.LetColumns.BinRequest.AlignTime)

	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.BinRequest.Field)
	assert.Equal(t, uint64(4), aggregator.OutputTransforms.LetColumns.BinRequest.MaxBins)
}

func Test_Bin8(t *testing.T) {
	query := `* | bin span=123.3d timestamp`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_Bin9(t *testing.T) {
	query := `* | bin aligntime=1234567 timestamp`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.NewColName)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.Start)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.End)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.AlignTime)
	assert.Equal(t, uint64(1234567), *aggregator.OutputTransforms.LetColumns.BinRequest.AlignTime)

	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.BinRequest.Field)
	assert.Equal(t, uint64(100), aggregator.OutputTransforms.LetColumns.BinRequest.MaxBins)
}

func Test_Bin10(t *testing.T) {
	query := `* | bin minspan=456.7mon timestamp`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_Bin11(t *testing.T) {
	query := `* | bin span=123log1 timestamp`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_Bin12(t *testing.T) {
	query := `* | bin aligntime=-24h timestamp`
	res, err := spl.Parse("", []byte(query))
	expAlignTime := time.Now().Add(-24 * time.Hour).UnixMilli()
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.NewColName)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.Start)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.End)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.AlignTime)

	alignTimeDiff := expAlignTime - int64(*aggregator.OutputTransforms.LetColumns.BinRequest.AlignTime)
	assert.True(t, alignTimeDiff <= int64(1000))

	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.BinRequest.Field)
	assert.Equal(t, uint64(100), aggregator.OutputTransforms.LetColumns.BinRequest.MaxBins)
}

func Test_Bin13(t *testing.T) {
	query := `* | bin span=123.456 minspan=100 bins=4 start=-123.456 end=456.789 aligntime=-1yr timestamp as timeStmp`
	res, err := spl.Parse("", []byte(query))
	expAlignTime := time.Now().AddDate(-1, 0, 0).UnixMilli()
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "timeStmp", aggregator.OutputTransforms.LetColumns.NewColName)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan)
	assert.Equal(t, float64(100), aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan.Num)
	assert.Equal(t, 0, int(aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan.TimeScale))

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength)
	assert.Equal(t, float64(123.456), aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength.Num)
	assert.Equal(t, utils.TMInvalid, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength.TimeScale)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.LogSpan)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.Start)
	assert.Equal(t, float64(-123.456), *aggregator.OutputTransforms.LetColumns.BinRequest.Start)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.End)
	assert.Equal(t, float64(456.789), *aggregator.OutputTransforms.LetColumns.BinRequest.End)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.AlignTime)
	alignTimeDiff := expAlignTime - int64(*aggregator.OutputTransforms.LetColumns.BinRequest.AlignTime)
	assert.True(t, alignTimeDiff <= int64(1000))

	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.BinRequest.Field)
	assert.Equal(t, uint64(4), aggregator.OutputTransforms.LetColumns.BinRequest.MaxBins)
}

func Test_Bin14(t *testing.T) {
	query := `* | bin aligntime=-1y@qtr timestamp`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.NewColName)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.Start)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.End)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.AlignTime)
	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.BinRequest.Field)
	assert.Equal(t, uint64(100), aggregator.OutputTransforms.LetColumns.BinRequest.MaxBins)
}

func Test_Bin15(t *testing.T) {
	query := `* | bin span=250.5 minspan=125.5 bins=20 start=-123 end=456.789 aligntime=-1week@mon timestamp as bin_timestamp`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "bin_timestamp", aggregator.OutputTransforms.LetColumns.NewColName)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan)
	assert.Equal(t, float64(125.5), aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan.Num)
	assert.Equal(t, 0, int(aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan.TimeScale))

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength)
	assert.Equal(t, float64(250.5), aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength.Num)
	assert.Equal(t, utils.TMInvalid, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength.TimeScale)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.LogSpan)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.Start)
	assert.Equal(t, float64(-123), *aggregator.OutputTransforms.LetColumns.BinRequest.Start)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.End)
	assert.Equal(t, float64(456.789), *aggregator.OutputTransforms.LetColumns.BinRequest.End)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.AlignTime)

	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.BinRequest.Field)
	assert.Equal(t, uint64(20), aggregator.OutputTransforms.LetColumns.BinRequest.MaxBins)
}

func Test_Bin16(t *testing.T) {
	query := `* | bin timestamp AS bin_timestamp`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "bin_timestamp", aggregator.OutputTransforms.LetColumns.NewColName)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.Start)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.End)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.AlignTime)

	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.BinRequest.Field)
	assert.Equal(t, uint64(100), aggregator.OutputTransforms.LetColumns.BinRequest.MaxBins)
}

func Test_Bin17(t *testing.T) {
	query := `* | bin span=3log2 timestamp`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_Bin18(t *testing.T) {
	query := `* | bin span=11log timestamp`
	_, err := spl.Parse("", []byte(query))
	// default value of coeff: 1 and base: 10
	assert.NotNil(t, err)
}

func Test_Bin19(t *testing.T) {
	query := `* | bin span=log abc AS bin_abc`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "bin_abc", aggregator.OutputTransforms.LetColumns.NewColName)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.Start)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.End)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.AlignTime)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.LogSpan)
	// default value coefficient: 1, base: 10
	assert.Equal(t, float64(10), aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.LogSpan.Base)
	assert.Equal(t, float64(1), aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.LogSpan.Coefficient)

	assert.Equal(t, "abc", aggregator.OutputTransforms.LetColumns.BinRequest.Field)
	assert.Equal(t, uint64(100), aggregator.OutputTransforms.LetColumns.BinRequest.MaxBins)
}

func Test_Bin20(t *testing.T) {
	query := `* | bin span=3ds timestamp`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_Bin21(t *testing.T) {
	query := `* | bin span=101cs timestamp`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_Bin22(t *testing.T) {
	query := `* | bin span=2ds timestamp AS tmpStamp`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "tmpStamp", aggregator.OutputTransforms.LetColumns.NewColName)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength)
	assert.Equal(t, float64(2), aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength.Num)
	assert.Equal(t, utils.TMDecisecond, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength.TimeScale)

	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.Start)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.End)
	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.BinRequest.Field)
	assert.Equal(t, uint64(100), aggregator.OutputTransforms.LetColumns.BinRequest.MaxBins)
}

func Test_Bin23(t *testing.T) {
	query := `* | bin span=10cs timestamp AS tmpStamp`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.Equal(t, "tmpStamp", aggregator.OutputTransforms.LetColumns.NewColName)

	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength)
	assert.Equal(t, float64(10), aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength.Num)
	assert.Equal(t, utils.TMCentisecond, aggregator.OutputTransforms.LetColumns.BinRequest.BinSpanOptions.BinSpanLength.TimeScale)

	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.MinSpan)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.Start)
	assert.Nil(t, aggregator.OutputTransforms.LetColumns.BinRequest.End)
	assert.Equal(t, "timestamp", aggregator.OutputTransforms.LetColumns.BinRequest.Field)
	assert.Equal(t, uint64(100), aggregator.OutputTransforms.LetColumns.BinRequest.MaxBins)
}

func Test_Bin24(t *testing.T) {
	query := `* | bin bins=1 timestamp`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_Bin25(t *testing.T) {
	query := `* | bin bins=1us timestamp`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_Bin26(t *testing.T) {
	query := `* | bin span=5mon timestamp`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_Bin27(t *testing.T) {
	query := `* | bin span=7q timestamp`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_Bin28(t *testing.T) {
	query := `* | bin span=-2 timestamp`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_Bin29(t *testing.T) {
	query := `* | bin span=-2s timestamp`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_StreamStats(t *testing.T) {
	query := `* | streamstats allnum=true global=true time_window=1h count(timestamp) AS tmpStamp`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.StreamStatsOptions)
	assert.Equal(t, true, aggregator.StreamStatsOptions.AllNum)
	assert.Equal(t, true, aggregator.StreamStatsOptions.Current)
	assert.Equal(t, true, aggregator.StreamStatsOptions.Global)
	assert.Equal(t, uint64(0), aggregator.StreamStatsOptions.Window)
	assert.Equal(t, false, aggregator.StreamStatsOptions.ResetOnChange)
	assert.NotNil(t, aggregator.StreamStatsOptions.TimeWindow)
	assert.Equal(t, float64(1), aggregator.StreamStatsOptions.TimeWindow.Num)
	assert.Equal(t, utils.TMHour, aggregator.StreamStatsOptions.TimeWindow.TimeScale)
	assert.Nil(t, aggregator.StreamStatsOptions.ResetAfter)
	assert.Nil(t, aggregator.StreamStatsOptions.ResetBefore)

	assert.Nil(t, aggregator.GroupByRequest)
	assert.NotNil(t, aggregator.MeasureOperations)
	assert.Equal(t, 1, len(aggregator.MeasureOperations))
	assert.Equal(t, utils.Count, aggregator.MeasureOperations[0].MeasureFunc)
	assert.Equal(t, "timestamp", aggregator.MeasureOperations[0].MeasureCol)

	assert.Nil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.Next)
	assert.Nil(t, aggregator.Next.Next)

	assert.Equal(t, structs.OutputTransformType, aggregator.Next.PipeCommandType)
	assert.NotNil(t, aggregator.Next.OutputTransforms)
	assert.Nil(t, aggregator.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.OutputTransforms.OutputColumns)
	assert.NotNil(t, aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns)
	assert.Equal(t, 1, len(aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns))

	renameCol, exist := aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["count(timestamp)"]
	assert.True(t, exist)
	assert.Equal(t, "tmpStamp", renameCol)
}

func Test_StreamStats_2(t *testing.T) {
	query := `* | streamstats window=3 time_window=7s reset_on_change=true count as total_events, median(sale_amount) as median_sale, stdev(revenue) as revenue_stdev, range(price) as price_range, mode(product_category) as most_popular_category`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.StreamStatsOptions)
	assert.Equal(t, false, aggregator.StreamStatsOptions.AllNum)
	assert.Equal(t, true, aggregator.StreamStatsOptions.Current)
	assert.Equal(t, true, aggregator.StreamStatsOptions.Global)
	assert.Equal(t, uint64(3), aggregator.StreamStatsOptions.Window)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetOnChange)
	assert.NotNil(t, aggregator.StreamStatsOptions.TimeWindow)
	assert.Equal(t, float64(7), aggregator.StreamStatsOptions.TimeWindow.Num)
	assert.Equal(t, utils.TMSecond, aggregator.StreamStatsOptions.TimeWindow.TimeScale)
	assert.Nil(t, aggregator.StreamStatsOptions.ResetAfter)
	assert.Nil(t, aggregator.StreamStatsOptions.ResetBefore)

	assert.Nil(t, aggregator.GroupByRequest)
	assert.NotNil(t, aggregator.MeasureOperations)
	assert.Equal(t, 5, len(aggregator.MeasureOperations))
	assert.Equal(t, utils.Count, aggregator.MeasureOperations[0].MeasureFunc)
	assert.Equal(t, "*", aggregator.MeasureOperations[0].MeasureCol)
	assert.Equal(t, utils.Median, aggregator.MeasureOperations[1].MeasureFunc)
	assert.Equal(t, "sale_amount", aggregator.MeasureOperations[1].MeasureCol)
	assert.Equal(t, utils.Stdev, aggregator.MeasureOperations[2].MeasureFunc)
	assert.Equal(t, "revenue", aggregator.MeasureOperations[2].MeasureCol)
	assert.Equal(t, utils.Range, aggregator.MeasureOperations[3].MeasureFunc)
	assert.Equal(t, "price", aggregator.MeasureOperations[3].MeasureCol)
	assert.Equal(t, utils.Mode, aggregator.MeasureOperations[4].MeasureFunc)
	assert.Equal(t, "product_category", aggregator.MeasureOperations[4].MeasureCol)

	assert.Nil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.Next)
	assert.Nil(t, aggregator.Next.Next)

	assert.Equal(t, structs.OutputTransformType, aggregator.Next.PipeCommandType)
	assert.NotNil(t, aggregator.Next.OutputTransforms)
	assert.Nil(t, aggregator.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.OutputTransforms.OutputColumns)
	assert.NotNil(t, aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns)
	assert.Equal(t, 5, len(aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns))

	renameCol, exist := aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["count(*)"]
	assert.True(t, exist)
	assert.Equal(t, "total_events", renameCol)
	renameCol, exist = aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["median(sale_amount)"]
	assert.True(t, exist)
	assert.Equal(t, "median_sale", renameCol)
	renameCol, exist = aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["stdev(revenue)"]
	assert.True(t, exist)
	assert.Equal(t, "revenue_stdev", renameCol)
	renameCol, exist = aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["range(price)"]
	assert.True(t, exist)
	assert.Equal(t, "price_range", renameCol)
	renameCol, exist = aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["mode(product_category)"]
	assert.True(t, exist)
	assert.Equal(t, "most_popular_category", renameCol)
}

func Test_StreamStats_3(t *testing.T) {
	query := `* | streamstats window=3 time_window=7s reset_on_change=true reset_before=(a=b) reset_after=(c>d) max(abc) AS newAbc`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.StreamStatsOptions)
	assert.Nil(t, aggregator.OutputTransforms)
	assert.Equal(t, false, aggregator.StreamStatsOptions.AllNum)
	assert.Equal(t, true, aggregator.StreamStatsOptions.Current)
	assert.Equal(t, true, aggregator.StreamStatsOptions.Global)
	assert.Equal(t, uint64(3), aggregator.StreamStatsOptions.Window)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetOnChange)
	assert.NotNil(t, aggregator.StreamStatsOptions.TimeWindow)
	assert.Equal(t, float64(7), aggregator.StreamStatsOptions.TimeWindow.Num)
	assert.Equal(t, utils.TMSecond, aggregator.StreamStatsOptions.TimeWindow.TimeScale)

	assert.Nil(t, aggregator.GroupByRequest)
	assert.NotNil(t, aggregator.MeasureOperations)
	assert.Equal(t, 1, len(aggregator.MeasureOperations))
	assert.Equal(t, utils.Max, aggregator.MeasureOperations[0].MeasureFunc)
	assert.Equal(t, "abc", aggregator.MeasureOperations[0].MeasureCol)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore.LeftValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore.LeftValue.NumericExpr)
	assert.Equal(t, "a", aggregator.StreamStatsOptions.ResetBefore.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetBefore.LeftValue.NumericExpr.IsTerminal)
	assert.Equal(t, "=", aggregator.StreamStatsOptions.ResetBefore.ValueOp)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore.RightValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore.RightValue.NumericExpr)
	assert.Equal(t, "b", aggregator.StreamStatsOptions.ResetBefore.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetBefore.RightValue.NumericExpr.IsTerminal)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.LeftValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.LeftValue.NumericExpr)
	assert.Equal(t, "c", aggregator.StreamStatsOptions.ResetAfter.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetAfter.LeftValue.NumericExpr.IsTerminal)
	assert.Equal(t, ">", aggregator.StreamStatsOptions.ResetAfter.ValueOp)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.RightValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.RightValue.NumericExpr)
	assert.Equal(t, "d", aggregator.StreamStatsOptions.ResetAfter.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetAfter.RightValue.NumericExpr.IsTerminal)

	assert.NotNil(t, aggregator.Next)
	assert.Nil(t, aggregator.Next.Next)

	assert.Equal(t, structs.OutputTransformType, aggregator.Next.PipeCommandType)
	assert.NotNil(t, aggregator.Next.OutputTransforms)
	assert.Nil(t, aggregator.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.OutputTransforms.OutputColumns)
	assert.NotNil(t, aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns)
	assert.Equal(t, 1, len(aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns))

	renameCol, exist := aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["max(abc)"]
	assert.True(t, exist)
	assert.Equal(t, "newAbc", renameCol)
}

func Test_StreamStats_4(t *testing.T) {
	query := `* | streamstats window=3 time_window=7s reset_on_change=true reset_before=(a<b) reset_after=(c>d) median(abc) AS newAbc`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.StreamStatsOptions)
	assert.Nil(t, aggregator.OutputTransforms)
	assert.Equal(t, false, aggregator.StreamStatsOptions.AllNum)
	assert.Equal(t, true, aggregator.StreamStatsOptions.Current)
	assert.Equal(t, true, aggregator.StreamStatsOptions.Global)
	assert.Equal(t, uint64(3), aggregator.StreamStatsOptions.Window)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetOnChange)
	assert.NotNil(t, aggregator.StreamStatsOptions.TimeWindow)
	assert.Equal(t, float64(7), aggregator.StreamStatsOptions.TimeWindow.Num)
	assert.Equal(t, utils.TMSecond, aggregator.StreamStatsOptions.TimeWindow.TimeScale)

	assert.Nil(t, aggregator.GroupByRequest)
	assert.NotNil(t, aggregator.MeasureOperations)
	assert.Equal(t, 1, len(aggregator.MeasureOperations))
	assert.Equal(t, utils.Median, aggregator.MeasureOperations[0].MeasureFunc)
	assert.Equal(t, "abc", aggregator.MeasureOperations[0].MeasureCol)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore.LeftValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore.LeftValue.NumericExpr)
	assert.Equal(t, "a", aggregator.StreamStatsOptions.ResetBefore.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetBefore.LeftValue.NumericExpr.IsTerminal)
	assert.Equal(t, "<", aggregator.StreamStatsOptions.ResetBefore.ValueOp)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore.RightValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore.RightValue.NumericExpr)
	assert.Equal(t, "b", aggregator.StreamStatsOptions.ResetBefore.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetBefore.RightValue.NumericExpr.IsTerminal)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.LeftValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.LeftValue.NumericExpr)
	assert.Equal(t, "c", aggregator.StreamStatsOptions.ResetAfter.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetAfter.LeftValue.NumericExpr.IsTerminal)
	assert.Equal(t, ">", aggregator.StreamStatsOptions.ResetAfter.ValueOp)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.RightValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.RightValue.NumericExpr)
	assert.Equal(t, "d", aggregator.StreamStatsOptions.ResetAfter.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetAfter.RightValue.NumericExpr.IsTerminal)

	assert.NotNil(t, aggregator.Next)
	assert.Nil(t, aggregator.Next.Next)

	assert.Equal(t, structs.OutputTransformType, aggregator.Next.PipeCommandType)
	assert.NotNil(t, aggregator.Next.OutputTransforms)
	assert.Nil(t, aggregator.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.OutputTransforms.OutputColumns)
	assert.NotNil(t, aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns)
	assert.Equal(t, 1, len(aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns))

	renameCol, exist := aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["median(abc)"]
	assert.True(t, exist)
	assert.Equal(t, "newAbc", renameCol)
}

func Test_StreamStats_5(t *testing.T) {
	query := `* | streamstats window=10 time_window=1q reset_after=(a=1 OR b>2 AND c<=3) count as event_count, sum(latency) as avg_latency, min(bytes) as max_bytes by first_name, city`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.StreamStatsOptions)
	assert.Nil(t, aggregator.OutputTransforms)
	assert.Equal(t, false, aggregator.StreamStatsOptions.AllNum)
	assert.Equal(t, true, aggregator.StreamStatsOptions.Current)
	assert.Equal(t, true, aggregator.StreamStatsOptions.Global)
	assert.Equal(t, uint64(10), aggregator.StreamStatsOptions.Window)
	assert.Equal(t, false, aggregator.StreamStatsOptions.ResetOnChange)
	assert.NotNil(t, aggregator.StreamStatsOptions.TimeWindow)
	assert.Equal(t, float64(1), aggregator.StreamStatsOptions.TimeWindow.Num)
	assert.Equal(t, utils.TMQuarter, aggregator.StreamStatsOptions.TimeWindow.TimeScale)
	assert.Nil(t, aggregator.StreamStatsOptions.ResetBefore)

	assert.Nil(t, aggregator.MeasureOperations)
	assert.NotNil(t, aggregator.GroupByRequest)
	assert.Equal(t, 3, len(aggregator.GroupByRequest.MeasureOperations))
	assert.Equal(t, utils.Count, aggregator.GroupByRequest.MeasureOperations[0].MeasureFunc)
	assert.Equal(t, "*", aggregator.GroupByRequest.MeasureOperations[0].MeasureCol)
	assert.Equal(t, utils.Sum, aggregator.GroupByRequest.MeasureOperations[1].MeasureFunc)
	assert.Equal(t, "latency", aggregator.GroupByRequest.MeasureOperations[1].MeasureCol)
	assert.Equal(t, utils.Min, aggregator.GroupByRequest.MeasureOperations[2].MeasureFunc)
	assert.Equal(t, "bytes", aggregator.GroupByRequest.MeasureOperations[2].MeasureCol)

	assert.NotNil(t, aggregator.GroupByRequest.GroupByColumns)
	assert.Equal(t, true, putils.CompareStringSlices([]string{"first_name", "city"}, aggregator.GroupByRequest.GroupByColumns))

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter)
	assert.Nil(t, aggregator.StreamStatsOptions.ResetAfter.LeftValue)
	assert.Nil(t, aggregator.StreamStatsOptions.ResetAfter.RightValue)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.LeftBool)
	assert.Equal(t, structs.BoolOpOr, aggregator.StreamStatsOptions.ResetAfter.BoolOp)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.LeftBool.LeftValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.LeftBool.LeftValue.NumericExpr)
	assert.Equal(t, "a", aggregator.StreamStatsOptions.ResetAfter.LeftBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetAfter.LeftBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, "=", aggregator.StreamStatsOptions.ResetAfter.LeftBool.ValueOp)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.LeftBool.RightValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.LeftBool.RightValue.NumericExpr)
	assert.Equal(t, "1", aggregator.StreamStatsOptions.ResetAfter.LeftBool.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetAfter.LeftBool.RightValue.NumericExpr.IsTerminal)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.RightBool)
	assert.Nil(t, aggregator.StreamStatsOptions.ResetAfter.RightBool.LeftValue)
	assert.Nil(t, aggregator.StreamStatsOptions.ResetAfter.RightBool.RightValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.RightBool.LeftBool)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.RightBool.RightBool)

	assert.Equal(t, aggregator.StreamStatsOptions.ResetAfter.RightBool.BoolOp, structs.BoolOpAnd)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.RightBool.LeftBool.LeftValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.RightBool.LeftBool.LeftValue.NumericExpr)
	assert.Equal(t, "b", aggregator.StreamStatsOptions.ResetAfter.RightBool.LeftBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetAfter.RightBool.LeftBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, ">", aggregator.StreamStatsOptions.ResetAfter.RightBool.LeftBool.ValueOp)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.RightBool.LeftBool.RightValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.RightBool.LeftBool.RightValue.NumericExpr)
	assert.Equal(t, "2", aggregator.StreamStatsOptions.ResetAfter.RightBool.LeftBool.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetAfter.RightBool.LeftBool.RightValue.NumericExpr.IsTerminal)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.RightBool.RightBool.LeftValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.RightBool.RightBool.LeftValue.NumericExpr)
	assert.Equal(t, "c", aggregator.StreamStatsOptions.ResetAfter.RightBool.RightBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetAfter.RightBool.RightBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, "<=", aggregator.StreamStatsOptions.ResetAfter.RightBool.RightBool.ValueOp)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.RightBool.RightBool.RightValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetAfter.RightBool.RightBool.RightValue.NumericExpr)
	assert.Equal(t, "3", aggregator.StreamStatsOptions.ResetAfter.RightBool.RightBool.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetAfter.RightBool.RightBool.RightValue.NumericExpr.IsTerminal)

	assert.NotNil(t, aggregator.Next)
	assert.Nil(t, aggregator.Next.Next)

	assert.Equal(t, structs.OutputTransformType, aggregator.Next.PipeCommandType)
	assert.NotNil(t, aggregator.Next.OutputTransforms)
	assert.Nil(t, aggregator.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.OutputTransforms.OutputColumns)
	assert.NotNil(t, aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns)
	assert.Equal(t, 3, len(aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns))

	renameCol, exist := aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["count(*)"]
	assert.True(t, exist)
	assert.Equal(t, "event_count", renameCol)
	renameCol, exist = aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["sum(latency)"]
	assert.True(t, exist)
	assert.Equal(t, "avg_latency", renameCol)
	renameCol, exist = aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["min(bytes)"]
	assert.True(t, exist)
	assert.Equal(t, "max_bytes", renameCol)
}

func Test_StreamStats_6(t *testing.T) {
	query := `* | streamstats window=10001 count as cnt`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_StreamStats_7(t *testing.T) {
	query := `* | streamstats time_window=6q count as cnt`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_StreamStats_8(t *testing.T) {
	query := `* | streamstats timewindow=1us count as cnt`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_StreamStats_9(t *testing.T) {
	query := `* | streamstats window=-1 count as cnt`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_StreamStats_10(t *testing.T) {
	query := `* | streamstats timewindow=7ms count as cnt`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_StreamStats_11(t *testing.T) {
	query := `app_name=Bracecould | streamstats count as total_events, dc(user) as unique_users, avg(response_time) as avg_response_time, max(response_time) as max_response_time, min(response_time) as min_response_time, sum(bytes) as total_bytes by host, application`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.StreamStatsOptions)
	assert.Nil(t, aggregator.OutputTransforms)
	assert.Equal(t, false, aggregator.StreamStatsOptions.AllNum)
	assert.Equal(t, true, aggregator.StreamStatsOptions.Current)
	assert.Equal(t, true, aggregator.StreamStatsOptions.Global)
	assert.Equal(t, uint64(0), aggregator.StreamStatsOptions.Window)
	assert.Equal(t, false, aggregator.StreamStatsOptions.ResetOnChange)
	assert.Nil(t, aggregator.StreamStatsOptions.TimeWindow)
	assert.Nil(t, aggregator.StreamStatsOptions.ResetAfter)
	assert.Nil(t, aggregator.StreamStatsOptions.ResetBefore)
	assert.Nil(t, aggregator.MeasureOperations)

	assert.NotNil(t, aggregator.GroupByRequest)
	assert.Equal(t, 6, len(aggregator.GroupByRequest.MeasureOperations))
	assert.Equal(t, utils.Count, aggregator.GroupByRequest.MeasureOperations[0].MeasureFunc)
	assert.Equal(t, "*", aggregator.GroupByRequest.MeasureOperations[0].MeasureCol)
	assert.Equal(t, utils.Cardinality, aggregator.GroupByRequest.MeasureOperations[1].MeasureFunc)
	assert.Equal(t, "user", aggregator.GroupByRequest.MeasureOperations[1].MeasureCol)
	assert.Equal(t, utils.Avg, aggregator.GroupByRequest.MeasureOperations[2].MeasureFunc)
	assert.Equal(t, "response_time", aggregator.GroupByRequest.MeasureOperations[2].MeasureCol)
	assert.Equal(t, utils.Max, aggregator.GroupByRequest.MeasureOperations[3].MeasureFunc)
	assert.Equal(t, "response_time", aggregator.GroupByRequest.MeasureOperations[3].MeasureCol)
	assert.Equal(t, utils.Min, aggregator.GroupByRequest.MeasureOperations[4].MeasureFunc)
	assert.Equal(t, "response_time", aggregator.GroupByRequest.MeasureOperations[4].MeasureCol)
	assert.Equal(t, utils.Sum, aggregator.GroupByRequest.MeasureOperations[5].MeasureFunc)
	assert.Equal(t, "bytes", aggregator.GroupByRequest.MeasureOperations[5].MeasureCol)

	assert.NotNil(t, aggregator.GroupByRequest.GroupByColumns)
	assert.Equal(t, true, putils.CompareStringSlices([]string{"host", "application"}, aggregator.GroupByRequest.GroupByColumns))

	assert.NotNil(t, aggregator.Next)
	assert.Nil(t, aggregator.Next.Next)

	assert.Equal(t, structs.OutputTransformType, aggregator.Next.PipeCommandType)
	assert.NotNil(t, aggregator.Next.OutputTransforms)
	assert.Nil(t, aggregator.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.OutputTransforms.OutputColumns)
	assert.NotNil(t, aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns)
	assert.Equal(t, 6, len(aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns))

	renameCol, exist := aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["count(*)"]
	assert.True(t, exist)
	assert.Equal(t, "total_events", renameCol)
	renameCol, exist = aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["cardinality(user)"]
	assert.True(t, exist)
	assert.Equal(t, "unique_users", renameCol)
	renameCol, exist = aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["avg(response_time)"]
	assert.True(t, exist)
	assert.Equal(t, "avg_response_time", renameCol)
	renameCol, exist = aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["max(response_time)"]
	assert.True(t, exist)
	assert.Equal(t, "max_response_time", renameCol)
	renameCol, exist = aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["min(response_time)"]
	assert.True(t, exist)
	assert.Equal(t, "min_response_time", renameCol)
	renameCol, exist = aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["sum(bytes)"]
	assert.True(t, exist)
	assert.Equal(t, "total_bytes", renameCol)
}

func Test_StreamStats_12(t *testing.T) {
	query := `city=Boston | streamstats current=false reset_before=(col="abc" OR isbool(mycol)) count as group_count by first_name, city, age`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.StreamStatsOptions)
	assert.Nil(t, aggregator.OutputTransforms)
	assert.Equal(t, false, aggregator.StreamStatsOptions.AllNum)
	assert.Equal(t, false, aggregator.StreamStatsOptions.Current)
	assert.Equal(t, true, aggregator.StreamStatsOptions.Global)
	assert.Equal(t, uint64(0), aggregator.StreamStatsOptions.Window)
	assert.Equal(t, false, aggregator.StreamStatsOptions.ResetOnChange)
	assert.Nil(t, aggregator.StreamStatsOptions.TimeWindow)
	assert.Nil(t, aggregator.StreamStatsOptions.ResetAfter)
	assert.Nil(t, aggregator.MeasureOperations)

	assert.NotNil(t, aggregator.GroupByRequest)
	assert.Equal(t, 1, len(aggregator.GroupByRequest.MeasureOperations))
	assert.Equal(t, utils.Count, aggregator.GroupByRequest.MeasureOperations[0].MeasureFunc)
	assert.Equal(t, "*", aggregator.GroupByRequest.MeasureOperations[0].MeasureCol)

	assert.NotNil(t, aggregator.GroupByRequest.GroupByColumns)
	assert.Equal(t, true, putils.CompareStringSlices([]string{"first_name", "city", "age"}, aggregator.GroupByRequest.GroupByColumns))

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore)
	assert.Nil(t, aggregator.StreamStatsOptions.ResetBefore.LeftValue)
	assert.Nil(t, aggregator.StreamStatsOptions.ResetBefore.RightValue)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore.LeftBool)
	assert.Equal(t, structs.BoolOpOr, aggregator.StreamStatsOptions.ResetBefore.BoolOp)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore.LeftBool.LeftValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore.LeftBool.LeftValue.NumericExpr)
	assert.Equal(t, "col", aggregator.StreamStatsOptions.ResetBefore.LeftBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.StreamStatsOptions.ResetBefore.LeftBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, "=", aggregator.StreamStatsOptions.ResetBefore.LeftBool.ValueOp)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore.LeftBool.RightValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore.LeftBool.RightValue.StringExpr)
	assert.Equal(t, "abc", aggregator.StreamStatsOptions.ResetBefore.LeftBool.RightValue.StringExpr.RawString)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore.RightBool)
	assert.NotNil(t, "isbool", aggregator.StreamStatsOptions.ResetBefore.RightBool.ValueOp)

	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore.RightBool.LeftValue)
	assert.NotNil(t, aggregator.StreamStatsOptions.ResetBefore.RightBool.LeftValue.NumericExpr)
	assert.Equal(t, "mycol", aggregator.StreamStatsOptions.ResetBefore.RightBool.LeftValue.NumericExpr.Value)

	assert.NotNil(t, aggregator.Next)
	assert.Nil(t, aggregator.Next.Next)

	assert.Equal(t, structs.OutputTransformType, aggregator.Next.PipeCommandType)
	assert.NotNil(t, aggregator.Next.OutputTransforms)
	assert.Nil(t, aggregator.Next.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.Next.OutputTransforms.OutputColumns)
	assert.NotNil(t, aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns)
	assert.Equal(t, 1, len(aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns))

	renameCol, exist := aggregator.Next.OutputTransforms.OutputColumns.RenameAggregationColumns["count(*)"]
	assert.True(t, exist)
	assert.Equal(t, "group_count", renameCol)
}

func Test_StreamStats_13(t *testing.T) {
	query := `* | streamstats timewindow=7 count as cnt`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_StreamStats_14(t *testing.T) {
	query := `* | streamstats timewindow=7.5s count as cnt`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_StreamStats_15(t *testing.T) {
	query := `* | streamstats timewindow=5mon count as cnt`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_StreamStats_16(t *testing.T) {
	query := `* | streamstats time_window=11year count, max(abc), avg(def)`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(string(query), 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.StreamStatsOptions)
	assert.Equal(t, false, aggregator.StreamStatsOptions.AllNum)
	assert.Equal(t, true, aggregator.StreamStatsOptions.Current)
	assert.Equal(t, true, aggregator.StreamStatsOptions.Global)
	assert.Equal(t, uint64(0), aggregator.StreamStatsOptions.Window)
	assert.Equal(t, false, aggregator.StreamStatsOptions.ResetOnChange)
	assert.NotNil(t, aggregator.StreamStatsOptions.TimeWindow)
	assert.Equal(t, float64(11), aggregator.StreamStatsOptions.TimeWindow.Num)
	assert.Equal(t, utils.TMYear, aggregator.StreamStatsOptions.TimeWindow.TimeScale)
	assert.Nil(t, aggregator.StreamStatsOptions.ResetAfter)
	assert.Nil(t, aggregator.StreamStatsOptions.ResetBefore)
	assert.Nil(t, aggregator.GroupByRequest)

	assert.NotNil(t, aggregator.MeasureOperations)
	assert.Equal(t, 3, len(aggregator.MeasureOperations))
	assert.Equal(t, utils.Count, aggregator.MeasureOperations[0].MeasureFunc)
	assert.Equal(t, "*", aggregator.MeasureOperations[0].MeasureCol)
	assert.Equal(t, utils.Max, aggregator.MeasureOperations[1].MeasureFunc)
	assert.Equal(t, "abc", aggregator.MeasureOperations[1].MeasureCol)
	assert.Equal(t, utils.Avg, aggregator.MeasureOperations[2].MeasureFunc)
	assert.Equal(t, "def", aggregator.MeasureOperations[2].MeasureCol)

	assert.Nil(t, aggregator.OutputTransforms)
	assert.Nil(t, aggregator.Next)
}

func Test_StreamStats_17(t *testing.T) {
	query := `* | streamstats current=false timewindow=1s count as cnt`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_StreamStats_18(t *testing.T) {
	query := `* | streamstats global=false timewindow=1min count as cnt`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_FillNull_No_Args(t *testing.T) {
	query := `* | fillnull`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.FillNullRequest)
	assert.Equal(t, "0", aggregator.OutputTransforms.LetColumns.FillNullRequest.Value)
}

func Test_FillNull_ValueArg(t *testing.T) {
	query := `* | fillnull value=NULL`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.FillNullRequest)
	assert.Equal(t, "NULL", aggregator.OutputTransforms.LetColumns.FillNullRequest.Value)
}

func Test_FillNull_FiedList(t *testing.T) {
	query := `* | fillnull field1 field2`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.FillNullRequest)
	assert.Equal(t, "0", aggregator.OutputTransforms.LetColumns.FillNullRequest.Value)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.FillNullRequest.FieldList)
	assert.Equal(t, 2, len(aggregator.OutputTransforms.LetColumns.FillNullRequest.FieldList))
	assert.Equal(t, []string{"field1", "field2"}, aggregator.OutputTransforms.LetColumns.FillNullRequest.FieldList)
}

func Test_FillNull_ValueArg_FieldList(t *testing.T) {
	query := `* | fillnull value=NULL field1 field2`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.FillNullRequest)
	assert.Equal(t, "NULL", aggregator.OutputTransforms.LetColumns.FillNullRequest.Value)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.FillNullRequest.FieldList)
	assert.Equal(t, 2, len(aggregator.OutputTransforms.LetColumns.FillNullRequest.FieldList))
	assert.Equal(t, []string{"field1", "field2"}, aggregator.OutputTransforms.LetColumns.FillNullRequest.FieldList)
}

func getMeasureFuncStr(measureFunc utils.AggregateFunctions) (string, string) {
	switch measureFunc {
	case utils.Cardinality:
		return "dc", ""
	case utils.Perc, utils.ExactPerc, utils.UpperPerc:
		percentStr := fmt.Sprintf("%v", rand.Float64()*100)
		return measureFunc.String() + percentStr, percentStr
	default:
		return measureFunc.String(), ""
	}
}

func testSingleAggregateFunction(t *testing.T, aggFunc utils.AggregateFunctions) {
	measureFuncStr, param := getMeasureFuncStr(aggFunc)
	measureCol := putils.GetRandomString(10, putils.Alpha)
	query := []byte(`search A=1 | stats ` + measureFuncStr + `(` + measureCol + `)`)
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
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureCol, measureCol)
	assert.Equal(t, pipeCommands.MeasureOperations[0].MeasureFunc, aggFunc)

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
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureCol, measureCol)
	assert.Equal(t, aggregator.MeasureOperations[0].MeasureFunc, aggFunc)
	assert.Equal(t, aggregator.MeasureOperations[0].Param, param)
}

func performCommon_aggEval_BoolExpr(t *testing.T, measureFunc utils.AggregateFunctions) {
	// Query Form: city=Boston | stats max(latitude), measureFunc(eval(latitude >= 0 AND http_method="GET"))
	measureFuncStr, param := getMeasureFuncStr(measureFunc)
	measureWithEvalStr := measureFuncStr + `(eval(latitude >= 0 AND http_method="GET"))`

	query := []byte(`city=Boston | stats max(latitude), ` + measureWithEvalStr)
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

	assert.Equal(t, measureWithEvalStr, pipeCommands.MeasureOperations[1].StrEnc)
	assert.Equal(t, measureFunc, pipeCommands.MeasureOperations[1].MeasureFunc)
	assert.Equal(t, pipeCommands.MeasureOperations[1].Param, param)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest)
	assert.Equal(t, structs.VEMBooleanExpr, int(pipeCommands.MeasureOperations[1].ValueColRequest.ValueExprMode))
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.LeftBool)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.BoolOp)
	assert.Equal(t, structs.BoolOpAnd, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.BoolOp)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.RightBool)

	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.LeftBool.LeftValue)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.LeftBool.RightValue)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.LeftBool.LeftValue.NumericExpr)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.LeftBool.RightValue.NumericExpr)
	assert.Equal(t, "latitude", pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.LeftBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, "0", pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.LeftBool.RightValue.NumericExpr.Value)

	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.RightBool.LeftValue)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.RightBool.RightValue)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.RightBool.LeftValue.NumericExpr)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.RightBool.RightValue.StringExpr)
	assert.Equal(t, "http_method", pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.RightBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, "GET", pipeCommands.MeasureOperations[1].ValueColRequest.BooleanExpr.RightBool.RightValue.StringExpr.RawString)
}

func performCommon_aggEval_Constant_Field(t *testing.T, measureFunc utils.AggregateFunctions, isField bool) {
	// Query Form: city=Boston | stats max(latitude), measureFunc(eval(constantNum))
	var randomStr string
	if isField {
		randomStr = putils.GetRandomString(10, putils.Alpha)
	} else {
		randomStr = fmt.Sprintf("%v", rand.Float64())
	}
	measureFuncStr, param := getMeasureFuncStr(measureFunc)
	measureWithEvalStr := measureFuncStr + `(eval(` + randomStr + `))`

	query := []byte(`city=Boston | stats max(latitude), ` + measureWithEvalStr)
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

	assert.Equal(t, measureWithEvalStr, pipeCommands.MeasureOperations[1].StrEnc)
	assert.Equal(t, measureFunc, pipeCommands.MeasureOperations[1].MeasureFunc)
	assert.Equal(t, pipeCommands.MeasureOperations[1].Param, param)

	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest)
	assert.Equal(t, structs.VEMNumericExpr, int(pipeCommands.MeasureOperations[1].ValueColRequest.ValueExprMode))
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.NumericExpr)

	assert.Equal(t, randomStr, pipeCommands.MeasureOperations[1].ValueColRequest.NumericExpr.Value)
	assert.Equal(t, isField, pipeCommands.MeasureOperations[1].ValueColRequest.NumericExpr.ValueIsField)
}

func performCommon_aggEval_ConditionalExpr(t *testing.T, measureFunc utils.AggregateFunctions) {
	// Query Form: app_name=bracecould | stats sum(http_status), measureFunc(eval(if(http_status=500, trueValueField, falseValueConstant)))
	measureFuncStr, param := getMeasureFuncStr(measureFunc)
	trueValueField := putils.GetRandomString(10, putils.Alpha)
	falseValueConstant := fmt.Sprintf("%v", rand.Float64())
	measureWithEvalStr := measureFuncStr + `(eval(if(http_status=500, ` + trueValueField + `, ` + falseValueConstant + `)))`

	query := []byte(`app_name=bracecould | stats sum(http_status), ` + measureWithEvalStr)
	res, err := spl.Parse("", query)
	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, ast.NodeTerminal, filterNode.NodeType)
	assert.Equal(t, "app_name", filterNode.Comparison.Field)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "\"bracecould\"", filterNode.Comparison.Values)

	pipeCommands := res.(ast.QueryStruct).PipeCommands
	assert.NotNil(t, pipeCommands)
	assert.Equal(t, pipeCommands.PipeCommandType, structs.MeasureAggsType)
	assert.Len(t, pipeCommands.MeasureOperations, 2)
	assert.Equal(t, "http_status", pipeCommands.MeasureOperations[0].MeasureCol)
	assert.Equal(t, utils.Sum, pipeCommands.MeasureOperations[0].MeasureFunc)

	assert.Equal(t, measureWithEvalStr, pipeCommands.MeasureOperations[1].StrEnc)
	assert.Equal(t, measureFunc, pipeCommands.MeasureOperations[1].MeasureFunc)
	assert.Equal(t, pipeCommands.MeasureOperations[1].Param, param)

	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest)
	assert.Equal(t, structs.VEMConditionExpr, int(pipeCommands.MeasureOperations[1].ValueColRequest.ValueExprMode))

	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr)
	assert.Equal(t, "if", pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.Op)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.BoolExpr)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.BoolExpr.LeftValue)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.BoolExpr.RightValue)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.BoolExpr.RightValue.NumericExpr)
	assert.Equal(t, "http_status", pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.BoolExpr.LeftValue.NumericExpr.Value)
	assert.Equal(t, "=", pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.BoolExpr.ValueOp)
	assert.Equal(t, "500", pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.BoolExpr.RightValue.NumericExpr.Value)

	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.TrueValue)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.TrueValue.NumericExpr)
	assert.Equal(t, trueValueField, pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.TrueValue.NumericExpr.Value)
	assert.Equal(t, true, pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.TrueValue.NumericExpr.ValueIsField)

	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.FalseValue)
	assert.NotNil(t, pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.FalseValue.NumericExpr)
	assert.Equal(t, falseValueConstant, pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.FalseValue.NumericExpr.Value)
	assert.Equal(t, false, pipeCommands.MeasureOperations[1].ValueColRequest.ConditionExpr.FalseValue.NumericExpr.ValueIsField)
}

func getAggFunctions() []utils.AggregateFunctions {
	return []utils.AggregateFunctions{utils.Count, utils.Sum, utils.Avg, utils.Min, utils.Max,
		utils.Range, utils.Cardinality, utils.Values, utils.List,
		utils.Estdc, utils.EstdcError, utils.Median,
		utils.Mode, utils.Stdev, utils.Stdevp, utils.Sumsq, utils.Var,
		utils.Varp, utils.First, utils.Last, utils.Earliest, utils.Latest,
		utils.EarliestTime, utils.LatestTime, utils.StatsRate,
		utils.Perc, utils.ExactPerc, utils.UpperPerc,
	}
}

func Test_Aggs(t *testing.T) {
	aggFuncs := getAggFunctions()

	for _, aggFunc := range aggFuncs {
		testSingleAggregateFunction(t, aggFunc)
		performCommon_aggEval_BoolExpr(t, aggFunc)
		performCommon_aggEval_Constant_Field(t, aggFunc, false)
		performCommon_aggEval_Constant_Field(t, aggFunc, true)
		performCommon_aggEval_ConditionalExpr(t, aggFunc)
	}
}

func Test_MVExpand_NoLimit(t *testing.T) {
	query := `* | mvexpand batch`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest)
	assert.Equal(t, "mvexpand", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.Command)
	assert.Equal(t, "batch", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.ColName)
	assert.Equal(t, int64(0), aggregator.OutputTransforms.LetColumns.MultiValueColRequest.Limit)
}

func Test_MVExpand_WithLimit(t *testing.T) {
	query := `* | mvexpand app_name limit=5`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Equal(t, structs.OutputTransformType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.OutputTransforms)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns)
	assert.NotNil(t, aggregator.OutputTransforms.LetColumns.MultiValueColRequest)
	assert.Equal(t, "mvexpand", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.Command)
	assert.Equal(t, "app_name", aggregator.OutputTransforms.LetColumns.MultiValueColRequest.ColName)
	assert.Equal(t, int64(5), aggregator.OutputTransforms.LetColumns.MultiValueColRequest.Limit)
}

func Test_MVExpand_InvalidLimit(t *testing.T) {
	query := `* | mvexpand batch limit=invalid`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_MVExpand_MissingField(t *testing.T) {
	query := `* | mvexpand`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func getTimeAfterOffsetAndSnapDay(t *testing.T, offset int, currTime time.Time) (uint64, error) {
	var err error

	currTime, err = utils.ApplyOffsetToTime(int64(offset), utils.TMDay, currTime)
	if err != nil {
		return uint64(0), err
	}

	snapStr := fmt.Sprintf("%v", utils.TMDay)
	currTime, err = utils.ApplySnap(snapStr, currTime)
	if err != nil {
		return uint64(0), err
	}

	return uint64(currTime.UnixMilli()), err
}

func Test_GenTimes(t *testing.T) {
	query := `| gentimes start=1`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)
	assert.Equal(t, structs.GenerateEventType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.GenerateEvent)
	assert.NotNil(t, aggregator.GenerateEvent.GenTimes)

	currTime := time.Now()

	expectedStartTime, err := getTimeAfterOffsetAndSnapDay(t, 1, currTime)
	assert.Nil(t, err)
	assert.Equal(t, expectedStartTime, aggregator.GenerateEvent.GenTimes.StartTime)

	expectedEndTime, err := getTimeAfterOffsetAndSnapDay(t, 0, currTime)
	assert.Nil(t, err)
	assert.Equal(t, expectedEndTime, aggregator.GenerateEvent.GenTimes.EndTime)

	assert.NotNil(t, aggregator.GenerateEvent.GenTimes.Interval)
	assert.Equal(t, 1, aggregator.GenerateEvent.GenTimes.Interval.Num)
	assert.Equal(t, utils.TMDay, aggregator.GenerateEvent.GenTimes.Interval.TimeScalr)
}

func Test_GenTimes_2(t *testing.T) {
	query := `| gentimes start=-3 end=2`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)
	assert.Equal(t, structs.GenerateEventType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.GenerateEvent)
	assert.NotNil(t, aggregator.GenerateEvent.GenTimes)

	currTime := time.Now()

	expectedStartTime, err := getTimeAfterOffsetAndSnapDay(t, -3, currTime)
	assert.Nil(t, err)
	assert.Equal(t, expectedStartTime, aggregator.GenerateEvent.GenTimes.StartTime)

	expectedEndTime, err := getTimeAfterOffsetAndSnapDay(t, 2, currTime)
	assert.Nil(t, err)
	assert.Equal(t, expectedEndTime, aggregator.GenerateEvent.GenTimes.EndTime)

	assert.NotNil(t, aggregator.GenerateEvent.GenTimes.Interval)
	assert.Equal(t, 1, aggregator.GenerateEvent.GenTimes.Interval.Num)
	assert.Equal(t, utils.TMDay, aggregator.GenerateEvent.GenTimes.Interval.TimeScalr)
}

func Test_GenTimes_3(t *testing.T) {
	query := `| gentimes start=10/01/2022 end=12/03/2023:12:20:56`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)
	assert.Equal(t, structs.GenerateEventType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.GenerateEvent)
	assert.NotNil(t, aggregator.GenerateEvent.GenTimes)

	expectedStartTime, err := utils.ConvertCustomDateTimeFormatToEpochMs("10/01/2022:00:00:00")
	assert.Nil(t, err)
	assert.Equal(t, uint64(expectedStartTime), aggregator.GenerateEvent.GenTimes.StartTime)

	expectedEndTime, err := utils.ConvertCustomDateTimeFormatToEpochMs("12/03/2023:12:20:56")
	assert.Nil(t, err)
	assert.Equal(t, uint64(expectedEndTime), aggregator.GenerateEvent.GenTimes.EndTime)

	assert.NotNil(t, aggregator.GenerateEvent.GenTimes.Interval)
	assert.Equal(t, 1, aggregator.GenerateEvent.GenTimes.Interval.Num)
	assert.Equal(t, utils.TMDay, aggregator.GenerateEvent.GenTimes.Interval.TimeScalr)
}

func Test_GenTimes_4(t *testing.T) {
	query := `| gentimes start=10/01/2022 increment=3m end=-5`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)
	assert.Equal(t, structs.GenerateEventType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.GenerateEvent)
	assert.NotNil(t, aggregator.GenerateEvent.GenTimes)

	expectedStartTime, err := utils.ConvertCustomDateTimeFormatToEpochMs("10/01/2022:00:00:00")
	assert.Nil(t, err)
	assert.Equal(t, uint64(expectedStartTime), aggregator.GenerateEvent.GenTimes.StartTime)

	expectedEndTime, err := getTimeAfterOffsetAndSnapDay(t, -5, time.Now())
	assert.Nil(t, err)
	assert.Equal(t, expectedEndTime, aggregator.GenerateEvent.GenTimes.EndTime)

	assert.NotNil(t, aggregator.GenerateEvent.GenTimes.Interval)
	assert.Equal(t, 3, aggregator.GenerateEvent.GenTimes.Interval.Num)
	assert.Equal(t, utils.TMMinute, aggregator.GenerateEvent.GenTimes.Interval.TimeScalr)
}

func Test_GenTimes_5(t *testing.T) {
	query := `| gentimes start=6 increment=-11h end=12/03/2023:23:11:56`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)
	assert.Equal(t, structs.GenerateEventType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.GenerateEvent)
	assert.NotNil(t, aggregator.GenerateEvent.GenTimes)

	expectedStartTime, err := getTimeAfterOffsetAndSnapDay(t, 6, time.Now())
	assert.Nil(t, err)
	assert.Equal(t, expectedStartTime, aggregator.GenerateEvent.GenTimes.StartTime)

	expectedEndTime, err := utils.ConvertCustomDateTimeFormatToEpochMs("12/03/2023:23:11:56")
	assert.Nil(t, err)
	assert.Equal(t, uint64(expectedEndTime), aggregator.GenerateEvent.GenTimes.EndTime)

	assert.NotNil(t, aggregator.GenerateEvent.GenTimes.Interval)
	assert.Equal(t, -11, aggregator.GenerateEvent.GenTimes.Interval.Num)
	assert.Equal(t, utils.TMHour, aggregator.GenerateEvent.GenTimes.Interval.TimeScalr)
}

func Test_GenTimes_6(t *testing.T) {
	query := `| gentimes increment=-11h end=12/03/2023:23:11:56`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_GenTimes_7(t *testing.T) {
	query := `| gentimes start=-3 end=2 | eval myField=replace(date, "^(\d{1,2})/(\d{1,2})/", "\2/\1/")`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.Equal(t, structs.GenerateEventType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.GenerateEvent)
	assert.NotNil(t, aggregator.GenerateEvent.GenTimes)

	currTime := time.Now()

	expectedStartTime, err := getTimeAfterOffsetAndSnapDay(t, -3, currTime)
	assert.Nil(t, err)
	assert.Equal(t, expectedStartTime, aggregator.GenerateEvent.GenTimes.StartTime)

	expectedEndTime, err := getTimeAfterOffsetAndSnapDay(t, 2, currTime)
	assert.Nil(t, err)
	assert.Equal(t, expectedEndTime, aggregator.GenerateEvent.GenTimes.EndTime)

	assert.NotNil(t, aggregator.GenerateEvent.GenTimes.Interval)
	assert.Equal(t, 1, aggregator.GenerateEvent.GenTimes.Interval.Num)
	assert.Equal(t, utils.TMDay, aggregator.GenerateEvent.GenTimes.Interval.TimeScalr)

	assert.Equal(t, aggregator.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.OutputTransforms.LetColumns.NewColName, "myField")
	assert.NotNil(t, aggregator.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMStringExpr)
	assert.NotNil(t, aggregator.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.NotNil(t, aggregator.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, aggregator.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "replace")
	assert.Equal(t, aggregator.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.Value, "date")
	assert.Equal(t, aggregator.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[0].RawString, `^(\d{1,2})/(\d{1,2})/`)
	assert.Equal(t, aggregator.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[1].RawString, `\2/\1/`)

	assert.Nil(t, aggregator.Next.Next)
}

func Test_GenTimes_8(t *testing.T) {
	query := `| gentimes start=10/01/2022 end=12/03/2023:12:20:56 increment=2`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)
	assert.Equal(t, structs.GenerateEventType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.GenerateEvent)
	assert.NotNil(t, aggregator.GenerateEvent.GenTimes)

	expectedStartTime, err := utils.ConvertCustomDateTimeFormatToEpochMs("10/01/2022:00:00:00")
	assert.Nil(t, err)
	assert.Equal(t, uint64(expectedStartTime), aggregator.GenerateEvent.GenTimes.StartTime)

	expectedEndTime, err := utils.ConvertCustomDateTimeFormatToEpochMs("12/03/2023:12:20:56")
	assert.Nil(t, err)
	assert.Equal(t, uint64(expectedEndTime), aggregator.GenerateEvent.GenTimes.EndTime)

	assert.NotNil(t, aggregator.GenerateEvent.GenTimes.Interval)
	assert.Equal(t, 2, aggregator.GenerateEvent.GenTimes.Interval.Num)
	assert.Equal(t, utils.TMSecond, aggregator.GenerateEvent.GenTimes.Interval.TimeScalr)
}

func Test_ParseRelativeTimeModifier_Chained_1(t *testing.T) {
	query := `* | earliest=-mon@mon latest=+mon@mon+7d`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, astNode.TimeRange)

	// Get the current time in the local time zone
	now := time.Now().In(time.Local)

	// Calculate the expected earliest time: one month ago, snapped to the first of the month at midnight
	firstOfLastMonth := time.Date(now.Year(), now.Month()-1, 1, 0, 0, 0, 0, time.Local)
	expectedEarliestTime := firstOfLastMonth

	// Calculate the expected latest time: one month from now, snapped to the first of the month at midnight, plus 7 days
	firstOfNextMonth := time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.Local)
	expectedLatestTime := firstOfNextMonth.AddDate(0, 0, 7)

	// Convert the actual times from Unix milliseconds to local time
	actualEarliestTime := time.UnixMilli(int64(astNode.TimeRange.StartEpochMs)).In(time.Local)
	actualLatestTime := time.UnixMilli(int64(astNode.TimeRange.EndEpochMs)).In(time.Local)

	// Compare the expected and actual times
	assert.Equal(t, expectedEarliestTime, actualEarliestTime)
	assert.Equal(t, expectedLatestTime, actualLatestTime)
}

func Test_ParseRelativeTimeModifier_Chained_2(t *testing.T) {
	query := `* | earliest=@d-1d+12h latest=@d-1s`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, astNode.TimeRange)

	// Get the current time in the local time zone
	now := time.Now().In(time.Local)

	// Calculate the expected earliest time: yesterday at noon
	yesterdayNoon := time.Date(now.Year(), now.Month(), now.Day()-1, 12, 0, 0, 0, time.Local)
	expectedEarliestTime := yesterdayNoon

	// Calculate the expected latest time: end of yesterday
	endOfYesterday := time.Date(now.Year(), now.Month(), now.Day()-1, 23, 59, 59, 0, time.Local)
	expectedLatestTime := endOfYesterday

	// Convert the actual times from Unix milliseconds to local time
	actualEarliestTime := time.UnixMilli(int64(astNode.TimeRange.StartEpochMs)).In(time.Local)
	actualLatestTime := time.UnixMilli(int64(astNode.TimeRange.EndEpochMs)).In(time.Local)

	// Compare the expected and actual times
	assert.Equal(t, expectedEarliestTime, actualEarliestTime)
	assert.Equal(t, expectedLatestTime, actualLatestTime)
}

func Test_ParseRelativeTimeModifier_Chained_3(t *testing.T) {
	query := `* | earliest=@w1-7d+9h latest=@w1-7d+17h`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, astNode.TimeRange)

	// Get the current time in the local time zone
	now := time.Now().In(time.Local)

	daysToSubtract := int(now.Weekday())
	if daysToSubtract == 0 { // Check if it's Sunday
		daysToSubtract = 7
	}
	// Calculate the expected earliest time: last week's Monday at 9 AM
	lastMonday9AM := time.Date(now.Year(), now.Month(), now.Day()-daysToSubtract-7+int(time.Monday), 9, 0, 0, 0, now.Location())
	expectedEarliestTime := lastMonday9AM

	// Calculate the expected latest time: last week's Monday at 5 PM
	lastMonday5PM := time.Date(now.Year(), now.Month(), now.Day()-daysToSubtract-7+int(time.Monday), 17, 0, 0, 0, now.Location())
	expectedLatestTime := lastMonday5PM

	// Convert the actual times from Unix milliseconds to local time
	actualEarliestTime := time.UnixMilli(int64(astNode.TimeRange.StartEpochMs)).In(time.Local)
	actualLatestTime := time.UnixMilli(int64(astNode.TimeRange.EndEpochMs)).In(time.Local)

	// Compare the expected and actual times
	assert.Equal(t, expectedEarliestTime, actualEarliestTime)
	assert.Equal(t, expectedLatestTime, actualLatestTime)
}

func Test_ParseRelativeTimeModifier_Chained_4(t *testing.T) {
	query := `* | earliest=-26h@h latest=-2h@h`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, astNode.TimeRange)

	// Get the current time in the local time zone
	now := time.Now().In(time.Local)

	// Manually floor the time to the start of the hour
	floorToHour := func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	}

	// Calculate the expected earliest time: 26 hours ago, floored to the start of the hour
	expectedEarliestTime := floorToHour(now.Add(-26 * time.Hour))

	// Calculate the expected latest time: 2 hours ago, floored to the start of the hour
	expectedLatestTime := floorToHour(now.Add(-2 * time.Hour))

	// Convert the actual times from Unix milliseconds to local time
	actualEarliestTime := time.UnixMilli(int64(astNode.TimeRange.StartEpochMs)).In(time.Local)
	actualLatestTime := time.UnixMilli(int64(astNode.TimeRange.EndEpochMs)).In(time.Local)

	assert.Equal(t, expectedEarliestTime, actualEarliestTime)
	assert.Equal(t, expectedLatestTime, actualLatestTime)
}

func Test_ParseRelativeTimeModifier_Chained_5(t *testing.T) {
	query := `* | earliest=-1h@h latest=-45m@m`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, astNode.TimeRange)

	// Get the current time in the local time zone
	now := time.Now().In(time.Local)

	// Manually floor the time to the start of the hour
	floorToHour := func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, t.Location())
	}

	// Manually floor the time to the start of the minute
	floorToMinute := func(t time.Time) time.Time {
		return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	}

	// Calculate the expected earliest time: 1 hour ago, floored to the start of the hour
	expectedEarliestTime := floorToHour(now.Add(-1 * time.Hour))

	// Calculate the expected latest time: 45 minutes ago, floored to the start of the minute
	expectedLatestTime := floorToMinute(now.Add(-45 * time.Minute))

	// Convert the actual times from Unix milliseconds to local time
	actualEarliestTime := time.UnixMilli(int64(astNode.TimeRange.StartEpochMs)).In(time.Local)
	actualLatestTime := time.UnixMilli(int64(astNode.TimeRange.EndEpochMs)).In(time.Local)

	assert.Equal(t, expectedEarliestTime, actualEarliestTime)
	assert.Equal(t, expectedLatestTime, actualLatestTime)
}
func Test_InputLookup(t *testing.T) {
	query := `| inputlookup mylookup.csv`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)
	assert.Equal(t, structs.GenerateEventType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.GenerateEvent)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup)

	assert.Equal(t, "mylookup.csv", aggregator.GenerateEvent.InputLookup.Filename)
	assert.Equal(t, uint64(1000000000), aggregator.GenerateEvent.InputLookup.Max)
	assert.Equal(t, uint64(0), aggregator.GenerateEvent.InputLookup.Start)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.Strict)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.Append)
	assert.Nil(t, aggregator.GenerateEvent.InputLookup.WhereExpr)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.HasPrevResults)
}

func Test_InputLookup_2(t *testing.T) {
	query := `| inputlookup start=3 abc.csv`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)
	assert.Equal(t, structs.GenerateEventType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.GenerateEvent)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup)

	assert.Equal(t, "abc.csv", aggregator.GenerateEvent.InputLookup.Filename)
	assert.Equal(t, uint64(1000000000), aggregator.GenerateEvent.InputLookup.Max)
	assert.Equal(t, uint64(3), aggregator.GenerateEvent.InputLookup.Start)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.Strict)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.Append)
	assert.Nil(t, aggregator.GenerateEvent.InputLookup.WhereExpr)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.HasPrevResults)
}

func Test_InputLookup_3(t *testing.T) {
	query := `| inputlookup start=5 strict=true max=3 append=true abc.csv`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)
	assert.Equal(t, structs.GenerateEventType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.GenerateEvent)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup)

	assert.Equal(t, "abc.csv", aggregator.GenerateEvent.InputLookup.Filename)
	assert.Equal(t, uint64(3), aggregator.GenerateEvent.InputLookup.Max)
	assert.Equal(t, uint64(5), aggregator.GenerateEvent.InputLookup.Start)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.Strict)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.Append)
	assert.Nil(t, aggregator.GenerateEvent.InputLookup.WhereExpr)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.HasPrevResults)
}

func Test_InputLookup_4(t *testing.T) {
	query := `| inputlookup max=3 append=true abc.csv where a="text" OR b>2 AND c<=3`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)
	assert.Equal(t, structs.GenerateEventType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.GenerateEvent)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup)

	assert.Equal(t, "abc.csv", aggregator.GenerateEvent.InputLookup.Filename)
	assert.Equal(t, uint64(3), aggregator.GenerateEvent.InputLookup.Max)
	assert.Equal(t, uint64(0), aggregator.GenerateEvent.InputLookup.Start)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.Strict)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.Append)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.HasPrevResults)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool)
	assert.Equal(t, structs.BoolOpOr, aggregator.GenerateEvent.InputLookup.WhereExpr.BoolOp)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.LeftValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.LeftValue.NumericExpr)
	assert.Equal(t, "a", aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, "=", aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.ValueOp)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.RightValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.RightValue.StringExpr)
	assert.Equal(t, "text", aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.RightValue.StringExpr.RawString)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool)
	assert.Nil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftValue)
	assert.Nil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool)

	assert.Equal(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.BoolOp, structs.BoolOpAnd)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.LeftValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.LeftValue.NumericExpr)
	assert.Equal(t, "b", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, ">", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.ValueOp)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.RightValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.RightValue.NumericExpr)
	assert.Equal(t, "2", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.RightValue.NumericExpr.IsTerminal)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.LeftValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.LeftValue.NumericExpr)
	assert.Equal(t, "c", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, "<=", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.ValueOp)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.RightValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.RightValue.NumericExpr)
	assert.Equal(t, "3", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.RightValue.NumericExpr.IsTerminal)
}

func Test_InputLookup_5(t *testing.T) {
	query := `| inputlookup max=3 abc.csv where a="text" OR b>2 AND c<=3 | eval myField=replace(date, "^(\d{1,2})/(\d{1,2})/", "\2/\1/")`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.NotNil(t, aggregator.Next)
	assert.Equal(t, structs.GenerateEventType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.GenerateEvent)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup)

	assert.Equal(t, "abc.csv", aggregator.GenerateEvent.InputLookup.Filename)
	assert.Equal(t, uint64(3), aggregator.GenerateEvent.InputLookup.Max)
	assert.Equal(t, uint64(0), aggregator.GenerateEvent.InputLookup.Start)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.Strict)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.Append)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.HasPrevResults)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool)
	assert.Equal(t, structs.BoolOpOr, aggregator.GenerateEvent.InputLookup.WhereExpr.BoolOp)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.LeftValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.LeftValue.NumericExpr)
	assert.Equal(t, "a", aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, "=", aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.ValueOp)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.RightValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.RightValue.StringExpr)
	assert.Equal(t, "text", aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.RightValue.StringExpr.RawString)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool)
	assert.Nil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftValue)
	assert.Nil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool)

	assert.Equal(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.BoolOp, structs.BoolOpAnd)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.LeftValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.LeftValue.NumericExpr)
	assert.Equal(t, "b", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, ">", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.ValueOp)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.RightValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.RightValue.NumericExpr)
	assert.Equal(t, "2", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.RightValue.NumericExpr.IsTerminal)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.LeftValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.LeftValue.NumericExpr)
	assert.Equal(t, "c", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, "<=", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.ValueOp)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.RightValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.RightValue.NumericExpr)
	assert.Equal(t, "3", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.RightValue.NumericExpr.IsTerminal)

	assert.Equal(t, aggregator.Next.PipeCommandType, structs.OutputTransformType)
	assert.NotNil(t, aggregator.Next.OutputTransforms.LetColumns)
	assert.Equal(t, aggregator.Next.OutputTransforms.LetColumns.NewColName, "myField")
	assert.NotNil(t, aggregator.Next.OutputTransforms.LetColumns.ValueColRequest)
	assert.Equal(t, int(aggregator.Next.OutputTransforms.LetColumns.ValueColRequest.ValueExprMode), structs.VEMStringExpr)
	assert.NotNil(t, aggregator.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr)
	assert.NotNil(t, aggregator.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr)
	assert.Equal(t, aggregator.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Op, "replace")
	assert.Equal(t, aggregator.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.Val.NumericExpr.Value, "date")
	assert.Equal(t, aggregator.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[0].RawString, `^(\d{1,2})/(\d{1,2})/`)
	assert.Equal(t, aggregator.Next.OutputTransforms.LetColumns.ValueColRequest.StringExpr.TextExpr.ValueList[1].RawString, `\2/\1/`)

	assert.Nil(t, aggregator.Next.Next)
}

func Test_InputLookup_6(t *testing.T) {
	query := `city=Boston | inputlookup max=3 append=true abc.csv where a="text" OR b>2 AND c<=3`
	res, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	assert.Nil(t, err)
	filterNode := res.(ast.QueryStruct).SearchFilter
	assert.NotNil(t, filterNode)

	assert.Equal(t, ast.NodeTerminal, filterNode.NodeType)
	assert.Equal(t, "city", filterNode.Comparison.Field)
	assert.Equal(t, "=", filterNode.Comparison.Op)
	assert.Equal(t, "\"Boston\"", filterNode.Comparison.Values)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")

	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)
	assert.Equal(t, structs.GenerateEventType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.GenerateEvent)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup)

	assert.Equal(t, "abc.csv", aggregator.GenerateEvent.InputLookup.Filename)
	assert.Equal(t, uint64(3), aggregator.GenerateEvent.InputLookup.Max)
	assert.Equal(t, uint64(0), aggregator.GenerateEvent.InputLookup.Start)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.Strict)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.Append)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.HasPrevResults)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool)
	assert.Equal(t, structs.BoolOpOr, aggregator.GenerateEvent.InputLookup.WhereExpr.BoolOp)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.LeftValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.LeftValue.NumericExpr)
	assert.Equal(t, "a", aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, "=", aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.ValueOp)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.RightValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.RightValue.StringExpr)
	assert.Equal(t, "text", aggregator.GenerateEvent.InputLookup.WhereExpr.LeftBool.RightValue.StringExpr.RawString)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool)
	assert.Nil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftValue)
	assert.Nil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool)

	assert.Equal(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.BoolOp, structs.BoolOpAnd)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.LeftValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.LeftValue.NumericExpr)
	assert.Equal(t, "b", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, ">", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.ValueOp)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.RightValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.RightValue.NumericExpr)
	assert.Equal(t, "2", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.LeftBool.RightValue.NumericExpr.IsTerminal)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.LeftValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.LeftValue.NumericExpr)
	assert.Equal(t, "c", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.LeftValue.NumericExpr.IsTerminal)

	assert.Equal(t, "<=", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.ValueOp)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.RightValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.RightValue.NumericExpr)
	assert.Equal(t, "3", aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.RightBool.RightBool.RightValue.NumericExpr.IsTerminal)
}

func Test_InputLookup_7(t *testing.T) {
	query := `city=Boston | inputlookup max=3 abc.csv where a="text" OR b>2 AND c<=3`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_InputLookup_8(t *testing.T) {
	query := `| inputlookup max=-1 abc.csv`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_InputLookup_9(t *testing.T) {
	query := `| inputlookup start=-1 abc.csv`
	_, err := spl.Parse("", []byte(query))
	assert.NotNil(t, err)
}

func Test_InputLookup_10(t *testing.T) {
	query := `| inputlookup myfile where (a=b)`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)
	assert.Equal(t, structs.GenerateEventType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.GenerateEvent)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup)

	assert.Equal(t, "myfile", aggregator.GenerateEvent.InputLookup.Filename)
	assert.Equal(t, uint64(1000000000), aggregator.GenerateEvent.InputLookup.Max)
	assert.Equal(t, uint64(0), aggregator.GenerateEvent.InputLookup.Start)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.Strict)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.Append)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.HasPrevResults)

	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftValue.NumericExpr)
	assert.Equal(t, "a", aggregator.GenerateEvent.InputLookup.WhereExpr.LeftValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.LeftValue.NumericExpr.IsTerminal)
	assert.Equal(t, "=", aggregator.GenerateEvent.InputLookup.WhereExpr.ValueOp)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightValue)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup.WhereExpr.RightValue.NumericExpr)
	assert.Equal(t, "b", aggregator.GenerateEvent.InputLookup.WhereExpr.RightValue.NumericExpr.Value)
	assert.Equal(t, true, aggregator.GenerateEvent.InputLookup.WhereExpr.RightValue.NumericExpr.IsTerminal)
}

func Test_InputLookup_11(t *testing.T) {
	query := `| inputlookup myfile.csv`
	_, err := spl.Parse("", []byte(query))
	assert.Nil(t, err)

	astNode, aggregator, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.Nil(t, err)
	assert.NotNil(t, astNode)
	assert.NotNil(t, aggregator)
	assert.Nil(t, aggregator.Next)
	assert.Equal(t, structs.GenerateEventType, aggregator.PipeCommandType)
	assert.NotNil(t, aggregator.GenerateEvent)
	assert.NotNil(t, aggregator.GenerateEvent.InputLookup)

	assert.Equal(t, "myfile.csv", aggregator.GenerateEvent.InputLookup.Filename)
	assert.Equal(t, uint64(1000000000), aggregator.GenerateEvent.InputLookup.Max)
	assert.Equal(t, uint64(0), aggregator.GenerateEvent.InputLookup.Start)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.Strict)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.Append)
	assert.Equal(t, false, aggregator.GenerateEvent.InputLookup.HasPrevResults)
}

func Test_RemoveRedundantSearches(t *testing.T) {
	query := `* | search foo=bar`
	astNode, aggregator := parseWithoutError(t, query)

	equivalentQuery := `foo=bar`
	expectedAstNode, expectedAggregator := parseWithoutError(t, equivalentQuery)

	assert.Equal(t, expectedAstNode, astNode)
	assert.Equal(t, expectedAggregator, aggregator)

	query = `foo=bar | search *`
	astNode, aggregator = parseWithoutError(t, query)
	assert.Equal(t, expectedAstNode, astNode)
	assert.Equal(t, expectedAggregator, aggregator)

	query = `* | search foo=bar | search *`
	astNode, aggregator = parseWithoutError(t, query)
	assert.Equal(t, expectedAstNode, astNode)
	assert.Equal(t, expectedAggregator, aggregator)

	query = `* | search * | search foo=bar`
	astNode, aggregator = parseWithoutError(t, query)
	assert.Equal(t, expectedAstNode, astNode)
	assert.Equal(t, expectedAggregator, aggregator)

	query = `* | search * | search foo=bar | search *`
	astNode, aggregator = parseWithoutError(t, query)
	assert.Equal(t, expectedAstNode, astNode)
	assert.Equal(t, expectedAggregator, aggregator)

	query = `foo=bar | search * | search *`
	astNode, aggregator = parseWithoutError(t, query)
	assert.Equal(t, expectedAstNode, astNode)
	assert.Equal(t, expectedAggregator, aggregator)

	query = `* | search * | search foo=bar | search * | search *`
	astNode, aggregator = parseWithoutError(t, query)
	assert.Equal(t, expectedAstNode, astNode)
	assert.Equal(t, expectedAggregator, aggregator)
}
