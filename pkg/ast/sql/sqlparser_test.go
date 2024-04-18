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

package sql

import (
	"testing"

	query "github.com/siglens/siglens/pkg/es/query"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_ParseSelect(t *testing.T) {
	query_string := "SELECT COUNT(latency) FROM `*` GROUP BY country"
	_, aggs, _, err := ConvertToASTNodeSQL(query_string, 0)
	assert.Nil(t, err)

	testMeasureOp := append(make([]*structs.MeasureAggregator, 0), &structs.MeasureAggregator{
		MeasureCol: "latency", MeasureFunc: utils.Count,
	})
	testGroupBy := &structs.GroupByRequest{GroupByColumns: make([]string, 0), MeasureOperations: make([]*structs.MeasureAggregator, 0), BucketCount: 100}
	testGroupBy.GroupByColumns = append(testGroupBy.GroupByColumns, "country")
	testGroupBy.MeasureOperations = append(testGroupBy.MeasureOperations, &structs.MeasureAggregator{
		MeasureCol: "latency", MeasureFunc: utils.Count,
	})
	_, err = query.GetMatchAllASTNode(0)

	assert.Nil(t, err)
	assert.Equal(t, aggs.GroupByRequest, testGroupBy)
	assert.Equal(t, aggs.MeasureOperations, testMeasureOp)
}

func Test_ParseSelectAliased(t *testing.T) {
	testRenameColumns := make(map[string]string, 0)
	query_string := "select batch as bt from ind-0"
	_, aggs, _, err := ConvertToASTNodeSQL(query_string, 0)
	assert.Nil(t, err)
	testRenameColumns["batch"] = "bt"
	assert.Equal(t, aggs.OutputTransforms.OutputColumns.RenameColumns, testRenameColumns)

	query_string = "select city, batch as bt from ind-0"
	_, aggs, _, err = ConvertToASTNodeSQL(query_string, 0)
	assert.Nil(t, err)
	assert.Equal(t, aggs.OutputTransforms.OutputColumns.RenameColumns, testRenameColumns)
	assert.Equal(t, aggs.OutputTransforms.OutputColumns.IncludeColumns[0], "city")

	query_string = "select batch as bt from `ind-0`"
	_, aggs, _, err = ConvertToASTNodeSQL(query_string, 0)
	assert.Nil(t, err)
	assert.Equal(t, aggs.OutputTransforms.OutputColumns.RenameColumns, testRenameColumns)

	query_string = "select batch as bt, city as ct from `*`"
	_, aggs, _, err = ConvertToASTNodeSQL(query_string, 0)
	assert.Nil(t, err)
	testRenameColumns["city"] = "ct"
	assert.Equal(t, aggs.OutputTransforms.OutputColumns.RenameColumns, testRenameColumns)

	query_string = "select batch as bt, city as `ct` from ind-0"
	_, aggs, _, err = ConvertToASTNodeSQL(query_string, 0)
	assert.Nil(t, err)
	assert.Equal(t, aggs.OutputTransforms.OutputColumns.RenameColumns, testRenameColumns)

	query_string = "select batch as bt, city as `ct`, country as cy, host from ind-0"
	_, aggs, _, err = ConvertToASTNodeSQL(query_string, 0)
	testRenameColumns["country"] = "cy"
	assert.Nil(t, err)
	assert.Equal(t, aggs.OutputTransforms.OutputColumns.RenameColumns, testRenameColumns)
	assert.Equal(t, aggs.OutputTransforms.OutputColumns.IncludeColumns[3], "host")

}

func Test_ParseOrderBy(t *testing.T) {
	query_string := "select batch as bt, city as `ct`, country as cy, host from ind-0 order by batch"
	_, aggs, _, err := ConvertToASTNodeSQL(query_string, 0)
	assert.Nil(t, err)
	assert.Equal(t, aggs.Sort.Ascending, true)
	assert.Equal(t, aggs.Sort.ColName, "batch")

	query_string = "select batch as bt, city as `ct`, country as cy, host from ind-0 order by batch desc"
	_, aggs, _, err = ConvertToASTNodeSQL(query_string, 0)
	assert.Nil(t, err)
	assert.Equal(t, aggs.Sort.Ascending, false)
	assert.Equal(t, aggs.Sort.ColName, "batch")

	query_string = "select batch as bt, city as `ct`, country as cy, host from ind-0 order by batch asc"
	_, aggs, _, err = ConvertToASTNodeSQL(query_string, 0)
	assert.Nil(t, err)
	assert.Equal(t, aggs.Sort.Ascending, true)
	assert.Equal(t, aggs.Sort.ColName, "batch")

}

func Test_ParseShow(t *testing.T) {
	query_show := "show columns from `ind-0`"
	_, aggs, _, err := ConvertToASTNodeSQL(query_show, 0)
	assert.Nil(t, err)
	testShowRequest := &structs.ShowRequest{ColumnsRequest: &structs.ShowColumns{InTable: "ind-0"}}
	assert.Equal(t, aggs.ShowRequest, testShowRequest)

	query_describe := "describe ind-0"
	_, aggs, _, err = ConvertToASTNodeSQL(query_describe, 0)
	assert.Nil(t, err)
	assert.Equal(t, aggs.ShowRequest, testShowRequest)
}

func Test_ParseGroupByRound(t *testing.T) {
	query_string := "SELECT ROUND(sum(latitude), 2) FROM `*` GROUP BY city"

	_, aggs, _, err := ConvertToASTNodeSQL(query_string, 0)

	assert.Nil(t, err)

	assert.NotNil(t, aggs.OutputTransforms.LetColumns)
	assert.NotNil(t, aggs.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, aggs.OutputTransforms.LetColumns.ValueColRequest.NumericExpr)

	leftExpr := aggs.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left
	rightExpr := aggs.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right
	Op := aggs.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op

	assert.Equal(t, Op, "round")
	assert.Equal(t, leftExpr.Value, "sum(latitude)")
	assert.Equal(t, rightExpr.Value, "2")
	assert.Equal(t, leftExpr.ValueIsField, true)
	assert.Equal(t, rightExpr.ValueIsField, false)
	assert.Nil(t, leftExpr.Val)
	assert.Equal(t, aggs.OutputTransforms.LetColumns.NewColName, "round(sum(latitude))")

	// round with alias
	query_string = "SELECT ROUND(sum(latitude), 2) as lat_sum FROM `*` GROUP BY city"
	_, aggs, _, err = ConvertToASTNodeSQL(query_string, 0)

	assert.Nil(t, err)

	assert.NotNil(t, aggs.OutputTransforms.LetColumns)

	assert.NotNil(t, aggs.OutputTransforms.LetColumns.ValueColRequest)

	assert.NotNil(t, aggs.OutputTransforms.LetColumns.ValueColRequest.NumericExpr)

	leftExpr = aggs.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left
	rightExpr = aggs.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right
	Op = aggs.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op

	assert.Equal(t, Op, "round")
	assert.Equal(t, leftExpr.Value, "sum(latitude)")
	assert.Equal(t, rightExpr.Value, "2")
	assert.Equal(t, leftExpr.ValueIsField, true)
	assert.Equal(t, rightExpr.ValueIsField, false)
	assert.Nil(t, leftExpr.Val)
	assert.Equal(t, aggs.OutputTransforms.LetColumns.NewColName, "lat_sum")
}

func Test_ParseMathFunctions(t *testing.T) {
	query_string := "SELECT city, ROUND(latitude, 2)"

	_, aggs, _, err := ConvertToASTNodeSQL(query_string, 0)

	assert.Nil(t, err)
	assert.NotNil(t, aggs.MathOperations)
	assert.Equal(t, aggs.MathOperations[0].MathCol, "latitude")
	assert.Equal(t, aggs.MathOperations[0].MathFunc, utils.Round)
	assert.Equal(t, aggs.MathOperations[0].ValueColRequest.NumericExpr.Op, "round")
	assert.Equal(t, aggs.MathOperations[0].ValueColRequest.NumericExpr.Left.Value, "latitude")
	assert.Equal(t, aggs.MathOperations[0].ValueColRequest.NumericExpr.Right.Value, "2")

	// math function with alias
	query_string = "SELECT city, ROUND(latitude, 2) as lat_round"

	_, aggs, _, err = ConvertToASTNodeSQL(query_string, 0)

	assert.Nil(t, err)
	assert.NotNil(t, aggs.MathOperations)
	assert.Equal(t, aggs.MathOperations[0].MathCol, "latitude")
	assert.Equal(t, aggs.MathOperations[0].MathFunc, utils.Round)
	assert.Equal(t, aggs.MathOperations[0].ValueColRequest.NumericExpr.Op, "round")
	assert.Equal(t, aggs.MathOperations[0].ValueColRequest.NumericExpr.Left.Value, "latitude")
	assert.Equal(t, aggs.MathOperations[0].ValueColRequest.NumericExpr.Right.Value, "2")
	assert.Equal(t, aggs.OutputTransforms.OutputColumns.RenameColumns["latitude"], "lat_round")

	// another math function: abs
	query_string = "SELECT city, ABS(latitude) as lat_abs"

	_, aggs, _, err = ConvertToASTNodeSQL(query_string, 0)

	assert.Nil(t, err)
	assert.NotNil(t, aggs.MathOperations)
	assert.Equal(t, aggs.MathOperations[0].MathCol, "latitude")
	assert.Equal(t, aggs.MathOperations[0].MathFunc, utils.Abs)
	assert.Equal(t, aggs.MathOperations[0].ValueColRequest.NumericExpr.Op, "abs")
	assert.Equal(t, aggs.MathOperations[0].ValueColRequest.NumericExpr.Left.Value, "latitude")
	assert.Equal(t, aggs.OutputTransforms.OutputColumns.RenameColumns["latitude"], "lat_abs")
}
