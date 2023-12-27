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
	query_string := "SELECT ROUND(latency, 1) FROM `*` GROUP BY country"

	_, aggs, _, err := ConvertToASTNodeSQL(query_string, 0)

	assert.Nil(t, err)

	assert.NotNil(t, aggs.OutputTransforms.LetColumns)

	assert.NotNil(t, aggs.OutputTransforms.LetColumns.ValueColRequest)

	assert.NotNil(t, aggs.OutputTransforms.LetColumns.ValueColRequest.NumericExpr)

	assert.NotNil(t, aggs.GroupByRequest)

	leftExpr := aggs.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Left
	rightExpr := aggs.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Right
	Op := aggs.OutputTransforms.LetColumns.ValueColRequest.NumericExpr.Op

	assert.Equal(t, Op, "round")
	assert.Equal(t, leftExpr.Value, "0(latency)")
	assert.Equal(t, rightExpr.Value, "1")
	assert.Equal(t, leftExpr.ValueIsField, true)
	assert.Equal(t, rightExpr.ValueIsField, false)
	assert.Equal(t, aggs.OutputTransforms.LetColumns.NewColName, "Round(0(latency))")

	assert.NotNil(t, aggs.Next)

	next := aggs.Next

	assert.NotNil(t, next.OutputTransforms)
	assert.NotNil(t, next.OutputTransforms.LetColumns)
	assert.Equal(t, next.PipeCommandType, structs.PipeCommandType(0x1))

	query_string = "SELECT ROUND(sum(latitude), 2) FROM `*` GROUP BY city"

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
	assert.Equal(t, aggs.OutputTransforms.LetColumns.NewColName, "Round(sum(latitude))")
}
