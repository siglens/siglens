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

package querytracker

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/siglens/siglens/pkg/config"
	. "github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/valyala/fasthttp"
)

// used only by tests to reset tracked info
func resetInternalQTInfo() {
	localPersistentAggs = make(map[string]*PersistentAggregation)
	localPersistentQueries = make(map[string]*PersistentSearchNode)
	allNodesPQsSorted = []*PersistentSearchNode{}
	allPersistentAggsSorted = []*PersistentAggregation{}
}

func Test_GetQTUsageInfo(t *testing.T) {
	resetInternalQTInfo()
	config.SetPQSEnabled(true)
	qVal, err := sutils.CreateDtypeEnclosure("iOS", 0)
	assert.Nil(t, err)

	sNode := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{
				{
					ExpressionFilter: &SearchExpression{
						LeftSearchInput:  &SearchExpressionInput{ColumnName: "os"},
						FilterOp:         sutils.Equals,
						RightSearchInput: &SearchExpressionInput{ColumnValue: qVal},
					},
					SearchType: SimpleExpression,
				},
			},
		},
		NodeType: ColumnValueQuery,
	}
	sNodeHash := GetHashForQuery(sNode)
	tableName := []string{"ind-tab-v1"}
	for i := 0; i < 90; i++ {
		UpdateQTUsage(tableName, sNode, nil, "os=iOS")
	}

	us, err := GetQTUsageInfo(tableName, sNode)
	assert.Nil(t, err)
	assert.NotNil(t, us)
	expected := uint32(90)
	assert.Equal(t, expected, us.TotalUsage, "expected %v usagecount but got %v", expected, us.TotalUsage)

	ok, err := IsQueryPersistent(tableName, sNode)
	assert.Nil(t, err)
	assert.Equal(t, true, ok, "query was supposed to be persistent")

	sNode.AndSearchConditions.SearchQueries[0].ExpressionFilter.LeftSearchInput.ColumnName = "os2"
	ok, err = IsQueryPersistent(tableName, sNode)
	assert.Nil(t, err)
	assert.Equal(t, false, ok, "query was supposed to be NOT persistent")

	res, err := GetTopNPersistentSearches(tableName[0], 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(res), "There should be 1 persistent query but got=%v", len(res))

	wildCard, err := sutils.CreateDtypeEnclosure("*", 0)
	assert.Nil(t, err)

	matchAllOne := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{
				{
					ExpressionFilter: &SearchExpression{
						LeftSearchInput:  &SearchExpressionInput{ColumnName: "*"},
						FilterOp:         sutils.Equals,
						RightSearchInput: &SearchExpressionInput{ColumnValue: wildCard},
					},
					SearchType: SimpleExpression,
				},
			},
		},
		NodeType: MatchAllQuery,
	}

	UpdateQTUsage(tableName, matchAllOne, nil, "*")

	_, err = GetQTUsageInfo(tableName, matchAllOne)
	assert.NotNil(t, err, "match all should not be added to query tracker")

	ok, err = IsQueryPersistent(tableName, matchAllOne)
	assert.Nil(t, err)
	assert.Equal(t, false, ok, "query is  not persistent")

	matchAllHash := GetHashForQuery(matchAllOne)
	res, err = GetTopNPersistentSearches(tableName[0], 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(res), "There should be 1 persistent query but got=%v", len(res))
	assert.Contains(t, res, sNodeHash, "sNodeHash=%v should exist in result=%+v", sNodeHash, res)
	assert.NotContains(t, res, matchAllHash, "matchAllHash=%v should not exist in result=%+v", matchAllHash, res)
	assert.Equal(t, ColumnValueQuery, res[sNodeHash].NodeType, "non match all result %+v should exist", res[sNodeHash])
	assert.Nil(t, res[matchAllHash], "match all result %+v should exist", res[matchAllHash])
}

func Test_ReadWriteQTUsage(t *testing.T) {
	resetInternalQTInfo()
	config.SetPQSEnabled(true)
	config.SetSSInstanceName("qt-test")
	err := config.InitDerivedConfig("test")
	assert.NoError(t, err)
	_ = os.RemoveAll("./ingestnodes")
	_ = os.RemoveAll("./querynodes")

	qVal, err := sutils.CreateDtypeEnclosure("batch-101", 0)
	assert.Nil(t, err)
	sNode := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{
				{
					ExpressionFilter: &SearchExpression{
						LeftSearchInput:  &SearchExpressionInput{ColumnName: "batch"},
						FilterOp:         sutils.Equals,
						RightSearchInput: &SearchExpressionInput{ColumnValue: qVal},
					},
					SearchType: SimpleExpression,
				},
			},
		},
		NodeType: ColumnValueQuery,
	}

	sNodeHash := GetHashForQuery(sNode)
	tableName := []string{"test-1"}

	aggs := &QueryAggregators{
		GroupByRequest: &GroupByRequest{
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "col3", MeasureFunc: sutils.Avg},
				{MeasureCol: "col4", MeasureFunc: sutils.Count},
			},
			GroupByColumns: []string{"col1", "col2"},
		},
	}
	aggsHash := GetHashForAggs(aggs)
	UpdateQTUsage(tableName, sNode, aggs, "batch=batch-101")

	flushPQueriesToDisk()
	resetInternalQTInfo()
	readSavedQueryInfo()
	assert.Len(t, allNodesPQsSorted, 1)
	assert.Len(t, localPersistentQueries, 1)
	assert.Len(t, allPersistentAggsSorted, 1)
	assert.Len(t, localPersistentAggs, 1)

	assert.Contains(t, localPersistentQueries, sNodeHash)
	assert.Contains(t, localPersistentAggs, aggsHash)
}

func Test_GetTopPersistentAggs(t *testing.T) {
	resetInternalQTInfo()
	config.SetPQSEnabled(true)
	aggs := &QueryAggregators{
		GroupByRequest: &GroupByRequest{
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "col3", MeasureFunc: sutils.Avg},
				{MeasureCol: "col4", MeasureFunc: sutils.Count},
			},
			GroupByColumns: []string{"col1", "col2"},
		},
	}
	tableName := []string{"test-1"}
	UpdateQTUsage(tableName, nil, aggs, "*")
	grpCols, measure := GetTopPersistentAggs("test-2")
	assert.Len(t, grpCols, 0)
	assert.Len(t, measure, 0)

	grpCols, measure = GetTopPersistentAggs("test-1")
	assert.Len(t, grpCols, 2)
	assert.Len(t, measure, 2)
	assert.Contains(t, grpCols, "col1")
	assert.Contains(t, grpCols, "col2")

	var mCol3 string
	var mCol4 string
	for mcol := range measure {
		if mcol == "col3" {
			mCol3 = mcol
		} else if mcol == "col4" {
			mCol4 = mcol
		}
	}
	assert.NotEqual(t, "", mCol3)
	assert.NotEqual(t, "", mCol4)

	aggs2 := &QueryAggregators{
		GroupByRequest: &GroupByRequest{
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "col3", MeasureFunc: sutils.Cardinality},
			},
			GroupByColumns: []string{"col3", "col2"},
		},
	}
	UpdateQTUsage(tableName, nil, aggs2, "*")
	grpCols, measure = GetTopPersistentAggs("test-1")
	assert.Len(t, grpCols, 3)
	_, ok := grpCols["col2"]
	assert.True(t, ok, "col2 must exist")
	var mCol3_1 string
	mCol4 = ""
	for m := range measure {
		if m == "col3" {
			if mCol3_1 == "" {
				mCol3_1 = m
			}
		} else if m == "col4" {
			mCol4 = m
		}
	}
	assert.NotEqual(t, "", mCol3_1)
	assert.NotEqual(t, "", mCol4)
}

func Test_GetTopPersistentAggs_Jaeger(t *testing.T) {
	resetInternalQTInfo()
	config.SetPQSEnabled(true)
	aggs := &QueryAggregators{
		GroupByRequest: &GroupByRequest{
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "col3", MeasureFunc: sutils.Avg},
				{MeasureCol: "col4", MeasureFunc: sutils.Count},
			},
			GroupByColumns: []string{"col1", "col2"},
		},
	}
	tableName := []string{"jaeger-1"}
	UpdateQTUsage(tableName, nil, aggs, "*")

	grpCols, measure := GetTopPersistentAggs("jaeger-1")
	assert.Len(t, grpCols, 5)
	assert.Len(t, measure, 3)
	assert.Contains(t, grpCols, "col1")
	assert.Contains(t, grpCols, "col2")
	assert.Contains(t, grpCols, "traceID")
	assert.Contains(t, grpCols, "serviceName")
	assert.Contains(t, grpCols, "operationName")

	var mCol3 string
	var mCol4 string
	for mcol := range measure {
		if mcol == "col3" {
			mCol3 = mcol
		} else if mcol == "col4" {
			mCol4 = mcol
		}
	}
	assert.NotEqual(t, "", mCol3)
	assert.NotEqual(t, "", mCol4)

	aggs2 := &QueryAggregators{
		GroupByRequest: &GroupByRequest{
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "startTime", MeasureFunc: sutils.Max},
			},
			GroupByColumns: []string{"col3", "col2"},
		},
	}
	UpdateQTUsage(tableName, nil, aggs2, "*")
	grpCols, measure = GetTopPersistentAggs("jaeger-1")
	assert.Len(t, grpCols, 6)

	_, ok := grpCols["traceID"]
	assert.True(t, ok, "traceID should be present")
	var mCol3_1 string
	mCol4 = ""
	for m := range measure {
		if m == "col3" {
			if mCol3_1 == "" {
				mCol3_1 = m
			}
		} else if m == "col4" {
			mCol4 = m
		}
	}
	assert.NotEqual(t, "", mCol3_1)
	assert.NotEqual(t, "", mCol4)
}

func Test_AggsHasher(t *testing.T) {
	resetInternalQTInfo()
	config.SetPQSEnabled(true)
	aggs1 := &QueryAggregators{
		GroupByRequest: &GroupByRequest{
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "col3", MeasureFunc: sutils.Avg},
				{MeasureCol: "col4", MeasureFunc: sutils.Count},
			},
			GroupByColumns: []string{"col1", "col2"},
		},
	}
	id1 := GetHashForAggs(aggs1)
	aggs2 := &QueryAggregators{
		GroupByRequest: &GroupByRequest{
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "col4", MeasureFunc: sutils.Count},
				{MeasureCol: "col3", MeasureFunc: sutils.Avg},
			},
			GroupByColumns: []string{"col2", "col1"},
		},
	}
	id2 := GetHashForAggs(aggs2)
	assert.Equal(t, id1, id2)

	aggs3 := &QueryAggregators{
		GroupByRequest: &GroupByRequest{
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "col4", MeasureFunc: sutils.Count},
				{MeasureCol: "col3", MeasureFunc: sutils.Count},
			},
			GroupByColumns: []string{"col2", "col1"},
		},
	}
	id3 := GetHashForAggs(aggs3)
	assert.NotEqual(t, id1, id3)
}

func Test_PostPqsClear(t *testing.T) {
	resetInternalQTInfo()
	config.SetPQSEnabled(true)
	qVal, err := sutils.CreateDtypeEnclosure("iOS", 0)
	assert.Nil(t, err)

	sNode := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{
				{
					ExpressionFilter: &SearchExpression{
						LeftSearchInput:  &SearchExpressionInput{ColumnName: "os"},
						FilterOp:         sutils.Equals,
						RightSearchInput: &SearchExpressionInput{ColumnValue: qVal},
					},
					SearchType: SimpleExpression,
				},
			},
		},
		NodeType: ColumnValueQuery,
	}
	pqid := GetHashForQuery(sNode)
	tableName := []string{"test-1"}
	assert.NotNil(t, tableName)
	UpdateQTUsage(tableName, sNode, nil, "os=iOS")

	expected := map[string]interface{}{
		"promoted_aggregations": []map[string]interface{}{},
		"promoted_searches":     []map[string]interface{}{},
		"total_tracked_queries": 0,
	}
	var pqsSummary, clearPqsSummary map[string]interface{}
	pqsSummary = getPQSSummary()
	assert.NotNil(t, pqsSummary)
	totalQueries := pqsSummary["total_tracked_queries"]
	assert.NotNil(t, totalQueries)
	totalQueriesInt, ok := totalQueries.(int)
	assert.Equal(t, true, ok, "converting total persistent queries to int did not work")
	assert.Equal(t, totalQueriesInt, 1, "There should be 1 persistent query but got=%v", totalQueriesInt)

	pqsinfo := getPqsById(pqid)
	assert.NotNil(t, pqsinfo)
	ClearPqs()
	clearPqsSummary = getPQSSummary()
	assert.NotNil(t, clearPqsSummary)
	assert.Equal(t, expected, clearPqsSummary, "the pqsinfo was supposed to be cleared")
}

func Test_fillAggPQS_ReturnsErrorWhenAggsAreEmpty(t *testing.T) {
	resetInternalQTInfo()
	config.SetPQSEnabled(true)
	ctx := fasthttp.RequestCtx{}
	err := fillAggPQS(&ctx, "id")
	assert.EqualError(t, err, "pqid id does not exist in aggs")
}

func Test_getAggPQSById(t *testing.T) {
	resetInternalQTInfo()
	config.SetPQSEnabled(true)
	var st uint64 = 3
	qa := &QueryAggregators{TimeHistogram: &TimeBucket{StartTime: st}}
	pqid := GetHashForAggs(qa)
	tableName := []string{"test-1"}
	assert.NotNil(t, tableName)
	UpdateQTUsage(tableName, nil, qa, "TimeHistogramStartTime=3")

	aggPQS, err := getAggPQSById(pqid)
	assert.Nil(t, err)
	assert.Equal(t, pqid, aggPQS["pqid"])
	assert.Equal(t, uint32(1), aggPQS["total_usage"])
	restored_aggs := aggPQS["search_aggs"].(map[string]interface{})
	restored_th := restored_aggs["TimeHistogram"].(map[string]interface{})
	assert.Equal(t, float64(st), restored_th["StartTime"])
}

func Test_processPostAggs_NoErrorWhenEmptySliceOfStrings(t *testing.T) {
	inputValueParam := []interface{}{}
	_, err := processPostAggs(inputValueParam)
	assert.Nil(t, err)
}

func Test_processPostAggs_NoErrorWhenSliceOfStrings(t *testing.T) {
	key := "new_column"
	inputValueParam := []interface{}{key}
	got, err := processPostAggs(inputValueParam)
	assert.Nil(t, err)
	assert.Len(t, got, 1)
	assert.True(t, got[key])
}

func Test_processPostAggs_ErrorWhenSliceOfNotStrings(t *testing.T) {
	inputValueParam := []interface{}{1, 2, 8}
	_, err := processPostAggs(inputValueParam)
	assert.NotNil(t, err)
}

func Test_processPostAggs_ErrorWhenNotSlice(t *testing.T) {
	inputValueParam := map[int]string{}
	_, err := processPostAggs(inputValueParam)
	assert.NotNil(t, err)
}

func Test_parsePostPqsAggBody_ErrIfTableNameIsntString(t *testing.T) {
	json := map[string]interface{}{
		"tableName": 1,
	}
	err := parsePostPqsAggBody(json)
	assert.EqualError(t, err, "PostPqsAggCols: Invalid key=[tableName] with value of type [int]")
}

func Test_parsePostPqsAggBody_ErrIfTableNameIsWildcard(t *testing.T) {
	json := map[string]interface{}{
		"tableName": "*",
	}
	err := parsePostPqsAggBody(json)
	assert.EqualError(t, err, "PostPqsAggCols: tableName can not be *")
}

func Test_parsePostPqsAggBody_ErrIfNoTableNameSpecified(t *testing.T) {
	json := map[string]interface{}{
		"groupByColumns": []interface{}{"col1", "col2"},
	}
	err := parsePostPqsAggBody(json)
	assert.EqualError(t, err, "PostPqsAggCols: No tableName specified")
}

func Test_parsePostPqsAggBody_NoErrIfColsAreSlices(t *testing.T) {
	json := map[string]interface{}{
		"tableName":      "some_table",
		"groupByColumns": []interface{}{"col1", "col2"},
		"measureColumns": []interface{}{"col1", "col2"},
	}
	err := parsePostPqsAggBody(json)
	assert.Nil(t, err)
}

func Test_parsePostPqsAggBody_ErrIfColsAreSlicesOfNotInterfaces(t *testing.T) {
	json := map[string]interface{}{
		"tableName":      "some_table",
		"groupByColumns": []string{"col1", "col2"}, // should be []interfaces
	}
	err := parsePostPqsAggBody(json)
	assert.EqualError(t, err, "PostPqsAggCols: Invalid key=[groupByColumns] with value of type [[]string]")
}

func Test_parsePostPqsAggBody_ErrIfColsAreNotSlicesOfStrings(t *testing.T) {
	json := map[string]interface{}{
		"tableName":      "some_table",
		"groupByColumns": []interface{}{1, "col2"},
	}
	err := parsePostPqsAggBody(json)
	assert.EqualError(t, err, "processPostAggs type = int not accepted")
}

func Test_GetPQSById_400IfPQDoesNotExist(t *testing.T) {
	resetInternalQTInfo()
	config.SetPQSEnabled(true)
	ctx := fasthttp.RequestCtx{}
	ctx.SetUserValue("pqid", 1)
	GetPQSById(&ctx)
	assert.Equal(t, 400, ctx.Response.Header.StatusCode())
}

func Test_GetPQSById_400IfPQIsNotSpecified(t *testing.T) {
	resetInternalQTInfo()
	config.SetPQSEnabled(true)
	ctx := fasthttp.RequestCtx{}
	GetPQSById(&ctx)
	assert.Equal(t, 400, ctx.Response.Header.StatusCode())
}

func Test_GetPQSById_200IfPQIsSpecified(t *testing.T) {
	resetInternalQTInfo()
	config.SetPQSEnabled(true)
	qa := &QueryAggregators{TimeHistogram: &TimeBucket{StartTime: 3}}
	pqid := GetHashForAggs(qa)
	tableName := []string{"test-1"}
	UpdateQTUsage(tableName, nil, qa, "")

	ctx := fasthttp.RequestCtx{}
	ctx.SetUserValue("pqid", pqid)

	GetPQSById(&ctx)
	assert.Equal(t, 200, ctx.Response.Header.StatusCode())
}
