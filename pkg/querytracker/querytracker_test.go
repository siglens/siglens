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

package querytracker

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/siglens/siglens/pkg/config"
	. "github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
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
	qVal, err := utils.CreateDtypeEnclosure("iOS", 0)
	assert.Nil(t, err)

	sNode := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{
				{
					ExpressionFilter: &SearchExpression{
						LeftSearchInput:  &SearchExpressionInput{ColumnName: "os"},
						FilterOp:         utils.Equals,
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
		UpdateQTUsage(tableName, sNode, nil)
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

	wildCard, err := utils.CreateDtypeEnclosure("*", 0)
	assert.Nil(t, err)

	matchAllOne := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{
				{
					ExpressionFilter: &SearchExpression{
						LeftSearchInput:  &SearchExpressionInput{ColumnName: "*"},
						FilterOp:         utils.Equals,
						RightSearchInput: &SearchExpressionInput{ColumnValue: wildCard},
					},
					SearchType: SimpleExpression,
				},
			},
		},
		NodeType: MatchAllQuery,
	}

	UpdateQTUsage(tableName, matchAllOne, nil)

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

	qVal, err := utils.CreateDtypeEnclosure("batch-101", 0)
	assert.Nil(t, err)
	sNode := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{
				{
					ExpressionFilter: &SearchExpression{
						LeftSearchInput:  &SearchExpressionInput{ColumnName: "batch"},
						FilterOp:         utils.Equals,
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
				{MeasureCol: "col3", MeasureFunc: utils.Avg},
				{MeasureCol: "col4", MeasureFunc: utils.Count},
			},
			GroupByColumns: []string{"col1", "col2"},
		},
	}
	aggsHash := GetHashForAggs(aggs)
	UpdateQTUsage(tableName, sNode, aggs)

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
				{MeasureCol: "col3", MeasureFunc: utils.Avg},
				{MeasureCol: "col4", MeasureFunc: utils.Count},
			},
			GroupByColumns: []string{"col1", "col2"},
		},
	}
	tableName := []string{"test-1"}
	UpdateQTUsage(tableName, nil, aggs)
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
				{MeasureCol: "col3", MeasureFunc: utils.Cardinality},
			},
			GroupByColumns: []string{"col3", "col2"},
		},
	}
	UpdateQTUsage(tableName, nil, aggs2)
	grpCols, measure = GetTopPersistentAggs("test-1")
	assert.Len(t, grpCols, 3)
	assert.Equal(t, "col2", grpCols[0], "only col2 exists in both usages, so it should be first")
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
				{MeasureCol: "col3", MeasureFunc: utils.Avg},
				{MeasureCol: "col4", MeasureFunc: utils.Count},
			},
			GroupByColumns: []string{"col1", "col2"},
		},
	}
	tableName := []string{"jaeger-1"}
	UpdateQTUsage(tableName, nil, aggs)

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
				{MeasureCol: "startTime", MeasureFunc: utils.Max},
			},
			GroupByColumns: []string{"col3", "col2"},
		},
	}
	UpdateQTUsage(tableName, nil, aggs2)
	grpCols, measure = GetTopPersistentAggs("jaeger-1")
	assert.Len(t, grpCols, 6)
	assert.Equal(t, "traceID", grpCols[0], "traceID exists in both usages, so it should be first")
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
				{MeasureCol: "col3", MeasureFunc: utils.Avg},
				{MeasureCol: "col4", MeasureFunc: utils.Count},
			},
			GroupByColumns: []string{"col1", "col2"},
		},
	}
	id1 := GetHashForAggs(aggs1)
	aggs2 := &QueryAggregators{
		GroupByRequest: &GroupByRequest{
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "col4", MeasureFunc: utils.Count},
				{MeasureCol: "col3", MeasureFunc: utils.Avg},
			},
			GroupByColumns: []string{"col2", "col1"},
		},
	}
	id2 := GetHashForAggs(aggs2)
	assert.Equal(t, id1, id2)

	aggs3 := &QueryAggregators{
		GroupByRequest: &GroupByRequest{
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "col4", MeasureFunc: utils.Count},
				{MeasureCol: "col3", MeasureFunc: utils.Count},
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
	qVal, err := utils.CreateDtypeEnclosure("iOS", 0)
	assert.Nil(t, err)

	sNode := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{
				{
					ExpressionFilter: &SearchExpression{
						LeftSearchInput:  &SearchExpressionInput{ColumnName: "os"},
						FilterOp:         utils.Equals,
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
	UpdateQTUsage(tableName, sNode, nil)

	expected := map[string]interface{}{
		"promoted_aggregations": make(map[string]int),
		"promoted_searches":     make(map[string]int),
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
