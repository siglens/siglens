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
	"context"
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func getSampleSearchNode() *structs.SearchNode {
	astNode := &structs.ASTNode{
		AndFilterCondition: &structs.Condition{
			FilterCriteria: []*structs.FilterCriteria{
				{
					MatchFilter: &structs.MatchFilter{
						MatchColumn: "col1",
						MatchWords:  [][]byte{[]byte("*")},
						MatchType:   structs.MATCH_WORDS,
					},
				},
			},
		},
	}
	return query.ConvertASTNodeToSearchNode(astNode, 0)
}

func Test_GetFullResult_notTruncated(t *testing.T) {
	err := initTestConfig(t)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go query.PullQueriesToRun(ctx)
	defer cancel()

	qid := uint64(0)
	rQuery, err := query.StartQuery(qid, true, nil, false)
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)

	query.InitProgressForRRCCmd(3, qid)
	stream := &mockStreamer{
		allRecords: map[string][]sutils.CValueEnclosure{
			"col1": {
				sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "a"},
				sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "b"},
				sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "c"},
			},
		},
		qid: qid,
	}
	searchNode := getSampleSearchNode()

	querySummary := summary.InitQuerySummary(summary.LOGS, qid)
	queryInfo, err := query.InitQueryInformation(searchNode, nil, nil, nil, 0, 0, qid, nil, 0, 0, false)
	assert.NoError(t, err)

	queryProcessor, err := newQueryProcessorHelper(structs.RRCCmd, stream, nil, qid, 0, false, true)
	assert.NoError(t, err)
	queryProcessor.queryInfo = queryInfo
	queryProcessor.querySummary = querySummary
	queryProcessor.startTime = rQuery.GetStartTime()

	err = query.IncProgressForRRCCmd(0, 3, qid) // Dummy increment for units searched
	assert.NoError(t, err)

	response, err := queryProcessor.GetFullResult()
	assert.NoError(t, err)
	hitsCount, ok := response.Hits.TotalMatched.(utils.HitsCount)
	assert.True(t, ok)
	assert.Equal(t, 3, int(hitsCount.Value))
	assert.Equal(t, "eq", hitsCount.Relation)

	query.DeleteQuery(qid)
}

func Test_GetFullResult_truncated(t *testing.T) {
	err := initTestConfig(t)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go query.PullQueriesToRun(ctx)
	defer cancel()

	qid := uint64(0)
	rQuery, err := query.StartQuery(qid, true, nil, false)
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)

	totalRecords := sutils.QUERY_EARLY_EXIT_LIMIT + 10
	query.InitProgressForRRCCmd(totalRecords, qid)
	stream := &mockStreamer{
		allRecords: map[string][]sutils.CValueEnclosure{"col1": {}},
		qid:        qid,
	}

	for i := 0; i < int(totalRecords); i++ {
		stream.allRecords["col1"] = append(stream.allRecords["col1"], sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_SIGNED_NUM,
			CVal:  i,
		})
	}
	searchNode := getSampleSearchNode()

	querySummary := summary.InitQuerySummary(summary.LOGS, qid)
	queryInfo, err := query.InitQueryInformation(searchNode, nil, nil, nil, 0, 0, qid, nil, 0, 0, false)
	assert.NoError(t, err)

	queryProcessor, err := newQueryProcessorHelper(structs.RRCCmd, stream, nil, qid, 0, false, true)
	assert.NoError(t, err)
	queryProcessor.queryInfo = queryInfo
	queryProcessor.querySummary = querySummary
	queryProcessor.startTime = rQuery.GetStartTime()

	err = query.IncProgressForRRCCmd(0, totalRecords-1, qid) // Dummy increment for units searched
	assert.NoError(t, err)

	response, err := queryProcessor.GetFullResult()
	assert.NoError(t, err)
	hitsCount, ok := response.Hits.TotalMatched.(utils.HitsCount)
	assert.True(t, ok)
	assert.Equal(t, int(sutils.QUERY_EARLY_EXIT_LIMIT), int(hitsCount.Value))
	assert.Equal(t, "gte", hitsCount.Relation)

	query.DeleteQuery(qid)
}

func Test_NewQueryProcessor_simple(t *testing.T) {
	err := initTestConfig(t)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go query.PullQueriesToRun(ctx)
	defer cancel()

	_, err = query.StartQuery(0, true, nil, false)
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)

	agg1 := structs.QueryAggregators{
		WhereExpr: &structs.BoolExpr{},
	}
	agg2 := structs.QueryAggregators{
		SortExpr: &structs.SortExpr{},
	}
	agg1.Next = &agg2

	queryInfo := &query.QueryInformation{}
	querySummary := summary.InitQuerySummary(summary.LOGS, 0)
	queryProcessor, err := NewQueryProcessor(&agg1, queryInfo, querySummary, 0, false, time.Now(), false)
	assert.NoError(t, err)
	assert.NotNil(t, queryProcessor)

	query.DeleteQuery(0)
}

func Test_NewQueryProcessor_allCommands(t *testing.T) {
	err := initTestConfig(t)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	go query.PullQueriesToRun(ctx)
	defer cancel()

	_, err = query.StartQuery(0, true, nil, false)
	assert.NoError(t, err)
	time.Sleep(1 * time.Second)

	aggs := []structs.QueryAggregators{
		{BinExpr: &structs.BinCmdOptions{}},
		{DedupExpr: &structs.DedupExpr{}},
		{EvalExpr: &structs.EvalExpr{}},
		{FieldsExpr: &structs.ColumnsRequest{}},
		{FillNullExpr: &structs.FillNullExpr{}},
		{GentimesExpr: &structs.GenTimes{}},
		{InputLookupExpr: &structs.InputLookup{}},
		{HeadExpr: &structs.HeadExpr{}},
		{MakeMVExpr: &structs.MultiValueColLetRequest{}},
		{RegexExpr: &structs.RegexExpr{}},
		{RexExpr: &structs.RexExpr{}},
		{SortExpr: &structs.SortExpr{}},
		{StatsExpr: &structs.StatsExpr{}},
		{StreamstatsExpr: &structs.StreamStatsOptions{}},
		{TailExpr: &structs.TailExpr{}},
		{TimechartExpr: &structs.TimechartExpr{}},
		{StatisticExpr: &structs.StatisticExpr{}},
		{TransactionExpr: &structs.TransactionArguments{}},
		{WhereExpr: &structs.BoolExpr{}},
	}

	for i := 1; i < len(aggs); i++ {
		aggs[i-1].Next = &aggs[i]
	}

	queryInfo := &query.QueryInformation{}
	querySummary := summary.InitQuerySummary(summary.LOGS, 0)
	queryProcessor, err := NewQueryProcessor(&aggs[0], queryInfo, querySummary, 0, false, time.Now(), false)
	assert.NoError(t, err)
	assert.NotNil(t, queryProcessor)

	query.DeleteQuery(0)
}

func TestIsLogsQuery_AllRRCCmd(t *testing.T) {
	agg := &structs.QueryAggregators{}
	result := query.IsLogsQuery(agg)
	assert.True(t, result)
}

func TestIsLogsQuery_ChainOfRRCCmd(t *testing.T) {
	agg := &structs.QueryAggregators{
		Next: &structs.QueryAggregators{
			Next: &structs.QueryAggregators{},
		},
	}
	result := query.IsLogsQuery(agg)
	assert.True(t, result)
}

func TestIsLogsQuery_WithGroupByCmd(t *testing.T) {
	agg := &structs.QueryAggregators{
		GroupByRequest: &structs.GroupByRequest{
			MeasureOperations: []*structs.MeasureAggregator{},
			GroupByColumns:    []string{"col1"},
		},
	}
	result := query.IsLogsQuery(agg)
	assert.False(t, result)
}

func TestIsLogsQuery_WithSegmentStatsCmd(t *testing.T) {
	agg := &structs.QueryAggregators{
		GroupByRequest: &structs.GroupByRequest{
			MeasureOperations: []*structs.MeasureAggregator{},
			GroupByColumns:    nil,
		},
	}
	result := query.IsLogsQuery(agg)
	assert.False(t, result)
}

func TestIsLogsQuery_WithMeasureOnly_SegmentStatsCmd(t *testing.T) {
	agg := &structs.QueryAggregators{
		MeasureOperations: []*structs.MeasureAggregator{},
	}
	result := query.IsLogsQuery(agg)
	assert.False(t, result)
}

func TestIsLogsQuery_MixedRRCCmdAndGroupByCmd(t *testing.T) {
	agg := &structs.QueryAggregators{
		Next: &structs.QueryAggregators{
			GroupByRequest: &structs.GroupByRequest{
				MeasureOperations: []*structs.MeasureAggregator{},
				GroupByColumns:    []string{"col1"},
			},
		},
	}
	result := query.IsLogsQuery(agg)
	assert.False(t, result)
}
