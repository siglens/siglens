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
	"testing"

	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func Test_GetFullResult_notTruncated(t *testing.T) {
	stream := &mockStreamer{
		allRecords: map[string][]utils.CValueEnclosure{
			"col1": {
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			},
		},
		qid: 0,
	}

	queryProcessor, err := newQueryProcessorHelper(structs.RRCCmd, stream, nil)
	assert.NoError(t, err)

	response, err := queryProcessor.GetFullResult()
	assert.NoError(t, err)
	hitsCount, ok := response.Hits.TotalMatched.(toputils.HitsCount)
	assert.True(t, ok)
	assert.Equal(t, 3, int(hitsCount.Value))
	assert.Equal(t, "eq", hitsCount.Relation)
}

func Test_GetFullResult_truncated(t *testing.T) {
	stream := &mockStreamer{
		allRecords: map[string][]utils.CValueEnclosure{"col1": {}},
		qid:        0,
	}

	for i := 0; i < int(utils.QUERY_EARLY_EXIT_LIMIT+10); i++ {
		stream.allRecords["col1"] = append(stream.allRecords["col1"], utils.CValueEnclosure{
			Dtype: utils.SS_DT_SIGNED_NUM,
			CVal:  i,
		})
	}

	queryProcessor, err := newQueryProcessorHelper(structs.RRCCmd, stream, nil)
	assert.NoError(t, err)

	response, err := queryProcessor.GetFullResult()
	assert.NoError(t, err)
	hitsCount, ok := response.Hits.TotalMatched.(toputils.HitsCount)
	assert.True(t, ok)
	assert.Equal(t, int(utils.QUERY_EARLY_EXIT_LIMIT), int(hitsCount.Value))
	assert.Equal(t, "eq", hitsCount.Relation)
}

func Test_NewQueryProcessor_simple(t *testing.T) {
	agg1 := structs.QueryAggregators{
		WhereExpr: &structs.BoolExpr{},
	}
	agg2 := structs.QueryAggregators{
		SortExpr: &structs.SortExpr{},
	}
	agg1.Next = &agg2

	queryInfo := &query.QueryInformation{}
	querySummary := &summary.QuerySummary{}
	queryProcessor, err := NewQueryProcessor(&agg1, queryInfo, querySummary)
	assert.NoError(t, err)
	assert.NotNil(t, queryProcessor)
}

func Test_NewQueryProcessor_allCommands(t *testing.T) {
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
		{RareExpr: &structs.StatisticExpr{}},
		{RegexExpr: &structs.RegexExpr{}},
		{RexExpr: &structs.RexExpr{}},
		{SortExpr: &structs.SortExpr{}},
		{StatsExpr: &structs.StatsOptions{}},
		{StreamstatsExpr: &structs.StreamStatsOptions{}},
		{TailExpr: &structs.TailExpr{}},
		{TimechartExpr: &structs.TimechartExpr{}},
		{TopExpr: &structs.StatisticExpr{}},
		{TransactionExpr: &structs.TransactionArguments{}},
		{WhereExpr: &structs.BoolExpr{}},
	}

	for i := 1; i < len(aggs); i++ {
		aggs[i-1].Next = &aggs[i]
	}

	queryInfo := &query.QueryInformation{}
	querySummary := &summary.QuerySummary{}
	queryProcessor, err := NewQueryProcessor(&aggs[0], queryInfo, querySummary)
	assert.NoError(t, err)
	assert.NotNil(t, queryProcessor)
}
