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

package search

import (
	"fmt"
	"math"
	"os"
	"regexp"
	"testing"
	"time"

	localstorage "github.com/siglens/siglens/pkg/blob/local"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/reader/microreader"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_simpleRawSearch(t *testing.T) {
	config.InitializeTestingConfig()
	config.SetSSInstanceName("mock-host")
	err := config.InitDerivedConfig("test")
	assert.NoError(t, err)
	_ = localstorage.InitLocalStorage()

	dataDir := "data/"
	err = os.MkdirAll(dataDir+"mock-host.test/", 0755)
	if err != nil {
		assert.FailNow(t, "failed to create dir %+v", err)
	}
	numBuffers := 5
	numEntriesForBuffer := 10
	segKey := dataDir + "mock-host.test/raw_search_test"
	_, allBlockSummaries, _, allCols, blockMetadata, _ := writer.WriteMockColSegFile(segKey, numBuffers, numEntriesForBuffer)

	searchReq := &SegmentSearchRequest{
		SegmentKey:        segKey,
		AllBlocksToSearch: blockMetadata,
		SearchMetadata: &SearchMetadataHolder{
			BlockSummaries: allBlockSummaries,
		},
		VirtualTableName:   "evts",
		AllPossibleColumns: allCols,
	}

	querySummary := summary.InitQuerySummary(summary.LOGS, 1)
	value1, _ := CreateDtypeEnclosure("value1", 0)
	query := &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  &SearchExpressionInput{ColumnName: "key1"},
			FilterOp:         Equals,
			RightSearchInput: &SearchExpressionInput{ColumnValue: value1},
		},
		SearchType: SimpleExpression,
	}
	timeRange := &dtu.TimeRange{
		StartEpochMs: 1,
		EndEpochMs:   5,
	}
	node := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{query},
		},
		NodeType: ColumnValueQuery,
	}
	allSegFileResults, err := segresults.InitSearchResults(10000, nil, RRCCmd, 1)
	assert.NoError(t, err)
	searchReq.SType = structs.RAW_SEARCH
	rawSearchColumnar(searchReq, node, timeRange, 10000, nil, 1, allSegFileResults, 1, querySummary)
	assert.Len(t, allSegFileResults.GetAllErrors(), 0)
	assert.Equal(t, numBuffers*5, len(allSegFileResults.GetResults()))
	assert.Equal(t, allSegFileResults.GetTotalCount(), uint64(len(allSegFileResults.GetResults())))

	config.SetPQSEnabled(true)
	// get file name
	pqid := querytracker.GetHashForQuery(node)
	pqidFname := fmt.Sprintf("%v/pqmr/%v.pqmr", searchReq.SegmentKey, pqid)

	// check if that file exist, assert on file not exist
	_, err = os.Stat(pqidFname)
	assert.Equal(t, true, os.IsNotExist(err))

	// make query persistent
	querytracker.UpdateQTUsage([]string{searchReq.VirtualTableName}, node, nil)

	// Call rawSearchColumnar
	rawSearchColumnar(searchReq, node, timeRange, 10000, nil, 1, allSegFileResults, 1, querySummary)
	// We need to sleep because pqmr files are written in background go routines
	time.Sleep(1 * time.Second)
	// Now make sure filename exists
	_, err = os.Stat(pqidFname)
	assert.Nil(t, err)

	// Read pqmr file
	pqmrReadResults, err := pqmr.ReadPqmr(&pqidFname)
	assert.Nil(t, err)
	assert.NotEmpty(t, pqmrReadResults)
	assert.Equal(t, numBuffers, int(pqmrReadResults.GetNumBlocks()))

	numOfRecs := uint(0)
	allBlocks := pqmrReadResults.GetAllBlocks()
	assert.Len(t, allBlocks, numBuffers)
	for _, blkNum := range allBlocks {
		block, _ := pqmrReadResults.GetBlockResults(blkNum)
		numOfRecs += block.GetNumberOfSetBits()
	}
	assert.Equal(t, uint(numBuffers*numEntriesForBuffer), numOfRecs)

	config.SetPQSEnabled(false)

	zero, _ := CreateDtypeEnclosure(false, 0)
	query = &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  &SearchExpressionInput{ColumnName: "key3"},
			FilterOp:         Equals,
			RightSearchInput: &SearchExpressionInput{ColumnValue: zero},
		},
		SearchType: SimpleExpression,
	}

	fullTimeRange := &dtu.TimeRange{
		StartEpochMs: 0,
		EndEpochMs:   uint64(numEntriesForBuffer),
	}
	node = &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{query},
		},
		NodeType: ColumnValueQuery,
	}
	allSegFileResults, err = segresults.InitSearchResults(10000, nil, RRCCmd, 1)
	assert.NoError(t, err)
	rawSearchColumnar(searchReq, node, fullTimeRange, 10000, nil, 1, allSegFileResults, 3, querySummary)
	assert.Len(t, allSegFileResults.GetAllErrors(), 0)
	assert.Equal(t, (numBuffers*numEntriesForBuffer)/2, len(allSegFileResults.GetResults()))

	query = &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  &SearchExpressionInput{ColumnName: "invalid_column"},
			FilterOp:         Equals,
			RightSearchInput: &SearchExpressionInput{ColumnValue: zero},
		},
		SearchType: SimpleExpression,
	}

	node = &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{query},
		},
		NodeType: ColumnValueQuery,
	}

	allSegFileResults, err = segresults.InitSearchResults(10000, nil, RRCCmd, 1)
	assert.NoError(t, err)
	rawSearchColumnar(searchReq, node, fullTimeRange, 10000, nil, 1, allSegFileResults, 0, querySummary)
	assert.NotEqual(t, 0, allSegFileResults.GetAllErrors(), "errors MUST happen")
	assert.Equal(t, 0, len(allSegFileResults.GetResults()))

	batchZero, _ := CreateDtypeEnclosure("batch-0-*", 0)
	batchOne, _ := CreateDtypeEnclosure("batch-1-*", 0)
	query = &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  &SearchExpressionInput{ColumnName: "key5"},
			FilterOp:         Equals,
			RightSearchInput: &SearchExpressionInput{ColumnValue: batchZero},
		},
		SearchType: RegexExpression,
	}

	node = &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{query},
		},
		NodeType: ColumnValueQuery,
	}

	allSegFileResults, err = segresults.InitSearchResults(10000, nil, RRCCmd, 1)
	assert.NoError(t, err)
	rawSearchColumnar(searchReq, node, fullTimeRange, 10000, nil, 1, allSegFileResults, 0, querySummary)
	assert.Len(t, allSegFileResults.GetAllErrors(), 0)
	assert.Equal(t, numEntriesForBuffer, len(allSegFileResults.GetResults()))

	query = &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  &SearchExpressionInput{ColumnName: "*"},
			FilterOp:         Equals,
			RightSearchInput: &SearchExpressionInput{ColumnValue: batchZero},
		},
		SearchType: RegexExpressionAllColumns,
	}

	node = &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{query},
		},
		NodeType: ColumnValueQuery,
	}

	allSegFileResults, err = segresults.InitSearchResults(10000, nil, RRCCmd, 1)
	assert.NoError(t, err)
	searchReq.CmiPassedCnames = make(map[uint16]map[string]bool)
	for blkNum := range searchReq.AllBlocksToSearch {
		searchReq.CmiPassedCnames[blkNum] = make(map[string]bool)
		for cname := range allCols {
			searchReq.CmiPassedCnames[blkNum][cname] = true
		}
	}

	rawSearchColumnar(searchReq, node, fullTimeRange, 10000, nil, 1, allSegFileResults, 5, querySummary)
	assert.Len(t, allSegFileResults.GetAllErrors(), 0)
	assert.Equal(t, numEntriesForBuffer, len(allSegFileResults.GetResults()))

	// // (col5==batch-0-* OR col5==batch-1-*) AND key1=value1
	batch0Query := &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  &SearchExpressionInput{ColumnName: "*"},
			FilterOp:         Equals,
			RightSearchInput: &SearchExpressionInput{ColumnValue: batchZero},
		},
		SearchType: RegexExpressionAllColumns,
	}

	batch1Query := &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  &SearchExpressionInput{ColumnName: "*"},
			FilterOp:         Equals,
			RightSearchInput: &SearchExpressionInput{ColumnValue: batchOne},
		},
		SearchType: RegexExpressionAllColumns,
	}

	valueQuery := &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  &SearchExpressionInput{ColumnName: "key1"},
			FilterOp:         Equals,
			RightSearchInput: &SearchExpressionInput{ColumnValue: value1},
		},
		SearchType: SimpleExpression,
	}

	orNode := &SearchNode{
		OrSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{batch0Query, batch1Query},
		},
		NodeType: ColumnValueQuery,
	}

	nestedQuery := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{valueQuery},
			SearchNode:    []*SearchNode{orNode},
		},
		NodeType: ColumnValueQuery,
	}

	allSegFileResults, err = segresults.InitSearchResults(10000, nil, RRCCmd, 1)
	assert.NoError(t, err)
	rawSearchColumnar(searchReq, nestedQuery, fullTimeRange, 10000, nil, 1, allSegFileResults, 0, querySummary)
	assert.Len(t, allSegFileResults.GetAllErrors(), 0)
	assert.Equal(t, numEntriesForBuffer*2, len(allSegFileResults.GetResults()))

	testAggsQuery(t, numEntriesForBuffer, searchReq)

	err = os.RemoveAll(dataDir)
	assert.Nil(t, err)
}

func Test_simpleRawSearch_jaeger(t *testing.T) {
	config.InitializeTestingConfig()
	config.SetSSInstanceName("mock-host")
	err := config.InitDerivedConfig("test")
	assert.Nil(t, err)
	_ = localstorage.InitLocalStorage()
	dataDir := "data/"
	err = os.MkdirAll(dataDir+"mock-host/", 0755)
	if err != nil {
		assert.FailNow(t, "failed to create dir %+v", err)
	}
	numBuffers := 1
	numEntriesForBuffer := 1
	segKey := dataDir + "mock-host/raw_search_test_jaeger"
	_, allBlockSummaries, _, allCols, blockMetadata := writer.WriteMockTraceFile(segKey, numBuffers, numEntriesForBuffer)

	searchReq := &SegmentSearchRequest{
		SegmentKey:        segKey,
		AllBlocksToSearch: blockMetadata,
		SearchMetadata: &SearchMetadataHolder{
			BlockSummaries: allBlockSummaries,
		},
		VirtualTableName:   "jaeger-evts",
		AllPossibleColumns: allCols,
	}
	value1, _ := CreateDtypeEnclosure("const", 0)
	querySummary := summary.InitQuerySummary(summary.LOGS, 1)
	query := &SearchQuery{
		MatchFilter: &MatchFilter{
			MatchColumn: "tags",
			MatchDictArray: &MatchDictArrayRequest{
				MatchKey:   []byte("sampler.type"),
				MatchValue: value1,
			},
			MatchType: MATCH_DICT_ARRAY,
		},
		SearchType: MatchDictArraySingleColumn,
	}
	timeRange := &dtu.TimeRange{
		StartEpochMs: 1,
		EndEpochMs:   5,
	}
	node := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{query},
		},
		NodeType: ColumnValueQuery,
	}
	allSegFileResults, err := segresults.InitSearchResults(10000, nil, RRCCmd, 1)
	assert.NoError(t, err)
	searchReq.SType = structs.RAW_SEARCH
	rawSearchColumnar(searchReq, node, timeRange, 10000, nil, 1, allSegFileResults, 1, querySummary)

	assert.Len(t, allSegFileResults.GetAllErrors(), 0)
	assert.Equal(t, numBuffers, len(allSegFileResults.GetResults()))
	assert.Equal(t, allSegFileResults.GetTotalCount(), uint64(len(allSegFileResults.GetResults())))

	value2, _ := CreateDtypeEnclosure("200", 1)
	query2 := &SearchQuery{
		MatchFilter: &MatchFilter{
			MatchColumn: "tags",
			MatchDictArray: &MatchDictArrayRequest{
				MatchKey:   []byte("http.status_code"),
				MatchValue: value2,
			},
			MatchType: MATCH_DICT_ARRAY,
		},
		SearchType: MatchDictArraySingleColumn,
	}

	node2 := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{query2},
		},
		NodeType: ColumnValueQuery,
	}
	assert.NoError(t, err)
	searchReq.SType = structs.RAW_SEARCH
	rawSearchColumnar(searchReq, node2, timeRange, 10000, nil, 1, allSegFileResults, 1, querySummary)

	assert.Len(t, allSegFileResults.GetAllErrors(), 0)
	assert.Equal(t, numBuffers*2, len(allSegFileResults.GetResults()))
	assert.Equal(t, allSegFileResults.GetTotalCount(), uint64(len(allSegFileResults.GetResults())))

	err = os.RemoveAll(dataDir)
	assert.Nil(t, err)
}

func testAggsQuery(t *testing.T, numEntriesForBuffer int, searchReq *structs.SegmentSearchRequest) {
	querySummary := summary.InitQuerySummary(summary.LOGS, 101010)

	batchZero, _ := CreateDtypeEnclosure("batch-0-*", 0)
	query := &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  &SearchExpressionInput{ColumnName: "key5"},
			FilterOp:         Equals,
			RightSearchInput: &SearchExpressionInput{ColumnValue: batchZero},
		},
		SearchType: RegexExpression,
	}
	fullTimeRange := &dtu.TimeRange{
		StartEpochMs: 0,
		EndEpochMs:   uint64(numEntriesForBuffer),
	}

	node := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{query},
		},
		NodeType: ColumnValueQuery,
	}
	measureOps := make([]*structs.MeasureAggregator, 2)
	measureOps[0] = &structs.MeasureAggregator{MeasureCol: "key0", MeasureFunc: utils.Cardinality}
	measureOps[1] = &structs.MeasureAggregator{MeasureCol: "key6", MeasureFunc: utils.Min}

	allSegFileResults, err := segresults.InitSearchResults(10000, nil, SegmentStatsCmd, 1000)
	assert.Nil(t, err)

	block0, err := RawComputeSegmentStats(searchReq, 5, node, fullTimeRange, measureOps, allSegFileResults, 123, querySummary)
	assert.Nil(t, err)
	assert.Len(t, block0, 2)
	assert.Contains(t, block0, "key0")
	assert.Contains(t, block0, "key6")
	key0Block0Stats := block0["key0"]
	assert.False(t, key0Block0Stats.IsNumeric)
	assert.Equal(t, key0Block0Stats.Count, uint64(numEntriesForBuffer))
	assert.GreaterOrEqual(t, key0Block0Stats.Hll.Estimate(), uint64(0))
	assert.LessOrEqual(t, key0Block0Stats.Hll.Estimate(), uint64(2), "key0 always has same value")

	key6Block0Stats := block0["key6"]
	assert.True(t, key6Block0Stats.IsNumeric)
	assert.Equal(t, key6Block0Stats.Count, uint64(numEntriesForBuffer))
	assert.Equal(t, key6Block0Stats.NumStats.Dtype, utils.SS_DT_FLOAT)
	assert.Equal(t, key6Block0Stats.NumStats.Min.FloatVal, float64(0))
	assert.Equal(t, key6Block0Stats.NumStats.Max.FloatVal, float64(numEntriesForBuffer-1)*2)
}

type BenchQueryConds struct {
	colNameToSearch   string
	colValStrToSearch string
	queryType         SearchQueryType
	isRegex           bool
}

func Benchmark_simpleRawSearch(b *testing.B) {
	config.InitializeDefaultConfig()
	config.SetDebugMode(true)

	querySummary := summary.InitQuerySummary(summary.LOGS, 1)

	cond1 := &BenchQueryConds{colNameToSearch: "device_type", colValStrToSearch: "mobile", queryType: SimpleExpression, isRegex: false}
	cond2 := &BenchQueryConds{colNameToSearch: "referer_medium", colValStrToSearch: "internal", queryType: SimpleExpression, isRegex: false}
	allconds := []*BenchQueryConds{cond1, cond2}

	// cond1 := &BenchQueryConds{colNameToSearch: "*", colValStrToSearch: "chrome", queryType: MatchAll, isRegex: false}
	// allconds := []*BenchQueryConds{cond1}

	segKey := "/Users/ssubramanian/Desktop/SigLens/siglens/data/Sris-MacBook-Pro.local/final/2022/03/03/03/ind-v1-valtix/1149711685912017186/0"
	start := time.Now()

	b.ReportAllocs()
	b.ResetTimer()

	node, searchReq, fullTimeRange, agg := createBenchQuery(b, segKey, allconds)

	count := 50
	for i := 0; i < count; i++ {
		allSegFileResults, err := segresults.InitSearchResults(100, agg, RRCCmd, 8)
		assert.NoError(b, err)
		rawSearchColumnar(searchReq, node, fullTimeRange, 100, agg, 8, allSegFileResults, uint64(i), querySummary)
		b := allSegFileResults.GetBucketResults()
		c := allSegFileResults.GetTotalCount()
		log.Infof("num buckets %+v, count %+v", len(b["date histogram"].Results), c)
	}

	totalTime := time.Since(start).Seconds()
	avgTime := totalTime / float64(count)
	log.Warnf("Total time=%f. Average time=%f", totalTime, avgTime)

	/*
	   cd pkg/segment/search
	   go test -run=Bench -bench=Benchmark_simpleRawSearch -cpuprofile cpuprofile.out -o rawsearch_cpu
	   go tool pprof ./rawsearch_cpu cpuprofile.out

	   (for mem profile)
	   go test -run=Bench -bench=Benchmark_simpleRawSearch -benchmem -memprofile memprofile.out -o rawsearch_mem
	   go tool pprof ./rawsearch_mem memprofile.out

	   **** History *****
	   recording history of this benchmark perf test
	   1-Dec: develop: 0.428 s, mem goes up only till 1GB

	*/
}

func Benchmark_simpleAggregations(b *testing.B) {
	config.InitializeDefaultConfig()
	config.SetDebugMode(true)

	querySummary := summary.InitQuerySummary(summary.LOGS, 1)

	cond1 := &BenchQueryConds{colNameToSearch: "j", colValStrToSearch: "group 0", queryType: SimpleExpression, isRegex: false}
	allconds := []*BenchQueryConds{cond1}

	segKey := "/Users/ssubramanian/Desktop/SigLens/siglens/data/Sris-MBP.lan/final/ind-0/0-3544697602014606120/0/0"
	node, searchReq, fullTimeRange, _ := createBenchQuery(b, segKey, allconds)
	agg := &structs.QueryAggregators{
		GroupByRequest: &GroupByRequest{
			GroupByColumns: []string{"a", "d"},
			MeasureOperations: []*structs.MeasureAggregator{
				{MeasureCol: "a", MeasureFunc: utils.Count},
				{MeasureCol: "a", MeasureFunc: utils.Avg},
			},
			AggName: "test",
		},
	}
	start := time.Now()
	b.ReportAllocs()
	b.ResetTimer()

	count := 50
	for i := 0; i < count; i++ {
		allSegFileResults, err := segresults.InitSearchResults(100, agg, RRCCmd, 8)
		assert.NoError(b, err)
		rawSearchColumnar(searchReq, node, fullTimeRange, 100, agg, 8, allSegFileResults, uint64(i), querySummary)
		b := allSegFileResults.GetBucketResults()
		c := allSegFileResults.GetTotalCount()
		log.Infof("num buckets %+v, count %+v", len(b["test"].Results), c)
		if len(b["test"].Results) > 0 {
			log.Infof("%+v %+v %+v", b["test"].Results[0].BucketKey, b["test"].Results[0].ElemCount, b["test"].Results[0].StatRes)
		}
	}

	totalTime := time.Since(start).Seconds()
	avgTime := totalTime / float64(count)
	log.Warnf("Total time=%f. Average time=%f", totalTime, avgTime)

	/*
	   cd pkg/segment/search
	   go test -run=Bench -bench=Benchmark_simpleRawSearch -cpuprofile cpuprofile.out -o rawsearch_cpu
	   go tool pprof ./rawsearch_cpu cpuprofile.out

	   (for mem profile)
	   go test -run=Bench -bench=Benchmark_simpleRawSearch -benchmem -memprofile memprofile.out -o rawsearch_mem
	   go tool pprof ./rawsearch_mem memprofile.out

	   **** History *****
	   recording history of this benchmark perf test
	   1-Dec: develop: 0.428 s, mem goes up only till 1GB

	*/
}

func createBenchQuery(b *testing.B, segKey string,
	allconds []*BenchQueryConds) (*SearchNode, *SegmentSearchRequest, *dtu.TimeRange, *QueryAggregators) {

	fullTimeRange := &dtu.TimeRange{
		StartEpochMs: 0,
		EndEpochMs:   math.MaxUint64,
	}

	allsqs := make([]*SearchQuery, 0)

	for _, cond := range allconds {
		dtype, err := CreateDtypeEnclosure(cond.colValStrToSearch, 0)
		if err != nil {
			b.Fatal(err)
		}
		if cond.colNameToSearch == "*" {
			continue
		}

		if cond.isRegex {
			rexpC, _ := regexp.Compile(dtu.ReplaceWildcardStarWithRegex(cond.colValStrToSearch))
			dtype.SetRegexp(rexpC)
		}

		sq := &SearchQuery{
			ExpressionFilter: &SearchExpression{
				LeftSearchInput:  &SearchExpressionInput{ColumnName: cond.colNameToSearch},
				FilterOp:         Equals,
				RightSearchInput: &SearchExpressionInput{ColumnValue: dtype},
			},
			SearchType: cond.queryType,
		}
		allsqs = append(allsqs, sq)

	}

	node := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: allsqs,
		},
		NodeType: ColumnValueQuery,
	}

	agg := &QueryAggregators{
		Sort: &SortRequest{
			ColName:   "timestamp",
			Ascending: true,
		},
		TimeHistogram: &TimeBucket{
			IntervalMillis: 60000,
		},
	}

	bSumFile := structs.GetBsuFnameFromSegKey(segKey)
	blockSummaries, allBlockInfo, _, err := microreader.ReadBlockSummaries(bSumFile, []byte{})
	if err != nil {
		log.Fatal(err)
	}

	searchReq := &SegmentSearchRequest{
		SegmentKey: segKey,
		SearchMetadata: &SearchMetadataHolder{
			BlockSummaries: blockSummaries,
		},
		AllBlocksToSearch: allBlockInfo,
	}

	allSearchColumns, _ := node.GetAllColumnsToSearch()
	searchReq.AllPossibleColumns = allSearchColumns

	return node, searchReq, fullTimeRange, agg

}
