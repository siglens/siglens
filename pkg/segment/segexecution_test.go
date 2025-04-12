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

package segment

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	esquery "github.com/siglens/siglens/pkg/es/query"
	"github.com/siglens/siglens/pkg/scroll"
	toputils "github.com/siglens/siglens/pkg/utils"

	"github.com/google/uuid"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/segment/memory/limit"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/reader/microreader"
	"github.com/siglens/siglens/pkg/segment/reader/segread"
	"github.com/siglens/siglens/pkg/segment/results/blockresults"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	server_utils "github.com/siglens/siglens/pkg/server/utils"
	serverutils "github.com/siglens/siglens/pkg/server/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var IndexName string = "segexecution"

func testESScroll(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	var qid uint64 = 1
	value1, _ := CreateDtypeEnclosure("*", qid)
	queryRange := &dtu.TimeRange{
		StartEpochMs: 1,
		EndEpochMs:   uint64(numEntriesForBuffer) + 1,
	}
	valueFilter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "*"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange:          queryRange,
	}
	ti := structs.InitTableInfo(IndexName, 0, false)
	sizeLimit := uint64(10000)
	qc := structs.InitQueryContextWithTableInfo(ti, sizeLimit, 0, 0, false)
	result := ExecuteQuery(simpleNode, &QueryAggregators{}, 58, qc)
	t.Logf("Execute Query Results :%v", result)
	assert.NotNil(t, result, "Query ran successfully")
	assert.Equal(t, len(result.AllRecords), numBuffers*numEntriesForBuffer*fileCount, "all logs in all files should have matched")
	assert.Len(t, result.ErrList, 0, "no errors should have occurred")
	timeout := time.Now().UTC().Add(time.Minute * 5).Unix()
	var scrollSize uint64 = 10
	var offset = scrollSize
	resulSet := []string{}
	scrollRecord := scroll.Scroll{
		Scroll_id: "faba624a-6428-4d78-8c70-571443f0d509",
		Results:   nil,
		Size:      scrollSize,
		TimeOut:   uint64(timeout),
		Expiry:    "5m",
		Offset:    0,
		Valid:     true,
	}
	rawResults := esquery.GetQueryResponseJson(result, IndexName, time.Now(), sizeLimit, qid, &QueryAggregators{})
	scrollRecord.Results = &rawResults
	scroll.SetScrollRecord("faba624a-6428-4d78-8c70-571443f0d509", &scrollRecord)
	httpresponse := esquery.GetQueryResponseJsonScroll(IndexName, time.Now().UTC(), sizeLimit, &scrollRecord, qid)
	t.Logf("Scroll Query results %v", httpresponse)
	assert.LessOrEqual(t, len(httpresponse.Hits.Hits), int(scrollSize), "scroll returned more records then the scroll size")
	assert.Equal(t, int(httpresponse.Hits.GetHits()), numBuffers*numEntriesForBuffer*fileCount, "all logs in all files should have matched")
	assert.Equal(t, int(scrollRecord.Offset), int(offset), "offset should have been increased by the scroll size")
	assert.Equal(t, checkScrollRecords(httpresponse.Hits.Hits, &resulSet), false, "all records in the scroll should be unique")
	iterations := int(httpresponse.Hits.GetHits() / scrollSize)
	for i := 1; i < iterations; i++ {
		t.Logf("Iteration No : %d for scroll", i)
		offset = offset + scrollSize
		httpresponse = esquery.GetQueryResponseJsonScroll(IndexName, time.Now().UTC(), sizeLimit, &scrollRecord, qid)
		assert.Equal(t, int(scrollRecord.Offset), int(offset), "offset should have been increased by the scroll size")
		assert.Equal(t, checkScrollRecords(httpresponse.Hits.Hits, &resulSet), false, "all records in the scroll should be unique")
	}
}

func testPipesearchScroll(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	var qid uint64 = 1
	value1, _ := CreateDtypeEnclosure("*", qid)
	queryRange := &dtu.TimeRange{
		StartEpochMs: 1,
		EndEpochMs:   uint64(numEntriesForBuffer) + 1,
	}
	valueFilter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "*"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange:          queryRange,
	}
	qc := structs.InitQueryContext(IndexName, uint64(10), 9, 0, false)
	result := ExecuteQuery(simpleNode, &QueryAggregators{}, 59, qc)
	assert.Len(t, result.AllRecords, 1)

	qc.Scroll = 10
	result = ExecuteQuery(simpleNode, &QueryAggregators{}, 60, qc)
	assert.Len(t, result.AllRecords, 0)

	maxPossible := uint64(numBuffers * numEntriesForBuffer * fileCount)
	qc.SizeLimit = maxPossible
	qc.Scroll = int(maxPossible)
	result = ExecuteQuery(simpleNode, &QueryAggregators{}, 61, qc)
	assert.Len(t, result.AllRecords, 0)

	qc.Scroll = int(maxPossible - 5)
	result = ExecuteQuery(simpleNode, &QueryAggregators{}, 62, qc)
	assert.Len(t, result.AllRecords, 5)

}

func checkScrollRecords(response []toputils.Hits, resultSet *[]string) bool {
	log.Printf("Length of records fetched %d", len(response))
	for _, hit := range response {
		if contains(*resultSet, fmt.Sprintf("%v", hit.Source["key5"])) {
			return false
		}
		*resultSet = append(*resultSet, fmt.Sprintf("%v", hit.Source["key5"]))
	}
	return false
}

func contains(slice []string, element string) bool {
	for _, value := range slice {
		if value == element {
			return true
		}
	}
	return false
}

func getMyIds() []int64 {
	myids := make([]int64, 1)
	myids[0] = 0
	return myids
}

func Test_Query(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go query.PullQueriesToRun(ctx)
	defer cancel()

	dir := t.TempDir()
	t.Cleanup(func() { os.RemoveAll(dir) })

	config.InitializeTestingConfig(dir)

	limit.InitMemoryLimiter()
	instrumentation.InitMetrics()

	err := query.InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	if err != nil {
		log.Fatalf("Failed to initialize query node: %v", err)
	}

	numBuffers := 5
	numEntriesForBuffer := 10
	fileCount := 2
	_, err = metadata.InitMockColumnarMetadataStore(0, IndexName, fileCount, numBuffers, numEntriesForBuffer)
	assert.Nil(t, err)

	groupByQueryTestsForAsteriskQueries(t, numBuffers, numEntriesForBuffer, fileCount)
}

func Test_Scroll(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go query.PullQueriesToRun(ctx)
	defer cancel()

	dir := t.TempDir()
	t.Cleanup(func() { os.RemoveAll(dir) })

	config.InitializeTestingConfig(dir)

	limit.InitMemoryLimiter()

	err := query.InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	if err != nil {
		log.Fatalf("Failed to initialize query node: %v", err)
	}
	numBuffers := 5
	numEntriesForBuffer := 10
	fileCount := 2
	_, err = metadata.InitMockColumnarMetadataStore(0, "segexecution", fileCount, numBuffers, numEntriesForBuffer)
	assert.Nil(t, err)
	testESScroll(t, numBuffers, numEntriesForBuffer, fileCount)
	testPipesearchScroll(t, numBuffers, numEntriesForBuffer, fileCount)
}

func Test_unrotatedQuery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go query.PullQueriesToRun(ctx)
	defer cancel()

	t.Cleanup(func() { os.RemoveAll(config.GetDataPath()) })

	config.InitializeTestingConfig(t.TempDir())
	config.SetDataPath("unrotatedtest/")
	limit.InitMemoryLimiter()
	err := query.InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	assert.Nil(t, err)
	writer.InitWriterNode()
	numBatch := 10
	numRec := 100
	_ = vtable.InitVTable(server_utils.GetMyIds)

	// disable dict encoding globally
	writer.SetCardinalityLimit(0)

	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [64]byte

	for batch := 0; batch < numBatch; batch++ {
		for rec := 0; rec < numRec; rec++ {
			record := make(map[string]interface{})
			record["col1"] = "abc"
			record["col2"] = strconv.Itoa(rec)
			record["col3"] = "batch-" + strconv.Itoa(batch)
			record["col4"] = uuid.New().String()
			if rec >= numRec/2 {
				// add new column after it has reached halfway into filling a block
				// so that past records can we backfilled
				record["col5"] = "def"
			}
			record["timestamp"] = uint64(rec)
			rawJson, err := json.Marshal(record)
			assert.Nil(t, err)

			index := "test"
			ple := writer.NewPLE()
			ple.SetRawJson(rawJson)
			ple.SetTimestamp(uint64(rec) + 1)
			ple.SetIndexName(index)
			tsKey := "timestamp"

			err = writer.ParseRawJsonObject("", rawJson, &tsKey, jsParsingStackbuf[:], ple)
			assert.Nil(t, err)
			err = writer.AddEntryToInMemBuf("test1", index, false, SIGNAL_EVENTS, 0, 0,
				cnameCacheByteHashToStr, jsParsingStackbuf[:], []*writer.ParsedLogEvent{ple})
			assert.Nil(t, err)
		}

		sleep := time.Duration(1)
		time.Sleep(sleep)
		writer.FlushWipBufferToFile(&sleep, nil)
	}
	sleep := time.Duration(1)
	time.Sleep(sleep)
	writer.FlushWipBufferToFile(&sleep, nil)
	aggs := &QueryAggregators{
		EarlyExit: false,
	}

	// col3=batch-1
	value1, _ := CreateDtypeEnclosure("batch-1", 0)
	valueFilter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "col3"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 0,
			EndEpochMs:   uint64(numRec) + 1,
		},
	}
	sizeLimit := uint64(10000)
	scroll := 0
	qc := structs.InitQueryContext("test", sizeLimit, scroll, 0, false)
	result := ExecuteQuery(simpleNode, aggs, 63, qc)
	assert.Equal(t, uint64(numRec), result.TotalResults.TotalCount)
	assert.Equal(t, Equals, result.TotalResults.Op)

	// *=batch-1
	valueFilter = FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "*"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode = &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 0,
			EndEpochMs:   uint64(numRec) + 1,
		},
	}
	result = ExecuteQuery(simpleNode, aggs, 64, qc)
	query.DeleteQuery(uint64(numBatch * numRec * 2))
	assert.Equal(t, uint64(numRec), result.TotalResults.TotalCount)
	assert.Equal(t, Equals, result.TotalResults.Op)

	// *=def
	def, _ := CreateDtypeEnclosure("def", 0)
	valueFilter = FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "*"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: def}}},
		},
	}
	simpleNode = &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 0,
			EndEpochMs:   uint64(numRec) + 1,
		},
	}
	result = ExecuteQuery(simpleNode, aggs, 65, qc)
	backfillExpectecd := uint64(numRec*numBatch) / 2 // since we added new column halfway through the block
	assert.Equal(t, backfillExpectecd, result.TotalResults.TotalCount,
		"backfillExpectecd: %v, actual: %v", backfillExpectecd, result.TotalResults.TotalCount)
	assert.Equal(t, Equals, result.TotalResults.Op)

	// col5=def
	valueFilter = FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "col5"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: def}}},
		},
	}
	simpleNode = &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 0,
			EndEpochMs:   uint64(numRec) + 1,
		},
	}
	result = ExecuteQuery(simpleNode, aggs, 66, qc)
	assert.Equal(t, backfillExpectecd, result.TotalResults.TotalCount)
	assert.Equal(t, Equals, result.TotalResults.Op)
}

func Test_EncodeDecodeBlockSummary(t *testing.T) {
	dir := "data/"
	t.Cleanup(func() { os.RemoveAll(dir) })

	batchSize := 10
	entryCount := 10
	err := os.MkdirAll(dir, os.FileMode(0755))

	if err != nil {
		log.Fatal(err)
	}
	currFile := dir + "query_test.seg"
	_, blockSummaries, _, _, allBmhInMem, _ := writer.WriteMockColSegFile(currFile, currFile, batchSize, entryCount)
	blockSumFile := dir + "query_test.bsu"

	writer.WriteMockBlockSummary(blockSumFile, blockSummaries, allBmhInMem)
	blockSums, readAllBmh, err := microreader.ReadBlockSummaries(blockSumFile)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < len(blockSums); i++ {
		assert.Equal(t, blockSums[i].HighTs, blockSummaries[i].HighTs)
		assert.Equal(t, blockSums[i].LowTs, blockSummaries[i].LowTs)
		assert.Equal(t, blockSums[i].RecCount, blockSummaries[i].RecCount)

		// cnames are create in WriteMockColSegFile, we will only verify one of cnames
		// cnames start from key0..key11
		// key1 stores "value1", and the blockLen was calculated by running thw writemock.. func with print statement
		assert.Equal(t, uint32(30), readAllBmh[uint16(i)].ColBlockOffAndLen["key1"].Length)

		// For the block offset, i*30 is from the block size. We write the
		// blocks using utils.ChecksumFile, which has an additional header for
		// each chunk; that header is 12 bytes. So the total offset is
		// i*(30+12)
		assert.Equal(t, int64(i*(30+12)), readAllBmh[uint16(i)].ColBlockOffAndLen["key1"].Offset)
	}
}

func Benchmark_agileTreeQueryReader(t *testing.B) {
	// go test -run=Bench -bench=Benchmark_agileTreeQueryReader -benchmem -memprofile memprofile.out -o rawsearch_mem
	// go test -run=Bench -bench=Benchmark_agileTreeQueryReader -cpuprofile cpuprofile.out -o rawsearch_cpu

	segKeyPref := "/Users/kunalnawale/work/perf/siglens/data/Kunals-MacBook-Pro.local/final/ind-0/0-3544697602014606120/"

	grpByCols := []string{"passenger_count", "pickup_date", "trip_distance"}
	measureOps := []*structs.MeasureAggregator{
		{MeasureCol: "total_amount", MeasureFunc: utils.Count},
	}
	grpByRequest := &GroupByRequest{MeasureOperations: measureOps, GroupByColumns: grpByCols}

	aggs := &QueryAggregators{
		GroupByRequest: grpByRequest,
	}

	agileTreeBuf := make([]byte, 300_000_000)
	qid := uint64(67)
	qType := structs.QueryType(structs.RRCCmd)

	allSearchResults, err1 := segresults.InitSearchResults(0, aggs, qType, qid)
	assert.NoError(t, err1)

	numSegs := 114
	for skNum := 0; skNum < numSegs; skNum++ {
		sTime := time.Now()

		segKey := fmt.Sprintf("%v/%v/%v", segKeyPref, skNum, skNum)
		blkResults, err := blockresults.InitBlockResults(0, aggs, qid)
		assert.NoError(t, err)

		str, err := segread.InitNewAgileTreeReader(segKey, qid)
		assert.NoError(t, err)

		err1 := str.ApplyGroupByJit(grpByRequest.GroupByColumns, measureOps, blkResults, qid, agileTreeBuf)
		assert.NoError(t, err1)

		//log.Infof("Aggs seg: %v query, time: %+v", skNum, time.Since(sTime))

		res := blkResults.GetGroupByBuckets()
		assert.NotNil(t, res)
		assert.NotEqual(t, 0, res.Results)

		allSearchResults.AddBlockResults(blkResults)

		srRes := allSearchResults.BlockResults.GetGroupByBuckets()
		assert.NotNil(t, srRes)
		assert.NotEqual(t, 0, srRes.Results)

		log.Infof("Aggs query, segNum: %v, Num of bkt key: %v, time: %v", skNum,
			len(srRes.Results), time.Since(sTime))

		_ = allSearchResults.GetBucketResults()
	}

	res := allSearchResults.BlockResults.GetGroupByBuckets()
	log.Infof("Aggs query, Num of bkt key: %v", len(res.Results))

}

func measureColsTest(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int, measureCol string, measureFunc AggregateFunctions) {
	value1, _ := CreateDtypeEnclosure("value1", 0)
	// wildcard all columns
	ti := structs.InitTableInfo(IndexName, 0, false)
	allColumns := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "*"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&allColumns}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 0,
			EndEpochMs:   uint64(numEntriesForBuffer),
		},
	}
	qc := structs.InitQueryContextWithTableInfo(ti, 10000, 0, 0, false)
	result := ExecuteQuery(simpleNode, &QueryAggregators{
		MeasureOperations: []*MeasureAggregator{
			{MeasureCol: measureCol, MeasureFunc: measureFunc},
		},
	}, 67, qc)

	if measureCol == "*" && measureFunc != Count {
		assert.Len(t, result.AllRecords, 0)
		assert.Equal(t, 100, int(result.TotalResults.TotalCount))
		assert.False(t, result.TotalResults.EarlyExit)
	}
}

func groupByAggQueryTest(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int, measureCol string, measureFunc AggregateFunctions) *NodeResult {
	value1, _ := CreateDtypeEnclosure("value1", 0)
	ti := structs.InitTableInfo(IndexName, 0, false)
	allColumns := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key1"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&allColumns}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 1,
			EndEpochMs:   uint64(numEntriesForBuffer) + 1,
		},
	}

	simpleGroupBy := &QueryAggregators{
		GroupByRequest: &GroupByRequest{
			GroupByColumns: []string{"key11"},
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: measureCol, MeasureFunc: measureFunc},
			},
			AggName:     "test",
			BucketCount: 100,
		},
	}

	qc := structs.InitQueryContextWithTableInfo(ti, 10000, 0, 0, false)
	result := ExecuteQuery(simpleNode, simpleGroupBy, 68, qc)
	lenHist := len(result.Histogram["test"].Results)
	assert.False(t, result.Histogram["test"].IsDateHistogram)
	if measureFunc == Count {
		assert.Equal(t, lenHist, 2, "only record-batch-1 and record-batch-0 exist")
		totalentries := numEntriesForBuffer * fileCount * numBuffers
		for i := 0; i < lenHist; i++ {
			assert.Equal(t, result.Histogram["test"].Results[i].ElemCount, uint64(totalentries/2))
			bKey := result.Histogram["test"].Results[i].BucketKey
			assert.Len(t, result.Histogram["test"].Results[i].StatRes, len(simpleGroupBy.GroupByRequest.MeasureOperations))
			log.Infof("bkey is %+v", bKey)
			res, ok := result.Histogram["test"].Results[i].StatRes[fmt.Sprintf("count(%v)", measureCol)]
			assert.True(t, ok)
			assert.Equal(t, res.CVal, uint64(50))
		}
	} else if measureCol == "*" && measureFunc != Count {
		assert.NotZero(t, len(result.ErrList))
		assert.Len(t, result.AllRecords, 0)
		assert.Equal(t, 0, int(result.TotalResults.TotalCount))
		assert.False(t, result.TotalResults.EarlyExit)
	}

	return result
}

func groupByQueryTestsForAsteriskQueries(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	asteriskResult := groupByAggQueryTest(t, numBuffers, numEntriesForBuffer, fileCount, "*", Count)
	columnarResult := groupByAggQueryTest(t, numBuffers, numEntriesForBuffer, fileCount, "key11", Count)

	assert.Equal(t, asteriskResult.TotalRRCCount, columnarResult.TotalRRCCount)
	assert.Equal(t, asteriskResult.TotalResults.TotalCount, columnarResult.TotalResults.TotalCount)

	for recIdx, rec := range asteriskResult.AllRecords {
		assert.Equal(t, rec.RecordNum, columnarResult.AllRecords[recIdx].RecordNum)
		assert.Equal(t, rec.SortColumnValue, columnarResult.AllRecords[recIdx].SortColumnValue)
		assert.Equal(t, rec.VirtualTableName, columnarResult.AllRecords[recIdx].VirtualTableName)
	}

	groupByAggQueryTest(t, numBuffers, numEntriesForBuffer, fileCount, "*", Avg)
	measureColsTest(t, numBuffers, numEntriesForBuffer, fileCount, "*", Avg)
}
