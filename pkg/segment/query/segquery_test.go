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

package query

import (
	"os"
	"testing"
	"time"

	localstorage "github.com/siglens/siglens/pkg/blob/local"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/segment/memory/limit"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/query/pqs"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	. "github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	serverutils "github.com/siglens/siglens/pkg/server/utils"
	"github.com/stretchr/testify/assert"
)

func Test_extractRangeFilter(t *testing.T) {

	// 1.0 > col1
	leftLiteralEncoded, err := CreateDtypeEnclosure(1.0, 0)
	if err != nil {
		assert.Fail(t, "failed to encode 1.0", err)
	}
	leftInput := &SearchExpressionInput{
		ColumnValue: leftLiteralEncoded,
	}
	rightInput := &SearchExpressionInput{
		ColumnName: "col1",
	}

	rangeMap, newOp, isValid := ExtractRangeFilterFromSearch(leftInput, GreaterThan, rightInput, 0)
	assert.True(t, isValid, "valid range as 1.0 can be converted to a float")
	assert.Equal(t, newOp, LessThan, "Need to reflect to keep column on left: 1.0 > col1 --> col1 < 1.0")
	assert.Contains(t, rangeMap, "col1")

	_, _, isValid = ExtractRangeFilterFromSearch(leftInput, IsNull, rightInput, 0)
	assert.False(t, isValid, "Range for isNull operation is unsupported")

	_, _, isValid = ExtractRangeFilterFromSearch(leftInput, IsNotNull, rightInput, 0)
	assert.False(t, isValid, "Range for isNotNull operation is unsupported")

	abcdLiteralEncoded, err := CreateDtypeEnclosure("abcd", 0)
	if err != nil {
		assert.Fail(t, "failed to encode abcd", err)
	}
	leftInvalidInput := &SearchExpressionInput{
		ColumnValue: abcdLiteralEncoded,
	}
	_, _, isValid = ExtractRangeFilterFromSearch(leftInvalidInput, GreaterThan, rightInput, 0)
	assert.False(t, isValid, "Invalid literal that is not a number")
}

func bloomMetadataFilter(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	value1, _ := CreateDtypeEnclosure("value1", 0)
	valueFilter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key1"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: value1}}},
		},
	}
	timeRange := &dtu.TimeRange{
		StartEpochMs: 0,
		EndEpochMs:   uint64(numEntriesForBuffer),
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange:          timeRange,
	}
	searchNode := ConvertASTNodeToSearchNode(simpleNode, 0)
	ti := InitTableInfo("evts", 0, false)
	queryInfo, err := InitQueryInformation(searchNode, nil, timeRange, ti, uint64(numEntriesForBuffer*numBuffers*fileCount),
		4, 0, &DistributedQueryService{}, 0)
	assert.NoError(t, err)
	allQuerySegKeys, rawCount, _, pqsCount, err := getAllSegmentsInQuery(queryInfo, false, time.Now(), 0)
	assert.NoError(t, err)
	assert.Len(t, allQuerySegKeys, fileCount)
	assert.Equal(t, rawCount, uint64(fileCount))
	assert.Equal(t, pqsCount, uint64(0))

	summary := &summary.QuerySummary{}
	for _, qsr := range allQuerySegKeys {
		assert.Equal(t, RAW_SEARCH, qsr.sType)
		toSearch, err := qsr.GetMicroIndexFilter()
		assert.NoError(t, err)
		allSearchReq := ExtractSSRFromSearchNode(searchNode, toSearch, timeRange, ti.GetQueryTables(), summary, 2, true, queryInfo.pqid)
		assert.Len(t, allSearchReq, 1)
		// all blocks have key1==value1
		for key, value := range allSearchReq {
			assert.Equal(t, value.SegmentKey, key)
			assert.NotNil(t, value.SearchMetadata)
			assert.NotNil(t, value.SearchMetadata.BlockSummaries)
		}
	}

	batchOne, _ := CreateDtypeEnclosure("batch-1", 0)
	batchFilter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key1"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: batchOne}}},
		},
	}
	simpleNode = &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&batchFilter}},
		TimeRange:          timeRange,
	}
	searchNode = ConvertASTNodeToSearchNode(simpleNode, 0)
	queryInfo, err = InitQueryInformation(searchNode, nil, timeRange, ti, uint64(numEntriesForBuffer*numBuffers*fileCount),
		4, 1, &DistributedQueryService{}, 0)
	assert.NoError(t, err)
	allQuerySegKeys, rawCount, _, pqsCount, err = getAllSegmentsInQuery(queryInfo, false, time.Now(), 0)
	assert.NoError(t, err)
	assert.Len(t, allQuerySegKeys, fileCount)
	assert.Equal(t, rawCount, uint64(fileCount))
	assert.Equal(t, pqsCount, uint64(0))
	for _, qsr := range allQuerySegKeys {
		assert.Equal(t, RAW_SEARCH, qsr.sType)
		toSearch, err := qsr.GetMicroIndexFilter()
		assert.NoError(t, err)
		allSearchReq := ExtractSSRFromSearchNode(searchNode, toSearch, timeRange, ti.GetQueryTables(), summary, 2, true, queryInfo.pqid)
		assert.Len(t, allSearchReq, 0, "key1=batch-1 never exists, it only exists for key6")

	}

	batchFilter = FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key7"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: batchOne}}},
		},
	}
	simpleNode = &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&batchFilter}},
		TimeRange:          timeRange,
	}
	searchNode = ConvertASTNodeToSearchNode(simpleNode, 0)
	queryInfo, err = InitQueryInformation(searchNode, nil, timeRange, ti, uint64(numEntriesForBuffer*numBuffers*fileCount),
		4, 2, &DistributedQueryService{}, 0)
	assert.NoError(t, err)
	allQuerySegKeys, rawCount, _, pqsCount, err = getAllSegmentsInQuery(queryInfo, false, time.Now(), 0)
	assert.NoError(t, err)
	assert.Len(t, allQuerySegKeys, fileCount)
	assert.Equal(t, rawCount, uint64(fileCount))
	assert.Equal(t, pqsCount, uint64(0))
	for _, qsr := range allQuerySegKeys {
		assert.Equal(t, RAW_SEARCH, qsr.sType)
		toSearch, err := qsr.GetMicroIndexFilter()
		assert.NoError(t, err)
		allSearchReq := ExtractSSRFromSearchNode(searchNode, toSearch, timeRange, ti.GetQueryTables(), summary, 2, true, queryInfo.pqid)
		assert.Len(t, allSearchReq, 1, "key7 will have batch-1 in only one block")
		for key, value := range allSearchReq {
			assert.Equal(t, value.SegmentKey, key)
			assert.NotNil(t, value.SearchMetadata)
			assert.Len(t, value.AllBlocksToSearch, 1, "key7 will have batch-1 in only one block")
			assert.NotNil(t, value.SearchMetadata.BlockSummaries)
			assert.Contains(t, value.AllBlocksToSearch, uint16(1))
		}

	}
}

func rangeMetadataFilter(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	ti := InitTableInfo("evts", 0, false)
	zeroValue, _ := CreateDtypeEnclosure(0, 0)
	valueFilter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key8"}}},
			FilterOperator: GreaterThan,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: zeroValue}}},
		},
	}
	timeRange := &dtu.TimeRange{
		StartEpochMs: 0,
		EndEpochMs:   uint64(numEntriesForBuffer),
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange:          timeRange,
	}
	searchNode := ConvertASTNodeToSearchNode(simpleNode, 0)
	queryInfo, err := InitQueryInformation(searchNode, nil, timeRange, ti, uint64(numEntriesForBuffer*numBuffers*fileCount),
		4, 2, &DistributedQueryService{}, 0)
	assert.NoError(t, err)
	allQuerySegKeys, rawCount, _, pqsCount, err := getAllSegmentsInQuery(queryInfo, false, time.Now(), 0)
	assert.NoError(t, err)
	assert.Len(t, allQuerySegKeys, fileCount)
	assert.Equal(t, rawCount, uint64(fileCount))
	assert.Equal(t, pqsCount, uint64(0))

	summary := &summary.QuerySummary{}
	for _, qsr := range allQuerySegKeys {
		assert.Equal(t, RAW_SEARCH, qsr.sType)
		toSearch, err := qsr.GetMicroIndexFilter()
		assert.NoError(t, err)
		allSearchReq := ExtractSSRFromSearchNode(searchNode, toSearch, timeRange, ti.GetQueryTables(), summary, 2, true, queryInfo.pqid)
		assert.Len(t, allSearchReq, 1, "shouldve generated 1 SSR")
		for key, value := range allSearchReq {
			assert.Equal(t, value.SegmentKey, key)
			assert.NotNil(t, value.SearchMetadata)
			assert.NotNil(t, value.SearchMetadata.BlockSummaries)
			assert.Len(t, value.AllBlocksToSearch, numBuffers-1, "match all except for block 0")
			assert.NotContains(t, value.AllBlocksToSearch, uint16(0))
		}
	}

	valueFilter = FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key8"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: zeroValue}}},
		},
	}
	simpleNode = &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange:          timeRange,
	}
	searchNode = ConvertASTNodeToSearchNode(simpleNode, 0)
	queryInfo, err = InitQueryInformation(searchNode, nil, timeRange, ti, uint64(numEntriesForBuffer*numBuffers*fileCount),
		4, 2, &DistributedQueryService{}, 0)
	assert.NoError(t, err)
	allQuerySegKeys, rawCount, _, pqsCount, err = getAllSegmentsInQuery(queryInfo, false, time.Now(), 0)
	assert.NoError(t, err)
	assert.Len(t, allQuerySegKeys, fileCount)
	assert.Equal(t, rawCount, uint64(fileCount))
	assert.Equal(t, pqsCount, uint64(0))

	for _, qsr := range allQuerySegKeys {
		assert.Equal(t, RAW_SEARCH, qsr.sType)
		toSearch, err := qsr.GetMicroIndexFilter()
		assert.NoError(t, err)
		allSearchReq := ExtractSSRFromSearchNode(searchNode, toSearch, timeRange, ti.GetQueryTables(), summary, 2, true, queryInfo.pqid)
		assert.Len(t, allSearchReq, 1, "shouldve generated 1 SSR")
		for key, value := range allSearchReq {
			assert.Equal(t, value.SegmentKey, key)
			// only block 0 should match, but bc blooms are random, there is a non-zero chance another block will pass.
			// it is unlikely for >1 to pass, but technically it is possible, so this test is on the generous side
			assert.Less(t, len(value.AllBlocksToSearch), numBuffers/2)
			assert.Contains(t, value.AllBlocksToSearch, uint16(0))
			assert.NotNil(t, value.SearchMetadata)
			assert.NotNil(t, value.SearchMetadata.BlockSummaries)
		}
	}

	valueFilter = FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key8"}}},
			FilterOperator: LessThan,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: zeroValue}}},
		},
	}
	simpleNode = &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange:          timeRange,
	}
	searchNode = ConvertASTNodeToSearchNode(simpleNode, 0)
	queryInfo, err = InitQueryInformation(searchNode, nil, timeRange, ti, uint64(numEntriesForBuffer*numBuffers*fileCount),
		4, 2, &DistributedQueryService{}, 0)
	assert.NoError(t, err)
	allQuerySegKeys, rawCount, _, pqsCount, err = getAllSegmentsInQuery(queryInfo, false, time.Now(), 0)
	assert.NoError(t, err)
	assert.Len(t, allQuerySegKeys, fileCount)
	assert.Equal(t, rawCount, uint64(fileCount))
	assert.Equal(t, pqsCount, uint64(0))
	for _, qsr := range allQuerySegKeys {
		assert.Equal(t, RAW_SEARCH, qsr.sType)
		toSearch, err := qsr.GetMicroIndexFilter()
		assert.NoError(t, err)
		allSearchReq := ExtractSSRFromSearchNode(searchNode, toSearch, timeRange, ti.GetQueryTables(), summary, 2, true, queryInfo.pqid)
		assert.Len(t, allSearchReq, 0, "no blocks have <0")
	}
}

func pqsSegQuery(t *testing.T, numBuffers int, numEntriesForBuffer int, fileCount int) {
	config.SetPQSEnabled(true)

	// new generate mock rotated with pqs for subset of blocks
	// make sure raw search actually does raw search for the blocks not in sqpmr
	ti := InitTableInfo("evts", 0, false)
	fullTimeRange := &dtu.TimeRange{
		StartEpochMs: 0,
		EndEpochMs:   uint64(numEntriesForBuffer),
	}
	zero, _ := CreateDtypeEnclosure("record-batch-0", 0)
	valueFilter := FilterCriteria{
		ExpressionFilter: &ExpressionFilter{
			LeftInput:      &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnName: "key11"}}},
			FilterOperator: Equals,
			RightInput:     &FilterInput{Expression: &Expression{LeftInput: &ExpressionInput{ColumnValue: zero}}},
		},
	}
	simpleNode := &ASTNode{
		AndFilterCondition: &Condition{FilterCriteria: []*FilterCriteria{&valueFilter}},
		TimeRange:          fullTimeRange,
	}
	searchNode := ConvertASTNodeToSearchNode(simpleNode, 0)

	allPossibleKeys, finalCount, totalCount := metadata.FilterSegmentsByTime(fullTimeRange, ti.GetQueryTables(), 0)
	assert.Equal(t, len(allPossibleKeys), 1)
	assert.Contains(t, allPossibleKeys, "evts")
	assert.Len(t, allPossibleKeys["evts"], fileCount)
	assert.Equal(t, finalCount, totalCount)
	assert.Equal(t, finalCount, uint64(fileCount))

	pqid := querytracker.GetHashForQuery(searchNode)
	for tName, segKeys := range allPossibleKeys {
		for segKey := range segKeys {
			spqmr := pqmr.InitSegmentPQMResults()
			currSPQMRFile := segKey + "/pqmr/" + pqid + ".pqmr"
			for blkNum := 0; blkNum < numBuffers; blkNum++ {
				if blkNum%2 == 0 {
					continue // force raw search of even blocks
				}
				currPQMR := pqmr.CreatePQMatchResults(uint(numEntriesForBuffer))
				for recNum := 0; recNum < numEntriesForBuffer; recNum++ {
					if recNum%2 == 0 {
						currPQMR.AddMatchedRecord(uint(recNum))
					}
				}
				spqmr.SetBlockResults(uint16(blkNum), currPQMR)
				err := currPQMR.FlushPqmr(&currSPQMRFile, uint16(blkNum))
				assert.Nil(t, err, "no error on flush")
			}
			pqs.AddPersistentQueryResult(segKey, tName, pqid)
		}
	}
	querySummary := summary.InitQuerySummary(summary.LOGS, 1)
	sizeLimit := uint64(numBuffers * numEntriesForBuffer * fileCount)
	allSegFileResults, err := segresults.InitSearchResults(sizeLimit, nil, RRCCmd, 4)
	assert.Nil(t, err, "no error on init")
	queryInfo, err := InitQueryInformation(searchNode, nil, fullTimeRange, ti, uint64(numEntriesForBuffer*numBuffers*fileCount),
		4, 2, &DistributedQueryService{}, 0)
	assert.NoError(t, err)
	querySegmentRequests, numRawSearchKeys, _, numPQSKeys, err := getAllSegmentsInQuery(queryInfo, false, time.Now(), 0)
	assert.NoError(t, err)
	assert.Len(t, querySegmentRequests, fileCount, "each file has a query segment request")
	assert.Equal(t, uint64(0), numRawSearchKeys)
	assert.Equal(t, uint64(fileCount), numPQSKeys)

	for _, qsr := range querySegmentRequests {
		assert.Equal(t, PQS, qsr.sType)
		err := applyFilterOperatorSingleRequest(qsr, allSegFileResults, querySummary)
		assert.NoError(t, err)
		assert.Equal(t, RAW_SEARCH, qsr.sType, "changed type to raw search after pqs filtering")
		assert.NotNil(t, qsr.blkTracker, "added blkTacker after pqs filtering")
		fullBlkTracker, err := qsr.GetMicroIndexFilter()
		assert.NoError(t, err)
		assert.Contains(t, fullBlkTracker, "evts", "pqs raw search table")
		assert.Len(t, fullBlkTracker["evts"], 1)
		assert.Contains(t, fullBlkTracker["evts"], qsr.segKey, "resulting map should be map[tableName]->map[segKey]->blkTracker")
		for _, blkTracker := range fullBlkTracker["evts"] {
			for i := uint16(0); i < uint16(numBuffers); i++ {
				if i%2 == 0 {
					assert.True(t, blkTracker.ShouldProcessBlock(i), "Block %+v should be raw searched", i)
				} else {
					assert.False(t, blkTracker.ShouldProcessBlock(i), "Block %+v should not be raw searched", i)
				}
			}
		}

		ssrForMissingPQS := ExtractSSRFromSearchNode(searchNode, fullBlkTracker, fullTimeRange, ti.GetQueryTables(), querySummary, 1, true, pqid)
		assert.Len(t, ssrForMissingPQS, 1, "generate SSR one file at a time")
		for _, ssr := range ssrForMissingPQS {
			for i := uint16(0); i < uint16(numBuffers); i++ {
				if i%2 == 0 {
					assert.Contains(t, ssr.AllBlocksToSearch, i)
				} else {
					assert.NotContains(t, ssr.AllBlocksToSearch, i)
				}
			}
			assert.NotNil(t, ssr.SearchMetadata)
			assert.NotNil(t, ssr.SearchMetadata.BlockSummaries)
			assert.Len(t, ssr.SearchMetadata.BlockSummaries, numBuffers)
		}
	}
	qc := InitQueryContextWithTableInfo(ti, sizeLimit, 0, 0, false)
	// run a single query end to end
	nodeRes := ApplyFilterOperator(simpleNode, fullTimeRange, nil, 5, qc)
	assert.NotNil(t, nodeRes)
	assert.Len(t, nodeRes.ErrList, 0, "no errors")
	expectedCount := uint64((numBuffers*numEntriesForBuffer)/2) * uint64(fileCount)
	assert.Equal(t, expectedCount, nodeRes.TotalResults.TotalCount, "match using pqmr & not")
	assert.Equal(t, Equals, nodeRes.TotalResults.Op, "no early exit")
}

func Test_segQueryFilter(t *testing.T) {
	numBuffers := 5
	numEntriesForBuffer := 10
	fileCount := 5
	instrumentation.InitMetrics()
	_ = localstorage.InitLocalStorage()
	config.InitializeTestingConfig()
	limit.InitMemoryLimiter()
	err := InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	if err != nil {
		t.Fatalf("Failed to initialize query node: %v", err)
	}
	metadata.InitMockColumnarMetadataStore("data/", fileCount, numBuffers, numEntriesForBuffer)

	bloomMetadataFilter(t, numBuffers, numEntriesForBuffer, fileCount)
	rangeMetadataFilter(t, numBuffers, numEntriesForBuffer, fileCount)
	pqsSegQuery(t, numBuffers, numEntriesForBuffer, fileCount)
	// add more simple, complex, and nested metadata checking
	time.Sleep(1 * time.Second) // sleep to give some time for background pqs threads to write out dirs
	err = os.RemoveAll("data/")
	assert.Nil(t, err)
}
