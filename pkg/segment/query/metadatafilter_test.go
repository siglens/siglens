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

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/memory/limit"
	segmetadata "github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	. "github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	serverutils "github.com/siglens/siglens/pkg/server/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var IndexName string = "metadatafilter"

func testTimeFilter(t *testing.T, numBlocks int, numEntriesInBlock int, fileCount int) {
	tRange := &dtu.TimeRange{
		StartEpochMs: 0,
		EndEpochMs:   uint64(numEntriesInBlock),
	}

	timeFilteredFiles, totalChecked, passedCheck := segmetadata.FilterSegmentsByTime(tRange, []string{IndexName}, 0)
	log.Infof("time filter: %v", timeFilteredFiles)
	assert.Equal(t, passedCheck, uint64(fileCount), "all files passed")
	assert.Equal(t, totalChecked, uint64(fileCount), "all files passed")
	assert.Len(t, timeFilteredFiles, 1, "one table")
	assert.Contains(t, timeFilteredFiles, IndexName, "one table")
	assert.Len(t, timeFilteredFiles[IndexName], fileCount)

	// adding extra tables that do not exist should not change results
	extraTableFiles, totalChecked, passedCheck := segmetadata.FilterSegmentsByTime(tRange, []string{IndexName, "extra-table"}, 0)
	assert.Equal(t, passedCheck, uint64(fileCount), "all files passed")
	assert.Equal(t, totalChecked, uint64(fileCount), "all files passed")
	assert.Len(t, extraTableFiles, 1, "one table")
	assert.Contains(t, extraTableFiles, IndexName, "one table")
	assert.Len(t, extraTableFiles[IndexName], fileCount)

	// no results when no tables are given
	noTableFiles, totalChecked, passedCheck := segmetadata.FilterSegmentsByTime(tRange, []string{}, 0)
	assert.Equal(t, passedCheck, uint64(0), "no tables")
	assert.Equal(t, totalChecked, uint64(0), "no tables")
	assert.Len(t, noTableFiles, 0)
	assert.Len(t, noTableFiles[IndexName], 0)
}

func testBloomFilter(t *testing.T, numBlocks int, numEntriesInBlock int, fileCount int) {
	tRange := &dtu.TimeRange{
		StartEpochMs: 0,
		EndEpochMs:   uint64(numEntriesInBlock),
	}
	indexNames := []string{IndexName}
	value1, _ := sutils.CreateDtypeEnclosure("value1", 0)
	baseQuery := &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  &SearchExpressionInput{ColumnName: "key1"},
			FilterOp:         sutils.Equals,
			RightSearchInput: &SearchExpressionInput{ColumnValue: value1},
		},
		SearchType: SimpleExpression,
	}
	allFiles, _, _ := segmetadata.FilterSegmentsByTime(tRange, indexNames, 0)
	ti := InitTableInfo(IndexName, 0, false, nil)
	sn := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{baseQuery},
		},
	}
	qInfo, err := InitQueryInformation(sn, nil, tRange, ti, uint64(numBlocks*numEntriesInBlock*fileCount), 5, 1, nil, 0, 0, false)
	assert.NoError(t, err)
	qsrs := ConvertSegKeysToQueryRequests(qInfo, allFiles)
	keysToRawSearch, _, _ := FilterSegKeysToQueryResults(qInfo, qsrs)

	_, _, isRange := baseQuery.ExtractRangeFilterFromQuery(1)
	assert.False(t, isRange)

	blockbloomKeywords, _, wildcard, blockOp := baseQuery.GetAllBlockBloomKeysToSearch()
	assert.False(t, wildcard)

	assert.Len(t, blockbloomKeywords, 1)
	assert.Equal(t, blockOp, sutils.And)
	assert.Contains(t, blockbloomKeywords, "value1")
	assert.Len(t, keysToRawSearch, fileCount, "raw search all keys but got %+v. expected %+v", keysToRawSearch, fileCount)
	var rangeOp sutils.FilterOperator = sutils.Equals
	for _, qsr := range keysToRawSearch {
		assert.Equal(t, RAW_SEARCH, qsr.sType)
		blkTracker, err := qsr.GetMicroIndexFilter()
		assert.NoError(t, err, "no error should occur when getting block tracker")
		searchRequests, checkedBlocks, matchedBlocks, errs := getAllSearchRequestsFromCmi(baseQuery, tRange, blkTracker,
			blockbloomKeywords, nil, blockOp, nil, rangeOp, false, wildcard, 0, true, qsr.pqid)
		assert.Len(t, errs, 0)
		assert.Len(t, searchRequests, 1, "one file at a time")
		assert.Equal(t, uint64(numBlocks), checkedBlocks, "checkedBlocks blocks is not as expected")
		assert.Equal(t, uint64(numBlocks), matchedBlocks, "matchedBlocks blocks is not as expected")
		for _, sReq := range searchRequests {
			assert.Len(t, sReq.AllBlocksToSearch, len(sReq.SearchMetadata.BlockSummaries))
		}
	}

	var randomFile string
	for fileName := range allFiles[IndexName] {
		randomFile = fileName
		break
	}
	log.Infof("Searching for file %s", randomFile)
	randomFileDTE, _ := sutils.CreateDtypeEnclosure(randomFile, 0)
	fileNameQuery := &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  &SearchExpressionInput{ColumnName: "key10"},
			FilterOp:         sutils.Equals,
			RightSearchInput: &SearchExpressionInput{ColumnValue: randomFileDTE},
		},
		SearchType: SimpleExpression,
	}
	blockbloomKeywords, _, wildcard, blockOp = fileNameQuery.GetAllBlockBloomKeysToSearch()
	assert.False(t, wildcard)
	assert.Len(t, blockbloomKeywords, 1)
	assert.Equal(t, blockOp, sutils.And)
	assert.Contains(t, blockbloomKeywords, randomFile)

	assert.Len(t, keysToRawSearch, fileCount, "raw search all keys but got %+v. expected %+v", keysToRawSearch, fileCount)
	for _, qsr := range keysToRawSearch {
		assert.Equal(t, RAW_SEARCH, qsr.sType)
		blkTracker, err := qsr.GetMicroIndexFilter()
		assert.NoError(t, err, "no error should occur when getting block tracker")
		searchRequests, checkedBlocks, matchedBlocks, errs := getAllSearchRequestsFromCmi(fileNameQuery, tRange, blkTracker,
			blockbloomKeywords, nil, blockOp, nil, rangeOp, false, wildcard, 0, true, qsr.pqid)
		assert.Len(t, errs, 0)
		assert.Equal(t, uint64(numBlocks), checkedBlocks, "all blocks will be checked")
		if qsr.segKey == randomFile {
			assert.Len(t, searchRequests, 1, "file with segKey == %+v should be the only match", qsr.segKey)
			assert.Equal(t, uint64(numBlocks), matchedBlocks, "a single file with have the right value for key10")
			for _, sReq := range searchRequests {
				assert.Len(t, sReq.AllBlocksToSearch, len(sReq.SearchMetadata.BlockSummaries))
			}
		} else {
			assert.Len(t, searchRequests, 0, "should not generate an ssr with key %+v when looking for %+v", qsr.segKey, randomFile)
			assert.Equal(t, uint64(0), matchedBlocks, "no matched blocks")
		}
	}

	// key7 == batch-1 test
	batchOne, _ := sutils.CreateDtypeEnclosure("batch-1", 0)
	batchQuery := &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  &SearchExpressionInput{ColumnName: "key7"},
			FilterOp:         sutils.Equals,
			RightSearchInput: &SearchExpressionInput{ColumnValue: batchOne},
		},
		SearchType: SimpleExpression,
	}
	allFiles, _, _ = segmetadata.FilterSegmentsByTime(tRange, []string{IndexName}, 0)
	qsrs = ConvertSegKeysToQueryRequests(qInfo, allFiles)
	keysToRawSearch, _, _ = FilterSegKeysToQueryResults(qInfo, qsrs)

	blockbloomKeywords, _, wildcard, blockOp = batchQuery.GetAllBlockBloomKeysToSearch()
	assert.False(t, wildcard)
	assert.Len(t, blockbloomKeywords, 1)
	assert.Equal(t, blockOp, sutils.And)
	assert.Contains(t, blockbloomKeywords, "batch-1")
	log.Infof("batch query block bloom keys : %v, block op %v", blockbloomKeywords, blockOp)

	assert.Len(t, keysToRawSearch, fileCount, "raw search all keys but got %+v. expected %+v", keysToRawSearch, fileCount)
	for _, qsr := range keysToRawSearch {
		assert.Equal(t, RAW_SEARCH, qsr.sType)
		blkTracker, err := qsr.GetMicroIndexFilter()
		assert.NoError(t, err, "no error should occur when getting block tracker")
		searchRequests, checkedBlocks, matchedBlocks, errs := getAllSearchRequestsFromCmi(batchQuery, tRange, blkTracker,
			blockbloomKeywords, nil, blockOp, nil, rangeOp, false, wildcard, 0, true, qsr.pqid)
		assert.Len(t, errs, 0)
		assert.Len(t, searchRequests, 1, "process single request at a time")
		assert.Equal(t, uint64(numBlocks), checkedBlocks, "each file will should have a single matching block")
		assert.Equal(t, uint64(1), matchedBlocks, "each file will should have a single matching block")
		for _, sReq := range searchRequests {
			assert.Len(t, sReq.AllBlocksToSearch, 1)
			assert.Contains(t, sReq.AllBlocksToSearch, uint16(1))
		}
	}

	batchWildcardQuery := &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  &SearchExpressionInput{ColumnName: "*"},
			FilterOp:         sutils.Equals,
			RightSearchInput: &SearchExpressionInput{ColumnValue: batchOne},
		},
		SearchType: SimpleExpression,
	}

	// changing col name has no effect on block bloom keys
	blockbloomKeywords, nil, wildcardValue, blockOp := batchWildcardQuery.GetAllBlockBloomKeysToSearch()
	assert.False(t, wildcardValue)
	assert.Len(t, blockbloomKeywords, 1)
	assert.Equal(t, blockOp, sutils.And)
	assert.Contains(t, blockbloomKeywords, "batch-1")
	cols, wildcard := batchWildcardQuery.GetAllColumnsInQuery()
	assert.True(t, wildcard)
	assert.Len(t, cols, 0)

	for _, qsr := range keysToRawSearch {
		blkTracker, err := qsr.GetMicroIndexFilter()
		assert.NoError(t, err, "no error should occur when getting block tracker")
		searchRequests, checkedBlocks, matchedBlocks, errs := getAllSearchRequestsFromCmi(batchWildcardQuery, tRange, blkTracker,
			blockbloomKeywords, nil, blockOp, nil, rangeOp, false, wildcardValue, 0, true, qsr.pqid)
		assert.Len(t, errs, 0)
		assert.Len(t, searchRequests, 1, "one file at a time key7=batch-1")
		assert.Equal(t, uint64(numBlocks), checkedBlocks, "each file will should have a single matching block")
		assert.GreaterOrEqual(t, matchedBlocks, uint64(1), "each file will should have a >=1 matching block")
		for _, sReq := range searchRequests {
			assert.GreaterOrEqual(t, len(sReq.AllBlocksToSearch), 1)
			assert.Contains(t, sReq.AllBlocksToSearch, uint16(1))
		}
	}
}

func testRangeFilter(t *testing.T, numBlocks int, numEntriesInBlock int, fileCount int) {
	tRange := &dtu.TimeRange{
		StartEpochMs: 0,
		EndEpochMs:   uint64(numEntriesInBlock),
	}
	rangeValue, _ := sutils.CreateDtypeEnclosure(int64(0), 0)
	rangeQuery := &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  &SearchExpressionInput{ColumnName: "key8"},
			FilterOp:         sutils.Equals,
			RightSearchInput: &SearchExpressionInput{ColumnValue: rangeValue},
		},
		SearchType: SimpleExpression,
	}
	allFiles, _, _ := segmetadata.FilterSegmentsByTime(tRange, []string{IndexName}, 0)
	ti := InitTableInfo(IndexName, 0, false, nil)
	sn := &SearchNode{
		AndSearchConditions: &SearchCondition{
			SearchQueries: []*SearchQuery{rangeQuery},
		},
	}
	qInfo, err := InitQueryInformation(sn, nil, tRange, ti, uint64(numBlocks*numEntriesInBlock*fileCount), 5, 1, nil, 0, 0, false)
	assert.NoError(t, err)
	qsrs := ConvertSegKeysToQueryRequests(qInfo, allFiles)
	keysToRawSearch, _, _ := FilterSegKeysToQueryResults(qInfo, qsrs)
	rangeFilter, rangeOp, isRange := rangeQuery.ExtractRangeFilterFromQuery(1)
	log.Infof("Extracting range query. Filter %+v, RangeOp %+v", rangeFilter, rangeOp)
	assert.True(t, isRange)

	for _, qsr := range keysToRawSearch {
		assert.Equal(t, RAW_SEARCH, qsr.sType)
		blkTracker, err := qsr.GetMicroIndexFilter()
		assert.NoError(t, err, "no error should occur when getting block tracker")
		finalRangeRequests, totalChecked, passedBlocks, errs := getAllSearchRequestsFromCmi(rangeQuery, tRange, blkTracker,
			nil, nil, sutils.And, rangeFilter, rangeOp, true, false, 0, true, qsr.pqid)
		assert.Len(t, errs, 0)
		assert.Equal(t, uint64(numBlocks), totalChecked)
		assert.Equal(t, uint64(1), passedBlocks, "one block in each file matches")
		for _, sReq := range finalRangeRequests {
			assert.Len(t, sReq.AllBlocksToSearch, 1)
			assert.Contains(t, sReq.AllBlocksToSearch, uint16(0))
			log.Infof("sReq %+v", sReq.AllBlocksToSearch)
		}
	}
}

func getMyIds() []int64 {
	myids := make([]int64, 1)
	myids[0] = 0
	return myids
}

func Test_MetadataFilter(t *testing.T) {
	numBlocks := 5
	numEntriesInBlock := 10
	numColumns := 5
	dir := t.TempDir()
	config.InitializeTestingConfig(dir)
	limit.InitMemoryLimiter()
	err := InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	if err != nil {
		t.Fatalf("Failed to initialize query node: %v", err)
	}

	_, err = metadata.InitMockColumnarMetadataStore(0, "metadatafilter", numColumns, numBlocks, numEntriesInBlock)
	assert.Nil(t, err)

	testTimeFilter(t, numBlocks, numEntriesInBlock, numColumns)
	testBloomFilter(t, numBlocks, numEntriesInBlock, numColumns)
	testRangeFilter(t, numBlocks, numEntriesInBlock, numColumns)

	os.RemoveAll(dir)
}
