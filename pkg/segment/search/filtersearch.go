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

package search

import (
	"sync"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/reader/segread"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

// Search a single SearchQuery and returns which records passes the filter
func RawSearchSingleQuery(query *structs.SearchQuery, searchReq *structs.SegmentSearchRequest, segmentSearch *SegmentSearchStatus,
	allBlockSearchHelpers []*structs.BlockSearchHelper, op utils.LogicalOperator, queryMetrics *structs.QueryProcessingMetrics, qid uint64,
	allSearchResults *segresults.SearchResults) *SegmentSearchStatus {

	queryType := query.GetQueryType()
	searchCols := getAllColumnsNeededForSearch(query, searchReq.AllPossibleColumns)
	sharedMultiReader, err := segread.InitSharedMultiColumnReaders(searchReq.SegmentKey, searchCols, searchReq.AllBlocksToSearch,
		searchReq.SearchMetadata.BlockSummaries, len(allBlockSearchHelpers), qid)

	if err != nil {
		// if we fail to read needed columns, we can convert it to a match none
		// TODO: what would this look like in complex relations
		queryType = structs.EditQueryTypeForInvalidColumn(queryType)
		log.Warnf("qid=%d, filterBlockRequestFromQuery: Unable to read all columns in query new query type %+v",
			qid, queryType)
		log.Warnf("qid=%d, filterBlockRequestFromQuery: Tried to initialized a multi reader for %+v. Error: %v",
			qid, searchCols, err)
	}

	defer sharedMultiReader.Close()
	// call N parallel block managers, each with their own block
	filterBlockRequestsChan := make(chan *BlockSearchStatus, len(segmentSearch.AllBlockStatus))
	for _, filterReq := range segmentSearch.AllBlockStatus {
		filterBlockRequestsChan <- filterReq
	}
	close(filterBlockRequestsChan)

	var runningBlockManagers sync.WaitGroup
	for i, blockHelper := range allBlockSearchHelpers {
		runningBlockManagers.Add(1)
		go filterBlockRequestFromQuery(sharedMultiReader.MultiColReaders[i], query, segmentSearch,
			filterBlockRequestsChan, blockHelper, &runningBlockManagers, op, queryType, qid,
			allSearchResults, searchReq)
	}
	runningBlockManagers.Wait()
	logSingleQuerySummary(segmentSearch, op, qid)
	return segmentSearch
}

func logSingleQuerySummary(segmentSearch *SegmentSearchStatus, op utils.LogicalOperator, qid uint64) {
	if config.IsDebugMode() {
		opStr := utils.ConvertOperatorToString(op)
		sumMatched, sumUnmatched := segmentSearch.getTotalCounts()
		log.Infof("qid=%d, After a %+v op, there are %+v total matched records and %+v total unmatched records",
			qid, opStr, sumMatched, sumUnmatched)
	}
}

func getAllColumnsNeededForSearch(query *structs.SearchQuery, allCols map[string]bool) map[string]bool {
	searchCols, wildcard := query.GetAllColumnsInQuery()
	if wildcard && query.SearchType != structs.MatchAll {
		searchCols = allCols
	}

	return searchCols
}

func filterBlockRequestFromQuery(multiColReader *segread.MultiColSegmentReader, query *structs.SearchQuery,
	segmentSearch *SegmentSearchStatus, resultsChan chan *BlockSearchStatus, blockHelper *structs.BlockSearchHelper,
	runningBlockManagers *sync.WaitGroup, op utils.LogicalOperator, queryType structs.SearchNodeType,
	qid uint64, allSearchResults *segresults.SearchResults, searchReq *structs.SegmentSearchRequest) {

	defer runningBlockManagers.Done() // defer in case of panics

	holderDte := &utils.DtypeEnclosure{}
	for blockReq := range resultsChan {
		blockHelper.ResetBlockHelper()
		recIT, err := segmentSearch.GetRecordIteratorForBlock(op, blockReq.BlockNum)
		if err != nil {
			log.Errorf("qid=%d filterBlockRequestFromQuery failed to get next search set for block %d! Err %+v", qid, blockReq.BlockNum, err)
			allSearchResults.AddError(err)
			break
		}
		switch queryType {
		case structs.MatchAllQuery:
			// time should have been checked before, and recsToSearch
			for i := uint(0); i < uint(recIT.AllRecLen); i++ {
				if recIT.ShouldProcessRecord(i) {
					blockHelper.AddMatchedRecord(i)
				}
			}
		case structs.ColumnValueQuery:
			filterRecordsFromSearchQuery(query, segmentSearch, blockHelper, multiColReader, recIT,
				blockReq.BlockNum, holderDte, qid, allSearchResults, searchReq)
		case structs.InvalidQuery:
			// don't match any records
		}
		matchedRecords := blockHelper.GetAllMatchedRecords()
		err = segmentSearch.updateMatchedRecords(blockReq.BlockNum, matchedRecords, op)
		if err != nil {
			log.Errorf("qid=%d, filterBlockRequestFromQuery failed to update segment search status with matched records %+v. Error %+v", qid, matchedRecords, err)
			allSearchResults.AddError(err)
			break
		}
	}
}

func filterRecordsFromSearchQuery(query *structs.SearchQuery, segmentSearch *SegmentSearchStatus,
	blockHelper *structs.BlockSearchHelper,
	multiColReader *segread.MultiColSegmentReader, recIT *BlockRecordIterator, blockNum uint16,
	holderDte *utils.DtypeEnclosure, qid uint64, allSearchResults *segresults.SearchResults,
	searchReq *structs.SegmentSearchRequest) {

	// first we walk through the search checking if this query can be satisfied by looking at the
	// dict encoding file for the column/s
	cmiPassedCnames := make(map[string]bool)
	for _, colInfo := range multiColReader.AllColums {
		_, ok := searchReq.CmiPassedCnames[blockNum][colInfo.ColumnName]
		if ok {
			cmiPassedCnames[colInfo.ColumnName] = true
		}
	}

	doRecLevelSearch, deCnames, err := applyColumnarSearchUsingDictEnc(query, multiColReader, blockNum, qid,
		recIT, blockHelper, searchReq, cmiPassedCnames)
	if err != nil {
		allSearchResults.AddError(err)
		// we still continue, since the reclevel may not yield an error
	}

	// we go through all of the cmi-passed-columnnames, if all of them have already been checked in
	// the dict-enc func above, then we don't need to do rec-by-rec search
	if doRecLevelSearch {
		for cname := range cmiPassedCnames {
			_, ok := deCnames[cname]
			if !ok {
				doRecLevelSearch = true
				break
			} else {
				doRecLevelSearch = false
			}
		}
	}

	if doRecLevelSearch {
		for i := uint(0); i < uint(recIT.AllRecLen); i++ {
			if recIT.ShouldProcessRecord(i) {
				matched, err := ApplyColumnarSearchQuery(query, multiColReader, blockNum, uint16(i), holderDte,
					qid, deCnames, searchReq, cmiPassedCnames)
				if err != nil {
					allSearchResults.AddError(err)
					break
				}
				if matched {
					blockHelper.AddMatchedRecord(i)
				}
			}
		}
	}
	multiColReader.ReorderColumnUsage()
}
