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

package query

import (
	"sync"
	"sync/atomic"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	. "github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

type ResultSegmentSearchRequestMap struct {
	Result map[string]*SegmentSearchRequest
	Err    error
}

/*
Top level micro index checking function. For a filter, input segkeys, timeRange, indexNames will do the following:
1. blockbloom/ blockrange filtering
2. search request generation

Assumes that filesToSearch has been time filtered
Returns a map[string]*SegmentSearchRequest mapping a segment key to the corresponding search request and any errors
*/
func MicroIndexCheck(currQuery *SearchQuery, filesToSearch map[string]map[string]*BlockTracker, timeRange *dtu.TimeRange,
	indexNames []string, querySummary *summary.QuerySummary, qid uint64, isQueryPersistent bool, pqid string) (map[string]*SegmentSearchRequest, error) {

	rangeFilter, rangeOp, isRange := currQuery.ExtractRangeFilterFromQuery(qid)
	bloomWords, wildcardBloom, bloomOp := currQuery.GetAllBlockBloomKeysToSearch()

	finalFilteredRequest, blocksChecked, blockCount := filterViaMicroIndices(currQuery, indexNames, timeRange,
		filesToSearch, bloomWords, bloomOp, rangeFilter, rangeOp, wildcardBloom, isRange, qid, isQueryPersistent, pqid)
	querySummary.UpdateCMIResults(blocksChecked, blockCount)
	return finalFilteredRequest, nil
}

// returns final SSRs, count of total blocks checked, count of blocks that passed
func filterViaMicroIndices(currQuery *structs.SearchQuery, indexNames []string, timeRange *dtu.TimeRange,
	filesToSearch map[string]map[string]*BlockTracker, bloomWords map[string]bool, bloomOp LogicalOperator,
	rangeFilter map[string]string, rangeOp utils.FilterOperator, wildCardValue bool,
	isRange bool, qid uint64, isQueryPersistent bool, pqid string) (map[string]*SegmentSearchRequest, uint64, uint64) {

	finalResults := make(map[string]*SegmentSearchRequest)

	serResults, totalBlocks, finalBlockCount, errors := getAllSearchRequestsFromCmi(currQuery, timeRange,
		filesToSearch, bloomWords, bloomOp, rangeFilter, rangeOp, isRange, wildCardValue, qid, isQueryPersistent, pqid)

	if len(errors) > 0 {
		for _, err := range errors {
			log.Errorf("qid=%d filterViaMicroIndices: Failed to get search request from microindices: %+v", qid, err)
		}
	}

	for _, sReq := range serResults {
		finalResults[sReq.SegmentKey] = sReq
	}
	return finalResults, totalBlocks, finalBlockCount
}

// returns a list of search request, max possible number of blocks, num blocks to be searched, error
func getAllSearchRequestsFromCmi(currQuery *structs.SearchQuery, timeRange *dtu.TimeRange,
	segkeysToCheck map[string]map[string]*BlockTracker, bloomKeys map[string]bool, bloomOp utils.LogicalOperator,
	rangeFilter map[string]string, rangeOp utils.FilterOperator, isRange bool, wildCardValue bool,
	qid uint64, isQueryPersistent bool, pqid string) ([]*structs.SegmentSearchRequest, uint64, uint64, []error) {

	sizeChannel := 0
	for _, segKeys := range segkeysToCheck {
		sizeChannel += len(segKeys)
	}
	finalTotalBlockCount := uint64(0)
	finalFilteredBlockCount := uint64(0)
	finalSearchRequests := make([]*structs.SegmentSearchRequest, 0)
	finalSearchRequestErrors := make([]error, 0)
	searchRequestResults := make(chan *structs.SegmentSearchRequest, sizeChannel)
	searchRequestErrors := make(chan error, sizeChannel)

	colsToCheck, wildcardColQuery := currQuery.GetAllColumnsInQuery()
	delete(colsToCheck, config.GetTimeStampKey()) // timestamp should not be checked in cmi
	var blockWG sync.WaitGroup
	for indexName, segKeys := range segkeysToCheck {
		for segkey, blockTracker := range segKeys {
			blockWG.Add(1)
			go func(key, indName string, blkT *BlockTracker) {
				defer blockWG.Done()
				finalReq, totalBlockCount, filteredBlockCount, err := metadata.RunCmiCheck(key, indName, timeRange, blkT, bloomKeys, bloomOp,
					rangeFilter, rangeOp, isRange, wildCardValue, currQuery, colsToCheck, wildcardColQuery, qid, isQueryPersistent, pqid)
				if err != nil {
					log.Errorf("qid=%d, getAllSearchRequestsFromCmi: Failed to get search request from cmi: %+v", qid, err)
					searchRequestErrors <- err
				} else {
					searchRequestResults <- finalReq
				}
				atomic.AddUint64(&finalTotalBlockCount, totalBlockCount)
				atomic.AddUint64(&finalFilteredBlockCount, filteredBlockCount)
			}(segkey, indexName, blockTracker)
		}
	}
	go func() {
		blockWG.Wait()
		close(searchRequestResults)
		close(searchRequestErrors)
	}()

	for req := range searchRequestResults {
		if req != nil {
			finalSearchRequests = append(finalSearchRequests, req)
		}
	}

	for err := range searchRequestErrors {
		finalSearchRequestErrors = append(finalSearchRequestErrors, err)
	}
	return finalSearchRequests, finalTotalBlockCount, finalFilteredBlockCount, finalSearchRequestErrors
}
