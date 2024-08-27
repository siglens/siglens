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

package metadata

import (
	"sync"
	"sync/atomic"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	log "github.com/sirupsen/logrus"
)

// returns the search request. The bool will tell if the request is valid or not
func createSearchRequestForUnrotated(fileName string, tableName string,
	filteredBlocks map[uint16]map[string]bool,
	unrotatedInfo *writer.UnrotatedSegmentInfo) (*structs.SegmentSearchRequest, bool) {
	if len(filteredBlocks) == 0 {
		return nil, false
	}

	blkSum, blkMeta, unrotatedCols := unrotatedInfo.GetUnrotatedBlockInfoForQuery()

	searchMeta := make(map[uint16]*structs.BlockMetadataHolder)
	for blkNum := range filteredBlocks {
		currBlkMeta, ok := blkMeta[blkNum]
		if !ok {
			log.Warnf("createSearchRequestForUnrotated: block %d does not exist in unrotated block list but passed initial filtering",
				blkNum)
			continue
		}
		searchMeta[blkNum] = currBlkMeta
	}

	finalReq := &structs.SegmentSearchRequest{
		SegmentKey: fileName,
		SearchMetadata: &structs.SearchMetadataHolder{
			BlockSummaries: blkSum,
		},
		VirtualTableName:   tableName,
		AllBlocksToSearch:  searchMeta,
		AllPossibleColumns: unrotatedCols,
		LatestEpochMS:      unrotatedInfo.GetTimeRange().EndEpochMs,
		CmiPassedCnames:    filteredBlocks,
	}
	return finalReq, true
}

// filters unrotated blocks based on search conditions
// returns the final search request, total blocks, sum of filtered blocks, and any errors
func CheckMicroIndicesForUnrotated(currQuery *structs.SearchQuery, lookupTimeRange *dtu.TimeRange, indexNames []string,
	allBlocksToSearch map[string]map[string]*structs.BlockTracker, bloomWords map[string]bool, bloomOp utils.LogicalOperator, rangeFilter map[string]string,
	rangeOp utils.FilterOperator, isRange bool, wildcardValue bool, qid uint64) (map[string]*structs.SegmentSearchRequest, uint64, uint64, error) {

	writer.UnrotatedInfoLock.RLock()
	defer writer.UnrotatedInfoLock.RUnlock()
	res := make(map[string]*structs.SegmentSearchRequest)
	matchedFiles := make(chan *structs.SegmentSearchRequest)
	var err error
	var wg sync.WaitGroup
	totalUnrotatedBlocks := uint64(0)
	totalFilteredBlocks := uint64(0)

	for _, rawSearchKeys := range allBlocksToSearch {
		for segKey, blkTracker := range rawSearchKeys {
			usi, ok := writer.AllUnrotatedSegmentInfo[segKey]
			if !ok {
				log.Errorf("qid=%d, CheckMicroIndicesForUnrotated: SegKey %+v does not exist in unrotated information", qid, segKey)
				continue
			}
			wg.Add(1)
			go func(sKey string, store *writer.UnrotatedSegmentInfo, blkT *structs.BlockTracker) {
				defer wg.Done()

				filteredBlocks, maxBlocks, numFiltered, err := store.DoCMICheckForUnrotated(currQuery, lookupTimeRange,
					blkT, bloomWords, bloomOp, rangeFilter, rangeOp, isRange, wildcardValue, qid)
				atomic.AddUint64(&totalUnrotatedBlocks, maxBlocks)
				atomic.AddUint64(&totalFilteredBlocks, numFiltered)
				if err != nil {
					log.Errorf("qid=%d, CheckMicroIndicesForUnrotated: Error getting block summaries from vtable %s and segfile %s err=%v",
						qid, store.TableName, sKey, err)
				} else {
					finalReq, valid := createSearchRequestForUnrotated(sKey, store.TableName,
						filteredBlocks, store)
					if valid && finalReq != nil {
						matchedFiles <- finalReq
					}
				}
			}(segKey, usi, blkTracker)
		}
	}

	go func() {
		wg.Wait()
		close(matchedFiles)
	}()
	for readRequest := range matchedFiles {
		res[readRequest.SegmentKey] = readRequest
	}
	return res, totalUnrotatedBlocks, totalFilteredBlocks, err

}

func ExtractUnrotatedSSRFromSearchNode(node *structs.SearchNode, timeRange *dtu.TimeRange, indexNames []string,
	rawSearchKeys map[string]map[string]*structs.BlockTracker, querySummary *summary.QuerySummary, qid uint64) map[string]*structs.SegmentSearchRequest {
	// todo: better joining of intermediate results of block summaries
	finalList := make(map[string]*structs.SegmentSearchRequest)

	if node.AndSearchConditions != nil {
		andSegmentFiles := extractUnrotatedSSRFromCondition(node.AndSearchConditions, segutils.And, timeRange, indexNames,
			rawSearchKeys, querySummary, qid)
		for fileName, searchReq := range andSegmentFiles {
			if _, ok := finalList[fileName]; !ok {
				finalList[fileName] = searchReq
				continue
			}
			finalList[fileName].JoinRequest(searchReq, segutils.And)
		}
	}

	if node.OrSearchConditions != nil {
		orSegmentFiles := extractUnrotatedSSRFromCondition(node.OrSearchConditions, segutils.Or, timeRange, indexNames,
			rawSearchKeys, querySummary, qid)
		for fileName, searchReq := range orSegmentFiles {
			if _, ok := finalList[fileName]; !ok {
				finalList[fileName] = searchReq
				continue
			}
			finalList[fileName].JoinRequest(searchReq, segutils.Or)
		}
	}
	// for exclusion, only join the column info for files that exist and not the actual search request info
	// exclusion conditions should not influence raw blocks to search
	if node.ExclusionSearchConditions != nil {
		exclustionSegmentFiles := extractUnrotatedSSRFromCondition(node.ExclusionSearchConditions, segutils.And, timeRange, indexNames,
			rawSearchKeys, querySummary, qid)
		for fileName, searchReq := range exclustionSegmentFiles {
			if _, ok := finalList[fileName]; !ok {
				continue
			}
			finalList[fileName].JoinColumnInfo(searchReq)
		}
	}

	return finalList
}

func extractUnrotatedSSRFromCondition(condition *structs.SearchCondition, op segutils.LogicalOperator, timeRange *dtu.TimeRange,
	indexNames []string, rawSearchKeys map[string]map[string]*structs.BlockTracker, querySummary *summary.QuerySummary,
	qid uint64) map[string]*structs.SegmentSearchRequest {
	finalSegFiles := make(map[string]*structs.SegmentSearchRequest)
	if condition.SearchQueries != nil {

		for _, query := range condition.SearchQueries {
			rangeFilter, rangeOp, isRange := query.ExtractRangeFilterFromQuery(qid)
			bloomWords, wildcardBloom, bloomOp := query.GetAllBlockBloomKeysToSearch()
			res, totalUnrotatedBlocks, filteredUnrotatedBlocks, err := CheckMicroIndicesForUnrotated(query, timeRange, indexNames,
				rawSearchKeys, bloomWords, bloomOp, rangeFilter, rangeOp, isRange, wildcardBloom, qid)

			if err != nil {
				log.Errorf("qid=%d, extractUnrotatedSSRFromCondition: an error occurred while checking unrotated data %+v", qid, err)
				continue
			}
			querySummary.UpdateCMIResults(totalUnrotatedBlocks, filteredUnrotatedBlocks)
			for fileName, searchReq := range res {
				if _, ok := finalSegFiles[fileName]; !ok {
					finalSegFiles[fileName] = searchReq
				} else {
					finalSegFiles[fileName].JoinRequest(searchReq, op)
				}
			}
		}
	}

	if condition.SearchNode != nil {
		for _, node := range condition.SearchNode {
			segmentFiles := ExtractUnrotatedSSRFromSearchNode(node, timeRange, indexNames, rawSearchKeys, querySummary, qid)
			for fileName, searchReq := range segmentFiles {
				if _, ok := finalSegFiles[fileName]; !ok {
					finalSegFiles[fileName] = searchReq
					continue
				}
				finalSegFiles[fileName].JoinRequest(searchReq, op)
			}
		}
	}
	return finalSegFiles
}
