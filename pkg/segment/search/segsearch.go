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
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	blob "github.com/siglens/siglens/pkg/blob"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/segment/memory/limit"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/query/pqs"
	pqsmeta "github.com/siglens/siglens/pkg/segment/query/pqs/meta"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/reader/microreader"
	"github.com/siglens/siglens/pkg/segment/reader/segread"
	"github.com/siglens/siglens/pkg/segment/results/blockresults"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/utils/semaphore"
	log "github.com/sirupsen/logrus"
)

var numConcurrentRawSearch *semaphore.WeightedSemaphore

func init() {
	// We may want to increase this to GOMAXPROCS; but testing on a 32-vCPU server
	// with this set to GOMAXPROCS sometimes lead to all threads waiting on the
	// GLOBAL_FD_LIMITER semaphore so all progress stopped.
	// With GOMAXPROCS / 2 we still get most of the benefit because this server is
	// also doing other things.
	max := runtime.GOMAXPROCS(0) / 2
	if max < 1 {
		max = 1
	}

	numConcurrentRawSearch = semaphore.NewWeightedSemaphore(int64(max), "rawsearch.limiter", time.Minute)
}

const BLOCK_BATCH_SIZE = 100

func RawSearchSegmentFileWrapper(req *structs.SegmentSearchRequest, parallelismPerFile int64,
	searchNode *structs.SearchNode, timeRange *dtu.TimeRange, sizeLimit uint64, aggs *structs.QueryAggregators,
	allSearchResults *segresults.SearchResults, qid uint64, qs *summary.QuerySummary) {
	err := numConcurrentRawSearch.TryAcquireWithBackoff(1, 5, fmt.Sprintf("qid.%d", qid))
	if err != nil {
		log.Errorf("qid=%d Failed to Acquire resources for raw search! error %+v", qid, err)
		allSearchResults.AddError(err)
		return
	}
	defer numConcurrentRawSearch.Release(1)
	searchMemory := req.GetMaxSearchMemorySize(searchNode, parallelismPerFile, PQMR_INITIAL_SIZE)
	err = limit.RequestSearchMemory(searchMemory)
	if err != nil {
		log.Errorf("qid=%d, Failed to acquire memory from global pool for search! Error: %v", qid, err)
		allSearchResults.AddError(err)
		return
	}
	loadMetadataForSearchRequest(req, qid)

	// only chunk when we have a query with no aggs. else, raw search with no chunks.
	shouldChunk := false
	if aggs == nil || (aggs.TimeHistogram == nil && aggs.GroupByRequest == nil) {
		shouldChunk = true
	}

	if !shouldChunk {
		rawSearchColumnar(req, searchNode, timeRange, sizeLimit, aggs, parallelismPerFile, allSearchResults, qid, qs)
		return
	}
	// if not match_all then do search in N chunk of blocks
	sortedAllBlks := make([]*structs.BlockMetadataHolder, len(req.AllBlocksToSearch))
	var i int
	for _, bmh := range req.AllBlocksToSearch {
		if bmh == nil {
			continue
		}
		sortedAllBlks[i] = bmh
		i++
	}
	sortedAllBlks = sortedAllBlks[:i]
	if aggs != nil && aggs.Sort != nil && aggs.Sort.Ascending {
		sort.Slice(sortedAllBlks, func(i, j int) bool { return sortedAllBlks[i].BlkNum < sortedAllBlks[j].BlkNum })
	} else {
		sort.Slice(sortedAllBlks, func(i, j int) bool { return sortedAllBlks[i].BlkNum > sortedAllBlks[j].BlkNum })
	}
	for i := 0; i < len(sortedAllBlks); {
		nm := make(map[uint16]*structs.BlockMetadataHolder, BLOCK_BATCH_SIZE)
		for j := 0; j < BLOCK_BATCH_SIZE && i < len(sortedAllBlks); {
			nm[sortedAllBlks[i].BlkNum] = sortedAllBlks[i]
			j++
			i++
		}
		req.AllBlocksToSearch = nm
		rawSearchColumnar(req, searchNode, timeRange, sizeLimit, aggs, parallelismPerFile, allSearchResults, qid, qs)
	}
}

func writePqmrFiles(segmentSearchRecords *SegmentSearchStatus, segmentKey string,
	virtualTableName string, qid uint64, pqid string, latestEpochMS uint64, cmiPassedCnames map[uint16]map[string]bool) error {
	pqidFname := fmt.Sprintf("%v/pqmr/%v.pqmr", segmentKey, pqid)
	reqLen := uint64(0)
	allPqmrFile := make([]string, 0)
	// Calculating the required size for the buffer that we need to write to
	for _, blkSearchResult := range segmentSearchRecords.AllBlockStatus {

		// Adding 2 bytes for blockNum and 2 bytes for blockLen
		size := 4 + blkSearchResult.allRecords.GetInMemSize()
		reqLen += size
	}

	var idxEmpty uint32
	emptyBitset := pqmr.CreatePQMatchResults(0)
	bufEmpty := make([]byte, (4+emptyBitset.GetInMemSize())*uint64(len(cmiPassedCnames)))
	for blockNum := range cmiPassedCnames {
		if _, ok := segmentSearchRecords.AllBlockStatus[blockNum]; !ok {
			packedLen, err := emptyBitset.EncodePqmr(bufEmpty[idxEmpty:], blockNum)
			if err != nil {
				log.Errorf("qid=%d, writePqmrFiles: failed to encode pqmr. Err:%v", qid, err)
				return err
			}
			idxEmpty += uint32(packedLen)
		}
	}

	// Creating a buffer of a required length
	buf := make([]byte, reqLen)
	var idx uint32
	for blockNum, blkSearchResult := range segmentSearchRecords.AllBlockStatus {
		packedLen, err := blkSearchResult.allRecords.EncodePqmr(buf[idx:], blockNum)
		if err != nil {
			log.Errorf("qid=%d, writePqmrFiles: failed to encode pqmr. Err:%v", qid, err)
			return err
		}
		idx += uint32(packedLen)
	}

	sizeToAdd := len(bufEmpty)
	if sizeToAdd > 0 {
		newArr := make([]byte, sizeToAdd)
		buf = append(buf, newArr...)
	}
	copy(buf[idx:], bufEmpty)
	idx += uint32(sizeToAdd)
	err := pqmr.WritePqmrToDisk(buf[0:idx], pqidFname)
	if err != nil {
		log.Errorf("qid=%d, writePqmrFiles: failed to flush pqmr results to fname %s. Err:%v", qid, pqidFname, err)
		return err
	}
	writer.BackFillPQSSegmetaEntry(segmentKey, pqid)
	pqs.AddPersistentQueryResult(segmentKey, virtualTableName, pqid)
	allPqmrFile = append(allPqmrFile, pqidFname)
	err = blob.UploadIngestNodeDir()
	if err != nil {
		log.Errorf("qid=%d, writePqmrFiles: failed to upload ingest node directory! Err: %v", qid, err)
	}
	err = blob.UploadSegmentFiles(allPqmrFile)
	if err != nil {
		log.Errorf("qid=%d, writePqmrFiles: failed to upload backfilled pqmr file! Err: %v", qid, err)
	}
	return nil
}

func rawSearchColumnar(searchReq *structs.SegmentSearchRequest, searchNode *structs.SearchNode, timeRange *dtu.TimeRange,
	sizeLimit uint64, aggs *structs.QueryAggregators, fileParallelism int64, allSearchResults *segresults.SearchResults, qid uint64,
	querySummary *summary.QuerySummary) {
	if fileParallelism <= 0 {
		log.Errorf("qid=%d, RawSearchSegmentFile: invalid fileParallelism of %d - must be > 0", qid, fileParallelism)
		allSearchResults.AddError(errors.New("invalid fileParallelism - must be > 0"))
		return
	} else if searchReq == nil {
		log.Errorf("qid=%d, RawSearchSegmentFile: received a nil search request for %s", qid, searchReq.SegmentKey)
		allSearchResults.AddError(errors.New("nil search request"))
		return
	} else if searchReq.SearchMetadata == nil {
		log.Errorf("qid=%d, RawSearchSegmentFile: search metadata not provided for %s", qid, searchReq.SegmentKey)
		allSearchResults.AddError(errors.New("search metadata not provided"))
		return
	}

	blockSummaries := searchReq.SearchMetadata.BlockSummaries
	if blockSummaries == nil {
		log.Errorf("qid=%d, RawSearchSegmentFile: received empty blocksummaries for %s", qid, searchReq.SegmentKey)
		allSearchResults.AddError(errors.New("block summaries not provided"))
		return
	}

	sTime := time.Now()

	queryMetrics := &structs.QueryProcessingMetrics{}
	searchNode.AddQueryInfoForNode()

	segmentSearchRecords := InitBlocksToSearch(searchReq, blockSummaries, allSearchResults, timeRange)
	queryMetrics.SetNumBlocksToRawSearch(uint64(segmentSearchRecords.numBlocksToSearch))
	queryMetrics.SetNumBlocksInSegFile(uint64(segmentSearchRecords.numBlocksInSegFile))
	numBlockFilteredRecords, _ := segmentSearchRecords.getTotalCounts()
	queryMetrics.SetNumRecordsToRawSearch(numBlockFilteredRecords)

	if len(segmentSearchRecords.AllBlockStatus) == 0 {
		log.Debugf("qid=%d, RawSearchSegmentFile: no blocks to search for %s", qid, searchReq.SegmentKey)
		return
	}
	allBlockSearchHelpers := structs.InitAllBlockSearchHelpers(fileParallelism)
	executeRawSearchOnNode(searchNode, searchReq, segmentSearchRecords, allBlockSearchHelpers, queryMetrics,
		qid, allSearchResults)
	err := applyAggregationsToResult(aggs, segmentSearchRecords, searchReq, blockSummaries, timeRange,
		sizeLimit, fileParallelism, queryMetrics, qid, allSearchResults)
	if err != nil {
		log.Errorf("qid=%d RawSearchColumnar failed to apply aggregations to result for segKey %+v. Error: %v", qid, searchReq.SegmentKey, err)
		allSearchResults.AddError(err)
		return
	}

	finalMatched, finalUnmatched := segmentSearchRecords.getTotalCounts()
	segmentSearchRecords.Close()
	queryMetrics.SetNumRecordsMatched(finalMatched)
	queryMetrics.SetNumRecordsUnmatched(finalUnmatched)

	if finalMatched > 0 {
		searchReq.HasMatchedRrc = true
	}

	timeElapsed := time.Since(sTime)
	querySummary.UpdateSummary(summary.RAW, timeElapsed, queryMetrics)

	if pqid, ok := shouldBackFillPQMR(searchNode, searchReq, qid); ok {
		if finalMatched == 0 {
			go writeEmptyPqmetaFilesWrapper(pqid, searchReq.SegmentKey, searchReq.VirtualTableName)
		} else {
			go writePqmrFilesWrapper(segmentSearchRecords, searchReq, qid, pqid)
		}
	}
}

func writeEmptyPqmetaFilesWrapper(pqid string, segKey string, vTableName string) {
	pqsmeta.AddEmptyResults(pqid, segKey, vTableName)
	writer.BackFillPQSSegmetaEntry(segKey, pqid)
}

func shouldBackFillPQMR(searchNode *structs.SearchNode, searchReq *structs.SegmentSearchRequest, qid uint64) (string, bool) {
	if config.IsPQSEnabled() {
		pqid := querytracker.GetHashForQuery(searchNode)

		ok, err := querytracker.IsQueryPersistent([]string{searchReq.VirtualTableName}, searchNode)
		if err != nil {
			log.Errorf("qid=%d, Failed to check if query is persistent Error: %v", qid, err)
			return "", false
		}
		if ok {
			if searchReq.SType == structs.RAW_SEARCH && searchNode.NodeType != structs.MatchAllQuery {
				return pqid, true
			}
		}
	}
	return "", false
}

func writePqmrFilesWrapper(segmentSearchRecords *SegmentSearchStatus, searchReq *structs.SegmentSearchRequest, qid uint64, pqid string) {
	if strings.Contains(searchReq.SegmentKey, "/active/") {
		return
	}
	if strings.Contains(searchReq.SegmentKey, config.GetHostID()) {
		err := writePqmrFiles(segmentSearchRecords, searchReq.SegmentKey, searchReq.VirtualTableName, qid, pqid, searchReq.LatestEpochMS, searchReq.CmiPassedCnames)
		if err != nil {
			log.Errorf(" qid:%d, Failed to write pqmr file.  Error: %v", qid, err)
		}
	}
}

func RawSearchPQMResults(req *structs.SegmentSearchRequest, fileParallelism int64, timeRange *dtu.TimeRange, aggs *structs.QueryAggregators,
	sizeLimit uint64, spqmr *pqmr.SegmentPQMRResults, allSearchResults *segresults.SearchResults, qid uint64, querySummary *summary.QuerySummary) {
	sTime := time.Now()

	err := numConcurrentRawSearch.TryAcquireWithBackoff(1, 5, fmt.Sprintf("qid.%d", qid))
	if err != nil {
		log.Errorf("qid=%d Failed to Acquire resources for pqs search! error %+v", qid, err)
		allSearchResults.AddError(err)
		return
	}
	defer numConcurrentRawSearch.Release(1)

	allTimestamps, err := segread.ReadAllTimestampsForBlock(req.AllBlocksToSearch, req.SegmentKey,
		req.SearchMetadata.BlockSummaries, fileParallelism)
	if err != nil {
		allSearchResults.AddError(err)
		return
	}
	defer segread.ReturnTimeBuffers(allTimestamps)

	sharedReader, err := segread.InitSharedMultiColumnReaders(req.SegmentKey, req.AllPossibleColumns, req.AllBlocksToSearch,
		req.SearchMetadata.BlockSummaries, int(fileParallelism), qid)
	if err != nil {
		log.Errorf("qid=%v, RawSearchPQMResults: failed to load all column files reader for %s. Needed cols %+v. Err: %+v",
			qid, req.SegmentKey, req.AllPossibleColumns, err)
		allSearchResults.AddError(err)
		return
	}
	defer sharedReader.Close()

	queryMetrics := &structs.QueryProcessingMetrics{}
	runningBlockManagers := &sync.WaitGroup{}
	filterBlockRequestsChan := make(chan uint16, spqmr.GetNumBlocks())

	rupReader, err := segread.InitNewRollupReader(req.SegmentKey, config.GetTimeStampKey(), qid)
	if err != nil {
		log.Errorf("qid=%d, RawSearchPQMResults: failed initialize rollup reader segkey %s. Error: %v",
			qid, req.SegmentKey, err)
	} else {
		defer rupReader.Close()
	}
	allBlocksToXRollup, aggsHasTimeHt, _ := getRollupForAggregation(aggs, rupReader)
	for i := int64(0); i < fileParallelism; i++ {
		runningBlockManagers.Add(1)
		go rawSearchSingleSPQMR(sharedReader.MultiColReaders[i], req, aggs, runningBlockManagers,
			filterBlockRequestsChan, spqmr, allSearchResults, allTimestamps, timeRange, sizeLimit, queryMetrics,
			allBlocksToXRollup, aggsHasTimeHt, qid)
	}

	sortedAllBlks := spqmr.GetAllBlocks()
	if aggs != nil && aggs.Sort != nil && aggs.Sort.Ascending {
		sort.Slice(sortedAllBlks, func(i, j int) bool { return sortedAllBlks[i] < sortedAllBlks[j] })
	} else {
		sort.Slice(sortedAllBlks, func(i, j int) bool { return sortedAllBlks[i] > sortedAllBlks[j] })
	}

	for _, blkNum := range sortedAllBlks {
		filterBlockRequestsChan <- blkNum
	}
	close(filterBlockRequestsChan)

	queryMetrics.SetNumBlocksInSegFile(uint64(spqmr.GetNumBlocks()))
	runningBlockManagers.Wait()

	timeElapsed := time.Since(sTime)
	querySummary.UpdateSummary(summary.PQS, timeElapsed, queryMetrics)
}

func rawSearchSingleSPQMR(multiReader *segread.MultiColSegmentReader, req *structs.SegmentSearchRequest, aggs *structs.QueryAggregators,
	runningWG *sync.WaitGroup, filterBlockRequestsChan chan uint16, sqpmr *pqmr.SegmentPQMRResults, allSearchResults *segresults.SearchResults,
	allTimestamps map[uint16][]uint64, tRange *dtu.TimeRange, sizeLimit uint64, queryMetrics *structs.QueryProcessingMetrics,
	allBlocksToXRollup map[uint16]map[uint64]*writer.RolledRecs, aggsHasTimeHt bool, qid uint64) {
	defer runningWG.Done()

	blkResults, err := blockresults.InitBlockResults(sizeLimit, aggs, qid)
	measureInfo, internalMops := blkResults.GetConvertedMeasureInfo()
	for blockNum := range filterBlockRequestsChan {
		if req.SearchMetadata == nil || int(blockNum) >= len(req.SearchMetadata.BlockSummaries) {
			log.Errorf("qid=%d, rawSearchSingleSPQMR unable to extract block summary for block %d, segkey=%v", qid, blockNum, req.SegmentKey)
			continue
		}
		blkSum := req.SearchMetadata.BlockSummaries[blockNum]
		if err != nil {
			log.Errorf("qid=%v, applyAggregationsToSingleBlock: failed to initialize block results reader for %s. Err: %v",
				qid, req.SegmentKey, err)
			allSearchResults.AddError(err)
		}
		if !tRange.CheckRangeOverLap(blkSum.LowTs, blkSum.HighTs) {
			continue
		}
		pqmr, found := sqpmr.GetBlockResults(blockNum)
		if !found {
			log.Errorf("qid=%d, rawSearchSingleSPQMR unable to get pqmr results for block %d, segkey=%v", qid, blockNum, req.SegmentKey)
			continue
		}

		numRecsInBlock := uint(blkSum.RecCount)
		currTS, ok := allTimestamps[blockNum]
		if !ok {
			log.Errorf("qid=%d, rawSearchSingleSPQMR failed to get timestamps for block %d. Number of read ts blocks %+v, segkey=%v", qid, blockNum, len(allTimestamps), req.SegmentKey)
			continue
		}
		isBlkFullyEncosed := tRange.AreTimesFullyEnclosed(blkSum.LowTs, blkSum.HighTs)
		if blkResults.ShouldIterateRecords(aggsHasTimeHt, isBlkFullyEncosed, blkSum.LowTs, blkSum.HighTs, false) {
			for recNum := uint(0); recNum < numRecsInBlock; recNum++ {
				if pqmr.DoesRecordMatch(recNum) {
					if int(recNum) > len(currTS) {
						log.Errorf("qid=%d, rawSearchSingleSPQMR tried to get the ts for recNum %+v but only %+v records exist, segkey=%v", qid, recNum, len(currTS), req.SegmentKey)
						continue
					}
					recTs := currTS[recNum]
					if !tRange.CheckInRange(recTs) {
						pqmr.ClearBit(recNum)
						continue
					}
					convertedRecNum := uint16(recNum)
					if err != nil {
						log.Errorf("qid=%d, rawSearchSingleSPQMR failed to get time stamp for record %+v in block %+v, segkey=%v, Err: %v",
							qid, recNum, blockNum, req.SegmentKey, err)
						continue
					}
					if blkResults.ShouldAddMore() {
						sortVal, invalidCol := extractSortVals(aggs, multiReader, blockNum, convertedRecNum, recTs, qid)
						if !invalidCol && blkResults.WillValueBeAdded(sortVal) {
							rrc := &utils.RecordResultContainer{
								SegKeyInfo: utils.SegKeyInfo{
									SegKeyEnc: allSearchResults.GetAddSegEnc(req.SegmentKey),
									IsRemote:  false,
								},
								BlockNum:         blockNum,
								RecordNum:        convertedRecNum,
								SortColumnValue:  sortVal,
								VirtualTableName: req.VirtualTableName,
								TimeStamp:        recTs,
							}
							blkResults.Add(rrc)
						}
					}
				}
			}

		}

		toXRollup, ok := allBlocksToXRollup[blockNum]
		if aggsHasTimeHt && ok {
			for rupTskey, rr := range toXRollup {
				rr.MatchedRes.InPlaceIntersection(pqmr)
				matchedRrCount := uint16(rr.MatchedRes.GetNumberOfSetBits())
				blkResults.AddKeyToTimeBucket(rupTskey, matchedRrCount)
			}
		}
		if aggs != nil && aggs.GroupByRequest != nil {
			recIT := InitIteratorFromPQMR(pqmr, numRecsInBlock)
			addRecordToAggregations(aggs.GroupByRequest, aggs.TimeHistogram, measureInfo, len(internalMops),
				multiReader, blockNum, recIT, blkResults, qid)
		}
		numRecsMatched := uint64(pqmr.GetNumberOfSetBits())

		if numRecsMatched > 0 {
			req.HasMatchedRrc = true
		}

		blkResults.AddMatchedCount(numRecsMatched)
		queryMetrics.IncrementNumRecordsNoMatch(uint64(numRecsInBlock) - numRecsMatched)
		queryMetrics.IncrementNumRecordsWithMatch(numRecsMatched)
		queryMetrics.IncrementNumBlocksToRawSearch(1)
	}
	allSearchResults.AddBlockResults(blkResults)
}

func executeRawSearchOnNode(node *structs.SearchNode, searchReq *structs.SegmentSearchRequest, segmentSearch *SegmentSearchStatus,
	allBlockSearchHelpers []*structs.BlockSearchHelper, queryMetrics *structs.QueryProcessingMetrics,
	qid uint64, allSearchResults *segresults.SearchResults) {

	if node.AndSearchConditions != nil {
		applyRawSearchToConditions(node.AndSearchConditions, searchReq, segmentSearch, allBlockSearchHelpers,
			utils.And, queryMetrics, qid, allSearchResults)
	}

	if node.OrSearchConditions != nil {
		applyRawSearchToConditions(node.OrSearchConditions, searchReq, segmentSearch, allBlockSearchHelpers,
			utils.Or, queryMetrics, qid, allSearchResults)
	}

	if node.ExclusionSearchConditions != nil {
		applyRawSearchToConditions(node.ExclusionSearchConditions, searchReq, segmentSearch, allBlockSearchHelpers,
			utils.Exclusion, queryMetrics, qid, allSearchResults)
	}
}

func applyRawSearchToConditions(cond *structs.SearchCondition, searchReq *structs.SegmentSearchRequest, segmentSearch *SegmentSearchStatus,
	allBlockSearchHelpers []*structs.BlockSearchHelper, op utils.LogicalOperator, queryMetrics *structs.QueryProcessingMetrics, qid uint64,
	allSearchResults *segresults.SearchResults) {

	if cond.SearchNode != nil {
		for _, sNode := range cond.SearchNode {
			executeRawSearchOnNode(sNode, searchReq, segmentSearch, allBlockSearchHelpers, queryMetrics,
				qid, allSearchResults)
		}
	}
	if cond.SearchQueries != nil {
		for _, query := range cond.SearchQueries {
			RawSearchSingleQuery(query, searchReq, segmentSearch, allBlockSearchHelpers, op, queryMetrics,
				qid, allSearchResults)
		}
	}
}

func extractSortVals(aggs *structs.QueryAggregators, multiColReader *segread.MultiColSegmentReader, blkNum uint16,
	recNum uint16, recTs uint64, qid uint64) (float64, bool) {

	var sortVal float64
	var err error
	var invalidAggsCol bool = false

	if aggs == nil || aggs.Sort == nil {
		return sortVal, invalidAggsCol
	}

	if aggs.Sort.ColName == config.GetTimeStampKey() {
		sortVal = float64(recTs)
		return sortVal, invalidAggsCol
	}

	colVal, err := multiColReader.ExtractValueFromColumnFile(aggs.Sort.ColName, blkNum, recNum, qid)
	if err != nil {
		invalidAggsCol = true
		return sortVal, invalidAggsCol
	}
	floatVal, err := colVal.GetFloatValue()
	if err != nil {
		invalidAggsCol = true
		return 0, invalidAggsCol
	}
	return floatVal, invalidAggsCol
}

func loadMetadataForSearchRequest(searchReq *structs.SegmentSearchRequest, qid uint64) {
	if searchReq.SearchMetadata.BlockSummaries == nil {
		sFile := fmt.Sprintf("%v.bsu", searchReq.SegmentKey)
		err := blob.DownloadSegmentBlob(sFile, false)
		if err != nil {
			log.Errorf("qid=%v, Failed to download bsu file for segment %s. SegSetFile struct %+v",
				qid, searchReq.SegmentKey, sFile)
			return
		}
		bSum, _, _, err := microreader.ReadBlockSummaries(searchReq.SearchMetadata.BlockSummariesFile, []byte{})
		if err != nil {
			log.Errorf("qid=%v, loadMetadataForSearchRequest: failed to read block summaries for segment %s. block summary file: %s. Error: %+v",
				qid, searchReq.SegmentKey, searchReq.SearchMetadata.BlockSummariesFile, err)
		} else {
			searchReq.SearchMetadata.BlockSummaries = bSum
		}
	}
}

// returns the rolled up blocks, a bool indicating whether aggregations has a time histogram and a bool indicating whether aggregations has a non time aggregation
func getRollupForAggregation(aggs *structs.QueryAggregators, rupReader *segread.RollupReader) (map[uint16]map[uint64]*writer.RolledRecs, bool, bool) {
	var allBlocksToXRollup map[uint16]map[uint64]*writer.RolledRecs = nil
	aggsHasTimeHt := false
	aggsHasNonTimeHt := false
	if aggs != nil {
		if aggs.TimeHistogram != nil {
			aggsHasTimeHt = true
			switch htInt := aggs.TimeHistogram.IntervalMillis; {
			case htInt < 3600_000:
				// sec or millisecond based time-histogram, we up it to minute based
				if rupReader != nil {
					val, err := rupReader.GetMinRollups()
					if err == nil {
						allBlocksToXRollup = val
					}
				}
			case htInt < 86400_000:
				if rupReader != nil {
					val, err := rupReader.GetHourRollups()
					if err == nil {
						allBlocksToXRollup = val
					}
				}
			default:
				if rupReader != nil {
					val, err := rupReader.GetDayRollups()
					if err == nil {
						allBlocksToXRollup = val
					}
				}
			}
		}
		if aggs.GroupByRequest != nil {
			aggsHasNonTimeHt = true
		}
	}
	return allBlocksToXRollup, aggsHasTimeHt, aggsHasNonTimeHt
}

func AggsFastPathWrapper(req *structs.SegmentSearchRequest, parallelismPerFile int64,
	searchNode *structs.SearchNode, timeRange *dtu.TimeRange, sizeLimit uint64, aggs *structs.QueryAggregators,
	allSearchResults *segresults.SearchResults, qid uint64, qs *summary.QuerySummary) {

	err := numConcurrentRawSearch.TryAcquireWithBackoff(1, 5, fmt.Sprintf("qid.%d", qid))
	if err != nil {
		log.Errorf("qid=%d Failed to Acquire resources for aggs fast path! error %+v", qid, err)
		allSearchResults.AddError(err)
		return
	}
	defer numConcurrentRawSearch.Release(1)
	searchMemory := req.GetMaxSearchMemorySize(searchNode, parallelismPerFile, PQMR_INITIAL_SIZE)
	err = limit.RequestSearchMemory(searchMemory)
	if err != nil {
		log.Errorf("qid=%d, Failed to acquire memory from global pool for search! Error: %v", qid, err)
		allSearchResults.AddError(err)
		return
	}
	loadMetadataForSearchRequest(req, qid)

	aggsFastPath(req, searchNode, timeRange, sizeLimit, aggs, parallelismPerFile, allSearchResults, qid, qs)
}

func aggsFastPath(searchReq *structs.SegmentSearchRequest, searchNode *structs.SearchNode, timeRange *dtu.TimeRange,
	sizeLimit uint64, aggs *structs.QueryAggregators, fileParallelism int64, allSearchResults *segresults.SearchResults, qid uint64,
	querySummary *summary.QuerySummary) {

	if fileParallelism <= 0 {
		log.Errorf("qid=%d, AggsFastPath: invalid fileParallelism of %d - must be > 0", qid, fileParallelism)
		allSearchResults.AddError(errors.New("invalid fileParallelism - must be > 0"))
		return
	} else if searchReq == nil {
		log.Errorf("qid=%d, AggsFastPath: received a nil search request for %s", qid, searchReq.SegmentKey)
		allSearchResults.AddError(errors.New("nil search request"))
		return
	} else if searchReq.SearchMetadata == nil {
		log.Errorf("qid=%d, AggsFastPath: search metadata not provided for %s", qid, searchReq.SegmentKey)
		allSearchResults.AddError(errors.New("search metadata not provided"))
		return
	}

	blockSummaries := searchReq.SearchMetadata.BlockSummaries
	if blockSummaries == nil {
		log.Errorf("qid=%d, AggsFastPath: received empty blocksummaries for %s", qid, searchReq.SegmentKey)
		allSearchResults.AddError(errors.New("block summaries not provided"))
		return
	}

	sTime := time.Now()

	queryMetrics := &structs.QueryProcessingMetrics{}
	searchNode.AddQueryInfoForNode()

	segmentSearchRecords := InitBlocksForAggsFastPath(searchReq, blockSummaries)
	queryMetrics.SetNumBlocksToRawSearch(uint64(segmentSearchRecords.numBlocksToSearch))
	queryMetrics.SetNumBlocksInSegFile(uint64(segmentSearchRecords.numBlocksInSegFile))
	numBlockFilteredRecords := segmentSearchRecords.getTotalCountsFastPath()
	queryMetrics.SetNumRecordsToRawSearch(numBlockFilteredRecords)

	if len(segmentSearchRecords.AllBlockStatus) == 0 {
		log.Errorf("qid=%d, Finished raw search for file %s in %+v", qid, searchReq.SegmentKey, time.Since(sTime))
		log.Errorf("qid=%d, numRecordsInSegFile=%+v numTimeFilteredRecords=%+v timeRange=%+v",
			qid, queryMetrics.NumRecordsToRawSearch, queryMetrics.NumBlocksToRawSearch, timeRange)
		return
	}

	err := applyAggregationsToResultFastPath(aggs, segmentSearchRecords, searchReq, blockSummaries, timeRange,
		sizeLimit, fileParallelism, queryMetrics, qid, allSearchResults)
	if err != nil {
		log.Errorf("qid=%d RawSearchColumnar failed to apply aggregations to result for segKey %+v. Error: %v", qid, searchReq.SegmentKey, err)
		allSearchResults.AddError(err)
		return
	}

	finalMatched := segmentSearchRecords.getTotalCountsFastPath()
	segmentSearchRecords.Close()
	queryMetrics.SetNumRecordsMatched(finalMatched)

	if finalMatched > 0 {
		searchReq.HasMatchedRrc = true
	}

	timeElapsed := time.Since(sTime)
	querySummary.UpdateSummary(summary.RAW, timeElapsed, queryMetrics)
}

// This function raw compute segment stats and will return a map[string]*structs.SegStats, for all the measureOps parameter
// This function will check for timestamp and so should be used for partially enclosed segments, and unrotated segments.
func RawComputeSegmentStats(req *structs.SegmentSearchRequest, fileParallelism int64,
	searchNode *structs.SearchNode, timeRange *dtu.TimeRange, measureOps []*structs.MeasureAggregator,
	allSearchResults *segresults.SearchResults, qid uint64, qs *summary.QuerySummary) (map[string]*structs.SegStats, error) {

	err := numConcurrentRawSearch.TryAcquireWithBackoff(1, 5, fmt.Sprintf("qid.%d", qid))
	if err != nil {
		log.Errorf("qid=%d Failed to Acquire resources for raw search! error %+v", qid, err)
		allSearchResults.AddError(err)
		return nil, errors.New("failed to acquire resources for segment stats")
	}
	defer numConcurrentRawSearch.Release(1)

	if fileParallelism <= 0 {
		log.Errorf("qid=%d, RawSearchSegmentFile: invalid fileParallelism of %d - must be > 0", qid, fileParallelism)
		return nil, errors.New("invalid fileParallelism - must be > 0")
	} else if req == nil {
		log.Errorf("qid=%d, RawSearchSegmentFile: received a nil search request for %s", qid, req.SegmentKey)
		return nil, errors.New("received a nil search request")
	} else if req.SearchMetadata == nil {
		log.Errorf("qid=%d, RawSearchSegmentFile: search metadata not provided for %s", qid, req.SegmentKey)
		return nil, errors.New("search metadata not provided")
	}

	blockSummaries := req.SearchMetadata.BlockSummaries
	if blockSummaries == nil {
		log.Errorf("qid=%d, RawSearchSegmentFile: received empty blocksummaries for %s", qid, req.SegmentKey)
		return nil, errors.New("search metadata not provided")
	}

	sTime := time.Now()

	queryMetrics := &structs.QueryProcessingMetrics{}
	searchNode.AddQueryInfoForNode()

	segmentSearchRecords := InitBlocksToSearch(req, blockSummaries, allSearchResults, timeRange)
	queryMetrics.SetNumBlocksToRawSearch(uint64(segmentSearchRecords.numBlocksToSearch))
	queryMetrics.SetNumBlocksInSegFile(uint64(segmentSearchRecords.numBlocksInSegFile))
	numBlockFilteredRecords, _ := segmentSearchRecords.getTotalCounts()
	queryMetrics.SetNumRecordsToRawSearch(numBlockFilteredRecords)

	retVal := make(map[string]*structs.SegStats)
	if len(segmentSearchRecords.AllBlockStatus) == 0 {
		return retVal, nil
	}

	allBlockSearchHelpers := structs.InitAllBlockSearchHelpers(fileParallelism)
	executeRawSearchOnNode(searchNode, req, segmentSearchRecords, allBlockSearchHelpers, queryMetrics,
		qid, allSearchResults)

	segStats, err := applySegStatsToMatchedRecords(measureOps, segmentSearchRecords, req, blockSummaries, timeRange,
		fileParallelism, queryMetrics, qid)
	if err != nil {
		log.Errorf("qid=%d, failed to raw compute segstats %+v", qid, err)
		return nil, err
	}

	finalMatched, finalUnmatched := segmentSearchRecords.getTotalCounts()
	segmentSearchRecords.Close()
	queryMetrics.SetNumRecordsMatched(finalMatched)
	queryMetrics.SetNumRecordsUnmatched(finalUnmatched)

	timeElapsed := time.Since(sTime)
	qs.UpdateSummary(summary.RAW, timeElapsed, queryMetrics)
	return segStats, nil
}
