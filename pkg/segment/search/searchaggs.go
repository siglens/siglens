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
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/reader/segread"
	"github.com/siglens/siglens/pkg/segment/results/blockresults"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/segment/writer/stats"
	bbp "github.com/valyala/bytebufferpool"

	log "github.com/sirupsen/logrus"
)

func applyAggregationsToResult(aggs *structs.QueryAggregators, segmentSearchRecords *SegmentSearchStatus,
	searchReq *structs.SegmentSearchRequest, blockSummaries []*structs.BlockSummary, queryRange *dtu.TimeRange,
	sizeLimit uint64, fileParallelism int64, queryMetrics *structs.QueryProcessingMetrics, qid uint64,
	allSearchResults *segresults.SearchResults) error {

	var blkWG sync.WaitGroup
	allBlocksChan := make(chan *BlockSearchStatus, fileParallelism)
	aggCols := GetAggColsAndTimestamp(aggs)
	sharedReader, err := segread.InitSharedMultiColumnReaders(searchReq.SegmentKey, aggCols, searchReq.AllBlocksToSearch,
		blockSummaries, int(fileParallelism), qid)
	if err != nil {
		log.Errorf("applyAggregationsToResult: failed to load all column files reader for %s. Needed cols %+v. Err: %+v",
			searchReq.SegmentKey, aggCols, err)
		if sharedReader != nil {
			sharedReader.Close()
		}
		return err
	}
	defer sharedReader.Close()

	if aggs != nil && aggs.GroupByRequest != nil {
		cname, ok := checkIfGrpColsPresent(aggs.GroupByRequest, sharedReader.MultiColReaders[0],
			allSearchResults)
		if !ok {
			log.Errorf("qid=%v, applyAggregationsToResult: cname: %v was not present", qid, cname)
			return fmt.Errorf("qid=%v, applyAggregationsToResult: cname: %v was not present", qid,
				cname)
		}
	}

	rupReader, err := segread.InitNewRollupReader(searchReq.SegmentKey, config.GetTimeStampKey(), qid)
	if err != nil {
		log.Errorf("qid=%d, applyAggregationsToResult: failed initialize rollup reader segkey %s. Error: %v",
			qid, searchReq.SegmentKey, err)
	} else {
		defer rupReader.Close()
	}
	allBlocksToXRollup, aggsHasTimeHt, aggsHasNonTimeHt := getRollupForAggregation(aggs, rupReader)
	for i := int64(0); i < fileParallelism; i++ {
		blkWG.Add(1)
		go applyAggregationsSingleBlock(sharedReader.MultiColReaders[i], aggs, allSearchResults, allBlocksChan,
			searchReq, queryRange, sizeLimit, &blkWG, queryMetrics, qid, blockSummaries, aggsHasTimeHt,
			aggsHasNonTimeHt, allBlocksToXRollup)
	}
	absKeys := make([]uint16, 0, len(segmentSearchRecords.AllBlockStatus))
	for k := range segmentSearchRecords.AllBlockStatus {
		absKeys = append(absKeys, k)
	}
	if aggs != nil && aggs.Sort != nil {
		if aggs.Sort.Ascending {
			sort.Slice(absKeys, func(i, j int) bool { return absKeys[i] < absKeys[j] })
		} else {
			sort.Slice(absKeys, func(i, j int) bool { return absKeys[i] > absKeys[j] })
		}
	}
	for _, k := range absKeys {
		blkResults := segmentSearchRecords.AllBlockStatus[k]
		if blkResults.hasAnyMatched {
			allBlocksChan <- blkResults
		}
	}
	close(allBlocksChan)
	blkWG.Wait()
	return nil
}

func applyAggregationsSingleBlock(multiReader *segread.MultiColSegmentReader, aggs *structs.QueryAggregators,
	allSearchResults *segresults.SearchResults, blockChan chan *BlockSearchStatus, searchReq *structs.SegmentSearchRequest,
	queryRange *dtu.TimeRange, sizeLimit uint64, wg *sync.WaitGroup, queryMetrics *structs.QueryProcessingMetrics,
	qid uint64, blockSummaries []*structs.BlockSummary, aggsHasTimeHt bool, aggsHasNonTimeHt bool,
	allBlocksToXRollup map[uint16]map[uint64]*writer.RolledRecs) {

	blkResults, err := blockresults.InitBlockResults(sizeLimit, aggs, qid)
	if err != nil {
		log.Errorf("applyAggregationsSingleBlock: failed to initialize block results reader for %s. Err: %v", searchReq.SegmentKey, err)
		allSearchResults.AddError(err)
	}
	defer wg.Done()

	for blockStatus := range blockChan {
		if !blockStatus.hasAnyMatched {
			continue
		}
		recIT, err := blockStatus.GetRecordIteratorCopyForBlock(utils.And)
		if err != nil {
			log.Errorf("qid=%d, applyAggregationsSingleBlock: failed to initialize record iterator for block %+v. Err: %v",
				qid, blockStatus.BlockNum, err)
			continue
		}

		var toXRollup map[uint64]*writer.RolledRecs = nil
		if allBlocksToXRollup != nil {
			toXRollup = allBlocksToXRollup[blockStatus.BlockNum]
		}

		isBlkFullyEncosed := queryRange.AreTimesFullyEnclosed(blockSummaries[blockStatus.BlockNum].LowTs,
			blockSummaries[blockStatus.BlockNum].HighTs)

		var addedTimeHt = false
		if aggs != nil && aggs.TimeHistogram != nil && aggsHasTimeHt && isBlkFullyEncosed &&
			toXRollup != nil {
			for rupTskey, rr := range toXRollup {
				rr.MatchedRes.InPlaceIntersection(recIT.AllRecords)
				matchedRrCount := uint16(rr.MatchedRes.GetNumberOfSetBits())
				blkResults.AddKeyToTimeBucket(rupTskey, matchedRrCount)
			}
			addedTimeHt = true
		}

		if blkResults.ShouldIterateRecords(aggsHasTimeHt, isBlkFullyEncosed,
			blockSummaries[blockStatus.BlockNum].LowTs,
			blockSummaries[blockStatus.BlockNum].HighTs, addedTimeHt) {
			iterRecsAddRrc(recIT, multiReader, blockStatus, queryRange, aggs, aggsHasTimeHt,
				addedTimeHt, blkResults, queryMetrics, allSearchResults, searchReq, qid)
		} else {
			// we did not iterate the records so now we need to just update the counts, so that early-exit
			// as well as hit.total has somewhat accurate value
			rrMc := uint64(recIT.AllRecords.GetNumberOfSetBits())
			if rrMc > 0 {
				blkResults.AddMatchedCount(rrMc)
				queryMetrics.IncrementNumBlocksWithMatch(1)
			}
		}
		doAggs(aggs, multiReader, blockStatus, recIT, blkResults, isBlkFullyEncosed, qid)
	}
	allSearchResults.AddBlockResults(blkResults)
}

func addRecordToAggregations(grpReq *structs.GroupByRequest, measureInfo map[string][]int, numMFuncs int, multiColReader *segread.MultiColSegmentReader,
	blockNum uint16, recIT *BlockRecordIterator, blockRes *blockresults.BlockResults, qid uint64) {

	measureResults := make([]utils.CValueEnclosure, numMFuncs)
	for recNum := uint16(0); recNum < recIT.AllRecLen; recNum++ {
		if !recIT.ShouldProcessRecord(uint(recNum)) {
			continue
		}

		var currKey bytes.Buffer
		for _, col := range grpReq.GroupByColumns {
			rawVal, err := multiColReader.ReadRawRecordFromColumnFile(col, blockNum, recNum, qid)
			if err != nil {
				log.Errorf("addRecordToAggregations: Failed to get key for column %v: %v", col, err)
				currKey.Write(utils.VALTYPE_ENC_BACKFILL)
			} else {
				currKey.Write(rawVal)
			}
		}

		for cName, indices := range measureInfo {
			rawVal, err := multiColReader.ExtractValueFromColumnFile(cName, blockNum, recNum, qid)
			if err != nil {
				log.Errorf("addRecordToAggregations: Failed to extract measure value from column %+v: %v", cName, err)
				rawVal = &utils.CValueEnclosure{Dtype: utils.SS_DT_BACKFILL}
			}
			for _, idx := range indices {
				measureResults[idx] = *rawVal
			}
		}
		blockRes.AddMeasureResultsToKey(currKey, measureResults, qid)
	}
}

// returns all columns in aggs and the timestamp column
func GetAggColsAndTimestamp(aggs *structs.QueryAggregators) map[string]bool {
	aggCols := make(map[string]bool)
	timestampKey := config.GetTimeStampKey()
	aggCols[timestampKey] = true
	if aggs == nil {
		return aggCols
	}

	if aggs.Sort != nil {
		aggCols[aggs.Sort.ColName] = true
	}
	if aggs.GroupByRequest != nil {
		for _, cName := range aggs.GroupByRequest.GroupByColumns {
			aggCols[cName] = true
		}
		for _, mOp := range aggs.GroupByRequest.MeasureOperations {
			aggCols[mOp.MeasureCol] = true
		}
	}
	return aggCols
}

func applyAggsToResultFastPath(aggs *structs.QueryAggregators, segmentSearchRecords *SegmentSearchStatus,
	searchReq *structs.SegmentSearchRequest, blockSummaries []*structs.BlockSummary, queryRange *dtu.TimeRange,
	sizeLimit uint64, fileParallelism int64, queryMetrics *structs.QueryProcessingMetrics,
	qid uint64, allSearchResults *segresults.SearchResults) error {

	var blkWG sync.WaitGroup
	allBlocksChan := make(chan *BlockSearchStatus, fileParallelism)

	rupReader, err := segread.InitNewRollupReader(searchReq.SegmentKey, config.GetTimeStampKey(), qid)
	if err != nil {
		log.Errorf("qid=%d, applyAggsToResultFastPath: failed initialize rollup reader segkey %s. Error: %v",
			qid, searchReq.SegmentKey, err)
	} else {
		defer rupReader.Close()
	}

	// we just call this func so that we load up the correct rollup files for the specified ht interval
	allBlocksToXRollup, _, _ := getRollupForAggregation(aggs, rupReader)
	for i := int64(0); i < fileParallelism; i++ {
		blkWG.Add(1)
		go applyAggsSingleBlockFastPath(aggs, allSearchResults, allBlocksChan,
			searchReq, queryRange, sizeLimit, &blkWG, queryMetrics, qid, blockSummaries,
			allBlocksToXRollup)
	}

	for _, blkResults := range segmentSearchRecords.AllBlockStatus {
		allBlocksChan <- blkResults
	}
	close(allBlocksChan)
	blkWG.Wait()
	return nil
}

func applyAggsSingleBlockFastPath(aggs *structs.QueryAggregators,
	allSearchResults *segresults.SearchResults, blockChan chan *BlockSearchStatus, searchReq *structs.SegmentSearchRequest,
	queryRange *dtu.TimeRange, sizeLimit uint64, wg *sync.WaitGroup, queryMetrics *structs.QueryProcessingMetrics,
	qid uint64, blockSummaries []*structs.BlockSummary,
	allBlocksToXRollup map[uint16]map[uint64]*writer.RolledRecs) {

	blkResults, err := blockresults.InitBlockResults(sizeLimit, aggs, qid)
	if err != nil {
		log.Errorf("applyAggsSingleBlockFastPath: failed to initialize block results reader for %s. Err: %v", searchReq.SegmentKey, err)
		allSearchResults.AddError(err)
	}

	defer wg.Done()

	for blockStatus := range blockChan {

		var toXRollup map[uint64]*writer.RolledRecs = nil
		if allBlocksToXRollup != nil {
			toXRollup = allBlocksToXRollup[blockStatus.BlockNum]
		}

		for rupTskey, rr := range toXRollup {
			matchedRrCount := uint16(rr.MatchedRes.GetNumberOfSetBits())
			blkResults.AddKeyToTimeBucket(rupTskey, matchedRrCount)
		}

		blkResults.AddMatchedCount(uint64(blockStatus.numRecords))
		queryMetrics.IncrementNumBlocksWithMatch(1)
	}
	allSearchResults.AddBlockResults(blkResults)
}

func applySegStatsToMatchedRecords(ops []*structs.MeasureAggregator, segmentSearchRecords *SegmentSearchStatus,
	searchReq *structs.SegmentSearchRequest, blockSummaries []*structs.BlockSummary, queryRange *dtu.TimeRange,
	fileParallelism int64, queryMetrics *structs.QueryProcessingMetrics, qid uint64) (map[string]*structs.SegStats, error) {

	var blkWG sync.WaitGroup
	allBlocksChan := make(chan *BlockSearchStatus, fileParallelism)

	measureColAndTS := getSegStatsMeasureCols(ops)
	sharedReader, err := segread.InitSharedMultiColumnReaders(searchReq.SegmentKey, measureColAndTS, searchReq.AllBlocksToSearch,
		blockSummaries, int(fileParallelism), qid)
	if err != nil {
		log.Errorf("applyAggregationsToResult: failed to load all column files reader for %s. Needed cols %+v. Err: %+v",
			searchReq.SegmentKey, measureColAndTS, err)
		return nil, errors.New("failed to init sharedmulticolreader")
	}
	defer sharedReader.Close()

	statRes := segresults.InitStatsResults()
	delete(measureColAndTS, config.GetTimeStampKey())
	for i := int64(0); i < fileParallelism; i++ {
		blkWG.Add(1)
		go segmentStatsWorker(statRes, measureColAndTS, sharedReader.MultiColReaders[i], allBlocksChan,
			searchReq, blockSummaries, queryRange, &blkWG, queryMetrics, qid)
	}

	absKeys := make([]uint16, 0, len(segmentSearchRecords.AllBlockStatus))
	for k := range segmentSearchRecords.AllBlockStatus {
		absKeys = append(absKeys, k)
	}
	for _, k := range absKeys {
		blkResults := segmentSearchRecords.AllBlockStatus[k]
		if blkResults.hasAnyMatched {
			allBlocksChan <- blkResults
		}
	}
	close(allBlocksChan)
	blkWG.Wait()

	return statRes.GetSegStats(), nil
}

// returns all columns (+timestamp) in the measure operations
func getSegStatsMeasureCols(ops []*structs.MeasureAggregator) map[string]bool {
	aggCols := make(map[string]bool)
	timestampKey := config.GetTimeStampKey()
	aggCols[timestampKey] = true
	for _, op := range ops {
		aggCols[op.MeasureCol] = true
	}
	return aggCols
}

func segmentStatsWorker(statRes *segresults.StatsResults, mCols map[string]bool, multiReader *segread.MultiColSegmentReader,
	blockChan chan *BlockSearchStatus, searchReq *structs.SegmentSearchRequest, blockSummaries []*structs.BlockSummary,
	queryRange *dtu.TimeRange, wg *sync.WaitGroup, queryMetrics *structs.QueryProcessingMetrics, qid uint64) {

	defer wg.Done()
	bb := bbp.Get()
	defer bbp.Put(bb)

	localStats := make(map[string]*structs.SegStats)
	for blockStatus := range blockChan {
		isBlkFullyEncosed := queryRange.AreTimesFullyEnclosed(blockSummaries[blockStatus.BlockNum].LowTs,
			blockSummaries[blockStatus.BlockNum].HighTs)
		recIT, err := blockStatus.GetRecordIteratorForBlock(utils.And)
		if err != nil {
			log.Errorf("qid=%d, segmentStatsWorker: failed to initialize record iterator for block %+v. Err: %v",
				qid, blockStatus.BlockNum, err)
			continue
		}

		sortedMatchedRecs := make([]uint16, recIT.AllRecLen)
		idx := 0
		for i := uint(0); i < uint(recIT.AllRecLen); i++ {
			if !recIT.ShouldProcessRecord(i) {
				continue
			}
			recNum16 := uint16(i)
			if !isBlkFullyEncosed {
				recTs, err := multiReader.GetTimeStampForRecord(blockStatus.BlockNum, recNum16, qid)
				if err != nil {
					log.Errorf("qid=%d, segmentStatsWorker failed to initalize time reader for block %+v. Err: %v", qid,
						blockStatus.BlockNum, err)
					continue
				}
				if !queryRange.CheckInRange(recTs) {
					continue
				}
			}
			sortedMatchedRecs[idx] = uint16(i)
			idx++
		}
		sortedMatchedRecs = sortedMatchedRecs[:idx]
		nonDeCols := applySegmentStatsUsingDictEncoding(multiReader, sortedMatchedRecs, mCols, blockStatus.BlockNum, recIT, localStats, bb, qid)

		for _, recNum := range sortedMatchedRecs {
			for colName := range nonDeCols {
				val, err := multiReader.ExtractValueFromColumnFile(colName, blockStatus.BlockNum, recNum, qid)
				if err != nil {
					log.Errorf("qid=%d, segmentStatsWorker failed to extract value for column %+v. Err: %v", qid, colName, err)
					continue
				}
				if val.Dtype == utils.SS_DT_STRING {
					str, err := val.GetString()
					if err != nil {
						log.Errorf("qid=%d, segmentStatsWorker failed to extract value for string although type check passed %+v. Err: %v", qid, colName, err)
						continue
					}
					stats.AddSegStatsStr(localStats, colName, str, bb)
				} else {
					fVal, err := val.GetFloatValue()
					if err != nil {
						log.Errorf("qid=%d, segmentStatsWorker failed to extract numerical value for type %+v. Err: %v", qid, val.Dtype, err)
						continue
					}
					stats.AddSegStatsNums(localStats, colName, utils.SS_FLOAT64, 0, 0, fVal, fmt.Sprintf("%v", fVal), bb)
				}
			}
		}
	}
	statRes.MergeSegStats(localStats)
}

// returns all columns that are not dict encoded
func applySegmentStatsUsingDictEncoding(mcr *segread.MultiColSegmentReader, filterdRecNums []uint16, mCols map[string]bool, blockNum uint16,
	bri *BlockRecordIterator, lStats map[string]*structs.SegStats, bb *bbp.ByteBuffer, qid uint64) map[string]bool {

	retVal := make(map[string]bool)
	for colName := range mCols {
		if colName == "*" {
			stats.AddSegStatsCount(lStats, colName, uint64(len(filterdRecNums)))
			continue
		}
		isDict, err := mcr.IsBlkDictEncoded(colName, blockNum)
		if err != nil {
			log.Errorf("qid=%d, segmentStatsWorker failed to check if column is dict encoded %+v. Err: %v", qid, colName, err)
			continue
		}
		if !isDict {
			retVal[colName] = true
			continue
		}
		results := make(map[uint16]map[string]interface{})
		ok := mcr.GetDictEncCvalsFromColFile(results, colName, blockNum, filterdRecNums, qid)
		if !ok {
			log.Errorf("qid=%d, segmentStatsWorker failed to get dict cvals for col %s", qid, colName)
			continue
		}
		for _, cMap := range results {
			for colName, rawVal := range cMap {
				switch val := rawVal.(type) {
				case string:
					stats.AddSegStatsStr(lStats, colName, val, bb)
				default:
					// This should never occur as dict encoding is only supported for string fields.
					log.Errorf("qid=%d, segmentStatsWorker found a non string in a dict encoded segment. CName %+s", qid, colName)
				}
			}
		}
	}
	return retVal
}

func iterRecsAddRrc(recIT *BlockRecordIterator, mcr *segread.MultiColSegmentReader,
	blockStatus *BlockSearchStatus, queryRange *dtu.TimeRange, aggs *structs.QueryAggregators,
	aggsHasTimeHt bool, addedTimeHt bool, blkResults *blockresults.BlockResults,
	queryMetrics *structs.QueryProcessingMetrics,
	allSearchResults *segresults.SearchResults, searchReq *structs.SegmentSearchRequest, qid uint64) {

	numRecsMatched := uint16(0)
	for recNum := uint(0); recNum < uint(recIT.AllRecLen); recNum++ {
		if !recIT.ShouldProcessRecord(recNum) {
			continue
		}
		recNumUint16 := uint16(recNum)
		recTs, err := mcr.GetTimeStampForRecord(blockStatus.BlockNum, recNumUint16, qid)
		if err != nil {
			break
		}
		if !queryRange.CheckInRange(recTs) {
			recIT.UnsetRecord(recNum)
			continue
		}
		if aggs != nil && aggsHasTimeHt && !addedTimeHt {
			blkResults.AddKeyToTimeBucket(recTs, 1)
		}
		numRecsMatched++
		if blkResults.ShouldAddMore() {
			sortVal, invalidCol := extractSortVals(aggs, mcr, blockStatus.BlockNum, recNumUint16, recTs, qid)
			if !invalidCol && blkResults.WillValueBeAdded(sortVal) {
				rrc := &utils.RecordResultContainer{
					SegKeyInfo: utils.SegKeyInfo{
						SegKeyEnc: allSearchResults.GetAddSegEnc(searchReq.SegmentKey),
						IsRemote:  false,
					},
					BlockNum:         blockStatus.BlockNum,
					RecordNum:        recNumUint16,
					SortColumnValue:  sortVal,
					VirtualTableName: searchReq.VirtualTableName,
					TimeStamp:        recTs,
				}
				blkResults.Add(rrc)
			}
		}
	}
	if numRecsMatched > 0 {
		blkResults.AddMatchedCount(uint64(numRecsMatched))
		queryMetrics.IncrementNumBlocksWithMatch(1)
	}
}

func doAggs(aggs *structs.QueryAggregators, mcr *segread.MultiColSegmentReader,
	bss *BlockSearchStatus, recIT *BlockRecordIterator, blkResults *blockresults.BlockResults,
	isBlkFullyEncosed bool, qid uint64) {

	if aggs == nil || aggs.GroupByRequest == nil {
		return // nothing to do
	}

	measureInfo, internalMops := blkResults.GetConvertedMeasureInfo()
	addRecordToAggregations(aggs.GroupByRequest, measureInfo, len(internalMops), mcr,
		bss.BlockNum, recIT, blkResults, qid)

}

func CanDoStarTree(segKey string, aggs *structs.QueryAggregators,
	qid uint64) (bool, *segread.AgileTreeReader) {

	// init agileTreeader
	str, err := segread.InitNewAgileTreeReader(segKey, qid)
	if err != nil {
		log.Errorf("qid=%v, CanDoStarTree: failed to init agileTreereader, err: %v", qid, err)
		return false, nil
	}

	ok, err := str.CanUseAgileTree(aggs.GroupByRequest)
	if err != nil {
		str.Close()
		return false, nil
	}

	if !ok {
		str.Close()
		return false, nil
	}
	return true, str // caller responsible to close str if we can use agileTree
}

func ApplyAgileTree(str *segread.AgileTreeReader, aggs *structs.QueryAggregators, allSearchResults *segresults.SearchResults,
	sizeLimit uint64, qid uint64, agileTreeBuf []byte) {

	_, internalMops := allSearchResults.BlockResults.GetConvertedMeasureInfo()

	// Note we are using AllSearchResults's blockresult directly here to avoid creating
	// blkRes for each seg and then merging it. This change has perf improvements
	// but the side effect is other threads (async wsSearchHandler threads can't access the
	// blkResuls, else will panic. ALSO this means we can only apply agileTree one seg at a time.
	err := str.ApplyGroupByJit(aggs.GroupByRequest.GroupByColumns, internalMops,
		allSearchResults.BlockResults, qid, agileTreeBuf)
	if err != nil {
		allSearchResults.AddError(err)
		log.Errorf("qid=%v, ApplyAgileTree: failed to JIT agileTree aggs, err: %v", qid, err)
		return
	}
}

func checkIfGrpColsPresent(grpReq *structs.GroupByRequest,
	mcsr *segread.MultiColSegmentReader, allSearchResults *segresults.SearchResults) (string, bool) {

	measureInfo, _ := allSearchResults.BlockResults.GetConvertedMeasureInfo()
	for _, cname := range grpReq.GroupByColumns {
		if !mcsr.IsColPresent(cname) {
			return cname, false
		}
	}

	for cname := range measureInfo {
		if !mcsr.IsColPresent(cname) {
			return cname, false
		}
	}
	return "", true
}
