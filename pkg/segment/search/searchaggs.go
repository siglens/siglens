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

	"github.com/axiomhq/hyperloglog"
	"github.com/dustin/go-humanize"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/aggregations"
	"github.com/siglens/siglens/pkg/segment/reader/segread"
	"github.com/siglens/siglens/pkg/segment/results/blockresults"
	"github.com/siglens/siglens/pkg/segment/results/segresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/segment/writer/stats"
	toputils "github.com/siglens/siglens/pkg/utils"
	bbp "github.com/valyala/bytebufferpool"

	log "github.com/sirupsen/logrus"
)

func applyAggregationsToResult(aggs *structs.QueryAggregators, segmentSearchRecords *SegmentSearchStatus,
	searchReq *structs.SegmentSearchRequest, blockSummaries []*structs.BlockSummary, queryRange *dtu.TimeRange,
	sizeLimit uint64, fileParallelism int64, queryMetrics *structs.QueryProcessingMetrics, qid uint64,
	allSearchResults *segresults.SearchResults) error {
	var blkWG sync.WaitGroup
	allBlocksChan := make(chan *BlockSearchStatus, fileParallelism)
	aggCols, _, _ := GetAggColsAndTimestamp(aggs)
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

	usedByTimechart := aggs.UsedByTimechart()
	if (aggs != nil && aggs.GroupByRequest != nil) || usedByTimechart {
		cname, ok := checkIfGrpColsPresent(aggs.GroupByRequest, sharedReader.MultiColReaders[0],
			allSearchResults)
		if !ok && !usedByTimechart {
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
		go applyAggregationsToSingleBlock(sharedReader.MultiColReaders[i], aggs, allSearchResults, allBlocksChan,
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

func applyAggregationsToSingleBlock(multiReader *segread.MultiColSegmentReader, aggs *structs.QueryAggregators,
	allSearchResults *segresults.SearchResults, blockChan chan *BlockSearchStatus, searchReq *structs.SegmentSearchRequest,
	queryRange *dtu.TimeRange, sizeLimit uint64, wg *sync.WaitGroup, queryMetrics *structs.QueryProcessingMetrics,
	qid uint64, blockSummaries []*structs.BlockSummary, aggsHasTimeHt bool, aggsHasNonTimeHt bool,
	allBlocksToXRollup map[uint16]map[uint64]*writer.RolledRecs) {

	blkResults, err := blockresults.InitBlockResults(sizeLimit, aggs, qid)
	if err != nil {
		log.Errorf("applyAggregationsToSingleBlock: failed to initialize block results reader for %s. Err: %v", searchReq.SegmentKey, err)
		allSearchResults.AddError(err)
	}
	defer wg.Done()

	for blockStatus := range blockChan {
		if !blockStatus.hasAnyMatched {
			continue
		}
		recIT, err := blockStatus.GetRecordIteratorCopyForBlock(utils.And)
		if err != nil {
			log.Errorf("qid=%d, applyAggregationsToSingleBlock: failed to initialize record iterator for block %+v. Err: %v",
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
		if aggs != nil && aggs.TimeHistogram != nil && aggs.TimeHistogram.Timechart == nil && aggsHasTimeHt && isBlkFullyEncosed &&
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

func addRecordToAggregations(grpReq *structs.GroupByRequest, timeHistogram *structs.TimeBucket, measureInfo map[string][]int, numMFuncs int, multiColReader *segread.MultiColSegmentReader,
	blockNum uint16, recIT *BlockRecordIterator, blockRes *blockresults.BlockResults, qid uint64) {
	measureResults := make([]utils.CValueEnclosure, numMFuncs)
	usedByTimechart := (timeHistogram != nil && timeHistogram.Timechart != nil)
	hasLimitOption := false
	groupByColValCnt := make(map[string]int, 0)
	var timeRangeBuckets []uint64
	if usedByTimechart {
		timeRangeBuckets = aggregations.GenerateTimeRangeBuckets(timeHistogram)
		hasLimitOption = timeHistogram.Timechart.LimitExpr != nil
	}
	for recNum := uint16(0); recNum < recIT.AllRecLen; recNum++ {
		if !recIT.ShouldProcessRecord(uint(recNum)) {
			continue
		}

		var currKey bytes.Buffer
		groupByColVal := ""

		if usedByTimechart {
			// Find out timePoint for current row
			ts, err := multiColReader.GetTimeStampForRecord(blockNum, recNum, qid)
			if err != nil {
				log.Errorf("addRecordToAggregations: Failed to extract value from timestamp: %v", err)
				continue
			}
			if ts < timeHistogram.StartTime || ts > timeHistogram.EndTime {
				continue
			}
			timePoint := aggregations.FindTimeRangeBucket(timeRangeBuckets, ts, timeHistogram.IntervalMillis)

			retVal := make([]byte, 9)
			copy(retVal[0:], utils.VALTYPE_ENC_UINT64[:])
			copy(retVal[1:], toputils.Uint64ToBytesLittleEndian(timePoint))
			currKey.Write(retVal)

			// Get timechart's group by col val, each different val will be a bucket inside each time range bucket
			byField := timeHistogram.Timechart.ByField
			if len(byField) > 0 {
				rawVal, err := multiColReader.ReadRawRecordFromColumnFile(byField, blockNum, recNum, qid)
				if err != nil {
					log.Errorf("addRecordToAggregations: Failed to get key for column %v: %v", byField, err)
				} else {
					strs, err := utils.ConvertGroupByKey(rawVal)
					if err != nil {
						log.Errorf("addRecordToAggregations: failed to extract raw key: %v", err)
					}
					if len(strs) == 1 {
						groupByColVal = strs[0]
					} else {
						log.Errorf("addRecordToAggregations: invalid length of groupByColVal")
					}
				}
				if hasLimitOption {
					cnt, exists := groupByColValCnt[groupByColVal]
					if exists {
						groupByColValCnt[groupByColVal] = cnt + 1
					} else {
						groupByColValCnt[groupByColVal] = 1
					}
				}
			}
		} else {
			for _, col := range grpReq.GroupByColumns {
				rawVal, err := multiColReader.ReadRawRecordFromColumnFile(col, blockNum, recNum, qid)
				if err != nil {
					log.Errorf("addRecordToAggregations: Failed to get key for column %v: %v", col, err)
					currKey.Write(utils.VALTYPE_ENC_BACKFILL)
				} else {
					currKey.Write(rawVal)
				}
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
		blockRes.AddMeasureResultsToKey(currKey, measureResults, groupByColVal, usedByTimechart, qid)
	}

	if usedByTimechart && len(timeHistogram.Timechart.ByField) > 0 {
		if len(blockRes.GroupByAggregation.GroupByColValCnt) > 0 {
			aggregations.MergeMap(blockRes.GroupByAggregation.GroupByColValCnt, groupByColValCnt)
		} else {
			blockRes.GroupByAggregation.GroupByColValCnt = groupByColValCnt
		}
	}
}

func PerformAggsOnRecs(nodeResult *structs.NodeResult, aggs *structs.QueryAggregators, recs map[string]map[string]interface{},
	finalCols map[string]bool, numTotalSegments uint64, finishesSegment bool, qid uint64) map[string]bool {

	if !nodeResult.PerformAggsOnRecs {
		return nil
	}

	if finishesSegment {
		nodeResult.RecsAggsProcessedSegments++
	}

	if nodeResult.RecsAggsType == structs.GroupByType {
		return PerformGroupByRequestAggsOnRecs(nodeResult, recs, finalCols, qid, numTotalSegments)
	} else if nodeResult.RecsAggsType == structs.MeasureAggsType {
		return PerformMeasureAggsOnRecs(nodeResult, recs, finalCols, qid, numTotalSegments)
	}

	return nil
}

func PerformGroupByRequestAggsOnRecs(nodeResult *structs.NodeResult, recs map[string]map[string]interface{}, finalCols map[string]bool, qid uint64, numTotalSegments uint64) map[string]bool {

	nodeResult.GroupByRequest.BucketCount = 3000

	blockRes, err := blockresults.InitBlockResults(uint64(len(recs)), &structs.QueryAggregators{GroupByRequest: nodeResult.GroupByRequest}, qid)
	if err != nil {
		log.Errorf("PerformGroupByRequestAggsOnRecs: failed to initialize block results reader. Err: %v", err)
		return nil
	}

	measureInfo, internalMops := blockRes.GetConvertedMeasureInfo()

	if nodeResult.GroupByRequest != nil && nodeResult.GroupByRequest.MeasureOperations != nil {
		for _, mOp := range nodeResult.GroupByRequest.MeasureOperations {
			if mOp.MeasureFunc == utils.Count {
				internalMops = append(internalMops, mOp)
			}
		}
	}

	measureResults := make([]utils.CValueEnclosure, len(internalMops))

	columnKeys := make(map[string][]interface{})

	finalRecInden := make(map[string]string)

	for recInden, record := range recs {
		colKeyValues := make([]interface{}, 0)
		byteKey := make([]byte, 0) // bucket Key
		for idx, colName := range nodeResult.GroupByCols {
			value, exists := record[colName]
			if !exists {
				value = ""
			}
			if idx > 0 {
				byteKey = append(byteKey, '_')
			}
			byteKey = append(byteKey, []byte(fmt.Sprintf("%v", value))...)
			colKeyValues = append(colKeyValues, value)
		}

		var currKey bytes.Buffer
		currKey.Write(byteKey)

		keyStr := toputils.UnsafeByteSliceToString(currKey.Bytes())

		if _, exists := columnKeys[keyStr]; !exists {
			columnKeys[keyStr] = colKeyValues
			finalRecInden[keyStr] = recInden
		}

		for cname, indices := range measureInfo {
			var cVal utils.CValueEnclosure
			value, exists := record[cname]
			if !exists {
				log.Errorf("qid=%d, PerformGroupByRequestAggsOnRecs: failed to find column %s in record", qid, cname)
				cVal = utils.CValueEnclosure{Dtype: utils.SS_DT_BACKFILL}
			} else {
				dval, err := utils.CreateDtypeEnclosure(value, qid)
				if dval.Dtype == utils.SS_DT_STRING {
					floatFieldVal, _ := dtu.ConvertToFloat(value, 64)
					if err == nil {
						value = floatFieldVal
						dval.Dtype = utils.SS_DT_FLOAT
					}
				}

				if err != nil {
					log.Errorf("qid=%d, PerformGroupByRequestAggsOnRecs: failed to create Dtype Value from rec: %v", qid, err)
					cVal = utils.CValueEnclosure{Dtype: utils.SS_DT_BACKFILL}
				} else {
					cVal = utils.CValueEnclosure{Dtype: dval.Dtype, CVal: value}
				}
			}

			for _, idx := range indices {
				measureResults[idx] = cVal
			}
		}

		blockRes.AddMeasureResultsToKey(currKey, measureResults, "", false, qid)
	}

	if nodeResult.RecsAggsBlockResults == nil {
		nodeResult.RecsAggsBlockResults = blockRes
	} else {
		recAggsBlockresults := nodeResult.RecsAggsBlockResults.(*blockresults.BlockResults)
		recAggsBlockresults.MergeBuckets(blockRes)
	}

	if nodeResult.RecsAggsProcessedSegments < numTotalSegments {
		for k := range recs {
			delete(recs, k)
		}
		return nil
	} else {
		blockRes = nodeResult.RecsAggsBlockResults.(*blockresults.BlockResults)
	}

	for k := range finalCols {
		delete(finalCols, k)
	}

	validRecIndens := make(map[string]bool)

	for bKey, index := range blockRes.GroupByAggregation.StringBucketIdx {
		recInden, exists := finalRecInden[bKey]
		if !exists {
			continue
		}
		validRecIndens[recInden] = true
		bucketValues, bucketCount := blockRes.GroupByAggregation.AllRunningBuckets[index].GetRunningStatsBucketValues()

		for idx, colName := range nodeResult.GroupByCols {
			if index == 0 {
				finalCols[colName] = true
			}
			recs[recInden][colName] = columnKeys[bKey][idx]
		}

		for i, mOp := range internalMops {
			if index == 0 {
				finalCols[mOp.String()] = true
			}

			if mOp.MeasureFunc == utils.Count {
				recs[recInden][mOp.String()] = bucketCount
			} else {
				if mOp.OverrodeMeasureAgg != nil && mOp.OverrodeMeasureAgg.MeasureFunc == utils.Avg {
					floatVal, err := dtu.ConvertToFloat(bucketValues[i].CVal, 64)
					if err != nil {
						log.Errorf("PerformGroupByRequestAggsOnRecs: failed to convert to float: %v", err)
						continue
					}
					recs[recInden][mOp.OverrodeMeasureAgg.String()] = (floatVal / float64(bucketCount))
					finalCols[mOp.OverrodeMeasureAgg.String()] = true
					if mOp.OverrodeMeasureAgg.String() != mOp.String() {
						delete(finalCols, mOp.String())
					}
				} else {
					recs[recInden][mOp.String()] = bucketValues[i].CVal
				}
			}
		}
	}

	for k := range recs {
		if _, exists := validRecIndens[k]; !exists {
			delete(recs, k)
		}
	}

	return map[string]bool{"CHECK_NEXT_AGG": true}
}

func PerformMeasureAggsOnRecs(nodeResult *structs.NodeResult, recs map[string]map[string]interface{}, finalCols map[string]bool, qid uint64, numTotalSegments uint64) map[string]bool {

	searchResults, err := segresults.InitSearchResults(uint64(len(recs)), &structs.QueryAggregators{MeasureOperations: nodeResult.MeasureOperations}, structs.SegmentStatsCmd, qid)
	if err != nil {
		log.Errorf("PerformMeasureAggsOnRecs: failed to initialize search results. Err: %v", err)
		return nil
	}

	searchResults.InitSegmentStatsResults(nodeResult.MeasureOperations)

	anyCountStat := -1
	lenRecords := len(recs)

	for idx, mOp := range nodeResult.MeasureOperations {
		if mOp.String() == "count(*)" {
			anyCountStat = idx
			break
		}
	}

	firstRecInden := ""

	for recInden := range recs {
		firstRecInden = recInden
		break
	}

	for recInden, record := range recs {
		sstMap := make(map[string]*structs.SegStats, 0)

		for _, mOp := range nodeResult.MeasureOperations {
			dtypeVal, err := utils.CreateDtypeEnclosure(record[mOp.MeasureCol], qid)
			if err != nil {
				log.Errorf("PerformMeasureAggsOnRecs: failed to create Dtype Value from rec: %v", err)
				continue
			}

			if !dtypeVal.IsNumeric() {
				floatVal, err := dtu.ConvertToFloat(record[mOp.MeasureCol], 64)
				if err != nil {
					log.Errorf("PerformMeasureAggsOnRecs: failed to convert to float: %v", err)
					continue
				}
				dtypeVal = &utils.DtypeEnclosure{Dtype: utils.SS_DT_FLOAT, FloatVal: floatVal}
			}

			nTypeEnclosure := &utils.NumTypeEnclosure{
				Ntype:    dtypeVal.Dtype,
				IntgrVal: int64(dtypeVal.FloatVal),
				FloatVal: dtypeVal.FloatVal,
			}

			sstMap[mOp.MeasureCol] = &structs.SegStats{
				IsNumeric:   dtypeVal.IsNumeric(),
				Count:       1,
				Hll:         nil,
				NumStats:    &structs.NumericStats{Min: *nTypeEnclosure, Max: *nTypeEnclosure, Sum: *nTypeEnclosure, Dtype: dtypeVal.Dtype},
				StringStats: nil,
				Records:     nil,
			}

		}

		err := searchResults.UpdateSegmentStats(sstMap, nodeResult.MeasureOperations, nil)
		if err != nil {
			log.Errorf("PerformMeasureAggsOnRecs: failed to update segment stats: %v", err)
		}

		delete(recs, recInden)
	}

	if nodeResult.RecsRunningSegStats == nil {
		nodeResult.RecsRunningSegStats = searchResults.GetSegmentRunningStats()
	} else {
		sstMap := make(map[string]*structs.SegStats, 0)

		for idx, mOp := range nodeResult.MeasureOperations {
			sstMap[mOp.MeasureCol] = nodeResult.RecsRunningSegStats[idx]
		}

		err := searchResults.UpdateSegmentStats(sstMap, nodeResult.MeasureOperations, nil)
		if err != nil {
			log.Errorf("PerformMeasureAggsOnRecs: failed to update segment stats: %v", err)
		}

		nodeResult.RecsRunningSegStats = searchResults.GetSegmentRunningStats()
	}

	if anyCountStat > -1 {
		nodeResult.TotalRRCCount += uint64(lenRecords)
	}

	if nodeResult.RecsAggsProcessedSegments < numTotalSegments {
		return nil
	} else {
		for k := range finalCols {
			delete(finalCols, k)
		}

		finalSegment := make(map[string]interface{}, 0)

		if anyCountStat > -1 {
			finalCols[nodeResult.MeasureOperations[anyCountStat].String()] = true
			finalSegment[nodeResult.MeasureOperations[anyCountStat].String()] = humanize.Comma(int64(nodeResult.TotalRRCCount))
		}

		for colName, value := range searchResults.GetSegmentStatsMeasureResults() {
			finalCols[colName] = true
			if value.Dtype == utils.SS_DT_FLOAT {
				value.CVal = humanize.CommafWithDigits(value.CVal.(float64), 3)
			} else {
				value.CVal = humanize.Comma(value.CVal.(int64))
			}
			finalSegment[colName] = value.CVal
		}

		recs[firstRecInden] = finalSegment
	}

	return map[string]bool{"CHECK_NEXT_AGG": true}
}

// returns all columns in aggs and the timestamp column
func GetAggColsAndTimestamp(aggs *structs.QueryAggregators) (map[string]bool, map[string]utils.AggColUsageMode, map[string]bool) {
	aggCols := make(map[string]bool)
	timestampKey := config.GetTimeStampKey()
	aggCols[timestampKey] = true
	if aggs == nil {
		return aggCols, nil, nil
	}

	// Determine if current col used by eval statements
	aggColUsage := make(map[string]utils.AggColUsageMode)
	// Determine if current col used by agg values() func
	valuesUsage := make(map[string]bool)
	if aggs.Sort != nil {
		aggCols[aggs.Sort.ColName] = true
	}
	if aggs.GroupByRequest != nil {
		for _, cName := range aggs.GroupByRequest.GroupByColumns {
			aggCols[cName] = true
		}
		for _, mOp := range aggs.GroupByRequest.MeasureOperations {
			aggregations.DetermineAggColUsage(mOp, aggCols, aggColUsage, valuesUsage)
		}
	}
	if aggs.TimeHistogram != nil && aggs.TimeHistogram.Timechart != nil && len(aggs.TimeHistogram.Timechart.ByField) > 0 {
		aggCols[aggs.TimeHistogram.Timechart.ByField] = true
	}
	return aggCols, aggColUsage, valuesUsage
}

func applyAggregationsToResultFastPath(aggs *structs.QueryAggregators, segmentSearchRecords *SegmentSearchStatus,
	searchReq *structs.SegmentSearchRequest, blockSummaries []*structs.BlockSummary, queryRange *dtu.TimeRange,
	sizeLimit uint64, fileParallelism int64, queryMetrics *structs.QueryProcessingMetrics,
	qid uint64, allSearchResults *segresults.SearchResults) error {

	var blkWG sync.WaitGroup
	allBlocksChan := make(chan *BlockSearchStatus, fileParallelism)

	rupReader, err := segread.InitNewRollupReader(searchReq.SegmentKey, config.GetTimeStampKey(), qid)
	if err != nil {
		log.Errorf("qid=%d, applyAggregationsToResultFastPath: failed initialize rollup reader segkey %s. Error: %v",
			qid, searchReq.SegmentKey, err)
	} else {
		defer rupReader.Close()
	}

	// we just call this func so that we load up the correct rollup files for the specified ht interval
	allBlocksToXRollup, _, _ := getRollupForAggregation(aggs, rupReader)
	for i := int64(0); i < fileParallelism; i++ {
		blkWG.Add(1)
		go applyAggregationsToSingleBlockFastPath(aggs, allSearchResults, allBlocksChan,
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

func applyAggregationsToSingleBlockFastPath(aggs *structs.QueryAggregators,
	allSearchResults *segresults.SearchResults, blockChan chan *BlockSearchStatus, searchReq *structs.SegmentSearchRequest,
	queryRange *dtu.TimeRange, sizeLimit uint64, wg *sync.WaitGroup, queryMetrics *structs.QueryProcessingMetrics,
	qid uint64, blockSummaries []*structs.BlockSummary,
	allBlocksToXRollup map[uint16]map[uint64]*writer.RolledRecs) {

	blkResults, err := blockresults.InitBlockResults(sizeLimit, aggs, qid)
	if err != nil {
		log.Errorf("applyAggregationsToSingleBlockFastPath: failed to initialize block results reader for %s. Err: %v", searchReq.SegmentKey, err)
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

	measureColAndTS, aggColUsage, valuesUsage := getSegStatsMeasureCols(ops)
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
		go segmentStatsWorker(statRes, measureColAndTS, aggColUsage, valuesUsage, sharedReader.MultiColReaders[i], allBlocksChan,
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
func getSegStatsMeasureCols(ops []*structs.MeasureAggregator) (map[string]bool, map[string]utils.AggColUsageMode, map[string]bool) {
	// Determine if current col used by eval statements
	aggColUsage := make(map[string]utils.AggColUsageMode)
	// Determine if current col used by agg values() func
	valuesUsage := make(map[string]bool)
	aggCols := make(map[string]bool)
	timestampKey := config.GetTimeStampKey()
	aggCols[timestampKey] = true
	for _, op := range ops {
		aggregations.DetermineAggColUsage(op, aggCols, aggColUsage, valuesUsage)
	}
	return aggCols, aggColUsage, valuesUsage
}

func segmentStatsWorker(statRes *segresults.StatsResults, mCols map[string]bool, aggColUsage map[string]utils.AggColUsageMode, valuesUsage map[string]bool,
	multiReader *segread.MultiColSegmentReader, blockChan chan *BlockSearchStatus, searchReq *structs.SegmentSearchRequest, blockSummaries []*structs.BlockSummary,
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
					log.Errorf("qid=%d, segmentStatsWorker failed to initialize time reader for block %+v. Err: %v", qid,
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
		nonDeCols := applySegmentStatsUsingDictEncoding(multiReader, sortedMatchedRecs, mCols, aggColUsage, valuesUsage, blockStatus.BlockNum, recIT, localStats, bb, qid)
		for _, recNum := range sortedMatchedRecs {
			for colName := range nonDeCols {
				val, err := multiReader.ExtractValueFromColumnFile(colName, blockStatus.BlockNum, recNum, qid)
				if err != nil {
					log.Errorf("qid=%d, segmentStatsWorker failed to extract value for column %+v. Err: %v", qid, colName, err)
					continue
				}

				hasValuesFunc, exists := valuesUsage[colName]
				if !exists {
					hasValuesFunc = false
				}

				if val.Dtype == utils.SS_DT_STRING {
					str, err := val.GetString()
					if err != nil {
						log.Errorf("qid=%d, segmentStatsWorker failed to extract value for string although type check passed %+v. Err: %v", qid, colName, err)
						continue
					}
					stats.AddSegStatsStr(localStats, colName, str, bb, aggColUsage, hasValuesFunc)
				} else {
					fVal, err := val.GetFloatValue()
					if err != nil {
						log.Errorf("qid=%d, segmentStatsWorker failed to extract numerical value for type %+v. Err: %v", qid, val.Dtype, err)
						continue
					}
					stats.AddSegStatsNums(localStats, colName, utils.SS_FLOAT64, 0, 0, fVal, fmt.Sprintf("%v", fVal), bb, aggColUsage, hasValuesFunc)
				}
			}
		}
	}
	statRes.MergeSegStats(localStats)
}

// returns all columns that are not dict encoded
func applySegmentStatsUsingDictEncoding(mcr *segread.MultiColSegmentReader, filterdRecNums []uint16, mCols map[string]bool, aggColUsage map[string]utils.AggColUsageMode, valuesUsage map[string]bool,
	blockNum uint16, bri *BlockRecordIterator, lStats map[string]*structs.SegStats, bb *bbp.ByteBuffer, qid uint64) map[string]bool {
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
				colUsage, exists := aggColUsage[colName]
				if !exists {
					colUsage = utils.NoEvalUsage
				}
				// If current col will be used by eval funcs, we should store the raw data and process it
				if colUsage == utils.WithEvalUsage || colUsage == utils.BothUsage {
					e := utils.CValueEnclosure{}
					err := e.ConvertValue(rawVal)
					if err != nil {
						log.Errorf("applySegmentStatsUsingDictEncoding: %v", err)
						continue
					}

					if e.Dtype != utils.SS_DT_STRING {
						retVal[colName] = true
						continue
					}

					var stats *structs.SegStats
					var ok bool
					stats, ok = lStats[colName]
					if !ok {
						stats = &structs.SegStats{
							IsNumeric: false,
							Count:     0,
							Hll:       hyperloglog.New16(),
							Records:   make([]*utils.CValueEnclosure, 0),
						}

						lStats[colName] = stats
					}
					stats.Records = append(stats.Records, &e)

					// Current col only used by eval statements
					if colUsage == utils.WithEvalUsage {
						continue
					}
				}

				hasValuesFunc, exists := valuesUsage[colName]
				if !exists {
					hasValuesFunc = false
				}

				switch val := rawVal.(type) {
				case string:
					stats.AddSegStatsStr(lStats, colName, val, bb, aggColUsage, hasValuesFunc)
				default:
					// This means the column is not dict encoded. So add it to the return value
					retVal[colName] = true
					log.Errorf("qid=%d, segmentStatsWorker found a non string or non-numeric in a dict encoded segment. CName %+s", qid, colName)
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
	addRecordToAggregations(aggs.GroupByRequest, aggs.TimeHistogram, measureInfo, len(internalMops), mcr,
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

func ApplyAgileTree(str *segread.AgileTreeReader, aggs *structs.QueryAggregators,
	allSearchResults *segresults.SearchResults, sizeLimit uint64, qid uint64,
	agileTreeBuf []byte) {

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
