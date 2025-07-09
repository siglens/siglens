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

package blockresults

import (
	"encoding/base64"
	"fmt"
	"math"
	"sort"

	"github.com/siglens/siglens/pkg/segment/aggregations"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type GroupByBuckets struct {
	AllRunningBuckets   []*RunningBucketResults
	StringBucketIdx     map[string]int
	internalMeasureFns  []*structs.MeasureAggregator // all converted measure requests in order they exist in running stats
	allMeasureCols      map[string][]int             // maps col name to all indices that it exist in internalMeasureFns
	reverseMeasureIndex []int                        // reverse index, so idx of original measure will store the index in internalMeasureFns. -1 is reserved for count
	maxBuckets          int                          // maximum number of buckets to create
	GroupByColValCnt    map[string]int               // calculate freq for group by col val
}

type SerializedGroupByBuckets struct {
	AllRunningBuckets []*SerializedRunningBucketResults
	StringBucketIdx   map[string]int
	GroupByColValCnt  map[string]int
}

type TimeBuckets struct {
	AllRunningBuckets []*RunningBucketResults
	UnsignedBucketIdx map[uint64]int
}

type SerializedTimeBuckets struct {
	AllRunningBuckets []*SerializedRunningBucketResults
	UnsignedBucketIdx map[uint64]int
}

type BlockResults struct {
	SortedResults      *SortResults
	UnsortedResults    []*sutils.RecordResultContainer
	TimeAggregation    *TimeBuckets
	GroupByAggregation *GroupByBuckets
	aggs               *structs.QueryAggregators

	MatchedCount uint64

	nextUnsortedIdx uint64 // next index to put result in
	sortResults     bool
	sizeLimit       uint64
	batchErr        *utils.BatchError
}

// json exportable and mergeable results for query
type GroupByBucketsJSON struct {
	AllGroupbyBuckets map[string]*RunningBucketResultsJSON `json:"allGroupbyBuckets"`
}

type TimeBucketsJSON struct {
	AllTimeBuckets map[uint64]*RunningBucketResultsJSON `json:"allTimeBuckets"`
}

type RunningBucketResultsJSON struct {
	RunningStats []RunningStatsJSON           `json:"runningStats"`
	CurrStats    []*structs.MeasureAggregator `json:"currStats"`
	Count        uint64                       `json:"count"`
}

func InitBlockResults(count uint64, aggs *structs.QueryAggregators, qid uint64) (*BlockResults, error) {
	blockRes := &BlockResults{aggs: aggs}
	if aggs != nil && aggs.TimeHistogram != nil {
		blockRes.TimeAggregation = &TimeBuckets{
			AllRunningBuckets: make([]*RunningBucketResults, 0),
			UnsignedBucketIdx: make(map[uint64]int),
		}
	}

	if aggs != nil && aggs.GroupByRequest != nil {
		if len(aggs.GroupByRequest.GroupByColumns) > 0 {
			usedByTimechart := aggs.UsedByTimechart()
			mCols, mFuns, revIndex := convertRequestToInternalStats(aggs.GroupByRequest, usedByTimechart)
			blockRes.GroupByAggregation = &GroupByBuckets{
				AllRunningBuckets:   make([]*RunningBucketResults, 0),
				StringBucketIdx:     make(map[string]int),
				allMeasureCols:      mCols,
				internalMeasureFns:  mFuns,
				reverseMeasureIndex: revIndex,
				maxBuckets:          aggs.GroupByRequest.BucketCount,
				GroupByColValCnt:    make(map[string]int),
			}
		}
	}

	if aggs != nil && aggs.Sort != nil {
		sortedRes, err := InitializeSort(count, aggs.Sort)
		if err != nil {
			log.Errorf("qid=%d, InitBlockResults: initialize sort request failed, err: %v", qid, err)
			return nil, err
		}
		blockRes.sortResults = true
		blockRes.SortedResults = sortedRes
	} else {
		initialSize := min(count, sutils.MAX_RECS_PER_WIP)
		blockRes.sortResults = false
		blockRes.UnsortedResults = make([]*sutils.RecordResultContainer, initialSize)
		blockRes.nextUnsortedIdx = 0
	}
	blockRes.sizeLimit = count
	blockRes.MatchedCount = 0
	blockRes.batchErr = utils.GetOrCreateBatchErrorWithQid(qid)
	return blockRes, nil
}

// This function will map[colName] -> []idx of measure functions, converted measure ops, and the reverse index of original to converted op
// Converted Measure Ops: for example, to calculate average the block will need to track sum
// Count is always tracked for each bucket
func convertRequestToInternalStats(req *structs.GroupByRequest, usedByTimechart bool) (map[string][]int, []*structs.MeasureAggregator, []int) {
	colToIdx := make(map[string][]int) // maps a column name to all indices in allConvertedMeasureOps it relates to
	allConvertedMeasureOps := make([]*structs.MeasureAggregator, 0)
	allReverseIndex := make([]int, 0)
	idx := 0
	for _, m := range req.MeasureOperations {
		measureColStr := m.MeasureCol
		var mFunc sutils.AggregateFunctions
		var overrodeMeasureAgg *structs.MeasureAggregator
		switch m.MeasureFunc {
		case sutils.Sum, sutils.Max, sutils.Min, sutils.List:
			if m.ValueColRequest != nil {
				curId, err := aggregations.SetupMeasureAgg(m, &allConvertedMeasureOps, m.MeasureFunc, &allReverseIndex, colToIdx, idx)
				if err != nil {
					log.Errorf("convertRequestToInternalStats: Error while setting up measure agg for %v, err: %v", m.MeasureFunc, err)
				}
				idx = curId
				continue
			} else {
				mFunc = m.MeasureFunc
			}
		case sutils.Range:
			if m.ValueColRequest != nil {
				curId, err := aggregations.SetupMeasureAgg(m, &allConvertedMeasureOps, sutils.Range, &allReverseIndex, colToIdx, idx)
				if err != nil {
					log.Errorf("convertRequestToInternalStats: Error while setting up measure agg for range, err: %v", err)
				}
				idx = curId
				continue
			} else {
				curId, err := aggregations.AddMeasureAggInRunningStatsForRange(m, &allConvertedMeasureOps, &allReverseIndex, colToIdx, idx)
				if err != nil {
					log.Errorf("convertRequestToInternalStats: Error while adding measure agg in running stats for range, err: %v", err)
				}
				idx = curId
				continue
			}
		case sutils.Earliest:
			fallthrough
		case sutils.Latest:
			isLatest := sutils.Latest == m.MeasureFunc
			if m.ValueColRequest == nil {
				curId, err := aggregations.AddMeasureAggInRunningStatsForLatestOrEarliest(m, &allConvertedMeasureOps, &allReverseIndex, colToIdx, idx, isLatest)
				if err != nil {
					log.Errorf("convertRequestToInternalStats: Error while adding measure agg in running stats for latest, err: %v", err)
				}
				idx = curId
				continue
			}
		case sutils.Count:
			if m.ValueColRequest != nil {
				curId, err := aggregations.AddMeasureAggInRunningStatsForCount(m, &allConvertedMeasureOps, &allReverseIndex, colToIdx, idx)
				if err != nil {
					log.Errorf("convertRequestToInternalStats: Error while adding measure agg in running stats for count, err: %v", err)
				}
				idx = curId
			} else {
				if usedByTimechart {
					aggregations.AddAggCountToTimechartRunningStats(m, &allConvertedMeasureOps, &allReverseIndex, colToIdx, idx)
					idx++
					continue
				}
				allReverseIndex = append(allReverseIndex, -1)
			}
			continue
		case sutils.Avg:
			if m.ValueColRequest != nil {
				curId, err := aggregations.SetupMeasureAgg(m, &allConvertedMeasureOps, sutils.Avg, &allReverseIndex, colToIdx, idx)
				if err != nil {
					log.Errorf("convertRequestToInternalStats: Error while adding measure agg in running stats for avg, err: %v", err)
				}
				idx = curId
				continue
			} else {
				if usedByTimechart {
					aggregations.AddAggAvgToTimechartRunningStats(m, &allConvertedMeasureOps, &allReverseIndex, colToIdx, idx)
					idx += 2
					continue
				}
				mFunc = sutils.Sum
				overrodeMeasureAgg = m
			}
		case sutils.Perc:
			if m.ValueColRequest != nil {
				curId, err := aggregations.SetupMeasureAgg(m, &allConvertedMeasureOps, sutils.Perc, &allReverseIndex, colToIdx, idx)
				if err != nil {
					log.Errorf("convertRequestToInternalStats: Erroro while adding measure agg in running stats for perc, err: %v", err)
				}
				idx = curId
				continue
			} else {
				mFunc = sutils.Perc
			}
		case sutils.Cardinality:
			if m.ValueColRequest != nil {
				curId, err := aggregations.AddMeasureAggInRunningStatsForCardinality(m, &allConvertedMeasureOps, &allReverseIndex, colToIdx, idx)
				if err != nil {
					log.Errorf("convertRequestToInternalStats: Error while adding measure agg in running stats for cardinality, err: %v", err)
				}
				idx = curId
				continue
			} else {
				mFunc = m.MeasureFunc
			}
		case sutils.EstdcError:
			if m.ValueColRequest != nil {
				curId, err := aggregations.AddMeasureAggInRunningStatsForEstdcError(m, &allConvertedMeasureOps, &allReverseIndex, colToIdx, idx)
				if err != nil {
					log.Errorf("convertRequestToInternalStats: Error while adding measure agg in running stats for estdc_error, err: %v", err)
				}
				idx = curId
				continue
			} else {
				mFunc = m.MeasureFunc
			}
		case sutils.Values:
			if m.ValueColRequest != nil {
				curId, err := aggregations.AddMeasureAggInRunningStatsForValues(m, &allConvertedMeasureOps, &allReverseIndex, colToIdx, idx)
				if err != nil {
					log.Errorf("convertRequestToInternalStats: Error while adding measure agg in running stats for cardinality, err: %v", err)
				}
				idx = curId
				continue
			} else {
				mFunc = m.MeasureFunc
			}
		default:
			mFunc = m.MeasureFunc
		}

		if _, ok := colToIdx[measureColStr]; !ok {
			colToIdx[measureColStr] = make([]int, 0)
		}
		allReverseIndex = append(allReverseIndex, idx)
		colToIdx[measureColStr] = append(colToIdx[measureColStr], idx)
		allConvertedMeasureOps = append(allConvertedMeasureOps, &structs.MeasureAggregator{
			MeasureCol:         m.MeasureCol,
			MeasureFunc:        mFunc,
			ValueColRequest:    m.ValueColRequest,
			StrEnc:             m.StrEnc,
			OverrodeMeasureAgg: overrodeMeasureAgg,
			Param:              m.Param,
		})
		idx++
	}
	allConvertedMeasureOps = allConvertedMeasureOps[:idx]
	return colToIdx, allConvertedMeasureOps, allReverseIndex
}

/*
Returns:
  - bool if this record was added
  - string for any remote records that were removed
*/
func (b *BlockResults) Add(rrc *sutils.RecordResultContainer) (bool, string) {
	if b.sortResults {
		return b.SortedResults.Add(rrc)
	}

	if b.nextUnsortedIdx < b.sizeLimit {
		var err error
		b.UnsortedResults, err = utils.GrowSliceInChunks(b.UnsortedResults, int(b.nextUnsortedIdx+1), sutils.MAX_RECS_PER_WIP)
		if err != nil {
			log.Errorf("BlockResults.Add: Error while growing slice, err: %v", err)
			return false, ""
		}
		b.UnsortedResults[b.nextUnsortedIdx] = rrc
		b.nextUnsortedIdx++
		if rrc.SegKeyInfo.IsRemote {
			return true, rrc.SegKeyInfo.RecordId
		}
		return true, ""
	}
	return false, ""
}

func (b *BlockResults) MergeBuckets(blockRes *BlockResults) {
	if b.TimeAggregation != nil && blockRes.TimeAggregation != nil && !blockRes.aggs.UsedByTimechart() {
		b.TimeAggregation.MergeBuckets(blockRes.TimeAggregation)
	}
	if b.GroupByAggregation != nil && blockRes.GroupByAggregation != nil {
		b.GroupByAggregation.MergeBuckets(blockRes.GroupByAggregation)
	}
}

func (b *BlockResults) MergeRemoteBuckets(grpBuckets *GroupByBucketsJSON, timeBuckets *TimeBucketsJSON) error {

	if timeBuckets != nil {
		remoteBuckets, err := timeBuckets.ToTimeBuckets()
		if err != nil {
			return err
		}
		if b.TimeAggregation == nil {
			b.TimeAggregation = remoteBuckets
		} else {
			b.TimeAggregation.MergeBuckets(remoteBuckets)
		}
	}
	if grpBuckets != nil {
		remoteBuckets, err := grpBuckets.ToGroupByBucket(b.aggs.GroupByRequest)
		if err != nil {
			return err
		}
		if b.GroupByAggregation == nil {
			b.GroupByAggregation = remoteBuckets
		} else {
			b.GroupByAggregation.MergeBuckets(remoteBuckets)
		}
	}
	return nil
}

// if sort is enabled, will call heap.Pop on the underlying results
func (b *BlockResults) GetResults() []*sutils.RecordResultContainer {
	if b.sortResults {
		return b.SortedResults.GetSortedResults()
	} else {
		return b.UnsortedResults[:b.nextUnsortedIdx]
	}
}

// if sort is enabled, will call heap.Pop on the underlying results
func (b *BlockResults) GetResultsCopy() []*sutils.RecordResultContainer {
	if b.sortResults {
		return b.SortedResults.GetSortedResultsCopy()
	} else {
		res := make([]*sutils.RecordResultContainer, b.nextUnsortedIdx)
		copy(res, b.UnsortedResults[:b.nextUnsortedIdx])
		return res
	}
}

func (b *BlockResults) AddMatchedCount(c uint64) {
	b.MatchedCount += c
}

func (b *BlockResults) ShouldAddMore() bool {
	if !b.sortResults {
		return b.nextUnsortedIdx < b.sizeLimit
	} else {
		return true
	}
}

func (b *BlockResults) WillValueBeAdded(valToAdd float64) bool {
	if !b.sortResults {
		return b.nextUnsortedIdx < b.sizeLimit
	} else {
		if b.sizeLimit == 0 {
			return false
		}
		if b.SortedResults.Results.Len() < int(b.sizeLimit) {
			return true
		}
		if b.SortedResults.Ascending {
			if valToAdd < b.SortedResults.LastValue {
				return true
			}
		} else {
			if valToAdd > b.SortedResults.LastValue {
				return true
			}
		}
		return false
	}
}

func (b *BlockResults) ShouldIterateRecords(aggsHasTimeHt bool, isBlkFullyEncosed bool,
	lowTs uint64, highTs uint64) bool {

	if !isBlkFullyEncosed {
		// We only want some records.
		return true
	}

	if aggsHasTimeHt {
		return false
	}

	if b.aggs != nil && b.aggs.Sort != nil {
		// Check if some records will be added.
		if b.aggs.Sort.Ascending {
			return b.WillValueBeAdded(float64(lowTs))
		} else {
			return b.WillValueBeAdded(float64(highTs))
		}
	}

	// Check if there's space to add more records.
	return b.nextUnsortedIdx < b.sizeLimit
}

func (b *BlockResults) AddMeasureResultsToKey(currKey []byte, measureResults []sutils.CValueEnclosure,
	groupByColVal string, usedByTimechart bool, qid uint64, unsetRecord map[string]sutils.CValueEnclosure) {

	if b.GroupByAggregation == nil {
		return
	}
	bKey := utils.UnsafeByteSliceToString(currKey)
	bucketIdx, ok := b.GroupByAggregation.StringBucketIdx[bKey]

	var bucket *RunningBucketResults
	if !ok {
		nBuckets := len(b.GroupByAggregation.AllRunningBuckets)
		if nBuckets >= b.GroupByAggregation.maxBuckets {
			return
		}
		bucket = initRunningGroupByBucket(b.GroupByAggregation.internalMeasureFns, qid)
		b.GroupByAggregation.AllRunningBuckets = append(b.GroupByAggregation.AllRunningBuckets, bucket)
		// only make a copy if this is the first time we are inserting it
		// so that the caller may free up the backing space for this currKey/bKey
		keyCopy := make([]byte, len(bKey))
		copy(keyCopy, bKey)
		b.GroupByAggregation.StringBucketIdx[utils.UnsafeByteSliceToString(keyCopy)] = nBuckets
	} else {
		bucket = b.GroupByAggregation.AllRunningBuckets[bucketIdx]
	}

	if usedByTimechart {
		var gRunningStats []runningStats
		var exists bool
		gRunningStats, exists = bucket.groupedRunningStats[groupByColVal]
		if !exists {
			gRunningStats = initRunningStats(b.GroupByAggregation.internalMeasureFns)
			bucket.groupedRunningStats[groupByColVal] = gRunningStats
		}
		bucket.AddMeasureResults(&gRunningStats, measureResults, qid, 1, true, b.batchErr, unsetRecord)
	} else {
		bucket.AddMeasureResults(&bucket.runningStats, measureResults, qid, 1, false, b.batchErr, unsetRecord)
	}

}

func (b *BlockResults) AddMeasureResultsToKeyAgileTree(bKey string,
	measureResults []sutils.CValueEnclosure, qid uint64, cnt uint64,
	unsetRecord map[string]sutils.CValueEnclosure) {

	if b.GroupByAggregation == nil {
		return
	}
	bucketIdx, ok := b.GroupByAggregation.StringBucketIdx[bKey]

	var bucket *RunningBucketResults
	if !ok {
		nBuckets := len(b.GroupByAggregation.AllRunningBuckets)
		if nBuckets >= b.GroupByAggregation.maxBuckets {
			return
		}
		bucket = initRunningGroupByBucket(b.GroupByAggregation.internalMeasureFns, qid)
		b.GroupByAggregation.AllRunningBuckets = append(b.GroupByAggregation.AllRunningBuckets, bucket)
		b.GroupByAggregation.StringBucketIdx[bKey] = nBuckets
	} else {
		bucket = b.GroupByAggregation.AllRunningBuckets[bucketIdx]
	}
	bucket.AddMeasureResults(&bucket.runningStats, measureResults, qid, cnt, false, b.batchErr, unsetRecord)
}

func (b *BlockResults) AddKeyToTimeBucket(bucketKey uint64, count uint16) {
	if b.TimeAggregation == nil {
		return
	}
	bucketIdx, ok := b.TimeAggregation.UnsignedBucketIdx[bucketKey]
	var bucket *RunningBucketResults
	if !ok {
		bucket = initRunningTimeBucket()
		b.TimeAggregation.AllRunningBuckets = append(b.TimeAggregation.AllRunningBuckets, bucket)
		b.TimeAggregation.UnsignedBucketIdx[bucketKey] = len(b.TimeAggregation.AllRunningBuckets) - 1
	} else {
		bucket = b.TimeAggregation.AllRunningBuckets[bucketIdx]
	}
	bucket.AddTimeToBucketStats(count)
}

func (b *BlockResults) GetTimeBuckets() *structs.AggregationResult {
	if b.TimeAggregation == nil {
		return &structs.AggregationResult{IsDateHistogram: true}
	}
	return b.TimeAggregation.ConvertToAggregationResult()
}

// returns a map[cName] -> []idx for measure cols and num measure functions
func (b *BlockResults) GetConvertedMeasureInfo() (map[string][]int, []*structs.MeasureAggregator) {
	if b.GroupByAggregation == nil {
		return nil, nil
	}
	return b.GroupByAggregation.allMeasureCols, b.GroupByAggregation.internalMeasureFns
}

func (tb *TimeBuckets) MergeBuckets(toMerge *TimeBuckets) {
	for key, idx := range toMerge.UnsignedBucketIdx {
		bucket := toMerge.AllRunningBuckets[idx]
		if idx, ok := tb.UnsignedBucketIdx[key]; !ok {
			tb.AllRunningBuckets = append(tb.AllRunningBuckets, bucket)
			tb.UnsignedBucketIdx[key] = len(tb.AllRunningBuckets) - 1
		} else {
			tb.AllRunningBuckets[idx].MergeRunningBuckets(bucket)
		}
	}
}

func (tb *TimeBuckets) ConvertToAggregationResult() *structs.AggregationResult {
	results := make([]*structs.BucketResult, len(tb.AllRunningBuckets))
	bucketNum := 0
	for key, idx := range tb.UnsignedBucketIdx {
		bucket := tb.AllRunningBuckets[idx]
		results[bucketNum] = &structs.BucketResult{
			ElemCount: bucket.count,
			BucketKey: key,
		}
		bucketNum++
	}
	results = results[:bucketNum]
	return &structs.AggregationResult{
		IsDateHistogram: true,
		Results:         results,
	}
}

func (b *BlockResults) GetGroupByBuckets() *structs.AggregationResult {
	if b.GroupByAggregation == nil {
		return &structs.AggregationResult{IsDateHistogram: false}
	}

	var timechart *structs.TimechartExpr
	if b.aggs.UsedByTimechart() {
		timechart = b.aggs.TimeHistogram.Timechart
	}
	return b.GroupByAggregation.ConvertToAggregationResult(b.aggs.GroupByRequest, timechart, b.batchErr)
}

// If the current GroupByBuckets are used by timechart, and timechart has a limit option, there are two different methods to add results at this point. This is because the limit option has two different ways of calculating scores, and we only return the top or bottom N results, with the remaining ones merged and placed into the 'other' col
// 1. Single Agg: The score is based on the sum of the values in the aggregation. It requires two iterations. In the first iteration, sum up scores for each groupVal
// 2. Multiple Aggs: The score is based on the frequency of each value of <field>. It only requires one iteration because we already have the frep for groupVal before the iteration begins
func (gb *GroupByBuckets) ConvertToAggregationResult(req *structs.GroupByRequest, timechart *structs.TimechartExpr, batchErr *utils.BatchError) *structs.AggregationResult {

	tmLimitResult := &structs.TMLimitResult{
		GroupValScoreMap: aggregations.InitialScoreMap(timechart, gb.GroupByColValCnt),
	}
	isRankBySum := aggregations.IsRankBySum(timechart)

	// Get scores for ranking
	if isRankBySum {
		for _, idx := range gb.StringBucketIdx {
			bucket := gb.AllRunningBuckets[idx]
			currRes := make(map[string]sutils.CValueEnclosure)
			// Add results for group by cols inside the time range bucket
			if len(bucket.groupedRunningStats) > 0 {
				for groupByColVal, gRunningStats := range bucket.groupedRunningStats {
					gb.AddResultToStatRes(req, bucket, gRunningStats, currRes, groupByColVal, timechart, tmLimitResult, batchErr)
				}
			}
		}
	}

	bucketNum := 0
	results := make([]*structs.BucketResult, len(gb.AllRunningBuckets))
	tmLimitResult.Hll = structs.CreateNewHll()
	tmLimitResult.StrSet = make(map[string]struct{}, 0)
	td, err := utils.CreateNewTDigest()
	if err != nil {
		batchErr.AddError("GroupByBuckets.ConvertToAggregationResult:INITIALIZING_A_DIGEST_TREE", fmt.Errorf("failed to create a new digest tree: err: %v", err))
	}
	tmLimitResult.TDigest = td
	tmLimitResult.ValIsInLimit = aggregations.CheckGroupByColValsAgainstLimit(timechart, gb.GroupByColValCnt, tmLimitResult.GroupValScoreMap, req.MeasureOperations, batchErr)
	for key, idx := range gb.StringBucketIdx {
		bucket := gb.AllRunningBuckets[idx]
		currRes := make(map[string]sutils.CValueEnclosure)

		// Add results for group by cols inside the time range bucket
		if len(bucket.groupedRunningStats) > 0 {
			// Every measure operator needs to check whether the current groupByColVal is within the limit
			// If it's not, its col name should be displayed as [aggOp: otherstr]
			otherCValArr := make([]*sutils.CValueEnclosure, len(req.MeasureOperations))
			for i := 0; i < len(req.MeasureOperations); i++ {
				otherCValArr[i] = &sutils.CValueEnclosure{CVal: nil, Dtype: sutils.SS_INVALID}
			}

			tmLimitResult.OtherCValArr = otherCValArr
			for groupByColVal, gRunningStats := range bucket.groupedRunningStats {
				gb.AddResultToStatRes(req, bucket, gRunningStats, currRes, groupByColVal, timechart, tmLimitResult, batchErr)
			}

			if timechart.LimitExpr != nil && timechart.LimitExpr.Num < len(bucket.groupedRunningStats) {
				for index, mInfo := range req.MeasureOperations {
					// To be modified: user can customize otherstr
					mInfoStr := mInfo.String() + ": other"
					currRes[mInfoStr] = *tmLimitResult.OtherCValArr[index]
				}
			}

		} else {
			gb.AddResultToStatRes(req, bucket, bucket.runningStats, currRes, "", nil, tmLimitResult, batchErr)
		}

		var bucketKey interface{}
		var err error

		bucketKey, err = sutils.ConvertGroupByKeyFromBytes([]byte(key))

		if err != nil {
			batchErr.AddError("GroupByBuckets.ConvertToAggregationResult:CONVERT_GROUP_BY_KEY", fmt.Errorf("failed to convert group by key: %v, err: %v", key, err))
		}

		results[bucketNum] = &structs.BucketResult{
			ElemCount:   bucket.count,
			BucketKey:   bucketKey,
			StatRes:     currRes,
			GroupByKeys: req.GroupByColumns,
		}
		bucketNum++
	}

	aggregations.SortTimechartRes(timechart, &results)
	return &structs.AggregationResult{
		IsDateHistogram: false,
		Results:         results,
	}
}

func (gb *GroupByBuckets) AddResultToStatRes(req *structs.GroupByRequest, bucket *RunningBucketResults, runningStats []runningStats, currRes map[string]sutils.CValueEnclosure,
	groupByColVal string, timechart *structs.TimechartExpr, tmLimitResult *structs.TMLimitResult, batchErr *utils.BatchError) {
	// Some aggregate functions require multiple measure funcs or raw field values to calculate the result. For example, range() needs both max() and min(), and aggregates with eval statements may require multiple raw field values
	// Therefore, it is essential to assign a value to 'idx' appropriately to skip the intermediate results generated during the computation from runningStats bucket
	idx := 0

	// If current col should be merged into the other col
	isOtherCol := aggregations.IsOtherCol(tmLimitResult.ValIsInLimit, groupByColVal)
	usedByTimechart := (timechart != nil)
	usedByTimechartGroupByCol := len(groupByColVal) > 0
	for index, mInfo := range req.MeasureOperations {
		mInfoStr := mInfo.String()
		if usedByTimechartGroupByCol {
			if !isOtherCol {
				mInfoStr = mInfoStr + ": " + groupByColVal
			}
		}

		var hllToMerge *utils.GobbableHll
		var strSetToMerge map[string]struct{}
		var tdToMerge *utils.GobbableTDigest
		var eVal sutils.CValueEnclosure

		gb.updateEValFromRunningBuckets(mInfo, runningStats, usedByTimechart, mInfoStr, currRes, bucket, &idx, &eVal, &hllToMerge, &strSetToMerge, &tdToMerge, batchErr)

		shouldAddRes := aggregations.ShouldAddRes(timechart, tmLimitResult, index, eVal, hllToMerge, strSetToMerge, tdToMerge, mInfo.MeasureFunc, mInfo.Param, groupByColVal, isOtherCol, batchErr)
		if shouldAddRes {
			if mInfo.MeasureFunc == sutils.Latest || mInfo.MeasureFunc == sutils.Earliest {
				castedEVal, ok := eVal.CVal.(structs.RunningLatestOrEarliestVal)
				if ok {
					elVal := castedEVal.Value
					currRes[mInfoStr] = elVal
				}
			} else {
				currRes[mInfoStr] = eVal
			}
		}
	}
}

func (gb *GroupByBuckets) updateEValFromRunningBuckets(mInfo *structs.MeasureAggregator, runningStats []runningStats, usedByTimechart bool, mInfoStr string,
	currRes map[string]sutils.CValueEnclosure, bucket *RunningBucketResults, idxPtr *int, eVal *sutils.CValueEnclosure, hllToMerge **utils.GobbableHll,
	strSetToMerge *map[string]struct{}, tdToMerge **utils.GobbableTDigest, batchErr *utils.BatchError) {
	if hllToMerge == nil || strSetToMerge == nil {
		// This should never happen
		log.Errorf("GroupByBuckets.AddResultToStatRes: hllToMerge or strSetToMerge is nil. hllToMerge: %v, strSetToMerge: %v", hllToMerge, strSetToMerge)
		return
	}

	incrementIdxBy := 1

	defer func() {
		*idxPtr += incrementIdxBy
	}()

	idx := *idxPtr

	switch mInfo.MeasureFunc {
	case sutils.Count:
		incrementIdxBy = 1
		if mInfo.ValueColRequest != nil || usedByTimechart {
			if !usedByTimechart && len(mInfo.ValueColRequest.GetFields()) == 0 {
				batchErr.AddError("GroupByBuckets.AddResultToStatRes:COUNT", fmt.Errorf("zero fields of ValueColRequest for count: %v", mInfoStr))
				return
			}

			countIdx := gb.reverseMeasureIndex[idx]
			runningStats[countIdx].syncRawValue()
			countVal, err := runningStats[countIdx].rawVal.GetUIntValue()
			if err != nil {
				currRes[mInfoStr] = sutils.CValueEnclosure{CVal: nil, Dtype: sutils.SS_INVALID}
				return
			}

			eVal.CVal = countVal
			eVal.Dtype = sutils.SS_DT_UNSIGNED_NUM
		} else {

			eVal.CVal = bucket.count
			eVal.Dtype = sutils.SS_DT_UNSIGNED_NUM
		}
	case sutils.Avg:
		var avg float64
		if mInfo.ValueColRequest != nil {
			incrementIdxBy = 1

			if len(mInfo.ValueColRequest.GetFields()) == 0 {
				batchErr.AddError("GroupByBuckets.AddResultToStatRes:AVG", fmt.Errorf("zero fields of ValueColRequest for avg: %v", mInfoStr))
				return
			}
			valIdx := gb.reverseMeasureIndex[idx]
			if runningStats[valIdx].avgStat != nil {
				sumVal := runningStats[valIdx].avgStat.Sum
				countVal := runningStats[valIdx].avgStat.Count
				if countVal == 0 {
					avg = 0
				} else {
					avg = sumVal / float64(countVal)
				}
			} else {
				currRes[mInfoStr] = sutils.CValueEnclosure{CVal: nil, Dtype: sutils.SS_INVALID}
				return
			}

			eVal.CVal = avg
			eVal.Dtype = sutils.SS_DT_FLOAT
		} else {
			if usedByTimechart {
				// If used by timechart, we need to calculate the average by dividing the sum of the values by the count of the values
				// so we will be using two indices one for sum and one for count
				// so incrementIdxBy will be 2
				incrementIdxBy = 2
			} else {
				incrementIdxBy = 1
			}

			sumIdx := gb.reverseMeasureIndex[idx]
			runningStats[sumIdx].syncRawValue()
			sumRawVal, err := runningStats[sumIdx].rawVal.GetFloatValue()
			if err != nil {
				currRes[mInfoStr] = sutils.CValueEnclosure{CVal: nil, Dtype: sutils.SS_INVALID}
				return
			}

			if usedByTimechart {
				sumIdx := gb.reverseMeasureIndex[idx]
				runningStats[sumIdx].syncRawValue()
				sumRawVal, err := runningStats[sumIdx].rawVal.GetFloatValue()
				if err != nil {
					currRes[mInfoStr] = sutils.CValueEnclosure{CVal: nil, Dtype: sutils.SS_INVALID}
					return
				}

				countIdx := gb.reverseMeasureIndex[idx+1]
				runningStats[countIdx].syncRawValue()
				countRawVal, err := runningStats[countIdx].rawVal.GetFloatValue()
				if err != nil {
					currRes[mInfoStr] = sutils.CValueEnclosure{CVal: nil, Dtype: sutils.SS_INVALID}
					return
				}

				eVal.CVal = sumRawVal / countRawVal
				eVal.Dtype = sutils.SS_DT_FLOAT
			} else {
				if bucket.count == 0 {
					avg = 0
				} else {
					avg = sumRawVal / float64(bucket.count)
				}

				eVal.CVal = avg
				eVal.Dtype = sutils.SS_DT_FLOAT
			}
		}
	case sutils.Earliest:
		fallthrough
	case sutils.Latest:
		if mInfo.ValueColRequest == nil {
			// order should be the same as defined in evalaggs.go -> @AddMeasureAggInRunningStatsForLatestOrEarliest
			// timestamp is present at index idx+1
			// the value (can be any dtype) present at index idx
			incrementIdxBy = 2
			elTsIdx := gb.reverseMeasureIndex[idx+1]
			elIdx := gb.reverseMeasureIndex[idx]
			runningStats[elTsIdx].syncRawValue()
			elTsVal, err := runningStats[elTsIdx].rawVal.GetUIntValue()
			if err != nil {
				currRes[mInfoStr] = sutils.CValueEnclosure{CVal: nil, Dtype: sutils.SS_INVALID}
				return
			}
			runningStats[elIdx].syncRawValue()
			elVal := runningStats[elIdx].rawVal

			eVal.CVal = structs.RunningLatestOrEarliestVal{
				Timestamp: elTsVal,
				Value:     elVal,
			}
			eVal.Dtype = sutils.SS_DT_BACKFILL
		}
	case sutils.Range:
		if mInfo.ValueColRequest != nil {
			incrementIdxBy = 1

			if len(mInfo.ValueColRequest.GetFields()) == 0 {
				batchErr.AddError("GroupByBuckets.AddResultToStatRes:RANGE", fmt.Errorf("zero fields of ValueColRequest for range: %v", mInfoStr))
				return
			}
			valIdx := gb.reverseMeasureIndex[idx]
			rangeVal := 0.0
			if runningStats[valIdx].rangeStat != nil {
				minVal := runningStats[valIdx].rangeStat.Min
				maxVal := runningStats[valIdx].rangeStat.Max
				rangeVal = maxVal - minVal
			} else {
				currRes[mInfoStr] = sutils.CValueEnclosure{CVal: nil, Dtype: sutils.SS_INVALID}
				return
			}

			eVal.CVal = rangeVal
			eVal.Dtype = sutils.SS_DT_FLOAT
		} else {
			incrementIdxBy = 2

			minIdx := gb.reverseMeasureIndex[idx]
			runningStats[minIdx].syncRawValue()
			minRawVal, err := runningStats[minIdx].rawVal.GetFloatValue()
			if err != nil {
				currRes[mInfoStr] = sutils.CValueEnclosure{CVal: nil, Dtype: sutils.SS_INVALID}
				return
			}

			maxIdx := gb.reverseMeasureIndex[idx+1]
			runningStats[maxIdx].syncRawValue()
			maxRawVal, err := runningStats[maxIdx].rawVal.GetFloatValue()
			if err != nil {
				currRes[mInfoStr] = sutils.CValueEnclosure{CVal: nil, Dtype: sutils.SS_INVALID}
				return
			}

			eVal.CVal = maxRawVal - minRawVal
			eVal.Dtype = sutils.SS_DT_FLOAT
		}
	case sutils.Cardinality:
		incrementIdxBy = 1

		valIdx := gb.reverseMeasureIndex[idx]
		if mInfo.ValueColRequest != nil {
			if len(mInfo.ValueColRequest.GetFields()) == 0 {
				batchErr.AddError("GroupByBuckets.AddResultToStatRes:CARDINALITY", fmt.Errorf("zero fields of ValueColRequest for cardinality: %v", mInfoStr))
				return
			}

			runningStats[valIdx].syncRawValue()
			hll, ok := runningStats[valIdx].rawVal.CVal.(*utils.GobbableHll)
			if !ok {
				currRes[mInfoStr] = sutils.CValueEnclosure{CVal: nil, Dtype: sutils.SS_INVALID}
				return
			}
			eVal.CVal = hll.Cardinality()
			eVal.Dtype = sutils.SS_DT_UNSIGNED_NUM
		} else {
			finalVal := runningStats[valIdx].hll.Cardinality()
			eVal.CVal = finalVal
			eVal.Dtype = sutils.SS_DT_UNSIGNED_NUM

			*hllToMerge = runningStats[valIdx].hll
		}
	case sutils.EstdcError:
		incrementIdxBy = 1

		valIdx := gb.reverseMeasureIndex[idx]
		if mInfo.ValueColRequest != nil {
			if len(mInfo.ValueColRequest.GetFields()) == 0 {
				batchErr.AddError("GroupByBuckets.AddResultToStatRes:ESTDC_ERROR", fmt.Errorf("zero fields of ValueColRequest for estimated dc error: %v", mInfoStr))
				return
			}

			runningStats[valIdx].syncRawValue()
			hll, ok := runningStats[valIdx].rawVal.CVal.(*utils.GobbableHll)
			if !ok {
				currRes[mInfoStr] = sutils.CValueEnclosure{CVal: nil, Dtype: sutils.SS_INVALID}
				return
			}
			eVal.CVal = hll.RelativeError()
			eVal.Dtype = sutils.SS_DT_UNSIGNED_NUM
		} else {
			finalVal := runningStats[valIdx].hll.RelativeError()
			eVal.CVal = finalVal
			eVal.Dtype = sutils.SS_DT_UNSIGNED_NUM

			*hllToMerge = runningStats[valIdx].hll
		}
	case sutils.Perc:
		incrementIdxBy = 1
		valIdx := gb.reverseMeasureIndex[idx]
		fltPercentileVal := mInfo.Param / 100
		if fltPercentileVal < 0 || fltPercentileVal > 1 {
			batchErr.AddError("GroupByBuckets.AddResultToStatRes:PERCENTILE", fmt.Errorf("percentile param out of range"))
			return
		}
		if mInfo.ValueColRequest != nil {
			if len(mInfo.ValueColRequest.GetFields()) == 0 {
				batchErr.AddError("GroupByBuckets.AddResultToStatRes:PERCENTILE", fmt.Errorf("zerofields of ValueColRequest for percentile: %v", mInfoStr))
				return
			}
		}
		td := runningStats[valIdx].tDigest
		mQuantile := td.GetQuantile(fltPercentileVal)
		if math.IsNaN(mQuantile) {
			eVal.CVal = 0.0
		} else {
			eVal.CVal = td.GetQuantile(fltPercentileVal)
		}
		eVal.Dtype = sutils.SS_DT_FLOAT
		*tdToMerge = runningStats[valIdx].tDigest
	case sutils.Values:
		incrementIdxBy = 1

		if mInfo.ValueColRequest != nil {
			if len(mInfo.ValueColRequest.GetFields()) == 0 {
				batchErr.AddError("GroupByBuckets.AddResultToStatRes:VALUES", fmt.Errorf("zero fields of ValueColRequest for values: %v", mInfoStr))
				return
			}
		}

		valIdx := gb.reverseMeasureIndex[idx]
		runningStats[valIdx].syncRawValue()
		strSet, ok := runningStats[valIdx].rawVal.CVal.(map[string]struct{})
		if !ok {
			currRes[mInfoStr] = sutils.CValueEnclosure{CVal: nil, Dtype: sutils.SS_INVALID}
			return
		}
		*strSetToMerge = strSet

		uniqueStrings := make([]string, 0)
		for str := range strSet {
			uniqueStrings = append(uniqueStrings, str)
		}

		sort.Strings(uniqueStrings)

		eVal.Dtype = sutils.SS_DT_STRING_SLICE
		eVal.CVal = uniqueStrings
	case sutils.List:
		incrementIdxBy = 1

		if mInfo.ValueColRequest != nil {
			if len(mInfo.ValueColRequest.GetFields()) == 0 {
				batchErr.AddError("GroupByBuckets.AddResultToStatRes:LIST", fmt.Errorf("zero fields of ValueColRequest for list: %v", mInfoStr))
				return
			}
		}
		valIdx := gb.reverseMeasureIndex[idx]
		runningStats[valIdx].syncRawValue()
		strList, ok := runningStats[valIdx].rawVal.CVal.([]string)
		if !ok {
			currRes[mInfoStr] = sutils.CValueEnclosure{CVal: nil, Dtype: sutils.SS_INVALID}
			return
		}
		if len(strList) > sutils.MAX_SPL_LIST_SIZE {
			strList = strList[:sutils.MAX_SPL_LIST_SIZE]
		}

		eVal.Dtype = sutils.SS_DT_STRING_SLICE
		eVal.CVal = strList
	case sutils.Sum, sutils.Max, sutils.Min:
		incrementIdxBy = 1

		if mInfo.ValueColRequest != nil {
			if len(mInfo.ValueColRequest.GetFields()) == 0 {
				batchErr.AddError("GroupByBuckets.AddResultToStatRes:SUM/MAX/MIN", fmt.Errorf("zero fields of ValueColRequest for sum/max/min: %v", mInfoStr))
				return
			}
		}
		valIdx := gb.reverseMeasureIndex[idx]
		runningStats[valIdx].syncRawValue()
		cTypeVal := runningStats[valIdx].rawVal
		eVal.CVal = cTypeVal.CVal
		eVal.Dtype = cTypeVal.Dtype
	default:
		incrementIdxBy = 1

		valIdx := gb.reverseMeasureIndex[idx]
		runningStats[valIdx].syncRawValue()
		cTypeVal := runningStats[valIdx].rawVal
		eVal.CVal = cTypeVal.CVal
		eVal.Dtype = cTypeVal.Dtype
	}
}

func (gb *GroupByBuckets) MergeBuckets(toMerge *GroupByBuckets) {

	if len(gb.GroupByColValCnt) > 0 {
		aggregations.MergeMap(gb.GroupByColValCnt, toMerge.GroupByColValCnt)
	} else {
		gb.GroupByColValCnt = toMerge.GroupByColValCnt
	}

	for key, idx := range toMerge.StringBucketIdx {
		bucket := toMerge.AllRunningBuckets[idx]
		if idx, ok := gb.StringBucketIdx[key]; !ok {
			if len(gb.AllRunningBuckets) >= gb.maxBuckets {
				continue
			}
			gb.AllRunningBuckets = append(gb.AllRunningBuckets, bucket)
			gb.StringBucketIdx[key] = len(gb.AllRunningBuckets) - 1
		} else {
			gb.AllRunningBuckets[idx].MergeRunningBuckets(bucket)
		}
	}
}

func (gb *GroupByBuckets) ConvertToJson() (*GroupByBucketsJSON, error) {
	retVal := &GroupByBucketsJSON{
		AllGroupbyBuckets: make(map[string]*RunningBucketResultsJSON, len(gb.AllRunningBuckets)),
	}
	for key, idx := range gb.StringBucketIdx {
		bucket := gb.AllRunningBuckets[idx]
		newBucket := &RunningBucketResultsJSON{
			Count:     bucket.count,
			CurrStats: bucket.currStats,
		}
		retVals := make([]RunningStatsJSON, 0, len(bucket.runningStats))
		for _, rs := range bucket.runningStats {
			retVals = append(retVals, rs.GetRunningStatJSON())
		}
		newBucket.RunningStats = retVals
		base64Key := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%v", key)))
		retVal.AllGroupbyBuckets[base64Key] = newBucket
	}
	return retVal, nil
}

func (tb *TimeBuckets) ConvertToJson() (*TimeBucketsJSON, error) {
	retVal := &TimeBucketsJSON{
		AllTimeBuckets: make(map[uint64]*RunningBucketResultsJSON, len(tb.AllRunningBuckets)),
	}
	for key, idx := range tb.UnsignedBucketIdx {
		bucket := tb.AllRunningBuckets[idx]
		newBucket := &RunningBucketResultsJSON{
			Count:     bucket.count,
			CurrStats: bucket.currStats,
		}
		retVals := make([]RunningStatsJSON, 0, len(bucket.runningStats))
		for _, rs := range bucket.runningStats {
			retVals = append(retVals, rs.GetRunningStatJSON())
		}
		newBucket.RunningStats = retVals
		retVal.AllTimeBuckets[key] = newBucket
	}
	return retVal, nil
}

func (tb *TimeBucketsJSON) ToTimeBuckets() (*TimeBuckets, error) {
	retVal := &TimeBuckets{
		AllRunningBuckets: make([]*RunningBucketResults, 0, len(tb.AllTimeBuckets)),
		UnsignedBucketIdx: make(map[uint64]int, len(tb.AllTimeBuckets)),
	}
	reverseIndex := 0
	for key, runningBucket := range tb.AllTimeBuckets {
		newBucket, err := runningBucket.Convert()
		if err != nil {
			return nil, err
		}
		retVal.AllRunningBuckets = append(retVal.AllRunningBuckets, newBucket)
		retVal.UnsignedBucketIdx[key] = reverseIndex
		reverseIndex++
	}
	return retVal, nil
}

func (gb *GroupByBucketsJSON) ToGroupByBucket(req *structs.GroupByRequest) (*GroupByBuckets, error) {
	mCols, mFuns, revIndex := convertRequestToInternalStats(req, false)
	retVal := &GroupByBuckets{
		AllRunningBuckets:   make([]*RunningBucketResults, 0, len(gb.AllGroupbyBuckets)),
		StringBucketIdx:     make(map[string]int, len(gb.AllGroupbyBuckets)),
		allMeasureCols:      mCols,
		internalMeasureFns:  mFuns,
		reverseMeasureIndex: revIndex,
		maxBuckets:          req.BucketCount,
	}
	reverseIndex := 0
	for base64Key, runningBucket := range gb.AllGroupbyBuckets {
		newBucket, err := runningBucket.Convert()
		if err != nil {
			return nil, err
		}
		retVal.AllRunningBuckets = append(retVal.AllRunningBuckets, newBucket)
		key, err := base64.StdEncoding.DecodeString(base64Key)
		if err != nil {
			log.Errorf("GroupByBucketsJSON.ToGroupByBucket: failed to decode base64Key: %v, err: %v", base64Key, err)
		}
		retVal.StringBucketIdx[string(key)] = reverseIndex
		reverseIndex++
	}
	return retVal, nil
}

func (rb *RunningBucketResultsJSON) Convert() (*RunningBucketResults, error) {
	newBucket := &RunningBucketResults{
		count:     rb.Count,
		currStats: rb.CurrStats,
	}
	currRunningStats := make([]runningStats, 0, len(rb.RunningStats))
	for _, rs := range rb.RunningStats {
		runningStat, err := rs.GetRunningStats()
		if err != nil {
			return nil, utils.TeeErrorf("RunningBucketResultsJSON.Convert: Error while converting running stats, err: %v", err)
		}
		currRunningStats = append(currRunningStats, runningStat)
	}
	newBucket.runningStats = currRunningStats
	return newBucket, nil
}

func (gb *GroupByBuckets) ToSerializedGroupByBuckets() *SerializedGroupByBuckets {
	if gb == nil {
		return nil
	}

	allRunningBuckets := make([]*SerializedRunningBucketResults, len(gb.AllRunningBuckets))
	for i, bucket := range gb.AllRunningBuckets {
		allRunningBuckets[i] = bucket.ToSerializedRunningBucketResults()
	}

	return &SerializedGroupByBuckets{
		AllRunningBuckets: allRunningBuckets,
		StringBucketIdx:   gb.StringBucketIdx,
		GroupByColValCnt:  gb.GroupByColValCnt,
	}
}

func (tb *TimeBuckets) ToSerializedTimeBuckets() *SerializedTimeBuckets {
	if tb == nil {
		return nil
	}

	allRunningBuckets := make([]*SerializedRunningBucketResults, len(tb.AllRunningBuckets))

	for i, bucket := range tb.AllRunningBuckets {
		allRunningBuckets[i] = bucket.ToSerializedRunningBucketResults()
	}

	return &SerializedTimeBuckets{
		AllRunningBuckets: allRunningBuckets,
		UnsignedBucketIdx: tb.UnsignedBucketIdx,
	}
}

func (sgb *SerializedGroupByBuckets) ToGroupByBuckets(groupByBuckets *GroupByBuckets, qid uint64) *GroupByBuckets {
	if sgb == nil {
		return nil
	}

	if groupByBuckets == nil {
		groupByBuckets = &GroupByBuckets{}
	}

	allRunningBuckets := make([]*RunningBucketResults, len(sgb.AllRunningBuckets))

	for i, bucket := range sgb.AllRunningBuckets {
		allRunningBuckets[i] = bucket.ToRunningBucketResults(qid)
	}

	return &GroupByBuckets{
		AllRunningBuckets:   allRunningBuckets,
		StringBucketIdx:     sgb.StringBucketIdx,
		GroupByColValCnt:    sgb.GroupByColValCnt,
		allMeasureCols:      groupByBuckets.allMeasureCols,
		internalMeasureFns:  groupByBuckets.internalMeasureFns,
		reverseMeasureIndex: groupByBuckets.reverseMeasureIndex,
		maxBuckets:          groupByBuckets.maxBuckets,
	}
}

func (stb *SerializedTimeBuckets) ToTimeBuckets(qid uint64) *TimeBuckets {
	if stb == nil {
		return nil
	}

	allRunningBuckets := make([]*RunningBucketResults, len(stb.AllRunningBuckets))

	for i, bucket := range stb.AllRunningBuckets {
		allRunningBuckets[i] = bucket.ToRunningBucketResults(qid)
	}

	return &TimeBuckets{
		AllRunningBuckets: allRunningBuckets,
		UnsignedBucketIdx: stb.UnsignedBucketIdx,
	}
}
