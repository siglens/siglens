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

package blockresults

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"sort"
	"strings"

	"github.com/axiomhq/hyperloglog"
	"github.com/siglens/siglens/pkg/segment/aggregations"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
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

type TimeBuckets struct {
	AllRunningBuckets []*RunningBucketResults
	UnsignedBucketIdx map[uint64]int
}

type BlockResults struct {
	SortedResults      *SortResults
	UnsortedResults    []*utils.RecordResultContainer
	TimeAggregation    *TimeBuckets
	GroupByAggregation *GroupByBuckets
	aggs               *structs.QueryAggregators

	MatchedCount uint64

	nextUnsortedIdx uint64 // next index to put result in
	sortResults     bool
	sizeLimit       uint64
}

// json exportable and mergeable results for query
type GroupByBucketsJSON struct {
	AllGroupbyBuckets map[string]*RunningBucketResultsJSON `json:"allGroupbyBuckets"`
}

type TimeBucketsJSON struct {
	AllTimeBuckets map[uint64]*RunningBucketResultsJSON `json:"allTimeBuckets"`
}

type RunningBucketResultsJSON struct {
	RunningStats []interface{}                `json:"runningStats"`
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
			log.Errorf("qid=%d, Initialize block results failed: %v", qid, err)
			return nil, err
		}
		blockRes.sortResults = true
		blockRes.SortedResults = sortedRes
	} else {
		blockRes.sortResults = false
		blockRes.UnsortedResults = make([]*utils.RecordResultContainer, count)
		blockRes.nextUnsortedIdx = 0
	}
	blockRes.sizeLimit = count
	blockRes.MatchedCount = 0
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
		var mFunc utils.AggregateFunctions
		var overrodeMeasureAgg *structs.MeasureAggregator
		switch m.MeasureFunc {
		case utils.Sum:
			fallthrough
		case utils.Max:
			fallthrough
		case utils.Min:
			if m.ValueColRequest != nil {
				fields := m.ValueColRequest.GetFields()
				if len(fields) != 1 {
					log.Errorf("convertRequestToInternalStats: Incorrect number of fields for aggCol: %v", m.String())
					continue
				}
				measureColStr = fields[0]
			}
			mFunc = m.MeasureFunc
		case utils.Range:
			curId, err := aggregations.AddMeasureAggInRunningStatsForRange(m, &allConvertedMeasureOps, &allReverseIndex, colToIdx, idx)
			if err != nil {
				log.Errorf("convertRequestToInternalStats: %v", err)
			}
			idx = curId
			continue
		case utils.Count:
			if m.ValueColRequest != nil {
				curId, err := aggregations.AddMeasureAggInRunningStatsForCount(m, &allConvertedMeasureOps, &allReverseIndex, colToIdx, idx)
				if err != nil {
					log.Errorf("convertRequestToInternalStats: %v", err)
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
		case utils.Avg:
			if m.ValueColRequest != nil {
				curId, err := aggregations.AddMeasureAggInRunningStatsForAvg(m, &allConvertedMeasureOps, &allReverseIndex, colToIdx, idx)
				if err != nil {
					log.Errorf("convertRequestToInternalStats: %v", err)
				}
				idx = curId
				continue
			} else {
				if usedByTimechart {
					aggregations.AddAggAvgToTimechartRunningStats(m, &allConvertedMeasureOps, &allReverseIndex, colToIdx, idx)
					idx += 2
					continue
				}
				mFunc = utils.Sum
				overrodeMeasureAgg = m
			}
		case utils.Cardinality:
			fallthrough
		case utils.Values:
			if m.ValueColRequest != nil {
				curId, err := aggregations.AddMeasureAggInRunningStatsForValuesOrCardinality(m, &allConvertedMeasureOps, &allReverseIndex, colToIdx, idx)
				if err != nil {
					log.Errorf("convertRequestToInternalStats: %v", err)
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
func (b *BlockResults) Add(rrc *utils.RecordResultContainer) (bool, string) {
	if b.sortResults {
		return b.SortedResults.Add(rrc)
	}

	if b.nextUnsortedIdx < b.sizeLimit {
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
func (b *BlockResults) GetResults() []*utils.RecordResultContainer {
	if b.sortResults {
		return b.SortedResults.GetSortedResults()
	} else {
		return b.UnsortedResults[:b.nextUnsortedIdx]
	}
}

// if sort is enabled, will call heap.Pop on the underlying results
func (b *BlockResults) GetResultsCopy() []*utils.RecordResultContainer {
	if b.sortResults {
		return b.SortedResults.GetSortedResultsCopy()
	} else {
		res := make([]*utils.RecordResultContainer, b.nextUnsortedIdx)
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

// return true if:
// 1.   if block not fuly enclosed
// 2  if time-HT but we did not use the rollup info to add time-HT
// 3.   if sort present and low/high ts can be added
// 4.   if rrcs left to be filled
func (b *BlockResults) ShouldIterateRecords(aggsHasTimeHt bool, isBlkFullyEncosed bool,
	lowTs uint64, highTs uint64, addedTimeHt bool) bool {

	// case 1
	if !isBlkFullyEncosed {
		return true
	}

	if aggsHasTimeHt && !addedTimeHt {
		return true // case 2
	}

	// case 3
	if b.aggs != nil && b.aggs.Sort != nil {
		if b.aggs.Sort.Ascending {
			return b.WillValueBeAdded(float64(lowTs))
		} else {
			return b.WillValueBeAdded(float64(highTs))
		}
	}

	// case 4
	return b.nextUnsortedIdx < b.sizeLimit

}

func (b *BlockResults) AddMeasureResultsToKey(currKey bytes.Buffer, measureResults []utils.CValueEnclosure, groupByColVal string, usedByTimechart bool, qid uint64) {

	if b.GroupByAggregation == nil {
		return
	}
	bKey := toputils.UnsafeByteSliceToString(currKey.Bytes())
	bucketIdx, ok := b.GroupByAggregation.StringBucketIdx[bKey]

	var bucket *RunningBucketResults
	if !ok {
		nBuckets := len(b.GroupByAggregation.AllRunningBuckets)
		if nBuckets >= b.GroupByAggregation.maxBuckets {
			return
		}
		bucket = initRunningGroupByBucket(b.GroupByAggregation.internalMeasureFns)
		b.GroupByAggregation.AllRunningBuckets = append(b.GroupByAggregation.AllRunningBuckets, bucket)
		b.GroupByAggregation.StringBucketIdx[bKey] = nBuckets
	} else {
		bucket = b.GroupByAggregation.AllRunningBuckets[bucketIdx]
	}

	if usedByTimechart {
		var gRunningStats []runningStats
		_, exists := bucket.groupedRunningStats[groupByColVal]
		if !exists {
			gRunningStats = initRunningStats(b.GroupByAggregation.internalMeasureFns)
			bucket.groupedRunningStats[groupByColVal] = gRunningStats
		}
		gRunningStats = bucket.groupedRunningStats[groupByColVal]
		bucket.AddMeasureResults(&gRunningStats, measureResults, qid, 1, true)
	} else {
		bucket.AddMeasureResults(&bucket.runningStats, measureResults, qid, 1, false)
	}

}

func (b *BlockResults) AddMeasureResultsToKeyAgileTree(bKey string,
	measureResults []utils.CValueEnclosure, qid uint64, cnt uint64) {
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
		bucket = initRunningGroupByBucket(b.GroupByAggregation.internalMeasureFns)
		b.GroupByAggregation.AllRunningBuckets = append(b.GroupByAggregation.AllRunningBuckets, bucket)
		b.GroupByAggregation.StringBucketIdx[bKey] = nBuckets
	} else {
		bucket = b.GroupByAggregation.AllRunningBuckets[bucketIdx]
	}
	bucket.AddMeasureResults(&bucket.runningStats, measureResults, qid, cnt, false)
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
	return b.GroupByAggregation.ConvertToAggregationResult(b.aggs.GroupByRequest, timechart)
}

// If the current GroupByBuckets are used by timechart, and timechart has a limit option, there are two different methods to add results at this point. This is because the limit option has two different ways of calculating scores, and we only return the top or bottom N results, with the remaining ones merged and placed into the 'other' col
// 1. Single Agg: The score is based on the sum of the values in the aggregation. It requires two iterations. In the first iteration, sum up scores for each groupVal
// 2. Multiple Aggs: The score is based on the frequency of each value of <field>. It only requires one iteration because we already have the frep for groupVal before the iteration begins
func (gb *GroupByBuckets) ConvertToAggregationResult(req *structs.GroupByRequest, timechart *structs.TimechartExpr) *structs.AggregationResult {

	tmLimitResult := &structs.TMLimitResult{
		GroupValScoreMap: aggregations.InitialScoreMap(timechart, gb.GroupByColValCnt),
	}
	isRankBySum := aggregations.IsRankBySum(timechart)

	// Get scores for ranking
	if isRankBySum {
		for _, idx := range gb.StringBucketIdx {
			bucket := gb.AllRunningBuckets[idx]
			currRes := make(map[string]utils.CValueEnclosure)
			// Add results for group by cols inside the time range bucket
			if len(bucket.groupedRunningStats) > 0 {
				for groupByColVal, gRunningStats := range bucket.groupedRunningStats {
					gb.AddResultToStatRes(req, bucket, gRunningStats, currRes, groupByColVal, timechart, tmLimitResult)
				}
			}
		}
	}

	bucketNum := 0
	results := make([]*structs.BucketResult, len(gb.AllRunningBuckets))
	tmLimitResult.Hll = hyperloglog.New14()
	tmLimitResult.StrSet = make(map[string]struct{}, 0)
	tmLimitResult.ValIsInLimit = aggregations.CheckGroupByColValsAgainstLimit(timechart, gb.GroupByColValCnt, tmLimitResult.GroupValScoreMap, req.MeasureOperations)
	for key, idx := range gb.StringBucketIdx {
		bucket := gb.AllRunningBuckets[idx]
		currRes := make(map[string]utils.CValueEnclosure)

		// Add results for group by cols inside the time range bucket
		if len(bucket.groupedRunningStats) > 0 {
			// Every measure operator needs to check whether the current groupByColVal is within the limit
			// If it's not, its col name should be displayed as [aggOp: otherstr]
			otherCValArr := make([]*utils.CValueEnclosure, len(req.MeasureOperations))
			for i := 0; i < len(req.MeasureOperations); i++ {
				otherCValArr[i] = &utils.CValueEnclosure{CVal: nil, Dtype: utils.SS_INVALID}
			}

			tmLimitResult.OtherCValArr = otherCValArr
			for groupByColVal, gRunningStats := range bucket.groupedRunningStats {
				gb.AddResultToStatRes(req, bucket, gRunningStats, currRes, groupByColVal, timechart, tmLimitResult)
			}

			if timechart.LimitExpr != nil && timechart.LimitExpr.Num < len(bucket.groupedRunningStats) {
				for index, mInfo := range req.MeasureOperations {
					// To be modified: user can customize otherstr
					mInfoStr := mInfo.String() + ": other"
					currRes[mInfoStr] = *tmLimitResult.OtherCValArr[index]
				}
			}

		} else {
			gb.AddResultToStatRes(req, bucket, bucket.runningStats, currRes, "", nil, tmLimitResult)
		}

		var bucketKey interface{}
		bucketKey, err := utils.ConvertGroupByKey([]byte(key))
		if len(bucketKey.([]string)) == 1 {
			bucketKey = bucketKey.([]string)[0]
		}
		if err != nil {
			log.Errorf("ConvertToAggregationResult: failed to extract raw key: %v", err)
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

func (gb *GroupByBuckets) AddResultToStatRes(req *structs.GroupByRequest, bucket *RunningBucketResults, runningStats []runningStats, currRes map[string]utils.CValueEnclosure,
	groupByColVal string, timechart *structs.TimechartExpr, tmLimitResult *structs.TMLimitResult) {
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

		var hllToMerge *hyperloglog.Sketch
		var strSetToMerge map[string]struct{}
		var eVal utils.CValueEnclosure
		switch mInfo.MeasureFunc {
		case utils.Count:
			if mInfo.ValueColRequest != nil || usedByTimechart {
				if !usedByTimechart && len(mInfo.ValueColRequest.GetFields()) == 0 {
					log.Errorf("AddResultToStatRes: Incorrect number of fields for aggCol: %v", mInfoStr)
					continue
				}

				countIdx := gb.reverseMeasureIndex[idx]
				countVal, err := runningStats[countIdx].rawVal.GetUIntValue()
				if err != nil {
					currRes[mInfoStr] = utils.CValueEnclosure{CVal: nil, Dtype: utils.SS_INVALID}
					continue
				}
				eVal = utils.CValueEnclosure{CVal: countVal, Dtype: utils.SS_DT_UNSIGNED_NUM}
			} else {
				eVal = utils.CValueEnclosure{CVal: bucket.count, Dtype: utils.SS_DT_UNSIGNED_NUM}
			}
			idx++
		case utils.Avg:
			sumIdx := gb.reverseMeasureIndex[idx]
			sumRawVal, err := runningStats[sumIdx].rawVal.GetFloatValue()
			if err != nil {
				currRes[mInfoStr] = utils.CValueEnclosure{CVal: nil, Dtype: utils.SS_INVALID}
				continue
			}

			var avg float64
			if mInfo.ValueColRequest != nil || usedByTimechart {
				countIdx := gb.reverseMeasureIndex[idx+1]
				countRawVal, err := runningStats[countIdx].rawVal.GetFloatValue()
				if err != nil {
					currRes[mInfoStr] = utils.CValueEnclosure{CVal: nil, Dtype: utils.SS_INVALID}
					continue
				}
				eVal = utils.CValueEnclosure{CVal: sumRawVal / countRawVal, Dtype: utils.SS_DT_FLOAT}
				idx += 2
			} else {
				if bucket.count == 0 {
					avg = 0
				} else {
					avg = sumRawVal / float64(bucket.count)
				}
				eVal = utils.CValueEnclosure{CVal: avg, Dtype: utils.SS_DT_FLOAT}
				idx++
			}
		case utils.Range:
			minIdx := gb.reverseMeasureIndex[idx]
			minRawVal, err := runningStats[minIdx].rawVal.GetFloatValue()
			if err != nil {
				currRes[mInfoStr] = utils.CValueEnclosure{CVal: nil, Dtype: utils.SS_INVALID}
				continue
			}

			maxIdx := gb.reverseMeasureIndex[idx+1]
			maxRawVal, err := runningStats[maxIdx].rawVal.GetFloatValue()
			if err != nil {
				currRes[mInfoStr] = utils.CValueEnclosure{CVal: nil, Dtype: utils.SS_INVALID}
				continue
			}

			eVal = utils.CValueEnclosure{CVal: maxRawVal - minRawVal, Dtype: utils.SS_DT_FLOAT}
			idx += 2
		case utils.Cardinality:
			valIdx := gb.reverseMeasureIndex[idx]
			if mInfo.ValueColRequest != nil {
				if len(mInfo.ValueColRequest.GetFields()) == 0 {
					log.Errorf("AddResultToStatRes: Incorrect number of fields for aggCol: %v", mInfoStr)
					continue
				}
				strSet, ok := runningStats[valIdx].rawVal.CVal.(map[string]struct{})
				if !ok {
					currRes[mInfoStr] = utils.CValueEnclosure{CVal: nil, Dtype: utils.SS_INVALID}
					continue
				}
				eVal = utils.CValueEnclosure{CVal: uint64(len(strSet)), Dtype: utils.SS_DT_UNSIGNED_NUM}
			} else {
				finalVal := runningStats[valIdx].hll.Estimate()
				eVal = utils.CValueEnclosure{CVal: finalVal, Dtype: utils.SS_DT_UNSIGNED_NUM}
				hllToMerge = runningStats[valIdx].hll
			}

			idx++
		case utils.Values:
			if mInfo.ValueColRequest != nil {
				if len(mInfo.ValueColRequest.GetFields()) == 0 {
					log.Errorf("AddResultToStatRes: Incorrect number of fields for aggCol: %v", mInfoStr)
					continue
				}
			}

			valIdx := gb.reverseMeasureIndex[idx]
			strSet, ok := runningStats[valIdx].rawVal.CVal.(map[string]struct{})
			if !ok {
				currRes[mInfoStr] = utils.CValueEnclosure{CVal: nil, Dtype: utils.SS_INVALID}
				continue
			}
			strSetToMerge = strSet

			uniqueStrings := make([]string, 0)
			for str := range strSet {
				uniqueStrings = append(uniqueStrings, str)
			}

			sort.Strings(uniqueStrings)

			strVal := strings.Join(uniqueStrings, "&nbsp")
			eVal = utils.CValueEnclosure{
				Dtype: utils.SS_DT_STRING,
				CVal:  strVal,
			}

			idx++
		default:
			valIdx := gb.reverseMeasureIndex[idx]
			eVal = runningStats[valIdx].rawVal
			idx++
		}
		shouldAddRes := aggregations.ShouldAddRes(timechart, tmLimitResult, index, eVal, hllToMerge, strSetToMerge, mInfo.MeasureFunc, groupByColVal, isOtherCol)
		if shouldAddRes {
			currRes[mInfoStr] = eVal
		}
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
		retVals := make([]interface{}, 0, len(bucket.currStats))
		for idx, rs := range bucket.runningStats {
			if bucket.currStats[idx].MeasureFunc == utils.Cardinality {
				encoded, err := rs.hll.MarshalBinary()
				if err != nil {
					log.Errorf("GroupByBuckets.ConvertToJson: failed to marshal hll: %v", err)
					return nil, err
				}
				retVals = append(retVals, encoded)
			} else {
				retVals = append(retVals, rs.rawVal.CVal)
			}
		}
		newBucket.RunningStats = retVals
		retVal.AllGroupbyBuckets[key] = newBucket
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
		retVals := make([]interface{}, 0, len(bucket.currStats))
		for idx, rs := range bucket.runningStats {
			if bucket.currStats[idx].MeasureFunc == utils.Cardinality {
				encoded, err := rs.hll.MarshalBinary()
				if err != nil {
					log.Errorf("TimeBuckets.ConvertToJson: failed to marshal hll: %v", err)
					return nil, err
				}
				retVals = append(retVals, encoded)
			} else {
				retVals = append(retVals, rs.rawVal.CVal)
			}
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
	for key, runningBucket := range gb.AllGroupbyBuckets {
		newBucket, err := runningBucket.Convert()
		if err != nil {
			return nil, err
		}
		retVal.AllRunningBuckets = append(retVal.AllRunningBuckets, newBucket)
		retVal.StringBucketIdx[key] = reverseIndex
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
	for statsIdx, rs := range rb.RunningStats {
		if rb.CurrStats[statsIdx].MeasureFunc == utils.Cardinality {
			hll := hyperloglog.New()
			hllString, ok := rs.(string)
			if !ok {
				log.Errorf("RunningBucketResultsJSON.Convert: failed to convert hll to byte array %+v %T", rs, rs)
				return nil, fmt.Errorf("failed to convert hll to byte array")
			}
			hllBytes, err := base64.StdEncoding.DecodeString(hllString)
			if err != nil {
				log.Errorf("RunningBucketResultsJSON.Convert: failed to decode hll: %v", err)
				return nil, err
			}
			err = hll.UnmarshalBinary(hllBytes)
			if err != nil {
				log.Errorf("RunningBucketResultsJSON.Convert: failed to unmarshal hll: %v", err)
				return nil, err
			}
			currRunningStats = append(currRunningStats, runningStats{hll: hll})
		} else {
			newVal := utils.CValueEnclosure{}
			err := newVal.ConvertValue(rs)
			if err != nil {
				log.Errorf("RunningBucketResultsJSON.Convert: failed to convert value: %v", err)
				return nil, err
			}
			currRunningStats = append(currRunningStats, runningStats{rawVal: newVal})
		}
	}
	newBucket.runningStats = currRunningStats
	return newBucket, nil
}
