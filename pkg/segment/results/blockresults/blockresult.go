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
			mCols, mFuns, revIndex := convertRequestToInternalStats(aggs.GroupByRequest)
			blockRes.GroupByAggregation = &GroupByBuckets{
				AllRunningBuckets:   make([]*RunningBucketResults, 0),
				StringBucketIdx:     make(map[string]int),
				allMeasureCols:      mCols,
				internalMeasureFns:  mFuns,
				reverseMeasureIndex: revIndex,
				maxBuckets:          aggs.GroupByRequest.BucketCount,
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
func convertRequestToInternalStats(req *structs.GroupByRequest) (map[string][]int, []*structs.MeasureAggregator, []int) {
	colToIdx := make(map[string][]int) // maps a column name to all indices in allConvertedMeasureOps it relates to
	allConvertedMeasureOps := make([]*structs.MeasureAggregator, 0)
	allReverseIndex := make([]int, 0)
	idx := 0
	for _, m := range req.MeasureOperations {
		var mFunc utils.AggregateFunctions
		switch m.MeasureFunc {
		case utils.Count:
			allReverseIndex = append(allReverseIndex, -1)
			continue
		case utils.Avg:
			mFunc = utils.Sum
		case utils.Range:
			// Record the index of range() in runningStats; the index is idx
			// To calculate the range(), we need both the min() and max(), which require two columns to store them
			// Since it is the runningStats not the stats for results, we can use one extra col to store the min/max
			// idx stores the result of min, and idx+1 stores the result of max.
			if _, ok := colToIdx[m.MeasureCol]; !ok {
				colToIdx[m.MeasureCol] = make([]int, 0)
			}
			allReverseIndex = append(allReverseIndex, idx)
			colToIdx[m.MeasureCol] = append(colToIdx[m.MeasureCol], idx)
			allConvertedMeasureOps = append(allConvertedMeasureOps, &structs.MeasureAggregator{
				MeasureCol:  m.MeasureCol,
				MeasureFunc: utils.Min,
			})
			idx++

			allReverseIndex = append(allReverseIndex, idx)
			colToIdx[m.MeasureCol] = append(colToIdx[m.MeasureCol], idx)
			allConvertedMeasureOps = append(allConvertedMeasureOps, &structs.MeasureAggregator{
				MeasureCol:  m.MeasureCol,
				MeasureFunc: utils.Max,
			})
			idx++
			continue
		default:
			mFunc = m.MeasureFunc
		}
		if _, ok := colToIdx[m.MeasureCol]; !ok {
			colToIdx[m.MeasureCol] = make([]int, 0)
		}
		allReverseIndex = append(allReverseIndex, idx)
		colToIdx[m.MeasureCol] = append(colToIdx[m.MeasureCol], idx)
		allConvertedMeasureOps = append(allConvertedMeasureOps, &structs.MeasureAggregator{
			MeasureCol:  m.MeasureCol,
			MeasureFunc: mFunc,
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
	if b.TimeAggregation != nil && blockRes.TimeAggregation != nil {
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

func (b *BlockResults) AddMeasureResultsToKey(currKey bytes.Buffer, measureResults []utils.CValueEnclosure, qid uint64) {
	if b.GroupByAggregation == nil {
		return
	}
	bKey := toputils.ByteSliceToString(currKey.Bytes())
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
	bucket.AddMeasureResults(measureResults, qid, 1)
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
	bucket.AddMeasureResults(measureResults, qid, cnt)
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
	return b.GroupByAggregation.ConvertToAggregationResult(b.aggs.GroupByRequest)
}

func (gb *GroupByBuckets) ConvertToAggregationResult(req *structs.GroupByRequest) *structs.AggregationResult {
	results := make([]*structs.BucketResult, len(gb.AllRunningBuckets))
	bucketNum := 0
	for key, idx := range gb.StringBucketIdx {
		bucket := gb.AllRunningBuckets[idx]
		currRes := make(map[string]utils.CValueEnclosure)
		for idx, mInfo := range req.MeasureOperations {
			mInfoStr := mInfo.String()
			switch mInfo.MeasureFunc {
			case utils.Count:
				currRes[mInfoStr] = utils.CValueEnclosure{CVal: bucket.count, Dtype: utils.SS_DT_UNSIGNED_NUM}
			case utils.Avg:
				sumIdx := gb.reverseMeasureIndex[idx]
				sumRawVal, err := bucket.runningStats[sumIdx].rawVal.GetFloatValue()
				if err != nil {
					currRes[mInfoStr] = utils.CValueEnclosure{CVal: nil, Dtype: utils.SS_INVALID}
					continue
				}
				var avg float64
				if bucket.count == 0 {
					avg = 0
				} else {
					avg = sumRawVal / float64(bucket.count)
				}
				currRes[mInfoStr] = utils.CValueEnclosure{CVal: avg, Dtype: utils.SS_DT_FLOAT}
			case utils.Range:
				minIdx := gb.reverseMeasureIndex[idx]
				minRawVal, err := bucket.runningStats[minIdx].rawVal.GetFloatValue()
				if err != nil {
					currRes[mInfoStr] = utils.CValueEnclosure{CVal: nil, Dtype: utils.SS_INVALID}
					continue
				}
				maxRawVal, err := bucket.runningStats[minIdx+1].rawVal.GetFloatValue()
				if err != nil {
					currRes[mInfoStr] = utils.CValueEnclosure{CVal: nil, Dtype: utils.SS_INVALID}
					continue
				}

				currRes[mInfoStr] = utils.CValueEnclosure{CVal: maxRawVal - minRawVal, Dtype: utils.SS_DT_FLOAT}
			case utils.Cardinality:
				valIdx := gb.reverseMeasureIndex[idx]
				finalVal := bucket.runningStats[valIdx].hll.Estimate()
				currRes[mInfoStr] = utils.CValueEnclosure{CVal: finalVal, Dtype: utils.SS_DT_UNSIGNED_NUM}
			case utils.Values:
				valIdx := gb.reverseMeasureIndex[idx]
				rawValStrArr, ok := bucket.runningStats[valIdx].rawVal.CVal.([]string)
				if !ok {
					currRes[mInfoStr] = utils.CValueEnclosure{CVal: nil, Dtype: utils.SS_INVALID}
					continue
				}

				uniqueSet := make(map[string]struct{})
				uniqueStrings := make([]string, 0)

				for _, str := range rawValStrArr {
					if _, exists := uniqueSet[str]; !exists {
						uniqueSet[str] = struct{}{}
						uniqueStrings = append(uniqueStrings, str)
					}
				}

				sort.Strings(uniqueStrings)

				strVal := strings.Join(uniqueStrings, "&nbsp")
				currRes[mInfoStr] = utils.CValueEnclosure{
					Dtype: utils.SS_DT_STRING,
					CVal:  strVal,
				}
			default:
				valIdx := gb.reverseMeasureIndex[idx]
				currRes[mInfoStr] = bucket.runningStats[valIdx].rawVal
			}
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
	return &structs.AggregationResult{
		IsDateHistogram: false,
		Results:         results,
	}
}

func (gb *GroupByBuckets) MergeBuckets(toMerge *GroupByBuckets) {
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
	mCols, mFuns, revIndex := convertRequestToInternalStats(req)
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
