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

package aggregations

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type scorePair struct {
	groupByColVal string
	score         float64
	index         int
}

type Range struct {
	start uint64
	end   uint64 // Exclusive
	step  uint64
}

func GenerateTimeRangeBuckets(timeHistogram *structs.TimeBucket) *Range {
	return &Range{
		start: timeHistogram.StartTime,
		end:   timeHistogram.EndTime,
		step:  timeHistogram.IntervalMillis,
	}
}

// TODO: delete this once we have confidence in the new implementation.
func oldGenerateTimeRangeBuckets(timeHistogram *structs.TimeBucket) []uint64 {
	numBuckets := (timeHistogram.EndTime-timeHistogram.StartTime)/timeHistogram.IntervalMillis + 1
	timeRangeBuckets := make([]uint64, 0, numBuckets)
	currentTime := timeHistogram.StartTime
	for currentTime < timeHistogram.EndTime {
		timeRangeBuckets = append(timeRangeBuckets, currentTime)
		nextTime := currentTime + timeHistogram.IntervalMillis
		if nextTime > timeHistogram.EndTime {
			break
		}

		currentTime = nextTime
	}

	return timeRangeBuckets
}

// Find correct time range bucket for timestamp
func FindTimeRangeBucket(r *Range, timestamp uint64) uint64 {
	if timestamp < r.start {
		return r.start
	}
	if timestamp >= r.end {
		return r.end - r.step
	}

	index := ((timestamp - r.start) / r.step)

	return r.start + index*r.step
}

// TODO: delete this once we have confidence in the new implementation.
func oldFindTimeRangeBucket(timePoints []uint64, timestamp uint64, intervalMillis uint64) uint64 {
	index := ((timestamp - timePoints[0]) / intervalMillis)
	if index >= uint64(len(timePoints)) {
		index = uint64(len(timePoints) - 1)
	}
	return timePoints[index]
}

func GetIntervalInMillis(num int, timeUnit sutils.TimeUnit) uint64 {
	numD := time.Duration(num)

	switch timeUnit {
	case sutils.TMMicrosecond:
		// Might not has effect for 'us', because smallest time unit for timestamp in siglens is ms
	case sutils.TMMillisecond:
		return uint64(numD)
	case sutils.TMCentisecond:
		return uint64(numD * 10 * time.Millisecond)
	case sutils.TMDecisecond:
		return uint64(numD * 100 * time.Millisecond)
	case sutils.TMSecond:
		return uint64((numD * time.Second).Milliseconds())
	case sutils.TMMinute:
		return uint64((numD * time.Minute).Milliseconds())
	case sutils.TMHour:
		return uint64((numD * time.Hour).Milliseconds())
	case sutils.TMDay:
		return uint64((numD * 24 * time.Hour).Milliseconds())
	case sutils.TMWeek:
		return uint64((numD * 7 * 24 * time.Hour).Milliseconds())
	case sutils.TMMonth:
		return uint64((numD * 30 * 24 * time.Hour).Milliseconds())
	case sutils.TMQuarter:
		return uint64((numD * 120 * 24 * time.Hour).Milliseconds())
	default:
		log.Errorf("GetIntervalInMillis: unexpected time unit: %v", timeUnit)
	}
	return uint64((10 * time.Minute).Milliseconds()) // 10 Minutes
}

func InitTimeBucket(num int, timeUnit sutils.TimeUnit, byField string, limitExpr *structs.LimitExpr, measureAggLength int, bOptions *structs.BinOptions) *structs.TimeBucket {

	intervalMillis := GetIntervalInMillis(num, timeUnit)

	timechartExpr := &structs.TimechartExpr{
		ByField:    byField,
		BinOptions: bOptions,
	}

	if len(byField) > 0 {
		if limitExpr != nil {
			timechartExpr.LimitExpr = limitExpr
		} else {
			timechartExpr.LimitExpr = &structs.LimitExpr{
				IsTop:          true,
				Num:            10,
				LimitScoreMode: structs.LSMBySum,
			}
			if measureAggLength > 1 {
				timechartExpr.LimitExpr.LimitScoreMode = structs.LSMByFreq
			}
		}
	}

	timeBucket := &structs.TimeBucket{
		IntervalMillis: intervalMillis,
		Timechart:      timechartExpr,
	}

	return timeBucket
}

func AddAggCountToTimechartRunningStats(m *structs.MeasureAggregator, allConvertedMeasureOps *[]*structs.MeasureAggregator, allReverseIndex *[]int, colToIdx map[string][]int, idx int) {
	*allReverseIndex = append(*allReverseIndex, idx)
	colToIdx[m.MeasureCol] = append(colToIdx[m.MeasureCol], idx)
	*allConvertedMeasureOps = append(*allConvertedMeasureOps, &structs.MeasureAggregator{
		MeasureCol:  m.MeasureCol,
		MeasureFunc: sutils.Count,
		StrEnc:      m.StrEnc,
	})
}

func AddAggAvgToTimechartRunningStats(m *structs.MeasureAggregator, allConvertedMeasureOps *[]*structs.MeasureAggregator, allReverseIndex *[]int, colToIdx map[string][]int, idx int) {
	*allReverseIndex = append(*allReverseIndex, idx)
	colToIdx[m.MeasureCol] = append(colToIdx[m.MeasureCol], idx)
	*allConvertedMeasureOps = append(*allConvertedMeasureOps, &structs.MeasureAggregator{
		MeasureCol:  m.MeasureCol,
		MeasureFunc: sutils.Sum,
		StrEnc:      m.StrEnc,
	})
	idx++
	*allReverseIndex = append(*allReverseIndex, idx)
	colToIdx[m.MeasureCol] = append(colToIdx[m.MeasureCol], idx)
	*allConvertedMeasureOps = append(*allConvertedMeasureOps, &structs.MeasureAggregator{
		MeasureCol:  m.MeasureCol,
		MeasureFunc: sutils.Count,
		StrEnc:      m.StrEnc,
	})
}

// Timechart will only display N highest/lowest scoring distinct values of the split-by field
// For Single agg, the score is based on the sum of the values in the aggregation. Therefore, we can only know groupByColVal's ranking after processing all the runningStats
// For multiple aggs, the score is based on the freq of the field. Which means we can rank groupByColVal at this time.
func CheckGroupByColValsAgainstLimit(timechart *structs.TimechartExpr, groupByColValCnt map[string]int, groupValScoreMap map[string]*sutils.CValueEnclosure,
	measureOperations []*structs.MeasureAggregator, batchErr *utils.BatchError) map[string]bool {

	if timechart == nil || timechart.LimitExpr == nil {
		return nil
	}

	// When there is only one agg and agg is values(), we can not score that based on the sum of the values in the aggregation
	onlyUseByValuesFunc := false
	if len(measureOperations) == 1 && measureOperations[0].MeasureFunc == sutils.Values {
		onlyUseByValuesFunc = true
	}

	index := 0
	valIsInLimit := make(map[string]bool)
	isRankBySum := IsRankBySum(timechart)

	// When there is only one aggregator and aggregator is values(), we can not score that based on the sum of the values in the aggregation
	if isRankBySum && !onlyUseByValuesFunc {
		scorePairs := make([]scorePair, 0)
		// []float64, 0: score; 1: index
		for groupByColVal, cVal := range groupValScoreMap {
			valIsInLimit[groupByColVal] = false
			score, err := cVal.GetFloatValue()
			if err != nil {
				batchErr.AddError("CheckGroupByColValsAgainstLimit:score", fmt.Errorf("%v does not have a score", groupByColVal))
				continue
			}
			scorePairs = append(scorePairs, scorePair{
				groupByColVal: groupByColVal,
				score:         score,
				index:         index,
			})
			index++
		}

		if timechart.LimitExpr.IsTop {
			sort.Slice(scorePairs, func(i, j int) bool {
				return scorePairs[i].score > scorePairs[j].score
			})
		} else {
			sort.Slice(scorePairs, func(i, j int) bool {
				return scorePairs[i].score < scorePairs[j].score
			})
		}

		limit := timechart.LimitExpr.Num
		if limit > len(scorePairs) {
			limit = len(scorePairs)
		}

		for i := 0; i < limit; i++ {
			valIsInLimit[scorePairs[i].groupByColVal] = true
		}

	} else { // rank by freq
		// []int, 0: cnt; 1: index
		cnts := make([][]int, 0)
		vals := make([]string, 0)

		for groupByColVal, cnt := range groupByColValCnt {
			vals = append(vals, groupByColVal)
			cnts = append(cnts, []int{cnt, index})
			valIsInLimit[groupByColVal] = false
			index++
		}

		if timechart.LimitExpr.IsTop {
			sort.Slice(cnts, func(i, j int) bool {
				return cnts[i][0] > cnts[j][0]
			})
		} else {
			sort.Slice(cnts, func(i, j int) bool {
				return cnts[i][0] < cnts[j][0]
			})
		}

		limit := timechart.LimitExpr.Num
		if limit > len(vals) {
			limit = len(vals)
		}

		for i := 0; i < limit; i++ {
			valIndex := cnts[i][1]
			valIsInLimit[vals[valIndex]] = true
		}
	}

	return valIsInLimit
}

// Initial score map for single agg: the score is based on the sum of the values in the aggregation
func InitialScoreMap(timechart *structs.TimechartExpr, groupByColValCnt map[string]int) map[string]*sutils.CValueEnclosure {

	if timechart == nil || timechart.LimitExpr == nil || timechart.LimitExpr.LimitScoreMode == structs.LSMByFreq {
		return nil
	}

	groupByColValScoreMap := make(map[string]*sutils.CValueEnclosure, 0)
	for groupByColVal := range groupByColValCnt {
		groupByColValScoreMap[groupByColVal] = &sutils.CValueEnclosure{CVal: nil, Dtype: sutils.SS_INVALID}
	}

	return groupByColValScoreMap
}

func SortTimechartRes(timechart *structs.TimechartExpr, results *[]*structs.BucketResult) {
	if timechart == nil || results == nil {
		return
	}

	sort.Slice(*results, func(i, j int) bool {
		timestamp1, err := extractTimestamp((*results)[i].BucketKey)
		if err != nil {
			log.Errorf("SortTimechartRes: bucketKey is invalid for index %d: %v", i, err)
			return false
		}
		timestamp2, err := extractTimestamp((*results)[j].BucketKey)
		if err != nil {
			log.Errorf("SortTimechartRes: bucketKey is invalid for index %d: %v", j, err)
			return true
		}
		return timestamp1 < timestamp2
	})
}

func extractTimestamp(bucketKey interface{}) (uint64, error) {
	if bucketKey == nil {
		return 0, errors.New("bucketKey is nil")
	}

	// Check if bucketKey is a slice and extract the first element
	if bucketKeySlice, ok := bucketKey.([]interface{}); ok {
		if len(bucketKeySlice) > 0 {
			bucketKey = bucketKeySlice[0]
		} else {
			return 0, errors.New("bucketKey slice is empty")
		}
	}

	// Attempt to assert bucketKey as uint64
	if timestamp, ok := bucketKey.(uint64); ok {
		return timestamp, nil
	}

	// Attempt to assert bucketKey as string and parse it
	if bucketKeyStr, ok := bucketKey.(string); ok {
		timestamp, err := strconv.ParseUint(bucketKeyStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert bucketKey to timestamp: %v", err)
		}
		return timestamp, nil
	}

	return 0, errors.New("bucketKey is not a string or uint64")
}

func IsOtherCol(valIsInLimit map[string]bool, groupByColVal string) bool {
	isOtherCol := false
	if valIsInLimit != nil {
		inLimit, exists := valIsInLimit[groupByColVal]
		if exists {
			isOtherCol = !inLimit
		}
	}
	return isOtherCol
}

// For numeric agg(not include dc), we can simply use addition to merge them
// For string values, it depends on the aggregation function
func MergeVal(eVal *sutils.CValueEnclosure, eValToMerge sutils.CValueEnclosure, hll *utils.GobbableHll, hllToMerge *utils.GobbableHll,
	strSet map[string]struct{}, strSetToMerge map[string]struct{}, aggFunc sutils.AggregateFunctions, useAdditionForMerge bool, batchErr *utils.BatchError) {

	switch aggFunc {
	case sutils.Count:
		fallthrough
	case sutils.Avg:
		fallthrough
	case sutils.Min:
		fallthrough
	case sutils.Max:
		fallthrough
	case sutils.Range:
		fallthrough
	case sutils.Sum:
		aggFunc = sutils.Sum
	case sutils.Cardinality:
		if useAdditionForMerge {
			aggFunc = sutils.Sum
		} else {
			err := hll.StrictUnion(hllToMerge.Hll)
			if err != nil {
				batchErr.AddError("MergeVal:HLL_STATS", err)
			}
			eVal.CVal = hll.Cardinality()
			eVal.Dtype = sutils.SS_DT_UNSIGNED_NUM
			return
		}
	case sutils.Values:
		// Can not do addition for values func
		if useAdditionForMerge {
			return
		}
		for str := range strSetToMerge {
			strSet[str] = struct{}{}
		}
		uniqueStrings := make([]string, 0)
		for str := range strSet {
			uniqueStrings = append(uniqueStrings, str)
		}
		sort.Strings(uniqueStrings)

		eVal.CVal = uniqueStrings
		eVal.Dtype = sutils.SS_DT_STRING_SLICE
		return
	default:
		log.Errorf("MergeVal: unsupported aggregation function: %v", aggFunc)
	}

	tmp := sutils.CValueEnclosure{
		Dtype: eVal.Dtype,
		CVal:  eVal.CVal,
	}
	retVal, err := sutils.Reduce(eValToMerge, tmp, aggFunc)
	if err != nil {
		batchErr.AddError("MergeVal:eVAL_INTO_cVAL", err)
		return
	}
	eVal.CVal = retVal.CVal
	eVal.Dtype = retVal.Dtype
}

func MergeMap(groupByColValCnt map[string]int, toMerge map[string]int) {

	for key, cnt := range groupByColValCnt {
		cntToMerge, exists := toMerge[key]
		if exists {
			groupByColValCnt[key] = cnt + cntToMerge
		}
	}

	for key, cnt := range toMerge {
		_, exists := groupByColValCnt[key]
		if !exists {
			groupByColValCnt[key] = cnt
		}
	}
}

func IsRankBySum(timechart *structs.TimechartExpr) bool {
	if timechart != nil && timechart.LimitExpr != nil && timechart.LimitExpr.LimitScoreMode == structs.LSMBySum {
		return true
	}
	return false
}

func ShouldAddRes(timechart *structs.TimechartExpr, tmLimitResult *structs.TMLimitResult, index int, eVal sutils.CValueEnclosure,
	hllToMerge *utils.GobbableHll, strSetToMerge map[string]struct{}, aggFunc sutils.AggregateFunctions, groupByColVal string,
	isOtherCol bool, batchErr *utils.BatchError) bool {

	useAdditionForMerge := (tmLimitResult.OtherCValArr == nil)
	isRankBySum := IsRankBySum(timechart)

	// If true, current col's val will be added into 'other' col. So its val should not be added into res at this time
	if isOtherCol {
		otherCVal := tmLimitResult.OtherCValArr[index]
		MergeVal(otherCVal, eVal, tmLimitResult.Hll, hllToMerge, tmLimitResult.StrSet, strSetToMerge, aggFunc, useAdditionForMerge, batchErr)
		return false
	} else {
		if isRankBySum && tmLimitResult.OtherCValArr == nil {
			scoreVal, ok := tmLimitResult.GroupValScoreMap[groupByColVal]
			if ok && scoreVal != nil {
				MergeVal(scoreVal, eVal, tmLimitResult.Hll, hllToMerge, tmLimitResult.StrSet, strSetToMerge, aggFunc, useAdditionForMerge, batchErr)
			}
			return false
		}
		return true
	}
}
