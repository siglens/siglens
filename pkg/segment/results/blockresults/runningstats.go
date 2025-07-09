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
	"fmt"
	"math"

	"github.com/cespare/xxhash"
	agg "github.com/siglens/siglens/pkg/segment/aggregations"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var ErrConvertToNumber = fmt.Errorf("failed to convert CVal to Number")
var ErrReduceNumbers = fmt.Errorf("failed to reduce Numbers")
var ErrReduceCVal = fmt.Errorf("failed to reduce CVals")

type RunningBucketResults struct {
	runningStats        []runningStats               // maps a stat name to running stats
	currStats           []*structs.MeasureAggregator // measure aggregators in result
	groupedRunningStats map[string][]runningStats    // maps timechart group by col's vals to corresponding running stats
	count               uint64                       // total number of elements belonging to the bucket
	qid                 uint64                       // query id
}

type SerializedRunningBucketResults struct {
	RunningStats        []SerializedRunningStats
	CurrStats           []*structs.MeasureAggregator
	GroupedRunningStats map[string][]SerializedRunningStats
	Count               uint64
}

type runningStats struct {
	rawVal sutils.CValueEnclosure // raw value
	number *sutils.Number         // If this is not nil, it's always correct
	dirty  bool                   // If true, "number" is correct but "rawVal" is not

	hll       *utils.GobbableHll
	rangeStat *structs.RangeStat
	avgStat   *structs.AvgStat
	tDigest   *utils.GobbableTDigest
}

func (rs *runningStats) syncRawValue() {
	if !rs.dirty || rs.number == nil {
		return
	}

	rs.dirty = false

	err := rs.number.ToCVal(&rs.rawVal)
	if err != nil {
		log.Debug(ErrConvertToNumber)
		return
	}
}

type RunningStatsJSON struct {
	RawVal    interface{}         `json:"rawVal"`
	Hll       []byte              `json:"hll"`
	RangeStat *structs.RangeStat  `json:"rangeStat"`
	AvgStat   *structs.AvgStat    `json:"avgStat"`
	StrSet    map[string]struct{} `json:"strSet"`
	StrList   []string            `json:"strList"`
}

type SerializedRunningStats struct {
	RawVal    sutils.CValueEnclosure
	Hll       *utils.GobbableHll
	RangeStat *structs.RangeStat
	AvgStat   *structs.AvgStat
}

func initRunningStats(internalMeasureFns []*structs.MeasureAggregator) []runningStats {
	retVal := make([]runningStats, len(internalMeasureFns))
	for i := 0; i < len(internalMeasureFns); i++ {
		if internalMeasureFns[i].MeasureFunc == sutils.Cardinality || internalMeasureFns[i].MeasureFunc == sutils.EstdcError {
			retVal[i] = runningStats{hll: structs.CreateNewHll()}
		} else if internalMeasureFns[i].MeasureFunc == sutils.Avg {
			retVal[i] = runningStats{avgStat: &structs.AvgStat{}}
		} else if internalMeasureFns[i].MeasureFunc == sutils.Range {
			retVal[i] = runningStats{rangeStat: agg.InitRangeStat()}
		} else if internalMeasureFns[i].MeasureFunc == sutils.Perc {
			t, err := utils.CreateNewTDigest()
			if err == nil {
				retVal[i] = runningStats{tDigest: t}
			}
		}
	}
	return retVal
}

func initRunningGroupByBucket(internalMeasureFns []*structs.MeasureAggregator, qid uint64) *RunningBucketResults {

	return &RunningBucketResults{
		count:               0,
		runningStats:        initRunningStats(internalMeasureFns),
		currStats:           internalMeasureFns,
		groupedRunningStats: make(map[string][]runningStats),
		qid:                 qid,
	}
}

func initRunningTimeBucket() *RunningBucketResults {

	return &RunningBucketResults{
		count: 0,
	}
}

func (rr *RunningBucketResults) AddTimeToBucketStats(count uint16) {
	rr.count += uint64(count)
}

func (rr *RunningBucketResults) AddMeasureResults(runningStats *[]runningStats, measureResults []sutils.CValueEnclosure, qid uint64,
	cnt uint64, usedByTimechart bool, batchErr *utils.BatchError, unsetRecord map[string]sutils.CValueEnclosure) {
	if runningStats == nil {
		if rr.runningStats == nil {
			return
		}
		runningStats = &rr.runningStats
	}

	for i := 0; i < len(*runningStats); i++ {
		fields := rr.currStats[i].ValueColRequest.GetFields()
		fieldToValue, err := PopulateFieldToValueFromMeasureResults(unsetRecord, fields, measureResults, i)
		if err != nil {
			batchErr.AddError("RunningBucketResults.AddMeasureResults: failed to populate field to value", err)
			continue
		}

		measureFunc := rr.currStats[i].MeasureFunc
		// TODO: Change All the Eval functions to return error
		// of type *ErrorWithCode
		switch measureFunc {
		case sutils.Sum:
			step, err := rr.AddEvalResultsForSum(runningStats, measureResults, i, fieldToValue)
			if err != nil {
				batchErr.AddError("RunningBucketResults.AddMeasureResults:Sum", err)
			}
			i += step
		case sutils.Avg:
			step, err := rr.AddEvalResultsForAvg(runningStats, measureResults, i, fieldToValue)
			if err != nil {
				batchErr.AddError("RunningBucketResults.AddMeasureResults:Avg", err)
			}
			i += step
		case sutils.Max:
			fallthrough
		case sutils.Min:
			isMin := measureFunc == sutils.Min
			step, err := rr.AddEvalResultsForMinMax(runningStats, measureResults, i, isMin, fieldToValue)
			if err != nil {
				batchErr.AddError("RunningBucketResults.AddMeasureResults:MinMax", err)
			}
			i += step
		case sutils.EarliestTime:
			fallthrough
		case sutils.LatestTime:
			isLatestTime := measureFunc == sutils.LatestTime
			step, err := rr.AddEvalResultsForMinMax(runningStats, measureResults, i, isLatestTime, fieldToValue)
			if err != nil {
				batchErr.AddError("RunningBucketResults.AddMeasureResults:LatestTime/EarliestTime", err)
			}
			i += step
		case sutils.Earliest:
			fallthrough
		case sutils.Latest:
			isLatest := sutils.Latest == measureFunc
			step, err := rr.AddEvalResultsForLatestOrEarliest(runningStats, measureResults, i, fieldToValue, isLatest)
			if err != nil {
				batchErr.AddError("RunningBucketResults.AddMeasureResults:Latest/Earliest", err)
			}
			i += step
		case sutils.Range:
			step, err := rr.AddEvalResultsForRange(runningStats, measureResults, i, fieldToValue)
			if err != nil {
				batchErr.AddError("RunningBucketResults.AddMeasureResults:Range", err)
			}
			i += step
		case sutils.Count:
			step, err := rr.AddEvalResultsForCount(runningStats, measureResults, i, usedByTimechart, cnt, fieldToValue)
			if err != nil {
				batchErr.AddError("RunningBucketResults.AddMeasureResults:Count", err)
			}
			i += step
		case sutils.Perc:
			if rr.currStats[i].ValueColRequest == nil {
				err := tDigestAddCval((*runningStats)[i].tDigest, &measureResults[i])
				if err != nil {
					batchErr.AddError("RunningBucketResults.AddMeasureResults:Perc", err)
					continue
				}
				continue
			}
			step, err := rr.AddEvalResultsForPerc(runningStats, measureResults, i, fieldToValue)
			if err != nil {
				batchErr.AddError("RunningBuckketResults.AddMeasureResults:Percentile", err)
			}
			i += step
		case sutils.Cardinality, sutils.EstdcError:
			if rr.currStats[i].ValueColRequest == nil {
				err := hllAddRawCval((*runningStats)[i].hll, &measureResults[i])
				if err != nil {
					batchErr.AddError("RunningBucketResults.AddMeasureResults:Cardinality/EstdcError", err)
					continue
				}
				continue
			}
			step, err := rr.AddEvalResultsForCardinalityOrEstdcError(runningStats, measureResults, i, fieldToValue)
			if err != nil {
				batchErr.AddError("RunningBucketResults.AddMeasureResults:Cardinality/EstdcError", err)
			}
			i += step
		case sutils.Values:
			step, err := rr.AddEvalResultsForValues(runningStats, measureResults, i, fieldToValue)
			if err != nil {
				batchErr.AddError("RunningBucketResults.AddMeasureResults:Values", err)
			}
			i += step
		case sutils.List:
			step, err := rr.AddEvalResultsForList(runningStats, measureResults, i, fieldToValue)
			if err != nil {
				batchErr.AddError("RunningBucketResults.AddMeasureResults:List", err)
			}
			i += step
		default:
			err := rr.ProcessReduce(runningStats, measureResults[i], i)
			if err != nil {
				batchErr.AddError("RunningBucketResults.AddMeasureResults:ProcessReduce", err)
			}
		}
	}
	rr.count += cnt
}

// This assumes the order of bucketResults.RunningStats are in the same order, referencing the same measure request
func (rr *RunningBucketResults) MergeRunningBuckets(toJoin *RunningBucketResults) {

	if toJoin == nil {
		return
	}

	// Merge group by bucket inside each time range bucket (For timechart)
	if toJoin.groupedRunningStats != nil && rr.groupedRunningStats == nil {
		rr.groupedRunningStats = toJoin.groupedRunningStats
	} else if rr.groupedRunningStats != nil && len(rr.groupedRunningStats) > 0 {
		for groupByColVal, runningStats := range rr.groupedRunningStats {
			toJoinRunningStats, exists := toJoin.groupedRunningStats[groupByColVal]
			if !exists {
				continue
			}
			rr.mergeRunningStats(&runningStats, toJoinRunningStats)
		}

		for groupByColVal, toJoinRunningStats := range toJoin.groupedRunningStats {
			_, exists := rr.groupedRunningStats[groupByColVal]
			if !exists {
				rr.groupedRunningStats[groupByColVal] = toJoinRunningStats
			}
		}
	}

	rr.mergeRunningStats(&rr.runningStats, toJoin.runningStats)
	rr.count += toJoin.count
}

func (rr *RunningBucketResults) mergeRunningStats(runningStats *[]runningStats, toJoinRunningStats []runningStats) {
	batchErr := utils.GetOrCreateBatchErrorWithQid(rr.qid)

	for i := 0; i < len(toJoinRunningStats); i++ {
		toJoinRunningStats[i].syncRawValue()

		switch rr.currStats[i].MeasureFunc {
		case sutils.Sum, sutils.Min, sutils.Max:
			if rr.currStats[i].ValueColRequest == nil {
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
				if err != nil {
					batchErr.AddError(fmt.Sprintf("RunningBucketResults.mergeRunningStats:%s", rr.currStats[i].MeasureFunc), err)
				}
			} else {
				fields := rr.currStats[i].ValueColRequest.GetFields()
				err := rr.ProcessReduceForEval(runningStats, toJoinRunningStats[i].rawVal, i, rr.currStats[i].MeasureFunc)
				if err != nil {
					batchErr.AddError(fmt.Sprintf("RunningBucketResults.mergeRunningStats:%s", rr.currStats[i].MeasureFunc), err)
				}
				i += (len(fields) - 1)
			}
		case sutils.Avg:
			if rr.currStats[i].ValueColRequest != nil {
				fields := rr.currStats[i].ValueColRequest.GetFields()
				(*runningStats)[i].avgStat = ReduceAvg((*runningStats)[i].avgStat, toJoinRunningStats[i].avgStat)
				i += (len(fields) - 1)
			} else {
				batchErr.AddError("RunningBucketResults.mergeRunningStats:Avg", fmt.Errorf("ValueColRequest is nil"))
			}
		case sutils.Latest:
			if rr.currStats[i].ValueColRequest == nil {
				latestTsIdx := i + 1
				latestIdx := i
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[latestTsIdx].rawVal, latestTsIdx)
				if err != nil {
					batchErr.AddError("RunningBucketResults.mergeRunningStats:Latest", err)
				}
				if (*runningStats)[latestTsIdx].rawVal.Dtype != sutils.SS_INVALID {
					if (*runningStats)[latestTsIdx].rawVal.CVal.(uint64) == toJoinRunningStats[latestTsIdx].rawVal.CVal.(uint64) {
						(*runningStats)[latestIdx].rawVal = toJoinRunningStats[latestIdx].rawVal
					}
				}
				i += 1
			}
		case sutils.Earliest:
			if rr.currStats[i].ValueColRequest == nil {
				earliestTsIdx := i + 1
				earliestIdx := i
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[earliestTsIdx].rawVal, earliestTsIdx)
				if err != nil {
					batchErr.AddError("RunningBucketResults.mergeRunningStats:Latest", err)
				}
				if (*runningStats)[earliestTsIdx].rawVal.Dtype != sutils.SS_INVALID {
					if (*runningStats)[earliestTsIdx].rawVal.CVal.(uint64) == toJoinRunningStats[earliestTsIdx].rawVal.CVal.(uint64) {
						(*runningStats)[earliestIdx].rawVal = toJoinRunningStats[earliestIdx].rawVal
					}
				}
				i += 1
			}
		case sutils.Range:
			if rr.currStats[i].ValueColRequest != nil {
				fields := rr.currStats[i].ValueColRequest.GetFields()
				(*runningStats)[i].rangeStat = ReduceRange((*runningStats)[i].rangeStat, toJoinRunningStats[i].rangeStat)
				i += (len(fields) - 1)
			} else {
				batchErr.AddError("RunningBucketResults.mergeRunningStats:Range", fmt.Errorf("ValueColRequest is nil"))
			}
		case sutils.Values:
			if rr.currStats[i].ValueColRequest == nil {
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
				if err != nil {
					batchErr.AddError("RunningBucketResults.mergeRunningStats:Values", err)
				}
			} else {
				fields := rr.currStats[i].ValueColRequest.GetFields()
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
				if err != nil {
					batchErr.AddError("RunningBucketResults.mergeRunningStats:Values", err)
				}
				i += (len(fields) - 1)
			}
		case sutils.Perc:
			err := (*runningStats)[i].tDigest.MergeTDigest(toJoinRunningStats[i].tDigest)
			if err != nil {
				batchErr.AddError("RunningBucketResults.mergeRunningStats:Perc", err)
			}
			if rr.currStats[i].ValueColRequest != nil {
				fields := rr.currStats[i].ValueColRequest.GetFields()
				i += (len(fields) - 1)
			}
		case sutils.List:
			if rr.currStats[i].ValueColRequest == nil {
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
				if err != nil {
					batchErr.AddError("RunningBucketResults.mergeRunningStats:List", err)
				}
			} else {
				fields := rr.currStats[i].ValueColRequest.GetFields()
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
				if err != nil {
					batchErr.AddError("RunningBucketResults.mergeRunningStats:List", err)
				}
				i += (len(fields) - 1)
			}
		case sutils.Cardinality, sutils.EstdcError:
			if rr.currStats[i].ValueColRequest == nil {
				err := (*runningStats)[i].hll.StrictUnion(toJoinRunningStats[i].hll.Hll)
				if err != nil {
					batchErr.AddError("RunningBucketResults.mergeRunningStats:Cardinality", utils.NewErrorWithCode("HLL_UNION", err))
				}
			} else {
				fields := rr.currStats[i].ValueColRequest.GetFields()
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
				if err != nil {
					batchErr.AddError("RunningBucketResults.mergeRunningStats:Cardinality", err)
				}
				i += (len(fields) - 1)
			}
		case sutils.Count:
			if rr.currStats[i].ValueColRequest == nil {
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
				if err != nil {
					batchErr.AddError("RunningBucketResults.mergeRunningStats:Count", err)
				}
			} else {
				fields := rr.currStats[i].ValueColRequest.GetFields()
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
				if err != nil {
					batchErr.AddError("RunningBucketResults.mergeRunningStats:Count", err)
				}
				i += (len(fields) - 1)
			}
		default:
			err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
			if err != nil {
				batchErr.AddError("RunningBucketResults.mergeRunningStats:ProcessReduce", err)
			}
		}
	}
}

func (rr *RunningBucketResults) ProcessReduce(runningStats *[]runningStats, e sutils.CValueEnclosure, i int) error {
	runningStat := &(*runningStats)[i]
	aggFunc := rr.currStats[i].MeasureFunc
	if aggFunc == sutils.Sum || aggFunc == sutils.Count {
		if runningStat.number == nil {
			if runningStat.rawVal.Dtype == sutils.SS_INVALID {
				runningStat.rawVal.Dtype = sutils.SS_DT_BACKFILL
			}

			runningStat.number = &sutils.Number{}
			err := runningStat.rawVal.ToNumber(runningStat.number)
			if err != nil {
				return ErrConvertToNumber
			}
		}

		other := &sutils.Number{}
		if e.Dtype == sutils.SS_INVALID {
			e.Dtype = sutils.SS_DT_BACKFILL
		}
		err := e.ToNumber(other)
		if err != nil {
			return ErrConvertToNumber
		}

		err = runningStat.number.ReduceFast(other, aggFunc)
		if err != nil {
			return ErrReduceNumbers
		}

		runningStat.dirty = true

		return nil
	}

	(*runningStats)[i].syncRawValue()
	retVal, err := sutils.Reduce((*runningStats)[i].rawVal, e, rr.currStats[i].MeasureFunc)
	if err != nil {
		return ErrReduceCVal
	} else {
		(*runningStats)[i].rawVal = retVal
		(*runningStats)[i].number = nil
	}
	return nil
}

func ReduceForEval(e1 sutils.CValueEnclosure, e2 sutils.CValueEnclosure, fun sutils.AggregateFunctions) (sutils.CValueEnclosure, error) {
	if e1.Dtype == sutils.SS_INVALID {
		return e2, nil
	} else if e2.Dtype == sutils.SS_INVALID {
		return e1, nil
	}

	switch fun {
	case sutils.Sum:
		if e1.Dtype != e2.Dtype || e1.Dtype != sutils.SS_DT_FLOAT {
			return e1, fmt.Errorf("ReduceForEval: unsupported CVal Dtype: %v", e1.Dtype)
		}
		return sutils.CValueEnclosure{Dtype: e1.Dtype, CVal: e1.CVal.(float64) + e2.CVal.(float64)}, nil
	case sutils.Min:
		return sutils.ReduceMinMax(e1, e2, true)
	case sutils.Max:
		return sutils.ReduceMinMax(e1, e2, false)
	case sutils.Count:
		if e1.Dtype != e2.Dtype || e1.Dtype != sutils.SS_DT_FLOAT {
			return e1, fmt.Errorf("ReduceForEval: unsupported CVal Dtype: %v", e1.Dtype)
		}
		return sutils.CValueEnclosure{Dtype: e1.Dtype, CVal: e1.CVal.(float64) + e2.CVal.(float64)}, nil
	default:
		return e1, fmt.Errorf("ReduceForEval: unsupported function: %v", fun)
	}
}

func ReduceRange(rangeStat1 *structs.RangeStat, rangeStat2 *structs.RangeStat) *structs.RangeStat {
	if rangeStat1 == nil {
		return rangeStat2
	} else if rangeStat2 == nil {
		return rangeStat1
	}

	return &structs.RangeStat{
		Min: math.Min(rangeStat1.Min, rangeStat2.Min),
		Max: math.Max(rangeStat1.Max, rangeStat2.Max),
	}
}

func ReduceAvg(avgStat1 *structs.AvgStat, avgStat2 *structs.AvgStat) *structs.AvgStat {
	if avgStat1 == nil {
		return avgStat2
	} else if avgStat2 == nil {
		return avgStat1
	}

	return &structs.AvgStat{
		Sum:   avgStat1.Sum + avgStat2.Sum,
		Count: avgStat1.Count + avgStat2.Count,
	}
}

func (rr *RunningBucketResults) ProcessReduceForEval(runningStats *[]runningStats, e sutils.CValueEnclosure, i int, measureFunc sutils.AggregateFunctions) error {
	(*runningStats)[i].syncRawValue()
	retVal, err := ReduceForEval((*runningStats)[i].rawVal, e, measureFunc)
	if err != nil {
		return fmt.Errorf("RunningBucketResults.ProcessReduceForEval: failed to add measurement to running stats, err: %v", err)
	} else {
		(*runningStats)[i].rawVal = retVal
		(*runningStats)[i].number = nil
	}
	return nil
}

// The `fieldToValue` map will be overwritten with the keys in `fields` and the
// values in `measureResults`.
func PopulateFieldToValueFromMeasureResults(fieldToValue map[string]sutils.CValueEnclosure, fields []string,
	measureResults []sutils.CValueEnclosure, index int) (map[string]sutils.CValueEnclosure, error) {

	if fieldToValue == nil {
		fieldToValue = make(map[string]sutils.CValueEnclosure, len(fields))
	}

	for _, field := range fields {
		if index >= len(measureResults) {
			return nil, fmt.Errorf("RunningBucketResults.PopulateFieldToValueFromMeasureResults: index out of bounds, index: %v, len(measureResults): %v", index, len(measureResults))
		}
		fieldToValue[field] = measureResults[index]
		index++
	}

	if len(fieldToValue) != len(fields) {
		// The map has stale values we need to remove.
		for field := range fieldToValue {
			if !utils.SliceHas(fields, field) {
				delete(fieldToValue, field)
			}
		}
	}

	return fieldToValue, nil
}

func (rr *RunningBucketResults) AddEvalResultsForSum(runningStats *[]runningStats, measureResults []sutils.CValueEnclosure, i int, fieldToValue map[string]sutils.CValueEnclosure) (int, error) {
	if rr.currStats[i].ValueColRequest == nil {
		return 0, rr.ProcessReduce(runningStats, measureResults[i], i)
	}

	numFields := len(fieldToValue)
	if numFields == 0 {
		return 0, utils.NewErrorWithCode("RunningBucketResults.AddEvalResultsForSum:NON_ZERO_FIELDS",
			fmt.Errorf("need non zero number of fields in expression for eval stats for sum for aggCol: %v", rr.currStats[i].String()))
	}

	(*runningStats)[i].syncRawValue()
	exists := (*runningStats)[i].rawVal.Dtype != sutils.SS_INVALID
	result, err := agg.PerformEvalAggForSum(rr.currStats[i], 1, exists, (*runningStats)[i].rawVal, fieldToValue)
	if err != nil {
		return 0, utils.NewErrorWithCode("RunningBucketResults.AddEvalResultsForSum:PerformEvalAggForSum", err)
	}
	(*runningStats)[i].rawVal = result
	(*runningStats)[i].number = nil

	return numFields - 1, nil
}

func (rr *RunningBucketResults) AddEvalResultsForAvg(runningStats *[]runningStats,
	measureResults []sutils.CValueEnclosure, i int, fieldToValue map[string]sutils.CValueEnclosure) (int, error) {

	if rr.currStats[i].ValueColRequest == nil {
		return 0, rr.ProcessReduce(runningStats, measureResults[i], i)
	}
	numFields := len(fieldToValue)
	if numFields == 0 {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForAvg: Need non zero number of fields in expression for eval stats for avg for aggCol: %v", rr.currStats[i].String())
	}

	(*runningStats)[i].syncRawValue()
	exists := (*runningStats)[i].rawVal.Dtype != sutils.SS_INVALID

	curr := rr.currStats[i]
	avgStat := (*runningStats)[i].avgStat
	result, err := agg.PerformEvalAggForAvg(curr, 1, exists, avgStat, fieldToValue)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForAvg: failed to evaluate ValueColRequest, err: %v", err)
	}
	avg := 0.0
	if result.Count > 0 {
		avg = result.Sum / float64(result.Count)
	}
	(*runningStats)[i].rawVal = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_FLOAT,
		CVal:  avg,
	}
	(*runningStats)[i].number = nil

	return numFields - 1, nil
}

func (rr *RunningBucketResults) AddEvalResultsForLatestOrEarliest(runningStats *[]runningStats, measureResults []sutils.CValueEnclosure, i int, fieldToValue map[string]sutils.CValueEnclosure, isLatest bool) (int, error) {
	if rr.currStats[i].ValueColRequest == nil {
		// order should be the same as defined in evalaggs.go -> @AddMeasureAggInRunningStatsForLatestOrEarliest
		// timestamp is present at index i+1
		// the value (can be any dtype) present at index i
		elTsIdx := i + 1
		elIdx := i
		(*runningStats)[elTsIdx].syncRawValue()
		(*runningStats)[elIdx].syncRawValue()
		elTsChanged := false
		retVal, err := sutils.Reduce((*runningStats)[elTsIdx].rawVal, measureResults[elTsIdx], rr.currStats[elTsIdx].MeasureFunc)
		if err != nil {
			return 1, ErrReduceCVal
		} else {
			if (*runningStats)[elIdx].rawVal.Dtype != sutils.SS_INVALID {
				if retVal.CVal.(uint64) != (*runningStats)[elTsIdx].rawVal.CVal.(uint64) {
					elTsChanged = true
				}
			} else {
				elTsChanged = true
			}
			(*runningStats)[elTsIdx].rawVal = retVal
			(*runningStats)[elTsIdx].number = nil
			if elTsChanged {
				(*runningStats)[elIdx].rawVal = measureResults[elIdx]
				(*runningStats)[elIdx].number = nil
			}
		}
		return 1, nil
	}
	return 1, nil
}

func (rr *RunningBucketResults) AddEvalResultsForMinMax(runningStats *[]runningStats, measureResults []sutils.CValueEnclosure, i int, isMin bool, fieldToValue map[string]sutils.CValueEnclosure) (int, error) {
	if rr.currStats[i].ValueColRequest == nil {
		return 0, rr.ProcessReduce(runningStats, measureResults[i], i)
	}
	numFields := len(fieldToValue)
	if numFields == 0 {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForMinMax: Need non zero number of fields in expression for eval stats for min/max for aggCol: %v", rr.currStats[i].String())
	}

	(*runningStats)[i].syncRawValue()
	exists := (*runningStats)[i].rawVal.Dtype != sutils.SS_INVALID

	result, err := agg.PerformEvalAggForMinOrMax(rr.currStats[i], exists, (*runningStats)[i].rawVal, fieldToValue, isMin)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForMinMax: failed to evaluate ValueColRequest, err: %v", err)
	}
	(*runningStats)[i].rawVal = result
	(*runningStats)[i].number = nil

	return numFields - 1, nil
}

func (rr *RunningBucketResults) AddEvalResultsForRange(runningStats *[]runningStats, measureResults []sutils.CValueEnclosure, i int, fieldToValue map[string]sutils.CValueEnclosure) (int, error) {
	if rr.currStats[i].ValueColRequest == nil {
		return 0, rr.ProcessReduce(runningStats, measureResults[i], i)
	}
	numFields := len(fieldToValue)
	if numFields == 0 {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForRange: Need non zero number of fields in expression for eval stats for range for aggCol: %v", rr.currStats[i].String())
	}

	(*runningStats)[i].syncRawValue()
	exists := (*runningStats)[i].rawVal.Dtype != sutils.SS_INVALID

	result, err := agg.PerformEvalAggForRange(rr.currStats[i], exists, (*runningStats)[i].rangeStat, fieldToValue)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForRange: failed to evaluate ValueColRequest, err: %v", err)
	}
	(*runningStats)[i].rawVal = sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_FLOAT,
		CVal:  result.Max - result.Min,
	}
	(*runningStats)[i].number = nil

	return numFields - 1, nil
}

func (rr *RunningBucketResults) AddEvalResultsForCount(runningStats *[]runningStats, measureResults []sutils.CValueEnclosure, i int, usedByTimechart bool, cnt uint64, fieldToValue map[string]sutils.CValueEnclosure) (int, error) {
	var err error
	if rr.currStats[i].ValueColRequest == nil {
		if usedByTimechart {
			eVal := &sutils.CValueEnclosure{
				Dtype: sutils.SS_DT_UNSIGNED_NUM,
				CVal:  cnt,
			}
			return 0, rr.ProcessReduce(runningStats, *eVal, i)
		} else {
			return 0, rr.ProcessReduce(runningStats, measureResults[i], i)
		}
	}

	boolResult := true
	if rr.currStats[i].ValueColRequest.BooleanExpr != nil {
		boolResult, err = rr.currStats[i].ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
		if err != nil {
			return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForCount: there are some errors in the eval function that is inside the count function, err: %v", err)
		}
	}

	(*runningStats)[i].syncRawValue()
	if (*runningStats)[i].rawVal.CVal == nil {
		(*runningStats)[i].rawVal = sutils.CValueEnclosure{
			CVal:  int64(0),
			Dtype: sutils.SS_DT_SIGNED_NUM,
		}
	}
	if boolResult {
		(*runningStats)[i].rawVal.CVal = (*runningStats)[i].rawVal.CVal.(int64) + 1
		(*runningStats)[i].number = nil
	}

	return len(fieldToValue) - 1, nil
}

func (rr *RunningBucketResults) AddEvalResultsForPerc(runningStats *[]runningStats, measureResults []sutils.CValueEnclosure, i int, fieldToValue map[string]sutils.CValueEnclosure) (int, error) {
	if (*runningStats)[i].tDigest == nil {
		td, err := utils.CreateNewTDigest()
		if err != nil {
			return 0, fmt.Errorf("RunningBuckets.AddEvalResultsForPerc: unable to initialize the digest tree; err: %v", err)
		}
		(*runningStats)[i].tDigest = td
	}
	td := (*runningStats)[i].tDigest
	err := agg.PerformEvalAggForPerc(rr.currStats[i], 0, td, fieldToValue)
	if err != nil {
		return 0, fmt.Errorf("RunningBuckets.AddEvalResultsForPerc: failed to evaluate ValueColRequest to string, err: %v", err)
	}
	(*runningStats)[i].tDigest = td
	(*runningStats)[i].number = nil
	return len(fieldToValue) - 1, nil
}

// used for cardinality and estdc_error
func (rr *RunningBucketResults) AddEvalResultsForCardinalityOrEstdcError(runningStats *[]runningStats, measureResults []sutils.CValueEnclosure, i int, fieldToValue map[string]sutils.CValueEnclosure) (int, error) {

	(*runningStats)[i].syncRawValue()
	if (*runningStats)[i].rawVal.CVal == nil {
		(*runningStats)[i].rawVal = sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_GOBBABLE_HLL_PTR,
			CVal:  structs.CreateNewHll(),
		}
	}
	hll := (*runningStats)[i].rawVal.CVal.(*utils.GobbableHll)

	if rr.currStats[i].ValueColRequest == nil {
		strVal, err := measureResults[i].GetString()
		if err != nil {
			return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForCardinalityOrEstdcError: failed to add measurement to running stats, err: %v", err)
		}
		hll.AddRaw(xxhash.Sum64String(strVal))
		(*runningStats)[i].rawVal.CVal = hll
		(*runningStats)[i].number = nil
		return 0, nil
	}

	err := agg.PerformAggEvalForCardinality(rr.currStats[i], hll, fieldToValue)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForCardinalityOrEstdcError: failed to evaluate ValueColRequest to string, err: %v", err)
	}
	(*runningStats)[i].rawVal.CVal = hll
	(*runningStats)[i].number = nil

	return len(fieldToValue) - 1, nil
}

func (rr *RunningBucketResults) AddEvalResultsForValues(runningStats *[]runningStats, measureResults []sutils.CValueEnclosure, i int, fieldToValue map[string]sutils.CValueEnclosure) (int, error) {

	(*runningStats)[i].syncRawValue()
	if (*runningStats)[i].rawVal.CVal == nil {
		(*runningStats)[i].rawVal = sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_STRING_SET,
			CVal:  make(map[string]struct{}, 0),
		}
	}
	strSet := (*runningStats)[i].rawVal.CVal.(map[string]struct{})

	if rr.currStats[i].ValueColRequest == nil {
		strVal, err := measureResults[i].GetString()
		if err != nil {
			return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForValues: failed to add measurement to running stats, err: %v", err)
		}
		strSet[strVal] = struct{}{}
		(*runningStats)[i].rawVal.CVal = strSet
		(*runningStats)[i].number = nil
		return 0, nil
	}

	_, err := agg.PerformAggEvalForValues(rr.currStats[i], strSet, fieldToValue)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForValues: failed to evaluate ValueColRequest to string, err: %v", err)
	}
	(*runningStats)[i].rawVal.CVal = strSet
	(*runningStats)[i].number = nil

	return len(fieldToValue) - 1, nil
}

func (rr *RunningBucketResults) AddEvalResultsForList(runningStats *[]runningStats, measureResults []sutils.CValueEnclosure, i int, fieldToValue map[string]sutils.CValueEnclosure) (int, error) {

	(*runningStats)[i].syncRawValue()
	if (*runningStats)[i].rawVal.CVal == nil {
		(*runningStats)[i].rawVal = sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_STRING_SLICE,
			CVal:  make([]string, 0),
		}
	}
	strList, ok := (*runningStats)[i].rawVal.CVal.([]string)
	if !ok {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForList: failed to convert CVal to list, err: %v", (*runningStats)[i].rawVal.CVal)
	}
	if rr.currStats[i].ValueColRequest == nil {
		strVal, err := measureResults[i].GetString()
		if err != nil {
			return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForList: failed to add measurement to running stats, err: %v", err)
		}
		strList = append(strList, strVal)
		(*runningStats)[i].rawVal.CVal = strList
		(*runningStats)[i].number = nil
		return 0, nil
	}

	result, err := agg.PerformAggEvalForList(rr.currStats[i], strList, fieldToValue)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForList: failed to evaluate ValueColRequest to string, err: %v", err)
	}
	if len(result) > sutils.MAX_SPL_LIST_SIZE {
		result = result[:sutils.MAX_SPL_LIST_SIZE]
	}
	(*runningStats)[i].rawVal.CVal = result
	(*runningStats)[i].number = nil

	return len(fieldToValue) - 1, nil
}

func (rr *RunningBucketResults) GetRunningStatsBucketValues() ([]sutils.CValueEnclosure, uint64) {
	retVal := make([]sutils.CValueEnclosure, len(rr.runningStats))
	for i := 0; i < len(rr.runningStats); i++ {
		rr.runningStats[i].syncRawValue()
		retVal[i] = rr.runningStats[i].rawVal
	}
	return retVal, rr.count
}

func (rs runningStats) GetRunningStatJSON() RunningStatsJSON {
	rs.syncRawValue()
	rsJson := RunningStatsJSON{
		RawVal:    rs.rawVal.CVal,
		RangeStat: rs.rangeStat,
		AvgStat:   rs.avgStat,
	}
	if rs.hll != nil {
		rsJson.Hll = rs.hll.ToBytes()
	}
	if rs.rawVal.Dtype == sutils.SS_DT_STRING_SET {
		rsJson.StrSet = rs.rawVal.CVal.(map[string]struct{})
		rsJson.RawVal = nil
	}
	if rs.rawVal.Dtype == sutils.SS_DT_STRING_SLICE {
		rsJson.StrList = rs.rawVal.CVal.([]string)
		rsJson.RawVal = nil
	}

	return rsJson
}

func (rj RunningStatsJSON) GetRunningStats() (runningStats, error) {
	rs := runningStats{
		rangeStat: rj.RangeStat,
		avgStat:   rj.AvgStat,
	}
	if rj.RawVal != nil {
		CVal := sutils.CValueEnclosure{}
		err := CVal.ConvertValue(rj.RawVal)
		if err != nil {
			return runningStats{}, utils.TeeErrorf("RunningStatsJSON.GetRunningStats: failed to convert value, err: %v", err)
		}
		rs.rawVal = CVal
	}
	if rj.StrSet != nil {
		rs.rawVal = sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_STRING_SET,
			CVal:  rj.StrSet,
		}
	}
	if rj.Hll != nil {
		hll, err := structs.CreateHllFromBytes(rj.Hll)
		if err != nil {
			return runningStats{}, utils.TeeErrorf("RunningStatsJSON.GetRunningStats: failed to create HLL from bytes, err: %v", err)
		}
		rs.hll = &utils.GobbableHll{Hll: *hll}
	}
	if rj.StrList != nil {
		rs.rawVal = sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_STRING_SLICE,
			CVal:  rj.StrList,
		}
	}
	return rs, nil
}

func (rbr *RunningBucketResults) ToSerializedRunningBucketResults() *SerializedRunningBucketResults {
	runningStats := make([]SerializedRunningStats, len(rbr.runningStats))
	for i := 0; i < len(rbr.runningStats); i++ {
		runningStats[i] = *rbr.runningStats[i].ToSerializedRunningStats()
	}
	groupedRunningStats := make(map[string][]SerializedRunningStats)
	for k, v := range rbr.groupedRunningStats {
		groupedRunningStats[k] = make([]SerializedRunningStats, len(v))
		for i := 0; i < len(v); i++ {
			groupedRunningStats[k][i] = *v[i].ToSerializedRunningStats()
		}
	}

	return &SerializedRunningBucketResults{
		RunningStats:        runningStats,
		CurrStats:           rbr.currStats,
		GroupedRunningStats: groupedRunningStats,
		Count:               rbr.count,
	}
}

func (rs *runningStats) ToSerializedRunningStats() *SerializedRunningStats {
	rs.syncRawValue()
	return &SerializedRunningStats{
		RawVal:    rs.rawVal,
		Hll:       rs.hll,
		RangeStat: rs.rangeStat,
		AvgStat:   rs.avgStat,
	}
}

func (srb *SerializedRunningBucketResults) ToRunningBucketResults(qid uint64) *RunningBucketResults {
	runStats := make([]runningStats, len(srb.RunningStats))
	for i := 0; i < len(srb.RunningStats); i++ {
		runStats[i] = *srb.RunningStats[i].ToRunningStats()
	}
	groupedRunningStats := make(map[string][]runningStats)
	for k, v := range srb.GroupedRunningStats {
		groupedRunningStats[k] = make([]runningStats, len(v))
		for i := 0; i < len(v); i++ {
			groupedRunningStats[k][i] = *v[i].ToRunningStats()
		}
	}
	return &RunningBucketResults{
		runningStats:        runStats,
		currStats:           srb.CurrStats,
		groupedRunningStats: groupedRunningStats,
		count:               srb.Count,
		qid:                 qid,
	}
}

func (srs *SerializedRunningStats) ToRunningStats() *runningStats {
	return &runningStats{
		rawVal:    srs.RawVal,
		hll:       srs.Hll,
		rangeStat: srs.RangeStat,
		avgStat:   srs.AvgStat,
	}
}

func GetRunningBucketResultsSliceForTest() []*RunningBucketResults {
	hll := structs.CreateNewHll()
	runningBucketResults := make([]*RunningBucketResults, 0)

	hll.AddRaw(1)
	hll.AddRaw(2)
	hll.AddRaw(1)

	valueExpr := &structs.ValueExpr{
		ValueExprMode: structs.VEMBooleanExpr,
		FloatValue:    10.0,
		BooleanExpr: &structs.BoolExpr{
			IsTerminal: true,
			LeftValue: &structs.ValueExpr{
				ValueExprMode: structs.VEMNumericExpr,
				NumericExpr: &structs.NumericExpr{
					NumericExprMode: structs.NEMNumberField,
					IsTerminal:      true,
					ValueIsField:    true,
					Value:           "col2",
				},
			},
			RightValue: &structs.ValueExpr{
				ValueExprMode: structs.VEMNumericExpr,
				NumericExpr: &structs.NumericExpr{
					NumericExprMode: structs.NEMNumericExpr,
					IsTerminal:      false,
					Op:              "+",
					Left: &structs.NumericExpr{
						NumericExprMode: structs.NEMNumberField,
						IsTerminal:      true,
						ValueIsField:    true,
						Value:           "col1",
					},
					Right: &structs.NumericExpr{
						NumericExprMode: structs.NEMNumber,
						IsTerminal:      true,
						ValueIsField:    true,
						Value:           "1",
					},
				},
			},
			ValueOp: "=",
		},
	}

	rs := runningStats{
		rawVal: sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_FLOAT,
			CVal:  10.0,
		},
		hll: hll,
		rangeStat: &structs.RangeStat{
			Min: 0.0,
			Max: 10.0,
		},
		avgStat: &structs.AvgStat{
			Sum:   10.0,
			Count: 1,
		},
	}

	groupedRunningStats := make(map[string][]runningStats)
	groupedRunningStats["group1"] = []runningStats{rs, rs}
	groupedRunningStats["group2"] = []runningStats{rs}

	runningBucketResults = append(runningBucketResults, &RunningBucketResults{
		count:        1,
		runningStats: []runningStats{rs},
		currStats: []*structs.MeasureAggregator{
			{
				MeasureCol:      "test",
				MeasureFunc:     sutils.Sum,
				StrEnc:          "sum(test)",
				ValueColRequest: valueExpr,
			},
		},
		groupedRunningStats: groupedRunningStats,
		qid:                 0,
	})
	runningBucketResults = append(runningBucketResults, &RunningBucketResults{
		count: 2,
		runningStats: []runningStats{
			{
				rawVal: sutils.CValueEnclosure{
					Dtype: sutils.SS_DT_FLOAT,
					CVal:  10.0,
				},
				hll: hll,
				rangeStat: &structs.RangeStat{
					Min: 0.0,
					Max: 10.0,
				},
				avgStat: &structs.AvgStat{
					Sum:   10.0,
					Count: 1,
				},
			},
		},
		currStats: []*structs.MeasureAggregator{
			{
				MeasureCol:      "test",
				MeasureFunc:     sutils.Count,
				StrEnc:          "count(test)",
				ValueColRequest: valueExpr,
			},
		},
		groupedRunningStats: groupedRunningStats,
	})

	return runningBucketResults
}

func tDigestAddCval(td *utils.GobbableTDigest, cval *sutils.CValueEnclosure) error {
	var err error
	switch cval.Dtype {
	case sutils.SS_DT_FLOAT:
		err = td.Add(cval.CVal.(float64))
	case sutils.SS_DT_SIGNED_NUM:
		err = td.Add(float64(cval.CVal.(int64)))
	case sutils.SS_DT_UNSIGNED_NUM:
		err = td.Add(float64(cval.CVal.(uint64)))
	case sutils.SS_DT_BACKFILL:
		return utils.NewErrorWithCode(utils.NIL_VALUE_ERR, fmt.Errorf("CValueEnclosure GetString: nil value"))
	default:
		return fmt.Errorf("tDigestAddCval: Works only on numerical columns. Received: %v", cval.Dtype)
	}
	if err != nil {
		return fmt.Errorf("tDigestAddCval: Unable to add value to digest tree; val: %v, err: %v", cval.CVal, err)
	}
	return nil
}

func hllAddRawCval(hll *utils.GobbableHll, cval *sutils.CValueEnclosure) error {
	switch cval.Dtype {
	case sutils.SS_DT_STRING:
		hll.AddRaw(xxhash.Sum64String(cval.CVal.(string)))
	case sutils.SS_DT_STRING_SLICE:
		hll.AddRaw(xxhash.Sum64String(fmt.Sprintf("%v", cval.CVal.([]string))))
	case sutils.SS_DT_BOOL:
		// for boolean-valued fields, the cardinality is small, and this implementation of HLL stores the values explicitly for smaller cardinalities
		// so we don't need to worry about hashing and the statistical properties associated with it.
		if cval.CVal.(bool) {
			hll.AddRaw(1) // 1, 2 serve as sentinel values
		} else {
			hll.AddRaw(2) // cannot use AddRaw(0), 0 is not a valid value for AddRaw(). See: Hll.AddRaw()
		}
	case sutils.SS_DT_UNSIGNED_NUM:
		hll.AddRaw(xxhash.Sum64(utils.Uint64ToBytesLittleEndian(cval.CVal.(uint64))))
	case sutils.SS_DT_SIGNED_NUM:
		hll.AddRaw(xxhash.Sum64(utils.Int64ToBytesLittleEndian(cval.CVal.(int64))))
	case sutils.SS_DT_FLOAT:
		hll.AddRaw(xxhash.Sum64(utils.Float64ToBytesLittleEndian(cval.CVal.(float64))))
	case sutils.SS_DT_BACKFILL:
		return utils.NewErrorWithCode(utils.NIL_VALUE_ERR, fmt.Errorf("CValueEnclosure GetString: nil value"))
	default:
		return fmt.Errorf("CValueEnclosure GetString: unsupported Dtype: %v", cval.Dtype)
	}
	return nil
}
