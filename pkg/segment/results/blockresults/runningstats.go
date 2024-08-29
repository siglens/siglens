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
	"github.com/siglens/siglens/pkg/segment/utils"
	putils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	bbp "github.com/valyala/bytebufferpool"
)

type RunningBucketResults struct {
	runningStats        []runningStats               // maps a stat name to running stats
	currStats           []*structs.MeasureAggregator // measure aggregators in result
	groupedRunningStats map[string][]runningStats    // maps timechart group by col's vals to corresponding running stats
	count               uint64                       // total number of elements belonging to the bucket
}

type runningStats struct {
	rawVal    utils.CValueEnclosure // raw value
	hll       *putils.GobbableHll
	rangeStat *structs.RangeStat
	avgStat   *structs.AvgStat
}

func initRunningStats(internalMeasureFns []*structs.MeasureAggregator) []runningStats {
	retVal := make([]runningStats, len(internalMeasureFns))
	for i := 0; i < len(internalMeasureFns); i++ {
		if internalMeasureFns[i].MeasureFunc == utils.Cardinality {
			retVal[i] = runningStats{hll: structs.CreateNewHll()}
		} else if internalMeasureFns[i].MeasureFunc == utils.Avg {
			retVal[i] = runningStats{avgStat: &structs.AvgStat{}}
		} else if internalMeasureFns[i].MeasureFunc == utils.Range {
			retVal[i] = runningStats{rangeStat: agg.InitRangeStat()}
		}
	}
	return retVal
}

func initRunningGroupByBucket(internalMeasureFns []*structs.MeasureAggregator) *RunningBucketResults {

	return &RunningBucketResults{
		count:               0,
		runningStats:        initRunningStats(internalMeasureFns),
		currStats:           internalMeasureFns,
		groupedRunningStats: make(map[string][]runningStats),
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

func (rr *RunningBucketResults) AddMeasureResults(runningStats *[]runningStats, measureResults []utils.CValueEnclosure, qid uint64,
	cnt uint64, usedByTimechart bool) {
	if runningStats == nil {
		if rr.runningStats == nil {
			return
		}
		runningStats = &rr.runningStats
	}

	for i := 0; i < len(*runningStats); i++ {
		switch rr.currStats[i].MeasureFunc {
		case utils.Sum:
			step, err := rr.AddEvalResultsForSum(runningStats, measureResults, i)
			if err != nil {
				log.Errorf("RunningBucketResults.AddMeasureResults: failed to add eval results for sum, err: %v", err)
			}
			i += step
		case utils.Avg:
			step, err := rr.AddEvalResultsForAvg(runningStats, measureResults, i)
			if err != nil {
				log.Errorf("RunningBucketResults.AddMeasureResults: failed to add eval results for avg, err: %v", err)
			}
			i += step
		case utils.Max:
			fallthrough
		case utils.Min:
			isMin := rr.currStats[i].MeasureFunc == utils.Min
			step, err := rr.AddEvalResultsForMinMax(runningStats, measureResults, i, isMin)
			if err != nil {
				log.Errorf("RunningBucketResults.AddMeasureResults: failed to add eval results for min/max, err: %v", err)
			}
			i += step
		case utils.Range:
			step, err := rr.AddEvalResultsForRange(runningStats, measureResults, i)
			if err != nil {
				log.Errorf("RunningBucketResults.AddMeasureResults: failed to add eval results for range, err: %v", err)
			}
			i += step
		case utils.Count:
			step, err := rr.AddEvalResultsForCount(runningStats, measureResults, i, usedByTimechart, cnt)
			if err != nil {
				log.Errorf("RunningBucketResults.AddMeasureResults: failed to add eval results for count, err: %v", err)
			}
			i += step
		case utils.Cardinality:
			if rr.currStats[i].ValueColRequest == nil {
				rawVal, err := measureResults[i].GetString()
				if err != nil {
					log.Errorf("RunningBucketResults.AddMeasureResults: failed to add measurement to running stats: %v", err)
					continue
				}
				bb := bbp.Get()
				defer bbp.Put(bb)
				bb.Reset()
				_, _ = bb.WriteString(rawVal)
				(*runningStats)[i].hll.AddRaw(xxhash.Sum64(bb.B))
				continue
			}
			fallthrough
		case utils.Values:
			step, err := rr.AddEvalResultsForValuesOrCardinality(runningStats, measureResults, i)
			if err != nil {
				log.Errorf("RunningBucketResults.AddMeasureResults: failed to add eval results for values/cardinality, err: %v", err)
			}
			i += step
		case utils.List:
			step, err := rr.AddEvalResultsForList(runningStats, measureResults, i)
			if err != nil {
				log.Errorf("RunningBucketResults.AddMeasureResults: failed to add eval results for list, err: %v", err)
			}
			i += step
		default:
			err := rr.ProcessReduce(runningStats, measureResults[i], i)
			if err != nil {
				log.Errorf("RunningBucketResults.AddMeasureResults: Error while ProcessReduce, err: %v", err)
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
	for i := 0; i < len(toJoinRunningStats); i++ {
		switch rr.currStats[i].MeasureFunc {
		case utils.Sum, utils.Min, utils.Max:
			if rr.currStats[i].ValueColRequest == nil {
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
				if err != nil {
					log.Errorf("RunningBucketResults.mergeRunningStats: Error merging running stats for %v while ValueColRequest is nil, err: %v", rr.currStats[i].MeasureFunc, err)
				}
			} else {
				fields := rr.currStats[i].ValueColRequest.GetFields()
				err := rr.ProcessReduceForEval(runningStats, toJoinRunningStats[i].rawVal, i, rr.currStats[i].MeasureFunc)
				if err != nil {
					log.Errorf("RunningBucketResults.mergeRunningStats: Error merging running stats for %v err: %v", rr.currStats[i].MeasureFunc, err)
				}
				i += (len(fields) - 1)
			}
		case utils.Avg:
			if rr.currStats[i].ValueColRequest != nil {
				fields := rr.currStats[i].ValueColRequest.GetFields()
				(*runningStats)[i].avgStat = ReduceAvg((*runningStats)[i].avgStat, toJoinRunningStats[i].avgStat)
				i += (len(fields) - 1)
			} else {
				log.Errorf("RunningBucketResults.mergeRunningStats: Error merging running stats for 'avg' while ValueColRequest is nil")
			}
		case utils.Range:
			if rr.currStats[i].ValueColRequest != nil {
				fields := rr.currStats[i].ValueColRequest.GetFields()
				(*runningStats)[i].rangeStat = ReduceRange((*runningStats)[i].rangeStat, toJoinRunningStats[i].rangeStat)
				i += (len(fields) - 1)
			} else {
				log.Errorf("RunningBucketResults.mergeRunningStats: Error merging running stats for 'range' while ValueColRequest is nil")
			}
		case utils.Values:
			if rr.currStats[i].ValueColRequest == nil {
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
				if err != nil {
					log.Errorf("RunningBucketResults.mergeRunningStats: Error merging running stats for 'values' while ValueColRequest is nil, err: %v", err)
				}
			} else {
				fields := rr.currStats[i].ValueColRequest.GetFields()
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
				if err != nil {
					log.Errorf("RunningBucketResults.mergeRunningStats: Error merging running stats for 'values' err: %v", err)
				}
				i += (len(fields) - 1)
			}
		case utils.List:
			if rr.currStats[i].ValueColRequest == nil {
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
				if err != nil {
					log.Errorf("RunningBucketResults.mergeRunningStats: Error merging running stats for 'list' while ValueColRequest is nil, err: %v", err)
				}
			} else {
				fields := rr.currStats[i].ValueColRequest.GetFields()
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
				if err != nil {
					log.Errorf("RunningBucketResults.mergeRunningStats: Error merging running stats for 'list' err: %v", err)
				}
				i += (len(fields) - 1)
			}
		case utils.Cardinality:
			if rr.currStats[i].ValueColRequest == nil {
				err := (*runningStats)[i].hll.StrictUnion(toJoinRunningStats[i].hll.Hll)
				if err != nil {
					log.Errorf("RunningBucketResults.mergeRunningStats: failed merge HLL!: %v", err)
				}
			} else {
				fields := rr.currStats[i].ValueColRequest.GetFields()
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
				if err != nil {
					log.Errorf("RunningBucketResults.mergeRunningStats: Error merging running stats for 'cardinality', err: %v", err)
				}
				i += (len(fields) - 1)
			}
		case utils.Count:
			if rr.currStats[i].ValueColRequest == nil {
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
				if err != nil {
					log.Errorf("RunningBucketResults.mergeRunningStats: Error merging running stats for 'count', err: %v", err)
				}
			} else {
				fields := rr.currStats[i].ValueColRequest.GetFields()
				err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
				if err != nil {
					log.Errorf("RunningBucketResults.mergeRunningStats: failed to add measurement to running stats, err: %v", err)
				}
				i += (len(fields) - 1)
			}
		default:
			err := rr.ProcessReduce(runningStats, toJoinRunningStats[i].rawVal, i)
			if err != nil {
				log.Errorf("RunningBucketResults.mergeRunningStats: err: %v", err)
			}
		}
	}
}

func (rr *RunningBucketResults) ProcessReduce(runningStats *[]runningStats, e utils.CValueEnclosure, i int) error {
	retVal, err := utils.Reduce((*runningStats)[i].rawVal, e, rr.currStats[i].MeasureFunc)
	if err != nil {
		return fmt.Errorf("RunningBucketResults.ProcessReduce: failed to add measurement to running stats, err: %v", err)
	} else {
		(*runningStats)[i].rawVal = retVal
	}
	return nil
}

func ReduceForEval(e1 utils.CValueEnclosure, e2 utils.CValueEnclosure, fun utils.AggregateFunctions) (utils.CValueEnclosure, error) {
	if e1.Dtype == utils.SS_INVALID {
		return e2, nil
	} else if e2.Dtype == utils.SS_INVALID {
		return e1, nil
	}

	switch fun {
	case utils.Sum:
		if e1.Dtype != e2.Dtype || e1.Dtype != utils.SS_DT_FLOAT {
			return e1, fmt.Errorf("ReduceForEval: unsupported CVal Dtype: %v", e1.Dtype)
		}
		return utils.CValueEnclosure{Dtype: e1.Dtype, CVal: e1.CVal.(float64) + e2.CVal.(float64)}, nil
	case utils.Min:
		return utils.ReduceMinMax(e1, e2, true)
	case utils.Max:
		return utils.ReduceMinMax(e1, e2, false)
	case utils.Count:
		if e1.Dtype != e2.Dtype || e1.Dtype != utils.SS_DT_FLOAT {
			return e1, fmt.Errorf("ReduceForEval: unsupported CVal Dtype: %v", e1.Dtype)
		}
		return utils.CValueEnclosure{Dtype: e1.Dtype, CVal: e1.CVal.(float64) + e2.CVal.(float64)}, nil
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

func (rr *RunningBucketResults) ProcessReduceForEval(runningStats *[]runningStats, e utils.CValueEnclosure, i int, measureFunc utils.AggregateFunctions) error {
	retVal, err := ReduceForEval((*runningStats)[i].rawVal, e, measureFunc)
	if err != nil {
		return fmt.Errorf("RunningBucketResults.ProcessReduceForEval: failed to add measurement to running stats, err: %v", err)
	} else {
		(*runningStats)[i].rawVal = retVal
	}
	return nil
}

func PopulateFieldToValueFromMeasureResults(fields []string, measureResults []utils.CValueEnclosure, index int) (map[string]utils.CValueEnclosure, error) {
	fieldToValue := make(map[string]utils.CValueEnclosure)
	for _, field := range fields {
		if index >= len(measureResults) {
			return nil, fmt.Errorf("RunningBucketResults.PopulateFieldToValueFromMeasureResults: index out of bounds, index: %v, len(measureResults): %v", index, len(measureResults))
		}
		fieldToValue[field] = measureResults[index]
		index++
	}
	return fieldToValue, nil
}

func (rr *RunningBucketResults) AddEvalResultsForSum(runningStats *[]runningStats, measureResults []utils.CValueEnclosure, i int) (int, error) {
	if rr.currStats[i].ValueColRequest == nil {
		return 0, rr.ProcessReduce(runningStats, measureResults[i], i)
	}
	fields := rr.currStats[i].ValueColRequest.GetFields()
	if len(fields) == 0 {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForSum: Need non zero number of fields in expression for eval stats for sum for aggCol: %v", rr.currStats[i].String())
	}
	fieldToValue, err := PopulateFieldToValueFromMeasureResults(fields, measureResults, i)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForSum: failed to populate field to value, err: %v", err)
	}
	exists := (*runningStats)[i].rawVal.Dtype != utils.SS_INVALID

	result, err := agg.PerformEvalAggForSum(rr.currStats[i], 1, exists, (*runningStats)[i].rawVal, fieldToValue)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForSum: failed to evaluate ValueColRequest, err: %v", err)
	}
	(*runningStats)[i].rawVal = result

	return len(fields) - 1, nil
}

func (rr *RunningBucketResults) AddEvalResultsForAvg(runningStats *[]runningStats, measureResults []utils.CValueEnclosure, i int) (int, error) {
	if rr.currStats[i].ValueColRequest == nil {
		return 0, rr.ProcessReduce(runningStats, measureResults[i], i)
	}
	fields := rr.currStats[i].ValueColRequest.GetFields()
	if len(fields) == 0 {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForAvg: Need non zero number of fields in expression for eval stats for avg for aggCol: %v", rr.currStats[i].String())
	}
	fieldToValue, err := PopulateFieldToValueFromMeasureResults(fields, measureResults, i)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForAvg: failed to populate field to value, err: %v", err)
	}
	exists := (*runningStats)[i].rawVal.Dtype != utils.SS_INVALID

	result, err := agg.PerformEvalAggForAvg(rr.currStats[i], 1, exists, *(*runningStats)[i].avgStat, fieldToValue)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForAvg: failed to evaluate ValueColRequest, err: %v", err)
	}
	(*runningStats)[i].avgStat = &result
	avg := 0.0
	if result.Count > 0 {
		avg = result.Sum / float64(result.Count)
	}
	(*runningStats)[i].rawVal = utils.CValueEnclosure{
		Dtype: utils.SS_DT_FLOAT,
		CVal:  avg,
	}

	return len(fields) - 1, nil
}

func (rr *RunningBucketResults) AddEvalResultsForMinMax(runningStats *[]runningStats, measureResults []utils.CValueEnclosure, i int, isMin bool) (int, error) {
	if rr.currStats[i].ValueColRequest == nil {
		return 0, rr.ProcessReduce(runningStats, measureResults[i], i)
	}
	fields := rr.currStats[i].ValueColRequest.GetFields()
	if len(fields) == 0 {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForMinMax: Need non zero number of fields in expression for eval stats for min/max for aggCol: %v", rr.currStats[i].String())
	}
	fieldToValue, err := PopulateFieldToValueFromMeasureResults(fields, measureResults, i)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForMinMax: failed to populate field to value, err: %v", err)
	}
	exists := (*runningStats)[i].rawVal.Dtype != utils.SS_INVALID

	result, err := agg.PerformEvalAggForMinOrMax(rr.currStats[i], exists, (*runningStats)[i].rawVal, fieldToValue, isMin)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForMinMax: failed to evaluate ValueColRequest, err: %v", err)
	}
	(*runningStats)[i].rawVal = result

	return len(fields) - 1, nil
}

func (rr *RunningBucketResults) AddEvalResultsForRange(runningStats *[]runningStats, measureResults []utils.CValueEnclosure, i int) (int, error) {
	if rr.currStats[i].ValueColRequest == nil {
		return 0, rr.ProcessReduce(runningStats, measureResults[i], i)
	}
	fields := rr.currStats[i].ValueColRequest.GetFields()
	if len(fields) == 0 {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForRange: Need non zero number of fields in expression for eval stats for range for aggCol: %v", rr.currStats[i].String())
	}
	fieldToValue, err := PopulateFieldToValueFromMeasureResults(fields, measureResults, i)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForRange: failed to populate field to value, err: %v", err)
	}
	exists := (*runningStats)[i].rawVal.Dtype != utils.SS_INVALID

	result, err := agg.PerformEvalAggForRange(rr.currStats[i], exists, *(*runningStats)[i].rangeStat, fieldToValue)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForRange: failed to evaluate ValueColRequest, err: %v", err)
	}
	(*runningStats)[i].rangeStat = &result
	(*runningStats)[i].rawVal = utils.CValueEnclosure{
		Dtype: utils.SS_DT_FLOAT,
		CVal:  result.Max - result.Min,
	}

	return len(fields) - 1, nil
}

func (rr *RunningBucketResults) AddEvalResultsForCount(runningStats *[]runningStats, measureResults []utils.CValueEnclosure, i int, usedByTimechart bool, cnt uint64) (int, error) {
	var err error
	if rr.currStats[i].ValueColRequest == nil {
		if usedByTimechart {
			eVal := &utils.CValueEnclosure{
				Dtype: utils.SS_DT_UNSIGNED_NUM,
				CVal:  cnt,
			}
			return 0, rr.ProcessReduce(runningStats, *eVal, i)
		} else {
			return 0, rr.ProcessReduce(runningStats, measureResults[i], i)
		}
	}

	fields := rr.currStats[i].ValueColRequest.GetFields()
	fieldToValue, err := PopulateFieldToValueFromMeasureResults(fields, measureResults, i)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForCount: failed to populate field to value, err: %v", err)
	}

	boolResult := true
	if rr.currStats[i].ValueColRequest.BooleanExpr != nil {
		boolResult, err = rr.currStats[i].ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
		if err != nil {
			return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForCount: there are some errors in the eval function that is inside the count function, err: %v", err)
		}
	}
	if (*runningStats)[i].rawVal.CVal == nil {
		(*runningStats)[i].rawVal = utils.CValueEnclosure{
			CVal:  int64(0),
			Dtype: utils.SS_DT_SIGNED_NUM,
		}
	}
	if boolResult {
		(*runningStats)[i].rawVal.CVal = (*runningStats)[i].rawVal.CVal.(int64) + 1
	}

	return len(fields) - 1, nil
}

func (rr *RunningBucketResults) AddEvalResultsForValuesOrCardinality(runningStats *[]runningStats, measureResults []utils.CValueEnclosure, i int) (int, error) {
	if (*runningStats)[i].rawVal.CVal == nil {
		(*runningStats)[i].rawVal = utils.CValueEnclosure{
			Dtype: utils.SS_DT_STRING_SET,
			CVal:  make(map[string]struct{}, 0),
		}
	}
	strSet := (*runningStats)[i].rawVal.CVal.(map[string]struct{})

	if rr.currStats[i].ValueColRequest == nil {
		strVal, err := measureResults[i].GetString()
		if err != nil {
			return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForValuesOrCardinality: failed to add measurement to running stats, err: %v", err)
		}
		strSet[strVal] = struct{}{}
		(*runningStats)[i].rawVal.CVal = strSet
		return 0, nil
	}

	fields := rr.currStats[i].ValueColRequest.GetFields()
	fieldToValue, err := PopulateFieldToValueFromMeasureResults(fields, measureResults, i)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForValuesOrCardinality: failed to populate field to value, err: %v", err)
	}

	_, err = agg.PerformAggEvalForCardinality(rr.currStats[i], strSet, fieldToValue)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForValuesOrCardinality: failed to evaluate ValueColRequest to string, err: %v", err)
	}
	(*runningStats)[i].rawVal.CVal = strSet

	return len(fields) - 1, nil
}

func (rr *RunningBucketResults) AddEvalResultsForList(runningStats *[]runningStats, measureResults []utils.CValueEnclosure, i int) (int, error) {
	if (*runningStats)[i].rawVal.CVal == nil {
		(*runningStats)[i].rawVal = utils.CValueEnclosure{
			Dtype: utils.SS_DT_STRING_SLICE,
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
		return 0, nil
	}

	fields := rr.currStats[i].ValueColRequest.GetFields()
	fieldToValue, err := PopulateFieldToValueFromMeasureResults(fields, measureResults, i)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForList: failed to populate field to value, err: %v", err)
	}

	result, err := agg.PerformAggEvalForList(rr.currStats[i], strList, fieldToValue)
	if err != nil {
		return 0, fmt.Errorf("RunningBucketResults.AddEvalResultsForList: failed to evaluate ValueColRequest to string, err: %v", err)
	}
	(*runningStats)[i].rawVal.CVal = result

	return len(fields) - 1, nil
}

func (rr *RunningBucketResults) GetRunningStatsBucketValues() ([]utils.CValueEnclosure, uint64) {
	retVal := make([]utils.CValueEnclosure, len(rr.runningStats))
	for i := 0; i < len(rr.runningStats); i++ {
		retVal[i] = rr.runningStats[i].rawVal
	}
	return retVal, rr.count
}
