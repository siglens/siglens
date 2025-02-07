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
	"container/heap"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func applyTimeRangeHistogram(nodeResult *structs.NodeResult, rangeHistogram *structs.TimeBucket, aggName string) {

	if nodeResult.Histogram == nil || rangeHistogram.Timechart != nil {
		return
	}
	res, ok := nodeResult.Histogram[aggName]
	if !ok || res == nil {
		return
	}

	nodeResult.Histogram[aggName].IsDateHistogram = true
	if rangeHistogram.EndTime != 0 || rangeHistogram.StartTime != 0 { // default values of uint64
		finalList := make([]*structs.BucketResult, 0)
		for _, recs := range nodeResult.Histogram[aggName].Results {
			bucketTime, ok := recs.BucketKey.(uint64)
			if !ok {
				log.Errorf("time for bucket aggregation is not uint64!")
				continue
			}
			if rangeHistogram.EndTime != 0 && bucketTime > rangeHistogram.EndTime {
				continue
			}
			if rangeHistogram.StartTime != 0 && bucketTime < rangeHistogram.StartTime {
				continue
			}
			finalList = append(finalList, recs)
		}
		nodeResult.Histogram[aggName].Results = finalList
	}
	sort.Slice(nodeResult.Histogram[aggName].Results, func(i, j int) bool {
		iVal, ok := nodeResult.Histogram[aggName].Results[i].BucketKey.(uint64)
		if !ok {
			return false
		}
		jVal, ok := nodeResult.Histogram[aggName].Results[j].BucketKey.(uint64)
		if !ok {
			return true
		}
		return iVal < jVal
	})
}

func CheckIfTimeSort(agg *structs.QueryAggregators) (bool, bool) {
	if agg == nil {
		return false, false
	}

	if agg.HasSortBlock() {
		if len(agg.OutputTransforms.LetColumns.SortColRequest.SortEles) == 1 && agg.OutputTransforms.LetColumns.SortColRequest.SortEles[0].Field == "timestamp" {
			return true, agg.OutputTransforms.LetColumns.SortColRequest.SortEles[0].SortByAsc
		} else {
			return false, false
		}
	}

	if agg.Sort != nil && agg.Sort.ColName == "timestamp" {
		return true, agg.Sort.Ascending
	}

	return false, false

}

// Function to clean up results based on input query aggregations.
// This will make sure all buckets respect the minCount & is returned in a sorted order
func PostQueryBucketCleaning(nodeResult *structs.NodeResult, post *structs.QueryAggregators, recs map[string]map[string]interface{},
	recordIndexInFinal map[string]int, finalCols map[string]bool, numTotalSegments uint64, finishesSegment bool) *structs.NodeResult {
	if post == nil {
		return nodeResult
	}

	if post.TimeHistogram != nil {
		applyTimeRangeHistogram(nodeResult, post.TimeHistogram, post.TimeHistogram.AggName)
	}

	if post.GroupByRequest != nil {
		nodeResult.GroupByCols = post.GroupByRequest.GroupByColumns
		nodeResult.GroupByRequest = post.GroupByRequest
	}

	if post.TransactionArguments != nil && len(recs) == 0 {
		return nodeResult
	}

	// For the query without groupby, skip the first aggregator without a QueryAggergatorBlock
	// For the query that has a groupby, groupby block's aggregation is in the post.Next. Therefore, we should start from the groupby's aggregation.
	if !post.HasQueryAggergatorBlock() && post.TransactionArguments == nil {
		post = post.Next
	}

	hasSort := false
	timeSort := false
	timeSortAsc := false
	for agg := post; agg != nil; agg = agg.Next {
		if agg.HasSortBlock() {
			hasSort = true
		}
		if agg.Sort != nil || agg.HasSortBlock() {
			timeSort, timeSortAsc = CheckIfTimeSort(agg)
		}
		err := performAggOnResult(nodeResult, agg, recs, recordIndexInFinal, finalCols, numTotalSegments, finishesSegment, hasSort, timeSort, timeSortAsc)

		if len(nodeResult.TransactionEventRecords) > 0 {
			nodeResult.NextQueryAgg = agg
			return nodeResult
		} else if nodeResult.PerformAggsOnRecs && recs != nil {
			nodeResult.NextQueryAgg = agg
			return nodeResult
		}

		if err != nil {
			log.Errorf("PostQueryBucketCleaning: %v", err)
			nodeResult.ErrList = append(nodeResult.ErrList, err)
		}
	}

	return nodeResult
}

/*
* TODO: The processing logic for SPL commands that need to be implemented is outlined below. These commands may need to be implemented elsewhere.
 1. Stats cmd:
    1.1 stats options: dedup_splitvals, allnum, partitions, delim
    1.2 stats functions: estdc, estdc_error, exactperc99, perc66.6, median, stdev, stdevp, sumsq, upperperc6.6, var, varp, first, last, list, earliest, earliest_time, latest, latest_time, rate
 2. Eval cmd:
    2.1 Mathematical functions: sigfig
    2.2 Statistical eval functions: random
    2.3 Multivalue eval functions: mvappend, mvcount, mvdedup, mvfilter, mvfind, mvindex, mvjoin, mvmap, mvrange, mvsort, mvzip, mv_to_json_array
    2.4 Comparison and Conditional functions: case, coalesce, searchmatch, validate, nullif
    2.5 Conversion functions: ipmask, object_to_array, printf, tojson
    2.6 Date and Time functions: relative_time, time, strftime, strptime
    2.7 Trig and Hyperbolic functions: acos, acosh, asin, asinh, atan, atanh, cos, cosh, sin, sinh, tan, tanh, atan2, hypot
    2.8 Informational functions: cluster, getfields, isnotnull, isnum, typeof
    2.9 Text functions: replace, spath, upper, trim
*/

func performAggOnResult(nodeResult *structs.NodeResult, agg *structs.QueryAggregators, recs map[string]map[string]interface{},
	recordIndexInFinal map[string]int, finalCols map[string]bool, numTotalSegments uint64, finishesSegment bool, hasSort bool, timeSort bool, timeSortAsc bool) error {
	if agg.StreamStatsOptions != nil {
		return PerformStreamStats(nodeResult, agg, recs, recordIndexInFinal, finalCols, finishesSegment, timeSort, timeSortAsc)
	}
	switch agg.PipeCommandType {
	case structs.GenerateEventType:
		return performGenEvent(nodeResult, agg, recs, recordIndexInFinal, finalCols, numTotalSegments, finishesSegment)
	case structs.OutputTransformType:
		if agg.OutputTransforms == nil {
			return errors.New("performAggOnResult: expected non-nil OutputTransforms")
		}

		colReq := agg.OutputTransforms.OutputColumns
		if colReq != nil {
			err := performColumnsRequest(nodeResult, colReq, recs, finalCols)

			if err != nil {
				return fmt.Errorf("performAggOnResult: %v", err)
			}
		}

		if agg.OutputTransforms.LetColumns != nil {
			err := performLetColumnsRequest(nodeResult, agg, agg.OutputTransforms.LetColumns, recs, recordIndexInFinal, finalCols, numTotalSegments, finishesSegment)

			if err != nil {
				return fmt.Errorf("performAggOnResult: %v", err)
			}
		}

		if agg.OutputTransforms.FilterRows != nil {
			err := performFilterRows(nodeResult, agg.OutputTransforms.FilterRows, recs)

			if err != nil {
				return fmt.Errorf("performAggOnResult: %v", err)
			}
		}

		if agg.OutputTransforms.HeadRequest != nil {
			headExpr := agg.OutputTransforms.HeadRequest
			var err error
			if headExpr.BoolExpr != nil {
				err = performConditionalHead(nodeResult, headExpr, recs, recordIndexInFinal, numTotalSegments, finishesSegment, hasSort)
			} else {
				err = performMaxRows(nodeResult, headExpr, agg.OutputTransforms.HeadRequest.MaxRows, recs)
			}
			if err != nil {
				return fmt.Errorf("performAggOnResult: %v", err)
			}
		}

		if agg.OutputTransforms.TailRequest != nil {
			err := performTail(nodeResult, agg.OutputTransforms.TailRequest, recs, recordIndexInFinal, finishesSegment, numTotalSegments, hasSort)

			if err != nil {
				return fmt.Errorf("performAggOnResult: %v", err)
			}
		}
	case structs.GroupByType:
		nodeResult.PerformAggsOnRecs = true
		nodeResult.RecsAggsType = structs.GroupByType
		nodeResult.GroupByCols = agg.GroupByRequest.GroupByColumns
		nodeResult.GroupByRequest = agg.GroupByRequest
	case structs.MeasureAggsType:
		nodeResult.PerformAggsOnRecs = true
		nodeResult.RecsAggsType = structs.MeasureAggsType
		nodeResult.MeasureOperations = agg.MeasureOperations
	case structs.TransactionType:
		performTransactionCommandRequest(nodeResult, agg, recs, finalCols, numTotalSegments, finishesSegment)
	default:
		return errors.New("performAggOnResult: multiple QueryAggregators is currently only supported for OutputTransformType")
	}

	return nil
}

func ExactPerc(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}
	sort.Float64s(values)
	index := (percentile / 100.0) * float64(len(values)-1)
	lowIndex := int(math.Floor(index))
	highIndex := int(math.Ceil(index))
	if lowIndex == highIndex || highIndex >= len(values) {
		return values[lowIndex]
	}
	weight := index - float64(lowIndex)
	return values[lowIndex]*(1-weight) + values[highIndex]*weight
}

func Perc(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}
	sort.Float64s(values)
	index := (percentile / 100.0) * float64(len(values)-1)
	rankIndex := int(math.Round(index))
	return values[rankIndex]
}

func UpperPerc(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return math.NaN()
	}
	sort.Float64s(values)
	index := int(math.Ceil((percentile / 100.0) * float64(len(values)-1)))
	if index >= len(values) {
		index = len(values) - 1
	}
	return values[index]
}

func GetOrderedRecs(recs map[string]map[string]interface{}, recordIndexInFinal map[string]int) ([]string, error) {
	currentOrder := make([]string, len(recs))

	for recordKey := range recs {
		idx, exist := recordIndexInFinal[recordKey]
		if !exist {
			return nil, fmt.Errorf("processSegmentRecordsForHeadExpr: Index not found in recordIndexInFinal for record: %v", recordKey)
		}
		currentOrder[idx] = recordKey
	}

	return currentOrder, nil
}

func performGenEvent(nodeResult *structs.NodeResult, agg *structs.QueryAggregators, recs map[string]map[string]interface{}, recordIndexInFinal map[string]int, finalCols map[string]bool, numTotalSegments uint64, finishesSegment bool) error {
	if agg.GenerateEvent == nil {
		return nil
	}
	if agg.GenerateEvent.GenTimes != nil {
		return performGenTimes(agg, recs, recordIndexInFinal, finalCols)
	}
	if agg.GenerateEvent.InputLookup != nil {
		return performInputLookup(nodeResult, agg, recs, recordIndexInFinal, finalCols, numTotalSegments, finishesSegment)
	}

	return nil
}

func PopulateGeneratedRecords(genEvent *structs.GenerateEvent, recs map[string]map[string]interface{}, recordIndexInFinal map[string]int, finalCols map[string]bool, offset int) error {
	for cols := range genEvent.GeneratedCols {
		finalCols[cols] = true
	}

	for recordKey, recIndex := range genEvent.GeneratedRecordsIndex {
		record, exists := genEvent.GeneratedRecords[recordKey]
		if !exists {
			return fmt.Errorf("PopulateGeneratedRecords: Record not found for recordKey: %v", recordKey)
		}
		recs[recordKey] = record
		recordIndexInFinal[recordKey] = offset + recIndex
	}

	return nil
}

func performGenTimes(agg *structs.QueryAggregators, recs map[string]map[string]interface{}, recordIndexInFinal map[string]int, finalCols map[string]bool) error {
	if agg.GenerateEvent.GenTimes == nil {
		return nil
	}
	if recs == nil {
		return nil
	}

	return PopulateGeneratedRecords(agg.GenerateEvent, recs, recordIndexInFinal, finalCols, 0)
}

func performInputLookup(nodeResult *structs.NodeResult, agg *structs.QueryAggregators, recs map[string]map[string]interface{}, recordIndexInFinal map[string]int, finalCols map[string]bool, numTotalSegments uint64, finishesSegment bool) error {
	if agg.GenerateEvent.InputLookup == nil {
		return nil
	}

	if agg.GenerateEvent.GeneratedRecords == nil {
		err := PerformInputLookup(agg)
		if err != nil {
			return fmt.Errorf("performInputLookup: Error while performing input lookup, err: %v", err)
		}
	}

	if nodeResult.Histogram != nil {
		return performInputLookupOnHistogram(nodeResult, agg)
	}
	// inputLookup for measure results is not supported

	if recs == nil {
		return nil
	}

	if !agg.GenerateEvent.InputLookup.HasPrevResults {
		return PopulateGeneratedRecords(agg.GenerateEvent, recs, recordIndexInFinal, finalCols, 0)
	}

	// When the first block of the last segment arrives update the records and record index once
	if !agg.GenerateEvent.InputLookup.UpdatedRecordIndex && agg.GenerateEvent.InputLookup.NumProcessedSegments == numTotalSegments-1 {
		offset := -1
		for _, recIndex := range recordIndexInFinal {
			if recIndex > offset {
				offset = recIndex
			}
		}
		offset++

		err := PopulateGeneratedRecords(agg.GenerateEvent, recs, recordIndexInFinal, finalCols, offset)
		if err != nil {
			return fmt.Errorf("performInputLookup: Error while populating generated records, err: %v", err)
		}

		agg.GenerateEvent.InputLookup.UpdatedRecordIndex = true
	}

	if finishesSegment {
		agg.GenerateEvent.InputLookup.NumProcessedSegments++
	}

	return nil
}

func performInputLookupOnHistogram(nodeResult *structs.NodeResult, agg *structs.QueryAggregators) error {
	for _, aggregationResult := range nodeResult.Histogram {
		orderedRecs, err := GetOrderedRecs(agg.GenerateEvent.GeneratedRecords, agg.GenerateEvent.GeneratedRecordsIndex)
		if err != nil {
			return fmt.Errorf("performInputLookupOnHistogram: Error while getting generated records order, err: %v", err)
		}

		for _, recordKey := range orderedRecs {
			record, exists := agg.GenerateEvent.GeneratedRecords[recordKey]
			if !exists {
				return fmt.Errorf("performInputLookupOnHistogram: Generated record not found for recordKey: %v", recordKey)
			}

			statRes := make(map[string]segutils.CValueEnclosure, 0)

			for col, recordValue := range record {
				statRes[col] = segutils.CValueEnclosure{
					Dtype: segutils.SS_DT_STRING,
					CVal:  fmt.Sprintf("%v", recordValue),
				}
			}
			// Add the record as bucket result to aggregation results
			bucketRes := &structs.BucketResult{
				StatRes: statRes,
			}

			aggregationResult.Results = append(aggregationResult.Results, bucketRes)
		}
		break
	}

	return nil
}

func performTail(nodeResult *structs.NodeResult, tailExpr *structs.TailExpr, recs map[string]map[string]interface{}, recordIndexInFinal map[string]int, finishesSegment bool, numTotalSegments uint64, hasSort bool) error {

	if nodeResult.Histogram != nil {
		for _, aggResult := range nodeResult.Histogram {
			diff := len(aggResult.Results) - int(tailExpr.TailRows)
			if diff > 0 {
				aggResult.Results = aggResult.Results[diff:]
				tailExpr.TailRows = 0
			} else {
				tailExpr.TailRows -= uint64(len(aggResult.Results))
			}
			n := len(aggResult.Results)
			for i := 0; i < n/2; i++ {
				aggResult.Results[i], aggResult.Results[n-i-1] = aggResult.Results[n-i-1], aggResult.Results[i]
			}
			if tailExpr.TailRows == 0 {
				break
			}
		}

		return nil
	}

	if finishesSegment {
		tailExpr.NumProcessedSegments++
	}

	if tailExpr.TailRecords == nil {
		tailExpr.TailRecords = make(map[string]map[string]interface{}, 0)
	}
	if tailExpr.TailPQ == nil {
		pq := make(utils.PriorityQueue, 0)
		tailExpr.TailPQ = &pq
		heap.Init(tailExpr.TailPQ)
	}

	if !hasSort {
		for recordKey, record := range recs {
			timeVal, exists := record["timestamp"]
			if !exists {
				continue
			}
			heap.Push(tailExpr.TailPQ, &utils.Item{
				Priority: float64(timeVal.(uint64)),
				Value:    recordKey,
			})
			tailExpr.TailRecords[recordKey] = record
			if tailExpr.TailPQ.Len() > int(tailExpr.TailRows) {
				item := heap.Pop(tailExpr.TailPQ).(*utils.Item)
				delete(tailExpr.TailRecords, item.Value)
			}
			delete(recs, recordKey)
		}
	}

	if tailExpr.NumProcessedSegments < numTotalSegments {
		if hasSort && len(recs) > 0 {
			return fmt.Errorf("performTail: Sort was applied but still records are found in recs for a non-last segment")
		}
		return nil
	}

	// if sort is present before use the recs and recordIndexInFinal that sort has updated
	if hasSort {
		currentSortOrder := make([]string, len(recs))
		for recordKey := range recs {
			idx, exists := recordIndexInFinal[recordKey]
			if !exists {
				return fmt.Errorf("performTail: After sort, index not found in recordIndexInFinal for rec: %v", recordKey)
			}
			currentSortOrder[idx] = recordKey
		}
		if tailExpr.TailRows < uint64(len(currentSortOrder)) {
			diff := len(currentSortOrder) - int(tailExpr.TailRows)
			for i := 0; i < diff; i++ {
				delete(recs, currentSortOrder[i])
			}
			currentSortOrder = currentSortOrder[diff:]
		}

		n := len(currentSortOrder)
		for i := 0; i < n/2; i++ {
			currentSortOrder[i], currentSortOrder[n-i-1] = currentSortOrder[n-i-1], currentSortOrder[i]
		}
		for idx, recordKey := range currentSortOrder {
			recordIndexInFinal[recordKey] = idx
		}
	} else {
		for recordKey, record := range tailExpr.TailRecords {
			recs[recordKey] = record
		}

		idx := tailExpr.TailPQ.Len() - 1
		for tailExpr.TailPQ.Len() > 0 {
			item := heap.Pop(tailExpr.TailPQ).(*utils.Item)
			recordIndexInFinal[item.Value] = idx
			idx -= 1
		}
	}

	return nil
}

// only called when headExpr has BoolExpr
func performConditionalHeadOnHistogram(nodeResult *structs.NodeResult, headExpr *structs.HeadExpr) error {
	fieldsInExpr := headExpr.BoolExpr.GetFields()
	fieldToValue := make(map[string]segutils.CValueEnclosure, 0)

	for _, aggregationResult := range nodeResult.Histogram {
		newResults := make([]*structs.BucketResult, 0)

		if !headExpr.Done {
			for rowIndex, bucketResult := range aggregationResult.Results {
				// Get the values of all the necessary fields.
				err := getAggregationResultFieldValues(fieldToValue, fieldsInExpr, aggregationResult, rowIndex)
				if err != nil {
					return fmt.Errorf("performConditionalHeadOnHistogram: error while getting agg result fields values, err: %v", err)
				}

				// Evaluate the expression to a value.
				conditionPassed, err := headExpr.BoolExpr.Evaluate(fieldToValue)
				if err != nil {
					nullFields, errGetNullFields := headExpr.BoolExpr.GetNullFields(fieldToValue)
					if errGetNullFields != nil {
						return fmt.Errorf("performConditionalHeadOnHistogram: Error while getting null fields, err: %v", errGetNullFields)
					} else if len(nullFields) > 0 {
						// evaluation failed due to null fields
						if headExpr.Null {
							newResults = append(newResults, bucketResult)
							headExpr.RowsAdded++
						} else if headExpr.Keeplast {
							newResults = append(newResults, bucketResult)
							headExpr.RowsAdded++
							headExpr.Done = true
							break
						} else {
							headExpr.Done = true
							break
						}
					} else {
						return fmt.Errorf("performConditionalHeadOnHistogram: Error while evaluating expression on histogram, err: %v", err)
					}
				} else {
					if conditionPassed {
						newResults = append(newResults, bucketResult)
						headExpr.RowsAdded++
					} else {
						// false condition so adding last record if keeplast
						if headExpr.Keeplast {
							newResults = append(newResults, bucketResult)
							headExpr.RowsAdded++
						}
						headExpr.Done = true
						break
					}
				}

				if headExpr.MaxRows > 0 && headExpr.RowsAdded == headExpr.MaxRows {
					headExpr.Done = true
					break
				}

			}
		}

		aggregationResult.Results = newResults
	}

	return nil
}

func addRecordToHeadExpr(headExpr *structs.HeadExpr, record map[string]interface{}, recordKey string, hasSort bool) {
	headExpr.RowsAdded++
	if hasSort {
		// we do not need to accumulate the results in case of sort
		return
	}
	headExpr.ResultRecords = append(headExpr.ResultRecords, record)
	headExpr.ResultRecordKeys = append(headExpr.ResultRecordKeys, recordKey)
	delete(headExpr.SegmentRecords, recordKey)
}

func processSegmentRecordsForHeadExpr(headExpr *structs.HeadExpr, recordMap map[string]map[string]interface{}, recordIndexInFinal map[string]int, hasSort bool) error {
	fieldsInExpr := headExpr.BoolExpr.GetFields()
	currentOrder := make([]string, len(recordMap))

	for recordKey := range recordMap {
		idx, exist := recordIndexInFinal[recordKey]
		if !exist {
			return fmt.Errorf("processSegmentRecordsForHeadExpr: Index not found in recordIndexInFinal for record: %v", recordKey)
		}
		currentOrder[idx] = recordKey
	}

	for _, recordKey := range currentOrder {
		rec, exist := recordMap[recordKey]
		if !exist {
			return fmt.Errorf("processSegmentRecordsForHeadExpr: record %v not found in segment records", recordKey)
		}

		fieldToValue := make(map[string]segutils.CValueEnclosure, 0)
		err := getRecordFieldValues(fieldToValue, fieldsInExpr, rec)
		if err != nil {
			return fmt.Errorf("processSegmentRecordsForHeadExpr: Error while retrieving values, err: %v", err)
		}

		conditionPassed, err := headExpr.BoolExpr.Evaluate(fieldToValue)
		if err != nil {
			nullFields, errGetNullFields := headExpr.BoolExpr.GetNullFields(fieldToValue)
			if errGetNullFields != nil {
				return fmt.Errorf("processSegmentRecordsForHeadExpr: Error while getting null fields, err: %v", errGetNullFields)
			} else if len(nullFields) > 0 {
				// evaluation failed due to null fields
				if headExpr.Null {
					addRecordToHeadExpr(headExpr, rec, recordKey, hasSort)
				} else if headExpr.Keeplast {
					addRecordToHeadExpr(headExpr, rec, recordKey, hasSort)
					headExpr.Done = true
					break
				} else {
					headExpr.Done = true
					break
				}
			} else {
				return fmt.Errorf("processSegmentRecordsForHeadExpr: Error while evaluating expression, err: %v", err)
			}
		} else {
			if conditionPassed {
				addRecordToHeadExpr(headExpr, rec, recordKey, hasSort)
			} else {
				// false condition so adding last record if keeplast
				if headExpr.Keeplast {
					addRecordToHeadExpr(headExpr, rec, recordKey, hasSort)
				}
				headExpr.Done = true
				break
			}
		}

		if headExpr.MaxRows > 0 && headExpr.RowsAdded == headExpr.MaxRows {
			headExpr.Done = true
			break
		}
	}

	if hasSort {
		// delete everything after RowsAdded
		for i := headExpr.RowsAdded; i < uint64(len(currentOrder)); i++ {
			delete(recordMap, currentOrder[i])
		}
	} else {
		// we have processed the records, clearing extra records if exists
		for recordKey := range recordMap {
			delete(recordMap, recordKey)
		}
	}

	return nil
}

func processHeadExprWithSort(headExpr *structs.HeadExpr, recs map[string]map[string]interface{}, recordIndexInFinal map[string]int, numTotalSegments uint64, finishesSegment bool) error {
	if !finishesSegment {
		return nil
	}
	headExpr.NumProcessedSegments++
	// if it is the last segment, sort would have populated the records
	if len(recs) > 0 && headExpr.NumProcessedSegments != numTotalSegments {
		return fmt.Errorf("processHeadExprWithSort: Records are present even when there is sort")
	}

	if headExpr.NumProcessedSegments == numTotalSegments {
		return processSegmentRecordsForHeadExpr(headExpr, recs, recordIndexInFinal, true)
	}

	return nil
}

func performConditionalHead(nodeResult *structs.NodeResult, headExpr *structs.HeadExpr, recs map[string]map[string]interface{}, recordIndexInFinal map[string]int, numTotalSegments uint64, finishesSegment bool, hasSort bool) error {

	if nodeResult.Histogram != nil {
		err := performConditionalHeadOnHistogram(nodeResult, headExpr)
		if err != nil {
			return fmt.Errorf("performConditionalHead: Error while filtering histogram, err: %v", err)
		}

		return nil
	}

	if headExpr.SegmentRecords == nil {
		headExpr.SegmentRecords = make(map[string]map[string]interface{}, 0)
	}

	if hasSort {
		return processHeadExprWithSort(headExpr, recs, recordIndexInFinal, numTotalSegments, finishesSegment)
	}

	if headExpr.Done {
		// delete records as we are done
		for recordKey := range recs {
			delete(recs, recordKey)
		}
	} else {
		// accumulate segment records
		for recordKey, record := range recs {
			headExpr.SegmentRecords[recordKey] = record
			delete(recs, recordKey)
		}
	}

	if finishesSegment {
		headExpr.NumProcessedSegments++

		if !headExpr.Done {
			err := processSegmentRecordsForHeadExpr(headExpr, headExpr.SegmentRecords, recordIndexInFinal, hasSort)
			if err != nil {
				return fmt.Errorf("performConditionalHead: Error while processing segment records, err: %v", err)
			}
		}

		if headExpr.NumProcessedSegments == numTotalSegments {
			headExpr.Done = true
			// save the results
			for idx, recordKey := range headExpr.ResultRecordKeys {
				recordIndexInFinal[recordKey] = idx
				recs[recordKey] = headExpr.ResultRecords[idx]
			}
		}
	}

	return nil
}

func performMaxRows(nodeResult *structs.NodeResult, headExpr *structs.HeadExpr, maxRows uint64, recs map[string]map[string]interface{}) error {

	if maxRows == 0 {
		return nil
	}

	if recs != nil {
		// If the number of records plus the already added Rows is less than the maxRows, we don't need to do anything.
		if (uint64(len(recs)) + headExpr.RowsAdded) <= maxRows {
			headExpr.RowsAdded += uint64(len(recs))
			return nil
		}

		// If the number of records is greater than the maxRows, we need to remove the extra records.
		for key := range recs {
			if headExpr.RowsAdded >= maxRows {
				delete(recs, key)
				continue
			}
			headExpr.RowsAdded++
		}

		return nil
	}

	// Follow group by
	if nodeResult.Histogram != nil {
		for _, aggResult := range nodeResult.Histogram {
			if (uint64(len(aggResult.Results)) + headExpr.RowsAdded) <= maxRows {
				headExpr.RowsAdded += uint64(len(aggResult.Results))
				continue
			}

			// If the number of records is greater than the maxRows, we need to remove the extra records.
			aggResult.Results = aggResult.Results[:maxRows-headExpr.RowsAdded]
			headExpr.RowsAdded = maxRows
			break
		}
		return nil
	}

	return nil
}

func performColumnsRequestWithoutGroupby(nodeResult *structs.NodeResult, colReq *structs.ColumnsRequest, recs map[string]map[string]interface{},
	finalCols map[string]bool) error {

	if colReq.RenameAggregationColumns != nil {
		for oldCName, newCName := range colReq.RenameAggregationColumns {
			if _, exists := finalCols[oldCName]; !exists {
				log.Errorf("performColumnsRequestWithoutGroupby: column %v does not exist", oldCName)
				continue
			}
			finalCols[newCName] = true
			delete(finalCols, oldCName)

			if colIndex, exists := nodeResult.ColumnsOrder[oldCName]; exists {
				nodeResult.ColumnsOrder[newCName] = colIndex
				delete(nodeResult.ColumnsOrder, oldCName)
			}

			for _, record := range recs {
				if val, exists := record[oldCName]; exists {
					record[newCName] = val
					delete(record, oldCName)
				}
			}
		}
	}

	if colReq.RenameColumns != nil {
		for oldCName, newCName := range colReq.RenameColumns {
			if _, exists := finalCols[oldCName]; !exists {
				log.Errorf("performColumnsRequestWithoutGroupby: column %v does not exist", oldCName)
				continue
			}
			finalCols[newCName] = true
			delete(finalCols, oldCName)

			for _, record := range recs {
				if val, exists := record[oldCName]; exists {
					record[newCName] = val
					delete(record, oldCName)
				}
			}
		}
	}

	if colReq.ExcludeColumns != nil {
		// Remove the specified columns, which may have wildcards.
		matchingCols := getMatchingColumns(colReq.ExcludeColumns, finalCols)
		for _, matchingCol := range matchingCols {
			delete(finalCols, matchingCol)
		}
	}

	if colReq.IncludeColumns != nil {
		// Remove all columns except the specified ones, which may have wildcards.
		if finalCols == nil {
			return errors.New("performColumnsRequest: finalCols is nil")
		}

		matchingCols := getMatchingColumns(colReq.IncludeColumns, finalCols)

		// First remove everything.
		for col := range finalCols {
			delete(finalCols, col)
		}

		// Add the matching columns.
		for index, matchingCol := range matchingCols {
			finalCols[matchingCol] = true
			nodeResult.ColumnsOrder[matchingCol] = index
		}
	}

	return nil
}

func performColumnsRequest(nodeResult *structs.NodeResult, colReq *structs.ColumnsRequest, recs map[string]map[string]interface{},
	finalCols map[string]bool) error {

	if recs != nil {
		if err := performColumnsRequestWithoutGroupby(nodeResult, colReq, recs, finalCols); err != nil {
			return fmt.Errorf("performColumnsRequest: %v", err)
		}
		return nil
	}

	nodeResult.RenameColumns = colReq.RenameAggregationColumns
RenamingLoop:
	for oldCName, newCName := range colReq.RenameAggregationColumns {
		for i, cName := range nodeResult.MeasureFunctions {
			if cName == oldCName {
				nodeResult.MeasureFunctions[i] = newCName

				// Change the name in MeasureResults.
				for _, bucketHolder := range nodeResult.MeasureResults {
					bucketHolder.MeasureVal[newCName] = bucketHolder.MeasureVal[oldCName]
					delete(bucketHolder.MeasureVal, oldCName)
				}

				// Change the name in Histogram.
				for _, aggResult := range nodeResult.Histogram {
					for _, bucketResult := range aggResult.Results {
						for cName, value := range bucketResult.StatRes {
							if cName == oldCName {
								bucketResult.StatRes[newCName] = value
								delete(bucketResult.StatRes, oldCName)
							}
						}
					}
				}

				continue RenamingLoop
			}
		}

		log.Warnf("performColumnsRequest: column %v does not exist or is not an aggregation column", oldCName)
	}

	if colReq.RenameColumns != nil {

		for oldCName, newCName := range colReq.RenameColumns {
			// Rename in MeasureFunctions
			for i, cName := range nodeResult.MeasureFunctions {
				if cName == oldCName {
					nodeResult.MeasureFunctions[i] = newCName
				}
			}

			// Rename in MeasureResults
			for _, bucketHolder := range nodeResult.MeasureResults {
				if _, exists := bucketHolder.MeasureVal[oldCName]; exists {
					bucketHolder.MeasureVal[newCName] = bucketHolder.MeasureVal[oldCName]
					delete(bucketHolder.MeasureVal, oldCName)
				}
			}

			// Rename in Histogram
			for _, aggResult := range nodeResult.Histogram {
				for _, bucketResult := range aggResult.Results {
					if value, exists := bucketResult.StatRes[oldCName]; exists {
						bucketResult.StatRes[newCName] = value
						delete(bucketResult.StatRes, oldCName)
					}
				}
			}
		}

		return nil
	}

	if colReq.ExcludeColumns != nil {
		if nodeResult.GroupByRequest == nil {
			return errors.New("performColumnsRequest: expected non-nil GroupByRequest while handling ExcludeColumns")
		}

		groupByColIndicesToKeep, groupByColNamesToKeep, _ := getColumnsToKeepAndRemove(nodeResult.GroupByRequest.GroupByColumns, colReq.ExcludeColumns, false)
		_, _, measureColNamesToRemove := getColumnsToKeepAndRemove(nodeResult.MeasureFunctions, colReq.ExcludeColumns, false)

		err := removeAggColumns(nodeResult, groupByColIndicesToKeep, groupByColNamesToKeep, measureColNamesToRemove)
		if err != nil {
			return fmt.Errorf("performColumnsRequest: error handling ExcludeColumns: %v", err)
		}
	}
	if colReq.IncludeColumns != nil {
		if nodeResult.GroupByRequest == nil {
			return errors.New("performColumnsRequest: expected non-nil GroupByRequest while handling IncludeColumns")
		}

		groupByColIndicesToKeep, groupByColNamesToKeep, _ := getColumnsToKeepAndRemove(nodeResult.GroupByRequest.GroupByColumns, colReq.IncludeColumns, true)
		_, _, measureColNamesToRemove := getColumnsToKeepAndRemove(nodeResult.MeasureFunctions, colReq.IncludeColumns, true)
		nodeResult.ColumnsOrder = getColumnsInOrder(nodeResult.GroupByRequest.GroupByColumns, nodeResult.MeasureFunctions, colReq.IncludeColumns)

		err := removeAggColumns(nodeResult, groupByColIndicesToKeep, groupByColNamesToKeep, measureColNamesToRemove)
		if err != nil {
			return fmt.Errorf("performColumnsRequest: error handling IncludeColumns: %v", err)
		}
	}
	if colReq.IncludeValues != nil {
		return errors.New("performColumnsRequest: processing ColumnsRequest.IncludeValues is not implemented")
	}
	if colReq.Logfmt {
		return errors.New("performColumnsRequest: processing ColumnsRequest for Logfmt is not implemented")
	}

	return nil
}

// Return all the columns in finalCols that match any of the wildcardCols,
// which may or may not contain wildcards.
// Note that the results may have duplicates if a column in finalCols matches
// multiple wildcardCols.
func getMatchingColumns(wildcardCols []string, finalCols map[string]bool) []string {
	currentCols := make([]string, len(finalCols))
	i := 0
	for col := range finalCols {
		currentCols[i] = col
		i++
	}

	matchingCols := make([]string, 0)
	for _, wildcardCol := range wildcardCols {
		matchingCols = append(matchingCols, utils.SelectMatchingStringsWithWildcard(wildcardCol, currentCols)...)
	}

	return matchingCols
}

// This function finds which columns in `cols` match any of the wildcardCols,
// which may or may not contain wildcards. It returns the indices and the names
// of the columns to keep, as well as the names of the columns to remove.
// When keepMatches is true, a column is kept only if it matches at least one
// wildcardCol. When keepMatches is false, a column is kept only if it matches
// no wildcardCol.
// The results are returned in the same order as the input `cols`.
func getColumnsToKeepAndRemove(cols []string, wildcardCols []string, keepMatches bool) ([]int, []string, []string) {
	indicesToKeep := make([]int, 0)
	colsToKeep := make([]string, 0)
	colsToRemove := make([]string, 0)

	for i, col := range cols {
		keep := !keepMatches
		for _, wildcardCol := range wildcardCols {
			isMatch := len(utils.SelectMatchingStringsWithWildcard(wildcardCol, []string{col})) > 0
			if isMatch {
				keep = keepMatches
				break
			}
		}

		if keep {
			indicesToKeep = append(indicesToKeep, i)
			colsToKeep = append(colsToKeep, col)
		} else {
			colsToRemove = append(colsToRemove, col)
		}
	}

	return indicesToKeep, colsToKeep, colsToRemove
}

// Maintain fields cmd columns order
func getColumnsInOrder(groupByCols []string, measureFunctions []string, wildcardCols []string) map[string]int {
	columnsOrder := make(map[string]int)

	idx := 0
	for _, wildcardCol := range wildcardCols {
		for _, col := range groupByCols {
			isMatch := len(utils.SelectMatchingStringsWithWildcard(wildcardCol, []string{col})) > 0
			_, exists := columnsOrder[col]
			if isMatch && !exists {
				columnsOrder[col] = idx
				idx++
			}
		}

		for _, col := range measureFunctions {
			isMatch := len(utils.SelectMatchingStringsWithWildcard(wildcardCol, []string{col})) > 0
			_, exists := columnsOrder[col]
			if isMatch && !exists {
				columnsOrder[col] = idx
				idx++
			}
		}
	}

	return columnsOrder
}

func removeAggColumns(nodeResult *structs.NodeResult, groupByColIndicesToKeep []int, groupByColNamesToKeep []string, measureColNamesToRemove []string) error {
	// Remove columns from Histogram.
	for _, aggResult := range nodeResult.Histogram {
		for _, bucketResult := range aggResult.Results {
			bucketResult.GroupByKeys = groupByColNamesToKeep

			// Update the BucketKey.
			bucketKeySlice, err := decodeBucketKey(bucketResult.BucketKey)
			if err != nil {
				return fmt.Errorf("removeAggColumns: failed to decode bucket key %v, err=%v", bucketResult.BucketKey, err)
			}
			bucketKeySlice = utils.SelectIndicesFromSlice(bucketKeySlice, groupByColIndicesToKeep)
			bucketResult.BucketKey = encodeBucketKey(bucketKeySlice)

			// Remove measure columns.
			for _, bucketResult := range aggResult.Results {
				for _, measureColName := range measureColNamesToRemove {
					delete(bucketResult.StatRes, measureColName)
				}
			}
		}
	}

	// Remove columns from MeasureResults.
	for _, bucketHolder := range nodeResult.MeasureResults {
		// Remove groupby columns.
		bucketHolder.GroupByValues = utils.SelectIndicesFromSlice(bucketHolder.GroupByValues, groupByColIndicesToKeep)

		// Remove measure columns.
		for _, measureColName := range measureColNamesToRemove {
			delete(bucketHolder.MeasureVal, measureColName)
		}
	}

	if nodeResult.GroupByRequest == nil {
		return fmt.Errorf("removeAggColumns: expected non-nil GroupByRequest")
	} else {
		nodeResult.GroupByRequest.GroupByColumns = groupByColNamesToKeep
	}

	return nil
}

func performLetColumnsRequest(nodeResult *structs.NodeResult, aggs *structs.QueryAggregators, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{},
	recordIndexInFinal map[string]int, finalCols map[string]bool, numTotalSegments uint64, finishesSegment bool) error {

	if letColReq.NewColName == "" && !aggs.HasQueryAggergatorBlock() && letColReq.StatisticColRequest == nil {
		return errors.New("performLetColumnsRequest: expected non-empty NewColName")
	}

	// Exactly one of MultiColsRequest, SingleColRequest, ValueColRequest, RexColRequest, RenameColRequest should contain data.
	if letColReq.MultiColsRequest != nil {
		return errors.New("performLetColumnsRequest: processing LetColumnsRequest.MultiColsRequest is not implemented")
	} else if letColReq.SingleColRequest != nil {
		return errors.New("performLetColumnsRequest: processing LetColumnsRequest.SingleColRequest is not implemented")
	} else if letColReq.ValueColRequest != nil {
		if err := performValueColRequest(nodeResult, aggs, letColReq, recs, finalCols); err != nil {
			return fmt.Errorf("performLetColumnsRequest: %v", err)
		}
	} else if letColReq.RexColRequest != nil {
		if err := performRexColRequest(nodeResult, aggs, letColReq, recs, finalCols); err != nil {
			return fmt.Errorf("performLetColumnsRequest: %v", err)
		}
	} else if letColReq.RenameColRequest != nil {
		if err := performRenameColRequest(nodeResult, aggs, letColReq, recs, finalCols); err != nil {
			return fmt.Errorf("performLetColumnsRequest: %v", err)
		}
	} else if letColReq.StatisticColRequest != nil {
		if err := performStatisticColRequest(nodeResult, aggs, letColReq, recs); err != nil {
			return fmt.Errorf("performLetColumnsRequest: %v", err)
		}
	} else if letColReq.DedupColRequest != nil {
		if err := performDedupColRequest(nodeResult, aggs, letColReq, recs, finalCols, numTotalSegments, finishesSegment); err != nil {
			return fmt.Errorf("performLetColumnsRequest: %v", err)
		}
	} else if letColReq.SortColRequest != nil {
		if err := performSortColRequest(nodeResult, aggs, letColReq, recs, recordIndexInFinal, finalCols, numTotalSegments, finishesSegment); err != nil {
			return fmt.Errorf("performLetColumnsRequest: %v", err)
		}
	} else if letColReq.MultiValueColRequest != nil {
		if err := performMultiValueColRequest(nodeResult, letColReq, recs); err != nil {
			return fmt.Errorf("performLetColumnsRequest: %v", err)
		}
	} else if letColReq.BinRequest != nil {
		if err := performBinRequest(nodeResult, letColReq, recs, finalCols, recordIndexInFinal, numTotalSegments, finishesSegment); err != nil {
			return fmt.Errorf("performLetColumnsRequest: %v", err)
		}
	} else if letColReq.FillNullRequest != nil {
		if err := performFillNullRequest(nodeResult, letColReq, recs, finalCols, numTotalSegments, finishesSegment); err != nil {
			return fmt.Errorf("performLetColumnsRequest: %v", err)
		}
	} else if letColReq.AppendRequest != nil {
		if err := performAppendRequest(); err != nil {
			return fmt.Errorf("performAppendRequest: %v", err)
		}
	} else {
		return errors.New("performLetColumnsRequest: expected one of MultiColsRequest, SingleColRequest, ValueColRequest, RexColRequest to have a value")
	}

	return nil
}

func performAppendRequest() error {
	return errors.New("append command is not implemented yet")
}

func performRenameColRequest(nodeResult *structs.NodeResult, aggs *structs.QueryAggregators, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{},
	finalCols map[string]bool) error {
	//Without following group by
	if recs != nil {
		if err := performRenameColRequestWithoutGroupby(nodeResult, letColReq, recs, finalCols); err != nil {
			return fmt.Errorf("performRenameColRequest: %v", err)
		}
		return nil
	}

	switch letColReq.RenameColRequest.RenameExprMode {
	case structs.REMPhrase:
		fallthrough
	case structs.REMOverride:
		colIndex, exists := nodeResult.ColumnsOrder[letColReq.RenameColRequest.OriginalPattern]
		if exists {
			delete(nodeResult.ColumnsOrder, letColReq.RenameColRequest.OriginalPattern)
			nodeResult.ColumnsOrder[letColReq.RenameColRequest.NewPattern] = colIndex
		}
	case structs.REMRegex:
		for colName, index := range nodeResult.ColumnsOrder {
			newColName, err := letColReq.RenameColRequest.ProcessRenameRegexExpression(colName)
			if err != nil {
				return fmt.Errorf("performRenameColRequest: %v", err)
			}

			if colName == newColName || len(newColName) == 0 {
				continue
			}

			delete(nodeResult.ColumnsOrder, colName)
			nodeResult.ColumnsOrder[newColName] = index
		}
	}

	//Follow group by
	if err := performRenameColRequestOnHistogram(nodeResult, letColReq); err != nil {
		return fmt.Errorf("performRenameColRequest: %v", err)
	}
	if err := performRenameColRequestOnMeasureResults(nodeResult, letColReq); err != nil {
		return fmt.Errorf("performRenameColRequest: %v", err)
	}

	// Modify the nodeResults.groupByCols
	nodeResult.GroupByCols = structs.GetRenameGroupByCols(nodeResult.GroupByCols, aggs)

	return nil
}

func performRenameColRequestWithoutGroupby(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{},
	finalCols map[string]bool) error {

	fieldsToAdd := make([]string, 0)
	fieldsToRemove := make([]string, 0)

	switch letColReq.RenameColRequest.RenameExprMode {
	case structs.REMPhrase:
		fallthrough
	case structs.REMOverride:

		// Suppose you rename fieldA to fieldB, but fieldA does not exist.
		// If fieldB does not exist, nothing happens.
		// If fieldB does exist, the result of the rename is that the data in fieldB is removed. The data in fieldB will contain null values.
		if _, exist := finalCols[letColReq.RenameColRequest.OriginalPattern]; !exist {
			if _, exist := finalCols[letColReq.RenameColRequest.NewPattern]; !exist {
				return nil
			}
		}

		fieldsToAdd = append(fieldsToAdd, letColReq.RenameColRequest.NewPattern)
		fieldsToRemove = append(fieldsToRemove, letColReq.RenameColRequest.OriginalPattern)
	case structs.REMRegex:
		for colName := range finalCols {
			newColName, err := letColReq.RenameColRequest.ProcessRenameRegexExpression(colName)
			if err != nil {
				return fmt.Errorf("performRenameColRequestWithoutGroupby: %v", err)
			}
			if len(newColName) == 0 {
				continue
			}
			fieldsToAdd = append(fieldsToAdd, newColName)
			fieldsToRemove = append(fieldsToRemove, colName)
		}
	default:
		return fmt.Errorf("performRenameColRequestWithoutGroupby: RenameColRequest has an unexpected type")
	}

	for _, record := range recs {
		for index, newColName := range fieldsToAdd {
			record[newColName] = record[fieldsToRemove[index]]
		}
	}
	for index, newColName := range fieldsToAdd {
		finalCols[newColName] = true
		delete(finalCols, fieldsToRemove[index])

		colIndex, exists := nodeResult.ColumnsOrder[fieldsToRemove[index]]
		if exists {
			delete(nodeResult.ColumnsOrder, fieldsToRemove[index])
			nodeResult.ColumnsOrder[newColName] = colIndex
		}
	}

	return nil
}

func performRenameColRequestOnHistogram(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest) error {

	for _, aggregationResult := range nodeResult.Histogram {
		for _, bucketResult := range aggregationResult.Results {
			switch letColReq.RenameColRequest.RenameExprMode {
			case structs.REMPhrase:
				fallthrough
			case structs.REMOverride:

				// The original pattern should be a field, and the field may come from GroupByCol or the Stat Res. The same rule applies to the new pattern
				// We should delete new pattern key-val pair, and override the original field to new col name

				// If new pattern comes from GroupByCols, we should delete it in the GroupByCols
				for index, groupByCol := range bucketResult.GroupByKeys {
					if groupByCol == letColReq.RenameColRequest.NewPattern {
						letColReq.RenameColRequest.RemoveBucketResGroupByColumnsByIndex(bucketResult, []int{index})
						break
					}
				}

				// If new pattern comes from Stat Res, its key-value pair will be deleted
				delete(bucketResult.StatRes, letColReq.RenameColRequest.NewPattern)

				// After delete new pattern in GroupByCols or Stat Res, we should override the name of original field to new field

				// If original pattern comes from Stat Res
				val, exists := bucketResult.StatRes[letColReq.RenameColRequest.OriginalPattern]
				if exists {
					bucketResult.StatRes[letColReq.RenameColRequest.NewPattern] = val
					delete(bucketResult.StatRes, letColReq.RenameColRequest.OriginalPattern)
					continue
				}

				// If original pattern comes from GroupByCol, just override its name
				for index, groupByCol := range bucketResult.GroupByKeys {
					if letColReq.RenameColRequest.OriginalPattern == groupByCol {
						// The GroupByKeys in the aggregationResult.Results array is a reference slice.
						// If we just modify GroupByKeys in one bucket, the GroupByKeys in other buckets will also be updated
						groupByKeys := make([]string, len(bucketResult.GroupByKeys))
						copy(groupByKeys, bucketResult.GroupByKeys)
						groupByKeys[index] = letColReq.RenameColRequest.NewPattern
						bucketResult.GroupByKeys = groupByKeys
						break
					}
				}

			case structs.REMRegex:

				// If we override original field to a new field, we should remove new field key-val pair and just modify the key name of original field to new field
				//Rename statistic functions name
				for statColName, val := range bucketResult.StatRes {
					newColName, err := letColReq.RenameColRequest.ProcessRenameRegexExpression(statColName)

					if err != nil {
						return fmt.Errorf("performRenameColRequestOnHistogram: %v", err)
					}
					if len(newColName) == 0 {
						continue
					}
					bucketResult.StatRes[newColName] = val
					delete(bucketResult.StatRes, statColName)
				}

				indexToRemove := make([]int, 0)
				//Rename Group by column name
				for index, groupByColName := range bucketResult.GroupByKeys {
					newColName, err := letColReq.RenameColRequest.ProcessRenameRegexExpression(groupByColName)
					if err != nil {
						return fmt.Errorf("performRenameColRequestOnHistogram: %v", err)
					}
					if len(newColName) == 0 {
						continue
					}

					for i, groupByCol := range bucketResult.GroupByKeys {
						if groupByCol == newColName {
							indexToRemove = append(indexToRemove, i)
							break
						}
					}

					groupByKeys := make([]string, len(bucketResult.GroupByKeys))
					copy(groupByKeys, bucketResult.GroupByKeys)
					groupByKeys[index] = newColName
					bucketResult.GroupByKeys = groupByKeys
				}

				letColReq.RenameColRequest.RemoveBucketResGroupByColumnsByIndex(bucketResult, indexToRemove)

			default:
				return fmt.Errorf("performRenameColRequestOnHistogram: RenameColRequest has an unexpected type")
			}
		}
	}

	return nil
}

func performRenameColRequestOnMeasureResults(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest) error {

	// Compute the value for each row.
	for _, bucketHolder := range nodeResult.MeasureResults {

		switch letColReq.RenameColRequest.RenameExprMode {
		case structs.REMPhrase:
			fallthrough
		case structs.REMOverride:

			// If new pattern comes from GroupByCols, we should delete it in the GroupByCols
			for index, groupByCol := range nodeResult.GroupByCols {
				if groupByCol == letColReq.RenameColRequest.NewPattern {
					letColReq.RenameColRequest.RemoveBucketHolderGroupByColumnsByIndex(bucketHolder, nodeResult.GroupByCols, []int{index})
					break
				}
			}

			// If new pattern comes from Stat Res, its key-value pair will be deleted
			delete(bucketHolder.MeasureVal, letColReq.RenameColRequest.NewPattern)

			// After delete new pattern in GroupByCols or MeasureVal, we should override the name of original field to new field

			// If original pattern comes from MeasureVal
			val, exists := bucketHolder.MeasureVal[letColReq.RenameColRequest.OriginalPattern]
			if exists {
				bucketHolder.MeasureVal[letColReq.RenameColRequest.NewPattern] = val
				delete(bucketHolder.MeasureVal, letColReq.RenameColRequest.OriginalPattern)
				continue
			}

			// If original pattern comes from GroupByCol, just override its name
			// There is no GroupByKeys in bucketHolder, so we can skip this step
		case structs.REMRegex:

			//Rename MeasurVal name
			for measureName, val := range bucketHolder.MeasureVal {
				newColName, err := letColReq.RenameColRequest.ProcessRenameRegexExpression(measureName)
				if err != nil {
					return fmt.Errorf("performRenameColRequestOnMeasureResults: %v", err)
				}
				if len(newColName) == 0 {
					continue
				}
				// Being able to match indicates that the original field comes from MeasureVal
				bucketHolder.MeasureVal[newColName] = val
				delete(bucketHolder.MeasureVal, measureName)
			}

			indexToRemove := make([]int, 0)
			//Rename Group by column name
			for _, groupByColName := range nodeResult.GroupByCols {
				newColName, err := letColReq.RenameColRequest.ProcessRenameRegexExpression(groupByColName)
				if err != nil {
					return fmt.Errorf("performRenameColRequestOnMeasureResults: %v", err)
				}
				if len(newColName) == 0 {
					continue
				}

				for i, groupByCol := range nodeResult.GroupByCols {
					if groupByCol == newColName {
						indexToRemove = append(indexToRemove, i)
						break
					}
				}
			}
			letColReq.RenameColRequest.RemoveBucketHolderGroupByColumnsByIndex(bucketHolder, nodeResult.GroupByCols, indexToRemove)
		}
	}
	return nil
}

func performDedupColRequest(nodeResult *structs.NodeResult, aggs *structs.QueryAggregators, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{},
	finalCols map[string]bool, numTotalSegments uint64, finishesSegment bool) error {
	// Without following a group by
	if recs != nil {
		if err := performDedupColRequestWithoutGroupby(nodeResult, letColReq, recs, finalCols, numTotalSegments, finishesSegment); err != nil {
			return fmt.Errorf("performDedupColRequest: %v", err)
		}
		return nil
	}

	// Following a group by
	if err := performDedupColRequestOnHistogram(nodeResult, letColReq); err != nil {
		return fmt.Errorf("performDedupColRequest: %v", err)
	}

	// Reset DedupCombinations so we can use it for computing dedup on the
	// MeasureResults without the deduped records from the Histogram
	// interfering.
	// Note that this is only ok because we never again need the dedup buckets
	// from the Histogram, and this is ok even when there's multiple segments
	// because this post-processing logic is run on group by data only after
	// the data from all the segments has been compiled into one NodeResult.
	letColReq.DedupColRequest.DedupCombinations = make(map[string]map[int][]structs.SortValue, 0)

	if err := performDedupColRequestOnMeasureResults(nodeResult, letColReq); err != nil {
		return fmt.Errorf("performDedupColRequest: %v", err)
	}

	return nil
}

func performDedupColRequestWithoutGroupby(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{},
	finalCols map[string]bool, numTotalSegments uint64, finishesSegment bool) error {

	letColReq.DedupColRequest.AcquireProcessedSegmentsLock()
	defer letColReq.DedupColRequest.ReleaseProcessedSegmentsLock()
	if finishesSegment {
		letColReq.DedupColRequest.NumProcessedSegments++
	}

	// Keep track of all the matched records across all segments, and only run
	// the dedup logic once all the records are gathered.
	if letColReq.DedupColRequest.NumProcessedSegments < numTotalSegments {
		for k, v := range recs {
			letColReq.DedupColRequest.DedupRecords[k] = v
			delete(recs, k)
		}

		return nil
	}

	fieldList := letColReq.DedupColRequest.FieldList
	combinationSlice := make([]interface{}, len(fieldList))
	sortbyValues := make([]structs.SortValue, len(letColReq.DedupColRequest.DedupSortEles))
	sortbyFields := make([]string, len(letColReq.DedupColRequest.DedupSortEles))

	for i, sortEle := range letColReq.DedupColRequest.DedupSortEles {
		sortbyFields[i] = sortEle.Field
		sortbyValues[i] = structs.SortValue{
			InterpretAs: sortEle.Op,
		}
	}

	for k, v := range letColReq.DedupColRequest.DedupRecords {
		recs[k] = v
	}

	recsIndexToKey := make([]string, len(recs))
	recsIndex := 0
	for key, record := range recs {
		// Initialize combination for current row
		for index, field := range fieldList {
			val, exists := record[field]
			if !exists {
				combinationSlice[index] = nil
			} else {
				combinationSlice[index] = val
			}
		}

		for i, field := range sortbyFields {
			val, exists := record[field]
			if !exists {
				val = nil
			}

			sortbyValues[i].Val = fmt.Sprintf("%v", val)
		}

		passes, evictionIndex, err := combinationPassesDedup(combinationSlice, recsIndex, sortbyValues, letColReq.DedupColRequest)
		if err != nil {
			return fmt.Errorf("performDedupColRequestWithoutGroupby: %v", err)
		}

		if evictionIndex != -1 {
			// Evict the item at evictionIndex.
			delete(recs, recsIndexToKey[evictionIndex])
		}

		if !passes {
			if !letColReq.DedupColRequest.DedupOptions.KeepEvents {
				delete(recs, key)
			} else {
				// Keep this record, but clear all the values for the fieldList fields.
				for _, field := range fieldList {
					if _, exists := record[field]; exists {
						record[field] = nil
					}
				}
			}
		}

		recsIndexToKey[recsIndex] = key
		recsIndex++
	}

	return nil
}

func performDedupColRequestOnHistogram(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest) error {
	fieldList := letColReq.DedupColRequest.FieldList
	dedupRawValues := make(map[string]segutils.CValueEnclosure, len(fieldList))
	combinationSlice := make([]interface{}, len(fieldList))
	sortbyRawValues := make(map[string]segutils.CValueEnclosure, len(letColReq.DedupColRequest.DedupSortEles))
	sortbyValues := make([]structs.SortValue, len(letColReq.DedupColRequest.DedupSortEles))
	sortbyFields := make([]string, len(letColReq.DedupColRequest.DedupSortEles))

	for i, sortEle := range letColReq.DedupColRequest.DedupSortEles {
		sortbyFields[i] = sortEle.Field
		sortbyValues[i] = structs.SortValue{
			InterpretAs: sortEle.Op,
		}
	}

	for _, aggregationResult := range nodeResult.Histogram {
		newResults := make([]*structs.BucketResult, 0)
		evictedFromNewResults := make([]bool, 0) // Only used when dedup has a sortby.
		numEvicted := 0

		for bucketIndex, bucketResult := range aggregationResult.Results {
			err := getAggregationResultFieldValues(dedupRawValues, fieldList, aggregationResult, bucketIndex)
			if err != nil {
				return fmt.Errorf("performDedupColRequestOnHistogram: error getting dedup values: %v", err)
			}

			for i, field := range fieldList {
				combinationSlice[i] = dedupRawValues[field]
			}

			// If the dedup has a sortby, get the sort values.
			if len(letColReq.DedupColRequest.DedupSortEles) > 0 {
				err = getAggregationResultFieldValues(sortbyRawValues, sortbyFields, aggregationResult, bucketIndex)
				if err != nil {
					return fmt.Errorf("performDedupColRequestOnHistogram: error getting sort values: %v", err)
				}

				for i, field := range sortbyFields {
					enclosure := sortbyRawValues[field]
					sortbyValues[i].Val, err = enclosure.GetString()
					if err != nil {
						return fmt.Errorf("performDedupColRequestOnHistogram: error converting sort values: %v", err)
					}
				}
			}

			recordIndex := len(newResults)
			passes, evictionIndex, err := combinationPassesDedup(combinationSlice, recordIndex, sortbyValues, letColReq.DedupColRequest)
			if err != nil {
				return fmt.Errorf("performDedupColRequestOnHistogram: %v", err)
			}

			if evictionIndex != -1 {
				// Evict the item at evictionIndex.
				evictedFromNewResults[evictionIndex] = true
				numEvicted++
			}

			if passes {
				newResults = append(newResults, bucketResult)
				evictedFromNewResults = append(evictedFromNewResults, false)
			} else if letColReq.DedupColRequest.DedupOptions.KeepEvents {
				// Keep this bucketResult, but clear all the values for the fieldList fields.

				// Decode the bucketKey into a slice of strings.
				var bucketKeySlice []string
				switch bucketKey := bucketResult.BucketKey.(type) {
				case []string:
					bucketKeySlice = bucketKey
				case string:
					bucketKeySlice = []string{bucketKey}
				default:
					return fmt.Errorf("performDedupColRequestOnHistogram: unexpected type for bucketKey %v", bucketKey)
				}

				for _, field := range fieldList {
					if _, exists := bucketResult.StatRes[field]; exists {
						bucketResult.StatRes[field] = segutils.CValueEnclosure{
							Dtype: segutils.SS_DT_BACKFILL,
						}
					} else {
						for i, groupByCol := range bucketResult.GroupByKeys {
							if groupByCol == field {
								bucketKeySlice[i] = ""
								break
							}
						}
					}
				}

				// Set the bucketKey.
				if len(bucketKeySlice) == 1 {
					bucketResult.BucketKey = bucketKeySlice[0]
				} else {
					bucketResult.BucketKey = bucketKeySlice
				}

				newResults = append(newResults, bucketResult)
				evictedFromNewResults = append(evictedFromNewResults, false)
			}
		}

		// Get the final results by removing the evicted items.
		finalResults := make([]*structs.BucketResult, len(newResults)-numEvicted)
		finalResultsIndex := 0
		for i, bucketResult := range newResults {
			if !evictedFromNewResults[i] {
				finalResults[finalResultsIndex] = bucketResult
				finalResultsIndex++
			}
		}

		aggregationResult.Results = finalResults
	}

	return nil
}

func performDedupColRequestOnMeasureResults(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest) error {
	fieldList := letColReq.DedupColRequest.FieldList
	dedupRawValues := make(map[string]segutils.CValueEnclosure, len(fieldList))
	combinationSlice := make([]interface{}, len(fieldList))
	sortbyRawValues := make(map[string]segutils.CValueEnclosure, len(letColReq.DedupColRequest.DedupSortEles))
	sortbyValues := make([]structs.SortValue, len(letColReq.DedupColRequest.DedupSortEles))
	sortbyFields := make([]string, len(letColReq.DedupColRequest.DedupSortEles))

	for i, sortEle := range letColReq.DedupColRequest.DedupSortEles {
		sortbyFields[i] = sortEle.Field
		sortbyValues[i] = structs.SortValue{
			InterpretAs: sortEle.Op,
		}
	}

	newResults := make([]*structs.BucketHolder, 0)
	evictedFromNewResults := make([]bool, 0) // Only used when dedup has a sortby.
	numEvicted := 0

	for bucketIndex, bucketHolder := range nodeResult.MeasureResults {
		err := getMeasureResultsFieldValues(dedupRawValues, fieldList, nodeResult, bucketIndex)
		if err != nil {
			return fmt.Errorf("performDedupColRequestOnMeasureResults: error getting dedup values: %v", err)
		}

		for i, field := range fieldList {
			combinationSlice[i] = dedupRawValues[field]
		}

		// If the dedup has a sortby, get the sort values.
		if len(letColReq.DedupColRequest.DedupSortEles) > 0 {
			err = getMeasureResultsFieldValues(sortbyRawValues, sortbyFields, nodeResult, bucketIndex)
			if err != nil {
				return fmt.Errorf("performDedupColRequestOnMeasureResults: error getting sort values: %v", err)
			}

			for i, field := range sortbyFields {
				enclosure := sortbyRawValues[field]
				sortbyValues[i].Val, err = enclosure.GetString()
				if err != nil {
					return fmt.Errorf("performDedupColRequestOnMeasureResults: error converting sort values: %v", err)
				}
			}
		}

		recordIndex := len(newResults)
		passes, evictionIndex, err := combinationPassesDedup(combinationSlice, recordIndex, sortbyValues, letColReq.DedupColRequest)
		if err != nil {
			return fmt.Errorf("performDedupColRequestOnMeasureResults: %v", err)
		}

		if evictionIndex != -1 {
			// Evict the item at evictionIndex.
			evictedFromNewResults[evictionIndex] = true
			numEvicted++
		}

		if passes {
			newResults = append(newResults, bucketHolder)
			evictedFromNewResults = append(evictedFromNewResults, false)
		} else if letColReq.DedupColRequest.DedupOptions.KeepEvents {
			// Keep this bucketHolder, but clear all the values for the fieldList fields.
			for _, field := range fieldList {
				if _, exists := bucketHolder.MeasureVal[field]; exists {
					bucketHolder.MeasureVal[field] = nil
				} else {
					for i, groupByCol := range nodeResult.GroupByCols {
						if groupByCol == field {
							bucketHolder.GroupByValues[i] = ""
							break
						}
					}
				}
			}

			newResults = append(newResults, bucketHolder)
			evictedFromNewResults = append(evictedFromNewResults, false)
		}
	}

	// Get the final results by removing the evicted items.
	finalResults := make([]*structs.BucketHolder, len(newResults)-numEvicted)
	finalResultsIndex := 0
	for i, bucketHolder := range newResults {
		if !evictedFromNewResults[i] {
			finalResults[finalResultsIndex] = bucketHolder
			finalResultsIndex++
		}
	}

	nodeResult.MeasureResults = finalResults
	nodeResult.BucketCount = len(newResults)

	return nil
}

// Return whether the combination should be kept, and the index of the record
// that should be evicted if the combination is kept. The returned record index
// is only useful when the dedup has a sortby, and the record index to evict
// will be -1 if nothing should be evicted.
//
// Note: this will update dedupExpr.DedupCombinations if the combination is kept.
// Note: this ignores the dedupExpr.DedupOptions.KeepEvents option; the caller
// is responsible for the extra logic when that is set.
func combinationPassesDedup(combinationSlice []interface{}, recordIndex int, sortValues []structs.SortValue, dedupExpr *structs.DedupExpr) (bool, int, error) {
	// If the keepempty option is set, keep every combination will a nil value.
	// Otherwise, discard every combination with a nil value.
	for _, val := range combinationSlice {
		if val == nil {
			return dedupExpr.DedupOptions.KeepEmpty, -1, nil
		}
	}

	combinationBytes, err := json.Marshal(combinationSlice)
	if err != nil {
		return false, -1, fmt.Errorf("checkDedupCombination: failed to marshal combintion %v: %v", combinationSlice, err)
	}

	combination := string(combinationBytes)
	combinations := dedupExpr.DedupCombinations

	if dedupExpr.DedupOptions.Consecutive {
		// Only remove consecutive duplicates.
		passes := combination != dedupExpr.PrevCombination
		dedupExpr.PrevCombination = combination
		return passes, -1, nil
	}

	recordsMap, exists := combinations[combination]
	if !exists {
		recordsMap = make(map[int][]structs.SortValue, 0)
		combinations[combination] = recordsMap
	}

	if !exists || uint64(len(recordsMap)) < dedupExpr.Limit {
		sortValuesCopy := make([]structs.SortValue, len(sortValues))
		copy(sortValuesCopy, sortValues)
		recordsMap[recordIndex] = sortValuesCopy

		return true, -1, nil
	} else if len(dedupExpr.DedupSortEles) > 0 {

		// Check if this record gets sorted higher than another record with
		// this combination, so it should evict the lowest sorted record.
		foundLower := false
		indexOfLowest := recordIndex
		sortValuesOfLowest := sortValues
		for index, otherSortValues := range recordsMap {
			comparison, err := structs.CompareSortValueSlices(sortValuesOfLowest, otherSortValues, dedupExpr.DedupSortAscending)
			if err != nil {
				err := fmt.Errorf("checkDedupCombination: failed to compare sort values %v and %v: with ascending %v: %v",
					sortValuesOfLowest, otherSortValues, dedupExpr.DedupSortAscending, err)
				return false, -1, err
			}

			if comparison > 0 {
				foundLower = true
				indexOfLowest = index
				sortValuesOfLowest = otherSortValues
			}
		}

		if foundLower {
			delete(recordsMap, indexOfLowest)

			sortValuesCopy := make([]structs.SortValue, len(sortValues))
			copy(sortValuesCopy, sortValues)
			recordsMap[recordIndex] = sortValuesCopy

			return true, indexOfLowest, nil
		} else {
			return false, -1, nil
		}
	}

	return false, -1, nil
}

func performSortColRequest(nodeResult *structs.NodeResult, aggs *structs.QueryAggregators, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{},
	recordIndexInFinal map[string]int, finalCols map[string]bool, numTotalSegments uint64, finishesSegment bool) error {
	// Without following a group by
	if recs != nil {
		if err := performSortColRequestWithoutGroupby(nodeResult, letColReq, recs, recordIndexInFinal, finalCols, numTotalSegments, finishesSegment); err != nil {
			return fmt.Errorf("performSortColRequest: %v", err)
		}
		return nil
	}

	// Following a group by
	if err := performSortColRequestOnHistogram(nodeResult, letColReq); err != nil {
		return fmt.Errorf("performSortColRequest: %v", err)
	}

	if err := performSortColRequestOnMeasureResults(nodeResult, letColReq); err != nil {
		return fmt.Errorf("performSortColRequest: %v", err)
	}

	return nil
}

func performSortColRequestWithoutGroupby(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{},
	recordIndexInFinal map[string]int, finalCols map[string]bool, numTotalSegments uint64, finishesSegment bool) error {

	letColReq.SortColRequest.AcquireProcessedSegmentsLock()
	defer letColReq.SortColRequest.ReleaseProcessedSegmentsLock()
	if finishesSegment {
		letColReq.SortColRequest.NumProcessedSegments++
	}

	if letColReq.SortColRequest.SortRecords == nil {
		letColReq.SortColRequest.SortRecords = make(map[string]map[string]interface{}, 0)
	}

	if letColReq.SortColRequest.NumProcessedSegments < numTotalSegments {
		for k, v := range recs {
			letColReq.SortColRequest.SortRecords[k] = v
			delete(recs, k)
		}

		return nil
	}

	for k, v := range letColReq.SortColRequest.SortRecords {
		recs[k] = v
	}

	recKeys := make([]string, 0)
	keyToSortByValues := make(map[string][]structs.SortValue, 0)
	for recInden, record := range recs {
		recKeys = append(recKeys, recInden)
		sortValue := make([]structs.SortValue, len(letColReq.SortColRequest.SortEles))
		for i, sortEle := range letColReq.SortColRequest.SortEles {
			val, exists := record[sortEle.Field]
			if !exists {
				val = nil
			}

			sortValue[i].Val = fmt.Sprintf("%v", val)
			sortValue[i].InterpretAs = sortEle.Op
		}
		keyToSortByValues[recInden] = sortValue
	}

	// Sort the recKeys array to ensure that keys with higher priority appear first
	sort.Slice(recKeys, func(i, j int) bool {
		key1 := recKeys[i]
		key2 := recKeys[j]
		comparisonRes, err := structs.CompareSortValueSlices(keyToSortByValues[key1], keyToSortByValues[key2], letColReq.SortColRequest.SortAscending)
		if err != nil {
			return true
		}
		return comparisonRes == -1
	})

	for index, recInden := range recKeys {
		recordIndexInFinal[recInden] = index
	}
	return nil
}

func performSortColRequestOnHistogram(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest) error {

	// Setup a map from each of the fields used in this expression to its value for a certain row.
	fieldToValue := make(map[string]segutils.CValueEnclosure, 0)

	sortbyFields := make([]string, len(letColReq.SortColRequest.SortEles))
	for i, sortEle := range letColReq.SortColRequest.SortEles {
		sortbyFields[i] = sortEle.Field
	}

	for _, aggregationResult := range nodeResult.Histogram {
		recKeys := make([]int, 0)
		keyToSortByValues := make(map[int][]structs.SortValue, 0)
		for rowIndex := range aggregationResult.Results {
			recKeys = append(recKeys, rowIndex)

			// Get the values of all the necessary fields.
			err := getAggregationResultFieldValues(fieldToValue, sortbyFields, aggregationResult, rowIndex)
			if err != nil {
				return fmt.Errorf("performSortColRequestOnHistogram: %v", err)
			}
			sortValue := make([]structs.SortValue, len(letColReq.SortColRequest.SortEles))
			for i, sortEle := range letColReq.SortColRequest.SortEles {
				enclosure := fieldToValue[sortEle.Field]
				sortValue[i].Val, err = enclosure.GetString()
				if err != nil {
					return fmt.Errorf("performSortColRequestOnHistogram: error converting sort values: %v", err)
				}
				sortValue[i].InterpretAs = sortEle.Op
			}
			keyToSortByValues[rowIndex] = sortValue
		}

		// Sort aggregationResult.Results' keys and map results to the correct order
		sort.Slice(recKeys, func(i, j int) bool {
			key1 := recKeys[i]
			key2 := recKeys[j]
			comparisonRes, err := structs.CompareSortValueSlices(keyToSortByValues[key1], keyToSortByValues[key2], letColReq.SortColRequest.SortAscending)
			if err != nil {
				return true
			}
			return comparisonRes == -1
		})

		limit := len(aggregationResult.Results)
		if letColReq.SortColRequest.Limit < uint64(len(aggregationResult.Results)) {
			limit = int(letColReq.SortColRequest.Limit)
		}

		resInOrder := make([]*structs.BucketResult, limit)
		for index, key := range recKeys {
			if index >= limit {
				break
			}
			resInOrder[index] = aggregationResult.Results[key]
		}

		aggregationResult.Results = resInOrder
	}

	return nil
}

func performSortColRequestOnMeasureResults(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest) error {

	fieldToValue := make(map[string]segutils.CValueEnclosure, 0)

	sortbyFields := make([]string, len(letColReq.SortColRequest.SortEles))
	for i, sortEle := range letColReq.SortColRequest.SortEles {
		sortbyFields[i] = sortEle.Field
	}

	recKeys := make([]int, 0)
	keyToSortByValues := make(map[int][]structs.SortValue, 0)
	for rowIndex := range nodeResult.MeasureResults {
		recKeys = append(recKeys, rowIndex)

		// Get the values of all the necessary fields.
		err := getMeasureResultsFieldValues(fieldToValue, sortbyFields, nodeResult, rowIndex)
		if err != nil {
			return fmt.Errorf("performSortColRequestOnMeasureResults: %v", err)
		}

		sortValue := make([]structs.SortValue, len(letColReq.SortColRequest.SortEles))
		for i, sortEle := range letColReq.SortColRequest.SortEles {
			enclosure := fieldToValue[sortEle.Field]
			sortValue[i].Val, err = enclosure.GetString()
			if err != nil {
				return fmt.Errorf("performSortColRequestOnMeasureResults: error converting sort values: %v", err)
			}
			sortValue[i].InterpretAs = sortEle.Op
		}
		keyToSortByValues[rowIndex] = sortValue
	}

	sort.Slice(recKeys, func(i, j int) bool {
		key1 := recKeys[i]
		key2 := recKeys[j]
		comparisonRes, err := structs.CompareSortValueSlices(keyToSortByValues[key1], keyToSortByValues[key2], letColReq.SortColRequest.SortAscending)
		if err != nil {
			return true
		}
		return comparisonRes == -1
	})

	limit := len(nodeResult.MeasureResults)
	if letColReq.SortColRequest.Limit < uint64(len(nodeResult.MeasureResults)) {
		limit = int(letColReq.SortColRequest.Limit)
	}

	resInOrder := make([]*structs.BucketHolder, limit)
	for index, key := range recKeys {
		if index >= limit {
			break
		}
		resInOrder[index] = nodeResult.MeasureResults[key]
	}

	nodeResult.MeasureResults = resInOrder
	return nil
}

func performMultiValueColRequest(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}) error {
	if recs != nil {
		if err := performMultiValueColRequestWithoutGroupby(letColReq, recs); err != nil {
			return fmt.Errorf("performMultiValueColRequest: %v", err)
		}
		return nil

	}

	if err := performMultiValueColRequestOnHistogram(nodeResult, letColReq); err != nil {
		return fmt.Errorf("performMultiValueColRequest: %v", err)

	}

	return nil
}

func performMultiValueColRequestWithoutGroupby(letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}) error {
	mvColReq := letColReq.MultiValueColRequest

	newRecs := make(map[string]map[string]interface{})

	for key, rec := range recs {
		fieldValue, ok := rec[mvColReq.ColName]
		if !ok {
			continue
		}

		fieldValueStr, ok := fieldValue.(string)
		if !ok {
			fieldValueStr = fmt.Sprintf("%v", fieldValue) // Convert to string
		}

		switch mvColReq.Command {
		case "makemv":
			finalValue := performMakeMV(fieldValueStr, mvColReq)
			rec[mvColReq.ColName] = finalValue
		case "mvexpand":
			expandedRecs := performMVExpand(fieldValue)

			// Apply limit if mvColReq.Limit is greater than 0
			limit, hasLimit := mvColReq.Limit.Get()
			if hasLimit && len(expandedRecs) > int(limit) {
				expandedRecs = expandedRecs[:limit]
			}
			delete(recs, key)
			for i, expandedValue := range expandedRecs {
				newRec := make(map[string]interface{})
				for k, v := range rec {
					newRec[k] = v
				}
				newRec[mvColReq.ColName] = expandedValue
				newRecs[fmt.Sprintf("%s_%d", key, i)] = newRec
			}
		default:
			return fmt.Errorf("performMultiValueColRequestWithoutGroupby: unknown command %s", mvColReq.Command)
		}
	}
	if mvColReq.Command == "mvexpand" {
		for k, v := range newRecs {
			recs[k] = v
		}
	}
	return nil
}

func performMVExpand(fieldValue interface{}) []interface{} {
	var values []interface{}

	isArrayOrSlice, v, _ := utils.IsArrayOrSlice(fieldValue)
	if !isArrayOrSlice {
		return nil
	}

	for i := 0; i < v.Len(); i++ {
		values = append(values, v.Index(i).Interface())
	}

	return values
}

func performMultiValueColRequestOnHistogram(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest) error {

	for _, aggregationResult := range nodeResult.Histogram {
		for _, bucketResult := range aggregationResult.Results {
			fieldValue, index, isStatRes := bucketResult.GetBucketValueForGivenField(letColReq.MultiValueColRequest.ColName)
			if fieldValue == nil {
				continue
			}
			fieldValueStr, ok := fieldValue.(string)

			if isStatRes && !ok {
				return fmt.Errorf("performMultiValueColRequestOnHistogram: field %s is a statistic result. Cannot perform Multi value string operations on a Statistic result", letColReq.MultiValueColRequest.ColName)
			}

			if !ok {
				fieldValueStr = fmt.Sprintf("%v", fieldValue) // Convert to string
			}

			switch letColReq.MultiValueColRequest.Command {
			case "makemv":
				finalValue := performMakeMV(fieldValueStr, letColReq.MultiValueColRequest)
				err := bucketResult.SetBucketValueForGivenField(letColReq.MultiValueColRequest.ColName, finalValue, index, isStatRes)
				if err != nil {
					log.Errorf("performMultiValueColRequestOnHistogram: error setting bucket value for field %s: %v", letColReq.MultiValueColRequest.ColName, err)
					continue
				}
			default:
				return fmt.Errorf("performMultiValueColRequestOnHistogram: unknown command %s", letColReq.MultiValueColRequest.Command)
			}
		}
	}

	return nil

}

func performMakeMV(strVal string, mvColReq *structs.MultiValueColLetRequest) interface{} {
	if strVal == "" {
		return ""
	}

	var values []string
	if mvColReq.IsRegex {
		re := regexp.MustCompile(mvColReq.DelimiterString)
		matches := re.FindAllStringSubmatch(strVal, -1)
		for _, match := range matches {
			if len(match) > 1 {
				values = append(values, match[1])
			}
		}
	} else {
		values = strings.Split(strVal, mvColReq.DelimiterString)
	}

	if !mvColReq.AllowEmpty {
		// Remove empty values
		var nonEmptyValues []string
		for _, value := range values {
			if value != "" {
				nonEmptyValues = append(nonEmptyValues, value)
			}
		}
		values = nonEmptyValues
	}

	if mvColReq.Setsv {
		// Combine values into a single string
		return strings.Join(values, " ")
	} else {
		// Store the split values
		return values
	}
}

func performFillNullRequest(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}, finalCols map[string]bool, numTotalSegments uint64, finishesSegment bool) error {
	if recs != nil {
		if err := performFillNullRequestWithoutGroupby(nodeResult, letColReq, recs, finalCols); err != nil {
			return fmt.Errorf("performFillNullRequest: %v", err)
		}
		return nil
	}

	// Applying fillnull for MeasureResults or GroupByCols is not possible case. So, we will not handle it.

	return nil
}

func performFillNullRequestWithoutGroupby(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}, finalCols map[string]bool) error {
	fillNullReq := letColReq.FillNullRequest
	currentFillNullRecsCount := len(fillNullReq.Records) + len(recs) // Records that are stored by the fillnull request + records that are currently in recs

	if !nodeResult.RawSearchFinished || currentFillNullRecsCount < nodeResult.CurrentSearchResultCount {
		// If the search is not finished, we cannot fill nulls.
		// If the current records are less than the total search records, we cannot fill nulls.
		// But we need to store the current records for later use and delete them from recs.
		for recIndex, record := range recs {
			fillNullReq.Records[recIndex] = record
			delete(recs, recIndex)
		}

		if len(fillNullReq.FieldList) == 0 {
			// No Fields are provided. This means fill null should be applied to all fields.
			utils.MergeMapsRetainingFirst(fillNullReq.FinalCols, finalCols)
		}

		return nil
	}

	colsToCheck := fillNullReq.FinalCols

	if len(fillNullReq.FieldList) > 0 {
		colsToCheck = make(map[string]bool, 0)
		for _, field := range fillNullReq.FieldList {
			colsToCheck[field] = true
			if _, exists := finalCols[field]; !exists {
				finalCols[field] = true
			}
		}
	} else {

		fillNullColReq := fillNullReq.ColumnsRequest

		for fillNullColReq != nil {
			// Apply any Columns Transforms and deletions that are present in the previous search results.
			err := performColumnsRequestWithoutGroupby(nodeResult, fillNullColReq, nil, nodeResult.AllSearchColumnsByTimeRange)
			if err != nil {
				log.Errorf("performFillNullRequestWithoutGroupby: error applying columns request: %v", err)
			}

			fillNullColReq = fillNullColReq.Next
		}

		// Add any Columns that would be there in the previous search results but not in the current.
		utils.MergeMapsRetainingFirst(colsToCheck, nodeResult.AllSearchColumnsByTimeRange)

		// Check And Add the fields to colsToCheck(fillNullReq.FinalCols) from the current Block Final Cols.
		utils.MergeMapsRetainingFirst(colsToCheck, finalCols)

		// Add all these columns to the finalCols List, so that they are not removed from the final result.
		utils.MergeMapsRetainingFirst(finalCols, colsToCheck)
	}

	for _, record := range recs {
		performFillNullForARecord(record, colsToCheck, fillNullReq.Value)
	}

	for recIndex, record := range fillNullReq.Records {
		if _, exists := recs[recIndex]; exists {
			log.Errorf("performFillNullRequestWithoutGroupby: record with index %s already exists in recs", recIndex)
			continue
		}

		performFillNullForARecord(record, colsToCheck, fillNullReq.Value)
		recs[recIndex] = record
		delete(fillNullReq.Records, recIndex)
	}

	return nil
}

func performFillNullForARecord(record map[string]interface{}, colsToCheck map[string]bool, fillValue string) {
	for field := range colsToCheck {
		value, exists := record[field]
		if value == nil || !exists {
			record[field] = fillValue
		}
	}
}

func performStatisticColRequest(nodeResult *structs.NodeResult, aggs *structs.QueryAggregators, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}) error {

	if err := performStatisticColRequestOnHistogram(nodeResult, letColReq); err != nil {
		return fmt.Errorf("performStatisticColRequest: %v", err)
	}
	if err := performStatisticColRequestOnMeasureResults(nodeResult, letColReq); err != nil {
		return fmt.Errorf("performStatisticColRequest: %v", err)
	}

	return nil
}

func performStatisticColRequestOnHistogram(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest) error {

	countIsGroupByCol := utils.SliceContainsString(letColReq.StatisticColRequest.GetFields(), letColReq.StatisticColRequest.StatisticOptions.CountField)
	percentIsGroupByCol := utils.SliceContainsString(letColReq.StatisticColRequest.GetFields(), letColReq.StatisticColRequest.StatisticOptions.PercentField)

	for _, aggregationResult := range nodeResult.Histogram {

		if len(aggregationResult.Results) == 0 {
			continue
		}
		resTotal := uint64(0)
		for _, bucketResult := range aggregationResult.Results {
			resTotal += (bucketResult.ElemCount)
		}
		//Sort results according to requirements
		err := letColReq.StatisticColRequest.SortBucketResult(&aggregationResult.Results)
		if err != nil {
			return fmt.Errorf("performStatisticColRequestOnHistogram: %v", err)
		}
		//Process bucket result
		otherCnt := resTotal
		for _, bucketResult := range aggregationResult.Results {

			countName := "count(*)"
			newCountName, exists := nodeResult.RenameColumns["count(*)"]
			if exists {
				countName = newCountName
			}
			countIsStatisticGroupByCol := utils.SliceContainsString(letColReq.StatisticColRequest.GetFields(), countName)
			//Delete count generated by the stats groupby block
			if !countIsStatisticGroupByCol {
				delete(bucketResult.StatRes, countName)
			}

			//Delete fields not in statistic expr
			err := letColReq.StatisticColRequest.RemoveFieldsNotInExprForBucketRes(bucketResult)
			if err != nil {
				return fmt.Errorf("performStatisticColRequestOnHistogram: %v", err)
			}

			otherCnt -= (bucketResult.ElemCount)

			// Set the appropriate column to the computed value
			if countIsGroupByCol || percentIsGroupByCol {
				err := letColReq.StatisticColRequest.OverrideGroupByCol(bucketResult, resTotal)
				if err != nil {
					return fmt.Errorf("performStatisticColRequestOnHistogram: %v", err)
				}
			}

			if letColReq.StatisticColRequest.StatisticOptions.ShowCount && !countIsGroupByCol {
				//Set Count to StatResult
				letColReq.StatisticColRequest.SetCountToStatRes(bucketResult.StatRes, bucketResult.ElemCount)
			}

			if letColReq.StatisticColRequest.StatisticOptions.ShowPerc && !percentIsGroupByCol {
				//Set Percent to StatResult
				letColReq.StatisticColRequest.SetPercToStatRes(bucketResult.StatRes, bucketResult.ElemCount, resTotal)
			}
		}

		//If useother=true, a row representing all other values is added to the results.
		if letColReq.StatisticColRequest.StatisticOptions.UseOther {
			statRes := make(map[string]segutils.CValueEnclosure)
			groupByKeys := aggregationResult.Results[0].GroupByKeys
			bucketKey := make([]string, len(groupByKeys))
			otherEnclosure := segutils.CValueEnclosure{
				Dtype: segutils.SS_DT_STRING,
				CVal:  letColReq.StatisticColRequest.StatisticOptions.OtherStr,
			}
			for i := 0; i < len(groupByKeys); i++ {
				if groupByKeys[i] == letColReq.StatisticColRequest.StatisticOptions.CountField || groupByKeys[i] == letColReq.StatisticColRequest.StatisticOptions.PercentField {
					continue
				}
				bucketKey[i] = letColReq.StatisticColRequest.StatisticOptions.OtherStr
			}

			for key := range aggregationResult.Results[0].StatRes {
				if key == letColReq.StatisticColRequest.StatisticOptions.CountField || key == letColReq.StatisticColRequest.StatisticOptions.PercentField {
					continue
				}
				statRes[key] = otherEnclosure
			}

			otherBucketRes := &structs.BucketResult{
				ElemCount:   otherCnt,
				StatRes:     statRes,
				BucketKey:   bucketKey,
				GroupByKeys: groupByKeys,
			}

			if countIsGroupByCol || percentIsGroupByCol {
				err := letColReq.StatisticColRequest.OverrideGroupByCol(otherBucketRes, resTotal)
				if err != nil {
					return fmt.Errorf("performStatisticColRequestOnHistogram: %v", err)
				}
			}

			if letColReq.StatisticColRequest.StatisticOptions.ShowCount && !countIsGroupByCol {
				letColReq.StatisticColRequest.SetCountToStatRes(statRes, otherCnt)
			}

			if letColReq.StatisticColRequest.StatisticOptions.ShowPerc && !percentIsGroupByCol {
				letColReq.StatisticColRequest.SetPercToStatRes(statRes, otherCnt, resTotal)
			}

			aggregationResult.Results = append(aggregationResult.Results, otherBucketRes)
		}
	}

	return nil
}

func performStatisticColRequestOnMeasureResults(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest) error {

	// Because the position of GroupByVals inside the bucketholder is related to nodeResult.GroupByCols
	// If there is a stats groupby block before the statistic block, that mapping relationship is based on the stats groupby cols
	// So we should update it
	preGroupByColToIndex := make(map[string]int, len(nodeResult.GroupByCols))
	for index, groupByCol := range nodeResult.GroupByCols {
		preGroupByColToIndex[groupByCol] = index
	}

	var countIsGroupByCol, percentIsGroupByCol bool
	countColIndex := -1
	percentColIndex := -1
	for i, measureCol := range nodeResult.MeasureFunctions {
		if letColReq.StatisticColRequest.StatisticOptions.ShowCount && letColReq.StatisticColRequest.StatisticOptions.CountField == measureCol {
			// We'll write over this existing column.
			countIsGroupByCol = false
			countColIndex = i
		}

		if letColReq.StatisticColRequest.StatisticOptions.ShowPerc && letColReq.StatisticColRequest.StatisticOptions.PercentField == measureCol {
			// We'll write over this existing column.
			percentIsGroupByCol = false
			percentColIndex = i
		}
	}

	for i, groupByCol := range nodeResult.GroupByCols {
		if letColReq.StatisticColRequest.StatisticOptions.ShowCount && letColReq.StatisticColRequest.StatisticOptions.CountField == groupByCol {
			// We'll write over this existing column.
			countIsGroupByCol = true
			countColIndex = i
		}
		if letColReq.StatisticColRequest.StatisticOptions.ShowPerc && letColReq.StatisticColRequest.StatisticOptions.PercentField == groupByCol {
			// We'll write over this existing column.
			percentIsGroupByCol = true
			percentColIndex = i
		}
	}

	if letColReq.StatisticColRequest.StatisticOptions.ShowCount && countColIndex == -1 {
		nodeResult.MeasureFunctions = append(nodeResult.MeasureFunctions, letColReq.StatisticColRequest.StatisticOptions.CountField)
	}

	if letColReq.StatisticColRequest.StatisticOptions.ShowPerc && percentColIndex == -1 {
		nodeResult.MeasureFunctions = append(nodeResult.MeasureFunctions, letColReq.StatisticColRequest.StatisticOptions.PercentField)
	}

	countName := "count(*)"
	newCountName, exists := nodeResult.RenameColumns["count(*)"]
	if exists {
		countName = newCountName
	}

	resTotal := uint64(0)
	if letColReq.StatisticColRequest.StatisticOptions.ShowPerc {
		for _, bucketHolder := range nodeResult.MeasureResults {
			value, exists := bucketHolder.MeasureVal[countName]
			if !exists {
				bucketHolder.MeasureVal[countName] = uint64(0)
				continue
			}
			resTotal += value.(uint64)
		}
	}

	statisticGroupByCols := letColReq.StatisticColRequest.GetFields()
	// Compute the value for each row.
	for _, bucketHolder := range nodeResult.MeasureResults {

		countVal := bucketHolder.MeasureVal[countName]

		if letColReq.StatisticColRequest.StatisticOptions.ShowCount {
			// Set the appropriate column to the computed value.
			if countIsGroupByCol {
				count, ok := countVal.(uint64)
				if !ok {
					return fmt.Errorf("performStatisticColRequestOnMeasureResults: Can not convert count to uint64")
				}
				bucketHolder.GroupByValues[countColIndex] = strconv.FormatUint(count, 10)
			} else {
				bucketHolder.MeasureVal[letColReq.StatisticColRequest.StatisticOptions.CountField] = countVal
			}
		}

		//Delete count generated by the stats groupby block
		countIsStatisticGroupByCol := utils.SliceContainsString(letColReq.StatisticColRequest.GetFields(), countName)
		if !countIsStatisticGroupByCol {
			delete(bucketHolder.MeasureVal, countName)
		}

		if letColReq.StatisticColRequest.StatisticOptions.ShowPerc {
			count, ok := countVal.(uint64)
			if !ok {
				return fmt.Errorf("performStatisticColRequestOnMeasureResults: Can not convert count to uint64")
			}
			percent := float64(count) / float64(resTotal) * 100
			if percentIsGroupByCol {
				bucketHolder.GroupByValues[percentColIndex] = fmt.Sprintf("%.6f", percent)
			} else {
				bucketHolder.MeasureVal[letColReq.StatisticColRequest.StatisticOptions.PercentField] = fmt.Sprintf("%.6f", percent)
			}
		}

		//Put groupByVals to the correct position
		groupByVals := make([]string, 0)
		for i := 0; i < len(statisticGroupByCols); i++ {
			colName := statisticGroupByCols[i]
			val, exists := bucketHolder.MeasureVal[colName]
			if exists {
				str := ""
				switch v := val.(type) {
				case string:
					str = v
				case []byte:
					str = string(v)
				}
				groupByVals = append(groupByVals, str)
				continue
			}
			index, exists := preGroupByColToIndex[colName]
			if exists {
				groupByVals = append(groupByVals, bucketHolder.GroupByValues[index])
			}
		}
		bucketHolder.GroupByValues = groupByVals
	}
	return nil
}

func performRexColRequest(nodeResult *structs.NodeResult, aggs *structs.QueryAggregators, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}, finalCols map[string]bool) error {

	//Without following group by
	if recs != nil {
		if err := performRexColRequestWithoutGroupby(nodeResult, letColReq, recs, finalCols); err != nil {
			return fmt.Errorf("performRexColRequest: %v", err)
		}
		return nil
	}

	//Follow group by
	if err := performRexColRequestOnHistogram(nodeResult, letColReq); err != nil {
		return fmt.Errorf("performRexColRequest: %v", err)
	}
	if err := performRexColRequestOnMeasureResults(nodeResult, letColReq); err != nil {
		return fmt.Errorf("performRexColRequest: %v", err)
	}

	return nil
}

func performRexColRequestWithoutGroupby(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}, finalCols map[string]bool) error {

	rexExp, err := regexp.Compile(letColReq.RexColRequest.Pattern)
	if err != nil {
		return fmt.Errorf("performRexColRequestWithoutGroupby: There are some errors in the pattern: %v", err)
	}

	fieldName := letColReq.RexColRequest.FieldName
	for _, record := range recs {
		fieldValue := fmt.Sprintf("%v", record[fieldName])
		if len(fieldValue) == 0 {
			return fmt.Errorf("performRexColRequestWithoutGroupby: Field does not exist: %v", fieldName)
		}

		rexResultMap, err := structs.MatchAndExtractGroups(fieldValue, rexExp)
		if err != nil {
			log.Errorf("performRexColRequestWithoutGroupby: %v", err)
			continue
		}

		for rexColName, Value := range rexResultMap {
			record[rexColName] = Value
		}
	}

	for _, rexColName := range letColReq.RexColRequest.RexColNames {
		finalCols[rexColName] = true
	}

	return nil
}

func performRexColRequestOnHistogram(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest) error {

	rexExp, err := regexp.Compile(letColReq.RexColRequest.Pattern)
	if err != nil {
		return fmt.Errorf("performRexColRequestOnHistogram: There are some errors in the pattern: %v", err)
	}

	fieldsInExpr := letColReq.RexColRequest.GetFields()
	fieldToValue := make(map[string]segutils.CValueEnclosure, 0)

	for _, aggregationResult := range nodeResult.Histogram {
		for rowIndex, bucketResult := range aggregationResult.Results {
			err := getAggregationResultFieldValues(fieldToValue, fieldsInExpr, aggregationResult, rowIndex)
			if err != nil {
				return err
			}

			rexColResult, err := letColReq.RexColRequest.Evaluate(fieldToValue, rexExp)
			if err != nil {
				return err
			}
			for rexColName, rexColVal := range rexColResult {
				// Set the appropriate column to the computed value.
				if utils.SliceContainsString(nodeResult.GroupByCols, rexColName) {
					for keyIndex, groupByCol := range bucketResult.GroupByKeys {
						if rexColName != groupByCol {
							continue
						}

						// Set the appropriate element of BucketKey to cellValueStr.
						switch bucketKey := bucketResult.BucketKey.(type) {
						case []string:
							bucketKey[keyIndex] = rexColVal
							bucketResult.BucketKey = bucketKey
						case string:
							if keyIndex != 0 {
								return fmt.Errorf("performRexColRequestOnHistogram: expected keyIndex to be 0, not %v", keyIndex)
							}
							bucketResult.BucketKey = rexColVal
						default:
							return fmt.Errorf("performRexColRequestOnHistogram: bucket key has unexpected type: %T", bucketKey)
						}

					}
				} else {
					aggregationResult.Results[rowIndex].StatRes[rexColName] = segutils.CValueEnclosure{
						Dtype: segutils.SS_DT_STRING,
						CVal:  rexColVal,
					}
				}
			}
		}
	}

	return nil
}

func performRexColRequestOnMeasureResults(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest) error {
	// Check if the column already exists.
	rexColNameInMeasureCol := make([]string, 0)
	rexColNameToGroupByColIndex := make(map[string]int)
	rexColNameInExistingField := make(map[string]bool)

	for _, rexColName := range letColReq.RexColRequest.RexColNames {
		for _, measureCol := range nodeResult.MeasureFunctions {
			if rexColName == measureCol {
				rexColNameInMeasureCol = append(rexColNameInMeasureCol, rexColName)
				rexColNameInExistingField[rexColName] = true
			}
		}

		for i, groupByCol := range nodeResult.GroupByCols {
			if rexColName == groupByCol {
				rexColNameToGroupByColIndex[rexColName] = i
				rexColNameInExistingField[rexColName] = true
			}
		}
	}

	//Append new fields which not in groupby or measurecol to MeasureFunctions
	for _, rexColName := range letColReq.RexColRequest.RexColNames {
		_, exists := rexColNameInExistingField[rexColName]
		if exists {
			nodeResult.MeasureFunctions = append(nodeResult.MeasureFunctions, rexColName)
		}
	}

	// Setup a map from each of the fields used in this expression to its value for a certain row.
	fieldsInExpr := letColReq.RexColRequest.GetFields()
	fieldToValue := make(map[string]segutils.CValueEnclosure, 0)
	rexExp, err := regexp.Compile(letColReq.RexColRequest.Pattern)
	if err != nil {
		return fmt.Errorf("performRexColRequestOnMeasureResults: There are some errors in the pattern: %v", err)
	}
	// Compute the value for each row.
	for rowIndex, bucketHolder := range nodeResult.MeasureResults {
		// Get the values of all the necessary fields.
		err := getMeasureResultsFieldValues(fieldToValue, fieldsInExpr, nodeResult, rowIndex)
		if err != nil {
			return fmt.Errorf("performRexColRequestOnMeasureResults: %v", err)
		}

		rexColResult, err := letColReq.RexColRequest.Evaluate(fieldToValue, rexExp)

		// Evaluate the rex pattern to a value.
		if err != nil {
			return fmt.Errorf("performRexColRequestOnMeasureResults: %v", err)
		}

		for rexColName, index := range rexColNameToGroupByColIndex {
			bucketHolder.GroupByValues[index] = rexColResult[rexColName]
		}

		for _, rexColName := range rexColNameInMeasureCol {
			bucketHolder.MeasureVal[rexColName] = rexColResult[rexColName]
		}
	}
	return nil
}

func performValueColRequest(nodeResult *structs.NodeResult, aggs *structs.QueryAggregators, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}, finalCols map[string]bool) error {
	if recs != nil {
		if err := performValueColRequestWithoutGroupBy(nodeResult, letColReq, recs, finalCols); err != nil {
			return fmt.Errorf("performValueColRequest: %v", err)
		}
		return nil
	}

	if len(nodeResult.AllRecords) > 0 {
		return errors.New("performValueColRequest: ValueColRequest is only implemented for aggregation fields")
	}

	if err := performValueColRequestOnHistogram(nodeResult, letColReq); err != nil {
		return fmt.Errorf("performValueColRequest: %v", err)
	}
	if err := performValueColRequestOnMeasureResults(nodeResult, letColReq); err != nil {
		return fmt.Errorf("performValueColRequest: %v", err)
	}

	return nil
}

// Get the float/numeric value from the record or fieldToValue map if possible
// Should pass either record or fieldToValue
func getFloatValForBin(fieldToValue map[string]segutils.CValueEnclosure, record map[string]interface{}, field string) (float64, error) {
	var fieldValue interface{}
	var exist bool
	if record != nil {
		fieldValue, exist = record[field]
		if !exist {
			return 0, fmt.Errorf("getFloatValForBin: field %s does not exist in record", field)
		}
	} else {
		fieldCValue, exist := fieldToValue[field]
		if !exist {
			return 0, fmt.Errorf("getFloatValForBin: field %s does not exist in record", field)
		}
		fieldValue = fieldCValue.CVal
	}

	fieldValueFloat, err := dtypeutils.ConvertToFloat(fieldValue, 64)
	if err != nil {
		return 0, fmt.Errorf("getFloatValForBin: field %s is not a numeric, has value: %v, err: %v", field, fieldValue, err)
	}

	return fieldValueFloat, nil
}

// Function to find the span range length
func findSpan(minValue float64, maxValue float64, maxBins uint64, minSpan *structs.BinSpanLength, field string) (*structs.BinSpanOptions, error) {
	if field == "timestamp" {
		return findEstimatedTimeSpan(minValue, maxValue, maxBins, minSpan)
	}
	if minValue == maxValue {
		return &structs.BinSpanOptions{
			BinSpanLength: &structs.BinSpanLength{
				Num:       1,
				TimeScale: segutils.TMInvalid,
			},
		}, nil
	}

	// span ranges estimated are in powers of 10
	span := (maxValue - minValue) / float64(maxBins)
	exponent := math.Log10(span)
	exponent = math.Ceil(exponent)
	spanRange := math.Pow(10, exponent)

	// verify if estimated span gives correct number of bins, refer the edge case like 301-500 for bins = 2
	for {
		lowerBound, _ := getBinRange(minValue, spanRange)
		_, upperBound := getBinRange(maxValue, spanRange)

		if (upperBound-lowerBound)/spanRange > float64(maxBins) && spanRange <= math.MaxFloat64/10 {
			spanRange = spanRange * 10
		} else {
			break
		}
	}

	// increase the spanRange till minSpan is satisfied
	if minSpan != nil {
		for {
			if spanRange < minSpan.Num && spanRange <= math.MaxFloat64/10 {
				spanRange = spanRange * 10
			} else {
				break
			}
		}
	}

	return &structs.BinSpanOptions{
		BinSpanLength: &structs.BinSpanLength{
			Num:       spanRange,
			TimeScale: segutils.TMInvalid,
		},
	}, nil
}

// Function to bin ranges with the given span length
func getBinRange(val float64, spanRange float64) (float64, float64) {
	lowerbound := math.Floor(val/spanRange) * spanRange
	upperbound := math.Ceil(val/spanRange) * spanRange
	if lowerbound == upperbound {
		upperbound += spanRange
	}

	return lowerbound, upperbound
}

func getSecsFromMinSpan(minSpan *structs.BinSpanLength) (float64, error) {
	if minSpan == nil {
		return 0, nil
	}

	switch minSpan.TimeScale {
	case segutils.TMMillisecond, segutils.TMCentisecond, segutils.TMDecisecond:
		// smallest granularity of estimated span is 1 second
		return 1, nil
	case segutils.TMSecond:
		return minSpan.Num, nil
	case segutils.TMMinute:
		return minSpan.Num * 60, nil
	case segutils.TMHour:
		return minSpan.Num * 3600, nil
	case segutils.TMDay:
		return minSpan.Num * 86400, nil
	case segutils.TMWeek, segutils.TMMonth, segutils.TMQuarter, segutils.TMYear:
		// default returning num*(seconds in a month)
		return minSpan.Num * 2592000, nil
	default:
		return 0, fmt.Errorf("getSecsFromMinSpan: Invalid time unit: %v", minSpan.TimeScale)
	}
}

// These time ranges are estimated based on different queries executed in splunk, no documentation is present
func findEstimatedTimeSpan(minValueMillis float64, maxValueMillis float64, maxBins uint64, minSpan *structs.BinSpanLength) (*structs.BinSpanOptions, error) {
	minSpanSecs, err := getSecsFromMinSpan(minSpan)
	if err != nil {
		return nil, fmt.Errorf("findEstimatedTimeSpan: Error while getting seconds from minspan, err: %v", err)
	}
	intervalSec := (maxValueMillis/1000 - minValueMillis/1000) / float64(maxBins)
	if minSpanSecs > intervalSec {
		intervalSec = minSpanSecs
	}
	var num float64
	timeUnit := segutils.TMSecond
	if intervalSec < 1 {
		num = 1
	} else if intervalSec <= 10 {
		num = 10
	} else if intervalSec <= 30 {
		num = 30
	} else if intervalSec <= 60 {
		num = 1
		timeUnit = segutils.TMMinute
	} else if intervalSec <= 300 {
		num = 5
		timeUnit = segutils.TMMinute
	} else if intervalSec <= 600 {
		num = 10
		timeUnit = segutils.TMMinute
	} else if intervalSec <= 1800 {
		num = 30
		timeUnit = segutils.TMMinute
	} else if intervalSec <= 3600 {
		num = 1
		timeUnit = segutils.TMHour
	} else if intervalSec <= 86400 {
		num = 1
		timeUnit = segutils.TMDay
	} else {
		// maximum granularity is 1 month as per experiments
		num = 1
		timeUnit = segutils.TMMonth
	}

	estimatedSpan := &structs.BinSpanOptions{
		BinSpanLength: &structs.BinSpanLength{
			Num:       num,
			TimeScale: timeUnit,
		},
	}

	return estimatedSpan, nil
}

// Initial method to perform bin request
func performBinRequest(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}, finalCols map[string]bool, recordIndexInFinal map[string]int, numTotalSegments uint64, finishesSegment bool) error {
	if recs != nil {
		if letColReq.BinRequest.BinSpanOptions != nil {
			return performBinRequestOnRawRecordWithSpan(nodeResult, letColReq, recs, finalCols)
		} else {
			return performBinRequestOnRawRecordWithoutSpan(nodeResult, letColReq, recs, finalCols, recordIndexInFinal, numTotalSegments, finishesSegment)
		}
	}

	if len(nodeResult.Histogram) > 0 {
		err := performBinRequestOnHistogram(nodeResult, letColReq)
		if err != nil {
			return fmt.Errorf("performBinRequest: Error while performing bin request on histogram, err: %v", err)
		}
	}

	if len(nodeResult.MeasureResults) > 0 {
		err := performBinRequestOnMeasureResults(nodeResult, letColReq)
		if err != nil {
			return fmt.Errorf("performBinRequest: Error while performing bin request on measure results, err: %v", err)
		}
	}

	return nil
}

func performBinWithSpanOptions(value float64, spanOptions *structs.BinSpanOptions, binReq *structs.BinCmdOptions) (interface{}, error) {
	if spanOptions != nil {
		if binReq.Field == "timestamp" {
			return performBinWithSpanTime(value, spanOptions, binReq.AlignTime)
		}
		return performBinWithSpan(value, spanOptions)
	}

	return nil, fmt.Errorf("performBinWithSpanOptions: BinSpanOptions is nil")
}

// This function either returns a float or a string
func performBinWithSpan(value float64, spanOpt *structs.BinSpanOptions) (interface{}, error) {
	if spanOpt.BinSpanLength != nil {
		lowerBound, upperBound := getBinRange(value, spanOpt.BinSpanLength.Num)
		if spanOpt.BinSpanLength.TimeScale == segutils.TMInvalid {
			return fmt.Sprintf("%v-%v", lowerBound, upperBound), nil
		} else {
			return lowerBound, nil
		}
	}

	if spanOpt.LogSpan != nil {
		if value <= 0 {
			return value, nil
		}

		val := value / spanOpt.LogSpan.Coefficient
		logVal := math.Log10(val) / math.Log10(spanOpt.LogSpan.Base)
		floorVal := math.Floor(logVal)
		ceilVal := math.Ceil(logVal)
		if ceilVal == floorVal {
			ceilVal += 1
		}
		lowerBound := math.Pow(spanOpt.LogSpan.Base, floorVal) * spanOpt.LogSpan.Coefficient
		upperBound := math.Pow(spanOpt.LogSpan.Base, ceilVal) * spanOpt.LogSpan.Coefficient

		return fmt.Sprintf("%v-%v", lowerBound, upperBound), nil
	}

	return "", fmt.Errorf("performBinWithSpan: BinSpanLength is nil")
}

func getTimeBucketWithAlign(utcTime time.Time, durationScale time.Duration, spanOpt *structs.BinSpanOptions, alignTime *uint64) int {
	if alignTime == nil {
		return int(utcTime.Truncate(time.Duration(spanOpt.BinSpanLength.Num) * durationScale).UnixMilli())
	}

	factorInMillisecond := float64((time.Duration(spanOpt.BinSpanLength.Num) * durationScale) / time.Millisecond)
	currTime := float64(utcTime.UnixMilli())
	baseTime := float64(*alignTime)
	diff := math.Floor((currTime - baseTime) / factorInMillisecond)
	bucket := int(baseTime + diff*factorInMillisecond)
	if bucket < 0 {
		bucket = 0
	}

	return bucket
}

// Find the bucket month based on the given number of months as span.
func findBucketMonth(utcTime time.Time, numOfMonths int) uint64 {
	var finalTime time.Time
	if numOfMonths == 12 {
		finalTime = time.Date(utcTime.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	} else {
		currMonth := int(utcTime.Month())
		month := ((currMonth-1)/numOfMonths)*numOfMonths + 1
		finalTime = time.Date(utcTime.Year(), time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	}

	return uint64(finalTime.UnixMilli())
}

// Perform bin with span for time
func performBinWithSpanTime(value float64, spanOpt *structs.BinSpanOptions, alignTime *uint64) (uint64, error) {
	if spanOpt == nil || spanOpt.BinSpanLength == nil {
		return 0, fmt.Errorf("performBinWithSpanTime: BinSpanLength is nil")
	}

	unixMilli := int64(value)
	utcTime := time.UnixMilli(unixMilli)
	startTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	bucket := 0

	//Align time is only supported for units less than days
	switch spanOpt.BinSpanLength.TimeScale {
	case segutils.TMMillisecond:
		durationScale := time.Millisecond
		bucket = getTimeBucketWithAlign(utcTime, durationScale, spanOpt, alignTime)
	case segutils.TMCentisecond:
		durationScale := time.Millisecond * 10
		bucket = getTimeBucketWithAlign(utcTime, durationScale, spanOpt, alignTime)
	case segutils.TMDecisecond:
		durationScale := time.Millisecond * 100
		bucket = getTimeBucketWithAlign(utcTime, durationScale, spanOpt, alignTime)
	case segutils.TMSecond:
		durationScale := time.Second
		bucket = getTimeBucketWithAlign(utcTime, durationScale, spanOpt, alignTime)
	case segutils.TMMinute:
		durationScale := time.Minute
		bucket = getTimeBucketWithAlign(utcTime, durationScale, spanOpt, alignTime)
	case segutils.TMHour:
		durationScale := time.Hour
		bucket = getTimeBucketWithAlign(utcTime, durationScale, spanOpt, alignTime)
	case segutils.TMDay:
		totalDays := int(utcTime.Sub(startTime).Hours() / 24)
		slotDays := (totalDays / (int(spanOpt.BinSpanLength.Num))) * (int(spanOpt.BinSpanLength.Num))
		bucket = int(startTime.AddDate(0, 0, slotDays).UnixMilli())
	case segutils.TMWeek:
		totalDays := int(utcTime.Sub(startTime).Hours() / 24)
		slotDays := (totalDays / (int(spanOpt.BinSpanLength.Num) * 7)) * (int(spanOpt.BinSpanLength.Num) * 7)
		bucket = int(startTime.AddDate(0, 0, slotDays).UnixMilli())
	case segutils.TMMonth:
		return findBucketMonth(utcTime, int(spanOpt.BinSpanLength.Num)), nil
	case segutils.TMQuarter:
		return findBucketMonth(utcTime, int(spanOpt.BinSpanLength.Num)*3), nil
	case segutils.TMYear:
		num := int(spanOpt.BinSpanLength.Num)
		currYear := int(utcTime.Year())
		bucketYear := ((currYear-1970)/num)*num + 1970
		bucket = int(time.Date(bucketYear, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	default:
		return 0, fmt.Errorf("performBinWithSpanTime: Time scale %v is not supported", spanOpt.BinSpanLength.TimeScale)
	}

	return uint64(bucket), nil
}

func performBinRequestOnRawRecordWithSpan(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}, finalCols map[string]bool) error {
	for _, record := range recs {
		fieldValueFloat, err := getFloatValForBin(nil, record, letColReq.BinRequest.Field)
		if err != nil {
			return fmt.Errorf("performBinRequestOnRawRecordWithSpan: Error while getting numeric value of the field of record, err: %v", err)
		}

		var binValue interface{}
		binValue, err = performBinWithSpanOptions(fieldValueFloat, letColReq.BinRequest.BinSpanOptions, letColReq.BinRequest)

		if err != nil {
			return fmt.Errorf("performBinRequestOnRawRecordWithSpan: Error while performing bin on record, err: %v", err)
		}

		record[letColReq.NewColName] = binValue
	}

	finalCols[letColReq.NewColName] = true

	return nil
}

func performBinRequestOnRawRecordWithoutSpan(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}, finalCols map[string]bool, recordIndexInFinal map[string]int, numTotalSegments uint64, finishesSegment bool) error {
	var err error
	if letColReq.BinRequest.Records == nil {
		letColReq.BinRequest.Records = make(map[string]map[string]interface{}, 0)
	}

	if letColReq.BinRequest.RecordIndex == nil {
		letColReq.BinRequest.RecordIndex = make(map[int]map[string]int, 0)
	}

	_, exist := letColReq.BinRequest.RecordIndex[int(letColReq.BinRequest.NumProcessedSegments)]
	if !exist {
		letColReq.BinRequest.RecordIndex[int(letColReq.BinRequest.NumProcessedSegments)] = make(map[string]int)
	}

	for recordKey, record := range recs {
		letColReq.BinRequest.Records[recordKey] = record
		idx, exist := recordIndexInFinal[recordKey]
		if !exist {
			return fmt.Errorf("performBinRequestOnRawRecordWithoutSpan: Index for record %s does not exist in recordIndexInFinal", recordKey)
		}
		letColReq.BinRequest.RecordIndex[int(letColReq.BinRequest.NumProcessedSegments)][recordKey] = idx
		delete(recs, recordKey)
	}

	if finishesSegment {
		letColReq.BinRequest.NumProcessedSegments++
	}

	if letColReq.BinRequest.NumProcessedSegments < numTotalSegments {
		return nil
	}

	minVal := math.MaxFloat64
	maxVal := -math.MaxFloat64
	// iterate over all records to find min and max values
	for _, record := range letColReq.BinRequest.Records {
		fieldValueFloat, err := getFloatValForBin(nil, record, letColReq.BinRequest.Field)
		if err != nil {
			return fmt.Errorf("performBinRequestOnRawRecordWithoutSpan: Error while getting numeric value of the field of record, err: %v", err)
		}

		if fieldValueFloat < minVal {
			minVal = fieldValueFloat
		}
		if fieldValueFloat > maxVal {
			maxVal = fieldValueFloat
		}
	}

	if letColReq.BinRequest.Field != "timestamp" {
		if letColReq.BinRequest.Start != nil && *letColReq.BinRequest.Start < minVal {
			minVal = *letColReq.BinRequest.Start
		}
		if letColReq.BinRequest.End != nil && *letColReq.BinRequest.End > maxVal {
			maxVal = *letColReq.BinRequest.End
		}
	}

	// Find the span range
	letColReq.BinRequest.BinSpanOptions, err = findSpan(minVal, maxVal, letColReq.BinRequest.MaxBins, letColReq.BinRequest.MinSpan, letColReq.BinRequest.Field)
	if err != nil {
		return fmt.Errorf("performBinRequestOnRawRecordWithoutSpan: Error while finding span, err: %v", err)
	}
	// find the bin value for each record
	for recordKey, record := range letColReq.BinRequest.Records {
		fieldValueFloat, err := getFloatValForBin(nil, record, letColReq.BinRequest.Field)
		if err != nil {
			return fmt.Errorf("performBinRequestOnRawRecordWithoutSpan: Error while getting numeric value for record, err: %v", err)
		}
		binValue, err := performBinWithSpanOptions(fieldValueFloat, letColReq.BinRequest.BinSpanOptions, letColReq.BinRequest)
		if err != nil {
			return fmt.Errorf("performBinRequestOnRawRecordWithoutSpan: Error while performing bin for record, err: %v", err)
		}
		record[letColReq.NewColName] = binValue
		recs[recordKey] = record
	}

	// populate index for each record
	// sort the segnums and then iterate, map iteration is not deterministic
	segNums := make([]int, 0)
	for segNum := range letColReq.BinRequest.RecordIndex {
		segNums = append(segNums, segNum)
	}
	sort.Ints(segNums)
	prevSegCount := 0

	for _, segNum := range segNums {
		for recordKey, recordIndex := range letColReq.BinRequest.RecordIndex[segNum] {
			recordIndexInFinal[recordKey] = prevSegCount + recordIndex
		}
		prevSegCount += len(letColReq.BinRequest.RecordIndex[segNum])
	}

	finalCols[letColReq.NewColName] = true

	return nil
}

func performBinRequestOnHistogram(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest) error {
	var err error
	// Check if the column to create already exists and is a GroupBy column.
	isGroupByCol := utils.SliceContainsString(nodeResult.GroupByCols, letColReq.NewColName)

	// Setup a map for fetching values of field
	fieldsInExpr := []string{letColReq.BinRequest.Field}
	fieldToValue := make(map[string]segutils.CValueEnclosure, 0)

	minVal := math.MaxFloat64
	maxVal := -math.MaxFloat64
	guessSpan := letColReq.BinRequest.BinSpanOptions == nil
	var spanOptions *structs.BinSpanOptions

	if guessSpan {
		// iterate over all records to find min and max values
		for _, aggregationResult := range nodeResult.Histogram {
			for rowIndex := range aggregationResult.Results {
				// Get the values of all the necessary fields.
				err := getAggregationResultFieldValues(fieldToValue, fieldsInExpr, aggregationResult, rowIndex)
				if err != nil {
					return fmt.Errorf("performBinRequestOnHistogram: Error while getting value from agg results, err: %v", err)
				}
				fieldValueFloat, err := getFloatValForBin(fieldToValue, nil, letColReq.BinRequest.Field)
				if err != nil {
					return fmt.Errorf("performBinRequestOnHistogram: Error while getting numeric value from agg results, err: %v", err)
				}
				if fieldValueFloat < minVal {
					minVal = fieldValueFloat
				}
				if fieldValueFloat > maxVal {
					maxVal = fieldValueFloat
				}
			}
		}
		spanOptions, err = findSpan(minVal, maxVal, letColReq.BinRequest.MaxBins, letColReq.BinRequest.MinSpan, letColReq.BinRequest.Field)
		if err != nil {
			return fmt.Errorf("performBinRequestOnHistogram: Error while finding span, err: %v", err)
		}
	} else {
		spanOptions = letColReq.BinRequest.BinSpanOptions
	}

	for _, aggregationResult := range nodeResult.Histogram {
		for rowIndex, bucketResult := range aggregationResult.Results {
			// Get the values of all the necessary fields.
			err := getAggregationResultFieldValues(fieldToValue, fieldsInExpr, aggregationResult, rowIndex)
			if err != nil {
				return fmt.Errorf("performBinRequestOnHistogram: Error while getting value from agg results, err: %v", err)
			}

			fieldValueFloat, err := getFloatValForBin(fieldToValue, nil, letColReq.BinRequest.Field)
			if err != nil {
				return fmt.Errorf("performBinRequestOnHistogram: Error while getting numeric value from agg results, err: %v", err)
			}

			binValue, err := performBinWithSpanOptions(fieldValueFloat, spanOptions, letColReq.BinRequest)
			if err != nil {
				return fmt.Errorf("performBinRequestOnHistogram: Error while performing bin, err: %v", err)
			}

			var valType segutils.SS_DTYPE

			switch binValue.(type) {
			case float64:
				valType = segutils.SS_DT_FLOAT
			case uint64:
				valType = segutils.SS_DT_UNSIGNED_NUM
			case string:
				valType = segutils.SS_DT_STRING
			default:
				return fmt.Errorf("performBinRequestOnHistogram: binValue has unexpected type: %T", binValue)
			}

			// Set the appropriate column to the computed value.
			if isGroupByCol {
				for keyIndex, groupByCol := range bucketResult.GroupByKeys {
					if letColReq.NewColName != groupByCol {
						continue
					}

					binValStr := fmt.Sprintf("%v", binValue)

					// Set the appropriate element of BucketKey to cellValueStr.
					switch bucketKey := bucketResult.BucketKey.(type) {
					case []string:
						bucketKey[keyIndex] = binValStr
						bucketResult.BucketKey = bucketKey
					case string:
						if keyIndex != 0 {
							return fmt.Errorf("performBinRequestOnHistogram: expected keyIndex to be 0, not %v", keyIndex)
						}
						bucketResult.BucketKey = binValStr
					default:
						return fmt.Errorf("performBinRequestOnHistogram: bucket key has unexpected type: %T", bucketKey)
					}
				}
			} else {
				aggregationResult.Results[rowIndex].StatRes[letColReq.NewColName] = segutils.CValueEnclosure{
					Dtype: valType,
					CVal:  binValue,
				}
			}
		}
	}

	return nil
}

func performBinRequestOnMeasureResults(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest) error {
	var err error
	// Check if the column already exists.
	var isGroupByCol bool // If false, it should be a MeasureFunctions column.
	colIndex := -1        // Index in GroupByCols or MeasureFunctions.
	for i, measureCol := range nodeResult.MeasureFunctions {
		if letColReq.NewColName == measureCol {
			// We'll write over this existing column.
			isGroupByCol = false
			colIndex = i
			break
		}
	}

	for i, groupByCol := range nodeResult.GroupByCols {
		if letColReq.NewColName == groupByCol {
			// We'll write over this existing column.
			isGroupByCol = true
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		// Append the column as a MeasureFunctions column.
		isGroupByCol = false
		colIndex = len(nodeResult.MeasureFunctions)
		nodeResult.MeasureFunctions = append(nodeResult.MeasureFunctions, letColReq.NewColName)
	}

	// Setup a map for fetching values of field
	fieldsInExpr := []string{letColReq.BinRequest.Field}
	fieldToValue := make(map[string]segutils.CValueEnclosure, 0)

	minVal := math.MaxFloat64
	maxVal := -math.MaxFloat64
	guessSpan := letColReq.BinRequest.BinSpanOptions == nil
	var spanOptions *structs.BinSpanOptions

	if guessSpan {
		// iterate over all records to find min and max values
		for rowIndex := range nodeResult.MeasureResults {
			// Get the values of all the necessary fields.
			err := getMeasureResultsFieldValues(fieldToValue, fieldsInExpr, nodeResult, rowIndex)
			if err != nil {
				return fmt.Errorf("performBinRequestOnMeasureResults: Error while getting value from measure results, err: %v", err)
			}
			fieldValueFloat, err := getFloatValForBin(fieldToValue, nil, letColReq.BinRequest.Field)
			if err != nil {
				return fmt.Errorf("performBinRequestOnMeasureResults: Error while getting numeric value from measure results, err: %v", err)
			}
			if fieldValueFloat < minVal {
				minVal = fieldValueFloat
			}
			if fieldValueFloat > maxVal {
				maxVal = fieldValueFloat
			}
		}
		spanOptions, err = findSpan(minVal, maxVal, letColReq.BinRequest.MaxBins, letColReq.BinRequest.MinSpan, letColReq.BinRequest.Field)
		if err != nil {
			return fmt.Errorf("performBinRequestOnMeasureResults: Error while finding span, err: %v", err)
		}
	} else {
		spanOptions = letColReq.BinRequest.BinSpanOptions
	}

	// Compute the value for each row.
	for rowIndex, bucketHolder := range nodeResult.MeasureResults {
		// Get the values of all the necessary fields.
		err := getMeasureResultsFieldValues(fieldToValue, fieldsInExpr, nodeResult, rowIndex)
		if err != nil {
			return fmt.Errorf("performBinRequestOnMeasureResults: Error while getting value from measure results, err: %v", err)
		}

		fieldValueFloat, err := getFloatValForBin(fieldToValue, nil, letColReq.BinRequest.Field)
		if err != nil {
			return fmt.Errorf("performBinRequestOnMeasureResults: Error while getting numeric value from measure results, err: %v", err)
		}

		binValue, err := performBinWithSpanOptions(fieldValueFloat, spanOptions, letColReq.BinRequest)
		if err != nil {
			return fmt.Errorf("performBinRequestOnMeasureResults: Error while performing bin, err: %v", err)
		}

		// Set the appropriate column to the computed value.
		if isGroupByCol {
			bucketHolder.GroupByValues[colIndex] = fmt.Sprintf("%v", binValue)
		} else {
			bucketHolder.MeasureVal[letColReq.NewColName] = binValue
		}
	}
	return nil
}

func getRecordFieldValues(fieldToValue map[string]segutils.CValueEnclosure, fieldsInExpr []string, record map[string]interface{}) error {
	for _, field := range fieldsInExpr {
		value, exists := record[field]
		if !exists {
			return fmt.Errorf("getRecordFieldValues: field %v does not exist in record", field)
		}

		dVal, err := segutils.CreateDtypeEnclosure(value, 0)
		if err != nil {
			log.Errorf("failed to create dtype enclosure for field %s, err=%v", field, err)
			dVal = &segutils.DtypeEnclosure{Dtype: segutils.SS_DT_STRING, StringVal: fmt.Sprintf("%v", value), StringValBytes: []byte(fmt.Sprintf("%v", value))}
			value = fmt.Sprintf("%v", value)
		}

		fieldToValue[field] = segutils.CValueEnclosure{Dtype: dVal.Dtype, CVal: value}
	}

	return nil
}

func performValueColRequestWithoutGroupBy(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}, finalCols map[string]bool) error {
	fieldsInExpr := letColReq.ValueColRequest.GetFields()

	if len(fieldsInExpr) == 1 && fieldsInExpr[0] == "*" {
		fieldsInExpr = []string{}
		for _, record := range recs {
			for fieldName := range record {
				fieldsInExpr = append(fieldsInExpr, fieldName)
			}
			break
		}
	}

	for _, record := range recs {
		fieldToValue := make(map[string]segutils.CValueEnclosure, 0)
		err := getRecordFieldValues(fieldToValue, fieldsInExpr, record)
		if err != nil {
			log.Errorf("performValueColRequestWithoutGroupBy: %v", err)
			continue
		}

		value, err := performValueColRequestOnRawRecord(letColReq, fieldToValue)
		if err != nil {
			log.Errorf("performValueColRequestWithoutGroupBy: %v", err)
			continue
		}

		record[letColReq.NewColName] = value
	}
	finalCols[letColReq.NewColName] = true

	return nil
}

func performValueColRequestOnRawRecord(letColReq *structs.LetColumnsRequest, fieldToValue map[string]segutils.CValueEnclosure) (interface{}, error) {
	if letColReq == nil || letColReq.ValueColRequest == nil {
		return nil, fmt.Errorf("invalid letColReq")
	}

	return letColReq.ValueColRequest.EvaluateValueExpr(fieldToValue)
}

func performValueColRequestOnHistogram(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest) error {
	// Check if the column to create already exists and is a GroupBy column.
	isGroupByCol := utils.SliceContainsString(nodeResult.GroupByCols, letColReq.NewColName)

	// Setup a map from each of the fields used in this expression to its value for a certain row.
	fieldsInExpr := letColReq.ValueColRequest.GetFields()
	fieldToValue := make(map[string]segutils.CValueEnclosure, 0)

	for _, aggregationResult := range nodeResult.Histogram {
		for rowIndex, bucketResult := range aggregationResult.Results {
			// Get the values of all the necessary fields.
			err := getAggregationResultFieldValues(fieldToValue, fieldsInExpr, aggregationResult, rowIndex)
			if err != nil {
				return fmt.Errorf("performValueColRequestOnHistogram: %v", err)
			}

			// Evaluate the expression to a value. We do not know this expression represent a number or str
			//Firstly, try to evaluate it as a float, if it fail. Try to evaluate it as a str
			var cellValueStr string
			var cellValueFloat float64
			var cellValueSlice []string
			switch letColReq.ValueColRequest.ValueExprMode {
			case structs.VEMConditionExpr:
				err := getAggregationResultFieldValues(fieldToValue, fieldsInExpr, aggregationResult, rowIndex)
				if err != nil {
					return fmt.Errorf("performValueColRequestOnHistogram: %v", err)
				}
				// Evaluate the condition expression to a value.
				cellValue, err := letColReq.ValueColRequest.ConditionExpr.EvaluateCondition(fieldToValue)
				if err != nil {
					return fmt.Errorf("performValueColRequestOnHistogram: %v", err)
				}
				if cellValue != nil {
					cellValueStr = fmt.Sprintf("%v", cellValue)
				}
			case structs.VEMStringExpr:
				cellValueStr, err = letColReq.ValueColRequest.EvaluateValueExprAsString(fieldToValue)
				if err != nil {
					return fmt.Errorf("performValueColRequestOnHistogram: %v", err)
				}
			case structs.VEMNumericExpr:
				cellValueFloat, err = letColReq.ValueColRequest.EvaluateToFloat(fieldToValue)
				if err != nil {
					return fmt.Errorf("performValueColRequestOnHistogram: %v", err)
				}
			case structs.VEMBooleanExpr:
				cellValueStr, err = letColReq.ValueColRequest.EvaluateToString(fieldToValue)
				if err != nil {
					return fmt.Errorf("performValueColRequestOnHistogram: %v", err)
				}
			case structs.VEMMultiValueExpr:
				cellValueSlice, err = letColReq.ValueColRequest.EvaluateToMultiValue(fieldToValue)
				if err != nil {
					return fmt.Errorf("failed to evaluate multi value expr, err: %v", err)
				}
			}

			if err != nil {
				return fmt.Errorf("performValueColRequestOnHistogram: %v", err)
			}

			// Set the appropriate column to the computed value.
			if isGroupByCol {
				for keyIndex, groupByCol := range bucketResult.GroupByKeys {
					if letColReq.NewColName != groupByCol {
						continue
					}

					if len(cellValueStr) == 0 {
						if len(cellValueSlice) > 0 {
							cellValueStr = fmt.Sprintf("%v", cellValueSlice)
						} else {
							cellValueStr = fmt.Sprintf("%v", cellValueFloat)
						}
					}

					// Set the appropriate element of BucketKey to cellValueStr.
					switch bucketKey := bucketResult.BucketKey.(type) {
					case []string:
						bucketKey[keyIndex] = cellValueStr
						bucketResult.BucketKey = bucketKey
					case string:
						if keyIndex != 0 {
							return fmt.Errorf("performValueColRequestOnHistogram: expected keyIndex to be 0, not %v", keyIndex)
						}
						bucketResult.BucketKey = cellValueStr
					default:
						return fmt.Errorf("performValueColRequestOnHistogram: bucket key has unexpected type: %T", bucketKey)
					}
				}
			} else {
				if len(cellValueSlice) > 0 {
					aggregationResult.Results[rowIndex].StatRes[letColReq.NewColName] = segutils.CValueEnclosure{
						Dtype: segutils.SS_DT_STRING_SLICE,
						CVal:  cellValueSlice,
					}
				} else if len(cellValueStr) > 0 {
					aggregationResult.Results[rowIndex].StatRes[letColReq.NewColName] = segutils.CValueEnclosure{
						Dtype: segutils.SS_DT_STRING,
						CVal:  cellValueStr,
					}
				} else {
					aggregationResult.Results[rowIndex].StatRes[letColReq.NewColName] = segutils.CValueEnclosure{
						Dtype: segutils.SS_DT_FLOAT,
						CVal:  cellValueFloat,
					}
				}
			}
		}
	}

	return nil
}

func performValueColRequestOnMeasureResults(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest) error {
	// Check if the column already exists.
	var isGroupByCol bool // If false, it should be a MeasureFunctions column.
	colIndex := -1        // Index in GroupByCols or MeasureFunctions.
	for i, measureCol := range nodeResult.MeasureFunctions {
		if letColReq.NewColName == measureCol {
			// We'll write over this existing column.
			isGroupByCol = false
			colIndex = i
			break
		}
	}

	for i, groupByCol := range nodeResult.GroupByCols {
		if letColReq.NewColName == groupByCol {
			// We'll write over this existing column.
			isGroupByCol = true
			colIndex = i
			break
		}
	}

	if colIndex == -1 {
		// Append the column as a MeasureFunctions column.
		isGroupByCol = false
		colIndex = len(nodeResult.MeasureFunctions)
		nodeResult.MeasureFunctions = append(nodeResult.MeasureFunctions, letColReq.NewColName)
	}

	// Setup a map from each of the fields used in this expression to its value for a certain row.
	fieldsInExpr := letColReq.ValueColRequest.GetFields()
	fieldToValue := make(map[string]segutils.CValueEnclosure, 0)

	// Compute the value for each row.
	for rowIndex, bucketHolder := range nodeResult.MeasureResults {
		// Get the values of all the necessary fields.
		err := getMeasureResultsFieldValues(fieldToValue, fieldsInExpr, nodeResult, rowIndex)
		if err != nil {
			return fmt.Errorf("performValueColRequestOnMeasureResults: %v", err)
		}

		// Evaluate the expression to a value.
		cellValueStr, err := letColReq.ValueColRequest.EvaluateToString(fieldToValue)
		if err != nil {
			return fmt.Errorf("performValueColRequestOnMeasureResults: %v", err)
		}

		// Set the appropriate column to the computed value.
		if isGroupByCol {
			bucketHolder.GroupByValues[colIndex] = cellValueStr
		} else {
			bucketHolder.MeasureVal[letColReq.NewColName] = cellValueStr
		}
	}
	return nil
}

func performFilterRows(nodeResult *structs.NodeResult, filterRows *structs.BoolExpr, recs map[string]map[string]interface{}) error {

	if recs != nil {
		if err := performFilterRowsWithoutGroupBy(filterRows, recs); err != nil {
			return fmt.Errorf("performFilterRows: %v", err)
		}
		return nil
	}

	// Ensure all referenced columns are valid.
	for _, field := range filterRows.GetFields() {
		if !utils.SliceContainsString(nodeResult.GroupByCols, field) &&
			!utils.SliceContainsString(nodeResult.MeasureFunctions, field) {

			return fmt.Errorf("performFilterRows: invalid field: %v", field)
		}
	}

	if err := performFilterRowsOnHistogram(nodeResult, filterRows); err != nil {
		return fmt.Errorf("performFilterRows: %v", err)
	}
	if err := performFilterRowsOnMeasureResults(nodeResult, filterRows); err != nil {
		return fmt.Errorf("performFilterRows: %v", err)
	}

	return nil
}

func performFilterRowsWithoutGroupBy(filterRows *structs.BoolExpr, recs map[string]map[string]interface{}) error {
	fieldsInExpr := filterRows.GetFields()

	for key, record := range recs {
		fieldToValue := make(map[string]segutils.CValueEnclosure, 0)
		err := getRecordFieldValues(fieldToValue, fieldsInExpr, record)
		if err != nil {
			log.Errorf("performFilterRowsWithoutGroupBy: %v", err)
			continue
		}

		shouldKeep, err := filterRows.Evaluate(fieldToValue)
		if err != nil {
			log.Errorf("performFilterRowsWithoutGroupBy: %v", err)
			continue
		}

		if !shouldKeep {
			delete(recs, key)
		}
	}

	return nil
}

func performFilterRowsOnHistogram(nodeResult *structs.NodeResult, filterRows *structs.BoolExpr) error {
	fieldsInExpr := filterRows.GetFields()
	fieldToValue := make(map[string]segutils.CValueEnclosure, 0)

	for _, aggregationResult := range nodeResult.Histogram {
		newResults := make([]*structs.BucketResult, 0, len(aggregationResult.Results))

		for rowIndex, bucketResult := range aggregationResult.Results {
			// Get the values of all the necessary fields.
			err := getAggregationResultFieldValues(fieldToValue, fieldsInExpr, aggregationResult, rowIndex)
			if err != nil {
				return fmt.Errorf("performFilterRowsOnHistogram: %v", err)
			}

			// Evaluate the expression to a value.
			shouldKeep, err := filterRows.Evaluate(fieldToValue)
			if err != nil {
				return fmt.Errorf("performFilterRowsOnHistogram: failed to evaluate condition: %v", err)
			}

			if shouldKeep {
				newResults = append(newResults, bucketResult)
			}
		}

		aggregationResult.Results = newResults
	}

	return nil
}

func performFilterRowsOnMeasureResults(nodeResult *structs.NodeResult, filterRows *structs.BoolExpr) error {
	fieldsInExpr := filterRows.GetFields()
	fieldToValue := make(map[string]segutils.CValueEnclosure, 0)
	newMeasureResults := make([]*structs.BucketHolder, 0, len(nodeResult.MeasureResults))

	for rowIndex, bucketHolder := range nodeResult.MeasureResults {
		// Get the values of all the necessary fields.
		err := getMeasureResultsFieldValues(fieldToValue, fieldsInExpr, nodeResult, rowIndex)
		if err != nil {
			return fmt.Errorf("performFilterRowsOnMeasureResults: %v", err)
		}

		// Evaluate the expression to a value.
		shouldKeep, err := filterRows.Evaluate(fieldToValue)
		if err != nil {
			return fmt.Errorf("performFilterRowsOnMeasureResults: failed to evaluate condition: %v", err)
		}

		if shouldKeep {
			newMeasureResults = append(newMeasureResults, bucketHolder)
		}
	}

	nodeResult.MeasureResults = newMeasureResults
	return nil
}

func getMeasureResultsCell(nodeResult *structs.NodeResult, rowIndex int, col string) (interface{}, bool) {
	if value, ok := getMeasureResultsMeasureFunctionCell(nodeResult, rowIndex, col); ok {
		return value, true
	}
	if value, ok := getMeasureResultsGroupByCell(nodeResult, rowIndex, col); ok {
		return value, true
	}

	return nil, false
}

func getMeasureResultsMeasureFunctionCell(nodeResult *structs.NodeResult, rowIndex int, measureCol string) (interface{}, bool) {
	value, ok := nodeResult.MeasureResults[rowIndex].MeasureVal[measureCol]
	return value, ok
}

func getMeasureResultsGroupByCell(nodeResult *structs.NodeResult, rowIndex int, groupByCol string) (string, bool) {
	for i, col := range nodeResult.GroupByCols {
		if groupByCol == col {
			return nodeResult.MeasureResults[rowIndex].GroupByValues[i], true
		}
	}

	return "", false
}

func getAggregationResultCell(aggResult *structs.AggregationResult, rowIndex int, col string) (interface{}, bool) {
	if value, ok := getAggregationResultMeasureFunctionCell(aggResult, rowIndex, col); ok {
		return value, true
	}
	if value, ok := getAggregationResultGroupByCell(aggResult, rowIndex, col); ok {
		return value, true
	}

	return nil, false
}

func getAggregationResultMeasureFunctionCell(aggResult *structs.AggregationResult, rowIndex int, measureCol string) (segutils.CValueEnclosure, bool) {
	value, ok := aggResult.Results[rowIndex].StatRes[measureCol]
	return value, ok
}

func getAggregationResultGroupByCell(aggResult *structs.AggregationResult, rowIndex int, groupByCol string) (string, bool) {
	for keyIndex, groupByKey := range aggResult.Results[rowIndex].GroupByKeys {
		if groupByCol != groupByKey {
			continue
		}

		// Index into BucketKey.
		switch bucketKey := aggResult.Results[rowIndex].BucketKey.(type) {
		case []string:
			return bucketKey[keyIndex], true
		case string:
			if keyIndex != 0 {
				log.Errorf("getAggregationResultGroupByCell: expected keyIndex to be 0, not %v", keyIndex)
				return "", false
			}
			return bucketKey, true
		default:
			log.Errorf("getAggregationResultGroupByCell: bucket key has unexpected type: %T", bucketKey)
			return "", false
		}
	}

	return "", false
}

// Replaces values in `fieldToValue` for the specified `fields`, but doesn't
// remove the extra entries in `fieldToValue`.
func getMeasureResultsFieldValues(fieldToValue map[string]segutils.CValueEnclosure, fields []string,
	nodeResult *structs.NodeResult, rowIndex int) error {

	for _, field := range fields {
		var enclosure segutils.CValueEnclosure

		value, ok := getMeasureResultsCell(nodeResult, rowIndex, field)
		if !ok {
			return fmt.Errorf("getMeasureResultsFieldValues: failed to extract field %v from row %v of MeasureResults", field, rowIndex)
		}

		switch value := value.(type) {
		case string:
			enclosure.Dtype = segutils.SS_DT_STRING
			enclosure.CVal = value
		case float64:
			enclosure.Dtype = segutils.SS_DT_FLOAT
			enclosure.CVal = value
		case uint64:
			enclosure.Dtype = segutils.SS_DT_UNSIGNED_NUM
			enclosure.CVal = value
		case int64:
			enclosure.Dtype = segutils.SS_DT_SIGNED_NUM
			enclosure.CVal = value
		default:
			return fmt.Errorf("getMeasureResultsFieldValues: expected field to have a string or float value but got %T", value)
		}

		fieldToValue[field] = enclosure
	}

	return nil
}

// Replaces values in `fieldToValue` for the specified `fields`, but doesn't
// remove the extra entries in `fieldToValue`.
func getAggregationResultFieldValues(fieldToValue map[string]segutils.CValueEnclosure, fields []string,
	aggResult *structs.AggregationResult, rowIndex int) error {

	for _, field := range fields {
		var enclosure segutils.CValueEnclosure
		value, ok := getAggregationResultCell(aggResult, rowIndex, field)
		if !ok {
			return fmt.Errorf("getAggregationResultFieldValues: failed to extract field %v from row %v of AggregationResult", field, rowIndex)
		}

		switch value := value.(type) {
		case string:
			enclosure.Dtype = segutils.SS_DT_STRING
			enclosure.CVal = value
		case segutils.CValueEnclosure:
			enclosure = value
		default:
			return fmt.Errorf("getAggregationResultFieldValues: expected field to have a string or float value but got %T", value)
		}

		fieldToValue[field] = enclosure
	}

	return nil
}

func performTransactionCommandRequest(nodeResult *structs.NodeResult, aggs *structs.QueryAggregators, recs map[string]map[string]interface{}, finalCols map[string]bool, numTotalSegments uint64, finishesSegment bool) {

	if recs != nil {

		if nodeResult.TransactionEventRecords == nil {
			nodeResult.TransactionEventRecords = make(map[string]map[string]interface{})
		}

		if nodeResult.TransactionsProcessed == nil {
			nodeResult.TransactionsProcessed = make(map[string]map[string]interface{}, 0)
		}

		if aggs.TransactionArguments.SortedRecordsSlice == nil {
			aggs.TransactionArguments.SortedRecordsSlice = make([]map[string]interface{}, 0)
		}

		for k, v := range recs {
			nodeResult.TransactionEventRecords[k] = recs[k]
			aggs.TransactionArguments.SortedRecordsSlice = append(aggs.TransactionArguments.SortedRecordsSlice, map[string]interface{}{"key": k, "timestamp": v["timestamp"]})
			delete(recs, k)
		}

		var cols []string
		var err error

		if finishesSegment {
			nodeResult.RecsAggsProcessedSegments++

			// Sort the records by timestamp. The records in the segment may not be sorted. We need to sort them before processing.
			// This method also assumes that all records in the segment will come before the records in the next segment(Segments are Sorted).
			sort.Slice(aggs.TransactionArguments.SortedRecordsSlice, func(i, j int) bool {
				return aggs.TransactionArguments.SortedRecordsSlice[i]["timestamp"].(uint64) < aggs.TransactionArguments.SortedRecordsSlice[j]["timestamp"].(uint64)
			})

			cols, err = processTransactionsOnRecords(nodeResult.TransactionEventRecords, nodeResult.TransactionsProcessed, nil, aggs.TransactionArguments, nodeResult.RecsAggsProcessedSegments == numTotalSegments)
			if err != nil {
				log.Errorf("performTransactionCommandRequest: %v", err)
				return
			}

			nodeResult.TransactionEventRecords = nil // Clear the transaction records. Release the memory.
			nodeResult.TransactionEventRecords = make(map[string]map[string]interface{})

			// Creating a single Map after processing the segment.
			// This tells the PostQueryBucketCleaning function to return to the rrcreader.go to process the further segments.
			nodeResult.TransactionEventRecords["PROCESSED_SEGMENT_"+fmt.Sprint(nodeResult.RecsAggsProcessedSegments)] = make(map[string]interface{})

			aggs.TransactionArguments.SortedRecordsSlice = nil // Clear the sorted records slice.
		}

		if nodeResult.RecsAggsProcessedSegments == numTotalSegments {
			nodeResult.TransactionEventRecords = nil
			nodeResult.TransactionEventRecords = make(map[string]map[string]interface{})
			nodeResult.TransactionEventRecords["CHECK_NEXT_AGG"] = make(map[string]interface{}) // All segments have been processed. Check the next aggregation.

			// Clear the Open/Pending Transactions
			aggs.TransactionArguments.OpenTransactionEvents = nil
			aggs.TransactionArguments.OpenTransactionsState = nil

			// Assign the final processed transactions to the recs.
			for i, record := range nodeResult.TransactionsProcessed {
				recs[i] = record
				delete(nodeResult.TransactionsProcessed, i)
			}

			for k := range finalCols {
				delete(finalCols, k)
			}

			for _, col := range cols {
				finalCols[col] = true
			}
		}

		return

	}

}

// Evaluate a boolean expression
func evaluateBoolExpr(boolExpr *structs.BoolExpr, record map[string]interface{}) bool {
	// Terminal condition
	if boolExpr.IsTerminal {
		return evaluateSimpleCondition(boolExpr, record)
	}

	// Recursive evaluation
	leftResult := evaluateBoolExpr(boolExpr.LeftBool, record)
	rightResult := evaluateBoolExpr(boolExpr.RightBool, record)

	// Combine results based on the boolean operation
	switch boolExpr.BoolOp {
	case structs.BoolOpAnd:
		return leftResult && rightResult
	case structs.BoolOpOr:
		return leftResult || rightResult
	default:
		// Handle other cases or throw an error
		return false
	}
}

// Evaluate a simple condition (terminal node)
func evaluateSimpleCondition(term *structs.BoolExpr, record map[string]interface{}) bool {
	leftVal, err := getValuesFromValueExpr(term.LeftValue, record)
	if err != nil {
		return false
	}

	rightVal, err := getValuesFromValueExpr(term.RightValue, record)
	if err != nil {
		return false
	}

	// If the left or right value is nil, return false
	if leftVal == nil || rightVal == nil {
		return false
	}

	return conditionMatch(leftVal, term.ValueOp, rightVal)
}

func getValuesFromValueExpr(valueExpr *structs.ValueExpr, record map[string]interface{}) (interface{}, error) {
	if valueExpr == nil {
		return nil, fmt.Errorf("getValuesFromValueExpr: valueExpr is nil")
	}

	switch valueExpr.ValueExprMode {
	case structs.VEMNumericExpr:
		if valueExpr.NumericExpr == nil {
			return nil, fmt.Errorf("getValuesFromValueExpr: valueExpr.NumericExpr is nil")
		}
		if valueExpr.NumericExpr.ValueIsField {
			fieldValue, exists := record[valueExpr.NumericExpr.Value]
			if !exists {
				return nil, fmt.Errorf("getValuesFromValueExpr: valueExpr.NumericExpr.Value does not exist in record")
			}
			floatFieldVal, err := dtypeutils.ConvertToFloat(fieldValue, 64)
			if err != nil {
				return fieldValue, nil
			}
			return floatFieldVal, nil
		} else {
			floatVal, err := dtypeutils.ConvertToFloat(valueExpr.NumericExpr.Value, 64)
			return floatVal, err
		}
	case structs.VEMStringExpr:
		if valueExpr.StringExpr == nil {
			return nil, fmt.Errorf("getValuesFromValueExpr: valueExpr.StringExpr is nil")
		}
		switch valueExpr.StringExpr.StringExprMode {
		case structs.SEMRawString:
			return valueExpr.StringExpr.RawString, nil
		case structs.SEMField:
			fieldValue, exists := record[valueExpr.StringExpr.FieldName]
			if !exists {
				return nil, fmt.Errorf("getValuesFromValueExpr: valueExpr.StringExpr.Field does not exist in record")
			}
			return fieldValue, nil
		default:
			return nil, fmt.Errorf("getValuesFromValueExpr: valueExpr.StringExpr.StringExprMode is invalid")
		}
	default:
		return nil, fmt.Errorf("getValuesFromValueExpr: valueExpr.ValueExprMode is invalid")
	}
}

func conditionMatch(fieldValue interface{}, Op string, searchValue interface{}) bool {
	switch Op {
	case "=", "eq":
		return fmt.Sprint(fieldValue) == fmt.Sprint(searchValue)
	case "!=", "neq":
		return fmt.Sprint(fieldValue) != fmt.Sprint(searchValue)
	default:
		fieldValFloat, err := dtypeutils.ConvertToFloat(fieldValue, 64)
		if err != nil {
			return false
		}
		searchValFloat, err := dtypeutils.ConvertToFloat(searchValue, 64)
		if err != nil {
			return false
		}
		switch Op {
		case ">", "gt":
			return fieldValFloat > searchValFloat
		case ">=", "gte":
			return fieldValFloat >= searchValFloat
		case "<", "lt":
			return fieldValFloat < searchValFloat
		case "<=", "lte":
			return fieldValFloat <= searchValFloat
		default:
			return false
		}
	}
}

func evaluateASTNode(node *structs.ASTNode, record map[string]interface{}, recordMapStr string) bool {
	if node.AndFilterCondition != nil && !evaluateCondition(node.AndFilterCondition, record, recordMapStr, segutils.And) {
		return false
	}

	if node.OrFilterCondition != nil && !evaluateCondition(node.OrFilterCondition, record, recordMapStr, segutils.Or) {
		return false
	}

	// If the node has an exclusion filter, and the exclusion filter matches, return false.
	if node.ExclusionFilterCondition != nil && evaluateCondition(node.ExclusionFilterCondition, record, recordMapStr, segutils.Exclusion) {
		return false
	}

	return true
}

func evaluateCondition(condition *structs.Condition, record map[string]interface{}, recordMapStr string, logicalOp segutils.LogicalOperator) bool {
	for _, nestedNode := range condition.NestedNodes {
		if !evaluateASTNode(nestedNode, record, recordMapStr) {
			return false
		}
	}

	for _, criteria := range condition.FilterCriteria {
		validMatch := false
		if criteria.MatchFilter != nil {
			validMatch = evaluateMatchFilter(criteria.MatchFilter, record, recordMapStr)
		} else if criteria.ExpressionFilter != nil {
			validMatch = evaluateExpressionFilter(criteria.ExpressionFilter, record, recordMapStr)
		}

		// If the logical operator is Or and at least one of the criteria matches, return true.
		if logicalOp == segutils.Or && validMatch {
			return true
		} else if logicalOp == segutils.And && !validMatch { // If the logical operator is And and at least one of the criteria does not match, return false.
			return false
		}
	}

	return logicalOp == segutils.And
}

func evaluateMatchFilter(matchFilter *structs.MatchFilter, record map[string]interface{}, recordMapStr string) bool {
	var fieldValue interface{}
	var exists bool

	if matchFilter.MatchColumn == "*" {
		fieldValue = recordMapStr
	} else {
		fieldValue, exists = record[matchFilter.MatchColumn]
		if !exists {
			return false
		}
	}

	dVal, err := segutils.CreateDtypeEnclosure(fieldValue, 0)
	if err != nil {
		return false
	}

	switch matchFilter.MatchType {
	case structs.MATCH_WORDS:
		return evaluateMatchWords(matchFilter, dVal.StringVal)
	case structs.MATCH_PHRASE:
		return evaluateMatchPhrase(string(matchFilter.MatchPhrase), dVal.StringVal)
	default:
		return false
	}
}

func evaluateMatchWords(matchFilter *structs.MatchFilter, fieldValueStr string) bool {
	for _, word := range matchFilter.MatchWords {
		if evaluateMatchPhrase(string(word), fieldValueStr) {
			if matchFilter.MatchOperator == segutils.Or {
				return true
			}
		} else if matchFilter.MatchOperator == segutils.And {
			return false
		}
	}

	return matchFilter.MatchOperator == segutils.And
}

func evaluateMatchPhrase(matchPhrase string, fieldValueStr string) bool {
	// Create a regular expression to match the whole word, using \b for word boundaries
	pattern := `\b` + regexp.QuoteMeta(string(matchPhrase)) + `\b`
	r, err := regexp.Compile(pattern)
	if err != nil {
		return false
	}

	// Use the regular expression to find a match
	return r.MatchString(fieldValueStr)
}

func evaluateExpressionFilter(expressionFilter *structs.ExpressionFilter, record map[string]interface{}, recordMapStr string) bool {
	leftValue, errL := evaluateFilterInput(expressionFilter.LeftInput, record, recordMapStr)
	if errL != nil {
		return false
	}
	rightValue, errR := evaluateFilterInput(expressionFilter.RightInput, record, recordMapStr)
	if errR != nil {
		return false
	}

	return conditionMatch(leftValue, expressionFilter.FilterOperator.ToString(), rightValue)
}

func evaluateFilterInput(filterInput *structs.FilterInput, record map[string]interface{}, recordMapStr string) (interface{}, error) {
	if filterInput.SubTree != nil {
		return evaluateASTNode(filterInput.SubTree, record, recordMapStr), nil
	} else if filterInput.Expression != nil {
		return evaluateExpression(filterInput.Expression, record)
	}

	return nil, fmt.Errorf("evaluateFilterInput: filterInput is invalid")
}

func evaluateExpression(expr *structs.Expression, record map[string]interface{}) (interface{}, error) {
	var leftValue, rightValue, err interface{}

	if expr.LeftInput != nil {
		leftValue, err = getInputValueFromExpression(expr.LeftInput, record)
		if err != nil {
			return nil, err.(error)
		}
	}

	if expr.RightInput != nil {
		rightValue, err = getInputValueFromExpression(expr.RightInput, record)
		if err != nil {
			return nil, err.(error)
		}
	}

	if leftValue != nil && rightValue != nil {
		return performArithmeticOperation(leftValue, rightValue, expr.ExpressionOp)
	}

	return leftValue, nil
}

func performArithmeticOperation(leftValue interface{}, rightValue interface{}, Op segutils.ArithmeticOperator) (interface{}, error) {
	switch Op {
	case segutils.Add:
		// Handle the case where both operands are strings
		if lv, ok := leftValue.(string); ok {
			if rv, ok := rightValue.(string); ok {
				return lv + rv, nil
			}
			return nil, fmt.Errorf("rightValue is not a string")
		}
		// Continue to handle the case where both operands are numbers
		fallthrough
	case segutils.Subtract, segutils.Multiply, segutils.Divide, segutils.Modulo, segutils.BitwiseAnd, segutils.BitwiseOr, segutils.BitwiseExclusiveOr:
		lv, errL := dtypeutils.ConvertToFloat(leftValue, 64)
		rv, errR := dtypeutils.ConvertToFloat(rightValue, 64)
		if errL != nil || errR != nil {
			return nil, fmt.Errorf("performArithmeticOperation: leftValue or rightValue is not a number")
		}
		switch Op {
		case segutils.Add:
			return lv + rv, nil
		case segutils.Subtract:
			return lv - rv, nil
		case segutils.Multiply:
			return lv * rv, nil
		case segutils.Divide:
			if rv == 0 {
				return nil, fmt.Errorf("performArithmeticOperation: cannot divide by zero")
			}
			return lv / rv, nil
		case segutils.Modulo:
			return int64(lv) % int64(rv), nil
		case segutils.BitwiseAnd:
			return int64(lv) & int64(rv), nil
		case segutils.BitwiseOr:
			return int64(lv) | int64(rv), nil
		case segutils.BitwiseExclusiveOr:
			return int64(lv) ^ int64(rv), nil
		default:
			return nil, fmt.Errorf("performArithmeticOperation: invalid arithmetic operator")
		}
	default:
		return nil, fmt.Errorf("performArithmeticOperation: invalid arithmetic operator")
	}
}

func getInputValueFromExpression(expr *structs.ExpressionInput, record map[string]interface{}) (interface{}, error) {
	if expr.ColumnName != "" {
		value, exists := record[expr.ColumnName]
		if !exists {
			return nil, fmt.Errorf("getInputValueFromExpression: expr.ColumnName does not exist in record")
		}
		dval, err := segutils.CreateDtypeEnclosure(value, 0)
		if err != nil {
			return value, nil
		} else {
			value, _ = dval.GetValue()
		}
		return value, nil
	} else if expr.ColumnValue != nil {
		return expr.ColumnValue.GetValue()
	}

	return nil, fmt.Errorf("getInputValueFromExpression: expr is invalid")
}

func isTransactionMatchedWithTheFliterStringCondition(with *structs.FilterStringExpr, recordMapStr string, record map[string]interface{}) bool {
	if with.StringValue != "" {
		return evaluateMatchPhrase(with.StringValue, recordMapStr)
	} else if with.EvalBoolExpr != nil {
		return evaluateBoolExpr(with.EvalBoolExpr, record)
	} else if with.SearchNode != nil {
		return evaluateASTNode(with.SearchNode.(*structs.ASTNode), record, recordMapStr)
	}

	return false
}

// Splunk Transaction command based on the TransactionArguments on the JSON records. map[string]map[string]interface{}
func processTransactionsOnRecords(records map[string]map[string]interface{}, processedTransactions map[string]map[string]interface{}, allCols []string, transactionArgs *structs.TransactionArguments, closeAllTransactions bool) ([]string, error) {

	if transactionArgs == nil {
		return allCols, nil
	}

	transactionFields := transactionArgs.Fields

	if len(transactionFields) == 0 {
		transactionFields = []string{"timestamp"}
	}

	transactionStartsWith := transactionArgs.StartsWith
	transactionEndsWith := transactionArgs.EndsWith

	if transactionArgs.OpenTransactionEvents == nil {
		transactionArgs.OpenTransactionEvents = make(map[string][]map[string]interface{})
	}

	if transactionArgs.OpenTransactionsState == nil {
		transactionArgs.OpenTransactionsState = make(map[string]*structs.TransactionGroupState)
	}

	appendGroupedRecords := func(currentState *structs.TransactionGroupState, transactionKey string) {

		records, exists := transactionArgs.OpenTransactionEvents[transactionKey]

		if !exists || len(records) == 0 {
			return
		}

		groupedRecord := make(map[string]interface{})
		groupedRecord["timestamp"] = currentState.Timestamp
		groupedRecord["event"] = records
		lastRecord := records[len(transactionArgs.OpenTransactionEvents[transactionKey])-1]
		lastRecordTimestamp, _ := lastRecord["timestamp"].(uint64)
		var duration uint64
		if lastRecordTimestamp < currentState.Timestamp {
			duration = currentState.Timestamp - lastRecordTimestamp
		} else {
			duration = lastRecordTimestamp - currentState.Timestamp
		}
		groupedRecord["duration"] = duration
		groupedRecord["eventcount"] = uint64(len(records))
		groupedRecord["transactionKey"] = transactionKey

		for _, key := range transactionFields {
			groupedRecord[key] = lastRecord[key]
		}

		processedTransactions[currentState.RecInden] = groupedRecord

		// Clear the group records and state
		delete(transactionArgs.OpenTransactionEvents, transactionKey)
		delete(transactionArgs.OpenTransactionsState, transactionKey)
	}

	for _, sortedRecord := range transactionArgs.SortedRecordsSlice {

		recInden := sortedRecord["key"].(string)
		record := records[recInden]

		recordMapStr := fmt.Sprintf("%v", record)

		// Generate the transaction key from the record.
		transactionKey := ""

		for _, field := range transactionFields {
			if record[field] != nil {
				transactionKey += "_" + fmt.Sprintf("%v", record[field])
			}
		}

		// If the transaction key is empty, then skip this record.
		if transactionKey == "" {
			continue
		}

		// Initialize the group state for new transaction keys
		if _, exists := transactionArgs.OpenTransactionsState[transactionKey]; !exists {
			transactionArgs.OpenTransactionsState[transactionKey] = &structs.TransactionGroupState{
				Key:       transactionKey,
				Open:      false,
				RecInden:  recInden,
				Timestamp: 0,
			}
		}

		currentState := transactionArgs.OpenTransactionsState[transactionKey]

		// If StartsWith is given, then the transaction Should only Open when the record matches the StartsWith. OR
		// if StartsWith not present, then the transaction should open for all records.
		if !currentState.Open {
			openState := false

			if transactionStartsWith != nil {
				openState = isTransactionMatchedWithTheFliterStringCondition(transactionStartsWith, recordMapStr, record)
			} else {
				openState = true
			}

			if openState {
				currentState.Open = true
				currentState.Timestamp = uint64(record["timestamp"].(uint64))

				transactionArgs.OpenTransactionsState[transactionKey] = currentState
				transactionArgs.OpenTransactionEvents[transactionKey] = make([]map[string]interface{}, 0)
			}

		} else if currentState.Open && transactionEndsWith == nil && transactionStartsWith != nil {
			// If StartsWith is given, but endsWith is not given, then the startswith will be the end of the transaction.
			// So close with last record and open a new transaction.

			closeAndOpenState := isTransactionMatchedWithTheFliterStringCondition(transactionStartsWith, recordMapStr, record)

			if closeAndOpenState {
				appendGroupedRecords(currentState, transactionKey)

				currentState.Timestamp = uint64(record["timestamp"].(uint64))
				currentState.Open = true
				currentState.RecInden = recInden

				transactionArgs.OpenTransactionsState[transactionKey] = currentState
				transactionArgs.OpenTransactionEvents[transactionKey] = make([]map[string]interface{}, 0)
			}

		}

		// If the transaction is open, then append the record to the group.
		if currentState.Open {
			transactionArgs.OpenTransactionEvents[transactionKey] = append(transactionArgs.OpenTransactionEvents[transactionKey], record)
		}

		if transactionEndsWith != nil {
			if currentState.Open {
				closeState := isTransactionMatchedWithTheFliterStringCondition(transactionEndsWith, recordMapStr, record)

				if closeState {
					appendGroupedRecords(currentState, transactionKey)

					currentState.Open = false
					currentState.Timestamp = 0
					currentState.RecInden = recInden
					transactionArgs.OpenTransactionsState[transactionKey] = currentState
				}
			}
		}
	}

	// Transaction EndsWith is not given In this case, most or all of the transactionArgs.OpenTransactionEvents will not be appended to the groupedRecords.
	// Even if we are appending the transactionArgs.OpenTransactionEvents at StartsWith, not all the transactionArgs.OpenTransactionEvents will be appended to the groupedRecords.
	// So we need to append them here.
	if transactionEndsWith == nil && closeAllTransactions {
		for key := range transactionArgs.OpenTransactionEvents {
			appendGroupedRecords(transactionArgs.OpenTransactionsState[key], key)
		}
	}

	allCols = make([]string, 0)
	allCols = append(allCols, "timestamp")
	allCols = append(allCols, "duration")
	allCols = append(allCols, "eventcount")
	allCols = append(allCols, "event")
	allCols = append(allCols, transactionFields...)

	return allCols, nil
}

// Decode the bucketKey into a slice of strings.
func decodeBucketKey(bucketKey interface{}) ([]string, error) {
	switch castedKey := bucketKey.(type) {
	case []string:
		return castedKey, nil
	case string:
		return []string{castedKey}, nil
	default:
		return nil, fmt.Errorf("decodeBucketKey: unexpected type %T for bucketKey %v", castedKey, bucketKey)
	}
}

// Return a string if the slice has length 1, otherwise return the slice.
func encodeBucketKey(bucketKeySlice []string) interface{} {
	if len(bucketKeySlice) == 1 {
		return bucketKeySlice[0]
	}

	return bucketKeySlice
}
