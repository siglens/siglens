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
	"container/list"
	"fmt"
	"math"
	"time"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	putils "github.com/siglens/siglens/pkg/utils"
)

func GetBucketKey(record map[string]interface{}, groupByRequest *structs.GroupByRequest) (string, error) {
	bucketKey := ""
	for _, colName := range groupByRequest.GroupByColumns {
		val, ok := record[colName]
		if !ok {
			return "", fmt.Errorf("getBucketKey Error, column: %v not found in the record", colName)
		}
		bucketKey += fmt.Sprintf("%v_", val)
	}
	return bucketKey, nil
}

func InitRunningStreamStatsResults(defaultVal float64) *structs.RunningStreamStatsResults {
	return &structs.RunningStreamStatsResults{
		Window:     list.New().Init(),
		CurrResult: defaultVal,
	}
}

func PerformGlobalStreamStatsOnSingleFunc(ssOption *structs.StreamStatsOptions, ssResults *structs.RunningStreamStatsResults, measureAgg utils.AggregateFunctions, colValue float64) (float64, bool, error) {
	result := ssResults.CurrResult
	valExist := ssResults.NumProcessedRecords > 0

	if measureAgg == utils.Avg && valExist {
		result = result / float64(ssResults.NumProcessedRecords)
	}

	switch measureAgg {
	case utils.Count:
		ssResults.CurrResult++
	case utils.Sum, utils.Avg:
		ssResults.CurrResult += colValue
	case utils.Min:
		if colValue < ssResults.CurrResult {
			ssResults.CurrResult = colValue
		}
	case utils.Max:
		if colValue > ssResults.CurrResult {
			ssResults.CurrResult = colValue
		}
	default:
		return 0.0, false, fmt.Errorf("performGlobalStreamStatsOnSingleFunc Error, measureAgg: %v not supported", measureAgg)
	}

	ssResults.NumProcessedRecords++

	if !ssOption.Current {
		return result, valExist, nil
	}

	if measureAgg == utils.Avg {
		return ssResults.CurrResult / float64(ssResults.NumProcessedRecords), true, nil
	}

	return ssResults.CurrResult, true, nil
}

// Remove the front element from the window
func removeFrontElementFromWindow(ssResults *structs.RunningStreamStatsResults, measureAgg utils.AggregateFunctions) error {
	front := ssResults.Window.Front()
	frontElement, correctType := front.Value.(*structs.RunningStreamStatsWindowElement)
	if !correctType {
		return fmt.Errorf("removeFrontElementFromWindow: Error value in the window is not an IndexValue element")
	}

	// Update the current result
	if measureAgg == utils.Avg || measureAgg == utils.Sum {
		ssResults.CurrResult -= frontElement.Value
	} else if measureAgg == utils.Count {
		ssResults.CurrResult--
	}

	ssResults.Window.Remove(ssResults.Window.Front())

	return nil
}

// Remove elements from the window that are outside the window size
func cleanWindow(currIndex int, global bool, ssResults *structs.RunningStreamStatsResults, windowSize int, measureAgg utils.AggregateFunctions) error {
	if global {
		for ssResults.Window.Len() > 0 {
			front := ssResults.Window.Front()
			frontVal, correctType := front.Value.(*structs.RunningStreamStatsWindowElement)
			if !correctType {
				return fmt.Errorf("cleanWindow: Error value in the window is not an IndexValue element")
			}
			if frontVal.Index+windowSize <= currIndex {
				err := removeFrontElementFromWindow(ssResults, measureAgg)
				if err != nil {
					return fmt.Errorf("cleanWindow: Error while removing front element from the window, err: %v", err)
				}
			} else {
				break
			}
		}
	} else {
		for ssResults.Window.Len() > windowSize {
			err := removeFrontElementFromWindow(ssResults, measureAgg)
			if err != nil {
				return fmt.Errorf("cleanWindow: Error while removing front element from the window, err: %v", err)
			}
		}
	}

	return nil
}

// Remove elements from the window that are outside the time window
func cleanTimeWindow(currTimestamp uint64, timeSortAsc bool, timeWindow *structs.BinSpanLength, ssResults *structs.RunningStreamStatsResults, measureAgg utils.AggregateFunctions) error {

	currTime := time.UnixMilli(int64(currTimestamp)).In(time.Local)
	var thresholdTime uint64
	if timeSortAsc {
		offsetTime, err := utils.ApplyOffsetToTime(-int64(timeWindow.Num), timeWindow.TimeScale, currTime)
		if err != nil {
			return fmt.Errorf("cleanTimeWindow: Error while applying offset to time, timeSortAsc: %v, err: %v", timeSortAsc, err)
		}
		thresholdTime = uint64(offsetTime.UnixMilli())
	} else {
		offsetTime, err := utils.ApplyOffsetToTime(int64(timeWindow.Num), timeWindow.TimeScale, currTime)
		if err != nil {
			return fmt.Errorf("cleanTimeWindow: Error while applying offset to time, timeSortAsc: %v, err: %v", timeSortAsc, err)
		}
		thresholdTime = uint64(offsetTime.UnixMilli())
	}

	for ssResults.Window.Len() > 0 {
		front := ssResults.Window.Front()
		frontVal, correctType := front.Value.(*structs.RunningStreamStatsWindowElement)
		if !correctType {
			return fmt.Errorf("cleanTimeWindow: Error value in the window is not an IndexValue element")
		}
		eventTimestamp := frontVal.TimeInMilli
		if timeSortAsc {
			if eventTimestamp < thresholdTime {
				err := removeFrontElementFromWindow(ssResults, measureAgg)
				if err != nil {
					return fmt.Errorf("cleanTimeWindow: Error while removing front element from the window, timeSortAsc: %v, err: %v", timeSortAsc, err)
				}
			} else {
				break
			}
		} else {
			if eventTimestamp > thresholdTime {
				err := removeFrontElementFromWindow(ssResults, measureAgg)
				if err != nil {
					return fmt.Errorf("cleanTimeWindow: Error while removing front element from the window, timeSortAsc: %v, err: %v", timeSortAsc, err)
				}
			} else {
				break
			}
		}
	}

	return nil
}

func getResults(ssResults *structs.RunningStreamStatsResults, measureAgg utils.AggregateFunctions) (float64, bool, error) {
	if ssResults.Window.Len() == 0 {
		return 0.0, false, nil
	}
	switch measureAgg {
	case utils.Count:
		return ssResults.CurrResult, true, nil
	case utils.Sum:
		return ssResults.CurrResult, true, nil
	case utils.Avg:
		return ssResults.CurrResult / float64(ssResults.Window.Len()), true, nil
	case utils.Min, utils.Max:
		return ssResults.Window.Front().Value.(*structs.RunningStreamStatsWindowElement).Value, true, nil
	default:
		return 0.0, false, fmt.Errorf("getResults Error, measureAgg: %v not supported", measureAgg)
	}
}

func performMeasureFunc(currIndex int, ssResults *structs.RunningStreamStatsResults, measureAgg utils.AggregateFunctions, colValue float64, timestamp uint64) (float64, error) {
	switch measureAgg {
	case utils.Count:
		ssResults.CurrResult++
		ssResults.Window.PushBack(&structs.RunningStreamStatsWindowElement{Index: currIndex, Value: colValue, TimeInMilli: timestamp})
	case utils.Sum, utils.Avg:
		ssResults.CurrResult += colValue
		ssResults.Window.PushBack(&structs.RunningStreamStatsWindowElement{Index: currIndex, Value: colValue, TimeInMilli: timestamp})
	case utils.Min:
		for ssResults.Window.Len() > 0 {
			lastElement, correctType := ssResults.Window.Back().Value.(*structs.RunningStreamStatsWindowElement)
			if !correctType {
				return 0.0, fmt.Errorf("performWindowStreamStatsOnSingleFunc Error, value in the window is not an IndexValue element")
			}
			if lastElement.Value >= colValue {
				ssResults.Window.Remove(ssResults.Window.Back())
			} else {
				break
			}
		}
		ssResults.Window.PushBack(&structs.RunningStreamStatsWindowElement{Index: currIndex, Value: colValue, TimeInMilli: timestamp})
		ssResults.CurrResult = ssResults.Window.Front().Value.(*structs.RunningStreamStatsWindowElement).Value
	case utils.Max:
		for ssResults.Window.Len() > 0 {
			lastElement, correctType := ssResults.Window.Back().Value.(*structs.RunningStreamStatsWindowElement)
			if !correctType {
				return 0.0, fmt.Errorf("performWindowStreamStatsOnSingleFunc Error, value in the window is not an IndexValue element")
			}
			if lastElement.Value <= colValue {
				ssResults.Window.Remove(ssResults.Window.Back())
			} else {
				break
			}
		}
		ssResults.Window.PushBack(&structs.RunningStreamStatsWindowElement{Index: currIndex, Value: colValue, TimeInMilli: timestamp})
		ssResults.CurrResult = ssResults.Window.Front().Value.(*structs.RunningStreamStatsWindowElement).Value
	default:
		return 0.0, fmt.Errorf("performGlobalStreamStatsOnSingleFunc Error, measureAgg: %v not supported", measureAgg)
	}

	if measureAgg == utils.Avg {
		return ssResults.CurrResult / float64(ssResults.Window.Len()), nil
	}

	return ssResults.CurrResult, nil
}

func PerformWindowStreamStatsOnSingleFunc(currIndex int, ssOption *structs.StreamStatsOptions, ssResults *structs.RunningStreamStatsResults, windowSize int, measureAgg utils.AggregateFunctions, colValue float64, timestamp uint64, timeSortAsc bool) (float64, bool, error) {
	var err error
	result := ssResults.CurrResult
	exist := ssResults.Window.Len() > 0
	if exist && measureAgg == utils.Avg {
		result = result / float64(ssResults.Window.Len())
	}

	if ssOption.TimeWindow != nil {
		err := cleanTimeWindow(timestamp, timeSortAsc, ssOption.TimeWindow, ssResults, measureAgg)
		if err != nil {
			return 0.0, false, fmt.Errorf("performWindowStreamStatsOnSingleFunc: Error while cleaning the time window, err: %v", err)
		}
	}

	// If current is false, compute result before adding the new element to the window
	if !ssOption.Current && windowSize != 0 {
		err := cleanWindow(currIndex-1, ssOption.Global, ssResults, windowSize, measureAgg)
		if err != nil {
			return 0.0, false, fmt.Errorf("performWindowStreamStatsOnSingleFunc: Error while cleaning the window, err: %v", err)
		}
		result, exist, err = getResults(ssResults, measureAgg)
		if err != nil {
			return 0.0, false, fmt.Errorf("performWindowStreamStatsOnSingleFunc: Error while getting results from the window, err: %v", err)
		}
	}

	if windowSize != 0 {
		if ssOption.Global {
			err = cleanWindow(currIndex, ssOption.Global, ssResults, windowSize, measureAgg)
		} else {
			err = cleanWindow(currIndex, ssOption.Global, ssResults, windowSize-1, measureAgg)
		}
		if err != nil {
			return 0.0, false, fmt.Errorf("performWindowStreamStatsOnSingleFunc: Error while cleaning the window, err: %v", err)
		}
	}

	// Add the new element to the window
	latestResult, err := performMeasureFunc(currIndex, ssResults, measureAgg, colValue, timestamp)
	if err != nil {
		return 0.0, false, fmt.Errorf("performWindowStreamStatsOnSingleFunc: Error while performing measure function %v, err: %v", measureAgg, err)
	}

	if !ssOption.Current {
		return result, exist, nil
	}

	return latestResult, true, nil
}

func PerformStreamStatsOnSingleFunc(currIndex int, bucketKey string, ssOption *structs.StreamStatsOptions, measureFuncIndex int, measureAgg *structs.MeasureAggregator, record map[string]interface{}, timestamp uint64, timeSortAsc bool) (float64, bool, error) {

	floatVal := 0.0
	var err error
	var result float64

	if measureAgg.MeasureFunc != utils.Count {
		recordVal, exist := record[measureAgg.MeasureCol]
		if !exist {
			return 0.0, false, fmt.Errorf("performStreamStatsOnSingleFunc Error, measure column: %v not found in the record", measureAgg.MeasureCol)
		}
		floatVal, err = dtypeutils.ConvertToFloat(recordVal, 64)
		// currently only supporting basic agg functions
		if err != nil {
			return 0.0, false, fmt.Errorf("performStreamStatsOnSingleFunc Error measure column %v does not have a numeric value, err: %v", measureAgg.MeasureCol, err)
		}
	}

	_, exist := ssOption.RunningStreamStats[measureFuncIndex]
	if !exist {
		ssOption.RunningStreamStats[measureFuncIndex] = make(map[string]*structs.RunningStreamStatsResults)
	}

	_, exist = ssOption.RunningStreamStats[measureFuncIndex][bucketKey]
	if !exist {
		defaultVal := 0.0
		if measureAgg.MeasureFunc == utils.Min {
			defaultVal = math.MaxFloat64
		} else if measureAgg.MeasureFunc == utils.Max {
			defaultVal = -math.MaxFloat64
		}
		ssOption.RunningStreamStats[measureFuncIndex][bucketKey] = InitRunningStreamStatsResults(defaultVal)
	}

	if ssOption.Window == 0 && ssOption.TimeWindow == nil {
		result, exist, err = PerformGlobalStreamStatsOnSingleFunc(ssOption, ssOption.RunningStreamStats[measureFuncIndex][bucketKey], measureAgg.MeasureFunc, floatVal)
		if err != nil {
			return 0.0, false, fmt.Errorf("performStreamStatsOnSingleFunc Error while performing global stream stats on function %v for value %v, err: %v", measureAgg.MeasureFunc, floatVal, err)
		}
	} else {
		result, exist, err = PerformWindowStreamStatsOnSingleFunc(currIndex, ssOption, ssOption.RunningStreamStats[measureFuncIndex][bucketKey], int(ssOption.Window), measureAgg.MeasureFunc, floatVal, timestamp, timeSortAsc)
		if err != nil {
			return 0.0, false, fmt.Errorf("performStreamStatsOnSingleFunc Error while performing window stream stats on function %v for value %v, err: %v", measureAgg.MeasureFunc, floatVal, err)
		}
	}

	return result, exist, nil
}

func resetAccumulatedStreamStats(ssOption *structs.StreamStatsOptions) {
	ssOption.NumProcessedRecords = 0
	ssOption.RunningStreamStats = make(map[int]map[string]*structs.RunningStreamStatsResults, 0)
}

func evaluateResetCondition(boolExpr *structs.BoolExpr, record map[string]interface{}) (bool, error) {
	if boolExpr == nil {
		return false, nil
	}

	fieldsInExpr := boolExpr.GetFields()
	fieldToValue := make(map[string]utils.CValueEnclosure, 0)
	err := getRecordFieldValues(fieldToValue, fieldsInExpr, record)
	if err != nil {
		return false, fmt.Errorf("evaluateResetCondition: Error while retrieving values, err: %v", err)
	}

	conditionPassed, err := boolExpr.Evaluate(fieldToValue)
	if err != nil {
		return false, fmt.Errorf("evaluateResetCondition: Error while evaluating the condition, err: %v", err)
	}

	return conditionPassed, nil
}

func PerformStreamStatsOnRawRecord(nodeResult *structs.NodeResult, agg *structs.QueryAggregators, recs map[string]map[string]interface{}, recordIndexInFinal map[string]int, finalCols map[string]bool, finishesSegment bool, timeSort bool, timeSortAsc bool) error {
	if !timeSort && agg.StreamStatsOptions.TimeWindow != nil {
		return fmt.Errorf("performStreamStats Error: For timewindow to be used the records must be sorted by time")
	}

	if agg.StreamStatsOptions.SegmentRecords == nil {
		agg.StreamStatsOptions.SegmentRecords = make(map[string]map[string]interface{}, 0)
	}

	for recordKey, record := range recs {
		agg.StreamStatsOptions.SegmentRecords[recordKey] = record
		delete(recs, recordKey)
	}

	if !finishesSegment {
		return nil
	}

	bucketKey := ""
	currentBucketKey := bucketKey
	var err error

	currentOrder, err := GetOrderedRecs(agg.StreamStatsOptions.SegmentRecords, recordIndexInFinal)
	if err != nil {
		return fmt.Errorf("performStreamStats Error while fetching the order of the records, err: %v", err)
	}

	measureAggs := agg.MeasureOperations
	if agg.GroupByRequest != nil {
		measureAggs = agg.GroupByRequest.MeasureOperations
	}

	numPrevSegmentProcessedRecords := agg.StreamStatsOptions.NumProcessedRecords

	currIndex := 0
	for _, recordKey := range currentOrder {
		record, exist := agg.StreamStatsOptions.SegmentRecords[recordKey]
		if !exist {
			return fmt.Errorf("performStreamStats: Error: record not found")
		}

		timestamp, exist := record["timestamp"]
		if !exist {
			return fmt.Errorf("performStreamStats: Error: timestamp not found in the record")
		}
		timeInMilli, err := dtypeutils.ConvertToUInt(timestamp, 64)
		if err != nil {
			return fmt.Errorf("performStreamStats: Error: timestamp not a valid uint64 value")
		}

		if agg.GroupByRequest != nil {
			bucketKey, err = GetBucketKey(record, agg.GroupByRequest)
			if err != nil {
				return fmt.Errorf("performStreamStats: Error while creating bucket key, err: %v", err)
			}
		}

		if agg.StreamStatsOptions.ResetOnChange && currentBucketKey != bucketKey {
			resetAccumulatedStreamStats(agg.StreamStatsOptions)
			currentBucketKey = bucketKey
			currIndex = 0
		}

		shouldResetBefore, err := evaluateResetCondition(agg.StreamStatsOptions.ResetBefore, record)
		if err != nil {
			return fmt.Errorf("performStreamStats: Error while evaluating resetBefore condition, err: %v", err)
		}
		if shouldResetBefore {
			resetAccumulatedStreamStats(agg.StreamStatsOptions)
			currIndex = 0
		}

		for measureFuncIndex, measureAgg := range measureAggs {
			streamStatsResult, exist, err := PerformStreamStatsOnSingleFunc(int(numPrevSegmentProcessedRecords)+currIndex, bucketKey, agg.StreamStatsOptions, measureFuncIndex, measureAgg, record, timeInMilli, timeSortAsc)
			if err != nil {
				return fmt.Errorf("performStreamStats: Error while performing stream stats on function %v, err: %v", measureAgg.MeasureFunc, err)
			}
			if exist {
				record[measureAgg.String()] = streamStatsResult
			} else {
				if measureAgg.MeasureFunc == utils.Count {
					record[measureAgg.String()] = 0
				} else {
					record[measureAgg.String()] = ""
				}
			}
		}
		agg.StreamStatsOptions.NumProcessedRecords++
		currIndex++

		shouldResetAfter, err := evaluateResetCondition(agg.StreamStatsOptions.ResetAfter, record)
		if err != nil {
			return fmt.Errorf("performStreamStats: Error while evaluating resetAfter condition, err: %v", err)
		}
		if shouldResetAfter {
			resetAccumulatedStreamStats(agg.StreamStatsOptions)
			currIndex = 0
		}
	}

	for _, measureAgg := range measureAggs {
		finalCols[measureAgg.String()] = true
	}

	for recordKey, record := range agg.StreamStatsOptions.SegmentRecords {
		recs[recordKey] = record
		delete(agg.StreamStatsOptions.SegmentRecords, recordKey)
	}

	return nil
}

func PerformStreamStats(nodeResult *structs.NodeResult, agg *structs.QueryAggregators, recs map[string]map[string]interface{}, recordIndexInFinal map[string]int, finalCols map[string]bool, finishesSegment bool, timeSort bool, timeSortAsc bool) error {

	if agg.StreamStatsOptions.RunningStreamStats == nil {
		agg.StreamStatsOptions.RunningStreamStats = make(map[int]map[string]*structs.RunningStreamStatsResults, 0)
	}

	if recs != nil {
		return PerformStreamStatsOnRawRecord(nodeResult, agg, recs, recordIndexInFinal, finalCols, finishesSegment, timeSort, timeSortAsc)
	}

	if len(nodeResult.Histogram) > 0 {
		return performStreamStatsOnHistogram(nodeResult, agg.StreamStatsOptions, agg)
	}

	return nil
}

func getRecordFromFieldToValue(fieldToValue map[string]utils.CValueEnclosure) map[string]interface{} {
	record := make(map[string]interface{}, 0)
	for field, val := range fieldToValue {
		record[field] = val.CVal
	}

	return record
}

func performStreamStatsOnHistogram(nodeResult *structs.NodeResult, ssOption *structs.StreamStatsOptions, agg *structs.QueryAggregators) error {

	if ssOption.TimeWindow != nil {
		return fmt.Errorf("performStreamStatsOnHistogram Error: Time window cannot be applied to histograms")
	}

	// Setup a map for fetching values of field
	fieldsInExpr := []string{}
	measureAggs := agg.MeasureOperations
	if agg.GroupByRequest != nil {
		fieldsInExpr = agg.GroupByRequest.GroupByColumns
		measureAggs = agg.GroupByRequest.MeasureOperations
	}
	for _, measureAgg := range measureAggs {
		fieldsInExpr = append(fieldsInExpr, measureAgg.MeasureCol)
	}

	currIndex := 0
	bucketKey := ""
	currentBucketKey := bucketKey
	for _, aggregationResult := range nodeResult.Histogram {
		for rowIndex, bucketResult := range aggregationResult.Results {
			// Get the values of all the necessary fields.
			fieldToValue := make(map[string]utils.CValueEnclosure, 0)
			err := getAggregationResultFieldValues(fieldToValue, fieldsInExpr, aggregationResult, rowIndex)
			if err != nil {
				return fmt.Errorf("performStreamStatsOnHistogram: Error while getting value from agg results, err: %v", err)
			}
			record := getRecordFromFieldToValue(fieldToValue)

			if agg.GroupByRequest != nil {
				bucketKey, err = GetBucketKey(record, agg.GroupByRequest)
				if err != nil {
					return fmt.Errorf("performStreamStatsOnHistogram: Error while creating bucket key, err: %v", err)
				}
			}

			if agg.StreamStatsOptions.ResetOnChange && currentBucketKey != bucketKey {
				resetAccumulatedStreamStats(agg.StreamStatsOptions)
				currentBucketKey = bucketKey
				currIndex = 0
			}

			shouldResetBefore, err := evaluateResetCondition(agg.StreamStatsOptions.ResetBefore, record)
			if err != nil {
				return fmt.Errorf("performStreamStatsOnHistogram: Error while evaluating resetBefore condition, err: %v", err)
			}
			if shouldResetBefore {
				resetAccumulatedStreamStats(agg.StreamStatsOptions)
				currIndex = 0
			}

			for measureFuncIndex, measureAgg := range measureAggs {
				streamStatsResult, exist, err := PerformStreamStatsOnSingleFunc(currIndex, bucketKey, agg.StreamStatsOptions, measureFuncIndex, measureAgg, record, 0, false)
				if err != nil {
					return fmt.Errorf("performStreamStatsOnHistogram: Error while performing stream stats on function %v, err: %v", measureAgg.MeasureFunc, err)
				}

				// Check if the column to create already exists and is a GroupBy column.
				isGroupByCol := putils.SliceContainsString(nodeResult.GroupByCols, measureAgg.String())

				// Set the appropriate column to the computed value.
				if isGroupByCol {
					for keyIndex, groupByCol := range bucketResult.GroupByKeys {
						if measureAgg.String() != groupByCol {
							continue
						}

						streamStatsStr := ""
						if exist {
							streamStatsStr = fmt.Sprintf("%v", streamStatsResult)
						} else {
							if measureAgg.MeasureFunc == utils.Count {
								streamStatsStr = "0"
							}
						}

						// Set the appropriate element of BucketKey to cellValueStr.
						switch bucketKey := bucketResult.BucketKey.(type) {
						case []string:
							bucketKey[keyIndex] = streamStatsStr
							bucketResult.BucketKey = bucketKey
						case string:
							if keyIndex != 0 {
								return fmt.Errorf("performBinRequestOnHistogram: expected keyIndex to be 0, not %v", keyIndex)
							}
							bucketResult.BucketKey = streamStatsStr
						default:
							return fmt.Errorf("performBinRequestOnHistogram: bucket key has unexpected type: %T", bucketKey)
						}
					}
				} else {
					if exist {
						aggregationResult.Results[rowIndex].StatRes[measureAgg.String()] = utils.CValueEnclosure{
							Dtype: utils.SS_DT_FLOAT,
							CVal:  streamStatsResult,
						}
					} else {
						if measureAgg.MeasureFunc == utils.Count {
							aggregationResult.Results[rowIndex].StatRes[measureAgg.String()] = utils.CValueEnclosure{
								Dtype: utils.SS_DT_FLOAT,
								CVal:  streamStatsResult,
							}
						} else {
							aggregationResult.Results[rowIndex].StatRes[measureAgg.String()] = utils.CValueEnclosure{
								Dtype: utils.SS_DT_STRING,
								CVal:  "",
							}
						}
					}
				}
			}

			agg.StreamStatsOptions.NumProcessedRecords++
			currIndex++

			shouldResetAfter, err := evaluateResetCondition(agg.StreamStatsOptions.ResetAfter, record)
			if err != nil {
				return fmt.Errorf("performStreamStatsOnHistogram: Error while evaluating resetAfter condition, err: %v", err)
			}
			if shouldResetAfter {
				resetAccumulatedStreamStats(agg.StreamStatsOptions)
				currIndex = 0
			}

		}
	}

	return nil
}
