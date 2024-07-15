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

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
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
		Window: list.New().Init(),
		CurrResult: defaultVal,
	}
}


func PerformGlobalStreamStatsOnSingleFunc(ssOption *structs.StreamStatsOptions, ssResults *structs.RunningStreamStatsResults, measureAgg utils.AggregateFunctions, colValue float64) (float64, error) {
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
		return 0.0, fmt.Errorf("performGlobalStreamStatsOnSingleFunc Error, measureAgg: %v not supported", measureAgg)
	}

	ssOption.NumProcessedRecords++

	if measureAgg == utils.Avg {
		return ssResults.CurrResult / float64(ssOption.NumProcessedRecords), nil
	}

	return ssResults.CurrResult, nil
}


func PerformWindowStreamStatsOnSingleFunc(currIndex int, ssResults *structs.RunningStreamStatsResults, windowSize int, measureAgg utils.AggregateFunctions, colValue float64) (float64, error) {

	// Remove elements from the window that are outside the window size
	if windowSize != 0 {
		for ssResults.Window.Len() > 0 {
			front := ssResults.Window.Front()
			frontVal, correctType := front.Value.(*structs.IndexValue)
			if !correctType {
				return 0.0, fmt.Errorf("performWindowStreamStatsOnSingleFunc Error, value in the window is not an IndexValue element")
			}
			if frontVal.Index + windowSize <= currIndex {
				if measureAgg == utils.Avg || measureAgg == utils.Sum {
					ssResults.CurrResult -= frontVal.Value
				} else if measureAgg == utils.Count {
					ssResults.CurrResult--
				}
				ssResults.Window.Remove(front)
			} else {
				break
			}
		}
	}
	

	// Add the new element to the window
	switch measureAgg {
	case utils.Count:
		ssResults.CurrResult++
		ssResults.Window.PushBack(&structs.IndexValue{Index: currIndex, Value: colValue,})
	case utils.Sum, utils.Avg:
		ssResults.CurrResult += colValue
		ssResults.Window.PushBack(&structs.IndexValue{Index: currIndex, Value: colValue,})
	case utils.Min:
		for ssResults.Window.Len() > 0 {
			lastElement, correctType := ssResults.Window.Back().Value.(*structs.IndexValue)
			if !correctType {
				return 0.0, fmt.Errorf("performWindowStreamStatsOnSingleFunc Error, value in the window is not an IndexValue element")
			}
			if lastElement.Value >= colValue {
				ssResults.Window.Remove(ssResults.Window.Back())
			} else {
				break
			}
		}
		ssResults.Window.PushBack(&structs.IndexValue{Index: currIndex, Value: colValue,})
		ssResults.CurrResult = ssResults.Window.Front().Value.(*structs.IndexValue).Value
	case utils.Max:
		for ssResults.Window.Len() > 0 {
			lastElement, correctType := ssResults.Window.Back().Value.(*structs.IndexValue)
			if !correctType {
				return 0.0, fmt.Errorf("performWindowStreamStatsOnSingleFunc Error, value in the window is not an IndexValue element")
			}
			if lastElement.Value <= colValue {
				ssResults.Window.Remove(ssResults.Window.Back())
			} else {
				break
			}
		}
		ssResults.Window.PushBack(&structs.IndexValue{Index: currIndex, Value: colValue,})
		ssResults.CurrResult = ssResults.Window.Front().Value.(*structs.IndexValue).Value
	default:
		return 0.0, fmt.Errorf("performGlobalStreamStatsOnSingleFunc Error, measureAgg: %v not supported", measureAgg)
	}

	if measureAgg == utils.Avg {
		return ssResults.CurrResult / float64(ssResults.Window.Len()), nil
	}

	return ssResults.CurrResult, nil
}

func PerformStreamStatsOnSingleFunc(currIndex int, bucketKey string, ssOption *structs.StreamStatsOptions, measureFuncIndex int, measureAgg *structs.MeasureAggregator, record map[string]interface{}) (float64, error) {

	floatVal := 0.0
	var err error
	var result float64

	if measureAgg.MeasureFunc != utils.Count {
		recordVal, exist := record[measureAgg.MeasureCol]
		if !exist {
			return 0.0, fmt.Errorf("performStreamStatsOnSingleFunc Error, measure column: %v not found in the record", measureAgg.MeasureCol)
		}
		floatVal, err = dtypeutils.ConvertToFloat(recordVal, 64)
		// currently only supporting basic agg functions
		if err != nil {
			return 0.0, fmt.Errorf("performStreamStatsOnSingleFunc Error measure column %v does not have a numeric value, err: %v", measureAgg.MeasureCol, err)
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

	if bucketKey == "" && ssOption.Window == 0 {
		result, err = PerformGlobalStreamStatsOnSingleFunc(ssOption, ssOption.RunningStreamStats[measureFuncIndex][bucketKey], measureAgg.MeasureFunc, floatVal)
		if err != nil {
			return 0.0, fmt.Errorf("performStreamStatsOnSingleFunc Error while performing global stream stats on function %v for value %v, err: %v", measureAgg.MeasureFunc, floatVal, err)
		}
	} else {
		result, err = PerformWindowStreamStatsOnSingleFunc(currIndex, ssOption.RunningStreamStats[measureFuncIndex][bucketKey], int(ssOption.Window), measureAgg.MeasureFunc, floatVal)
		if err != nil {
			return 0.0, fmt.Errorf("performStreamStatsOnSingleFunc Error while performing window stream stats on function %v for value %v, err: %v", measureAgg.MeasureFunc, floatVal, err)
		}
	}

	return result, nil
}


func PerformStreamStats(nodeResult *structs.NodeResult, agg *structs.QueryAggregators, recs map[string]map[string]interface{}, recordIndexInFinal map[string]int, finalCols map[string]bool, finishesSegment bool) error {
	bucketKey := ""
	var err error

	if recs == nil {
		return nil
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

	if agg.StreamStatsOptions.RunningStreamStats == nil {
		agg.StreamStatsOptions.RunningStreamStats = make(map[int]map[string]*structs.RunningStreamStatsResults, 0)
	}

	currentOrder, err := GetOrderedRecs(agg.StreamStatsOptions.SegmentRecords, recordIndexInFinal)
	if err != nil {
		return fmt.Errorf("performStreamStats Error while fetching the order of the records, err: %v", err)
	}

	measureAggs := agg.MeasureOperations
	if agg.GroupByRequest != nil {
		measureAggs = agg.GroupByRequest.MeasureOperations
	}


	for currIndex, recordKey := range currentOrder {
		record, exist := agg.StreamStatsOptions.SegmentRecords[recordKey]
		if !exist {
			return fmt.Errorf("performStreamStats Error, record not found")
		}

		if agg.GroupByRequest != nil {
			bucketKey, err = GetBucketKey(record, agg.GroupByRequest)
			if err != nil {
				return fmt.Errorf("performStreamStats Error while creating bucket key, err: %v", err)
			}
		}
		
		for measureFuncIndex, measureAgg := range measureAggs {
			streamStatsResult, err := PerformStreamStatsOnSingleFunc(currIndex, bucketKey, agg.StreamStatsOptions, measureFuncIndex, measureAgg, record)
			if err != nil {
				return fmt.Errorf("performStreamStats Error while performing stream stats on function %v, err: %v", measureAgg.MeasureFunc, err)
			}
			record[measureAgg.String()] = streamStatsResult
		}
	}


	for _, measureAgg := range measureAggs {
		finalCols[measureAgg.String()] = true
	}

	for recordKey, record := range agg.StreamStatsOptions.SegmentRecords {
		recs[recordKey] = record
	}

	return nil
}