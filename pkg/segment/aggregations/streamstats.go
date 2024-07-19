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
	"sort"
	"strings"
	"time"

	"github.com/axiomhq/hyperloglog"
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
			return "", fmt.Errorf("GetBucketKey: Error: column: %v not found in the record", colName)
		}
		bucketKey += fmt.Sprintf("%v_", val)
	}
	return bucketKey, nil
}

func InitRunningStreamStatsResults(measureFunc utils.AggregateFunctions) *structs.RunningStreamStatsResults {
	defaultVal := 0.0
	if measureFunc == utils.Min {
		defaultVal = math.MaxFloat64
	} else if measureFunc == utils.Max {
		defaultVal = -math.MaxFloat64
	}
	return &structs.RunningStreamStatsResults{
		Window:          list.New(),
		CurrResult:      defaultVal,
		SecondaryWindow: list.New(),
	}
}

func InitRangeStat() *structs.RangeStat {
	return &structs.RangeStat{
		Min: math.MaxFloat64,
		Max: -math.MaxFloat64,
	}
}

func getValuesNoWindow(valuesMap map[string]struct{}) string {
	uniqueStrings := make([]string, 0)
	for str := range valuesMap {
		uniqueStrings = append(uniqueStrings, str)
	}
	sort.Strings(uniqueStrings)
	return strings.Join(uniqueStrings, "&nbsp")
}

func getValuesWindow(valuesMap map[string]int) string {
	uniqueStrings := make([]string, 0)
	for str := range valuesMap {
		uniqueStrings = append(uniqueStrings, str)
	}
	sort.Strings(uniqueStrings)
	return strings.Join(uniqueStrings, "&nbsp")
}

func PerformNoWindowStreamStatsOnSingleFunc(ssOption *structs.StreamStatsOptions, ssResults *structs.RunningStreamStatsResults, measureAgg utils.AggregateFunctions, colValue interface{}) (interface{}, bool, error) {
	var result interface{}
	if measureAgg == utils.Values && !ssOption.Current {
		// getting values is expensive only do when required
		result = getValuesNoWindow(ssResults.ValuesMap)
	} else {
		result = ssResults.CurrResult
	}

	valExist := ssResults.NumProcessedRecords > 0

	if measureAgg == utils.Avg && valExist {
		result = ssResults.CurrResult / float64(ssResults.NumProcessedRecords)
	}

	switch measureAgg {
	case utils.Count:
		ssResults.CurrResult++
	case utils.Sum, utils.Avg:
		floatVal, err := dtypeutils.ConvertToFloat(colValue, 64)
		if err != nil {
			return 0.0, false, fmt.Errorf("PerformNoWindowStreamStatsOnSingleFunc: Error: measure column %v does not have a numeric value, function: %v, err: %v", colValue, measureAgg, err)
		}
		ssResults.CurrResult += floatVal
	case utils.Min:
		floatVal, err := dtypeutils.ConvertToFloat(colValue, 64)
		if err != nil {
			return 0.0, false, fmt.Errorf("PerformNoWindowStreamStatsOnSingleFunc: Error: measure column %v does not have a numeric value, function: %v, err: %v", colValue, measureAgg, err)
		}
		if floatVal < ssResults.CurrResult {
			ssResults.CurrResult = floatVal
		}
	case utils.Max:
		floatVal, err := dtypeutils.ConvertToFloat(colValue, 64)
		if err != nil {
			return 0.0, false, fmt.Errorf("PerformNoWindowStreamStatsOnSingleFunc: Error: measure column %v does not have a numeric value, function: %v, err: %v", colValue, measureAgg, err)
		}
		if floatVal > ssResults.CurrResult {
			ssResults.CurrResult = floatVal
		}
	case utils.Range:
		floatVal, err := dtypeutils.ConvertToFloat(colValue, 64)
		if err != nil {
			return 0.0, false, fmt.Errorf("PerformNoWindowStreamStatsOnSingleFunc: Error: measure column %v does not have a numeric value, function: %v, err: %v", colValue, measureAgg, err)
		}
		if ssResults.RangeStat == nil {
			ssResults.RangeStat = InitRangeStat()
		}
		if floatVal < ssResults.RangeStat.Min {
			ssResults.RangeStat.Min = floatVal
		}
		if floatVal > ssResults.RangeStat.Max {
			ssResults.RangeStat.Max = floatVal
		}
		ssResults.CurrResult = ssResults.RangeStat.Max - ssResults.RangeStat.Min
	case utils.Cardinality:
		strValue := fmt.Sprintf("%v", colValue)
		if ssResults.CardinalityHLL == nil {
			ssResults.CardinalityHLL = hyperloglog.New()
		}
		ssResults.CardinalityHLL.Insert([]byte(strValue))
		ssResults.CurrResult = float64(ssResults.CardinalityHLL.Estimate())
	case utils.Values:
		strValue := fmt.Sprintf("%v", colValue)
		if ssResults.ValuesMap == nil {
			ssResults.ValuesMap = make(map[string]struct{}, 0)
		}
		ssResults.ValuesMap[strValue] = struct{}{}
	default:
		return 0.0, false, fmt.Errorf("PerformNoWindowStreamStatsOnSingleFunc: Error: measureAgg: %v not supported", measureAgg)
	}

	ssResults.NumProcessedRecords++

	if !ssOption.Current {
		return result, valExist, nil
	}

	if measureAgg == utils.Avg {
		return ssResults.CurrResult / float64(ssResults.NumProcessedRecords), true, nil
	}
	if measureAgg == utils.Values {
		return getValuesNoWindow(ssResults.ValuesMap), true, nil
	}

	return ssResults.CurrResult, true, nil
}

// Remove the front element from the window
func removeFrontElementFromWindow(window *list.List, ssResults *structs.RunningStreamStatsResults, measureAgg utils.AggregateFunctions) error {
	front := window.Front()
	frontElement, correctType := front.Value.(*structs.RunningStreamStatsWindowElement)
	if !correctType {
		return fmt.Errorf("removeFrontElementFromWindow: Error: element in the window is not a RunningStreamStatsWindowElement, it's of type: %T", front.Value)
	}

	// Update the current result
	if measureAgg == utils.Avg || measureAgg == utils.Sum {
		floatVal, err := dtypeutils.ConvertToFloat(frontElement.Value, 64)
		if err != nil {
			return fmt.Errorf("removeFrontElementFromWindow: Error: front element in the window does not have a numeric value, has value: %v, function: %v, err: %v", frontElement.Value, measureAgg, err)
		}
		ssResults.CurrResult -= floatVal
	} else if measureAgg == utils.Count {
		ssResults.CurrResult--
	} else if measureAgg == utils.Cardinality || measureAgg == utils.Values {
		strValue := fmt.Sprintf("%v", frontElement.Value)
		_, exist := ssResults.CardinalityMap[strValue]
		if exist {
			ssResults.CardinalityMap[strValue]--
			if ssResults.CardinalityMap[strValue] == 0 {
				delete(ssResults.CardinalityMap, strValue)
			}
		} else {
			return fmt.Errorf("removeFrontElementFromWindow: Error: cardinality map does not contain the value: %v which is present in the window", strValue)
		}
		ssResults.CurrResult = float64(len(ssResults.CardinalityMap))
	}

	window.Remove(window.Front())

	return nil
}

func performCleanWindow(currIndex int, global bool, window *list.List, ssResults *structs.RunningStreamStatsResults, windowSize int, measureAgg utils.AggregateFunctions) error {
	if global {
		for window.Len() > 0 {
			front := window.Front()
			frontVal, correctType := front.Value.(*structs.RunningStreamStatsWindowElement)
			if !correctType {
				return fmt.Errorf("cleanWindow: Error: element in the window is not a RunningStreamStatsWindowElement, it's of type: %T", front.Value)
			}
			if frontVal.Index+windowSize <= currIndex {
				err := removeFrontElementFromWindow(window, ssResults, measureAgg)
				if err != nil {
					return fmt.Errorf("cleanWindow: Error while removing front element from the window, err: %v", err)
				}
			} else {
				break
			}
		}
	} else {
		for window.Len() > windowSize {
			err := removeFrontElementFromWindow(window, ssResults, measureAgg)
			if err != nil {
				return fmt.Errorf("cleanWindow: Error while removing front element from the window, err: %v", err)
			}
		}
	}

	return nil
}

// Remove elements from the window that are outside the window size
func cleanWindow(currIndex int, global bool, ssResults *structs.RunningStreamStatsResults, windowSize int, measureAgg utils.AggregateFunctions) error {

	err := performCleanWindow(currIndex, global, ssResults.Window, ssResults, windowSize, measureAgg)
	if err != nil {
		return fmt.Errorf("cleanWindow: Error while cleaning the primary window, err: %v", err)
	}

	if measureAgg == utils.Range {
		err = performCleanWindow(currIndex, global, ssResults.SecondaryWindow, ssResults, windowSize, measureAgg)
		if err != nil {
			return fmt.Errorf("cleanWindow: Error while cleaning the secondary window, err: %v", err)
		}
	}

	return nil
}

func performCleanTimeWindow(thresholdTime uint64, timeSortAsc bool, window *list.List, ssResults *structs.RunningStreamStatsResults, measureAgg utils.AggregateFunctions) error {
	for window.Len() > 0 {
		front := window.Front()
		frontVal, correctType := front.Value.(*structs.RunningStreamStatsWindowElement)
		if !correctType {
			return fmt.Errorf("cleanWindow: Error: element in the window is not a RunningStreamStatsWindowElement, it's of type: %T", front.Value)
		}
		eventTimestamp := frontVal.TimeInMilli
		if (timeSortAsc && eventTimestamp < thresholdTime) || (!timeSortAsc && eventTimestamp > thresholdTime) {
			err := removeFrontElementFromWindow(window, ssResults, measureAgg)
			if err != nil {
				return fmt.Errorf("cleanTimeWindow: Error while removing front element from the window, timeSortAsc: %v, err: %v", timeSortAsc, err)
			}
		} else {
			break
		}
	}

	return nil
}

// Remove elements from the window that are outside the time window
func cleanTimeWindow(currTimestamp uint64, timeSortAsc bool, timeWindow *structs.BinSpanLength, ssResults *structs.RunningStreamStatsResults, measureAgg utils.AggregateFunctions) error {

	currTime := time.UnixMilli(int64(currTimestamp)).In(time.Local)
	offsetNum := int64(timeWindow.Num)
	if timeSortAsc {
		offsetNum = -offsetNum
	}
	offsetTime, err := utils.ApplyOffsetToTime(offsetNum, timeWindow.TimeScale, currTime)
	if err != nil {
		return fmt.Errorf("cleanTimeWindow: Error while applying offset to time, timeSortAsc: %v, err: %v", timeSortAsc, err)
	}
	thresholdTime := uint64(offsetTime.UnixMilli())

	err = performCleanTimeWindow(thresholdTime, timeSortAsc, ssResults.Window, ssResults, measureAgg)
	if err != nil {
		return fmt.Errorf("cleanTimeWindow: Error while cleaning the primary window, err: %v", err)
	}
	if measureAgg == utils.Range {
		err = performCleanTimeWindow(thresholdTime, timeSortAsc, ssResults.SecondaryWindow, ssResults, measureAgg)
		if err != nil {
			return fmt.Errorf("cleanTimeWindow: Error while cleaning the secondary window, err: %v", err)
		}
	}

	return nil
}

func getResults(ssResults *structs.RunningStreamStatsResults, measureAgg utils.AggregateFunctions) (interface{}, bool, error) {
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
		firstElementFloatVal, err := getListElementAsFloatFromWindow(ssResults.Window.Front())
		if err != nil {
			return 0.0, false, fmt.Errorf("getResults: Error while getting float value from first window element, err: %v", err)
		}
		ssResults.CurrResult = firstElementFloatVal
		return ssResults.CurrResult, true, nil
	case utils.Range:
		maxFloatVal, err := getListElementAsFloatFromWindow(ssResults.Window.Front())
		if err != nil {
			return 0.0, false, fmt.Errorf("getResults: Error while getting float value from first window element, err: %v", err)
		}
		minFloatval, err := getListElementAsFloatFromWindow(ssResults.SecondaryWindow.Front())
		if err != nil {
			return 0.0, false, fmt.Errorf("getResults: Error while getting float value from first window element, err: %v", err)
		}
		ssResults.CurrResult = maxFloatVal - minFloatval
		return ssResults.CurrResult, true, nil
	case utils.Cardinality:
		return ssResults.CurrResult, true, nil
	case utils.Values:
		return getValuesWindow(ssResults.CardinalityMap), true, nil
	default:
		return 0.0, false, fmt.Errorf("getResults: Error measureAgg: %v not supported", measureAgg)
	}
}

func getListElementAsFloatFromWindow(listElement *list.Element) (float64, error) {
	if listElement == nil {
		return 0.0, fmt.Errorf("getListElementAsFloatFromWindow: Error: listElement is nil")
	}

	windowElement, correctType := listElement.Value.(*structs.RunningStreamStatsWindowElement)
	if !correctType {
		return 0, fmt.Errorf("getListElementAsFloatFromWindow: Error: element in the window is not a RunningStreamStatsWindowElement, it's of type: %T", listElement.Value)
	}
	floatVal, err := dtypeutils.ConvertToFloat(windowElement.Value, 64)
	if err != nil {
		return 0.0, fmt.Errorf("getListElementAsFloatFromWindow: Error: element in window does not have a numeric value, has value %v, err: %v", windowElement.Value, err)
	}

	return floatVal, nil
}

func manageMinWindow(window *list.List, index int, newValue float64, timestamp uint64) error {
	for window.Len() > 0 {
		lastElementFloatVal, err := getListElementAsFloatFromWindow(window.Back())
		if err != nil {
			return fmt.Errorf("performMeasureFunc: Error while getting float value from last window element, err: %v", err)
		}
		if lastElementFloatVal >= newValue {
			window.Remove(window.Back())
		} else {
			break
		}
	}
	window.PushBack(&structs.RunningStreamStatsWindowElement{Index: index, Value: newValue, TimeInMilli: timestamp})
	return nil
}

func manageMaxWindow(window *list.List, index int, newValue float64, timestamp uint64) error {
	for window.Len() > 0 {
		lastElementFloatVal, err := getListElementAsFloatFromWindow(window.Back())
		if err != nil {
			return fmt.Errorf("performMeasureFunc: Error while getting float value from last window element, err: %v", err)
		}
		if lastElementFloatVal <= newValue {
			window.Remove(window.Back())
		} else {
			break
		}
	}
	window.PushBack(&structs.RunningStreamStatsWindowElement{Index: index, Value: newValue, TimeInMilli: timestamp})
	return nil
}

func performMeasureFunc(currIndex int, ssResults *structs.RunningStreamStatsResults, measureAgg utils.AggregateFunctions, colValue interface{}, timestamp uint64) (interface{}, error) {
	switch measureAgg {
	case utils.Count:
		ssResults.CurrResult++
		ssResults.Window.PushBack(&structs.RunningStreamStatsWindowElement{Index: currIndex, Value: colValue, TimeInMilli: timestamp})
	case utils.Sum, utils.Avg:
		floatVal, err := dtypeutils.ConvertToFloat(colValue, 64)
		if err != nil {
			return 0.0, fmt.Errorf("performMeasureFunc: Error measure column value %v is not a numeric value, err: %v", colValue, err)
		}
		ssResults.CurrResult += floatVal
		ssResults.Window.PushBack(&structs.RunningStreamStatsWindowElement{Index: currIndex, Value: colValue, TimeInMilli: timestamp})
	case utils.Min:
		floatColVal, err := dtypeutils.ConvertToFloat(colValue, 64)
		if err != nil {
			return 0.0, fmt.Errorf("performMeasureFunc: Error measure column value %v is not a numeric value, err: %v", colValue, err)
		}
		err = manageMinWindow(ssResults.Window, currIndex, floatColVal, timestamp)
		if err != nil {
			return 0.0, fmt.Errorf("performMeasureFunc: Error while managing min window, err: %v", err)
		}
		firstElementFloatVal, err := getListElementAsFloatFromWindow(ssResults.Window.Front())
		if err != nil {
			return 0.0, fmt.Errorf("performMeasureFunc: Error while getting float value from first window element, err: %v", err)
		}
		ssResults.CurrResult = firstElementFloatVal
	case utils.Max:
		floatColVal, err := dtypeutils.ConvertToFloat(colValue, 64)
		if err != nil {
			return 0.0, fmt.Errorf("performMeasureFunc: Error measure column value %v is not a numeric value, err: %v", colValue, err)
		}
		err = manageMaxWindow(ssResults.Window, currIndex, floatColVal, timestamp)
		if err != nil {
			return 0.0, fmt.Errorf("performMeasureFunc: Error while managing max window, err: %v", err)
		}
		firstElementFloatVal, err := getListElementAsFloatFromWindow(ssResults.Window.Front())
		if err != nil {
			return 0.0, fmt.Errorf("performMeasureFunc: Error while getting float value from first window element, err: %v", err)
		}
		ssResults.CurrResult = firstElementFloatVal
	case utils.Range:
		floatColVal, err := dtypeutils.ConvertToFloat(colValue, 64)
		if err != nil {
			return 0.0, fmt.Errorf("performMeasureFunc: Error measure column value %v is not a numeric value, err: %v", colValue, err)
		}
		err = manageMaxWindow(ssResults.Window, currIndex, floatColVal, timestamp)
		if err != nil {
			return 0.0, fmt.Errorf("performMeasureFunc: Error while managing max window, err: %v", err)
		}
		err = manageMinWindow(ssResults.SecondaryWindow, currIndex, floatColVal, timestamp)
		if err != nil {
			return 0.0, fmt.Errorf("performMeasureFunc: Error while managing min window, err: %v", err)
		}
		maxFloatVal, err := getListElementAsFloatFromWindow(ssResults.Window.Front())
		if err != nil {
			return 0.0, fmt.Errorf("performMeasureFunc: Error while getting float value from max window element, err: %v", err)
		}
		minFloatval, err := getListElementAsFloatFromWindow(ssResults.SecondaryWindow.Front())
		if err != nil {
			return 0.0, fmt.Errorf("performMeasureFunc: Error while getting float value from min window element, err: %v", err)
		}
		ssResults.CurrResult = maxFloatVal - minFloatval
	case utils.Cardinality, utils.Values:
		if ssResults.CardinalityMap == nil {
			ssResults.CardinalityMap = make(map[string]int, 0)
		}
		strValue := fmt.Sprintf("%v", colValue)
		_, exist := ssResults.CardinalityMap[strValue]
		if !exist {
			ssResults.CardinalityMap[strValue] = 1
		} else {
			ssResults.CardinalityMap[strValue]++
		}
		ssResults.CurrResult = float64(len(ssResults.CardinalityMap))
		ssResults.Window.PushBack(&structs.RunningStreamStatsWindowElement{Index: currIndex, Value: colValue, TimeInMilli: timestamp})
	default:
		return 0.0, fmt.Errorf("performMeasureFunc: Error measureAgg: %v not supported", measureAgg)
	}

	if measureAgg == utils.Avg {
		return ssResults.CurrResult / float64(ssResults.Window.Len()), nil
	}
	if measureAgg == utils.Values {
		return getValuesWindow(ssResults.CardinalityMap), nil
	}

	return ssResults.CurrResult, nil
}

func PerformWindowStreamStatsOnSingleFunc(currIndex int, ssOption *structs.StreamStatsOptions, ssResults *structs.RunningStreamStatsResults,
	windowSize int, measureAgg utils.AggregateFunctions, colValue interface{}, timestamp uint64,
	timeSortAsc bool) (interface{}, bool, error) {
	var err error
	var result interface{}
	result = ssResults.CurrResult
	exist := ssResults.Window.Len() > 0
	if exist && measureAgg == utils.Avg {
		result = ssResults.CurrResult / float64(ssResults.Window.Len())
	}

	if ssOption.TimeWindow != nil {
		err := cleanTimeWindow(timestamp, timeSortAsc, ssOption.TimeWindow, ssResults, measureAgg)
		if err != nil {
			return 0.0, false, fmt.Errorf("PerformWindowStreamStatsOnSingleFunc: Error while cleaning the time window, err: %v", err)
		}
	}

	// If current is false, compute result before adding the new element to the window
	if !ssOption.Current && windowSize != 0 {
		err := cleanWindow(currIndex-1, ssOption.Global, ssResults, windowSize, measureAgg)
		if err != nil {
			return 0.0, false, fmt.Errorf("PerformWindowStreamStatsOnSingleFunc: Error while cleaning the window, err: %v", err)
		}
		result, exist, err = getResults(ssResults, measureAgg)
		if err != nil {
			return 0.0, false, fmt.Errorf("PerformWindowStreamStatsOnSingleFunc: Error while getting results from the window, err: %v", err)
		}
	}

	if windowSize != 0 {
		if ssOption.Global {
			err = cleanWindow(currIndex, ssOption.Global, ssResults, windowSize, measureAgg)
		} else {
			err = cleanWindow(currIndex, ssOption.Global, ssResults, windowSize-1, measureAgg)
		}
		if err != nil {
			return 0.0, false, fmt.Errorf("PerformWindowStreamStatsOnSingleFunc: Error while cleaning the window, err: %v", err)
		}
	}

	// Add the new element to the window
	latestResult, err := performMeasureFunc(currIndex, ssResults, measureAgg, colValue, timestamp)
	if err != nil {
		return 0.0, false, fmt.Errorf("PerformWindowStreamStatsOnSingleFunc: Error while performing measure function %v, err: %v", measureAgg, err)
	}

	if !ssOption.Current {
		return result, exist, nil
	}

	return latestResult, true, nil
}

func PerformStreamStatsOnSingleFunc(currIndex int, bucketKey string, ssOption *structs.StreamStatsOptions, measureFuncIndex int, measureAgg *structs.MeasureAggregator, record map[string]interface{}, timestamp uint64, timeSortAsc bool) (interface{}, bool, error) {

	var err error
	var result interface{}

	colValue, exist := record[measureAgg.MeasureCol]
	if !exist {
		return 0.0, false, fmt.Errorf("PerformStreamStatsOnSingleFunc: Error, measure column: %v not found in the record", measureAgg.MeasureCol)
	}

	_, exist = ssOption.RunningStreamStats[measureFuncIndex]
	if !exist {
		ssOption.RunningStreamStats[measureFuncIndex] = make(map[string]*structs.RunningStreamStatsResults)
	}

	_, exist = ssOption.RunningStreamStats[measureFuncIndex][bucketKey]
	if !exist {
		ssOption.RunningStreamStats[measureFuncIndex][bucketKey] = InitRunningStreamStatsResults(measureAgg.MeasureFunc)
	}

	if ssOption.Window == 0 && ssOption.TimeWindow == nil {
		result, exist, err = PerformNoWindowStreamStatsOnSingleFunc(ssOption, ssOption.RunningStreamStats[measureFuncIndex][bucketKey], measureAgg.MeasureFunc, colValue)
		if err != nil {
			return 0.0, false, fmt.Errorf("PerformStreamStatsOnSingleFunc: Error while performing global stream stats on function %v for value %v, err: %v", measureAgg.MeasureFunc, colValue, err)
		}
	} else {
		result, exist, err = PerformWindowStreamStatsOnSingleFunc(currIndex, ssOption, ssOption.RunningStreamStats[measureFuncIndex][bucketKey], int(ssOption.Window), measureAgg.MeasureFunc, colValue, timestamp, timeSortAsc)
		if err != nil {
			return 0.0, false, fmt.Errorf("PerformStreamStatsOnSingleFunc: Error while performing window stream stats on function %v for value %v, err: %v", measureAgg.MeasureFunc, colValue, err)
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

func PerformStreamStatOnSingleRecord(nodeResult *structs.NodeResult, agg *structs.QueryAggregators, currIndex int, currentBucketKey string, record map[string]interface{}, measureAggs []*structs.MeasureAggregator, numPrevSegmentProcessedRecords uint64, timeInMilli uint64, timeSort bool, timeSortAsc bool) (int, uint64, string, error) {
	bucketKey := ""
	var err error
	if agg.GroupByRequest != nil {
		bucketKey, err = GetBucketKey(record, agg.GroupByRequest)
		if err != nil {
			return 0, 0, "", fmt.Errorf("PerformStreamStatOnSingleRecord: Error while creating bucket key, err: %v", err)
		}
	}

	if agg.StreamStatsOptions.ResetOnChange && currentBucketKey != bucketKey {
		resetAccumulatedStreamStats(agg.StreamStatsOptions)
		currIndex = 0
		numPrevSegmentProcessedRecords = 0
	}

	shouldResetBefore, err := evaluateResetCondition(agg.StreamStatsOptions.ResetBefore, record)
	if err != nil {
		return 0, 0, "", fmt.Errorf("PerformStreamStatOnSingleRecord: Error while evaluating resetBefore condition, err: %v", err)
	}
	if shouldResetBefore {
		resetAccumulatedStreamStats(agg.StreamStatsOptions)
		currIndex = 0
		numPrevSegmentProcessedRecords = 0
	}

	for measureFuncIndex, measureAgg := range measureAggs {
		streamStatsResult, exist, err := PerformStreamStatsOnSingleFunc(int(numPrevSegmentProcessedRecords)+currIndex, bucketKey, agg.StreamStatsOptions, measureFuncIndex, measureAgg, record, timeInMilli, timeSortAsc)
		if err != nil {
			return 0, 0, "", fmt.Errorf("PerformStreamStatOnSingleRecord: Error while performing stream stats on function %v, err: %v", measureAgg.MeasureFunc, err)
		}
		if exist {
			record[measureAgg.String()] = streamStatsResult
		} else {
			if measureAgg.MeasureFunc == utils.Count || measureAgg.MeasureFunc == utils.Cardinality {
				record[measureAgg.String()] = 0.0
			} else {
				record[measureAgg.String()] = ""
			}
		}
	}
	agg.StreamStatsOptions.NumProcessedRecords++
	currIndex++

	shouldResetAfter, err := evaluateResetCondition(agg.StreamStatsOptions.ResetAfter, record)
	if err != nil {
		return 0, 0, "", fmt.Errorf("PerformStreamStatOnSingleRecord: Error while evaluating resetAfter condition, err: %v", err)
	}
	if shouldResetAfter {
		resetAccumulatedStreamStats(agg.StreamStatsOptions)
		currIndex = 0
		numPrevSegmentProcessedRecords = 0
	}

	return currIndex, numPrevSegmentProcessedRecords, bucketKey, nil
}

func PerformStreamStatsOnRawRecord(nodeResult *structs.NodeResult, agg *structs.QueryAggregators, recs map[string]map[string]interface{}, recordIndexInFinal map[string]int, finalCols map[string]bool, finishesSegment bool, timeSort bool, timeSortAsc bool) error {
	if !timeSort && agg.StreamStatsOptions.TimeWindow != nil {
		return fmt.Errorf("PerformStreamStatsOnRawRecord: Error: For time_window to be used the records must be sorted by time")
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
		return fmt.Errorf("PerformStreamStatsOnRawRecord: Error while fetching the order of the records, err: %v", err)
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
			return fmt.Errorf("PerformStreamStatsOnRawRecord: Error: record not found")
		}

		timestamp, exist := record["timestamp"]
		if !exist {
			return fmt.Errorf("PerformStreamStatsOnRawRecord: Error: timestamp not found in the record")
		}
		timeInMilli, err := dtypeutils.ConvertToUInt(timestamp, 64)
		if err != nil {
			return fmt.Errorf("PerformStreamStatsOnRawRecord: Error: timestamp not a valid uint64 value, has value: %v", timestamp)
		}

		// record would be updated in this method
		currIndex, numPrevSegmentProcessedRecords, currentBucketKey, err = PerformStreamStatOnSingleRecord(nodeResult, agg, currIndex, currentBucketKey, record, measureAggs, numPrevSegmentProcessedRecords, timeInMilli, timeSort, timeSortAsc)
		if err != nil {
			return fmt.Errorf("PerformStreamStatsOnRawRecord: Error while performing stream stats on record, err: %v", err)
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
		err := performStreamStatsOnHistogram(nodeResult, agg.StreamStatsOptions, agg)
		if err != nil {
			return fmt.Errorf("PerformStreamStats: Error while performing stream stats on histogram, err: %v", err)
		}
	}

	if len(nodeResult.MeasureResults) > 0 {
		err := performStreamStatsOnMeasureResults(nodeResult, agg.StreamStatsOptions, agg)
		if err != nil {
			return fmt.Errorf("PerformStreamStats: Error while performing stream stats on measure results, err: %v", err)
		}
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
		return fmt.Errorf("performStreamStatsOnHistogram: Error: Time window cannot be applied to histograms")
	}

	// Fetch the fields from group by request and measure operations
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
	currentBucketKey := ""
	numPrevSegmentProcessedRecords := ssOption.NumProcessedRecords
	for _, aggregationResult := range nodeResult.Histogram {
		for rowIndex, bucketResult := range aggregationResult.Results {
			// Get the values of all the necessary fields.
			fieldToValue := make(map[string]utils.CValueEnclosure, 0)
			err := getAggregationResultFieldValues(fieldToValue, fieldsInExpr, aggregationResult, rowIndex)
			if err != nil {
				return fmt.Errorf("performStreamStatsOnHistogram: Error while getting value from agg results, err: %v", err)
			}
			record := getRecordFromFieldToValue(fieldToValue)

			// record would be updated in this method
			currIndex, numPrevSegmentProcessedRecords, currentBucketKey, err = PerformStreamStatOnSingleRecord(nodeResult, agg, currIndex, currentBucketKey, record, measureAggs, numPrevSegmentProcessedRecords, 0, false, false)
			if err != nil {
				return fmt.Errorf("performStreamStatsOnHistogram: Error while performing stream stats on record, err: %v", err)
			}

			for _, measureAgg := range measureAggs {
				// Check if the column to create already exists and is a GroupBy column.
				isGroupByCol := putils.SliceContainsString(nodeResult.GroupByCols, measureAgg.String())

				streamStatsResult, resultPresent := record[measureAgg.String()]
				if !resultPresent {
					return fmt.Errorf("performStreamStatsOnHistogram: Error while fetching result for measureAgg: %v", measureAgg.String())
				}

				// Set the appropriate column to the computed value.
				if isGroupByCol {
					for keyIndex, groupByCol := range bucketResult.GroupByKeys {
						if measureAgg.String() != groupByCol {
							continue
						}

						streamStatsStr := fmt.Sprintf("%v", streamStatsResult)

						// Set the appropriate element of BucketKey to cellValueStr.
						switch bucketKey := bucketResult.BucketKey.(type) {
						case []string:
							bucketKey[keyIndex] = streamStatsStr
							bucketResult.BucketKey = bucketKey
						case string:
							if keyIndex != 0 {
								return fmt.Errorf("performStreamStatsOnHistogram: expected keyIndex to be 0, not %v", keyIndex)
							}
							bucketResult.BucketKey = streamStatsStr
						default:
							return fmt.Errorf("performStreamStatsOnHistogram: bucket key has unexpected type: %T", bucketKey)
						}
					}
				} else {
					if streamStatsResult != "" {
						dataType := utils.SS_DT_FLOAT
						if measureAgg.MeasureFunc == utils.Values {
							dataType = utils.SS_DT_STRING
						}
						aggregationResult.Results[rowIndex].StatRes[measureAgg.String()] = utils.CValueEnclosure{
							Dtype: dataType,
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
	}

	return nil
}

func performStreamStatsOnMeasureResults(nodeResult *structs.NodeResult, ssOption *structs.StreamStatsOptions, agg *structs.QueryAggregators) error {

	if ssOption.TimeWindow != nil {
		return fmt.Errorf("performStreamStatsOnMeasureResults: Error: Time window cannot be applied to measure results")
	}

	// Fetch the fields from group by request and measure operations
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
	currentBucketKey := ""
	numPrevSegmentProcessedRecords := ssOption.NumProcessedRecords
	// Compute the value for each row.
	for rowIndex, bucketHolder := range nodeResult.MeasureResults {
		// Get the values of all the necessary fields.
		fieldToValue := make(map[string]utils.CValueEnclosure, 0)
		err := getMeasureResultsFieldValues(fieldToValue, fieldsInExpr, nodeResult, rowIndex)
		if err != nil {
			return fmt.Errorf("performStreamStatsOnMeasureResults: Error while getting value from measure results, err: %v", err)
		}

		record := getRecordFromFieldToValue(fieldToValue)
		// record would be updated in this method
		currIndex, numPrevSegmentProcessedRecords, currentBucketKey, err = PerformStreamStatOnSingleRecord(nodeResult, agg, currIndex, currentBucketKey, record, measureAggs, numPrevSegmentProcessedRecords, 0, false, false)
		if err != nil {
			return fmt.Errorf("performStreamStatsOnMeasureResults: Error while performing stream stats on record, err: %v", err)
		}

		for _, measureAgg := range measureAggs {
			streamStatsResult, resultPresent := record[measureAgg.String()]
			if !resultPresent {
				return fmt.Errorf("performStreamStatsOnMeasureResults: Error while fetching result for measureAgg: %v", measureAgg.String())
			}

			// Check if the column already exists.
			isGroupByCol := false
			colIndex := -1 // Index in GroupByCols or MeasureFunctions.
			for i, measureCol := range nodeResult.MeasureFunctions {
				if measureAgg.String() == measureCol {
					// We'll write over this existing column.
					isGroupByCol = false
					colIndex = i
					break
				}
			}

			for i, groupByCol := range nodeResult.GroupByCols {
				if measureAgg.String() == groupByCol {
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
				nodeResult.MeasureFunctions = append(nodeResult.MeasureFunctions, measureAgg.String())
			}

			// Set the appropriate column to the computed value.
			if isGroupByCol {
				bucketHolder.GroupByValues[colIndex] = fmt.Sprintf("%v", streamStatsResult)
			} else {
				bucketHolder.MeasureVal[measureAgg.String()] = fmt.Sprintf("%v", streamStatsResult)
			}
		}
	}

	return nil
}
