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

package processor

import (
	"container/list"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/aggregations"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
)

type streamstatsProcessor struct {
	options          *structs.StreamStatsOptions
	currentIndex     int
	currentBucketKey string
}

func (p *streamstatsProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, io.EOF
	}

	timeValues, err := p.validateTimeWindow(iqr)
	if err != nil {
		return nil, err
	}

	measureAggs := p.options.MeasureOperations
	if p.options.GroupByRequest != nil {
		measureAggs = p.options.GroupByRequest.MeasureOperations
	}

	if p.options.RunningStreamStats == nil {
		p.options.RunningStreamStats = make(map[int]map[string]*structs.RunningStreamStatsResults)
	}

	requiredColumns := make(map[string]struct{})

	for _, measureAgg := range measureAggs {
		if measureAgg.MeasureCol != "" {
			requiredColumns[measureAgg.MeasureCol] = struct{}{}
		}
		if measureAgg.ValueColRequest != nil {
			for _, field := range measureAgg.ValueColRequest.GetFields() {
				requiredColumns[field] = struct{}{}
			}
		}
	}

	if p.options.GroupByRequest != nil {
		for _, col := range p.options.GroupByRequest.GroupByColumns {
			requiredColumns[col] = struct{}{}
		}
	}

	if p.options.ResetBefore != nil {
		for _, field := range p.options.ResetBefore.GetFields() {
			requiredColumns[field] = struct{}{}
		}
	}
	if p.options.ResetAfter != nil {
		for _, field := range p.options.ResetAfter.GetFields() {
			requiredColumns[field] = struct{}{}
		}
	}

	requiredValues := make(map[string][]sutils.CValueEnclosure)
	for colName := range requiredColumns {
		colValues, err := iqr.ReadColumn(colName)
		if err != nil {
			return nil, fmt.Errorf("streamstats.Process: failed to read column %s: %v", colName, err)
		}
		requiredValues[colName] = colValues
	}

	knownValues := make(map[string][]sutils.CValueEnclosure)

	for _, measureAgg := range measureAggs {
		resultCol := measureAgg.String()
		knownValues[resultCol] = make([]sutils.CValueEnclosure, iqr.NumberOfRecords())
	}

	bucketKey := ""
	p.currentBucketKey = bucketKey
	p.currentIndex = 0

	for i := 0; i < iqr.NumberOfRecords(); i++ {
		record := make(map[string]interface{})
		for colName, values := range requiredValues {
			if i < len(values) {
				record[colName] = values[i].CVal
			}
		}

		// Get bucket key first
		if p.options.GroupByRequest != nil {
			bucketKey = ""
			for _, colName := range p.options.GroupByRequest.GroupByColumns {
				if val, ok := requiredValues[colName]; ok && i < len(val) {
					bucketKey += fmt.Sprintf("%v_", val[i].CVal)
				}
			}
		}

		if p.options.ResetOnChange && p.currentBucketKey != bucketKey {
			resetAccumulatedStreamStats(p.options)
			p.currentIndex = 0
		}

		shouldResetBefore, err := evaluateResetCondition(p.options.ResetBefore, record)
		if err != nil {
			return nil, err
		}
		if shouldResetBefore {
			resetAccumulatedStreamStats(p.options)
			p.currentIndex = 0
		}

		for measIdx, measureAgg := range measureAggs {
			if _, ok := p.options.RunningStreamStats[measIdx]; !ok {
				p.options.RunningStreamStats[measIdx] = make(map[string]*structs.RunningStreamStatsResults)
			}
			if _, ok := p.options.RunningStreamStats[measIdx][bucketKey]; !ok {
				p.options.RunningStreamStats[measIdx][bucketKey] = InitRunningStreamStatsResults(measureAgg.MeasureFunc)
			}

			colValue := CreateCValueFromColValue(record[measureAgg.MeasureCol])
			fieldToValue := make(map[string]sutils.CValueEnclosure)
			if measureAgg.ValueColRequest != nil {
				for _, field := range measureAgg.ValueColRequest.GetFields() {
					if values, ok := requiredValues[field]; ok && i < len(values) {
						fieldToValue[field] = values[i]
					}
				}
			}

			finalColValue, include := CreateCValueFromValueExpression(measureAgg, fieldToValue, colValue)

			var result sutils.CValueEnclosure
			var exists bool

			if p.options.Window == 0 && p.options.TimeWindow == nil {
				result, exists, err = PerformNoWindowStreamStatsOnSingleFunc(
					p.options,
					p.options.RunningStreamStats[measIdx][bucketKey],
					measureAgg,
					finalColValue,
					include,
				)
			} else {
				var timeInMilli uint64
				if p.options.TimeWindow != nil {
					if err != nil {
						return nil, err
					}
					timeInMilli = timeValues[i].CVal.(uint64)
				}

				result, exists, err = PerformWindowStreamStatsOnSingleFunc(
					p.currentIndex,
					p.options,
					p.options.RunningStreamStats[measIdx][bucketKey],
					int(p.options.Window),
					measureAgg,
					finalColValue,
					timeInMilli,
					p.options.TimeSortAsc,
					include,
				)
			}

			if err != nil {
				return nil, fmt.Errorf("streamstats.Process: error processing measure %v: %v", measureAgg.String(), err)
			}

			resultCol := measureAgg.String()
			if exists {
				knownValues[resultCol][i] = result
			} else {
				if measureAgg.MeasureFunc == sutils.Count || measureAgg.MeasureFunc == sutils.Cardinality {
					knownValues[resultCol][i] = sutils.CValueEnclosure{
						Dtype: sutils.SS_DT_FLOAT,
						CVal:  float64(0),
					}
				} else {
					knownValues[resultCol][i] = sutils.CValueEnclosure{
						Dtype: sutils.SS_DT_STRING,
						CVal:  "",
					}
				}
			}
		}

		p.options.NumProcessedRecords++
		p.currentIndex++

		shouldResetAfter, err := evaluateResetCondition(p.options.ResetAfter, record)
		if err != nil {
			return nil, err
		}
		if shouldResetAfter {
			resetAccumulatedStreamStats(p.options)
			p.currentIndex = 0
		}

		p.currentBucketKey = bucketKey
	}

	err = iqr.AppendKnownValues(knownValues)
	if err != nil {
		return nil, fmt.Errorf("streamstats.Process: failed to append results: %v", err)
	}

	return iqr, nil
}

func (p *streamstatsProcessor) Rewind() {
	p.currentIndex = 0
	p.currentBucketKey = ""

	if p.options != nil {
		resetAccumulatedStreamStats(p.options)
	}
}

func (p *streamstatsProcessor) Cleanup() {
	// Nothing to cleanup
}

func (p *streamstatsProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}

func resetAccumulatedStreamStats(ssOption *structs.StreamStatsOptions) {
	ssOption.NumProcessedRecords = 0
	ssOption.RunningStreamStats = make(map[int]map[string]*structs.RunningStreamStatsResults, 0)
}

func InitRunningStreamStatsResults(measureFunc sutils.AggregateFunctions) *structs.RunningStreamStatsResults {
	runningSSResult := &structs.RunningStreamStatsResults{
		Window:          &utils.GobbableList{},
		SecondaryWindow: &utils.GobbableList{},
	}

	switch measureFunc {
	case sutils.Count, sutils.Sum, sutils.Avg, sutils.Range, sutils.Cardinality, sutils.Perc:
		runningSSResult.CurrResult = sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_FLOAT,
			CVal:  0.0,
		}
	default:
		runningSSResult.CurrResult = sutils.CValueEnclosure{}
	}

	return runningSSResult
}

func InitRangeStat() *structs.RangeStat {
	return &structs.RangeStat{
		Min: math.MaxFloat64,
		Max: -math.MaxFloat64,
	}
}

func getValues[T any](valuesMap map[string]T) sutils.CValueEnclosure {
	uniqueStrings := make([]string, 0)
	for str := range valuesMap {
		uniqueStrings = append(uniqueStrings, str)
	}
	sort.Strings(uniqueStrings)

	return sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING_SLICE,
		CVal:  uniqueStrings,
	}
}

// Incoming value e2 has to have float or string dtype
func GetNoWindowMinMax(e1 sutils.CValueEnclosure, e2 sutils.CValueEnclosure, isMin bool) (sutils.CValueEnclosure, error) {
	if e2.Dtype != sutils.SS_DT_FLOAT && e2.Dtype != sutils.SS_DT_STRING {
		return e1, fmt.Errorf("GetNoWindowMinMax: Error: e2 is invalid")
	}
	if e1.Dtype == sutils.SS_INVALID {
		return e2, nil
	}

	return sutils.ReduceMinMax(e1, e2, isMin)
}

func calculateAvg(ssResults *structs.RunningStreamStatsResults, window bool) sutils.CValueEnclosure {
	count := ssResults.NumProcessedRecords
	if window {
		count = uint64(ssResults.Window.Len())
	}
	return sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_FLOAT,
		CVal:  ssResults.CurrResult.CVal.(float64) / float64(count),
	}
}

func validateCurrResultDType(measureAgg sutils.AggregateFunctions, currResult sutils.CValueEnclosure) error {

	switch measureAgg {
	case sutils.Count, sutils.Sum, sutils.Avg, sutils.Range, sutils.Cardinality, sutils.Perc:
		if currResult.Dtype != sutils.SS_DT_FLOAT {
			return fmt.Errorf("validateCurrResultDType: Error: currResult value is not a float for measureAgg: %v", measureAgg)
		}
	default:
		// TODO: should this be an error?
		return nil
	}

	return nil
}

func PerformNoWindowStreamStatsOnSingleFunc(ssOption *structs.StreamStatsOptions, ssResults *structs.RunningStreamStatsResults,
	measureAgg *structs.MeasureAggregator, colValue sutils.CValueEnclosure, include bool) (sutils.CValueEnclosure, bool, error) {
	var result sutils.CValueEnclosure
	valExist := ssResults.NumProcessedRecords > 0

	if measureAgg.MeasureFunc == sutils.Values && !ssOption.Current {
		// getting values is expensive only do when required
		result = getValues(ssResults.ValuesMap)
	} else {
		if valExist {
			result = ssResults.CurrResult
		} else {
			result = sutils.CValueEnclosure{}
		}
	}

	if measureAgg.MeasureFunc == sutils.Avg && valExist {
		result = calculateAvg(ssResults, false)
	}

	if !include {
		return result, valExist, nil
	}

	err := validateCurrResultDType(measureAgg.MeasureFunc, ssResults.CurrResult)
	if err != nil {
		return sutils.CValueEnclosure{}, false, fmt.Errorf("PerformNoWindowStreamStatsOnSingleFunc: Error while validating currResult, err: %v", err)
	}

	switch measureAgg.MeasureFunc {
	case sutils.Count:
		ssResults.CurrResult.CVal = ssResults.CurrResult.CVal.(float64) + 1
	case sutils.Sum, sutils.Avg:
		if colValue.Dtype != sutils.SS_DT_FLOAT {
			return result, valExist, nil
		}
		ssResults.CurrResult.CVal = ssResults.CurrResult.CVal.(float64) + colValue.CVal.(float64)
	case sutils.Min, sutils.Max:
		isMin := measureAgg.MeasureFunc == sutils.Min
		resultCVal, err := GetNoWindowMinMax(ssResults.CurrResult, colValue, isMin)
		if err != nil {
			return result, valExist, nil
		}
		ssResults.CurrResult = resultCVal
	case sutils.Range:
		if colValue.Dtype != sutils.SS_DT_FLOAT {
			return result, valExist, nil
		}
		if ssResults.RangeStat == nil {
			ssResults.RangeStat = InitRangeStat()
		}
		aggregations.UpdateRangeStat(colValue.CVal.(float64), ssResults.RangeStat)
		ssResults.CurrResult.CVal = ssResults.RangeStat.Max - ssResults.RangeStat.Min
	case sutils.Cardinality:
		strValue := fmt.Sprintf("%v", colValue.CVal)
		if ssResults.CardinalityHLL == nil {
			ssResults.CardinalityHLL = structs.CreateNewHll()
		}
		ssResults.CardinalityHLL.AddRaw(xxhash.Sum64String(strValue))
		ssResults.CurrResult.CVal = float64(ssResults.CardinalityHLL.Cardinality())
	case sutils.Perc:
		if ssResults.PercTDigest == nil {
			ssResults.PercTDigest, err = utils.CreateNewTDigest()
			if err != nil {
				return result, valExist, nil
			}
		}
		err = ssResults.PercTDigest.InsertIntoTDigest(colValue.CVal.(float64))
		if err != nil {
			return result, valExist, err
		}
		// always between 0 and 100 (enforced by the peg parser)
		percentile := measureAgg.Param / 100
		ssResults.CurrResult.CVal = ssResults.PercTDigest.GetQuantile(percentile)
	case sutils.Values:
		strValue := fmt.Sprintf("%v", colValue.CVal)
		if ssResults.ValuesMap == nil {
			ssResults.ValuesMap = make(map[string]struct{}, 0)
		}
		ssResults.ValuesMap[strValue] = struct{}{}
	default:
		return sutils.CValueEnclosure{}, false, fmt.Errorf("PerformNoWindowStreamStatsOnSingleFunc: Error: measureAgg: %v not supported", measureAgg)
	}

	ssResults.NumProcessedRecords++

	if !ssOption.Current {
		return result, valExist, nil
	}

	if measureAgg.MeasureFunc == sutils.Avg {
		return calculateAvg(ssResults, false), true, nil
	}

	if measureAgg.MeasureFunc == sutils.Values {
		return getValues(ssResults.ValuesMap), true, nil
	}

	return ssResults.CurrResult, true, nil
}

// Remove the front element from the window
func removeFrontElementFromWindow(window *utils.GobbableList, ssResults *structs.RunningStreamStatsResults, measureAgg sutils.AggregateFunctions) error {
	front := window.Front()
	frontElement, correctType := front.Value.(*structs.RunningStreamStatsWindowElement)
	if !correctType {
		return fmt.Errorf("removeFrontElementFromWindow: Error: element in the window is not a RunningStreamStatsWindowElement, it's of type: %T", front.Value)
	}

	err := validateCurrResultDType(measureAgg, ssResults.CurrResult)
	if err != nil {
		return fmt.Errorf("removeFrontElementFromWindow: Error while validating currResult, err: %v", err)
	}

	// Update the current result
	if measureAgg == sutils.Avg || measureAgg == sutils.Sum {
		if frontElement.Value.Dtype != sutils.SS_DT_FLOAT {
			return fmt.Errorf("removeFrontElementFromWindow: Error: front element in the window does not have a numeric value, has value: %v, function: %v", frontElement.Value, measureAgg)
		}
		ssResults.CurrResult.CVal = ssResults.CurrResult.CVal.(float64) - frontElement.Value.CVal.(float64)
	} else if measureAgg == sutils.Count {
		ssResults.CurrResult.CVal = ssResults.CurrResult.CVal.(float64) - 1
	} else if measureAgg == sutils.Cardinality || measureAgg == sutils.Values {
		if frontElement.Value.Dtype != sutils.SS_DT_STRING {
			return fmt.Errorf("removeFrontElementFromWindow: Error: front element in the window does not have a string value, has value: %v, function: %v", frontElement.Value, measureAgg)
		}
		strValue := fmt.Sprintf("%v", frontElement.Value.CVal.(string))

		ssResults.CardinalityMap[strconv.FormatInt(int64(len(ssResults.CardinalityMap)), 10)+": remove "+strValue] = 1
		// _, exist := ssResults.CardinalityMap[strValue]
		// if exist {
		// 	ssResults.CardinalityMap[strValue]--
		// 	if ssResults.CardinalityMap[strValue] == 0 {
		// 		delete(ssResults.CardinalityMap, strValue)
		// 	}
		// } else {
		// 	return fmt.Errorf("removeFrontElementFromWindow: Error: cardinality map does not contain the value: %v which is present in the window", strValue)
		// }
		ssResults.CurrResult.CVal = float64(len(ssResults.CardinalityMap))
	}

	window.Remove(window.Front())

	return nil
}

func performCleanWindow(currIndex int, window *utils.GobbableList, ssResults *structs.RunningStreamStatsResults, windowSize int, measureAgg sutils.AggregateFunctions) error {
	for window.Len() > 0 {
		front := window.Front()
		frontVal, correctType := front.Value.(*structs.RunningStreamStatsWindowElement)
		if !correctType {
			return fmt.Errorf("cleanWindow: Error: element in the window is not a *RunningStreamStatsWindowElement, it's of type: %T", front.Value)
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

	return nil
}

// // Remove elements from the window that are outside the window size
func cleanWindow(currIndex int, ssResults *structs.RunningStreamStatsResults, windowSize int, measureAgg sutils.AggregateFunctions) error {

	err := performCleanWindow(currIndex, ssResults.Window, ssResults, windowSize, measureAgg)
	if err != nil {
		return fmt.Errorf("cleanWindow: Error while cleaning the primary window, err: %v", err)
	}

	if measureAgg == sutils.Range || measureAgg == sutils.Min || measureAgg == sutils.Max {
		err = performCleanWindow(currIndex, ssResults.SecondaryWindow, ssResults, windowSize, measureAgg)
		if err != nil {
			return fmt.Errorf("cleanWindow: Error while cleaning the secondary window, err: %v", err)
		}
	}

	return nil
}

func performCleanTimeWindow(thresholdTime uint64, timeSortAsc bool, window *utils.GobbableList, ssResults *structs.RunningStreamStatsResults, measureAgg sutils.AggregateFunctions) error {
	for window.Len() > 0 {
		front := window.Front()
		frontVal, correctType := front.Value.(*structs.RunningStreamStatsWindowElement)
		if !correctType {
			return fmt.Errorf("cleanWindow: Error: element in the window is not a *RunningStreamStatsWindowElement, it's of type: %T", front.Value)
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

// // Remove elements from the window that are outside the time window
func cleanTimeWindow(currTimestamp uint64, timeSortAsc bool, timeWindow *structs.BinSpanLength, ssResults *structs.RunningStreamStatsResults, measureAgg sutils.AggregateFunctions) error {

	currTime := time.UnixMilli(int64(currTimestamp)).In(time.Local)
	offsetNum := int64(timeWindow.Num)
	if timeSortAsc {
		offsetNum = -offsetNum
	}
	offsetTime, err := sutils.ApplyOffsetToTime(offsetNum, timeWindow.TimeScale, currTime)
	if err != nil {
		return fmt.Errorf("cleanTimeWindow: Error while applying offset to time, timeSortAsc: %v, err: %v", timeSortAsc, err)
	}
	thresholdTime := uint64(offsetTime.UnixMilli())

	err = performCleanTimeWindow(thresholdTime, timeSortAsc, ssResults.Window, ssResults, measureAgg)
	if err != nil {
		return fmt.Errorf("cleanTimeWindow: Error while cleaning the primary window, err: %v", err)
	}
	if measureAgg == sutils.Range || measureAgg == sutils.Min || measureAgg == sutils.Max {
		err = performCleanTimeWindow(thresholdTime, timeSortAsc, ssResults.SecondaryWindow, ssResults, measureAgg)
		if err != nil {
			return fmt.Errorf("cleanTimeWindow: Error while cleaning the secondary window, err: %v", err)
		}
	}

	return nil
}

func getResults(ssResults *structs.RunningStreamStatsResults, measureAgg sutils.AggregateFunctions) (sutils.CValueEnclosure, bool, error) {
	if ssResults.Window.Len() == 0 && ssResults.SecondaryWindow.Len() == 0 {
		return sutils.CValueEnclosure{}, false, nil
	}
	switch measureAgg {
	case sutils.Count:
		return ssResults.CurrResult, true, nil
	case sutils.Sum:
		return ssResults.CurrResult, true, nil
	case sutils.Avg:
		return calculateAvg(ssResults, true), true, nil
	case sutils.Min, sutils.Max:
		firstElementVal, err := getMinMaxElement(ssResults)
		if err != nil {
			return sutils.CValueEnclosure{}, false, nil
		}
		ssResults.CurrResult = firstElementVal
		return ssResults.CurrResult, true, nil
	case sutils.Range:
		if ssResults.Window.Len() == 0 {
			return sutils.CValueEnclosure{}, false, nil
		}

		// If both windows have data
		if ssResults.SecondaryWindow.Len() > 0 {
			maxFloatVal, err := getListElementAsFloatFromWindow(ssResults.Window.Front())
			if err != nil {
				return sutils.CValueEnclosure{}, false, fmt.Errorf("getResults: Error getting max value, err: %v", err)
			}

			minFloatval, err := getListElementAsFloatFromWindow(ssResults.SecondaryWindow.Front())
			if err != nil {
				return sutils.CValueEnclosure{}, false, fmt.Errorf("getResults: Error getting min value, err: %v", err)
			}

			return sutils.CValueEnclosure{
				Dtype: sutils.SS_DT_FLOAT,
				CVal:  maxFloatVal - minFloatval,
			}, true, nil
		}

		var maxVal, minVal float64
		first := true

		for e := ssResults.Window.Front(); e != nil; e = e.Next() {
			val, err := getListElementAsFloatFromWindow(e)
			if err != nil {
				continue
			}
			if first {
				maxVal, minVal = val, val
				first = false
				continue
			}
			if val > maxVal {
				maxVal = val
			}
			if val < minVal {
				minVal = val
			}
		}

		if first {
			return sutils.CValueEnclosure{}, false, nil
		}

		return sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_FLOAT,
			CVal:  maxVal - minVal,
		}, true, nil
	case sutils.Cardinality:
		return ssResults.CurrResult, true, nil
	case sutils.Perc:
		return ssResults.CurrResult, true, nil
	case sutils.Values:
		return getValues(ssResults.CardinalityMap), true, nil
	default:
		return sutils.CValueEnclosure{}, false, fmt.Errorf("getResults: Error measureAgg: %v not supported", measureAgg)
	}
}

func getListElementAsFloatFromWindow(listElement *list.Element) (float64, error) {
	if listElement == nil {
		return 0.0, fmt.Errorf("getListElementAsFloatFromWindow: Error: listElement is nil")
	}

	windowElement, correctType := listElement.Value.(*structs.RunningStreamStatsWindowElement)
	if !correctType {
		return 0, fmt.Errorf("getListElementAsFloatFromWindow: Error: element in the window is not a *RunningStreamStatsWindowElement, it's of type: %T", listElement.Value)
	}
	if windowElement.Value.Dtype != sutils.SS_DT_FLOAT {
		return 0.0, fmt.Errorf("getListElementAsFloatFromWindow: Error: element in window does not have a numeric value, has value %v", windowElement.Value)
	}

	return windowElement.Value.CVal.(float64), nil
}

func getListElementFromWindow(listElement *list.Element) (sutils.CValueEnclosure, error) {
	if listElement == nil {
		return sutils.CValueEnclosure{}, fmt.Errorf("getListElementFromWindow: Error: listElement is nil")
	}

	windowElement, correctType := listElement.Value.(*structs.RunningStreamStatsWindowElement)
	if !correctType {
		return sutils.CValueEnclosure{}, fmt.Errorf("getListElementFromWindow: Error: element in the window is not a *RunningStreamStatsWindowElement, it's of type: %T", listElement.Value)
	}

	return windowElement.Value, nil
}

func manageMinWindow(window *utils.GobbableList, index int, newValue sutils.CValueEnclosure, timestamp uint64) error {
	for window.Len() > 0 {
		lastElementVal, err := getListElementFromWindow(window.Back())
		if err != nil {
			return fmt.Errorf("manageMinWindow: Error while getting value from last window element, err: %v", err)
		}
		if lastElementVal.Dtype != newValue.Dtype {
			return fmt.Errorf("manageMinWindow: Error while comparing values because of different types, lastElementVal: %v, newValue: %v", lastElementVal, newValue)
		}
		if lastElementVal.Dtype == sutils.SS_DT_FLOAT {
			if lastElementVal.CVal.(float64) >= newValue.CVal.(float64) {
				window.Remove(window.Back())
			} else {
				break
			}
		} else if lastElementVal.Dtype == sutils.SS_DT_STRING {
			if lastElementVal.CVal.(string) >= newValue.CVal.(string) {
				window.Remove(window.Back())
			} else {
				break
			}
		} else {
			return fmt.Errorf("manageMinWindow: lastElement is of type %v which is not supported", lastElementVal.Dtype)
		}
	}

	window.PushBack(&structs.RunningStreamStatsWindowElement{Index: index, Value: newValue, TimeInMilli: timestamp})
	return nil
}

func manageMaxWindow(window *utils.GobbableList, index int, newValue sutils.CValueEnclosure, timestamp uint64) error {
	for window.Len() > 0 {
		lastElementVal, err := getListElementFromWindow(window.Back())
		if err != nil {
			return fmt.Errorf("manageMaxWindow: Error while getting value from last window element, err: %v", err)
		}
		if lastElementVal.Dtype != newValue.Dtype {
			return fmt.Errorf("manageMaxWindow: Error while comparing values because of different types, lastElementVal: %v, newValue: %v", lastElementVal, newValue)
		}
		if lastElementVal.Dtype == sutils.SS_DT_FLOAT {
			if lastElementVal.CVal.(float64) <= newValue.CVal.(float64) {
				window.Remove(window.Back())
			} else {
				break
			}
		} else if lastElementVal.Dtype == sutils.SS_DT_STRING {
			if lastElementVal.CVal.(string) <= newValue.CVal.(string) {
				window.Remove(window.Back())
			} else {
				break
			}
		} else {
			return fmt.Errorf("manageMaxWindow: lastElement is of type %v which is not supported", lastElementVal.Dtype)
		}
	}

	window.PushBack(&structs.RunningStreamStatsWindowElement{Index: index, Value: newValue, TimeInMilli: timestamp})
	return nil
}

func getMinMaxElement(ssResult *structs.RunningStreamStatsResults) (sutils.CValueEnclosure, error) {

	// try to get a numeric element from the primary window if not present get string element from secondary window
	if ssResult.Window.Len() > 0 {
		return getListElementFromWindow(ssResult.Window.Front())
	} else if ssResult.SecondaryWindow.Len() > 0 {
		return getListElementFromWindow(ssResult.SecondaryWindow.Front())
	} else {
		return sutils.CValueEnclosure{}, nil
	}

}

func performMeasureFunc(currIndex int, ssResults *structs.RunningStreamStatsResults, measureAgg *structs.MeasureAggregator,
	colValue sutils.CValueEnclosure, timestamp uint64) (sutils.CValueEnclosure, error) {

	defaultResult, _, err := getResults(ssResults, measureAgg.MeasureFunc)
	if err != nil {
		return sutils.CValueEnclosure{}, fmt.Errorf("performMeasureFunc: Error while getting default results from the window, err: %v", err)
	}
	ssResults.NumProcessedRecords++

	err = validateCurrResultDType(measureAgg.MeasureFunc, ssResults.CurrResult)
	if err != nil {
		return sutils.CValueEnclosure{}, fmt.Errorf("performMeasureFunc: Error while validating currResult, err: %v", err)
	}

	switch measureAgg.MeasureFunc {
	case sutils.Count:
		ssResults.CurrResult.CVal = ssResults.CurrResult.CVal.(float64) + 1
		ssResults.Window.PushBack(&structs.RunningStreamStatsWindowElement{Index: currIndex, Value: colValue, TimeInMilli: timestamp})
	case sutils.Sum, sutils.Avg:
		if colValue.Dtype != sutils.SS_DT_FLOAT {
			return defaultResult, nil
		}
		ssResults.CurrResult.CVal = ssResults.CurrResult.CVal.(float64) + colValue.CVal.(float64)
		ssResults.Window.PushBack(&structs.RunningStreamStatsWindowElement{Index: currIndex, Value: colValue, TimeInMilli: timestamp})
	case sutils.Min:
		if colValue.Dtype != sutils.SS_DT_FLOAT && colValue.Dtype != sutils.SS_DT_STRING {
			return defaultResult, nil
		}
		window := ssResults.Window
		if colValue.Dtype == sutils.SS_DT_STRING {
			window = ssResults.SecondaryWindow
		}
		err := manageMinWindow(window, currIndex, colValue, timestamp)
		if err != nil {
			return sutils.CValueEnclosure{}, fmt.Errorf("performMeasureFunc: Error while managing min window, err: %v", err)
		}
		firstElement, err := getMinMaxElement(ssResults)
		if err != nil {
			return sutils.CValueEnclosure{}, fmt.Errorf("performMeasureFunc: Error while getting value from first window element, err: %v", err)
		}
		ssResults.CurrResult = firstElement
	case sutils.Max:
		if colValue.Dtype != sutils.SS_DT_FLOAT && colValue.Dtype != sutils.SS_DT_STRING {
			return defaultResult, nil
		}
		window := ssResults.Window
		if colValue.Dtype == sutils.SS_DT_STRING {
			window = ssResults.SecondaryWindow
		}
		err := manageMaxWindow(window, currIndex, colValue, timestamp)
		if err != nil {
			return sutils.CValueEnclosure{}, fmt.Errorf("performMeasureFunc: Error while managing min window, err: %v", err)
		}
		firstElement, err := getMinMaxElement(ssResults)
		if err != nil {
			return sutils.CValueEnclosure{}, fmt.Errorf("performMeasureFunc: Error while getting value from first window element, err: %v", err)
		}
		ssResults.CurrResult = firstElement
	case sutils.Range:
		if colValue.Dtype != sutils.SS_DT_FLOAT {
			return defaultResult, nil
		}
		err := manageMaxWindow(ssResults.Window, currIndex, colValue, timestamp)
		if err != nil {
			return sutils.CValueEnclosure{}, fmt.Errorf("performMeasureFunc: Error while managing max window, err: %v", err)
		}
		err = manageMinWindow(ssResults.SecondaryWindow, currIndex, colValue, timestamp)
		if err != nil {
			return sutils.CValueEnclosure{}, fmt.Errorf("performMeasureFunc: Error while managing min window, err: %v", err)
		}
		maxFloatVal, err := getListElementAsFloatFromWindow(ssResults.Window.Front())
		if err != nil {
			return sutils.CValueEnclosure{}, fmt.Errorf("performMeasureFunc: Error while getting float value from max window element, err: %v", err)
		}
		minFloatval, err := getListElementAsFloatFromWindow(ssResults.SecondaryWindow.Front())
		if err != nil {
			return sutils.CValueEnclosure{}, fmt.Errorf("performMeasureFunc: Error while getting float value from min window element, err: %v", err)
		}
		ssResults.CurrResult.CVal = maxFloatVal - minFloatval
	case sutils.Cardinality, sutils.Values:
		if ssResults.CardinalityMap == nil {
			ssResults.CardinalityMap = make(map[string]int, 0)
		}
		strValue := fmt.Sprintf("%v", colValue.CVal)

		ssResults.CardinalityMap[strconv.FormatInt(int64(len(ssResults.CardinalityMap)), 10)+": insert "+strValue] = 1
		// _, exist := ssResults.CardinalityMap[strValue]
		// if !exist {
		// 	ssResults.CardinalityMap[strValue] = 1
		// } else {
		// 	ssResults.CardinalityMap[strValue]++
		// }
		ssResults.CurrResult.CVal = float64(len(ssResults.CardinalityMap))
		cvalue := sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_STRING,
			CVal:  strValue,
		}

		ssResults.Window.PushBack(&structs.RunningStreamStatsWindowElement{Index: currIndex, Value: cvalue, TimeInMilli: timestamp})
	case sutils.Perc:
		if ssResults.PercTDigest == nil {
			ssResults.PercTDigest, err = utils.CreateNewTDigest()
			if err != nil {
				return sutils.CValueEnclosure{}, fmt.Errorf("performMeasureFunc: Error creating a new TDigest; measureAgg: %v, err: %v", measureAgg, err)
			}
		}
		err = ssResults.PercTDigest.InsertIntoTDigest(colValue.CVal.(float64))
		if err != nil {
			return sutils.CValueEnclosure{}, fmt.Errorf("performMeasureFunc: Error inserting val: %v into TDigest; err: %v", colValue.CVal.(float64), err)
		}
		// always between 0 and 100 (enforced by the peg parser)
		percentile := measureAgg.Param / 100
		ssResults.CurrResult.CVal = ssResults.PercTDigest.GetQuantile(percentile)
		ssResults.Window.PushBack(&structs.RunningStreamStatsWindowElement{Index: currIndex, Value: colValue, TimeInMilli: timestamp})
	default:
		return sutils.CValueEnclosure{}, fmt.Errorf("performMeasureFunc: Error measureAgg: %v not supported", measureAgg)
	}

	if measureAgg.MeasureFunc == sutils.Avg {
		return calculateAvg(ssResults, true), nil
	}
	if measureAgg.MeasureFunc == sutils.Values {
		return getValues(ssResults.CardinalityMap), nil
	}

	return ssResults.CurrResult, nil
}

func PerformWindowStreamStatsOnSingleFunc(currIndex int, ssOption *structs.StreamStatsOptions, ssResults *structs.RunningStreamStatsResults,
	windowSize int, measureAgg *structs.MeasureAggregator, colValue sutils.CValueEnclosure, timestamp uint64,
	timeSortAsc bool, include bool) (sutils.CValueEnclosure, bool, error) {
	var err error
	var result sutils.CValueEnclosure
	result = ssResults.CurrResult
	exist := ssResults.Window.Len() > 0
	if exist && measureAgg.MeasureFunc == sutils.Avg {
		result = calculateAvg(ssResults, true)
	}
	if !ssOption.Global {
		// when global is false use numProcessedRecords to determine the current index
		currIndex = int(ssResults.NumProcessedRecords)
	}

	if ssOption.TimeWindow != nil {
		err := cleanTimeWindow(timestamp, timeSortAsc, ssOption.TimeWindow, ssResults, measureAgg.MeasureFunc)
		if err != nil {
			return sutils.CValueEnclosure{}, false, fmt.Errorf("PerformWindowStreamStatsOnSingleFunc: Error while cleaning the time window, err: %v", err)
		}
	}

	// If current is false, compute result before adding the new element to the window
	if !ssOption.Current && windowSize != 0 {
		err = cleanWindow(currIndex-1, ssResults, windowSize, measureAgg.MeasureFunc)
		if err != nil {
			return sutils.CValueEnclosure{}, false, fmt.Errorf("PerformWindowStreamStatsOnSingleFunc: Error while cleaning the window, err: %v", err)
		}
		result, exist, err = getResults(ssResults, measureAgg.MeasureFunc)
		if err != nil {
			return sutils.CValueEnclosure{}, false, fmt.Errorf("PerformWindowStreamStatsOnSingleFunc: Error while getting results from the window, err: %v", err)
		}
	}

	if windowSize != 0 {
		err = cleanWindow(currIndex, ssResults, windowSize, measureAgg.MeasureFunc)
		if err != nil {
			return sutils.CValueEnclosure{}, false, fmt.Errorf("PerformWindowStreamStatsOnSingleFunc: Error while cleaning the window, err: %v", err)
		}
	}

	// Check if value should be included or not
	if !include {
		ssResults.NumProcessedRecords++
		if !ssOption.Current {
			return result, exist, nil
		}
		return getResults(ssResults, measureAgg.MeasureFunc)
	}

	// Add the new element to the window
	latestResult, err := performMeasureFunc(currIndex, ssResults, measureAgg, colValue, timestamp)
	if err != nil {
		return sutils.CValueEnclosure{}, false, fmt.Errorf("PerformWindowStreamStatsOnSingleFunc: Error while performing measure function %v, err: %v", measureAgg, err)
	}

	if !ssOption.Current {
		return result, exist, nil
	}

	return latestResult, true, nil
}

func CreateCValueFromColValue(colValue interface{}) sutils.CValueEnclosure {
	if colValue == nil {
		return sutils.CValueEnclosure{}
	}
	floatVal, err := dtypeutils.ConvertToFloat(colValue, 64)
	if err == nil {
		return sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_FLOAT,
			CVal:  floatVal,
		}
	}
	strVal := fmt.Sprintf("%v", colValue)
	return sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  strVal,
	}
}

func CreateCValueFromValueExpression(measureAgg *structs.MeasureAggregator, fieldToValue map[string]sutils.CValueEnclosure, colValue sutils.CValueEnclosure) (sutils.CValueEnclosure, bool) {
	if measureAgg.ValueColRequest == nil {
		return colValue, true
	}
	if measureAgg.ValueColRequest.BooleanExpr != nil {
		conditionPassed, err := measureAgg.ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
		if err != nil || !conditionPassed {
			return sutils.CValueEnclosure{}, false
		} else {
			return sutils.CValueEnclosure{
				Dtype: sutils.SS_DT_FLOAT,
				CVal:  1.0,
			}, true
		}
	}
	floatVal, strVal, isNumeric, err := GetFloatValueAfterEvaluation(measureAgg, fieldToValue)
	if err != nil {
		return sutils.CValueEnclosure{}, false
	}
	if isNumeric {
		return sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_FLOAT,
			CVal:  floatVal,
		}, true
	}
	return sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
		CVal:  strVal,
	}, true
}

func GetFloatValueAfterEvaluation(measureAgg *structs.MeasureAggregator, fieldToValue map[string]sutils.CValueEnclosure) (float64, string, bool, error) {
	valueStr, err := measureAgg.ValueColRequest.EvaluateToString(fieldToValue)
	if err != nil {
		return 0, "", false, fmt.Errorf("GetFloatValueAfterEvaluation: Error while evaluating eval function: %v", err)
	}
	floatVal, err := dtypeutils.ConvertToFloat(valueStr, 64)
	if err != nil {
		return 0, valueStr, false, nil
	}
	return floatVal, valueStr, true, nil
}

func evaluateResetCondition(boolExpr *structs.BoolExpr, record map[string]interface{}) (bool, error) {
	if boolExpr == nil {
		return false, nil
	}

	fieldsInExpr := boolExpr.GetFields()
	fieldToValue := make(map[string]sutils.CValueEnclosure, 0)
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
func getRecordFieldValues(fieldToValue map[string]sutils.CValueEnclosure, fieldsInExpr []string, record map[string]interface{}) error {
	for _, field := range fieldsInExpr {
		value, exists := record[field]
		if !exists {
			return fmt.Errorf("getRecordFieldValues: field %v does not exist in record", field)
		}

		dVal, err := sutils.CreateDtypeEnclosure(value, 0)
		if err != nil {
			log.Errorf("failed to create dtype enclosure for field %s, err=%v", field, err)
			dVal = &sutils.DtypeEnclosure{Dtype: sutils.SS_DT_STRING, StringVal: fmt.Sprintf("%v", value), StringValBytes: []byte(fmt.Sprintf("%v", value))}
			value = fmt.Sprintf("%v", value)
		}

		fieldToValue[field] = sutils.CValueEnclosure{Dtype: dVal.Dtype, CVal: value}
	}

	return nil
}

func (p *streamstatsProcessor) validateTimeWindow(iqr *iqr.IQR) ([]sutils.CValueEnclosure, error) {
	if p.options.TimeWindow == nil {
		return nil, nil
	}

	timeValues, err := iqr.ReadColumn("timestamp")
	if err != nil {
		return nil, fmt.Errorf("streamstats.validateTimeWindow: failed to read timestamp: %v", err)
	}

	// Validate timestamp order
	for i := 1; i < len(timeValues); i++ {
		curr := timeValues[i].CVal.(uint64)
		prev := timeValues[i-1].CVal.(uint64)

		if p.options.TimeSortAsc {
			if curr < prev {
				return nil, fmt.Errorf("streamstats.validateTimeWindow: records must be sorted by time in ascending order for time_window")
			}
		} else {
			if curr > prev {
				return nil, fmt.Errorf("streamstats.validateTimeWindow: records must be sorted by time in descending order for time_window")
			}
		}
	}

	return timeValues, nil
}
