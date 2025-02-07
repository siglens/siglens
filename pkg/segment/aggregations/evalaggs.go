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
	"fmt"
	"math"
	"sort"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
)

func PerformEvalAggForMinOrMax(measureAgg *structs.MeasureAggregator, currResultExists bool, currResult utils.CValueEnclosure, fieldToValue map[string]utils.CValueEnclosure, isMin bool) (utils.CValueEnclosure, error) {
	fields := measureAgg.ValueColRequest.GetFields()
	finalResult := utils.CValueEnclosure{}

	if len(fields) == 0 {
		floatValue, strValue, isNumeric, err := GetFloatValueAfterEvaluation(measureAgg, fieldToValue)
		if err != nil {
			return currResult, fmt.Errorf("PerformEvalAggForMinOrMax: Error while evaluating value col request to a numeric value, err: %v", err)
		}
		if isNumeric {
			finalResult.Dtype = utils.SS_DT_FLOAT
			finalResult.CVal = floatValue
		} else {
			finalResult.Dtype = utils.SS_DT_STRING
			finalResult.CVal = strValue
		}
	} else {
		if measureAgg.ValueColRequest.BooleanExpr != nil {
			boolResult, err := measureAgg.ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
			if err != nil {
				return currResult, fmt.Errorf("PerformEvalAggForMinOrMax: there are some errors in the eval function that is inside the %v function: %v", measureAgg.MeasureFunc, err)
			}
			if boolResult {
				finalResult.Dtype = utils.SS_DT_FLOAT
				finalResult.CVal = float64(1)
				return finalResult, nil
			} else {
				// return current result when no value needs to be updated
				return currResult, nil
			}
		} else {
			floatValue, strValue, isNumeric, err := GetFloatValueAfterEvaluation(measureAgg, fieldToValue)
			if err != nil {
				return currResult, fmt.Errorf("PerformEvalAggForMinOrMax: Error while evaluating value col request, err: %v", err)
			}

			if !currResultExists {
				if isNumeric {
					finalResult.Dtype = utils.SS_DT_FLOAT
					finalResult.CVal = floatValue
				} else {
					finalResult.Dtype = utils.SS_DT_STRING
					finalResult.CVal = strValue
				}
				return finalResult, nil
			}

			currType := currResult.Dtype
			if currType == utils.SS_DT_STRING {
				// if new value is numeric override the string result
				if isNumeric {
					finalResult.Dtype = utils.SS_DT_FLOAT
					finalResult.CVal = floatValue
				} else {
					strEncValue, isString := currResult.CVal.(string)
					if !isString {
						return currResult, fmt.Errorf("PerformEvalAggForMinOrMax: String type enclosure does not have a string value")
					}

					if (isMin && strValue < strEncValue) || (!isMin && strValue > strEncValue) {
						finalResult.Dtype = utils.SS_DT_STRING
						finalResult.CVal = strValue
					} else {
						// return current result when no value needs to be updated
						return currResult, nil
					}
				}
			} else if currType == utils.SS_DT_FLOAT {
				// only check if the current value is numeric
				if isNumeric {
					floatEncValue, isFloat := currResult.CVal.(float64)
					if !isFloat {
						return currResult, fmt.Errorf("PerformEvalAggForMinOrMax: Float type enclosure does not have a float value")
					}

					if (isMin && floatValue < floatEncValue) || (!isMin && floatValue > floatEncValue) {
						finalResult.Dtype = utils.SS_DT_FLOAT
						finalResult.CVal = floatValue
					} else {
						// return current result when no value needs to be updated
						return currResult, nil
					}
				} else {
					// string value cannot override numeric value for min max
					return currResult, nil
				}
			} else {
				return currResult, fmt.Errorf("PerformEvalAggForMinOrMax: Enclosure does not have a valid data type")
			}
		}
	}

	return finalResult, nil
}

func ComputeAggEvalForMinOrMax(measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, measureResults map[string]utils.CValueEnclosure, isMin bool) error {
	fields := measureAgg.ValueColRequest.GetFields()
	fieldToValue := make(map[string]utils.CValueEnclosure)
	var err error

	if len(fields) == 0 {
		enclosure, currResultExists := measureResults[measureAgg.String()]
		if !currResultExists {
			enclosure, err = PerformEvalAggForMinOrMax(measureAgg, currResultExists, enclosure, fieldToValue, isMin)
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForMinOrMax: Error while performing eval agg for min or max, err: %v", err)
			}
		}
		measureResults[measureAgg.String()] = enclosure
	} else {
		sst, ok := sstMap[fields[0]]
		if !ok {
			return fmt.Errorf("ComputeAggEvalForMinOrMax: sstMap did not have segstats for field %v, measureAgg: %v", fields[0], measureAgg.String())
		}

		length := len(sst.Records)
		for i := 0; i < length; i++ {
			enclosure, currResultExists := measureResults[measureAgg.String()]

			fieldToValue = make(map[string]utils.CValueEnclosure)
			err := PopulateFieldToValueFromSegStats(fields, measureAgg, sstMap, fieldToValue, i)
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForMinOrMax: Error while populating fieldToValue from sstMap, err: %v", err)
			}
			result, err := PerformEvalAggForMinOrMax(measureAgg, currResultExists, enclosure, fieldToValue, isMin)
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForMinOrMax: Error while performing eval agg for min/max, err: %v", err)
			}
			measureResults[measureAgg.String()] = result
		}
	}

	return nil
}

func UpdateRangeStat(floatValue float64, rangeStat *structs.RangeStat) {
	if floatValue < rangeStat.Min {
		rangeStat.Min = floatValue
	}
	if floatValue > rangeStat.Max {
		rangeStat.Max = floatValue
	}
}

func PerformEvalAggForRange(measureAgg *structs.MeasureAggregator, currResultExists bool, currRangeStat structs.RangeStat, fieldToValue map[string]utils.CValueEnclosure) (structs.RangeStat, error) {
	fields := measureAgg.ValueColRequest.GetFields()
	finalRangeStat := structs.RangeStat{
		Min: math.MaxFloat64,
		Max: -math.MaxFloat64,
	}
	if len(fields) == 0 {
		floatValue, _, isNumeric, err := GetFloatValueAfterEvaluation(measureAgg, fieldToValue)
		// We cannot compute if constant is not numeric
		if err != nil || !isNumeric {
			return currRangeStat, fmt.Errorf("PerformEvalAggForRange: Error while evaluating value col request to a numeric value, err: %v", err)
		}
		UpdateRangeStat(floatValue, &finalRangeStat)
	} else {
		if measureAgg.ValueColRequest.BooleanExpr != nil {
			boolResult, err := measureAgg.ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
			if err != nil {
				return currRangeStat, fmt.Errorf("PerformEvalAggForRange: there are some errors in the eval function that is inside the range function: %v", err)
			}
			if boolResult {
				finalRangeStat.Max = 1
				finalRangeStat.Min = 1
			}
		} else {
			floatValue, _, isNumeric, err := GetFloatValueAfterEvaluation(measureAgg, fieldToValue)
			if err != nil {
				return currRangeStat, fmt.Errorf("PerformEvalAggForRange: Error while evaluating value col request, err: %v", err)
			}
			// records that are not float will be ignored
			if isNumeric {
				UpdateRangeStat(floatValue, &finalRangeStat)
			}
		}
	}

	if !currResultExists {
		return finalRangeStat, nil
	}

	if finalRangeStat.Min > currRangeStat.Min {
		finalRangeStat.Min = currRangeStat.Min
	}
	if finalRangeStat.Max < currRangeStat.Max {
		finalRangeStat.Max = currRangeStat.Max
	}

	return finalRangeStat, nil
}

func ComputeAggEvalForRange(measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, measureResults map[string]utils.CValueEnclosure, runningEvalStats map[string]interface{}) error {
	fields := measureAgg.ValueColRequest.GetFields()
	fieldToValue := make(map[string]utils.CValueEnclosure)
	var err error
	rangeStat := structs.RangeStat{
		Min: math.MaxFloat64,
		Max: -math.MaxFloat64,
	}
	rangeStatVal, currResultExists := runningEvalStats[measureAgg.String()]
	if currResultExists {
		rangeStat.Min = rangeStatVal.(*structs.RangeStat).Min
		rangeStat.Max = rangeStatVal.(*structs.RangeStat).Max
	}

	if len(fields) == 0 {
		rangeStat, err = PerformEvalAggForRange(measureAgg, currResultExists, rangeStat, fieldToValue)
		if err != nil {
			return fmt.Errorf("ComputeAggEvalForRange: Error while performing eval agg for range, err: %v", err)
		}
	} else {
		sst, ok := sstMap[fields[0]]
		if !ok {
			return fmt.Errorf("ComputeAggEvalForRange: sstMap did not have segstats for field %v, measureAgg: %v", fields[0], measureAgg.String())
		}

		length := len(sst.Records)
		for i := 0; i < length; i++ {
			fieldToValue = make(map[string]utils.CValueEnclosure)
			err := PopulateFieldToValueFromSegStats(fields, measureAgg, sstMap, fieldToValue, i)
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForRange: Error while populating fieldToValue from sstMap, err: %v", err)
			}

			rangeStat, err = PerformEvalAggForRange(measureAgg, currResultExists, rangeStat, fieldToValue)
			currResultExists = true
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForRange: Error while performing eval agg for range, err: %v", err)
			}
		}
	}

	runningEvalStats[measureAgg.String()] = &rangeStat
	rangeVal := rangeStat.Max - rangeStat.Min

	enclosure := utils.CValueEnclosure{
		Dtype: utils.SS_DT_FLOAT,
		CVal:  rangeVal,
	}
	measureResults[measureAgg.String()] = enclosure

	return nil
}

func GetFloatValueAfterEvaluation(measureAgg *structs.MeasureAggregator, fieldToValue map[string]utils.CValueEnclosure) (float64, string, bool, error) {
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

func PopulateFieldToValueFromSegStats(fields []string, measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, fieldToValue map[string]utils.CValueEnclosure, i int) error {
	for _, field := range fields {
		sst, ok := sstMap[field]
		if !ok {
			return fmt.Errorf("PopulateFieldToValueFromSegStats: sstMap did not have segstats for field %v, measureAgg: %v", field, measureAgg.String())
		}

		if i >= len(sst.Records) {
			return fmt.Errorf("PopulateFieldToValueFromSegStats: Incorrect number of records in segstats for field: %v for measureAgg: %v", field, measureAgg.String())
		}
		fieldToValue[field] = *sst.Records[i]
	}

	return nil
}

func PerformEvalAggForSum(measureAgg *structs.MeasureAggregator, count uint64, currResultExists bool, currResult utils.CValueEnclosure, fieldToValue map[string]utils.CValueEnclosure) (utils.CValueEnclosure, error) {
	fields := measureAgg.ValueColRequest.GetFields()
	finalResult := utils.CValueEnclosure{
		Dtype: utils.SS_DT_FLOAT,
		CVal:  float64(0),
	}
	finalValue := float64(0)

	if len(fields) == 0 {
		floatValue, _, isNumeric, err := GetFloatValueAfterEvaluation(measureAgg, fieldToValue)
		// We cannot compute sum if constant is not numeric
		if err != nil || !isNumeric {
			return currResult, fmt.Errorf("PerformEvalAggForSum: Error while evaluating value col request to a numeric value, err: %v", err)
		}
		finalValue = floatValue * float64(count)
	} else {
		if measureAgg.ValueColRequest.BooleanExpr != nil {
			boolResult, err := measureAgg.ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
			if err != nil {
				return currResult, fmt.Errorf("PerformEvalAggForSum: there are some errors in the eval function that is inside the sum function: %v", err)
			}
			if boolResult {
				finalValue = float64(1)
			}
		} else {
			floatValue, _, isNumeric, err := GetFloatValueAfterEvaluation(measureAgg, fieldToValue)
			if err != nil {
				return currResult, fmt.Errorf("PerformEvalAggForSum: Error while evaluating value col request, err: %v", err)
			}
			// records that are not float will be ignored
			if isNumeric {
				finalValue = floatValue
			}
		}
	}

	if !currResultExists {
		finalResult.CVal = finalValue
		return finalResult, nil
	}

	currValue, isFloat := currResult.CVal.(float64)
	if !isFloat {
		return currResult, fmt.Errorf("PerformEvalAggForSum: Float type enclosure does not have a float value")
	}

	finalValue += currValue
	finalResult.CVal = finalValue

	return finalResult, nil
}

func ComputeAggEvalForSum(measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, measureResults map[string]utils.CValueEnclosure) error {
	fields := measureAgg.ValueColRequest.GetFields()
	fieldToValue := make(map[string]utils.CValueEnclosure)

	if len(fields) == 0 {
		countStat, exist := sstMap["*"]
		if !exist {
			return fmt.Errorf("ComputeAggEvalForSum: sstMap did not have count when constant was used for measureAgg: %v", measureAgg.String())
		}
		currResult, currResultExists := measureResults[measureAgg.String()]
		result, err := PerformEvalAggForSum(measureAgg, countStat.Count, currResultExists, currResult, fieldToValue)
		if err != nil {
			return fmt.Errorf("ComputeAggEvalForSum: Error while performing eval agg for sum, err: %v", err)
		}
		measureResults[measureAgg.String()] = result
	} else {
		sst, ok := sstMap[fields[0]]
		if !ok {
			return fmt.Errorf("ComputeAggEvalForSum: sstMap did not have segstats for field %v, measureAgg: %v", fields[0], measureAgg.String())
		}

		length := len(sst.Records)
		for i := 0; i < length; i++ {
			fieldToValue = make(map[string]utils.CValueEnclosure)
			err := PopulateFieldToValueFromSegStats(fields, measureAgg, sstMap, fieldToValue, i)
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForSum: Error while populating fieldToValue from sstMap, err: %v", err)
			}

			currResult, currResultExists := measureResults[measureAgg.String()]
			result, err := PerformEvalAggForSum(measureAgg, uint64(length), currResultExists, currResult, fieldToValue)
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForSum: Error while performing eval agg for sum, err: %v", err)
			}
			measureResults[measureAgg.String()] = result
		}
	}

	return nil
}

func PerformEvalAggForStdev(measureAgg *structs.MeasureAggregator, count uint64, currResultExists bool, currResult utils.CValueEnclosure, fieldToValue map[string]utils.CValueEnclosure) (utils.CValueEnclosure, error) {
	fields := measureAgg.ValueColRequest.GetFields()
	finalResult := utils.CValueEnclosure{
		Dtype: utils.SS_DT_FLOAT,
		CVal:  float64(0),
	}
	finalValue := float64(0)
	sumValue := float64(0)
	meanValue := float64(0)

	if len(fields) == 0 {
		floatValue, _, isNumeric, err := GetFloatValueAfterEvaluation(measureAgg, fieldToValue)
		if err != nil || !isNumeric {
			return currResult, fmt.Errorf("PerformEvalAggForStdev: Error while evaluating value col request to a numeric value, err: %v", err)
		}
		sumValue = floatValue * float64(count)
		meanValue = sumValue / float64(count)
		variance := 0.0
		for _, v := range fieldToValue {
			value, ok := v.CVal.(float64)
			if !ok {
				continue // Skip non-float64 values
			}
			variance += (value - meanValue) * (value - meanValue)
		}
		variance /= float64(count - 1)
		finalValue = math.Sqrt(variance)
	} else {
		if measureAgg.ValueColRequest.BooleanExpr != nil {
			boolResult, err := measureAgg.ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
			if err != nil {
				return currResult, fmt.Errorf("PerformEvalAggForStdev: there are some errors in the eval function that is inside the stdev function: %v", err)
			}
			if boolResult {
				finalValue = float64(1)
			}
		} else {
			floatValue, _, isNumeric, err := GetFloatValueAfterEvaluation(measureAgg, fieldToValue)
			if err != nil {
				return currResult, fmt.Errorf("PerformEvalAggForStdev: Error while evaluating value col request, err: %v", err)
			}
			if isNumeric {
				finalValue = floatValue
			}
		}
	}
	if !currResultExists {
		finalResult.CVal = finalValue
		return finalResult, nil
	}
	currValue, isFloat := currResult.CVal.(float64)
	if !isFloat {
		return currResult, fmt.Errorf("PerformEvalAggForStdev: Float type enclosure does not have a float value")
	}
	finalValue += currValue
	finalResult.CVal = finalValue
	return finalResult, nil
}

func PerformEvalAggForStdevp(measureAgg *structs.MeasureAggregator, count uint64, currResultExists bool, currResult utils.CValueEnclosure, fieldToValue map[string]utils.CValueEnclosure) (utils.CValueEnclosure, error) {
	fields := measureAgg.ValueColRequest.GetFields()
	finalResult := utils.CValueEnclosure{
		Dtype: utils.SS_DT_FLOAT,
		CVal:  float64(0),
	}
	finalValue := float64(0)
	sumValue := float64(0)
	meanValue := float64(0)

	if len(fields) == 0 {
		floatValue, _, isNumeric, err := GetFloatValueAfterEvaluation(measureAgg, fieldToValue)
		if err != nil || !isNumeric {
			return currResult, fmt.Errorf("PerformEvalAggForStdevp: Error while evaluating value col request to a numeric value, err: %v", err)
		}
		sumValue = floatValue * float64(count)
		meanValue = sumValue / float64(count)
		variance := 0.0
		for _, v := range fieldToValue {
			value, ok := v.CVal.(float64)
			if !ok {
				continue // Skip non-float64 values
			}
			variance += (value - meanValue) * (value - meanValue)
		}
		variance /= float64(count)
		finalValue = math.Sqrt(variance)
	} else {
		if measureAgg.ValueColRequest.BooleanExpr != nil {
			boolResult, err := measureAgg.ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
			if err != nil {
				return currResult, fmt.Errorf("PerformEvalAggForStdevp: there are some errors in the eval function that is inside the stdevp function: %v", err)
			}
			if boolResult {
				finalValue = float64(1)
			}
		} else {
			floatValue, _, isNumeric, err := GetFloatValueAfterEvaluation(measureAgg, fieldToValue)
			if err != nil {
				return currResult, fmt.Errorf("PerformEvalAggForStdevp: Error while evaluating value col request, err: %v", err)
			}
			if isNumeric {
				finalValue = floatValue
			}
		}
	}
	if !currResultExists {
		finalResult.CVal = finalValue
		return finalResult, nil
	}
	currValue, isFloat := currResult.CVal.(float64)
	if !isFloat {
		return currResult, fmt.Errorf("PerformEvalAggForStdevp: Float type enclosure does not have a float value")
	}
	finalValue += currValue
	finalResult.CVal = finalValue
	return finalResult, nil
}

func PerformEvalAggForCount(measureAgg *structs.MeasureAggregator, count uint64, currResultExists bool, currResult utils.CValueEnclosure, fieldToValue map[string]utils.CValueEnclosure) (utils.CValueEnclosure, error) {
	fields := measureAgg.ValueColRequest.GetFields()
	finalResult := utils.CValueEnclosure{
		Dtype: utils.SS_DT_FLOAT,
		CVal:  float64(0),
	}
	finalValue := float64(0)

	if len(fields) == 0 {
		finalValue = float64(count)
	} else {
		if measureAgg.ValueColRequest.BooleanExpr != nil {
			boolResult, err := measureAgg.ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
			if err != nil {
				return currResult, fmt.Errorf("PerformEvalAggForCount: there are some errors in the eval function that is inside the count function: %v", err)
			}
			if boolResult {
				finalValue++
			}
		} else {
			// Always count the record if eval function is not a boolean expression
			finalValue++
		}
	}

	if !currResultExists {
		finalResult.CVal = finalValue
		return finalResult, nil
	}

	currValue, isFloat := currResult.CVal.(float64)
	if !isFloat {
		return currResult, fmt.Errorf("PerformEvalAggForCount: Float type enclosure does not have a float value")
	}

	finalValue += currValue
	finalResult.CVal = finalValue

	return finalResult, nil
}

func ComputeAggEvalForCount(measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, measureResults map[string]utils.CValueEnclosure) error {
	fields := measureAgg.ValueColRequest.GetFields()

	if len(fields) == 0 {
		countStat, exist := sstMap["*"]
		if !exist {
			return fmt.Errorf("ComputeAggEvalForCount: sstMap did not have count when constant was used for measureAgg: %v", measureAgg.String())
		}
		currResult, currResultExists := measureResults[measureAgg.String()]
		result, err := PerformEvalAggForCount(measureAgg, countStat.Count, currResultExists, currResult, nil)
		if err != nil {
			return fmt.Errorf("ComputeAggEvalForCount: Error while performing eval agg for sum, err: %v", err)
		}
		measureResults[measureAgg.String()] = result
	} else {
		sst, ok := sstMap[fields[0]]
		if !ok {
			return fmt.Errorf("ComputeAggEvalForCount: sstMap did not have segstats for field %v, measureAgg: %v", fields[0], measureAgg.String())
		}
		length := len(sst.Records)
		for i := 0; i < length; i++ {
			fieldToValue := make(map[string]utils.CValueEnclosure)
			err := PopulateFieldToValueFromSegStats(fields, measureAgg, sstMap, fieldToValue, i)
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForCount: Error while populating fieldToValue from sstMap, err: %v", err)
			}

			currResult, currResultExists := measureResults[measureAgg.String()]
			result, err := PerformEvalAggForCount(measureAgg, uint64(length), currResultExists, currResult, fieldToValue)
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForSum: Error while performing eval agg for count, err: %v", err)
			}
			measureResults[measureAgg.String()] = result
		}
	}

	return nil
}

func PerformEvalAggForAvg(measureAgg *structs.MeasureAggregator, count uint64, currResultExists bool, currAvgStat structs.AvgStat, fieldToValue map[string]utils.CValueEnclosure) (structs.AvgStat, error) {
	fields := measureAgg.ValueColRequest.GetFields()
	finalAvgStat := structs.AvgStat{
		Sum:   float64(0),
		Count: int64(0),
	}

	if len(fields) == 0 {
		floatValue, _, isNumeric, err := GetFloatValueAfterEvaluation(measureAgg, fieldToValue)
		// We cannot compute avg if constant is not numeric
		if err != nil || !isNumeric {
			return currAvgStat, fmt.Errorf("PerformEvalAggForAvg: Error while evaluating value col request to a numeric value, err: %v", err)
		}
		finalAvgStat.Sum = floatValue * float64(count)
		finalAvgStat.Count = int64(count)
	} else {
		if measureAgg.ValueColRequest.BooleanExpr != nil {
			boolResult, err := measureAgg.ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
			if err != nil {
				return currAvgStat, fmt.Errorf("PerformEvalAggForAvg: there are some errors in the eval function that is inside the avg function: %v", err)
			}
			if boolResult {
				finalAvgStat.Sum++
				finalAvgStat.Count++
			}
		} else {
			floatValue, _, isNumeric, err := GetFloatValueAfterEvaluation(measureAgg, fieldToValue)
			if err != nil {
				return currAvgStat, fmt.Errorf("PerformEvalAggForAvg: Error while evaluating value col request, err: %v", err)
			}
			// records that are not float will be ignored
			if isNumeric {
				finalAvgStat.Sum += floatValue
				finalAvgStat.Count++
			}
		}
	}

	finalAvgStat.Sum += currAvgStat.Sum
	finalAvgStat.Count += currAvgStat.Count

	return finalAvgStat, nil
}

func ComputeAggEvalForAvg(measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, measureResults map[string]utils.CValueEnclosure, runningEvalStats map[string]interface{}) error {
	fields := measureAgg.ValueColRequest.GetFields()
	fieldToValue := make(map[string]utils.CValueEnclosure)
	avgStat := structs.AvgStat{}
	var err error
	avgStatVal, currResultExists := runningEvalStats[measureAgg.String()]
	if currResultExists {
		avgStat.Sum = avgStatVal.(*structs.AvgStat).Sum
		avgStat.Count = avgStatVal.(*structs.AvgStat).Count
	}

	if len(fields) == 0 {
		countStat, exist := sstMap["*"]
		if !exist {
			return fmt.Errorf("ComputeAggEvalForAvg: sstMap did not have count when constant was used for measureAgg: %v", measureAgg.String())
		}
		avgStat, err = PerformEvalAggForAvg(measureAgg, countStat.Count, currResultExists, avgStat, fieldToValue)
		if err != nil {
			return fmt.Errorf("ComputeAggEvalForAvg: Error while performing eval agg for sum, err: %v", err)
		}
	} else {
		sst, ok := sstMap[fields[0]]
		if !ok {
			return fmt.Errorf("ComputeAggEvalForAvg: sstMap did not have segstats for field %v, measureAgg: %v", fields[0], measureAgg.String())
		}

		length := len(sst.Records)
		for i := 0; i < length; i++ {
			fieldToValue = make(map[string]utils.CValueEnclosure)
			err := PopulateFieldToValueFromSegStats(fields, measureAgg, sstMap, fieldToValue, i)
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForAvg: Error while populating fieldToValue from sstMap, err: %v", err)
			}
			avgStat, err = PerformEvalAggForAvg(measureAgg, uint64(length), currResultExists, avgStat, fieldToValue)
			currResultExists = true
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForAvg: Error while performing eval agg for avg, err: %v", err)
			}
		}
	}

	runningEvalStats[measureAgg.String()] = &avgStat

	measureResults[measureAgg.String()] = utils.CValueEnclosure{
		Dtype: utils.SS_DT_FLOAT,
		CVal:  avgStat.Sum / float64(avgStat.Count),
	}

	return nil
}

// Always pass a non-nil strSet when using this function
func PerformAggEvalForCardinality(measureAgg *structs.MeasureAggregator, strSet map[string]struct{}, fieldToValue map[string]utils.CValueEnclosure) (float64, error) {
	fields := measureAgg.ValueColRequest.GetFields()

	if len(fields) == 0 {
		valueStr, err := measureAgg.ValueColRequest.EvaluateToString(fieldToValue)
		if err != nil {
			return 0.0, fmt.Errorf("PerformAggEvalForCardinality: Error while evaluating value col request function: %v", err)
		}
		strSet[valueStr] = struct{}{}
	} else {
		if measureAgg.ValueColRequest.BooleanExpr != nil {
			boolResult, err := measureAgg.ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
			if err != nil {
				return 0.0, fmt.Errorf("PerformAggEvalForCardinality: there are some errors in the eval function that is inside the values function: %v", err)
			}
			if boolResult {
				strSet["1"] = struct{}{}
			}
		} else {
			cellValueStr, err := measureAgg.ValueColRequest.EvaluateToString(fieldToValue)
			if err != nil {
				return 0.0, fmt.Errorf("PerformAggEvalForCardinality: Error while evaluating value col request, err: %v", err)
			}
			strSet[cellValueStr] = struct{}{}
		}
	}

	return float64(len(strSet)), nil
}

func PerformAggEvalForList(measureAgg *structs.MeasureAggregator, currentList []string, fieldToValue map[string]utils.CValueEnclosure) ([]string, error) {
	fields := measureAgg.ValueColRequest.GetFields()
	finalList := []string{}

	if len(fields) == 0 || measureAgg.ValueColRequest.BooleanExpr == nil {
		valueStr, err := measureAgg.ValueColRequest.EvaluateToString(fieldToValue)
		if err != nil {
			return []string{}, fmt.Errorf("PerformAggEvalForList: Error while evaluating value col request function: %v", err)
		}
		finalList = append(currentList, valueStr)
	} else {
		boolResult, err := measureAgg.ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
		if err != nil {
			return []string{}, fmt.Errorf("PerformAggEvalForList: there are some errors in the eval function that is inside the values function: %v", err)
		}
		if boolResult {
			finalList = append(currentList, "1")
		}
	}
	return finalList, nil
}

func ComputeAggEvalForCardinality(measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, measureResults map[string]utils.CValueEnclosure, runningEvalStats map[string]interface{}) error {
	fields := measureAgg.ValueColRequest.GetFields()
	result := 0.0
	var err error
	var strSet map[string]struct{}
	_, ok := runningEvalStats[measureAgg.String()]
	if !ok {
		strSet = make(map[string]struct{}, 0)
		runningEvalStats[measureAgg.String()] = strSet
	} else {
		strSet, ok = runningEvalStats[measureAgg.String()].(map[string]struct{})
		if !ok {
			return fmt.Errorf("ComputeAggEvalForCardinality: can not convert strSet for measureAgg: %v", measureAgg.String())
		}
	}

	if len(fields) == 0 {
		result, err = PerformAggEvalForCardinality(measureAgg, strSet, nil)
		if err != nil {
			return fmt.Errorf("ComputeAggEvalForCardinality: Error while performing eval agg for cardinality, err: %v", err)
		}
	} else {
		sst, ok := sstMap[fields[0]]
		if !ok {
			return fmt.Errorf("ComputeAggEvalForCardinality: sstMap did not have segstats for field %v, measureAgg: %v", fields[0], measureAgg.String())
		}

		length := len(sst.Records)
		for i := 0; i < length; i++ {
			fieldToValue := make(map[string]utils.CValueEnclosure)
			err := PopulateFieldToValueFromSegStats(fields, measureAgg, sstMap, fieldToValue, i)
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForCardinality: Error while populating fieldToValue from sstMap, err: %v", err)
			}

			result, err = PerformAggEvalForCardinality(measureAgg, strSet, fieldToValue)
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForCardinality: Error while performing eval agg for cardinality, err: %v", err)
			}
		}
	}

	measureResults[measureAgg.String()] = utils.CValueEnclosure{
		Dtype: utils.SS_DT_SIGNED_NUM,
		CVal:  int64(result),
	}

	return nil
}

func ComputeAggEvalForValues(measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, measureResults map[string]utils.CValueEnclosure, runningEvalStats map[string]interface{}) error {
	fields := measureAgg.ValueColRequest.GetFields()

	var valueSet map[string]struct{}
	_, ok := runningEvalStats[measureAgg.String()]
	if !ok {
		valueSet = make(map[string]struct{}, 0)
		runningEvalStats[measureAgg.String()] = valueSet
	} else {
		valueSet, ok = runningEvalStats[measureAgg.String()].(map[string]struct{})
		if !ok {
			return fmt.Errorf("ComputeAggEvalForValues: can not convert strSet for measureAgg: %v", measureAgg.String())
		}
	}

	if len(fields) == 0 {
		_, err := PerformAggEvalForCardinality(measureAgg, valueSet, nil)
		if err != nil {
			return fmt.Errorf("ComputeAggEvalForValues: Error while performing eval agg for values, err: %v", err)
		}
	} else {
		sst, ok := sstMap[fields[0]]
		if !ok {
			return fmt.Errorf("ComputeAggEvalForValues: sstMap did not have segstats for field %v, measureAgg: %v", fields[0], measureAgg.String())
		}

		length := len(sst.Records)
		for i := 0; i < length; i++ {
			fieldToValue := make(map[string]utils.CValueEnclosure)
			err := PopulateFieldToValueFromSegStats(fields, measureAgg, sstMap, fieldToValue, i)
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForValues: Error while populating fieldToValue from sstMap, err: %v", err)
			}

			_, err = PerformAggEvalForCardinality(measureAgg, valueSet, fieldToValue)
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForValues: Error while performing eval agg for values, err: %v", err)
			}
		}
	}

	uniqueStrings := make([]string, 0)
	for str := range valueSet {
		uniqueStrings = append(uniqueStrings, str)
	}
	sort.Strings(uniqueStrings)

	runningEvalStats[measureAgg.String()] = valueSet

	measureResults[measureAgg.String()] = utils.CValueEnclosure{
		Dtype: utils.SS_DT_STRING_SLICE,
		CVal:  uniqueStrings,
	}

	return nil
}

func ComputeAggEvalForList(measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, measureResults map[string]utils.CValueEnclosure, runningEvalStats map[string]interface{}) error {
	fields := measureAgg.ValueColRequest.GetFields()
	if len(fields) == 0 {
		// For list, if there are no fields, we will use the default timestamp field
		fields = []string{config.GetTimeStampKey()}
	}
	var finalList []string
	_, ok := runningEvalStats[measureAgg.String()]
	if !ok {
		finalList = make([]string, 0)
		runningEvalStats[measureAgg.String()] = finalList
	} else {
		finalList, ok = runningEvalStats[measureAgg.String()].([]string)
		if !ok {
			return fmt.Errorf("ComputeAggEvalForList: can not convert to list for measureAgg: %v", measureAgg.String())
		}
	}

	if len(fields) == 0 {
		fieldToValue := make(map[string]utils.CValueEnclosure)
		list, err := PerformAggEvalForList(measureAgg, finalList, fieldToValue)
		if err != nil {
			return fmt.Errorf("ComputeAggEvalForList: Error while performing eval agg for list, err: %v", err)
		}
		finalList = list
	} else {
		sst, ok := sstMap[fields[0]]
		if !ok {
			return fmt.Errorf("ComputeAggEvalForList: sstMap did not have segstats for field %v, measureAgg: %v", fields[0], measureAgg.String())
		}

		for i := range sst.Records {
			fieldToValue := make(map[string]utils.CValueEnclosure)
			err := PopulateFieldToValueFromSegStats(fields, measureAgg, sstMap, fieldToValue, i)
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForList: Error while populating fieldToValue from sstMap, err: %v", err)
			}

			list, err := PerformAggEvalForList(measureAgg, finalList, fieldToValue)
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForList: Error while performing eval agg for list, err: %v", err)
			}
			finalList = list
		}
	}

	// limit the list to MAX_SPL_LIST_SIZE
	if len(finalList) > utils.MAX_SPL_LIST_SIZE {
		finalList = finalList[:utils.MAX_SPL_LIST_SIZE]
	}
	measureResults[measureAgg.String()] = utils.CValueEnclosure{
		Dtype: utils.SS_DT_STRING_SLICE,
		CVal:  finalList,
	}
	runningEvalStats[measureAgg.String()] = finalList
	return nil
}

func AddMeasureAggInRunningStatsForCount(m *structs.MeasureAggregator, allConvertedMeasureOps *[]*structs.MeasureAggregator, allReverseIndex *[]int, colToIdx map[string][]int, idx int) (int, error) {

	fields := m.ValueColRequest.GetFields()
	if len(fields) == 0 {
		return idx, fmt.Errorf("AddMeasureAggInRunningStatsForCount: Incorrect number of fields for aggCol: %v", m.String())
	}

	// Use the index of agg to map to the corresponding index of the runningStats result, so that we can determine which index of the result set contains the result we need.
	*allReverseIndex = append(*allReverseIndex, idx)
	for _, field := range fields {
		if _, ok := colToIdx[field]; !ok {
			colToIdx[field] = make([]int, 0)
		}
		colToIdx[field] = append(colToIdx[field], idx)
		*allConvertedMeasureOps = append(*allConvertedMeasureOps, &structs.MeasureAggregator{
			MeasureCol:      field,
			MeasureFunc:     utils.Count,
			ValueColRequest: m.ValueColRequest,
			StrEnc:          m.StrEnc,
		})
		idx++
	}
	return idx, nil
}

func SetupMeasureAgg(measureAgg *structs.MeasureAggregator, allConvertedMeasureOps *[]*structs.MeasureAggregator, measureFunc utils.AggregateFunctions, allReverseIndex *[]int, colToIdx map[string][]int, idx int) (int, error) {
	fields := measureAgg.ValueColRequest.GetFields()
	if len(fields) == 0 {
		return 0, fmt.Errorf("SetupMeasureAgg: Zero fields of ValueColRequest for %v", measureFunc)
	}
	// Use the index of agg to map to the corresponding index of the runningStats result, so that we can determine which index of the result set contains the result we need.
	*allReverseIndex = append(*allReverseIndex, idx)
	for _, field := range fields {
		if _, ok := colToIdx[field]; !ok {
			colToIdx[field] = make([]int, 0)
		}
		colToIdx[field] = append(colToIdx[field], idx)
		*allConvertedMeasureOps = append(*allConvertedMeasureOps, &structs.MeasureAggregator{
			MeasureCol:      field,
			MeasureFunc:     measureFunc,
			ValueColRequest: measureAgg.ValueColRequest,
			StrEnc:          measureAgg.StrEnc,
		})
		idx++
	}
	return idx, nil
}

// Record the index of range() in runningStats; the index is idx
// To calculate the range(), we need both the min() and max(), which require two columns to store them
// Since it is the runningStats not the stats for results, we can use one extra col to store the min/max
// idx stores the result of min, and idx+1 stores the result of max.
func AddMeasureAggInRunningStatsForRange(m *structs.MeasureAggregator, allConvertedMeasureOps *[]*structs.MeasureAggregator, allReverseIndex *[]int, colToIdx map[string][]int, idx int) (int, error) {

	measureCol := m.MeasureCol
	if m.ValueColRequest != nil {
		fields := m.ValueColRequest.GetFields()
		if len(fields) != 1 {
			return idx, fmt.Errorf("AddMeasureAggInRunningStatsForRange: Incorrect number of fields for aggCol: %v", m.String())
		}
		measureCol = fields[0]
	}

	if _, ok := colToIdx[measureCol]; !ok {
		colToIdx[measureCol] = make([]int, 0)
	}
	*allReverseIndex = append(*allReverseIndex, idx)
	colToIdx[measureCol] = append(colToIdx[measureCol], idx)
	*allConvertedMeasureOps = append(*allConvertedMeasureOps, &structs.MeasureAggregator{
		MeasureCol:      measureCol,
		MeasureFunc:     utils.Min,
		ValueColRequest: m.ValueColRequest,
		StrEnc:          m.StrEnc,
	})
	idx++

	*allReverseIndex = append(*allReverseIndex, idx)
	colToIdx[measureCol] = append(colToIdx[measureCol], idx)
	*allConvertedMeasureOps = append(*allConvertedMeasureOps, &structs.MeasureAggregator{
		MeasureCol:      measureCol,
		MeasureFunc:     utils.Max,
		ValueColRequest: m.ValueColRequest,
		StrEnc:          m.StrEnc,
	})
	idx++

	return idx, nil
}

func AddMeasureAggInRunningStatsForValuesOrCardinality(m *structs.MeasureAggregator, allConvertedMeasureOps *[]*structs.MeasureAggregator, allReverseIndex *[]int, colToIdx map[string][]int, idx int) (int, error) {

	fields := m.ValueColRequest.GetFields()
	if len(fields) == 0 {
		return idx, fmt.Errorf("AddMeasureAggInRunningStatsForValuesOrCardinality: Incorrect number of fields for aggCol: %v", m.String())
	}

	// Use the index of agg to map to the corresponding index of the runningStats result, so that we can determine which index of the result set contains the result we need.
	*allReverseIndex = append(*allReverseIndex, idx)
	for _, field := range fields {
		if _, ok := colToIdx[field]; !ok {
			colToIdx[field] = make([]int, 0)
		}
		colToIdx[field] = append(colToIdx[field], idx)
		*allConvertedMeasureOps = append(*allConvertedMeasureOps, &structs.MeasureAggregator{
			MeasureCol:      field,
			MeasureFunc:     utils.Values,
			ValueColRequest: m.ValueColRequest,
			StrEnc:          m.StrEnc,
		})
		idx++
	}
	return idx, nil
}

// Determine if cols used by eval statements or not
func DetermineAggColUsage(measureAgg *structs.MeasureAggregator, aggCols map[string]bool, aggColUsage map[string]utils.AggColUsageMode, valuesUsage map[string]bool, listUsage map[string]bool) {
	if measureAgg.ValueColRequest != nil {
		fields := measureAgg.ValueColRequest.GetFields()
		for _, field := range fields {
			aggCols[field] = true
			colUsage, exists := aggColUsage[field]
			if exists {
				if colUsage == utils.NoEvalUsage {
					aggColUsage[field] = utils.BothUsage
				}
			} else {
				aggColUsage[field] = utils.WithEvalUsage
			}
		}
		if len(fields) == 0 && measureAgg.MeasureFunc == utils.List {
			// If there are no fields in the value col request, then it is a constant
			// But even if it is a constant, the evaluation should be done for every record
			// And the evaluated value should be added to the list.
			// So, we will use the timestamp Column as the default field.
			aggCols[config.GetTimeStampKey()] = true
			aggColUsage[config.GetTimeStampKey()] = utils.WithEvalUsage
		} else if len(aggColUsage) == 0 {
			defaultColName := "*"
			aggCols[defaultColName] = true
			aggColUsage[defaultColName] = utils.WithEvalUsage
		}
		measureAgg.MeasureCol = measureAgg.StrEnc
	} else {
		aggCols[measureAgg.MeasureCol] = true
		if measureAgg.MeasureFunc == utils.Values {
			valuesUsage[measureAgg.MeasureCol] = true
		} else if measureAgg.MeasureFunc == utils.List {
			listUsage[measureAgg.MeasureCol] = true
		}

		colUsage, exists := aggColUsage[measureAgg.MeasureCol]
		if exists {
			if colUsage == utils.WithEvalUsage {
				aggColUsage[measureAgg.MeasureCol] = utils.BothUsage
			}
		} else {
			aggColUsage[measureAgg.MeasureCol] = utils.NoEvalUsage
		}
	}
}
