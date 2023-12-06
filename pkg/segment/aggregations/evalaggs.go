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

package aggregations

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
)

func ComputeAggEvalForMinOrMax(measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, measureResults map[string]utils.CValueEnclosure, isMin bool) error {
	fields := measureAgg.ValueColRequest.GetFields()
	if len(fields) != 1 {
		return fmt.Errorf("ComputeAggEvalForMinOrMax: Incorrect number of fields for aggCol: %v", measureAgg.String())
	}

	sst, ok := sstMap[fields[0]]
	if !ok {
		return fmt.Errorf("ComputeAggEvalForMinOrMax: applyAggOpOnSegments sstMap was nil for aggCol %v", measureAgg.MeasureCol)
	}
	fieldToValue := make(map[string]utils.CValueEnclosure)

	edgeValue := math.SmallestNonzeroFloat64
	if isMin {
		edgeValue = math.MaxFloat64
	}

	for _, eVal := range sst.Records {
		fieldToValue[fields[0]] = *eVal
		boolResult, err := measureAgg.ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
		if err != nil {
			return fmt.Errorf("ComputeAggEvalForMinOrMax: there are some errors in the eval function that is inside the min/max function: %v", err)
		}

		if boolResult {
			eValFloat, err := eVal.GetFloatValue()
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForMinOrMax: can not get the float value: %v", err)
			}
			// Keep maximum and minimum values
			if (isMin && eValFloat < edgeValue) || (!isMin && eValFloat > edgeValue) {
				edgeValue = eValFloat
			}
		}
	}
	enclosure, exists := measureResults[measureAgg.String()]
	if !exists {

		cVal := math.SmallestNonzeroFloat64
		if isMin {
			cVal = math.MaxFloat64
		}

		enclosure = utils.CValueEnclosure{
			Dtype: utils.SS_DT_FLOAT,
			CVal:  cVal,
		}
		measureResults[measureAgg.String()] = enclosure
	}

	eValFloat, err := enclosure.GetFloatValue()
	if err != nil {
		return fmt.Errorf("ComputeAggEvalForMinOrMax: Attempted to perform aggregate min(), but the column %s is not a float value", fields[0])
	}

	if (isMin && eValFloat > edgeValue) || (!isMin && eValFloat < edgeValue) {
		enclosure.CVal = edgeValue
		measureResults[measureAgg.String()] = enclosure
	}
	return nil
}

func ComputeAggEvalForRange(measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, measureResults map[string]utils.CValueEnclosure, runningEvalStats map[string]interface{}) error {
	fields := measureAgg.ValueColRequest.GetFields()
	if len(fields) != 1 {
		return fmt.Errorf("ComputeAggEvalForRange: Incorrect number of fields for aggCol: %v", measureAgg.String())
	}

	sst, ok := sstMap[fields[0]]
	if !ok {
		return fmt.Errorf("ComputeAggEvalForRange: applyAggOpOnSegments sstMap was nil for aggCol %v", measureAgg.MeasureCol)
	}
	fieldToValue := make(map[string]utils.CValueEnclosure)

	maxVal := math.SmallestNonzeroFloat64
	minVal := math.MaxFloat64

	for _, eVal := range sst.Records {
		fieldToValue[fields[0]] = *eVal
		boolResult, err := measureAgg.ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
		if err != nil {
			return fmt.Errorf("ComputeAggEvalForRange: there are some errors in the eval function that is inside the range function: %v", err)
		}

		if boolResult {
			eValFloat, err := eVal.GetFloatValue()
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForRange: can not get the float value: %v", err)
			}
			// Keep maximum and minimum values
			if eValFloat < minVal {
				minVal = eValFloat
			}
			if eValFloat > maxVal {
				maxVal = eValFloat
			}
		}
	}

	rangeStat := &structs.RangeStat{}
	rangeStatVal, exists := runningEvalStats[measureAgg.String()]
	if !exists {
		rangeStat.Min = minVal
		rangeStat.Max = maxVal
	} else {
		rangeStat = rangeStatVal.(*structs.RangeStat)
		if rangeStat.Min > minVal {
			rangeStat.Min = minVal
		}
		if rangeStat.Max < maxVal {
			rangeStat.Max = maxVal
		}
	}
	runningEvalStats[measureAgg.String()] = rangeStat
	rangeVal := rangeStat.Max - rangeStat.Min

	enclosure, exists := measureResults[measureAgg.String()]
	if !exists {
		enclosure = utils.CValueEnclosure{
			Dtype: utils.SS_DT_FLOAT,
			CVal:  rangeVal,
		}
		measureResults[measureAgg.String()] = enclosure
	}

	eValFloat, err := enclosure.GetFloatValue()
	if err != nil {
		return fmt.Errorf("ComputeAggEvalForRange: Attempted to perform aggregate min(), but the column %s is not a float value", fields[0])
	}

	if eValFloat < rangeVal {
		enclosure.CVal = rangeVal
		measureResults[measureAgg.String()] = enclosure
	}
	return nil
}

func ComputeAggEvalForSum(measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, measureResults map[string]utils.CValueEnclosure) error {
	fields := measureAgg.ValueColRequest.GetFields()
	if len(fields) != 1 {
		return fmt.Errorf("ComputeAggEvalForSum: Incorrect number of fields for aggCol: %v", measureAgg.String())
	}

	sst, ok := sstMap[fields[0]]
	if !ok {
		return fmt.Errorf("ComputeAggEvalForSum: applyAggOpOnSegments sstMap was nil for aggCol %v", measureAgg.MeasureCol)
	}
	fieldToValue := make(map[string]utils.CValueEnclosure)

	sumVal := float64(0)

	for _, eVal := range sst.Records {
		fieldToValue[fields[0]] = *eVal
		boolResult, err := measureAgg.ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
		if err != nil {
			return fmt.Errorf("ComputeAggEvalForSum: there are some errors in the eval function that is inside the sum function: %v", err)
		}

		if boolResult {
			eValFloat, err := eVal.GetFloatValue()
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForSum: can not get the float value: %v", err)
			}
			sumVal += eValFloat
		}
	}
	enclosure, exists := measureResults[measureAgg.String()]
	if !exists {
		enclosure = utils.CValueEnclosure{
			Dtype: utils.SS_DT_FLOAT,
			CVal:  float64(0),
		}
		measureResults[measureAgg.String()] = enclosure
	}

	eValFloat, err := enclosure.GetFloatValue()
	if err != nil {
		return fmt.Errorf("ComputeAggEvalForSum: Attempted to perform aggregate min(), but the column %s is not a float value", fields[0])
	}

	enclosure.CVal = eValFloat + sumVal
	measureResults[measureAgg.String()] = enclosure

	return nil
}

func ComputeAggEvalForCount(measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, measureResults map[string]utils.CValueEnclosure) error {

	countVal := int64(0)
	fields := measureAgg.ValueColRequest.GetFields()
	if len(fields) == 0 {
		return fmt.Errorf("ComputeAggEvalForCount: Incorrect number of fields for aggCol: %v", measureAgg.String())
	}

	sst, ok := sstMap[fields[0]]
	if !ok {
		return fmt.Errorf("ComputeAggEvalForCount: applyAggOpOnSegments sstMap was nil for aggCol %v", measureAgg.MeasureCol)
	}

	length := len(sst.Records)
	for i := 0; i < length; i++ {
		fieldToValue := make(map[string]utils.CValueEnclosure)
		// Initialize fieldToValue
		for _, field := range fields {
			sst, ok := sstMap[field]
			if !ok {
				return fmt.Errorf("ComputeAggEvalForCount: applyAggOpOnSegments sstMap was nil for aggCol %v", measureAgg.MeasureCol)
			}

			if i >= len(sst.Records) {
				return fmt.Errorf("ComputeAggEvalForCount: Incorrect length of field: %v for aggCol: %v", field, measureAgg.String())
			}
			fieldToValue[field] = *sst.Records[i]
		}

		boolResult, err := measureAgg.ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
		if err != nil {
			return fmt.Errorf("ComputeAggEvalForCount: there are some errors in the eval function that is inside the count function: %v", err)
		}

		if boolResult {
			countVal++
		}
	}

	enclosure, exists := measureResults[measureAgg.String()]
	if !exists {
		enclosure = utils.CValueEnclosure{
			Dtype: utils.SS_DT_SIGNED_NUM,
			CVal:  int64(0),
		}
		measureResults[measureAgg.String()] = enclosure
	}

	eVal, err := enclosure.GetValue()
	if err != nil {
		return fmt.Errorf("ComputeAggEvalForCount: Attempted to perform aggregate min(), but the column %s is not a float value", fields[0])
	}

	enclosure.CVal = eVal.(int64) + countVal
	measureResults[measureAgg.String()] = enclosure

	return nil
}

func ComputeAggEvalForAvg(measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, measureResults map[string]utils.CValueEnclosure, runningEvalStats map[string]interface{}) error {
	fields := measureAgg.ValueColRequest.GetFields()
	if len(fields) != 1 {
		return fmt.Errorf("ComputeAggEvalForAvg: Incorrect number of fields for aggCol: %v", measureAgg.String())
	}

	sst, ok := sstMap[fields[0]]
	if !ok {
		return fmt.Errorf("ComputeAggEvalForAvg: applyAggOpOnSegments sstMap was nil for aggCol %v", measureAgg.MeasureCol)
	}
	fieldToValue := make(map[string]utils.CValueEnclosure)

	sumVal := float64(0)
	countVal := int64(0)
	for _, eVal := range sst.Records {
		fieldToValue[fields[0]] = *eVal
		boolResult, err := measureAgg.ValueColRequest.BooleanExpr.Evaluate(fieldToValue)
		if err != nil {
			return fmt.Errorf("ComputeAggEvalForAvg: there are some errors in the eval function that is inside the avg function: %v", err)
		}

		if boolResult {
			eValFloat, err := eVal.GetFloatValue()
			if err != nil {
				return fmt.Errorf("ComputeAggEvalForAvg: can not get the float value: %v", err)
			}
			sumVal += eValFloat
			countVal++
		}
	}

	avgStat := &structs.AvgStat{}
	avgStatVal, exists := runningEvalStats[measureAgg.String()]
	if !exists {
		avgStat.Sum = sumVal
		avgStat.Count = countVal
	} else {
		avgStat = avgStatVal.(*structs.AvgStat)
		avgStat.Sum += sumVal
		avgStat.Count += countVal
	}
	runningEvalStats[measureAgg.String()] = avgStat

	measureResults[measureAgg.String()] = utils.CValueEnclosure{
		Dtype: utils.SS_DT_FLOAT,
		CVal:  avgStat.Sum / float64(avgStat.Count),
	}

	return nil
}

func ComputeAggEvalForCardinality(measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, measureResults map[string]utils.CValueEnclosure, strSet map[string]struct{}) error {
	fields := measureAgg.ValueColRequest.GetFields()
	if len(fields) == 0 {
		return fmt.Errorf("ComputeAggEvalForCount: Incorrect number of fields for aggCol: %v", measureAgg.String())
	}

	sst, ok := sstMap[fields[0]]
	if !ok {
		return fmt.Errorf("ComputeAggEvalForCount: applyAggOpOnSegments sstMap was nil for aggCol %v", measureAgg.MeasureCol)
	}

	length := len(sst.Records)
	for i := 0; i < length; i++ {
		fieldToValue := make(map[string]utils.CValueEnclosure)
		// Initialize fieldToValue
		for _, field := range fields {
			sst, ok := sstMap[field]
			if !ok {
				return fmt.Errorf("ComputeAggEvalForCount: applyAggOpOnSegments sstMap was nil for aggCol %v", measureAgg.MeasureCol)
			}

			if i >= len(sst.Records) {
				return fmt.Errorf("ComputeAggEvalForCount: Incorrect length of field: %v for aggCol: %v", field, measureAgg.String())
			}
			fieldToValue[field] = *sst.Records[i]
		}

		cellValueStr, err := measureAgg.ValueColRequest.EvaluateToString(fieldToValue)
		if err != nil {
			return fmt.Errorf("ComputeAggEvalForCount: there are some errors in the eval function that is inside the cardinality function: %v", err)
		}

		strSet[cellValueStr] = struct{}{}
	}

	measureResults[measureAgg.String()] = utils.CValueEnclosure{
		Dtype: utils.SS_DT_SIGNED_NUM,
		CVal:  int64(len(strSet)),
	}

	return nil
}

func ComputeAggEvalForValues(measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, measureResults map[string]utils.CValueEnclosure, strSet map[string]struct{}) error {
	fields := measureAgg.ValueColRequest.GetFields()
	if len(fields) == 0 {
		return fmt.Errorf("ComputeAggEvalForValues: Incorrect number of fields for aggCol: %v", measureAgg.String())
	}

	sst, ok := sstMap[fields[0]]
	if !ok {
		return fmt.Errorf("ComputeAggEvalForValues: applyAggOpOnSegments sstMap was nil for aggCol %v", measureAgg.MeasureCol)
	}

	length := len(sst.Records)
	for i := 0; i < length; i++ {
		fieldToValue := make(map[string]utils.CValueEnclosure)
		// Initialize fieldToValue
		for _, field := range fields {
			sst, ok := sstMap[field]
			if !ok {
				return fmt.Errorf("ComputeAggEvalForValues: applyAggOpOnSegments sstMap was nil for aggCol %v", measureAgg.MeasureCol)
			}

			if i >= len(sst.Records) {
				return fmt.Errorf("ComputeAggEvalForValues: Incorrect length of field: %v for aggCol: %v", field, measureAgg.String())
			}
			fieldToValue[field] = *sst.Records[i]
		}

		cellValueStr, err := measureAgg.ValueColRequest.EvaluateToString(fieldToValue)
		if err != nil {
			return fmt.Errorf("ComputeAggEvalForValues: there are some errors in the eval function that is inside the values function: %v", err)
		}

		strSet[cellValueStr] = struct{}{}
	}

	uniqueStrings := make([]string, 0)
	for str := range strSet {
		uniqueStrings = append(uniqueStrings, str)
	}
	sort.Strings(uniqueStrings)

	strVal := strings.Join(uniqueStrings, "&nbsp")
	measureResults[measureAgg.String()] = utils.CValueEnclosure{
		Dtype: utils.SS_DT_STRING,
		CVal:  strVal,
	}

	return nil
}
