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

	edgeValue := -1.7976931348623157e+308
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

		cVal := -1.7976931348623157e+308
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

	maxVal := -1.7976931348623157e+308
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

func ComputeAggEvalForCardinality(measureAgg *structs.MeasureAggregator, sstMap map[string]*structs.SegStats, measureResults map[string]utils.CValueEnclosure, runningEvalStats map[string]interface{}) error {
	fields := measureAgg.ValueColRequest.GetFields()
	if len(fields) == 0 {
		return fmt.Errorf("ComputeAggEvalForCount: Incorrect number of fields for aggCol: %v", measureAgg.String())
	}

	sst, ok := sstMap[fields[0]]
	if !ok {
		return fmt.Errorf("ComputeAggEvalForCount: applyAggOpOnSegments sstMap was nil for aggCol %v", measureAgg.MeasureCol)
	}

	strSet := make(map[string]struct{}, 0)
	valuesStrSetVal, exists := runningEvalStats[measureAgg.String()]
	if !exists {
		runningEvalStats[measureAgg.String()] = make(map[string]struct{}, 0)
	} else {
		strSet, ok = valuesStrSetVal.(map[string]struct{})
		if !ok {
			return fmt.Errorf("ComputeAggEvalForCardinality: can not convert strSet for aggCol: %v", measureAgg.String())
		}
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

func AddMeasureAggInRunningStatsForAvg(m *structs.MeasureAggregator, allConvertedMeasureOps *[]*structs.MeasureAggregator, allReverseIndex *[]int, colToIdx map[string][]int, idx int) (int, error) {

	fields := m.ValueColRequest.GetFields()
	if len(fields) != 1 {
		return idx, fmt.Errorf("AddMeasureAggInRunningStatsForAvg: Incorrect number of fields for aggCol: %v", m.String())
	}
	field := fields[0]

	if _, ok := colToIdx[field]; !ok {
		colToIdx[field] = make([]int, 0)
	}

	// We need to use sum() and count() to calculate the avg()
	*allReverseIndex = append(*allReverseIndex, idx)
	colToIdx[field] = append(colToIdx[field], idx)
	*allConvertedMeasureOps = append(*allConvertedMeasureOps, &structs.MeasureAggregator{
		MeasureCol:      field,
		MeasureFunc:     utils.Sum,
		ValueColRequest: m.ValueColRequest,
		StrEnc:          m.StrEnc,
	})
	idx++

	*allReverseIndex = append(*allReverseIndex, idx)
	colToIdx[field] = append(colToIdx[field], idx)
	*allConvertedMeasureOps = append(*allConvertedMeasureOps, &structs.MeasureAggregator{
		MeasureCol:      field,
		MeasureFunc:     utils.Count,
		ValueColRequest: m.ValueColRequest,
		StrEnc:          m.StrEnc,
	})
	idx++
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
func DetermineAggColUsage(measureAgg *structs.MeasureAggregator, aggCols map[string]bool, aggColUsage map[string]utils.AggColUsageMode, valuesUsage map[string]bool) {
	if measureAgg.ValueColRequest != nil {
		for _, field := range measureAgg.ValueColRequest.GetFields() {
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
		measureAgg.MeasureCol = measureAgg.StrEnc
	} else {
		aggCols[measureAgg.MeasureCol] = true
		if measureAgg.MeasureFunc == utils.Values {
			valuesUsage[measureAgg.MeasureCol] = true
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
