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
	"errors"
	"fmt"
	"regexp"
	"sort"

	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func applyTimeRangeHistogram(nodeResult *structs.NodeResult, rangeHistogram *structs.TimeBucket, aggName string) {

	if nodeResult.Histogram == nil {
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

// Function to clean up results based on input query aggregations.
// This will make sure all buckets respect the minCount & is returned in a sorted order
func PostQueryBucketCleaning(nodeResult *structs.NodeResult, post *structs.QueryAggregators, recs map[string]map[string]interface{}) *structs.NodeResult {
	if post.TimeHistogram != nil {
		applyTimeRangeHistogram(nodeResult, post.TimeHistogram, post.TimeHistogram.AggName)
	}

	// For the query without groupby, skip the first aggregator without a rex block
	// For the query that has a groupby, groupby block's aggregation is in the post.Next. Therefore, we should start from the groupby's aggregation.
	if !(post.HasRexBlock()) {
		post = post.Next
	}

	for agg := post; agg != nil; agg = agg.Next {
		err := performAggOnResult(nodeResult, agg, recs)

		if err != nil {
			log.Errorf("PostQueryBucketCleaning: %v", err)
			nodeResult.ErrList = append(nodeResult.ErrList, err)
		}
	}

	return nodeResult
}

func performAggOnResult(nodeResult *structs.NodeResult, agg *structs.QueryAggregators, recs map[string]map[string]interface{}) error {
	switch agg.PipeCommandType {
	case structs.OutputTransformType:
		if agg.OutputTransforms == nil {
			return errors.New("performAggOnResult: expected non-nil OutputTransforms")
		}

		colReq := agg.OutputTransforms.OutputColumns
		if colReq != nil {
			err := performColumnsRequest(nodeResult, colReq)

			if err != nil {
				return fmt.Errorf("performAggOnResult: %v", err)
			}
		}

		if agg.OutputTransforms.LetColumns != nil {
			err := performLetColumnsRequest(nodeResult, agg, agg.OutputTransforms.LetColumns, recs)

			if err != nil {
				return fmt.Errorf("performAggOnResult: %v", err)
			}
		}

		if agg.OutputTransforms.FilterRows != nil {
			err := performFilterRows(nodeResult, agg.OutputTransforms.FilterRows)

			if err != nil {
				return fmt.Errorf("performAggOnResult: %v", err)
			}
		}
	default:
		return errors.New("performAggOnResult: multiple QueryAggregators is currently only supported for OutputTransformType")
	}

	return nil
}

func performColumnsRequest(nodeResult *structs.NodeResult, colReq *structs.ColumnsRequest) error {
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
		return errors.New("performColumnsRequest: processing ColumnsRequest.RenameColumns is not implemented")
	}
	if colReq.ExcludeColumns != nil {
		return errors.New("performColumnsRequest: processing ColumnsRequest.ExcludeColumns is not implemented")
	}
	if colReq.IncludeColumns != nil {
		return errors.New("performColumnsRequest: processing ColumnsRequest.IncludeColumns is not implemented")
	}
	if colReq.IncludeValues != nil {
		return errors.New("performColumnsRequest: processing ColumnsRequest.IncludeValues is not implemented")
	}
	if colReq.Logfmt {
		return errors.New("performColumnsRequest: processing ColumnsRequest for Logfmt is not implemented")
	}

	return nil
}

func performLetColumnsRequest(nodeResult *structs.NodeResult, aggs *structs.QueryAggregators, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}) error {

	if letColReq.NewColName == "" && letColReq.RexColRequest == nil {
		return errors.New("performLetColumnsRequest: expected non-empty NewColName")
	}

	// Exactly one of MultiColsRequest, SingleColRequest, ConcatColRequest, NumericColRequestNode should contain data.
	if letColReq.MultiColsRequest != nil {
		return errors.New("performLetColumnsRequest: processing LetColumnsRequest.MultiColsRequest is not implemented")
	} else if letColReq.SingleColRequest != nil {
		return errors.New("performLetColumnsRequest: processing LetColumnsRequest.SingleColRequest is not implemented")
	} else if letColReq.ValueColRequest != nil {
		if err := performValueColRequest(nodeResult, letColReq); err != nil {
			return fmt.Errorf("performLetColumnsRequest: %v", err)
		}
	} else if letColReq.RexColRequest != nil {
		if err := performRexColRequest(nodeResult, aggs, letColReq, recs); err != nil {
			return fmt.Errorf("performLetColumnsRequest: %v", err)
		}
	} else {
		return errors.New("performLetColumnsRequest: expected one of MultiColsRequest, SingleColRequest, ValueColRequest, RexColRequest to have a value")
	}

	return nil
}

func performRexColRequest(nodeResult *structs.NodeResult, aggs *structs.QueryAggregators, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}) error {

	//Without following group by
	if recs != nil {
		if err := performRexColRequestWithoutGroupby(nodeResult, letColReq, recs); err != nil {
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

func performRexColRequestWithoutGroupby(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}) error {

	rexExp, err := regexp.Compile(letColReq.RexColRequest.Pattern)
	if err != nil {
		return fmt.Errorf("performRexColRequestWithoutGroupby: There are some errors in the pattern: %v", err)
	}

	nodeResult.MeasureFunctions = letColReq.RexColRequest.RexColNames

	fieldName := letColReq.RexColRequest.FieldName
	for _, record := range recs {
		fieldValue := fmt.Sprintf("%v", record[fieldName])
		if len(fieldValue) == 0 {
			return fmt.Errorf("performRexColRequestWithoutGroupby: Field does not exist: %v", fieldName)
		}

		rexResultMap, err := structs.MatchAndExtractGroups(fieldValue, rexExp)
		if err != nil {
			return fmt.Errorf("performRexColRequestWithoutGroupby: %v", err)
		}

		for rexColName, Value := range rexResultMap {
			record[rexColName] = Value
		}
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

func performValueColRequest(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest) error {
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
			switch letColReq.ValueColRequest.ValueExprMode {
			case structs.VEMConditionExpr:
				err := getAggregationResultFieldValues(fieldToValue, fieldsInExpr, aggregationResult, rowIndex)
				if err != nil {
					return fmt.Errorf("performValueColRequestOnHistogram: %v", err)
				}
				// Evaluate the condition expression to a value.
				cellValueStr, err = letColReq.ValueColRequest.ConditionExpr.EvaluateCondition(fieldToValue)
				if err != nil {
					return fmt.Errorf("performValueColRequestOnHistogram: %v", err)
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
						cellValueStr = fmt.Sprintf("%v", cellValueFloat)
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
				if len(cellValueStr) > 0 {
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

func performFilterRows(nodeResult *structs.NodeResult, filterRows *structs.BoolExpr) error {
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
