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
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"

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

	for agg := post; agg != nil; agg = agg.Next {
		err := performAggOnResult(nodeResult, agg, recs, recordIndexInFinal, finalCols, numTotalSegments, finishesSegment)

		if len(nodeResult.TransactionEventRecords) > 0 {
			nodeResult.NextQueryAgg = agg
			return nodeResult
		} else if nodeResult.PerformAggsOnRecs && len(recs) > 0 {
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

func performAggOnResult(nodeResult *structs.NodeResult, agg *structs.QueryAggregators, recs map[string]map[string]interface{},
	recordIndexInFinal map[string]int, finalCols map[string]bool, numTotalSegments uint64, finishesSegment bool) error {
	switch agg.PipeCommandType {
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
			err := performFilterRows(nodeResult, agg.OutputTransforms.FilterRows)

			if err != nil {
				return fmt.Errorf("performAggOnResult: %v", err)
			}
		}

		if agg.OutputTransforms.MaxRows > 0 {
			err := performMaxRows(nodeResult, agg, agg.OutputTransforms.MaxRows, recs)

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

func performMaxRows(nodeResult *structs.NodeResult, aggs *structs.QueryAggregators, maxRows uint64, recs map[string]map[string]interface{}) error {

	if maxRows == 0 {
		return nil
	}

	if recs != nil {
		// If the number of records plus the already added Rows is less than the maxRows, we don't need to do anything.
		if (uint64(len(recs)) + aggs.OutputTransforms.RowsAdded) <= maxRows {
			aggs.OutputTransforms.RowsAdded += uint64(len(recs))
			return nil
		}

		// If the number of records is greater than the maxRows, we need to remove the extra records.
		for key := range recs {
			if aggs.OutputTransforms.RowsAdded >= maxRows {
				delete(recs, key)
				continue
			}
			aggs.OutputTransforms.RowsAdded++
		}

		return nil
	}

	// Follow group by
	if nodeResult.Histogram != nil {
		for _, aggResult := range nodeResult.Histogram {
			if (uint64(len(aggResult.Results)) + aggs.OutputTransforms.RowsAdded) <= maxRows {
				aggs.OutputTransforms.RowsAdded += uint64(len(aggResult.Results))
				continue
			}

			// If the number of records is greater than the maxRows, we need to remove the extra records.
			aggResult.Results = aggResult.Results[:maxRows-aggs.OutputTransforms.RowsAdded]
			aggs.OutputTransforms.RowsAdded = maxRows
			break
		}
		return nil
	}

	return nil
}

func performColumnsRequestWithoutGroupby(nodeResult *structs.NodeResult, colReq *structs.ColumnsRequest, recs map[string]map[string]interface{}, finalCols map[string]bool) error {
	if colReq.RenameAggregationColumns != nil {
		for oldCName, newCName := range colReq.RenameAggregationColumns {
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
		for _, matchingCol := range matchingCols {
			finalCols[matchingCol] = true
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
	} else {
		return errors.New("performLetColumnsRequest: expected one of MultiColsRequest, SingleColRequest, ValueColRequest, RexColRequest to have a value")
	}

	return nil
}

func performRenameColRequest(nodeResult *structs.NodeResult, aggs *structs.QueryAggregators, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}, finalCols map[string]bool) error {
	//Without following group by
	if recs != nil {
		if err := performRenameColRequestWithoutGroupby(nodeResult, letColReq, recs, finalCols); err != nil {
			return fmt.Errorf("performRenameColRequest: %v", err)
		}
		return nil
	}

	//Follow group by
	if err := performRenameColRequestOnHistogram(nodeResult, letColReq); err != nil {
		return fmt.Errorf("performRenameColRequest: %v", err)
	}
	if err := performRenameColRequestOnMeasureResults(nodeResult, letColReq); err != nil {
		return fmt.Errorf("performRenameColRequest: %v", err)
	}

	return nil
}

func performRenameColRequestWithoutGroupby(nodeResult *structs.NodeResult, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}, finalCols map[string]bool) error {

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

	letColReq.DedupColRequest.ProcessedSegmentsLock.Lock()
	defer letColReq.DedupColRequest.ProcessedSegmentsLock.Unlock()
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

	letColReq.SortColRequest.ProcessedSegmentsLock.Lock()
	defer letColReq.SortColRequest.ProcessedSegmentsLock.Unlock()
	if finishesSegment {
		letColReq.SortColRequest.NumProcessedSegments++
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

		resInOrder := make([]*structs.BucketResult, len(aggregationResult.Results))
		for index, key := range recKeys {
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

	resInOrder := make([]*structs.BucketHolder, len(nodeResult.MeasureResults))
	for index, key := range recKeys {
		resInOrder[index] = nodeResult.MeasureResults[key]
	}

	nodeResult.MeasureResults = resInOrder
	return nil
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

	countIsGroupByCol := utils.SliceContainsString(letColReq.StatisticColRequest.GetGroupByCols(), letColReq.StatisticColRequest.StatisticOptions.CountField)
	percentIsGroupByCol := utils.SliceContainsString(letColReq.StatisticColRequest.GetGroupByCols(), letColReq.StatisticColRequest.StatisticOptions.PercentField)

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
			countIsStatisticGroupByCol := utils.SliceContainsString(letColReq.StatisticColRequest.GetGroupByCols(), countName)
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
			resTotal += bucketHolder.MeasureVal[countName].(uint64)
		}
	}

	statisticGroupByCols := letColReq.StatisticColRequest.GetGroupByCols()
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
		countIsStatisticGroupByCol := utils.SliceContainsString(letColReq.StatisticColRequest.GetGroupByCols(), countName)
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
		finalCols[letColReq.NewColName] = true
	}

	return nil
}

func performValueColRequestOnRawRecord(letColReq *structs.LetColumnsRequest, fieldToValue map[string]segutils.CValueEnclosure) (interface{}, error) {
	if letColReq == nil || letColReq.ValueColRequest == nil {
		return nil, fmt.Errorf("invalid letColReq")
	}

	switch letColReq.ValueColRequest.ValueExprMode {
	case structs.VEMConditionExpr:
		value, err := letColReq.ValueColRequest.ConditionExpr.EvaluateCondition(fieldToValue)
		if err != nil {
			log.Errorf("failed to evaluate condition expr, err=%v", err)
			return nil, err
		}
		return value, nil
	case structs.VEMStringExpr:
		value, err := letColReq.ValueColRequest.EvaluateValueExprAsString(fieldToValue)
		if err != nil {
			log.Errorf("failed to evaluate string expr, err=%v", err)
			return nil, err
		}
		return value, nil
	case structs.VEMNumericExpr:
		value, err := letColReq.ValueColRequest.EvaluateToFloat(fieldToValue)
		if err != nil {
			log.Errorf("failed to evaluate numeric expr, err=%v", err)
			return nil, err
		}
		return value, nil
	case structs.VEMBooleanExpr:
		value, err := letColReq.ValueColRequest.EvaluateToString(fieldToValue)
		if err != nil {
			log.Errorf(" failed to evaluate boolean expr, err=%v", err)
			return nil, err
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unknown value expr mode %v", letColReq.ValueColRequest.ValueExprMode)
	}
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
			case structs.VEMBooleanExpr:
				cellValueStr, err = letColReq.ValueColRequest.EvaluateToString(fieldToValue)
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
			// This tells the PostBucketQueryCleaning function to return to the rrcreader.go to process the further segments.
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
		groupedRecord["duration"] = uint64(lastRecord["timestamp"].(uint64)) - currentState.Timestamp
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
