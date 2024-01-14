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
	"strconv"

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
func PostQueryBucketCleaning(nodeResult *structs.NodeResult, post *structs.QueryAggregators, recs map[string]map[string]interface{}, finalCols map[string]bool) *structs.NodeResult {
	if post.TimeHistogram != nil {
		applyTimeRangeHistogram(nodeResult, post.TimeHistogram, post.TimeHistogram.AggName)
	}

	// For the query without groupby, skip the first aggregator without a QueryAggergatorBlock
	// For the query that has a groupby, groupby block's aggregation is in the post.Next. Therefore, we should start from the groupby's aggregation.
	if !post.HasQueryAggergatorBlock() {
		post = post.Next
	}

	for agg := post; agg != nil; agg = agg.Next {
		err := performAggOnResult(nodeResult, agg, recs, finalCols)

		if err != nil {
			log.Errorf("PostQueryBucketCleaning: %v", err)
			nodeResult.ErrList = append(nodeResult.ErrList, err)
		}
	}

	return nodeResult
}

func performAggOnResult(nodeResult *structs.NodeResult, agg *structs.QueryAggregators, recs map[string]map[string]interface{}, finalCols map[string]bool) error {
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
			err := performLetColumnsRequest(nodeResult, agg, agg.OutputTransforms.LetColumns, recs, finalCols)

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

func performLetColumnsRequest(nodeResult *structs.NodeResult, aggs *structs.QueryAggregators, letColReq *structs.LetColumnsRequest, recs map[string]map[string]interface{}, finalCols map[string]bool) error {

	if letColReq.NewColName == "" && !aggs.HasQueryAggergatorBlock() && letColReq.StatisticColRequest == nil {
		return errors.New("performLetColumnsRequest: expected non-empty NewColName")
	}

	// Exactly one of MultiColsRequest, SingleColRequest, ValueColRequest, RexColRequest, RenameColRequest should contain data.
	if letColReq.MultiColsRequest != nil {
		return errors.New("performLetColumnsRequest: processing LetColumnsRequest.MultiColsRequest is not implemented")
	} else if letColReq.SingleColRequest != nil {
		return errors.New("performLetColumnsRequest: processing LetColumnsRequest.SingleColRequest is not implemented")
	} else if letColReq.ValueColRequest != nil {
		if err := performValueColRequest(nodeResult, letColReq); err != nil {
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

	countIsGroupByCol := utils.SliceContainsString(letColReq.StatisticColRequest.GetGroupByCols(), letColReq.StatisticColRequest.Options.CountField)
	percentIsGroupByCol := utils.SliceContainsString(letColReq.StatisticColRequest.GetGroupByCols(), letColReq.StatisticColRequest.Options.PercentField)

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

			if letColReq.StatisticColRequest.Options.ShowCount && !countIsGroupByCol {
				//Set Count to StatResult
				letColReq.StatisticColRequest.SetCountToStatRes(bucketResult.StatRes, bucketResult.ElemCount)
			}

			if letColReq.StatisticColRequest.Options.ShowPerc && !percentIsGroupByCol {
				//Set Percent to StatResult
				letColReq.StatisticColRequest.SetPercToStatRes(bucketResult.StatRes, bucketResult.ElemCount, resTotal)
			}
		}

		//If useother=true, a row representing all other values is added to the results.
		if letColReq.StatisticColRequest.Options.UseOther {
			statRes := make(map[string]segutils.CValueEnclosure)
			groupByKeys := aggregationResult.Results[0].GroupByKeys
			bucketKey := make([]string, len(groupByKeys))
			otherEnclosure := segutils.CValueEnclosure{
				Dtype: segutils.SS_DT_STRING,
				CVal:  letColReq.StatisticColRequest.Options.OtherStr,
			}
			for i := 0; i < len(groupByKeys); i++ {
				if groupByKeys[i] == letColReq.StatisticColRequest.Options.CountField || groupByKeys[i] == letColReq.StatisticColRequest.Options.PercentField {
					continue
				}
				bucketKey[i] = letColReq.StatisticColRequest.Options.OtherStr
			}

			for key := range aggregationResult.Results[0].StatRes {
				if key == letColReq.StatisticColRequest.Options.CountField || key == letColReq.StatisticColRequest.Options.PercentField {
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

			if letColReq.StatisticColRequest.Options.ShowCount && !countIsGroupByCol {
				letColReq.StatisticColRequest.SetCountToStatRes(statRes, otherCnt)
			}

			if letColReq.StatisticColRequest.Options.ShowPerc && !percentIsGroupByCol {
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
		if letColReq.StatisticColRequest.Options.ShowCount && letColReq.StatisticColRequest.Options.CountField == measureCol {
			// We'll write over this existing column.
			countIsGroupByCol = false
			countColIndex = i
		}

		if letColReq.StatisticColRequest.Options.ShowPerc && letColReq.StatisticColRequest.Options.PercentField == measureCol {
			// We'll write over this existing column.
			percentIsGroupByCol = false
			percentColIndex = i
		}
	}

	for i, groupByCol := range nodeResult.GroupByCols {
		if letColReq.StatisticColRequest.Options.ShowCount && letColReq.StatisticColRequest.Options.CountField == groupByCol {
			// We'll write over this existing column.
			countIsGroupByCol = true
			countColIndex = i
		}
		if letColReq.StatisticColRequest.Options.ShowPerc && letColReq.StatisticColRequest.Options.PercentField == groupByCol {
			// We'll write over this existing column.
			percentIsGroupByCol = true
			percentColIndex = i
		}
	}

	if letColReq.StatisticColRequest.Options.ShowCount && countColIndex == -1 {
		nodeResult.MeasureFunctions = append(nodeResult.MeasureFunctions, letColReq.StatisticColRequest.Options.CountField)
	}

	if letColReq.StatisticColRequest.Options.ShowPerc && percentColIndex == -1 {
		nodeResult.MeasureFunctions = append(nodeResult.MeasureFunctions, letColReq.StatisticColRequest.Options.PercentField)
	}

	countName := "count(*)"
	newCountName, exists := nodeResult.RenameColumns["count(*)"]
	if exists {
		countName = newCountName
	}

	resTotal := uint64(0)
	if letColReq.StatisticColRequest.Options.ShowPerc {
		for _, bucketHolder := range nodeResult.MeasureResults {
			resTotal += bucketHolder.MeasureVal[countName].(uint64)
		}
	}

	statisticGroupByCols := letColReq.StatisticColRequest.GetGroupByCols()
	// Compute the value for each row.
	for _, bucketHolder := range nodeResult.MeasureResults {

		countVal := bucketHolder.MeasureVal[countName]

		if letColReq.StatisticColRequest.Options.ShowCount {
			// Set the appropriate column to the computed value.
			if countIsGroupByCol {
				count, ok := countVal.(uint64)
				if !ok {
					return fmt.Errorf("performStatisticColRequestOnMeasureResults: Can not convert count to uint64")
				}
				bucketHolder.GroupByValues[countColIndex] = strconv.FormatUint(count, 10)
			} else {
				bucketHolder.MeasureVal[letColReq.StatisticColRequest.Options.CountField] = countVal
			}
		}

		//Delete count generated by the stats groupby block
		countIsStatisticGroupByCol := utils.SliceContainsString(letColReq.StatisticColRequest.GetGroupByCols(), countName)
		if !countIsStatisticGroupByCol {
			delete(bucketHolder.MeasureVal, countName)
		}

		if letColReq.StatisticColRequest.Options.ShowPerc {
			count, ok := countVal.(uint64)
			if !ok {
				return fmt.Errorf("performStatisticColRequestOnMeasureResults: Can not convert count to uint64")
			}
			percent := float64(count) / float64(resTotal) * 100
			if percentIsGroupByCol {
				bucketHolder.GroupByValues[percentColIndex] = fmt.Sprintf("%.6f", percent)
			} else {
				bucketHolder.MeasureVal[letColReq.StatisticColRequest.Options.PercentField] = fmt.Sprintf("%.6f", percent)
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
