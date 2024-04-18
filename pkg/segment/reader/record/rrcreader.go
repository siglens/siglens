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

package record

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/siglens/siglens/pkg/config"
	agg "github.com/siglens/siglens/pkg/segment/aggregations"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/search"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

var (
	nodeResMap = make(map[uint64]*structs.NodeResult)
	mapMutex   sync.Mutex
)

func GetOrCreateNodeRes(qid uint64) *structs.NodeResult {
	mapMutex.Lock()
	defer mapMutex.Unlock()

	// Check if the nodeRes instance exists for the given qid
	if nr, exists := nodeResMap[qid]; exists {
		return nr
	}

	// If not exists, create a new instance and add it to the map
	nr := &structs.NodeResult{}
	nodeResMap[qid] = nr

	return nr
}

func buildSegMap(allrrc []*utils.RecordResultContainer, segEncToKey map[uint16]string) (map[string]*utils.BlkRecIdxContainer, map[string]int) {
	segmap := make(map[string]*utils.BlkRecIdxContainer)
	recordIndexInFinal := make(map[string]int)

	for idx, rrc := range allrrc {
		if rrc.SegKeyInfo.IsRemote {
			log.Debugf("buildSegMap: skipping remote segment:%v", rrc.SegKeyInfo.RecordId)
			continue
		}
		segkey, ok := segEncToKey[rrc.SegKeyInfo.SegKeyEnc]
		if !ok {
			log.Errorf("buildSegMap: could not find segenc:%v in map", rrc.SegKeyInfo.SegKeyEnc)
			continue
		}
		blkIdxsCtr, ok := segmap[segkey]
		if !ok {
			innermap := make(map[uint16]map[uint16]uint64)
			blkIdxsCtr = &utils.BlkRecIdxContainer{BlkRecIndexes: innermap, VirtualTableName: rrc.VirtualTableName}
			segmap[segkey] = blkIdxsCtr
		}
		_, ok = blkIdxsCtr.BlkRecIndexes[rrc.BlockNum]
		if !ok {
			blkIdxsCtr.BlkRecIndexes[rrc.BlockNum] = make(map[uint16]uint64)
		}
		blkIdxsCtr.BlkRecIndexes[rrc.BlockNum][rrc.RecordNum] = rrc.TimeStamp

		recordIndent := fmt.Sprintf("%s_%d_%d", segkey, rrc.BlockNum, rrc.RecordNum)
		recordIndexInFinal[recordIndent] = idx
	}

	return segmap, recordIndexInFinal
}

func prepareOutputTransforms(aggs *structs.QueryAggregators) (map[string]int, map[string]string, bool, bool, []string, map[string]string) {
	rawIncludeValuesIndicies := make(map[string]int)
	valuesToLabels := make(map[string]string)
	logfmtRequest := false
	tableColumnsExist := false
	if aggs != nil && aggs.OutputTransforms != nil && aggs.OutputTransforms.OutputColumns != nil {
		logfmtRequest = aggs.OutputTransforms.OutputColumns.Logfmt
		tableColumnsExist = true
		for _, rawIncludeValue := range aggs.OutputTransforms.OutputColumns.IncludeValues {
			if !logfmtRequest {
				rawIncludeValuesIndicies[rawIncludeValue.ColName] = rawIncludeValue.Index
			}
			valuesToLabels[rawIncludeValue.ColName] = rawIncludeValue.Label
		}
	}
	var hardcodedArray = []string{}
	var renameHardcodedColumns = make(map[string]string)
	if aggs != nil && aggs.OutputTransforms != nil && aggs.OutputTransforms.HarcodedCol != nil {
		hardcodedArray = append(hardcodedArray, aggs.OutputTransforms.HarcodedCol...)

		for key, value := range aggs.OutputTransforms.RenameHardcodedColumns {

			renameHardcodedColumns[value] = key
		}

	}

	return rawIncludeValuesIndicies, valuesToLabels, logfmtRequest, tableColumnsExist, hardcodedArray, renameHardcodedColumns
}

func applyHardcodedColumns(hardcodedArray []string, renameHardcodedColumns map[string]string, allRecords []map[string]interface{}, finalCols map[string]bool) ([]map[string]interface{}, map[string]bool) {
	if len(hardcodedArray) > 0 {
		for key := range renameHardcodedColumns {
			finalCols[key] = true
		}
		record := make(map[string]interface{})
		for key, val := range renameHardcodedColumns {
			record[key] = val

		}
		allRecords[0] = record
		allRecords = allRecords[:1]
	}

	return allRecords, finalCols
}

func finalizeRecords(allRecords []map[string]interface{}, finalCols map[string]bool, numProcessedRecords int, recsAggRecords []map[string]interface{}, transactionArgsExist bool) ([]map[string]interface{}, []string) {
	colsSlice := make([]string, len(finalCols))
	idx := 0
	for colName := range finalCols {
		colsSlice[idx] = colName
		idx++
	}

	// Some commands (like dedup) can remove records from the final result, so
	// remove the blank records from allRecords to get finalRecords.
	var finalRecords []map[string]interface{}
	if transactionArgsExist {
		finalRecords = recsAggRecords
	} else if numProcessedRecords == len(allRecords) {
		finalRecords = allRecords
	} else {
		finalRecords = make([]map[string]interface{}, numProcessedRecords)
		idx = 0
		for _, record := range allRecords {
			if idx >= numProcessedRecords {
				break
			}

			if record != nil {
				finalRecords[idx] = record
				idx++
			}
		}
	}

	sort.Strings(colsSlice)

	return finalRecords, colsSlice
}

// Gets all raw json records from RRCs. If esResponse is false, _id and _type will not be added to any record
func GetJsonFromAllRrc(allrrc []*utils.RecordResultContainer, esResponse bool, qid uint64,
	segEncToKey map[uint16]string, aggs *structs.QueryAggregators) ([]map[string]interface{}, []string, error) {

	sTime := time.Now()
	nodeRes := GetOrCreateNodeRes(qid)
	segmap, recordIndexInFinal := buildSegMap(allrrc, segEncToKey)
	rawIncludeValuesIndicies, valuesToLabels, logfmtRequest, tableColumnsExist, hardcodedArray, renameHardcodedColumns := prepareOutputTransforms(aggs)

	allRecords := make([]map[string]interface{}, len(allrrc))
	finalCols := make(map[string]bool)
	numProcessedRecords := 0

	var resultRecMap map[string]bool

	hasQueryAggergatorBlock := aggs.HasQueryAggergatorBlockInChain()
	transactionArgsExist := aggs.HasTransactionArgumentsInChain()
	recsAggRecords := make([]map[string]interface{}, 0)
	var numTotalSegments uint64

	processSingleSegment := func(currSeg string, virtualTableName string, blkRecIndexes map[uint16]map[uint16]uint64, isLastBlk bool) {
		recs, cols, err := GetRecordsFromSegment(currSeg, virtualTableName, blkRecIndexes,
			config.GetTimeStampKey(), esResponse, qid, aggs)
		if err != nil {
			log.Errorf("GetJsonFromAllRrc: failed to read recs from segfile=%v, err=%v", currSeg, err)
			return
		}
		for cName := range cols {
			finalCols[cName] = true
		}

		for key := range renameHardcodedColumns {
			finalCols[key] = true
		}

		if hasQueryAggergatorBlock || transactionArgsExist {

			numTotalSegments, err = query.GetTotalSegmentsToSearch(qid)
			if err != nil {
				// For synchronous queries, the query is deleted by this
				// point, but segmap has all the segments that the query
				// searched.
				// For async queries, the segmap has just one segment
				// because we process them as the search completes, but the
				// query isn't deleted until all segments get processed, so
				// we shouldn't get to this block for async queries.
				numTotalSegments = uint64(len(segmap))
			}

			/**
			* Overview of Aggregation Processing:
			* 1. Initiate the process by executing PostQueryBucketCleaning to prepare records for aggregation.
			* 2. Evaluate the PerformAggsOnRecs flag post-cleanup:
			*    - True: Indicates not all aggregations were processed. In this case:
			*       a. Perform aggregations on records using performAggsOnRecs. This function requires all the segments to be processed before proceeding to the next step.
			*       b. Evaluate the CheckNextAgg flag from the result:
			*          i. If true, reset PerformAggsOnRecs to false, update aggs with NextQueryAgg, and loop for additional cleaning.
			*          ii. If false or if resultRecMap is empty, it implies additional segments may require processing; exit the loop for further segment evaluation.
			*    - False: All aggregations for the current segment have been processed; exit the loop to either process the next segment or return the final results.
			* 3. The loop facilitates sequential data processing, ensuring each or all the segments are thoroughly processed before proceeding to the next,
			*    adapting dynamically based on the flags set by the PostQueryBucketCleaning and PerformAggsOnRecs functions.
			 */
			for {
				finishesSegment := isLastBlk
				agg.PostQueryBucketCleaning(nodeRes, aggs, recs, recordIndexInFinal, finalCols, numTotalSegments, finishesSegment)

				// If TransactionEventRecords exist, process them first. This implies there might be segments left for TransactionEvent processing.
				if len(nodeRes.TransactionEventRecords) > 0 {

					_, exists := nodeRes.TransactionEventRecords["CHECK_NEXT_AGG"]

					if exists {
						// Reset the TransactionEventRecords and update aggs with NextQueryAgg to loop for next Aggs processing.
						delete(nodeRes.TransactionEventRecords, "CHECK_NEXT_AGG")
						aggs = &structs.QueryAggregators{Next: nodeRes.NextQueryAgg.Next}
					} else {
						break // Break out of the loop to process next segment.
					}
				} else if nodeRes.PerformAggsOnRecs {
					resultRecMap = search.PerformAggsOnRecs(nodeRes, aggs, recs, finalCols, numTotalSegments, finishesSegment, qid)
					// By default reset PerformAggsOnRecs flag, otherwise the execution will immediately return here from PostQueryBucketCleaning;
					// Without performing the aggs from the start for the next segment or next bulk.
					nodeRes.PerformAggsOnRecs = false
					if len(resultRecMap) > 0 {
						boolVal, exists := resultRecMap["CHECK_NEXT_AGG"]
						if exists && boolVal {
							// Update aggs with NextQueryAgg to loop for additional cleaning.
							aggs = nodeRes.NextQueryAgg
						} else {
							break
						}
					} else {
						// Not checking or processing Next Agg. This implies that there might be more segments to process.
						// Break out of the loop and continue processing the next segment.
						break
					}
				} else {
					// No need to perform aggs on recs. All the Aggs are Processed.
					break
				}
			}
			// For other cmds, if we cannot map recInden to an index, we simply append the record to allRecords
			// However, for the sort cmd, we should assign the length of the result set to be the same as recordIndexInFinal
			// This way, when mapping the results to allRecords, we can preserve the order of the results rather than just appending them to the end of allRecords
			if len(recordIndexInFinal) > len(allRecords) {
				allRecords = make([]map[string]interface{}, len(recordIndexInFinal))
			}
		}

		numProcessedRecords += len(recs)
		limit := aggs.GetSortLimit()
		if uint64(numProcessedRecords) > limit {
			numProcessedRecords = int(limit)
		}

		for recInden, record := range recs {
			for key, val := range renameHardcodedColumns {
				record[key] = val
			}

			unknownIndex := false
			idx, ok := recordIndexInFinal[recInden]
			if !ok {
				// For async queries where we need all records before we
				// can return any (like dedup with a sortby), once we can
				// get to this block because processing the dedup may
				// return some records from previous segments and since
				// it's an async query we're running this function with
				// len(segmap)=1 because we try to process the data as the
				// searched complete.
				log.Infof("qid=%d, GetJsonFromAllRrc: Did not find index for record indentifier %s.", qid, recInden)
				unknownIndex = true
			}
			if logfmtRequest {
				record = addKeyValuePairs(record)
			}
			includeValues := make(map[string]interface{})
			for cname, val := range record {
				if len(valuesToLabels[cname]) > 0 {
					actualIndex := rawIncludeValuesIndicies[cname]
					switch valType := val.(type) {
					case []interface{}:
						if actualIndex > len(valType)-1 || actualIndex < 0 {
							log.Errorf("GetJsonFromAllRrc: index=%v out of bounds for column=%v of length %v", actualIndex, cname, len(valType))
							continue
						}
						includeValues[valuesToLabels[cname]] = valType[actualIndex]
					case interface{}:
						log.Errorf("GetJsonFromAllRrc: accessing object in %v as array!", cname)
						continue
					default:
						log.Errorf("GetJsonFromAllRrc: unsupported value type")
						continue
					}
				}

			}
			for label, val := range includeValues {
				if record[label] != nil {
					log.Errorf("GetJsonFromAllRrc: accessing object in %v as array!", label) //case where label == original column
					continue
				}
				record[label] = val
			}

			delete(recordIndexInFinal, recInden)

			if unknownIndex {
				allRecords = append(allRecords, record)
			} else {
				allRecords[idx] = record
			}

			if transactionArgsExist {
				recsAggRecords = append(recsAggRecords, record)
			}
		}
	}

	if !(tableColumnsExist || aggs.OutputTransforms == nil || hasQueryAggergatorBlock || transactionArgsExist) {
		allRecords, finalCols = applyHardcodedColumns(hardcodedArray, renameHardcodedColumns, allRecords, finalCols)
	} else {
		for currSeg, blkIds := range segmap {
			blkIdsIndex := 0
			for blkNum, recNums := range blkIds.BlkRecIndexes {
				blkIdsIndex++
				isLastBlk := blkIdsIndex == len(blkIds.BlkRecIndexes)

				blkRecIndexes := make(map[uint16]map[uint16]uint64)
				blkRecIndexes[blkNum] = recNums
				processSingleSegment(currSeg, blkIds.VirtualTableName, blkRecIndexes, isLastBlk)
			}
		}
	}

	if nodeRes.RecsAggsProcessedSegments >= numTotalSegments {
		delete(nodeResMap, qid)
	}

	finalRecords, colsSlice := finalizeRecords(allRecords, finalCols, numProcessedRecords, recsAggRecords, transactionArgsExist)
	log.Infof("qid=%d, GetJsonFromAllRrc: Got %v raw records from files in %+v", qid, len(finalRecords), time.Since(sTime))

	return finalRecords, colsSlice, nil
}

func addKeyValuePairs(record map[string]interface{}) map[string]interface{} {
	for _, value := range record {
		if strValue, ok := value.(string); ok {
			// Check if the string value has key-value pairs
			keyValuePairs, err := extractKeyValuePairsFromString(strValue)
			if err == nil {
				// Add key-value pairs to the record
				for k, v := range keyValuePairs {
					record[k] = v
				}
			}
		}
	}
	return record
}

func extractKeyValuePairsFromString(str string) (map[string]interface{}, error) {
	keyValuePairs := make(map[string]interface{})
	pairs := strings.Split(str, ",")

	for _, pair := range pairs {
		parts := strings.Split(pair, "=")
		if len(parts) == 2 {
			key := strings.TrimSpace(parts[0])
			value := strings.TrimSpace(parts[1])
			keyValuePairs[key] = utils.GetLiteralFromString(value)
		} else {
			return nil, fmt.Errorf("invalid key-value pair: %s", pair)
		}
	}

	return keyValuePairs, nil
}
