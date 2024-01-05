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

package record

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/siglens/siglens/pkg/config"
	agg "github.com/siglens/siglens/pkg/segment/aggregations"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

// Gets all raw json records from RRCs. If esResponse is false, _id and _type will not be added to any record
func GetJsonFromAllRrc(allrrc []*utils.RecordResultContainer, esResponse bool, qid uint64,
	segEncToKey map[uint16]string, aggs *structs.QueryAggregators) ([]map[string]interface{}, []string, error) {

	sTime := time.Now()
	segmap := make(map[string]*utils.BlkRecIdxContainer)
	recordIndexInFinal := make(map[string]int)
	for idx, rrc := range allrrc {
		if rrc.SegKeyInfo.IsRemote {
			log.Debugf("GetJsonFromAllRrc: skipping remote segment:%v", rrc.SegKeyInfo.RecordId)
			continue
		}
		segkey, ok := segEncToKey[rrc.SegKeyInfo.SegKeyEnc]
		if !ok {
			log.Errorf("GetJsonFromAllRrc: could not find segenc:%v in map", rrc.SegKeyInfo.SegKeyEnc)
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
	allRecords := make([]map[string]interface{}, len(allrrc))
	finalCols := make(map[string]bool)
	hasQueryAggergatorBlock := aggs.HasQueryAggergatorBlockInChain()
	if tableColumnsExist || aggs.OutputTransforms == nil || hasQueryAggergatorBlock {
		for currSeg, blkIds := range segmap {
			recs, cols, err := GetRecordsFromSegment(currSeg, blkIds.VirtualTableName, blkIds.BlkRecIndexes,
				config.GetTimeStampKey(), esResponse, qid, aggs)
			if err != nil {
				log.Errorf("GetJsonFromAllRrc: failed to read recs from segfile=%v, err=%v", currSeg, err)
				continue
			}
			for cName := range cols {
				finalCols[cName] = true
			}

			for key := range renameHardcodedColumns {
				finalCols[key] = true
			}

			for recInden, record := range recs {

				if hasQueryAggergatorBlock {
					nodeRes := &structs.NodeResult{}
					agg.PostQueryBucketCleaning(nodeRes, aggs, recs, finalCols)
				}

				for key, val := range renameHardcodedColumns {
					record[key] = val
				}

				idx, ok := recordIndexInFinal[recInden]
				if !ok {
					log.Errorf("qid=%d, GetJsonFromAllRrc: Did not find index for record indentifier %s.", qid, recInden)
					continue
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
				allRecords[idx] = record

			}
		}
	} else {
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
	}

	colsSlice := make([]string, len(finalCols))
	idx := 0
	for colName := range finalCols {
		colsSlice[idx] = colName
		idx++
	}

	sort.Strings(colsSlice)
	log.Infof("qid=%d, GetJsonFromAllRrc: Got %v raw records from files in %+v", qid, len(allRecords), time.Since(sTime))
	return allRecords, colsSlice, nil
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
