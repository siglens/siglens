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

package metautils

import (
	"github.com/bits-and-blooms/bloom/v3"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

func CheckBloomIndex(colNames map[string]string, bloom *bloom.BloomFilter) bool {
	for colName := range colNames {
		if !bloom.TestString(colName) {
			return false
		}
	}
	return true
}

func CheckRangeIndex(filterCol map[string]string, allRangeEntries map[string]*structs.Numbers, operator utils.FilterOperator, qid uint64) bool {
	var valueInRangeIndex bool
	for rawColName, colVal := range filterCol {
		if rawColName == "*" {
			for _, ri := range allRangeEntries {
				valueInRangeIndex = checkRangeIndexHelper(ri, colVal, operator, qid)
				if valueInRangeIndex {
					return true
				}
			}
		} else {
			if ri, ok := allRangeEntries[rawColName]; ok {
				valueInRangeIndex = checkRangeIndexHelper(ri, colVal, operator, qid)
				if valueInRangeIndex {
					return true
				}
			}
		}
	}
	return false
}

func checkRangeIndexHelper(ri *structs.Numbers, colVal string, operator utils.FilterOperator, qid uint64) bool {
	var valueInRangeIndex bool
	if operator == utils.Equals || operator == utils.NotEquals || operator == utils.LessThan || operator == utils.LessThanOrEqualTo || operator == utils.GreaterThan || operator == utils.GreaterThanOrEqualTo {
		switch ri.NumType {
		case utils.RNT_UNSIGNED_INT:
			convertedVal, err := dtu.ConvertToUInt(colVal, 64)
			if err != nil {
				log.Errorf("qid=%d checkRangeIndexHelper: Got an invalid literal for range filter: %s", qid, err)
				return false
			}
			valueInRangeIndex = doesUintPassRangeFilter(operator, convertedVal, ri.Min_uint64, ri.Max_uint64)
		case utils.RNT_SIGNED_INT:
			convertedVal, err := dtu.ConvertToInt(colVal, 64)
			if err != nil {
				log.Errorf("qid=%d checkRangeIndexHelper: Got an invalid literal for range filter: %s", qid, err)
				return false
			}
			valueInRangeIndex = doesIntPassRangeFilter(operator, convertedVal, ri.Min_int64, ri.Max_int64)
		case utils.RNT_FLOAT64:
			convertedVal, err := dtu.ConvertToFloat(colVal, 64)
			if err != nil {
				log.Errorf("qid=%d checkRangeIndexHelper: Got an invalid literal for range filter: %s", qid, err)
				return false
			}
			valueInRangeIndex = doesFloatPassRangeFilter(operator, convertedVal, ri.Min_float64, ri.Max_float64)
		default:
			log.Errorf("qid=%d checkRangeIndexHelper: Got an invalid range index type: %d", qid, ri.NumType)
		}
	}
	return valueInRangeIndex
}

func FilterBlocksByTime(bSum []*structs.BlockSummary, blkTracker *structs.BlockTracker,
	timeRange *dtu.TimeRange) map[uint16]map[string]bool {

	timeFilteredBlocks := make(map[uint16]map[string]bool)
	for i, blockSummary := range bSum {
		blkNum := uint16(i)
		if blkTracker.ShouldProcessBlock(blkNum) && timeRange.CheckRangeOverLap(blockSummary.LowTs, blockSummary.HighTs) {
			timeFilteredBlocks[blkNum] = make(map[string]bool)
		}
	}
	return timeFilteredBlocks
}

func doesUintPassRangeFilter(op utils.FilterOperator, lookupValue uint64, minVal uint64, maxVal uint64) bool {
	switch op {
	case utils.Equals:
		return lookupValue >= minVal && lookupValue <= maxVal
	case utils.NotEquals:
		if lookupValue == minVal || lookupValue == maxVal {
			return false
		}
		return true
	case utils.GreaterThan:
		return lookupValue < minVal || lookupValue < maxVal
	case utils.GreaterThanOrEqualTo:
		return lookupValue <= minVal || lookupValue <= maxVal
	case utils.LessThan:
		return lookupValue > minVal || lookupValue > maxVal
	case utils.LessThanOrEqualTo:
		return lookupValue >= minVal || lookupValue >= maxVal
	default:
		return true
	}
}

func doesIntPassRangeFilter(op utils.FilterOperator, lookupValue int64, minVal int64, maxVal int64) bool {
	switch op {
	case utils.Equals:
		return lookupValue >= minVal && lookupValue <= maxVal
	case utils.NotEquals:
		if lookupValue == minVal || lookupValue == maxVal {
			return false
		}
		return true
	case utils.GreaterThan:
		return lookupValue < minVal || lookupValue < maxVal
	case utils.GreaterThanOrEqualTo:
		return lookupValue <= minVal || lookupValue <= maxVal
	case utils.LessThan:
		return lookupValue > minVal || lookupValue > maxVal
	case utils.LessThanOrEqualTo:
		return lookupValue >= minVal || lookupValue >= maxVal
	default:
		return true
	}
}

func doesFloatPassRangeFilter(op utils.FilterOperator, lookupValue float64, minVal float64, maxVal float64) bool {
	switch op {
	case utils.Equals:
		return lookupValue >= minVal && lookupValue <= maxVal
	case utils.NotEquals:
		if lookupValue == minVal || lookupValue == maxVal {
			return false
		}
		return true
	case utils.GreaterThan:
		return lookupValue < minVal || lookupValue < maxVal
	case utils.GreaterThanOrEqualTo:
		return lookupValue <= minVal || lookupValue <= maxVal
	case utils.LessThan:
		return lookupValue > minVal || lookupValue > maxVal
	case utils.LessThanOrEqualTo:
		return lookupValue >= minVal || lookupValue >= maxVal
	default:
		return true
	}
}
