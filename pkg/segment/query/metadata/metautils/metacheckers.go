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

package metautils

import (
	"github.com/bits-and-blooms/bloom/v3"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
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

func CheckRangeIndex(filterCol map[string]string, allRangeEntries map[string]*structs.Numbers, operator segutils.FilterOperator, qid uint64) bool {
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

func checkRangeIndexHelper(ri *structs.Numbers, colVal string, operator segutils.FilterOperator, qid uint64) bool {
	var valueInRangeIndex bool
	if operator == segutils.Equals || operator == segutils.NotEquals || operator == segutils.LessThan || operator == segutils.LessThanOrEqualTo || operator == segutils.GreaterThan || operator == segutils.GreaterThanOrEqualTo {
		switch ri.NumType {
		case segutils.RNT_UNSIGNED_INT:
			convertedVal, err := dtu.ConvertToUInt(colVal, 64)
			if err != nil {
				log.Errorf("qid=%d checkRangeIndexHelper: Got an invalid literal for range filter: %s", qid, err)
				return false
			}
			valueInRangeIndex = doesUintPassRangeFilter(operator, convertedVal, ri.Min_uint64, ri.Max_uint64)
		case segutils.RNT_SIGNED_INT:
			convertedVal, err := dtu.ConvertToInt(colVal, 64)
			if err != nil {
				log.Errorf("qid=%d checkRangeIndexHelper: Got an invalid literal for range filter: %s", qid, err)
				return false
			}
			valueInRangeIndex = doesIntPassRangeFilter(operator, convertedVal, ri.Min_int64, ri.Max_int64)
		case segutils.RNT_FLOAT64:
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

// This function checks if an actualValue exists in the range for actualValue op lookupValue
// minval and maxVal are included in the range
func doesUintPassRangeFilter(op segutils.FilterOperator, lookupValue uint64, minVal uint64, maxVal uint64) bool {
	switch op {
	case segutils.Equals:
		return lookupValue >= minVal && lookupValue <= maxVal
	case segutils.NotEquals:
		if minVal == maxVal && lookupValue == minVal {
			return false
		}
		return true
	case segutils.GreaterThan:
		return lookupValue < minVal || lookupValue < maxVal
	case segutils.GreaterThanOrEqualTo:
		return lookupValue <= minVal || lookupValue <= maxVal
	case segutils.LessThan:
		return lookupValue > minVal || lookupValue > maxVal
	case segutils.LessThanOrEqualTo:
		return lookupValue >= minVal || lookupValue >= maxVal
	default:
		return true
	}
}

// This function checks if an actualValue exists in the range for actualValue op lookupValue
// minval and maxVal are included in the range
func doesIntPassRangeFilter(op segutils.FilterOperator, lookupValue int64, minVal int64, maxVal int64) bool {
	switch op {
	case segutils.Equals:
		return lookupValue >= minVal && lookupValue <= maxVal
	case segutils.NotEquals:
		if minVal == maxVal && lookupValue == minVal {
			return false
		}
		return true
	case segutils.GreaterThan:
		return lookupValue < minVal || lookupValue < maxVal
	case segutils.GreaterThanOrEqualTo:
		return lookupValue <= minVal || lookupValue <= maxVal
	case segutils.LessThan:
		return lookupValue > minVal || lookupValue > maxVal
	case segutils.LessThanOrEqualTo:
		return lookupValue >= minVal || lookupValue >= maxVal
	default:
		return true
	}
}

// This function checks if an actualValue exists in the range for actualValue op lookupValue
// minval and maxVal are included in the range
func doesFloatPassRangeFilter(op segutils.FilterOperator, lookupValue float64, minVal float64, maxVal float64) bool {
	switch op {
	case segutils.Equals:
		return lookupValue >= minVal && lookupValue <= maxVal
	case segutils.NotEquals:
		if minVal == maxVal && lookupValue == minVal {
			return false
		}
		return true
	case segutils.GreaterThan:
		return lookupValue < minVal || lookupValue < maxVal
	case segutils.GreaterThanOrEqualTo:
		return lookupValue <= minVal || lookupValue <= maxVal
	case segutils.LessThan:
		return lookupValue > minVal || lookupValue > maxVal
	case segutils.LessThanOrEqualTo:
		return lookupValue >= minVal || lookupValue >= maxVal
	default:
		return true
	}
}
