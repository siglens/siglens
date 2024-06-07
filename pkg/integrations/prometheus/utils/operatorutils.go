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

package utils

import (
	"fmt"
	"math"
	"regexp"
	"sort"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

func GetLogicalAndArithmeticOperation(op parser.ItemType) segutils.LogicalAndArithmeticOperator {
	switch op {
	case parser.ADD:
		return segutils.LetAdd
	case parser.SUB:
		return segutils.LetSubtract
	case parser.MUL:
		return segutils.LetMultiply
	case parser.DIV:
		return segutils.LetDivide
	case parser.MOD:
		return segutils.LetModulo
	case parser.POW:
		return segutils.LetPower
	case parser.GTR:
		return segutils.LetGreaterThan
	case parser.GTE:
		return segutils.LetGreaterThanOrEqualTo
	case parser.LSS:
		return segutils.LetLessThan
	case parser.LTE:
		return segutils.LetLessThanOrEqualTo
	case parser.EQLC:
		return segutils.LetEquals
	case parser.NEQ:
		return segutils.LetNotEquals
	case parser.LAND:
		return segutils.LetAnd
	case parser.LOR:
		return segutils.LetOr
	case parser.LUNLESS:
		return segutils.LetUnless
	default:
		log.Errorf("getArithmeticOperation: unexpected op: %v", op)
		return 0
	}
}

func IsLogicalOperator(op segutils.LogicalAndArithmeticOperator) bool {
	switch op {
	case segutils.LetAnd:
		return true
	case segutils.LetOr:
		return true
	case segutils.LetUnless:
		return true
	default:
		return false
	}
}

func SetFinalResult(queryOp *structs.QueryArithmetic, finalResult map[string]map[uint32]float64, groupID string, timestamp uint32, valueLHS float64, valueRHS float64, swapped bool) {

	if swapped {
		tmp := valueLHS
		valueLHS = valueRHS
		valueRHS = tmp
	}

	if queryOp.ReturnBool {
		finalResult[groupID][timestamp] = 0
	}

	switch queryOp.Operation {
	case utils.LetAdd:
		finalResult[groupID][timestamp] = valueLHS + valueRHS
	case utils.LetDivide:
		if valueRHS == 0 {
			return
		}
		finalResult[groupID][timestamp] = valueLHS / valueRHS
	case utils.LetMultiply:
		finalResult[groupID][timestamp] = valueLHS * valueRHS
	case utils.LetSubtract:
		val := valueLHS - valueRHS
		finalResult[groupID][timestamp] = val
	case utils.LetModulo:
		finalResult[groupID][timestamp] = math.Mod(valueLHS, valueRHS)
	case utils.LetPower:
		finalResult[groupID][timestamp] = math.Pow(valueLHS, valueRHS)
	case utils.LetGreaterThan:
		isGtr := valueLHS > valueRHS
		if isGtr {
			if queryOp.ReturnBool {
				finalResult[groupID][timestamp] = 1
				return
			}
			// For a constant and a vector, we only swapped it when vector is in the right side in the original query
			if queryOp.ConstantOp && swapped {
				finalResult[groupID][timestamp] = valueRHS
			} else {
				finalResult[groupID][timestamp] = valueLHS
			}
		}
	case utils.LetGreaterThanOrEqualTo:
		isGte := valueLHS >= valueRHS
		if isGte {
			if queryOp.ReturnBool {
				finalResult[groupID][timestamp] = 1
				return
			}
			if queryOp.ConstantOp && swapped {
				finalResult[groupID][timestamp] = valueRHS
			} else {
				finalResult[groupID][timestamp] = valueLHS
			}
		}
	case utils.LetLessThan:
		isLss := valueLHS < valueRHS
		if isLss {
			if queryOp.ReturnBool {
				finalResult[groupID][timestamp] = 1
				return
			}
			if queryOp.ConstantOp && swapped {
				finalResult[groupID][timestamp] = valueRHS
			} else {
				finalResult[groupID][timestamp] = valueLHS
			}
		}
	case utils.LetLessThanOrEqualTo:
		isLte := valueLHS <= valueRHS
		if isLte {
			if queryOp.ReturnBool {
				finalResult[groupID][timestamp] = 1
				return
			}
			if queryOp.ConstantOp && swapped {
				finalResult[groupID][timestamp] = valueRHS
			} else {
				finalResult[groupID][timestamp] = valueLHS
			}
		}
	case utils.LetEquals:
		if valueLHS == valueRHS {
			if queryOp.ReturnBool {
				finalResult[groupID][timestamp] = 1
			} else {
				finalResult[groupID][timestamp] = valueLHS
			}
		}
	case utils.LetNotEquals:
		if valueLHS != valueRHS {
			if queryOp.ReturnBool {
				finalResult[groupID][timestamp] = 1
			} else {
				finalResult[groupID][timestamp] = valueLHS
			}
		}
	default:
		log.Errorf("SetFinalResult: does not support using this operator: %v", queryOp.Operation)
	}

	// Logical ops can only been used between 2 vectors
	if !queryOp.ConstantOp && IsLogicalOperator(queryOp.Operation) {
		switch queryOp.Operation {
		case utils.LetAnd:
			finalResult[groupID][timestamp] = valueLHS
		case utils.LetOr:
			finalResult[groupID][timestamp] = valueLHS
		case utils.LetUnless:
			finalResult[groupID][timestamp] = valueLHS
		default:
			log.Errorf("SetFinalResult: does not support using this operator: %v", queryOp.Operation)
		}
	}
}

// This method extracts tag key-value pairs from the string and processes the corresponding tags according to the include or exclude rules of matchingLabels.
// Finally, it concatenates the tag keys and values in dictionary order and returns the result.
// If matchingLabels = [tag1], groupIDStr = "metricName{tag1:val1,tag2:val2,tag3:val3}"
// When includeColumns is true, extract labels and generate result from matchingLabels based on the previous groupIDStr. The result is "tag1:val1,"
// Otherwise, exclude the labels, so the result is "tag2:val2,tag3:val3,"
func ExtractMatchingLabelSet(groupIDStr string, matchingLabels []string, includeColumns bool) string {

	labelKeysToValuesMap := make(map[string]string)

	re := regexp.MustCompile(`(.*)\{(.*)`)

	labelSetStr := ""
	match := re.FindStringSubmatch(groupIDStr)
	if len(match) == 3 {
		labelSetStr = match[2]
	} else {
		return groupIDStr
	}

	re = regexp.MustCompile(`\s*([\w\s]+):\s*([\w\s]+)`)

	matches := re.FindAllStringSubmatch(labelSetStr, -1)
	for _, match := range matches {
		if len(match) == 3 {
			labelKey := match[1]
			labelVal := match[2]
			labelKeysToValuesMap[labelKey] = labelVal
		} else {
			log.Errorf("ExtractMatchingLabelSet: can not correctly extract tags from labelStr: %v", labelSetStr)
		}
	}

	matchingLabelValStr := ""
	// When includeColumns is true, use matchingLabels to create a combination.
	// Otherwise, use all other labels except matchingLabels to create a combination.
	if includeColumns {
		for _, label := range matchingLabels {
			val, exists := labelKeysToValuesMap[label]
			if exists {
				matchingLabelValStr += fmt.Sprintf("%v:%v,", label, val)
			}
		}
	} else { // exclude matchingLabels
		for _, labelKeyToRemove := range matchingLabels {
			delete(labelKeysToValuesMap, labelKeyToRemove)
		}

		labelKeyStrs := make([]string, 0)
		for labelKey := range labelKeysToValuesMap {
			labelKeyStrs = append(labelKeyStrs, labelKey)
		}

		sort.Strings(labelKeyStrs)

		for _, labelKey := range labelKeyStrs {
			val := labelKeysToValuesMap[labelKey]
			matchingLabelValStr += fmt.Sprintf("%v:%v,", labelKey, val)
		}
	}

	return matchingLabelValStr
}

func CalculateStandardVariance(values []float64) float64 {
	sum := 0.0
	length := float64(len(values))
	for _, val := range values {
		sum += val
	}
	avg := sum / length
	sumValSquare := 0.0
	for _, val := range values {
		sumValSquare += (val - avg) * (val - avg)
	}
	return sumValSquare / length
}
