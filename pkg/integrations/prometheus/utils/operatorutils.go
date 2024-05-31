package utils

import (
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

func SetFinalResult(queryOp structs.QueryArithmetic, finalResult map[string]map[uint32]float64, groupID string, timestamp uint32, valueLHS float64, valueRHS float64, swapped bool) {

	switch queryOp.Operation {
	case utils.LetAdd:
		finalResult[groupID][timestamp] = valueLHS + valueRHS
	case utils.LetDivide:
		if valueRHS == 0 {
			return
		}
		if swapped {
			finalResult[groupID][timestamp] = valueRHS / valueLHS
		} else {
			finalResult[groupID][timestamp] = valueLHS / valueRHS
		}
	case utils.LetMultiply:
		finalResult[groupID][timestamp] = valueLHS * valueRHS
	case utils.LetSubtract:
		val := valueLHS - valueRHS
		if swapped {
			val = val * -1
		}
		finalResult[groupID][timestamp] = val
	case utils.LetModulo:
		if swapped {
			finalResult[groupID][timestamp] = math.Mod(valueRHS, valueLHS)
		} else {
			finalResult[groupID][timestamp] = math.Mod(valueLHS, valueRHS)
		}
	case utils.LetPower:
		if swapped {
			finalResult[groupID][timestamp] = math.Pow(valueRHS, valueLHS)
		} else {
			finalResult[groupID][timestamp] = math.Pow(valueLHS, valueRHS)
		}
	case utils.LetGreaterThan:
		isGtr := valueLHS > valueRHS
		if swapped {
			isGtr = valueLHS < valueRHS
		}
		if isGtr {
			finalResult[groupID][timestamp] = valueLHS
		}
	case utils.LetGreaterThanOrEqualTo:
		isGte := valueLHS >= valueRHS
		if swapped {
			isGte = valueLHS <= valueRHS
		}
		if isGte {
			finalResult[groupID][timestamp] = valueLHS
		}
	case utils.LetLessThan:
		isLss := valueLHS < valueRHS
		if swapped {
			isLss = valueLHS > valueRHS
		}
		if isLss {
			finalResult[groupID][timestamp] = valueLHS
		}
	case utils.LetLessThanOrEqualTo:
		isLte := valueLHS <= valueRHS
		if swapped {
			isLte = valueLHS >= valueRHS
		}
		if isLte {
			finalResult[groupID][timestamp] = valueLHS
		}
	case utils.LetEquals:
		if valueLHS == valueRHS {
			finalResult[groupID][timestamp] = valueLHS
		}
	case utils.LetNotEquals:
		if valueLHS != valueRHS {
			finalResult[groupID][timestamp] = valueLHS
		}
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
		}
	}
}

func ExtractMatchingLabelSet(groupIDStr string, matchingLabels []string, on bool) string {

	labelSetMap := make(map[string]string)

	re, err := regexp.Compile(`(.*)\{(.*)`)
	if err != nil {
		return groupIDStr
	}

	labelSetStr := ""
	match := re.FindStringSubmatch(groupIDStr)
	if len(match) == 3 {
		labelSetStr = match[2]
	} else {
		return groupIDStr
	}

	re, err = regexp.Compile(`\s*([\w\s]+):\s*([\w\s]+)`)
	if err != nil {
		return labelSetStr
	}

	matches := re.FindAllStringSubmatch(labelSetStr, -1)
	for _, match := range matches {
		if len(match) == 3 {
			labelKey := match[1]
			labelVal := match[2]
			labelSetMap[labelKey] = labelVal
		}
	}

	matchingLabelValStr := ""
	if on {
		for _, label := range matchingLabels {
			val, exists := labelSetMap[label]
			if exists {
				matchingLabelValStr += val
			}
		}
	} else { // exclude matchingLabels
		for _, labelKeyToRemove := range matchingLabels {
			delete(labelSetMap, labelKeyToRemove)
		}

		labelKeyStrs := make([]string, 0)
		for labelKey := range labelSetMap {
			labelKeyStrs = append(labelKeyStrs, labelKey)
		}

		sort.Strings(labelKeyStrs)

		for _, labelKey := range labelKeyStrs {
			val, exists := labelSetMap[labelKey]
			if exists {
				matchingLabelValStr += (val + ",")
			}
		}
	}

	return matchingLabelValStr
}
