// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

const TOLERANCE = 0.001
const MIN_IN_SEC = 60
const HOUR_IN_SEC = 3600
const DAY_IN_SEC = 86400

// Only string comparisons for equality are allowed
func VerifyInequalityForStr(actual string, relation, expected string) (bool, error) {
	if relation == "eq" {
		if actual == expected {
			return true, nil
		} else {
			return false, fmt.Errorf("verifyInequalityForStr: actual: \"%v\" and expected: \"%v\" are not equal", actual, expected)
		}
	} else {
		log.Errorf("verifyInequalityForStr: Invalid relation: %v", relation)
		return false, fmt.Errorf("verifyInequalityForStr: Invalid relation: %v", relation)
	}
}

// verifyInequality verifies the expected inequality returned by the query.
// returns true, nil if relation is ""
func VerifyInequality(actual float64, relation, expected string) (bool, error) {
	if relation == "" {
		return true, nil
	}
	fltVal, err := strconv.ParseFloat(expected, 64)
	if err != nil {
		log.Errorf("verifyInequality: Error in parsing expected value: %v, err: %v", expected, err)
		return false, err
	}
	switch relation {
	case "eq":
		if actual == fltVal {
			return true, nil
		}
	case "gt":
		if actual > fltVal {
			return true, nil
		}
	case "lt":
		if actual < fltVal {
			return true, nil
		}
	case "approx":
		return math.Abs(actual-fltVal) < TOLERANCE, nil
	default:
		log.Errorf("verifyInequality: Invalid relation: %v", relation)
		return false, fmt.Errorf("verifyInequality: Invalid relation: %v", relation)
	}
	return false, nil
}

func ConvertStringToEpochSec(nowTs uint64, inp string, defValue uint64) uint64 {
	sanTime := strings.ReplaceAll(inp, " ", "")

	if sanTime == "now" {
		return nowTs
	}

	retVal := defValue

	strln := len(sanTime)
	if strln < 6 {
		return retVal
	}

	unit := sanTime[strln-1]
	num, err := strconv.ParseInt(sanTime[4:strln-1], 0, 64)
	if err != nil {
		return defValue
	}

	switch unit {
	case 'm':
		retVal = nowTs - MIN_IN_SEC*uint64(num)
	case 'h':
		retVal = nowTs - HOUR_IN_SEC*uint64(num)
	case 'd':
		retVal = nowTs - DAY_IN_SEC*uint64(num)
	default:
		log.Errorf("convertStringToEpochSec: Unknown time unit %v", unit)
	}
	return retVal
}

func RemoveValues[K comparable, T any](values []K, valuesToRemove map[K]T) []K {
	finalValues := []K{}
	for _, value := range values {
		if _, exist := valuesToRemove[value]; !exist {
			finalValues = append(finalValues, value)
		}
	}

	return finalValues
}

func CompareSlices[K comparable](a []K, b []K) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func ElementsMatch[K comparable](a []K, b []K) bool {
	if len(a) != len(b) {
		return false
	}
	mp := make(map[K]int)
	for _, v := range a {
		mp[v]++
	}
	for _, v := range b {
		if _, ok := mp[v]; !ok {
			return false
		}
		mp[v]--
		if mp[v] == 0 {
			delete(mp, v)
		}
	}

	return len(mp) == 0
}

func AlmostEqual(actual, expected, tolerancePercentage float64) bool {
	if actual == expected {
		return true
	}
	diff := math.Abs(actual - expected)
	if expected == 0 {
		return diff < tolerancePercentage
	}

	return (diff / math.Abs(expected)) < tolerancePercentage
}

func GetKeysOfMap[K comparable, T any](m map[K]T) []K {
	keys := make([]K, 0)
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
