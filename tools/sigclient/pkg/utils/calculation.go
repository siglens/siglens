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
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

const TOLERANCE = 0.000001
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
