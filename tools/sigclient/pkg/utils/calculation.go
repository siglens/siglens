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
	"strconv"

	log "github.com/sirupsen/logrus"
)

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
	default:
		log.Errorf("verifyInequality: Invalid relation: %v", relation)
		return false, fmt.Errorf("verifyInequality: Invalid relation: %v", relation)
	}
	return false, nil
}
