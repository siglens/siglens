// Copyright (c) 2021-2025 SigScalr, Inc.
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
	"errors"
)

var INVALID_FLOAT_ERR = errors.New("invalid float")

func FastParseFloat(val []byte) (float64, error) {
	if len(val) == 0 {
		return 0, INVALID_FLOAT_ERR
	}

	var sign float64 = 1
	var i int

	if val[0] == '-' {
		sign = -1
		i++
	}

	var intPart float64
	for ; i < len(val); i++ {
		c := val[i]
		if c >= '0' && c <= '9' {
			intPart = intPart*10 + float64(c-'0')
		} else {
			break
		}
	}

	var fracPart float64
	divisor := 1.0

	if i < len(val) && val[i] == '.' {
		i++
		for ; i < len(val); i++ {
			c := val[i]
			if c >= '0' && c <= '9' {
				fracPart = fracPart*10 + float64(c-'0')
				divisor *= 10
			} else {
				break
			}
		}
	}

	if i != len(val) {
		return 0, INVALID_FLOAT_ERR
	}

	return sign * (intPart + fracPart/divisor), nil
}
