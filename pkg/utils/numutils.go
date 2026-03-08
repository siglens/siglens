// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"errors"
	"math"
	"strconv"
	"strings"
)

var INVALID_FLOAT_ERR = errors.New("invalid float")

func FastParseFloat(val []byte) (float64, error) {
	if len(val) == 0 {
		return 0, INVALID_FLOAT_ERR
	}

	var sign float64 = 1
	i := 0

	if val[i] == '-' {
		sign = -1
		i++
	} else if val[i] == '+' {
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

	result := sign * (intPart + fracPart/divisor)

	if i < len(val) && (val[i] == 'e' || val[i] == 'E') {
		i++
		if i == len(val) {
			return 0, INVALID_FLOAT_ERR
		}

		expSign := 1
		if val[i] == '-' {
			expSign = -1
			i++
		} else if val[i] == '+' {
			i++
		}

		if i == len(val) || val[i] < '0' || val[i] > '9' {
			return 0, INVALID_FLOAT_ERR
		}

		exp := 0
		for ; i < len(val); i++ {
			c := val[i]
			if c >= '0' && c <= '9' {
				exp = exp*10 + int(c-'0')
			} else {
				return 0, INVALID_FLOAT_ERR
			}
		}
		multiplier := math.Pow(10, float64(exp)*float64(expSign))
		result *= multiplier
	}

	if i != len(val) {
		return 0, INVALID_FLOAT_ERR
	}

	return result, nil
}

func HumanizeUints(v uint64) string {
	if v < 1000 {
		return strconv.FormatUint(v, 10)
	}
	parts := []string{"", "", "", "", "", "", ""}
	j := len(parts) - 1
	for v > 999 {
		parts[j] = strconv.FormatUint(v%1000, 10)
		switch len(parts[j]) {
		case 2:
			parts[j] = "0" + parts[j]
		case 1:
			parts[j] = "00" + parts[j]
		}
		v = v / 1000
		j--
	}
	parts[j] = strconv.FormatUint(v, 10)
	return strings.Join(parts[j:], ",")
}
