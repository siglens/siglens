// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"fmt"
	"strconv"
)

func AsUint64(x interface{}) (uint64, bool) {
	s := fmt.Sprintf("%v", x)
	result, err := strconv.ParseUint(s, 10, 64)
	if err == nil {
		return result, true
	}

	// Try to parse as float.
	f, ok := AsFloat64(x)
	if !ok {
		return 0, false
	}

	return uint64(f), true
}

func AsFloat64(x interface{}) (float64, bool) {
	s := fmt.Sprintf("%v", x)
	result, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}

	return result, true
}
