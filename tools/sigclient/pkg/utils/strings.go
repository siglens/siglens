// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0
package utils

import "unicode"

func IsAscii(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > unicode.MaxASCII {
			return false
		}
	}

	return true
}
