// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import "bytes"

func ReadLine(buf []byte) ([]byte, []byte) {
	end := bytes.IndexByte(buf, '\n')
	if end == -1 {
		return buf, nil
	}

	return buf[:end], buf[end+1:]
}

func ContainsAnyCase(buf []byte, word []byte) bool {
	if len(word) == 0 {
		return true
	}

	for i := 0; i < len(buf)-len(word)+1; i++ {
		if EqualAnyCase(buf[i:i+len(word)], word) {
			return true
		}
	}

	return false
}

func EqualAnyCase(s1, s2 []byte) bool {
	if len(s1) != len(s2) {
		return false
	}

	for i := 0; i < len(s1); i++ {
		if !equalAsciiAnyCase(s1[i], s2[i]) {
			return false
		}
	}

	return true
}

func equalAsciiAnyCase(a, b byte) bool {
	if a == b {
		// Fast path for equals.
		return true
	}

	if a^b != 32 {
		// Fast path for not equals.
		return false
	}

	return (a >= 'A' && a <= 'Z') || (a >= 'a' && a <= 'z')
}
