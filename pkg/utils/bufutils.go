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
