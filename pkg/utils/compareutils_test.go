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
	"bytes"
	"strings"
	"testing"
	"unsafe"
)

var (
	str  = strings.Repeat("a", 100)
	bArr = []byte(strings.Repeat("a", 99) + "b")
)

func Benchmark_UnsafeEqual(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		bbp := *(*string)(unsafe.Pointer(&bArr))
		_ = str == bbp
	}
}

func Benchmark_StrEqual(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = string(bArr) == str
	}
}

func Benchmark_ByteEqual(b *testing.B) {
	strBarr := []byte(str)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = bytes.Equal(bArr, strBarr)
	}
}
