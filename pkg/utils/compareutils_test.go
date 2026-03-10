// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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
