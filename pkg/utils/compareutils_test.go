/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
