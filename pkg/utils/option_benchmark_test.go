// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import "testing"

func BenchmarkIntOption(b *testing.B) {
	option := None[int]()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		option.Set(i)
		value, ok := option.Get()
		if !ok {
			panic("value should be set")
		}
		if value != i {
			panic("incorrect value")
		}
	}
}

func BenchmarkIntPointer(b *testing.B) {
	var value int
	var valuePtr *int
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		value = i
		valuePtr = &value

		if *valuePtr != i {
			panic("incorrect value")
		}
	}
}
