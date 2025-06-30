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
