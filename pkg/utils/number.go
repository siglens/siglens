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

type numberType = byte

const (
	_ numberType = iota
	uint64Type
	float64Type
)

type Number struct {
	bytes [9]byte // 8 bytes for the number; last byte for the type
}

func (n *Number) SetUint64(i uint64) {
	copy(n.bytes[:8], Uint64ToBytesLittleEndian(i))
	n.bytes[8] = uint64Type
}

func (n *Number) SetFloat64(f float64) {
	copy(n.bytes[:8], Float64ToBytesLittleEndian(f))
	n.bytes[8] = float64Type
}

func (n *Number) Uint64() uint64 {
	if n.bytes[8] != uint64Type {
		panic("Not a uint64")
	}

	return BytesToUint64LittleEndian(n.bytes[:8])
}

func (n *Number) Float64() float64 {
	if n.bytes[8] != float64Type {
		panic("Not a float64")
	}

	return BytesToFloat64LittleEndian(n.bytes[:8])
}
