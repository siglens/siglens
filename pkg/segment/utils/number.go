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
	"fmt"
	"math"

	"github.com/siglens/siglens/pkg/utils"
)

type numberType = byte

const (
	invalidType numberType = iota
	uint64Type
	float64Type
)

type Number struct {
	bytes [9]byte // 8 bytes for the number; last byte for the type
}

func (n *Number) SetUint64(i uint64) {
	copy(n.bytes[:8], utils.Uint64ToBytesLittleEndian(i))
	n.bytes[8] = uint64Type
}

func (n *Number) SetFloat64(f float64) {
	copy(n.bytes[:8], utils.Float64ToBytesLittleEndian(f))
	n.bytes[8] = float64Type
}

func (n *Number) Uint64() uint64 {
	if n.bytes[8] != uint64Type {
		panic("Not a uint64")
	}

	return utils.BytesToUint64LittleEndian(n.bytes[:8])
}

func (n *Number) Float64() float64 {
	if n.bytes[8] != float64Type {
		panic("Not a float64")
	}

	return utils.BytesToFloat64LittleEndian(n.bytes[:8])
}

func (n *Number) ConvertToFloat64() {
	if n.bytes[8] == uint64Type {
		n.SetFloat64(float64(n.Uint64()))
	}

	if n.bytes[8] != float64Type {
		panic("Unexpected type")
	}
}

func (n *Number) ConvertToUint64() {
	if n.bytes[8] == float64Type {
		n.SetUint64(uint64(n.Float64()))
	}

	if n.bytes[8] != uint64Type {
		panic("Unexpected type")
	}
}

func (n *Number) Reset() {
	n.bytes[8] = invalidType
}

func (n *Number) Type() numberType {
	return n.bytes[8]
}

func (n *Number) CopyToBuffer(buf []byte) {
	copy(buf, n.bytes[:])
}

func (n *Number) ReduceFast(other *Number, fun AggregateFunctions) error {
	if n.Type() == invalidType {
		copy(n.bytes[:], other.bytes[:])
		return nil
	}

	if other.Type() == invalidType {
		return nil
	}

	// TODO: handle backfill?

	if n.Type() == uint64Type || other.Type() == uint64Type {
		n.ConvertToUint64()
		other.ConvertToUint64()
	}

	switch n.Type() {
	case uint64Type:
		switch fun {
		case Sum:
			n.SetUint64(n.Uint64() + other.Uint64())
		case Min:
			n.SetUint64(MinUint64(n.Uint64(), other.Uint64()))
		case Max:
			n.SetUint64(MaxUint64(n.Uint64(), other.Uint64()))
		case Count:
			n.SetUint64(n.Uint64() + other.Uint64())
		default:
			return fmt.Errorf("ReduceFast: unsupported reduce function: %v", fun)
		}
	case float64Type:
		switch fun {
		case Sum:
			n.SetFloat64(n.Float64() + other.Float64())
		case Min:
			n.SetFloat64(math.Min(n.Float64(), other.Float64()))
		case Max:
			n.SetFloat64(math.Max(n.Float64(), other.Float64()))
		case Count:
			n.SetFloat64(n.Float64() + other.Float64())
		default:
			return fmt.Errorf("ReduceFast: unsupported reduce function: %v", fun)
		}
	default:
		panic("Unexpected type")
	}

	return nil
}
