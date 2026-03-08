// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"fmt"
	"math"

	"github.com/siglens/siglens/pkg/utils"
)

type numberType = byte

const (
	invalidType numberType = iota
	int64Type
	float64Type
	backfillType
)

type Number struct {
	bytes [9]byte // 8 bytes for the number; last byte for the type
}

func (n *Number) SetInvalidType() {
	n.bytes[8] = invalidType
}

func (n *Number) SetBackfillType() {
	n.bytes[8] = backfillType
}

func (n *Number) SetInt64(i int64) {
	utils.Int64ToBytesLittleEndianInplace(i, n.bytes[:8])
	n.bytes[8] = int64Type
}

func (n *Number) SetFloat64(f float64) {
	utils.Float64ToBytesLittleEndianInplace(f, n.bytes[:8])
	n.bytes[8] = float64Type
}

func (n *Number) Int64() (int64, error) {

	if n.bytes[8] == backfillType {
		return 0, nil
	}

	if n.bytes[8] != int64Type {
		return 0, fmt.Errorf("Not a int64, t: %v", n.bytes[8])
	}

	return utils.BytesToInt64LittleEndian(n.bytes[:8]), nil
}

func (n *Number) Float64() (float64, error) {

	if n.bytes[8] == backfillType {
		return 0, nil
	}

	if n.bytes[8] != float64Type {
		return 0, fmt.Errorf("Not a float64, t: %v", n.bytes[8])
	}

	return utils.BytesToFloat64LittleEndian(n.bytes[:8]), nil
}

func ConvertBytesToNumber(buf []byte) (int64, float64, SS_DTYPE) {
	var intVal int64
	var fltVal float64
	var dtype SS_DTYPE

	if len(buf) < 9 {
		return intVal, fltVal, SS_INVALID
	}
	switch buf[8] {
	case invalidType:
		dtype = SS_INVALID
	case backfillType:
		dtype = SS_DT_BACKFILL
	case int64Type:
		intVal = utils.BytesToInt64LittleEndian(buf[:8])
		dtype = SS_DT_SIGNED_NUM
	case float64Type:
		fltVal = utils.BytesToFloat64LittleEndian(buf[:8])
		dtype = SS_DT_UNSIGNED_NUM
	}
	return intVal, fltVal, dtype
}

func (n *Number) ConvertToFloat64() error {

	switch n.bytes[8] {
	case backfillType, float64Type:
		return nil
	case int64Type:
		ni, err := n.Int64()
		if err != nil {
			return err
		}
		n.SetFloat64(float64(ni))
		return nil
	default:
		return fmt.Errorf("Not a float64, t: %v", n.bytes[8])
	}
}

func (n *Number) ConvertToInt64() error {

	switch n.bytes[8] {
	case backfillType, int64Type:
		return nil
	case float64Type:
		nf, err := n.Float64()
		if err != nil {
			return err
		}
		n.SetInt64(int64(nf))
		return nil
	default:
		return fmt.Errorf("Not a int64, t: %v", n.bytes[8])
	}
}

func (n *Number) Reset() {
	n.bytes[8] = invalidType
}

func (n *Number) ntype() numberType {
	return n.bytes[8]
}

func (n *Number) CopyToBuffer(buf []byte) {
	copy(buf, n.bytes[:])
}
func (n *Number) Copy(other *Number) {
	copy(n.bytes[:], other.bytes[:])
}

func (n *Number) ReduceFast(other *Number, fun AggregateFunctions) error {

	if n.ntype() == invalidType {
		copy(n.bytes[:], other.bytes[:])
		return nil
	} else if other.ntype() == invalidType {
		return nil
	} else if other.ntype() == backfillType {
		return nil
	} else if n.ntype() == backfillType {
		copy(n.bytes[:], other.bytes[:])
		return nil
	}

	// If I am float and other is int then Convert other to float
	if n.ntype() == float64Type && other.ntype() == int64Type {
		err := other.ConvertToFloat64()
		if err != nil {
			return err
		}
	} else if n.ntype() == int64Type && other.ntype() == float64Type {
		// If I am int and other is float then Convert me to float
		err := n.ConvertToFloat64()
		if err != nil {
			return err
		}
	}

	switch n.ntype() {
	case int64Type:
		ni, err := n.Int64()
		if err != nil {
			return err
		}
		oi, err := other.Int64()
		if err != nil {
			return err
		}
		switch fun {
		case Sum:
			n.SetInt64(ni + oi)
		case Min:
			n.SetInt64(MinInt64(ni, oi))
		case Max:
			n.SetInt64(MaxInt64(ni, oi))
		case Count:
			n.SetInt64(ni + oi)
		default:
			return fmt.Errorf("ReduceFast: unsupported reduce function: %v", fun)
		}
	case float64Type:
		nf, err := n.Float64()
		if err != nil {
			return err
		}
		of, err := other.Float64()
		if err != nil {
			return err
		}
		switch fun {
		case Sum:
			n.SetFloat64(nf + of)
		case Min:
			n.SetFloat64(math.Min(nf, of))
		case Max:
			n.SetFloat64(math.Max(nf, of))
		case Count:
			n.SetFloat64(nf + of)
		default:
			return fmt.Errorf("ReduceFast: unsupported reduce function: %v", fun)
		}
	default:
		return fmt.Errorf("ReduceFast: unexpected Type: %v", n.ntype())
	}

	return nil
}

func (n *Number) ToCVal(enclosure *CValueEnclosure) error {
	if enclosure == nil {
		return fmt.Errorf("enclosure is nil")
	}

	switch n.ntype() {
	case invalidType:
		enclosure.Dtype = SS_INVALID
		enclosure.CVal = nil
	case backfillType:
		enclosure.Dtype = SS_DT_BACKFILL
		enclosure.CVal = nil
	case int64Type:
		i, err := n.Int64()
		if err != nil {
			return err
		}
		enclosure.CVal = i
		enclosure.Dtype = SS_DT_SIGNED_NUM
	case float64Type:
		f, err := n.Float64()
		if err != nil {
			return err
		}
		enclosure.CVal = f
		enclosure.Dtype = SS_DT_FLOAT
	}

	return nil
}
