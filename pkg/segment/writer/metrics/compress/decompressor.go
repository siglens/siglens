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

package compress

import (
	"errors"
	"fmt"
	"io"
	"math"
)

// Compressor decompresses time-series data based on Facebook's paper.
// Link to the paper: https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
type Decompressor struct {
	br            *bitReader
	header        uint32
	t             uint32
	delta         uint32
	leadingZeros  uint8
	trailingZeros uint8
	value         uint64
}

// NewDecompressIterator initializes Decompressor and returns decompressed header.
func NewDecompressIterator(r io.Reader) (*DecompressIterator, error) {
	d := &Decompressor{
		br: newBitReader(r),
	}
	h, err := d.br.readBits(32)
	if err != nil {
		return nil, err
	}
	d.header = uint32(h)
	return &DecompressIterator{0, 0, nil, d}, nil
}

// Iterator returns an iterator of decompressor.
func (d *Decompressor) Iterator() *DecompressIterator {
	return &DecompressIterator{0, 0, nil, d}
}

// DecompressIterator is an iterator of Decompressor.
type DecompressIterator struct {
	t   uint32
	v   float64
	err error
	d   *Decompressor
}

// At returns decompressed time-series data.
func (di *DecompressIterator) At() (t uint32, v float64) {
	return di.t, di.v
}

// Err returns error during decompression.
func (di *DecompressIterator) Err() error {
	if errors.Is(di.err, io.EOF) {
		return nil
	}
	return di.err
}

// Next proceeds decompressing time-series data unitil EOF.
func (di *DecompressIterator) Next() bool {
	if di.d.t == 0 {
		di.t, di.v, di.err = di.d.decompressFirst()
	} else {
		di.t, di.v, di.err = di.d.decompress()
	}
	return di.err == nil
}

func (d *Decompressor) decompressFirst() (t uint32, v float64, err error) {
	delta, err := d.br.readBits(firstDeltaBits)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to decompress delta at first: %w", err)
	}
	if delta == 1<<firstDeltaBits-1 {
		return 0, 0, io.EOF
	}

	value, err := d.br.readBits(64)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to decompress value at first: %w", err)
	}

	d.delta = uint32(delta)
	d.t = d.header + d.delta
	d.value = value

	return d.t, math.Float64frombits(d.value), nil
}

func (d *Decompressor) decompress() (t uint32, v float64, err error) {
	t, err = d.decompressTimestamp()
	if err != nil {
		return 0, 0, err
	}

	v, err = d.decompressValue()
	if err != nil {
		return 0, 0, err
	}

	return t, v, nil
}

func (d *Decompressor) decompressTimestamp() (uint32, error) {
	n, err := d.dodTimestampBitN()
	if err != nil {
		return 0, err
	}

	if n == 0 {
		d.t += d.delta
		return d.t, nil
	}

	bits, err := d.br.readBits(int(n))
	if err != nil {
		return 0, fmt.Errorf("failed to read timestamp: %w", err)
	}

	if n == 32 && bits == 0xFFFFFFFF {
		return 0, io.EOF
	}

	var dod int64 = int64(bits)
	if n != 32 && 1<<(n-1) < int64(bits) {
		dod = int64(bits - 1<<n)
	}

	d.delta += uint32(dod)
	d.t += d.delta
	return d.t, nil
}

// returning the amount of delta-of-delta timestamp bits.
func (d *Decompressor) dodTimestampBitN() (n uint, err error) {
	var dod byte
	for i := 0; i < 4; i++ {
		dod <<= 1
		b, err := d.br.readBit()
		if err != nil {
			return 0, err
		}
		if b {
			dod |= 1
		} else {
			break
		}
	}

	switch dod {
	case 0x00: // 0
		return 0, nil
	case 0x02: // 10
		return 7, nil
	case 0x06: // 110
		return 9, nil
	case 0x0E: // 1110
		return 12, nil
	case 0x0F: // 1111
		return 32, nil
	default:
		return 0, errors.New("invalid bit header for bit length to read")
	}
}

func (d *Decompressor) decompressValue() (float64, error) {
	var read byte
	for i := 0; i < 2; i++ {
		bit, err := d.br.readBit()
		if err != nil {
			return 0, fmt.Errorf("failed to read value: %w", err)
		}
		if bit {
			read <<= 1
			read++
		} else {
			break
		}
	}
	if read == 0x1 || read == 0x3 { // read byte is '1' or '11'
		if read == 0x3 { // read byte is '11'
			leadingZeros, err := d.br.readBits(5)
			if err != nil {
				return 0, fmt.Errorf("failed to read value: %w", err)
			}
			significantBits, err := d.br.readBits(6)
			if err != nil {
				return 0, fmt.Errorf("failed to read value: %w", err)
			}
			if significantBits == 0 {
				significantBits = 64
			}
			d.leadingZeros = uint8(leadingZeros)
			d.trailingZeros = 64 - uint8(significantBits) - d.leadingZeros
		}
		// read byte is '11' or '1'
		valueBits, err := d.br.readBits(int(64 - d.leadingZeros - d.trailingZeros))
		if err != nil {
			return 0, fmt.Errorf("failed to read value: %w", err)
		}
		valueBits <<= uint64(d.trailingZeros)
		d.value ^= valueBits
	}
	return math.Float64frombits(d.value), nil
}
