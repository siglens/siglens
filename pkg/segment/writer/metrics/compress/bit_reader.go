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
)

// A reader reads bits from an io.reader
type bitReader struct {
	r      io.Reader
	buffer [1]byte
	count  uint8 // The number of right-most bits valid to read (from left) in the current 8 byte buffer.
}

// newReader returns a reader that returns a single bit at a time from 'r'
func newBitReader(r io.Reader) *bitReader {
	return &bitReader{r: r}
}

// readBit returns the next bit from the stream, reading a new byte
// from the underlying reader if required.
func (b *bitReader) readBit() (bit, error) {
	if b.count == 0 {
		n, err := b.r.Read(b.buffer[:])
		if err != nil {
			return zero, fmt.Errorf("failed to read a byte: %w", err)
		}
		if n != 1 {
			return zero, errors.New("read more than a byte")
		}
		b.count = 8
	}
	b.count--
	// bitwise AND
	// (e.g.)
	// 11111111 & 10000000 = 10000000
	// 11000011 & 10000000 = 10000000
	d := (b.buffer[0] & 0x80)
	// Left shift to read next bit
	b.buffer[0] <<= 1
	return d != 0, nil
}

// readBits constructs a uint64 with the nbits right-most bits
// read from the stream, and any other bits 0.
func (b *bitReader) readByte() (byte, error) {
	if b.count == 0 {
		n, err := b.r.Read(b.buffer[:])
		if err != nil {
			return b.buffer[0], fmt.Errorf("failed to read a byte: %w", err)
		}
		if n != 1 {
			return b.buffer[0], errors.New("read more than a byte")
		}
		return b.buffer[0], nil
	}

	byt := b.buffer[0]

	n, err := b.r.Read(b.buffer[:])
	if err != nil {
		return 0, fmt.Errorf("failed to read a byte: %w", err)
	}
	if n != 1 {
		return b.buffer[0], errors.New("read more than a byte")
	}

	byt |= b.buffer[0] >> b.count
	b.buffer[0] <<= (8 - b.count)

	return byt, nil
}

// readBits reads nbits from the stream
func (b *bitReader) readBits(nbits int) (uint64, error) {
	var u uint64

	for 8 <= nbits {
		byt, err := b.readByte()
		if err != nil {
			return 0, err
		}

		u = (u << 8) | uint64(byt)
		nbits -= 8
	}

	var err error
	for nbits > 0 && err != io.EOF {
		byt, err := b.readBit()
		if err != nil {
			return 0, err
		}
		u <<= 1
		if byt {
			u |= 1
		}
		nbits--
	}

	return u, nil
}
