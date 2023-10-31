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
