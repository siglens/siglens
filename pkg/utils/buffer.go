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
	"sync"

	log "github.com/sirupsen/logrus"
)

const chunkSize = 16 * 1024

var chunkPool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		slice := make([]byte, chunkSize)
		return &slice
	},
}

type Buffer struct {
	chunks [][]byte
	offset int // Index of first unused byte in the last chunk
}

func (b *Buffer) Len() int {
	if b == nil {
		return 0
	}

	if len(b.chunks) == 0 {
		return b.offset
	}

	return b.offset + (len(b.chunks)-1)*chunkSize
}

func (b *Buffer) Cap() int {
	if b == nil {
		return 0
	}

	return len(b.chunks) * chunkSize
}

func (b *Buffer) Append(data []byte) {
	if b == nil {
		return
	}

	for {
		if len(data) == 0 {
			return
		}

		var availableBytes int
		if b.offset > 0 {
			availableBytes = chunkSize - b.offset
		} else {
			nextChunk, ok := chunkPool.Get().(*[]byte)
			if ok {
				b.chunks = append(b.chunks, *nextChunk)
			} else {
				log.Warnf("Buffer.Append: failed to get chunk from pool")
				b.chunks = append(b.chunks, make([]byte, chunkSize))
			}
			availableBytes = chunkSize
		}

		if len(data) <= availableBytes {
			copy(b.chunks[len(b.chunks)-1][b.offset:], data)
			b.offset += len(data)
			return
		}

		copy(b.chunks[len(b.chunks)-1][b.offset:], data[:availableBytes])
		data = data[availableBytes:]
		b.offset = 0
	}
}

// TODO: check if optimizing this (and similar) functions by avoiding the
// temporary buffer is worthwhile.
func (b *Buffer) AppendUint16LittleEndian(val uint16) {
	buf := [2]byte{}
	Uint16ToBytesLittleEndianInplace(val, buf[:])
	b.Append(buf[:])
}

func (b *Buffer) AppendUint32LittleEndian(val uint32) {
	buf := [4]byte{}
	Uint32ToBytesLittleEndianInplace(val, buf[:])
	b.Append(buf[:])
}

func (b *Buffer) AppendUint64LittleEndian(val uint64) {
	buf := [8]byte{}
	Uint64ToBytesLittleEndianInplace(val, buf[:])
	b.Append(buf[:])
}

func (b *Buffer) AppendInt64LittleEndian(val int64) {
	buf := [8]byte{}
	Int64ToBytesLittleEndianInplace(val, buf[:])
	b.Append(buf[:])
}

func (b *Buffer) AppendFloat64LittleEndian(val float64) {
	buf := [8]byte{}
	Float64ToBytesLittleEndianInplace(val, buf[:])
	b.Append(buf[:])
}

// If Buffer was a normal contiguous slice, Slice(start, end) would be
// equivalent to Buffer[start:end].
func (b *Buffer) Slice(start int, end int) []byte {
	if b == nil {
		return nil
	}

	if start < 0 || end < start || end > b.Len() {
		return nil
	}

	if start == end {
		return []byte{}
	}

	if start/chunkSize == (end-1)/chunkSize {
		// This range is entirely within a single chunk.
		chunkStart := start % chunkSize
		chunkEnd := end % chunkSize
		if chunkEnd == 0 {
			chunkEnd = chunkSize
		}

		return b.chunks[start/chunkSize][chunkStart:chunkEnd]
	}

	buf := make([]byte, end-start)
	numSkippedChunks := start / chunkSize
	copy(buf, b.chunks[numSkippedChunks][start%chunkSize:])
	offset := chunkSize - (start % chunkSize)
	for i := numSkippedChunks + 1; i <= end/chunkSize && i < len(b.chunks); i++ {
		// Note: on the last iteration, we generally don't wnat to copy the
		// whole chunk, but since copy() copies the minimum of the two slice
		// lengths, we don't need to do anything special.
		copy(buf[offset:], b.chunks[i])
		offset += chunkSize
	}

	return buf
}

func (b *Buffer) At(pos int) (byte, error) {
	slice := b.Slice(pos, pos+1)
	if slice == nil {
		return 0, TeeErrorf("Buffer.At: invalid position %v; len=%v", pos, b.Len())
	}

	return slice[0], nil
}

func (b *Buffer) ReadAll() []byte {
	return b.Slice(0, b.Len())
}

func (b *Buffer) CopyTo(dst []byte) error {
	if b == nil {
		return TeeErrorf("Buffer.CopyTo: nil buffer")
	}

	if len(dst) < b.Len() {
		return TeeErrorf("Buffer.CopyTo: destination has %v but needs %v bytes",
			len(dst), b.Len())
	}

	offset := 0
	for i := 0; i < len(b.chunks)-1; i++ {
		copy(dst[offset:], b.chunks[i])
		offset += chunkSize
	}

	copy(dst[offset:], b.chunks[len(b.chunks)-1][:b.offset])

	return nil
}

func (b *Buffer) WriteAt(src []byte, start int) error {
	if b == nil {
		return TeeErrorf("Buffer.WriteAt: nil buffer")
	}

	if start < 0 || start > b.Len() {
		return TeeErrorf("Buffer.WriteAt: invalid start position %v; len=%v", start, b.Len())
	}

	if start+len(src) > b.Len() {
		return TeeErrorf("Buffer.WriteAt: src has %v bytes but needs %v bytes",
			len(src), b.Len()-start)
	}

	numSkippedChunks := start / chunkSize
	offset := start % chunkSize
	for i := numSkippedChunks; i < len(b.chunks) && len(src) > 0; i++ {
		numBytesToCopy := Min(len(src), chunkSize-offset)
		copy(b.chunks[i][offset:], src[:numBytesToCopy])
		src = src[numBytesToCopy:]
		offset = 0
	}

	return nil
}

func (b *Buffer) Reset() {
	for i := range b.chunks {
		chunkPool.Put(&b.chunks[i])
	}

	b.chunks = nil
	b.offset = 0
}
