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

const chunkSize = 1024

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

func (b *Buffer) Read(start int, end int) ([]byte, error) {
	return nil, nil
}

func (b *Buffer) ReadAll() ([]byte, error) {
	if b == nil {
		return nil, TeeErrorf("Buffer.ReadAll: nil buffer")
	}

	if len(b.chunks) == 0 {
		return nil, nil
	}

	buf := make([]byte, b.Len())
	err := b.CopyTo(buf)
	if err != nil {
		log.Errorf("Buffer.ReadAll: failed to copy; err=%v", err)
		return nil, err
	}

	return buf, nil
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

func (b *Buffer) Reset() {
	for i := range b.chunks {
		chunkPool.Put(&b.chunks[i])
	}

	b.chunks = nil
	b.offset = 0
}
