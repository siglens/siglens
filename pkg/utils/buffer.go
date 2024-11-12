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

const chunkSize = 1024

type Buffer struct {
	chunks [][]byte
	offset int // Index of first unused byte in the last chunk
}

func (b *Buffer) Len() int {
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
			b.chunks = append(b.chunks, make([]byte, chunkSize))
			availableBytes = chunkSize
		}

		if len(data) <= availableBytes {
			copy(b.chunks[len(b.chunks)-1][b.offset:], data)
			b.offset += len(data)
			return
		}

		copy(b.chunks[len(b.chunks)-1][b.offset:], data[:availableBytes])
		data = data[availableBytes:]
	}
}

func (b *Buffer) Read(start int, end int) ([]byte, error) {
	return nil, nil
}

func (b *Buffer) ReadAll() []byte {
	if len(b.chunks) == 0 {
		return nil
	}

	buf := make([]byte, b.Len())
	offset := 0
	for i := 0; i < len(b.chunks)-1; i++ {
		copy(buf[offset:], b.chunks[i])
		offset += chunkSize
	}

	copy(buf[offset:], b.chunks[len(b.chunks)-1][:b.offset])

	return buf
}
