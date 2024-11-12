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
	"testing"

	"github.com/stretchr/testify/assert"
)

func testAppendAndRead(t *testing.T, buffer *Buffer, data ...[]byte) {
	joinedData := []byte{}
	for _, d := range data {
		buffer.Append(d)
		joinedData = append(joinedData, d...)
		assert.Equal(t, len(joinedData), buffer.Len())

		readData := buffer.ReadAll()
		assert.Equal(t, joinedData, readData)
	}
}

func Test_Append_toEmpty(t *testing.T) {
	testAppendAndRead(t, &Buffer{}, []byte("hello"))
}

func Test_Append_spanningChunk(t *testing.T) {
	seed := 42
	data := RandomBuffer(chunkSize+10, seed)
	testAppendAndRead(t, &Buffer{}, data)
}

func Test_Append_spanningMultipleChunks(t *testing.T) {
	seed := 42
	data := RandomBuffer(chunkSize*3+10, seed)
	testAppendAndRead(t, &Buffer{}, data)
}

func Test_Append_multiple(t *testing.T) {
	seed := 42
	data1 := RandomBuffer(chunkSize*1+50, seed)
	data2 := RandomBuffer(chunkSize*2+10, seed+1)
	testAppendAndRead(t, &Buffer{}, data1, data2)
}
