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
		assert.Equal(t, len(readData), buffer.Len())
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

func Test_CopyTo(t *testing.T) {
	seed := 42
	data := RandomBuffer(chunkSize*3+10, seed)
	buffer := &Buffer{}
	buffer.Append(data)

	// Insufficient space
	bytes := make([]byte, 10)
	err := buffer.CopyTo(bytes)
	assert.Error(t, err)

	// Sufficient space
	bytes = make([]byte, buffer.Len())
	err = buffer.CopyTo(bytes)
	assert.NoError(t, err)
	assert.Equal(t, data, bytes)
}

func Test_CopyFrom(t *testing.T) {
	data := make([]byte, chunkSize*2)
	buffer := &Buffer{}
	buffer.Append(data)

	newData := []byte("hello")
	err := buffer.WriteAt(newData, 0)
	assert.NoError(t, err)
	assert.Equal(t, newData, buffer.Slice(0, len(newData)))

	err = buffer.WriteAt(newData, chunkSize-len(newData))
	assert.NoError(t, err)
	assert.Equal(t, newData, buffer.Slice(chunkSize-len(newData), chunkSize))

	err = buffer.WriteAt(newData, chunkSize-1)
	assert.NoError(t, err)
	assert.Equal(t, newData, buffer.Slice(chunkSize-1, chunkSize+len(newData)-1))

	// Invalid start
	err = buffer.WriteAt(newData, chunkSize*2-1)
	assert.Error(t, err)

	err = buffer.WriteAt(newData, -1)
	assert.Error(t, err)
}

func Test_Len(t *testing.T) {
	buffer := &Buffer{}
	assert.Equal(t, 0, buffer.Len())

	seed := 42
	data1 := RandomBuffer(chunkSize*0+10, seed)
	data2 := RandomBuffer(chunkSize*1+10, seed+1)
	testAppendAndRead(t, buffer, data1, data2)

	assert.Equal(t, len(data1)+len(data2), buffer.Len())
}

func Test_Reset(t *testing.T) {
	seed := 42
	data1 := RandomBuffer(chunkSize*3+10, seed)
	data2 := RandomBuffer(chunkSize*2+10, seed+1)
	buffer := &Buffer{}
	testAppendAndRead(t, buffer, data1, data2)

	buffer.Reset()
	assert.Equal(t, 0, buffer.Len())
	data3 := RandomBuffer(chunkSize*1+10, seed+2)
	testAppendAndRead(t, buffer, data3)
}

func Test_Slice(t *testing.T) {
	seed := 42
	data := RandomBuffer(chunkSize*3+10, seed)
	buffer := &Buffer{}
	assert.Equal(t, []byte{}, buffer.Slice(0, 0))
	buffer.Append(data)

	assert.Equal(t, data[:10], buffer.Slice(0, 10))
	assert.Equal(t, data[chunkSize*3:], buffer.Slice(chunkSize*3, buffer.Len()))
	assert.Equal(t, data[chunkSize-5:chunkSize], buffer.Slice(chunkSize-5, chunkSize))
	assert.Equal(t, data[chunkSize-5:chunkSize+1], buffer.Slice(chunkSize-5, chunkSize+1))
	assert.Equal(t, data[chunkSize-1:chunkSize+1], buffer.Slice(chunkSize-1, chunkSize+1))
	assert.Equal(t, data[chunkSize-5:chunkSize+5], buffer.Slice(chunkSize-5, chunkSize+5))
	assert.Equal(t, data, buffer.Slice(0, buffer.Len()))
	assert.Equal(t, data[0:0], buffer.Slice(0, 0))
	assert.Equal(t, []byte{}, buffer.Slice(buffer.Len(), buffer.Len()))
}

func Test_Slice_fullLastChunk(t *testing.T) {
	seed := 42
	data := RandomBuffer(chunkSize*3, seed)
	buffer := &Buffer{}
	buffer.Append(data)

	assert.Equal(t, data, buffer.Slice(0, buffer.Len()))
}

func Test_AppendLittleEndian(t *testing.T) {
	buffer := &Buffer{}
	buffer.AppendUint16LittleEndian(1)
	assert.Equal(t, 2, buffer.Len())

	buffer.AppendUint32LittleEndian(2)
	assert.Equal(t, 6, buffer.Len())

	buffer.AppendUint64LittleEndian(3)
	assert.Equal(t, 14, buffer.Len())

	buffer.AppendInt64LittleEndian(-4)
	assert.Equal(t, 22, buffer.Len())

	buffer.AppendFloat64LittleEndian(5.5)
	assert.Equal(t, 30, buffer.Len())

	bytes := buffer.ReadAll()
	assert.Equal(t, uint16(1), BytesToUint16LittleEndian(bytes[0:2]))
	assert.Equal(t, uint32(2), BytesToUint32LittleEndian(bytes[2:6]))
	assert.Equal(t, uint64(3), BytesToUint64LittleEndian(bytes[6:14]))
	assert.Equal(t, int64(-4), BytesToInt64LittleEndian(bytes[14:22]))
	assert.Equal(t, float64(5.5), BytesToFloat64LittleEndian(bytes[22:30]))
}
