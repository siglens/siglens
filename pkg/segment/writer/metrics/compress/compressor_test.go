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
	"bytes"
	"math/rand"
	"testing"
	"time"

	fuzz "github.com/google/gofuzz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_Compress_Decompress(t *testing.T) {
	type data struct {
		t uint32
		v float64
	}
	header := uint32(time.Now().Unix())

	const dataLen = 50_000
	expected := make([]data, dataLen)
	valueFuzz := fuzz.New().NilChance(0)
	ts := header
	for i := 0; i < dataLen; i++ {
		if 0 < i && i%10 == 0 {
			ts -= uint32(rand.Intn(100))
		} else {
			ts += uint32(rand.Int31n(100))
		}
		var v float64
		valueFuzz.Fuzz(&v)
		expected[i] = data{ts, v}
	}

	buf := new(bytes.Buffer)

	// Compression
	c, cFinishFn, err := NewCompressor(buf, header)
	require.Nil(t, err)
	for _, data := range expected {
		b, err := c.Compress(data.t, data.v)
		require.Nil(t, err)
		require.Greater(t, b, uint64(0))
	}

	// copy the buffer
	copyBytesBuffer := bytes.NewBuffer(make([]byte, 0, buf.Len()))
	_, err = copyBytesBuffer.Write(buf.Bytes())
	require.Nil(t, err)

	clonedCompressor, clonedFinish := c.CloneCompressor(copyBytesBuffer)
	require.NotNil(t, clonedCompressor)
	require.NotNil(t, clonedFinish)
	require.Equal(t, c, clonedCompressor)

	// Finish the cloned compressor
	// This should not affect the original compressor
	err = clonedFinish()
	require.Nil(t, err)

	// Decompression using the cloned compressor
	var actual []data
	iter, err := NewDecompressIterator(copyBytesBuffer)
	require.Nil(t, err)
	for iter.Next() {
		t, v := iter.At()
		actual = append(actual, data{t, v})
	}
	require.Nil(t, iter.Err())
	assert.Equal(t, expected, actual)

	// Second Iteration of compression and decompression
	for i := 0; i < dataLen; i++ {
		if 0 < i && i%10 == 0 {
			ts -= uint32(rand.Intn(100))
		} else {
			ts += uint32(rand.Int31n(100))
		}
		var v float64
		valueFuzz.Fuzz(&v)
		expected = append(expected, data{ts, v})
	}

	// Compression
	for _, data := range expected[dataLen:] {
		b, err := c.Compress(data.t, data.v)
		require.Nil(t, err)
		require.Greater(t, b, uint64(0))
	}

	// reset the copy buffer
	copyBytesBuffer.Reset()
	_, err = copyBytesBuffer.Write(buf.Bytes())
	require.Nil(t, err)

	clonedCompressor, clonedFinish = c.CloneCompressor(copyBytesBuffer)
	require.NotNil(t, clonedCompressor)
	require.NotNil(t, clonedFinish)
	require.Equal(t, c, clonedCompressor)

	// Finish the cloned compressor
	err = clonedFinish()
	require.Nil(t, err)

	// Decompression using the cloned compressor
	actual = []data{}
	iter, err = NewDecompressIterator(copyBytesBuffer)
	require.Nil(t, err)
	for iter.Next() {
		t, v := iter.At()
		actual = append(actual, data{t, v})
	}
	require.Nil(t, iter.Err())
	assert.Equal(t, expected, actual)

	// Finish the original compressor
	err = cFinishFn()
	require.Nil(t, err)

	// Decompression using the original compressor
	actual = []data{}
	iter, err = NewDecompressIterator(buf)
	require.Nil(t, err)
	for iter.Next() {
		t, v := iter.At()
		actual = append(actual, data{t, v})
	}
	require.Nil(t, iter.Err())
	assert.Equal(t, expected, actual)
}
