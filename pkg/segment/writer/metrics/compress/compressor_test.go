// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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
			ts += uint32(rand.Intn(100))
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

	// copy buffer
	copyBytesBuffer := bytes.NewBuffer(make([]byte, 0, buf.Len()))

	clonedCompressor, clonedFinish, err := CloneCompressor(c, copyBytesBuffer)
	require.Nil(t, err)
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
			ts += uint32(rand.Intn(100))
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

	clonedCompressor, clonedFinish, err = CloneCompressor(c, copyBytesBuffer)
	require.Nil(t, err)
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
