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

	const dataLen = 50000
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
	c, finish, err := NewCompressor(buf, header)
	require.Nil(t, err)
	for _, data := range expected {
		b, err := c.Compress(data.t, data.v)
		require.Nil(t, err)
		require.Greater(t, b, uint64(0))
	}
	require.Nil(t, finish())

	// Decompression
	var actual []data
	iter, err := NewDecompressIterator(buf)
	require.Nil(t, err)
	for iter.Next() {
		t, v := iter.At()
		actual = append(actual, data{t, v})
	}
	require.Nil(t, iter.Err())
	assert.Equal(t, expected, actual)
}
