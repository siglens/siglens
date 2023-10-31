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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_bitReader_readBit(t *testing.T) {
	var b byte = 0x1
	for i := 0; i < 256; i++ {
		buf := new(bytes.Buffer)
		require.Nil(t, buf.WriteByte(b))
		br := newBitReader(buf)
		actual, err := br.readBit()
		require.Nil(t, err)
		assert.Equal(t, bit((b&0x80) != 0), actual)
		b++
	}
}

func Test_bitReader_readBits(t *testing.T) {
	tests := []struct {
		name       string
		nbits      int
		byteToRead byte
		want       uint64
		wantErr    error
	}{
		{
			name:       "read a bit from 00000001",
			nbits:      1,
			byteToRead: 0x1,
			want:       0,
			wantErr:    nil,
		},
		{
			name:       "read 5 bits from 00000001",
			nbits:      5,
			byteToRead: 0x1,
			want:       0,
			wantErr:    nil,
		},
		{
			name:       "read 8 bits from 00000001",
			nbits:      8,
			byteToRead: 0x1,
			want:       0x1,
			wantErr:    nil,
		},
		{
			name:       "read a bit from 11111111",
			nbits:      1,
			byteToRead: 0xff,
			want:       0x1,
			wantErr:    nil,
		},
		{
			name:       "read 5 bits from 11111111",
			nbits:      5,
			byteToRead: 0xff,
			want:       0x1f,
			wantErr:    nil,
		},
		{
			name:       "read 8 bits from 11111111",
			nbits:      8,
			byteToRead: 0xff,
			want:       0xff,
			wantErr:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			err := buf.WriteByte(tt.byteToRead)
			require.Nil(t, err)
			b := newBitReader(buf)
			got, err := b.readBits(tt.nbits)
			assert.Equal(t, tt.wantErr, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func Test_bitReader_readByte(t *testing.T) {
	var b byte = 0x1
	for i := 0; i < 256; i++ {
		buf := new(bytes.Buffer)
		require.Nil(t, buf.WriteByte(b))
		br := newBitReader(buf)
		byt, err := br.readByte()
		require.Nil(t, err)
		assert.Equal(t, b, byt)
		b++
	}
}
