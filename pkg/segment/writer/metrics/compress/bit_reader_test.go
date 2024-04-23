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
