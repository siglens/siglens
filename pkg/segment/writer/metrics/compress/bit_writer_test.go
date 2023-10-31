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
	"encoding/binary"
	"testing"

	fuzz "github.com/google/gofuzz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_bitWriter_writeBit(t *testing.T) {
	tests := []struct {
		name   string
		binary string
		hex    uint8
	}{
		{
			name:   "write 1",
			binary: "00000001",
			hex:    0x1,
		},
		{
			name:   "write 8",
			binary: "00001000",
			hex:    0x8,
		},
		{
			name:   "write 113",
			binary: "01110001",
			hex:    0x71,
		},
		{
			name:   "write 255",
			binary: "11111111",
			hex:    0xff,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			bw := newBitWriter(buf)
			for i := 0; i < len(tt.binary); i++ {
				var err error
				if tt.binary[i] == '1' {
					err = bw.writeBit(one)
				} else {
					err = bw.writeBit(zero)
				}
				require.Nil(t, err)
			}
			assert.Equal(t, tt.hex, buf.Bytes()[0])
		})
	}
}

func Test_bitWriter_writeBits(t *testing.T) {
	f := fuzz.New().NilChance(0)

	for i := 0; i < 10; i++ {
		var u64 uint64
		f.Fuzz(&u64)

		buf := new(bytes.Buffer)
		bw := newBitWriter(buf)
		require.Nil(t, bw.writeBits(u64, 64))

		wantBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(wantBytes, u64)

		assert.Equal(t, wantBytes, buf.Bytes())
	}
}

func Test_bitWriter_writeByte(t *testing.T) {
	var b byte = 0x1
	for i := 0; i < 256; i++ {
		buf := new(bytes.Buffer)
		require.Nil(t, buf.WriteByte(b))
		bw := newBitWriter(buf)
		require.Nil(t, bw.writeByte(b))
		assert.Equal(t, b, buf.Bytes()[0])
		b++
	}
}
