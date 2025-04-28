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

func Test_ReadLine_Empty(t *testing.T) {
	buf := []byte{}

	line, rest := ReadLine(buf)
	assert.Len(t, line, 0)
	assert.Len(t, rest, 0)

	buf = nil
	line, rest = ReadLine(buf)
	assert.Len(t, line, 0)
	assert.Len(t, rest, 0)
}

func Test_ReadLine_NoNewline(t *testing.T) {
	buf := []byte("hello")

	line, rest := ReadLine(buf)
	assert.Equal(t, "hello", string(line))
	assert.Len(t, rest, 0)
}

func Test_ReadLine_MultipleLines(t *testing.T) {
	buf := []byte("hello\nworld\n")

	line, rest := ReadLine(buf)
	assert.Equal(t, "hello", string(line))
	assert.Equal(t, "world\n", string(rest))

	line, rest = ReadLine(rest)
	assert.Equal(t, "world", string(line))
	assert.Len(t, rest, 0)
}

func Test_ReadLine_BlankLines(t *testing.T) {
	buf := []byte("hello\n\nworld")

	line, rest := ReadLine(buf)
	assert.Equal(t, "hello", string(line))
	assert.Equal(t, "\nworld", string(rest))

	line, rest = ReadLine(rest)
	assert.Equal(t, "", string(line))
	assert.Equal(t, "world", string(rest))

	line, rest = ReadLine(rest)
	assert.Equal(t, "world", string(line))
	assert.Len(t, rest, 0)
}

func Test_ContainsAnyCase(t *testing.T) {
	assert.True(t, ContainsAnyCase([]byte("hello, world!"), []byte("h")))
	assert.True(t, ContainsAnyCase([]byte("hello, world!"), []byte("hello")))
	assert.True(t, ContainsAnyCase([]byte("hello, world!"), []byte("world")))
	assert.True(t, ContainsAnyCase([]byte("hello, world!"), []byte("lo, wo")))
	assert.True(t, ContainsAnyCase([]byte("hello, world!"), []byte("lO, Wo")))
	assert.False(t, ContainsAnyCase([]byte("hello, world!"), []byte("x")))
	assert.False(t, ContainsAnyCase([]byte("hello, world!"), []byte("hey")))
	assert.False(t, ContainsAnyCase([]byte("hello, world!"), []byte("HELLO,!")))
	assert.True(t, ContainsAnyCase([]byte("hello, world!"), []byte("HELLO, WORLD!")))
}
