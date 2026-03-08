// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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

func Test_equalAsciiAnyCase(t *testing.T) {
	assert.True(t, equalAsciiAnyCase('a', 'a'))
	assert.True(t, equalAsciiAnyCase('A', 'A'))
	assert.True(t, equalAsciiAnyCase('a', 'A'))
	assert.True(t, equalAsciiAnyCase('A', 'a'))
	assert.False(t, equalAsciiAnyCase('*', '*'+32))
	assert.False(t, equalAsciiAnyCase('*', '*'-32))
	assert.False(t, equalAsciiAnyCase('a', 'b'))
}
