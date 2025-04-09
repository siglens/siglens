// Copyright (c) 2021-2025 SigScalr, Inc.
//
// # This file is part of SigLens Observability Solution
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
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_checksumFile_ReadAndWrite(t *testing.T) {
	dir := t.TempDir()
	fileName := filepath.Join(dir, "test")
	data := []byte("Hello, world!")

	fd, err := os.Create(fileName)
	require.NoError(t, err)
	csf, err := NewChecksumFile(fd)
	require.NoError(t, err)
	defer csf.Close()

	err = csf.AppendChunk(data)
	require.NoError(t, err)

	t.Run("ReadFirstChunk", func(t *testing.T) {
		actualData := make([]byte, len(data))
		n, err := csf.ReadAt(actualData, 0)
		assert.NoError(t, err)
		assert.Equal(t, len(data), n)
		assert.Equal(t, data, actualData)
	})

	t.Run("ReadFromInvalidOffset", func(t *testing.T) {
		// Test reading from an offset that's not the start of a chunk.
		actualData := make([]byte, len(data))
		n, err := csf.ReadAt(actualData, 1)
		assert.Error(t, err)
		assert.NotEqual(t, io.EOF, err)
		assert.Equal(t, 0, n)
	})

	t.Run("ReadSecondChunk", func(t *testing.T) {
		// Write another chunk.
		data2 := []byte("Goodbye!")
		offset, err := csf.fd.Seek(0, io.SeekEnd)
		require.NoError(t, err)

		err = csf.AppendChunk(data2)
		require.NoError(t, err)

		actualData := make([]byte, len(data2))
		n, err := csf.ReadAt(actualData, offset)
		assert.NoError(t, err)
		assert.Equal(t, len(data2), n)
		assert.Equal(t, data2, actualData)
	})
}

func Test_checksumFile_BackwardCompatibility(t *testing.T) {
	dir := t.TempDir()
	fileName := filepath.Join(dir, "test")
	data := []byte("Hello, world!")

	// Write directly to the file.
	err := os.WriteFile(fileName, data, 0644)
	require.NoError(t, err)

	fd, err := os.OpenFile(fileName, os.O_RDWR, 0644)
	require.NoError(t, err)
	csf, err := NewChecksumFile(fd)
	require.NoError(t, err)
	defer csf.Close()

	actualData := make([]byte, len(data))
	n, err := csf.ReadAt(actualData, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, actualData)
}
