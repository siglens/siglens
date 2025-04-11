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
	"sync"
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
	defer fd.Close()
	csf := ChecksumFile{Fd: fd}

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
		offset, err := csf.Fd.Seek(0, io.SeekEnd)
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
	defer fd.Close()
	csf := ChecksumFile{Fd: fd}

	actualData := make([]byte, len(data))
	n, err := csf.ReadAt(actualData, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, actualData)
}

func Test_checksumFile_PartialWrites(t *testing.T) {
	dir := t.TempDir()
	file1 := filepath.Join(dir, "test1")
	file2 := filepath.Join(dir, "test2")
	fd1, err := os.Create(file1)
	require.NoError(t, err)
	defer fd1.Close()
	fd2, err := os.Create(file2)
	require.NoError(t, err)
	defer fd2.Close()

	csf1 := &ChecksumFile{Fd: fd1}
	csf2 := &ChecksumFile{Fd: fd2}

	// Write the same data to both files (and chunked the the same way), but
	// using different methods.
	appendChunksNoError(t, csf1, [][]byte{[]byte("foo"), []byte("bar")})
	appendPartialChunksNoError(t, csf2, [][]byte{[]byte("f"), []byte("o"), []byte("o")})
	err = csf2.Flush()
	assert.NoError(t, err)
	appendPartialChunksNoError(t, csf2, [][]byte{[]byte("ba"), []byte("r")})
	err = csf2.Flush()
	assert.NoError(t, err)

	// The files should be identical.
	content1, err := os.ReadFile(file1)
	assert.NoError(t, err)
	content2, err := os.ReadFile(file2)
	assert.NoError(t, err)
	assert.Equal(t, content1, content2)
}

func Test_checksumFile_FlushFailsOnAppendOnly(t *testing.T) {
	dir := t.TempDir()
	fd, err := os.OpenFile(filepath.Join(dir, "test"), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	require.NoError(t, err)
	defer fd.Close()

	csf := &ChecksumFile{Fd: fd}
	err = csf.AppendPartialChunk([]byte("foo"))
	assert.NoError(t, err)
	err = csf.Flush()
	assert.Error(t, err) // The fd was opened with O_APPEND.
}

func Test_checksumFile_ConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	fd, err := os.Create(filepath.Join(dir, "test"))
	require.NoError(t, err)
	defer fd.Close()

	csf := &ChecksumFile{Fd: fd}
	allChunksData := [][]byte{[]byte("foo"), []byte("bar"), []byte("baz")}
	appendChunksNoError(t, csf, allChunksData)

	offsets := []int64{0 * 12, 1*12 + 3, 2*12 + 3 + 3} // 12 is the header size.
	waitGroup := sync.WaitGroup{}
	for i := range offsets {
		waitGroup.Add(1)
		go func(expectedData []byte, offset int64) {
			defer waitGroup.Done()

			for k := 0; k < 100; k++ {
				actualData := make([]byte, len(expectedData))
				n, err := csf.ReadAt(actualData, offset)
				assert.NoError(t, err)
				assert.Equal(t, len(expectedData), n)
				assert.Equal(t, expectedData, actualData)
			}
		}(allChunksData[i], offsets[i])
	}
	waitGroup.Wait()
}

func Test_checksumFile_ReadMultipleChunks(t *testing.T) {
	dir := t.TempDir()
	fd, err := os.Create(filepath.Join(dir, "test"))
	require.NoError(t, err)
	defer fd.Close()

	csf := &ChecksumFile{Fd: fd}
	allChunksData := [][]byte{[]byte("foo"), []byte("bar"), []byte("baz")}
	appendChunksNoError(t, csf, allChunksData)

	// Read the first two chunks.
	buf := make([]byte, 6)
	n, err := csf.ReadAt(buf, 0)
	assert.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, []byte("foobar"), buf)

	// Read the second and third chunks.
	buf = make([]byte, 6)
	n, err = csf.ReadAt(buf, 3+12) // 12 is the header size.
	assert.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.Equal(t, []byte("barbaz"), buf)

	// Read all the chunks.
	buf = make([]byte, 9)
	n, err = csf.ReadAt(buf, 0)
	assert.NoError(t, err)
	assert.Equal(t, 9, n)
	assert.Equal(t, []byte("foobarbaz"), buf)
}

func appendChunksNoError(t *testing.T, csf *ChecksumFile, data [][]byte) {
	t.Helper()
	for _, chunk := range data {
		err := csf.AppendChunk(chunk)
		assert.NoError(t, err)
	}
}

func appendPartialChunksNoError(t *testing.T, csf *ChecksumFile, data [][]byte) {
	t.Helper()
	for _, chunk := range data {
		err := csf.AppendPartialChunk(chunk)
		assert.NoError(t, err)
	}
}
