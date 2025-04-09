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

	t.Run("UsingChecksums", func(t *testing.T) {
		csf, err := NewChecksumFile(fileName)
		require.NoError(t, err)
		defer csf.Close()

		err = csf.AppendChunk(data)
		require.NoError(t, err)

		actualData := make([]byte, len(data))
		n, err := csf.ReadAt(actualData, 0)
		assert.NoError(t, err)
		assert.Equal(t, len(data), n)
		assert.Equal(t, data, actualData)

		rawData, err := os.ReadFile(fileName)
		require.NoError(t, err)
		t.Logf("rawData: % 02x", rawData)
	})
}
