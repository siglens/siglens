// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_AtomicWriteFile_Truncate(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "testfile.txt")

	err := AtomicWriteFile(filePath, []byte("apple"), Truncate)
	assert.NoError(t, err)

	got, err := os.ReadFile(filePath)
	assert.NoError(t, err)
	assert.Equal(t, []byte("apple"), got)

	err = AtomicWriteFile(filePath, []byte("banana"), Truncate)
	assert.NoError(t, err)

	got, err = os.ReadFile(filePath)
	assert.NoError(t, err)
	assert.Equal(t, []byte("banana"), got)
}

func Test_AtomicWriteFile_Append(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "testfile.txt")

	err := AtomicWriteFile(filePath, []byte("apple"), Append)
	assert.NoError(t, err)

	got, err := os.ReadFile(filePath)
	assert.NoError(t, err)
	assert.Equal(t, []byte("apple"), got)

	err = AtomicWriteFile(filePath, []byte("banana"), Append)
	assert.NoError(t, err)

	got, err = os.ReadFile(filePath)
	assert.NoError(t, err)
	assert.Equal(t, []byte("applebanana"), got)
}

func Test_AtomicWriteFile_InvalidMode(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "testfile.txt")

	err := os.WriteFile(filePath, []byte("previous data"), 0644)
	assert.NoError(t, err)

	err = AtomicWriteFile(filePath, []byte("apple"), WriteMode(42))
	assert.Error(t, err)

	got, err := os.ReadFile(filePath)
	assert.NoError(t, err)
	assert.Equal(t, []byte("previous data"), got)
}
