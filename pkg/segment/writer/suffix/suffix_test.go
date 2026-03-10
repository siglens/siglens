// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package suffix

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_getNextSuffix(t *testing.T) {
	dir := t.TempDir()
	fileName := filepath.Join(dir, "suffix.json")

	// Test non-existent file.
	suffix, err := getAndIncrementSuffixFromFile(fileName, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), suffix)

	// Now the file exists.
	suffix, err = getAndIncrementSuffixFromFile(fileName, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), suffix)
}

func Test_getNextSuffix_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	fileName := filepath.Join(dir, "suffix.json")
	fd, err := os.Create(fileName)
	assert.NoError(t, err)
	fd.Close()

	suffix, err := getAndIncrementSuffixFromFile(fileName, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), suffix)

	suffix, err = getAndIncrementSuffixFromFile(fileName, nil)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), suffix)
}
