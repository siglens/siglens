package utils

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_OnFileChange(t *testing.T) {
	dir := t.TempDir()

	file1 := filepath.Join(dir, "file1")
	err := os.WriteFile(file1, []byte("file1 content"), 0644)
	require.NoError(t, err)

	file2 := filepath.Join(dir, "file2")
	err = os.WriteFile(file2, []byte("file2 content"), 0644)
	require.NoError(t, err)

	updateChan := make(chan struct{}, 10)
	err = OnFileChange([]string{file1, file2}, func() {
		updateChan <- struct{}{}
	})
	require.NoError(t, err)

	err = os.WriteFile(file1, []byte("new content"), 0644)
	require.NoError(t, err)
	readChannelOrFail(t, updateChan)

	// Ensure another change triggers it again
	err = os.WriteFile(file1, []byte("hello world"), 0644)
	require.NoError(t, err)
	readChannelOrFail(t, updateChan)

	// Write to the other file.
	err = os.WriteFile(file2, []byte("new content"), 0644)
	require.NoError(t, err)
	readChannelOrFail(t, updateChan)

	// There should be no more updates.
	assert.Equal(t, 0, len(updateChan), "unexpected updates")
}

func readChannelOrFail(t *testing.T, updateChan chan struct{}) {
	t.Helper()

	select {
	case <-updateChan:
		// Do nothing.
	case <-time.After(time.Second):
		t.Fatal("reached timeout")
	}
}
