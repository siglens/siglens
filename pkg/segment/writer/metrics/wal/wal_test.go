package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWALAppendAndRead(t *testing.T) {
	filename := "testwal.wal"
	dirPath := t.TempDir()

	wal, err := NewWAL(dirPath, filename)
	assert.NoError(t, err)
	defer wal.Close()

	numDatapoints := 500

	datapoints := generateRandomDatapoints(numDatapoints)

	err = wal.AppendToWAL(datapoints)
	assert.NoError(t, err)

	it, err := NewReaderWAL(dirPath, filename)
	assert.NoError(t, err)
	defer it.Close()

	var readDatapoints []walDatapoint
	for {
		dp, ok, err := it.Next()
		assert.NoError(t, err)
		if !ok {
			break
		}
		readDatapoints = append(readDatapoints, *dp)
	}

	assert.Equal(t, len(datapoints), len(readDatapoints))

	for i := 0; i < numDatapoints; i++ {
		assert.Equal(t, datapoints[i], readDatapoints[i])
	}
}

func TestDeleteWALFile(t *testing.T) {
	dir := t.TempDir()
	filename := "deletewaltest.wal"
	filePath := filepath.Join(dir, filename)
	wal, err := NewWAL(dir, filename)
	assert.NoError(t, err)
	_, err = os.Stat(filePath)
	assert.False(t, os.IsNotExist(err))
	err = wal.Close()
	assert.NoError(t, err)

	err = wal.DeleteWAL()
	assert.NoError(t, err)

	_, err = os.Stat(filePath)
	assert.True(t, os.IsNotExist(err))
}

func TestWALStats(t *testing.T) {
	dir := t.TempDir()
	filename := "testwal4.wal"

	w, err := NewWAL(dir, filename)
	assert.NoError(t, err)
	defer w.Close()

	dps := generateRandomDatapoints(500)
	err = w.AppendToWAL(dps)
	assert.NoError(t, err)

	fname, totalDps, encodedSize := w.GetWALStats()
	assert.Contains(t, fname, filepath.Join(dir, filename))
	assert.Equal(t, uint32(500), totalDps)
	assert.True(t, encodedSize > 0)
}

func generateRandomDatapoints(n int) []walDatapoint {
	var dps []walDatapoint
	currentMillis := time.Now().UnixMilli()
	for i := 0; i < n; i++ {
		dp := walDatapoint{
			timestamp: uint64(currentMillis + int64(i*1000)),
			dpVal:     float64(10 + i),
			tsid:      uint64(i + 1),
		}
		dps = append(dps, dp)
	}
	return dps
}

func TestWALAppendAndRead_MultipleAppends(t *testing.T) {
	filename := "testwal.wal"
	dirPath := t.TempDir()

	wal, err := NewWAL(dirPath, filename)
	assert.NoError(t, err)
	defer wal.Close()

	numDatapoints1 := 500
	datapoints1 := generateRandomDatapoints(numDatapoints1)

	err = wal.AppendToWAL(datapoints1)
	assert.NoError(t, err)

	it, err := NewReaderWAL(dirPath, filename)
	assert.NoError(t, err)
	defer it.Close()

	var readDatapoints1 []walDatapoint
	for {
		dp, ok, err := it.Next()
		assert.NoError(t, err)
		if !ok {
			break
		}
		readDatapoints1 = append(readDatapoints1, *dp)
	}

	assert.Equal(t, len(datapoints1), len(readDatapoints1))
	for i := 0; i < numDatapoints1; i++ {
		assert.Equal(t, datapoints1[i], readDatapoints1[i])
	}

	numDatapoints2 := 1000
	datapoints2 := generateRandomDatapoints(numDatapoints2)

	err = wal.AppendToWAL(datapoints2)
	assert.NoError(t, err)

	it2, err := NewReaderWAL(dirPath, filename)
	assert.NoError(t, err)
	defer it2.Close()

	var totalReadDatapoints []walDatapoint
	for {
		dp, ok, err := it2.Next()
		assert.NoError(t, err)
		if !ok {
			break
		}
		totalReadDatapoints = append(totalReadDatapoints, *dp)
	}

	expectedDatapoints := append(datapoints1, datapoints2...)
	assert.Equal(t, len(expectedDatapoints), len(totalReadDatapoints))

	for i := 0; i < len(expectedDatapoints); i++ {
		assert.Equal(t, expectedDatapoints[i], totalReadDatapoints[i])
	}
}
