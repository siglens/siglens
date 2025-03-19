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

	var readDatapoints []WALDatapoint
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

func TestDeleteWAL(t *testing.T) {
	dir := t.TempDir()
	filename := "testwal3.wal"
	filePath := filepath.Join(dir, filename)

	f, err := os.Create(filePath)
	assert.NoError(t, err)
	f.Close()

	err = DeleteWAL(dir, filename)
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
	assert.Contains(t, fname, filename)
	assert.Equal(t, uint32(3), totalDps)
	assert.True(t, encodedSize > 0)
}

func generateRandomDatapoints(n int) []WALDatapoint {
	var dps []WALDatapoint
	currentMillis := time.Now().UnixMilli()
	for i := 0; i < n; i++ {
		dp := WALDatapoint{
			Timestamp: uint64(currentMillis + int64(i*1000)),
			DpVal:     float64(10 + i),
			Tsid:      uint64(i + 1),
		}
		dps = append(dps, dp)
	}
	return dps
}
