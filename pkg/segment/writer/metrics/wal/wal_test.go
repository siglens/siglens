package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWALAppendAndRead(t *testing.T) {
	filename := "testwal.wal"
	dirPath := t.TempDir()
	filePath := filepath.Join(dirPath, filename)

	wal, err := NewWAL(filePath)
	assert.NoError(t, err)
	defer wal.Close()

	numDatapoints := 500

	datapoints := generateRandomDatapoints(numDatapoints)

	err = wal.Append(datapoints)
	assert.NoError(t, err)

	it, err := NewWALReader(filePath)
	assert.NoError(t, err)
	defer it.Close()

	var readDatapoints []WalDatapoint
	for {
		dp, err := it.Next()
		assert.NoError(t, err)
		if dp == nil {
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
	filename := "testwal.wal"
	filePath := filepath.Join(dir, filename)
	wal, err := NewWAL(filePath)
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
	filename := "testwal.wal"

	filePath := filepath.Join(dir, filename)

	w, err := NewWAL(filePath)
	assert.NoError(t, err)
	defer w.Close()

	dps := generateRandomDatapoints(500)
	err = w.Append(dps)
	assert.NoError(t, err)

	fname, totalDps, encodedSize := w.GetWALStats()
	assert.Equal(t, fname, filepath.Join(dir, filename))
	assert.Equal(t, uint32(500), totalDps)
	assert.True(t, encodedSize > 0)
}

func generateRandomDatapoints(n int) []WalDatapoint {
	var dps []WalDatapoint
	currentMillis := time.Now().UnixMilli()
	for i := 0; i < n; i++ {
		dp := WalDatapoint{
			Timestamp: uint32(currentMillis + int64(i*1000)),
			DpVal:     float64(i + 10),
			Tsid:      uint64(i + 1),
		}
		dps = append(dps, dp)
	}
	return dps
}

func TestWALAppendAndRead_MultipleAppends(t *testing.T) {
	filename := "testwal.wal"
	dirPath := t.TempDir()
	filePath := filepath.Join(dirPath, filename)

	wal, err := NewWAL(filePath)
	assert.NoError(t, err)
	defer wal.Close()

	numDatapoints1 := 500
	datapoints1 := generateRandomDatapoints(numDatapoints1)

	err = wal.Append(datapoints1)
	assert.NoError(t, err)

	it, err := NewWALReader(filePath)
	assert.NoError(t, err)
	defer it.Close()

	var readDatapoints1 []WalDatapoint
	for {
		dp, err := it.Next()
		assert.NoError(t, err)
		if dp == nil {
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

	err = wal.Append(datapoints2)
	assert.NoError(t, err)

	it2, err := NewWALReader(filePath)
	assert.NoError(t, err)
	defer it2.Close()

	var totalReadDatapoints []WalDatapoint
	for {
		dp, err := it2.Next()
		assert.NoError(t, err)
		if dp == nil {
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

func TestMNameWALMultipleAppendsAndRead(t *testing.T) {
	filename := "testwal.wal"
	dirPath := t.TempDir()
	filePath := filepath.Join(dirPath, filename)

	wal, err := NewWAL(filePath)
	assert.NoError(t, err)

	totalAppends := 5
	metricsPerAppend := 50000
	expectedMetrics := []string{}

	for i := 0; i < totalAppends; i++ {
		metrics := generateMetricNames(metricsPerAppend)
		expectedMetrics = append(expectedMetrics, metrics...)
		err = wal.AppendMName(metrics)
		assert.NoError(t, err)
	}

	assert.NoError(t, wal.fd.Close())

	reader, err := NewMNameWalReader(filePath)
	assert.NoError(t, err)

	readMetrics := []string{}
	for {
		metric, err := reader.Next()
		if err != nil {
			break
		}
		if metric == nil {
			break
		}
		readMetrics = append(readMetrics, *metric)
	}

	assert.ElementsMatch(t, expectedMetrics, readMetrics)
}

func generateMetricNames(n int) []string {
	metrics := make([]string, n)
	for i := 0; i < n; i++ {
		metrics[i] = fmt.Sprintf("metric_%d", i+1)
	}
	return metrics
}
