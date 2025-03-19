package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/stretchr/testify/assert"
)

func init() {
	config.SetDataPath(os.TempDir() + "/test-wal/")
	runningConfig := config.GetTestConfig(os.TempDir())
	config.SetConfig(runningConfig)
}

func cleanUpWalFile() {
	os.RemoveAll(config.GetDataPath())
}

func TestWALAppendAndRead(t *testing.T) {
	filename := "testwal"

	cleanUpWalFile()
	defer cleanUpWalFile()

	wal, err := NewWAL(filename)
	assert.NoError(t, err)
	defer wal.Close()

	datapoints := []WALDatapoint{
		{Timestamp: 1001, DpVal: 12.5, Tsid: 1},
		{Timestamp: 1002, DpVal: 13.7, Tsid: 2},
		{Timestamp: 1003, DpVal: 15.1, Tsid: 3},
	}

	err = wal.AppendToWAL(datapoints)
	assert.NoError(t, err)

	it, err := NewReaderWAL(filename)
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

	for i, dp := range datapoints {
		assert.Equal(t, dp.Timestamp, readDatapoints[i].Timestamp)
		assert.Equal(t, dp.DpVal, readDatapoints[i].DpVal)
		assert.Equal(t, dp.Tsid, readDatapoints[i].Tsid)
	}
}

func GetWalFilePath(filename string) (string, error) {
	dirPath, err := getBaseWalDir()
	filePath := filepath.Join(dirPath, filename+".wal")
	return filePath, err
}

func TestNewWAL(t *testing.T) {
	filename := "test_newwal"
	defer cleanUpWalFile()

	wal, err := NewWAL(filename)
	assert.NoError(t, err)
	assert.NotNil(t, wal)
	defer wal.Close()

	filePath, _ := GetWalFilePath(filename)
	_, err = os.Stat(filePath)
	assert.NoError(t, err)
}

func TestDeleteWAL(t *testing.T) {
	filename := "test_deletewal"

	wal, err := NewWAL(filename)
	assert.NoError(t, err)
	wal.Close()

	err = DeleteWAL(filename)
	assert.NoError(t, err)

	filePath, _ := GetWalFilePath(filename)
	_, err = os.Stat(filePath)
	assert.True(t, os.IsNotExist(err))
}

func TestAppendToWAL(t *testing.T) {
	filename := "test_appendwal"
	defer cleanUpWalFile()

	wal, err := NewWAL(filename)
	assert.NoError(t, err)
	defer wal.Close()

	dps := []WALDatapoint{
		{Timestamp: 1010, DpVal: 20.0, Tsid: 10},
	}

	err = wal.AppendToWAL(dps)
	assert.NoError(t, err)

	filePath, _ := GetWalFilePath(filename)
	info, err := os.Stat(filePath)
	assert.NoError(t, err)
	assert.Greater(t, info.Size(), int64(0))
}
