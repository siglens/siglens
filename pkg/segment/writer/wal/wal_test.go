package wal

import (
	"os"
	"testing"

	"github.com/siglens/siglens/pkg/segment/writer/metrics"

	"github.com/siglens/siglens/pkg/config"
	"github.com/stretchr/testify/assert"
)

func init() {
	config.SetDataPath(os.TempDir() + "/test-wal/")
	runningConfig := config.GetTestConfig(os.TempDir())
	config.SetConfig(runningConfig)
}

func cleanupTestDir(dir string) {
	_ = os.RemoveAll(dir)

}

func TestCreateWalAndFlushRead(t *testing.T) {
	shardID := "1"
	segID := "100"
	blockID := "blk1"
	index := uint64(0)

	manager, err := CreateWal(shardID, segID, blockID, index)
	assert.NoError(t, err)
	assert.NotNil(t, manager)

	tsTimeSeriesInput := timeSeriesMackData()

	var walData = &WalData{
		TsidLookup: make(map[uint64]int),
		AllSeries:  []*metrics.TimeSeries{},
	}
	var ts *metrics.TimeSeries
	var tsExists bool
	for tsid, dataPoints := range tsTimeSeriesInput {
		for _, dp := range dataPoints {
			ts, tsExists = getTimeSeries(tsid, walData)
			if !tsExists {
				ts, _, _ = metrics.InitTimeSeries(tsid, dp.Value, dp.Timestamp)
				insertTimeSeries(tsid, ts, walData)
			} else {
				_, _ = ts.Compressor.Compress(uint32(dp.Timestamp), float64(dp.Value))
				ts.NEntries++
				ts.LastKnownTS = uint32(dp.Value)

			}
		}
	}

	fulshWal(walData)
	tsWalFileOutput := manager.ReadWal()
	assert.Equal(t, tsTimeSeriesInput, tsWalFileOutput)
	cleanupTestDir(config.GetDataPath())
}

func timeSeriesMackData() map[uint64][]DataPoint {
	tsTimeSeriesInput := make(map[uint64][]DataPoint)
	tsTimeSeriesInput[1001] = []DataPoint{
		{Timestamp: 161000, Value: 12},
		{Timestamp: 161000, Value: 23.50},
		{Timestamp: 161000, Value: 34},
	}
	tsTimeSeriesInput[1002] = []DataPoint{
		{Timestamp: 16100010, Value: 45.67},
		{Timestamp: 16100010, Value: 56.78},
		{Timestamp: 16100010, Value: 67.89},
	}
	tsTimeSeriesInput[1003] = []DataPoint{
		{Timestamp: 16100020, Value: 78.90},
		{Timestamp: 16100020, Value: 89.01},
		{Timestamp: 16100020, Value: 90.12},
	}
	return tsTimeSeriesInput
}

type WalData struct {
	TsidLookup map[uint64]int
	AllSeries  []*metrics.TimeSeries
}

func fulshWal(tsrd *WalData) {
	var Tsids []uint64
	Compressed := make(map[uint64][]byte)
	for tsid, index := range tsrd.TsidLookup {
		Tsids = append(Tsids, tsid)
		_ = tsrd.AllSeries[index].CFinishFn()
		Compressed[tsid] = tsrd.AllSeries[index].RawEncoding.Bytes()
	}
	shardID := "1"
	segID := "100"
	blockID := "blk1"
	index := uint64(0)

	manager, _ := CreateWal(shardID, segID, blockID, index)
	block := TimeSeriesBlock{
		Tsids:      Tsids,
		Compressed: Compressed,
	}
	manager.FlushWal(block)
}

func insertTimeSeries(tsid uint64, ts *metrics.TimeSeries, wd *WalData) {
	_, ok := wd.TsidLookup[tsid]
	if !ok {
		wd.TsidLookup[tsid] = len(wd.AllSeries)
		wd.AllSeries = append(wd.AllSeries, ts)
	}
}

func getTimeSeries(tsid uint64, wd *WalData) (*metrics.TimeSeries, bool) {
	var ts *metrics.TimeSeries
	idx, ok := wd.TsidLookup[tsid]
	if !ok {
		return ts, false
	}
	ts = wd.AllSeries[idx]
	return ts, true
}

func TestRotateWalFile(t *testing.T) {
	shardID := "2"
	segID := "200"
	blockID := "blk2"
	index := uint64(0)

	manager, err := CreateWal(shardID, segID, blockID, index)
	assert.NoError(t, err)
	manager.fileSize = manager.maxFileSize
	err = manager.rotateFile()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), manager.index)

	cleanupTestDir(config.GetDataPath())
}

func TestCollectAndDeleteWalFiles(t *testing.T) {
	shardID := "3"
	segID := "300"
	blockID := "blk3"
	index := uint64(0)
	manager, err := CreateWal(shardID, segID, blockID, index)
	assert.NoError(t, err)
	manager.file.WriteString("dummy data")

	walFiles := CollectWALFiles(manager.dirPath)
	assert.Greater(t, len(walFiles), 0)

	err = DeleteWALFiles(walFiles)
	assert.NoError(t, err)
	for _, f := range walFiles {
		_, err := os.Stat(f)
		assert.True(t, os.IsNotExist(err))
	}
	cleanupTestDir(config.GetDataPath())
}
