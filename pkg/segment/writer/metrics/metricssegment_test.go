// Copyright (c) 2021-2024 SigScalr, Inc.
//
// This file is part of SigLens Observability Solution
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

package metrics

import (
	"bytes"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	fuzz "github.com/google/gofuzz"
	"github.com/siglens/siglens/pkg/segment/reader/metrics/series"
	"github.com/siglens/siglens/pkg/segment/reader/microreader"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer/metrics/compress"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func writeMockMetricsBlockSummaryFile(file string, blockSums []*structs.MBlockSummary) error {
	fd, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("WriteMockMetricsBlockSummary: open failed blockSummaryFname=%v, err=%v", file, err)
		return err
	}

	defer fd.Close()

	if _, err := fd.Write(utils.VERSION_MBLOCKSUMMARY); err != nil {
		log.Errorf("writeMockMetricsBlockSummaryFile: Cannot write version byte for filename=%v: err= %v", file, err)
		return err
	}

	for _, block := range blockSums {
		mbs := structs.MBlockSummary{
			HighTs: block.HighTs,
			LowTs:  block.LowTs,
			Blknum: block.Blknum}
		err = mbs.FlushSummary(file)
		if err != nil {
			log.Errorf("WriteMockMetricsBlockSummary: Failed to write in file at %v, err: %v", file, err)
			return err
		}
	}
	err = fd.Sync()
	if err != nil {
		log.Errorf("WriteMockMetricsBlockSummary: Failed to sync file at %v, err: %v", file, err)
		return err
	}
	return nil
}

func Test_ReadMetricsBlockSummary(t *testing.T) {

	entryCount := 10
	dir := "data/"
	err := os.MkdirAll(dir, os.FileMode(0755))

	if err != nil {
		log.Fatal(err)
	}
	blockSumFile := dir + "query_test.mbsu"
	blockSummaries := make([]*structs.MBlockSummary, entryCount)
	i := 0
	for i < entryCount {
		blockSummaries[i] = &structs.MBlockSummary{
			Blknum: uint16(i),
			HighTs: 1676089340,
			LowTs:  1676089310,
		}
		i++
	}
	err = writeMockMetricsBlockSummaryFile(blockSumFile, blockSummaries)

	if err != nil {
		log.Errorf("Failed to write mock block summary at %v", blockSumFile)
	}

	blockSums, err := microreader.ReadMetricsBlockSummaries(blockSumFile)
	if err != nil {
		os.RemoveAll(dir)
		log.Errorf("Failed to read mock block summary at %v", blockSumFile)
	}

	for i := 0; i < len(blockSums); i++ {
		assert.Equal(t, blockSums[i].HighTs, blockSummaries[i].HighTs)
		assert.Equal(t, blockSums[i].LowTs, blockSummaries[i].LowTs)
		assert.Equal(t, blockSums[i].Blknum, blockSums[i].Blknum)
	}
	os.RemoveAll(dir)
}

func generateRandomTsid() uint64 {
	return uint64(rand.Uint32())<<32 + uint64(rand.Uint32())
}

type data struct {
	t uint32
	v float64
}

func generateFakeTimeSeries() ([]data, uint32) {
	header := uint32(time.Now().Unix())

	const dataLen = 5000
	series := make([]data, dataLen)
	valueFuzz := fuzz.New().NilChance(0)
	ts := header
	for i := 0; i < dataLen; i++ {
		if 0 < i && i%10 == 0 {
			ts -= uint32(rand.Intn(100))
		} else {
			ts += uint32(rand.Int31n(100))
		}
		var v float64
		valueFuzz.Fuzz(&v)
		series[i] = data{ts, v}
	}
	return series, header
}

func Test_ReadWriteTsoTsgFiles(t *testing.T) {

	dir := "data/"
	err := os.MkdirAll(dir, os.FileMode(0755))
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	tsid_1 := generateRandomTsid()
	tsid_2 := generateRandomTsid()

	mb := initFakeMetricsBlock()

	writeToTsidLookup(mb, 0, tsid_1)
	writeToTsidLookup(mb, 1, tsid_2)

	writeSortedTsids(mb, tsid_1, tsid_2)

	series_1 := writeToTimeSeries(mb, 0)
	series_2 := writeToTimeSeries(mb, 1)

	err = mb.FlushTSOAndTSGFiles("data/mock_0")
	assert.NoError(t, err)

	tssr, err := series.InitTimeSeriesReader("data/mock")
	assert.NoError(t, err)

	queryMetrics := &structs.MetricsQueryProcessingMetrics{
		UpdateLock: &sync.Mutex{},
	}
	tssr_block, err := tssr.InitReaderForBlock(uint16(0), queryMetrics)
	assert.NoError(t, err)

	// verify series 1
	ts_itr, exists, err := tssr_block.GetTimeSeriesIterator(tsid_1)
	assert.NoError(t, err)
	assert.True(t, exists)

	count_1 := 0
	for ts_itr.Next() {
		ts, val := ts_itr.At()
		assert.Equal(t, series_1[count_1].t, ts)
		assert.Equal(t, series_1[count_1].v, val)
		count_1++
	}

	// verify series 2
	ts_itr, exists, err = tssr_block.GetTimeSeriesIterator(tsid_2)
	assert.NoError(t, err)

	assert.True(t, exists)
	count_2 := 0
	for ts_itr.Next() {
		ts, val := ts_itr.At()
		assert.Equal(t, series_2[count_2].t, ts)
		assert.Equal(t, series_2[count_2].v, val)
		count_2++
	}

	assert.Equal(t, count_1, 5000)
	assert.Equal(t, count_2, 5000)
}

func Test_ReadOldTso(t *testing.T) {
	dir := "data/"
	err := os.MkdirAll(dir, os.FileMode(0755))
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// A version 1 TSO file
	tsoData := make([]byte, 1+2+12)
	tsoData[0] = utils.VERSION_TSOFILE_V1[0]
	copy(tsoData[1:], toputils.Uint16ToBytesLittleEndian(uint16(1)))
	tsid := uint64(42)
	copy(tsoData[3:], toputils.Uint64ToBytesLittleEndian(tsid))
	offset := uint32(0)
	copy(tsoData[11:], toputils.Uint32ToBytesLittleEndian(offset))
	err = os.WriteFile("data/mock_0.tso", tsoData, 0644)
	assert.NoError(t, err)

	// This test is for a TSO file, but we need to make a fake TSG file as well
	// because the reader will try to read it.
	//
	// Here, the first 13 bytes are important, but the last part can be
	// garbage, so we'll keep it set to 0. I'm not certain, but I think the
	// only requirement is that it's at least 32 bytes (for the gorilla
	// encoding header).
	tsgData := make([]byte, 1+12+100)
	copy(tsgData[0:], utils.VERSION_TSGFILE)
	copy(tsgData[1:], toputils.Uint64ToBytesLittleEndian(tsid))
	copy(tsgData[9:], toputils.Uint32ToBytesLittleEndian(uint32(100)))
	err = os.WriteFile("data/mock_0.tsg", tsgData, 0644)
	assert.NoError(t, err)

	tssr, err := series.InitTimeSeriesReader("data/mock")
	assert.NoError(t, err)

	queryMetrics := &structs.MetricsQueryProcessingMetrics{
		UpdateLock: &sync.Mutex{},
	}
	tssr_block, err := tssr.InitReaderForBlock(uint16(0), queryMetrics)
	assert.NoError(t, err)

	_, exists, err := tssr_block.GetTimeSeriesIterator(tsid)
	assert.NoError(t, err)
	assert.True(t, exists)

	badTsid := tsid + 1
	_, exists, err = tssr_block.GetTimeSeriesIterator(badTsid)
	assert.NoError(t, err)
	assert.False(t, exists)
}

func initFakeMetricsBlock() *MetricsBlock {
	mb := &MetricsBlock{
		tsidLookup:  make(map[uint64]int),
		sortedTsids: make([]uint64, 2),
		allSeries:   make([]*TimeSeries, 2),
		mBlockSummary: &structs.MBlockSummary{
			// garbage values
			Blknum: 0,
			HighTs: 1676089340,
			LowTs:  1676089310,
		},
		blkEncodedSize: 0,
	}
	return mb
}

func writeSortedTsids(mb *MetricsBlock, tsid_1, tsid_2 uint64) {
	mb.sortedTsids[0] = tsid_2
	mb.sortedTsids[1] = tsid_1
	if tsid_1 < tsid_2 {
		mb.sortedTsids[0] = tsid_1
		mb.sortedTsids[1] = tsid_2
	}
}

func writeToTsidLookup(mb *MetricsBlock, i int, tsid uint64) {
	mb.tsidLookup[tsid] = i
}

func writeToTimeSeries(mb *MetricsBlock, index int) []data {
	series, header := generateFakeTimeSeries()
	buf := new(bytes.Buffer)
	c, finish, err := compress.NewCompressor(buf, header)
	if err != nil {
		log.Error("writeToTimeSeries: Error writing mock metrics time series")
	}
	mb.allSeries[index] = &TimeSeries{
		lock:        &sync.Mutex{},
		rawEncoding: buf,
		cFinishFn:   finish,
		compressor:  c,
	}
	for _, data := range series {
		_, err := mb.allSeries[index].compressor.Compress(data.t, data.v)
		if err != nil {
			log.Error("writeToTimeSeries: Error writing mock metrics time series")
		}
	}
	return series
}
