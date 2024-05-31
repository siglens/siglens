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

package series

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer/metrics/compress"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

/*
Holder struct to read a single time series segment

Exposes function to access readers for each block
*/
type TimeSeriesSegmentReader struct {
	mKey   string // base metrics key directory
	tsoBuf []byte // raw buffer used to decode the TSO
	tsgBuf []byte // raw buffer used to decode the TSO
}

/*
Struct to access data within a single block.

Exposes functions that will return a TimeSeriesIterator for the given tsids
*/
type TimeSeriesBlockReader struct {
	rawTSO   []byte // raw read TSO file
	rawTSG   []byte // raw read TSG file
	numTSIDs uint16

	lastTSID  uint64
	lastTSidx uint32 // index of the last tsid in the tso file
	first     bool
}

type SharedTimeSeriesSegmentReader struct {
	TimeSeriesSegmentReadersList []*TimeSeriesSegmentReader
	numReaders                   int
	rwLock                       *sync.Mutex
}

type customPool struct {
	items []poolItem
	mutex sync.Mutex
}

type poolItem struct {
	buf   []byte
	inUse bool
	ptr   unsafe.Pointer
}

var globalPool = customPool{}

const numPoolItems = 4

func init() {
	globalPool.items = make([]poolItem, 0)
	globalPool.mutex = sync.Mutex{}

	globalPool.mutex.Lock()
	defer globalPool.mutex.Unlock()

	for i := 0; i < numPoolItems; i++ {
		buf := make([]byte, segutils.METRICS_SEARCH_ALLOCATE_BLOCK)
		item := poolItem{
			buf:   buf[:0],
			inUse: false,
			ptr:   unsafe.Pointer(&buf[0]),
		}

		globalPool.items = append(globalPool.items, item)
	}
}

func (cp *customPool) expandItemToMinSize(i int, minSize uint64) {
	if cap(cp.items[i].buf) < int(minSize) {
		cp.items[i].buf = make([]byte, 1, minSize)
		cp.items[i].ptr = unsafe.Pointer(&cp.items[i].buf[0])
		cp.items[i].buf = cp.items[i].buf[:0]
	}
}

func (cp *customPool) Get(minSize uint64) ([]byte, error) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	for i := range cp.items {
		if !cp.items[i].inUse {
			cp.expandItemToMinSize(i, minSize)
			cp.items[i].inUse = true

			return cp.items[i].buf, nil
		}
	}

	return nil, fmt.Errorf("No more buffers available in the pool")
}

func (cp *customPool) Put(buf []byte) error {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	if len(buf) == 0 {
		buf = buf[:1]
	}

	bufPtr := unsafe.Pointer(&buf[0])
	for i := range cp.items {
		if cp.items[i].ptr == bufPtr {
			cp.items[i].inUse = false
			return nil
		}
	}

	// We should not get here. The returned buffer is not in the pool.
	allBufferPointers := make([]string, 0)
	for i := range cp.items {
		allBufferPointers = append(allBufferPointers, fmt.Sprintf("%p", cp.items[i].ptr))
	}
	log.Errorf("customPool.Put: Buffer at %p not found in the pool; expected one of: %+v", bufPtr, allBufferPointers)

	return fmt.Errorf("Buffer not found in the pool")
}

/*
Exposes init functions for timeseries block readers.

# This allocates all required buffers for the readers

It is up to the caller to call .Close() to return all buffers
*/
func InitTimeSeriesReader(mKey string) (*TimeSeriesSegmentReader, error) {
	tsoBuf, err := globalPool.Get(segutils.METRICS_SEARCH_ALLOCATE_BLOCK)
	if err != nil {
		return nil, fmt.Errorf("Error getting tsoBuf from the pool")
	}

	tsgBuf, err := globalPool.Get(segutils.METRICS_SEARCH_ALLOCATE_BLOCK)
	if err != nil {
		return nil, fmt.Errorf("Error getting tsgBuf from the pool")
	}

	return &TimeSeriesSegmentReader{
		mKey:   mKey,
		tsoBuf: tsoBuf,
		tsgBuf: tsgBuf,
	}, nil
}

/*
Closes the iterator by returning all buffers back to the pool
*/
func (tssr *TimeSeriesSegmentReader) Close() error {
	err1 := globalPool.Put(tssr.tsoBuf)
	if err1 != nil {
		log.Errorf("TimeSeriesSegmentReader.Close: Error putting tsoBuf back to the pool: %v", err1)
	}

	err2 := globalPool.Put(tssr.tsgBuf)
	if err2 != nil {
		log.Errorf("TimeSeriesSegmentReader.Close: Error putting tsgBuf back to the pool: %v", err2)
	}

	if err1 != nil || err2 != nil {
		return fmt.Errorf("Error putting buffers back to the pool")
	}

	return nil
}

func InitSharedTimeSeriesSegmentReader(mKey string, numReaders int) (*SharedTimeSeriesSegmentReader, error) {
	sharedTimeSeriesSegmentReader := &SharedTimeSeriesSegmentReader{
		TimeSeriesSegmentReadersList: make([]*TimeSeriesSegmentReader, numReaders),
		numReaders:                   numReaders,
		rwLock:                       &sync.Mutex{},
	}

	for i := 0; i < numReaders; i++ {
		currReader, err := InitTimeSeriesReader(mKey)
		if err != nil {
			sharedTimeSeriesSegmentReader.Close()
			return sharedTimeSeriesSegmentReader, err
		}
		sharedTimeSeriesSegmentReader.TimeSeriesSegmentReadersList[i] = currReader
	}
	return sharedTimeSeriesSegmentReader, nil
}

func (stssr *SharedTimeSeriesSegmentReader) Close() error {
	for _, reader := range stssr.TimeSeriesSegmentReadersList {
		reader.Close()
	}
	return nil
}

/*
Exposes init functions for timeseries block readers.

After calling this function, all previous blockreaders will become invalid.

It is up to the caller to ensure that all previous blockreaders are no longer being used
*/
func (tssr *TimeSeriesSegmentReader) InitReaderForBlock(blkNum uint16, queryMetrics *structs.MetricsQueryProcessingMetrics) (*TimeSeriesBlockReader, error) {
	// load tso/tsg file as need
	tsoFName := fmt.Sprintf("%s_%d.tso", tssr.mKey, blkNum)
	sTime := time.Now()
	readTSO, nTSIDs, err := tssr.loadTSOFile(tsoFName)
	if err != nil {
		log.Errorf("InitReaderForBlock: failed to init reader for block %v! Err:%+v", blkNum, err)
		return nil, err
	}

	queryMetrics.SetTimeLoadingTSOFiles(time.Since(sTime))
	queryMetrics.IncrementNumTSOFilesLoaded(1)

	tsgFName := fmt.Sprintf("%s_%d.tsg", tssr.mKey, blkNum)
	sTime = time.Now()
	readTSG, err := tssr.loadTSGFile(tsgFName)

	if err != nil {
		log.Errorf("InitReaderForBlock: failed to init reader for block %v! Err:%+v", blkNum, err)
		return nil, err
	}

	queryMetrics.SetTimeLoadingTSGFiles(time.Since(sTime))
	queryMetrics.IncrementNumTSGFilesLoaded(1)

	return &TimeSeriesBlockReader{
		rawTSO:    readTSO,
		rawTSG:    readTSG,
		numTSIDs:  nTSIDs,
		first:     true,
		lastTSidx: 0,
		lastTSID:  0,
	}, nil
}

/*
Exposes function that will return a TimeSeriesIterator for a given tsid

# Returns a Series Iterator, a bool, or an error

The bool indicates if the series was found. If the series is not found, the iterator will be nil

Internally, looks up the tsid in the .tso file and returns a TimeSeriesIterator after loading the csg at the read offset
This function will keep the encoded csg values as a []byte
*/
func (tsbr *TimeSeriesBlockReader) GetTimeSeriesIterator(tsid uint64) (*compress.DecompressIterator, bool, error) {
	// load tso/tsg file as needd

	var found bool
	var offset uint32
	var tsIDX uint32
	if !tsbr.first {
		if tsid < tsbr.lastTSID {
			found, tsIDX, offset = getOffsetFromTsoFile(0, tsbr.lastTSidx, uint32(tsbr.numTSIDs), tsid, tsbr.rawTSO)
		} else if tsid > tsbr.lastTSID {
			found, tsIDX, offset = getOffsetFromTsoFile(tsbr.lastTSidx, uint32(tsbr.numTSIDs-1), uint32(tsbr.numTSIDs), tsid, tsbr.rawTSO)
		}
	} else {
		found, tsIDX, offset = getOffsetFromTsoFile(0, uint32(tsbr.numTSIDs-1), uint32(tsbr.numTSIDs), tsid, tsbr.rawTSO)
	}

	if !found {
		return nil, false, nil
	}
	tsbr.first = false
	tsbr.lastTSID = tsid
	tsbr.lastTSidx = tsIDX

	offset += 9 // 1 byte for version + 8 bytes is for tsid
	tsgLen := utils.BytesToUint32LittleEndian(tsbr.rawTSG[offset : offset+4])
	offset += 4
	rawSeries := bytes.NewReader(tsbr.rawTSG[offset : offset+tsgLen])
	it, err := compress.NewDecompressIterator(rawSeries)
	if err != nil {
		log.Errorf("GetTimeSeriesIterator: Error initialising a decompressor! err: %v", err)
		return nil, true, err
	}
	return it, true, nil
}

// returns bool if found. If true, returns the tsidx and offset in the TSG file
func getOffsetFromTsoFile(low uint32, high uint32, nTsids uint32, tsid uint64, tsoBuf []byte) (bool, uint32, uint32) {
	for low <= high {
		mid := (high + low) / 2
		// adding 3 because the first byte for version and the next two bytes are for number of entries
		// multiplying 'mid' by 12 because every tsid info takes 8 bytes for tsid and 4 bytes for tsid offset
		offsetMid := 3 + mid*12
		// tsid takes 8 bytes in the tso buffer
		tempBuffer := tsoBuf[offsetMid : offsetMid+8]
		midTsid := utils.BytesToUint64LittleEndian(tempBuffer)
		if midTsid < tsid {
			low = mid + 1
		} else if midTsid > tsid {
			if mid == 0 {
				return false, mid, 0
			}
			high = mid - 1
		} else {
			off := tsoBuf[offsetMid+8 : offsetMid+12]
			return true, mid, utils.BytesToUint32LittleEndian(off)
		}
	}
	return false, 0, 0
}

func (tssr *TimeSeriesSegmentReader) expandTSOBufferToMinSize(minSize uint64) error {
	if cap(tssr.tsoBuf) < int(minSize) {
		err := globalPool.Put(tssr.tsoBuf)
		if err != nil {
			log.Errorf("expandTSOBufferToMinSize: Error putting buffer back to the pool: %v", err)
			return err
		}

		buf, err := globalPool.Get(minSize)
		if err != nil {
			log.Errorf("expandTSOBufferToMinSize: Error getting buffer from the pool: %v", err)
			return err
		}

		tssr.tsoBuf = buf
	}

	tssr.tsoBuf = tssr.tsoBuf[:minSize]

	return nil
}

func (tssr *TimeSeriesSegmentReader) expandTSGBufferToMinSize(minSize uint64) error {
	if cap(tssr.tsgBuf) < int(minSize) {
		err := globalPool.Put(tssr.tsgBuf)
		if err != nil {
			log.Errorf("expandTSGBufferToMinSize: Error putting buffer back to the pool: %v", err)
			return err
		}

		buf, err := globalPool.Get(minSize)
		if err != nil {
			log.Errorf("expandTSGBufferToMinSize: Error getting buffer from the pool: %v", err)
			return err
		}

		tssr.tsgBuf = buf
	}

	tssr.tsgBuf = tssr.tsgBuf[:minSize]

	return nil
}

func (tssr *TimeSeriesSegmentReader) loadTSOFile(fileName string) ([]byte, uint16, error) {

	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		log.Infof("loadTSOFile: failed to open fileName: %v  Error: %v", fileName, err)
		return nil, 0, err
	}
	defer fd.Close()

	finfo, err := fd.Stat()
	if err != nil {
		log.Errorf("loadTSOFile: error when trying to stat file=%+v. Error=%+v", fileName, err)
		return nil, 0, err
	}

	fileSize := finfo.Size()
	err = tssr.expandTSOBufferToMinSize(uint64(fileSize))
	if err != nil {
		log.Errorf("loadTSOFile: Error expanding TSO buffer: %v", err)
		return nil, 0, err
	}

	rbuf := tssr.tsoBuf[:]

	_, err = fd.ReadAt(rbuf, 0)
	if err != nil {
		log.Errorf("loadTSOFile: Error reading TSO file: %v, err: %v", fileName, err)
		return nil, 0, err
	}

	// rbuf[0] gives the version byte
	versionTsoFile := make([]byte, 1)
	copy(versionTsoFile, rbuf[:1])
	if versionTsoFile[0] != segutils.VERSION_TSOFILE[0] {
		return nil, 0, fmt.Errorf("loadTSOFile: the file version doesn't match; expected=%+v, got=%+v", segutils.VERSION_TSOFILE[0], versionTsoFile[0])
	}
	nEntries := utils.BytesToUint16LittleEndian(rbuf[1:3])
	return rbuf, nEntries, nil
}

func (tssr *TimeSeriesSegmentReader) loadTSGFile(fileName string) ([]byte, error) {
	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("loadTSGFile: error when trying to open file=%+v. Error=%+v", fileName, err)
		return nil, err
	}
	defer fd.Close()

	finfo, err := fd.Stat()
	if err != nil {
		log.Errorf("loadTSGFile: error when trying to stat file=%+v. Error=%+v", fileName, err)
		return nil, err
	}

	fileSize := finfo.Size()
	err = tssr.expandTSGBufferToMinSize(uint64(fileSize))
	if err != nil {
		log.Errorf("loadTSGFile: Error expanding TSG buffer: %v", err)
		return nil, err
	}

	rbuf := tssr.tsgBuf[:]

	_, err = fd.ReadAt(rbuf, 0)
	if err != nil {
		log.Errorf("loadTSGFile: Error reading TSG file: %v, err: %v", fileName, err)
		return nil, err
	}

	versionTsgFile := make([]byte, 1)
	copy(versionTsgFile, rbuf[:1])
	if versionTsgFile[0] != segutils.VERSION_TSGFILE[0] {
		return nil, fmt.Errorf("loadTSGFile: the file version doesn't match; expected=%+v, got=%+v", segutils.VERSION_TSGFILE[0], versionTsgFile[0])
	}
	return rbuf, nil
}

/*
TODO: Use the buffer pools for such kinds of memory accesses, it will reduce GC pressures.
*/
func (tssr *TimeSeriesSegmentReader) GetAllMetricNames() (map[string]bool, error) {

	filePath := fmt.Sprintf("%s.mnm", tssr.mKey)

	fd, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("GetAllMetricNames: failed to open fileName: %v  Error: %v", filePath, err)
		return nil, err
	}

	defer fd.Close()

	finfo, err := fd.Stat()
	if err != nil {
		log.Errorf("GetAllMetricNames: error when trying to stat file=%+v. Error=%+v", filePath, err)
		return nil, err
	}

	fileSize := finfo.Size()
	buf := make([]byte, fileSize)

	_, err = fd.Read(buf)
	if err != nil {
		log.Errorf("GetAllMetricNames: Error reading the Metric Names file: %v, err: %v", filePath, err)
		return nil, err
	}

	metricNames := make(map[string]bool)

	for i := 0; i < len(buf); {
		metricNameLen := int(utils.BytesToUint16LittleEndian(buf[i : i+2]))
		i += 2
		metricName := string(buf[i : i+metricNameLen])
		i += metricNameLen
		metricNames[metricName] = true
	}

	buf = nil

	return metricNames, nil
}
