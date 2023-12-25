/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package series

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

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

	allBuffers [][]byte // list of all buffers used to read TSO/TSG files
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
	TimeSeriesBlockReader []*TimeSeriesSegmentReader
	numReaders            int
	rwLock                *sync.Mutex
}

var seriesBufferPool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:

		buff := float64(segutils.METRICS_SEARCH_ALLOCATE_BLOCK)
		slice := make([]byte, 0, int(buff))
		return &slice
	},
}

/*
Exposes init functions for timeseries block readers.

# This allocates all required buffers for the readers

It is up to the caller to call .Close() to return all buffers
*/
func InitTimeSeriesReader(mKey string) (*TimeSeriesSegmentReader, error) {
	// load tso/tsg file as needd
	return &TimeSeriesSegmentReader{
		mKey:       mKey,
		tsoBuf:     *seriesBufferPool.Get().(*[]byte),
		tsgBuf:     *seriesBufferPool.Get().(*[]byte),
		allBuffers: make([][]byte, 0),
	}, nil
}

/*
Closes the iterator by returning all buffers back to the pool
*/
func (tssr *TimeSeriesSegmentReader) Close() error {
	// load tso/tsg file as needd

	seriesBufferPool.Put(&tssr.tsoBuf)
	seriesBufferPool.Put(&tssr.tsgBuf)
	for i := range tssr.allBuffers {
		seriesBufferPool.Put(&tssr.allBuffers[i])
	}

	return nil
}

func InitSharedTimeSeriesSegmentReader(mKey string, numReaders int) (*SharedTimeSeriesSegmentReader, error) {
	sharedTimeSeriesSegmentReader := &SharedTimeSeriesSegmentReader{
		TimeSeriesBlockReader: make([]*TimeSeriesSegmentReader, numReaders),
		numReaders:            numReaders,
		rwLock:                &sync.Mutex{},
	}

	for i := 0; i < numReaders; i++ {
		currReader, err := InitTimeSeriesReader(mKey)
		if err != nil {
			sharedTimeSeriesSegmentReader.Close()
			return sharedTimeSeriesSegmentReader, err
		}
		sharedTimeSeriesSegmentReader.TimeSeriesBlockReader[i] = currReader
	}
	return sharedTimeSeriesSegmentReader, nil
}

func (stssr *SharedTimeSeriesSegmentReader) Close() error {
	for _, reader := range stssr.TimeSeriesBlockReader {
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
	readTSO, nTSIDs, err := tssr.loadTSOFile(tsoFName, tssr.tsoBuf)
	if err != nil {
		log.Errorf("InitReaderForBlock: failed to init reader for block %v! Err:%+v", blkNum, err)
		return nil, err
	}

	queryMetrics.SetTimeLoadingTSOFiles(time.Since(sTime))
	queryMetrics.IncrementNumTSOFilesLoaded(1)

	tsgFName := fmt.Sprintf("%s_%d.tsg", tssr.mKey, blkNum)
	sTime = time.Now()
	readTSG, err := tssr.loadTSGFile(tsgFName, tssr.tsgBuf)

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

func (tssr *TimeSeriesSegmentReader) loadTSOFile(fileName string, rbuf []byte) ([]byte, uint16, error) {

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
	rbuf = rbuf[:cap(rbuf)]
	sizeToAdd := fileSize - int64(len(rbuf))
	if sizeToAdd > 0 {
		newArr := *seriesBufferPool.Get().(*[]byte)
		if diff := sizeToAdd - int64(len(newArr)); diff <= 0 {
			newArr = newArr[:sizeToAdd]
		} else {
			extend := make([]byte, diff)
			newArr = append(newArr, extend...)
		}
		tssr.allBuffers = append(tssr.allBuffers, newArr)
		rbuf = append(rbuf, newArr...)
	} else {
		rbuf = rbuf[:fileSize]
	}
	_, err = fd.ReadAt(rbuf, 0)
	if err != nil {
		log.Errorf("loadTSOFile: Error reading TSO file: %v, err: %v", fileName, err)
		return nil, 0, err
	}
	// rbuf[0] gives the version byte
	versionTsoFile := make([]byte, 1)
	copy(versionTsoFile, rbuf[:1])
	if versionTsoFile[0] != segutils.VERSION_TSOFILE[0] {
		return nil, 0, fmt.Errorf("loadTSOFile: the file version doesn't match")
	}
	nEntries := utils.BytesToUint16LittleEndian(rbuf[1:3])
	return rbuf, nEntries, nil
}

func (tssr *TimeSeriesSegmentReader) loadTSGFile(fileName string, rbuf []byte) ([]byte, error) {
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
	rbuf = rbuf[:cap(rbuf)]
	sizeToAdd := fileSize - int64(len(rbuf))
	if sizeToAdd > 0 {
		newArr := *seriesBufferPool.Get().(*[]byte)
		if diff := sizeToAdd - int64(len(newArr)); diff <= 0 {
			newArr = newArr[:sizeToAdd]
		} else {
			extend := make([]byte, diff)
			newArr = append(newArr, extend...)
		}
		tssr.allBuffers = append(tssr.allBuffers, newArr)
		rbuf = append(rbuf, newArr...)
	} else {
		rbuf = rbuf[:fileSize]
	}
	_, err = fd.ReadAt(rbuf, 0)
	if err != nil {
		log.Errorf("loadTSGFile: Error reading TSG file: %v, err: %v", fileName, err)
		return nil, err
	}
	versionTsgFile := make([]byte, 1)
	copy(versionTsgFile, rbuf[:1])
	if versionTsgFile[0] != segutils.VERSION_TSGFILE[0] {
		return nil, fmt.Errorf("loadTSGFile: the file version doesn't match")
	}
	return rbuf, nil
}
