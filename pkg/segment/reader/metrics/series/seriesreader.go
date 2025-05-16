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

	"github.com/siglens/siglens/pkg/memorypool"
	"github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
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
	tsoVersion byte

	rawTSO   []byte // raw read TSO file
	rawTSG   []byte // raw read TSG file
	numTSIDs uint64

	lastTSID  uint64
	lastTSidx uint32 // index of the last tsid in the tso file
	first     bool
}

type SharedTimeSeriesSegmentReader struct {
	TimeSeriesSegmentReadersList []*TimeSeriesSegmentReader
	numReaders                   int
	rwLock                       *sync.Mutex
}

const S_1_KB = 1024
const S_4_KB = 4096
const S_32_KB = 32768
const S_128_KB = 131072
const S_256_KB = 262144
const S_1_MB = 1048576
const S_4_MB = 4194304
const S_16_MB = 16777216
const S_32_MB = 33554432
const S_64_MB = 67108864
const S_128_MB = 134217728
const S_256_MB = 268435456

var pool1K = memorypool.NewMemoryPool(0, S_1_KB)
var pool4K = memorypool.NewMemoryPool(0, S_4_KB)
var pool32K = memorypool.NewMemoryPool(0, S_32_KB)
var pool128K = memorypool.NewMemoryPool(0, S_128_KB)
var pool256K = memorypool.NewMemoryPool(0, S_256_KB)
var pool1M = memorypool.NewMemoryPool(0, S_1_MB)
var pool4M = memorypool.NewMemoryPool(0, S_4_MB)
var pool16M = memorypool.NewMemoryPool(0, S_16_MB)
var pool32M = memorypool.NewMemoryPool(0, S_32_MB)
var pool64M = memorypool.NewMemoryPool(0, S_64_MB)
var pool128M = memorypool.NewMemoryPool(0, S_128_MB)
var pool256M = memorypool.NewMemoryPool(0, S_256_MB)

func GetBufFromPool(size int64) []byte {
	switch {
	case size <= S_1_KB:
		return pool1K.Get(S_1_KB)[:size]
	case size <= S_4_KB:
		return pool4K.Get(S_4_KB)[:size]
	case size <= S_32_KB:
		return pool32K.Get(S_32_KB)[:size]
	case size <= S_128_KB:
		return pool128K.Get(S_128_KB)[:size]
	case size <= S_256_KB:
		return pool256K.Get(S_256_KB)[:size]
	case size <= S_1_MB:
		return pool1M.Get(S_1_MB)[:size]
	case size <= S_4_MB:
		return pool4M.Get(S_4_MB)[:size]
	case size <= S_16_MB:
		return pool16M.Get(S_16_MB)[:size]
	case size <= S_32_MB:
		return pool32M.Get(S_32_MB)[:size]
	case size <= S_64_MB:
		return pool64M.Get(S_64_MB)[:size]
	case size <= S_128_MB:
		return pool128M.Get(S_128_MB)[:size]
	case size <= S_256_MB:
		return pool256M.Get(S_256_MB)[:size]
	default:
		return make([]byte, 0, size) // too big, don't pool
	}
}

func PutBufToPool(buf []byte) error {
	switch cap(buf) {
	case S_1_KB:
		return pool1K.Put(buf)
	case S_4_KB:
		return pool4K.Put(buf)
	case S_32_KB:
		return pool32K.Put(buf)
	case S_128_KB:
		return pool128K.Put(buf)
	case S_256_KB:
		return pool256K.Put(buf)
	case S_1_MB:
		return pool1M.Put(buf)
	case S_4_MB:
		return pool4M.Put(buf)
	case S_16_MB:
		return pool16M.Put(buf)
	case S_32_MB:
		return pool32M.Put(buf)
	case S_64_MB:
		return pool64M.Put(buf)
	case S_128_MB:
		return pool128M.Put(buf)
	case S_256_MB:
		return pool256M.Put(buf)
	default:
		// too large or not from a known pool; discard
		return nil
	}
}

/*
Exposes init functions for timeseries block readers.

# This allocates all required buffers for the readers

It is up to the caller to call .Close() to return all buffers
*/
func InitTimeSeriesReader(mKey string) (*TimeSeriesSegmentReader, error) {
	return &TimeSeriesSegmentReader{
		mKey:   mKey,
		tsoBuf: nil,
		tsgBuf: nil,
	}, nil
}

var ErrGiveBackToPool = fmt.Errorf("Error putting buffers back to the pool")

/*
Closes the iterator by returning all buffers back to the pool
*/
func (tssr *TimeSeriesSegmentReader) Close() error {

	var err1, err2 error
	if tssr.tsoBuf != nil {
		err1 = PutBufToPool(tssr.tsoBuf)
	}

	if tssr.tsgBuf != nil {
		err2 = PutBufToPool(tssr.tsgBuf)
	}

	if err1 != nil || err2 != nil {
		return ErrGiveBackToPool
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

var ErrInitReader = fmt.Errorf("failed to init reader for block")

/*
Exposes init functions for timeseries block readers.

After calling this function, all previous blockreaders will become invalid.

It is up to the caller to ensure that all previous blockreaders are no longer being used
*/
func (tssr *TimeSeriesSegmentReader) InitReaderForBlock(blkNum uint16, queryMetrics *structs.MetricsQueryProcessingMetrics) (*TimeSeriesBlockReader, error) {
	// load tso/tsg file as need
	tsoFName := fmt.Sprintf("%s_%d.tso", tssr.mKey, blkNum)
	sTime := time.Now()

	tsoFileSize, err := utils.GetFileSize(tsoFName)
	if err != nil {
		return nil, err
	}

	// we may get called multiple time for different blocks, when we get called first time
	// it will be nil, if it called subsequently then first release the buf and then get a new one
	// based on the size of the block being asked
	if tssr.tsoBuf != nil {
		err := PutBufToPool(tssr.tsoBuf)
		if err != nil {
			return nil, err
		}
	}

	tssr.tsoBuf = GetBufFromPool(tsoFileSize)

	tsoVersion, readTSO, nTSIDs, err := tssr.loadTSOFile(tsoFName)
	if err != nil {
		log.Error(ErrInitReader)
		return nil, err
	}

	queryMetrics.SetTimeLoadingTSOFiles(time.Since(sTime))
	queryMetrics.IncrementNumTSOFilesLoaded(1)

	tsgFName := fmt.Sprintf("%s_%d.tsg", tssr.mKey, blkNum)
	sTime = time.Now()

	tsgFileSize, err := utils.GetFileSize(tsgFName)
	if err != nil {
		return nil, err
	}

	if tssr.tsgBuf != nil {
		err := PutBufToPool(tssr.tsgBuf)
		if err != nil {
			return nil, err
		}
	}

	tssr.tsgBuf = GetBufFromPool(tsgFileSize)

	readTSG, err := tssr.loadTSGFile(tsgFName)

	if err != nil {
		log.Error(ErrInitReader)
		return nil, err
	}

	queryMetrics.SetTimeLoadingTSGFiles(time.Since(sTime))
	queryMetrics.IncrementNumTSGFilesLoaded(1)

	return &TimeSeriesBlockReader{
		tsoVersion: tsoVersion,
		rawTSO:     readTSO,
		rawTSG:     readTSG,
		numTSIDs:   nTSIDs,
		first:      true,
		lastTSidx:  0,
		lastTSID:   0,
	}, nil
}

var ErrInitDecompressor = fmt.Errorf("failed to init decompressor")

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
			found, tsIDX, offset = getOffsetFromTsoFile(tsbr.tsoVersion, 0, tsbr.lastTSidx, uint32(tsbr.numTSIDs), tsid, tsbr.rawTSO)
		} else if tsid > tsbr.lastTSID {
			found, tsIDX, offset = getOffsetFromTsoFile(tsbr.tsoVersion, tsbr.lastTSidx, uint32(tsbr.numTSIDs-1), uint32(tsbr.numTSIDs), tsid, tsbr.rawTSO)
		}
	} else {
		found, tsIDX, offset = getOffsetFromTsoFile(tsbr.tsoVersion, 0, uint32(tsbr.numTSIDs-1), uint32(tsbr.numTSIDs), tsid, tsbr.rawTSO)
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
		log.Error(ErrInitDecompressor)
		return nil, true, err
	}
	return it, true, nil
}

var ErrBadTsoVersion = fmt.Errorf("invalid TSO version")

// returns bool if found. If true, returns the tsidx and offset in the TSG file
func getOffsetFromTsoFile(tsoVersion byte, low uint32, high uint32, nTsids uint32, tsid uint64,
	tsoBuf []byte) (bool, uint32, uint32) {

	switch tsoVersion {
	case sutils.VERSION_TSOFILE_V1[0]:
		tsoBuf = tsoBuf[3:] // strip the version and number of entries
	case sutils.VERSION_TSOFILE_V2[0]:
		tsoBuf = tsoBuf[9:] // strip the version and number of entries
	default:
		log.Error(ErrBadTsoVersion)
		return false, 0, 0
	}

	for low <= high {
		mid := (high + low) / 2
		// multiplying 'mid' by 12 because every tsid info takes 8 bytes for tsid and 4 bytes for tsid offset
		offsetMid := mid * 12
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

var ErrOpenFileForPoolBuffer = fmt.Errorf("failed to open file for pool buffer")
var ErrReadFileForPoolBuffer = fmt.Errorf("failed to read file for pool buffer")

func loadFileIntoPoolBuffer(fileName string, bufferFromPool []byte) error {
	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		log.Error(ErrOpenFileForPoolBuffer)
		return err
	}
	defer fd.Close()

	_, err = fd.ReadAt(bufferFromPool, 0)
	if err != nil {
		log.Error(ErrReadFileForPoolBuffer)
		return err
	}

	return nil
}

var ErrLoadTsoFile = fmt.Errorf("failed to load TSO file")

func (tssr *TimeSeriesSegmentReader) loadTSOFile(fileName string) (byte, []byte, uint64, error) {

	err := loadFileIntoPoolBuffer(fileName, tssr.tsoBuf)
	if err != nil {
		log.Error(ErrLoadTsoFile)
		return 0, nil, 0, err
	}

	tsoVersion := tssr.tsoBuf[0]
	nEntries := uint64(0)
	switch tsoVersion {
	case sutils.VERSION_TSOFILE_V1[0]:
		nEntries = uint64(utils.BytesToUint16LittleEndian(tssr.tsoBuf[1:3]))
	case sutils.VERSION_TSOFILE_V2[0]:
		nEntries = utils.BytesToUint64LittleEndian(tssr.tsoBuf[1:9])
	default:
		return 0, nil, 0, ErrBadTsoVersion
	}

	return tsoVersion, tssr.tsoBuf, nEntries, nil
}

var ErrLoadTsgFile = fmt.Errorf("failed to load TSG file")
var ErrBadTsgVersion = fmt.Errorf("bad TSG version")

func (tssr *TimeSeriesSegmentReader) loadTSGFile(fileName string) ([]byte, error) {
	err := loadFileIntoPoolBuffer(fileName, tssr.tsgBuf)
	if err != nil {
		log.Error(ErrLoadTsgFile)
		return nil, err
	}

	versionTsgFile := make([]byte, 1)
	copy(versionTsgFile, tssr.tsgBuf[:1])
	if versionTsgFile[0] != sutils.VERSION_TSGFILE[0] {
		return nil, ErrBadTsgVersion
	}
	return tssr.tsgBuf, nil
}

func GetAllMetricNames(mKey string) (map[string]bool, error) {

	filePath := fmt.Sprintf("%s.mnm", mKey)
	return metadata.ReadMetricNames(filePath)
}
