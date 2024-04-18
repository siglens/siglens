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

package segread

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var rawTimestampsBufferPool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		slice := make([]uint64, utils.DEFAULT_TIME_SLICE_SIZE)
		return &slice
	},
}

type timeBlockRequest struct {
	tsRec   []byte
	blkNum  uint16
	numRecs uint16
}

type TimeRangeReader struct {
	timeFD        *os.File
	timestampKey  string
	timeMetadata  map[uint16]*structs.BlockMetadataHolder
	blockRecCount map[uint16]uint16

	loadedBlockNum          uint16
	blockTimestamps         []uint64
	blockReadBuffer         []byte
	blockUncompressedBuffer []byte // raw buffer to re-use for decompressing
	numBlockReadTimestamps  uint16
	loadedBlock             bool
	allInUseFiles           []string
}

// returns a new TimeRangeReader and any errors encountered
// the caller is responsible for calling TimeRangeReader.Close() when finished using it to close the fd
func InitNewTimeReader(segKey string, tsKey string, blockMetadata map[uint16]*structs.BlockMetadataHolder,
	blkRecCount map[uint16]uint16, qid uint64) (*TimeRangeReader, error) {
	allInUseFiles := make([]string, 0)
	var err error
	fName := fmt.Sprintf("%v_%v.csg", segKey, xxhash.Sum64String(tsKey))
	if tsKey != "" {
		err = blob.DownloadSegmentBlob(fName, true)
	} else {
		err = fmt.Errorf("InitNewTimeReader: failed to download segsetfile due to unknown segset col %+v", fName)
	}
	if err != nil {
		log.Errorf("qid=%d, InitNewTimeReader failed to download file. %+v, err=%v", qid, fName, err)
		return nil, err
	}
	fd, err := os.OpenFile(fName, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("qid=%d, InitNewSegFileReader: failed to open time column file %s. Error: %+v", qid, fName, err)
		return &TimeRangeReader{}, err
	}
	allInUseFiles = append(allInUseFiles, fName)

	return &TimeRangeReader{
		timeFD:                  fd,
		timestampKey:            tsKey,
		timeMetadata:            blockMetadata,
		blockRecCount:           blkRecCount,
		blockTimestamps:         *rawTimestampsBufferPool.Get().(*[]uint64),
		blockReadBuffer:         *fileReadBufferPool.Get().(*[]byte),
		blockUncompressedBuffer: *uncompressedReadBufferPool.Get().(*[]byte),
		loadedBlock:             false,
		allInUseFiles:           allInUseFiles,
	}, nil
}

func InitNewTimeReaderWithFD(tsFD *os.File, tsKey string, blockMetadata map[uint16]*structs.BlockMetadataHolder,
	blkRecCount map[uint16]uint16, qid uint64) (*TimeRangeReader, error) {

	return &TimeRangeReader{
		timeFD:                  tsFD,
		timestampKey:            tsKey,
		timeMetadata:            blockMetadata,
		blockRecCount:           blkRecCount,
		blockTimestamps:         *rawTimestampsBufferPool.Get().(*[]uint64),
		blockReadBuffer:         *fileReadBufferPool.Get().(*[]byte),
		blockUncompressedBuffer: *uncompressedReadBufferPool.Get().(*[]byte),
		loadedBlock:             false,
	}, nil
}

func InitNewTimeReaderFromBlockSummaries(segKey string, tsKey string, blockMetadata map[uint16]*structs.BlockMetadataHolder,
	blockSummaries []*structs.BlockSummary, qid uint64) (*TimeRangeReader, error) {

	blkRecCount := make(map[uint16]uint16)
	for blkIdx, blkSum := range blockSummaries {
		blkRecCount[uint16(blkIdx)] = blkSum.RecCount
	}
	return InitNewTimeReader(segKey, tsKey, blockMetadata, blkRecCount, qid)
}

// highly optimized for subsequent calls to handle the same blockNum
func (trr *TimeRangeReader) GetTimeStampForRecord(blockNum uint16, recordNum uint16, qid uint64) (uint64, error) {
	if !trr.loadedBlock || trr.loadedBlockNum != blockNum {
		err := trr.readAllTimestampsForBlock(blockNum)
		if err != nil {
			return 0, err
		}
	}

	if recordNum >= uint16(trr.numBlockReadTimestamps) {
		log.Errorf("qid=%v, GetTimeStampForRecord: failed to get timestamp for recNum %d blkNum %d. Number of read timestamps is %d",
			qid, recordNum, blockNum, trr.numBlockReadTimestamps)
		return 0, errors.New("record number is out of range")
	}
	return trr.blockTimestamps[recordNum], nil
}

func (trr *TimeRangeReader) GetAllTimeStampsForBlock(blockNum uint16) ([]uint64, error) {
	if !trr.loadedBlock || trr.loadedBlockNum != blockNum {
		err := trr.readAllTimestampsForBlock(blockNum)
		if err != nil {
			return nil, err
		}
	}

	return trr.blockTimestamps[:trr.numBlockReadTimestamps], nil
}

func (trr *TimeRangeReader) readAllTimestampsForBlock(blockNum uint16) error {

	err := trr.resizeSliceForBlock(blockNum)
	if err != nil {
		log.Errorf("readAllTimestampsForBlock: failed to resize internal slice for block %d. Err: %+v", blockNum, err)
		return errors.New("requested blockNum does not exist")
	}

	blockMeta, ok := trr.timeMetadata[blockNum]
	if !ok {
		log.Errorf("readAllTimestampsForBlock: failed to find block %d in all block metadata %+v", blockNum, trr.timeMetadata)
		return errors.New("requested blockNum does not exist")
	}
	blkOff, ok := blockMeta.ColumnBlockOffset[trr.timestampKey]
	if !ok {
		log.Errorf("readAllTimestampsForBlock: failed to find block offset for timestamp key %+v in block %d. All block offsets %+v",
			trr.timestampKey, blockNum, blockMeta.ColumnBlockOffset)
		return errors.New("requested blockNum does not exist")
	}
	blkLen, ok := blockMeta.ColumnBlockLen[trr.timestampKey]
	if !ok {
		log.Errorf("readAllTimestampsForBlock: failed to find block length for timestamp key %+v in block %d. All block offsets %+v",
			trr.timestampKey, blockNum, blockMeta.ColumnBlockLen)
		return errors.New("requested blockNum does not exist")
	}

	if uint32(len(trr.blockReadBuffer)) < blkLen {
		newArr := make([]byte, blkLen-uint32(len(trr.blockReadBuffer)))
		trr.blockReadBuffer = append(trr.blockReadBuffer, newArr...)
	}
	_, err = trr.timeFD.ReadAt(trr.blockReadBuffer[:blkLen], blkOff)
	if err != nil {
		if err != io.EOF {
			trr.loadedBlock = false
			log.Errorf("readAllTimestampsForBlock: error reading file at blkLen: %+v blkOff: %+v error: %+v", blkLen, blkOff, err)
			return err
		}
		return nil
	}

	rawTSVal := trr.blockReadBuffer[:blkLen]
	numRecs := trr.blockRecCount[blockNum]
	decoded, err := convertRawRecordsToTimestamps(rawTSVal, numRecs, trr.blockTimestamps)
	if err != nil {
		log.Errorf("convertRawRecordsToTimestamps failed %+v", err)
		return err
	}

	trr.numBlockReadTimestamps = numRecs
	trr.blockTimestamps = decoded
	trr.loadedBlock = true
	trr.loadedBlockNum = blockNum

	return nil
}

func (trr *TimeRangeReader) resizeSliceForBlock(blockNum uint16) error {

	numRecs, ok := trr.blockRecCount[blockNum]
	if !ok {
		log.Errorf("readAllTimestampsForBlock: failed to find block %d in all blocks: %+v", blockNum, trr.blockRecCount)
		return errors.New("blockNum not found")
	}
	if currBufSize := len(trr.blockTimestamps); int(numRecs) > currBufSize {
		toAdd := int(numRecs) - currBufSize
		newSlice := make([]uint64, toAdd)
		trr.blockTimestamps = append(trr.blockTimestamps, newSlice...)
	}
	return nil
}

func (trr *TimeRangeReader) Close() error {
	if trr.timeFD == nil {
		return errors.New("tried to close an unopened time reader")
	}
	trr.returnBuffers()
	err := blob.SetSegSetFilesAsNotInUse(trr.allInUseFiles)
	if err != nil {
		log.Errorf("Failed to release needed segment files from local storage %+v!  Err: %+v", trr.allInUseFiles, err)
	}
	return trr.timeFD.Close()
}

func (trr *TimeRangeReader) returnBuffers() {
	uncompressedReadBufferPool.Put(&trr.blockUncompressedBuffer)
	fileReadBufferPool.Put(&trr.blockReadBuffer)
	rawTimestampsBufferPool.Put(&trr.blockTimestamps)
}

func convertRawRecordsToTimestamps(rawRec []byte, numRecs uint16, bufToUse []uint64) ([]uint64, error) {
	if bufToUse == nil {
		bufToUse = make([]uint64, numRecs)
	}
	if currBufSize := len(bufToUse); int(numRecs) > currBufSize {
		toAdd := int(numRecs) - currBufSize
		newSlice := make([]uint64, toAdd)
		bufToUse = append(bufToUse, newSlice...)
	}

	oPtr := uint32(0)
	if rawRec[oPtr] != utils.TIMESTAMP_TOPDIFF_VARENC[0] {
		log.Errorf("received an unknown encoding type for typestamp column! expected %+v got %+v",
			utils.TIMESTAMP_TOPDIFF_VARENC[0], rawRec[oPtr])
		return nil, fmt.Errorf("received an unknown encoding type for typestamp column! expected %+v got %+v",
			utils.TIMESTAMP_TOPDIFF_VARENC[0], rawRec[oPtr])
	}
	oPtr++

	tsType := rawRec[oPtr]
	oPtr++

	lowTs := toputils.BytesToUint64LittleEndian(rawRec[oPtr:])
	oPtr += 8

	switch tsType {
	case structs.TS_Type8:
		var tsVal uint8
		for i := uint16(0); i < numRecs; i++ {
			tsVal = uint8(rawRec[oPtr])
			bufToUse[i] = uint64(tsVal) + lowTs
			oPtr += 1
		}
	case structs.TS_Type16:
		var tsVal uint16
		for i := uint16(0); i < numRecs; i++ {
			tsVal = toputils.BytesToUint16LittleEndian(rawRec[oPtr:])
			bufToUse[i] = uint64(tsVal) + lowTs
			oPtr += 2
		}
	case structs.TS_Type32:
		var tsVal uint32
		for i := uint16(0); i < numRecs; i++ {
			tsVal = toputils.BytesToUint32LittleEndian(rawRec[oPtr:])
			bufToUse[i] = uint64(tsVal) + lowTs
			oPtr += 4
		}
	case structs.TS_Type64:
		var tsVal uint64
		for i := uint16(0); i < numRecs; i++ {
			tsVal = toputils.BytesToUint64LittleEndian(rawRec[oPtr:])
			bufToUse[i] = uint64(tsVal) + lowTs
			oPtr += 8
		}
	}

	return bufToUse, nil
}

func readChunkFromFile(fd *os.File, buf []byte, blkLen uint32, blkOff int64) ([]byte, error) {
	if uint32(len(buf)) < blkLen {
		newArr := make([]byte, blkLen-uint32(len(buf)))
		buf = append(buf, newArr...)
	}
	buf = buf[:blkLen]
	_, err := fd.ReadAt(buf, blkOff)
	if err != nil {
		log.Errorf("readChunkFromFile: failed to read timestamp file! %+v bytes at offset %+v. error %+v",
			blkLen, blkOff, err)
		return nil, err
	}
	return buf, nil
}

// When the caller of this function is done with retVal, they should call
// ReturnTimeBuffers(retVal) to return the buffers to rawTimestampsBufferPool.
func processTimeBlocks(allRequests chan *timeBlockRequest, wg *sync.WaitGroup, retVal map[uint16][]uint64,
	retLock *sync.Mutex) {
	defer wg.Done()
	for req := range allRequests {
		bufToUse := *rawTimestampsBufferPool.Get().(*[]uint64)
		decoded, err := convertRawRecordsToTimestamps(req.tsRec, req.numRecs, bufToUse)
		if err != nil {
			log.Errorf("convertRawRecordsToTimestamps failed %+v", err)
			continue
		}
		retLock.Lock()
		retVal[req.blkNum] = decoded
		retLock.Unlock()
	}
}

// When the caller of this function is done with the returned map, they should
// call ReturnTimeBuffers() on it to return the buffers to rawTimestampsBufferPool.
func ReadAllTimestampsForBlock(blks map[uint16]*structs.BlockMetadataHolder, segKey string,
	blockSummaries []*structs.BlockSummary, parallelism int64) (map[uint16][]uint64, error) {

	if len(blks) == 0 {
		return make(map[uint16][]uint64), nil
	}
	tsKey := config.GetTimeStampKey()
	fName := fmt.Sprintf("%s_%v.csg", segKey, xxhash.Sum64String(tsKey))
	err := blob.DownloadSegmentBlob(fName, true)
	if err != nil {
		log.Errorf("ReadAllTimestampsForBlock: failed to download time column file %s. Error: %+v", fName, err)
		return nil, err
	}

	defer func() {
		err := blob.SetBlobAsNotInUse(fName)
		if err != nil {
			log.Errorf("ReadAllTimestampsForBlock: failed to set blob as not in use %s. Error: %+v", fName, err)
		}
	}()
	fd, err := os.OpenFile(fName, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("ReadAllTimestampsForBlock: failed to open time column file %s. Error: %+v", fName, err)
		return nil, err
	}
	defer fd.Close()

	allBlocks := make([]uint16, 0)
	for n := range blks {
		allBlocks = append(allBlocks, n)
	}

	sort.Slice(allBlocks, func(i, j int) bool {
		return allBlocks[i] < allBlocks[j]
	})

	retVal := make(map[uint16][]uint64)
	var retLock sync.Mutex
	minIdx := 0
	allReadJob := make(chan *timeBlockRequest)
	var readerWG sync.WaitGroup
	for i := int64(0); i < parallelism; i++ {
		readerWG.Add(1)
		go processTimeBlocks(allReadJob, &readerWG, retVal, &retLock)
	}

	for minIdx < len(allBlocks) {
		minBlkNum := allBlocks[minIdx]
		lastBlkNum := minBlkNum
		firstBlkOff := blks[minBlkNum].ColumnBlockOffset[tsKey]
		blkLen := blks[minBlkNum].ColumnBlockLen[tsKey]
		maxIdx := minIdx
		for {
			nextIdx := maxIdx + 1
			if nextIdx >= len(allBlocks) {
				break
			}
			nextBlkNum := allBlocks[nextIdx]
			if nextBlkNum == lastBlkNum+1 {
				maxIdx++
				blkLen += blks[nextBlkNum].ColumnBlockLen[tsKey]
				lastBlkNum = nextBlkNum
			} else {
				break
			}
		}
		buffer := *uncompressedReadBufferPool.Get().(*[]byte)
		rawChunk, err := readChunkFromFile(fd, buffer, blkLen, firstBlkOff)
		defer uncompressedReadBufferPool.Put(&rawChunk)
		if err != nil {
			log.Errorf("ReadAllTimestampsForBlock: Failed to read chunk from file! %+v", err)
			continue
		}

		for currBlk := minBlkNum; currBlk <= allBlocks[maxIdx]; currBlk++ {
			readOffset := blks[currBlk].ColumnBlockOffset[tsKey] - firstBlkOff
			readLen := int64(blks[currBlk].ColumnBlockLen[tsKey])
			rawBlock := rawChunk[readOffset : readOffset+readLen]
			numRecs := blockSummaries[currBlk].RecCount
			allReadJob <- &timeBlockRequest{tsRec: rawBlock, blkNum: currBlk, numRecs: numRecs}
		}
		minIdx = maxIdx + 1
	}
	close(allReadJob)
	readerWG.Wait()
	return retVal, nil
}

func ReturnTimeBuffers(og map[uint16][]uint64) {
	for _, v := range og {
		rawTimestampsBufferPool.Put(&v)
	}
}
