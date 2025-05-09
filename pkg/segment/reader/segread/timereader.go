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
	segmetadata "github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/reader/segread/segreader"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var rawTimestampsBufferPool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		slice := make([]uint64, segutils.DEFAULT_TIME_SLICE_SIZE)
		return &slice
	},
}

type timeBlockRequest struct {
	tsRec   []byte
	blkNum  uint16
	numRecs uint16
}

type TimeRangeReader struct {
	timeFD            *os.File
	timestampKey      string
	allBlocksToSearch map[uint16]struct{}
	blockRecCount     map[uint16]uint16

	loadedBlockNum          uint16
	blockTimestamps         []uint64
	blockReadBuffer         []byte
	blockUncompressedBuffer []byte // raw buffer to re-use for decompressing
	numBlockReadTimestamps  uint16
	loadedBlock             bool
	allInUseFiles           []string
	allBmi                  *structs.AllBlksMetaInfo
}

// returns a new TimeRangeReader and any errors encountered
// the caller is responsible for calling TimeRangeReader.Close() when finished using it to close the fd
func InitNewTimeReader(segKey string, tsKey string, allBlocksToSearch map[uint16]struct{},
	blkRecCount map[uint16]uint16, qid uint64, allBmi *structs.AllBlksMetaInfo,
) (*TimeRangeReader, error) {
	allInUseFiles := make([]string, 0)
	var err error
	fName := fmt.Sprintf("%v_%v.csg", segKey, xxhash.Sum64String(tsKey))
	if tsKey != "" {
		err = blob.DownloadSegmentBlob(fName, true)
	} else {
		err = fmt.Errorf("InitNewTimeReader: failed to download segsetfile due to unknown segset col, file: %+v", fName)
	}
	if err != nil {
		return nil, fmt.Errorf("qid=%d, InitNewTimeReader: failed to download file: %+v, err: %v", qid, fName, err)
	}
	fd, err := os.OpenFile(fName, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("qid=%d, InitNewTimeReader: failed to open time column file: %s, err: %+v", qid, fName, err)
	}
	allInUseFiles = append(allInUseFiles, fName)

	return &TimeRangeReader{
		timeFD:                  fd,
		timestampKey:            tsKey,
		allBlocksToSearch:       allBlocksToSearch,
		blockRecCount:           blkRecCount,
		blockTimestamps:         *rawTimestampsBufferPool.Get().(*[]uint64),
		blockReadBuffer:         nil,
		blockUncompressedBuffer: nil,
		loadedBlock:             false,
		allInUseFiles:           allInUseFiles,
		allBmi:                  allBmi,
	}, nil
}

func InitNewTimeReaderWithFD(tsFD *os.File, tsKey string, allBlocksToSearch map[uint16]struct{},
	blkRecCount map[uint16]uint16, qid uint64,
	allBmi *structs.AllBlksMetaInfo,
) (*TimeRangeReader, error) {
	return &TimeRangeReader{
		timeFD:                  tsFD,
		timestampKey:            tsKey,
		allBlocksToSearch:       allBlocksToSearch,
		blockRecCount:           blkRecCount,
		blockTimestamps:         *rawTimestampsBufferPool.Get().(*[]uint64),
		blockReadBuffer:         nil,
		blockUncompressedBuffer: nil,
		loadedBlock:             false,
		allBmi:                  allBmi,
	}, nil
}

func InitNewTimeReaderFromBlockSummaries(segKey string, tsKey string,
	allBlocksToSearch map[uint16]struct{},
	blockSummaries []*structs.BlockSummary, qid uint64,
	allBmi *structs.AllBlksMetaInfo,
) (*TimeRangeReader, error) {
	blkRecCount := make(map[uint16]uint16)
	for blkIdx, blkSum := range blockSummaries {
		blkRecCount[uint16(blkIdx)] = blkSum.RecCount
	}
	return InitNewTimeReader(segKey, tsKey, allBlocksToSearch, blkRecCount, qid, allBmi)
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
		return 0, fmt.Errorf("qid=%v, TimeRangeReader.GetTimeStampForRecord: record number is out of range", qid)
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
		return errors.New("TimeRangeReader.readAllTimestampsForBlock: failed to resize internal")
	}

	blockMeta, ok := trr.allBmi.AllBmh[blockNum]
	if !ok || blockMeta == nil {
		return errors.New("TimeRangeReader.readAllTimestampsForBlock: failed to find block")
	}

	cnameIdx, ok := trr.allBmi.CnameDict[trr.timestampKey]
	if !ok || cnameIdx >= len(blockMeta.ColBlockOffAndLen) {
		return fmt.Errorf("TimeRangeReader.readAllTimestampsForBlock: unexpected found: %v, cnameIdx: %v, len(blockMeta.ColBlockOffAndLen): %v, cnameDict: %v",
			ok, cnameIdx, len(blockMeta.ColBlockOffAndLen), trr.allBmi.CnameDict)
	}

	cOffLen := blockMeta.ColBlockOffAndLen[cnameIdx]

	if trr.blockReadBuffer == nil {
		trr.blockReadBuffer = segreader.GetBufFromPool(int64(cOffLen.Length))
	} else if len(trr.blockReadBuffer) < int(cOffLen.Length) {
		if err := segreader.PutBufToPool(trr.blockReadBuffer); trr.blockReadBuffer != nil && err != nil {
			log.Errorf("TimeReader.readAllTimestampsForBlock: Error putting block buffer back to pool, err: %v", err)
		}
		trr.blockReadBuffer = segreader.GetBufFromPool(int64(cOffLen.Length))
	}
	checksumFile := &utils.ChecksumFile{Fd: trr.timeFD}
	_, err = checksumFile.ReadAt(trr.blockReadBuffer[:cOffLen.Length], cOffLen.Offset)
	if err != nil {
		if err != io.EOF {
			trr.loadedBlock = false
			return fmt.Errorf("TimeRangeReader.readAllTimestampsForBlock: error reading file at blk error: %+v", err)
		}
		return nil
	}

	rawTSVal := trr.blockReadBuffer[:cOffLen.Length]
	numRecs := trr.blockRecCount[blockNum]
	decoded, err := convertRawRecordsToTimestamps(rawTSVal, numRecs, trr.blockTimestamps)
	if err != nil {
		return fmt.Errorf("TimeRangeReader.readAllTimestampsForBlock: convertRawRecordsToTimestamps failed, err: %+v", err)
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
		return errors.New("TimeRangeReader.resizeSliceForBlock blockNum not found")
	}

	trr.blockTimestamps = utils.ResizeSlice(trr.blockTimestamps, int(numRecs))

	return nil
}

func (trr *TimeRangeReader) Close() error {
	if trr.timeFD == nil {
		return errors.New("TimeRangeReader.Close: tried to close an unopened time reader")
	}
	trr.returnBuffers()
	err := blob.SetSegSetFilesAsNotInUse(trr.allInUseFiles)
	if err != nil {
		log.Errorf("TimeRangeReader.Close: Failed to release needed segment files from local storage %+v! err: %+v", trr.allInUseFiles, err)
	}
	return trr.timeFD.Close()
}

func (trr *TimeRangeReader) returnBuffers() {
	rawTimestampsBufferPool.Put(&trr.blockTimestamps)
	if err := segreader.PutBufToPool(trr.blockReadBuffer); trr.blockReadBuffer != nil && err != nil {
		log.Errorf("TimeReader.returnBuffers: Error putting buffer back to pool, err: %v", err)
	}
	if err := segreader.PutBufToPool(trr.blockUncompressedBuffer); trr.blockUncompressedBuffer != nil && err != nil {
		log.Errorf("Timereader.returnBuffers: Error putting raw block buffer back to pool, err: %v", err)
	}
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

	if len(rawRec) < 1+1+8 {
		return nil, fmt.Errorf("rawRec is too small to contain a timestamp")
	}

	oPtr := uint32(0)
	if rawRec[oPtr] != segutils.TIMESTAMP_TOPDIFF_VARENC[0] {
		return nil, fmt.Errorf("convertRawRecordsToTimestamps: received an unknown encoding type for timestamp column! expected %+v got %+v",
			segutils.TIMESTAMP_TOPDIFF_VARENC[0], rawRec[oPtr])
	}
	oPtr++

	tsType := rawRec[oPtr]
	oPtr++

	lowTs := utils.BytesToUint64LittleEndian(rawRec[oPtr:])
	oPtr += 8

	numValidRecs := numRecs
	switch tsType {
	case structs.TS_Type8:
		numValidRecs = min(numRecs, uint16(len(rawRec)-int(oPtr)))
		var tsVal uint8
		for i := uint16(0); i < numValidRecs; i++ {
			tsVal = uint8(rawRec[oPtr])
			bufToUse[i] = uint64(tsVal) + lowTs
			oPtr += 1
		}
	case structs.TS_Type16:
		numValidRecs = min(numRecs, uint16((len(rawRec)-int(oPtr))/2))
		var tsVal uint16
		for i := uint16(0); i < numValidRecs; i++ {
			tsVal = utils.BytesToUint16LittleEndian(rawRec[oPtr:])
			bufToUse[i] = uint64(tsVal) + lowTs
			oPtr += 2
		}
	case structs.TS_Type32:
		numValidRecs = min(numRecs, uint16((len(rawRec)-int(oPtr))/4))
		var tsVal uint32
		for i := uint16(0); i < numValidRecs; i++ {
			tsVal = utils.BytesToUint32LittleEndian(rawRec[oPtr:])
			bufToUse[i] = uint64(tsVal) + lowTs
			oPtr += 4
		}
	case structs.TS_Type64:
		numValidRecs = min(numRecs, uint16((len(rawRec)-int(oPtr))/8))
		var tsVal uint64
		for i := uint16(0); i < numValidRecs; i++ {
			tsVal = utils.BytesToUint64LittleEndian(rawRec[oPtr:])
			bufToUse[i] = uint64(tsVal) + lowTs
			oPtr += 8
		}
	}

	if numValidRecs != numRecs {
		return bufToUse, fmt.Errorf("convertRawRecordsToTimestamps: expected %d records, but rawRec only had %d records",
			numRecs, numValidRecs)
	}

	return bufToUse, nil
}

func readChunkFromFile(fd *os.File, buf []byte, blkLen uint32, blkOff int64) ([]byte, error) {
	buf = buf[:blkLen]
	checksumFile := &utils.ChecksumFile{Fd: fd}
	_, err := checksumFile.ReadAt(buf, blkOff)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// When the caller of this function is done with retVal, they should call
// ReturnTimeBuffers(retVal) to return the buffers to rawTimestampsBufferPool.
func processTimeBlocks(allRequests chan *timeBlockRequest, wg *sync.WaitGroup, retVal map[uint16][]uint64,
	retLock *sync.Mutex,
) {
	defer wg.Done()
	var err error
	var decoded []uint64
	for req := range allRequests {
		bufToUse := *rawTimestampsBufferPool.Get().(*[]uint64)
		decoded, err = convertRawRecordsToTimestamps(req.tsRec, req.numRecs, bufToUse)
		if err != nil {
			continue
		}
		retLock.Lock()
		retVal[req.blkNum] = decoded
		retLock.Unlock()
	}

	if err != nil {
		log.Errorf("processTimeBlocks: convertRawRecordsToTimestamps failed, err: %+v", err)
	}
}

// When the caller of this function is done with the returned map, they should
// call ReturnTimeBuffers() on it to return the buffers to rawTimestampsBufferPool.
func ReadAllTimestampsForBlock(blkNums map[uint16]struct{}, segKey string,
	blockSummaries []*structs.BlockSummary, parallelism int64,
) (map[uint16][]uint64, error) {
	if len(blkNums) == 0 {
		return make(map[uint16][]uint64), nil
	}
	tsKey := config.GetTimeStampKey()
	fName := fmt.Sprintf("%s_%v.csg", segKey, xxhash.Sum64String(tsKey))
	err := blob.DownloadSegmentBlob(fName, true)
	if err != nil {
		return nil, fmt.Errorf("ReadAllTimestampsForBlock: failed to download time column file %s. Error: %+v", fName, err)
	}

	allBmi, _, err := segmetadata.GetSearchInfoAndSummary(segKey)
	if err != nil {
		return nil, fmt.Errorf("ReadAllTimestampsForBlock: failed to get allBmis segKey: %s. Error: %+v", segKey, err)
	}
	cnameIdx, ok := allBmi.CnameDict[tsKey]
	if !ok {
		return nil, fmt.Errorf("ReadAllTimestampsForBlock: could not find tsKey in cnameDict")
	}

	defer func() {
		err := blob.SetBlobAsNotInUse(fName)
		if err != nil {
			log.Errorf("ReadAllTimestampsForBlock: failed to set blob as not in use %s. Error: %+v", fName, err)
		}
	}()
	fd, err := os.OpenFile(fName, os.O_RDONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("ReadAllTimestampsForBlock: failed to open time column file %s. Error: %+v", fName, err)
	}
	defer fd.Close()

	allBlocks := make([]uint16, 0)
	for n := range blkNums {
		allBlocks = append(allBlocks, n)
	}

	sort.Slice(allBlocks, func(i, j int) bool {
		return allBlocks[i] < allBlocks[j]
	})

	retVal := make(map[uint16][]uint64)
	var retLock sync.Mutex
	allReadJob := make(chan *timeBlockRequest)
	var readerWG sync.WaitGroup
	for i := int64(0); i < parallelism; i++ {
		readerWG.Add(1)
		go processTimeBlocks(allReadJob, &readerWG, retVal, &retLock)
	}

	var retErr error
	for minIdx, maxIdx := 0, 0; minIdx < len(allBlocks); minIdx = maxIdx + 1 {
		minBlkNum := allBlocks[minIdx]
		lastBlkNum := minBlkNum
		cOffLen := allBmi.AllBmh[minBlkNum].ColBlockOffAndLen[cnameIdx]
		firstBlkOff := cOffLen.Offset
		blkLen := cOffLen.Length
		maxIdx = minIdx
		for {
			nextIdx := maxIdx + 1
			if nextIdx >= len(allBlocks) {
				break
			}
			nextBlkNum := allBlocks[nextIdx]
			if nextBlkNum == lastBlkNum+1 {
				maxIdx++
				blkLen += allBmi.AllBmh[nextBlkNum].ColBlockOffAndLen[cnameIdx].Length
				lastBlkNum = nextBlkNum
			} else {
				break
			}
		}
		buffer := segreader.GetBufFromPool(int64(blkLen))
		rawChunk, err := readChunkFromFile(fd, buffer, blkLen, firstBlkOff)
		if err != nil {
			retErr = fmt.Errorf("ReadAllTimestampsForBlock: Failed to read chunk from file: %v of length: %v and offset: %v, err: %+v", fName, blkLen, firstBlkOff, err)
			continue
		}
		defer func() {
			err := segreader.PutBufToPool(rawChunk)
			if err != nil {
				log.Errorf("Timereader.ReadAllTimestampsForBlock: Error putting raw block buffer back to pool, err: %v", err)
			}
		}()

		readOffset := int64(0)
		for currBlk := minBlkNum; currBlk <= allBlocks[maxIdx]; currBlk++ {
			readLen := int64(allBmi.AllBmh[currBlk].ColBlockOffAndLen[cnameIdx].Length)
			rawBlock := rawChunk[readOffset : readOffset+readLen]
			numRecs := blockSummaries[currBlk].RecCount
			allReadJob <- &timeBlockRequest{tsRec: rawBlock, blkNum: currBlk, numRecs: numRecs}
			readOffset += readLen
		}
	}
	close(allReadJob)
	readerWG.Wait()
	return retVal, retErr
}

func ReturnTimeBuffers(og map[uint16][]uint64) {
	for k := range og {
		// Due to a bug in Go 1.19, scope of the loop variable is per loop not per iteration
		// and thus inserting the same value multiple times in the pool which can cause the same pointer
		// to a slice being returned multiple times.
		// Refer https://go.dev/blog/loopvar-preview and https://github.com/golang/go/discussions/56010 for more details.
		timeBuffer := og[k]
		rawTimestampsBufferPool.Put(&timeBuffer)
	}
}
