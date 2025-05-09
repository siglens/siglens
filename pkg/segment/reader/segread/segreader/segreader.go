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

package segreader

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/klauspost/compress/zstd"
	"github.com/siglens/siglens/pkg/memorypool"
	segmetadata "github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

const (
	// TODO do some heuristics to figure out what buffer sizes are typically needed
	S_1_KB   = 1024
	S_4_KB   = 4096
	S_32_KB  = 32768
	S_64_KB  = 65536
	S_128_KB = 131072
	S_256_KB = 262144
	S_512_KB = 524288
	S_1_MB   = 1048576
	S_2_MB   = 2097152
	S_4_MB   = 4194304
	S_6_MB   = 6291456
	S_8_MB   = 8388608
	S_10_MB  = 10485760
	S_12_MB  = 12582912
	S_14_MB  = 14680064
	S_16_MB  = 16777216
	S_18_MB  = 18874368
	S_20_MB  = 20971520

	COMPRESSION_FACTOR = 8 // SegmentFileReader.currRawBlockBuffer holds uncompressed block.
)

var (
	pool1K   = memorypool.NewMemoryPool(0, S_1_KB)
	pool4K   = memorypool.NewMemoryPool(0, S_4_KB)
	pool32K  = memorypool.NewMemoryPool(0, S_32_KB)
	pool64K  = memorypool.NewMemoryPool(0, S_64_KB)
	pool128K = memorypool.NewMemoryPool(0, S_128_KB)
	pool256K = memorypool.NewMemoryPool(0, S_256_KB)
	pool512K = memorypool.NewMemoryPool(0, S_512_KB)
	pool1M   = memorypool.NewMemoryPool(0, S_1_MB)
	pool2M   = memorypool.NewMemoryPool(0, S_2_MB)
	pool4M   = memorypool.NewMemoryPool(0, S_4_MB)
	pool6M   = memorypool.NewMemoryPool(0, S_6_MB)
	pool8M   = memorypool.NewMemoryPool(0, S_8_MB)
	pool10M  = memorypool.NewMemoryPool(0, S_10_MB)
	pool12M  = memorypool.NewMemoryPool(0, S_12_MB)
	pool14M  = memorypool.NewMemoryPool(0, S_14_MB)
	pool16M  = memorypool.NewMemoryPool(0, S_16_MB)
	pool18M  = memorypool.NewMemoryPool(0, S_18_MB)
	pool20M  = memorypool.NewMemoryPool(0, S_20_MB)
)

// Use zstd.WithDecoderConcurrency(0) so that it can have GOMAXPROCS goroutines.
// If this option is not given it defaults to 4 or GOMAXPROCS, whichever is
// smaller.
var decoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))

type SegmentFileReader struct {
	ColName           string   // column name this file references
	fileName          string   // file name to iterate
	currFD            *os.File // current file descriptor
	allBlocksToSearch map[uint16]struct{}

	currBlockNum             uint16
	currRecordNum            uint16
	currOffset               uint32
	currUncompressedBlockLen uint32
	currRecLen               uint32
	consistentColValueLen    uint32

	isBlockLoaded      bool
	currFileBuffer     []byte   // buffer re-used for file reads values
	currRawBlockBuffer []byte   // raw uncompressed block
	encType            uint8    // encoding type for this block
	deTlv              [][]byte // deTlv[dWordIdx] --> []byte (the TLV byte slice)
	deRecToTlv         []uint16 // deRecToTlv[recNum] --> dWordIdx
	blockSummaries     []*structs.BlockSummary
	someBlksAbsent     bool // this is used to not log some errors
	allBmi             *structs.AllBlksMetaInfo
}

func GetBufFromPool(size int64) []byte {
	switch {
	case size <= S_1_KB:
		return pool1K.Get(S_1_KB)[:size]
	case size <= S_4_KB:
		return pool4K.Get(S_4_KB)[:size]
	case size <= S_32_KB:
		return pool32K.Get(S_32_KB)[:size]
	case size <= S_64_KB:
		return pool64K.Get(S_64_KB)[:size]
	case size <= S_128_KB:
		return pool128K.Get(S_128_KB)[:size]
	case size <= S_256_KB:
		return pool256K.Get(S_256_KB)[:size]
	case size <= S_512_KB:
		return pool512K.Get(S_512_KB)[:size]
	case size <= S_1_MB:
		return pool1M.Get(S_1_MB)[:size]
	case size <= S_2_MB:
		return pool2M.Get(S_2_MB)[:size]
	case size <= S_4_MB:
		return pool4M.Get(S_4_MB)[:size]
	case size <= S_6_MB:
		return pool6M.Get(S_6_MB)[:size]
	case size <= S_8_MB:
		return pool8M.Get(S_8_MB)[:size]
	case size <= S_10_MB:
		return pool10M.Get(S_10_MB)[:size]
	case size <= S_12_MB:
		return pool12M.Get(S_12_MB)[:size]
	case size <= S_14_MB:
		return pool14M.Get(S_14_MB)[:size]
	case size <= S_16_MB:
		return pool16M.Get(S_16_MB)[:size]
	case size <= S_18_MB:
		return pool18M.Get(S_18_MB)[:size]
	case size <= S_20_MB:
		return pool20M.Get(S_20_MB)[:size]
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
	case S_64_KB:
		return pool64K.Put(buf)
	case S_128_KB:
		return pool128K.Put(buf)
	case S_256_KB:
		return pool256K.Put(buf)
	case S_512_KB:
		return pool512K.Put(buf)
	case S_1_MB:
		return pool1M.Put(buf)
	case S_2_MB:
		return pool2M.Put(buf)
	case S_4_MB:
		return pool4M.Put(buf)
	case S_6_MB:
		return pool6M.Put(buf)
	case S_8_MB:
		return pool8M.Put(buf)
	case S_10_MB:
		return pool10M.Put(buf)
	case S_12_MB:
		return pool12M.Put(buf)
	case S_14_MB:
		return pool14M.Put(buf)
	case S_16_MB:
		return pool16M.Put(buf)
	case S_18_MB:
		return pool18M.Put(buf)
	case S_20_MB:
		return pool20M.Put(buf)
	default:
		return nil
	}
}

// Returns a map of blockNum -> slice, where each element of the slice has the
// raw data for the corresponding record.
func ReadAllRecords(segkey string, cname string) (map[uint16][][]byte, error) {
	colCSG := fmt.Sprintf("%s_%v.csg", segkey, xxhash.Sum64String(cname))
	fd, err := os.Open(colCSG)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	allBmi, blockSummaries, err := segmetadata.GetSearchInfoAndSummary(segkey)
	if err != nil {
		return nil, fmt.Errorf("ReadAllRecords: failed to get block info for segkey %s; err=%+v", segkey, err)
	}

	allBlocksToSearch := utils.MapToSet(allBmi.AllBmh)

	fileReader, err := InitNewSegFileReader(fd, cname, allBlocksToSearch, 0, blockSummaries,
		segutils.INCONSISTENT_CVAL_SIZE, allBmi)
	if err != nil {
		return nil, err
	}
	defer fileReader.Close()

	blockToRecords := make(map[uint16][][]byte)

	for blockNum := range allBmi.AllBmh {
		_, err := fileReader.readBlock(blockNum)
		if err != nil {
			return nil, fmt.Errorf("ReadAllRecords: error reading block %v; err=%+v", blockNum, err)
		}

		numRecs := blockSummaries[blockNum].RecCount
		blockToRecords[blockNum] = make([][]byte, 0, numRecs)
		for i := uint16(0); i < numRecs; i++ {
			bytes, err := fileReader.ReadRecord(i)
			if err != nil {
				return nil, fmt.Errorf("ReadAllRecords: error reading record %v in block %v; err=%+v", i, blockNum, err)
			}

			// TODO: don't copy so much; without copying, there's a data
			// integrity issue when there's multiple blocks because `bytes` is
			// a slice of sfr.currRawBlockBuffer, but that buffer gets reused
			// every time a new block is loaded
			bytesCopy := make([]byte, len(bytes))
			copy(bytesCopy, bytes)
			blockToRecords[blockNum] = append(blockToRecords[blockNum], bytesCopy)
		}
	}

	return blockToRecords, nil
}

// returns a new SegmentFileReader and any errors encountered
// The returned SegmentFileReader must call .Close() when finished using it to close the fd
func InitNewSegFileReader(fd *os.File, colName string, allBlocksToSearch map[uint16]struct{},
	qid uint64, blockSummaries []*structs.BlockSummary, colValueRecLen uint32,
	allBmi *structs.AllBlksMetaInfo,
) (*SegmentFileReader, error) {
	fileName := ""
	if fd != nil {
		fileName = fd.Name()
	}

	return &SegmentFileReader{
		ColName:               colName,
		fileName:              fileName,
		currFD:                fd,
		allBlocksToSearch:     allBlocksToSearch,
		currOffset:            0,
		currFileBuffer:        nil,
		currRawBlockBuffer:    nil,
		consistentColValueLen: colValueRecLen,
		isBlockLoaded:         false,
		encType:               255,
		blockSummaries:        blockSummaries,
		deTlv:                 make([][]byte, 0),
		deRecToTlv:            make([]uint16, 0),
		allBmi:                allBmi,
	}, nil
}

func (sfr *SegmentFileReader) Close() error {
	if sfr.currFD == nil {
		return errors.New("SegmentFileReader.Close: tried to close an unopened segment file reader")
	}
	err := sfr.ReturnBuffers()
	if err != nil {
		return err
	}
	return sfr.currFD.Close()
}

func (sfr *SegmentFileReader) ReturnBuffers() error {
	var errorMessages []string
	if err := PutBufToPool(sfr.currFileBuffer); sfr.currFileBuffer != nil && err != nil {
		errorMessages = append(errorMessages, fmt.Sprintf("Segreader.ReturnBuffers: Error putting buffer back to pool, err: %v", err))
	}

	if err := PutBufToPool(sfr.currRawBlockBuffer); sfr.currRawBlockBuffer != nil && err != nil {
		errorMessages = append(errorMessages, fmt.Sprintf("ReturnBuffers: Error putting raw block buffer back to pool, err: %v", err))
	}

	if len(errorMessages) > 0 {
		combinedMessage := strings.Join(errorMessages, "\n")
		return fmt.Errorf("%s", combinedMessage)
	}

	return nil
}

// returns a bool indicating if blockNum is valid, and any error encountered
func (sfr *SegmentFileReader) readBlock(blockNum uint16) (bool, error) {
	validBlock, err := sfr.loadBlockUsingBuffer(blockNum)
	if err != nil {
		return true, fmt.Errorf("SegmentFileReader.readBlock: error trying to read block %v in file %s. Error: %+v",
			blockNum, sfr.fileName, err)
	}
	if !validBlock {
		return false, fmt.Errorf("SegmentFileReader.readBlock: column does not exist in block: %v", blockNum)
	}

	sfr.currBlockNum = blockNum
	sfr.isBlockLoaded = true
	return true, nil
}

// Helper function to decompresses and loads block using passed buffers.
// Returns whether the block is valid, and any error encountered.
//
// The block will not be valid if the column is not found in block metadata.
// This means that the column never existed for this block and only existed for
// other blocks
func (sfr *SegmentFileReader) loadBlockUsingBuffer(blockNum uint16) (bool, error) {
	if sfr == nil {
		return false, fmt.Errorf("SegmentFileReader.loadBlockUsingBuffer: SegmentFileReader is nil")
	}

	blockMeta, blockExists := sfr.allBmi.AllBmh[blockNum]
	if !blockExists {
		return true, fmt.Errorf("SegmentFileReader.loadBlockUsingBuffer: block %v does not exist", blockNum)
	}

	if blockMeta == nil {
		return false, fmt.Errorf("SegmentFileReader.loadBlockUsingBuffer: block %v is nil", blockNum)
	}

	cnameIdx := sfr.allBmi.CnameDict[sfr.ColName]

	if cnameIdx >= len(blockMeta.ColBlockOffAndLen) {
		return false, fmt.Errorf("SegmentFileReader.loadBlockUsingBuffer: blkNum: %v, cname: %v, cnameIdx: %v was higher than len(blockMeta.ColBlockOffAndLen): %v",
			blockNum, sfr.ColName, cnameIdx, len(blockMeta.ColBlockOffAndLen))
	}

	cOffAndLen := blockMeta.ColBlockOffAndLen[cnameIdx]

	if cOffAndLen.Length == 0 {
		// This is an invalid block & not an error because this column never existed for this block
		return false, nil
	}
	if sfr.currRawBlockBuffer == nil {
		sfr.currRawBlockBuffer = GetBufFromPool(int64(COMPRESSION_FACTOR * cOffAndLen.Length))
	} else if len(sfr.currRawBlockBuffer) < COMPRESSION_FACTOR*int(cOffAndLen.Length) {
		if err := PutBufToPool(sfr.currRawBlockBuffer); sfr.currRawBlockBuffer != nil && err != nil {
			log.Errorf("loadBlockUsingBuffer: Error putting raw block buffer back to pool, err: %v", err)
		}
		sfr.currRawBlockBuffer = GetBufFromPool(int64(COMPRESSION_FACTOR * cOffAndLen.Length))
	}

	if sfr.currFileBuffer == nil {
		sfr.currFileBuffer = GetBufFromPool(int64(cOffAndLen.Length))
	} else if len(sfr.currFileBuffer) < int(cOffAndLen.Length) {
		if err := PutBufToPool(sfr.currFileBuffer); sfr.currFileBuffer != nil && err != nil {
			log.Errorf("loadBlockUsingBuffer: Error putting file buffer back to pool, err: %v", err)
		}
		sfr.currFileBuffer = GetBufFromPool(int64(cOffAndLen.Length))
	}

	checksumFile := utils.ChecksumFile{Fd: sfr.currFD}
	_, err := checksumFile.ReadAt(sfr.currFileBuffer[:cOffAndLen.Length], cOffAndLen.Offset)
	if err != nil {
		return true, fmt.Errorf("SegmentFileReader.loadBlockUsingBuffer: read file error at offset: %v, err: %+v", cOffAndLen.Offset, err)
	}
	oPtr := uint32(0)
	sfr.encType = sfr.currFileBuffer[oPtr]
	oPtr++

	if sfr.encType == segutils.ZSTD_COMLUNAR_BLOCK[0] {
		err := sfr.unpackRawCsg(sfr.currFileBuffer[oPtr:cOffAndLen.Length], blockNum)
		return true, err
	} else if sfr.encType == segutils.ZSTD_DICTIONARY_BLOCK[0] {
		err := sfr.ReadDictEnc(sfr.currFileBuffer[oPtr:cOffAndLen.Length], blockNum)
		return true, err
	} else {
		return true, fmt.Errorf("SegmentFileReader.loadBlockUsingBuffer: received an unknown encoding type for %v column! expected zstd or dictenc got %+v",
			sfr.ColName, sfr.encType)
	}
}

// Returns the raw bytes of the record in the currently loaded block
func (sfr *SegmentFileReader) ReadRecord(recordNum uint16) ([]byte, error) {
	// if dict encoding, we use the dictmapping
	if sfr.encType == segutils.ZSTD_DICTIONARY_BLOCK[0] {
		ret, err := sfr.deGetRec(recordNum)
		return ret, err
	}

	if sfr.currRecordNum > recordNum {
		// we have to start offset over and iterate until we reach recordNum bc we do not how to go backwards in a block
		sfr.currOffset = 0
		currRecLen, err := sfr.getCurrentRecordLength()
		if err != nil {
			return nil, fmt.Errorf("SegmentFileReader.ReadRecord: error resetting SegmentFileReader %s. Error: %+v",
				sfr.fileName, err)
		}
		sfr.currRecLen = currRecLen
		sfr.currRecordNum = 0
	} else if sfr.currRecordNum == recordNum {
		return sfr.currRawBlockBuffer[sfr.currOffset : sfr.currOffset+sfr.currRecLen], nil
	}

	for {
		if sfr.currRecordNum == recordNum {
			return sfr.currRawBlockBuffer[sfr.currOffset : sfr.currOffset+sfr.currRecLen], nil
		} else if sfr.currRecordNum > recordNum {
			break // we cannot go backwards
		}
		err := sfr.iterateNextRecord()
		if err != nil {
			break
		}
	}

	if !sfr.someBlksAbsent {
		errStr := fmt.Sprintf("SegmentFileReader.ReadRecord: reached end of block before matching recNum %+v, currRecordNum: %+v. blockNum %+v, File %+v, colname %v, sfr.currOffset: %v, sfr.currRecLen: %v, sfr.currUncompressedBlockLen: %v",
			recordNum, sfr.currRecordNum, sfr.currBlockNum, sfr.fileName, sfr.ColName, sfr.currOffset,
			sfr.currRecLen, sfr.currUncompressedBlockLen)

		return nil, errors.New(errStr)
	}

	// if some bllks are absent for this column then its not really an error
	return nil, nil
}

// returns the new record number and if any errors are encountered
// an error will be returned if no more records are available
func (sfr *SegmentFileReader) iterateNextRecord() error {
	nextOff := sfr.currOffset + sfr.currRecLen
	if nextOff >= sfr.currUncompressedBlockLen {
		if !sfr.someBlksAbsent {
			log.Debugf("SegmentFileReader.iterateNextRecord: reached end of block, next Offset: %+v, curr uncompressed blklen: %+v", nextOff, sfr.currUncompressedBlockLen)
		}
		// we don't log an error, but we are returning err so that, the caller does not
		// get stuck an loop
		return io.EOF
	}
	sfr.currOffset = nextOff
	currRecLen, err := sfr.getCurrentRecordLength()
	if err != nil {
		sfr.currOffset -= sfr.currRecLen
		return err
	}
	sfr.currRecLen = currRecLen
	sfr.currRecordNum = sfr.currRecordNum + 1
	return nil
}

func (sfr *SegmentFileReader) getCurrentRecordLength() (uint32, error) {
	// if we have the positive column value rec len, that is the current record length
	// This value comes from the segment metadata, where we store the column value size
	// at segment level. If the value is >0 and is != INCONSISTENT_CVAL_SIZE it means all records in the segment for this column
	// have the same length and if it is equal to INCONSISTENT_CVAL_SIZE, it means the records have different lengths
	if sfr.consistentColValueLen > 0 && sfr.consistentColValueLen != segutils.INCONSISTENT_CVAL_SIZE {
		return sfr.consistentColValueLen, nil
	}
	var reclen uint32
	switch sfr.currRawBlockBuffer[sfr.currOffset] {
	case segutils.VALTYPE_ENC_SMALL_STRING[0]:
		// 1 byte for type, 2 for str-len, then str-len for actual string
		reclen = 3 + uint32(utils.BytesToUint16LittleEndian(sfr.currRawBlockBuffer[sfr.currOffset+1:]))
	case segutils.VALTYPE_ENC_BOOL[0]:
		reclen = 2
	case segutils.VALTYPE_ENC_INT8[0]:
		reclen = 2
	case segutils.VALTYPE_ENC_INT16[0]:
		reclen = 3
	case segutils.VALTYPE_ENC_INT32[0]:
		reclen = 5
	case segutils.VALTYPE_ENC_INT64[0]:
		reclen = 9
	case segutils.VALTYPE_ENC_UINT8[0]:
		reclen = 2
	case segutils.VALTYPE_ENC_UINT16[0]:
		reclen = 3
	case segutils.VALTYPE_ENC_UINT32[0]:
		reclen = 5
	case segutils.VALTYPE_ENC_UINT64[0]:
		reclen = 9
	case segutils.VALTYPE_ENC_FLOAT64[0]:
		reclen = 9
	case segutils.VALTYPE_ENC_BACKFILL[0]:
		reclen = 1
	case segutils.VALTYPE_DICT_ARRAY[0]:
		reclen = 3 + uint32(utils.BytesToUint16LittleEndian(sfr.currRawBlockBuffer[sfr.currOffset+1:]))
	case segutils.VALTYPE_RAW_JSON[0]:
		reclen = 3 + uint32(utils.BytesToUint16LittleEndian(sfr.currRawBlockBuffer[sfr.currOffset+1:]))

	default:
		return 0, fmt.Errorf("SegmentFileReader.getCurrentRecordLength: Received an unknown encoding type %+v at offset %+v", sfr.currRawBlockBuffer[sfr.currOffset], sfr.currOffset)
	}
	return reclen, nil
}

func (sfr *SegmentFileReader) IsBlkDictEncoded(blockNum uint16) (bool, error) {
	if !sfr.isBlockLoaded || sfr.currBlockNum != blockNum {
		valid, err := sfr.readBlock(blockNum)
		if !valid {
			return false, err
		}
		if err != nil {
			return false, fmt.Errorf("SegmentFileReader.IsBlkDictEncoded: error loading blockNum: %v. Error: %+v", blockNum, err)
		}
	}

	if sfr.encType != segutils.ZSTD_DICTIONARY_BLOCK[0] {
		return false, nil
	}

	return true, nil
}

func (sfr *SegmentFileReader) ReadDictEnc(buf []byte, blockNum uint16) error {
	idx := uint32(0)

	// read num of dict words
	numWords := utils.BytesToUint16LittleEndian(buf[idx : idx+2])
	idx += 2

	sfr.deTlv = utils.ResizeSlice(sfr.deTlv, int(numWords))
	sfr.deRecToTlv = utils.ResizeSlice(sfr.deRecToTlv, int(sfr.blockSummaries[blockNum].RecCount))

	var numRecs uint16
	var soffW uint32
	var err error
	numErrors := 0
	for w := uint16(0); w < numWords; w++ {

		soffW = idx
		// read dictWord 'T'
		switch buf[idx] {
		case segutils.VALTYPE_ENC_SMALL_STRING[0]:
			//  3 => 1 for 'T' and 2 for 'L' of string
			idx += uint32(3 + utils.BytesToUint16LittleEndian(buf[idx+1:idx+3]))
		case segutils.VALTYPE_ENC_BOOL[0]:
			idx += 2 // 1 for T and 1 for Boolean value
		case segutils.VALTYPE_ENC_INT64[0], segutils.VALTYPE_ENC_FLOAT64[0]:
			idx += 9 // 1 for T and 8 bytes for 'L' int64
		case segutils.VALTYPE_ENC_BACKFILL[0]:
			idx += 1 // 1 for T
		default:
			return fmt.Errorf("SegmentFileReader.ReadDictEnc: unknown dictEnc: %v only supported flt/int64/str/bool", buf[idx])
		}

		sfr.deTlv[w] = buf[soffW:idx]

		// read num of records
		numRecs = utils.BytesToUint16LittleEndian(buf[idx : idx+2])
		idx += 2

		for i := uint16(0); i < numRecs; i++ {
			// at this recNum's position in the array store the idx of the TLV byte slice
			recNum := utils.BytesToUint16LittleEndian(buf[idx : idx+2])
			idx += 2

			if int(recNum) >= len(sfr.deRecToTlv) {
				numErrors++
				if err == nil {
					err = fmt.Errorf("recNum %+v exceeds the number of records %+v in block %+v, fileName: %v, colname: %v",
						recNum, len(sfr.deRecToTlv), blockNum, sfr.fileName, sfr.ColName)
				}

				continue
			}
			sfr.deRecToTlv[recNum] = w
		}
	}

	if err != nil {
		log.Errorf("SegmentFileReader.ReadDictEnc: got %v errors like: %v", numErrors, err)
	}

	return err
}

func (sfr *SegmentFileReader) unpackRawCsg(buf []byte, blockNum uint16) error {
	initialBufferPtr := unsafe.SliceData(sfr.currRawBlockBuffer)
	uncompressed, err := decoder.DecodeAll(buf[0:], sfr.currRawBlockBuffer[:0])
	if err != nil {
		return fmt.Errorf("SegmentFileReader.unpackRawCsg: decompress error: %+v", err)
	}

	if initialBufferPtr != unsafe.SliceData(uncompressed) {
		log.Debugf("SegmentFileReader.unpackRawCsg: Uncomressed buffer after decoding is different than originally allocated ")
		if err := PutBufToPool(sfr.currRawBlockBuffer); sfr.currRawBlockBuffer != nil && err != nil {
			log.Errorf("unpackRawCsg: Error putting raw block buffer back to pool, err: %v", err)
		}
	}

	sfr.currRawBlockBuffer = uncompressed
	sfr.currOffset = 0

	currRecLen, err := sfr.getCurrentRecordLength()
	if err != nil {
		return fmt.Errorf("SegmentFileReader.unpackRawCsg: error getting record length for the first record in file %s. Error: %+v",
			sfr.fileName, err)
	}
	sfr.currRecLen = currRecLen
	sfr.currRecordNum = 0
	sfr.currUncompressedBlockLen = uint32(len(sfr.currRawBlockBuffer))

	return nil
}

func (sfr *SegmentFileReader) GetDictEncCvalsFromColFileOldPipeline(results map[uint16]map[string]interface{},
	blockNum uint16, orderedRecNums []uint16,
) bool {
	if !sfr.isBlockLoaded || sfr.currBlockNum != blockNum {
		valid, err := sfr.readBlock(blockNum)
		if !valid {
			return false
		}
		if err != nil {
			return false
		}
	}

	if sfr.encType != segutils.ZSTD_DICTIONARY_BLOCK[0] {
		return false
	}

	return sfr.DeToResultOldPipeline(results, orderedRecNums)
}

func (sfr *SegmentFileReader) GetDictEncCvalsFromColFile(results map[string][]segutils.CValueEnclosure,
	blockNum uint16, orderedRecNums []uint16,
) bool {
	if !sfr.isBlockLoaded || sfr.currBlockNum != blockNum {
		valid, err := sfr.readBlock(blockNum)
		if !valid {
			return false
		}
		if err != nil {
			return false
		}
	}

	if sfr.encType != segutils.ZSTD_DICTIONARY_BLOCK[0] {
		return false
	}

	return sfr.deToResults(results, orderedRecNums)
}

func (sfr *SegmentFileReader) DeToResultOldPipeline(results map[uint16]map[string]interface{},
	orderedRecNums []uint16,
) bool {
	for _, rn := range orderedRecNums {
		dwIdx := sfr.deRecToTlv[rn]
		dWord := sfr.deTlv[dwIdx]
		_, ok := results[rn]
		if !ok {
			results[rn] = make(map[string]interface{})
		}
		if dWord[0] == segutils.VALTYPE_ENC_SMALL_STRING[0] {
			results[rn][sfr.ColName] = string(dWord[3:])
		} else if dWord[0] == segutils.VALTYPE_ENC_BOOL[0] {
			results[rn][sfr.ColName] = utils.BytesToBoolLittleEndian(dWord[1:])
		} else if dWord[0] == segutils.VALTYPE_ENC_INT64[0] {
			results[rn][sfr.ColName] = utils.BytesToInt64LittleEndian(dWord[1:])
		} else if dWord[0] == segutils.VALTYPE_ENC_FLOAT64[0] {
			results[rn][sfr.ColName] = utils.BytesToFloat64LittleEndian(dWord[1:])
		} else if dWord[0] == segutils.VALTYPE_ENC_BACKFILL[0] {
			results[rn][sfr.ColName] = nil
		} else {
			log.Errorf("SegmentFileReader.DeToResultsOldPipeline: de only supported for str/int64/float64/bool")
			return false
		}
	}
	return true
}

func (sfr *SegmentFileReader) deToResults(results map[string][]segutils.CValueEnclosure,
	orderedRecNums []uint16,
) bool {
	for recIdx, rn := range orderedRecNums {
		dwIdx := sfr.deRecToTlv[rn]
		if int(dwIdx) >= len(sfr.deTlv) {
			log.Debugf("deToResults: dwIdx: %v was greater than len(sfr.deTlv): %v, cname: %v, csgfname: %v",
				dwIdx, len(sfr.deTlv), sfr.ColName, sfr.fileName)
			return false
		}
		dWord := sfr.deTlv[dwIdx]

		switch dWord[0] {
		case segutils.VALTYPE_ENC_SMALL_STRING[0]:
			results[sfr.ColName][recIdx].CVal = string(dWord[3:])
			results[sfr.ColName][recIdx].Dtype = segutils.SS_DT_STRING
		case segutils.VALTYPE_ENC_BOOL[0]:
			results[sfr.ColName][recIdx].CVal = utils.BytesToBoolLittleEndian(dWord[1:])
			results[sfr.ColName][recIdx].Dtype = segutils.SS_DT_BOOL
		case segutils.VALTYPE_ENC_INT64[0]:
			results[sfr.ColName][recIdx].CVal = utils.BytesToInt64LittleEndian(dWord[1:])
			results[sfr.ColName][recIdx].Dtype = segutils.SS_DT_SIGNED_NUM
		case segutils.VALTYPE_ENC_FLOAT64[0]:
			results[sfr.ColName][recIdx].CVal = utils.BytesToFloat64LittleEndian(dWord[1:])
			results[sfr.ColName][recIdx].Dtype = segutils.SS_DT_FLOAT
		case segutils.VALTYPE_ENC_BACKFILL[0]:
			results[sfr.ColName][recIdx].Dtype = segutils.SS_DT_BACKFILL
		default:
			log.Debugf("SegmentFileReader.deToResults: de only supported for str/int64/float64/bool but received %v", dWord[0])
			return false
		}
	}

	return true
}

func (sfr *SegmentFileReader) deGetRec(rn uint16) ([]byte, error) {
	if int(rn) >= len(sfr.deRecToTlv) {
		return nil, fmt.Errorf("SegmentFileReader.deGetRec: recNum %+v does not exist, len: %+v", rn, len(sfr.deRecToTlv))
	}
	dwIdx := sfr.deRecToTlv[rn]

	if int(dwIdx) >= len(sfr.deTlv) {
		return nil, fmt.Errorf("SegmentFileReader.deGetRec: dwIdx: %v was greater than len(sfr.deTlv): %v, cname: %v, csgfname: %v",
			dwIdx, len(sfr.deTlv), sfr.ColName, sfr.fileName)
	}

	dWord := sfr.deTlv[dwIdx]
	return dWord, nil
}

func (sfr *SegmentFileReader) AddRecNumsToMr(dwordIdx uint16, bsh *structs.BlockSearchHelper) {
	// If validRecords is nil, then all records are considered valid;
	// otherwise, only those in validRecords can be added to the results.
	validRecords := bsh.GetValidRecords()

	if validRecords == nil {
		recCount := sfr.blockSummaries[sfr.currBlockNum].RecCount
		for i, idx := range sfr.deRecToTlv[:recCount] {
			if idx == dwordIdx {
				bsh.AddMatchedRecord(uint(i))
			}
		}
	} else {
		for _, i := range validRecords {
			if sfr.deRecToTlv[i] == dwordIdx {
				bsh.AddMatchedRecord(uint(i))
			}
		}
	}
}

func (sfr *SegmentFileReader) GetFileName() string {
	return sfr.fileName
}

func (sfr *SegmentFileReader) GetDeTlv() [][]byte {
	return sfr.deTlv
}

func (sfr *SegmentFileReader) GetDeRecToTlv() []uint16 {
	return sfr.deRecToTlv
}

func (sfr *SegmentFileReader) ValidateAndReadBlock(blockNum uint16) error {
	if !sfr.isBlockLoaded || sfr.currBlockNum != blockNum {
		valid, err := sfr.readBlock(blockNum)
		if !valid {
			sfr.someBlksAbsent = true
			log.Debugf("Skipped invalid block %d, error: %v", blockNum, err)
			// This can happen if the column does not exist.
			return nil
		}
		if err != nil {
			return fmt.Errorf("SegmentFileReader.ValidateAndReadBlock: error loading blockNum: %v. Error: %+v", blockNum, err)
		}
	}

	return nil
}
