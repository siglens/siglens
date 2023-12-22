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

package segread

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var uncompressedReadBufferPool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		slice := make([]byte, 0, utils.WIP_SIZE)
		return &slice
	},
}

var fileReadBufferPool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		slice := make([]byte, utils.FILE_READ_BUFFER_SIZE)
		return &slice
	},
}

// Use zstd.WithDecoderConcurrency(0) so that it can have GOMAXPROCS goroutines.
// If this option is not given it defaults to 4 or GOMAXPROCS, whichever is
// smaller.
var decoder, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(0))

type SegmentFileReader struct {
	ColName       string   // column name this file references
	fileName      string   // file name to iterate
	currFD        *os.File // current file descriptor
	blockMetadata map[uint16]*structs.BlockMetadataHolder

	currBlockNum             uint16
	currRecordNum            uint16
	currOffset               uint32
	currUncompressedBlockLen uint32
	currRecLen               uint32

	isBlockLoaded        bool
	currFileBuffer       []byte   // buffer re-used for file reads values
	currUncompressBuffer []byte   // buffer for zstd uncompress
	currRawBlockBuffer   []byte   // raw uncompressed block
	encType              uint8    // encoding type for this block
	deTlv                [][]byte // deTlv[dWordIdx] --> []byte (the TLV byte slice)
	deRecToTlv           []uint16 // deRecToTlv[recNum] --> dWordIdx
	blockSummaries       []*structs.BlockSummary
}

// returns a new SegmentFileReader and any errors encountered
// The returned SegmentFileReader must call .Close() when finished using it to close the fd
func InitNewSegFileReader(fd *os.File, colName string, blockMetadata map[uint16]*structs.BlockMetadataHolder,
	qid uint64, blockSummaries []*structs.BlockSummary) (*SegmentFileReader, error) {
	return &SegmentFileReader{
		ColName:              colName,
		fileName:             fd.Name(),
		currFD:               fd,
		blockMetadata:        blockMetadata,
		currOffset:           0,
		currFileBuffer:       *fileReadBufferPool.Get().(*[]byte),
		currUncompressBuffer: *uncompressedReadBufferPool.Get().(*[]byte),
		isBlockLoaded:        false,
		encType:              255,
		blockSummaries:       blockSummaries,
		deTlv:                make([][]byte, 0),
		deRecToTlv:           make([]uint16, 0),
	}, nil
}

func (sfr *SegmentFileReader) Close() error {
	if sfr.currFD == nil {
		return errors.New("tried to close an unopened segment file reader")
	}
	sfr.returnBuffers()
	return sfr.currFD.Close()
}

func (sfr *SegmentFileReader) returnBuffers() {
	uncompressedReadBufferPool.Put(&sfr.currRawBlockBuffer)
	fileReadBufferPool.Put(&sfr.currFileBuffer)
}

// returns a bool indicating if blockNum is valid, and any error encountered
func (sfr *SegmentFileReader) readBlock(blockNum uint16) (bool, error) {
	validBlock, err := sfr.loadBlockUsingBuffer(blockNum)
	if err != nil {
		log.Errorf("readBlock: error trying to read block %v in file %s. Error: %+v",
			blockNum, sfr.fileName, err)
		return true, err
	}
	if !validBlock {
		return false, fmt.Errorf("column does not exist in block")
	}

	sfr.currBlockNum = blockNum
	sfr.isBlockLoaded = true
	return true, nil
}

// helper function to decompresses and loads block using passed buffers
// returns the raw buffer, if the block is valid, and any error encountered
// The block will not be valid if the column is not found in block metadata. This means that the column never existed for this block and only existed for other blocks
func (sfr *SegmentFileReader) loadBlockUsingBuffer(blockNum uint16) (bool, error) {

	blockMetata, blockExists := sfr.blockMetadata[blockNum]
	if !blockExists {
		return true, errors.New("block  number does not exist for this segment file reader")
	}
	colBlockLen, colExists := blockMetata.ColumnBlockLen[sfr.ColName]
	if !colExists {
		// This is an invalid block & not an error because this column never existed for this block if sfr.blockMetadata[blockNum] exists
		return false, nil
	}

	colBlockOffset, colExists := blockMetata.ColumnBlockOffset[sfr.ColName]
	if !colExists {
		return false, nil
	}

	if uint32(len(sfr.currFileBuffer)) < colBlockLen {
		newArr := make([]byte, colBlockLen-uint32(len(sfr.currFileBuffer)))
		sfr.currFileBuffer = append(sfr.currFileBuffer, newArr...)
	}
	_, err := sfr.currFD.ReadAt(sfr.currFileBuffer[:colBlockLen], colBlockOffset)
	if err != nil {
		log.Errorf("loadBlockUsingBuffer read file error: %+v", err)
		return true, err
	}
	oPtr := uint32(0)
	sfr.encType = sfr.currFileBuffer[oPtr]
	oPtr++

	if sfr.encType == utils.ZSTD_COMLUNAR_BLOCK[0] {
		err := sfr.unpackRawCsg(sfr.currFileBuffer[oPtr:colBlockLen], blockNum)
		return true, err
	} else if sfr.encType == utils.ZSTD_DICTIONARY_BLOCK[0] {
		err := sfr.readDictEnc(sfr.currFileBuffer[oPtr:colBlockLen], blockNum)
		return true, err
	} else {
		log.Errorf("received an unknown encoding type for %v column! expected zstd or dictenc got %+v",
			sfr.ColName, sfr.encType)
		return true, fmt.Errorf("received an unknown encoding type for %v column! expected zstd or dictenc got %+v",
			sfr.ColName, sfr.encType)
	}
}

// returns the raw bytes of the blockNum:recordNum combination in the current segfile
// optimized for subsequent calls to have the same blockNum
// returns : encodedVal, error
func (sfr *SegmentFileReader) ReadRecordFromBlock(blockNum uint16, recordNum uint16) ([]byte, error) {

	if !sfr.isBlockLoaded || sfr.currBlockNum != blockNum {
		valid, err := sfr.readBlock(blockNum)
		if !valid {
			return nil, err
		}
		if err != nil {
			log.Errorf("ReadRecordFromBlock: error loading blockNum: %v. Error: %+v", blockNum, err)
			return nil, err
		}
	}

	// if dict encoding, we use the dictmapping
	if sfr.encType == utils.ZSTD_DICTIONARY_BLOCK[0] {
		ret, err := sfr.deGetRec(recordNum)
		return ret, err
	}

	if sfr.currRecordNum > recordNum {
		// we have to start offset over and iterate until we reach recordNum bc we do not how to go backwards in a block
		sfr.currOffset = 0
		currRecLen, err := sfr.getCurrentRecordLength()
		if err != nil {
			log.Errorf("ReadRecordFromBlock: error resetting SegmentFileReader %s. Error: %+v",
				sfr.fileName, err)
			return nil, err
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

	errStr := fmt.Sprintf("ReadRecordFromBlock: reached end of block before matching recNum %+v, blockNum %+v, Currently at rec %+v. File %+v, colname %v", recordNum, blockNum,
		sfr.currRecordNum, sfr.fileName, sfr.ColName)
	log.Error(errStr)
	log.Errorf("Current offset %+v, blkLen: %+v", sfr.currOffset, sfr.currUncompressedBlockLen)
	return nil, errors.New(errStr)
}

// returns the new record number and if any errors are encountered
// an error will be returned if no more records are available
func (sfr *SegmentFileReader) iterateNextRecord() error {
	nextOff := sfr.currOffset + sfr.currRecLen
	if nextOff >= sfr.currUncompressedBlockLen {
		log.Errorf("iterateNextRecord: reached end of block next Offset:%+v, curr uncompressed blklen: %+v", nextOff, sfr.currUncompressedBlockLen)
		return errors.New("no more records to iterate")
	}
	sfr.currOffset = nextOff
	currRecLen, err := sfr.getCurrentRecordLength()
	if err != nil {
		log.Errorf("iterateNextRecord: an error occurred while iterating to the next record %+v. Skipping...", err)
		sfr.currOffset -= sfr.currRecLen
		return err
	}
	sfr.currRecLen = currRecLen
	sfr.currRecordNum = sfr.currRecordNum + 1
	return nil
}

func (sfr *SegmentFileReader) getCurrentRecordLength() (uint32, error) {
	var reclen uint32
	switch sfr.currRawBlockBuffer[sfr.currOffset] {
	case utils.VALTYPE_ENC_SMALL_STRING[0]:
		// 1 byte for type, 2 for str-len, then str-len for actual string
		reclen = 3 + uint32(toputils.BytesToUint16LittleEndian(sfr.currRawBlockBuffer[sfr.currOffset+1:]))
	case utils.VALTYPE_ENC_BOOL[0]:
		reclen = 2
	case utils.VALTYPE_ENC_INT8[0]:
		reclen = 2
	case utils.VALTYPE_ENC_INT16[0]:
		reclen = 3
	case utils.VALTYPE_ENC_INT32[0]:
		reclen = 5
	case utils.VALTYPE_ENC_INT64[0]:
		reclen = 9
	case utils.VALTYPE_ENC_UINT8[0]:
		reclen = 2
	case utils.VALTYPE_ENC_UINT16[0]:
		reclen = 3
	case utils.VALTYPE_ENC_UINT32[0]:
		reclen = 5
	case utils.VALTYPE_ENC_UINT64[0]:
		reclen = 9
	case utils.VALTYPE_ENC_FLOAT64[0]:
		reclen = 9
	case utils.VALTYPE_ENC_BACKFILL[0]:
		reclen = 1
	case utils.VALTYPE_DICT_ARRAY[0]:
		reclen = 3 + uint32(toputils.BytesToUint16LittleEndian(sfr.currRawBlockBuffer[sfr.currOffset+1:]))
	case utils.VALTYPE_RAW_JSON[0]:
		reclen = 3 + uint32(toputils.BytesToUint16LittleEndian(sfr.currRawBlockBuffer[sfr.currOffset+1:]))

	default:
		log.Errorf("getCurrentRecordLength: Received an unknown encoding type %+v at offset %+v", sfr.currRawBlockBuffer[sfr.currOffset], sfr.currOffset)
		return 0, errors.New("received an unknown encoding type")
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
			log.Errorf("IsBlkDictEncoded: error loading blockNum: %v. Error: %+v", blockNum, err)
			return false, err
		}
	}

	if sfr.encType != utils.ZSTD_DICTIONARY_BLOCK[0] {
		return false, nil
	}

	return true, nil
}

func (sfr *SegmentFileReader) readDictEnc(buf []byte, blockNum uint16) error {

	idx := uint32(0)

	// read num of dict words
	numWords := toputils.BytesToUint16LittleEndian(buf[idx : idx+2])
	idx += 2

	if uint16(len(sfr.deTlv)) < numWords {
		extLen := numWords - uint16(len(sfr.deTlv))
		newArr := make([][]byte, extLen)
		sfr.deTlv = append(sfr.deTlv, newArr...)
	}

	if uint16(len(sfr.deRecToTlv)) < sfr.blockSummaries[blockNum].RecCount {
		extLen := sfr.blockSummaries[blockNum].RecCount - uint16(len(sfr.deRecToTlv))
		newArr := make([]uint16, extLen)
		sfr.deRecToTlv = append(sfr.deRecToTlv, newArr...)
	}

	var numRecs uint16
	var soffW uint32
	for w := uint16(0); w < numWords; w++ {

		soffW = idx
		// read dictWord 'T'
		switch buf[idx] {
		case utils.VALTYPE_ENC_SMALL_STRING[0]:
			//  3 => 1 for 'T' and 2 for 'L' of string
			idx += uint32(3 + toputils.BytesToUint16LittleEndian(buf[idx+1:idx+3]))
		case utils.VALTYPE_ENC_INT64[0], utils.VALTYPE_ENC_FLOAT64[0]:
			idx += 9 // 1 for T and 8 bytes for 'L' int64
		case utils.VALTYPE_ENC_BACKFILL[0]:
			idx += 1 // 1 for T
		default:
			return fmt.Errorf("readDictEnc unknown dictEnc: %v only supported flt/int64/str", buf[idx])
		}

		sfr.deTlv[w] = buf[soffW:idx]

		// read num of records
		numRecs = toputils.BytesToUint16LittleEndian(buf[idx : idx+2])
		idx += 2

		for i := uint16(0); i < numRecs; i++ {
			// at this recNum's position in the array store the idx of the TLV byte slice
			sfr.deRecToTlv[toputils.BytesToUint16LittleEndian(buf[idx:idx+2])] = w
			idx += 2
		}
	}

	return nil
}

func (sfr *SegmentFileReader) unpackRawCsg(buf []byte, blockNum uint16) error {

	uncompressed, err := decoder.DecodeAll(buf[0:], sfr.currUncompressBuffer[:0])
	if err != nil {
		log.Errorf("unpackRawCsg decompress error: %+v", err)
		return err
	}
	sfr.currRawBlockBuffer = uncompressed
	sfr.currOffset = 0

	currRecLen, err := sfr.getCurrentRecordLength()
	if err != nil {
		log.Errorf("unpackRawCsg: error getting record length for the first record in block %v in file %s. Error: %+v",
			blockNum, sfr.fileName, err)
		return err
	}
	sfr.currRecLen = currRecLen
	sfr.currRecordNum = 0
	sfr.currUncompressedBlockLen = uint32(len(sfr.currRawBlockBuffer))

	return nil
}

func (sfr *SegmentFileReader) GetDictEncCvalsFromColFile(results map[uint16]map[string]interface{},
	blockNum uint16, orderedRecNums []uint16) bool {

	if !sfr.isBlockLoaded || sfr.currBlockNum != blockNum {
		valid, err := sfr.readBlock(blockNum)
		if !valid {
			return false
		}
		if err != nil {
			return false
		}
	}

	if sfr.encType != utils.ZSTD_DICTIONARY_BLOCK[0] {
		return false
	}

	return sfr.deToResults(results, orderedRecNums)
}

func (sfr *SegmentFileReader) deToResults(results map[uint16]map[string]interface{},
	orderedRecNums []uint16) bool {

	for _, rn := range orderedRecNums {
		dwIdx := sfr.deRecToTlv[rn]
		dWord := sfr.deTlv[dwIdx]
		_, ok := results[rn]
		if !ok {
			results[rn] = make(map[string]interface{})
		}
		if dWord[0] == utils.VALTYPE_ENC_SMALL_STRING[0] {
			results[rn][sfr.ColName] = string(dWord[3:])
		} else if dWord[0] == utils.VALTYPE_ENC_INT64[0] {
			results[rn][sfr.ColName] = toputils.BytesToInt64LittleEndian(dWord[1:])
		} else if dWord[0] == utils.VALTYPE_ENC_FLOAT64[0] {
			results[rn][sfr.ColName] = toputils.BytesToFloat64LittleEndian(dWord[1:])
		} else if dWord[0] == utils.VALTYPE_ENC_BACKFILL[0] {
			results[rn][sfr.ColName] = nil
		} else {
			log.Errorf("deToResults: de only supported for str/int64/float64")
			return false
		}
	}
	return true
}

func (sfr *SegmentFileReader) deGetRec(rn uint16) ([]byte, error) {

	if rn >= uint16(len(sfr.deRecToTlv)) {
		return nil, fmt.Errorf("recNum %+v does not exist, len=%+v", rn, len(sfr.deRecToTlv))
	}
	dwIdx := sfr.deRecToTlv[rn]
	dWord := sfr.deTlv[dwIdx]
	return dWord, nil
}
