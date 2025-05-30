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

package pqmr

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/bits-and-blooms/bitset"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type SegmentPQMRResults struct {
	allBlockResults map[uint16]*PQMatchResults
	accessLock      *sync.RWMutex
}

type PQMatchResults struct {
	b *bitset.BitSet
}

func CreatePQMatchResults(maxEntries uint) *PQMatchResults {
	retval := &PQMatchResults{}
	retval.b = bitset.New(maxEntries)
	return retval
}

func CreatePQMatchResultsFromBs(b *bitset.BitSet) *PQMatchResults {
	retval := &PQMatchResults{}
	retval.b = b
	return retval
}

func GetPQMRFileNameFromSegKey(segmentKey string, pqid string) string {
	return fmt.Sprintf("%v/pqmr/%v.pqmr", segmentKey, pqid)
}

func (pqmr *PQMatchResults) AddMatchedRecord(recNum uint) {
	pqmr.b.Set(recNum)
}

func (pqmr *PQMatchResults) DoesRecordMatch(recNum uint) bool {
	return pqmr.b.Test(recNum)
}

func (pqmr *PQMatchResults) ClearBit(recNum uint) {
	pqmr.b.Clear(recNum)
}

func (pqmr *PQMatchResults) FlipRange(start, end uint) {
	pqmr.b.FlipRange(start, end)
}

func (pqmr *PQMatchResults) ResetAll() {
	pqmr.b.ClearAll()
}

func (pqmr *PQMatchResults) InPlaceIntersection(compare *PQMatchResults) {
	pqmr.b.InPlaceIntersection(compare.b)
}

func (pqmr *PQMatchResults) InPlaceUnion(compare *PQMatchResults) {
	pqmr.b.InPlaceUnion(compare.b)
}

func (pqmr *PQMatchResults) Any() bool {
	return pqmr.b.Any()
}

func Clone(srcPqmr *PQMatchResults) *PQMatchResults {
	retval := &PQMatchResults{}
	retval.b = srcPqmr.b.Clone()
	return retval
}

func (pqmr *PQMatchResults) GetNumberOfBits() uint {
	return pqmr.b.Len()
}

func (pqmr *PQMatchResults) GetNumberOfSetBits() uint {
	return pqmr.b.Count()
}

func (pqmr *PQMatchResults) GetInMemSize() uint64 {
	return uint64(pqmr.b.BinaryStorageSize())
}

func (pqmr *PQMatchResults) All() bool {
	return pqmr.b.All()
}

func (pqmr *PQMatchResults) Copy() *PQMatchResults {
	return &PQMatchResults{
		b: pqmr.b.Clone(),
	}
}

func InitSegmentPQMResults() *SegmentPQMRResults {
	return &SegmentPQMRResults{
		allBlockResults: make(map[uint16]*PQMatchResults),
		accessLock:      &sync.RWMutex{},
	}
}

// Returns the PQMatchResults, and a boolean indicating whether if blkNum was found
// if bool is false, PQMatchResults is nil
func (spqmr *SegmentPQMRResults) GetBlockResults(blkNum uint16) (*PQMatchResults, bool) {
	spqmr.accessLock.RLock()
	pqmr, ok := spqmr.allBlockResults[blkNum]
	spqmr.accessLock.RUnlock()
	return pqmr, ok
}

// Returns a boolean indicating whether blkNum exists for the spqmr
func (spqmr *SegmentPQMRResults) DoesBlockExist(blkNum uint16) bool {
	spqmr.accessLock.RLock()
	_, ok := spqmr.allBlockResults[blkNum]
	spqmr.accessLock.RUnlock()
	return ok
}

func (spqmr *SegmentPQMRResults) GetNumBlocks() uint16 {
	spqmr.accessLock.Lock()
	len := uint16(len(spqmr.allBlockResults))
	spqmr.accessLock.Unlock()
	return len
}

// returns all the blocks found in the spqmr
func (spqmr *SegmentPQMRResults) GetAllBlocks() []uint16 {
	i := 0
	spqmr.accessLock.Lock()
	retVal := make([]uint16, len(spqmr.allBlockResults))
	for blkNum := range spqmr.allBlockResults {
		retVal[i] = blkNum
		i++
	}
	spqmr.accessLock.Unlock()
	return retVal
}

// returns the size of the copy
func (spqmr *SegmentPQMRResults) CopyBlockResults(blkNum uint16, og *PQMatchResults) uint64 {

	spqmr.accessLock.Lock()
	new := bitset.New(og.b.Len())
	_ = og.b.Copy(new)
	spqmr.allBlockResults[blkNum] = &PQMatchResults{new}
	spqmr.accessLock.Unlock()
	return uint64(new.BinaryStorageSize())
}

// Sets the block results. This should only be used for testing
func (spqmr *SegmentPQMRResults) SetBlockResults(blkNum uint16, og *PQMatchResults) {
	spqmr.accessLock.Lock()
	spqmr.allBlockResults[blkNum] = og
	spqmr.accessLock.Unlock()
}

func (spqmr *SegmentPQMRResults) GetCopyOfBlockResults(blkNums []uint16) *SegmentPQMRResults {
	spqmr.accessLock.Lock()
	defer spqmr.accessLock.Unlock()

	new := &SegmentPQMRResults{
		allBlockResults: make(map[uint16]*PQMatchResults),
		accessLock:      &sync.RWMutex{},
	}
	for _, blkNum := range blkNums {
		if pqmr, ok := spqmr.allBlockResults[blkNum]; ok {
			new.allBlockResults[blkNum] = pqmr.Copy()
		}
	}

	return new
}

// [blkNum - uint16][bitSetLen - uint16][raw bitset….]
func (pqmr *PQMatchResults) FlushPqmr(fname *string, blkNum uint16) error {

	dirName := filepath.Dir(*fname)
	if _, err := os.Stat(dirName); os.IsNotExist(err) {
		err := os.MkdirAll(dirName, os.FileMode(0764))
		if err != nil {
			log.Errorf("FlushPqmr: Failed to create directory %s: Error=%v", dirName, err)
			return err
		}
	}
	fd, err := os.OpenFile(*fname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("FlushPqmr: open failed fname=%v, err=%v", *fname, err)
		return err
	}

	defer fd.Close()

	if _, err = fd.Write(utils.Uint16ToBytesLittleEndian(blkNum)); err != nil {
		log.Errorf("FlushPqmr: blkNum size write failed fname=%v, err=%v", *fname, err)
		return err
	}

	bytesWritten := uint16(pqmr.b.BinaryStorageSize())
	// copy the blockLen
	if _, err = fd.Write(utils.Uint16ToBytesLittleEndian(uint16(bytesWritten))); err != nil {
		log.Errorf("FlushPqmr: blklen write failed fname=%v, err=%v", *fname, err)
		return err
	}

	// copy the actual bitset
	_, err = pqmr.b.WriteTo(fd)
	if err != nil {
		log.Errorf("FlushPqmr: bitset write failed fname=%v, err=%v", *fname, err)
		return err
	}

	return nil
}

// read the pqmr file which has match results for each block
// return each of those pqmr blocks
func ReadPqmr(fname *string) (*SegmentPQMRResults, error) {

	res := make(map[uint16]*PQMatchResults)
	// todo pass the pre-alloced bsBlk so that we can reuse it, divide by 8 because one record takes 1 bit
	bsBlk := make([]byte, sutils.WIP_NUM_RECS/8)

	fd, err := os.OpenFile(*fname, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("ReadPqmr: open failed fname=%v, err=[%v]", *fname, err)
		return nil, err
	}
	defer fd.Close()

	bbBlkNum := make([]byte, sutils.LEN_BLKNUM_CMI_SIZE) // blkNum (2)
	bbBlkSize := make([]byte, sutils.LEN_PQMR_BLK_SIZE)
	offset := int64(0)
	var blkNum, bsSize uint16

	for {
		_, err = fd.ReadAt(bbBlkNum, offset)
		if err != nil {
			if err != io.EOF {
				log.Errorf("ReadPqmr: failed to read blkNum len err=[%+v]", err)
				return nil, err
			}
			break
		}
		offset += sutils.LEN_BLKNUM_CMI_SIZE
		blkNum = utils.BytesToUint16LittleEndian(bbBlkNum[:])

		_, err = fd.ReadAt(bbBlkSize, offset)
		if err != nil {
			if err != io.EOF {
				log.Errorf("ReadPqmr: failed to read bitsetSize len err=[%+v]", err)
				return nil, err
			}
			break
		}
		offset += sutils.LEN_PQMR_BLK_SIZE
		bsSize = utils.BytesToUint16LittleEndian(bbBlkSize[:])

		bsBlk = utils.ResizeSlice(bsBlk, int(bsSize))

		_, err = fd.ReadAt(bsBlk[:bsSize], offset)
		if err != nil {
			if err != io.EOF {
				log.Errorf("ReadPqmr: failed to read bitset err=[%+v]", err)
				return nil, err
			}
			break
		}
		offset += int64(bsSize)

		bs := bitset.New(0)
		err = bs.UnmarshalBinary(bsBlk[:bsSize])
		if err != nil {
			if err != io.EOF {
				log.Errorf("ReadPqmr: failed to unmarshall bitset err=[%+v] blkNum=%v", err, blkNum)
				return nil, err
			}
			break
		}

		pqmr := &PQMatchResults{b: bs}

		res[blkNum] = pqmr
	}

	return &SegmentPQMRResults{allBlockResults: res, accessLock: &sync.RWMutex{}}, nil
}

func (pqmr *PQMatchResults) Shrink(lastIdx uint) *PQMatchResults {
	retval := &PQMatchResults{}
	retval.b = pqmr.b.Shrink(lastIdx)
	return retval
}

func (pqmr *PQMatchResults) WriteTo(fd *os.File) error {
	_, err := pqmr.b.WriteTo(fd)
	return err
}

func (pqmr *PQMatchResults) EncodePqmr(buf []byte, blkNum uint16) (uint16, error) {
	var idx uint16
	// write blkNum
	utils.Uint16ToBytesLittleEndianInplace(blkNum, buf[idx:])
	idx += 2
	// write the size of bitset
	bitsetSize := uint16(pqmr.b.BinaryStorageSize())
	utils.Uint16ToBytesLittleEndianInplace(bitsetSize, buf[idx:])
	idx += 2
	// write actual bitset
	actualBitset, err := pqmr.b.MarshalBinary()
	if err != nil {
		log.Errorf("EncodePqmr: Error in encoding a BitSet into a binary form, err=%v", err)
		return idx, err
	}
	copy(buf[idx:], actualBitset)
	idx += uint16(len(actualBitset))
	return idx, nil

}

func WritePqmrToDisk(buf []byte, fileName string) error {
	dirName := filepath.Dir(fileName)
	if _, err := os.Stat(dirName); os.IsNotExist(err) {
		err := os.MkdirAll(dirName, os.FileMode(0764))
		if err != nil {
			log.Errorf("WritePqmrToDisk: Failed to create directory %s: %v", dirName, err)
			return err
		}
	}
	fd, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("WritePqmrToDisk: open failed fname=%v, err=%v", fileName, err)
		return err
	}

	defer fd.Close()

	_, err = fd.Write(buf)
	if err != nil {
		log.Errorf("WritePqmrToDisk: buf write failed fname=%v, err=%v", fileName, err)
		return err
	}

	err = fd.Sync()
	if err != nil {
		log.Errorf("WritePqmrToDisk: sync failed filename=%v,err=%v", fileName, err)
		return err
	}
	return nil
}
