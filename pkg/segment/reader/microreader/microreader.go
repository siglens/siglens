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

package microreader

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"unsafe"

	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"

	log "github.com/sirupsen/logrus"
)

const SECONDS_REREAD_META = 10

var internCnamesPool sync.Map // map[string]string

func internCnamesBytes(b []byte) string {
	tmp := *(*string)(unsafe.Pointer(&b))
	if val, ok := internCnamesPool.Load(tmp); ok {
		return val.(string)
	}
	// Force copy to make sure interned string is safe
	safe := string(b)
	internCnamesPool.Store(safe, safe)
	return safe
}

func ReadBlockSummaries(fileName string,
	summaryOnly bool) ([]*structs.BlockSummary, *structs.AllBlksMetaInfo, error) {

	blockSummaries := make([]*structs.BlockSummary, 0)
	var allBmi *structs.AllBlksMetaInfo

	if !summaryOnly {
		allBmi = &structs.AllBlksMetaInfo{CnameDict: make(map[string]int),
			AllBmh: make(map[uint16]*structs.BlockMetadataHolder),
		}
	}

	err := blob.DownloadSegmentBlob(fileName, false)
	if err != nil {
		log.Errorf("ReadBlockSummaries: Error downloading block summary file at %s, err: %v", fileName, err)
		return blockSummaries, allBmi, err
	}

	finfo, err := os.Stat(fileName)
	if err != nil {
		log.Errorf("ReadBlockSummaries: error when trying to stat file=%+v. Error=%+v", fileName, err)
		return blockSummaries, allBmi, err
	}

	fileSize := finfo.Size()
	rbuf := make([]byte, int(fileSize))

	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		log.Infof("ReadBlockSummaries: failed to open fileName: %v  Error: %v.",
			fileName, err)
		return blockSummaries, allBmi, err
	}
	defer fd.Close()

	_, err = fd.ReadAt(rbuf[:fileSize], 0)
	if err != nil {
		log.Errorf("ReadBlockSummaries: Error reading bsu file: %v, err: %v", fileName, err)
		return blockSummaries, allBmi, err
	}

	offset := int64(0)

	for offset < fileSize {

		// todo kunal do we need blksumlen ?
		offset += 4 // for blkSumLen

		if len(rbuf[offset:]) < 2+8+8+2+2 {
			log.Errorf("ReadBlockSummaries: expected at least %d more bytes for block header, got %d more bytes; file=%v, offset=%d",
				2+8+8+2+2, len(rbuf[offset:]), fileName, offset)
			return blockSummaries, allBmi, errors.New("bad data")
		}

		// read blknum
		blkNum := utils.BytesToUint16LittleEndian(rbuf[offset:])
		offset += 2

		// read highTs
		highTs := utils.BytesToUint64LittleEndian(rbuf[offset:])
		offset += 8

		// read lowTs
		lowTs := utils.BytesToUint64LittleEndian(rbuf[offset:])
		offset += 8

		// read recCount
		recCount := utils.BytesToUint16LittleEndian(rbuf[offset:])
		offset += 2

		// read numCols
		numCols := utils.BytesToUint16LittleEndian(rbuf[offset:])
		offset += 2

		var bmh *structs.BlockMetadataHolder
		if !summaryOnly {
			bmh = &structs.BlockMetadataHolder{
				BlkNum:            blkNum,
				ColBlockOffAndLen: make([]structs.ColOffAndLen, numCols),
			}
		}

		for i := uint16(0); i < numCols; i++ {
			if len(rbuf[offset:]) < 2 {
				log.Errorf("ReadBlockSummaries: expected at least %d more bytes for column name length, got %d more bytes; file=%v, offset=%d",
					2, len(rbuf[offset:]), fileName, offset)
				return blockSummaries, allBmi, errors.New("bad data")
			}
			cnamelen := utils.BytesToUint16LittleEndian(rbuf[offset:])
			offset += 2

			if minLen := int(offset + int64(cnamelen) + 12); len(rbuf) < minLen {
				log.Errorf("ReadBlockSummaries: expected at least size %d, got %d; file=%v, offset=%d",
					minLen, len(rbuf), fileName, offset)
				return blockSummaries, allBmi, errors.New("bad data")
			}

			if summaryOnly {
				offset += int64(cnamelen)
				offset += 8 + 4 // Blk Offset and Blk Len
			} else {
				cname := internCnamesBytes(rbuf[offset : offset+int64(cnamelen)])

				offset += int64(cnamelen)
				blkOff := utils.BytesToInt64LittleEndian(rbuf[offset:])
				offset += 8
				blkLen := utils.BytesToUint32LittleEndian(rbuf[offset:])
				offset += 4

				cnameIdx, ok := allBmi.CnameDict[cname]
				if !ok {
					cnameIdx = len(allBmi.CnameDict)
					allBmi.CnameDict[cname] = cnameIdx
				}

				// extend array in cases where we get new columns names that were
				// not there in previous blocks
				arrLen := len(bmh.ColBlockOffAndLen)
				if arrLen <= cnameIdx {
					bmh.ColBlockOffAndLen = append(bmh.ColBlockOffAndLen,
						make([]structs.ColOffAndLen, cnameIdx-arrLen+1)...)
				}
				bmh.ColBlockOffAndLen[cnameIdx] = structs.ColOffAndLen{Offset: blkOff,
					Length: blkLen,
				}
			}
		}
		if !summaryOnly {
			allBmi.AllBmh[blkNum] = bmh
		}

		blkSumm := &structs.BlockSummary{HighTs: highTs,
			LowTs:    lowTs,
			RecCount: recCount}

		blockSummaries = append(blockSummaries, blkSumm)
	}

	return blockSummaries, allBmi, nil
}

func ReadSegMeta(fname string) (*structs.SegMeta, error) {

	var sm structs.SegMeta
	rdata, err := os.ReadFile(fname)
	if err != nil {
		log.Errorf("ReadSegMeta: error reading file = %v, err= %v", fname, err)
		return nil, err

	}

	err = json.Unmarshal(rdata, &sm)
	if err != nil {
		log.Errorf("ReadSegMeta: Cannot unmarshal data = %v, err= %v", string(rdata), err)
		return nil, err
	}
	return &sm, nil
}

func ReadMetricsBlockSummaries(fileName string) ([]*structs.MBlockSummary, error) {
	mBlockSummaries := make([]*structs.MBlockSummary, 0)
	err := blob.DownloadSegmentBlob(fileName, false)
	if err != nil {
		log.Errorf("ReadMetricsBlockSummaries: Error downloading metrics block summary file at %s, err: %v", fileName, err)
		return mBlockSummaries, err
	}

	finfo, err := os.Stat(fileName)
	if err != nil {
		return mBlockSummaries, err
	}

	fileSize := finfo.Size()

	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		log.Infof("ReadMetricsBlockSummaries: failed to open fileName: %v  Error: %v.", fileName, err)
		return mBlockSummaries, err
	}
	defer fd.Close()

	data := make([]byte, fileSize)
	n, err := fd.Read(data)
	if err != nil {
		log.Errorf("ReadMetricsBlockSummaries: Error reading mbsu file: %v, err: %v", fileName, err)
		return mBlockSummaries, err
	}

	if n < 1 {
		log.Errorf("ReadMetricsBlockSummaries: Insufficient data in mbsu file: %v", fileName)
		return mBlockSummaries, errors.New("insufficient data in file")
	}

	versionBlockSummary := make([]byte, 1)
	copy(versionBlockSummary, data[:1])
	if versionBlockSummary[0] != sutils.VERSION_MBLOCKSUMMARY[0] {
		return mBlockSummaries, fmt.Errorf("ReadMetricsBlockSummaries: the file version doesn't match. Expected Version: %v, Got Version: %v", sutils.VERSION_MBLOCKSUMMARY[0], versionBlockSummary[0])
	}
	offset := int64(1)
	for offset < fileSize {
		blkNum := utils.BytesToUint16LittleEndian(data[offset:])
		offset += 2

		// todo fix bug here, the highTs/lowTs are only 4 bytes but we are reading 8
		// once the writer is switched to 4 bytes and version updated, do same here
		// read highTs
		highTs := utils.BytesToUint32LittleEndian(data[offset:])
		offset += 8

		// read lowTs
		lowTs := utils.BytesToUint32LittleEndian(data[offset:])
		offset += 8

		blkSumm := &structs.MBlockSummary{HighTs: highTs,
			LowTs:  lowTs,
			Blknum: blkNum}

		mBlockSummaries = append(mBlockSummaries, blkSumm)
	}

	return mBlockSummaries, nil
}
