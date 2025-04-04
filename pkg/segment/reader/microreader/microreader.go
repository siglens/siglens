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

	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"

	log "github.com/sirupsen/logrus"
)

const SECONDS_REREAD_META = 10

func ReadBlockSummaries(fileName string, rbuf []byte) ([]*structs.BlockSummary,
	map[uint16]*structs.BlockMetadataHolder, []byte, error) {

	blockSummaries := make([]*structs.BlockSummary, 0)
	allBmh := make(map[uint16]*structs.BlockMetadataHolder)
	err := blob.DownloadSegmentBlob(fileName, false)
	if err != nil {
		log.Errorf("ReadBlockSummaries: Error downloading block summary file at %s, err: %v", fileName, err)
		return blockSummaries, allBmh, rbuf, err
	}

	finfo, err := os.Stat(fileName)
	if err != nil {
		log.Errorf("ReadBlockSummaries: error when trying to stat file=%+v. Error=%+v", fileName, err)
		return blockSummaries, allBmh, rbuf, err
	}

	fileSize := finfo.Size()
	rbuf = toputils.ResizeSlice(rbuf, int(fileSize))

	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		log.Infof("ReadBlockSummaries: failed to open fileName: %v  Error: %v.",
			fileName, err)
		return blockSummaries, allBmh, rbuf, err
	}
	defer fd.Close()

	_, err = fd.ReadAt(rbuf[:fileSize], 0)
	if err != nil {
		log.Errorf("ReadBlockSummaries: Error reading bsu file: %v, err: %v", fileName, err)
		return blockSummaries, allBmh, rbuf, err
	}

	offset := int64(0)

	for offset < fileSize {

		// todo kunal do we need blksumlen ?
		offset += 4 // for blkSumLen

		if len(rbuf[offset:]) < 2+8+8+2+2 {
			log.Errorf("ReadBlockSummaries: expected at least %d more bytes for block header, got %d more bytes; file=%v, offset=%d",
				2+8+8+2+2, len(rbuf[offset:]), fileName, offset)
			return blockSummaries, allBmh, rbuf, errors.New("bad data")
		}

		// read blknum
		blkNum := toputils.BytesToUint16LittleEndian(rbuf[offset:])
		offset += 2

		// read highTs
		highTs := toputils.BytesToUint64LittleEndian(rbuf[offset:])
		offset += 8

		// read lowTs
		lowTs := toputils.BytesToUint64LittleEndian(rbuf[offset:])
		offset += 8

		// read recCount
		recCount := toputils.BytesToUint16LittleEndian(rbuf[offset:])
		offset += 2

		// read numCols
		numCols := toputils.BytesToUint16LittleEndian(rbuf[offset:])
		offset += 2
		bmh := &structs.BlockMetadataHolder{
			BlkNum:            blkNum,
			ColumnBlockOffset: make(map[string]int64, numCols),
			ColumnBlockLen:    make(map[string]uint32, numCols),
		}

		for i := uint16(0); i < numCols; i++ {
			if len(rbuf[offset:]) < 2 {
				log.Errorf("ReadBlockSummaries: expected at least %d more bytes for column name length, got %d more bytes; file=%v, offset=%d",
					2, len(rbuf[offset:]), fileName, offset)
				return blockSummaries, allBmh, rbuf, errors.New("bad data")
			}
			cnamelen := toputils.BytesToUint16LittleEndian(rbuf[offset:])
			offset += 2

			if minLen := int(offset + int64(cnamelen) + 12); len(rbuf) < minLen {
				log.Errorf("ReadBlockSummaries: expected at least size %d, got %d; file=%v, offset=%d",
					minLen, len(rbuf), fileName, offset)
				return blockSummaries, allBmh, rbuf, errors.New("bad data")
			}

			cname := string(rbuf[offset : offset+int64(cnamelen)])
			offset += int64(cnamelen)
			blkOff := toputils.BytesToInt64LittleEndian(rbuf[offset:])
			offset += 8
			blkLen := toputils.BytesToUint32LittleEndian(rbuf[offset:])
			offset += 4
			bmh.ColumnBlockOffset[cname] = blkOff
			bmh.ColumnBlockLen[cname] = blkLen
		}
		allBmh[blkNum] = bmh

		blkSumm := &structs.BlockSummary{HighTs: highTs,
			LowTs:    lowTs,
			RecCount: recCount}

		blockSummaries = append(blockSummaries, blkSumm)
	}

	return blockSummaries, allBmh, rbuf, nil
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
	if versionBlockSummary[0] != utils.VERSION_MBLOCKSUMMARY[0] {
		return mBlockSummaries, fmt.Errorf("ReadMetricsBlockSummaries: the file version doesn't match. Expected Version: %v, Got Version: %v", utils.VERSION_MBLOCKSUMMARY[0], versionBlockSummary[0])
	}
	offset := int64(1)
	for offset < fileSize {
		blkNum := toputils.BytesToUint16LittleEndian(data[offset:])
		offset += 2

		// todo fix bug here, the highTs/lowTs are only 4 bytes but we are reading 8
		// once the writer is switched to 4 bytes and version updated, do same here
		// read highTs
		highTs := toputils.BytesToUint32LittleEndian(data[offset:])
		offset += 8

		// read lowTs
		lowTs := toputils.BytesToUint32LittleEndian(data[offset:])
		offset += 8

		blkSumm := &structs.MBlockSummary{HighTs: highTs,
			LowTs:  lowTs,
			Blknum: blkNum}

		mBlockSummaries = append(mBlockSummaries, blkSumm)
	}

	return mBlockSummaries, nil
}
