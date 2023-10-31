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

package microreader

import (
	"encoding/json"
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
	sizeToAdd := fileSize - int64(len(rbuf))
	if sizeToAdd > 0 {
		newArr := make([]byte, sizeToAdd)
		rbuf = append(rbuf, newArr...)
	}

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
			cnamelen := toputils.BytesToUint16LittleEndian(rbuf[offset:])
			offset += 2
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
		log.Errorf("Cannot unmarshal data = %v, err= %v", string(rdata), err)
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
		log.Infof("ReadMetricsBlockSummaries: failed to open fileName: %v  Error: %v.",
			fileName, err)
		return mBlockSummaries, err
	}
	defer fd.Close()

	data := make([]byte, fileSize)
	_, err = fd.Read(data)
	if err != nil {
		log.Errorf("ReadMetricsBlockSummaries: Error reading mbsu file: %v, err: %v", fileName, err)
		return mBlockSummaries, err
	}

	versionBlockSummary := make([]byte, 1)
	copy(versionBlockSummary, data[:1])
	if versionBlockSummary[0] != utils.VERSION_MBLOCKSUMMARY[0] {
		return mBlockSummaries, fmt.Errorf("ReadMetricsBlockSummaries: the file version doesn't match")
	}
	offset := int64(1)
	for offset < fileSize {
		blkNum := toputils.BytesToUint16LittleEndian(data[offset:])
		offset += 2

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
