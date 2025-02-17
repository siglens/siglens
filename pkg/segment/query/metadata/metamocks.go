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

package metadata

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	server_utils "github.com/siglens/siglens/pkg/server/utils"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
)

func InitMockColumnarMetadataStoreInternal(myid int64, indexName string, count int, numBlocks int, entryCount int) ([]string, error) {
	err := virtualtable.AddVirtualTable(&indexName, myid)
	if err != nil {
		return nil, fmt.Errorf("InitMockColumnarMetadataStore: AddVirtualTable failed err=%v", err)
	}

	vTableBaseDir, err := writer.GetMockVTableDirForTest(myid, indexName)
	if err != nil {
		return nil, fmt.Errorf("InitMockColumnarMetadataStore: GetMockVTableDirForTest failed err=%v", err)
	}

	segKeys := make([]string, count)

	for i := 0; i < count; i++ {
		segBaseDir := filepath.Join(vTableBaseDir, fmt.Sprint(i), "/")
		err := os.MkdirAll(segBaseDir, 0755)
		if err != nil {
			return nil, fmt.Errorf("InitMockColumnarMetadataStore: MkdirAll failed err=%v", err)
		}
		segkey := filepath.Join(segBaseDir, fmt.Sprint(i))
		bsumFname := segkey + ".bsu"
		colBlooms, blockSummaries, colRis, cnames, allBmh, allColsSizes := writer.WriteMockColSegFile(segBaseDir, segkey,
			numBlocks, entryCount)

		for colName := range cnames {
			fname := fmt.Sprintf("%v_%v.cmi", segkey, xxhash.Sum64String(colName))
			allBlooms := make([]*bloom.BloomFilter, len(colBlooms))

			var foundBlooms bool
			for blockNum, allBooms := range colBlooms {
				bloomIdx, ok := allBooms[colName]
				if !ok {
					continue
				}
				foundBlooms = true
				allBlooms[blockNum] = bloomIdx.Bf
			}
			if foundBlooms {
				writeMockBlockBloom(fname, allBlooms)
				continue
			}

			allRange := make([]map[string]*structs.Numbers, len(colBlooms))
			var foundRange bool
			for blockNum, allRanges := range colRis {
				rangeIndex, ok := allRanges[colName]
				if !ok {
					continue
				}
				foundRange = true
				allRange[blockNum] = rangeIndex.Ranges
			}
			if foundRange {
				writeMockBlockRI(fname, allRange)
				continue
			}
		}

		writeMockBlockSummary(allBmh, bsumFname, blockSummaries)

		sInfo := &structs.SegMeta{
			SegmentKey:       segkey,
			VirtualTableName: indexName,
			SegbaseDir:       segBaseDir,
			EarliestEpochMS:  0,
			LatestEpochMS:    uint64(entryCount),
			ColumnNames:      allColsSizes,
			NumBlocks:        uint16(numBlocks),
		}
		segMetadata := metadata.InitSegmentMicroIndex(sInfo, false)
		metadata.BulkAddSegmentMicroIndex([]*metadata.SegmentMicroIndex{segMetadata})

		writer.WriteRunningSegMeta(segkey, sInfo)

		segKeys[i] = segkey
	}

	return segKeys, nil
}

// function to init mock server in memory. Should only be called by tests
func InitMockColumnarMetadataStore(myid int64, indexName string, count int, numBlocks int, entryCount int) ([]string, error) {

	metadata.ResetGlobalMetadataForTest()

	writer.SetCardinalityLimit(1)

	err := virtualtable.InitVTable(server_utils.GetMyIds)
	if err != nil {
		return nil, fmt.Errorf("InitMockColumnarMetadataStore: InitVTable failed err=%v", err)
	}

	return InitMockColumnarMetadataStoreInternal(myid, indexName, count, numBlocks, entryCount)
}

func BulkInitMockColumnarMetadataStore(myids []int64, indexNames []string, count int, numBlocks int, entryCount int) (map[string]struct{}, error) {
	metadata.ResetGlobalMetadataForTest()

	writer.SetCardinalityLimit(1)

	err := virtualtable.InitVTable(server_utils.GetMyIds)
	if err != nil {
		return nil, fmt.Errorf("InitMockColumnarMetadataStore: InitVTable failed err=%v", err)
	}

	segKeysSet := make(map[string]struct{})

	for i, myid := range myids {
		indexName := indexNames[i]

		segKeysSlice, err := InitMockColumnarMetadataStoreInternal(myid, indexName, count, numBlocks, entryCount)
		if err != nil {
			return nil, fmt.Errorf("BulkInitMockColumnarMetadataStore: InitMockColumnarMetadataStore failed err=%v", err)
		}

		utils.AddSliceToSet(segKeysSet, segKeysSlice)
	}

	return segKeysSet, nil
}

func writeMockBlockBloom(file string, blockBlooms []*bloom.BloomFilter) {
	bffd, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("writeMockBlockBloom: open failed fname=%v, err=%v", file, err)
		return
	}

	defer bffd.Close()

	for blkNum, blockBloom := range blockBlooms {
		var buf bytes.Buffer
		bufWriter := bufio.NewWriter(&buf)
		bytesWritten, bferr := blockBloom.WriteTo(bufWriter)
		if bferr != nil {
			log.Errorf("writeMockBlockBloom: write buf failed fname=%v, err=%v", file, bferr)
			return
		}

		bytesWritten += segutils.LEN_BLKNUM_CMI_SIZE // for blkNum
		bytesWritten += 1                            // reserve for CMI type

		if _, err = bffd.Write(utils.Uint32ToBytesLittleEndian(uint32(bytesWritten))); err != nil {
			log.Errorf("writeMockBlockBloom: bloomsize write failed fname=%v, err=%v", file, err)
			return
		}

		// copy the blockNum
		if _, err = bffd.Write(utils.Uint16ToBytesLittleEndian(uint16(blkNum))); err != nil {
			log.Errorf("writeMockBlockBloom: bloomsize write failed fname=%v, err=%v", file, err)
			return
		}

		// write CMI type
		if _, err = bffd.Write(segutils.CMI_BLOOM_INDEX); err != nil {
			log.Errorf("writeMockBlockBloom: CMI Type write failed fname=%v, err=%v", file, err)
			return
		}

		_, bferr = blockBloom.WriteTo(bffd)
		if bferr != nil {
			log.Errorf("flushBlockBloom: write blockbloom failed fname=%v, err=%v", file, bferr)
			return
		}
	}
}

func writeMockBlockRI(file string, blockRange []map[string]*structs.Numbers) {
	bffd, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("writeMockBlockBloom: open failed fname=%v, err=%v", file, err)
		return
	}

	defer bffd.Close()

	for blkNum, blockRI := range blockRange {
		packedLen, blkRIBuf, err := writer.EncodeRIBlock(blockRI, uint16(blkNum))
		if err != nil {
			log.Errorf("writeMockBlockRI: EncodeRIBlock: Failed to encode BlockRangeIndex=%+v, err=%v", blockRI, err)
			return
		}
		if _, err := bffd.Write(blkRIBuf[0:packedLen]); err != nil {
			log.Errorf("writeMockBlockRI:  write failed blockRangeIndexFname=%v, err=%v", file, err)
			return
		}
	}
}

func writeMockBlockSummary(allbmh map[uint16]*structs.BlockMetadataHolder, file string,
	blockSums []*structs.BlockSummary) {

	fd, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("flushBlockSummary: open failed blockSummaryFname=%v, err=%v", file, err)
		return
	}

	defer fd.Close()

	for blkNum, block := range blockSums {
		blkSumBuf := make([]byte, segutils.BLOCK_SUMMARY_SIZE)
		packedLen, blkSumBuf, err := writer.EncodeBlocksum(allbmh[uint16(blkNum)], block,
			blkSumBuf[0:], uint16(blkNum))

		if err != nil {
			log.Errorf("writeMockBlockSummary: EncodeBlocksum: Failed to encode blocksummary=%+v, err=%v", block, err)
			return
		}
		if _, err := fd.Write(blkSumBuf[:packedLen]); err != nil {
			log.Errorf("WriteBlockSummary:  write failed blockSummaryFname=%v, err=%v", file, err)
			return
		}
	}
	err = fd.Sync()
	if err != nil {
		log.Fatal(err)
	}
}
