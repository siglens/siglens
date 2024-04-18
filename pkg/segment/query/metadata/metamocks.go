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
	"sync"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// function to init mock server in memory. Should only be called by tests
func InitMockColumnarMetadataStore(dir string, count int, numBlocks int, entryCount int) {

	_ = os.Remove(dir)

	globalMetadata = &allSegmentMetadata{
		allSegmentMicroIndex:        make([]*SegmentMicroIndex, 0),
		segmentMetadataReverseIndex: make(map[string]*SegmentMicroIndex),
		tableSortedMetadata:         make(map[string][]*SegmentMicroIndex),
		updateLock:                  &sync.RWMutex{},
	}

	writer.SetCardinalityLimit(1)
	err := os.MkdirAll(dir, os.FileMode(0755))
	if err != nil {
		log.Fatalf("InitMockColumnarMetadataStore: Could not create directory %v", err)
	}
	for i := 0; i < count; i++ {
		segkey := dir + "query_test_" + fmt.Sprint(i)
		bsumFname := dir + "query_test_" + fmt.Sprint(i) + ".bsu"
		colBlooms, blockSummaries, colRis, cnames, allBmh, allColsSizes := writer.WriteMockColSegFile(segkey,
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
			VirtualTableName: "evts",
			SegbaseDir:       segkey, // its actually one dir up, but for mocks its fine
			EarliestEpochMS:  0,
			LatestEpochMS:    uint64(entryCount),
			ColumnNames:      allColsSizes,
			NumBlocks:        uint16(numBlocks),
		}
		segMetadata := InitSegmentMicroIndex(sInfo)
		BulkAddSegmentMicroIndex([]*SegmentMicroIndex{segMetadata})
	}
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
