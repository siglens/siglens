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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/reader/microreader"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_segReader(t *testing.T) {

	segDir := "data/"
	_ = os.MkdirAll(segDir, 0755)
	segKey := segDir + "test"
	numBlocks := 10
	numEntriesInBlock := 10
	_, bsm, _, cols, blockmeta, _ := writer.WriteMockColSegFile(segKey, numBlocks, numEntriesInBlock)

	assert.Greater(t, len(cols), 1)
	var queryCol string

	// test across multiple columns types
	for queryCol = range cols {
		if queryCol == config.GetTimeStampKey() {
			continue // ingore ts
		}
		fileName := fmt.Sprintf("%s_%v.csg", segKey, xxhash.Sum64String(queryCol))

		log.Infof("testing with %s", fileName)
		fd, err := os.Open(fileName)
		assert.NoError(t, err)
		sReader, err := InitNewSegFileReader(fd, queryCol, blockmeta, 0, bsm)
		assert.Nil(t, err)

		// invalid block
		_, err = sReader.ReadRecordFromBlock(uint16(numBlocks), uint16(numEntriesInBlock))
		assert.NotNil(t, err)
		// correct block, incorrect recordNum
		_, err = sReader.ReadRecordFromBlock(0, uint16(numEntriesInBlock))
		assert.NotNil(t, err, "col %s should not have %+v entries", queryCol, numEntriesInBlock+1)

		// correct block, correct recordNum
		arr, err := sReader.ReadRecordFromBlock(0, uint16(numEntriesInBlock-3))
		assert.Nil(t, err)
		assert.NotNil(t, arr)
		cVal, _, err := writer.GetCvalFromRec(arr, 23)
		assert.Nil(t, err)
		assert.NotNil(t, cVal)
		log.Infof("GetCvalFromRec: %+v for column %s", cVal, queryCol)

		err = sReader.Close()
		assert.Nil(t, err)
	}

	os.RemoveAll(segDir)
}

func Test_timeReader(t *testing.T) {

	config.InitializeTestingConfig()
	segDir := "data/"
	_ = os.MkdirAll(segDir, 0755)
	segKey := segDir + "test-time"
	numBlocks := 10
	numEntriesInBlock := 10
	_, bSum, _, cols, blockmeta, _ := writer.WriteMockColSegFile(segKey, numBlocks, numEntriesInBlock)

	assert.Greater(t, len(cols), 1)
	timeReader, err := InitNewTimeReaderFromBlockSummaries(segKey, config.GetTimeStampKey(), blockmeta, bSum, 0)
	assert.Nil(t, err)

	// test across multiple columns types
	for blockNum := 0; blockNum < numBlocks; blockNum++ {
		currRecs, err := timeReader.GetAllTimeStampsForBlock(uint16(blockNum))
		assert.Nil(t, err)
		assert.Len(t, currRecs, numEntriesInBlock)

		startTs := uint64(1)
		for _, readTs := range currRecs {
			assert.Equal(t, startTs, readTs)
			startTs++
		}
	}
	os.RemoveAll(segDir)
}

func Benchmark_readColumnarFile(b *testing.B) {
	segKey := "/Users/ssubramanian/Desktop/SigLens/siglens/data/Sris-MacBook-Pro.local/final/2022/02/21/01/valtix2/10005995996882630313/0"
	sumFile := structs.GetBsuFnameFromSegKey(segKey)

	numRecsPerBlock := make(map[uint16]uint16)
	maxRecReadInBlock := make(map[uint16]uint16)
	blockSums, allBlockInfo, _, err := microreader.ReadBlockSummaries(sumFile, []byte{})
	assert.Nil(b, err)

	for idx, bSum := range blockSums {
		numRecsPerBlock[uint16(idx)] = bSum.RecCount
	}

	colName := "device_type"

	colCSG := fmt.Sprintf("%s_%v.csg", segKey, xxhash.Sum64String(colName))
	fd, err := os.Open(colCSG)
	assert.NoError(b, err)
	fileReader, err := InitNewSegFileReader(fd, colName, allBlockInfo, 0, blockSums)
	assert.Nil(b, err)

	b.ResetTimer()
	failedBlocks := make(map[uint16]bool)

	sTime := time.Now()
	numRead := 0
	for blkNum := range allBlockInfo {
		for i := uint16(0); i < numRecsPerBlock[blkNum]; i++ {
			rawRec, err := fileReader.ReadRecordFromBlock(blkNum, i)
			numRead++
			assert.Nil(b, err)
			assert.NotNil(b, rawRec)
			if err != nil {
				log.Errorf("Failed to read rec %+d from block %d: %v", i, blkNum, err)
				failedBlocks[blkNum] = true
				break
			}
			maxRecReadInBlock[blkNum] = i
		}
	}

	log.Infof("Read %+v records in %v", numRead, time.Since(sTime))
	err = fileReader.Close()
	assert.Nil(b, err)
}

func Test_packUnpackDictEnc(t *testing.T) {

	colWip := &writer.ColWip{}
	deCount := uint16(100)

	deMap := make(map[string][]uint16)
	recCounts := uint16(100)

	allBlockSummaries := make([]*structs.BlockSummary, 1)
	allBlockSummaries[0] = &structs.BlockSummary{RecCount: recCounts}

	cname := "muycname"
	sfr := &SegmentFileReader{
		blockSummaries: allBlockSummaries,
		deTlv:          make([][]byte, 0),
		deRecToTlv:     make([]uint16, 0),
		currBlockNum:   0,
		ColName:        cname,
	}

	recNum := uint16(0)
	for dwIdx := uint16(0); dwIdx < deCount; dwIdx++ {

		cval := fmt.Sprintf("mycval-%v", dwIdx)
		cvalBytes := make([]byte, 3+len(cval))
		cvalBytes[0] = segutils.VALTYPE_ENC_SMALL_STRING[0]
		copy(cvalBytes[1:], utils.Uint16ToBytesLittleEndian(uint16(len(cval))))
		copy(cvalBytes[3:], cval)

		arr := make([]uint16, recCounts/deCount)
		deMap[string(cvalBytes)] = arr
		for rn := uint16(0); rn < recCounts/deCount; rn++ {
			arr[rn] = recNum + rn
		}
		recNum += recCounts / deCount
	}

	colWip.SetDeCount(deCount)
	colWip.SetDeMap(deMap)

	writer.PackDictEnc(colWip)
	buf, idx := colWip.GetBufAndIdx()

	err := sfr.readDictEnc(buf[0:idx], 0)
	assert.Nil(t, err)

	orderedRecNums := make([]uint16, recCounts)
	for i := uint16(0); i < recCounts; i++ {
		orderedRecNums[i] = i
	}

	results := make(map[uint16]map[string]interface{})
	_ = sfr.deToResults(results, orderedRecNums)

	for rn, val := range results {
		dWord := val[cname]
		expected := fmt.Sprintf("mycval-%v", rn)
		assert.Equal(t, dWord, expected)
	}
}
