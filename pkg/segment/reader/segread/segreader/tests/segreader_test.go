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

package tests

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/segment/reader/microreader"
	"github.com/siglens/siglens/pkg/segment/reader/segread/segreader"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Benchmark_readColumnarFile(b *testing.B) {
	segKey := "/Users/ssubramanian/Desktop/SigLens/siglens/data/Sris-MacBook-Pro.local/final/2022/02/21/01/valtix2/10005995996882630313/0"
	sumFile := structs.GetBsuFnameFromSegKey(segKey)

	numRecsPerBlock := make(map[uint16]uint16)
	maxRecReadInBlock := make(map[uint16]uint16)
	blockSums, allBmi, err := microreader.ReadBlockSummaries(sumFile, false)
	assert.Nil(b, err)

	for idx, bSum := range blockSums {
		numRecsPerBlock[uint16(idx)] = bSum.RecCount
	}

	allBlocksToSearch := utils.MapToSet(allBmi.AllBmh)

	colName := "device_type"

	colCSG := fmt.Sprintf("%s_%v.csg", segKey, xxhash.Sum64String(colName))
	fd, err := os.Open(colCSG)
	assert.NoError(b, err)
	fileReader, err := segreader.InitNewSegFileReader(fd, colName, allBlocksToSearch, 0, blockSums, sutils.INCONSISTENT_CVAL_SIZE, allBmi)
	assert.Nil(b, err)

	b.ResetTimer()
	failedBlocks := make(map[uint16]bool)

	sTime := time.Now()
	numRead := 0
	for blkNum := range allBlocksToSearch {
		for i := uint16(0); i < numRecsPerBlock[blkNum]; i++ {
			rawRec, err := fileReader.ReadRecord(i)
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

	cname := "muycname"
	colWip := writer.InitColWip("mysegkey", cname)

	deCount := uint16(100)

	deMap := make(map[string][]uint16)

	recCounts := uint16(100)

	allBlockSummaries := make([]*structs.BlockSummary, 1)
	allBlockSummaries[0] = &structs.BlockSummary{RecCount: recCounts}

	sfr, err := segreader.InitNewSegFileReader(nil, cname, nil, 0, allBlockSummaries, sutils.INCONSISTENT_CVAL_SIZE, nil)
	assert.NoError(t, err)

	recNum := uint16(0)
	tempWipCbuf := make([]byte, 2_000_000)
	wipIdx := uint32(0)
	for dwIdx := uint16(0); dwIdx < deCount; dwIdx++ {

		cval := fmt.Sprintf("mycval-%v", dwIdx)
		cvalBytes := make([]byte, 3+len(cval))
		cvalBytes[0] = sutils.VALTYPE_ENC_SMALL_STRING[0]
		utils.Uint16ToBytesLittleEndianInplace(uint16(len(cval)), cvalBytes[1:])
		copy(cvalBytes[3:], cval)

		cvTlvLen := uint32(len(cvalBytes))

		copy(tempWipCbuf[wipIdx:], cvalBytes)
		wipIdx += cvTlvLen

		arr := make([]uint16, recCounts/deCount)
		deMap[string(cvalBytes)] = arr

		for rn := uint16(0); rn < recCounts/deCount; rn++ {
			arr[rn] = recNum + rn
		}
		recNum += recCounts / deCount
	}
	colWip.CopyWipForTestOnly(tempWipCbuf, wipIdx)
	colWip.SetDeDataForTest(deCount, deMap)

	writer.PackDictEnc(colWip, recCounts)
	buf, idx := colWip.GetBufAndIdx()

	err = sfr.ReadDictEnc(buf[0:idx], 0)
	assert.Nil(t, err)

	orderedRecNums := make([]uint16, recCounts)
	for i := uint16(0); i < recCounts; i++ {
		orderedRecNums[i] = i
	}

	results := make(map[uint16]map[string]interface{})
	_ = sfr.DeToResultOldPipeline(results, orderedRecNums)

	for rn, val := range results {
		dWord := val[cname]
		expected := fmt.Sprintf("mycval-%v", rn)
		assert.Equal(t, dWord, expected)
	}
}

func Test_readDictEncDiscardsOldData(t *testing.T) {
	encodeString := func(s string) []byte {
		encoding := make([]byte, 3+len(s))
		encoding[0] = sutils.VALTYPE_ENC_SMALL_STRING[0]
		utils.Uint16ToBytesLittleEndianInplace(uint16(len(s)), encoding[1:3])
		copy(encoding[3:], []byte(s))

		return encoding
	}

	encodeDict := func(strings []string, recordsWithValue [][]uint16) []byte {
		encoding := make([]byte, 0)
		encoding = append(encoding, utils.Uint16ToBytesLittleEndian(uint16(len(strings)))...)

		for i, s := range strings {
			encoding = append(encoding, encodeString(s)...)

			encoding = append(encoding, utils.Uint16ToBytesLittleEndian(uint16(len(recordsWithValue[i])))...)
			for _, rec := range recordsWithValue[i] {
				encoding = append(encoding, utils.Uint16ToBytesLittleEndian(rec)...)
			}
		}

		return encoding
	}

	block0RecordCount := uint16(8)
	block1RecordCount := uint16(5)
	blockSummaries := []*structs.BlockSummary{{RecCount: block0RecordCount}, {RecCount: block1RecordCount}}
	segFileReader, err := segreader.InitNewSegFileReader(nil, "", nil, 0, blockSummaries, 0, nil)
	assert.NoError(t, err)

	block0Strings := []string{"apple", "banana", "cherry"}
	err = segFileReader.ReadDictEnc(encodeDict(block0Strings, [][]uint16{[]uint16{0, 1, 2, 3}, []uint16{4, 5}, []uint16{6, 7}}), 0)
	assert.NoError(t, err)
	assert.Equal(t, len(block0Strings), len(segFileReader.GetDeTlv()))
	assert.Equal(t, block0RecordCount, uint16(len(segFileReader.GetDeRecToTlv())))

	block1Strings := []string{"alphabet", "zebra"}
	err = segFileReader.ReadDictEnc(encodeDict(block1Strings, [][]uint16{[]uint16{0, 3, 4}, []uint16{1, 2}}), 1)
	assert.NoError(t, err)
	assert.Equal(t, len(block1Strings), len(segFileReader.GetDeTlv()))
	assert.Equal(t, block1RecordCount, uint16(len(segFileReader.GetDeRecToTlv())))
}
