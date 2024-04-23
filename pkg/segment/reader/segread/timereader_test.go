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
	"os"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/reader/microreader"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/writer"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_readTimeStamps(t *testing.T) {

	config.InitializeTestingConfig()
	segDir := "data/"
	_ = os.MkdirAll(segDir, 0755)
	segKey := segDir + "test"
	numBlocks := 10
	numEntriesInBlock := 20000
	_, blockSums, _, _, blockmeta, _ := writer.WriteMockColSegFile(segKey, numBlocks, numEntriesInBlock)

	colName := config.GetTimeStampKey()
	fileReader, err := InitNewTimeReaderFromBlockSummaries(segKey, colName, blockmeta, blockSums, 0)
	assert.Nil(t, err)

	totalRead := uint64(0)
	totalInSummaries := uint64(0)
	idx := 0
	for blkNum, bSum := range blockSums {
		allTime, err := fileReader.GetAllTimeStampsForBlock(uint16(blkNum))
		assert.Nil(t, err, "no errors should occur")
		assert.Equal(t, len(allTime), int(bSum.RecCount))
		log.Infof("block %+v has %+v records. Supposed to have %+v", blkNum, len(allTime), int(blockSums[idx].RecCount))
		totalRead += uint64(len(allTime))
		totalInSummaries += uint64(blockSums[idx].RecCount)
		idx++
	}

	err = fileReader.Close()
	assert.Nil(t, err)

	log.Infof("Total time stamps read %+v num in summaries %+v", totalRead, totalInSummaries)
	assert.Equal(t, totalRead, totalInSummaries)
	os.RemoveAll(segDir)
}

func Benchmark_readTimeFile(b *testing.B) {
	config.InitializeTestingConfig()
	segKey := "/Users/ssubramanian/Desktop/SigLens/siglens/data/Sris-MacBook-Pro.local/final/2022/02/21/01/valtix2/10005995996882630313/0"
	sumFile := structs.GetBsuFnameFromSegKey(segKey)

	blockSums, allBlockInfo, _, err := microreader.ReadBlockSummaries(sumFile, []byte{})
	assert.Nil(b, err)

	colName := config.GetTimeStampKey()

	fileReader, err := InitNewTimeReaderFromBlockSummaries(segKey, colName, allBlockInfo, blockSums, 0)
	assert.Nil(b, err)

	// b.ResetTimer()
	totalRead := uint64(0)
	totalInSummaries := uint64(0)

	numBlocks := len(blockSums)
	for i := numBlocks - 1; i >= 0; i-- {
		allTime, err := fileReader.GetAllTimeStampsForBlock(uint16(i))
		assert.Nil(b, err, "no errors should occur")
		expectedCount := int(blockSums[i].RecCount)
		assert.Equal(b, len(allTime), expectedCount)
		totalRead += uint64(len(allTime))
		totalInSummaries += uint64(expectedCount)

	}

	err = fileReader.Close()
	assert.Nil(b, err)

	log.Infof("Total time stamps read %+v num in summaries %+v", totalRead, totalInSummaries)
}
