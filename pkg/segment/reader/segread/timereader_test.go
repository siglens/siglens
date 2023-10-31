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
