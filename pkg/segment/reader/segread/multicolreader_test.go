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
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_multiSegReader(t *testing.T) {

	config.InitializeTestingConfig()
	segDir := "data/"
	_ = os.MkdirAll(segDir, 0755)
	segKey := segDir + "test"
	numBlocks := 10
	numEntriesInBlock := 10
	_, bSum, _, cols, blockmeta, _ := writer.WriteMockColSegFile(segKey, numBlocks, numEntriesInBlock)

	assert.Greater(t, len(cols), 1)
	sharedReader, foundErr := InitSharedMultiColumnReaders(segKey, cols, blockmeta, bSum, 3, 9)
	assert.Nil(t, foundErr)
	assert.Len(t, sharedReader.MultiColReaders, sharedReader.numReaders)
	assert.Equal(t, 3, sharedReader.numReaders)

	multiReader := sharedReader.MultiColReaders[0]
	var cVal *utils.CValueEnclosure
	var err error
	for colName := range cols {
		if colName == config.GetTimeStampKey() {
			continue
		}
		// invalid block
		_, err = multiReader.ExtractValueFromColumnFile(colName, uint16(numBlocks), 0, 0)
		assert.NotNil(t, err)

		// correct block, incorrect recordNum
		_, err = multiReader.ExtractValueFromColumnFile(colName, 0, uint16(numEntriesInBlock), 0)
		assert.NotNil(t, err)

		cVal, err = multiReader.ExtractValueFromColumnFile(colName, 0, uint16(numEntriesInBlock-3), 0)
		assert.Nil(t, err)
		assert.NotNil(t, cVal)
		log.Infof("ExtractValueFromColumnFile: %+v for column %s", cVal, colName)
	}

	for blkNum := 0; blkNum < numBlocks; blkNum++ {
		for recNum := 0; recNum < numEntriesInBlock; recNum++ {
			ts, err := multiReader.GetTimeStampForRecord(uint16(blkNum), uint16(recNum), 0)
			assert.Nil(t, err)
			assert.Equal(t, uint64(recNum)+1, ts)
		}
	}

	sharedReader.Close()
	assert.Nil(t, err)
	os.RemoveAll(segDir)
}

func Test_InitSharedMultiColumnReaders(t *testing.T) {

	config.InitializeTestingConfig()
	segDir := "data/test_cols_with_asterisk/"
	_ = os.MkdirAll(segDir, 0755)
	segKey := segDir + "test"
	numBlocks := 10
	numEntriesInBlock := 10
	_, bSum, _, cols, blockmeta, _ := writer.WriteMockColSegFile(segKey, numBlocks, numEntriesInBlock)

	assert.Greater(t, len(cols), 1)
	sharedReader, foundErr := InitSharedMultiColumnReaders(segKey, cols, blockmeta, bSum, 3, 9)
	assert.Nil(t, foundErr)
	assert.Len(t, sharedReader.MultiColReaders, sharedReader.numReaders)
	assert.Equal(t, 3, sharedReader.numReaders)

	cols["*"] = true
	sharedAsteriskReader, foundErr := InitSharedMultiColumnReaders(segKey, cols, blockmeta, bSum, 3, 9)
	assert.Nil(t, foundErr)
	assert.Len(t, sharedAsteriskReader.MultiColReaders, sharedAsteriskReader.numReaders)
	assert.Equal(t, 3, sharedAsteriskReader.numReaders)

	assert.Equal(t, sharedReader.numReaders, sharedAsteriskReader.numReaders)
	assert.Equal(t, sharedReader.numOpenFDs, sharedAsteriskReader.numOpenFDs)
	assert.Equal(t, len(sharedReader.MultiColReaders), len(sharedAsteriskReader.MultiColReaders))

	for i := 0; i < len(sharedReader.MultiColReaders); i++ {
		assert.ObjectsAreEqualValues(sharedReader.MultiColReaders[i], sharedAsteriskReader.MultiColReaders[i])
	}

	for col, f := range sharedReader.allFDs {
		assert.Equal(t, (*((*sharedReader).allFDs[col])).Name(), f.Name())
	}

	var sharedAsteriskReaderMCR map[string]map[int]string = make(map[string]map[int]string)
	var sharedReaderMCR map[string]map[int]string = make(map[string]map[int]string)

	for i := 0; i < len(sharedReader.MultiColReaders); i++ {
		assert.Equal(t, len(sharedReader.MultiColReaders[i].allFileReaders), len(sharedAsteriskReader.MultiColReaders[i].allFileReaders))
		for j, aFR := range sharedReader.MultiColReaders[i].allFileReaders {
			_, ok := sharedReaderMCR[aFR.ColName]
			if !ok {
				sharedReaderMCR[aFR.ColName] = make(map[int]string)
			}

			sharedReaderMCR[aFR.ColName][i] = aFR.fileName

			aFR = sharedAsteriskReader.MultiColReaders[i].allFileReaders[j]
			_, ok = sharedAsteriskReaderMCR[aFR.ColName]
			if !ok {
				sharedAsteriskReaderMCR[aFR.ColName] = make(map[int]string)
			}
			sharedAsteriskReaderMCR[aFR.ColName][i] = aFR.fileName
		}
	}

	assert.Equal(t, len(sharedReaderMCR), len(sharedAsteriskReaderMCR))
	for col := range sharedReaderMCR {
		assert.Equal(t, len(sharedReaderMCR[col]), len(sharedAsteriskReaderMCR[col]))
		for i := 0; i < len(sharedAsteriskReader.MultiColReaders); i++ {
			assert.Equal(t, sharedAsteriskReaderMCR[col][i], sharedReaderMCR[col][i])
		}
	}

	sharedReader.Close()
	sharedAsteriskReader.Close()

	os.RemoveAll(segDir)
}
