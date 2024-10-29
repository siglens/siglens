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
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_multiSegReader(t *testing.T) {
	var err error
	dataDir := t.TempDir()
	config.InitializeTestingConfig(dataDir)
	segBaseDir, segKey, err := writer.GetMockSegBaseDirAndKeyForTest(dataDir, "timereader")
	assert.Nil(t, err)

	numBlocks := 10
	numEntriesInBlock := 10
	_, bSum, _, cols, blockmeta, _ := writer.WriteMockColSegFile(segBaseDir, segKey, numBlocks, numEntriesInBlock)

	assert.Greater(t, len(cols), 1)
	sharedReader, foundErr := InitSharedMultiColumnReaders(segKey, cols, blockmeta, bSum, 3, nil, 9)
	assert.Nil(t, foundErr)
	assert.Len(t, sharedReader.MultiColReaders, sharedReader.numReaders)
	assert.Equal(t, 3, sharedReader.numReaders)

	multiReader := sharedReader.MultiColReaders[0]

	var cKeyidx int

	colsToReadIndices := make(map[int]struct{})
	for colName := range cols {
		if colName == config.GetTimeStampKey() {
			continue
		}

		cKeyidx, exists := multiReader.GetColKeyIndex(colName)
		assert.True(t, exists)
		colsToReadIndices[cKeyidx] = struct{}{}
	}

	// invalid block
	err = multiReader.ValidateAndReadBlock(colsToReadIndices, 12345)
	assert.NotNil(t, err)

	err = multiReader.ValidateAndReadBlock(colsToReadIndices, 0)
	assert.Nil(t, err)

	for colName := range cols {
		if colName == config.GetTimeStampKey() {
			continue
		}

		cKeyidx, _ = multiReader.GetColKeyIndex(colName)

		var cValEnc utils.CValueEnclosure

		// correct block, incorrect recordNum
		err = multiReader.ExtractValueFromColumnFile(cKeyidx, 0, uint16(numEntriesInBlock), 0,
			false, &cValEnc)
		assert.NotNil(t, err)

		err = multiReader.ExtractValueFromColumnFile(cKeyidx, 0, uint16(numEntriesInBlock-3), 0,
			false, &cValEnc)
		assert.Nil(t, err)
		assert.NotNil(t, cValEnc)
		log.Infof("ExtractValueFromColumnFile: %+v for column %s", cValEnc, colName)
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
	os.RemoveAll(dataDir)
}

func Test_InitSharedMultiColumnReaders(t *testing.T) {

	dataDir := t.TempDir()
	config.InitializeTestingConfig(dataDir)
	segBaseDir, segKey, err := writer.GetMockSegBaseDirAndKeyForTest(dataDir, "timereader")
	assert.Nil(t, err)

	numBlocks := 10
	numEntriesInBlock := 10
	_, bSum, _, cols, blockmeta, _ := writer.WriteMockColSegFile(segBaseDir, segKey, numBlocks, numEntriesInBlock)

	assert.Greater(t, len(cols), 1)
	sharedReader, foundErr := InitSharedMultiColumnReaders(segKey, cols, blockmeta, bSum, 3, nil, 9)
	assert.Nil(t, foundErr)
	assert.Len(t, sharedReader.MultiColReaders, sharedReader.numReaders)
	assert.Equal(t, 3, sharedReader.numReaders)

	cols["*"] = true
	sharedAsteriskReader, foundErr := InitSharedMultiColumnReaders(segKey, cols, blockmeta, bSum, 3, nil, 9)
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

	os.RemoveAll(dataDir)
}
