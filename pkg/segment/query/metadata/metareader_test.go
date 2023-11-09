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

package metadata

import (
	"os"
	"sync"
	"testing"

	localstorage "github.com/siglens/siglens/pkg/blob/local"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/writer"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_readWriteMicroIndices(t *testing.T) {
	globalMetadata = &allSegmentMetadata{
		allSegmentMicroIndex:        make([]*SegmentMicroIndex, 0),
		segmentMetadataReverseIndex: make(map[string]*SegmentMicroIndex),
		tableSortedMetadata:         make(map[string][]*SegmentMicroIndex),
		updateLock:                  &sync.RWMutex{},
	}
	segDir := "data/"
	_ = os.MkdirAll(segDir, 0755)
	segKey := segDir + "test"
	blockSummariesFile := structs.GetBsuFnameFromSegKey(segKey)
	numBlocks := 10
	_, blockSummaries, _, _, allBmh, allColsSizes := writer.WriteMockColSegFile(segKey, numBlocks, 30)
	writer.WriteMockBlockSummary(blockSummariesFile, blockSummaries, allBmh)

	bMicro := &SegmentMicroIndex{
		SegMeta: structs.SegMeta{
			SegmentKey:       segKey,
			ColumnNames:      allColsSizes,
			VirtualTableName: "test",
			SegbaseDir:       segKey, // its actually one dir up but for mocks/tests its fine
		},
	}
	bMicro.SegbaseDir = segKey // for mocks its fine

	_ = localstorage.InitLocalStorage()
	_, blockSum, _, err := bMicro.readBlockSummaries([]byte{})
	assert.Nil(t, err)
	log.Infof("num block summaries: %d", len(blockSum))
	assert.Len(t, blockSum, numBlocks)
	os.RemoveAll(blockSummariesFile)
	os.RemoveAll(segDir)
}

func Test_readEmptyColumnMicroIndices(t *testing.T) {

	globalMetadata = &allSegmentMetadata{
		allSegmentMicroIndex:        make([]*SegmentMicroIndex, 0),
		segmentMetadataReverseIndex: make(map[string]*SegmentMicroIndex),
		tableSortedMetadata:         make(map[string][]*SegmentMicroIndex),
		updateLock:                  &sync.RWMutex{},
	}
	_ = localstorage.InitLocalStorage()

	cnames := make(map[string]*structs.ColSizeInfo)
	cnames["clickid"] = &structs.ColSizeInfo{CmiSize: 0, CsgSize: 0}
	bMicro := &SegmentMicroIndex{
		SegMeta: structs.SegMeta{
			SegmentKey:       "test-key",
			ColumnNames:      cnames,
			VirtualTableName: "test",
		},
	}

	err := bMicro.loadMicroIndices(map[uint16]map[string]bool{}, true, map[string]bool{}, false)
	if err != nil {
		log.Errorf("failed to read cmi, err=%v", err)
	}
	assert.Nil(t, err)
}
