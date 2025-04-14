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
	"os"
	"testing"

	segmetadata "github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/reader/microreader"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/writer"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_readWriteMicroIndices(t *testing.T) {
	segmetadata.ResetGlobalMetadataForTest()
	segDir := "data/"
	_ = os.MkdirAll(segDir, 0755)
	segKey := segDir + "test"
	blockSummariesFile := structs.GetBsuFnameFromSegKey(segKey)
	numBlocks := 10
	_, blockSummaries, _, _, allBmh, _ := writer.WriteMockColSegFile(segKey, segKey, numBlocks, 30)
	writer.WriteMockBlockSummary(blockSummariesFile, blockSummaries, allBmh)

	blockSum, _, err := microreader.ReadBlockSummaries(blockSummariesFile, true)
	assert.Nil(t, err)
	log.Infof("num block summaries: %d", len(blockSum))
	assert.Len(t, blockSum, numBlocks)
	os.RemoveAll(blockSummariesFile)
	os.RemoveAll(segDir)
}
