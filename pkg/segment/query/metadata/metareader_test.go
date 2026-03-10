// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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
	_, _, _, _, _, _ = writer.WriteMockColSegFile(segKey, segKey, numBlocks, 30)

	blockSum, _, err := microreader.ReadBlockSummaries(blockSummariesFile, true)
	assert.Nil(t, err)
	log.Infof("num block summaries: %d", len(blockSum))
	assert.Len(t, blockSum, numBlocks)
	os.RemoveAll(blockSummariesFile)
	os.RemoveAll(segDir)
}
