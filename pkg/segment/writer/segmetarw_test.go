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

package writer

import (
	"fmt"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"os"
	"path/filepath"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	log "github.com/sirupsen/logrus"
)

func cleanupSfmFiles() {
	curDir, err := os.Getwd()
	if err != nil {
		log.Errorf("Error could not get curDir err: %v", err)
		return
	}
	sfmPath := filepath.Join(curDir, "*.sfm")
	matches, err := filepath.Glob(sfmPath)
	if err != nil {
		log.Errorf("Error finding sfmfiles err: %v", err)
		return
	}

	for _, match := range matches {
		err := os.Remove(match)
		if err != nil {
			log.Errorf("Error deleting sfmfile: %v,  err: %v", match, err)
			continue
		}
	}
}

func newTestSegKey(suffix int) string {
	return fmt.Sprintf("test-data/hostid/final/index/streamid/%d/%d", suffix, suffix)
}

func Test_ReplaceSingleSegMeta(t *testing.T) {
	t.Cleanup(cleanupSfmFiles)

	config.InitializeDefaultConfig(t.TempDir())
	initSmr()

	segMetasArr := ReadLocalSegmeta(false)
	segMetas := make(map[string]*structs.SegMeta)
	for _, smentry := range segMetasArr {
		if smentry.SegmentKey == newTestSegKey(1) {
			segMetas[smentry.SegmentKey] = smentry
			break
		}
	}
	assert.Empty(t, segMetas)

	segMetaV1 := structs.SegMeta{
		SegmentKey:  newTestSegKey(1),
		RecordCount: 20,
	}

	AddOrReplaceRotatedSegmeta(segMetaV1)

	segMetasArr = ReadLocalSegmeta(false)
	segMetas = make(map[string]*structs.SegMeta)
	for _, smentry := range segMetasArr {
		if smentry.SegmentKey == newTestSegKey(1) {
			segMetas[smentry.SegmentKey] = smentry
			break
		}
	}

	assert.Len(t, segMetas, 1)
	assert.Equal(t, segMetaV1.SegmentKey, segMetas[newTestSegKey(1)].SegmentKey)
	assert.Equal(t, segMetaV1.RecordCount, segMetas[newTestSegKey(1)].RecordCount)

	segMetaV2 := structs.SegMeta{
		SegmentKey:  newTestSegKey(1),
		RecordCount: 50,
	}

	AddOrReplaceRotatedSegmeta(segMetaV2)

	segMetasArr = ReadLocalSegmeta(false)
	segMetas = make(map[string]*structs.SegMeta)
	for _, smentry := range segMetasArr {
		if smentry.SegmentKey == newTestSegKey(1) {
			segMetas[smentry.SegmentKey] = smentry
			break
		}
	}

	assert.Len(t, segMetas, 1)
	assert.Equal(t, segMetaV2.SegmentKey, segMetas[newTestSegKey(1)].SegmentKey)
	assert.Equal(t, segMetaV2.RecordCount, segMetas[newTestSegKey(1)].RecordCount)
}

func Test_ReplaceMiddleSegMeta(t *testing.T) {
	t.Cleanup(cleanupSfmFiles)

	config.InitializeDefaultConfig(t.TempDir())
	initSmr()

	segMeta1 := structs.SegMeta{
		SegmentKey:  newTestSegKey(1),
		RecordCount: 20,
	}
	segMeta2V1 := structs.SegMeta{
		SegmentKey:  newTestSegKey(2),
		RecordCount: 50,
	}
	segMeta3 := structs.SegMeta{
		SegmentKey:  newTestSegKey(3),
		RecordCount: 100,
	}

	AddOrReplaceRotatedSegmeta(segMeta1)
	AddOrReplaceRotatedSegmeta(segMeta2V1)
	AddOrReplaceRotatedSegmeta(segMeta3)

	segMeta2V2 := structs.SegMeta{
		SegmentKey:  newTestSegKey(2),
		RecordCount: 80,
	}
	AddOrReplaceRotatedSegmeta(segMeta2V2)

	segMetasArr := ReadLocalSegmeta(false)
	segMetas := make(map[string]*structs.SegMeta)
	for _, smentry := range segMetasArr {
		if smentry.SegmentKey == newTestSegKey(1) ||
			smentry.SegmentKey == newTestSegKey(2) ||
			smentry.SegmentKey == newTestSegKey(3) {
			segMetas[smentry.SegmentKey] = smentry
		}
	}

	assert.Len(t, segMetas, 3)
	assert.Equal(t, segMeta1.SegmentKey, segMetas[newTestSegKey(1)].SegmentKey)
	assert.Equal(t, segMeta1.RecordCount, segMetas[newTestSegKey(1)].RecordCount)
	assert.Equal(t, segMeta2V2.SegmentKey, segMetas[newTestSegKey(2)].SegmentKey)
	assert.Equal(t, segMeta2V2.RecordCount, segMetas[newTestSegKey(2)].RecordCount)
	assert.Equal(t, segMeta3.SegmentKey, segMetas[newTestSegKey(3)].SegmentKey)
	assert.Equal(t, segMeta3.RecordCount, segMetas[newTestSegKey(3)].RecordCount)
}

func Test_removeSegmetas_byIndex(t *testing.T) {
	t.Cleanup(cleanupSfmFiles)

	config.InitializeDefaultConfig(t.TempDir())
	initSmr()

	segMeta1 := structs.SegMeta{
		SegmentKey:       newTestSegKey(1),
		VirtualTableName: "ind-0",
	}
	segMeta2 := structs.SegMeta{
		SegmentKey:       newTestSegKey(2),
		VirtualTableName: "ind-0",
	}
	segMeta3 := structs.SegMeta{
		SegmentKey:       newTestSegKey(3),
		VirtualTableName: "ind-1",
	}

	AddOrReplaceRotatedSegmeta(segMeta1)
	AddOrReplaceRotatedSegmeta(segMeta2)
	AddOrReplaceRotatedSegmeta(segMeta3)

	segMetasArr := ReadLocalSegmeta(false)
	require.Len(t, segMetasArr, 3)

	removeSegmetas(nil, "ind-0")

	segMetasArr = ReadLocalSegmeta(false)
	assert.Len(t, segMetasArr, 1)
	assert.Equal(t, segMeta3.SegmentKey, segMetasArr[0].SegmentKey)
}

func Test_removeSegmetas_bySegkey(t *testing.T) {
	t.Cleanup(cleanupSfmFiles)

	config.InitializeDefaultConfig(t.TempDir())
	initSmr()

	segMeta1 := structs.SegMeta{
		SegmentKey:       newTestSegKey(1),
		VirtualTableName: "ind-0",
	}
	segMeta2 := structs.SegMeta{
		SegmentKey:       newTestSegKey(2),
		VirtualTableName: "ind-0",
	}
	segMeta3 := structs.SegMeta{
		SegmentKey:       newTestSegKey(3),
		VirtualTableName: "ind-1",
	}

	AddOrReplaceRotatedSegmeta(segMeta1)
	AddOrReplaceRotatedSegmeta(segMeta2)
	AddOrReplaceRotatedSegmeta(segMeta3)

	segMetasArr := ReadLocalSegmeta(false)
	require.Len(t, segMetasArr, 3)

	removeSegmetas(map[string]struct{}{
		newTestSegKey(1): {},
		newTestSegKey(3): {},
	}, "")

	segMetasArr = ReadLocalSegmeta(false)
	assert.Len(t, segMetasArr, 1)
	assert.Equal(t, segMeta2.SegmentKey, segMetasArr[0].SegmentKey)
}
