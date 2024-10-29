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
	"github.com/stretchr/testify/assert"

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

func Test_ReplaceSingleSegMeta(t *testing.T) {
	t.Cleanup(cleanupSfmFiles)

	config.InitializeDefaultConfig(t.TempDir())
	initSmr()

	segMetasArr := ReadLocalSegmeta(false)
	segMetas := make(map[string]*structs.SegMeta)
	for _, smentry := range segMetasArr {
		if smentry.SegmentKey == "key1" {
			segMetas[smentry.SegmentKey] = smentry
			break
		}
	}
	assert.Empty(t, segMetas)

	segMetaV1 := structs.SegMeta{
		SegmentKey:  "key1",
		RecordCount: 20,
	}

	AddOrReplaceRotatedSegmeta(segMetaV1)

	segMetasArr = ReadLocalSegmeta(false)
	segMetas = make(map[string]*structs.SegMeta)
	for _, smentry := range segMetasArr {
		if smentry.SegmentKey == "key1" {
			segMetas[smentry.SegmentKey] = smentry
			break
		}
	}

	assert.Len(t, segMetas, 1)
	assert.Equal(t, segMetaV1.SegmentKey, segMetas["key1"].SegmentKey)
	assert.Equal(t, segMetaV1.RecordCount, segMetas["key1"].RecordCount)

	segMetaV2 := structs.SegMeta{
		SegmentKey:  "key1",
		RecordCount: 50,
	}

	AddOrReplaceRotatedSegmeta(segMetaV2)

	segMetasArr = ReadLocalSegmeta(false)
	segMetas = make(map[string]*structs.SegMeta)
	for _, smentry := range segMetasArr {
		if smentry.SegmentKey == "key1" {
			segMetas[smentry.SegmentKey] = smentry
			break
		}
	}

	assert.Len(t, segMetas, 1)
	assert.Equal(t, segMetaV2.SegmentKey, segMetas["key1"].SegmentKey)
	assert.Equal(t, segMetaV2.RecordCount, segMetas["key1"].RecordCount)
}

func Test_ReplaceMiddleSegMeta(t *testing.T) {
	t.Cleanup(cleanupSfmFiles)

	config.InitializeDefaultConfig(t.TempDir())
	initSmr()

	segMeta1 := structs.SegMeta{
		SegmentKey:  "key1",
		RecordCount: 20,
	}
	segMeta2V1 := structs.SegMeta{
		SegmentKey:  "key2",
		RecordCount: 50,
	}
	segMeta3 := structs.SegMeta{
		SegmentKey:  "key3",
		RecordCount: 100,
	}

	AddOrReplaceRotatedSegmeta(segMeta1)
	AddOrReplaceRotatedSegmeta(segMeta2V1)
	AddOrReplaceRotatedSegmeta(segMeta3)

	segMeta2V2 := structs.SegMeta{
		SegmentKey:  "key2",
		RecordCount: 80,
	}
	AddOrReplaceRotatedSegmeta(segMeta2V2)

	segMetasArr := ReadLocalSegmeta(false)
	segMetas := make(map[string]*structs.SegMeta)
	for _, smentry := range segMetasArr {
		if smentry.SegmentKey == "key1" ||
			smentry.SegmentKey == "key2" ||
			smentry.SegmentKey == "key3" {
			segMetas[smentry.SegmentKey] = smentry
		}
	}

	assert.Len(t, segMetas, 3)
	assert.Equal(t, segMeta1.SegmentKey, segMetas["key1"].SegmentKey)
	assert.Equal(t, segMeta1.RecordCount, segMetas["key1"].RecordCount)
	assert.Equal(t, segMeta2V2.SegmentKey, segMetas["key2"].SegmentKey)
	assert.Equal(t, segMeta2V2.RecordCount, segMetas["key2"].RecordCount)
	assert.Equal(t, segMeta3.SegmentKey, segMetas["key3"].SegmentKey)
	assert.Equal(t, segMeta3.RecordCount, segMetas["key3"].RecordCount)
}
