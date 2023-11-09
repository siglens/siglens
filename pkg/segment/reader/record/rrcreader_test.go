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

package record

import (
	"fmt"
	"os"
	"testing"

	localstorage "github.com/siglens/siglens/pkg/blob/local"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_GetJsonFromAllRrc(t *testing.T) {
	dir := "test/"
	config.InitializeDefaultConfig()
	_ = localstorage.InitLocalStorage()
	numSegKeys := 1
	numBlocks := 1
	numRecords := 2
	metadata.InitMockColumnarMetadataStore(dir, numSegKeys, numBlocks, numRecords)

	segkey := dir + "query_test_" + fmt.Sprint(0)

	segencmap := make(map[uint16]string)
	segencmap[uint16(0)] = segkey

	allrrc := []*utils.RecordResultContainer{
		{
			SegKeyInfo: utils.SegKeyInfo{
				SegKeyEnc: 0,
				IsRemote:  false,
			},
			BlockNum:         0,
			RecordNum:        0,
			SortColumnValue:  0,
			TimeStamp:        0,
			VirtualTableName: "evts",
		},
		{
			SegKeyInfo: utils.SegKeyInfo{
				SegKeyEnc: 0,
				IsRemote:  false,
			},
			BlockNum:         0,
			RecordNum:        1,
			SortColumnValue:  0,
			TimeStamp:        1,
			VirtualTableName: "evts",
		},
	}
	qid := uint64(0)
	allRecords, _, err := GetJsonFromAllRrc(allrrc, false, qid, segencmap, &structs.QueryAggregators{})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(allRecords))

	// 11 columns + timestamp + index
	assert.Equal(t, 14, len(allRecords[0]))

	// checking decoding non random column values

	indexName := "evts"
	key0Val := "match words 123 abc"
	key1Val := "value1"
	key2Vals := []int64{0, 1}
	key3Vals := []bool{true, false}
	key6Vals := []int64{0, 2}
	key7Val := "batch-" + fmt.Sprint(0)
	key8Val := int64(0)
	key10Val := segkey
	for i := 0; i < numRecords; i++ {
		assert.Equal(t, indexName, allRecords[i]["_index"])
		assert.Equal(t, key0Val, allRecords[i]["key0"])
		assert.Equal(t, key1Val, allRecords[i]["key1"])
		assert.Equal(t, key2Vals[i], allRecords[i]["key2"])
		assert.Equal(t, key3Vals[i], allRecords[i]["key3"])
		assert.Equal(t, key6Vals[i], allRecords[i]["key6"])
		assert.Equal(t, key7Val, allRecords[i]["key7"])
		assert.Equal(t, key8Val, allRecords[i]["key8"])
		assert.Equal(t, key10Val, allRecords[i]["key10"])
		assert.Contains(t, allRecords[i], "key11")
		assert.Equal(t, uint64(i), allRecords[i][config.GetTimeStampKey()])
	}

	os.RemoveAll(dir)
}

func Test_GetJsonFromAllRrc_withAggs_IncludeCols(t *testing.T) {
	dir := "test/"
	config.InitializeDefaultConfig()
	_ = localstorage.InitLocalStorage()
	numSegKeys := 1
	numBlocks := 1
	numRecords := 2
	metadata.InitMockColumnarMetadataStore(dir, numSegKeys, numBlocks, numRecords)

	segkey := dir + "query_test_" + fmt.Sprint(0)

	segencmap := make(map[uint16]string)
	segencmap[uint16(0)] = segkey

	allrrc := []*utils.RecordResultContainer{
		{
			SegKeyInfo: utils.SegKeyInfo{
				SegKeyEnc: 0,
				IsRemote:  false,
			},
			BlockNum:         0,
			RecordNum:        0,
			SortColumnValue:  0,
			TimeStamp:        0,
			VirtualTableName: "evts",
		},
		{
			SegKeyInfo: utils.SegKeyInfo{
				SegKeyEnc: 0,
				IsRemote:  false,
			},
			BlockNum:         0,
			RecordNum:        1,
			SortColumnValue:  0,
			TimeStamp:        1,
			VirtualTableName: "evts",
		},
	}
	qid := uint64(0)
	aggNode := &structs.QueryAggregators{}
	aggNode.PipeCommandType = structs.OutputTransformType
	aggNode.OutputTransforms = &structs.OutputTransforms{}
	aggNode.OutputTransforms.OutputColumns = &structs.ColumnsRequest{}
	aggNode.OutputTransforms.OutputColumns.IncludeColumns = append(aggNode.OutputTransforms.OutputColumns.IncludeColumns, "key0")
	allRecords, _, err := GetJsonFromAllRrc(allrrc, false, qid, segencmap, aggNode)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(allRecords))

	// 11 columns + timestamp + index
	assert.Equal(t, 3, len(allRecords[0]))

	// checking decoding non random column values

	indexName := "evts"
	key0Val := "match words 123 abc"

	for i := 0; i < numRecords; i++ {
		assert.Equal(t, indexName, allRecords[i]["_index"])
		assert.Equal(t, key0Val, allRecords[i]["key0"])
		assert.Equal(t, uint64(i), allRecords[i][config.GetTimeStampKey()])
	}
	os.RemoveAll(dir)
}

func Test_GetJsonFromAllRrc_withAggs_ExcludeCols(t *testing.T) {
	dir := "test/"
	config.InitializeDefaultConfig()
	_ = localstorage.InitLocalStorage()
	numSegKeys := 1
	numBlocks := 1
	numRecords := 2
	metadata.InitMockColumnarMetadataStore(dir, numSegKeys, numBlocks, numRecords)

	segkey := dir + "query_test_" + fmt.Sprint(0)

	segencmap := make(map[uint16]string)
	segencmap[uint16(0)] = segkey

	allrrc := []*utils.RecordResultContainer{
		{
			SegKeyInfo: utils.SegKeyInfo{
				SegKeyEnc: 0,
				IsRemote:  false,
			},
			BlockNum:         0,
			RecordNum:        0,
			SortColumnValue:  0,
			TimeStamp:        0,
			VirtualTableName: "evts",
		},
		{
			SegKeyInfo: utils.SegKeyInfo{
				SegKeyEnc: 0,
				IsRemote:  false,
			},
			BlockNum:         0,
			RecordNum:        1,
			SortColumnValue:  0,
			TimeStamp:        1,
			VirtualTableName: "evts",
		},
	}
	qid := uint64(0)
	aggNode := &structs.QueryAggregators{}
	aggNode.PipeCommandType = structs.OutputTransformType
	aggNode.OutputTransforms = &structs.OutputTransforms{}
	aggNode.OutputTransforms.OutputColumns = &structs.ColumnsRequest{}
	aggNode.OutputTransforms.OutputColumns.ExcludeColumns = append(aggNode.OutputTransforms.OutputColumns.ExcludeColumns, "key0")
	allRecords, _, err := GetJsonFromAllRrc(allrrc, false, qid, segencmap, aggNode)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(allRecords))

	// 11 columns + timestamp + index
	assert.Equal(t, 13, len(allRecords[0]))

	// checking decoding non random column values

	indexName := "evts"
	// key0Val := "match words 123 abc"
	key1Val := "value1"
	key2Vals := []int64{0, 1}
	key3Vals := []bool{true, false}
	key6Vals := []int64{0, 2}
	key7Val := "batch-" + fmt.Sprint(0)
	key8Val := int64(0)
	key10Val := segkey
	for i := 0; i < numRecords; i++ {
		assert.Equal(t, indexName, allRecords[i]["_index"])
		// assert.Equal(t, key0Val, allRecords[i]["key0"])
		assert.Equal(t, key1Val, allRecords[i]["key1"])
		assert.Equal(t, key2Vals[i], allRecords[i]["key2"]) // we only encode floats
		assert.Equal(t, key3Vals[i], allRecords[i]["key3"])
		assert.Equal(t, key6Vals[i], allRecords[i]["key6"]) // we only encode floats
		assert.Equal(t, key7Val, allRecords[i]["key7"])
		assert.Equal(t, key8Val, allRecords[i]["key8"])
		assert.Equal(t, key10Val, allRecords[i]["key10"])
		assert.Contains(t, allRecords[i], "key11")
		assert.Equal(t, uint64(i), allRecords[i][config.GetTimeStampKey()])
	}

	os.RemoveAll(dir)
}
