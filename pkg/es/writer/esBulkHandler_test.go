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

package writer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	jp "github.com/buger/jsonparser"
	"github.com/siglens/siglens/pkg/config"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

func Test_ProcessDeleteIndex(t *testing.T) {
	// Create a new HTTP request context
	config.InitializeTestingConfig()
	_ = vtable.InitVTable()

	dataPath := config.GetDataPath()

	ctx := &fasthttp.RequestCtx{}
	indexName := "test_Index"
	orgId := uint64(0)
	numberOfSegments := 5

	localIndexMap := make(map[string]string)
	indexNameConverted := addAndGetRealIndexName(indexName, localIndexMap, orgId)

	indexPresent := vtable.IsVirtualTablePresent(&indexNameConverted, orgId)
	assert.Equal(t, true, indexPresent, "Index could not be created")

	setupData(t, numberOfSegments, indexNameConverted)
	allSegMetas := segwriter.ReadAllSegmetas()
	assert.Equal(t, numberOfSegments, len(allSegMetas))

	baseDirs := []string{}
	for i := 0; i < len(allSegMetas); i++ {
		baseDirs = append(baseDirs, allSegMetas[i].SegbaseDir)
	}

	indexAlias := "test_IndexAlias"
	_ = vtable.AddAliases(indexNameConverted, []string{indexAlias}, 0)
	aliases, _ := vtable.GetAliases(indexNameConverted, orgId)
	assert.Contains(t, aliases, indexAlias, "Index alias could not be created")

	ctx.SetUserValue("indexName", indexNameConverted)

	segwriter.InitWriterNode()
	smrBaseDir := dataPath + "ingestnodes" + "/" + config.GetHostID() + "/"
	config.SetSmrBaseDirForTestOnly(smrBaseDir)

	ProcessDeleteIndex(ctx, 0) // Not deleting aliases of index

	indexPresent = vtable.IsVirtualTablePresent(&indexNameConverted, orgId)
	assert.Equal(t, false, indexPresent, "Index exists")

	allSegMetas = segwriter.ReadAllSegmetas()
	assert.Equal(t, 0, len(allSegMetas))

	for i := 0; i < len(baseDirs); i++ {
		assert.NoDirExists(t, baseDirs[i])
	}

	os.RemoveAll(dataPath)
}

func setupData(t *testing.T, numberOfSegments int, indexName string) {
	sleep := time.Duration(1)
	for segNum := 0; segNum < numberOfSegments; segNum++ {
		for batch := 0; batch < 10; batch++ {
			for rec := 0; rec < 100; rec++ {
				record := make(map[string]interface{})
				record["col1"] = "abc"
				record["col2"] = strconv.Itoa(rec)
				record["timestamp"] = uint64(rec)
				rawJson, err := json.Marshal(record)
				assert.Nil(t, err)
				err = segwriter.AddEntryToInMemBuf("deleteIndexTest", rawJson, uint64(rec)+1, indexName, 100, false,
					segutils.SIGNAL_EVENTS, 0)
				assert.Nil(t, err)
			}
			time.Sleep(sleep)
			segwriter.FlushWipBufferToFile(&sleep)
		}
		segwriter.ForcedFlushToSegfile()
	}
}

func flattenJson(currKey string, data []byte) error {
	handler := func(key []byte, value []byte, valueType jp.ValueType, off int) error {
		// Maybe push some state onto a stack here?
		var finalKey string
		if currKey == "" {
			finalKey = string(key)
		} else {
			finalKey = fmt.Sprintf("%s.%s", currKey, key)
		}
		if valueType == jp.Object {
			return flattenJson(finalKey, value)
		} else if valueType == jp.Array {
			return flattenArray(finalKey, value)
		}
		return fmt.Errorf("unknown value %+v", value)
	}
	return jp.ObjectEach(data, handler)

}

func flattenArray(currKey string, data []byte) error {
	i := 0
	_, _ = jp.ArrayEach(data, func(value []byte, valueType jp.ValueType, offset int, err error) {
		var finalKey string
		if currKey == "" {
			finalKey = fmt.Sprintf("%d", i)
		} else {
			finalKey = fmt.Sprintf("%s.%d", currKey, i)
		}
		if valueType == jp.Object {
			_ = flattenJson(finalKey, value)
			return
		}
		// log.Infof("key: '%s', value: '%s', value type: %s", finalKey, string(value), valueType)
		i++
	})
	return nil
}

func Benchmark_OriginalJson(b *testing.B) {
	data := []byte(`{
		"person": {
		  "name": {
			"first": "Leonid",
			"last": "Bugaev",
			"fullName": "Leonid Bugaev"
		  },
		  "github": {
			"handle": "buger",
			"followers": 109
		  },
		  "avatars": [
			{ "url": "https://avatars1.githubusercontent.com/u/14009?v=3&s=460", "type": "thumbnail" }
		  ]
		},
		"company": {
		  "name": "Acme"
		}
	  }`)
	jsonAction := make(map[string]interface{})
	for i := 0; i < b.N; i++ {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		_ = decoder.Decode(&jsonAction)
	}
}

func Benchmark_bugerJsonParse(b *testing.B) {
	data := []byte(`{
		"person": {
		  "name": {
			"first": "Leonid",
			"last": "Bugaev",
			"fullName": "Leonid Bugaev"
		  },
		  "github": {
			"handle": "buger",
			"followers": 109
		  },
		  "avatars": [
			{ "url": "https://avatars1.githubusercontent.com/u/14009?v=3&s=460", "type": "thumbnail" }
		  ]
		},
		"company": {
		  "name": "Acme"
		}
	  }`)
	for i := 0; i < b.N; i++ {
		_ = flattenJson("", data)
	}
}
