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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	jp "github.com/buger/jsonparser"
	"github.com/siglens/siglens/pkg/config"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	server_utils "github.com/siglens/siglens/pkg/server/utils"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	"github.com/stretchr/testify/assert"
)

// Test ingesting multiple types of values into one column.
// Currently the only test is that it doesn't crash.
func Test_IngestMultipleTypesIntoOneColumn(t *testing.T) {
	// Setup ingestion parameters.
	now := utils.GetCurrentTimeInMs()
	indexName := "traces"
	shouldFlush := true
	localIndexMap := make(map[string]string)
	orgId := int64(0)
	tsKey := config.GetTimeStampKey()

	idxToStreamIdCache := make(map[string]string)
	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [64]byte

	flush := func() {
		jsonBytes := []byte(`{"hello": "world"}`)
		pleArray := make([]*segwriter.ParsedLogEvent, 0)
		defer segwriter.ReleasePLEs(pleArray)
		ple, err := segwriter.GetNewPLE(jsonBytes, now, indexName, &tsKey, jsParsingStackbuf[:])
		assert.Nil(t, err)
		pleArray = append(pleArray, ple)
		err = ProcessIndexRequestPle(now, indexName, true, localIndexMap, orgId, 0, idxToStreamIdCache, cnameCacheByteHashToStr, jsParsingStackbuf[:], pleArray)
		assert.Nil(t, err)
	}

	config.InitializeTestingConfig(t.TempDir())
	_ = vtable.InitVTable(server_utils.GetMyIds)

	// Ingest some data that can all be converted to numbers.
	jsons := [][]byte{
		[]byte(`{"age": "171"}`),
		[]byte(`{"age": 103}`),
		[]byte(`{"age": 5.123}`),
		[]byte(`{"age": "181"}`),
		[]byte(`{"age": 30}`),
		[]byte(`{"age": 6.321}`),
	}

	pleArray := make([]*segwriter.ParsedLogEvent, 0)
	for _, jsonBytes := range jsons {
		ple, err := segwriter.GetNewPLE(jsonBytes, now, indexName, &tsKey, jsParsingStackbuf[:])
		assert.Nil(t, err)
		pleArray = append(pleArray, ple)
	}
	err := ProcessIndexRequestPle(now, indexName, shouldFlush, localIndexMap, orgId, 0, idxToStreamIdCache, cnameCacheByteHashToStr, jsParsingStackbuf[:], pleArray)
	assert.Nil(t, err)
	segwriter.ReleasePLEs(pleArray)

	flush()

	// Ingest some data that will need to be converted to strings.
	jsons = [][]byte{
		[]byte(`{"age": "171"}`),
		[]byte(`{"age": 103}`),
		[]byte(`{"age": 5.123}`),
		[]byte(`{"age": true}`),
		[]byte(`{"age": "181"}`),
		[]byte(`{"age": 30}`),
		[]byte(`{"age": 6.321}`),
		[]byte(`{"age": false}`),
		[]byte(`{"age": "hello"}`),
	}

	pleArray = make([]*segwriter.ParsedLogEvent, 0)
	for _, jsonBytes := range jsons {
		ple, err := segwriter.GetNewPLE(jsonBytes, now, indexName, &tsKey, jsParsingStackbuf[:])
		assert.Nil(t, err)
		pleArray = append(pleArray, ple)
	}
	err = ProcessIndexRequestPle(now, indexName, shouldFlush, localIndexMap, orgId, 0, idxToStreamIdCache, cnameCacheByteHashToStr, jsParsingStackbuf[:], pleArray)
	assert.Nil(t, err)
	segwriter.ReleasePLEs(pleArray)

	flush()

	// Cleanup
	os.RemoveAll(config.GetDataPath())
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
