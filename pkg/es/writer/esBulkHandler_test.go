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
	"testing"

	jp "github.com/buger/jsonparser"
	"github.com/siglens/siglens/pkg/config"
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
	shouldFlush := false
	localIndexMap := make(map[string]string)
	orgId := uint64(0)

	flush := func() {
		jsonBytes := []byte(`{"hello": "world"}`)
		err := ProcessIndexRequest(jsonBytes, now, indexName, uint64(len(jsonBytes)), true, localIndexMap, orgId)
		assert.Nil(t, err)
	}

	config.InitializeTestingConfig()
	_ = vtable.InitVTable()

	// Ingest some data that can all be converted to numbers.
	jsons := [][]byte{
		[]byte(`{"age": "171"}`),
		[]byte(`{"age": 103}`),
		[]byte(`{"age": 5.123}`),
		[]byte(`{"age": "181"}`),
		[]byte(`{"age": 30}`),
		[]byte(`{"age": 6.321}`),
	}

	for _, jsonBytes := range jsons {
		err := ProcessIndexRequest(jsonBytes, now, indexName, uint64(len(jsonBytes)), shouldFlush, localIndexMap, orgId)
		assert.Nil(t, err)
	}
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

	for _, jsonBytes := range jsons {
		err := ProcessIndexRequest(jsonBytes, now, indexName, uint64(len(jsonBytes)), shouldFlush, localIndexMap, orgId)
		assert.Nil(t, err)
	}
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
