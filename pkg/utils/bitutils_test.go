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

package utils

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ExtractTimeStamp(t *testing.T) {
	m := make(map[string]interface{})

	// Case 1: timestamp as json.Number
	m["timestamp"] = json.Number("950823120000")
	rawJson, _ := json.Marshal(m)
	tskeyCfg := "timestamp"
	ts_millis := ExtractTimeStamp(rawJson, &tskeyCfg)
	assert.Equal(t, uint64(950823120000), ts_millis)

	// Case 2: timestamp as String
	m["timestamp"] = "950823120000"
	rawJson, _ = json.Marshal(m)
	ts_millis = ExtractTimeStamp(rawJson, &tskeyCfg)
	assert.Equal(t, uint64(950823120000), ts_millis)
}

func Test_ConvertTimestampToMillis(t *testing.T) {
	// valid timestamps
	value := "950823120000"
	ts_millis, _ := convertTimestampToMillis(value)
	assert.Equal(t, uint64(950823120000), ts_millis)

	value = "950823120"
	ts_millis, _ = convertTimestampToMillis(value)
	assert.Equal(t, uint64(950823120000), ts_millis)

	value = "2019-06-11T16:33:51Z"
	ts_millis, _ = convertTimestampToMillis(value)
	assert.Equal(t, uint64(1560270831000), ts_millis)

	value = "2020-08-03T07:10:20.123456+02:00"
	ts_millis, _ = convertTimestampToMillis(value)
	assert.Equal(t, uint64(1596431420123), ts_millis)

	// invalid timestamps
	value = "random string"
	_, err := convertTimestampToMillis(value)
	assert.NotNil(t, err)

	value = "20201-08-03T07:10:20.123456+02:00"
	_, err = convertTimestampToMillis(value)
	assert.NotNil(t, err)
}
