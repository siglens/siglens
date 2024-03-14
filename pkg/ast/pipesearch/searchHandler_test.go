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

package pipesearch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_parseSearchBody(t *testing.T) {

	jssrc := make(map[string]interface{})
	jssrc["searchText"] = "abc def"
	jssrc["indexName"] = "svc-2"
	jssrc["size"] = 200
	nowTs := uint64(1659874108987)
	jssrc["startEpoch"] = "now-15m"
	jssrc["endEpoch"] = "now"
	jssrc["scroll"] = 0

	stext, sepoch, eepoch, fsize, idxname, scroll, _ := ParseSearchBody(jssrc, nowTs)
	assert.Equal(t, "abc def", stext)
	assert.Equal(t, nowTs-15*60_000, sepoch, "expected=%v, actual=%v", nowTs-15*60_000, sepoch)
	assert.Equal(t, nowTs, eepoch, "expected=%v, actual=%v", nowTs, eepoch)
	assert.Equal(t, uint64(200), fsize, "expected=%v, actual=%v", uint64(200), fsize)
	assert.Equal(t, "svc-2", idxname, "expected=%v, actual=%v", "svc-2", idxname)
	assert.Equal(t, 0, scroll, "expected=%v, actual=%v", 0, scroll)

	jssrc["from"] = 500
	_, _, _, finalSize, _, scroll, _ := ParseSearchBody(jssrc, nowTs)
	assert.Equal(t, uint64(700), finalSize, "expected=%v, actual=%v", 700, scroll)
	assert.Equal(t, 500, scroll, "expected=%v, actual=%v", 500, scroll)

	// Test : unexpected field in body
	jssrc["test"] = "unexpected field"
	_, _, _, _, _, _, err := ParseSearchBody(jssrc, nowTs)
	assert.EqualError(t, err, "parseSearchBody: unexpected field test")
	delete(jssrc, "test")

	// Test : wrong type of value for searchText
	jssrc["searchText"] = 1
	_, _, _, _, _, _, err = ParseSearchBody(jssrc, nowTs)
	assert.EqualError(t, err, "parseSearchBody: invalid value for field searchText = 1")
	jssrc["searchText"] = "valid text"

	// Test : wrong type of value for indexName
	jssrc["indexName"] = 1
	_, _, _, _, _, _, err = ParseSearchBody(jssrc, nowTs)
	assert.EqualError(t, err, "parseSearchBody: invalid value for field indexName = 1")
	jssrc["indexName"] = "svc-2"

	// Test : wrong type of value for startEpoch
	jssrc["startEpoch"] = []int{}
	_, _, _, _, _, _, err = ParseSearchBody(jssrc, nowTs)
	assert.EqualError(t, err, "parseSearchBody: invalid value for field startEpoch = []")
	jssrc["startEpoch"] = "now-15m"

	// Test : wrong type of value for endEpoch
	jssrc["endEpoch"] = []int{}
	_, _, _, _, _, _, err = ParseSearchBody(jssrc, nowTs)
	assert.EqualError(t, err, "parseSearchBody: invalid value for field endEpoch = []")
	jssrc["endEpoch"] = "now"

	// Test : wrong type of value for size
	jssrc["size"] = "invalid-value"
	_, _, _, _, _, _, err = ParseSearchBody(jssrc, nowTs)
	assert.EqualError(t, err, "parseSearchBody: invalid value for field size = invalid-value")
	jssrc["size"] = 200

	// Test : wrong type of value for from
	jssrc["from"] = "invalid-value"
	_, _, _, _, _, _, err = ParseSearchBody(jssrc, nowTs)
	assert.EqualError(t, err, "parseSearchBody: invalid value for field from = invalid-value")
	jssrc["from"] = 200

}

func Test_parseAlphaNumTime(t *testing.T) {

	nowTs := uint64(1659874108987)

	defValue := uint64(12345)

	inp := "now"
	expected := nowTs
	actual := parseAlphaNumTime(nowTs, inp, defValue)
	assert.Equal(t, expected, actual, "expected=%v, actual=%v", expected, actual)

	inp = "now-1m"
	expected = nowTs - 1*60_000
	actual = parseAlphaNumTime(nowTs, inp, defValue)
	assert.Equal(t, expected, actual, "expected=%v, actual=%v", expected, actual)

	inp = "now-12345m"
	expected = nowTs - 12345*60_000
	actual = parseAlphaNumTime(nowTs, inp, defValue)
	assert.Equal(t, expected, actual, "expected=%v, actual=%v", expected, actual)

	inp = "now-1h"
	expected = nowTs - 1*3600_000
	actual = parseAlphaNumTime(nowTs, inp, defValue)
	assert.Equal(t, expected, actual, "expected=%v, actual=%v", expected, actual)

	inp = "now-365d"
	expected = nowTs - 365*24*3600*1_000
	actual = parseAlphaNumTime(nowTs, inp, defValue)
	assert.Equal(t, expected, actual, "expected=%v, actual=%v", expected, actual)

}
