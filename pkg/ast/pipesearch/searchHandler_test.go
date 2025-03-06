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
	jssrc[runTimechartFlag] = true

	stext, sepoch, eepoch, fsize, idxname, scroll, includeNulls, runTimechart, err := ParseSearchBody(jssrc, nowTs)
	assert.NoError(t, err, "Expected no error for valid input")
	assert.Equal(t, "abc def", stext)
	assert.Equal(t, nowTs-15*60_000, sepoch, "expected=%v, actual=%v", nowTs-15*60_000, sepoch)
	assert.Equal(t, nowTs, eepoch, "expected=%v, actual=%v", nowTs, eepoch)
	assert.Equal(t, uint64(200), fsize, "expected=%v, actual=%v", uint64(200), fsize)
	assert.Equal(t, "svc-2", idxname, "expected=%v, actual=%v", "svc-2", idxname)
	assert.Equal(t, 0, scroll, "expected=%v, actual=%v", 0, scroll)
	assert.False(t, includeNulls, "includeNulls should default to false")
	assert.True(t, runTimechart, "runTimechart should be true")

	jssrc["from"] = 500
	_, _, _, finalSize, _, scroll, _, _, err := ParseSearchBody(jssrc, nowTs)
	assert.NoError(t, err, "Expected no error for valid input")
	assert.Equal(t, uint64(700), finalSize, "expected=%v, actual=%v", 700, scroll)
	assert.Equal(t, 500, scroll, "expected=%v, actual=%v", 500, scroll)

	jssrc["includeNulls"] = true
	_, _, _, _, _, _, includeNulls, _, err = ParseSearchBody(jssrc, nowTs)
	assert.NoError(t, err, "Expected no error for valid input")
	assert.True(t, includeNulls, "includeNulls should be true")
}

func Test_parseSearchBody_InvalidSearchTextType(t *testing.T) {
	jssrc := make(map[string]interface{})
	jssrc["searchText"] = 123 // Invalid type (should be string)
	jssrc["indexName"] = "svc-2"
	jssrc["size"] = 200
	nowTs := uint64(1659874108987)
	jssrc["startEpoch"] = "now-15m"
	jssrc["endEpoch"] = "now"
	jssrc["scroll"] = 0
	jssrc[runTimechartFlag] = true

	_, _, _, _, _, _, _, _, err := ParseSearchBody(jssrc, nowTs)
	assert.Error(t, err, "Expected an error for invalid searchText type")
}

func Test_parseSearchBody_MissingFields(t *testing.T) {
	jssrc := make(map[string]interface{})
	jssrc["indexName"] = "svc-2"
	jssrc["size"] = 200
	nowTs := uint64(1659874108987)
	jssrc["startEpoch"] = "now-15m"
	jssrc["endEpoch"] = "now"

	stext, sepoch, eepoch, fsize, idxname, scroll, includeNulls, runTimechart, err := ParseSearchBody(jssrc, nowTs)
	assert.NoError(t, err, "Expected no error for missing fields")
	assert.Equal(t, "*", stext, "Expected default searchText '*'")
	assert.Equal(t, nowTs-15*60_000, sepoch, "expected=%v, actual=%v", nowTs-15*60_000, sepoch)
	assert.Equal(t, nowTs, eepoch, "expected=%v, actual=%v", nowTs, eepoch)
	assert.Equal(t, uint64(200), fsize, "expected=%v, actual=%v", uint64(200), fsize)
	assert.Equal(t, "svc-2", idxname, "expected=%v, actual=%v", "svc-2", idxname)
	assert.Equal(t, 0, scroll, "expected=%v, actual=%v", 0, scroll)
	assert.False(t, includeNulls, "includeNulls should default to false")
	assert.False(t, runTimechart, "runTimechart should default to false")
}

func Test_parseSearchBody_UnexpectedField(t *testing.T) {
	jssrc := make(map[string]interface{})
	jssrc["searchText"] = "abc def"
	jssrc["indexName"] = "svc-2"
	jssrc["size"] = 200
	nowTs := uint64(1659874108987)
	jssrc["startEpoch"] = "now-15m"
	jssrc["endEpoch"] = "now"
	jssrc["scroll"] = 0
	jssrc[runTimechartFlag] = true
	jssrc["unknownField"] = "value" // Unexpected field

	_, _, _, _, _, _, _, _, err := ParseSearchBody(jssrc, nowTs)
	assert.NoError(t, err, "Expected no error for unexpected field")
}

func Test_parseSearchBody_InvalidSizeType(t *testing.T) {
	jssrc := make(map[string]interface{})
	jssrc["searchText"] = "abc def"
	jssrc["indexName"] = "svc-2"
	jssrc["size"] = "invalid" // Invalid type (should be number)
	nowTs := uint64(1659874108987)
	jssrc["startEpoch"] = "now-15m"
	jssrc["endEpoch"] = "now"
	jssrc["scroll"] = 0
	jssrc[runTimechartFlag] = true

	_, _, _, _, _, _, _, _, err := ParseSearchBody(jssrc, nowTs)
	assert.Error(t, err, "Expected an error for invalid size type")
}
