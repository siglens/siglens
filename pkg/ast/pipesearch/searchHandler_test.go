// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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
	jssrc["from"] = 0
	jssrc[runTimechartFlag] = true

	stext, sepoch, eepoch, fsize, idxname, scroll, includeNulls, runTimechart, _ := ParseSearchBody(jssrc, nowTs)
	assert.Equal(t, "abc def", stext)
	assert.Equal(t, nowTs-15*60_000, sepoch, "expected=%v, actual=%v", nowTs-15*60_000, sepoch)
	assert.Equal(t, nowTs, eepoch, "expected=%v, actual=%v", nowTs, eepoch)
	assert.Equal(t, uint64(200), fsize, "expected=%v, actual=%v", uint64(200), fsize)
	assert.Equal(t, "svc-2", idxname, "expected=%v, actual=%v", "svc-2", idxname)
	assert.Equal(t, 0, scroll, "expected=%v, actual=%v", 0, scroll)
	assert.False(t, includeNulls, "includeNulls should default to false")
	assert.True(t, runTimechart, "runTimechart should be true")

	jssrc["from"] = 500
	_, _, _, finalSize, _, scroll, _, _, _ := ParseSearchBody(jssrc, nowTs)
	assert.Equal(t, uint64(700), finalSize, "expected=%v, actual=%v", 700, scroll)
	assert.Equal(t, 500, scroll, "expected=%v, actual=%v", 500, scroll)

	jssrc["includeNulls"] = true
	_, _, _, _, _, _, includeNulls, _, _ = ParseSearchBody(jssrc, nowTs)
	assert.True(t, includeNulls, "includeNulls should be true")
}
