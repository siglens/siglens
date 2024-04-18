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

	stext, sepoch, eepoch, fsize, idxname, scroll := ParseSearchBody(jssrc, nowTs)
	assert.Equal(t, "abc def", stext)
	assert.Equal(t, nowTs-15*60_000, sepoch, "expected=%v, actual=%v", nowTs-15*60_000, sepoch)
	assert.Equal(t, nowTs, eepoch, "expected=%v, actual=%v", nowTs, eepoch)
	assert.Equal(t, uint64(200), fsize, "expected=%v, actual=%v", uint64(200), fsize)
	assert.Equal(t, "svc-2", idxname, "expected=%v, actual=%v", "svc-2", idxname)
	assert.Equal(t, 0, scroll, "expected=%v, actual=%v", 0, scroll)

	jssrc["from"] = 500
	_, _, _, finalSize, _, scroll := ParseSearchBody(jssrc, nowTs)
	assert.Equal(t, uint64(700), finalSize, "expected=%v, actual=%v", 700, scroll)
	assert.Equal(t, 500, scroll, "expected=%v, actual=%v", 500, scroll)
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
