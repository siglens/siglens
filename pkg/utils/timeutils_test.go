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

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_parseAlphaNumTime(t *testing.T) {

	nowTs := uint64(1659874108987)

	defValue := uint64(12345)

	inp := "now"
	expected := nowTs
	actual := ParseAlphaNumTime(nowTs, inp, defValue)
	assert.Equal(t, expected, actual, "expected=%v, actual=%v", expected, actual)

	inp = "now-1m"
	expected = nowTs - 1*60_000
	actual = ParseAlphaNumTime(nowTs, inp, defValue)
	assert.Equal(t, expected, actual, "expected=%v, actual=%v", expected, actual)

	inp = "now-12345m"
	expected = nowTs - 12345*60_000
	actual = ParseAlphaNumTime(nowTs, inp, defValue)
	assert.Equal(t, expected, actual, "expected=%v, actual=%v", expected, actual)

	inp = "now-1h"
	expected = nowTs - 1*3600_000
	actual = ParseAlphaNumTime(nowTs, inp, defValue)
	assert.Equal(t, expected, actual, "expected=%v, actual=%v", expected, actual)

	inp = "now-365d"
	expected = nowTs - 365*24*3600*1_000
	actual = ParseAlphaNumTime(nowTs, inp, defValue)
	assert.Equal(t, expected, actual, "expected=%v, actual=%v", expected, actual)

	inp = "invalidTimeString"
	expected = 12345
	actual = ParseAlphaNumTime(nowTs, inp, defValue)
	assert.Equal(t, expected, actual, "expected=%v, actual=%v", expected, actual)

	inp = "now-365x"
	expected = 12345
	actual = ParseAlphaNumTime(nowTs, inp, defValue)
	assert.Equal(t, expected, actual, "expected=%v, actual=%v", expected, actual)

	inp = "now-5xm"
	expected = 12345
	actual = ParseAlphaNumTime(nowTs, inp, defValue)
	assert.Equal(t, expected, actual, "expected=%v, actual=%v", expected, actual)

}
