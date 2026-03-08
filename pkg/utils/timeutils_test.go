// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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
