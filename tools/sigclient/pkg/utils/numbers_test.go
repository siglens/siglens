// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0
package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_AsUint64(t *testing.T) {
	v, ok := AsUint64("123")
	assert.True(t, ok)
	assert.Equal(t, uint64(123), v)

	v, ok = AsUint64("1.04475e+06")
	assert.True(t, ok)
	assert.Equal(t, uint64(1044750), v)
}
