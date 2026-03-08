// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0
package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_IsAscii(t *testing.T) {
	assert.True(t, IsAscii("Hello, World!"))
	assert.False(t, IsAscii("Hello, 世界!"))
	assert.False(t, IsAscii("Åland Islands"))
}
