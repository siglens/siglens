// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_FullUnwrapError(t *testing.T) {
	assert.Equal(t, FullUnwrapError(nil), nil)

	err1 := fmt.Errorf("first error")
	err2 := fmt.Errorf("first wrap: %w", err1)
	err3 := fmt.Errorf("second wrap: %w", err2)
	assert.Equal(t, FullUnwrapError(err3), err1)
}
