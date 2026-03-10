// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0
package summary

import (
	"testing"
)

func Test_IdempotentCleanup(t *testing.T) {
	qs := InitQuerySummary(LOGS, 0)
	qs.Cleanup()
	qs.Cleanup() // Just don't panic.
}
