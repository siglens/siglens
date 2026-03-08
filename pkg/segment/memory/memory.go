// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package memory

import (
	"github.com/siglens/siglens/pkg/segment/structs"
)

var GlobalMemoryTracker *structs.MemoryTracker

/*
Returns the maximum number of bytes that can be allocated for metrics segments
*/
func GetAvailableMetricsIngestMemory() uint64 {
	if GlobalMemoryTracker == nil {
		return 0
	}
	return GlobalMemoryTracker.MetricsSegmentMaxSize
}
