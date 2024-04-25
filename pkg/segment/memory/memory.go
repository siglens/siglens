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
