/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
