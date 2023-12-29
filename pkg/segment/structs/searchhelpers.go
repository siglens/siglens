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

package structs

import (
	"github.com/siglens/siglens/pkg/segment/pqmr"
)

// a helper struct to keep track to the blocks search status.
// This struct will be used to re-use slices of matched time stamps
// it is important to know that the slice is re-used, so callers would need to copy the values if needed
// else, will leak memory
type BlockSearchHelper struct {
	matchedRecs *pqmr.PQMatchResults
}

func InitBlockSearchHelper() *BlockSearchHelper {
	return &BlockSearchHelper{
		matchedRecs: pqmr.CreatePQMatchResults(uint(0)),
	}
}

func InitAllBlockSearchHelpers(fileParallelism int64) []*BlockSearchHelper {
	allHelpers := make([]*BlockSearchHelper, fileParallelism)

	for i := int64(0); i < fileParallelism; i++ {
		allHelpers[i] = InitBlockSearchHelper()
	}
	return allHelpers
}

// keep allocated slice
func (h *BlockSearchHelper) ResetBlockHelper() {
	h.matchedRecs.ResetAll()
}

func (h *BlockSearchHelper) GetAllMatchedRecords() *pqmr.PQMatchResults {
	return h.matchedRecs
}

func (h *BlockSearchHelper) AddMatchedRecord(recNum uint) {
	h.matchedRecs.AddMatchedRecord(recNum)
}

func (h *BlockSearchHelper) ClearBit(recNum uint) {
	h.matchedRecs.ClearBit(recNum)
}

func (h *BlockSearchHelper) DoesRecordMatch(recNum uint) bool {
	return h.matchedRecs.DoesRecordMatch(recNum)
}
