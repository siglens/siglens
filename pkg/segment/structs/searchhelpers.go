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
