// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package structs

import (
	"slices"
	"sort"

	"github.com/siglens/siglens/pkg/segment/pqmr"
)

// a helper struct to keep track to the blocks search status.
// This struct will be used to re-use slices of matched time stamps
// it is important to know that the slice is re-used, so callers would need to copy the values if needed
// else, will leak memory
type BlockSearchHelper struct {
	matchedRecs        *pqmr.PQMatchResults
	sortedValidRecords []uint // If not nil, only these record numbers can be added to matchedRecs
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
	h.sortedValidRecords = nil
}

func (h *BlockSearchHelper) GetAllMatchedRecords() *pqmr.PQMatchResults {
	return h.matchedRecs
}

func (h *BlockSearchHelper) AddMatchedRecord(recNum uint) {
	if h.sortedValidRecords != nil {
		if _, isValid := slices.BinarySearch(h.sortedValidRecords, recNum); !isValid {
			return
		}
	}

	h.matchedRecs.AddMatchedRecord(recNum)
}

func (h *BlockSearchHelper) ClearBit(recNum uint) {
	h.matchedRecs.ClearBit(recNum)
}

func (h *BlockSearchHelper) DoesRecordMatch(recNum uint) bool {
	return h.matchedRecs.DoesRecordMatch(recNum)
}

func (h *BlockSearchHelper) SetValidRecords(validRecords []uint) {
	sort.Slice(validRecords, func(i, j int) bool {
		return validRecords[i] < validRecords[j]
	})

	h.sortedValidRecords = validRecords
}

func (h *BlockSearchHelper) GetValidRecords() []uint {
	return h.sortedValidRecords
}
