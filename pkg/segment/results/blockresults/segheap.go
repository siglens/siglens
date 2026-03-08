// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package blockresults

import (
	. "github.com/siglens/siglens/pkg/segment/utils"
)

type ResultRecordSort struct {
	Index     int
	Rrc       *RecordResultContainer
	Ascending bool
}

type SortedResultRecords []*ResultRecordSort

func (srr SortedResultRecords) Len() int { return len(srr) }

func (srr SortedResultRecords) Less(i, j int) bool {

	if !srr[i].Ascending {
		// We want Pop to give us the highest priority so we use greater than here.
		return srr[i].Rrc.SortColumnValue > srr[j].Rrc.SortColumnValue
	} else {
		// We want Pop to give us the lowest priority so we use less than here.
		return srr[i].Rrc.SortColumnValue < srr[j].Rrc.SortColumnValue
	}
}

func (pq SortedResultRecords) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].Index = i
	pq[j].Index = j
}

func (pq *SortedResultRecords) Push(x interface{}) {
	n := len(*pq)
	item := x.(*ResultRecordSort)
	item.Index = n
	*pq = append(*pq, item)
}

func (pq *SortedResultRecords) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}
