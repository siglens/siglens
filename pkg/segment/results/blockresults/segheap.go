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
