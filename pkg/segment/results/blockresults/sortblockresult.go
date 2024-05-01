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
	"container/heap"
	"errors"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
)

type SortResults struct {
	Results   *SortedResultRecords
	Ascending bool                 // ascending or descending order
	Count     uint64               // number of results
	LastValue float64              // value of the record to be replaced (ex. the highest min, the lowest max for Count elems)
	Sort      *structs.SortRequest // request for aggregations
}

func GetRrsFromRrc(rec *utils.RecordResultContainer, sortReq *structs.SortRequest) *ResultRecordSort {

	currRrs := &ResultRecordSort{
		Rrc:       rec,
		Ascending: sortReq.Ascending,
	}
	return currRrs
}

func InitializeSort(count uint64, sort *structs.SortRequest) (*SortResults, error) {
	if sort == nil {
		return nil, errors.New("received a sort request with no aggregations")
	}
	var sortedResRecs SortedResultRecords
	heap.Init(&sortedResRecs)
	return &SortResults{
		Results:   &sortedResRecs,
		Count:     count,
		Ascending: sort.Ascending,
		Sort:      sort,
	}, nil
}

/*
Returns:
  - bool if this record was added
  - string for any remote records that were removed
*/
func (s *SortResults) Add(rrc *utils.RecordResultContainer) (bool, string) {

	// first record seen
	if currRes := uint64(s.Results.Len()); currRes == 0 {
		sortedRec := GetRrsFromRrc(rrc, s.Sort)
		heap.Push(s.Results, sortedRec)
		s.LastValue = rrc.SortColumnValue
		return true, ""
	} else if currRes < s.Count {
		// less than Count records seen
		sortedRec := GetRrsFromRrc(rrc, s.Sort)
		heap.Push(s.Results, sortedRec)
		if s.Ascending { // last value should be largest value (first 'min' to be replaced)
			if rrc.SortColumnValue > s.LastValue {
				s.LastValue = rrc.SortColumnValue
			}
		} else { // last value should be smallest value (first 'max' to be replaced)
			if rrc.SortColumnValue < s.LastValue {
				s.LastValue = rrc.SortColumnValue
			}
		}
		return true, ""
	} else { // heap has Count elements
		if s.ShouldAddRecord(rrc) {
			sortedRec := GetRrsFromRrc(rrc, s.Sort)
			heap.Push(s.Results, sortedRec)
			removedVal := s.UpdateLastValue()
			return true, removedVal
		}
	}
	return false, ""
}

func (s *SortResults) ShouldAddRecord(rrc *utils.RecordResultContainer) bool {
	if s.Ascending {
		if rrc.SortColumnValue < s.LastValue {
			return true
		}
	} else {
		if rrc.SortColumnValue > s.LastValue {
			return true
		}
	}
	return false
}

/*
		Returns:
			- string for any records that were removed

	 TODO: be smarter about updating and removing last value
*/
func (s *SortResults) UpdateLastValue() string {

	idx := 0
	var found bool
	var removed string // id of the record that was removed
	if !s.Ascending {
		for i := int(s.Count) - 1; i >= 0; i-- {
			if (*s.Results)[i].Rrc.SortColumnValue == s.LastValue {
				idx = i
				found = true
				removed = (*s.Results)[i].Rrc.SegKeyInfo.RecordId
				break
			}
		}
	} else {
		for i := 0; i < int(s.Count); i++ {
			if (*s.Results)[i].Rrc.SortColumnValue == s.LastValue {
				idx = i
				found = true
				removed = (*s.Results)[i].Rrc.SegKeyInfo.RecordId
				break
			}
		}
	}

	if found {
		heap.Remove(s.Results, idx)
	}
	s.LastValue = s.GetLastValue()
	return removed
}

func (s *SortResults) GetLastValue() float64 {
	var lastVal float64

	if !s.Ascending {
		if s.Count > 0 {
			lastVal = (*s.Results)[s.Count-1].Rrc.SortColumnValue
		}
		for i := int(s.Count) - 2; i >= 0; i-- {
			if (*s.Results)[i].Rrc.SortColumnValue < lastVal {
				lastVal = (*s.Results)[i].Rrc.SortColumnValue
			}
		}
	} else {
		if s.Count > 0 {
			lastVal = (*s.Results)[0].Rrc.SortColumnValue
		}
		for i := uint64(1); i < s.Count; i++ {
			if (*s.Results)[i].Rrc.SortColumnValue > lastVal {
				lastVal = (*s.Results)[i].Rrc.SortColumnValue
			}
		}
	}

	return lastVal
}

// This function uses heap.Pop, therefore can only be called once
func (s *SortResults) GetSortedResults() []*utils.RecordResultContainer {
	size := uint64(s.Results.Len())
	if s.Count < size {
		size = s.Count
	}

	allSorts := make([]*utils.RecordResultContainer, size)
	resultIdx := uint64(0)
	for s.Results.Len() > 0 && resultIdx < s.Count {
		item := heap.Pop(s.Results).(*ResultRecordSort)
		allSorts[resultIdx] = item.Rrc
		resultIdx++
	}
	allSorts = allSorts[:resultIdx]
	return allSorts
}

func (s *SortResults) GetSortedResultsCopy() []*utils.RecordResultContainer {
	size := uint64(s.Results.Len())
	if s.Count < size {
		size = s.Count
	}

	allSorts := make([]*utils.RecordResultContainer, size)
	resultIdx := uint64(0)

	var newHeap SortedResultRecords
	heap.Init(&newHeap)
	for s.Results.Len() > 0 && resultIdx < s.Count {
		item := heap.Pop(s.Results).(*ResultRecordSort)
		allSorts[resultIdx] = item.Rrc
		resultIdx++
		newHeap.Push(item)
	}
	allSorts = allSorts[:resultIdx]
	s.Results = &newHeap
	return allSorts
}
