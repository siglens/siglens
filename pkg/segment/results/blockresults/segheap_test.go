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
	"math/rand"
	"testing"

	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_recordHeapAscending(t *testing.T) {

	count := 10
	ascending := true
	pq := make(SortedResultRecords, 0)
	heap.Init(&pq)

	for i := count; i > 0; i-- {
		currRRC := &utils.RecordResultContainer{
			SortColumnValue: float64(i),
		}
		currItem := &ResultRecordSort{
			Ascending: ascending,
			Rrc:       currRRC,
		}
		heap.Push(&pq, currItem)
	}
	currRRC := &utils.RecordResultContainer{
		SortColumnValue: float64(1.1),
	}
	currItem := &ResultRecordSort{
		Ascending: ascending,
		Rrc:       currRRC,
	}
	heap.Push(&pq, currItem)

	var prevVal float64
	firstVal := true
	for i := 0; i < count; i++ {
		item := heap.Pop(&pq).(*ResultRecordSort)
		if firstVal {
			prevVal = item.Rrc.SortColumnValue
			firstVal = false
		} else {
			assert.True(t, item.Rrc.SortColumnValue > prevVal)
			prevVal = item.Rrc.SortColumnValue
		}
		log.Infof("%+v: %+v ", item.Index, item.Rrc.SortColumnValue)
	}
	_ = heap.Pop(&pq).(*ResultRecordSort)
	assert.Equal(t, pq.Len(), 0)
}

func Test_recordHeapDescending(t *testing.T) {

	count := 10
	ascending := false
	pq := make(SortedResultRecords, 0)
	heap.Init(&pq)

	currRRC := &utils.RecordResultContainer{
		SortColumnValue: float64(1.1),
	}
	currItem := &ResultRecordSort{
		Ascending: ascending,
		Rrc:       currRRC,
	}
	heap.Push(&pq, currItem)

	for i := count; i > 0; i-- {
		currRRC := &utils.RecordResultContainer{
			SortColumnValue: rand.Float64(),
		}
		currItem := &ResultRecordSort{
			Ascending: ascending,
			Rrc:       currRRC,
		}
		heap.Push(&pq, currItem)
	}

	var prevVal float64
	firstVal := true
	for i := 0; i < count; i++ {
		item := heap.Pop(&pq).(*ResultRecordSort)
		if firstVal {
			prevVal = item.Rrc.SortColumnValue
			firstVal = false
		} else {
			assert.True(t, item.Rrc.SortColumnValue < prevVal)
			prevVal = item.Rrc.SortColumnValue
		}
		log.Infof("%+v: %+v ", item.Index, item.Rrc.SortColumnValue)
	}
	_ = heap.Pop(&pq).(*ResultRecordSort)
	assert.Equal(t, pq.Len(), 0)
}
