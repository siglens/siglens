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
