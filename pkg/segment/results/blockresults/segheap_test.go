// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package blockresults

import (
	"container/heap"
	"math/rand"
	"testing"

	sutils "github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_recordHeapAscending(t *testing.T) {

	count := 10
	ascending := true
	pq := make(SortedResultRecords, 0)
	heap.Init(&pq)

	for i := count; i > 0; i-- {
		currRRC := &sutils.RecordResultContainer{
			SortColumnValue: float64(i),
		}
		currItem := &ResultRecordSort{
			Ascending: ascending,
			Rrc:       currRRC,
		}
		heap.Push(&pq, currItem)
	}
	currRRC := &sutils.RecordResultContainer{
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

	currRRC := &sutils.RecordResultContainer{
		SortColumnValue: float64(1.1),
	}
	currItem := &ResultRecordSort{
		Ascending: ascending,
		Rrc:       currRRC,
	}
	heap.Push(&pq, currItem)

	for i := count; i > 0; i-- {
		currRRC := &sutils.RecordResultContainer{
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
