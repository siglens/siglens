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
	"math/rand"
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_simpleRecordSortAscendingInvalid(t *testing.T) {

	count := uint64(10)
	ascending := true
	sortReq := &structs.SortRequest{
		ColName:   "name",
		Ascending: ascending,
	}
	sort, _ := InitializeSort(count, sortReq)

	for i := count; i > 0; i-- {
		currItem := &utils.RecordResultContainer{
			SortColumnValue: float64(i),
		}
		sort.Add(currItem)
	}

	currItem := &utils.RecordResultContainer{
		SortColumnValue: float64(count + 1),
	}
	sort.Add(currItem)
	allVals := sort.GetSortedResults()
	minVal := allVals[0].SortColumnValue
	maxVal := allVals[len(allVals)-1].SortColumnValue
	assert.Equal(t, minVal, float64(1))
	assert.Equal(t, maxVal, float64(count))

	var prevVal float64
	firstVal := true
	for i := uint64(0); i < count; i++ {
		item := allVals[i]
		if firstVal {
			prevVal = item.SortColumnValue
			firstVal = false
		} else {
			assert.True(t, item.SortColumnValue > prevVal)
			prevVal = item.SortColumnValue
		}
		log.Infof("%+v: %+v ", i, item.SortColumnValue)
	}
	assert.Len(t, allVals, 10, "dont add invalid column/more than count")
}

func Test_RecordSortAscendingReplace(t *testing.T) {

	count := uint64(10)
	ascending := true
	sortReq := &structs.SortRequest{
		ColName:   "name",
		Ascending: ascending,
	}
	sort, _ := InitializeSort(count, sortReq)

	for i := count; i > 0; i-- {
		currItem := &utils.RecordResultContainer{
			SortColumnValue: float64(i),
		}
		sort.Add(currItem)
	}
	currItem := &utils.RecordResultContainer{
		SortColumnValue: float64(1.1),
	}
	sort.Add(currItem)

	currItem = &utils.RecordResultContainer{
		SortColumnValue: float64(count + 1),
	}
	sort.Add(currItem)
	allVals := sort.GetSortedResults()
	minVal := allVals[0].SortColumnValue
	maxVal := allVals[len(allVals)-1].SortColumnValue
	assert.Equal(t, minVal, float64(1))
	assert.Equal(t, allVals[1].SortColumnValue, float64(1.1))
	assert.Equal(t, maxVal, float64(count-1))

	var prevVal float64
	firstVal := true
	for i := uint64(0); i < count; i++ {
		item := allVals[i]
		if firstVal {
			prevVal = item.SortColumnValue
			firstVal = false
		} else {
			assert.True(t, item.SortColumnValue > prevVal)
			prevVal = item.SortColumnValue
		}
		log.Infof("%+v: %+v ", i, item.SortColumnValue)
	}
	assert.Len(t, allVals, 10, "dont add invalid column/more than count")
}

func Test_RecordSortDescendingInvalid(t *testing.T) {

	count := uint64(10)
	ascending := false
	sortReq := &structs.SortRequest{
		ColName:   "name",
		Ascending: ascending,
	}
	sort, _ := InitializeSort(count, sortReq)

	currItem := &utils.RecordResultContainer{
		SortColumnValue: float64(1.1),
	}
	sort.Add(currItem)

	for i := count; i > 0; i-- {
		currItem := &utils.RecordResultContainer{
			SortColumnValue: rand.Float64(),
		}
		sort.Add(currItem)
	}

	currItem = &utils.RecordResultContainer{
		SortColumnValue: float64(count + 1),
	}
	sort.Add(currItem)
	allVals := sort.GetSortedResults()

	// we know first two values should be (count + 1) and 1.1 as rand.Float64() gives a float (0, 1)
	assert.Equal(t, allVals[0].SortColumnValue, float64(11))
	assert.Equal(t, allVals[1].SortColumnValue, float64(1.1))
	var prevVal float64
	firstVal := true
	for i := uint64(0); i < count; i++ {
		item := allVals[i]
		if firstVal {
			prevVal = item.SortColumnValue
			firstVal = false
		} else {
			assert.True(t, item.SortColumnValue < prevVal)
			prevVal = item.SortColumnValue
		}
		log.Infof("%+v: %+v ", i, item.SortColumnValue)
	}
	assert.Len(t, allVals, 10, "dont add invalid column/more than count")
}

func Test_RecordSortDescendingReplace(t *testing.T) {

	count := uint64(10)
	ascending := false
	sortReq := &structs.SortRequest{
		ColName:   "name",
		Ascending: ascending,
	}
	sort, _ := InitializeSort(count, sortReq)

	currItem := &utils.RecordResultContainer{
		SortColumnValue: float64(1.1),
	}
	sort.Add(currItem)

	for i := count; i > 0; i-- {
		currItem := &utils.RecordResultContainer{
			SortColumnValue: rand.Float64(),
		}
		sort.Add(currItem)
	}

	currItem = &utils.RecordResultContainer{
		SortColumnValue: float64(count + 1),
	}
	sort.Add(currItem)
	allVals := sort.GetSortedResults()

	var prevVal float64
	firstVal := true
	for i := uint64(0); i < count; i++ {
		item := allVals[i]
		if firstVal {
			prevVal = item.SortColumnValue
			firstVal = false
		} else {
			assert.True(t, item.SortColumnValue < prevVal)
			prevVal = item.SortColumnValue
		}
		log.Infof("%+v: %+v ", i, item.SortColumnValue)
	}
	assert.Len(t, allVals, 10, "dont add invalid column/more than count")
}
