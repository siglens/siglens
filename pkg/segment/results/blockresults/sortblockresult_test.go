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
