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
	"bytes"
	"fmt"
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

// TODO: more tests for more types of AggFunc
func Test_RunningStatsAdd(t *testing.T) {

	aggs := &structs.QueryAggregators{
		TimeHistogram: &structs.TimeBucket{},
	}
	bRes, err := InitBlockResults(10, aggs, 0)
	assert.NoError(t, err)
	for i := uint64(0); i < 10; i++ {
		bRes.AddKeyToTimeBucket(i, 5)
	}
	assert.NotNil(t, bRes.TimeAggregation)
	assert.Len(t, bRes.TimeAggregation.AllRunningBuckets, 10)
	for _, bucket := range bRes.TimeAggregation.AllRunningBuckets {
		assert.Equal(t, bucket.count, uint64(5))
	}
}

func Test_JoinStats(t *testing.T) {
	aggs := &structs.QueryAggregators{
		GroupByRequest: &structs.GroupByRequest{
			GroupByColumns: []string{"a", "b"},
			MeasureOperations: []*structs.MeasureAggregator{
				{MeasureCol: "c", MeasureFunc: utils.Min},
				{MeasureCol: "d", MeasureFunc: utils.Max},
			},
			BucketCount: 100,
		},
	}
	bRes, err := InitBlockResults(10, aggs, 0)
	assert.NoError(t, err)
	for i := uint64(0); i < 10; i++ {
		var buf bytes.Buffer
		buf.WriteString(fmt.Sprintf("%v", i))
		mRes := []utils.CValueEnclosure{
			{CVal: uint64(1), Dtype: utils.SS_DT_UNSIGNED_NUM},
			{CVal: uint64(1), Dtype: utils.SS_DT_UNSIGNED_NUM},
		}
		bRes.AddMeasureResultsToKey(buf, mRes, "", false, 5)
	}
	assert.NotNil(t, bRes.GroupByAggregation)
	assert.Len(t, bRes.GroupByAggregation.AllRunningBuckets, 10)
	for _, bucket := range bRes.GroupByAggregation.AllRunningBuckets {
		assert.Len(t, bucket.runningStats, 2)
		assert.Equal(t, bucket.runningStats[0].rawVal, utils.CValueEnclosure{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)})
		assert.Equal(t, bucket.runningStats[1].rawVal, utils.CValueEnclosure{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)})
		assert.Equal(t, bucket.count, uint64(1))
	}

	toMerge, err := InitBlockResults(10, aggs, 0)
	assert.NoError(t, err)
	for i := uint64(0); i < 10; i++ {
		var buf bytes.Buffer
		buf.WriteString(fmt.Sprintf("%v", i))
		mRes := []utils.CValueEnclosure{
			{CVal: uint64(1), Dtype: utils.SS_DT_UNSIGNED_NUM},
			{CVal: i, Dtype: utils.SS_DT_UNSIGNED_NUM},
		}
		toMerge.AddMeasureResultsToKey(buf, mRes, "", false, 5)
	}

	bRes.MergeBuckets(toMerge)
	assert.Len(t, bRes.GroupByAggregation.StringBucketIdx, 10)
	for i := uint64(0); i < 10; i++ {
		key := fmt.Sprintf("%v", i)
		idx, ok := bRes.GroupByAggregation.StringBucketIdx[key]
		assert.True(t, ok)
		bucket := bRes.GroupByAggregation.AllRunningBuckets[idx]
		assert.Equal(t, bucket.count, uint64(2))
		assert.Equal(t, bucket.runningStats[0].rawVal, utils.CValueEnclosure{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)}, "min stays the same")

		var max = i
		if i == 0 {
			max = uint64(1)
		}
		assert.Equal(t, bucket.runningStats[1].rawVal, utils.CValueEnclosure{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: max}, "max should be i or 1 if i==0")
	}
}
