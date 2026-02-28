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

package structs

import (
	"testing"

	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_GetAllMeasureAggsInChain(t *testing.T) {
	qa := &QueryAggregators{
		MeasureOperations: []*MeasureAggregator{
			{MeasureCol: "measure1"},
		},
	}

	actual := qa.GetAllMeasureAggsInChain()
	assert.Equal(t, [][]*MeasureAggregator{{{MeasureCol: "measure1"}}}, actual)

	qa = &QueryAggregators{
		Next: &QueryAggregators{
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "measure2"},
			},
		},
	}

	actual = qa.GetAllMeasureAggsInChain()
	assert.Equal(t, [][]*MeasureAggregator{{{MeasureCol: "measure2"}}}, actual)

	qa = &QueryAggregators{
		MeasureOperations: []*MeasureAggregator{
			{MeasureCol: "measure1"},
			{MeasureCol: "measure2"},
		},
		Next: &QueryAggregators{
			Sort: &SortRequest{},
			Next: &QueryAggregators{
				GroupByRequest: &GroupByRequest{
					GroupByColumns:    []string{"column1"},
					MeasureOperations: []*MeasureAggregator{{MeasureCol: "measure3"}},
				},
			},
		},
	}

	actual = qa.GetAllMeasureAggsInChain()
	assert.Equal(t, [][]*MeasureAggregator{
		{{MeasureCol: "measure1"}, {MeasureCol: "measure2"}},
		{{MeasureCol: "measure3"}},
	}, actual)
}

func Test_GetBucketValueForGivenField_StatRes(t *testing.T) {
	br := &BucketResult{
		StatRes: map[string]sutils.CValueEnclosure{
			"field1": {Dtype: sutils.SS_DT_STRING, CVal: "value1"},
		},
		BucketKey:   []string{"key1", "key2"},
		GroupByKeys: []string{"groupKey1", "groupKey2"},
	}

	value, index, foundInStat := br.GetBucketValueForGivenField("field1")
	assert.True(t, foundInStat)
	assert.Equal(t, "value1", value)
	assert.Equal(t, -1, index)
}

func Test_GetBucketValueForGivenField_GroupByKey(t *testing.T) {
	br := &BucketResult{
		StatRes: map[string]sutils.CValueEnclosure{
			"field1": {Dtype: sutils.SS_DT_STRING, CVal: "value1"},
		},
		BucketKey:   []string{"key1", "key2"},
		GroupByKeys: []string{"groupKey1", "groupKey2"},
	}

	value, index, foundInStat := br.GetBucketValueForGivenField("groupKey2")
	assert.Equal(t, "key2", value)
	assert.Equal(t, 1, index)
	assert.False(t, foundInStat)
}

func Test_GetBucketValueForGivenField_GroupByKey_Int(t *testing.T) {
	br := &BucketResult{
		StatRes: map[string]sutils.CValueEnclosure{
			"field1": {Dtype: sutils.SS_DT_STRING, CVal: "value1"},
		},
		BucketKey:   []int{1, 2},
		GroupByKeys: []string{"groupKey1", "groupKey2"},
	}

	value, index, foundInStat := br.GetBucketValueForGivenField("groupKey2")
	assert.Equal(t, 2, value)
	assert.Equal(t, 1, index)
	assert.False(t, foundInStat)
}

func Test_GetBucketValueForGivenField_NotFound(t *testing.T) {
	br := &BucketResult{
		StatRes: map[string]sutils.CValueEnclosure{
			"field1": {Dtype: sutils.SS_DT_STRING, CVal: "value1"},
		},
		BucketKey:   []string{"key1", "key2"},
		GroupByKeys: []string{"groupKey1", "groupKey2"},
	}

	value, index, foundInStat := br.GetBucketValueForGivenField("nonExistentField")
	assert.Nil(t, value)
	assert.Equal(t, -1, index)
	assert.False(t, foundInStat)
}

func Test_GetBucketValueForGivenField_SingleBucketKey(t *testing.T) {
	br := &BucketResult{
		StatRes: map[string]sutils.CValueEnclosure{
			"field1": {Dtype: sutils.SS_DT_STRING, CVal: "value1"},
		},
		BucketKey:   "singleKey",
		GroupByKeys: []string{"groupKey1"},
	}

	value, index, foundInStat := br.GetBucketValueForGivenField("groupKey1")
	assert.Equal(t, "singleKey", value)
	assert.Equal(t, -1, index)
	assert.False(t, foundInStat)
}

func Test_GetBucketValueForGivenField_IndexOutOfRange(t *testing.T) {
	br := &BucketResult{
		StatRes: map[string]sutils.CValueEnclosure{
			"field1": {Dtype: sutils.SS_DT_STRING, CVal: "value1"},
		},
		BucketKey:   []string{"key1"},
		GroupByKeys: []string{"groupKey1", "groupKey2"},
	}

	value, index, foundInStat := br.GetBucketValueForGivenField("groupKey2")
	assert.Nil(t, value)
	assert.Equal(t, -1, index)
	assert.False(t, foundInStat)
}

func Test_GetBucketValueForGivenField_GroupByKeyNotList(t *testing.T) {
	br := &BucketResult{
		StatRes: map[string]sutils.CValueEnclosure{
			"field1": {Dtype: sutils.SS_DT_STRING, CVal: "value1"},
		},
		BucketKey:   "notAList",
		GroupByKeys: []string{"groupKey1"},
	}

	value, index, foundInStat := br.GetBucketValueForGivenField("groupKey1")
	assert.Equal(t, "notAList", value)
	assert.Equal(t, -1, index)
	assert.False(t, foundInStat)
}

func Test_SetBucketValueForGivenField_ValidString(t *testing.T) {
	br := &BucketResult{
		BucketKey:   []string{"key1", "key2"},
		GroupByKeys: []string{"groupKey1", "groupKey2"},
	}

	err := br.SetBucketValueForGivenField("groupKey2", "newKey2", 1, false)
	assert.Nil(t, err)
	assert.Equal(t, []interface{}{"key1", "newKey2"}, br.BucketKey)
}

func Test_SetBucketValueForGivenField_ValidStringList(t *testing.T) {
	br := &BucketResult{
		BucketKey:   []string{"key1", "key2"},
		GroupByKeys: []string{"groupKey1", "groupKey2"},
	}

	err := br.SetBucketValueForGivenField("groupKey2", []string{"newKey2", "anotherKey2"}, 1, false)
	assert.Nil(t, err)
	assert.Equal(t, []interface{}{"key1", []string{"newKey2", "anotherKey2"}}, br.BucketKey)
}

func Test_SetBucketValueForGivenField_InvalidIndex(t *testing.T) {
	br := &BucketResult{
		BucketKey:   []string{"key1", "key2"},
		GroupByKeys: []string{"groupKey1", "groupKey2"},
	}

	err := br.SetBucketValueForGivenField("groupKey2", "newKey2", 2, false)
	assert.NotNil(t, err)
}

func Test_SetBucketValueForGivenField_FieldNotFound(t *testing.T) {
	br := &BucketResult{
		BucketKey:   []string{"key1", "key2"},
		GroupByKeys: []string{"groupKey1", "groupKey2"},
	}

	err := br.SetBucketValueForGivenField("nonExistentField", "value", 1, false)
	assert.NotNil(t, err)
}

func Test_SetBucketValueForGivenField_NotListType(t *testing.T) {
	br := &BucketResult{
		BucketKey:   "notAList",
		GroupByKeys: []string{"groupKey1"},
	}

	err := br.SetBucketValueForGivenField("groupKey1", "newValue", -1, false)
	assert.Nil(t, err)
	assert.Equal(t, "newValue", br.BucketKey)
}

func Test_SetBucketValueForGivenField_StatisticResult(t *testing.T) {
	br := &BucketResult{
		StatRes: map[string]sutils.CValueEnclosure{
			"field1": {Dtype: sutils.SS_DT_STRING, CVal: "value1"},
		},
		BucketKey:   []string{"key1", "key2"},
		GroupByKeys: []string{"groupKey1", "groupKey2"},
	}

	err := br.SetBucketValueForGivenField("field1", "value", 1, true)
	assert.Nil(t, err)
	assert.Equal(t, []string{"key1", "key2"}, br.BucketKey)
	assert.Equal(t, "value", br.StatRes["field1"].CVal)
}

func Test_SetBucketValueForGivenField_ConvertToSlice(t *testing.T) {
	br := &BucketResult{
		BucketKey:   []interface{}{"key1", "key2"},
		GroupByKeys: []string{"groupKey1", "groupKey2"},
	}

	err := br.SetBucketValueForGivenField("groupKey2", "newKey2", 1, false)
	assert.Nil(t, err)
	assert.Equal(t, []interface{}{"key1", "newKey2"}, br.BucketKey)
}

func Test_SetBucketValueForGivenField_IndexOutOfRange(t *testing.T) {
	br := &BucketResult{
		BucketKey:   []string{"key1"},
		GroupByKeys: []string{"groupKey1", "groupKey2"},
	}

	err := br.SetBucketValueForGivenField("groupKey2", "newKey2", 1, false)
	assert.NotNil(t, err)
}

func Test_IsStatsAggPresentInChain(t *testing.T) {
	tests := []struct {
		name     string
		qa       *QueryAggregators
		expected bool
	}{
		{
			name:     "Nil QueryAggregators",
			qa:       nil,
			expected: false,
		},
		{
			name: "Only GroupByRequest Present",
			qa: &QueryAggregators{
				GroupByRequest: &GroupByRequest{},
			},
			expected: true,
		},
		{
			name: "Only MeasureOperations Present",
			qa: &QueryAggregators{
				MeasureOperations: []*MeasureAggregator{
					{MeasureCol: "col1"},
				},
			},
			expected: true,
		},
		{
			name:     "Both GroupByRequest and MeasureOperations Absent",
			qa:       &QueryAggregators{},
			expected: false,
		},
		{
			name: "Next Aggregator in the Chain",
			qa: &QueryAggregators{
				Next: &QueryAggregators{
					GroupByRequest: &GroupByRequest{},
				},
			},
			expected: true,
		},
		{
			name: "Nested Next Aggregator Chain",
			qa: &QueryAggregators{
				Next: &QueryAggregators{
					Next: &QueryAggregators{
						MeasureOperations: []*MeasureAggregator{
							{MeasureCol: "col1"},
						},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		actual := tt.qa.IsStatsAggPresentInChain()
		assert.Equal(t, tt.expected, actual, tt.name)
	}
}

func Test_EncodeDecodeSegStats(t *testing.T) {
	segStatsList := []*SegStats{
		{
			IsNumeric: true,
			Count:     123,
			Min: sutils.CValueEnclosure{
				Dtype: sutils.SS_DT_SIGNED_NUM,
				CVal:  int64(1),
			},
			Max: sutils.CValueEnclosure{
				Dtype: sutils.SS_DT_SIGNED_NUM,
				CVal:  int64(42),
			},
			NumStats: &NumericStats{
				NumericCount: 10,
				Sum: sutils.NumTypeEnclosure{
					Ntype:    sutils.SS_DT_SIGNED_NUM,
					IntgrVal: 200,
				},
			},
			StringStats: nil,
			Records:     nil,
		},
		{
			IsNumeric: false,
			Count:     42,
			Min:       sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "str1"},
			Max:       sutils.CValueEnclosure{Dtype: sutils.SS_DT_STRING, CVal: "str2"},
			NumStats:  nil,
			StringStats: &StringStats{
				StrSet: map[string]struct{}{
					"str1": {},
					"str2": {},
				},
				StrList: []string{
					"str1",
					"str2",
				},
			},
			Records: nil,
		},
	}

	for _, originalSegStats := range segStatsList {
		originalSegStats.CreateNewHll()

		segStatsJson, err := originalSegStats.ToJSON()
		assert.NoError(t, err)
		assert.NotNil(t, segStatsJson)

		recoveredSegStats, err := segStatsJson.ToStats()
		assert.NoError(t, err)
		assert.NotNil(t, recoveredSegStats)

		assert.Equal(t, originalSegStats, recoveredSegStats)
	}
}

func Test_EqualsIsDeepEquals(t *testing.T) {
	segStat1 := &SegStats{
		IsNumeric: false,
		Count:     42,
		NumStats:  nil,
		StringStats: &StringStats{
			StrSet: map[string]struct{}{
				"str1": {},
				"str2": {},
			},
			StrList: []string{
				"str1",
				"str2",
			},
		},
		Records: nil,
	}

	segStat2 := &SegStats{
		IsNumeric: false,
		Count:     42,
		NumStats:  nil,
		StringStats: &StringStats{
			StrSet: map[string]struct{}{
				"str1": {},
				"str2": {},
			},
			StrList: []string{
				"str1",
				"str2",
			},
		},
		Records: nil,
	}

	segStat1.CreateNewHll()

	segStat2.CreateNewHll()

	assert.Equal(t, segStat1, segStat2)

	segStat2.StringStats.StrSet = map[string]struct{}{
		"str1": {},
		"str2": {},
		"str3": {},
	}

	assert.NotEqual(t, segStat1, segStat2)

	segStat2.StringStats.StrSet = map[string]struct{}{
		"str1": {},
		"str2": {},
	}

	segStat2.StringStats.StrList = []string{
		"str1",
		"str2",
		"str3",
	}

	assert.NotEqual(t, segStat1, segStat2)
}
