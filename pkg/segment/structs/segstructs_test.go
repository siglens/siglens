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

	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

/*
	The unit tests will cover different scenarios:
		- when the QueryAggregators instance is nil,
		- when it is not nil but the TransactionArguments field is nil,
		- when both the instance and its TransactionArguments field are not nil.
*/

// Test_HasTransactionArguments_NilReceiver tests the case where the QueryAggregators receiver is nil.
func Test_HasTransactionArguments_NilReceiver(t *testing.T) {
	var qa *QueryAggregators
	assert.Equal(t, false, qa.HasTransactionArguments(), "Expected false for nil receiver, got true")
}

// Test_HasTransactionArguments_NilTransactionArguments tests the case where TransactionArguments is nil.
func Test_HasTransactionArguments_NilTransactionArguments(t *testing.T) {
	qa := &QueryAggregators{}
	assert.Equal(t, false, qa.HasTransactionArguments(), "Expected false when TransactionArguments is nil, got true")
}

// Test_HasTransactionArguments_NonNilTransactionArguments tests the case where TransactionArguments is not nil.
func Test_HasTransactionArguments_NonNilTransactionArguments(t *testing.T) {
	transactionArgs := TransactionArguments{
		Fields:     []string{"test1", "test2"},
		StartsWith: nil,
		EndsWith:   nil,
	}
	qa := &QueryAggregators{
		TransactionArguments: &transactionArgs,
	}
	assert.Equal(t, true, qa.HasTransactionArguments(), "Expected true when TransactionArguments is not nil, got false")
}

func Test_HasQueryAggergatorBlock_NilQueryAggregators(t *testing.T) {
	var qa *QueryAggregators

	assert.False(t, qa.HasQueryAggergatorBlock())
}
func Test_HasQueryAggergatorBlock_NilOutputTransforms(t *testing.T) {
	var ot *OutputTransforms

	qa := &QueryAggregators{
		OutputTransforms: ot,
	}

	assert.False(t, qa.HasQueryAggergatorBlock())
}

func Test_HasQueryAggergatorBlock_HasLetColumnsRequest(t *testing.T) {
	lcr := &LetColumnsRequest{
		RexColRequest: &RexExpr{},
	}

	ot := &OutputTransforms{
		LetColumns: lcr,
	}

	qa := &QueryAggregators{
		OutputTransforms: ot,
	}

	assert.True(t, qa.HasQueryAggergatorBlock())
}

func Test_HasQueryAggergatorBlock_HasNoLetColumnsRequest(t *testing.T) {
	lcr := &LetColumnsRequest{}

	ot := &OutputTransforms{
		LetColumns: lcr,
	}

	qa := &QueryAggregators{
		OutputTransforms: ot,
	}

	assert.False(t, qa.HasQueryAggergatorBlock())
}

func Test_HasQueryAggergatorBlock_MaxRowsGreaterThanRowAdded(t *testing.T) {
	ot := &OutputTransforms{
		MaxRows:   12,
		RowsAdded: 1,
	}

	qa := &QueryAggregators{
		OutputTransforms: ot,
	}

	assert.True(t, qa.HasQueryAggergatorBlock())
}

func Test_HasQueryAggergatorBlock_MaxRowsLessThanRowAdded(t *testing.T) {
	ot := &OutputTransforms{
		MaxRows:   1,
		RowsAdded: 9,
	}

	qa := &QueryAggregators{
		OutputTransforms: ot,
	}

	assert.False(t, qa.HasQueryAggergatorBlock())
}

func Test_HasQueryAggergatorBlock(t *testing.T) {
	lcr := &LetColumnsRequest{
		RexColRequest: &RexExpr{},
	}

	ot := &OutputTransforms{
		LetColumns: lcr,
		MaxRows:    5,
		RowsAdded:  1,
	}

	qa := &QueryAggregators{
		OutputTransforms: ot,
	}

	assert.True(t, qa.HasQueryAggergatorBlock())
}

func Test_HasDedupBlock_NilQueryAggregators(t *testing.T) {
	var qa *QueryAggregators

	assert.False(t, qa.HasDedupBlock())
}

func Test_HasDedupBlock_NilOutputTransforms(t *testing.T) {
	var ot *OutputTransforms

	qa := &QueryAggregators{
		OutputTransforms: ot,
	}

	assert.False(t, qa.HasDedupBlock())
}

func Test_HasDedupBlock_NilLetcolumns(t *testing.T) {
	var lcr *LetColumnsRequest

	ot := &OutputTransforms{
		LetColumns: lcr,
	}

	qa := &QueryAggregators{
		OutputTransforms: ot,
	}

	assert.False(t, qa.HasDedupBlock())
}

func Test_HasDedupBlock_NilDedupColRequest(t *testing.T) {
	var dcr *DedupExpr

	lcr := &LetColumnsRequest{
		DedupColRequest: dcr,
	}

	ot := &OutputTransforms{
		LetColumns: lcr,
	}

	qa := &QueryAggregators{
		OutputTransforms: ot,
	}

	assert.False(t, qa.HasDedupBlock())
}

func Test_HasDedupBlock_NonNilDedupColRequest(t *testing.T) {
	dcr := &DedupExpr{}

	lcr := &LetColumnsRequest{
		DedupColRequest: dcr,
	}

	ot := &OutputTransforms{
		LetColumns: lcr,
	}

	qa := &QueryAggregators{
		OutputTransforms: ot,
	}

	assert.True(t, qa.HasDedupBlock())
}

func Test_HasQueryAggergatorBlockInChain_EmptyChain(t *testing.T) {
	qa := &QueryAggregators{}

	assert.False(t, qa.HasQueryAggergatorBlockInChain(), "Expected false when the chain is empty, got true")
}

func Test_HasQueryAggergatorBlockInChain_SingleNodeWithBlock(t *testing.T) {
	qa := &QueryAggregators{
		OutputTransforms: &OutputTransforms{
			MaxRows:   10,
			RowsAdded: 1,
		},
	}

	assert.True(t, qa.HasQueryAggergatorBlockInChain(), "Expected true when single node has a query aggregator block, got false")
}

func Test_HasQueryAggergatorBlockInChain_SingleNodeWithoutBlock(t *testing.T) {
	qa := &QueryAggregators{
		OutputTransforms: &OutputTransforms{
			MaxRows:   1,
			RowsAdded: 10,
		},
	}

	assert.False(t, qa.HasQueryAggergatorBlockInChain(), "Expected false when single node does not have a query aggregator block, got true")
}

func Test_HasQueryAggergatorBlockInChain_MultipleNodesWithBlockAtEnd(t *testing.T) {
	qa := &QueryAggregators{
		Next: &QueryAggregators{
			OutputTransforms: &OutputTransforms{
				MaxRows:   10,
				RowsAdded: 1,
			},
		},
	}

	assert.True(t, qa.HasQueryAggergatorBlockInChain(), "Expected true when a node in the chain has a query aggregator block, got false")
}

func Test_HasQueryAggergatorBlockInChain_MultipleNodesWithoutBlock(t *testing.T) {
	qa := &QueryAggregators{
		Next: &QueryAggregators{},
	}

	assert.False(t, qa.HasQueryAggergatorBlockInChain(), "Expected false when no nodes in the chain have a query aggregator block, got true")
}

func Test_HasQueryAggergatorBlockInChain_MultipleNodesWithBlockAtStart(t *testing.T) {
	qa := &QueryAggregators{
		OutputTransforms: &OutputTransforms{
			MaxRows:   10,
			RowsAdded: 1,
		},
		Next: &QueryAggregators{},
	}

	assert.True(t, qa.HasQueryAggergatorBlockInChain(), "Expected true when the first node in the chain has a query aggregator block, got false")
}

func Test_HasQueryAggergatorBlockInChain_MultipleNodesWithBlockInEnd(t *testing.T) {
	qa := &QueryAggregators{
		Next: &QueryAggregators{
			Next: &QueryAggregators{
				OutputTransforms: &OutputTransforms{
					MaxRows:   10,
					RowsAdded: 1,
				},
			},
		},
	}

	assert.True(t, qa.HasQueryAggergatorBlockInChain(), "Expected true when a middle node in the chain has a query aggregator block, got false")
}

func Test_HasQueryAggergatorBlockInChain_MultipleNodesWithBlockInMiddle(t *testing.T) {
	qa := &QueryAggregators{
		Next: &QueryAggregators{
			OutputTransforms: &OutputTransforms{
				MaxRows:   10,
				RowsAdded: 1,
			},
			Next: &QueryAggregators{},
		},
	}

	assert.True(t, qa.HasQueryAggergatorBlockInChain(), "Expected true when a middle node in the chain has a query aggregator block, got false")
}

func Test_HasGroupByOrMeasureAggsInChain_EmptyChain(t *testing.T) {
	qa := &QueryAggregators{}

	assert.False(t, qa.HasGroupByOrMeasureAggsInChain(), "Expected false when the chain is empty, got true")
}

func Test_HasGroupByOrMeasureAggsInChain_SingleNodeWithGroupBy(t *testing.T) {
	qa := &QueryAggregators{
		GroupByRequest: &GroupByRequest{
			GroupByColumns: []string{"column1"},
		},
	}

	assert.True(t, qa.HasGroupByOrMeasureAggsInChain(), "Expected true when single node has a group by aggregator block, got false")
}

func Test_HasGroupByOrMeasureAggsInChain_SingleNodeWithMeasure(t *testing.T) {
	qa := &QueryAggregators{
		MeasureOperations: []*MeasureAggregator{
			{MeasureCol: "measure1"},
		},
	}

	assert.True(t, qa.HasGroupByOrMeasureAggsInChain(), "Expected true when single node has a measure aggregator block, got false")
}

func Test_HasGroupByOrMeasureAggsInChain_MultipleNodesWithGroupByAtEnd(t *testing.T) {
	qa := &QueryAggregators{
		Next: &QueryAggregators{
			GroupByRequest: &GroupByRequest{
				GroupByColumns: []string{"column1"},
			},
		},
	}

	assert.True(t, qa.HasGroupByOrMeasureAggsInChain(), "Expected true when a node in the chain has a group by aggregator block, got false")
}

func Test_HasGroupByOrMeasureAggsInChain_MultipleNodesWithMeasureAtStart(t *testing.T) {
	qa := &QueryAggregators{
		MeasureOperations: []*MeasureAggregator{
			{MeasureCol: "measure1"},
		},
		Next: &QueryAggregators{},
	}

	assert.True(t, qa.HasGroupByOrMeasureAggsInChain(), "Expected true when the first node in the chain has a measure aggregator block, got false")
}

func Test_HasGroupByOrMeasureAggsInChain_MultipleNodesWithoutAggs(t *testing.T) {
	qa := &QueryAggregators{
		Next: &QueryAggregators{},
	}

	assert.False(t, qa.HasGroupByOrMeasureAggsInChain(), "Expected false when no nodes in the chain have a group by or measure aggregator block, got true")
}

func Test_HasGroupByOrMeasureAggsInChain_MultipleNodesWithMeasureInMiddle(t *testing.T) {
	qa := &QueryAggregators{
		Next: &QueryAggregators{
			MeasureOperations: []*MeasureAggregator{
				{MeasureCol: "measure1"},
			},
			Next: &QueryAggregators{},
		},
	}

	assert.True(t, qa.HasGroupByOrMeasureAggsInChain(), "Expected true when a middle node in the chain has a measure aggregator block, got false")
}

func Test_HasGroupByOrMeasureAggsInBlock_NilInstance(t *testing.T) {
	var qa *QueryAggregators

	assert.False(t, qa.HasGroupByOrMeasureAggsInBlock(), "Expected false when instance is nil, got true")
}

func Test_HasGroupByOrMeasureAggsInBlock_NoGroupByNoMeasure(t *testing.T) {
	qa := &QueryAggregators{}

	assert.False(t, qa.HasGroupByOrMeasureAggsInBlock(), "Expected false when there are no group by or measure aggregators, got true")
}

func Test_HasGroupByOrMeasureAggsInBlock_WithGroupBy(t *testing.T) {
	qa := &QueryAggregators{
		GroupByRequest: &GroupByRequest{
			GroupByColumns: []string{"column1"},
		},
	}

	assert.True(t, qa.HasGroupByOrMeasureAggsInBlock(), "Expected true when there is a group by aggregator, got false")
}

func Test_HasGroupByOrMeasureAggsInBlock_WithMeasure(t *testing.T) {
	qa := &QueryAggregators{
		MeasureOperations: []*MeasureAggregator{
			{MeasureCol: "measure1"},
		},
	}

	assert.True(t, qa.HasGroupByOrMeasureAggsInBlock(), "Expected true when there are measure aggregators, got false")
}

func Test_HasGroupByOrMeasureAggsInBlock_WithBothGroupByAndMeasure(t *testing.T) {
	qa := &QueryAggregators{
		GroupByRequest: &GroupByRequest{
			GroupByColumns: []string{"column1"},
		},
		MeasureOperations: []*MeasureAggregator{
			{MeasureCol: "measure1"},
		},
	}

	assert.True(t, qa.HasGroupByOrMeasureAggsInBlock(), "Expected true when there are both group by and measure aggregators, got false")
}

func Test_HasDedupBlockInChain_HasDedupBlock(t *testing.T) {
	qa := &QueryAggregators{
		OutputTransforms: &OutputTransforms{
			LetColumns: &LetColumnsRequest{
				DedupColRequest: &DedupExpr{},
			},
		},
	}

	assert.True(t, qa.HasDedupBlockInChain())
}

func Test_HasDedupBlockInChain_NotNilNext(t *testing.T) {
	next := &QueryAggregators{
		OutputTransforms: &OutputTransforms{
			LetColumns: &LetColumnsRequest{
				DedupColRequest: &DedupExpr{},
			},
		},
	}

	qa := &QueryAggregators{
		Next: next,
	}

	assert.True(t, qa.HasDedupBlockInChain())
}

func Test_HasDedupBlockInChain_HasNoDedupBlockAndNilNext(t *testing.T) {
	var next *QueryAggregators

	qa := &QueryAggregators{
		Next: next,
	}

	assert.False(t, qa.HasDedupBlockInChain())
}

func Test_GetBucketValueForGivenField_StatRes(t *testing.T) {
	br := &BucketResult{
		StatRes: map[string]utils.CValueEnclosure{
			"field1": {Dtype: utils.SS_DT_STRING, CVal: "value1"},
		},
		BucketKey:   []string{"key1", "key2"},
		GroupByKeys: []string{"groupKey1", "groupKey2"},
	}

	value, index, foundInStat := br.GetBucketValueForGivenField("field1")
	assert.True(t, foundInStat)
	assert.Equal(t, "value1", value.(utils.CValueEnclosure).CVal)
	assert.Equal(t, -1, index)
}

func Test_GetBucketValueForGivenField_GroupByKey(t *testing.T) {
	br := &BucketResult{
		StatRes: map[string]utils.CValueEnclosure{
			"field1": {Dtype: utils.SS_DT_STRING, CVal: "value1"},
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
		StatRes: map[string]utils.CValueEnclosure{
			"field1": {Dtype: utils.SS_DT_STRING, CVal: "value1"},
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
		StatRes: map[string]utils.CValueEnclosure{
			"field1": {Dtype: utils.SS_DT_STRING, CVal: "value1"},
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
		StatRes: map[string]utils.CValueEnclosure{
			"field1": {Dtype: utils.SS_DT_STRING, CVal: "value1"},
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
		StatRes: map[string]utils.CValueEnclosure{
			"field1": {Dtype: utils.SS_DT_STRING, CVal: "value1"},
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
		StatRes: map[string]utils.CValueEnclosure{
			"field1": {Dtype: utils.SS_DT_STRING, CVal: "value1"},
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
	assert.Equal(t, []string{"key1", "newKey2"}, br.BucketKey)
}

func Test_SetBucketValueForGivenField_ValidStringList(t *testing.T) {
	br := &BucketResult{
		BucketKey:   []string{"key1", "key2"},
		GroupByKeys: []string{"groupKey1", "groupKey2"},
	}

	err := br.SetBucketValueForGivenField("groupKey2", []string{"newKey2", "anotherKey2"}, 1, false)
	assert.Nil(t, err)
	assert.Equal(t, []string{"key1", `[ "newKey2", "anotherKey2" ]`}, br.BucketKey)
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
		StatRes: map[string]utils.CValueEnclosure{
			"field1": {Dtype: utils.SS_DT_STRING, CVal: "value1"},
		},
		BucketKey:   []string{"key1", "key2"},
		GroupByKeys: []string{"groupKey1", "groupKey2"},
	}

	err := br.SetBucketValueForGivenField("field1", "value", 1, true)
	assert.Nil(t, err)
	assert.Equal(t, []string{"key1", "key2"}, br.BucketKey)
	assert.Equal(t, "value1", br.StatRes["field1"].CVal)
}

func Test_SetBucketValueForGivenField_ConvertToSlice(t *testing.T) {
	br := &BucketResult{
		BucketKey:   []interface{}{"key1", "key2"},
		GroupByKeys: []string{"groupKey1", "groupKey2"},
	}

	err := br.SetBucketValueForGivenField("groupKey2", "newKey2", 1, false)
	assert.Nil(t, err)
	assert.Equal(t, []string{"key1", "newKey2"}, br.BucketKey)
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
