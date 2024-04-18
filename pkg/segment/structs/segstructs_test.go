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
