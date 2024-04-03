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

	assert.Equal(t, false, qa.HasQueryAggergatorBlock(), "Expected false when QueryAggregators is nil, got true")
}
func Test_HasQueryAggergatorBlock_NilOutputTransforms(t *testing.T) {
	var ot *OutputTransforms

	qa := &QueryAggregators{
		OutputTransforms: ot,
	}

	assert.Equal(t, false, qa.HasQueryAggergatorBlock(), "Expected false when OutputTransforms is nil, got true")
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

	assert.Equal(t, true, qa.HasQueryAggergatorBlock(), "Expected true when hasLetColumnsRequest is true, got false")
}

func Test_HasQueryAggergatorBlock_HasNoLetColumnsRequest(t *testing.T) {
	lcr := &LetColumnsRequest{}

	ot := &OutputTransforms{
		LetColumns: lcr,
	}

	qa := &QueryAggregators{
		OutputTransforms: ot,
	}

	assert.Equal(t, false, qa.HasQueryAggergatorBlock(), "Expected false when hasLetColumnsRequest is false, got true")
}

func Test_HasQueryAggergatorBlock_MaxRowsGreaterThanRowAdded(t *testing.T) {
	ot := &OutputTransforms{
		MaxRows:   12,
		RowsAdded: 1,
	}

	qa := &QueryAggregators{
		OutputTransforms: ot,
	}

	assert.Equal(t, true, qa.HasQueryAggergatorBlock(), "Expected true when MaxRows is greater than RowsAdded, got false")
}

func Test_HasQueryAggergatorBlock_MaxRowsLessThanRowAdded(t *testing.T) {
	ot := &OutputTransforms{
		MaxRows:   1,
		RowsAdded: 9,
	}

	qa := &QueryAggregators{
		OutputTransforms: ot,
	}

	assert.Equal(t, false, qa.HasQueryAggergatorBlock(), "Expected false when MaxRows is less than RowsAdded, got true")
}
