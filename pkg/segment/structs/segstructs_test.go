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

func Test_HasQueryAggergatorBlock(t *testing.T) {
	lcr := &LetColumnsRequest{
		RexColRequest:    &RexExpr{},
		RenameColRequest: &RenameExpr{},
		DedupColRequest:  &DedupExpr{},
		ValueColRequest:  &ValueExpr{},
		SortColRequest:   &SortExpr{},
	}

	ot := &OutputTransforms{
		HarcodedCol: []string{"test1", "test2"},
		MaxRows:     2,
		RowsAdded:   1,
		LetColumns:  lcr,
	}

	qa := &QueryAggregators{
		OutputTransforms: ot,
	}

	assert.NotNil(t, qa)
	assert.NotNil(t, qa.OutputTransforms)
	assert.NotNil(t, qa.OutputTransforms.LetColumns)
	assert.NotNil(t, qa.OutputTransforms.LetColumns.RexColRequest)
	assert.NotNil(t, qa.OutputTransforms.LetColumns.RenameColRequest)
	assert.NotNil(t, qa.OutputTransforms.LetColumns.DedupColRequest)
	assert.NotNil(t, qa.OutputTransforms.LetColumns.ValueColRequest)
	assert.NotNil(t, qa.OutputTransforms.LetColumns.SortColRequest)
	assert.Equal(t, true, qa.OutputTransforms.MaxRows > qa.OutputTransforms.RowsAdded, "Expected true when MaxRows greater than RowsAdded, got false")
	assert.Equal(t, true, qa.HasQueryAggergatorBlock(), "Expected true when QueryAggergatorBlock is not nil, got false")
}
