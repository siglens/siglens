package structs

import (
	"testing"
)

/*
	The unit tests will cover different scenarios:
		- when the QueryAggregators instance is nil,
		- when it is not nil but the TransactionArguments field is nil,
		- when both the instance and its TransactionArguments field are not nil.
*/

// Test_HasTransactionArguments_NilReceiver tests the case where the QueryAggregators receiver is nil.
func Test_HasTransactionArguments_NilReceiver(t *testing.T) {
	var qa *QueryAggregators // qa is implicitly nil
	if qa.HasTransactionArguments() {
		t.Errorf("Expected false for nil receiver, got true")
	}
}

// Test_HasTransactionArguments_NilTransactionArguments tests the case where TransactionArguments is nil.
func Test_HasTransactionArguments_NilTransactionArguments(t *testing.T) {
	qa := &QueryAggregators{} // TransactionArguments is nil by default
	if qa.HasTransactionArguments() {
		t.Errorf("Expected false when TransactionArguments is nil, got true")
	}
}

// Test_HasTransactionArguments_NonNilTransactionArguments tests the case where TransactionArguments is not nil.
func Test_HasTransactionArguments_NonNilTransactionArguments(t *testing.T) {
	// Now initializing TransactionArguments directly according to its actual structure
	transactionArgs := TransactionArguments{
		Fields:     []string{"test1", "test2"}, // Example initialization
		StartsWith: nil,
		EndsWith:   nil,
	}
	qa := &QueryAggregators{
		TransactionArguments: &transactionArgs, // Correctly using a pointer to TransactionArguments
	}
	if !qa.HasTransactionArguments() {
		t.Errorf("Expected true when TransactionArguments is not nil, got false")
	}
}
