package handler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExtractTraceID(t *testing.T) {
	// Test case 1: Valid trace ID
	isTraceID, traceID := ExtractTraceID("trace_id=abc123")
	assert.True(t, isTraceID)
	assert.Equal(t, "abc123", traceID)

	// Test case 2: Invalid trace ID
	isTraceID, traceID = ExtractTraceID("trace_id=invalid*id")
	assert.False(t, isTraceID)
	assert.Equal(t, "", traceID)

	// Test case 3: Empty input
	isTraceID, traceID = ExtractTraceID("")
	assert.False(t, isTraceID)
	assert.Equal(t, "", traceID)

	// Test case 4: No trace ID in input
	isTraceID, traceID = ExtractTraceID("no_trace_id_here")
	assert.False(t, isTraceID)
	assert.Equal(t, "", traceID)
}
