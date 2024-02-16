package handler

import (
	"encoding/json"
	"testing"

	pipesearch "github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
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

func TestParseAndValidateRequestBody(t *testing.T) {
	// Test case 1: Valid request body
	ctx := &fasthttp.RequestCtx{
		Request: fasthttp.Request{},
	}
	ctx.Request.SetBody([]byte(`{
        "searchText": "*",
        "startEpoch": "now-90d",
        "endEpoch": "now",
        "queryLanguage": "Splunk QL",
        "page": 1
    }`))
	body, readJSON, err := ParseAndValidateRequestBody(ctx)
	assert.Nil(t, err)
	assert.Equal(t, "*", body.SearchText)
	assert.Equal(t, "now-90d", body.StartEpoch)
	assert.Equal(t, "now", body.EndEpoch)
	assert.Equal(t, "Splunk QL", body.QueryLanguage)
	assert.Equal(t, "traces", body.IndexName)
	page, err := readJSON["page"].(json.Number).Int64()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), page)
	assert.Equal(t, "*", readJSON["searchText"])
	assert.Equal(t, "now-90d", readJSON["startEpoch"])
	assert.Equal(t, "now", readJSON["endEpoch"])
	assert.Equal(t, "Splunk QL", readJSON["queryLanguage"])
	assert.Equal(t, json.Number("1"), readJSON["page"])

	// Test case 2: Invalid JSON
	ctx.Request.SetBody([]byte(`{
        searchText: *,
        "startEpoch": "now-90d",
        "endEpoch": "now",
        "queryLanguage": "Splunk QL"
        "page": 2
    }`))
	_, _, err = ParseAndValidateRequestBody(ctx)
	assert.NotNil(t, err)

	ctx.Request.SetBody([]byte(``))
	body, readJSON, err = ParseAndValidateRequestBody(ctx)
	assert.NotNil(t, err)
	assert.Nil(t, body)
	assert.Nil(t, readJSON)
}

func TestGetTotalUniqueTraceIds(t *testing.T) {
	//Non-empty PipeSearchResponseOuter
	pipeSearchResponseOuter := &pipesearch.PipeSearchResponseOuter{
		Aggs: map[string]pipesearch.AggregationResults{
			"": {
				Buckets: []map[string]interface{}{
					{"key": "trace1"},
					{"key": "trace2"},
					{"key": "trace3"},
				},
			},
		},
	}
	totalTraces := GetTotalUniqueTraceIds(pipeSearchResponseOuter)
	assert.Equal(t, 3, totalTraces)

	//Empty PipeSearchResponseOuter
	pipeSearchResponseOuter = &pipesearch.PipeSearchResponseOuter{}
	totalTraces = GetTotalUniqueTraceIds(pipeSearchResponseOuter)
	assert.Equal(t, 0, totalTraces)
}

func TestGetUniqueTraceIds(t *testing.T) {
	// Non-empty PipeSearchResponseOuter
	pipeSearchResponseOuter := &pipesearch.PipeSearchResponseOuter{
		Aggs: map[string]pipesearch.AggregationResults{
			"": {
				Buckets: []map[string]interface{}{
					{"key": "trace1"},
					{"key": "trace2"},
					{"key": "trace3"},
				},
			},
		},
	}
	traceIds := GetUniqueTraceIds(pipeSearchResponseOuter, 0, 0, 1)
	assert.Equal(t, []string{"trace1", "trace2", "trace3"}, traceIds)

	// Empty PipeSearchResponseOuter
	pipeSearchResponseOuter = &pipesearch.PipeSearchResponseOuter{}
	traceIds = GetUniqueTraceIds(pipeSearchResponseOuter, 0, 0, 1)
	assert.Equal(t, []string{}, traceIds)
}
