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

package handler

import (
	"encoding/json"
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
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
	pipeSearchResponseOuter := &structs.PipeSearchResponseOuter{
		Aggs: map[string]structs.AggregationResults{
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
	pipeSearchResponseOuter = &structs.PipeSearchResponseOuter{}
	totalTraces = GetTotalUniqueTraceIds(pipeSearchResponseOuter)
	assert.Equal(t, 0, totalTraces)
}

func TestGetUniqueTraceIds(t *testing.T) {
	// Non-empty PipeSearchResponseOuter
	pipeSearchResponseOuter := &structs.PipeSearchResponseOuter{
		Aggs: map[string]structs.AggregationResults{
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
	pipeSearchResponseOuter = &structs.PipeSearchResponseOuter{}
	traceIds = GetUniqueTraceIds(pipeSearchResponseOuter, 0, 0, 1)
	assert.Equal(t, []string{}, traceIds)
}

func Test_parseRequest(t *testing.T) {
	// Case 1: Valid JSON input
	validJSON := `{
		"start": 1625248200,
		"end": 1625248300,
		"serviceName": "some_service",
		"query": {
			"JoinOperator": "AND",
			"RatePerSec": 100.0,
			"ErrorPercentage": 5.0,
			"DurationP50Ms": 200.0,
			"DurationP90Ms": 500.0,
			"DurationP99Ms": 1000.0
		}
	}`

	start, end, serviceName, _, redMetricsMap, err := ParseRedMetricsRequest([]byte(validJSON))
	assert.NoError(t, err)
	assert.Equal(t, uint32(1625248200), start)
	assert.Equal(t, uint32(1625248300), end)
	assert.Equal(t, "some_service", serviceName)
	assert.Equal(t, "AND", redMetricsMap["join_operator"])
	assert.Equal(t, 100.0, redMetricsMap["RatePerSec"])
	assert.Equal(t, 5.0, redMetricsMap["ErrorPercentage"])
	assert.Equal(t, 200.0, redMetricsMap["DurationP50Ms"])
	assert.Equal(t, 500.0, redMetricsMap["DurationP90Ms"])
	assert.Equal(t, 1000.0, redMetricsMap["DurationP99Ms"])

	// Case 2: Valid JSON input with default JoinOperator (OR)
	validJSONDefaultJoinOperator := `{
		"start": 1625248200,
		"end": 1625248300,
		"serviceName": "some_service",
		"query": {
			"RatePerSec": 100.0	}
	}`

	start, end, serviceName, _, redMetricsMap, err = ParseRedMetricsRequest([]byte(validJSONDefaultJoinOperator))
	assert.NoError(t, err)
	assert.Equal(t, uint32(1625248200), start)
	assert.Equal(t, uint32(1625248300), end)
	assert.Equal(t, "some_service", serviceName)
	assert.Equal(t, "OR", redMetricsMap["join_operator"]) // Default should be OR
	assert.Equal(t, 100.0, redMetricsMap["RatePerSec"])

	// Case 3: Invalid JSON input (missing 'startTime')
	invalidJSONMissingStartTime := `{
		"end": 1625248300,
		"serviceName": "some_service",
		"query": {
			"RatePerSec": 100.0
		}
	}`

	start, end, serviceName, _, redMetricsMap, err = ParseRedMetricsRequest([]byte(invalidJSONMissingStartTime))
	assert.Error(t, err)
	assert.NotNil(t, start)
	assert.NotNil(t, end)
	assert.NotNil(t, serviceName)
	assert.NotNil(t, redMetricsMap)

	// Case 4: Invalid JSON input (missing 'endTime')
	invalidJSONMissingEndTime := `{
		"start":1625248200,
		"serviceName": "some_service",
		"query": {
			"RatePerSec": 100.0
		}
	}`

	start, end, serviceName, _, redMetricsMap, err = ParseRedMetricsRequest([]byte(invalidJSONMissingEndTime))
	assert.Error(t, err)
	assert.NotNil(t, start)
	assert.NotNil(t, end)
	assert.NotNil(t, serviceName)
	assert.NotNil(t, redMetricsMap)

	// Case 5: Invalid JSON input (missing 'serviceName')
	invalidJSONMissingServiceName := `{
		"start": 1625248200,
		"end":1625248300 ,
		"query": {
			"RatePerSec": 100.0
		}
	}`

	start, end, serviceName, _, redMetricsMap, err = ParseRedMetricsRequest([]byte(invalidJSONMissingServiceName))
	assert.Error(t, err)
	assert.NotNil(t, start)
	assert.NotNil(t, end)
	assert.NotNil(t, serviceName)
	assert.NotNil(t, redMetricsMap)

	// Case 7: Invalid JSON input (malformed query parameters)
	invalidJSONMalformedQueryParams := `{
		"startTime": 1625248200,
		"endTime": 1625248300,
		"serviceName": "some_service",
		"query": {
			"JoinOperator": "INVALID_OPERATOR",
			"RatePerSec": "not_a_number"
		}
	}`

	start, end, serviceName, _, redMetricsMap, err = ParseRedMetricsRequest([]byte(invalidJSONMalformedQueryParams))
	assert.Error(t, err)
	assert.NotNil(t, start)
	assert.NotNil(t, end)
	assert.NotNil(t, serviceName)
	assert.NotNil(t, redMetricsMap)

}
