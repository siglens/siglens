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
		BucketCount: 3,
	}
	totalTraces := GetTotalUniqueTraceIds(pipeSearchResponseOuter)
	assert.Equal(t, 3, totalTraces)

	//Empty PipeSearchResponseOuter
	pipeSearchResponseOuter = &structs.PipeSearchResponseOuter{}
	totalTraces = GetTotalUniqueTraceIds(pipeSearchResponseOuter)
	assert.Equal(t, 0, totalTraces)
}

func TestConvertTimeToUint64(t *testing.T) {
	tests := []struct {
		name      string
		input     interface{}
		want      uint64
		shouldErr bool
	}{
		{"Float64 input", float64(1616161616.0), 1616161616, false},
		{"Int input", int(1616161616), 1616161616, false},
		{"Int64 input", int64(1616161616), 1616161616, false},
		{"Uint64 input", uint64(1616161616), 1616161616, false},
		{"Valid string input", "1616161616", 1616161616, false},
		{"Invalid string input", "notanumber", 0, true},
		{"Unsupported type input", struct{}{}, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := convertTimeToUint64(tt.input)
			if tt.shouldErr {
				assert.Error(t, err)
				assert.Equal(t, uint64(0), got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}

}
