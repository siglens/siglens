package handler

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	"testing"
)

func TestProcessGetDependencies(t *testing.T) {
	ctx := &fasthttp.RequestCtx{}
	ctx.QueryArgs().Set("endTs", "now")
	ctx.QueryArgs().Set("lookback", "24h")

	ProcessGetDependencies(ctx, 123)

	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())

	var response DependenciesResponse
	err := json.Unmarshal(ctx.Response.Body(), &response)
	assert.NoError(t, err)
	assert.NotNil(t, response.Data)
}

func TestProcessGetTracesSearch(t *testing.T) {
	ctx := &fasthttp.RequestCtx{}
	ctx.QueryArgs().Set("start", "now-24h")
	ctx.QueryArgs().Set("end", "now")
	ctx.QueryArgs().Set("lookback", "24h")
	ctx.QueryArgs().Set("service", "test-service")

	ProcessGetTracesSearch(ctx, 123)

	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())

	var response TracesResponse
	err := json.Unmarshal(ctx.Response.Body(), &response)
	assert.NoError(t, err)
	assert.NotNil(t, response.Data)
}

func TestComputeStartTime(t *testing.T) {
	tests := []struct {
		name          string
		startTs       string
		endTs         string
		lookBack      string
		expectedStart int64
		expectedEnd   int64
		expectErr     bool
	}{
		{
			name:          "Valid startTs provided",
			startTs:       "1610000000",
			endTs:         "1610003600",
			lookBack:      "600",
			expectedStart: 1610000000,
			expectedEnd:   1610003600,
			expectErr:     false,
		},
		{
			name:          "Valid lookBack used to compute start",
			startTs:       "",
			endTs:         "1610003600",
			lookBack:      "600",
			expectedStart: 1610003000,
			expectedEnd:   1610003600,
			expectErr:     false,
		},
		{
			name:          "Missing endTs",
			startTs:       "1610000000",
			endTs:         "",
			lookBack:      "600",
			expectedStart: 0,
			expectedEnd:   0,
			expectErr:     true,
		},
		{
			name:          "Missing lookBack when startTs is empty",
			startTs:       "",
			endTs:         "1610003600",
			lookBack:      "",
			expectedStart: 0,
			expectedEnd:   0,
			expectErr:     true,
		},
		{
			name:          "Invalid endTs format",
			startTs:       "1610000000",
			endTs:         "invalid",
			lookBack:      "600",
			expectedStart: 0,
			expectedEnd:   0,
			expectErr:     true,
		},
		{
			name:          "Invalid lookBack format",
			startTs:       "",
			endTs:         "1610003600",
			lookBack:      "invalid",
			expectedStart: 0,
			expectedEnd:   0,
			expectErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, end, err := computeStartTime(tt.startTs, tt.endTs, tt.lookBack)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedStart, start)
				assert.Equal(t, tt.expectedEnd, end)
			}
		})
	}
}
