// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package structs

import (
	"encoding/json"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/assert"
)

func Test_PromQLInstantResponseMarshal(t *testing.T) {
	promQlResp := &MetricsPromQLInstantQueryResponse{
		Status: "success",
		Data: &PromQLInstantData{
			ResultType: parser.ValueTypeVector,
			VectorResult: []InstantVectorResult{
				{
					Metric: map[string]string{
						"__name__": "up",
						"job":      "prometheus",
					},
					Value: []interface{}{
						1, "123",
					},
				},
				{
					Metric: map[string]string{
						"__name__": "up",
						"job":      "node",
					},
					Value: []interface{}{
						1, "345",
					},
				},
			},
		},
	}

	expectedMarshal := `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"up","job":"prometheus"},"value":[1,"123"]},{"metric":{"__name__":"up","job":"node"},"value":[1,"345"]}]}}`

	marshaled, err := json.Marshal(promQlResp)
	assert.Nil(t, err)
	assert.Equal(t, expectedMarshal, string(marshaled))

	promQlResp.Data.ResultType = parser.ValueTypeScalar
	promQlResp.Data.SliceResult = []interface{}{1, "123"}
	expectedMarshal = `{"status":"success","data":{"resultType":"scalar","result":[1,"123"]}}`

	marshaled, err = json.Marshal(promQlResp)
	assert.Nil(t, err)
	assert.Equal(t, expectedMarshal, string(marshaled))
}
