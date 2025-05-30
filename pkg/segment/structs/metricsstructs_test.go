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
