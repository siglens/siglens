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

package query

import (
	"encoding/json"
	"os"
	"sort"
	"testing"
	"time"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/query/metricsevaluator"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/stretchr/testify/assert"
)

func Test_GetAllMetricNamesOverTheTimeRange(t *testing.T) {
	// Create Mock Segment Files.

	config.InitializeTestingConfig(t.TempDir())

	err := metadata.InitMockMetricsMetadataStore(10000)
	assert.Nil(t, err)

	timeRange := &dtu.MetricsTimeRange{
		StartEpochSec: uint32(time.Now().Unix() - int64(24*60*60)),
		EndEpochSec:   uint32(time.Now().Unix()),
	}

	mNames, err := GetAllMetricNamesOverTheTimeRange(timeRange, 0)
	assert.Nil(t, err)

	ingestedMetricNames := []string{"test.metric.0", "test.metric.1", "test.metric.2", "test.metric.3"}

	assert.True(t, len(mNames) == 4)
	sort.Slice(mNames, func(i, j int) bool {
		return mNames[i] < mNames[j]
	})

	assert.Equal(t, mNames, ingestedMetricNames)

	// Cleanup
	_ = os.RemoveAll(config.GetDataPath())
}

func Test_ExecuteInstantQuery(t *testing.T) {
	t.Run("Simple", func(t *testing.T) {
		mockReader := &metricsevaluator.MockReader{
			Data: map[metricsevaluator.SeriesId][]metricsevaluator.Sample{
				"metric": {
					{Ts: 1700000000, Value: 1.0},
					{Ts: 1700000001, Value: 2.0},
					{Ts: 1700000005, Value: 3.0},
				},
			},
		}

		assertInstantQueryYieldsJson(t, mockReader, 1699999999, `metric`,
			`{"status":"success","data":{"resultType":"vector","result":[]}}`,
		)
		assertInstantQueryYieldsJson(t, mockReader, 1700000000, `metric`,
			`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"metric"},"value":[1700000000,"1"]}]}}`,
		)
		assertInstantQueryYieldsJson(t, mockReader, 1700000003, `metric`,
			`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"metric"},"value":[1700000003,"2"]}]}}`,
		)
		assertInstantQueryYieldsJson(t, mockReader, 1700000304, `metric`,
			`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"metric"},"value":[1700000304,"3"]}]}}`,
		)
		assertInstantQueryYieldsJson(t, mockReader, 1700000305, `metric`,
			`{"status":"success","data":{"resultType":"vector","result":[]}}`,
		)
	})

	t.Run("Multiple Series", func(t *testing.T) {
		mockReader := &metricsevaluator.MockReader{
			Data: map[metricsevaluator.SeriesId][]metricsevaluator.Sample{
				`metric{color="red"}`: {
					{Ts: 1700000000, Value: 1.0},
					{Ts: 1700000003, Value: 2.0},
				},
				`metric{color="blue"}`: {
					{Ts: 1700000001, Value: 3.0},
				},
				`metric{color="blue",region="us-east"}`: {
					{Ts: 1700000004, Value: 4.0},
				},
			},
		}
		assertInstantQueryYieldsJson(t, mockReader, 1700000001, `metric`,
			`{"status":"success","data":{"resultType":"vector","result":[{"metric":{"__name__":"metric","color":"red"},"value":[1700000001,"1"]},{"metric":{"__name__":"metric","color":"blue"},"value":[1700000001,"3"]}]}}`,
		)
	})
}

func assertInstantQueryYieldsJson(t *testing.T, mockReader *metricsevaluator.MockReader,
	evalTime uint32, query string, expectedJson string) {
	t.Helper()

	qid := uint64(0)
	myId := int64(0)
	qs := summary.InitQuerySummary(summary.METRICS, qid)

	result, err := ExecuteInstantQuery(qid, myId, mockReader, query, evalTime, qs)
	assert.Nil(t, err)
	assert.NotNil(t, result)

	jsonBytes, err := json.Marshal(result)
	assert.Nil(t, err)
	assert.Equal(t, expectedJson, string(jsonBytes))
}

func Test_ExecuteRangeQuery(t *testing.T) {
	mockReader := &metricsevaluator.MockReader{
		Data: map[metricsevaluator.SeriesId][]metricsevaluator.Sample{
			"metric": {
				{Ts: 1700000000, Value: 1.0},
				{Ts: 1700000001, Value: 2.0},
				{Ts: 1700000005, Value: 3.0},
			},
		},
	}

	assertRangeQueryYieldsJson(t, mockReader, 1699999998, 1700000006, 1, `metric`,
		`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"metric"},"values":[[1700000000,"1"],[1700000001,"2"],[1700000002,"2"],[1700000003,"2"],[1700000004,"2"],[1700000005,"3"],[1700000006,"3"]]}]}}`,
	)
	assertRangeQueryYieldsJson(t, mockReader, 1700000298, 1700000306, 1, `metric`,
		`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"metric"},"values":[[1700000298,"3"],[1700000299,"3"],[1700000300,"3"],[1700000301,"3"],[1700000302,"3"],[1700000303,"3"],[1700000304,"3"]]}]}}`,
	)
	assertRangeQueryYieldsJson(t, mockReader, 1700000005, 1700000010, 2, `metric`,
		`{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"metric"},"values":[[1700000005,"3"],[1700000007,"3"],[1700000009,"3"]]}]}}`,
	)
}

func assertRangeQueryYieldsJson(t *testing.T, mockReader *metricsevaluator.MockReader,
	startTime, endTime, step uint32, query string, expectedJson string) {
	t.Helper()

	qid := uint64(0)
	myId := int64(0)
	qs := summary.InitQuerySummary(summary.METRICS, qid)

	result, err := ExecuteRangeQuery(qid, myId, mockReader, query, startTime, endTime, step, qs)
	assert.Nil(t, err)
	assert.NotNil(t, result)

	jsonBytes, err := json.Marshal(result)
	assert.Nil(t, err)
	assert.Equal(t, expectedJson, string(jsonBytes))
}
