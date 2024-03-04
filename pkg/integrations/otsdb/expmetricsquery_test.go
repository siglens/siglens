/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package otsdbquery

import (
	"os"
	"testing"

	localstorage "github.com/siglens/siglens/pkg/blob/local"
	"github.com/siglens/siglens/pkg/config"
	otsdbquery "github.com/siglens/siglens/pkg/integrations/otsdb/query"
	"github.com/siglens/siglens/pkg/segment/memory/limit"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	serverutils "github.com/siglens/siglens/pkg/server/utils"
	"github.com/stretchr/testify/assert"
)

// func Test_ExpMetricsQuery(t *testing.T) {
//	config.InitializeTestingConfig()
//	limit.InitMemoryLimiter()
//	writer.InitWriterNode()
//	err := query.InitQueryNode()
//	assert.Nil(t, err)
//	localstorage.InitLocalStorage()
//	metrics.InitTestingConfig()
//	err = metadata.InitMockMetricsMetadataStore(10000)
//	assert.Nil(t, err)
//	request := &structs.OTSDBMetricsQueryExpRequest{
//		Time: structs.OTSDBMetricsQueryExpTime{
//			Start:      "1h-ago",
//			End:        "",
//			Timezone:   "UTC",
//			Aggregator: "sum",
//			Downsampler: structs.OTSDBMetricsQueryExpDownsampler{
//				Interval:   "1m",
//				Aggregator: "sum",
//			},
//		},
//		Filters: []structs.OTSDBMetricsQueryExpFilter{
//			{
//				Tags: []structs.OTSDBMetricsQueryExpTags{
//					{
//						Type:    "literal_or",
//						Tagk:    "color",
//						Filter:  "*",
//						GroupBy: false,
//					},
//				},
//				Id: "f1",
//			},
//		},
//		Metrics: []structs.OTSDBMetricsQueryExpMetric{
//			{
//				Id:         "m1",
//				MetricName: "test.metric.1",
//				Filter:     "f1",
//			},
//		},
//		Expressions: []structs.OTSDBMetricsQueryExpressions{},
//		Outputs: []structs.OTSDBMetricsQueryExpOutput{
//			{
//				Id:    "m1",
//				Alias: "output-m1",
//			},
//		},
//	}
//	metricQueryRequest, err := otsdbquery.MetricsQueryExpressionsParseRequest(request)
//	assert.NoError(t, err)
//	assert.NotNil(t, metricQueryRequest)
//	assert.Len(t, metricQueryRequest, 1)
//	var expMetricsQueryResult map[string][]*structs.MetricsQueryResponse = make(map[string][]*structs.MetricsQueryResponse)
//	for alias, req := range metricQueryRequest {
//		qid := rutils.GetNextQid()
//		mQResponse := segment.ExecuteMetricsQuery(&req.MetricsQuery, &req.TimeRange, qid)
//		assert.NotNil(t, mQResponse)
//		assert.GreaterOrEqual(t, len(mQResponse), 1)
//		assert.Equal(t, "output-m1", alias)
//		expMetricsQueryResult[alias] = mQResponse
//	}
//	assert.Len(t, expMetricsQueryResult, 1)
//	os.RemoveAll(config.GetDataPath())
// }

// func Test_ExpMetricsQueryMultipleMetricsAndFilters(t *testing.T) {
//	config.InitializeTestingConfig()
//	limit.InitMemoryLimiter()
//	writer.InitWriterNode()
//	err := query.InitQueryNode()
//	assert.Nil(t, err)
//	localstorage.InitLocalStorage()
//	metrics.InitTestingConfig()
//	err = metadata.InitMockMetricsMetadataStore(100000)
//	assert.Nil(t, err)
//	request := &structs.OTSDBMetricsQueryExpRequest{
//		Time: structs.OTSDBMetricsQueryExpTime{
//			Start:      "1d-ago",
//			End:        "",
//			Timezone:   "UTC",
//			Aggregator: "sum",
//			Downsampler: structs.OTSDBMetricsQueryExpDownsampler{
//				Interval:   "1m",
//				Aggregator: "sum",
//			},
//		},
//		Filters: []structs.OTSDBMetricsQueryExpFilter{
//			{
//				Tags: []structs.OTSDBMetricsQueryExpTags{
//					{
//						Type:    "literal_or",
//						Tagk:    "color",
//						Filter:  "*",
//						GroupBy: false,
//					},
//				},
//				Id: "f1",
//			},
//			{
//				Tags: []structs.OTSDBMetricsQueryExpTags{
//					{
//						Type:    "literal_or",
//						Tagk:    "model",
//						Filter:  "*",
//						GroupBy: false,
//					},
//				},
//				Id: "f2",
//			},
//		},
//		Metrics: []structs.OTSDBMetricsQueryExpMetric{
//			{
//				Id:         "m1",
//				MetricName: "test.metric.0",
//				Filter:     "f1",
//			},
//			{
//				Id:         "m2",
//				MetricName: "test.metric.1",
//				Filter:     "f2",
//			},
//		},
//		Expressions: []structs.OTSDBMetricsQueryExpressions{},
//		Outputs: []structs.OTSDBMetricsQueryExpOutput{
//			{
//				Id:    "m1",
//				Alias: "output-m1",
//			},
//			{
//				Id:    "m2",
//				Alias: "output-m2",
//			},
//		},
//	}
//	expectedAlias := []string{"output-m1", "output-m2"}
//	metricQueryRequest, err := otsdbquery.MetricsQueryExpressionsParseRequest(request)
//	assert.NoError(t, err)
//	assert.NotNil(t, metricQueryRequest)
//	assert.Len(t, metricQueryRequest, 2)
//	var expMetricsQueryResult map[string][]*structs.MetricsQueryResponse = make(map[string][]*structs.MetricsQueryResponse)
//	for alias, req := range metricQueryRequest {
//		qid := rutils.GetNextQid()
//		mQResponse := segment.ExecuteMetricsQuery(&req.MetricsQuery, &req.TimeRange, qid)
//		assert.NotNil(t, mQResponse)
//		assert.Contains(t, expectedAlias, alias)
//		expMetricsQueryResult[alias] = mQResponse
//	}
//	assert.Len(t, expMetricsQueryResult, 2)
//	os.RemoveAll(config.GetDataPath())
// }

func getMyIds() []uint64 {
	myids := make([]uint64, 1)
	myids[0] = 0
	return myids
}

func Test_ExpMetricsQueryIncorrectID(t *testing.T) {
	config.InitializeTestingConfig()
	limit.InitMemoryLimiter()
	writer.InitWriterNode()
	err := query.InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	assert.Nil(t, err)
	_ = localstorage.InitLocalStorage()
	metrics.InitTestingConfig()
	request := &structs.OTSDBMetricsQueryExpRequest{
		Time: structs.OTSDBMetricsQueryExpTime{
			Start:      "1d-ago",
			End:        "",
			Timezone:   "UTC",
			Aggregator: "sum",
			Downsampler: structs.OTSDBMetricsQueryExpDownsampler{
				Interval:   "1m",
				Aggregator: "sum",
			},
		},
		Filters: []structs.OTSDBMetricsQueryExpFilter{
			{
				Tags: []structs.OTSDBMetricsQueryExpTags{
					{
						Type:    "literal_or",
						Tagk:    "color",
						Filter:  "*",
						GroupBy: false,
					},
				},
				Id: "f1",
			},
		},
		Metrics: []structs.OTSDBMetricsQueryExpMetric{
			{
				Id:         "m1",
				MetricName: "test.metric.0",
				Filter:     "f1",
			},
		},
		Expressions: []structs.OTSDBMetricsQueryExpressions{},
		Outputs: []structs.OTSDBMetricsQueryExpOutput{
			{
				Id:    "m2",
				Alias: "output-m1",
			},
		},
	}

	metricQueryRequest, err := otsdbquery.MetricsQueryExpressionsParseRequest(request)
	assert.NoError(t, err)
	assert.NotNil(t, metricQueryRequest)
	assert.Len(t, metricQueryRequest, 0)
	os.RemoveAll(config.GetDataPath())
}
