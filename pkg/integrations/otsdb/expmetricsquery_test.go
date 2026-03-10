// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package otsdbquery

import (
	"os"
	"testing"

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

func getMyIds() []int64 {
	myids := make([]int64, 1)
	myids[0] = 0
	return myids
}

func Test_ExpMetricsQueryIncorrectID(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())
	limit.InitMemoryLimiter()
	writer.InitWriterNode()
	err := query.InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	assert.Nil(t, err)
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
