// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package otsdbquery

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/stretchr/testify/assert"
)

func Test_ExpMetricsQueryParsing(t *testing.T) {
	mainQuery := &structs.OTSDBMetricsQueryExpRequest{
		Time: structs.OTSDBMetricsQueryExpTime{
			Start:      "1h-ago",
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
				MetricName: "test.metric.1",
				Filter:     "f1",
			},
		},
		Expressions: []structs.OTSDBMetricsQueryExpressions{},
		Outputs: []structs.OTSDBMetricsQueryExpOutput{
			{
				Id:    "m1",
				Alias: "output-m1",
			},
		},
	}
	var err error
	invalidTimeQuery := *mainQuery
	invalidTimeQuery.Time = structs.OTSDBMetricsQueryExpTime{
		Start:      "",
		End:        "2022-01-01",
		Timezone:   "UTC",
		Aggregator: "sum",
		Downsampler: structs.OTSDBMetricsQueryExpDownsampler{
			Interval:   "1m",
			Aggregator: "sum",
		},
	}
	_, err = MetricsQueryExpressionsParseRequest(&invalidTimeQuery)
	assert.Error(t, err)

	invalidAggQuery := *mainQuery
	invalidAggQuery.Time.Aggregator = "Multiply"
	_, err = MetricsQueryExpressionsParseRequest(&invalidAggQuery)
	assert.Error(t, err)

	invalidDownsamplerQuery := *mainQuery
	invalidDownsamplerQuery.Time.Downsampler = structs.OTSDBMetricsQueryExpDownsampler{
		Interval:   "",
		Aggregator: "sum",
	}
	_, err = MetricsQueryExpressionsParseRequest(&invalidDownsamplerQuery)
	assert.Error(t, err)
}
