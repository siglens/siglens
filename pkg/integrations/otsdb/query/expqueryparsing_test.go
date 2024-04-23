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
