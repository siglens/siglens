// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package otsdbquery

import (
	"context"
	"os"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	otsdbquery "github.com/siglens/siglens/pkg/integrations/otsdb/query"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/memory/limit"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	serverutils "github.com/siglens/siglens/pkg/server/utils"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func Test_MetricsQuery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go query.PullQueriesToRun(ctx)
	defer cancel()

	config.InitializeTestingConfig(t.TempDir())
	limit.InitMemoryLimiter()
	writer.InitWriterNode()
	err := query.InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	assert.Nil(t, err)
	metrics.InitTestingConfig()
	err = metadata.InitMockMetricsMetadataStore(10000)
	assert.Nil(t, err)
	startTime := "1d-ago"
	endTime := ""
	m := "test.metric.2{color=*,group=group 1}"
	expectedcolorsValues := []string{"olive", "green", "maroon", "lime", "yellow", "white", "purple", "navy", "aqua"}
	mQRequest, err := otsdbquery.ParseRequest(startTime, endTime, m, 0)
	assert.NoError(t, err)
	assert.NotNil(t, mQRequest)
	mQRequest.MetricsQuery.SubsequentAggs = &structs.MetricQueryAgg{
		AggBlockType:    structs.AggregatorBlock,
		AggregatorBlock: &mQRequest.MetricsQuery.FirstAggregator,
	}
	res := segment.ExecuteMetricsQuery(&mQRequest.MetricsQuery, &mQRequest.TimeRange, uint64(0))
	mQResponse, err := res.GetOTSDBResults(&mQRequest.MetricsQuery)
	assert.NotNil(t, mQRequest)
	assert.NotNil(t, mQResponse)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(mQResponse), 1)
	for _, response := range mQResponse {
		log.Infof("response: %+v", response.Tags)
		assert.Equal(t, "test.metric.2", response.MetricName)
		assert.Equal(t, 2, len(response.Tags))
		colorVal, ok := response.Tags["color"]
		assert.True(t, ok)
		log.Infof("val: [%s]", colorVal)
		assert.Contains(t, expectedcolorsValues, colorVal)
	}

	os.RemoveAll(config.GetDataPath())
}

func Test_MetricsQueryMultipleTagValues(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())
	limit.InitMemoryLimiter()
	writer.InitWriterNode()
	err := query.InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	assert.Nil(t, err)
	metrics.InitTestingConfig()
	assert.Nil(t, err)
	startTime := "1d-ago"
	endTime := ""
	m := "test.metric.2{group=group 0|group 1,color=yellow}"
	expectedTagKeys := []string{"group", "color"}
	expectedgroupValues := []string{"group 0", "group 1"}
	mQRequest, err := otsdbquery.ParseRequest(startTime, endTime, m, 0)
	assert.NoError(t, err)
	assert.NotNil(t, mQRequest)
	assert.Len(t, mQRequest.MetricsQuery.TagsFilters, 3)
	mQRequest.MetricsQuery.SubsequentAggs = &structs.MetricQueryAgg{
		AggBlockType:    structs.AggregatorBlock,
		AggregatorBlock: &mQRequest.MetricsQuery.FirstAggregator,
	}
	for _, tags := range mQRequest.MetricsQuery.TagsFilters {
		assert.Contains(t, expectedTagKeys, tags.TagKey)
		if tags.TagKey == "group" {
			assert.Contains(t, expectedgroupValues, tags.RawTagValue)
		}
	}
	os.RemoveAll(config.GetDataPath())
}
