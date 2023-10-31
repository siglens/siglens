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

package tagstree

import (
	"encoding/json"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/config"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// Helper structs/functions

type timeSeries struct {
	metric string
	tags   map[string]string
}

func writeMockMetrics(forceRotate bool, allTimeSeries []timeSeries) ([]*metrics.MetricsSegment, error) {
	timestamp := uint64(time.Now().Unix() - 24*3600)
	metrics.InitMetricsSegStore()

	// Create the metrics.
	for i, tSeries := range allTimeSeries {
		entry := make(map[string]interface{})
		entry["metric"] = tSeries.metric
		entry["tags"] = tSeries.tags

		entry["timestamp"] = timestamp + uint64(i)
		entry["value"] = rand.Intn(500)
		rawJson, _ := json.Marshal(entry)
		err := writer.AddTimeSeriesEntryToInMemBuf(rawJson, segutils.SIGNAL_METRICS_OTSDB, 0)
		if err != nil {
			log.Errorf("writeMockMetrics: error adding time series entry to in memory buffer: %s", err)
			return nil, err
		}
	}

	// Check and rotate each segement.
	retVal := make([]*metrics.MetricsSegment, len(metrics.GetAllMetricsSegments()))

	for idx, mSeg := range metrics.GetAllMetricsSegments() {
		err := mSeg.CheckAndRotate(forceRotate)
		if err != nil {
			log.Errorf("writeMockMetrics: unable to force rotate: %s", err)
			return nil, err
		}
		retVal[idx] = mSeg
	}

	return retVal, nil
}

func numTSIDs(rawTagValueToTSIDs map[string]map[uint64]struct{}) int {
	total := 0
	for _, set := range rawTagValueToTSIDs {
		total += len(set)
	}

	return total
}

// Test cases

func Test_ReadWriteTagsTree(t *testing.T) {
	config.SetSSInstanceName("./tagstree-test")
	err := config.InitDerivedConfig("test")
	assert.NoError(t, err)
	metrics.InitTestingConfig()
	mSegs, err := writer.WriteMockMetricsSegment(false, 1000)
	assert.Nil(t, err)
	assert.Greater(t, len(mSegs), 0)
	mSeg := mSegs[0]

	tagsTreeHolder := metrics.GetTagsTreeHolder(mSeg.Orgid, mSeg.Mid)
	err = tagsTreeHolder.EncodeTagsTreeHolder()
	assert.Nil(t, err)
	tagsTreeDir := metrics.GetFinalTagsTreeDir(mSeg.Mid, mSeg.Suffix)
	attr, err := InitAllTagsTreeReader(tagsTreeDir)
	assert.Nil(t, err)
	assert.NotNil(t, attr)

	var metricName uint64
	var tagValue uint64
	var tagKey string
	metricName = xxhash.Sum64String("test.metric.0")
	tagKey = "color"
	tagValue = xxhash.Sum64String("yellow")
	exists, rawTagValueToTSIDs, tagHashValue, err := attr.GetMatchingTSIDs(metricName, tagKey, tagValue, segutils.Equal)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.Equal(t, tagValue, tagHashValue)
	assert.Len(t, rawTagValueToTSIDs, 1)
	assert.Greater(t, len(rawTagValueToTSIDs["yellow"]), 0)

	metricName = xxhash.Sum64String("test.metric.1")
	tagKey = "group"
	expectedtagValues := []string{"group 0", "group 1"}
	itr, found, err := attr.GetValueIteratorForMetric(metricName, tagKey)
	assert.Nil(t, err)
	assert.True(t, found)
	count := 0
	for {
		_, filterByteSlice, _, more := itr.Next()
		if !more {
			break
		}
		assert.Contains(t, expectedtagValues, string(filterByteSlice))
		count += 1

	}
	assert.Equal(t, len(expectedtagValues), count)

	_ = os.RemoveAll("./tagstree-test.test")
	_ = os.RemoveAll("./ingestnodes")
}

func Test_SelectOneTagKeyValuePair(t *testing.T) {
	config.SetSSInstanceName("./tagstree-test")
	err := config.InitDerivedConfig("test")
	assert.NoError(t, err)
	metrics.InitTestingConfig()

	allTimeSeries := []timeSeries{
		timeSeries{metric: "metric1", tags: map[string]string{"color": "blue", "fruit": "apple"}},
		timeSeries{metric: "metric1", tags: map[string]string{"color": "green", "fruit": "pear"}},
		timeSeries{metric: "metric1", tags: map[string]string{"color": "blue", "fruit": "pear"}},
		timeSeries{metric: "metric1", tags: map[string]string{"color": "red", "fruit": "pear"}},
		timeSeries{metric: "metric1", tags: map[string]string{"color": "red", "fruit": "apple"}},
	}

	mSegs, err := writeMockMetrics(false, allTimeSeries)
	assert.Nil(t, err)
	assert.Greater(t, len(mSegs), 0)

	tagsTreeHolder := metrics.GetTagsTreeHolder(mSegs[0].Orgid, mSegs[0].Mid)
	err = tagsTreeHolder.EncodeTagsTreeHolder()
	assert.Nil(t, err)
	tagsTreeDir := metrics.GetFinalTagsTreeDir(mSegs[0].Mid, mSegs[0].Suffix)
	attr, err := InitAllTagsTreeReader(tagsTreeDir)
	assert.Nil(t, err)
	assert.NotNil(t, attr)

	var exists bool
	var rawTagValueToTSIDs map[string]map[uint64]struct{}
	metric1 := xxhash.Sum64String("metric1")

	// Test selecting for key = value
	exists, rawTagValueToTSIDs, _, err = attr.GetMatchingTSIDs(metric1, "color", xxhash.Sum64String("blue"), segutils.Equal)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.Equal(t, numTSIDs(rawTagValueToTSIDs), 2)

	exists, rawTagValueToTSIDs, _, err = attr.GetMatchingTSIDs(metric1, "fruit", xxhash.Sum64String("pear"), segutils.Equal)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.Equal(t, numTSIDs(rawTagValueToTSIDs), 3)

	// Test selecting for key != value
	exists, rawTagValueToTSIDs, _, err = attr.GetMatchingTSIDs(metric1, "color", xxhash.Sum64String("green"), segutils.NotEqual)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.Equal(t, numTSIDs(rawTagValueToTSIDs), 4)

	exists, rawTagValueToTSIDs, _, err = attr.GetMatchingTSIDs(metric1, "fruit", xxhash.Sum64String("pear"), segutils.NotEqual)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.Equal(t, numTSIDs(rawTagValueToTSIDs), 2)

	exists, rawTagValueToTSIDs, _, err = attr.GetMatchingTSIDs(metric1, "fruit", xxhash.Sum64String("this-doesn't-match-anything"), segutils.NotEqual)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.Equal(t, numTSIDs(rawTagValueToTSIDs), 5)

	// Cleanup
	_ = os.RemoveAll("./tagstree-test.test")
	_ = os.RemoveAll("./ingestnodes")
}
