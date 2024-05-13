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
	tagKeyFileExists, fInfo := attr.getTagTreeFileInfoForTagKey(tagKey)
	assert.True(t, tagKeyFileExists)
	exists, tagValExists, rawTagValueToTSIDs, tagHashValue, err := attr.GetMatchingTSIDs(metricName, tagKey, tagValue, segutils.Equal, fInfo)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.True(t, tagValExists)
	assert.Equal(t, tagValue, tagHashValue)
	assert.Len(t, rawTagValueToTSIDs, 1)
	assert.Greater(t, len(rawTagValueToTSIDs["yellow"]), 0)

	metricName = xxhash.Sum64String("test.metric.1")
	tagKey = "group"
	expectedtagValues := []string{"group 0", "group 1"}
	tagKeyFileExists, fInfo = attr.getTagTreeFileInfoForTagKey(tagKey)
	assert.True(t, tagKeyFileExists)
	itr, found, err := attr.GetValueIteratorForMetric(metricName, tagKey, fInfo)
	assert.Nil(t, err)
	assert.True(t, found)
	count := 0
	for {
		_, filterByteSlice, _, _, more := itr.Next()
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
		{metric: "metric1", tags: map[string]string{"color": "blue", "fruit": "apple"}},
		{metric: "metric1", tags: map[string]string{"color": "green", "fruit": "pear"}},
		{metric: "metric1", tags: map[string]string{"color": "blue", "fruit": "pear"}},
		{metric: "metric1", tags: map[string]string{"color": "red", "fruit": "pear"}},
		{metric: "metric1", tags: map[string]string{"color": "red", "fruit": "apple"}},
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
	colorTagKeyFileExists, colorFInfo := attr.getTagTreeFileInfoForTagKey("color")
	assert.True(t, colorTagKeyFileExists)
	exists, tagValExists, rawTagValueToTSIDs, _, err := attr.GetMatchingTSIDs(metric1, "color", xxhash.Sum64String("blue"), segutils.Equal, colorFInfo)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.True(t, tagValExists)
	assert.Equal(t, numTSIDs(rawTagValueToTSIDs), 2)

	fruitTagKeyFileExists, fruitFInfo := attr.getTagTreeFileInfoForTagKey("fruit")
	assert.True(t, fruitTagKeyFileExists)
	exists, tagValExists, rawTagValueToTSIDs, _, err = attr.GetMatchingTSIDs(metric1, "fruit", xxhash.Sum64String("pear"), segutils.Equal, fruitFInfo)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.True(t, tagValExists)
	assert.Equal(t, numTSIDs(rawTagValueToTSIDs), 3)

	// Test selecting for key != value
	exists, tagValExists, rawTagValueToTSIDs, _, err = attr.GetMatchingTSIDs(metric1, "color", xxhash.Sum64String("green"), segutils.NotEqual, colorFInfo)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.True(t, tagValExists)
	assert.Equal(t, numTSIDs(rawTagValueToTSIDs), 4)

	exists, tagValExists, rawTagValueToTSIDs, _, err = attr.GetMatchingTSIDs(metric1, "fruit", xxhash.Sum64String("pear"), segutils.NotEqual, fruitFInfo)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.True(t, tagValExists)
	assert.Equal(t, numTSIDs(rawTagValueToTSIDs), 2)

	exists, tagValExists, rawTagValueToTSIDs, _, err = attr.GetMatchingTSIDs(metric1, "fruit", xxhash.Sum64String("this-doesn't-match-anything"), segutils.NotEqual, fruitFInfo)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.True(t, tagValExists)
	assert.Equal(t, numTSIDs(rawTagValueToTSIDs), 5)

	// Cleanup
	_ = os.RemoveAll("./tagstree-test.test")
	_ = os.RemoveAll("./ingestnodes")
}
