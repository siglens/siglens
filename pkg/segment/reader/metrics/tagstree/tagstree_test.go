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
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/buger/jsonparser"
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
	tagKeyFileExists := attr.tagTreeFileExists(tagKey)
	assert.True(t, tagKeyFileExists)
	exists, tagValExists, rawTagValueToTSIDs, tagHashValue, err := attr.GetMatchingTSIDsOrCount(metricName, tagKey, tagValue, segutils.Equal, nil)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.True(t, tagValExists)
	assert.Equal(t, tagValue, tagHashValue)
	assert.Len(t, rawTagValueToTSIDs, 1)
	assert.Greater(t, len(rawTagValueToTSIDs["yellow"]), 0)

	metricName = xxhash.Sum64String("test.metric.1")
	tagKey = "group"
	expectedtagValues := []string{"group 0", "group 1"}
	tagKeyFileExists = attr.tagTreeFileExists(tagKey)
	assert.True(t, tagKeyFileExists)
	itr, found, err := attr.GetValueIteratorForMetric(metricName, tagKey)
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
	colorTagKeyFileExists := attr.tagTreeFileExists("color")
	assert.True(t, colorTagKeyFileExists)
	exists, tagValExists, rawTagValueToTSIDs, _, err := attr.GetMatchingTSIDsOrCount(metric1, "color", xxhash.Sum64String("blue"), segutils.Equal, nil)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.True(t, tagValExists)
	assert.Equal(t, numTSIDs(rawTagValueToTSIDs), 2)

	fruitTagKeyFileExists := attr.tagTreeFileExists("fruit")
	assert.True(t, fruitTagKeyFileExists)
	exists, tagValExists, rawTagValueToTSIDs, _, err = attr.GetMatchingTSIDsOrCount(metric1, "fruit", xxhash.Sum64String("pear"), segutils.Equal, nil)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.True(t, tagValExists)
	assert.Equal(t, numTSIDs(rawTagValueToTSIDs), 3)

	// Test selecting for key != value
	exists, tagValExists, rawTagValueToTSIDs, _, err = attr.GetMatchingTSIDsOrCount(metric1, "color", xxhash.Sum64String("green"), segutils.NotEqual, nil)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.True(t, tagValExists)
	assert.Equal(t, numTSIDs(rawTagValueToTSIDs), 4)

	exists, tagValExists, rawTagValueToTSIDs, _, err = attr.GetMatchingTSIDsOrCount(metric1, "fruit", xxhash.Sum64String("pear"), segutils.NotEqual, nil)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.True(t, tagValExists)
	assert.Equal(t, numTSIDs(rawTagValueToTSIDs), 2)

	exists, tagValExists, rawTagValueToTSIDs, _, err = attr.GetMatchingTSIDsOrCount(metric1, "fruit", xxhash.Sum64String("this-doesn't-match-anything"), segutils.NotEqual, nil)
	assert.Nil(t, err)
	assert.True(t, exists)
	assert.True(t, tagValExists)
	assert.Equal(t, numTSIDs(rawTagValueToTSIDs), 5)

	// Cleanup
	_ = os.RemoveAll("./tagstree-test.test")
	_ = os.RemoveAll("./ingestnodes")
}

func Test_SimpleReadWrite(t *testing.T) {
	initTestConfig(t)

	// Create writer
	treeHolder, err := metrics.InitTagsTreeHolder("test_mid")
	assert.NoError(t, err)

	tagsHolder := metrics.GetTagsHolder()
	key1 := "key1"
	key2 := "key2"
	value1 := "value1"
	value2 := "value2"
	tagsHolder.Insert(key1, []byte(value1), jsonparser.String)
	tagsHolder.Insert(key2, []byte(value2), jsonparser.String)
	tsid, err := tagsHolder.GetTSID([]byte("metric1"))
	assert.NoError(t, err)

	err = treeHolder.AddTagsForTSID([]byte("metric1"), tagsHolder, tsid)
	assert.NoError(t, err)

	// Flush to disk
	err = treeHolder.EncodeTagsTreeHolder()
	assert.NoError(t, err)

	// Read from disk
	baseDir := metrics.GetFinalTagsTreeDir("test_mid", 0)
	reader, err := InitAllTagsTreeReader(baseDir)
	assert.NoError(t, err)
	defer reader.CloseAllTagTreeReaders()

	tagPairs, err := reader.GetAllTagPairs()
	assert.NoError(t, err)
	assert.Equal(t, 2, len(tagPairs))

	key1Values, ok := tagPairs[key1]
	assert.True(t, ok)
	assert.Len(t, key1Values, 1)
	_, ok = key1Values[value1]
	assert.True(t, ok)

	key2Values, ok := tagPairs[key2]
	assert.True(t, ok)
	assert.Len(t, key2Values, 1)
	_, ok = key2Values[value2]
	assert.True(t, ok)
}

func Test_ConcurrentReadWrite(t *testing.T) {
	initTestConfig(t)

	// Create writer
	treeHolder, err := metrics.InitTagsTreeHolder("test_mid")
	assert.NoError(t, err)

	tagsHolder := metrics.GetTagsHolder()
	tagKey := "host"
	tagsHolder.Insert(tagKey, []byte("server1"), jsonparser.String)

	firstFlushChan := make(chan struct{})
	numIters := 1000
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		for i := 0; i < numIters; i++ {
			metric := []byte(fmt.Sprintf("metric%d", i))
			tsid, err := tagsHolder.GetTSID(metric)
			assert.NoError(t, err)

			err = treeHolder.AddTagsForTSID(metric, tagsHolder, tsid)
			assert.NoError(t, err)

			// Flush to disk
			err = treeHolder.EncodeTagsTreeHolder()
			assert.NoError(t, err)
			if i == 0 {
				firstFlushChan <- struct{}{}
			}
		}
	}()

	baseDir := metrics.GetFinalTagsTreeDir("test_mid", 0)
	uniqueNumMetrics := make(map[int]struct{})

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		// Wait for first flush
		<-firstFlushChan

		numMetrics := 0
		for i := 0; i < numIters; i++ {
			// Read from disk
			reader, err := InitAllTagsTreeReader(baseDir)
			assert.NoError(t, err)

			tagPairs, err := reader.GetAllTagPairs()
			assert.NoError(t, err)
			assert.Len(t, tagPairs, 1)
			values, ok := tagPairs[tagKey]
			assert.True(t, ok)
			assert.Len(t, values, 1)

			metrics, err := reader.GetHashedMetricNames()
			assert.NoError(t, err)
			assert.True(t, len(metrics) >= numMetrics)
			numMetrics = len(metrics)

			uniqueNumMetrics[len(metrics)] = struct{}{}

			reader.CloseAllTagTreeReaders()
		}
	}()
	waitGroup.Wait()

	if len(uniqueNumMetrics) < 5 { // Somewhat arbitrary number
		// The purpose of this unit test is to test concurrent read/write, and
		// we expect a lot of interleaving of those operations. If we got here,
		// the test doesn't sufficiently test concurrency, so fail the test. We
		// may need to increase the number of iterations.
		t.Errorf("Insufficient testing; only got %d unique number of metrics", len(uniqueNumMetrics))
		t.FailNow()
	}

	reader, err := InitAllTagsTreeReader(baseDir)
	assert.NoError(t, err)
	defer reader.CloseAllTagTreeReaders()

	metrics, err := reader.GetHashedMetricNames()
	assert.NoError(t, err)
	assert.Len(t, metrics, numIters)
}

func initTestConfig(t *testing.T) {
	runningConfig := config.GetTestConfig(t.TempDir())
	config.SetConfig(runningConfig)

	err := config.InitDerivedConfig("test")
	assert.NoError(t, err)
}
