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

package e2etests

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/integrations/prometheus/promql"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/memory/limit"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/results/mresults"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	"github.com/siglens/siglens/pkg/segment/writer/metrics/meta"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

/*
	This test suite is to test the metrics query E2E.

	# Whenever a new test is added, do not forget to call the cleanUp function.
	All the tests below will have `defer CleanUp(t)` as the first line at the start of any testing function.

	Steps to follow for adding a new metric query test:
	1. Define the Test function name appropriately.
	2. defer CleanUp(t) as the first line of the test function. This will ensure that the data is cleaned up after the test is run.
	3. init the testing config by calling initTestConfig()
	4. Get the test metrics data by calling GetTestMetricsData(startTimestamp). This function returns the test metrics timeseries data, metric names, tag keys and tag key values.
	5. Ingest the test metrics data by calling ingestTestMetricsData(allTimeSeries). This function ingests the test metrics data into the in-memory buffer.
	6. Rotate the metrics data by calling rotateMetricsDataAndClearSegStore(true). This function forces the rotation of the metrics data. And clear the unrotated segments in memory.
	7. Initialize the metrics data and query node by calling initializeMetricsMetaData(). This function populates the metrics data.
	8. Define the time range for the query by creating a MetricsTimeRange object.
	9. Define the expected results for the query.
	10. Parse the query by calling ConvertPromQLToMetricsQuery. This function converts the promql query to metrics query.
	11. Execute the metrics query by calling ExecuteMetricsQuery/ExecuteMultipleMetricsQuery depening on the query Type. This function executes the metrics query and returns the results.
	12. Validate the results by comparing the expected results with the actual results.
*/

type timeSeries struct {
	Metric    string            `json:"metric"`
	Tags      map[string]string `json:"tags"`
	Timestamp uint32            `json:"timestamp"`
	Value     int               `json:"value"`
}

// dataStartTimestamp is the start timestamp for the test data
// This is used to generate the test data and every query must use this timestamp as the start timestamp
// You can change this value to some other value or set it dynamically based on the current time. For example: uint32(time.Now().Unix() - 24*3600)
// But this might cause the test cases to fail. But that does not mean there is an error with the code.
// The problem is with the assumption of the test case. The timeseries at T1 and T2 has time diff = 1 second.
// The below test cases assume them to be downsampled to the same time bucket. But sometimes when you use dynamic timestamp that might not happen and the test case might fail.
// This will happen because of how we calculate the downsampledTime using the formula: downsampledTime = (ts / s.dsSeconds) * s.dsSeconds
// So, it is better to keep this value to be constant and not change this.
const dataStartTimestamp uint32 = 1718052279

func GetTestMetricsData(startTimestamp uint32) ([]timeSeries, []string, map[string][]string, map[string][]string) {
	allTimeSeries := []timeSeries{
		{
			Metric: "testmetric0",
			Tags: map[string]string{
				"color": "red",
				"shape": "circle",
				"size":  "small",
				"type":  "solid",
			},
			Timestamp: startTimestamp + 1,
			Value:     10,
		},
		{
			Metric: "testmetric0",
			Tags: map[string]string{
				"color":  "red",
				"shape":  "circle",
				"radius": "10",
				"type":   "solid",
			},
			Timestamp: startTimestamp + 2,
			Value:     40,
		},
		{
			Metric: "testmetric0",
			Tags: map[string]string{
				"color": "red",
				"shape": "circle",
				"size":  "small",
				"type":  "solid",
			},
			Timestamp: startTimestamp + 650,
			Value:     50,
		},
		{
			Metric: "testmetric0",
			Tags: map[string]string{
				"color": "red",
				"shape": "circle",
				"size":  "small",
				"type":  "solid",
			},
			Timestamp: startTimestamp + 2700,
			Value:     60,
		},
		{
			Metric: "testmetric0",
			Tags: map[string]string{
				"color": "red",
				"shape": "circle",
				"size":  "small",
				"type":  "solid",
			},
			Timestamp: startTimestamp + 3600,
			Value:     70,
		},
		{
			Metric: "testmetric1",
			Tags: map[string]string{
				"color": "blue",
				"shape": "square",
				"size":  "medium",
				"type":  "solid",
			},
			Timestamp: startTimestamp + 1,
			Value:     20,
		},
		{
			Metric: "testmetric1",
			Tags: map[string]string{
				"color": "blue",
				"shape": "square",
				"size":  "medium",
				"type":  "solid",
			},
			Timestamp: startTimestamp + 2,
			Value:     80,
		},
		{
			Metric: "testmetric1",
			Tags: map[string]string{
				"color": "blue",
				"shape": "square",
				"size":  "medium",
				"type":  "solid",
			},
			Timestamp: startTimestamp + 650,
			Value:     90,
		},
		{
			Metric: "testmetric1",
			Tags: map[string]string{
				"color": "blue",
				"shape": "square",
				"size":  "medium",
				"type":  "solid",
			},
			Timestamp: startTimestamp + 2700,
			Value:     100,
		},
		{
			Metric: "testmetric1",
			Tags: map[string]string{
				"color": "blue",
				"shape": "square",
				"size":  "medium",
				"type":  "solid",
			},
			Timestamp: startTimestamp + 3600,
			Value:     110,
		},
		{
			Metric: "testmetric2",
			Tags: map[string]string{
				"color": "green",
				"shape": "triangle",
				"size":  "large",
				"type":  "solid",
			},
			Timestamp: startTimestamp + 1,
			Value:     30,
		},
		{
			Metric: "testmetric2",
			Tags: map[string]string{
				"color": "green",
				"shape": "triangle",
				"size":  "large",
				"type":  "solid",
			},
			Timestamp: startTimestamp + 2,
			Value:     120,
		},
		{
			Metric: "testmetric2",
			Tags: map[string]string{
				"color": "green",
				"shape": "triangle",
				"size":  "large",
				"type":  "solid",
			},
			Timestamp: startTimestamp + 650,
			Value:     130,
		},
		{
			Metric: "testmetric2",
			Tags: map[string]string{
				"color": "green",
				"shape": "triangle",
				"size":  "large",
				"type":  "solid",
			},
			Timestamp: startTimestamp + 2700,
			Value:     140,
		},
		{
			Metric: "testmetric2",
			Tags: map[string]string{
				"color": "green",
				"shape": "triangle",
				"size":  "large",
				"type":  "solid",
			},
			Timestamp: startTimestamp + 3600,
			Value:     150,
		},
	}

	allMetricNames := []string{"testmetric0", "testmetric1", "testmetric2"}
	allTagKeys := map[string][]string{
		"testmetric0": {"color", "shape", "size", "radius", "type"},
		"testmetric1": {"color", "shape", "size", "type"},
		"testmetric2": {"color", "shape", "size", "type"},
	}
	allTagKeyValues := map[string][]string{
		"testmetric0": {
			"color:red",
			"shape:circle",
			"size:small",
			"radius:10",
			"type:solid",
		},
		"testmetric1": {
			"color:blue",
			"shape:square",
			"size:medium",
			"type:solid",
		},
		"testmetric2": {
			"color:green",
			"shape:triangle",
			"size:large",
			"type:solid",
		},
	}

	return allTimeSeries, allMetricNames, allTagKeys, allTagKeyValues
}

func initTestConfig() error {
	runningConfig := config.GetTestConfig()
	runningConfig.DataPath = "metrics-e2etest-data/"
	runningConfig.SSInstanceName = "test"
	config.SetConfig(runningConfig)
	err := config.InitDerivedConfig("test")
	if err != nil {
		log.Errorf("initTestConfig: Error initializing config: %v", err)
		return err
	}
	limit.InitMemoryLimiter()

	metrics.InitTestingConfig()

	err = meta.InitMetricsMeta()
	if err != nil {
		log.Errorf("initTestConfig: failed to initialize metrics meta")
		return err
	}

	return nil
}

func initializeMetricsMetaData() error {
	metricMetaFileName := meta.GetLocalMetricsMetaFName()
	log.Infof("initTestConfig: metricMetaFileName: %s", metricMetaFileName)
	err := query.PopulateMetricsMetadataForTheFile_TestOnly(metricMetaFileName)
	if err != nil {
		log.Errorf("initTestConfig: failed to populate metrics meta")
		return err
	}

	return nil
}

func ingestTestMetricsData(allTimeSeries []timeSeries) error {
	// Ingest Data
	for _, ts := range allTimeSeries {
		rawJson, err := json.Marshal(ts)
		if err != nil {
			log.Errorf("IngestTestMetricsData: Error marshalling time series: %v", err)
			return err
		}
		err = writer.AddTimeSeriesEntryToInMemBuf(rawJson, utils.SIGNAL_METRICS_OTSDB, 0)
		if err != nil {
			log.Errorf("IngestTestMetricsData: Error adding time series entry to in memory buffer: %v", err)
			return err
		}
	}
	return nil
}

func rotateMetricsDataAndClearSegStore(forceRotate bool) ([]*metrics.MetricsSegment, error) {
	retVal := make([]*metrics.MetricsSegment, len(metrics.GetAllMetricsSegments()))

	for idx, mSeg := range metrics.GetAllMetricsSegments() {
		err := mSeg.CheckAndRotate(forceRotate)
		if err != nil {
			log.Errorf("writeMockMetrics: unable to force rotate: %s", err)
			return nil, err
		}
		retVal[idx] = mSeg
	}

	metrics.ResetMetricsSegStore_TestOnly() // reset the metrics segment store

	return retVal, nil
}

func cleanUp(t *testing.T) {
	metadata.ResetMetricsMetadata_TestOnly() // reset the rotated segments data
	metrics.ResetMetricsSegStore_TestOnly()  // reset the metrics segment store

	log.Infof("cleanUp: Removing data path: %s", config.GetDataPath())
	err := os.RemoveAll(config.GetDataPath())
	assert.Nil(t, err)
}

func Test_WriteMetrics(t *testing.T) {
	defer cleanUp(t)

	startTimestamp := dataStartTimestamp
	allTimeSeries, _, _, _ := GetTestMetricsData(startTimestamp)

	err := initTestConfig()
	assert.Nil(t, err)

	err = ingestTestMetricsData(allTimeSeries)
	assert.Nil(t, err)

	mSegs, err := rotateMetricsDataAndClearSegStore(true)
	assert.Nil(t, err)
	assert.Greater(t, len(mSegs), 0)
}

func Test_UnrotatedMetricNames(t *testing.T) {
	defer cleanUp(t)

	startTimestamp := dataStartTimestamp
	allTimeSeries, mNames, _, _ := GetTestMetricsData(startTimestamp)

	err := initTestConfig()
	assert.Nil(t, err)

	err = ingestTestMetricsData(allTimeSeries)
	assert.Nil(t, err)

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(startTimestamp),
		EndEpochSec:   uint32(startTimestamp + 4600),
	}

	metricNames, err := query.GetAllMetricNamesOverTheTimeRange(timeRange, 0)
	assert.Nil(t, err)
	assert.Greater(t, len(metricNames), 0)

	assert.ElementsMatch(t, mNames, metricNames)
}

func Test_RotatedMetricNames(t *testing.T) {
	defer cleanUp(t)

	startTimestamp := dataStartTimestamp
	allTimeSeries, mNames, _, _ := GetTestMetricsData(startTimestamp)

	err := initTestConfig()
	assert.Nil(t, err)

	err = ingestTestMetricsData(allTimeSeries)
	assert.Nil(t, err)

	mSegs, err := rotateMetricsDataAndClearSegStore(true)
	assert.Nil(t, err)
	assert.Greater(t, len(mSegs), 0)

	err = initializeMetricsMetaData()
	assert.Nil(t, err)

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(startTimestamp),
		EndEpochSec:   uint32(startTimestamp + 4600),
	}

	metricNames, err := query.GetAllMetricNamesOverTheTimeRange(timeRange, 0)
	assert.Nil(t, err)
	assert.Greater(t, len(metricNames), 0)

	assert.ElementsMatch(t, mNames, metricNames)
}

func Test_GetAllTagsForAMetric(t *testing.T) {
	defer cleanUp(t)

	startTimestamp := dataStartTimestamp
	allTimeSeries, mNames, tagKeys, tagKeyValues := GetTestMetricsData(startTimestamp)

	err := initTestConfig()
	assert.Nil(t, err)

	err = ingestTestMetricsData(allTimeSeries)
	assert.Nil(t, err)

	mSegs, err := rotateMetricsDataAndClearSegStore(true)
	assert.Nil(t, err)
	assert.Greater(t, len(mSegs), 0)

	err = initializeMetricsMetaData()
	assert.Nil(t, err)

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(startTimestamp),
		EndEpochSec:   uint32(startTimestamp + 4600),
	}

	for _, mName := range mNames {
		query := fmt.Sprintf("(%v)", mName)
		metricQueryRequest, _, _, err := promql.ConvertPromQLToMetricsQuery(query, timeRange.StartEpochSec, timeRange.EndEpochSec, 0)
		assert.Nil(t, err)

		metricQueryRequest[0].MetricsQuery.ExitAfterTagsSearch = true
		metricQueryRequest[0].MetricsQuery.TagIndicesToKeep = make(map[int]struct{})

		res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, 0)

		uniqueTagKeys, tagKeyValueSet, err := res.GetMetricTagsResultSet(&metricQueryRequest[0].MetricsQuery)
		assert.Nil(t, err)

		assert.Greater(t, len(uniqueTagKeys), 0)
		assert.Greater(t, len(tagKeyValueSet), 0)

		assert.ElementsMatch(t, tagKeys[mName], uniqueTagKeys)
		assert.ElementsMatch(t, tagKeyValues[mName], tagKeyValueSet)
	}
}

func Test_SimpleMetricQuery_v1(t *testing.T) {
	defer cleanUp(t)

	startTimestamp := dataStartTimestamp
	allTimeSeries, metricNames, _, _ := GetTestMetricsData(startTimestamp)

	err := initTestConfig()
	assert.Nil(t, err)

	err = ingestTestMetricsData(allTimeSeries)
	assert.Nil(t, err)

	mSegs, err := rotateMetricsDataAndClearSegStore(true)
	assert.Nil(t, err)
	assert.Greater(t, len(mSegs), 0)

	err = initializeMetricsMetaData()
	assert.Nil(t, err)

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(startTimestamp),
		EndEpochSec:   uint32(startTimestamp + 4600),
	}
	expectedResults := map[string][]float64{
		"testmetric0": {25, 50, 60, 70},
		"testmetric1": {50, 90, 100, 110},
		"testmetric2": {75, 130, 140, 150},
	}

	for _, mName := range metricNames {

		query := fmt.Sprintf("(%v)", mName)
		metricQueryRequest, _, _, err := promql.ConvertPromQLToMetricsQuery(query, timeRange.StartEpochSec, timeRange.EndEpochSec, 0)
		assert.Nil(t, err)

		res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, 0)
		assert.NotNil(t, res)
		assert.Equal(t, 1, len(res.Results))

		seriesDp := res.Results[fmt.Sprintf("%v{", mName)]
		assert.NotNil(t, seriesDp)
		assert.Greater(t, len(seriesDp), 0)

		seriesDpValues := make([]float64, 0)
		for _, dp := range seriesDp {
			seriesDpValues = append(seriesDpValues, dp)
		}
		sort.Slice(seriesDpValues, func(i, j int) bool {
			return seriesDpValues[i] < seriesDpValues[j]
		})

		assert.EqualValues(t, expectedResults[mName], seriesDpValues)
	}
}

func Test_SimpleMetricQuery_Regex_on_MetricName_Star(t *testing.T) {
	defer cleanUp(t)

	startTimestamp := dataStartTimestamp
	allTimeSeries, _, _, _ := GetTestMetricsData(startTimestamp)

	err := initTestConfig()
	assert.Nil(t, err)

	err = ingestTestMetricsData(allTimeSeries)
	assert.Nil(t, err)

	mSegs, err := rotateMetricsDataAndClearSegStore(true)
	assert.Nil(t, err)
	assert.Greater(t, len(mSegs), 0)

	err = initializeMetricsMetaData()
	assert.Nil(t, err)

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(startTimestamp),
		EndEpochSec:   uint32(startTimestamp + 4600),
	}

	intervalSeconds, err := mresults.CalculateInterval(timeRange.EndEpochSec - timeRange.StartEpochSec)
	assert.Nil(t, err)
	assert.Equal(t, uint32(20), intervalSeconds)

	query := `({__name__=~"testmetric.*"})`
	metricQueryRequest, _, _, err := promql.ConvertPromQLToMetricsQuery(query, timeRange.StartEpochSec, timeRange.EndEpochSec, 0)
	assert.Nil(t, err)

	/*
		Expected Results:
		At T1: testmetric0: 10, testmetric1: 20, testmetric2: 30
		At T2: testmetric0: 40, testmetric1: 80, testmetric2: 120
		At T3: testmetric0: 50, testmetric1: 90, testmetric2: 130
		At T4: testmetric0: 60, testmetric1: 100, testmetric2: 140
		At T5: testmetric0: 70, testmetric1: 110, testmetric2: 150

		The time diff between T1 and T2 is 1 second, so the values at T1 and T2 will be aggregated to the same bucket.

		Values at T1 bucket = 10 + 20 + 30 + 40 + 80 + 120 = 300 / 6 = 50
		Values at T2 bucket = 50 + 90 + 130  = 270 / 3 = 90
		Values at T3 bucket = 60 + 100 + 140 = 300 / 3 = 100
		Values at T4 bucket = 70 + 110 + 150 = 330 / 3 = 110
	*/
	expectedResults := []float64{50, 90, 100, 110}

	res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, 0)
	assert.NotNil(t, res)
	assert.Equal(t, 1, len(res.Results))

	seriesDp := res.Results["*{"]
	assert.NotNil(t, seriesDp)
	assert.Greater(t, len(seriesDp), 0)

	seriesDpValues := make([]float64, 0)
	for _, dp := range seriesDp {
		seriesDpValues = append(seriesDpValues, dp)
	}
	sort.Slice(seriesDpValues, func(i, j int) bool {
		return seriesDpValues[i] < seriesDpValues[j]
	})

	assert.EqualValues(t, expectedResults, seriesDpValues)
}

func Test_SimpleMetricQuery_Regex_on_MetricName_OR(t *testing.T) {
	defer cleanUp(t)

	startTimestamp := dataStartTimestamp
	allTimeSeries, _, _, _ := GetTestMetricsData(startTimestamp)

	err := initTestConfig()
	assert.Nil(t, err)

	err = ingestTestMetricsData(allTimeSeries)
	assert.Nil(t, err)

	mSegs, err := rotateMetricsDataAndClearSegStore(true)
	assert.Nil(t, err)
	assert.Greater(t, len(mSegs), 0)

	err = initializeMetricsMetaData()
	assert.Nil(t, err)

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(startTimestamp),
		EndEpochSec:   uint32(startTimestamp + 4600),
	}

	intervalSeconds, err := mresults.CalculateInterval(timeRange.EndEpochSec - timeRange.StartEpochSec)
	assert.Nil(t, err)
	assert.Equal(t, uint32(20), intervalSeconds)

	query := `({__name__=~"testmetric(0|1)"})`
	metricQueryRequest, _, _, err := promql.ConvertPromQLToMetricsQuery(query, timeRange.StartEpochSec, timeRange.EndEpochSec, 0)
	assert.Nil(t, err)

	/*
		Expected Results:
		At T1: testmetric0: 10, testmetric1: 20
		At T2: testmetric0: 40, testmetric1: 80
		At T3: testmetric0: 50, testmetric1: 90
		At T4: testmetric0: 60, testmetric1: 100
		At T5: testmetric0: 70, testmetric1: 110

		The time diff between T1 and T2 is 1 second, so the values at T1 and T2 will be aggregated to the same bucket.

		Values at T1 bucket = 10 + 20 + 40 + 80 = 150 / 4 = 37.5
		Values at T2 bucket = 50 + 90 = 140 / 2 = 70
		Values at T3 bucket = 60 + 100 = 160 / 2 = 80
		Values at T4 bucket = 70 + 110 = 180 / 2 = 90
	*/

	expectedResults := []float64{37.5, 70, 80, 90}

	res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, 0)
	assert.NotNil(t, res)
	assert.Equal(t, 1, len(res.Results))

	seriesDp := res.Results["*{"]
	assert.NotNil(t, seriesDp)
	assert.Greater(t, len(seriesDp), 0)

	seriesDpValues := make([]float64, 0)
	for _, dp := range seriesDp {
		seriesDpValues = append(seriesDpValues, dp)
	}

	sort.Slice(seriesDpValues, func(i, j int) bool {
		return seriesDpValues[i] < seriesDpValues[j]
	})

	assert.EqualValues(t, expectedResults, seriesDpValues)
}

func Test_SimpleMetricQuery_Regex_on_MetricName_GroupByMetric(t *testing.T) {
	defer cleanUp(t)

	startTimestamp := dataStartTimestamp
	allTimeSeries, _, _, _ := GetTestMetricsData(startTimestamp)

	err := initTestConfig()
	assert.Nil(t, err)

	err = ingestTestMetricsData(allTimeSeries)
	assert.Nil(t, err)

	mSegs, err := rotateMetricsDataAndClearSegStore(true)
	assert.Nil(t, err)
	assert.Greater(t, len(mSegs), 0)

	err = initializeMetricsMetaData()
	assert.Nil(t, err)

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(startTimestamp),
		EndEpochSec:   uint32(startTimestamp + 4600),
	}

	intervalSeconds, err := mresults.CalculateInterval(timeRange.EndEpochSec - timeRange.StartEpochSec)
	assert.Nil(t, err)
	assert.Equal(t, uint32(20), intervalSeconds)

	query := `avg ({__name__=~"testmetric.*"}) by (__name__)`
	metricQueryRequest, _, _, err := promql.ConvertPromQLToMetricsQuery(query, timeRange.StartEpochSec, timeRange.EndEpochSec, 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(metricQueryRequest))
	assert.True(t, metricQueryRequest[0].MetricsQuery.GroupByMetricName)

	/*
		Expected Results:
		This query will return the same results as the simple metric query for each metric name.

		So the expected results are:
		testmetric0: {25, 50, 60, 70}
		testmetric1: {50, 90, 100, 110}
		testmetric2: {75, 130, 140, 150}

	*/

	expectedResults := map[string][]float64{
		"testmetric0{": {25, 50, 60, 70},
		"testmetric1{": {50, 90, 100, 110},
		"testmetric2{": {75, 130, 140, 150},
	}

	res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, 0)
	assert.NotNil(t, res)
	assert.Equal(t, 3, len(res.Results))

	for mName, seriesDp := range res.Results {
		assert.NotNil(t, seriesDp)
		assert.Greater(t, len(seriesDp), 0)

		seriesDpValues := make([]float64, 0)
		for _, dp := range seriesDp {
			seriesDpValues = append(seriesDpValues, dp)
		}
		sort.Slice(seriesDpValues, func(i, j int) bool {
			return seriesDpValues[i] < seriesDpValues[j]
		})

		assert.EqualValues(t, expectedResults[mName], seriesDpValues)
	}
}

func Test_SimpleMetricQuery_Regex_on_MetricName_Plus_Filter(t *testing.T) {
	defer cleanUp(t)

	startTimestamp := dataStartTimestamp
	allTimeSeries, _, _, _ := GetTestMetricsData(startTimestamp)

	err := initTestConfig()
	assert.Nil(t, err)

	err = ingestTestMetricsData(allTimeSeries)
	assert.Nil(t, err)

	mSegs, err := rotateMetricsDataAndClearSegStore(true)
	assert.Nil(t, err)
	assert.Greater(t, len(mSegs), 0)

	err = initializeMetricsMetaData()
	assert.Nil(t, err)

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(startTimestamp),
		EndEpochSec:   uint32(startTimestamp + 4600),
	}

	intervalSeconds, err := mresults.CalculateInterval(timeRange.EndEpochSec - timeRange.StartEpochSec)
	assert.Nil(t, err)
	assert.Equal(t, uint32(20), intervalSeconds)

	query := `avg ({__name__=~"testmetric.*", color="red"})`
	metricQueryRequest, _, _, err := promql.ConvertPromQLToMetricsQuery(query, timeRange.StartEpochSec, timeRange.EndEpochSec, 0)
	assert.Nil(t, err)

	/*
		Expected Results:
		At T1: testmetric0: 10
		At T2: testmetric0: 40
		At T3: testmetric0: 50
		At T4: testmetric0: 60
		At T5: testmetric0: 70

		The time diff between T1 and T2 is 1 second, so the values at T1 and T2 will be aggregated to the same bucket.

		Values at T1 bucket = 10 + 40 = 50 / 2 = 25
		Values at T2 bucket = 50 = 50
		Values at T3 bucket = 60 = 60
		Values at T4 bucket = 70 = 70
	*/

	expectedResults := []float64{25, 50, 60, 70}

	res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, 0)
	assert.NotNil(t, res)
	assert.Equal(t, 1, len(res.Results))

	for seriesId, seriesDp := range res.Results {
		assert.NotNil(t, seriesDp)
		assert.Greater(t, len(seriesDp), 0)

		assert.True(t, strings.Contains(seriesId, "*{"))
		assert.True(t, strings.Contains(seriesId, "color:red"))

		seriesDpValues := make([]float64, 0)
		for _, dp := range seriesDp {
			seriesDpValues = append(seriesDpValues, dp)
		}
		sort.Slice(seriesDpValues, func(i, j int) bool {
			return seriesDpValues[i] < seriesDpValues[j]
		})

		assert.EqualValues(t, expectedResults, seriesDpValues)
	}
}

func Test_SimpleMetricQuery_Regex_on_MetricName_Plus_Filter_GroupByMetric_v1(t *testing.T) {
	defer cleanUp(t)

	startTimestamp := dataStartTimestamp
	allTimeSeries, _, _, _ := GetTestMetricsData(startTimestamp)

	err := initTestConfig()
	assert.Nil(t, err)

	err = ingestTestMetricsData(allTimeSeries)
	assert.Nil(t, err)

	mSegs, err := rotateMetricsDataAndClearSegStore(true)
	assert.Nil(t, err)
	assert.Greater(t, len(mSegs), 0)

	err = initializeMetricsMetaData()
	assert.Nil(t, err)

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(startTimestamp),
		EndEpochSec:   uint32(startTimestamp + 4600),
	}

	intervalSeconds, err := mresults.CalculateInterval(timeRange.EndEpochSec - timeRange.StartEpochSec)
	assert.Nil(t, err)
	assert.Equal(t, uint32(20), intervalSeconds)

	query := `avg ({__name__=~"testmetric.*", color="red"}) by (__name__)`
	metricQueryRequest, _, _, err := promql.ConvertPromQLToMetricsQuery(query, timeRange.StartEpochSec, timeRange.EndEpochSec, 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(metricQueryRequest))
	assert.True(t, metricQueryRequest[0].MetricsQuery.GroupByMetricName)

	/*
		Expected Results:
		This query will return the same results as the simple metric query for each metric name.

		So the expected results are:
		testmetric0: {25, 50, 60, 70}
	*/

	expectedResults := map[string][]float64{
		"testmetric0": {25, 50, 60, 70},
	}

	res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, 0)
	assert.NotNil(t, res)
	assert.Equal(t, 1, len(res.Results))

	for seriesId, seriesDp := range res.Results {
		assert.NotNil(t, seriesDp)
		assert.Greater(t, len(seriesDp), 0)

		mName := mresults.ExtractMetricNameFromGroupID(seriesId)
		assert.NotNil(t, mName)

		seriesDpValues := make([]float64, 0)
		for _, dp := range seriesDp {
			seriesDpValues = append(seriesDpValues, dp)
		}
		sort.Slice(seriesDpValues, func(i, j int) bool {
			return seriesDpValues[i] < seriesDpValues[j]
		})

		assert.EqualValues(t, expectedResults[mName], seriesDpValues)
	}
}

func Test_SimpleMetricQuery_Regex_on_MetricName_Plus_Filter_GroupByMetric_v2(t *testing.T) {
	defer cleanUp(t)

	startTimestamp := dataStartTimestamp
	allTimeSeries, _, _, _ := GetTestMetricsData(startTimestamp)

	err := initTestConfig()
	assert.Nil(t, err)

	err = ingestTestMetricsData(allTimeSeries)
	assert.Nil(t, err)

	mSegs, err := rotateMetricsDataAndClearSegStore(true)
	assert.Nil(t, err)
	assert.Greater(t, len(mSegs), 0)

	err = initializeMetricsMetaData()
	assert.Nil(t, err)

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(startTimestamp),
		EndEpochSec:   uint32(startTimestamp + 4600),
	}

	intervalSeconds, err := mresults.CalculateInterval(timeRange.EndEpochSec - timeRange.StartEpochSec)
	assert.Nil(t, err)
	assert.Equal(t, uint32(20), intervalSeconds)

	query := `avg ({__name__=~"testmetric.*", type="solid"}) by (__name__)`
	metricQueryRequest, _, _, err := promql.ConvertPromQLToMetricsQuery(query, timeRange.StartEpochSec, timeRange.EndEpochSec, 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(metricQueryRequest))
	assert.True(t, metricQueryRequest[0].MetricsQuery.GroupByMetricName)

	/*
		Expected Results:
		This query will return the same results as the simple metric query for each metric name.

		So the expected results are:
		testmetric0: {25, 50, 60, 70}
		testmetric1: {50, 90, 100, 110}
		testmetric2: {75, 130, 140, 150}
	*/

	expectedResults := map[string][]float64{
		"testmetric0": {25, 50, 60, 70},
		"testmetric1": {50, 90, 100, 110},
		"testmetric2": {75, 130, 140, 150},
	}

	res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, 0)
	assert.NotNil(t, res)
	assert.Equal(t, 3, len(res.Results))

	for seriesId, seriesDp := range res.Results {
		assert.NotNil(t, seriesDp)
		assert.Greater(t, len(seriesDp), 0)

		mName := mresults.ExtractMetricNameFromGroupID(seriesId)
		assert.NotNil(t, mName)

		seriesDpValues := make([]float64, 0)
		for _, dp := range seriesDp {
			seriesDpValues = append(seriesDpValues, dp)
		}
		sort.Slice(seriesDpValues, func(i, j int) bool {
			return seriesDpValues[i] < seriesDpValues[j]
		})

		assert.EqualValues(t, expectedResults[mName], seriesDpValues)
	}
}

func Test_SimpleMetricQuery_Regex_on_MetricName_Plus_Filter_GroupByMetric_v3(t *testing.T) {
	defer cleanUp(t)

	startTimestamp := dataStartTimestamp
	allTimeSeries, _, _, _ := GetTestMetricsData(startTimestamp)

	err := initTestConfig()
	assert.Nil(t, err)

	err = ingestTestMetricsData(allTimeSeries)
	assert.Nil(t, err)

	mSegs, err := rotateMetricsDataAndClearSegStore(true)
	assert.Nil(t, err)
	assert.Greater(t, len(mSegs), 0)

	err = initializeMetricsMetaData()
	assert.Nil(t, err)

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(startTimestamp),
		EndEpochSec:   uint32(startTimestamp + 4600),
	}

	intervalSeconds, err := mresults.CalculateInterval(timeRange.EndEpochSec - timeRange.StartEpochSec)
	assert.Nil(t, err)
	assert.Equal(t, uint32(20), intervalSeconds)

	query := `avg ({__name__=~"testmetric.*", radius="10"}) by (__name__)`
	metricQueryRequest, _, _, err := promql.ConvertPromQLToMetricsQuery(query, timeRange.StartEpochSec, timeRange.EndEpochSec, 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(metricQueryRequest))
	assert.True(t, metricQueryRequest[0].MetricsQuery.GroupByMetricName)

	/*
		Expected Results:
		This query will return just one metric name testmetric0 and datapoint value is 40. As only one series satisfies the filter condition.

		So the expected results are:
		testmetric0: {40}
	*/

	expectedResults := []float64{40}

	res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, 0)
	assert.NotNil(t, res)
	assert.Equal(t, 1, len(res.Results))

	for seriesId, seriesDp := range res.Results {
		assert.NotNil(t, seriesDp)
		assert.Greater(t, len(seriesDp), 0)

		assert.True(t, strings.Contains(seriesId, "testmetric0{"))
		assert.True(t, strings.Contains(seriesId, "radius:10"))

		seriesDpValues := make([]float64, 0)
		for _, dp := range seriesDp {
			seriesDpValues = append(seriesDpValues, dp)
		}
		sort.Slice(seriesDpValues, func(i, j int) bool {
			return seriesDpValues[i] < seriesDpValues[j]
		})

		assert.EqualValues(t, expectedResults, seriesDpValues)
	}
}

func Test_SimpleMetricQuery_Regex_on_MetricName_Plus_Filter_GroupByTag_v1(t *testing.T) {
	defer cleanUp(t)

	startTimestamp := dataStartTimestamp
	allTimeSeries, _, _, _ := GetTestMetricsData(startTimestamp)

	err := initTestConfig()
	assert.Nil(t, err)

	err = ingestTestMetricsData(allTimeSeries)
	assert.Nil(t, err)

	mSegs, err := rotateMetricsDataAndClearSegStore(true)
	assert.Nil(t, err)
	assert.Greater(t, len(mSegs), 0)

	err = initializeMetricsMetaData()
	assert.Nil(t, err)

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(startTimestamp),
		EndEpochSec:   uint32(startTimestamp + 4600),
	}

	intervalSeconds, err := mresults.CalculateInterval(timeRange.EndEpochSec - timeRange.StartEpochSec)
	assert.Nil(t, err)
	assert.Equal(t, uint32(20), intervalSeconds)

	query := `avg ({__name__=~"testmetric.*"}) by (color, shape)`
	metricQueryRequest, _, _, err := promql.ConvertPromQLToMetricsQuery(query, timeRange.StartEpochSec, timeRange.EndEpochSec, 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(metricQueryRequest))
	assert.False(t, metricQueryRequest[0].MetricsQuery.GroupByMetricName)

	/*
		Expected Results:
		This query will return the same results as the simple metric query for each metric name.

		So the expected results are:
		testmetric0: {25, 50, 60, 70}
		testmetric1: {50, 90, 100, 110}
		testmetric2: {75, 130, 140, 150}

	*/

	expectedResults := map[string][]float64{
		"red":   {25, 50, 60, 70},
		"blue":  {50, 90, 100, 110},
		"green": {75, 130, 140, 150},
	}

	groupByKeys := []string{"color", "shape"}

	res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, 0)
	assert.NotNil(t, res)
	assert.Equal(t, 3, len(res.Results))

	for seriesId, seriesDp := range res.Results {
		assert.NotNil(t, seriesDp)
		assert.Greater(t, len(seriesDp), 0)

		mName := mresults.ExtractMetricNameFromGroupID(seriesId)
		assert.NotNil(t, mName)
		assert.Equal(t, "*", mName)

		assert.True(t, strings.Contains(seriesId, "color:"))
		assert.True(t, strings.Contains(seriesId, "shape:"))

		keyValueSet := mresults.ExtractGroupByFieldsFromSeriesId(seriesId, groupByKeys)
		assert.NotNil(t, keyValueSet)
		assert.Equal(t, 2, len(keyValueSet))

		colorKeyVal := mresults.ExtractGroupByFieldsFromSeriesId(seriesId, []string{"color"})
		assert.NotNil(t, colorKeyVal)
		assert.Equal(t, 1, len(colorKeyVal))

		colorVal := strings.Split(colorKeyVal[0], ":")[1]

		seriesDpValues := make([]float64, 0)
		for _, dp := range seriesDp {
			seriesDpValues = append(seriesDpValues, dp)
		}
		sort.Slice(seriesDpValues, func(i, j int) bool {
			return seriesDpValues[i] < seriesDpValues[j]
		})

		assert.EqualValues(t, expectedResults[colorVal], seriesDpValues)
	}
}

func Test_SimpleMetricQuery_Regex_on_MetricName_Plus_Filter_GroupByMetric_plus_GroupByTag_v1(t *testing.T) {
	defer cleanUp(t)

	startTimestamp := dataStartTimestamp
	allTimeSeries, _, _, _ := GetTestMetricsData(startTimestamp)

	err := initTestConfig()
	assert.Nil(t, err)

	err = ingestTestMetricsData(allTimeSeries)
	assert.Nil(t, err)

	mSegs, err := rotateMetricsDataAndClearSegStore(true)
	assert.Nil(t, err)
	assert.Greater(t, len(mSegs), 0)

	err = initializeMetricsMetaData()
	assert.Nil(t, err)

	timeRange := &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(startTimestamp),
		EndEpochSec:   uint32(startTimestamp + 4600),
	}

	intervalSeconds, err := mresults.CalculateInterval(timeRange.EndEpochSec - timeRange.StartEpochSec)
	assert.Nil(t, err)
	assert.Equal(t, uint32(20), intervalSeconds)

	query := `avg ({__name__=~"testmetric.*", type="solid"}) by (__name__, type)`
	metricQueryRequest, _, _, err := promql.ConvertPromQLToMetricsQuery(query, timeRange.StartEpochSec, timeRange.EndEpochSec, 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(metricQueryRequest))
	assert.True(t, metricQueryRequest[0].MetricsQuery.GroupByMetricName)

	/*
		Expected Results:
		This query will return the same results as the simple metric query for each metric name.

		So the expected results are:
		testmetric0: {25, 50, 60, 70}
		testmetric1: {50, 90, 100, 110}
		testmetric2: {75, 130, 140, 150}

	*/

	expectedResults := map[string][]float64{
		"testmetric0": {25, 50, 60, 70},
		"testmetric1": {50, 90, 100, 110},
		"testmetric2": {75, 130, 140, 150},
	}

	groupByKeys := []string{"type"}

	res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, 0)
	assert.NotNil(t, res)
	assert.Equal(t, 3, len(res.Results))

	for seriesId, seriesDp := range res.Results {
		assert.NotNil(t, seriesDp)
		assert.Greater(t, len(seriesDp), 0)

		mName := mresults.ExtractMetricNameFromGroupID(seriesId)
		assert.NotNil(t, mName)

		shapeKeyVal := mresults.ExtractGroupByFieldsFromSeriesId(seriesId, groupByKeys)
		assert.NotNil(t, shapeKeyVal)
		assert.Equal(t, 1, len(shapeKeyVal))

		shapeVal := strings.Split(shapeKeyVal[0], ":")[1]
		assert.Equal(t, "solid", shapeVal)

		seriesDpValues := make([]float64, 0)
		for _, dp := range seriesDp {
			seriesDpValues = append(seriesDpValues, dp)
		}
		sort.Slice(seriesDpValues, func(i, j int) bool {
			return seriesDpValues[i] < seriesDpValues[j]
		})

		assert.EqualValues(t, expectedResults[mName], seriesDpValues)
	}
}
