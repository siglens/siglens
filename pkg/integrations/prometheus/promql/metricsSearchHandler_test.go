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

package promql

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/results/mresults"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

func Test_parseMetricTimeSeriesRequest(t *testing.T) {

	// Case 1: Valid JSON input
	validJSON := `{
		"start": 1625248200,
		"end": 1625248300,
		"queries": [
			{"name": "query1", "query": "(testmetric0)", "qlType": "PromQL"},
			{"name": "query2", "query": "(testmetric1)", "qlType": "PromQL"}
		],
		"formulas": [
			{"formula": "query1+query2"}
		]
	}`

	start, end, queries, formulas, _, _, err := ParseMetricTimeSeriesRequest([]byte(validJSON))
	assert.NoError(t, err)
	assert.Equal(t, uint32(1625248200), start)
	assert.Equal(t, uint32(1625248300), end)
	assert.Len(t, queries, 2)
	assert.Len(t, formulas, 1)
	assert.Equal(t, "query1", queries[0]["name"])
	assert.Equal(t, "(testmetric0)", queries[0]["query"])
	assert.Equal(t, "PromQL", queries[0]["qlType"])
	assert.Equal(t, "query2", queries[1]["name"])
	assert.Equal(t, "(testmetric1)", queries[1]["query"])
	assert.Equal(t, "PromQL", queries[1]["qlType"])
	assert.Equal(t, "query1+query2", formulas[0]["formula"])

	// Case 2: Invalid JSON input (missing 'start')
	invalidJSON := `{
		"end": 1625248300,
		"queries": [
			{"name": "query1", "query": "SELECT * FROM table", "qlType": "SQL"},
			{"name": "query2", "query": "SELECT * FROM table", "qlType": "SQL"}
		],
		"formulas": [
			{"formula": "formula1"},
			{"formula": "formula2"}
		]
	}`
	start, end, queries, formulas, _, _, err = ParseMetricTimeSeriesRequest([]byte(invalidJSON))
	assert.Error(t, err)
	assert.NotNil(t, start)
	assert.NotNil(t, end)
	assert.NotNil(t, queries)
	assert.NotNil(t, formulas)

	// Case 3: Invalid JSON input (missing 'end')
	invalidJSON = `{
		"start": 1625248200,
		"queries": [
			{"name": "query1", "query": "SELECT * FROM table", "qlType": "SQL"},
			{"name": "query2", "query": "SELECT * FROM table", "qlType": "SQL"}
		],
		"formulas": [
			{"formula": "formula1"},
			{"formula": "formula2"}
		]
	}`
	start, end, queries, formulas, _, _, err = ParseMetricTimeSeriesRequest([]byte(invalidJSON))
	assert.Error(t, err)
	assert.NotNil(t, start)
	assert.NotNil(t, end)
	assert.NotNil(t, queries)
	assert.NotNil(t, formulas)

	// Case 4: Invalid JSON input (missing 'queries')
	invalidJSON = `{
		"start": 1625248200,
		"end": 1625248300,
		"formulas": [
			{"formula": "formula1"},
			{"formula": "formula2"}
		]
	}`
	start, end, queries, formulas, _, _, err = ParseMetricTimeSeriesRequest([]byte(invalidJSON))
	assert.Error(t, err)
	assert.NotNil(t, start)
	assert.NotNil(t, end)
	assert.NotNil(t, queries)
	assert.NotNil(t, formulas)

	// Case 5: Invalid JSON input (missing 'formulas')
	invalidJSON = `{
		"start": 1625248200,
		"end": 1625248300,
		"queries": [
			{"name": "query1", "query": "SELECT * FROM table", "qlType": "SQL"},
			{"name": "query2", "query": "SELECT * FROM table", "qlType": "SQL"}
		]
	}`
	start, end, queries, formulas, _, _, err = ParseMetricTimeSeriesRequest([]byte(invalidJSON))
	assert.Error(t, err)
	assert.NotNil(t, start)
	assert.NotNil(t, end)
	assert.NotNil(t, queries)
	assert.NotNil(t, formulas)

	// Case 6: Invalid JSON input (malformed 'queries')
	invalidJSON = `{
		"start": 1625248200,
		"end": 1625248300,
		"queries": [
			{"name": "query1", "query": "SELECT * FROM table"},
			{"name": "query2", "query": "SELECT * FROM table", "qlType": "SQL"}
		],
		"formulas": [
			{"formula": "formula1"},
			{"formula": "formula2"}
		]
	}`
	start, end, queries, formulas, _, _, err = ParseMetricTimeSeriesRequest([]byte(invalidJSON))
	assert.Error(t, err)
	assert.NotNil(t, start)
	assert.NotNil(t, end)
	assert.NotNil(t, queries)
	assert.NotNil(t, formulas)

	// Case 7: Invalid JSON input (malformed 'formulas')
	invalidJSON = `{
		"start": 1625248200,
		"end": 1625248300,
		"queries": [
			{"name": "query1", "query": "SELECT * FROM table", "qlType": "SQL"},
			{"name": "query2", "query": "SELECT * FROM table", "qlType": "SQL"}
		],
		"formulas": [
			{"formula1": "formula1"},
			{"formula2": "formula2"}
		]
	}`
	start, end, queries, formulas, _, _, err = ParseMetricTimeSeriesRequest([]byte(invalidJSON))
	assert.Error(t, err)
	assert.NotNil(t, start)
	assert.NotNil(t, end)
	assert.NotNil(t, queries)
	assert.NotNil(t, formulas)
}

func Test_metricsFuncPromptJson(t *testing.T) {
	type Function struct {
		Fn              string `json:"fn"`
		Name            string `json:"name"`
		Desc            string `json:"desc"`
		Eg              string `json:"eg"`
		IsTimeRangeFunc bool   `json:"isTimeRangeFunc"`
	}

	attrMap := map[string]string{
		"Fn":              "string",
		"Name":            "string",
		"Desc":            "string",
		"Eg":              "string",
		"IsTimeRangeFunc": "bool",
	}

	var functions []Function
	err := json.Unmarshal([]byte(metricFunctions), &functions)

	assert.Nil(t, err, "The metrics function prompt should be valid JSON: %v", err)

	for _, function := range functions {
		val := reflect.ValueOf(function)
		for i := 0; i < val.NumField(); i++ {
			field := val.Type().Field(i)

			fieldType, exists := attrMap[field.Name]
			assert.True(t, exists, "Missing Field: %s", field.Name)
			assert.Equal(t, fieldType, field.Type.Kind().String())
		}
	}
}

func Test_PromQLBuildInfoJson(t *testing.T) {
	var js interface{}
	err := json.Unmarshal([]byte(PromQLBuildInfo), &js)
	assert.Nil(t, err, "The PromQL build info should be valid JSON: %v", err)
}

func Test_buildMetricQueryFromFormulaAndQueries(t *testing.T) {
	testCases := []struct {
		formula  string
		queries  map[string]string
		expected string
	}{
		{
			formula: "a + b",
			queries: map[string]string{
				"a": `avg by (car_type) (testmetric0{car_type="Passenger car heavy"})`,
				"b": `avg by (car_type) (testmetric0{car_type="Passenger car heavy"})`,
			},
			expected: `avg by (car_type) (testmetric0{car_type="Passenger car heavy"}) + avg by (car_type) (testmetric0{car_type="Passenger car heavy"})`,
		},
		{
			formula: "a - b",
			queries: map[string]string{
				"a": `sum by (region) (testmetric1{region="North"})`,
				"b": `sum by (region) (testmetric1{region="South"})`,
			},
			expected: `sum by (region) (testmetric1{region="North"}) - sum by (region) (testmetric1{region="South"})`,
		},
		{
			formula: "a + a",
			queries: map[string]string{
				"a": `sum by (region) (testmetric1{region="North"})`,
			},
			expected: `sum by (region) (testmetric1{region="North"}) + sum by (region) (testmetric1{region="North"})`,
		},
		{
			formula: "a + b * a",
			queries: map[string]string{
				"a": `sum by (region) (testmetric1{region="North"})`,
				"b": `sum by (region) (testmetric1{region="South"})`,
			},
			expected: `sum by (region) (testmetric1{region="North"}) + sum by (region) (testmetric1{region="South"}) * sum by (region) (testmetric1{region="North"})`,
		},
		{
			formula:  "a + b",
			queries:  map[string]string{},
			expected: "a + b",
		},
		{
			formula: "a / b",
			queries: map[string]string{
				"a": `sum by (region) (testmetric1{region="North"})`,
				"b": `sum by (region) (testmetric1{region="South"})`,
			},
			expected: `sum by (region) (testmetric1{region="North"}) / sum by (region) (testmetric1{region="South"})`,
		},
		{
			formula: "a + b",
			queries: map[string]string{
				"a": `sum by (region) (testmetric1{region="North"})`,
				"b": `avg by (region) (rate(testmetric1{region="North"}[5m])`,
			},
			expected: `sum by (region) (testmetric1{region="North"}) + avg by (region) (rate(testmetric1{region="North"}[5m])`,
		},
		{
			formula: "(a * b) / (c - d)",
			queries: map[string]string{
				"a": `avg by (type) (metric1)`,
				"b": `max by (type) (metric2)`,
				"c": `min by (type) (metric3)`,
				"d": `sum by (type) (metric4)`,
			},
			expected: `(avg by (type) (metric1) * max by (type) (metric2)) / (min by (type) (metric3) - sum by (type) (metric4))`,
		},
		{
			formula: "a + c + b",
			queries: map[string]string{
				"a": `metric1`,
				"c": `metric2`,
				"b": `metric3`,
			},
			expected: `metric1 + metric2 + metric3`,
		},
		{
			formula: "a + b",
			queries: map[string]string{
				"a": `avg(rate(metric1[5m]))`,
				"b": `sum by (type) (metric2{type="test"})`,
			},
			expected: `avg(rate(metric1[5m])) + sum by (type) (metric2{type="test"})`,
		},
		{
			formula: "atan(abs(ceil(123 * a + b))) + abs(ceil(c))",
			queries: map[string]string{
				"a": `ceil(avg by (type) (metric1))`,
				"b": `max by (type) (metric2)`,
				"c": `min by (type) (metric3)`,
			},
			expected: `atan(abs(ceil(123 * ceil(avg by (type) (metric1)) + max by (type) (metric2)))) + abs(ceil(min by (type) (metric3)))`,
		},
	}

	for i, tc := range testCases {
		t.Logf("Test case %d", i)
		actual, err := buildMetricQueryFromFormulaAndQueries(tc.formula, tc.queries)
		if i == 4 {
			assert.NotNil(t, err)
			continue
		}
		assert.Nil(t, err)
		assert.Equal(t, tc.expected, actual)
	}
}

// One to One
// Multiple Queries, nested operations
func Test_ProcessQueryArithmeticAndLogical_v1(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := int64(0)

	// Test: query1 - query2 * query3 + query2
	query := "node_cpu_seconds_total - node_memory_MemTotal_bytes * node_disk_reads_completed_total + node_memory_MemTotal_bytes"

	mQueryReqs, _, queryArithmetic, err := ConvertPromQLToMetricsQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(mQueryReqs))
	assert.Equal(t, 1, len(queryArithmetic))

	queryHash1 := xxhash.Sum64String("node_cpu_seconds_total")
	queryHash2 := xxhash.Sum64String("node_memory_MemTotal_bytes")
	queryHash3 := xxhash.Sum64String("node_disk_reads_completed_total")

	queryHashes := []uint64{queryHash1, queryHash2, queryHash3, queryHash2}
	sort.Slice(queryHashes, func(i, j int) bool {
		return queryHashes[i] < queryHashes[j]
	})

	actualQueryHashes := []uint64{}

	for _, mQueryReq := range mQueryReqs {
		actualQueryHashes = append(actualQueryHashes, mQueryReq.MetricsQuery.QueryHash)
	}

	sort.Slice(actualQueryHashes, func(i, j int) bool {
		return actualQueryHashes[i] < actualQueryHashes[j]
	})

	assert.Equal(t, queryHashes, actualQueryHashes)

	queryResMap1 := make(map[string]map[uint32]float64)
	queryResMap1["node_cpu_seconds_total{tk1:v1,tk2:v2"] = map[uint32]float64{
		1: 100.0,
		2: 200.0,
		3: 300.0,
		4: 400.0,
		5: 500.0,
	}
	queryResult1 := &mresults.MetricsResult{
		MetricName: "node_cpu_seconds_total",
		Results:    queryResMap1,
	}

	queryResMap2 := make(map[string]map[uint32]float64)
	queryResMap2["node_memory_MemTotal_bytes{tk1:v1,tk2:v2"] = map[uint32]float64{
		1: 1000.0,
		2: 2000.0,
		3: 3000.0,
		4: 4000.0,
		5: 5000.0,
	}
	queryResult2 := &mresults.MetricsResult{
		MetricName: "node_memory_MemTotal_bytes",
		Results:    queryResMap2,
	}

	queryResMap3 := make(map[string]map[uint32]float64)
	queryResMap3["node_disk_reads_completed_total{tk1:v1,tk2:v2"] = map[uint32]float64{
		1: 10.0,
		2: 20.0,
		3: 30.0,
		4: 40.0,
		5: 50.0,
	}
	queryResult3 := &mresults.MetricsResult{
		MetricName: "node_disk_reads_completed_total",
		Results:    queryResMap3,
	}

	queryResultsMap := make(map[uint64]*mresults.MetricsResult)
	queryResultsMap[queryHash1] = queryResult1
	queryResultsMap[queryHash2] = queryResult2
	queryResultsMap[queryHash3] = queryResult3

	mResult := segment.ProcessQueryArithmeticAndLogical(queryArithmetic, queryResultsMap, false)
	assert.NotNil(t, mResult)
	assert.Equal(t, 1, len(mResult.Results))

	expectedResults := map[uint32]float64{
		1: 100.0 - 1000.0*10.0 + 1000.0,
		2: 200.0 - 2000.0*20.0 + 2000.0,
		3: 300.0 - 3000.0*30.0 + 3000.0,
		4: 400.0 - 4000.0*40.0 + 4000.0,
		5: 500.0 - 5000.0*50.0 + 5000.0,
	}

	for _, tsMap := range mResult.Results {
		for ts, val := range tsMap {
			assert.Equal(t, expectedResults[ts], val, "At timestamp %d", ts)
		}
	}
}

// One to One
// two queries, no nested operations
func Test_ProcessQueryArithmeticAndLogical_TimeSeries_v1(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := int64(0)

	// Test: query1 + query2
	query := "node_cpu_seconds_total + node_memory_MemTotal_bytes"

	mQueryReqs, _, queryArithmetic, err := ConvertPromQLToMetricsQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(mQueryReqs))
	assert.Equal(t, 1, len(queryArithmetic))

	queryHash1 := xxhash.Sum64String("node_cpu_seconds_total")
	queryHash2 := xxhash.Sum64String("node_memory_MemTotal_bytes")

	queryHashes := []uint64{queryHash1, queryHash2}
	sort.Slice(queryHashes, func(i, j int) bool {
		return queryHashes[i] < queryHashes[j]
	})

	actualQueryHashes := []uint64{}

	for _, mQueryReq := range mQueryReqs {
		actualQueryHashes = append(actualQueryHashes, mQueryReq.MetricsQuery.QueryHash)
	}

	sort.Slice(actualQueryHashes, func(i, j int) bool {
		return actualQueryHashes[i] < actualQueryHashes[j]
	})

	assert.Equal(t, queryHashes, actualQueryHashes)

	queryResMap1 := make(map[string]map[uint32]float64)
	queryResMap1["node_cpu_seconds_total{tk1:v1,tk2:v2"] = map[uint32]float64{
		1: 100.0,
		2: 200.0,
		3: 300.0,
		4: 400.0,
		5: 500.0,
	}
	queryResult1 := &mresults.MetricsResult{
		MetricName: "node_cpu_seconds_total",
		Results:    queryResMap1,
	}

	queryResMap2 := make(map[string]map[uint32]float64)
	queryResMap2["node_memory_MemTotal_bytes{tk3:v1,tk4:v2"] = map[uint32]float64{
		1: 1000.0,
		2: 2000.0,
		3: 3000.0,
		4: 4000.0,
		5: 5000.0,
	}
	queryResult2 := &mresults.MetricsResult{
		MetricName: "node_memory_MemTotal_bytes",
		Results:    queryResMap2,
	}

	queryResultsMap := make(map[uint64]*mresults.MetricsResult)
	queryResultsMap[queryHash1] = queryResult1
	queryResultsMap[queryHash2] = queryResult2

	mResult := segment.ProcessQueryArithmeticAndLogical(queryArithmetic, queryResultsMap, true)
	assert.NotNil(t, mResult)
	assert.Equal(t, 1, len(mResult.Results))

	expectedResults := map[uint32]float64{
		1: 100.0 + 1000.0,
		2: 200.0 + 2000.0,
		3: 300.0 + 3000.0,
		4: 400.0 + 4000.0,
		5: 500.0 + 5000.0,
	}

	for _, tsMap := range mResult.Results {
		for ts, val := range tsMap {
			assert.Equal(t, expectedResults[ts], val, "At timestamp %d", ts)
		}
	}
}

// One to One
// Multiple Queries, nested operations
func Test_ProcessQueryArithmeticAndLogical_TimeSeries_v2(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := int64(0)

	// Test: query1 - query2 * query3 + query2
	query := "node_cpu_seconds_total - node_memory_MemTotal_bytes * node_disk_reads_completed_total + node_memory_MemTotal_bytes"

	mQueryReqs, _, queryArithmetic, err := ConvertPromQLToMetricsQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(mQueryReqs))
	assert.Equal(t, 1, len(queryArithmetic))

	queryHash1 := xxhash.Sum64String("node_cpu_seconds_total")
	queryHash2 := xxhash.Sum64String("node_memory_MemTotal_bytes")
	queryHash3 := xxhash.Sum64String("node_disk_reads_completed_total")

	queryHashes := []uint64{queryHash1, queryHash2, queryHash3, queryHash2}
	sort.Slice(queryHashes, func(i, j int) bool {
		return queryHashes[i] < queryHashes[j]
	})

	actualQueryHashes := []uint64{}

	for _, mQueryReq := range mQueryReqs {
		actualQueryHashes = append(actualQueryHashes, mQueryReq.MetricsQuery.QueryHash)
	}

	sort.Slice(actualQueryHashes, func(i, j int) bool {
		return actualQueryHashes[i] < actualQueryHashes[j]
	})

	assert.Equal(t, queryHashes, actualQueryHashes)

	queryResMap1 := make(map[string]map[uint32]float64)
	queryResMap1["node_cpu_seconds_total{tk1:v1,tk2:v2"] = map[uint32]float64{
		1: 100.0,
		2: 200.0,
		3: 300.0,
		4: 400.0,
		5: 500.0,
	}
	queryResult1 := &mresults.MetricsResult{
		MetricName: "node_cpu_seconds_total",
		Results:    queryResMap1,
	}

	queryResMap2 := make(map[string]map[uint32]float64)
	queryResMap2["node_memory_MemTotal_bytes{tk3:v1,tk4:v2"] = map[uint32]float64{
		1: 1000.0,
		2: 2000.0,
		3: 3000.0,
		4: 4000.0,
		5: 5000.0,
	}
	queryResult2 := &mresults.MetricsResult{
		MetricName: "node_memory_MemTotal_bytes",
		Results:    queryResMap2,
	}

	queryResMap3 := make(map[string]map[uint32]float64)
	queryResMap3["node_disk_reads_completed_total{tk5:v1,tk6:v2"] = map[uint32]float64{
		1: 10.0,
		2: 20.0,
		3: 30.0,
		4: 40.0,
		5: 50.0,
	}
	queryResult3 := &mresults.MetricsResult{
		MetricName: "node_disk_reads_completed_total",
		Results:    queryResMap3,
	}

	queryResultsMap := make(map[uint64]*mresults.MetricsResult)
	queryResultsMap[queryHash1] = queryResult1
	queryResultsMap[queryHash2] = queryResult2
	queryResultsMap[queryHash3] = queryResult3

	mResult := segment.ProcessQueryArithmeticAndLogical(queryArithmetic, queryResultsMap, true)
	assert.NotNil(t, mResult)
	assert.Equal(t, 1, len(mResult.Results))

	expectedResults := map[uint32]float64{
		1: 100.0 - 1000.0*10.0 + 1000.0,
		2: 200.0 - 2000.0*20.0 + 2000.0,
		3: 300.0 - 3000.0*30.0 + 3000.0,
		4: 400.0 - 4000.0*40.0 + 4000.0,
		5: 500.0 - 5000.0*50.0 + 5000.0,
	}

	for _, tsMap := range mResult.Results {
		for ts, val := range tsMap {
			assert.Equal(t, expectedResults[ts], val, "At timestamp %d", ts)
		}
	}
}

// One to Many
// two queries, no nested operations
func Test_ProcessQueryArithmeticAndLogical_TimeSeries_v3(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := int64(0)

	// Test: query1 + query2
	query := "node_cpu_seconds_total + node_memory_MemTotal_bytes"

	mQueryReqs, _, queryArithmetic, err := ConvertPromQLToMetricsQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(mQueryReqs))
	assert.Equal(t, 1, len(queryArithmetic))

	queryHash1 := xxhash.Sum64String("node_cpu_seconds_total")
	queryHash2 := xxhash.Sum64String("node_memory_MemTotal_bytes")

	queryHashes := []uint64{queryHash1, queryHash2}
	sort.Slice(queryHashes, func(i, j int) bool {
		return queryHashes[i] < queryHashes[j]
	})

	actualQueryHashes := []uint64{}

	for _, mQueryReq := range mQueryReqs {
		actualQueryHashes = append(actualQueryHashes, mQueryReq.MetricsQuery.QueryHash)
	}

	sort.Slice(actualQueryHashes, func(i, j int) bool {
		return actualQueryHashes[i] < actualQueryHashes[j]
	})

	assert.Equal(t, queryHashes, actualQueryHashes)

	queryResMap1 := make(map[string]map[uint32]float64)
	queryResMap1["node_cpu_seconds_total{tk1:v1,tk2:v2"] = map[uint32]float64{
		1: 100.0,
		2: 200.0,
		3: 300.0,
		4: 400.0,
		5: 500.0,
	}
	queryResMap1["node_cpu_seconds_total{tk3:v1,tk4:v2"] = map[uint32]float64{
		1: 1000.0,
		2: 2000.0,
		3: 3000.0,
		4: 4000.0,
		5: 5000.0,
	}
	queryResult1 := &mresults.MetricsResult{
		MetricName: "node_cpu_seconds_total",
		Results:    queryResMap1,
	}

	queryResMap2 := make(map[string]map[uint32]float64)
	queryResMap2["node_memory_MemTotal_bytes{tk4:v1,tk5:v2"] = map[uint32]float64{
		1: 1000.0,
		2: 2000.0,
		3: 3000.0,
		4: 4000.0,
		5: 5000.0,
	}
	queryResult2 := &mresults.MetricsResult{
		MetricName: "node_memory_MemTotal_bytes",
		Results:    queryResMap2,
	}

	queryResultsMap := make(map[uint64]*mresults.MetricsResult)
	queryResultsMap[queryHash1] = queryResult1
	queryResultsMap[queryHash2] = queryResult2

	mResult := segment.ProcessQueryArithmeticAndLogical(queryArithmetic, queryResultsMap, true)
	assert.NotNil(t, mResult)
	assert.Equal(t, 2, len(mResult.Results))

	expectedResults1 := map[uint32]float64{
		1: 100.0 + 1000.0,
		2: 200.0 + 2000.0,
		3: 300.0 + 3000.0,
		4: 400.0 + 4000.0,
		5: 500.0 + 5000.0,
	}

	expectedResults2 := map[uint32]float64{
		1: 1000.0 + 1000.0,
		2: 2000.0 + 2000.0,
		3: 3000.0 + 3000.0,
		4: 4000.0 + 4000.0,
		5: 5000.0 + 5000.0,
	}

	expectedResults := make(map[string]map[uint32]float64)
	expectedResults["node_cpu_seconds_total{tk1:v1,tk2:v2"] = expectedResults1
	expectedResults["node_cpu_seconds_total{tk3:v1,tk4:v2"] = expectedResults2

	for seriesId, tsMap := range mResult.Results {
		for ts, val := range tsMap {
			assert.Equal(t, expectedResults[seriesId][ts], val, "At timestamp %d", ts)
		}
	}
}

// One to Many
// Multiple Queries, nested operations
func Test_ProcessQueryArithmeticAndLogical_TimeSeries_v4(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := int64(0)

	// Test: query1 - query2 * query3 + query2
	query := "node_cpu_seconds_total - node_memory_MemTotal_bytes * node_disk_reads_completed_total + node_memory_MemTotal_bytes"

	mQueryReqs, _, queryArithmetic, err := ConvertPromQLToMetricsQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(mQueryReqs))
	assert.Equal(t, 1, len(queryArithmetic))

	queryHash1 := xxhash.Sum64String("node_cpu_seconds_total")
	queryHash2 := xxhash.Sum64String("node_memory_MemTotal_bytes")
	queryHash3 := xxhash.Sum64String("node_disk_reads_completed_total")

	queryHashes := []uint64{queryHash1, queryHash2, queryHash3, queryHash2}
	sort.Slice(queryHashes, func(i, j int) bool {
		return queryHashes[i] < queryHashes[j]
	})

	actualQueryHashes := []uint64{}

	for _, mQueryReq := range mQueryReqs {
		actualQueryHashes = append(actualQueryHashes, mQueryReq.MetricsQuery.QueryHash)
	}

	sort.Slice(actualQueryHashes, func(i, j int) bool {
		return actualQueryHashes[i] < actualQueryHashes[j]
	})

	assert.Equal(t, queryHashes, actualQueryHashes)

	queryResultsMap := getQueryResultMapForThreeTestQueries("node_cpu_seconds_total", "node_memory_MemTotal_bytes", "node_disk_reads_completed_total")

	mResult := segment.ProcessQueryArithmeticAndLogical(queryArithmetic, queryResultsMap, true)
	assert.NotNil(t, mResult)
	assert.Equal(t, 2, len(mResult.Results))

	expectedResults1 := map[uint32]float64{
		1: 100.0 - 1000.0*10.0 + 1000.0,
		2: 200.0 - 2000.0*20.0 + 2000.0,
		3: 300.0 - 3000.0*30.0 + 3000.0,
		4: 400.0 - 4000.0*40.0 + 4000.0,
		5: 500.0 - 5000.0*50.0 + 5000.0,
	}

	expectedResults2 := map[uint32]float64{
		1: 1000.0 - 1000.0*10.0 + 1000.0,
		2: 2000.0 - 2000.0*20.0 + 2000.0,
		3: 3000.0 - 3000.0*30.0 + 3000.0,
		4: 4000.0 - 4000.0*40.0 + 4000.0,
		5: 5000.0 - 5000.0*50.0 + 5000.0,
	}

	expectedResults := make(map[string]map[uint32]float64)
	expectedResults["node_cpu_seconds_total{tk1:v1,tk2:v2"] = expectedResults1
	expectedResults["node_cpu_seconds_total{tk3:v1,tk4:v2"] = expectedResults2

	for seriesId, tsMap := range mResult.Results {
		for ts, val := range tsMap {
			assert.Equal(t, expectedResults[seriesId][ts], val, "At timestamp %d", ts)
		}
	}
}

// Many to Many
func Test_ProcessQueryArithmeticAndLogical_TimeSeries_v5(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := int64(0)

	// Test: query1 - query2 * query3 + query2
	query := "node_cpu_seconds_total - node_memory_MemTotal_bytes * node_disk_reads_completed_total + node_memory_MemTotal_bytes"

	mQueryReqs, _, queryArithmetic, err := ConvertPromQLToMetricsQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(mQueryReqs))
	assert.Equal(t, 1, len(queryArithmetic))

	queryResMap1 := make(map[string]map[uint32]float64)
	queryResMap1["node_cpu_seconds_total{tk1:v1,tk2:v2"] = map[uint32]float64{
		1: 100.0,
		2: 200.0,
	}

	queryResMap1["node_cpu_seconds_total{tk3:v1,tk4:v2"] = map[uint32]float64{
		1: 1000.0,
		2: 2000.0,
	}

	queryResult1 := &mresults.MetricsResult{
		MetricName: "node_cpu_seconds_total",
		Results:    queryResMap1,
	}

	queryResMap2 := make(map[string]map[uint32]float64)
	queryResMap2["node_memory_MemTotal_bytes{tk1:v1,tk2:v2"] = map[uint32]float64{
		1: 1000.0,
		2: 2000.0,
	}
	queryResMap2["node_memory_MemTotal_bytes{tk4:v1,tk5:v2"] = map[uint32]float64{
		1: 1000.0,
		2: 2000.0,
	}
	queryResult2 := &mresults.MetricsResult{
		MetricName: "node_memory_MemTotal_bytes",
		Results:    queryResMap2,
	}

	queryResMap3 := make(map[string]map[uint32]float64)
	queryResMap3["node_disk_reads_completed_total{tk1:v1,tk2:v2"] = map[uint32]float64{
		1: 10.0,
		2: 20.0,
	}
	queryResMap3["node_disk_reads_completed_total{tk4:v1,tk5:v2"] = map[uint32]float64{
		1: 10.0,
		2: 20.0,
	}
	queryResult3 := &mresults.MetricsResult{
		MetricName: "node_disk_reads_completed_total",
		Results:    queryResMap3,
	}

	queryResultsMap := make(map[uint64]*mresults.MetricsResult)
	queryResultsMap[xxhash.Sum64String("node_cpu_seconds_total")] = queryResult1
	queryResultsMap[xxhash.Sum64String("node_memory_MemTotal_bytes")] = queryResult2
	queryResultsMap[xxhash.Sum64String("node_disk_reads_completed_total")] = queryResult3

	opLabelsDoNotNeedToMatch := false // Since there are multiple results that have more than one series, the labels need to match.
	mResult := segment.ProcessQueryArithmeticAndLogical(queryArithmetic, queryResultsMap, opLabelsDoNotNeedToMatch)
	assert.NotNil(t, mResult)
	assert.Equal(t, 1, len(mResult.Results))
	fmt.Println(mResult.Results)

	expectedResults := map[uint32]float64{
		1: 100.0 - (1000.0 * 10.0) + 1000.0,
		2: 200.0 - (2000.0 * 20.0) + 2000.0,
	}

	for _, tsMap := range mResult.Results {
		for ts, val := range tsMap {
			assert.Equal(t, expectedResults[ts], val, "At timestamp %d", ts)
		}
	}
}

func getQueryResultMapForThreeTestQueries(query1, query2, query3 string) map[uint64]*mresults.MetricsResult {
	queryHash1 := xxhash.Sum64String(query1)
	queryHash2 := xxhash.Sum64String(query2)
	queryHash3 := xxhash.Sum64String(query3)

	queryResMap1 := make(map[string]map[uint32]float64)
	queryResMap1[query1+"{tk1:v1,tk2:v2"] = map[uint32]float64{
		1: 100.0,
		2: 200.0,
		3: 300.0,
		4: 400.0,
		5: 500.0,
	}
	queryResMap1[query1+"{tk3:v1,tk4:v2"] = map[uint32]float64{
		1: 1000.0,
		2: 2000.0,
		3: 3000.0,
		4: 4000.0,
		5: 5000.0,
	}
	queryResult := &mresults.MetricsResult{
		MetricName: query1,
		Results:    queryResMap1,
	}

	queryResMap2 := make(map[string]map[uint32]float64)
	queryResMap2[query2+"{tk4:v1,tk5:v2"] = map[uint32]float64{
		1: 1000.0,
		2: 2000.0,
		3: 3000.0,
		4: 4000.0,
		5: 5000.0,
	}
	queryResult2 := &mresults.MetricsResult{
		MetricName: query2,
		Results:    queryResMap2,
	}

	queryResMap3 := make(map[string]map[uint32]float64)
	queryResMap3[query3+"{tk5:v1,tk6:v2"] = map[uint32]float64{
		1: 10.0,
		2: 20.0,
		3: 30.0,
		4: 40.0,
		5: 50.0,
	}
	queryResult3 := &mresults.MetricsResult{
		MetricName: query3,
		Results:    queryResMap3,
	}

	queryResultsMap := make(map[uint64]*mresults.MetricsResult)
	queryResultsMap[queryHash1] = queryResult
	queryResultsMap[queryHash2] = queryResult2
	queryResultsMap[queryHash3] = queryResult3

	return queryResultsMap
}

func Test_ProcessQueryArithmeticAndLogical_TimeSeries_Scalar_OP_v1(t *testing.T) {
	type MetricQueryTest struct {
		Query           string
		Hash            uint64
		QueryResult     mresults.MetricsResult
		ExpectedResults map[uint32]float64
		IsScalar        bool
	}

	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := int64(0)

	baseQuery := "node_cpu_seconds_total"
	baseQueryHash := xxhash.Sum64String(baseQuery)
	baseQueryResult := mresults.MetricsResult{
		MetricName: baseQuery,
		Results: map[string]map[uint32]float64{
			"node_cpu_seconds_total{tk1:v1,tk2:v2": {
				1: 100.0,
				2: 200.0,
				3: 300.0,
				4: 400.0,
				5: 500.0,
			},
		},
	}

	metricQueryTests := []MetricQueryTest{
		{
			Query:       "100 + 10 - 100 + " + baseQuery,
			Hash:        baseQueryHash,
			QueryResult: baseQueryResult,
			ExpectedResults: map[uint32]float64{
				1: 100.0 + 10.0 - 100.0 + 100.0,
				2: 100.0 + 10.0 - 100.0 + 200.0,
				3: 100.0 + 10.0 - 100.0 + 300.0,
				4: 100.0 + 10.0 - 100.0 + 400.0,
				5: 100.0 + 10.0 - 100.0 + 500.0,
			},
		},
		{
			Query:       "100 + 5 - 100 * " + baseQuery,
			Hash:        baseQueryHash,
			QueryResult: baseQueryResult,
			ExpectedResults: map[uint32]float64{
				1: 100.0 + 5.0 - 100.0*100.0,
				2: 100.0 + 5.0 - 100.0*200.0,
				3: 100.0 + 5.0 - 100.0*300.0,
				4: 100.0 + 5.0 - 100.0*400.0,
				5: 100.0 + 5.0 - 100.0*500.0,
			},
		},
		{
			Query:       baseQuery + " + 10 - 100 * 5",
			Hash:        baseQueryHash,
			QueryResult: baseQueryResult,
			ExpectedResults: map[uint32]float64{
				1: 100.0 + 10.0 - 100.0*5.0,
				2: 200.0 + 10.0 - 100.0*5.0,
				3: 300.0 + 10.0 - 100.0*5.0,
				4: 400.0 + 10.0 - 100.0*5.0,
				5: 500.0 + 10.0 - 100.0*5.0,
			},
		},
		{
			Query:       "100 + 5 - 100 * 5 * (" + baseQuery + " + 10 ) / 10",
			Hash:        baseQueryHash,
			QueryResult: baseQueryResult,
			ExpectedResults: map[uint32]float64{
				1: 100.0 + 5.0 - 100.0*5.0*(100.0+10.0)/10.0,
				2: 100.0 + 5.0 - 100.0*5.0*(200.0+10.0)/10.0,
				3: 100.0 + 5.0 - 100.0*5.0*(300.0+10.0)/10.0,
				4: 100.0 + 5.0 - 100.0*5.0*(400.0+10.0)/10.0,
				5: 100.0 + 5.0 - 100.0*5.0*(500.0+10.0)/10.0,
			},
		},
		{
			Query:       "1 + 2 - 3 * 4 * 5",
			Hash:        0,
			QueryResult: mresults.MetricsResult{},
			ExpectedResults: map[uint32]float64{
				endTime: 1.0 + 2.0 - 3.0*4.0*5.0,
			},
			IsScalar: true,
		},
		{
			Query:       "1 + 2 + 3 * 4",
			Hash:        0,
			QueryResult: mresults.MetricsResult{},
			ExpectedResults: map[uint32]float64{
				endTime: 1.0 + 2.0 + 3.0*4.0,
			},
			IsScalar: true,
		},
	}

	for i, queryTest := range metricQueryTests {

		mQueryReqs, _, queryArithmetic, err := ConvertPromQLToMetricsQuery(queryTest.Query, startTime, endTime, myId)
		assert.Nil(t, err)

		len_mQueryReqs := 1
		if queryTest.IsScalar {
			len_mQueryReqs = 0
		}

		assert.Equal(t, len_mQueryReqs, len(mQueryReqs), "At index: %v, mQueryReqs", i)
		assert.Equal(t, 1, len(queryArithmetic), "At index: %v, queryArithmetic", i)

		queryResultsMap := make(map[uint64]*mresults.MetricsResult)
		if !queryTest.IsScalar {
			queryResultsMap[queryTest.Hash] = &queryTest.QueryResult
		}

		mResult := segment.ProcessQueryArithmeticAndLogical(queryArithmetic, queryResultsMap, true)
		assert.NotNil(t, mResult, "At index: %v, mResult is nil", i)

		len_mResults := 1
		if queryTest.IsScalar {
			len_mResults = 0
		}

		assert.Equal(t, len_mResults, len(mResult.Results), "At index: %v, len(mResult.Results)", i)

		for _, tsMap := range mResult.Results {
			for ts, val := range tsMap {
				assert.Equal(t, queryTest.ExpectedResults[ts], val, "At index: %v, At timestamp %d", i, ts)
			}
		}
	}
}

func Test_ProcessQueryArithmeticAndLogical_TimeSeries_Scalar_OP_v2(t *testing.T) {

	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := int64(0)

	// Test: 100 + 10 - 50 + query1 - (100 - 10 + 110 + 2 * query2 + 10) * 10 * query3 + 100 + query2 - 50
	query := "100 + 10 - 50 + node_cpu_seconds_total - (100 - 10 + 110 + 2 * node_memory_MemTotal_bytes + 10) * 10 * node_disk_reads_completed_total + 100 + node_memory_MemTotal_bytes - 50"

	mQueryReqs, _, queryArithmetic, err := ConvertPromQLToMetricsQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(mQueryReqs))
	assert.Equal(t, 1, len(queryArithmetic))

	queryHash1 := xxhash.Sum64String("node_cpu_seconds_total")
	queryHash2 := xxhash.Sum64String("node_memory_MemTotal_bytes")
	queryHash3 := xxhash.Sum64String("node_disk_reads_completed_total")

	queryHashes := []uint64{queryHash1, queryHash2, queryHash3, queryHash2}
	sort.Slice(queryHashes, func(i, j int) bool {
		return queryHashes[i] < queryHashes[j]
	})

	actualQueryHashes := []uint64{}

	for _, mQueryReq := range mQueryReqs {
		actualQueryHashes = append(actualQueryHashes, mQueryReq.MetricsQuery.QueryHash)
	}

	sort.Slice(actualQueryHashes, func(i, j int) bool {
		return actualQueryHashes[i] < actualQueryHashes[j]
	})

	assert.Equal(t, queryHashes, actualQueryHashes)

	queryResultsMap := getQueryResultMapForThreeTestQueries("node_cpu_seconds_total", "node_memory_MemTotal_bytes", "node_disk_reads_completed_total")

	mResult := segment.ProcessQueryArithmeticAndLogical(queryArithmetic, queryResultsMap, true)
	assert.NotNil(t, mResult)
	assert.Equal(t, 2, len(mResult.Results))

	expectedResults1 := map[uint32]float64{
		1: 100.0 + 10.0 - 50.0 + 100.0 - (100.0-10.0+110.0+2.0*1000.0+10.0)*10.0*10.0 + 100.0 + 1000.0 - 50.0,
		2: 100.0 + 10.0 - 50.0 + 200.0 - (100.0-10.0+110.0+2.0*2000.0+10.0)*10.0*20.0 + 100.0 + 2000.0 - 50.0,
		3: 100.0 + 10.0 - 50.0 + 300.0 - (100.0-10.0+110.0+2.0*3000.0+10.0)*10.0*30.0 + 100.0 + 3000.0 - 50.0,
		4: 100.0 + 10.0 - 50.0 + 400.0 - (100.0-10.0+110.0+2.0*4000.0+10.0)*10.0*40.0 + 100.0 + 4000.0 - 50.0,
		5: 100.0 + 10.0 - 50.0 + 500.0 - (100.0-10.0+110.0+2.0*5000.0+10.0)*10.0*50.0 + 100.0 + 5000.0 - 50.0,
	}

	expectedResults2 := map[uint32]float64{
		1: 100.0 + 10.0 - 50.0 + 1000.0 - (100.0-10.0+110.0+2.0*1000.0+10.0)*10.0*10.0 + 100.0 + 1000.0 - 50.0,
		2: 100.0 + 10.0 - 50.0 + 2000.0 - (100.0-10.0+110.0+2.0*2000.0+10.0)*10.0*20.0 + 100.0 + 2000.0 - 50.0,
		3: 100.0 + 10.0 - 50.0 + 3000.0 - (100.0-10.0+110.0+2.0*3000.0+10.0)*10.0*30.0 + 100.0 + 3000.0 - 50.0,
		4: 100.0 + 10.0 - 50.0 + 4000.0 - (100.0-10.0+110.0+2.0*4000.0+10.0)*10.0*40.0 + 100.0 + 4000.0 - 50.0,
		5: 100.0 + 10.0 - 50.0 + 5000.0 - (100.0-10.0+110.0+2.0*5000.0+10.0)*10.0*50.0 + 100.0 + 5000.0 - 50.0,
	}

	expectedResults := make(map[string]map[uint32]float64)
	expectedResults["node_cpu_seconds_total{tk1:v1,tk2:v2"] = expectedResults1
	expectedResults["node_cpu_seconds_total{tk3:v1,tk4:v2"] = expectedResults2

	for seriesId, tsMap := range mResult.Results {
		for ts, val := range tsMap {
			assert.Equal(t, expectedResults[seriesId][ts], val, "At timestamp %d", ts)
		}
	}
}

func Test_processGetMetricSeriesCardinalityRequest(t *testing.T) {
	// Create a new fasthttp.RequestCtx for testing
	ctx := &fasthttp.RequestCtx{}

	// Set the request body with the input JSON
	inputJSON := []byte(`{
		"startEpoch": 1625248200,
		"endEpoch": 1625248300
	}`)
	ctx.Request.SetBody(inputJSON)

	// Call the function being tested
	ProcessGetMetricSeriesCardinalityRequest(ctx, 123)

	// Check the response status code
	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Errorf("Expected status code %d, but got %d", fasthttp.StatusOK, ctx.Response.StatusCode())
	}

	// Parse the response body
	var output struct {
		SeriesCardinality uint64 `json:"seriesCardinality"`
	}
	err := json.Unmarshal(ctx.Response.Body(), &output)
	assert.Nil(t, err)

	// Perform assertions on the output
	expectedCardinality := uint64(0)
	assert.Equal(t, expectedCardinality, output.SeriesCardinality)
}
