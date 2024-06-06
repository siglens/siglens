package promql

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/results/mresults"
	"github.com/stretchr/testify/assert"
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

	start, end, queries, formulas, _, err := parseMetricTimeSeriesRequest([]byte(validJSON))
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
	start, end, queries, formulas, _, err = parseMetricTimeSeriesRequest([]byte(invalidJSON))
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
	start, end, queries, formulas, _, err = parseMetricTimeSeriesRequest([]byte(invalidJSON))
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
	start, end, queries, formulas, _, err = parseMetricTimeSeriesRequest([]byte(invalidJSON))
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
	start, end, queries, formulas, _, err = parseMetricTimeSeriesRequest([]byte(invalidJSON))
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
	start, end, queries, formulas, _, err = parseMetricTimeSeriesRequest([]byte(invalidJSON))
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
	start, end, queries, formulas, _, err = parseMetricTimeSeriesRequest([]byte(invalidJSON))
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

func Test_ProcessQueryArithmeticAndLogical_v1(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := uint64(0)

	// Test: query1 - query2 * query3 + query2
	query := "node_cpu_seconds_total - node_memory_MemTotal_bytes * node_disk_reads_completed_total + node_memory_MemTotal_bytes"

	mQueryReqs, _, queryArithmetic, err := convertPromQLToMetricsQuery(query, startTime, endTime, myId)
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

	mResult := segment.ProcessQueryArithmeticAndLogical(queryArithmetic, queryResultsMap)
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
