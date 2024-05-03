package promql

import (
	"testing"

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
