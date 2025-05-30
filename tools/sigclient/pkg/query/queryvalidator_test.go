// Copyright (c) 2021-2025 SigScalr, Inc.
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

package query

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_implementsQueryValidator(t *testing.T) {
	var _ queryValidator = &filterQueryValidator{}
	var _ queryValidator = &countQueryValidator{}
}

func Test_FilterQueryValidator(t *testing.T) {
	logs := []map[string]interface{}{
		{"city": "Boston", "timestamp": uint64(1), "age": 30},
		{"city": "Boston", "timestamp": uint64(2), "age": 36},
		{"city": "New York", "timestamp": uint64(3), "age": 22},
		{"city": "Boston", "timestamp": uint64(4), "age": 22},
		{"city": "Boston", "timestamp": uint64(5), "latency": 100},
	}
	bostonFilter, err := Filter("city", "Boston")
	assert.NoError(t, err)

	expectedJsonMatchingFirstTwo := []byte(`{
		"hits": {
			"totalMatched": {
				"value": 2,
				"relation": "eq"
			},
			"records": [
				{"city": "Boston", "timestamp": 2, "age": 36},
				{"city": "Boston", "timestamp": 1, "age": 30}
			]
		},
		"allColumns": ["city", "timestamp", "age"]
	}`)

	t.Run("FewerThanHeadMatch", func(t *testing.T) {
		head, startEpoch, endEpoch := 3, uint64(0), uint64(10)
		validator, err := NewFilterQueryValidator(bostonFilter, "", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs[:2])
		assert.NoError(t, validator.MatchesResult(expectedJsonMatchingFirstTwo))
	})

	t.Run("FilterByValue", func(t *testing.T) {
		head, startEpoch, endEpoch := 3, uint64(0), uint64(10)
		validator, err := NewFilterQueryValidator(bostonFilter, "", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs[:3]) // The third log has a different city, so it should be ignored.
		assert.NoError(t, validator.MatchesResult(expectedJsonMatchingFirstTwo))
	})

	t.Run("FilterByTime", func(t *testing.T) {
		head, startEpoch, endEpoch := 3, uint64(1), uint64(2)
		validator, err := NewFilterQueryValidator(bostonFilter, "", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs)
		assert.NoError(t, validator.MatchesResult(expectedJsonMatchingFirstTwo))
	})

	t.Run("MissingColumns", func(t *testing.T) {
		head, startEpoch, endEpoch := 3, uint64(0), uint64(10)
		validator, err := NewFilterQueryValidator(bostonFilter, "", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs[3:])

		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 2,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 5, "latency": 100},
					{"city": "Boston", "timestamp": 4, "age": 22}
				]
			},
			"allColumns": ["city", "timestamp", "age", "latency"]
		}`)))
	})

	t.Run("MoreThanHeadMatch", func(t *testing.T) {
		head, startEpoch, endEpoch := 3, uint64(0), uint64(10)
		validator, err := NewFilterQueryValidator(bostonFilter, "", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs)

		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 3,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 5, "latency": 100},
					{"city": "Boston", "timestamp": 4, "age": 22},
					{"city": "Boston", "timestamp": 2, "age": 36}
				]
			},
			"allColumns": ["city", "timestamp", "age", "latency"]
		}`)))
	})

	t.Run("BadResponse", func(t *testing.T) {
		head, startEpoch, endEpoch := 3, uint64(0), uint64(10)
		validator, err := NewFilterQueryValidator(bostonFilter, "", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs[:1])

		// For reference, this is a correct response.
		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 1,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 1, "age": 30}
				]
			},
			"allColumns": ["age", "city", "timestamp"]
		}`)))

		// Incorrect totalMatched.
		assert.Error(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 2,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 1, "age": 30}
				]
			},
			"allColumns": ["age", "city", "timestamp"]
		}`)))

		// Incorrect record.
		assert.Error(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 1,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 1, "age": 150}
				]
			},
			"allColumns": ["age", "city", "timestamp"]
		}`)))

		// Bad JSON.
		assert.Error(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 1,
					"relation": "eq"
		`)))
	})

	t.Run("MultipleValidSortings", func(t *testing.T) {
		logs := []map[string]interface{}{
			{"city": "Boston", "timestamp": uint64(1), "age": 30},
			{"city": "Boston", "timestamp": uint64(2), "age": 36},
			{"city": "Boston", "timestamp": uint64(2), "age": 22},
		}
		head, startEpoch, endEpoch := 3, uint64(0), uint64(10)
		validator, err := NewFilterQueryValidator(bostonFilter, "", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs)

		// This order is valid: logs[1], logs[2], logs[0]
		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 3,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 2, "age": 36},
					{"city": "Boston", "timestamp": 2, "age": 22},
					{"city": "Boston", "timestamp": 1, "age": 30}
				]
			},
			"allColumns": ["city", "timestamp", "age"]
		}`)))

		// This is also valid: logs[2], logs[1], logs[0]
		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 3,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 2, "age": 22},
					{"city": "Boston", "timestamp": 2, "age": 36},
					{"city": "Boston", "timestamp": 1, "age": 30}
				]
			},
			"allColumns": ["city", "timestamp", "age"]
		}`)))

		// This is not valid: logs[0], logs[1], logs[2]
		assert.Error(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 3,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 1, "age": 30},
					{"city": "Boston", "timestamp": 2, "age": 36},
					{"city": "Boston", "timestamp": 2, "age": 22}
				]
			},
			"allColumns": ["city", "timestamp", "age"]
		}`)))
	})

	t.Run("AmbiguousTopN", func(t *testing.T) {
		logs := []map[string]interface{}{
			{"city": "Boston", "timestamp": uint64(1), "foo": 30},
			{"city": "Boston", "timestamp": uint64(1), "bar": 36},
			{"city": "Boston", "timestamp": uint64(1), "baz": 22},
			{"city": "Boston", "timestamp": uint64(2), "age": 42},
		}
		head, startEpoch, endEpoch := 3, uint64(0), uint64(10)
		validator, err := NewFilterQueryValidator(bostonFilter, "", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs)

		// This is valid: logs[3], logs[2], logs[1]
		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 3,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 2, "age": 42},
					{"city": "Boston", "timestamp": 1, "baz": 22},
					{"city": "Boston", "timestamp": 1, "bar": 36}
				]
			},
			"allColumns": ["city", "timestamp", "age", "baz", "bar"]
		}`)))

		// This is valid: logs[3], logs[2], logs[0]
		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 3,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 2, "age": 42},
					{"city": "Boston", "timestamp": 1, "baz": 22},
					{"city": "Boston", "timestamp": 1, "foo": 30}
				]
			},
			"allColumns": ["city", "timestamp", "age", "baz", "foo"]
		}`)))

		// This is valid: logs[3], logs[0], logs[1]
		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 3,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 2, "age": 42},
					{"city": "Boston", "timestamp": 1, "foo": 30},
					{"city": "Boston", "timestamp": 1, "bar": 36}
				]
			},
			"allColumns": ["city", "timestamp", "age", "foo", "bar"]
		}`)))
	})

	t.Run("Wildcard", func(t *testing.T) {
		head, startEpoch, endEpoch := 3, uint64(0), uint64(10)
		filter, err := Filter("city", "New *")
		assert.NoError(t, err)
		validator, err := NewFilterQueryValidator(filter, "", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, []map[string]interface{}{
			{"city": "New York", "timestamp": uint64(1), "age": 30},
			{"city": "Hello New York", "timestamp": uint64(2), "age": 30},
			{"city": "Newark", "timestamp": uint64(3), "age": 22},
			{"city": "Boston", "timestamp": uint64(4), "age": 22},
			{"city": "New Orleans", "timestamp": uint64(5), "age": 36},
		})

		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 2,
					"relation": "eq"
				},
				"records": [
					{"city": "New Orleans", "timestamp": 5, "age": 36},
					{"city": "New York", "timestamp": 1, "age": 30}
				]
			},
			"allColumns": ["city", "timestamp", "age"]
		}`)))
	})

	t.Run("MatchLiteralAsterisk", func(t *testing.T) {
		_, err := Filter("city", "2 \\* 5")
		assert.Error(t, err) // Change if we want to support this.
	})

	t.Run("ValueHasSpaces", func(t *testing.T) {
		head, startEpoch, endEpoch := 3, uint64(0), uint64(10)
		filter, err := Filter("city", "New York")
		assert.NoError(t, err)
		validator, err := NewFilterQueryValidator(filter, "", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		query, _, _ := validator.GetQuery()
		assert.Equal(t, `city="New York" | head 3`, query)
	})

	t.Run("CustomSortColumn", func(t *testing.T) {
		head, startEpoch, endEpoch := 3, uint64(0), uint64(10)
		validator, err := NewFilterQueryValidator(bostonFilter, "age", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, []map[string]interface{}{
			{"city": "Boston", "timestamp": uint64(1), "age": 30},
			{"city": "Boston", "timestamp": uint64(2), "age": 36},
			{"city": "Boston", "timestamp": uint64(3), "latency": 100}, // Missing the sort column.
			{"city": "Boston", "timestamp": uint64(4), "age": 22},
			{"city": "Boston", "timestamp": uint64(5), "age": 5}, // Ensure we don't sort lexographically.
			{"city": "New York", "timestamp": uint64(6), "age": 33},
			{"city": "Boston", "timestamp": uint64(7), "age": 22},
		})

		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 3,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 2, "age": 36},
					{"city": "Boston", "timestamp": 1, "age": 30},
					{"city": "Boston", "timestamp": 7, "age": 22}
				]
			},
			"allColumns": ["city", "timestamp", "age"]
		}`)))

		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 3,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 2, "age": 36},
					{"city": "Boston", "timestamp": 1, "age": 30},
					{"city": "Boston", "timestamp": 4, "age": 22}
				]
			},
			"allColumns": ["city", "timestamp", "age"]
		}`)))
	})

	t.Run("CustomSortMissingValues", func(t *testing.T) {
		head, startEpoch, endEpoch := 3, uint64(0), uint64(10)
		validator, err := NewFilterQueryValidator(bostonFilter, "latency", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs)

		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 3,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 5, "latency": 100},
					{"city": "Boston", "timestamp": 4, "age": 22},
					{"city": "Boston", "timestamp": 2, "age": 36}
				]
			},
			"allColumns": ["city", "timestamp", "age", "latency"]
		}`)))
	})

	t.Run("AllowAllStartTimes", func(t *testing.T) {
		head, startEpoch, endEpoch := 3, uint64(0), uint64(10)
		validator, err := NewFilterQueryValidator(bostonFilter, "", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		validator = validator.WithAllowAllStartTimes()
		addLogsWithoutError(t, validator, logs[3:])

		// Malformed JSON.
		assert.Error(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
		`)))

		// A potentially valid response.
		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 5,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 5, "latency": 100},
					{"city": "Boston", "timestamp": 4, "age": 22},
					{"city": "Boston", "timestamp": 2, "age": 36},
					{"city": "Boston", "timestamp": 1, "age": 30},
					{"city": "Boston", "timestamp": 3, "age": 22}
				]
			},
			"allColumns": ["city", "timestamp", "age", "latency"]
		}`)))
	})

	t.Run("Concurrency", func(t *testing.T) {
		head, startEpoch, endEpoch := 1, uint64(0), uint64(10)
		validator, err := NewFilterQueryValidator(bostonFilter, "", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs[:1])

		expectedJson := []byte(`{
			"hits": {
				"totalMatched": {
					"value": 1,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 1, "age": 30}
				]
			},
			"allColumns": ["age", "city", "timestamp"]
		}`)
		assert.NoError(t, validator.MatchesResult(expectedJson))

		numIters := 1000
		waitGroup := &sync.WaitGroup{}
		waitGroup.Add(2)
		go func() {
			defer waitGroup.Done()

			for i := 0; i < numIters; i++ {
				assert.NoError(t, validator.MatchesResult(expectedJson))
			}
		}()
		go func() {
			defer waitGroup.Done()

			for i := 0; i < numIters; i++ {
				addLogsWithoutError(t, validator, logs[:1])
			}
		}()

		waitGroup.Wait()
	})
}

func Test_CountQueryValidator(t *testing.T) {
	logs := []map[string]interface{}{
		{"city": "Boston", "timestamp": uint64(1), "age": 30},
		{"city": "Boston", "timestamp": uint64(2), "age": 36},
		{"city": "New York", "timestamp": uint64(3), "age": 22},
		{"city": "Boston", "timestamp": uint64(4), "age": 22},
		{"city": "Boston", "timestamp": uint64(5), "latency": 100},
	}
	bostonFilter, err := Filter("city", "Boston")
	assert.NoError(t, err)

	t.Run("NoMatches", func(t *testing.T) {
		startEpoch, endEpoch := uint64(0), uint64(10)
		validator, err := NewCountQueryValidator(bostonFilter, startEpoch, endEpoch)
		assert.NoError(t, err)
		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 0,
					"relation": "eq"
				}
			},
			"allColumns": ["count(*)"],
			"measureFunctions": ["count(*)"],
			"measure": [{
					"GroupByValues": ["*"],
					"MeasureVal": {"count(*)": 0}
			}]
		}`)))
	})

	t.Run("SomeMatches", func(t *testing.T) {
		startEpoch, endEpoch := uint64(0), uint64(10)
		validator, err := NewCountQueryValidator(bostonFilter, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs)

		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 4,
					"relation": "eq"
				}
			},
			"allColumns": ["count(*)"],
			"measureFunctions": ["count(*)"],
			"measure": [{
					"GroupByValues": ["*"],
					"MeasureVal": {"count(*)": 4}
			}]
		}`)))
	})

	t.Run("LargeCount", func(t *testing.T) {
		startEpoch, endEpoch := uint64(0), uint64(10)
		validator, err := NewCountQueryValidator(bostonFilter, startEpoch, endEpoch)
		assert.NoError(t, err)

		for i := 0; i < 1_000_000; i++ {
			addLogsWithoutError(t, validator, logs)
		}

		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 4000000,
					"relation": "eq"
				}
			},
			"allColumns": ["count(*)"],
			"measureFunctions": ["count(*)"],
			"measure": [{
					"GroupByValues": ["*"],
					"MeasureVal": {"count(*)": 4000000}
			}]
		}`)))
	})

	t.Run("MatchAllQuery", func(t *testing.T) {
		startEpoch, endEpoch := uint64(0), uint64(10)
		validator, err := NewCountQueryValidator(MatchAll(), startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs)

		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 5,
					"relation": "eq"
				}
			},
			"allColumns": ["count(*)"],
			"measureFunctions": ["count(*)"],
			"measure": [{
				"GroupByValues": ["*"],
				"MeasureVal": {"count(*)": 5}
			}]
		}`)))
	})

	t.Run("AllowAllStartTimes", func(t *testing.T) {
		startEpoch, endEpoch := uint64(0), uint64(10)
		validator, err := NewCountQueryValidator(bostonFilter, startEpoch, endEpoch)
		assert.NoError(t, err)
		validator = validator.WithAllowAllStartTimes()
		addLogsWithoutError(t, validator, logs[3:])

		// Malformed JSON.
		assert.Error(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
		`)))

		// A potentially valid response.
		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 5,
					"relation": "eq"
				}
			},
			"allColumns": ["count(*)"],
			"measureFunctions": ["count(*)"],
			"measure": [{
				"GroupByValues": ["*"],
				"MeasureVal": {"count(*)": 5}
			}]
		}`)))
	})

	t.Run("BadResponse", func(t *testing.T) {
		startEpoch, endEpoch := uint64(0), uint64(10)
		validator, err := NewCountQueryValidator(bostonFilter, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs)

		// Incorrect totalMatched.
		assert.Error(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 2,
					"relation": "eq"
				}
			},
			"allColumns": ["count(*)"],
			"measureFunctions": ["count(*)"],
			"measure": [{
					"GroupByValues": ["*"],
					"MeasureVal": {"count(*)": 4}
			}]
		}`)))

		// Bad allColumns.
		assert.Error(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 4,
					"relation": "eq"
				}
			},
			"allColumns": ["total"],
			"measureFunctions": ["count(*)"],
			"measure": [{
					"GroupByValues": ["*"],
					"MeasureVal": {"count(*)": 4}
			}]
		}`)))

		// Bad measureFunctions.
		assert.Error(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 4,
					"relation": "eq"
				}
			},
			"allColumns": ["count(*)"],
			"measureFunctions": ["someFunc"],
			"measure": [{
					"GroupByValues": ["*"],
					"MeasureVal": {"count(*)": 4}
			}]
		}`)))

		// Bad GroupByValues.
		assert.Error(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 4,
					"relation": "eq"
				}
			},
			"allColumns": ["count(*)"],
			"measureFunctions": ["count(*)"],
			"measure": [{
					"GroupByValues": [""],
					"MeasureVal": {"count(*)": 4}
			}]
		}`)))

		// Bad MeasureVal.
		assert.Error(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 4,
					"relation": "eq"
				}
			},
			"allColumns": ["count(*)"],
			"measureFunctions": ["count(*)"],
			"measure": [{
					"GroupByValues": ["*"],
					"MeasureVal": {"count(*)": 42}
			}]
		}`)))
	})
}

func Test_filterToString(t *testing.T) {
	filter, err := Filter("city", "Boston")
	assert.NoError(t, err)
	assert.Equal(t, `city="Boston"`, fmt.Sprintf("%v", filter))

	filter, err = Filter("city", "New *")
	assert.NoError(t, err)
	assert.Equal(t, `city="New *"`, fmt.Sprintf("%v", filter))

	filter, err = Filter("city", "*")
	assert.NoError(t, err)
	assert.Equal(t, `city="*"`, fmt.Sprintf("%v", filter))

	filter = MatchAll()
	assert.Equal(t, `*`, fmt.Sprintf("%v", filter))
}

func Test_dynamicFilter_setFrom(t *testing.T) {
	df := &dynamicFilter{}

	df.setFrom(map[string]interface{}{"city": "New York"})
	assert.Equal(t, `city="New York"`, fmt.Sprintf("%v", df))

	df.setFrom(map[string]interface{}{"city": 123}) // No string values.
	assert.Equal(t, `*`, fmt.Sprintf("%v", df))

	df.setFrom(map[string]interface{}{"country": "Åland Islands"}) // Non-ASCII value.
	assert.Equal(t, `*`, fmt.Sprintf("%v", df))

	for i := 0; i < 10; i++ {
		df.setFrom(map[string]interface{}{"city": "Boston", "country": "Åland Islands"})
		assert.Equal(t, `city="Boston"`, fmt.Sprintf("%v", df)) // Only the ASCII value is valid.
	}
}

func Test_dynamicFilter_query(t *testing.T) {
	logs := []map[string]interface{}{
		{"city": "Boston", "timestamp": uint64(1), "age": 30},
		{"city": "Boston", "timestamp": uint64(2), "age": 36},
		{"city": "New York", "timestamp": uint64(3), "age": 22},
		{"city": "Boston", "timestamp": uint64(4), "age": 22},
		{"city": "Boston", "timestamp": uint64(5), "latency": 100},
	}

	t.Run("RawLogs", func(t *testing.T) {
		head, startEpoch, endEpoch := 10, uint64(0), uint64(10)
		validator, err := NewFilterQueryValidator(&dynamicFilter{}, "", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs)

		// Based on the logs above, the only valid concrete filter for the
		// dynamic filter is city=Boston.
		query, _, _ := validator.GetQuery()
		assert.Equal(t, `city="Boston" | head 10`, query)
		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 4,
					"relation": "eq"
				},
				"records": [
					{"city": "Boston", "timestamp": 5, "latency": 100},
					{"city": "Boston", "timestamp": 4, "age": 22},
					{"city": "Boston", "timestamp": 2, "age": 36},
					{"city": "Boston", "timestamp": 1, "age": 30}
				]
			},
			"allColumns": ["city", "timestamp", "age", "latency"]
		}`)))
	})

	t.Run("Count", func(t *testing.T) {
		startEpoch, endEpoch := uint64(0), uint64(10)
		validator, err := NewCountQueryValidator(&dynamicFilter{}, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs)

		// Based on the logs above, the only valid concrete filter for the
		// dynamic filter is city=Boston.
		query, _, _ := validator.GetQuery()
		assert.Equal(t, `city="Boston" | stats count`, query) // The count query should not have a head in it.
		assert.NoError(t, validator.MatchesResult([]byte(`{
			"hits": {
				"totalMatched": {
					"value": 4,
					"relation": "eq"
				}
			},
			"allColumns": ["count(*)"],
			"measureFunctions": ["count(*)"],
			"measure": [{
				"GroupByValues": ["*"],
				"MeasureVal": {"count(*)": 4}
			}]
		}`)))
	})

}

func addLogsWithoutError(t *testing.T, validator queryValidator, logs []map[string]interface{}) {
	t.Helper()

	for _, log := range logs {
		err := validator.HandleLog(log, log["timestamp"].(uint64), false)
		assert.NoError(t, err)
	}
}
