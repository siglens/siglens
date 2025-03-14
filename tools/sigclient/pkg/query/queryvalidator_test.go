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
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_implmentsQueryValidator(t *testing.T) {
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
		validator, err := NewFilterQueryValidator("city", "Boston", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs[:2])
		assert.NoError(t, validator.MatchesResult(expectedJsonMatchingFirstTwo))
	})

	t.Run("FilterByValue", func(t *testing.T) {
		head, startEpoch, endEpoch := 3, uint64(0), uint64(10)
		validator, err := NewFilterQueryValidator("city", "Boston", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs[:3]) // The third log has a different city, so it should be ignored.
		assert.NoError(t, validator.MatchesResult(expectedJsonMatchingFirstTwo))
	})

	t.Run("FilterByTime", func(t *testing.T) {
		head, startEpoch, endEpoch := 3, uint64(1), uint64(2)
		validator, err := NewFilterQueryValidator("city", "Boston", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs)
		assert.NoError(t, validator.MatchesResult(expectedJsonMatchingFirstTwo))
	})

	t.Run("MissingColumns", func(t *testing.T) {
		head, startEpoch, endEpoch := 3, uint64(0), uint64(10)
		validator, err := NewFilterQueryValidator("city", "Boston", head, startEpoch, endEpoch)
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
		validator, err := NewFilterQueryValidator("city", "Boston", head, startEpoch, endEpoch)
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
		validator, err := NewFilterQueryValidator("city", "Boston", head, startEpoch, endEpoch)
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
		validator, err := NewFilterQueryValidator("city", "Boston", head, startEpoch, endEpoch)
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
		validator, err := NewFilterQueryValidator("city", "Boston", head, startEpoch, endEpoch)
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

	t.Run("Concurrency", func(t *testing.T) {
		head, startEpoch, endEpoch := 1, uint64(0), uint64(10)
		validator, err := NewFilterQueryValidator("city", "Boston", head, startEpoch, endEpoch)
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

	t.Run("NoMatches", func(t *testing.T) {
		startEpoch, endEpoch := uint64(0), uint64(10)
		validator, err := NewCountQueryValidator("city", "Boston", startEpoch, endEpoch)
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
			}],
			"bucketCount": 1
		}`)))
	})

	t.Run("SomeMatches", func(t *testing.T) {
		startEpoch, endEpoch := uint64(0), uint64(10)
		validator, err := NewCountQueryValidator("city", "Boston", startEpoch, endEpoch)
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
			}],
			"bucketCount": 1
		}`)))
	})

	t.Run("BadResponse", func(t *testing.T) {
		startEpoch, endEpoch := uint64(0), uint64(10)
		validator, err := NewCountQueryValidator("city", "Boston", startEpoch, endEpoch)
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
			}],
			"bucketCount": 1
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
			}],
			"bucketCount": 1
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
			}],
			"bucketCount": 1
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
			}],
			"bucketCount": 1
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
			}],
			"bucketCount": 1
		}`)))

	})
}

func addLogsWithoutError(t *testing.T, validator queryValidator, logs []map[string]interface{}) {
	t.Helper()

	for _, log := range logs {
		err := validator.HandleLog(log)
		assert.NoError(t, err)
	}
}
