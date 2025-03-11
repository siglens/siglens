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
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_FilterQueryValidator(t *testing.T) {
	logs := []map[string]interface{}{
		{"city": "Boston", "timestamp": uint64(1), "age": 30},
		{"city": "Boston", "timestamp": uint64(2), "age": 36},
		{"city": "New York", "timestamp": uint64(3), "age": 22},
		{"city": "Boston", "timestamp": uint64(4), "age": 22},
		{"city": "Boston", "timestamp": uint64(5)},
	}

	t.Run("FewerThanHeadMatch", func(t *testing.T) {
		head, startEpoch, endEpoch := 3, uint64(0), uint64(10)
		validator, err := NewFilterQueryValidator("city", "Boston", head, startEpoch, endEpoch)
		assert.NoError(t, err)
		addLogsWithoutError(t, validator, logs[:2])

		expectedJson := []byte(`{
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
		assert.NoError(t, validator.MatchesResult(expectedJson))
	})
}

func addLogsWithoutError(t *testing.T, validator queryValidator, logs []map[string]interface{}) {
	t.Helper()

	for _, log := range logs {
		err := validator.HandleLog(log)
		assert.NoError(t, err)
	}
}
