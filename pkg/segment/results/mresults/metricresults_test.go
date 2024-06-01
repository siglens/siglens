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

package mresults

import (
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_ApplyAggregationToResults_NoGroupBy_Single_Series(t *testing.T) {
	mResult := &MetricsResult{
		MetricName: "test",
		State:      AGGREGATED,
	}

	parallelism := int(config.GetParallelism()) * 2

	results := make(map[string]map[uint32]float64, 0)

	seriesId := "test{"

	results[seriesId] = make(map[uint32]float64, 0)
	results[seriesId][1] = 1.0
	results[seriesId][2] = 2.0
	results[seriesId][3] = 3.0

	mResult.Results = results

	aggregation := structs.Aggregation{
		AggregatorFunction: utils.Sum,
	}

	errors := mResult.ApplyAggregationToResults(parallelism, aggregation)
	assert.Equal(t, 0, len(errors))
	assert.Equal(t, 1, len(mResult.Results))
	assert.Equal(t, 3, len(mResult.Results[seriesId]))
	assert.Equal(t, 1.0, mResult.Results[seriesId][1])
	assert.Equal(t, 2.0, mResult.Results[seriesId][2])
	assert.Equal(t, 3.0, mResult.Results[seriesId][3])
}

func Test_ApplyAggregationToResults_NoGroupBy_Multiple_Series(t *testing.T) {
	mResult := &MetricsResult{
		MetricName: "test",
		State:      AGGREGATED,
	}

	parallelism := int(config.GetParallelism()) * 2

	results := make(map[string]map[uint32]float64, 0)

	seriesId1 := "test{tk1:v1,tk2:v2}"
	seriesId2 := "test{tk1:v1,tk2:v3}"

	results[seriesId1] = make(map[uint32]float64, 0)
	results[seriesId1][1] = 1.0
	results[seriesId1][2] = 2.0
	results[seriesId1][3] = 3.0

	results[seriesId2] = make(map[uint32]float64, 0)
	results[seriesId2][1] = 4.0
	results[seriesId2][2] = 5.0
	results[seriesId2][3] = 6.0

	aggSeriesId := "test{"

	mResult.Results = results

	aggregation := structs.Aggregation{
		AggregatorFunction: utils.Avg,
	}

	errors := mResult.ApplyAggregationToResults(parallelism, aggregation)
	assert.Equal(t, 0, len(errors))
	assert.Equal(t, 1, len(mResult.Results))
	assert.Equal(t, 3, len(mResult.Results[aggSeriesId]))
	assert.Equal(t, 2.5, mResult.Results[aggSeriesId][1])
	assert.Equal(t, 3.5, mResult.Results[aggSeriesId][2])
	assert.Equal(t, 4.5, mResult.Results[aggSeriesId][3])
}

func Test_ApplyAggregationToResults_GroupBy_Single_Series(t *testing.T) {
	mResult := &MetricsResult{
		MetricName: "test",
		State:      AGGREGATED,
	}

	parallelism := int(config.GetParallelism()) * 2

	results := make(map[string]map[uint32]float64, 0)

	seriesId := "test{tk1:v1,tk2:v2"

	results[seriesId] = make(map[uint32]float64, 0)
	results[seriesId][1] = 1.0
	results[seriesId][2] = 2.0
	results[seriesId][3] = 3.0

	mResult.Results = results

	aggregation := structs.Aggregation{
		AggregatorFunction: utils.Sum,
		GroupByFields:      []string{"tk1"},
	}

	aggSeriesId := "test{tk1:v1"

	errors := mResult.ApplyAggregationToResults(parallelism, aggregation)
	assert.Equal(t, 0, len(errors))
	assert.Equal(t, 1, len(mResult.Results))
	assert.Equal(t, 3, len(mResult.Results[aggSeriesId]))
	assert.Equal(t, 1.0, mResult.Results[aggSeriesId][1])
	assert.Equal(t, 2.0, mResult.Results[aggSeriesId][2])
	assert.Equal(t, 3.0, mResult.Results[aggSeriesId][3])
}

// Group By Field => Single key
// Matching values in all series.
func Test_ApplyAggregationToResults_GroupBy_Multiple_Series_v1(t *testing.T) {
	mResult := &MetricsResult{
		MetricName: "test",
		State:      AGGREGATED,
	}

	parallelism := int(config.GetParallelism()) * 2

	results := make(map[string]map[uint32]float64, 0)

	seriesId1 := "test{tk1:v1,tk2:v2}"
	seriesId2 := "test{tk1:v1,tk2:v3}"

	results[seriesId1] = make(map[uint32]float64, 0)
	results[seriesId1][1] = 1.0
	results[seriesId1][2] = 2.0
	results[seriesId1][3] = 3.0

	results[seriesId2] = make(map[uint32]float64, 0)
	results[seriesId2][1] = 4.0
	results[seriesId2][2] = 5.0
	results[seriesId2][3] = 6.0

	mResult.Results = results

	aggregation := structs.Aggregation{
		AggregatorFunction: utils.Avg,
		GroupByFields:      []string{"tk1"},
	}

	aggSeriesId := "test{tk1:v1"

	errors := mResult.ApplyAggregationToResults(parallelism, aggregation)
	assert.Equal(t, 0, len(errors))
	assert.Equal(t, 1, len(mResult.Results))
	assert.Equal(t, 3, len(mResult.Results[aggSeriesId]))
	assert.Equal(t, 2.5, mResult.Results[aggSeriesId][1])
	assert.Equal(t, 3.5, mResult.Results[aggSeriesId][2])
	assert.Equal(t, 4.5, mResult.Results[aggSeriesId][3])
}

// Group By Field => Single Key
// Missing values in one of the series.
func Test_ApplyAggregationToResults_GroupBy_Multiple_Series_v2(t *testing.T) {
	mResult := &MetricsResult{
		MetricName: "test",
		State:      AGGREGATED,
	}

	parallelism := int(config.GetParallelism()) * 2

	results := make(map[string]map[uint32]float64, 0)

	seriesId1 := "test{tk1:v1,tk2:v2}"
	seriesId2 := "test{tk1:v2,tk2:v3}"

	results[seriesId1] = make(map[uint32]float64, 0)
	results[seriesId1][1] = 1.0
	results[seriesId1][2] = 2.0
	results[seriesId1][3] = 3.0

	results[seriesId2] = make(map[uint32]float64, 0)
	results[seriesId2][1] = 4.0
	results[seriesId2][2] = 5.0
	results[seriesId2][3] = 6.0

	mResult.Results = results

	aggregation := structs.Aggregation{
		AggregatorFunction: utils.Avg,
		GroupByFields:      []string{"tk1"},
	}

	aggSeriesId1 := "test{tk1:v1"
	aggSeriesId2 := "test{tk1:v2"

	errors := mResult.ApplyAggregationToResults(parallelism, aggregation)
	assert.Equal(t, 0, len(errors))
	assert.Equal(t, 2, len(mResult.Results))
	assert.Equal(t, 3, len(mResult.Results[aggSeriesId1]))
	assert.Equal(t, 3, len(mResult.Results[aggSeriesId2]))
	assert.Equal(t, 1.0, mResult.Results[aggSeriesId1][1])
	assert.Equal(t, 2.0, mResult.Results[aggSeriesId1][2])
	assert.Equal(t, 3.0, mResult.Results[aggSeriesId1][3])
	assert.Equal(t, 4.0, mResult.Results[aggSeriesId2][1])
	assert.Equal(t, 5.0, mResult.Results[aggSeriesId2][2])
	assert.Equal(t, 6.0, mResult.Results[aggSeriesId2][3])
}

// Group By Field => Multiple Keys
// Missing values in one of the series.
// Missing tag keys in one of the series.
func Test_ApplyAggregationToResults_GroupBy_Multiple_Series_v3(t *testing.T) {
	mResult := &MetricsResult{
		MetricName: "test",
		State:      AGGREGATED,
	}

	parallelism := int(config.GetParallelism()) * 2

	results := make(map[string]map[uint32]float64, 0)

	seriesId1 := "test{tk1:v1,tk2:v2"
	seriesId2 := "test{tk1:v2,tk2:v3,"
	seriesId3 := "test{tk1:v1,tk2:v2,tk3:v1"
	seriesId4 := "test{tk1:v2,tk3:v3"

	results[seriesId1] = make(map[uint32]float64, 0)
	results[seriesId1][1] = 1.0
	results[seriesId1][2] = 2.0
	results[seriesId1][3] = 3.0

	results[seriesId2] = make(map[uint32]float64, 0)
	results[seriesId2][1] = 4.0
	results[seriesId2][2] = 5.0
	results[seriesId2][3] = 6.0

	results[seriesId3] = make(map[uint32]float64, 0)
	results[seriesId3][1] = 7.0
	results[seriesId3][2] = 8.0
	results[seriesId3][3] = 9.0

	results[seriesId4] = make(map[uint32]float64, 0)
	results[seriesId4][1] = 10.0
	results[seriesId4][2] = 11.0
	results[seriesId4][3] = 12.0

	mResult.Results = results

	aggregation := structs.Aggregation{
		AggregatorFunction: utils.Avg,
		GroupByFields:      []string{"tk1", "tk2"},
	}

	aggSeriesId1 := "test{tk1:v1,tk2:v2"
	aggSeriesId2 := "test{tk1:v2,tk2:v3"
	aggSeriesId3 := "test{tk1:v2"

	errors := mResult.ApplyAggregationToResults(parallelism, aggregation)
	assert.Equal(t, 0, len(errors))
	assert.Equal(t, 3, len(mResult.Results))
	assert.Equal(t, 3, len(mResult.Results[aggSeriesId1]))
	assert.Equal(t, 3, len(mResult.Results[aggSeriesId2]))
	assert.Equal(t, 3, len(mResult.Results[aggSeriesId3]))
	assert.Equal(t, 4.0, mResult.Results[aggSeriesId1][1])
	assert.Equal(t, 5.0, mResult.Results[aggSeriesId1][2])
	assert.Equal(t, 6.0, mResult.Results[aggSeriesId1][3])
	assert.Equal(t, 4.0, mResult.Results[aggSeriesId2][1])
	assert.Equal(t, 5.0, mResult.Results[aggSeriesId2][2])
	assert.Equal(t, 6.0, mResult.Results[aggSeriesId2][3])
	assert.Equal(t, 10.0, mResult.Results[aggSeriesId3][1])
	assert.Equal(t, 11.0, mResult.Results[aggSeriesId3][2])
	assert.Equal(t, 12.0, mResult.Results[aggSeriesId3][3])
}
