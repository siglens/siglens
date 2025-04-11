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

package query

import (
	"os"
	"sort"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/stretchr/testify/assert"
)

func Test_GetAllMetricNamesOverTheTimeRange(t *testing.T) {
	// Create Mock Segment Files.

	config.InitializeTestingConfig(t.TempDir())

	err := metadata.InitMockMetricsMetadataStore(10000)
	assert.Nil(t, err)

	timeRange := &dtu.MetricsTimeRange{
		StartEpochSec: uint32(time.Now().Unix() - int64(24*60*60)),
		EndEpochSec:   uint32(time.Now().Unix()),
	}

	mNames, err := GetAllMetricNamesOverTheTimeRange(timeRange, 0)
	assert.Nil(t, err)

	ingestedMetricNames := []string{"test.metric.0", "test.metric.1", "test.metric.2", "test.metric.3"}

	assert.True(t, len(mNames) == 4)
	sort.Slice(mNames, func(i, j int) bool {
		return mNames[i] < mNames[j]
	})

	assert.Equal(t, mNames, ingestedMetricNames)

	// Cleanup
	_ = os.RemoveAll(config.GetDataPath())
}

func Test_getSegmentFilterTimeRange(t *testing.T) {
	timeRef := uint32(1712659200)
	timeRange := &dtu.MetricsTimeRange{
		StartEpochSec: timeRef,
		EndEpochSec:   timeRef + uint32((60 * time.Minute).Seconds()),
	}

	query := `max_over_time(
	sum by (cluster) (
		1 - max by (cluster, instance, cpu, core) (
		rate(node_cpu_seconds_total{mode="idle", cluster=~".+"}[5m:1s])
		)
	)[1h:5m]
	)
	`

	// parse the query
	parsedQuery, err := parser.ParseExpr(query)
	assert.Nil(t, err)

	// get the time range
	segmentTimeRange := getSegmentFilterTimeRange(parsedQuery, *timeRange)

	expectedTimeRange := &dtu.MetricsTimeRange{
		StartEpochSec: timeRef - 60*60 - 5*60 - uint32(structs.DEFAULT_LOOKBACK_FOR_INSTANT_VECTOR.Seconds()),
		EndEpochSec:   timeRange.EndEpochSec,
	}

	assert.Equal(t, expectedTimeRange, segmentTimeRange)
}
