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

package ingest

import (
	"time"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/segment/query"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	log "github.com/sirupsen/logrus"
)

func InitIngestionMetrics() {
	go ingestionMetricsLooper()
	go metricsLooper()
}

func ingestionMetricsLooper() {
	for {
		time.Sleep(1 * time.Minute)

		currentEventCount := int64(0)
		currentBytesReceived := int64(0)
		currentOnDiskBytes := int64(0)

		allSegmetas := segwriter.ReadGlobalSegmetas()

		allCnts := segwriter.GetVTableCountsForAll(0, allSegmetas)

		segwriter.GetUnrotatedVTableCountsForAll(0, allCnts)

		for indexName, cnts := range allCnts {
			if indexName == "" {
				log.Errorf("ingestionMetricsLooper: skipping an empty index name len(indexName)=%v", len(indexName))
				continue
			}

			totalEventsForIndex := uint64(cnts.RecordCount)
			currentEventCount += int64(totalEventsForIndex)
			instrumentation.SetEventCountPerIndex(currentEventCount, "indexname", indexName)

			totalBytesReceivedForIndex := cnts.BytesCount
			currentBytesReceived += int64(totalBytesReceivedForIndex)
			instrumentation.SetBytesCountPerIndex(currentBytesReceived, "indexname", indexName)

			totalOnDiskBytesForIndex := cnts.OnDiskBytesCount
			currentOnDiskBytes += int64(totalOnDiskBytesForIndex)
			instrumentation.SetOnDiskBytesPerIndex(currentOnDiskBytes, "indexname", indexName)
		}

		instrumentation.SetTotalEventCount(currentEventCount)
		instrumentation.SetTotalBytesReceived(currentBytesReceived)
		instrumentation.SetTotalLogOnDiskBytes(currentOnDiskBytes)
	}
}

func metricsLooper() {
	for {
		time.Sleep(1 * time.Minute)

		setNumMetricNames()
		setNumSeries()
	}
}

func setNumMetricNames() {
	allPreviousTime := &dtu.MetricsTimeRange{
		StartEpochSec: 0,
		EndEpochSec:   uint32(time.Now().Unix()),
	}
	names, err := query.GetAllMetricNamesOverTheTimeRange(allPreviousTime, 0)
	if err != nil {
		log.Errorf("setNumMetricNames: failed to get all metric names: %v", err)
		return
	}

	instrumentation.SetTotalMetricNames(int64(len(names)))
}

func setNumSeries() {
	allPreviousTime := &dtu.MetricsTimeRange{
		StartEpochSec: 0,
		EndEpochSec:   uint32(time.Now().Unix()),
	}
	numSeries, err := query.GetSeriesCardinalityOverTimeRange(allPreviousTime, 0)
	if err != nil {
		log.Errorf("setNumSeries: failed to get all series: %v", err)
		return
	}

	instrumentation.SetTotalTimeSeries(int64(numSeries))
}
