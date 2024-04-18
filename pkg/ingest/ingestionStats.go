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

	"github.com/siglens/siglens/pkg/instrumentation"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	vtable "github.com/siglens/siglens/pkg/virtualtable"

	log "github.com/sirupsen/logrus"
)

func InitIngestionMetrics() {
	go ingestionMetricsLooper()
}

func ingestionMetricsLooper() {
	for {
		time.Sleep(1 * time.Minute)

		currentEventCount := int64(0)
		currentBytesReceived := int64(0)
		currentOnDiskBytes := int64(0)

		// change to loop for all orgs
		allVirtualTableNames, err := vtable.GetVirtualTableNames(0)

		if err != nil {
			log.Errorf("ingestionMetricsLooper: Error in getting virtual table names, err:%v", err)
		}
		for indexName := range allVirtualTableNames {
			if indexName == "" {
				log.Errorf("ingestionMetricsLooper: skipping an empty index name indexName=%v", indexName)
				continue
			}
			byteCount, eventCount, onDiskBytes := segwriter.GetVTableCounts(indexName, 0)
			unrotatedByteCount, unrotatedEventCount, unrotatedOnDiskBytes := segwriter.GetUnrotatedVTableCounts(indexName, 0)

			totalEventsForIndex := uint64(eventCount) + uint64(unrotatedEventCount)
			currentEventCount += int64(totalEventsForIndex)
			instrumentation.SetEventCountPerIndex(currentEventCount, "indexname", indexName)

			totalBytesReceivedForIndex := byteCount + unrotatedByteCount
			currentBytesReceived += int64(totalBytesReceivedForIndex)
			instrumentation.SetBytesCountPerIndex(currentBytesReceived, "indexname", indexName)

			totalOnDiskBytesForIndex := onDiskBytes + unrotatedOnDiskBytes
			currentOnDiskBytes += int64(totalOnDiskBytesForIndex)
			instrumentation.SetOnDiskBytesPerIndex(currentOnDiskBytes, "indexname", indexName)

		}
		instrumentation.SetGaugeCurrentEventCount(currentEventCount)
		instrumentation.SetGaugeCurrentBytesReceivedGauge(currentBytesReceived)
		instrumentation.SetGaugeOnDiskBytesGauge(currentOnDiskBytes)
	}
}
