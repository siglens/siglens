/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
