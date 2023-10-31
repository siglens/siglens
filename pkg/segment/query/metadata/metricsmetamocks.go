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

package metadata

import (
	"sync"

	"github.com/siglens/siglens/pkg/segment/writer"
	mmeta "github.com/siglens/siglens/pkg/segment/writer/metrics/meta"
	log "github.com/sirupsen/logrus"
)

func InitMockMetricsMetadataStore(entryCount int) error {
	globalMetricsMetadata = &allMetricsSegmentMetadata{
		sortedMetricsSegmentMeta: make([]*MetricsSegmentMetadata, 0),
		metricsSegmentMetaMap:    make(map[string]*MetricsSegmentMetadata),
		updateLock:               &sync.RWMutex{},
	}

	_, err := writer.WriteMockMetricsSegment(true, entryCount)
	if err != nil {
		log.Errorf("InitMockMetricsMetadataStore: Could not write mock metrics segment %v", err)
		return err
	}
	allMetricsMetas, err := mmeta.GetLocalMetricsMetaEntries()
	if err != nil {
		log.Errorf("InitMockMetricsMetadataStore: unable to get all the metrics meta entries. Error: %v", err)
		return err
	}

	allMetricsSegmentMeta := make([]*MetricsSegmentMetadata, 0)
	for _, mMetaInfo := range allMetricsMetas {
		currMSegMetadata := InitMetricsMicroIndex(mMetaInfo)
		allMetricsSegmentMeta = append(allMetricsSegmentMeta, currMSegMetadata)
	}

	BulkAddMetricsSegment(allMetricsSegmentMeta)
	return nil
}
