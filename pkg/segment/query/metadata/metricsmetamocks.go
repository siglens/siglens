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
