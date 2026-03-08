// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package metadata

import (
	"github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/writer"
	mmeta "github.com/siglens/siglens/pkg/segment/writer/metrics/meta"
	log "github.com/sirupsen/logrus"
)

func InitMockMetricsMetadataStore(entryCount int) error {
	metadata.ResetGlobalMetricsMetadataForTest()

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

	allMetricsSegmentMeta := make([]*metadata.MetricsSegmentMetadata, 0)
	for _, mMetaInfo := range allMetricsMetas {
		currMSegMetadata := metadata.InitMetricsMicroIndex(mMetaInfo)
		allMetricsSegmentMeta = append(allMetricsSegmentMeta, currMSegMetadata)
	}

	metadata.BulkAddMetricsSegment(allMetricsSegmentMeta)

	return nil
}
