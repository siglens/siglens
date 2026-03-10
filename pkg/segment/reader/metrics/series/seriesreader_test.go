// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package series

import (
	"fmt"
	"os"
	"testing"

	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	"github.com/stretchr/testify/assert"
)

func Test_GetAllMetricNames(t *testing.T) {
	// Flush Metric Names to a File.
	ms := &metrics.MetricsSegment{}

	mNamesCount := uint32(1000)
	mNameBase := "metric_"

	filePath := ms.SetMockMetricSegmentMNamesMap(mNamesCount, mNameBase)

	err := ms.FlushMetricNames()
	assert.Nil(t, err)

	_, err = os.Stat(filePath)
	assert.Nil(t, err)

	// Read Metric Names from the File.
	mNamesMap, err := GetAllMetricNames(filePath[:len(filePath)-4])
	assert.Nil(t, err)

	assert.Equal(t, len(mNamesMap), int(mNamesCount))

	for i := 0; i < int(mNamesCount); i++ {
		assert.True(t, mNamesMap[fmt.Sprintf("%s_%d", mNameBase, i)])
	}

	// Cleanup
	_ = os.RemoveAll(filePath)
}
