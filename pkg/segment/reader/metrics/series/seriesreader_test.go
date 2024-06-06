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
