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
	"fmt"
	"os"
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	"github.com/stretchr/testify/assert"
)

func Test_ReadMetricNamesBloom(t *testing.T) {
	ms := &metrics.MetricsSegment{}

	filePath := ms.SetMockMetricSegmentMNamesBloom()

	for i := 0; i < 100_000; i++ {
		ms.AddMNameToBloom([]byte("test" + fmt.Sprint(i)))
	}

	err := ms.FlushMetricNamesBloom()
	assert.Nil(t, err)

	_, err = os.Stat(filePath)
	assert.Nil(t, err)

	mm := InitMetricsMicroIndex(&structs.MetricsMeta{})
	err = mm.ReadMetricNamesBloom(filePath)
	assert.Nil(t, err)

	for i := 0; i < 100000; i++ {
		assert.True(t, mm.mNamesBloom.Test([]byte("test"+fmt.Sprint(i))))
	}

	assert.True(t, mm.mBlockSize > 0)

	// cleanup
	_ = os.RemoveAll(filePath)
}
