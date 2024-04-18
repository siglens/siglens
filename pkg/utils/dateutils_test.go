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

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_SanitizeHistogramInterval(t *testing.T) {

	// check for 1m interval
	startEpoch := uint64(0)
	endEpoch := uint64(3600_000)
	interval := uint64(1)
	expected := uint64(60_000) // 1m
	actual, err := SanitizeHistogramInterval(startEpoch, endEpoch, interval)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual, "expected  %v, actual %v", expected, actual)

	// check for 5m interval
	startEpoch = uint64(0)
	endEpoch = uint64(3600_000 * 2)
	interval = uint64(1)
	expected = uint64(300_000)
	actual, err = SanitizeHistogramInterval(startEpoch, endEpoch, interval)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual, "expected  %v, actual %v", expected, actual)

	// check for 10m interval
	startEpoch = uint64(0)
	endEpoch = uint64(3600_000 * 9)
	interval = uint64(1)
	expected = uint64(600_000)
	actual, err = SanitizeHistogramInterval(startEpoch, endEpoch, interval)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual, "expected  %v, actual %v", expected, actual)

	// check for 30m interval
	startEpoch = uint64(0)
	endEpoch = uint64(3600_000 * 17)
	interval = uint64(1)
	expected = uint64(1800_000)
	actual, err = SanitizeHistogramInterval(startEpoch, endEpoch, interval)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual, "expected  %v, actual %v", expected, actual)

	// check for 1h interval
	startEpoch = uint64(0)
	endEpoch = uint64(86400_000 * 2)
	interval = uint64(1)
	expected = uint64(3600_000)
	actual, err = SanitizeHistogramInterval(startEpoch, endEpoch, interval)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual, "expected  %v, actual %v", expected, actual)

	// check for 1h interval
	startEpoch = uint64(0)
	endEpoch = uint64(86400_000 * 2)
	interval = uint64(1)
	expected = uint64(3600_000)
	actual, err = SanitizeHistogramInterval(startEpoch, endEpoch, interval)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual, "expected  %v, actual %v", expected, actual)

	// check for 3h interval
	startEpoch = uint64(0)
	endEpoch = uint64(86400_000 * 8)
	interval = uint64(1)
	expected = uint64(10800_000)
	actual, err = SanitizeHistogramInterval(startEpoch, endEpoch, interval)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual, "expected  %v, actual %v", expected, actual)

	// check for 12h interval
	startEpoch = uint64(0)
	endEpoch = uint64(86400_000 * 12)
	interval = uint64(1)
	expected = uint64(43200_000)
	actual, err = SanitizeHistogramInterval(startEpoch, endEpoch, interval)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual, "expected  %v, actual %v", expected, actual)

	// check for 1d interval
	startEpoch = uint64(0)
	endEpoch = uint64(86400_000 * 50)
	interval = uint64(1)
	expected = uint64(86400_000)
	actual, err = SanitizeHistogramInterval(startEpoch, endEpoch, interval)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual, "expected  %v, actual %v", expected, actual)

	// check for 7d interval
	startEpoch = uint64(0)
	endEpoch = uint64(86400_000 * 100)
	interval = uint64(1)
	expected = uint64(604800_000)
	actual, err = SanitizeHistogramInterval(startEpoch, endEpoch, interval)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual, "expected  %v, actual %v", expected, actual)

	// check for 30d interval
	startEpoch = uint64(0)
	endEpoch = uint64(86400_000 * 365 * 2)
	interval = uint64(1)
	expected = uint64(2592000_000)
	actual, err = SanitizeHistogramInterval(startEpoch, endEpoch, interval)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual, "expected  %v, actual %v", expected, actual)

	// check for 90d interval
	startEpoch = uint64(0)
	endEpoch = uint64(86400_000 * 365 * 12)
	interval = uint64(1)
	expected = uint64(7776000_000)
	actual, err = SanitizeHistogramInterval(startEpoch, endEpoch, interval)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual, "expected  %v, actual %v", expected, actual)

	// check for 1y interval
	startEpoch = uint64(0)
	endEpoch = uint64(86400_000 * 365 * 50)
	interval = uint64(1)
	expected = uint64(31536000_000)
	actual, err = SanitizeHistogramInterval(startEpoch, endEpoch, interval)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual, "expected  %v, actual %v", expected, actual)

	// check for 1y interval
	startEpoch = uint64(0)
	endEpoch = uint64(86400_000 * 365 * 50)
	interval = uint64(1)
	expected = uint64(31536000_000)
	actual, err = SanitizeHistogramInterval(startEpoch, endEpoch, interval)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual, "expected  %v, actual %v", expected, actual)

	// check for 10y interval
	startEpoch = uint64(0)
	endEpoch = uint64(86400_000 * 365 * 100)
	interval = uint64(1)
	expected = uint64(315360000_000)
	actual, err = SanitizeHistogramInterval(startEpoch, endEpoch, interval)
	assert.Nil(t, err)
	assert.Equal(t, expected, actual, "expected  %v, actual %v", expected, actual)

}
