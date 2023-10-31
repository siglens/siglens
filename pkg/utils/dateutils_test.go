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
