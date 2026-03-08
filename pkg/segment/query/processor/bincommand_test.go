// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_findSpan(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())

	spanOpt, err := findSpan(301, 500, 100, nil, "abc")
	assert.Nil(t, err)
	assert.Equal(t, float64(10), spanOpt.BinSpanLength.Num)
	assert.Equal(t, sutils.TMInvalid, spanOpt.BinSpanLength.TimeScale)

	spanOpt, err = findSpan(301, 500, 2, nil, "abc")
	assert.Nil(t, err)
	assert.Equal(t, float64(1000), spanOpt.BinSpanLength.Num)
	assert.Equal(t, sutils.TMInvalid, spanOpt.BinSpanLength.TimeScale)

	minSpan := &structs.BinSpanLength{
		Num:       1001,
		TimeScale: sutils.TMInvalid,
	}

	spanOpt, err = findSpan(301, 500, 100, minSpan, "abc")
	assert.Nil(t, err)
	assert.Equal(t, float64(10000), spanOpt.BinSpanLength.Num)
	assert.Equal(t, sutils.TMInvalid, spanOpt.BinSpanLength.TimeScale)

	minTime := time.Date(2024, time.July, 7, 17, 0, 0, 0, time.UTC).UnixMilli()
	maxTime := time.Date(2024, time.July, 7, 17, 0, 35, 0, time.UTC).UnixMilli()

	spanOpt, err = findSpan(float64(minTime), float64(maxTime), 100, nil, "timestamp")
	assert.Nil(t, err)
	assert.Equal(t, float64(1), spanOpt.BinSpanLength.Num)
	assert.Equal(t, sutils.TMSecond, spanOpt.BinSpanLength.TimeScale)

	spanOpt, err = findSpan(float64(minTime), float64(maxTime), 10, nil, "timestamp")
	assert.Nil(t, err)
	assert.Equal(t, float64(10), spanOpt.BinSpanLength.Num)
	assert.Equal(t, sutils.TMSecond, spanOpt.BinSpanLength.TimeScale)

	minSpan.Num = 2
	minSpan.TimeScale = sutils.TMMinute

	spanOpt, err = findSpan(float64(minTime), float64(maxTime), 10, minSpan, "timestamp")
	assert.Nil(t, err)
	assert.Equal(t, float64(5), spanOpt.BinSpanLength.Num)
	assert.Equal(t, sutils.TMMinute, spanOpt.BinSpanLength.TimeScale)

	maxTime = time.Date(2024, time.July, 7, 17, 2, 35, 0, time.UTC).UnixMilli()

	spanOpt, err = findSpan(float64(minTime), float64(maxTime), 2, nil, "timestamp")
	assert.Nil(t, err)
	assert.Equal(t, float64(5), spanOpt.BinSpanLength.Num)
	assert.Equal(t, sutils.TMMinute, spanOpt.BinSpanLength.TimeScale)
}
