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

package processor

import (
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_findSpan(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())

	spanOpt, err := findSpan(301, 500, 100, nil, "abc")
	assert.Nil(t, err)
	assert.Equal(t, float64(10), spanOpt.BinSpanLength.Num)
	assert.Equal(t, utils.TMInvalid, spanOpt.BinSpanLength.TimeScale)

	spanOpt, err = findSpan(301, 500, 2, nil, "abc")
	assert.Nil(t, err)
	assert.Equal(t, float64(1000), spanOpt.BinSpanLength.Num)
	assert.Equal(t, utils.TMInvalid, spanOpt.BinSpanLength.TimeScale)

	minSpan := &structs.BinSpanLength{
		Num:       1001,
		TimeScale: utils.TMInvalid,
	}

	spanOpt, err = findSpan(301, 500, 100, minSpan, "abc")
	assert.Nil(t, err)
	assert.Equal(t, float64(10000), spanOpt.BinSpanLength.Num)
	assert.Equal(t, utils.TMInvalid, spanOpt.BinSpanLength.TimeScale)

	minTime := time.Date(2024, time.July, 7, 17, 0, 0, 0, time.UTC).UnixMilli()
	maxTime := time.Date(2024, time.July, 7, 17, 0, 35, 0, time.UTC).UnixMilli()

	spanOpt, err = findSpan(float64(minTime), float64(maxTime), 100, nil, "timestamp")
	assert.Nil(t, err)
	assert.Equal(t, float64(1), spanOpt.BinSpanLength.Num)
	assert.Equal(t, utils.TMSecond, spanOpt.BinSpanLength.TimeScale)

	spanOpt, err = findSpan(float64(minTime), float64(maxTime), 10, nil, "timestamp")
	assert.Nil(t, err)
	assert.Equal(t, float64(10), spanOpt.BinSpanLength.Num)
	assert.Equal(t, utils.TMSecond, spanOpt.BinSpanLength.TimeScale)

	minSpan.Num = 2
	minSpan.TimeScale = utils.TMMinute

	spanOpt, err = findSpan(float64(minTime), float64(maxTime), 10, minSpan, "timestamp")
	assert.Nil(t, err)
	assert.Equal(t, float64(5), spanOpt.BinSpanLength.Num)
	assert.Equal(t, utils.TMMinute, spanOpt.BinSpanLength.TimeScale)

	maxTime = time.Date(2024, time.July, 7, 17, 2, 35, 0, time.UTC).UnixMilli()

	spanOpt, err = findSpan(float64(minTime), float64(maxTime), 2, nil, "timestamp")
	assert.Nil(t, err)
	assert.Equal(t, float64(5), spanOpt.BinSpanLength.Num)
	assert.Equal(t, utils.TMMinute, spanOpt.BinSpanLength.TimeScale)
}
