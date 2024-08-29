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

package writer

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer/stats"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/stretchr/testify/assert"
	bbp "github.com/valyala/bytebufferpool"
)

func Test_writeSstToBuf(t *testing.T) {
	cname := "mycol1"
	sstMap := make(map[string]*structs.SegStats)
	numRecs := uint64(2)

	bb := bbp.Get()

	stats.AddSegStatsNums(sstMap, cname, sutils.SS_UINT64, 0, uint64(2345), 0, "2345", bb, nil, false, false)
	stats.AddSegStatsNums(sstMap, cname, sutils.SS_FLOAT64, 0, 0, float64(345.1), "345.1", bb, nil, false, false)

	assert.Equal(t, numRecs, sstMap[cname].Count)

	sst := sstMap[cname]

	buf := make([]byte, sutils.WIP_SIZE)

	_, err := writeSstToBuf(sst, buf)
	assert.Nil(t, err)

	idx := 0
	assert.Equal(t, sutils.VERSION_SEGSTATS[0], buf[idx])
	idx++

	isNumeric := utils.BytesToBoolLittleEndian(buf[idx : idx+1])
	assert.Equal(t, sst.IsNumeric, isNumeric)
	idx++

	count := utils.BytesToUint64LittleEndian(buf[idx : idx+8])
	assert.Equal(t, sst.Count, count)
	idx += 8

	hllDataExpectedSize := uint16(sst.GetHllDataSize())

	hllDataSize := utils.BytesToUint16LittleEndian(buf[idx : idx+2])
	assert.Equal(t, hllDataExpectedSize, hllDataSize)
	idx += 2

	hllData := buf[idx : idx+int(hllDataSize)]
	assert.Equal(t, sst.GetHllBytes(), hllData)
	idx += int(hllDataSize)

	// Min Num Type
	assert.Equal(t, uint8(sst.NumStats.Min.Ntype), buf[idx : idx+1][0])
	idx += 1

	// Min Num Value
	assert.Equal(t, sst.NumStats.Min.FloatVal, utils.BytesToFloat64LittleEndian(buf[idx:idx+8]))
	idx += 8

	// Max Num Type
	assert.Equal(t, uint8(sst.NumStats.Max.Ntype), buf[idx : idx+1][0])
	idx += 1

	// Max Num Value
	assert.Equal(t, sst.NumStats.Max.FloatVal, utils.BytesToFloat64LittleEndian(buf[idx:idx+8]))
	idx += 8

	// Sum Num Type
	assert.Equal(t, uint8(sst.NumStats.Sum.Ntype), buf[idx : idx+1][0])
	idx += 1

	// Sum Num Value
	assert.Equal(t, sst.NumStats.Sum.FloatVal, utils.BytesToFloat64LittleEndian(buf[idx:idx+8]))
}
