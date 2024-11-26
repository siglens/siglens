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
	"github.com/siglens/siglens/pkg/utils"
	"github.com/stretchr/testify/assert"
)

func verifyNumericStats(t *testing.T, sst *structs.SegStats, buf []byte, idx int) {
	// Min DType
	assert.Equal(t, uint8(sst.Min.Dtype), buf[idx : idx+1][0])
	idx += 1

	// Min Num Value
	assert.Equal(t, sst.Min.CVal, utils.BytesToFloat64LittleEndian(buf[idx:idx+8]))
	idx += 8

	// Max DType
	assert.Equal(t, uint8(sst.Max.Dtype), buf[idx : idx+1][0])
	idx += 1

	// Max Num Value
	assert.Equal(t, sst.Max.CVal, utils.BytesToFloat64LittleEndian(buf[idx:idx+8]))
	idx += 8

	// Sum Num Type
	assert.Equal(t, uint8(sst.NumStats.Sum.Ntype), buf[idx : idx+1][0])
	idx += 1

	// Sum Num Value
	assert.Equal(t, sst.NumStats.Sum.FloatVal, utils.BytesToFloat64LittleEndian(buf[idx:idx+8]))
	idx += 8

	// NumCount
	assert.Equal(t, sst.NumStats.NumericCount, utils.BytesToUint64LittleEndian(buf[idx:idx+8]))
}

func verifyCommon(t *testing.T, sst *structs.SegStats, buf []byte, idx int) int {
	assert.Equal(t, sutils.VERSION_SEGSTATS_BUF_V4[0], buf[idx])
	idx++

	isNumeric := utils.BytesToBoolLittleEndian(buf[idx : idx+1])
	assert.Equal(t, sst.IsNumeric, isNumeric)
	idx++

	count := utils.BytesToUint64LittleEndian(buf[idx : idx+8])
	assert.Equal(t, sst.Count, count)
	idx += 8

	hllDataExpectedSize := uint16(sst.GetHllDataSize())

	hllDataSize := utils.BytesToUint16LittleEndian(buf[idx : idx+4])
	assert.Equal(t, hllDataExpectedSize, hllDataSize)
	idx += 4

	hllData := buf[idx : idx+int(hllDataSize)]
	assert.Equal(t, sst.GetHllBytes(), hllData)
	idx += int(hllDataSize)

	return idx
}

func verifyNonNumeric(t *testing.T, sst *structs.SegStats, buf []byte, idx int) {
	// DType
	assert.Equal(t, uint8(sst.Min.Dtype), buf[idx : idx+1][0])
	idx += 1

	// Min Len
	minLen := uint16(len(sst.Min.CVal.(string)))
	assert.Equal(t, minLen, utils.BytesToUint16LittleEndian(buf[idx:idx+2]))
	idx += 2

	// Min Value
	assert.Equal(t, sst.Min.CVal, string(buf[idx:idx+int(minLen)]))
	idx += int(minLen)

	// Max Len
	maxLen := uint16(len(sst.Max.CVal.(string)))
	assert.Equal(t, uint16(maxLen), utils.BytesToUint16LittleEndian(buf[idx:idx+2]))
	idx += 2

	// Max Value
	assert.Equal(t, sst.Max.CVal, string(buf[idx:idx+int(maxLen)]))
}

func Test_writeSstToBufNumStats(t *testing.T) {
	cname := "mycol1"
	sstMap := make(map[string]*structs.SegStats)
	numRecs := uint64(6)

	addSegStatsStrIngestion(sstMap, cname, []byte("abc"))
	addSegStatsNums(sstMap, cname, sutils.SS_UINT64, 0, uint64(2345), 0, []byte("2345"))
	addSegStatsStrIngestion(sstMap, cname, []byte("def"))
	addSegStatsNums(sstMap, cname, sutils.SS_FLOAT64, 0, 0, float64(345.1), []byte("345.1"))
	addSegStatsStrIngestion(sstMap, cname, []byte("9999"))
	addSegStatsStrIngestion(sstMap, cname, []byte("ghi"))

	assert.Equal(t, numRecs, sstMap[cname].Count)

	sst := sstMap[cname]

	buf := make([]byte, sutils.WIP_SIZE)

	_, err := writeSstToBuf(sst, buf)
	assert.Nil(t, err)

	idx := verifyCommon(t, sst, buf, 0)

	verifyNumericStats(t, sst, buf, idx)
}

func Test_writeSstToBufStringStats(t *testing.T) {
	cname := "mycol1"
	sstMap := make(map[string]*structs.SegStats)
	numRecs := uint64(5)

	addSegStatsStrIngestion(sstMap, cname, []byte("abc"))
	addSegStatsStrIngestion(sstMap, cname, []byte("Abc"))
	addSegStatsStrIngestion(sstMap, cname, []byte("ABCDEF"))
	addSegStatsStrIngestion(sstMap, cname, []byte("ghi"))
	addSegStatsStrIngestion(sstMap, cname, []byte("wxyz"))

	assert.Equal(t, numRecs, sstMap[cname].Count)

	sst := sstMap[cname]

	buf := make([]byte, sutils.WIP_SIZE)

	_, err := writeSstToBuf(sst, buf)
	assert.Nil(t, err)

	idx := verifyCommon(t, sst, buf, 0)

	verifyNonNumeric(t, sst, buf, idx)
}

func Test_writeSstToBufMixed(t *testing.T) {
	cname := "mycol1"
	sstMap := make(map[string]*structs.SegStats)
	numRecs := uint64(4)

	addSegStatsStrIngestion(sstMap, cname, []byte("abc"))
	addSegStatsStrIngestion(sstMap, cname, []byte("123"))
	addSegStatsStrIngestion(sstMap, cname, []byte("def"))
	addSegStatsStrIngestion(sstMap, cname, []byte("345.67"))

	assert.Equal(t, numRecs, sstMap[cname].Count)

	sst := sstMap[cname]

	buf := make([]byte, sutils.WIP_SIZE)

	_, err := writeSstToBuf(sst, buf)
	assert.Nil(t, err)

	idx := verifyCommon(t, sst, buf, 0)

	verifyNumericStats(t, sst, buf, idx)
}
