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

package stats

import (
	"fmt"
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
	bbp "github.com/valyala/bytebufferpool"
)

func Test_addSegStatsStr(t *testing.T) {

	cname := "mycol1"
	sst := make(map[string]*structs.SegStats)
	numRecs := uint64(2000)

	bb := bbp.Get()

	for i := uint64(0); i < numRecs; i++ {
		AddSegStatsStr(sst, cname, fmt.Sprintf("%v", i), bb, nil, false, false)
	}

	assert.Equal(t, numRecs, sst[cname].Count)
}

func Test_addSegStatsNums(t *testing.T) {

	cname := "mycol1"
	sst := make(map[string]*SegStats)
	bb := bbp.Get()

	AddSegStatsNums(sst, cname, SS_UINT64, 0, uint64(2345), 0, "2345", bb, nil, false, false)
	assert.NotEqual(t, SS_DT_FLOAT, sst[cname].NumStats.Min.Ntype)
	assert.Equal(t, int64(2345), sst[cname].NumStats.Min.IntgrVal)

	AddSegStatsNums(sst, cname, SS_FLOAT64, 0, 0, float64(345.1), "345.1", bb, nil, false, false)
	assert.Equal(t, SS_DT_FLOAT, sst[cname].NumStats.Min.Ntype)
	assert.Equal(t, float64(345.1), sst[cname].NumStats.Min.FloatVal)

	assert.Equal(t, SS_DT_FLOAT, sst[cname].NumStats.Sum.Ntype)
	assert.Equal(t, float64(345.1+2345), sst[cname].NumStats.Sum.FloatVal)

}

func Test_addSegStatsNumsForEvalFunc(t *testing.T) {

	cname := "duration"
	cname2 := "latitude"
	sst := make(map[string]*SegStats)
	bb := bbp.Get()

	aggColUsage := make(map[string]AggColUsageMode, 0)
	aggColUsage["duration"] = WithEvalUsage
	aggColUsage["latitude"] = NoEvalUsage

	AddSegStatsNums(sst, cname, SS_UINT64, 0, uint64(111), 0, "111", bb, aggColUsage, false, false)
	AddSegStatsNums(sst, cname, SS_UINT64, 0, uint64(333), 0, "333", bb, aggColUsage, false, false)
	AddSegStatsNums(sst, cname, SS_UINT64, 0, uint64(222), 0, "222", bb, aggColUsage, false, false)
	assert.Len(t, sst[cname].Records, 3)
	assert.Equal(t, int64(111), sst[cname].Records[0].CVal)
	assert.Equal(t, int64(333), sst[cname].Records[1].CVal)
	assert.Equal(t, int64(222), sst[cname].Records[2].CVal)

	aggColUsage["latitude"] = NoEvalUsage
	AddSegStatsNums(sst, cname2, SS_FLOAT64, 0, 0, 40.7128, "40.7128", bb, aggColUsage, false, false)
	AddSegStatsNums(sst, cname2, SS_FLOAT64, 0, 0, -10.5218, "-10.5218", bb, aggColUsage, false, false)
	assert.Len(t, sst[cname2].Records, 0)
}

func Test_addSegStatsStrForValuesFunc(t *testing.T) {

	cname := "mycol1"
	sst := make(map[string]*structs.SegStats)

	bb := bbp.Get()

	AddSegStatsStr(sst, cname, "b", bb, nil, false, false)
	AddSegStatsStr(sst, cname, "d", bb, nil, false, false)

	assert.Nil(t, sst[cname].StringStats)

	AddSegStatsStr(sst, cname, "a", bb, nil, true, false)
	AddSegStatsStr(sst, cname, "c", bb, nil, true, false)

	assert.NotNil(t, sst[cname].StringStats)
	assert.NotNil(t, sst[cname].StringStats.StrSet)
	assert.Equal(t, map[string]struct{}{"a": {}, "c": {}}, sst[cname].StringStats.StrSet)
}

func Test_mergeSegStats(t *testing.T) {
	map1 := make(map[string]*SegStats)
	map2 := make(map[string]*SegStats)

	map1["col1"] = &SegStats{IsNumeric: true, Count: 1}
	map2["col1"] = &SegStats{IsNumeric: true, Count: 2}
	expectedCol1 := &SegStats{IsNumeric: true, Count: 3}

	map1["col2"] = &SegStats{IsNumeric: true, Count: 42}
	expectedCol2 := &SegStats{IsNumeric: true, Count: 42}

	map2["col3"] = &SegStats{IsNumeric: true, Count: 10}
	expectedCol3 := &SegStats{IsNumeric: true, Count: 10}

	mergedMap := MergeSegStats(map1, map2)
	assert.Equal(t, 3, len(mergedMap))
	assert.Equal(t, expectedCol1, mergedMap["col1"])
	assert.Equal(t, expectedCol2, mergedMap["col2"])
	assert.Equal(t, expectedCol3, mergedMap["col3"])
}
