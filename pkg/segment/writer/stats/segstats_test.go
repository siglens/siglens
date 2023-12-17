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
		AddSegStatsStr(sst, cname, fmt.Sprintf("%v", i), bb, nil, false)
	}

	assert.Equal(t, numRecs, sst[cname].Count)
}

func Test_addSegStatsNums(t *testing.T) {

	cname := "mycol1"
	sst := make(map[string]*SegStats)
	bb := bbp.Get()

	AddSegStatsNums(sst, cname, SS_UINT64, 0, uint64(2345), 0, "2345", bb, nil, false)
	assert.NotEqual(t, SS_DT_FLOAT, sst[cname].NumStats.Min.Ntype)
	assert.Equal(t, int64(2345), sst[cname].NumStats.Min.IntgrVal)

	AddSegStatsNums(sst, cname, SS_FLOAT64, 0, 0, float64(345.1), "345.1", bb, nil, false)
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

	AddSegStatsNums(sst, cname, SS_UINT64, 0, uint64(111), 0, "111", bb, aggColUsage, false)
	AddSegStatsNums(sst, cname, SS_UINT64, 0, uint64(333), 0, "333", bb, aggColUsage, false)
	AddSegStatsNums(sst, cname, SS_UINT64, 0, uint64(222), 0, "222", bb, aggColUsage, false)
	assert.Len(t, sst[cname].Records, 3)
	assert.Equal(t, int64(111), sst[cname].Records[0].CVal)
	assert.Equal(t, int64(333), sst[cname].Records[1].CVal)
	assert.Equal(t, int64(222), sst[cname].Records[2].CVal)

	aggColUsage["latitude"] = NoEvalUsage
	AddSegStatsNums(sst, cname2, SS_FLOAT64, 0, 0, 40.7128, "40.7128", bb, aggColUsage, false)
	AddSegStatsNums(sst, cname2, SS_FLOAT64, 0, 0, -10.5218, "-10.5218", bb, aggColUsage, false)
	assert.Len(t, sst[cname2].Records, 0)
}

func Test_addSegStatsStrForValuesFunc(t *testing.T) {

	cname := "mycol1"
	sst := make(map[string]*structs.SegStats)

	bb := bbp.Get()

	AddSegStatsStr(sst, cname, "b", bb, nil, false)
	AddSegStatsStr(sst, cname, "d", bb, nil, false)

	assert.Nil(t, sst[cname].StringStats)

	AddSegStatsStr(sst, cname, "a", bb, nil, true)
	AddSegStatsStr(sst, cname, "c", bb, nil, true)

	assert.NotNil(t, sst[cname].StringStats)
	assert.NotNil(t, sst[cname].StringStats.StrSet)
	assert.Equal(t, map[string]struct{}{"a": {}, "c": {}}, sst[cname].StringStats.StrSet)
}
