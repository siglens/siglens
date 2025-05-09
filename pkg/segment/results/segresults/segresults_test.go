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

package segresults

import (
	"fmt"
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_Remote_Stats(t *testing.T) {

	evalStatsMetaData := EvalStatsMetaData{
		RangeStat: &structs.RangeStat{
			Min: -1,
			Max: 1,
		},
		AvgStat: &structs.AvgStat{
			Sum:   10,
			Count: 2,
		},
		StrSet: map[string]struct{}{
			"test":  {},
			"test2": {},
		},
		StrList:       []string{"abc", "def"},
		MeasureResult: 123,
	}

	myNums := structs.NumericStats{
		Sum: segutils.NumTypeEnclosure{Ntype: segutils.SS_DT_SIGNED_NUM,
			IntgrVal: 789},
	}

	segStat := structs.SegStats{
		IsNumeric: true,
		Count:     2345,
		NumStats:  &myNums,
		Min: segutils.CValueEnclosure{Dtype: segutils.SS_DT_FLOAT,
			CVal: float64(456)},
		Max: segutils.CValueEnclosure{Dtype: segutils.SS_DT_FLOAT,
			CVal: 23.4567},
		StringStats: &structs.StringStats{
			StrSet: map[string]struct{}{
				"str1": {},
				"str2": {},
			},
			StrList: []string{
				"str1",
				"str2",
			},
		},
	}
	segStat.CreateNewHll()

	for i := 0; i < 3200; i++ {
		segStat.InsertIntoHll([]byte(fmt.Sprintf("mystr:%v", i)))
	}

	evalStats := map[string]EvalStatsMetaData{
		"eval1": evalStatsMetaData,
	}

	remoteStats := &RemoteStats{
		EvalStats: evalStats,
		SegStats:  []*structs.SegStats{&segStat},
	}

	expectedHllBytes := segStat.Hll.ToBytes()

	remoteStatsJson, err := remoteStats.RemoteStatsToJSON()
	assert.Nil(t, err)
	assert.NotNil(t, remoteStatsJson)
	assert.Equal(t, remoteStats.EvalStats, remoteStatsJson.EvalStats)

	assert.Equal(t, 1, len(remoteStatsJson.SegStats))
	assert.Equal(t, remoteStats.SegStats[0].Count, remoteStatsJson.SegStats[0].Count)
	assert.Equal(t, remoteStats.SegStats[0].IsNumeric, remoteStatsJson.SegStats[0].IsNumeric)
	assert.Equal(t, remoteStats.SegStats[0].NumStats, remoteStatsJson.SegStats[0].NumStats)
	assert.Equal(t, remoteStats.SegStats[0].StringStats, remoteStatsJson.SegStats[0].StringStats)
	assert.Equal(t, expectedHllBytes, remoteStatsJson.SegStats[0].RawHll)

	decodedRemoteStats, err := remoteStatsJson.ToRemoteStats()
	assert.Nil(t, err)
	assert.NotNil(t, decodedRemoteStats)

	assert.Equal(t, remoteStats, decodedRemoteStats)
}
