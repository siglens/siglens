// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package segresults

import (
	"fmt"
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
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
		Sum: sutils.NumTypeEnclosure{Ntype: sutils.SS_DT_SIGNED_NUM,
			IntgrVal: 789},
	}

	segStat := structs.SegStats{
		IsNumeric: true,
		Count:     2345,
		NumStats:  &myNums,
		Min: sutils.CValueEnclosure{Dtype: sutils.SS_DT_FLOAT,
			CVal: float64(456)},
		Max: sutils.CValueEnclosure{Dtype: sutils.SS_DT_FLOAT,
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
