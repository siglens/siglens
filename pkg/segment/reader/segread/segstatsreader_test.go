// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package segread

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/stretchr/testify/assert"
)

func Test_sstReadWrite(t *testing.T) {

	fname := "segkey-1.sst"

	_ = os.MkdirAll(path.Dir(fname), 0755)

	myNums := structs.NumericStats{
		NumericCount: 123,
		Sum: sutils.NumTypeEnclosure{Ntype: sutils.SS_DT_SIGNED_NUM,
			IntgrVal: 789},
	}

	inSst := structs.SegStats{
		IsNumeric: true,
		Count:     2345,
		Min: sutils.CValueEnclosure{Dtype: sutils.SS_DT_SIGNED_NUM,
			CVal: int64(456)},
		Max: sutils.CValueEnclosure{Dtype: sutils.SS_DT_FLOAT,
			CVal: 23.4567},
		NumStats: &myNums,
	}
	inSst.CreateNewHll()

	for i := 0; i < 3200; i++ {
		inSst.InsertIntoHll([]byte(fmt.Sprintf("mystr:%v", i)))
	}

	allSst := make(map[string]*structs.SegStats)

	allSst["col-a"] = &inSst
	allSst["col-b"] = &inSst

	ss := writer.NewSegStore(0)
	ss.SegmentKey = "segkey-1"
	ss.AllSst = allSst

	err := ss.FlushSegStats()
	assert.Nil(t, err)

	allSstMap, err := ReadSegStats("segkey-1", 123)
	assert.Nil(t, err)

	outSst, pres := allSstMap["col-b"]

	assert.True(t, pres)

	assert.Equal(t, inSst.IsNumeric, outSst.IsNumeric)

	assert.Equal(t, inSst.Count, outSst.Count)

	assert.Equal(t, inSst.NumStats, outSst.NumStats)

	assert.Equal(t, inSst.GetHllCardinality(), outSst.GetHllCardinality())

	assert.Equal(t, inSst.GetHllError(), outSst.GetHllError())

	assert.Equal(t, inSst.GetHllBytes(), outSst.GetHllBytes())

	assert.Equal(t, inSst.Min, outSst.Min)

	assert.Equal(t, inSst.Max, outSst.Max)

	_ = os.RemoveAll(fname)
}

func Test_GetSegMin(t *testing.T) {
	runningSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     3,
		Min: sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_SIGNED_NUM,
			CVal:  int64(30),
		},
	}

	currSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     2,
		Min: sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_FLOAT,
			CVal:  float64(20),
		},
	}

	expected := &sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_FLOAT,
		CVal:  20.0,
	}
	result, err := GetSegMin(runningSegStat, currSegStat)

	assert.Nil(t, err)
	assert.Equal(t, expected, result)

	runningSegStat2 := &structs.SegStats{
		IsNumeric: false,
		Min: sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_STRING,
			CVal:  "abc",
		},
	}

	result, err = GetSegMin(runningSegStat2, runningSegStat)
	assert.Nil(t, err)
	assert.Equal(t, expected, result)
	assert.True(t, runningSegStat2.IsNumeric)
	assert.Equal(t, runningSegStat.NumStats, runningSegStat2.NumStats)
}

func Test_GetSegMax(t *testing.T) {
	runningSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     3,
		Max: sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_SIGNED_NUM,
			CVal:  int64(30),
		},
	}

	currSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     2,
		Max: sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_FLOAT,
			CVal:  float64(20),
		},
	}

	expected := &sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_FLOAT,
		CVal:  30.0,
	}
	result, err := GetSegMax(runningSegStat, currSegStat)

	assert.Nil(t, err)
	assert.Equal(t, expected, result)

	runningSegStat2 := &structs.SegStats{
		IsNumeric: false,
		Max: sutils.CValueEnclosure{
			Dtype: sutils.SS_DT_STRING,
			CVal:  "abc",
		},
	}

	result, err = GetSegMax(runningSegStat2, runningSegStat)
	assert.Nil(t, err)
	assert.Equal(t, expected, result)
	assert.True(t, runningSegStat2.IsNumeric)
	assert.Equal(t, runningSegStat.NumStats, runningSegStat2.NumStats)
}
