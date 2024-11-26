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

package segread

import (
	"errors"
	"fmt"
	"os"
	"path"
	"reflect"
	"strconv"
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/stretchr/testify/assert"
)

func Test_sstReadWrite(t *testing.T) {

	fname := "segkey-1.sst"

	_ = os.MkdirAll(path.Dir(fname), 0755)

	myNums := structs.NumericStats{
		NumericCount: 123,
		Sum: utils.NumTypeEnclosure{Ntype: utils.SS_DT_SIGNED_NUM,
			IntgrVal: 789},
	}

	inSst := structs.SegStats{
		IsNumeric: true,
		Count:     2345,
		Min: utils.CValueEnclosure{Dtype: utils.SS_DT_SIGNED_NUM,
			CVal: int64(456)},
		Max: utils.CValueEnclosure{Dtype: utils.SS_DT_FLOAT,
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

	assert.Equal(t, inSst.GetHllBytes(), outSst.GetHllBytes())

	assert.Equal(t, inSst.Min, outSst.Min)

	assert.Equal(t, inSst.Max, outSst.Max)

	_ = os.RemoveAll(fname)
}

func TestGetSegList(t *testing.T) {
	// Utility function to generate a string list of a specific size
	generateStringList := func(size int) []string {
		list := make([]string, size)
		for i := 0; i < size; i++ {
			list[i] = "string" + strconv.Itoa(i)
		}
		return list
	}

	tests := []struct {
		name           string
		runningSegStat *structs.SegStats
		currSegStat    *structs.SegStats
		expectedRes    *utils.CValueEnclosure
		expectedErr    error
	}{
		{
			name:           "currSegStat is nil",
			runningSegStat: nil,
			currSegStat:    nil,
			expectedRes:    &utils.CValueEnclosure{Dtype: utils.SS_DT_STRING_SLICE, CVal: []string{}},
			expectedErr:    errors.New("GetSegList: currSegStat does not contain string list <nil>"),
		},
		{
			name:           "runningSegStat is nil, currSegStat has small list",
			runningSegStat: nil,
			currSegStat: &structs.SegStats{
				StringStats: &structs.StringStats{
					StrList: generateStringList(5),
				},
			},
			expectedRes: &utils.CValueEnclosure{
				Dtype: utils.SS_DT_STRING_SLICE,
				CVal:  generateStringList(5),
			},
			expectedErr: nil,
		},
		{
			name:           "runningSegStat is nil, currSegStat has large list",
			runningSegStat: nil,
			currSegStat: &structs.SegStats{
				StringStats: &structs.StringStats{
					StrList: generateStringList(utils.MAX_SPL_LIST_SIZE + 5),
				},
			},
			expectedRes: &utils.CValueEnclosure{
				Dtype: utils.SS_DT_STRING_SLICE,
				CVal:  generateStringList(utils.MAX_SPL_LIST_SIZE),
			},
			expectedErr: nil,
		},
		{
			name: "both runningSegStat and currSegStat have lists",
			runningSegStat: &structs.SegStats{
				StringStats: &structs.StringStats{
					StrList: generateStringList(3),
				},
			},
			currSegStat: &structs.SegStats{
				StringStats: &structs.StringStats{
					StrList: generateStringList(4),
				},
			},
			expectedRes: &utils.CValueEnclosure{
				Dtype: utils.SS_DT_STRING_SLICE,
				CVal:  append(generateStringList(3), generateStringList(4)...),
			},
			expectedErr: nil,
		},
		{
			name: "empty string lists",
			runningSegStat: &structs.SegStats{
				StringStats: &structs.StringStats{
					StrList: []string{},
				},
			},
			currSegStat: &structs.SegStats{
				StringStats: &structs.StringStats{
					StrList: []string{},
				},
			},
			expectedRes: &utils.CValueEnclosure{
				Dtype: utils.SS_DT_STRING_SLICE,
				CVal:  []string{},
			},
			expectedErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := GetSegList(tt.runningSegStat, tt.currSegStat)
			if !reflect.DeepEqual(res, tt.expectedRes) {
				t.Errorf("Expected result %v, got %v", tt.expectedRes, res)
			}
			if !reflect.DeepEqual(err, tt.expectedErr) {
				t.Errorf("Expected error %v, got %v", tt.expectedErr, err)
			}
		})
	}
}

func TestGetSegAvg_CurrSegStatIsNil(t *testing.T) {
	runningSegStat := &structs.SegStats{}
	currSegStat := (*structs.SegStats)(nil)

	expected := &utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_FLOAT,
		IntgrVal: 0,
	}
	result, err := GetSegAvg(runningSegStat, currSegStat)

	assert.Error(t, err)
	assert.Equal(t, expected, result)
}

func TestGetSegAvg_CurrSegStatIsNonNumeric(t *testing.T) {
	runningSegStat := &structs.SegStats{}
	currSegStat := &structs.SegStats{
		IsNumeric: false,
	}

	expected := &utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_FLOAT,
		IntgrVal: 0,
	}
	result, err := GetSegAvg(runningSegStat, currSegStat)

	assert.Error(t, err)
	assert.Equal(t, expected, result)
}

func TestGetSegAvg_FirstSegmentWithIntegerStats(t *testing.T) {
	currSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     2,
		NumStats: &structs.NumericStats{
			NumericCount: 2,
			Sum: utils.NumTypeEnclosure{
				Ntype:    utils.SS_DT_SIGNED_NUM,
				IntgrVal: 20,
			},
		},
	}

	expected := &utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_FLOAT,
		FloatVal: 10.0,
	}
	result, err := GetSegAvg(nil, currSegStat)

	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestGetSegAvg_FirstSegmentWithFloatStats(t *testing.T) {
	currSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     2,
		NumStats: &structs.NumericStats{
			NumericCount: 2,
			Sum: utils.NumTypeEnclosure{
				Ntype:    utils.SS_DT_FLOAT,
				FloatVal: 20.0,
			},
		},
	}

	expected := &utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_FLOAT,
		FloatVal: 10.0,
	}
	result, err := GetSegAvg(nil, currSegStat)

	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestGetSegAvg_RunningSegmentIntAndCurrentSegmentFloat(t *testing.T) {
	runningSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     3,
		NumStats: &structs.NumericStats{
			NumericCount: 3,
			Sum: utils.NumTypeEnclosure{
				Ntype:    utils.SS_DT_SIGNED_NUM,
				IntgrVal: 30,
			},
		},
	}

	currSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     2,
		NumStats: &structs.NumericStats{
			NumericCount: 2,
			Sum: utils.NumTypeEnclosure{
				Ntype:    utils.SS_DT_FLOAT,
				FloatVal: 20.0,
			},
		},
	}

	expected := &utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_FLOAT,
		FloatVal: 10.0,
	}
	result, err := GetSegAvg(runningSegStat, currSegStat)

	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestGetSegAvg_RunningAndCurrentSegmentWithFloatStats(t *testing.T) {
	runningSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     2,
		NumStats: &structs.NumericStats{
			NumericCount: 2,
			Sum: utils.NumTypeEnclosure{
				Ntype:    utils.SS_DT_FLOAT,
				FloatVal: 20.0,
			},
		},
	}

	currSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     2,
		NumStats: &structs.NumericStats{
			NumericCount: 2,
			Sum: utils.NumTypeEnclosure{
				Ntype:    utils.SS_DT_FLOAT,
				FloatVal: 10.0,
			},
		},
	}

	expected := &utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_FLOAT,
		FloatVal: 7.5,
	}
	result, err := GetSegAvg(runningSegStat, currSegStat)

	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestGetSegAvg_RunningSegmentFloatAndCurrentSegmentInt(t *testing.T) {
	runningSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     2,
		NumStats: &structs.NumericStats{
			NumericCount: 2,
			Sum: utils.NumTypeEnclosure{
				Ntype:    utils.SS_DT_FLOAT,
				FloatVal: 20.0,
			},
		},
	}

	currSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     2,
		NumStats: &structs.NumericStats{
			NumericCount: 2,
			Sum: utils.NumTypeEnclosure{
				Ntype:    utils.SS_DT_SIGNED_NUM,
				IntgrVal: 10,
			},
		},
	}

	expected := &utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_FLOAT,
		FloatVal: 7.5,
	}
	result, err := GetSegAvg(runningSegStat, currSegStat)

	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestGetSegAvg_BothSegmentsWithIntStats(t *testing.T) {
	runningSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     3,
		NumStats: &structs.NumericStats{
			NumericCount: 3,
			Sum: utils.NumTypeEnclosure{
				Ntype:    utils.SS_DT_SIGNED_NUM,
				IntgrVal: 30,
			},
		},
	}

	currSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     2,
		NumStats: &structs.NumericStats{
			NumericCount: 2,
			Sum: utils.NumTypeEnclosure{
				Ntype:    utils.SS_DT_SIGNED_NUM,
				IntgrVal: 20,
			},
		},
	}

	expected := &utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_FLOAT,
		FloatVal: 10.0,
	}
	result, err := GetSegAvg(runningSegStat, currSegStat)

	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestGetAverage_IntegerSum(t *testing.T) {
	sum := utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_SIGNED_NUM,
		IntgrVal: 20,
	}
	count := uint64(4)

	expected := 5.0
	result, err := getAverage(sum, count)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestGetAverage_FloatSum(t *testing.T) {
	sum := utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_FLOAT,
		FloatVal: 20.0,
	}
	count := uint64(4)

	expected := 5.0
	result, err := getAverage(sum, count)
	assert.NoError(t, err)
	assert.Equal(t, expected, result)
}

func TestGetAverage_ZeroCount(t *testing.T) {
	sum := utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_FLOAT,
		FloatVal: 20.0,
	}
	count := uint64(0)

	_, err := getAverage(sum, count)
	assert.Error(t, err)
}

func TestGetAverage_InvalidType(t *testing.T) {
	sum := utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_STRING,
		FloatVal: 20.0,
	}
	count := uint64(4)

	_, err := getAverage(sum, count)
	assert.Error(t, err)
}

func Test_GetSegMin(t *testing.T) {
	runningSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     3,
		Min: utils.CValueEnclosure{
			Dtype: utils.SS_DT_SIGNED_NUM,
			CVal:  int64(30),
		},
	}

	currSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     2,
		Min: utils.CValueEnclosure{
			Dtype: utils.SS_DT_FLOAT,
			CVal:  float64(20),
		},
	}

	expected := &utils.CValueEnclosure{
		Dtype: utils.SS_DT_FLOAT,
		CVal:  20.0,
	}
	result, err := GetSegMin(runningSegStat, currSegStat)

	assert.Nil(t, err)
	assert.Equal(t, expected, result)

	runningSegStat2 := &structs.SegStats{
		IsNumeric: false,
		Min: utils.CValueEnclosure{
			Dtype: utils.SS_DT_STRING,
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
		Max: utils.CValueEnclosure{
			Dtype: utils.SS_DT_SIGNED_NUM,
			CVal:  int64(30),
		},
	}

	currSegStat := &structs.SegStats{
		IsNumeric: true,
		Count:     2,
		Max: utils.CValueEnclosure{
			Dtype: utils.SS_DT_FLOAT,
			CVal:  float64(20),
		},
	}

	expected := &utils.CValueEnclosure{
		Dtype: utils.SS_DT_FLOAT,
		CVal:  30.0,
	}
	result, err := GetSegMax(runningSegStat, currSegStat)

	assert.Nil(t, err)
	assert.Equal(t, expected, result)

	runningSegStat2 := &structs.SegStats{
		IsNumeric: false,
		Max: utils.CValueEnclosure{
			Dtype: utils.SS_DT_STRING,
			CVal:  "abc",
		},
	}

	result, err = GetSegMax(runningSegStat2, runningSegStat)
	assert.Nil(t, err)
	assert.Equal(t, expected, result)
	assert.True(t, runningSegStat2.IsNumeric)
	assert.Equal(t, runningSegStat.NumStats, runningSegStat2.NumStats)
}
