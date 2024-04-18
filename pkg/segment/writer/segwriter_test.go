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
	"fmt"
	"strconv"

	//	"encoding/json"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/stretchr/testify/assert"

	//	"reflect"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
)

var rangeIndex map[string]*Numbers

func Test_isTimeRangeOverlapping(t *testing.T) {
	baseStart := uint64(4)
	baseEnd := uint64(8)
	cases := []struct {
		name     string
		start    uint64
		end      uint64
		expected bool
	}{
		{
			name:     "Completely before base range",
			start:    1,
			end:      2,
			expected: false,
		},
		{
			name:     "Touching start",
			start:    1,
			end:      4,
			expected: true,
		},
		{
			name:     "End within base range",
			start:    1,
			end:      6,
			expected: true,
		},
		{
			name:     "Start and end both within base range",
			start:    5,
			end:      7,
			expected: true,
		},
		{
			name:     "Touching end",
			start:    6,
			end:      8,
			expected: true,
		},
		{
			name:     "Start within base range",
			start:    6,
			end:      9,
			expected: true,
		},
		{
			name:     "Completely after base range",
			start:    9,
			end:      12,
			expected: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			isOverlapping := isTimeRangeOverlapping(baseStart, baseEnd, c.start, c.end)

			if isOverlapping != c.expected {
				t.Errorf("Failed: Expected=%v, got=%v, test name=%v", c.expected, isOverlapping, c.name)
			}
		})
	}
}

func isTimeRangeOverlapping(start1, end1, start2, end2 uint64) bool {
	return utils.Max(start1, start2) <= utils.Min(end1, end2)
}

func Test_getBaseSegDir(t *testing.T) {
	config.InitializeDefaultConfig()
	virtualTableName := "evts"
	streamid := "10005995996882630313"
	nextsuff_idx := uint64(1)
	basedir := getActiveBaseSegDir(streamid, virtualTableName, nextsuff_idx)
	assert.EqualValues(t, "data/"+config.GetHostID()+"/active/"+virtualTableName+"/"+streamid+"/1/", basedir)
}

func Test_getFinalBaseSegDir(t *testing.T) {
	config.InitializeDefaultConfig()
	virtualTableName := "evts"
	streamid := "10005995996882630313"
	nextsuff_idx := uint64(1)
	basedir := getFinalBaseSegDir(streamid, virtualTableName, nextsuff_idx)
	assert.EqualValues(t, "data/"+config.GetHostID()+"/final/"+virtualTableName+"/"+streamid+"/1/", basedir)
}

func Test_getNumberTypeAndVal(t *testing.T) {
	cases := []struct {
		input   string
		numType SS_IntUintFloatTypes
	}{
		{`-99`, SS_INT8},
		{`99`, SS_UINT8},
		{`-128`, SS_INT8},
		{`127`, SS_UINT8},
		{`-129`, SS_INT16},
		{`128`, SS_UINT8},
		{`-32768`, SS_INT16},
		{`32767`, SS_UINT16},
		{`-32769`, SS_INT32},
		{`32769`, SS_UINT16},
		{`-2147483648`, SS_INT32},
		{`-2147483648`, SS_INT32},
		{`-2147483649`, SS_INT64},
		{`2147483649`, SS_UINT32},
		{`-0`, SS_UINT8},
		{`0`, SS_UINT8},
		{`0.0`, SS_UINT8},
		{`65535`, SS_UINT16},
		{`65536`, SS_UINT32},
		{`4294967295`, SS_UINT32},
		{`4294967296`, SS_UINT64},
		{`-4294967296`, SS_INT64},
		{`124294967296`, SS_UINT64},
		{`-124294967296`, SS_INT64},
		{`-12429496729600`, SS_INT64},
		{`12429496729600`, SS_UINT64},
		{`-124294967.29600`, SS_FLOAT64},
		{`124294967.29600`, SS_FLOAT64},
	}

	for i, test := range cases {
		numType, _, _, _ := GetNumberTypeAndVal(test.input)
		assert.Equal(t, test.numType, numType, fmt.Sprintf("getNumberTypeAndVal testId: %d: Failed: actual: [%v], expected: [%v]", i+1, numType, test.numType))

	}
}

func getMin(key string) (interface{}, RangeNumType) {
	ri := rangeIndex[key]
	riNumType := ri.NumType
	switch riNumType {
	case RNT_UNSIGNED_INT:
		return ri.Min_uint64, riNumType
	case RNT_SIGNED_INT:
		return ri.Min_int64, riNumType
	case RNT_FLOAT64:
		return ri.Min_float64, riNumType
	}
	return nil, 0
}

func getMax(key string) (interface{}, RangeNumType) {
	ri := rangeIndex[key]
	riNumType := ri.NumType
	switch riNumType {
	case RNT_UNSIGNED_INT:
		return ri.Max_uint64, riNumType
	case RNT_SIGNED_INT:
		return ri.Max_int64, riNumType
	case RNT_FLOAT64:
		return ri.Max_float64, riNumType
	}
	return nil, 0
}

func Test_wrapperForUpdateRange(t *testing.T) {
	rangeIndex = map[string]*Numbers{}
	cases := []struct {
		key                  string
		numstr               string
		expectedMinVal       interface{}
		expectedMaxVal       interface{}
		expectedRangeNumType RangeNumType
	}{
		{key: "ID", numstr: "1", expectedMinVal: 1, expectedMaxVal: 1, expectedRangeNumType: RNT_UNSIGNED_INT},
		{key: "ID", numstr: "-1", expectedMinVal: -1, expectedMaxVal: 1, expectedRangeNumType: RNT_SIGNED_INT},
		{key: "ID", numstr: "10", expectedMinVal: -1, expectedMaxVal: 10, expectedRangeNumType: RNT_SIGNED_INT},
		{key: "AMOUNT", numstr: "655", expectedMinVal: 655, expectedMaxVal: 655, expectedRangeNumType: RNT_UNSIGNED_INT},
		{key: "AMOUNT", numstr: "-6", expectedMinVal: -6, expectedMaxVal: 655, expectedRangeNumType: RNT_SIGNED_INT},
		{key: "AMOUNT", numstr: "-6.5", expectedMinVal: -6.5, expectedMaxVal: 655, expectedRangeNumType: RNT_FLOAT64},
		{key: "AMOUNT", numstr: "700", expectedMinVal: -6.5, expectedMaxVal: 700, expectedRangeNumType: RNT_FLOAT64},
		{key: "AMOUNT", numstr: "712.5", expectedMinVal: -6.5, expectedMaxVal: 712.5, expectedRangeNumType: RNT_FLOAT64},
		{key: "SCORE", numstr: "80.2", expectedMinVal: 80.2, expectedMaxVal: 80.2, expectedRangeNumType: RNT_FLOAT64},
		{key: "SCORE", numstr: "-10", expectedMinVal: -10, expectedMaxVal: 80.2, expectedRangeNumType: RNT_FLOAT64},
		{key: "SCORE", numstr: "-1.3", expectedMinVal: -10, expectedMaxVal: 80.2, expectedRangeNumType: RNT_FLOAT64},
		{key: "SCORE", numstr: "100", expectedMinVal: -10, expectedMaxVal: 100, expectedRangeNumType: RNT_FLOAT64},
	}
	for i, test := range cases {
		wrapperForUpdateRange(test.key, test.numstr, rangeIndex)
		actualMinVal, numType := getMin(test.key)
		actualMaxVal, _ := getMax(test.key)
		switch numType {
		case RNT_UNSIGNED_INT:
			assert.EqualValues(t, test.expectedMinVal, actualMinVal.(uint64), fmt.Sprintf("Comparison failed, test=%v, expected=%v, actual=%v", i+1, test.expectedMinVal, actualMinVal))
			assert.EqualValues(t, test.expectedMaxVal, actualMaxVal.(uint64), fmt.Sprintf("Comparison failed, test=%v, expected=%v, actual=%v", i+1, test.expectedMaxVal, actualMaxVal))
		case RNT_SIGNED_INT:
			assert.EqualValues(t, test.expectedMinVal, actualMinVal.(int64), fmt.Sprintf("Comparison failed, test=%v, expected=%v, actual=%v", i+1, test.expectedMinVal, actualMinVal))
			assert.EqualValues(t, test.expectedMaxVal, actualMaxVal.(int64), fmt.Sprintf("Comparison failed, test=%v, expected=%v, actual=%v", i+1, test.expectedMaxVal, actualMaxVal))
		case RNT_FLOAT64:
			assert.EqualValues(t, test.expectedMinVal, actualMinVal.(float64), fmt.Sprintf("Comparison failed, test=%v, expected=%v, actual=%v", i+1, test.expectedMinVal, actualMinVal))
			assert.EqualValues(t, test.expectedMaxVal, actualMaxVal.(float64), fmt.Sprintf("Comparison failed, test=%v, expected=%v, actual=%v", i+1, test.expectedMaxVal, actualMaxVal))
		}
		assert.EqualValues(t, test.expectedRangeNumType, numType, fmt.Sprintf("Comparison failed, test=%v, expected=%v, actual=%v", i+1, test.expectedRangeNumType, numType))

	}
}

func Test_addToBlockBloom(t *testing.T) {
	cases := []struct {
		fullWord         []byte   // full word to add
		expectedMatches  []string // expected words to pass bloom check
		expectedAddCount uint32   // expected number of words added to the bloom
		matchedToFail    []string // words that should not pass bloom check
	}{
		{fullWord: []byte("nosubword"), expectedMatches: []string{"nosubword"}, expectedAddCount: 1, matchedToFail: []string{"no"}},
		{fullWord: []byte("many sub words"), expectedMatches: []string{"many", "sub", "words", "many sub words"}, expectedAddCount: 4, matchedToFail: []string{"many sub"}},
		{fullWord: []byte(strconv.FormatBool(true)), expectedMatches: []string{"true"}, expectedAddCount: 1, matchedToFail: []string{"false"}},
		{fullWord: []byte("end whitespace "), expectedMatches: []string{"end", "whitespace", "end whitespace "}, expectedAddCount: 3, matchedToFail: []string{" "}},
	}

	for i, test := range cases {
		mockBloom := bloom.NewWithEstimates(uint(1000), BLOOM_COLL_PROBABILITY)
		addedCount := addToBlockBloom(mockBloom, test.fullWord)
		assert.Equal(t, addedCount, test.expectedAddCount)

		for _, word := range test.expectedMatches {
			assert.True(t, mockBloom.TestString(word), fmt.Sprintf("test=%v failed to find %+v in bloom", i, word))
		}
		for _, word := range test.matchedToFail {
			assert.False(t, mockBloom.TestString(word), fmt.Sprintf("test=%v found %+v in bloom, when should not happen", i, word))
		}
	}
}

func Benchmark_wrapperForUpdateRange(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	rangeIndex = map[string]*Numbers{}

	cases := []struct {
		key    string
		numstr string
	}{
		{"b", "1.456"},
		{"f", "12"},
		{"g", "51456"},
		{"h", "7551456"},
		{"i", "13887551456"},
		{"j", "-12"},
		{"k", "-200"},
		{"l", "-7551456"},
		{"n", "-1.323232"},
	}
	for _, test := range cases {
		for i := 0; i < b.N; i++ {
			wrapperForUpdateRange(test.key, test.numstr, rangeIndex)
		}
	}
	//Date 2/9/2021 Benchmark stats
	//Benchmark_wrapperForUpdateRange-8        1224584               962.0 ns/op            96 B/op          2 allocs/op

}
func Benchmark_addToBloom(b *testing.B) {

	exampleWord := []byte("abc def ghi jkl mnop")
	mockBloom := bloom.NewWithEstimates(uint(1000), BLOOM_COLL_PROBABILITY)

	for i := 0; i < b.N; i++ {
		mockBloom.ClearAll()
		addToBlockBloom(mockBloom, exampleWord)
	}
}

func wrapperForUpdateRange(key string, numstr string, rangeIndex map[string]*structs.Numbers) {
	numType, intVal, uintVal, fltVal := GetNumberTypeAndVal(numstr)
	updateRangeIndex(key, rangeIndex, numType, intVal, uintVal, fltVal)
}
