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

package writer

import (
	"bytes"
	"fmt"
	"os"
	"time"

	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"

	"github.com/stretchr/testify/assert"

	"testing"

	"github.com/siglens/siglens/pkg/config"
	. "github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	bbp "github.com/valyala/bytebufferpool"
)

func TestBlockSumEncodeDecode(t *testing.T) {
	rangeIndex = map[string]*Numbers{}
	cases := []struct {
		input string
	}{
		{ // case#1
			`{
			   "highTS": 1639219919769,
			   "lowTs": 1639219912421,
			   "recCount": 2
			 }`,
		},
		{
			//case#4
			`{
				"highTS": 1639219919769,
				"lowTs": 1639219912421,
				"recCount": 0
			}`,
		},
	}

	for i, test := range cases {

		record_json := &BlockSummary{}
		var json = jsoniter.ConfigCompatibleWithStandardLibrary
		decoder := json.NewDecoder(bytes.NewReader([]byte(test.input)))
		decoder.UseNumber()
		err := decoder.Decode(&record_json)
		if err != nil {
			t.Errorf("testid: %d: Failed to parse json err:%v", i+1, err)
			continue
		}

		encoded := make([]byte, WIP_SIZE)
		bmh := &BlockMetadataHolder{
			ColumnBlockOffset: make(map[string]int64),
			ColumnBlockLen:    make(map[string]uint32),
		}
		bmh.ColumnBlockOffset["mycol"] = 29
		bmh.ColumnBlockLen["mycol"] = 22

		packedLen, _, err := EncodeBlocksum(bmh, record_json, encoded, 23)

		t.Logf("encoded len: %v, origlen=%v", packedLen, len(test.input))

		assert.Nil(t, err)

		t.Logf("input record_json=%v", record_json)

	}
}
func TestRecordEncodeDecode(t *testing.T) {
	config.InitializeTestingConfig()
	defer os.RemoveAll(config.GetDataPath())
	cases := []struct {
		input []byte
	}{
		{ // case#1
			[]byte(`{
			   "a":"val1",
			   "b":1.456,
			   "c":true,
			   "d":"John",
			   "e":null
			 }`,
			)},
		{ // case#2
			[]byte(`{
			   "a": 123456789012345678
			 }`,
			)},
		{
			//case#3
			[]byte(`{
				"f":-128,
				"g":-2147483649
			}`,
			)},
		{
			//case#4
			[]byte(`{
					"a":"val1",
					"b":1.456,
					"c":true,
					"d":"John",
					"e":null,
					"f":-12,
					"g":51456,
					"h":7551456,
					"i":13887551456,
					"j":12,
					"k":-200,
					"l":-7551456,
					"m":-3887551456,
					"n":-1.323232
			}`,
			)},
		{
			//case#5
			[]byte(`{
					"n":-1.323232,
					"o":-12343435565.323232
			}`,
			)},
	}
	for i, test := range cases {
		cTime := uint64(time.Now().UnixMilli())
		sId := fmt.Sprintf("test-%d", i)
		segstore, err := getSegStore(sId, cTime, "test", 0)
		if err != nil {
			log.Errorf("AddEntryToInMemBuf, getSegstore err=%v", err)
			t.Errorf("failed to get segstore! %v", err)
		}
		tsKey := config.GetTimeStampKey()
		maxIdx, _, err := segstore.EncodeColumns(test.input, cTime, &tsKey, SIGNAL_EVENTS)

		t.Logf("encoded len: %v, origlen=%v", maxIdx, len(test.input))

		assert.Nil(t, err)
		assert.GreaterOrEqual(t, maxIdx, uint32(0))
		colWips := allSegStores[sId].wipBlock.colWips
		for key, colwip := range colWips {
			val, _, _ := GetCvalFromRec(colwip.cbuf[colwip.cstartidx:colwip.cbufidx], 29)
			log.Infof("recNum %+v col %+v:%+v. type %+v", i, key, val, val.Dtype)
		}
	}
}

func TestJaegerRecordEncodeDecode(t *testing.T) {
	config.InitializeTestingConfig()
	defer os.RemoveAll(config.GetDataPath())
	cases := []struct {
		input []byte
	}{
		{ // case#1
			[]byte(`{"tags": [
			{
				"key": "sampler.type",
				"type": "string",
				"value": "const"
			},
			{
				"key": "sampler.param",
				"type": "bool",
				"value": "true"
			},
			{
				"key": "http.status_code",
				"type": "int64",
				"value": "200"
			},
			{
				"key": "component",
				"type": "string",
				"value": "gRPC"
			},
			{
				"key": "retry_no",
				"type": "int64",
				"value": "1"
			}
			
			],
		"logs": [
    {
      "timestamp": 1670445474307949,
      "fields": [
        {
          "key": "event",
          "type": "string",
          "value": "Searching for nearby drivers"
        },
        {
          "key": "level",
          "type": "string",
          "value": "info"
        },
        {
          "key": "location",
          "type": "string",
          "value": "577,322"
        }
      ]
    },
    {
      "timestamp": 1670445474370633,
      "fields": [
        {
          "key": "event",
          "type": "string",
          "value": "Retrying GetDriver after error"
        },
        {
          "key": "level",
          "type": "string",
          "value": "error"
        },
        {
          "key": "retry_no",
          "type": "int64",
          "value": "1"
        },
        {
          "key": "error",
          "type": "string",
          "value": "redis timeout"
        }

      
		]
	}],
		}`,
			)},
	}
	for i, test := range cases {
		cTime := uint64(time.Now().UnixMilli())
		sId := fmt.Sprintf("test-%d", i)
		segstore, err := getSegStore(sId, cTime, "test", 0)
		if err != nil {
			log.Errorf("AddEntryToInMemBuf, getSegstore err=%v", err)
			t.Errorf("failed to get segstore! %v", err)
		}
		tsKey := config.GetTimeStampKey()
		maxIdx, _, err := segstore.EncodeColumns(test.input, cTime, &tsKey, SIGNAL_JAEGER_TRACES)

		t.Logf("encoded len: %v, origlen=%v", maxIdx, len(test.input))

		assert.Nil(t, err)
		assert.GreaterOrEqual(t, maxIdx, uint32(0))
		colWips := allSegStores[sId].wipBlock.colWips
		for key, colwip := range colWips {
			val, _, _ := GetCvalFromRec(colwip.cbuf[colwip.cstartidx:colwip.cbufidx], 29)
			log.Infof("recNum %+v col %+v:%+v. type %+v", i, key, val, val.Dtype)
		}
	}
}

func TestTimestampRollups(t *testing.T) {

	wipBlock := createMockTsRollupWipBlock("data/test-segkey")

	// top-of-day validations
	expectedData := make(map[uint64]uint16)
	expectedData[1652227200000] = 412
	expectedData[1652140800000] = 588
	assert.Equal(t, len(expectedData), len(wipBlock.todRollup))
	for bkey, brup := range wipBlock.todRollup {
		actualmrcount := uint16(brup.MatchedRes.GetNumberOfSetBits())
		expectedmrcount := expectedData[bkey]
		assert.Equal(t, expectedmrcount, actualmrcount, "expectedmrcount=%v, actualmrcount=%v, bkey=%v",
			expectedmrcount, actualmrcount, bkey)
	}

	// top-of-hour validations
	expectedData = make(map[uint64]uint16)
	expectedData[1652220000000] = 88
	expectedData[1652223600000] = 500
	expectedData[1652227200000] = 412
	assert.Equal(t, len(expectedData), len(wipBlock.tohRollup))
	for bkey, brup := range wipBlock.tohRollup {
		actualmrcount := uint16(brup.MatchedRes.GetNumberOfSetBits())
		expectedmrcount := expectedData[bkey]
		assert.Equal(t, expectedmrcount, actualmrcount, "expectedmrcount=%v, actualmrcount=%v, bkey=%v",
			expectedmrcount, actualmrcount, bkey)
	}

	// top-of-min validations
	expectedData = make(map[uint64]uint16)
	expectedData[1652224380000] = 8
	expectedData[1652226300000] = 9
	expectedData[1652226480000] = 9
	expectedData[1652227140000] = 8
	expectedData[1652227440000] = 8
	expectedData[1652230020000] = 8
	expectedData[1652223780000] = 9
	expectedData[1652226060000] = 8
	expectedData[1652228700000] = 8
	expectedData[1652225220000] = 9
	expectedData[1652227260000] = 8
	expectedData[1652223240000] = 9
	expectedData[1652225760000] = 9
	expectedData[1652226120000] = 9
	expectedData[1652227620000] = 8
	expectedData[1652229900000] = 9
	expectedData[1652225820000] = 8
	expectedData[1652225880000] = 8
	expectedData[1652226840000] = 9
	expectedData[1652226900000] = 8
	expectedData[1652227020000] = 9
	expectedData[1652229960000] = 8
	expectedData[1652227740000] = 9
	expectedData[1652224440000] = 8
	expectedData[1652226180000] = 8
	expectedData[1652228340000] = 8
	expectedData[1652229780000] = 8
	expectedData[1652223540000] = 8
	expectedData[1652223720000] = 8
	expectedData[1652227680000] = 8
	expectedData[1652228520000] = 8
	expectedData[1652228640000] = 9
	expectedData[1652227500000] = 8
	expectedData[1652229600000] = 8
	expectedData[1652224200000] = 8
	expectedData[1652227080000] = 8
	expectedData[1652227380000] = 9
	expectedData[1652228220000] = 8
	expectedData[1652228460000] = 9
	expectedData[1652228820000] = 9
	expectedData[1652222940000] = 5
	expectedData[1652223000000] = 8
	expectedData[1652224740000] = 8
	expectedData[1652225700000] = 8
	expectedData[1652228160000] = 8
	expectedData[1652229360000] = 9
	expectedData[1652224620000] = 8
	expectedData[1652227980000] = 8
	expectedData[1652228100000] = 9
	expectedData[1652228940000] = 8
	expectedData[1652229660000] = 8
	expectedData[1652227920000] = 9
	expectedData[1652224800000] = 8
	expectedData[1652225460000] = 8
	expectedData[1652225940000] = 9
	expectedData[1652227320000] = 8
	expectedData[1652229120000] = 8
	expectedData[1652229300000] = 8
	expectedData[1652224140000] = 9
	expectedData[1652224260000] = 8
	expectedData[1652225040000] = 9
	expectedData[1652225520000] = 8
	expectedData[1652226720000] = 8
	expectedData[1652229240000] = 8
	expectedData[1652224080000] = 8
	expectedData[1652228760000] = 8
	expectedData[1652229060000] = 8
	expectedData[1652230080000] = 9
	expectedData[1652224920000] = 8
	expectedData[1652223120000] = 8
	expectedData[1652224560000] = 8
	expectedData[1652226660000] = 9
	expectedData[1652230140000] = 3
	expectedData[1652229420000] = 8
	expectedData[1652224320000] = 9
	expectedData[1652226360000] = 8
	expectedData[1652226540000] = 8
	expectedData[1652226780000] = 8
	expectedData[1652227200000] = 9
	expectedData[1652227560000] = 9
	expectedData[1652226960000] = 8
	expectedData[1652223480000] = 8
	expectedData[1652223660000] = 8
	expectedData[1652224860000] = 9
	expectedData[1652225280000] = 8
	expectedData[1652225340000] = 8
	expectedData[1652226000000] = 8
	expectedData[1652224020000] = 8
	expectedData[1652228400000] = 8
	expectedData[1652228580000] = 8
	expectedData[1652229000000] = 9
	expectedData[1652223840000] = 8
	expectedData[1652224500000] = 9
	expectedData[1652225100000] = 8
	expectedData[1652223900000] = 8
	expectedData[1652226240000] = 8
	expectedData[1652225580000] = 9
	expectedData[1652225640000] = 8
	expectedData[1652227800000] = 8
	expectedData[1652227860000] = 8
	expectedData[1652228880000] = 8
	expectedData[1652229840000] = 8
	expectedData[1652223600000] = 9
	expectedData[1652225400000] = 9
	expectedData[1652226420000] = 8
	expectedData[1652223180000] = 8
	expectedData[1652226600000] = 8
	expectedData[1652229480000] = 8
	expectedData[1652229720000] = 9
	expectedData[1652223360000] = 8
	expectedData[1652224680000] = 9
	expectedData[1652224980000] = 8
	expectedData[1652225160000] = 8
	expectedData[1652228040000] = 8
	expectedData[1652229540000] = 9
	expectedData[1652223060000] = 9
	expectedData[1652223300000] = 8
	expectedData[1652223420000] = 9
	expectedData[1652223960000] = 9
	expectedData[1652228280000] = 9
	expectedData[1652229180000] = 9
	assert.Equal(t, len(expectedData), len(wipBlock.tomRollup))
	for bkey, brup := range wipBlock.tomRollup {
		actualmrcount := uint16(brup.MatchedRes.GetNumberOfSetBits())
		expectedmrcount := expectedData[bkey]
		assert.Equal(t, expectedmrcount, actualmrcount, "expectedmrcount=%v, actualmrcount=%v, bkey=%v",
			expectedmrcount, actualmrcount, bkey)
	}
}

func Test_addSegStatsStr(t *testing.T) {
	cname := "mycol1"
	sst := make(map[string]*SegStats)
	numRecs := uint64(2000)

	bb := bbp.Get()

	for i := uint64(0); i < numRecs; i++ {
		addSegStatsStr(sst, cname, fmt.Sprintf("%v", i), bb)
	}

	assert.Equal(t, numRecs, sst[cname].Count)
}

func Test_addSegStatsNums(t *testing.T) {

	cname := "mycol1"
	sst := make(map[string]*SegStats)
	bb := bbp.Get()

	addSegStatsNums(sst, cname, SS_UINT64, 0, uint64(2345), 0, "2345", bb)
	assert.NotEqual(t, SS_DT_FLOAT, sst[cname].NumStats.Min.Ntype)
	assert.Equal(t, int64(2345), sst[cname].NumStats.Min.IntgrVal)

	addSegStatsNums(sst, cname, SS_FLOAT64, 0, 0, float64(345.1), "345.1", bb)
	assert.Equal(t, SS_DT_FLOAT, sst[cname].NumStats.Min.Ntype)
	assert.Equal(t, float64(345.1), sst[cname].NumStats.Min.FloatVal)

	assert.Equal(t, SS_DT_FLOAT, sst[cname].NumStats.Sum.Ntype)
	assert.Equal(t, float64(345.1+2345), sst[cname].NumStats.Sum.FloatVal)

}
