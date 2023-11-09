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
	"fmt"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
	bbp "github.com/valyala/bytebufferpool"

	"github.com/stretchr/testify/assert"

	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	. "github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
)

func Test_ApplySearchToMatchFilterRaw(t *testing.T) {
	config.InitializeTestingConfig()
	defer os.RemoveAll(config.GetDataPath())
	rangeIndex = map[string]*Numbers{}
	cases := []struct {
		input []byte
	}{
		{
			//case#1
			[]byte(`{
					"a":"val1 val2 val3 val4 val5",
					"c":true,
					"d":"John",
					"i":13887551456,
					"m":-3887551456,
					"n":-1.323232,
					"timestamp": 1234
			}`,
			)},
	}

	for i, test := range cases {
		cTime := uint64(time.Now().UnixMilli())
		sId := fmt.Sprintf("test-a-%d", i)
		segstore, err := getSegStore(sId, cTime, "test", 0)
		if err != nil {
			log.Errorf("AddEntryToInMemBuf, getSegstore err=%v", err)
			t.Errorf("failed to get segstore! %v", err)
		}
		tsKey := config.GetTimeStampKey()
		_, _, err = segstore.EncodeColumns(test.input, cTime, &tsKey, SIGNAL_EVENTS)
		assert.Nil(t, err)

		colWips := allSegStores[sId].wipBlock.colWips
		mf := MatchFilter{
			MatchColumn: "*",
			MatchWords:  [][]byte{[]byte("abcdefg"), []byte("val2")},

			MatchOperator: Or,
		}

		var found bool
		for _, colWip := range colWips {
			result, err := ApplySearchToMatchFilterRawCsg(&mf, colWip.cbuf[:])
			assert.Nil(t, err)
			found = result
			if found {
				break
			}
		}
		assert.Equal(t, true, found)

		t.Logf("searching for val2 in all columns worked")

		mf = MatchFilter{
			MatchColumn:   "a",
			MatchWords:    [][]byte{[]byte("abcdefg"), []byte("val2")},
			MatchOperator: Or,
		}

		result, err := ApplySearchToMatchFilterRawCsg(&mf, colWips[mf.MatchColumn].cbuf[:])
		assert.Nil(t, err)
		assert.Equal(t, true, result)
		t.Logf("searching for val2 in column-a worked")

		mf = MatchFilter{
			MatchColumn:   "d",
			MatchWords:    [][]byte{[]byte("abcdefg"), []byte("val2")},
			MatchOperator: Or,
		}

		result, err = ApplySearchToMatchFilterRawCsg(&mf, colWips[mf.MatchColumn].cbuf[:])
		assert.Nil(t, err)
		assert.Equal(t, false, result)
		t.Logf("searching for val2 in column-d worked (should not be found)")

		mf = MatchFilter{
			MatchColumn:   "a",
			MatchWords:    [][]byte{[]byte("abcdefg"), []byte("val2")},
			MatchOperator: And,
		}

		result, err = ApplySearchToMatchFilterRawCsg(&mf, colWips[mf.MatchColumn].cbuf[:])
		assert.Nil(t, err)
		assert.Equal(t, false, result)
		t.Logf("searching for two values in column-a worked (should not be found)")

		mf = MatchFilter{
			MatchColumn:   "a",
			MatchWords:    [][]byte{[]byte("val1"), []byte("val2"), []byte("val3"), []byte("val4")},
			MatchOperator: And,
		}

		result, err = ApplySearchToMatchFilterRawCsg(&mf, colWips[mf.MatchColumn].cbuf[:])
		assert.Nil(t, err)
		assert.Equal(t, true, result)
		t.Logf("searching for multiple values in column-a worked (all should be found)")
	}
}

func Test_applySearchToExpressionFilterSimpleHelper(t *testing.T) {
	rangeIndex = map[string]*Numbers{}
	cases := []struct {
		input []byte
	}{
		{
			//case#1
			[]byte(`{
					"cbool":true,
					"csigned":-2345,
					"cunsigned":2345,
					"cfloat":-2345.35,
					"cstr":"haystack",
					"timestamp": 1234
			}`,
			)},
	}

	for _, test := range cases {
		allCols := make(map[string]bool)
		segstats := make(map[string]*SegStats)

		var blockSummary BlockSummary
		colWips := make(map[string]*ColWip)
		wipBlock := WipBlock{
			columnBlooms:       make(map[string]*BloomIndex),
			columnRangeIndexes: make(map[string]*RangeIndex),
			colWips:            colWips,
			pqMatches:          make(map[string]*pqmr.PQMatchResults),
			columnsInBlock:     make(map[string]bool),
			blockSummary:       blockSummary,
			tomRollup:          make(map[uint64]*RolledRecs),
			tohRollup:          make(map[uint64]*RolledRecs),
			todRollup:          make(map[uint64]*RolledRecs),
			bb:                 bbp.Get(),
		}
		segstore := &SegStore{
			wipBlock:       wipBlock,
			SegmentKey:     "test-segkey",
			AllSeenColumns: allCols,
			pqTracker:      initPQTracker(),
			AllSst:         segstats,
			numBlocks:      0,
		}
		ts := config.GetTimeStampKey()
		maxIdx, _, err := segstore.EncodeColumns(test.input, 1234, &ts, SIGNAL_EVENTS)
		t.Logf("encoded len: %v, origlen=%v", maxIdx, len(test.input))

		assert.Nil(t, err)
		assert.Greater(t, maxIdx, uint32(0))

		var holderDte *DtypeEnclosure = &DtypeEnclosure{}
		var qValDte *DtypeEnclosure

		t.Logf("doing equals search for haystack in cstr")
		qValDte, _ = CreateDtypeEnclosure("haystack", 0)
		qValDte.AddStringAsByteSlice()
		var eOff uint16 = 3 + utils.BytesToUint16LittleEndian(colWips["cstr"].cbuf[1:3]) // 2 bytes stored for string type
		result, err := ApplySearchToExpressionFilterSimpleCsg(qValDte, Equals, colWips["cstr"].cbuf[:eOff], false, holderDte)
		assert.Nil(t, err)
		assert.Equal(t, true, result)
		qValDte.Reset()

		t.Logf("doing equals search for haystack for col that is not string")
		qValDte, _ = CreateDtypeEnclosure("haystack", 0)
		qValDte.AddStringAsByteSlice()
		result, _ = ApplySearchToExpressionFilterSimpleCsg(qValDte, Equals, colWips["cfloat"].cbuf[:], false, holderDte)
		assert.Equal(t, false, result)
		qValDte.Reset()

		//TODO: uncomment when ApplySearchToExpressionFilterSimpleCsg for numbers is implemented
		t.Logf("doing equals search for float ")
		t.Logf("cbuf:%s", string(colWips["cfloat"].cbuf[:]))
		qValDte, _ = CreateDtypeEnclosure(-2345.35, 0)
		result, _ = ApplySearchToExpressionFilterSimpleCsg(qValDte, Equals, colWips["cfloat"].cbuf[:], false, holderDte)
		assert.Equal(t, true, result)
		qValDte.Reset()

		t.Logf("doing equals search for unsigned ")
		qValDte, _ = CreateDtypeEnclosure(2345, 0)
		result, _ = ApplySearchToExpressionFilterSimpleCsg(qValDte, Equals, colWips["cunsigned"].cbuf[:], false, holderDte)
		assert.Equal(t, true, result)
		qValDte.Reset()
	}
}
