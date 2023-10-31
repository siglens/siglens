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

package segread

import (
	"os"
	"testing"

	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/stretchr/testify/assert"
)

func TestTimestampRollupReads(t *testing.T) {
	segkey := "data/rollup-readtest"
	err := writer.WriteMockTsRollup(segkey)
	assert.Nil(t, err)

	rur, err := InitNewRollupReader(segkey, "timestamp", 24)
	assert.Nil(t, err)
	defer rur.Close()

	// verify day rollups
	dayMapBlks, err := rur.GetDayRollups()
	assert.Nil(t, err)

	expectedData := make(map[uint64]uint16)
	expectedData[1652227200000] = 412
	expectedData[1652140800000] = 588
	blkZeroMap := dayMapBlks[0] // get blkNum 0
	assert.Equal(t, len(expectedData), len(blkZeroMap))
	for bkey, brup := range blkZeroMap {
		actualmrcount := uint16(brup.MatchedRes.GetNumberOfSetBits())
		expectedmrcount := expectedData[bkey]
		assert.Equal(t, expectedmrcount, actualmrcount, "expectedmrcount=%v, actualmrcount=%v, bkey=%v",
			expectedmrcount, actualmrcount, bkey)
	}

	// verify hour rollups
	hourMapBlks, err := rur.GetHourRollups()
	assert.Nil(t, err)

	expectedData = make(map[uint64]uint16)
	expectedData[1652220000000] = 88
	expectedData[1652223600000] = 500
	expectedData[1652227200000] = 412

	blkZeroMap = hourMapBlks[0] // get blkNum 0
	assert.Equal(t, len(expectedData), len(blkZeroMap))
	for bkey, brup := range blkZeroMap {
		actualmrcount := uint16(brup.MatchedRes.GetNumberOfSetBits())
		expectedmrcount := expectedData[bkey]
		assert.Equal(t, expectedmrcount, actualmrcount, "expectedmrcount=%v, actualmrcount=%v, bkey=%v",
			expectedmrcount, actualmrcount, bkey)
	}

	// verify min rollups
	minMapBlks, err := rur.GetMinRollups()
	assert.Nil(t, err)
	assert.Equal(t, 1, len(minMapBlks))

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

	blkZeroMap = minMapBlks[0]

	assert.Equal(t, len(expectedData), len(blkZeroMap))
	for bkey, brup := range blkZeroMap {
		actualmrcount := uint16(brup.MatchedRes.GetNumberOfSetBits())
		expectedmrcount := expectedData[bkey]
		assert.Equal(t, expectedmrcount, actualmrcount, "expectedmrcount=%v, actualmrcount=%v, bkey=%v",
			expectedmrcount, actualmrcount, bkey)
	}

	os.RemoveAll(segkey)
}
