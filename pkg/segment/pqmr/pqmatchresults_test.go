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

package pqmr

import (
	"os"

	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPqmrEncodeDecode(t *testing.T) {

	fname := "pqmr_res.bin"
	os.Remove(fname)

	numBlocks := uint16(10)
	numRecs := uint(20_000)

	for i := uint16(0); i < numBlocks; i++ {
		pqmr := CreatePQMatchResults(10)

		for recNum := uint(0); recNum < numRecs; recNum++ {
			if recNum%3 == 0 {
				pqmr.AddMatchedRecord(recNum)
			}
		}
		//		t.Logf("blknum=%v, numRecs=%v, bitset size=%v", i, numRecs, pqmr.b.BinaryStorageSize())
		err := pqmr.FlushPqmr(&fname, i)
		assert.Nil(t, err)
	}

	res, err := ReadPqmr(&fname)
	assert.Nil(t, err)

	assert.Equal(t, numBlocks, res.GetNumBlocks(), "expected numBlocks=%v, actual=%v", numBlocks, res.GetNumBlocks())

	for i := uint16(0); i < numBlocks; i++ {
		pqmr, exists := res.GetBlockResults(i)
		assert.True(t, exists, "bitset for blkNum=%v does not exist", i)
		for recNum := uint(0); recNum < numRecs; recNum++ {
			isMatched := pqmr.DoesRecordMatch(recNum)
			if recNum%3 == 0 {
				assert.Equal(t, true, isMatched, "blkNum=%v, recNum=%v, did not match", i, recNum)
			} else {
				assert.Equal(t, false, isMatched, "blkNum=%v, recNum=%v, did not match", i, recNum)
			}
		}
	}

	//cleanup
	os.Remove(fname)
}
