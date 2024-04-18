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
