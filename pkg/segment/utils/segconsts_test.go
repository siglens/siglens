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

package utils

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func Test_CVal_Equal(t *testing.T) {
	c1 := &CValueEnclosure{Dtype: SS_DT_STRING, CVal: "hello"}
	c2 := &CValueEnclosure{Dtype: SS_DT_STRING, CVal: "hello"}
	assert.True(t, c1.Equal(c2))

	c1 = &CValueEnclosure{Dtype: SS_DT_SIGNED_NUM, CVal: int64(123)}
	c2 = &CValueEnclosure{Dtype: SS_DT_SIGNED_NUM, CVal: int64(123)}
	assert.True(t, c1.Equal(c2))

	c1 = &CValueEnclosure{Dtype: SS_DT_SIGNED_NUM, CVal: uint64(123)}
	c2 = &CValueEnclosure{Dtype: SS_DT_UNSIGNED_NUM, CVal: uint64(123)}
	assert.False(t, c1.Equal(c2))

	c1 = &CValueEnclosure{Dtype: SS_DT_FLOAT, CVal: float64(123.456)}
	c2 = &CValueEnclosure{Dtype: SS_DT_FLOAT, CVal: float64(123.456001)}
	assert.True(t, c1.Equal(c2))

	c1 = &CValueEnclosure{Dtype: SS_DT_FLOAT, CVal: float64(123.456)}
	c2 = &CValueEnclosure{Dtype: SS_DT_FLOAT, CVal: float64(123.457)}
	assert.False(t, c1.Equal(c2))
}

func Test_CVal_Hash(t *testing.T) {
	assert.NotEqual(t,
		(&CValueEnclosure{Dtype: SS_DT_STRING, CVal: "hello"}).Hash(),
		(&CValueEnclosure{Dtype: SS_DT_STRING, CVal: "world"}).Hash(),
	)

	assert.NotEqual(t,
		(&CValueEnclosure{Dtype: SS_DT_SIGNED_NUM, CVal: int64(123)}).Hash(),
		(&CValueEnclosure{Dtype: SS_DT_UNSIGNED_NUM, CVal: uint64(123)}).Hash(),
	)

	assert.NotEqual(t,
		(&CValueEnclosure{Dtype: SS_DT_BOOL, CVal: false}).Hash(),
		(&CValueEnclosure{Dtype: SS_DT_BACKFILL, CVal: nil}).Hash(),
	)

	assert.Equal(t,
		(&CValueEnclosure{Dtype: SS_DT_STRING, CVal: "hello"}).Hash(),
		(&CValueEnclosure{Dtype: SS_DT_STRING, CVal: "hello"}).Hash(),
	)

	assert.Equal(t,
		(&CValueEnclosure{Dtype: SS_DT_BACKFILL, CVal: nil}).Hash(),
		(&CValueEnclosure{Dtype: SS_DT_BACKFILL, CVal: 123}).Hash(),
	)
}

func Test_RRCSize(t *testing.T) {
	type SegKeyInfo1 struct {
		// Encoded segment key
		SegKeyEnc uint16
		// If the RRC came from a remote node
		IsRemote bool
		// if IsRemote, Record will be initialized to a string of the form <<node_id>>-<<segkey>>-<<block_num>>-<<record_num>>
		RecordId string
	}

	type RecordResultContainer1 struct {
		SegKeyInfo       SegKeyInfo1 // Information about the segment key for a record (remote or not)
		BlockNum         uint16      // Block number of the record
		RecordNum        uint16      // Record number of the record
		SortColumnValue  float64     // Sort column value of the record
		TimeStamp        uint64      // Timestamp of the record
		VirtualTableName string      // Table name of the record
	}

	rrc1 := &RecordResultContainer1{
		SegKeyInfo: SegKeyInfo1{},
	}

	type SegKeyInfo2 struct {
		// Encoded segment key
		SegKeyEnc uint32
		// If the RRC came from a remote node
		IsRemote bool
		// if IsRemote, Record will be initialized to a string of the form <<node_id>>-<<segkey>>-<<block_num>>-<<record_num>>
		RecordId string
	}

	type RecordResultContainer2 struct {
		SegKeyInfo       SegKeyInfo2 // Information about the segment key for a record (remote or not)
		BlockNum         uint16      // Block number of the record
		RecordNum        uint16      // Record number of the record
		SortColumnValue  float64     // Sort column value of the record
		TimeStamp        uint64      // Timestamp of the record
		VirtualTableName string      // Table name of the record
	}

	rrc2 := &RecordResultContainer2{
		SegKeyInfo: SegKeyInfo2{},
	}

	fmt.Println("Size of RecordResultContainer1: ", unsafe.Sizeof(rrc1))
	fmt.Println("Size of RecordResultContainer2: ", unsafe.Sizeof(rrc2))
	assert.Equal(t, unsafe.Sizeof(rrc1), unsafe.Sizeof(rrc2))
}
