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

package iqr

import (
	"testing"

	"github.com/siglens/siglens/pkg/segment/reader/record"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func getTestIQRsWithMockReaders(t *testing.T) (*IQR, *IQR, *IQR, *IQR) {
	allRRCs1 := []*utils.RecordResultContainer{
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 1},
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1}, BlockNum: 1, RecordNum: 2},
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 2}, BlockNum: 1, RecordNum: 3},
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 2}, BlockNum: 1, RecordNum: 4},
	}
	mockReader1 := &record.MockRRCsReader{
		RRCs: allRRCs1,
		FieldToValues: map[string][]utils.CValueEnclosure{
			"col1": {
				{Dtype: utils.SS_DT_STRING, CVal: "a"},
				{Dtype: utils.SS_DT_STRING, CVal: "b"},
				{Dtype: utils.SS_DT_STRING, CVal: "c"},
				{Dtype: utils.SS_DT_STRING, CVal: "d"},
			},
			"timestamp": {
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(8)},
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(4)},
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(6)},
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
			},
		},
		ReaderId: 1,
	}

	iqr1 := NewIQRWithReader(0, mockReader1)
	err := iqr1.AppendRRCs(allRRCs1[:2], map[uint32]string{1: "r1/seg1"})
	assert.Nil(t, err)

	iqr2 := NewIQRWithReader(0, mockReader1)
	err = iqr2.AppendRRCs(allRRCs1[2:], map[uint32]string{2: "r1/seg2"})
	assert.Nil(t, err)

	allRRCs2 := []*utils.RecordResultContainer{
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1000}, BlockNum: 1, RecordNum: 1},
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1000}, BlockNum: 1, RecordNum: 2},
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1001}, BlockNum: 1, RecordNum: 3},
		{SegKeyInfo: utils.SegKeyInfo{SegKeyEnc: 1001}, BlockNum: 1, RecordNum: 4},
	}

	mockReader2 := &record.MockRRCsReader{
		RRCs: allRRCs2,
		FieldToValues: map[string][]utils.CValueEnclosure{
			"col1": {
				{Dtype: utils.SS_DT_STRING, CVal: "e"},
				{Dtype: utils.SS_DT_STRING, CVal: "f"},
				{Dtype: utils.SS_DT_STRING, CVal: "g"},
				{Dtype: utils.SS_DT_STRING, CVal: "h"},
			},
			"timestamp": {
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(7)},
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(5)},
				{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
			},
		},
		ReaderId: 2,
	}

	iqr3 := NewIQRWithReader(0, mockReader2)
	err = iqr3.AppendRRCs(allRRCs2[:2], map[uint32]string{1000: "r2/seg1"})
	assert.Nil(t, err)

	iqr4 := NewIQRWithReader(0, mockReader2)
	err = iqr4.AppendRRCs(allRRCs2[2:], map[uint32]string{1001: "r2/seg2"})
	assert.Nil(t, err)

	return iqr1, iqr2, iqr3, iqr4
}

func testSortByTimestampDesc(r1 *Record, r2 *Record) bool {
	r1Timestamp, err := r1.ReadColumn("timestamp")
	if err != nil {
		return false
	}

	r2Timestamp, err := r2.ReadColumn("timestamp")
	if err != nil {
		return true
	}

	return r1Timestamp.CVal.(uint64) > r2Timestamp.CVal.(uint64)
}

func Test_MergeIQRReaders_Append(t *testing.T) {
	iqr1, iqr2, iqr3, iqr4 := getTestIQRsWithMockReaders(t)

	err := iqr1.Append(iqr2)
	assert.Nil(t, err)

	assert.Equal(t, 4, iqr1.NumberOfRecords())
	assert.Equal(t, ReaderModeSingleReader, iqr1.reader.readerMode)

	err = iqr1.Append(iqr3)
	assert.Nil(t, err)

	assert.Equal(t, 6, iqr1.NumberOfRecords())
	assert.Equal(t, ReaderModeMultiReader, iqr1.reader.readerMode)

	err = iqr1.Append(iqr4)
	assert.Nil(t, err)

	assert.Equal(t, 8, iqr1.NumberOfRecords())
	assert.Equal(t, ReaderModeMultiReader, iqr1.reader.readerMode)

	expectedEncodingToReaderId := map[utils.T_SegEncoding]utils.T_SegReaderId{
		1:    1,
		2:    1,
		1000: 2,
		1001: 2,
	}

	assert.Equal(t, expectedEncodingToReaderId, iqr1.reader.encodingToReaderId)

	knownValues, err := iqr1.ReadAllColumns()
	assert.Nil(t, err)

	expectedKnownValues := map[string][]utils.CValueEnclosure{
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "d"},
			{Dtype: utils.SS_DT_STRING, CVal: "e"},
			{Dtype: utils.SS_DT_STRING, CVal: "f"},
			{Dtype: utils.SS_DT_STRING, CVal: "g"},
			{Dtype: utils.SS_DT_STRING, CVal: "h"},
		},
		"timestamp": {
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(8)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(4)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(6)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(7)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(5)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
		},
	}

	assert.Equal(t, expectedKnownValues, knownValues)
}

func Test_MergeIQRReaders_MergeIQRs(t *testing.T) {
	// iqr1 timestamps: 8, 4
	// iqr2 timestamps: 6, 1
	// iqr3 timestamps: 7, 2
	// iqr4 timestamps: 5, 3

	iqr1, iqr2, iqr3, iqr4 := getTestIQRsWithMockReaders(t)

	iqrs := []*IQR{iqr1, iqr2, iqr3, iqr4}

	mergedIQR1, exhaustedIQRIndex, err := MergeIQRs(iqrs, testSortByTimestampDesc)
	assert.Nil(t, err)
	// iqr1 will be exhausted first
	assert.Equal(t, 0, exhaustedIQRIndex)
	assert.Equal(t, 5, mergedIQR1.NumberOfRecords())
	assert.Equal(t, 0, iqr1.NumberOfRecords())
	assert.Equal(t, 1, iqr2.NumberOfRecords())
	assert.Equal(t, 1, iqr3.NumberOfRecords())
	assert.Equal(t, 1, iqr4.NumberOfRecords())

	readerIdsToExist := []utils.T_SegReaderId{1, 2}
	readerIdToReader := mergedIQR1.reader.readerIdToReader
	for _, readerId := range readerIdsToExist {
		_, ok := readerIdToReader[readerId]
		assert.True(t, ok)
	}

	expectedEncodingToReaderId := map[utils.T_SegEncoding]utils.T_SegReaderId{
		1:    1,
		2:    1,
		1000: 2,
		1001: 2,
	}

	assert.Equal(t, expectedEncodingToReaderId, mergedIQR1.reader.encodingToReaderId)

	// Assume that iqr1 is exhausted
	iqrs = []*IQR{iqr2, iqr3, iqr4}

	mergedIQR2, exhaustedIQRIndex, err := MergeIQRs(iqrs, testSortByTimestampDesc)
	assert.Nil(t, err)
	// iqr4 will be exhausted first
	assert.Equal(t, 2, exhaustedIQRIndex)
	assert.Equal(t, 1, mergedIQR2.NumberOfRecords())
	assert.Equal(t, 0, iqr4.NumberOfRecords())
	assert.Equal(t, 1, iqr2.NumberOfRecords())
	assert.Equal(t, 1, iqr3.NumberOfRecords())

	readerIdsToExist = []utils.T_SegReaderId{1, 2}
	readerIdToReader = mergedIQR2.reader.readerIdToReader
	for _, readerId := range readerIdsToExist {
		_, ok := readerIdToReader[readerId]
		assert.True(t, ok)
	}

	expectedEncodingToReaderId = map[utils.T_SegEncoding]utils.T_SegReaderId{
		2:    1,
		1000: 2,
		1001: 2,
	}

	assert.Equal(t, expectedEncodingToReaderId, mergedIQR2.reader.encodingToReaderId)

	// Now iqr1 and iqr4 are exhausted
	iqrs = []*IQR{iqr2, iqr3}

	mergedIQR3, exhaustedIQRIndex, err := MergeIQRs(iqrs, testSortByTimestampDesc)
	assert.Nil(t, err)
	// iqr3 will be exhausted first
	assert.Equal(t, 1, exhaustedIQRIndex)
	assert.Equal(t, 1, mergedIQR3.NumberOfRecords())
	assert.Equal(t, 0, iqr3.NumberOfRecords())
	assert.Equal(t, 1, iqr2.NumberOfRecords())

	readerIdsToExist = []utils.T_SegReaderId{1, 2}
	readerIdToReader = mergedIQR3.reader.readerIdToReader
	for _, readerId := range readerIdsToExist {
		_, ok := readerIdToReader[readerId]
		assert.True(t, ok)
	}

	expectedEncodingToReaderId = map[utils.T_SegEncoding]utils.T_SegReaderId{
		2:    1,
		1000: 2,
	}

	assert.Equal(t, expectedEncodingToReaderId, mergedIQR3.reader.encodingToReaderId)

	// Now iqr1, iqr4, and iqr3 are exhausted
	iqrs = []*IQR{iqr2}

	mergedIQR4, exhaustedIQRIndex, err := MergeIQRs(iqrs, testSortByTimestampDesc)
	assert.Nil(t, err)
	// iqr2 will be exhausted
	assert.Equal(t, 0, exhaustedIQRIndex)
	assert.Equal(t, 1, mergedIQR4.NumberOfRecords())
	assert.Equal(t, 0, iqr2.NumberOfRecords())

	// Since there is one IQR with only one SingleReaderMode reader, the encodingToReaderId should be empty
	assert.Equal(t, 0, len(mergedIQR4.reader.readerIdToReader))
	assert.Equal(t, 0, len(mergedIQR4.reader.encodingToReaderId))

	// Now in the usual query flow: iqr1, iqr2, iqr3, and iqr4 are individual streams for a data processor
	// The data processor will merge the streams through MergeIQRs
	// If a stream is exhausted, the mergedIQR till that point will be used for further processing
	// The remaining streams will be cached and again when more data is needed, the these streams will be fetched
	// The exhausted streams will be fetched from the source and the non-exhausted streams will be fetched from the cache
	// This process will continue until all streams are exhausted
	// And as the mergedIQR is generated for each batch, the mergedIQR will be used for further processing and appended to the final IQR

	finalIQR := mergedIQR1
	err = finalIQR.Append(mergedIQR2)
	assert.Nil(t, err)
	assert.Equal(t, 6, finalIQR.NumberOfRecords())
	readerIdsToExist = []utils.T_SegReaderId{1, 2}
	readerIdToReader = finalIQR.reader.readerIdToReader
	for _, readerId := range readerIdsToExist {
		_, ok := readerIdToReader[readerId]
		assert.True(t, ok)
	}
	expectedEncodingToReaderId = map[utils.T_SegEncoding]utils.T_SegReaderId{
		1:    1,
		2:    1,
		1000: 2,
		1001: 2,
	}

	assert.Equal(t, expectedEncodingToReaderId, finalIQR.reader.encodingToReaderId)

	err = finalIQR.Append(mergedIQR3)
	assert.Nil(t, err)
	assert.Equal(t, 7, finalIQR.NumberOfRecords())
	readerIdToReader = finalIQR.reader.readerIdToReader
	for _, readerId := range readerIdsToExist {
		_, ok := readerIdToReader[readerId]
		assert.True(t, ok)
	}
	assert.Equal(t, expectedEncodingToReaderId, finalIQR.reader.encodingToReaderId)

	err = finalIQR.Append(mergedIQR4)
	assert.Nil(t, err)
	assert.Equal(t, 8, finalIQR.NumberOfRecords())
	readerIdToReader = finalIQR.reader.readerIdToReader
	for _, readerId := range readerIdsToExist {
		_, ok := readerIdToReader[readerId]
		assert.True(t, ok)
	}
	assert.Equal(t, expectedEncodingToReaderId, finalIQR.reader.encodingToReaderId)

	knownValues, err := finalIQR.ReadAllColumns()
	assert.Nil(t, err)
	expectedKnownValues := map[string][]utils.CValueEnclosure{
		"timestamp": {
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(8)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(7)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(6)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(5)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(4)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(3)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(2)},
			{Dtype: utils.SS_DT_UNSIGNED_NUM, CVal: uint64(1)},
		},
		"col1": {
			{Dtype: utils.SS_DT_STRING, CVal: "a"},
			{Dtype: utils.SS_DT_STRING, CVal: "e"},
			{Dtype: utils.SS_DT_STRING, CVal: "c"},
			{Dtype: utils.SS_DT_STRING, CVal: "g"},
			{Dtype: utils.SS_DT_STRING, CVal: "b"},
			{Dtype: utils.SS_DT_STRING, CVal: "h"},
			{Dtype: utils.SS_DT_STRING, CVal: "f"},
			{Dtype: utils.SS_DT_STRING, CVal: "d"},
		},
	}

	assert.Equal(t, expectedKnownValues, knownValues)
}
