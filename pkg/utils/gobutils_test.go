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
	"math"
	"math/rand"
	"testing"

	tutils "github.com/siglens/siglens/pkg/segment/tracing/utils"
	"github.com/stretchr/testify/assert"
)

func Test_GetNilRegex(t *testing.T) {
	regex := GobbableRegex{}
	assert.Nil(t, regex.GetCompiledRegex())
}

func TestGetNonNilRegex(t *testing.T) {
	regex := GobbableRegex{}
	err := regex.SetRegex(".*")
	assert.NoError(t, err)
	assert.NotNil(t, regex.GetCompiledRegex())
}

func Test_SetInvalidRegex(t *testing.T) {
	regex := GobbableRegex{}
	err := regex.SetRegex("a(")
	assert.Error(t, err)
}

func Test_SetValidRegex(t *testing.T) {
	regex := GobbableRegex{}
	err := regex.SetRegex("a.*z")
	assert.NoError(t, err)

	compiledRegex := regex.GetCompiledRegex()
	assert.NotNil(t, compiledRegex)
	assert.Equal(t, "a.*z", compiledRegex.String())
}

func Test_SetInvalidRegexKeepsLastValidRegex(t *testing.T) {
	regex := GobbableRegex{}
	err := regex.SetRegex("a.*z")
	assert.NoError(t, err)

	err = regex.SetRegex("a(")
	assert.Error(t, err)

	compiledRegex := regex.GetCompiledRegex()
	assert.NotNil(t, compiledRegex)
	assert.Equal(t, "a.*z", compiledRegex.String())
}

func Test_GobEncode(t *testing.T) {
	regex := GobbableRegex{}
	encoded, err := regex.GobEncode()
	assert.NoError(t, err)
	assert.Equal(t, []byte(""), encoded)

	err = regex.SetRegex("a.*z")
	assert.NoError(t, err)

	encoded, err = regex.GobEncode()
	assert.NoError(t, err)
	assert.Equal(t, []byte("a.*z"), encoded)
}

func Test_GobEncodeDecode(t *testing.T) {
	originalRegex := GobbableRegex{}
	encoded, err := originalRegex.GobEncode()
	assert.NoError(t, err)

	decodedRegex := GobbableRegex{}
	err = decodedRegex.GobDecode(encoded)
	assert.NoError(t, err)
	assert.Nil(t, decodedRegex.GetCompiledRegex())

	err = originalRegex.SetRegex("a.*z")
	assert.NoError(t, err)

	encoded, err = originalRegex.GobEncode()
	assert.NoError(t, err)

	err = decodedRegex.GobDecode(encoded)
	assert.NoError(t, err)

	compiledRegex := decodedRegex.GetCompiledRegex()
	assert.NotNil(t, compiledRegex)
	assert.Equal(t, "a.*z", compiledRegex.String())
}

func Test_GobDecodeEmpty(t *testing.T) {
	regex := GobbableRegex{}
	err := regex.SetRegex("a.*z")
	assert.NoError(t, err)
	assert.NotNil(t, regex.GetCompiledRegex())
	assert.Equal(t, "a.*z", regex.GetCompiledRegex().String())

	err = regex.GobDecode([]byte{})
	assert.NoError(t, err)
	assert.Nil(t, regex.GetCompiledRegex())
}

func Test_GetEmptyList(t *testing.T) {
	list := GobbableList{}
	assert.Equal(t, 0, list.Len())
}

func Test_EncodeDecodeEmptyList(t *testing.T) {
	originalList := GobbableList{}
	encoded, err := originalList.GobEncode()
	assert.NoError(t, err)

	decodedList := GobbableList{}
	err = decodedList.GobDecode(encoded)
	assert.NoError(t, err)
	assert.Equal(t, 0, decodedList.Len())
}

func Test_EncodeDecodeSingleElementList(t *testing.T) {
	originalList := GobbableList{}
	originalList.PushBack(42)
	encoded, err := originalList.GobEncode()
	assert.NoError(t, err)

	decodedList := GobbableList{}
	err = decodedList.GobDecode(encoded)
	assert.NoError(t, err)
	assert.Equal(t, 1, decodedList.Len())
	assert.Equal(t, 42, decodedList.Front().Value)
}

func Test_EncodeDecodeMultipleElementList(t *testing.T) {
	originalList := GobbableList{}
	originalList.PushBack(42)
	originalList.PushBack(43)
	originalList.PushBack(44)
	encoded, err := originalList.GobEncode()
	assert.NoError(t, err)

	decodedList := GobbableList{}
	err = decodedList.GobDecode(encoded)
	assert.NoError(t, err)
	assert.Equal(t, 3, decodedList.Len())
	assert.Equal(t, 42, decodedList.Front().Value)
	assert.Equal(t, 43, decodedList.Front().Next().Value)
	assert.Equal(t, 44, decodedList.Front().Next().Next().Value)
}

func Test_EncodeDecodeMixedList(t *testing.T) {
	originalList := GobbableList{}
	originalList.PushBack(42)
	originalList.PushBack("hello")
	originalList.PushBack("world")
	encoded, err := originalList.GobEncode()
	assert.NoError(t, err)

	decodedList := GobbableList{}
	err = decodedList.GobDecode(encoded)
	assert.NoError(t, err)
	assert.Equal(t, 3, decodedList.Len())
	assert.Equal(t, 42, decodedList.Front().Value)
	assert.Equal(t, "hello", decodedList.Front().Next().Value)
	assert.Equal(t, "world", decodedList.Front().Next().Next().Value)
}

func Test_DecodeInvalidList(t *testing.T) {
	decodedList := GobbableList{}
	err := decodedList.GobDecode([]byte("invalid encoding"))
	assert.Error(t, err)
}

func Test_EncodeDecodeTDigest(t *testing.T) {
	originalTDigest, err := CreateNewTDigest()
	assert.NoError(t, err)
	numValsToInsert := uint64(5000)
	for i := 0.0; i < float64(numValsToInsert); i++ {
		err = originalTDigest.InsertIntoTDigest(i)
		assert.NoError(t, err)
	}
	encoded, err := originalTDigest.GobEncode()
	assert.NoError(t, err)
	decoded, err := CreateNewTDigest()
	assert.NoError(t, err)
	err = decoded.GobDecode(encoded)
	assert.NoError(t, err)
	assert.Equal(t, TDIGEST_COMPRESSION, decoded.Compression())
	assert.Equal(t, numValsToInsert, decoded.Count())
}

func Test_EncodeDecodeTDigestEmpty(t *testing.T) {
	originalTDigest, err := CreateNewTDigest()
	assert.NoError(t, err)
	encoded, err := originalTDigest.GobEncode()
	assert.NoError(t, err)
	decoded, err := CreateNewTDigest()
	assert.NoError(t, err)
	err = decoded.GobDecode(encoded)
	assert.NoError(t, err)
	assert.Equal(t, TDIGEST_COMPRESSION, decoded.Compression())
	assert.Equal(t, uint64(0), decoded.Count())
}

func Test_EncodeDecodeTDigest_LessThanCompression_PercentileCheck(t *testing.T) {
	originalTDigest, err := CreateNewTDigest()
	assert.NoError(t, err)
	const numValsToInsert float64 = TDIGEST_COMPRESSION - 5
	valArr := make([]float64, int(numValsToInsert))
	for i := 0.0; i < numValsToInsert; i++ {
		err = originalTDigest.InsertIntoTDigest(i)
		assert.NoError(t, err)
		valArr[int(i)] = i
	}
	percentileToCalc := rand.Intn(101)
	ansToTestAgainst := tutils.FindPercentileData(valArr, percentileToCalc)
	unEncodedRes := originalTDigest.GetQuantile(float64(percentileToCalc) / 100)
	assert.Equal(t, ansToTestAgainst, unEncodedRes)

	encoded, err := originalTDigest.GobEncode()
	assert.NoError(t, err)
	decoded, err := CreateNewTDigest()
	assert.NoError(t, err)
	err = decoded.GobDecode(encoded)
	assert.NoError(t, err)
	assert.Equal(t, TDIGEST_COMPRESSION, decoded.Compression())
	assert.Equal(t, uint64(numValsToInsert), decoded.Count())
	ansToTestAgainst = tutils.FindPercentileData(valArr, percentileToCalc)
	decodedRes := decoded.GetQuantile(float64(percentileToCalc) / 100)
	// round to three decimal places to avoid rounding errors
	rndAnsToTestAgainst := math.Round(ansToTestAgainst*1000) / 1000
	rndDecodedRes := math.Round(decodedRes*1000) / 1000
	assert.Equal(t, rndAnsToTestAgainst, rndDecodedRes)
}
