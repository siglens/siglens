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

package metadata

import (
	"bytes"
	"errors"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func readRangeIndexFromByteArray(blkRILen uint32, bbRI []byte) map[string]*structs.Numbers {
	var byteCounter uint32 = 0
	blkRI := map[string]*structs.Numbers{}

	for byteCounter < blkRILen {
		//read RangeKeyLen
		blkRangeKeyLen := toputils.BytesToUint16LittleEndian(bbRI[byteCounter : byteCounter+2])

		byteCounter += 2
		//read ActualRangeKey
		blkActualRangeKey := string(bbRI[byteCounter : byteCounter+uint32(blkRangeKeyLen)])
		byteCounter += uint32(blkRangeKeyLen)

		//read RangeNumType

		blkRangeNumType := utils.RangeNumType(bbRI[byteCounter : byteCounter+1][0])
		byteCounter += 1
		var blkRIToAdd *structs.Numbers
		blkRIToAdd, byteCounter = rangeIndexToBytes(blkActualRangeKey, blkRangeNumType, bbRI, byteCounter)
		blkRI[blkActualRangeKey] = blkRIToAdd

	}
	return blkRI
}

func rangeIndexToBytes(blkActualRangeKey string, blkRangeNumType utils.RangeNumType, bbBlockRI []byte, byteCounter uint32) (*structs.Numbers, uint32) {
	var finalRangeIndex *structs.Numbers
	switch blkRangeNumType {
	case utils.RNT_UNSIGNED_INT:
		minVal := toputils.BytesToUint64LittleEndian(bbBlockRI[byteCounter : byteCounter+8])
		byteCounter += 8
		maxVal := toputils.BytesToUint64LittleEndian(bbBlockRI[byteCounter : byteCounter+8])
		byteCounter += 8
		finalRangeIndex = &structs.Numbers{Min_uint64: minVal, Max_uint64: maxVal, NumType: utils.RNT_UNSIGNED_INT}
	case utils.RNT_SIGNED_INT:
		minVal := toputils.BytesToInt64LittleEndian(bbBlockRI[byteCounter : byteCounter+8])
		byteCounter += 8
		maxVal := toputils.BytesToInt64LittleEndian(bbBlockRI[byteCounter : byteCounter+8])
		byteCounter += 8
		finalRangeIndex = &structs.Numbers{Min_int64: minVal, Max_int64: maxVal, NumType: utils.RNT_SIGNED_INT}
	case utils.RNT_FLOAT64:
		minVal := toputils.BytesToFloat64LittleEndian(bbBlockRI[byteCounter : byteCounter+8])
		byteCounter += 8
		maxVal := toputils.BytesToFloat64LittleEndian(bbBlockRI[byteCounter : byteCounter+8])
		byteCounter += 8
		finalRangeIndex = &structs.Numbers{Min_float64: minVal, Max_float64: maxVal, NumType: utils.RNT_FLOAT64}
	}
	return finalRangeIndex, byteCounter
}

func getCmi(cmbuf []byte) (*structs.CmiContainer, error) {

	cmic := &structs.CmiContainer{}

	switch cmbuf[0] {
	case utils.CMI_BLOOM_INDEX[0]:
		bufRdr := bytes.NewReader(cmbuf[1:])
		blkBloom := &bloom.BloomFilter{}
		_, bferr := blkBloom.ReadFrom(bufRdr)
		if bferr != nil {
			log.Errorf("getCmi: failed to convert bloom cmi %+v", bferr)
			return nil, bferr
		}
		cmic.CmiType = utils.CMI_BLOOM_INDEX[0]
		cmic.Loaded = true
		cmic.Bf = blkBloom
	case utils.CMI_RANGE_INDEX[0]:
		blkRI := readRangeIndexFromByteArray(uint32(len(cmbuf)-1), cmbuf[1:])
		cmic.CmiType = utils.CMI_RANGE_INDEX[0]
		cmic.Loaded = true
		cmic.Ranges = blkRI
	default:
		log.Errorf("getCmi: unknown cmitype=%v", cmbuf[0])
		return nil, errors.New("getCmi: unknown cmitype")
	}

	return cmic, nil
}
