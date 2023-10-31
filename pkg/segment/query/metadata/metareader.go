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
