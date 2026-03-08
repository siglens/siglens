// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package metadata

import (
	"bytes"
	"errors"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func readRangeIndexFromByteArray(blkRILen uint32, bbRI []byte) map[string]*structs.Numbers {
	var byteCounter uint32 = 0
	blkRI := map[string]*structs.Numbers{}

	for byteCounter < blkRILen {
		//read RangeKeyLen
		blkRangeKeyLen := utils.BytesToUint16LittleEndian(bbRI[byteCounter : byteCounter+2])

		byteCounter += 2
		//read ActualRangeKey
		blkActualRangeKey := string(bbRI[byteCounter : byteCounter+uint32(blkRangeKeyLen)])
		byteCounter += uint32(blkRangeKeyLen)

		//read RangeNumType

		blkRangeNumType := sutils.RangeNumType(bbRI[byteCounter : byteCounter+1][0])
		byteCounter += 1
		var blkRIToAdd *structs.Numbers
		blkRIToAdd, byteCounter = rangeIndexToBytes(blkActualRangeKey, blkRangeNumType, bbRI, byteCounter)
		blkRI[blkActualRangeKey] = blkRIToAdd

	}
	return blkRI
}

func rangeIndexToBytes(blkActualRangeKey string, blkRangeNumType sutils.RangeNumType, bbBlockRI []byte, byteCounter uint32) (*structs.Numbers, uint32) {
	var finalRangeIndex *structs.Numbers
	switch blkRangeNumType {
	case sutils.RNT_UNSIGNED_INT:
		minVal := utils.BytesToUint64LittleEndian(bbBlockRI[byteCounter : byteCounter+8])
		byteCounter += 8
		maxVal := utils.BytesToUint64LittleEndian(bbBlockRI[byteCounter : byteCounter+8])
		byteCounter += 8
		finalRangeIndex = &structs.Numbers{Min_uint64: minVal, Max_uint64: maxVal, NumType: sutils.RNT_UNSIGNED_INT}
	case sutils.RNT_SIGNED_INT:
		minVal := utils.BytesToInt64LittleEndian(bbBlockRI[byteCounter : byteCounter+8])
		byteCounter += 8
		maxVal := utils.BytesToInt64LittleEndian(bbBlockRI[byteCounter : byteCounter+8])
		byteCounter += 8
		finalRangeIndex = &structs.Numbers{Min_int64: minVal, Max_int64: maxVal, NumType: sutils.RNT_SIGNED_INT}
	case sutils.RNT_FLOAT64:
		minVal := utils.BytesToFloat64LittleEndian(bbBlockRI[byteCounter : byteCounter+8])
		byteCounter += 8
		maxVal := utils.BytesToFloat64LittleEndian(bbBlockRI[byteCounter : byteCounter+8])
		byteCounter += 8
		finalRangeIndex = &structs.Numbers{Min_float64: minVal, Max_float64: maxVal, NumType: sutils.RNT_FLOAT64}
	}
	return finalRangeIndex, byteCounter
}

func getCmi(cmbuf []byte) (*structs.CmiContainer, error) {

	cmic := &structs.CmiContainer{}

	switch cmbuf[0] {
	case sutils.CMI_BLOOM_INDEX[0]:
		bufRdr := bytes.NewReader(cmbuf[1:])
		blkBloom := &bloom.BloomFilter{}
		_, bferr := blkBloom.ReadFrom(bufRdr)
		if bferr != nil {
			log.Errorf("getCmi: failed to convert bloom cmi %+v", bferr)
			return nil, bferr
		}
		cmic.CmiType = sutils.CMI_BLOOM_INDEX[0]
		cmic.Loaded = true
		cmic.Bf = blkBloom
	case sutils.CMI_RANGE_INDEX[0]:
		blkRI := readRangeIndexFromByteArray(uint32(len(cmbuf)-1), cmbuf[1:])
		cmic.CmiType = sutils.CMI_RANGE_INDEX[0]
		cmic.Loaded = true
		cmic.Ranges = blkRI
	default:
		log.Errorf("getCmi: unknown cmitype=%v", cmbuf[0])
		return nil, errors.New("getCmi: unknown cmitype")
	}

	return cmic, nil
}
