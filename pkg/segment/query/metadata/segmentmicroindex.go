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
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/cespare/xxhash"
	blob "github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/reader/microreader"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// Top level segment metadata for access of cmis/search metadata
type SegmentMicroIndex struct {
	structs.SegMeta
	SegmentMicroIndices
	SegmentSearchMetadata
	// Any time you add an element here, make sure you adjust mergeSegmentMicroIndex
}

// Holder structure for just the segment microindices
type SegmentMicroIndices struct {
	blockCmis          []map[string]*structs.CmiContainer
	MicroIndexSize     uint64
	loadedMicroIndices bool
}

// Holder structure for just the segment search metadata (blk summaries & blockSearchInfo)
type SegmentSearchMetadata struct {
	BlockSummaries       []*structs.BlockSummary
	BlockSearchInfo      map[uint16]*structs.BlockMetadataHolder
	SearchMetadataSize   uint64
	loadedSearchMetadata bool
}

func InitSegmentMicroIndex(segMetaInfo *structs.SegMeta) *SegmentMicroIndex {

	sm := &SegmentMicroIndex{
		SegMeta: *segMetaInfo,
	}
	sm.loadedMicroIndices = false
	sm.loadedSearchMetadata = false
	sm.initMetadataSize()
	return sm
}

// Initializes sm.searchMetadaSize and sm.microIndexSize values
func (sm *SegmentMicroIndex) initMetadataSize() {
	searchMetadataSize := uint64(0)
	searchMetadataSize += uint64(sm.NumBlocks * structs.SIZE_OF_BSUM) // block summaries
	// for values of the BlockMetadataHolder
	searchMetadataSize += uint64(sm.NumBlocks * uint16(len(sm.ColumnNames)) * structs.SIZE_OF_BlockInfo)
	// for keys of BlockMetadataHolder
	// 2 ==> two maps, 10 ==> avg colnamesize
	searchMetadataSize += uint64(sm.NumBlocks) * 2 * 10 * uint64(len(sm.ColumnNames))

	sm.SearchMetadataSize = searchMetadataSize

	microIndexSize := uint64(0)
	for _, colSizeInfo := range sm.ColumnNames {
		microIndexSize += colSizeInfo.CmiSize
	}
	sm.MicroIndexSize = microIndexSize
}

func (ssm *SegmentSearchMetadata) clearSearchMetadata() {
	ssm.BlockSearchInfo = nil
	ssm.BlockSummaries = nil
	ssm.loadedSearchMetadata = false
}

func (smi *SegmentMicroIndices) clearMicroIndices() {
	smi.blockCmis = nil
	smi.loadedMicroIndices = false
}

// Returns all columnar cmis for a given block or any errors encountered
func (smi *SegmentMicroIndices) GetCMIsForBlock(blkNum uint16) (map[string]*structs.CmiContainer, error) {
	if int(blkNum) >= len(smi.blockCmis) {
		return nil, fmt.Errorf("blkNum %+v does not exist", blkNum)
	}
	cmis := smi.blockCmis[blkNum]
	return cmis, nil
}

// Returns the cmi for a given block & column, or any errors encountered
func (smi *SegmentMicroIndices) GetCMIForBlockAndColumn(blkNum uint16, cname string) (*structs.CmiContainer, error) {
	allCmis, err := smi.GetCMIsForBlock(blkNum)
	if err != nil {
		return nil, err
	}
	retVal, ok := allCmis[cname]
	if !ok {
		return nil, fmt.Errorf("Failed to find column %+v in cmis for block %+v", cname, blkNum)
	}
	return retVal, nil
}

func (sm *SegmentMicroIndex) LoadSearchMetadata(rbuf []byte) ([]byte, error) {
	if sm.loadedSearchMetadata {
		return rbuf, nil
	}
	retbuf, blockSum, allBmh, err := sm.readBlockSummaries(rbuf)
	if err != nil {
		sm.clearSearchMetadata()
		return rbuf, err
	}
	sm.loadedSearchMetadata = true
	sm.BlockSummaries = blockSum
	sm.BlockSearchInfo = allBmh
	return retbuf, nil
}

func (sm *SegmentMicroIndex) readBlockSummaries(rbuf []byte) ([]byte, []*structs.BlockSummary,
	map[uint16]*structs.BlockMetadataHolder, error) {

	bsfname := structs.GetBsuFnameFromSegKey(sm.SegmentKey)
	blockSum, allBmh, retbuf, err := microreader.ReadBlockSummaries(bsfname, rbuf)
	if err != nil {
		log.Errorf("Failed to read block summary file: %v, err:%+v", bsfname, err)
		return rbuf, blockSum, allBmh, err
	}
	return retbuf, blockSum, allBmh, nil
}

func (sm *SegmentMicroIndex) loadMicroIndices(blocksToLoad map[uint16]map[string]bool, allBlocks bool, colsToCheck map[string]bool, wildcardCol bool) error {
	blkCmis, err := sm.readCmis(blocksToLoad, allBlocks, colsToCheck, wildcardCol)
	if err != nil {
		sm.clearMicroIndices()
		return err
	}
	sm.loadedMicroIndices = true
	sm.blockCmis = blkCmis
	return nil
}

func (sm *SegmentMicroIndex) readCmis(blocksToLoad map[uint16]map[string]bool, allBlocks bool,
	colsToCheck map[string]bool, wildcardCol bool) ([]map[string]*structs.CmiContainer, error) {

	if strings.Contains(sm.VirtualTableName, ".kibana") {
		// no error bc kibana does not generate any CMIs
		return []map[string]*structs.CmiContainer{}, nil
	}
	var allCols map[string]bool
	if wildcardCol {
		allCols = sm.getColumns()
	} else {
		allCols = colsToCheck
	}

	blkCmis := make([]map[string]*structs.CmiContainer, INITIAL_NUM_BLOCKS)
	for i := uint16(0); i < INITIAL_NUM_BLOCKS; i += 1 {
		blkCmis[i] = make(map[string]*structs.CmiContainer)
	}
	bb := make([]byte, utils.LEN_BLOCK_CMI_SIZE+utils.LEN_BLKNUM_CMI_SIZE) // for cmilen (4) and blkNum (2)
	cmbuf := make([]byte, 0)

	bulkDownloadFiles := make(map[string]string)
	var fName string
	for cname := range allCols {
		// timestamp, _type and _index col have no cmi
		if cname == config.GetTimeStampKey() || cname == "_type" || cname == "_index" {
			continue
		}
		if cname == "" {
			return nil, fmt.Errorf("readCmis: unknown seg set col")
		} else {
			fName = fmt.Sprintf("%v_%v.cmi", sm.SegmentKey, xxhash.Sum64String(cname))
		}
		bulkDownloadFiles[fName] = cname
	}
	err := blob.BulkDownloadSegmentBlob(bulkDownloadFiles, false)
	if err != nil {
		log.Errorf("readCmis: failed to bulk download seg files. err=%v", err)
		return nil, err
	}

	for fName, cname := range bulkDownloadFiles {
		fd, err := os.OpenFile(fName, os.O_RDONLY, 0644)
		if err != nil {
			log.Errorf("readCmis: open failed cname=%v, fname=%v, err=[%v], continuing with rest", cname, fName, err)
			continue
		}
		defer fd.Close()

		offset := int64(0)
		for {
			_, err = fd.ReadAt(bb, offset)
			if err != nil {
				if err != io.EOF {
					log.Errorf("readCmis: failed to read cmilen err=[%+v], continuing with rest cmis", err)
					break
				}
				break
			}
			offset += utils.LEN_BLOCK_CMI_SIZE + utils.LEN_BLKNUM_CMI_SIZE // for cmilenHolder (4) and blkNum (2)
			cmilen := toputils.BytesToUint32LittleEndian(bb[0:utils.LEN_BLOCK_CMI_SIZE])
			cmilen -= utils.LEN_BLKNUM_CMI_SIZE // for the blkNum(2)
			if bufflen := uint32(len(cmbuf)); bufflen < cmilen {
				newSlice := make([]byte, cmilen-bufflen)
				cmbuf = append(cmbuf, newSlice...)
			}

			blkNum := toputils.BytesToUint16LittleEndian(bb[utils.LEN_BLOCK_CMI_SIZE:])
			if _, shouldLoad := blocksToLoad[blkNum]; allBlocks || shouldLoad {
				_, err = fd.ReadAt(cmbuf[:cmilen], offset)
				if err != nil {
					if err != io.EOF {
						log.Errorf("readCmis: failed to read cmi err=[%+v], continuing with rest cmis", err)
						break
					}
					break
				}

				cmic, err := getCmi(cmbuf[:cmilen])
				if err != nil {
					log.Errorf("readCmis: failed to convert CMI, err=[%v], continuing with rest cmis", err)
					break
				}
				if intBlkNum := int(blkNum); len(blkCmis) <= intBlkNum {
					numToAdd := intBlkNum
					newArrEntry := make([]map[string]*structs.CmiContainer, numToAdd)
					for i := 0; i < numToAdd; i++ {
						newArrEntry[i] = make(map[string]*structs.CmiContainer)
					}
					blkCmis = append(blkCmis, newArrEntry...)
				}
				blkCmis[blkNum][cname] = cmic
			}
			offset += int64(cmilen)
		}
	}
	return blkCmis, nil
}

func (sm *SegmentMicroIndex) getColumns() map[string]bool {
	retVal := make(map[string]bool, len(sm.ColumnNames))
	for k := range sm.ColumnNames {
		retVal[k] = true
	}
	return retVal
}
