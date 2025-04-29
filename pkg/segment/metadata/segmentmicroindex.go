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
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/cespare/xxhash"
	blob "github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/reader/microreader"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var ErrCMIColNotFound = errors.New("column not found in cmi")

const INITIAL_NUM_BLOCKS = 1000

// Top level segment metadata for access of cmis/search metadata
type SegmentMicroIndex struct {
	smiLock *sync.RWMutex
	structs.SegMeta
	SegmentMicroIndices
	SegmentSearchMetadata
	// Any time you add an element here, make sure you adjust mergeSegmentMicroIndex
}

// Holder structure for just the segment microindices
type SegmentMicroIndices struct {
	// map[blknum] => map[cname] => CmiContainer
	blockCmis     map[uint16]map[string]*structs.CmiContainer
	loadedCmiSize uint64
}

// Holder structure for just the segment search metadata (blk summaries & blockSearchInfo)
type SegmentSearchMetadata struct {
	BlockSummaries       []*structs.BlockSummary
	BlockSearchInfo      map[uint16]*structs.BlockMetadataHolder
	SearchMetadataSize   uint64
	loadedSearchMetadata bool
}

func (ssm *SegmentSearchMetadata) isSearchMetadataLoaded() bool {
	return ssm.loadedSearchMetadata
}

func InitSegmentMicroIndex(segMetaInfo *structs.SegMeta, loadSsm bool) *SegmentMicroIndex {

	sm := &SegmentMicroIndex{
		SegMeta: *segMetaInfo,
		smiLock: &sync.RWMutex{},
	}
	sm.loadedSearchMetadata = false
	sm.initMetadataSize()

	if loadSsm {
		err := sm.loadSearchMetadata()
		if err != nil {
			log.Errorf("InitSegmentMicroIndex: Failed to load search metadata for segKey %+v! Error: %v", sm.SegmentKey, err)
			return nil
		}
	}

	return sm
}

// Initializes sm.searchMetadaSize
func (sm *SegmentMicroIndex) initMetadataSize() {
	searchMetadataSize := uint64(0)

	// The list of BlockSummary structs
	searchMetadataSize += uint64(sm.NumBlocks * structs.SIZE_OF_BSUM) // block summaries

	// The map of blockNum to BlockMetadataHolder structs
	// type BlockMetadataHolder struct {
	//	BlkNum            uint16
	//	ColBlockOffAndLen map[string]ColOffAndLen
	// }
	sumColSize := uint64(0)
	for cname := range sm.ColumnNames {
		sumColSize += uint64(len(cname))
	}

	blockHolderSize := sumColSize * (8 + 4) // int64 value + uint32 value
	blockHolderSize += 2 + 6 + 8 + 8        // blockNum, padding, 2 map pointers
	searchMetadataSize += uint64(sm.NumBlocks) * blockHolderSize

	sm.SearchMetadataSize = searchMetadataSize
}

func (smi *SegmentMicroIndex) clearSearchMetadataWithLock() {

	smi.BlockSearchInfo = nil
	smi.BlockSummaries = nil
	smi.loadedSearchMetadata = false
}

func (smi *SegmentMicroIndex) clearSearchMetadata() {
	smi.smiLock.Lock()
	smi.clearSearchMetadataWithLock()
	smi.smiLock.Unlock()
}

func (smi *SegmentMicroIndex) clearMicroIndices() {
	smi.smiLock.Lock()
	smi.blockCmis = nil
	smi.loadedCmiSize = 0
	smi.smiLock.Unlock()
}

// Returns all columnar cmis for a given block or any errors encountered
func (smi *SegmentMicroIndex) GetCMIsForBlock(blkNum uint16,
	qid uint64) (map[string]*structs.CmiContainer, error) {
	if len(smi.blockCmis) == 0 {
		log.Errorf("qid=%v, GetCMIsForBlock: NO block cmis are loaded. segkey: %v",
			qid, smi.SegmentKey)
		return nil, fmt.Errorf("no cmis are loaded")
	}

	_, exists := smi.blockCmis[blkNum]
	if !exists {
		return nil, fmt.Errorf("qid=%v, GetCMIsForBlock blkNum %+v does not exist, segkey: %v",
			qid, blkNum, smi.SegmentKey)
	}
	cmis := smi.blockCmis[blkNum]
	return cmis, nil
}

// Returns the cmi for a given block & column, or any errors encountered
func (smi *SegmentMicroIndex) GetCMIForBlockAndColumn(blkNum uint16, cname string,
	qid uint64) (*structs.CmiContainer, error) {
	allCmis, err := smi.GetCMIsForBlock(blkNum, qid)
	if err != nil {
		return nil, err
	}
	retVal, ok := allCmis[cname]
	if !ok {
		return nil, ErrCMIColNotFound
	}
	return retVal, nil
}

func (sm *SegmentMicroIndex) loadSearchMetadata() error {
	sm.smiLock.Lock()
	defer sm.smiLock.Unlock()

	if sm.loadedSearchMetadata {
		return nil
	}

	bsfname := structs.GetBsuFnameFromSegKey(sm.SegmentKey)
	blockSum, allBmh, err := microreader.ReadBlockSummaries(bsfname, false)
	if err != nil {
		log.Errorf("ReadBlockSummaries: Failed to read block summary file: %v, err:%+v", bsfname, err)
		sm.clearSearchMetadataWithLock()
		return err
	}

	sm.loadedSearchMetadata = true
	sm.BlockSummaries = blockSum
	sm.BlockSearchInfo = allBmh
	return nil
}

func (smi *SegmentMicroIndex) readCmis(blocksToLoad map[uint16]map[string]bool,
	colsToRead map[string]bool) error {

	if strings.Contains(smi.VirtualTableName, ".kibana") {
		// no error bc kibana does not generate any CMIs
		return nil
	}

	haveToRead := false

	for askedBlkNum := range blocksToLoad {
		cnameCmi, ok := smi.blockCmis[askedBlkNum]
		if !ok {
			haveToRead = true
			break
		}
		for askedCname := range colsToRead {
			_, ok = cnameCmi[askedCname]
			if !ok {
				haveToRead = true
				break
			}
		}
	}

	if !haveToRead {
		return nil
	}

	// for cmilen (4) and blkNum (2)
	bb := make([]byte, utils.LEN_BLOCK_CMI_SIZE)
	cmbuf := make([]byte, 0)

	bulkDownloadFiles := make(map[string]string)
	for cname := range colsToRead {
		// timestamp, _type and _index col have no cmi
		if cname == config.GetTimeStampKey() || cname == "_type" || cname == "_index" {
			continue
		}
		if cname == "" {
			return fmt.Errorf("readCmis: empty colname for segkey: %v", smi.SegmentKey)
		}

		fName := fmt.Sprintf("%v_%v.cmi", smi.SegmentKey, xxhash.Sum64String(cname))
		bulkDownloadFiles[fName] = cname
	}
	err := blob.BulkDownloadSegmentBlob(bulkDownloadFiles, false)
	if err != nil {
		log.Errorf("readCmis: failed to bulk download seg files. segkey: %v, err=%v",
			smi.SegmentKey, err)
		return err
	}

	for fName, cname := range bulkDownloadFiles {
		fd, err := os.OpenFile(fName, os.O_RDONLY, 0644)
		if os.IsNotExist(err) {
			// This can happen if a query specifies a column that does not
			// exist in the segment.
			continue
		}
		if err != nil {
			return toputils.TeeErrorf("readCmis: cannot open fname=%v, cname=%v, err=[%v]",
				fName, cname, err)
		}
		defer fd.Close()

		csf := toputils.ChecksumFile{Fd: fd}

		offset := int64(0)
		for {
			_, err = csf.ReadBlock(bb, offset)
			if err != nil {
				if err != io.EOF {
					log.Errorf("readCmis: failed to read cmilen err=[%+v], continuing with rest cmis", err)
					break
				}
				break
			}
			offset += utils.LEN_BLOCK_CMI_SIZE // for cmilenHolder (4)
			cmilen := toputils.BytesToUint32LittleEndian(bb[0:utils.LEN_BLOCK_CMI_SIZE])
			cmbuf = toputils.ResizeSlice(cmbuf, int(cmilen))

			_, err = csf.ReadBlock(cmbuf[:cmilen], offset)
			if err != nil {
				if err != io.EOF {
					log.Errorf("readCmis: failed to read cmi err=[%+v], continuing with rest cmis", err)
					break
				}
				break
			}
			blkNum := toputils.BytesToUint16LittleEndian(cmbuf[:utils.LEN_BLKNUM_CMI_SIZE])

			cmic, err := getCmi(cmbuf[utils.LEN_BLKNUM_CMI_SIZE:cmilen])
			if err != nil {
				log.Errorf("readCmis: failed to convert CMI, err=[%v], continuing with rest cmis", err)
				break
			}
			if smi.blockCmis == nil {
				smi.blockCmis = make(map[uint16]map[string]*structs.CmiContainer)
			}
			_, ok := smi.blockCmis[blkNum]
			if !ok {
				smi.blockCmis[blkNum] = make(map[string]*structs.CmiContainer)
			}
			smi.blockCmis[blkNum][cname] = cmic
			smi.loadedCmiSize += uint64(cmilen)
			offset += int64(cmilen)
		}
	}
	return nil
}

func (sm *SegmentMicroIndex) GetColumns() map[string]bool {
	retVal := make(map[string]bool, len(sm.ColumnNames))
	for colName := range sm.ColumnNames {
		retVal[colName] = true
	}
	return retVal
}

func (sm *SegmentMicroIndex) getAllColumnsRecSize() map[string]uint32 {
	retVal := make(map[string]uint32, len(sm.ColumnNames))
	for colName, colSizeInfo := range sm.ColumnNames {
		retVal[colName] = colSizeInfo.ConsistentCvalSize
	}
	return retVal
}

func (sm *SegmentMicroIndex) getRecordCount() uint32 {
	return uint32(sm.SegMeta.RecordCount)
}

func GetLoadSsm(segkey string, qid uint64) (*SegmentMicroIndex, error) {

	smi, exists := GetMicroIndex(segkey)
	if !exists {
		return nil, toputils.TeeErrorf("qid=%v, seg file %+v does not exist in block meta, but existed in time filtering", qid, segkey)
	}

	if !smi.loadedSearchMetadata {
		err := smi.loadSearchMetadata()
		if err != nil {
			return nil,
				toputils.TeeErrorf("qid=%d, Failed to load search metadata for segKey %+v! Error: %v", qid, smi.SegmentKey, err)
		}
	}

	return smi, nil
}

func (smi *SegmentMicroIndex) LoadCmiForSearchTime(segkey string,
	timeFilteredBlocks map[uint16]map[string]bool,
	colsToCheck map[string]bool, wildcardCol bool,
	qid uint64) (bool, error) {

	var finalColsToCheck map[string]bool

	smi.smiLock.Lock()
	defer smi.smiLock.Unlock()

	if wildcardCol {
		finalColsToCheck = smi.GetColumns()
	} else {
		finalColsToCheck = colsToCheck
	}

	var missingBlockCMI bool
	err := smi.readCmis(timeFilteredBlocks, finalColsToCheck)
	if err != nil {
		log.Errorf("qid=%d, Failed to load cmi for blocks and columns. Num blocks %+v, Num columns %+v. Error: %+v",
			qid, len(timeFilteredBlocks), len(colsToCheck), err)
		missingBlockCMI = true
	}

	return missingBlockCMI, nil
}

func (smi *SegmentMicroIndex) RLockSmi() {
	smi.smiLock.RLock()
}

func (smi *SegmentMicroIndex) RUnlockSmi() {
	smi.smiLock.RUnlock()
}

func GetSearchInfoAndSummary(segkey string) (map[uint16]*structs.BlockMetadataHolder, []*structs.BlockSummary, error) {

	smi, ok := GetMicroIndex(segkey)
	if !ok {
		return nil, nil, errors.New("GetSearchInfoAndSummary:failed to find segkey in all block micro")
	}

	smi.smiLock.RLock()
	defer smi.smiLock.RUnlock()

	if smi.isSearchMetadataLoaded() {
		return smi.BlockSearchInfo, smi.BlockSummaries, nil
	}

	bsfname := structs.GetBsuFnameFromSegKey(smi.SegmentKey)
	blockSum, allBmh, err := microreader.ReadBlockSummaries(bsfname, false)
	if err != nil {
		log.Errorf("GetSearchInfoAndSummary: failed to read column block sum infos for segkey %s: %v", segkey, err)
		return nil, nil, err
	}

	return allBmh, blockSum, nil
}

// returns block search info, block summaries, and any errors encountered
// block search info will be loaded for all possible columns
func GetSearchInfoAndSummaryForPQS(segkey string,
	spqmr *pqmr.SegmentPQMRResults) (map[uint16]*structs.BlockMetadataHolder,
	[]*structs.BlockSummary, error) {

	allBmh, blockSum, err := GetSearchInfoAndSummary(segkey)
	if err != nil {
		log.Errorf("GetSearchInfoAndSummaryForPQS: failed to get block infos for segKey %+v: err: %v",
			segkey, err)

		return nil, nil, err
	}

	retSearchInfo := make(map[uint16]*structs.BlockMetadataHolder)
	setBlocks := spqmr.GetAllBlocks()
	for _, blkNum := range setBlocks {
		if blkMetadata, ok := allBmh[blkNum]; ok {
			retSearchInfo[blkNum] = blkMetadata
		}
	}
	return retSearchInfo, blockSum, nil
}
