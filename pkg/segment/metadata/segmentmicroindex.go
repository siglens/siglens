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
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
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
	BlockSearchInfo      *structs.AllBlksMetaInfo
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
	sumCnamesSize := uint64(0)
	for cname := range sm.ColumnNames {
		sumCnamesSize += uint64(len(cname))
	}

	searchMetadataSize += sumCnamesSize * (8 + 4) // 8 (for mapkey of CnameDict) + 4 (for int idx)

	blockHolderSize := len(sm.ColumnNames) * (8 + 4) // int64 Offset + uint32 Length
	blockHolderSize += 2 + 6                         // blockNum, padding,
	searchMetadataSize += uint64(sm.NumBlocks) * uint64(blockHolderSize)

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
	blockSum, allBmi, err := microreader.ReadBlockSummaries(bsfname, false)
	if err != nil {
		log.Errorf("ReadBlockSummaries: Failed to read block summary file: %v, err:%+v", bsfname, err)
		sm.clearSearchMetadataWithLock()
		return err
	}

	sm.loadedSearchMetadata = true
	sm.BlockSummaries = blockSum
	sm.BlockSearchInfo = allBmi
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
	bb := make([]byte, sutils.LEN_BLOCK_CMI_SIZE+sutils.LEN_BLKNUM_CMI_SIZE)
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
			return utils.TeeErrorf("readCmis: cannot open fname=%v, cname=%v, err=[%v]",
				fName, cname, err)
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
			offset += sutils.LEN_BLOCK_CMI_SIZE + sutils.LEN_BLKNUM_CMI_SIZE // for cmilenHolder (4) and blkNum (2)
			cmilen := utils.BytesToUint32LittleEndian(bb[0:sutils.LEN_BLOCK_CMI_SIZE])
			cmilen -= sutils.LEN_BLKNUM_CMI_SIZE // for the blkNum(2)
			cmbuf = utils.ResizeSlice(cmbuf, int(cmilen))

			blkNum := utils.BytesToUint16LittleEndian(bb[sutils.LEN_BLOCK_CMI_SIZE:])

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

func (sm *SegmentMicroIndex) CollectColumnNames(resCnames map[string]struct{}) {
	for colName := range sm.ColumnNames {
		resCnames[colName] = struct{}{}
	}
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
		return nil, utils.TeeErrorf("qid=%v, seg file %+v does not exist in block meta, but existed in time filtering", qid, segkey)
	}

	if !smi.loadedSearchMetadata {
		err := smi.loadSearchMetadata()
		if err != nil {
			return nil,
				utils.TeeErrorf("qid=%d, Failed to load search metadata for segKey %+v! Error: %v", qid, smi.SegmentKey, err)
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

func GetSearchInfoAndSummary(segkey string) (*structs.AllBlksMetaInfo, []*structs.BlockSummary, error) {

	smi, ok := GetMicroIndex(segkey)
	if ok {
		smi.smiLock.RLock()
		defer smi.smiLock.RUnlock()

		if smi.isSearchMetadataLoaded() {
			return smi.BlockSearchInfo, smi.BlockSummaries, nil
		}
	}

	bsfname := structs.GetBsuFnameFromSegKey(segkey)
	blockSum, allBmi, err := microreader.ReadBlockSummaries(bsfname, false)
	if err != nil {
		log.Errorf("GetSearchInfoAndSummary: failed to read column block sum infos for segkey %s: %v", segkey, err)
		return nil, nil, err
	}

	// if found smi then load it for future
	if ok {
		smi.loadedSearchMetadata = true
		smi.BlockSummaries = blockSum
		smi.BlockSearchInfo = allBmi
		smi.initMetadataSize()
	}

	return allBmi, blockSum, nil
}

// returns block search info, block summaries, and any errors encountered
// block search info will be loaded for all possible columns
func GetSearchInfoAndSummaryForPQS(segkey string,
	spqmr *pqmr.SegmentPQMRResults) (map[uint16]struct{},
	[]*structs.BlockSummary, error) {

	allBmi, blockSum, err := GetSearchInfoAndSummary(segkey)
	if err != nil {
		log.Errorf("GetSearchInfoAndSummaryForPQS: failed to get block infos for segKey %+v: err: %v",
			segkey, err)

		return nil, nil, err
	}

	if allBmi == nil {
		return nil, blockSum, nil
	}

	allBlocksToSearch := make(map[uint16]struct{})

	setBlocks := spqmr.GetAllBlocks()
	for _, blkNum := range setBlocks {
		if _, ok := allBmi.AllBmh[blkNum]; ok {
			allBlocksToSearch[blkNum] = struct{}{}
		}
	}
	return allBlocksToSearch, blockSum, nil
}
