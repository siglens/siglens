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

package segread

import (
	"errors"
	"fmt"
	"os"
	"sort"

	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/common/fileutils"
	"github.com/siglens/siglens/pkg/config"
	segmetadata "github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/reader/segread/segreader"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/utils"

	log "github.com/sirupsen/logrus"
)

var ErrNilTimeReader = errors.New("uninitialized time reader")
var ErrReadBlock = errors.New("failed to read and validate block")

/*
Defines holder struct and functions to construct & manage SegmentFileReaders
across multiple columns
*/
type MultiColSegmentReader struct {
	allFileReaders      []*segreader.SegmentFileReader
	allColsReverseIndex map[string]int
	timeStampKey        string // timestamp key
	segKey              string // segment key
	timeReader          *TimeRangeReader

	AllColums              []*ColumnInfo
	allColInfoReverseIndex map[string]*ColumnInfo
	maxColIdx              int
}

type ColumnInfo struct {
	ColumnName string
	count      int
}

/*
Defines holder struct and functions to construct & manage SegmentFileReaders
across multiple columns
*/
type SharedMultiColReaders struct {
	MultiColReaders []*MultiColSegmentReader
	allFDs          map[string]*os.File // all fds shared across MultiColSegmentReaders
	allInUseFiles   []string            // all files that need to be released by blob
	numReaders      int
	numOpenFDs      int64
	columnErrorMap  map[string]error // column name -> error; Track errors while reading the column files for the shared readers
}

/*
Initialize a new MultipleColumnSegmentReader. This can be used to load & read any number of columns at once across any blocks

Caller is responsible for calling .CloseAll() to close all the fds.

Can also be used to get the timestamp for any arbitrary record in the Segment
*/
func initNewMultiColumnReader(segKey string, colFDs map[string]*os.File,
	allBlocksToSearch map[uint16]struct{},
	blockSummaries []*structs.BlockSummary,
	allColumnsRecSize map[string]uint32, qid uint64,
) (*MultiColSegmentReader, error) {
	readCols := make([]*ColumnInfo, 0)
	readColsReverseIndex := make(map[string]*ColumnInfo)
	colRevserseIndex := make(map[string]int)
	allFileReaders := make([]*segreader.SegmentFileReader, len(colFDs))

	tsKey := config.GetTimeStampKey()
	var idx int = 0
	retVal := &MultiColSegmentReader{
		allFileReaders:      allFileReaders,
		allColsReverseIndex: colRevserseIndex,
		timeStampKey:        tsKey,
		segKey:              segKey,
		maxColIdx:           -1,
	}

	var allBmi *structs.AllBlksMetaInfo
	var err error
	// todo blockSummaries don't need to be passed, we could just pick from this
	// below function
	if writer.IsSegKeyUnrotated(segKey) {
		allBmi, err = writer.GetBlockSearchInfoForKey(segKey)
		if err != nil {
			return nil, fmt.Errorf("InitSharedMultiColumnReaders: failed to get allBmi for unrotated segKey %s; err=%v", segKey, err)
		}
	} else {
		allBmi, _, err = segmetadata.GetSearchInfoAndSummary(segKey)
		if err != nil {
			return nil, fmt.Errorf("InitSharedMultiColumnReaders: failed to get allBmi segKey: %s. Error: %+v", segKey, err)
		}
	}

	for colName, colFD := range colFDs {
		if colName == tsKey {
			blkRecCount := make(map[uint16]uint16, len(blockSummaries))
			for blkIdx, blkSum := range blockSummaries {
				blkRecCount[uint16(blkIdx)] = blkSum.RecCount
			}
			currTimeReader, err := InitNewTimeReaderWithFD(colFD, tsKey, allBlocksToSearch, blkRecCount, qid, allBmi)
			if err != nil {
				log.Errorf("qid=%d, initNewMultiColumnReader: failed initialize timestamp reader for using timestamp key %s and segkey %s. Error: %v",
					qid, tsKey, segKey, err)
			} else {
				retVal.timeReader = currTimeReader
			}
			continue
		}

		colRecSize := sutils.INCONSISTENT_CVAL_SIZE
		if allColumnsRecSize != nil {
			if recSize, ok := allColumnsRecSize[colName]; ok {
				colRecSize = recSize
			}
		}

		segReader, err := segreader.InitNewSegFileReader(colFD, colName, allBlocksToSearch, qid, blockSummaries, colRecSize, allBmi)
		if err != nil {
			log.Errorf("qid=%d, initNewMultiColumnReader: failed initialize segfile reader for column %s Using file %s. Error: %v",
				qid, colName, colFD.Name(), err)
			continue
		}
		allFileReaders[idx] = segReader
		colRevserseIndex[colName] = idx
		currCol := &ColumnInfo{ColumnName: colName, count: 0}
		readCols = append(readCols, currCol)
		readColsReverseIndex[colName] = currCol
		idx++
	}

	retVal.allFileReaders = retVal.allFileReaders[:idx]
	retVal.AllColums = readCols[:idx]
	retVal.maxColIdx = idx
	retVal.allColInfoReverseIndex = readColsReverseIndex
	return retVal, nil
}

/*
Inializes N MultiColumnSegmentReaders, each of which share the same file descriptor.

Only columns that exist will be loaded, not guaranteed to load all columnns in colNames
It is up to the caller to close the open FDs using .Close()
*/
func InitSharedMultiColumnReaders(segKey string, colNames map[string]bool,
	allBlocksToSearch map[uint16]struct{},
	blockSummaries []*structs.BlockSummary, numReaders int, consistentCValLen map[string]uint32, qid uint64, nodeRes *structs.NodeResult,
) (*SharedMultiColReaders, error) {
	allInUseSegSetFiles := make([]string, 0)

	maxOpenFds := int64(0)
	for cname := range colNames {
		if cname != "*" {
			maxOpenFds += 1
		}
	}
	maxOpenFds += 2 + 1 // for time rollup files
	allFDs := make(map[string]*os.File)
	sharedReader := &SharedMultiColReaders{
		MultiColReaders: make([]*MultiColSegmentReader, numReaders),
		numReaders:      numReaders,
		numOpenFDs:      maxOpenFds,
		allFDs:          allFDs,
		columnErrorMap:  make(map[string]error),
	}

	err := fileutils.GLOBAL_FD_LIMITER.TryAcquireWithBackoff(maxOpenFds, 10, fmt.Sprintf("InitSharedMultiColumnReaders.qid=%d", qid))
	if err != nil {
		return sharedReader, fmt.Errorf("qid=%d, InitSharedMultiColumnReaders: Failed to acquire resources to be able to open %+v FDs. Error: %+v", qid, maxOpenFds, err)
	}
	csgFileToColNameMap := make(map[string]string)
	bulkDownloadFiles := make(map[string]string)

	var fName string
	for cname, fetchFromBlob := range colNames {
		if cname == "" {
			return nil, fmt.Errorf("InitSharedMultiColumnReaders: unknown seg set col")
		} else if cname == "*" {
			continue
		} else {
			fName = fmt.Sprintf("%v_%v.csg", segKey, xxhash.Sum64String(cname))
		}
		csgFileToColNameMap[fName] = cname

		if fetchFromBlob {
			// Check if the file exists in local storage
			if !fileutils.DoesFileExist(fName) {
				bulkDownloadFiles[fName] = cname
			}
		}
	}

	if len(bulkDownloadFiles) > 0 {
		err = blob.BulkDownloadSegmentBlob(bulkDownloadFiles, true)
		if err != nil {
			nodeRes.StoreGlobalSearchError("Error Downloading Segment Files", log.ErrorLevel, err)
		}
	}

	for fName, colName := range csgFileToColNameMap {
		fName := fName
		currFd, err := os.OpenFile(fName, os.O_RDONLY, 0644)
		if err != nil {
			// This segment may have been recently rotated; try reading the
			// rotated segment file.
			rotatedFName := writer.GetRotatedVersion(fName)
			var rotatedErr error
			currFd, rotatedErr = os.OpenFile(rotatedFName, os.O_RDONLY, 0644)
			if rotatedErr != nil {
				err := fmt.Errorf("qid=%d, InitSharedMultiColumnReaders: failed to open file %s for column %s."+
					" Error: %w. Also failed to open rotated file %s with error: %v",
					qid, fName, colName, err, rotatedFName, rotatedErr)
				if len(sharedReader.columnErrorMap) < sutils.MAX_SIMILAR_ERRORS_TO_LOG {
					sharedReader.columnErrorMap[colName] = err
				}
				nodeRes.StoreGlobalSearchError("Error Initializing SharedMultiColumnReaders", log.ErrorLevel, err)
				continue
			}
		}
		sharedReader.allFDs[colName] = currFd
		allInUseSegSetFiles = append(allInUseSegSetFiles, fName)
	}

	for i := 0; i < numReaders; i++ {
		currReader, err := initNewMultiColumnReader(segKey, sharedReader.allFDs, allBlocksToSearch,
			blockSummaries, consistentCValLen, qid)
		if err != nil {
			sharedReader.Close()
			err1 := blob.SetSegSetFilesAsNotInUse(allInUseSegSetFiles)
			if err1 != nil {
				log.Errorf("qid=%d, InitSharedMultiColumnReaders: Failed to release needed segment files from local storage %+v! err: %+v", qid, allInUseSegSetFiles, err1)
			}
			return sharedReader, err
		}
		sharedReader.MultiColReaders[i] = currReader
	}

	sharedReader.allInUseFiles = allInUseSegSetFiles
	return sharedReader, nil
}

// Returns all buffers to the pools, closes all FDs shared across multi readers, and updates global semaphore
func (scr *SharedMultiColReaders) Close() {
	for _, multiReader := range scr.MultiColReaders {
		if multiReader != nil {
			multiReader.returnBuffers()
		}
	}
	for _, reader := range scr.allFDs {
		if reader != nil {
			err := reader.Close()
			if err != nil {
				log.Errorf("SharedMultiColReaders.Close: Failed to close fd! err: %+v", err)
			}
		}
	}
	err := blob.SetSegSetFilesAsNotInUse(scr.allInUseFiles)
	if err != nil {
		log.Errorf("SharedMultiColReaders.Close: Failed to release needed segment files from local storage %+v! err: %+v", scr.allInUseFiles, err)
	}
	fileutils.GLOBAL_FD_LIMITER.Release(scr.numOpenFDs)
}

func (scr *SharedMultiColReaders) GetColumnsErrorsMap() map[string]error {
	return scr.columnErrorMap
}

func (mcsr *MultiColSegmentReader) GetTimeStampForRecord(blockNum uint16, recordNum uint16, qid uint64) (uint64, error) {
	if mcsr.timeReader == nil {
		return 0, ErrNilTimeReader
	}
	return mcsr.timeReader.GetTimeStampForRecord(blockNum, recordNum, qid)
}

func (mcsr *MultiColSegmentReader) GetAllTimeStampsForBlock(blockNum uint16) ([]uint64, error) {
	if mcsr.timeReader == nil {
		return nil, ErrNilTimeReader
	}
	return mcsr.timeReader.GetAllTimeStampsForBlock(blockNum)
}

// Reads the raw value and returns the []byte in TLV format (type-[length]-value encoding)
func (mcsr *MultiColSegmentReader) ReadRawRecordFromColumnFile(colKeyIndex int, blockNum uint16, recordNum uint16, qid uint64, isTsCol bool) ([]byte, error) {
	if isTsCol {
		ts, err := mcsr.GetTimeStampForRecord(blockNum, recordNum, qid)
		if err != nil {
			return nil, err
		}
		retVal := make([]byte, 9)
		copy(retVal[0:], sutils.VALTYPE_ENC_UINT64[:])
		utils.Uint64ToBytesLittleEndianInplace(ts, retVal[1:])
		return retVal, nil
	}

	if colKeyIndex == -1 || colKeyIndex >= mcsr.maxColIdx {
		// Debug to avoid log flood for when the column does not exist
		if config.IsDebugMode() {
			log.Debugf("MultiColSegmentReader.ReadRawRecordFromColumnFile: failed to find colKeyIndex %v in multi col reader. All cols: %+v", colKeyIndex, mcsr.allColsReverseIndex)
		}
		return nil, nil
	}

	return mcsr.allFileReaders[colKeyIndex].ReadRecord(recordNum)
}

// Reads the request value and converts it to a *utils.CValueEnclosure
func (mcsr *MultiColSegmentReader) ExtractValueFromColumnFile(colKeyIndex int, blockNum uint16,
	recordNum uint16, qid uint64, isTsCol bool, retCVal *sutils.CValueEnclosure,
) error {
	if isTsCol {
		ts, err := mcsr.GetTimeStampForRecord(blockNum, recordNum, qid)
		if err != nil {
			return err
		}
		retCVal.Dtype = sutils.SS_DT_UNSIGNED_NUM
		retCVal.CVal = ts

		return nil
	}

	rawVal, err := mcsr.ReadRawRecordFromColumnFile(colKeyIndex, blockNum, recordNum, qid, isTsCol)
	if err != nil {
		retCVal.Dtype = sutils.SS_DT_BACKFILL
		retCVal.CVal = nil

		return err
	}

	_, err = writer.GetCvalFromRec(rawVal, qid, retCVal)
	return err
}

func (mcsr *MultiColSegmentReader) returnBuffers() {
	if mcsr.allFileReaders != nil {
		for _, reader := range mcsr.allFileReaders {
			if reader != nil {
				err := reader.ReturnBuffers()
				if err != nil {
					log.Errorf("MultiColSegmentReader.returnBuffers: Error returning buffer back to memory: %v", err)
				}
			}
		}
	}
	if mcsr.timeReader != nil {
		mcsr.timeReader.returnBuffers()
	}
}

func (mcsr *MultiColSegmentReader) IncrementColumnUsageByName(colName string) {
	mcsr.allColInfoReverseIndex[colName].count++
}

func (mcsr *MultiColSegmentReader) IncrementColumnUsageByIdx(colKeyIndex int) {
	mcsr.AllColums[colKeyIndex].count++
}

// reorders mcsr.AllColumns to be ordered on usage
func (mcsr *MultiColSegmentReader) ReorderColumnUsage() {
	sort.Slice(mcsr.AllColums, func(i, j int) bool {
		return mcsr.AllColums[i].count > mcsr.AllColums[j].count
	})
}

func (mcsr *MultiColSegmentReader) IsBlkDictEncoded(cname string,
	blkNum uint16,
) (bool, error) {
	// reads the csg file and decides whether this particular block is encoded via dictionary encoding
	// or raw csg encoding, and returns if it is dict-enc, along with the map of each dict-key => recNums pairing

	keyIndex, ok := mcsr.allColsReverseIndex[cname]
	if !ok {
		// Debug to avoid log flood for when the column does not exist
		if config.IsDebugMode() {
			log.Debugf("MultiColSegmentReader.IsBlkDictEncoded: failed to find column %s in multi col reader. All cols: %+v", cname, mcsr.allColsReverseIndex)
		}
		return false, errors.New("column not found in MultipleColumnSegmentReader")
	}

	return mcsr.allFileReaders[keyIndex].IsBlkDictEncoded(blkNum)
}

// The results maps column name to the values for the records corresponding to orderedRecNums.
func (mcsr *MultiColSegmentReader) GetDictEncCvalsFromColFile(results map[string][]sutils.CValueEnclosure,
	col string, blockNum uint16, orderedRecNums []uint16, qid uint64,
) bool {
	keyIndex, ok := mcsr.allColsReverseIndex[col]
	if !ok {
		return false
	}

	return mcsr.allFileReaders[keyIndex].GetDictEncCvalsFromColFile(results, blockNum,
		orderedRecNums)
}

func (mcsr *MultiColSegmentReader) ApplySearchToMatchFilterDictCsg(match *structs.MatchFilter,
	bsh *structs.BlockSearchHelper, cname string, isCaseInsensitive bool,
) (bool, error) {
	keyIndex, ok := mcsr.allColsReverseIndex[cname]
	if !ok {
		return false, errors.New("could not find sfr for cname")
	}

	fileReader := mcsr.allFileReaders[keyIndex]
	return ApplySearchToMatchFilterDictCsg(fileReader, match, bsh, isCaseInsensitive)
}

func (mcsr *MultiColSegmentReader) ApplySearchToExpressionFilterDictCsg(qValDte *sutils.DtypeEnclosure,
	fop sutils.FilterOperator, isRegexSearch bool, bsh *structs.BlockSearchHelper,
	cname string, isCaseInsensitive bool,
) (bool, error) {
	keyIndex, ok := mcsr.allColsReverseIndex[cname]
	if !ok {
		return false, fmt.Errorf("MultiColSegmentReader.ApplySearchToExpressionFilterDictCsg: could not find sfr for cname: %v", cname)
	}

	fileReader := mcsr.allFileReaders[keyIndex]
	return ApplySearchToExpressionFilterDictCsg(fileReader, qValDte, fop, isRegexSearch, bsh, isCaseInsensitive)
}

func (mcsr *MultiColSegmentReader) IsColPresent(cname string) bool {
	_, ok := mcsr.allColsReverseIndex[cname]
	return ok
}

func (mcsr *MultiColSegmentReader) GetColKeyIndex(cname string) (int, bool) {
	idx, ok := mcsr.allColsReverseIndex[cname]
	return idx, ok
}

func (mcsr *MultiColSegmentReader) ValidateAndReadBlock(colsIndexMap map[int]struct{}, blockNum uint16) error {
	for keyIndex := range colsIndexMap {
		if keyIndex >= len(mcsr.allFileReaders) {
			continue // This can happen if the column does not exist
		}

		err := mcsr.allFileReaders[keyIndex].ValidateAndReadBlock(blockNum)
		if err != nil {
			return ErrReadBlock
		}
	}

	return nil
}
