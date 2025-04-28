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

package record

import (
	"errors"
	"fmt"
	"runtime"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/common/fileutils"
	"github.com/siglens/siglens/pkg/config"
	segmetadata "github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/reader/segread"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type RRCsReaderI interface {
	ReadAllColsForRRCs(segKey string, vTable string, rrcs []*utils.RecordResultContainer,
		qid uint64, ignoredCols map[string]struct{}) (map[string][]utils.CValueEnclosure, error)
	GetColsForSegKey(segKey string, vTable string) (map[string]struct{}, error)
	ReadColForRRCs(segKey string, rrcs []*utils.RecordResultContainer, cname string, qid uint64, fetchFromBlob bool) ([]utils.CValueEnclosure, error)
	GetReaderId() utils.T_SegReaderId
}

type RRCsReader struct{}

func (reader *RRCsReader) GetReaderId() utils.T_SegReaderId {
	return utils.T_SegReaderId(0)
}

func (reader *RRCsReader) ReadSegFilesFromBlob(segKey string, allCols map[string]struct{}) ([]string, error) {
	bulkDownloadFiles := make(map[string]string)
	allFiles := make([]string, 0)
	for col := range allCols {
		ssFile := fmt.Sprintf("%v_%v.csg", segKey, xxhash.Sum64String(col))
		if !fileutils.DoesFileExist(ssFile) {
			bulkDownloadFiles[ssFile] = col
		}
		allFiles = append(allFiles, ssFile)
	}

	err := blob.BulkDownloadSegmentBlob(bulkDownloadFiles, true)
	if err != nil {
		return nil, fmt.Errorf("readSegFilesFromBlob: failed to download col file. err=%v", err)
	}

	return allFiles, nil
}

func (reader *RRCsReader) ReadAllColsForRRCs(segKey string, vTable string, rrcs []*utils.RecordResultContainer,
	qid uint64, ignoredCols map[string]struct{}) (map[string][]utils.CValueEnclosure, error) {

	allCols, err := reader.GetColsForSegKey(segKey, vTable)
	if err != nil {
		log.Errorf("qid=%v, ReadAllColsForRRCs: failed to get columns for segKey %s; err=%v",
			qid, segKey, err)
		return nil, err
	}

	allFiles, err := reader.ReadSegFilesFromBlob(segKey, allCols)
	if err != nil {
		return nil, toputils.TeeErrorf("qid=%d, ReadAllColsForRRCs: failed to read seg files from blob; err=%v", qid, err)
	}

	defer func() {
		err := blob.SetSegSetFilesAsNotInUse(allFiles)
		if err != nil {
			log.Errorf("qid=%d, ReadAllColsForRRCs: failed to set segset files as not in use. err=%v", qid, err)
		}
	}()

	colToValues := make(map[string][]utils.CValueEnclosure)
	mapLock := sync.Mutex{}
	cnames := toputils.GetKeysOfMap(allCols)
	parallelism := runtime.GOMAXPROCS(0)

	err = toputils.ProcessWithParallelism(parallelism, cnames, func(cname string) error {
		if _, ignore := ignoredCols[cname]; ignore {
			return nil
		}
		columnValues, err := reader.ReadColForRRCs(segKey, rrcs, cname, qid, false)
		if err != nil {
			log.Errorf("qid=%v, ReadAllColsForRRCs: failed to read column %s for segKey %s; err=%v",
				qid, cname, segKey, err)
			return err
		}

		mapLock.Lock()
		colToValues[cname] = columnValues
		mapLock.Unlock()

		return nil
	})

	return colToValues, err
}

func (reader *RRCsReader) GetColsForSegKey(segKey string, vTable string) (map[string]struct{}, error) {
	allCols := make(map[string]struct{})
	exists := writer.CheckAndCollectColNamesForSegKey(segKey, allCols)
	if !exists {
		exists = segmetadata.CheckAndCollectColNamesForSegKey(segKey, allCols)
		if !exists {
			return nil, toputils.TeeErrorf("GetColsForSegKey: globalMetadata does not have segKey: %s", segKey)
		}
	}
	allCols[config.GetTimeStampKey()] = struct{}{}

	return allCols, nil
}

func (reader *RRCsReader) ReadColForRRCs(segKey string, rrcs []*utils.RecordResultContainer, cname string, qid uint64, fetchFromBlob bool) ([]utils.CValueEnclosure, error) {
	switch cname {
	case config.GetTimeStampKey():
		return readTimestampForRRCs(rrcs)
	case "_index":
		return readIndexForRRCs(rrcs)
	default:
		return readUserDefinedColForRRCs(segKey, rrcs, cname, qid, fetchFromBlob)
	}
}

func readTimestampForRRCs(rrcs []*utils.RecordResultContainer) ([]utils.CValueEnclosure, error) {
	result := make([]utils.CValueEnclosure, len(rrcs))
	for i, rrc := range rrcs {
		result[i] = utils.CValueEnclosure{
			Dtype: utils.SS_DT_UNSIGNED_NUM,
			CVal:  rrc.TimeStamp,
		}
	}

	return result, nil
}

func readIndexForRRCs(rrcs []*utils.RecordResultContainer) ([]utils.CValueEnclosure, error) {
	result := make([]utils.CValueEnclosure, len(rrcs))
	for i, rrc := range rrcs {
		result[i] = utils.CValueEnclosure{
			Dtype: utils.SS_DT_STRING,
			CVal:  rrc.VirtualTableName,
		}
	}

	return result, nil
}

// All the RRCs must belong to the same segment.
func readUserDefinedColForRRCs(segKey string, rrcs []*utils.RecordResultContainer,
	cname string, qid uint64, fetchFromBlob bool) ([]utils.CValueEnclosure, error) {

	if len(rrcs) == 0 {
		return nil, nil
	}

	err := fileutils.GLOBAL_FD_LIMITER.TryAcquireWithBackoff(1, 10, "readUserDefinedColForRRCs")
	if err != nil {
		log.Errorf("qid=%v, readUserDefinedColForRRCs failed to get lock for opening 1 file; err=%v", qid, err)
		return nil, err
	}
	defer fileutils.GLOBAL_FD_LIMITER.Release(1)

	allBlocksToSearch := make(map[uint16]struct{})
	for _, rrc := range rrcs {
		allBlocksToSearch[rrc.BlockNum] = struct{}{}
	}

	// todo we should not be reading blockSummary here, let the segreader read it
	var blockSummary []*structs.BlockSummary
	if writer.IsSegKeyUnrotated(segKey) {

		blockSummary, err = writer.GetBlockSummaryForKey(segKey)
		if err != nil {
			log.Errorf("qid=%v, readUserDefinedColForRRCs: failed to get block summary for segKey %s; err=%v", qid, segKey, err)
			return nil, err
		}
	} else {
		_, blockSummary, err = segmetadata.GetSearchInfoAndSummary(segKey)
		if err != nil {
			log.Errorf("getRecordsFromSegmentHelper: failed to get blocksearchinfo for segkey=%v, err=%v", segKey, err)
			return nil, err
		}
	}

	nodeRes, err := query.GetOrCreateQuerySearchNodeResult(qid)
	if err != nil {
		// This should not happen, unless qid is deleted.
		log.Errorf("qid=%v, readUserDefinedColForRRCs: failed to get or create query search node result; err=%v", qid, err)
		nodeRes = &structs.NodeResult{}
	}
	consistentCValLen := map[string]uint32{cname: utils.INCONSISTENT_CVAL_SIZE} // TODO: use correct value
	sharedReader, err := segread.InitSharedMultiColumnReaders(segKey, map[string]bool{cname: fetchFromBlob},
		allBlocksToSearch, blockSummary, 1, consistentCValLen, qid, nodeRes)
	if err != nil {
		log.Errorf("qid=%v, readUserDefinedColForRRCs: failed to initialize shared readers for segkey %v; err=%v", qid, segKey, err)
		return nil, err
	}
	defer sharedReader.Close()

	colErrorMap := sharedReader.GetColumnsErrorsMap()
	if len(colErrorMap) > 0 {
		return nil, colErrorMap[cname]
	}

	multiReader := sharedReader.MultiColReaders[0]

	batchingFunc := func(rrc *utils.RecordResultContainer) uint16 {
		return rrc.BlockNum
	}
	batchKeyLess := toputils.NewOptionWithValue(func(blockNum1, blockNum2 uint16) bool {
		// We want to read the file in order, so read the blocks in order.
		return blockNum1 < blockNum2
	})
	operation := func(rrcsInBatch []*utils.RecordResultContainer) ([]utils.CValueEnclosure, error) {
		if len(rrcsInBatch) == 0 {
			return nil, nil
		}

		return handleBlock(multiReader, rrcsInBatch[0].BlockNum, rrcsInBatch, qid)
	}

	enclosures, _ := toputils.BatchProcess(rrcs, batchingFunc, batchKeyLess, operation)
	return enclosures, nil
}

func handleBlock(multiReader *segread.MultiColSegmentReader, blockNum uint16,
	rrcs []*utils.RecordResultContainer, qid uint64) ([]utils.CValueEnclosure, error) {

	sortFunc := func(rrc1, rrc2 *utils.RecordResultContainer) bool {
		return rrc1.RecordNum < rrc2.RecordNum
	}
	operation := func(rrcs []*utils.RecordResultContainer) ([]utils.CValueEnclosure, error) {
		allRecNums := make([]uint16, len(rrcs))
		for i, rrc := range rrcs {
			allRecNums[i] = rrc.RecordNum
		}

		colToValues, err := readColsForRecords(multiReader, blockNum, allRecNums, qid)
		if err != nil {
			return nil, fmt.Errorf("handleBlock: failed to read columns for records; err=%v", err)
		}

		if len(colToValues) != 1 {
			return nil, fmt.Errorf("handleBlock: expected 1 column, got %v", len(colToValues))
		}

		for _, values := range colToValues {
			return values, nil
		}

		return nil, fmt.Errorf("handleBlock: should not reach here")
	}

	return toputils.SortThenProcessThenUnsort(rrcs, sortFunc, operation)
}

// returns a map of record identifiers to record maps, and all columns seen
// record identifiers is segfilename + blockNum + recordNum
// If esResponse is false, _id and _type will not be added to any record
func GetRecordsFromSegmentOldPipeline(segKey string, vTable string, blkRecIndexes map[uint16]map[uint16]uint64,
	tsKey string, esQuery bool, qid uint64, aggs *structs.QueryAggregators,
	colsIndexMap map[string]int, allColsInAggs map[string]struct{}, nodeRes *structs.NodeResult,
	consistentCValLen map[string]uint32) (map[string]map[string]interface{}, map[string]bool, error) {

	return nil, nil, errors.New("Old pipeline is deprecated")
}

func getMathOpsColMap(MathOps []*structs.MathEvaluator) map[string]int {
	colMap := make(map[string]int)
	for index, mathOp := range MathOps {
		colMap[mathOp.MathCol] = index
	}
	return colMap
}

func readColsForRecords(segReader *segread.MultiColSegmentReader, blockNum uint16,
	orderedRecNums []uint16, qid uint64) (map[string][]utils.CValueEnclosure, error) {

	allMatchedColumns := make(map[string]bool)
	esQuery := false
	aggs := &structs.QueryAggregators{}
	nodeRes := &structs.NodeResult{}
	results := make(map[string][]utils.CValueEnclosure)

	for _, colInfo := range segReader.AllColums {
		cname := colInfo.ColumnName

		if !esQuery && (cname == "_type" || cname == "_id") {
			continue
		}

		if cname == config.GetTimeStampKey() {
			continue
		}

		results[cname] = make([]utils.CValueEnclosure, len(orderedRecNums))
	}

	return results, readAllRawRecords(orderedRecNums, blockNum, segReader,
		allMatchedColumns, esQuery, qid, aggs, nodeRes, results)
}

func isDictCol(col string, esQuery bool) bool {
	if !esQuery && (col == "_type" || col == "_id") {
		return true
	}
	if col == config.GetTimeStampKey() {
		return true
	}
	return false
}

// TODO: remove calls to this function so that only readColsForRecords calls
// this function. Then remove the parameters that are not needed.
func readAllRawRecords(orderedRecNums []uint16, blockNum uint16, segReader *segread.MultiColSegmentReader,
	allMatchedColumns map[string]bool, esQuery bool, qid uint64, aggs *structs.QueryAggregators,
	nodeRes *structs.NodeResult,
	results map[string][]utils.CValueEnclosure) error {

	dictEncCols := make(map[string]bool)
	allColKeyIndices := make(map[int]string)

	for _, colInfo := range segReader.AllColums {
		col := colInfo.ColumnName

		if isDictCol(col, esQuery) {
			dictEncCols[col] = true
			continue
		}

		ok := segReader.GetDictEncCvalsFromColFile(results, col, blockNum,
			orderedRecNums, qid)
		if ok {
			dictEncCols[col] = true
			allMatchedColumns[col] = true
		}
		cKeyidx, ok := segReader.GetColKeyIndex(col)
		if ok {
			allColKeyIndices[cKeyidx] = col
		}
	}

	var mathColMap map[string]int
	var mathColOpsPresent bool

	if aggs != nil && aggs.MathOperations != nil && len(aggs.MathOperations) > 0 {
		mathColMap = getMathOpsColMap(aggs.MathOperations)
		mathColOpsPresent = true
	} else {
		mathColOpsPresent = false
		mathColMap = make(map[string]int)
	}

	colsToReadIndices := make(map[int]struct{})
	for colKeyIdx, cname := range allColKeyIndices {
		_, exists := dictEncCols[cname]
		if exists {
			continue
		}
		colsToReadIndices[colKeyIdx] = struct{}{}
	}

	err := segReader.ValidateAndReadBlock(colsToReadIndices, blockNum)
	if err != nil {
		return fmt.Errorf("qid=%d, readAllRawRecords: failed to validate and read block, err: %v",
			qid, err)
	}

	var isTsCol bool
	for idx, recNum := range orderedRecNums {

		for colKeyIdx, cname := range allColKeyIndices {

			_, ok := dictEncCols[cname]
			if ok {
				continue
			}

			isTsCol = (config.GetTimeStampKey() == cname)

			var cValEnc utils.CValueEnclosure

			err := segReader.ExtractValueFromColumnFile(colKeyIdx, blockNum, recNum,
				qid, isTsCol, &cValEnc)
			if err != nil {
				nodeRes.StoreGlobalSearchError(fmt.Sprintf("readAllRawRecords: Failed to extract value for column %v", cname), log.ErrorLevel, err)
			} else {

				if mathColOpsPresent {
					colIndex, exists := mathColMap[cname]
					if exists {
						mathOp := aggs.MathOperations[colIndex]
						fieldToValue := make(map[string]utils.CValueEnclosure)
						fieldToValue[mathOp.MathCol] = cValEnc
						valueFloat, err := mathOp.ValueColRequest.EvaluateToFloat(fieldToValue)
						if err != nil {
							return fmt.Errorf("qid=%d, failed to evaluate math operation for col %s, err=%v", qid, cname, err)
						} else {
							cValEnc.CVal = valueFloat
						}
					}
				}

				results[cname][idx] = cValEnc

				allMatchedColumns[cname] = true
			}
		}
	}
	return nil
}

func applyColNameTransform(allCols map[string]bool, aggs *structs.QueryAggregators, colsIndexMap map[string]int, qid uint64) map[string]bool {
	retCols := make(map[string]bool)
	if aggs == nil || aggs.OutputTransforms == nil {
		return allCols
	}

	if aggs.OutputTransforms.OutputColumns == nil {
		return allCols
	}

	allColNames := make([]string, len(allCols))
	i := 0
	for cName := range allCols {
		allColNames[i] = cName
		i++
	}

	if aggs.OutputTransforms.OutputColumns.IncludeColumns == nil {
		retCols = allCols
	} else {
		index := 0
		for _, cName := range aggs.OutputTransforms.OutputColumns.IncludeColumns {
			for _, matchingColumn := range toputils.SelectMatchingStringsWithWildcard(cName, allColNames) {
				retCols[matchingColumn] = true
				colsIndexMap[matchingColumn] = index
				index++
			}
		}
	}
	if len(aggs.OutputTransforms.OutputColumns.ExcludeColumns) != 0 {
		for _, cName := range aggs.OutputTransforms.OutputColumns.ExcludeColumns {
			for _, matchingColumn := range toputils.SelectMatchingStringsWithWildcard(cName, allColNames) {
				delete(retCols, matchingColumn)
			}
		}
	}
	//todo handle rename
	/*
		if aggs.OutputTransforms.OutputColumns.RenameColumns != nil {
		}
	*/

	return retCols
}
