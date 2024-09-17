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
	"os"
	"sort"

	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/common/fileutils"
	"github.com/siglens/siglens/pkg/config"
	segmetadata "github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/reader/segread"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// returns a map of record identifiers to record maps, and all columns seen
// record identifiers is segfilename + blockNum + recordNum
// If esResponse is false, _id and _type will not be added to any record
func GetRecordsFromSegment(segKey string, vTable string, blkRecIndexes map[uint16]map[uint16]uint64,
	tsKey string, esQuery bool, qid uint64, aggs *structs.QueryAggregators,
	colsIndexMap map[string]int, allColsInAggs map[string]struct{}, nodeRes *structs.NodeResult,
	consistentCValLen map[string]uint32) (map[string]map[string]interface{}, map[string]bool, error) {

	records, columns, err := getRecordsFromSegmentHelper(segKey, vTable, blkRecIndexes, tsKey,
		esQuery, qid, aggs, colsIndexMap, allColsInAggs, nodeRes, consistentCValLen)
	if err != nil {
		// This may have failed because we're using the unrotated key, but the
		// data has since been rotated. Try with the rotated key.
		rotatedKey := writer.GetRotatedVersion(segKey)
		var rotatedErr error
		records, columns, rotatedErr = getRecordsFromSegmentHelper(rotatedKey, vTable, blkRecIndexes, tsKey,
			esQuery, qid, aggs, colsIndexMap, allColsInAggs, nodeRes, consistentCValLen)
		if rotatedErr != nil {
			log.Errorf("GetRecordsFromSegment: failed to get records for segkey=%v, err=%v."+
				" Also failed for rotated segkey=%v with err=%v.",
				segKey, err, rotatedKey, rotatedErr)
			return nil, nil, err
		}
	}

	return records, columns, nil
}

func getRecordsFromSegmentHelper(segKey string, vTable string, blkRecIndexes map[uint16]map[uint16]uint64,
	tsKey string, esQuery bool, qid uint64, aggs *structs.QueryAggregators,
	colsIndexMap map[string]int, allColsInAggs map[string]struct{}, nodeRes *structs.NodeResult,
	consistentCValLen map[string]uint32) (map[string]map[string]interface{}, map[string]bool, error) {

	var err error
	segKey, err = checkRecentlyRotatedKey(segKey)
	if err != nil {
		log.Errorf("qid=%d getRecordsFromSegmentHelper failed to get recently rotated information for key %s table %s. err %+v",
			qid, segKey, vTable, err)
	}
	var allCols map[string]bool
	var exists bool
	allCols, exists = writer.CheckAndGetColsForUnrotatedSegKey(segKey)
	if !exists {
		allCols, exists = segmetadata.CheckAndGetColsForSegKey(segKey, vTable)
		if !exists {
			log.Errorf("getRecordsFromSegmentHelper: failed to get column for key: %s, table %s", segKey, vTable)
			return nil, allCols, errors.New("failed to get column names for segkey in rotated and unrotated files")
		}
	}

	// if len(allColsInAggs) > 0, then we need to intersect the allCols with allColsInAggs
	// this is because we only need to read the columns that are present in the aggregators.
	// if len(allColsInAggs) == 0, then we ignore this step, as this would mean that the
	// query does not have a stats block, and we need to read all columns.
	if len(allColsInAggs) > 0 {
		allCols = toputils.IntersectionWithFirstMapValues(allCols, allColsInAggs)
	}
	allCols = applyColNameTransform(allCols, aggs, colsIndexMap, qid)
	numOpenFds := int64(len(allCols))
	err = fileutils.GLOBAL_FD_LIMITER.TryAcquireWithBackoff(numOpenFds, 10, fmt.Sprintf("GetRecordsFromSegment.qid=%d", qid))
	if err != nil {
		log.Errorf("qid=%d getRecordsFromSegmentHelper failed to acquire lock for opening %+v file descriptors. err %+v", qid, numOpenFds, err)
		return nil, map[string]bool{}, err
	}
	defer fileutils.GLOBAL_FD_LIMITER.Release(numOpenFds)

	bulkDownloadFiles := make(map[string]string)
	allFiles := make([]string, 0)
	for col := range allCols {
		ssFile := fmt.Sprintf("%v_%v.csg", segKey, xxhash.Sum64String(col))
		bulkDownloadFiles[ssFile] = col
		allFiles = append(allFiles, ssFile)
	}
	err = blob.BulkDownloadSegmentBlob(bulkDownloadFiles, true)
	if err != nil {
		log.Errorf("qid=%d, getRecordsFromSegmentHelper failed to download col file. err=%v", qid, err)
		return nil, map[string]bool{}, err
	}

	defer func() {
		err = blob.SetSegSetFilesAsNotInUse(allFiles)
		if err != nil {
			log.Errorf("qid=%d, getRecordsFromSegmentHelper failed to set segset files as not in use. err=%v", qid, err)
		}
	}()

	for ssFile := range bulkDownloadFiles {
		fd, err := os.Open(ssFile)
		if err != nil {
			log.Errorf("qid=%d, getRecordsFromSegmentHelper failed to open col file. Tried to open file=%v, err=%v", qid, ssFile, err)
			return nil, map[string]bool{}, err
		}
		defer fd.Close()
	}

	var blockMetadata map[uint16]*structs.BlockMetadataHolder
	if writer.IsSegKeyUnrotated(segKey) {
		blockMetadata, err = writer.GetBlockSearchInfoForKey(segKey)
		if err != nil {
			log.Errorf("qid=%d getRecordsFromSegmentHelper failed to get block search info for unrotated key %s table %s", qid, segKey, vTable)
			return nil, map[string]bool{}, err
		}
	} else {
		blockMetadata, err = metadata.GetBlockSearchInfoForKey(segKey)
		if err != nil {
			log.Errorf("getRecordsFromSegmentHelper: failed to get blocksearchinfo for segkey=%v, err=%v", segKey, err)
			return nil, map[string]bool{}, err
		}
	}

	var blockSum []*structs.BlockSummary
	if writer.IsSegKeyUnrotated(segKey) {
		blockSum, err = writer.GetBlockSummaryForKey(segKey)
		if err != nil {
			log.Errorf("qid=%d getRecordsFromSegmentHelper failed to get block search info for unrotated key %s table %s", qid, segKey, vTable)
			return nil, map[string]bool{}, err
		}
	} else {
		blockSum, err = metadata.GetBlockSummariesForKey(segKey)
		if err != nil {
			log.Errorf("getRecordsFromSegmentHelper: failed to get blocksearchinfo for segkey=%v, err=%v", segKey, err)
			return nil, map[string]bool{}, err
		}
	}

	result := make(map[string]map[string]interface{})
	sharedReader, err := segread.InitSharedMultiColumnReaders(segKey, allCols, blockMetadata, blockSum, 1, consistentCValLen, qid)
	if err != nil {
		log.Errorf("getRecordsFromSegmentHelper: failed to initialize shared readers for segkey=%v, err=%v", segKey, err)
		return nil, map[string]bool{}, err
	}
	defer sharedReader.Close()
	multiReader := sharedReader.MultiColReaders[0]

	allMatchedColumns := make(map[string]bool)
	allMatchedColumns[config.GetTimeStampKey()] = true

	// get the keys (which is blocknums, and sort them
	sortedBlkNums := make([]uint16, len(blkRecIndexes))
	idx := 0
	for bnum := range blkRecIndexes {
		sortedBlkNums[idx] = bnum
		idx++
	}
	sort.Slice(sortedBlkNums, func(i, j int) bool { return sortedBlkNums[i] < sortedBlkNums[j] })

	var addedExtraFields bool
	for _, blockIdx := range sortedBlkNums {
		// traverse the sorted blocknums and use it to extract the recordIdxTSMap
		// and then do the search, this way we read the segfiles in sequence

		recordIdxTSMap := blkRecIndexes[blockIdx]

		allRecNums := make([]uint16, len(recordIdxTSMap))
		idx := 0
		for recNum := range recordIdxTSMap {
			allRecNums[idx] = recNum
			idx++
		}
		sort.Slice(allRecNums, func(i, j int) bool { return allRecNums[i] < allRecNums[j] })
		resultAllRawRecs := readAllRawRecords(allRecNums, blockIdx, multiReader, allMatchedColumns, esQuery, qid, aggs, nodeRes)

		for r := range resultAllRawRecs {
			resultAllRawRecs[r][config.GetTimeStampKey()] = recordIdxTSMap[r]
			resultAllRawRecs[r]["_index"] = vTable

			resId := fmt.Sprintf("%s_%d_%d", segKey, blockIdx, r)
			if esQuery {
				if _, ok := resultAllRawRecs[r]["_id"]; !ok {
					resultAllRawRecs[r]["_id"] = fmt.Sprintf("%d", xxhash.Sum64String(resId))
				}
			}
			result[resId] = resultAllRawRecs[r]
			addedExtraFields = true
		}
	}
	if addedExtraFields {
		allMatchedColumns["_index"] = true
	}

	return result, allMatchedColumns, nil
}

func checkRecentlyRotatedKey(segkey string) (string, error) {
	if writer.IsRecentlyRotatedSegKey(segkey) {
		return writer.GetFileNameForRotatedSegment(segkey)
	}
	return segkey, nil
}

func getMathOpsColMap(MathOps []*structs.MathEvaluator) map[string]int {
	colMap := make(map[string]int)
	for index, mathOp := range MathOps {
		colMap[mathOp.MathCol] = index
	}
	return colMap
}

func readAllRawRecords(orderedRecNums []uint16, blockIdx uint16, segReader *segread.MultiColSegmentReader,
	allMatchedColumns map[string]bool, esQuery bool, qid uint64, aggs *structs.QueryAggregators,
	nodeRes *structs.NodeResult) map[uint16]map[string]interface{} {

	results := make(map[uint16]map[string]interface{})

	dictEncCols := make(map[string]bool)
	allColKeyIndices := make(map[int]string)
	for _, colInfo := range segReader.AllColums {
		col := colInfo.ColumnName
		if !esQuery && (col == "_type" || col == "_id") {
			dictEncCols[col] = true
			continue
		}
		if col == config.GetTimeStampKey() {
			dictEncCols[col] = true
			continue
		}
		ok := segReader.GetDictEncCvalsFromColFile(results, col, blockIdx, orderedRecNums, qid)
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

	err := segReader.ValidateAndReadBlock(colsToReadIndices, blockIdx)
	if err != nil {
		log.Errorf("qid=%d, readAllRawRecords: failed to validate and read block: %d, err: %v", qid, blockIdx, err)
		return results
	}

	var isTsCol bool
	for _, recNum := range orderedRecNums {
		_, ok := results[recNum]
		if !ok {
			results[recNum] = make(map[string]interface{})
		}

		for colKeyIdx, cname := range allColKeyIndices {

			_, ok := dictEncCols[cname]
			if ok {
				continue
			}

			isTsCol = (config.GetTimeStampKey() == cname)

			var cValEnc utils.CValueEnclosure

			err := segReader.ExtractValueFromColumnFile(colKeyIdx, blockIdx, recNum,
				qid, isTsCol, &cValEnc)
			if err != nil {
				nodeRes.StoreGlobalSearchError(fmt.Sprintf("extractSortVals: Failed to extract value for column %v", cname), log.ErrorLevel, err)
			} else {

				if mathColOpsPresent {
					colIndex, exists := mathColMap[cname]
					if exists {
						mathOp := aggs.MathOperations[colIndex]
						fieldToValue := make(map[string]utils.CValueEnclosure)
						fieldToValue[mathOp.MathCol] = cValEnc
						valueFloat, err := mathOp.ValueColRequest.EvaluateToFloat(fieldToValue)
						if err != nil {
							log.Errorf("qid=%d, failed to evaluate math operation for col %s, err=%v", qid, cname, err)
						} else {
							cValEnc.CVal = valueFloat
						}
					}
				}

				results[recNum][cname] = cValEnc.CVal
				allMatchedColumns[cname] = true
			}
		}

		if aggs != nil && aggs.OutputTransforms != nil {
			if aggs.OutputTransforms.OutputColumns != nil && aggs.OutputTransforms.OutputColumns.RenameColumns != nil {
				for oldCname, newCname := range aggs.OutputTransforms.OutputColumns.RenameColumns {
					for _, logLine := range results {
						if logLine[oldCname] != nil && oldCname != newCname {
							logLine[newCname] = logLine[oldCname]
							delete(logLine, oldCname)
							allMatchedColumns[newCname] = true
							delete(allMatchedColumns, oldCname)
						}
					}
				}
			}
		}

	}
	return results
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
	if aggs.OutputTransforms.OutputColumns.RenameColumns != nil {
		log.Info("handle aggs.OutputTransforms.OutputColumns.RenameColumn")
		//todo handle rename
	}
	return retCols
}
