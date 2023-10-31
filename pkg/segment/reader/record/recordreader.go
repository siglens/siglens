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

package record

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/common/fileutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/reader/segread"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/writer"
	log "github.com/sirupsen/logrus"
)

// returns a map of record identifiers to record maps, and all columns seen
// record identifiers is segfilename + blockNum + recordNum
// If esResponse is false, _id and _type will not be added to any record
func GetRecordsFromSegment(segKey string, vTable string, blkRecIndexes map[uint16]map[uint16]uint64,
	tsKey string, esQuery bool, qid uint64,
	aggs *structs.QueryAggregators) (map[string]map[string]interface{}, map[string]bool, error) {

	var err error
	segKey, err = checkRecentlyRotatedKey(segKey)
	if err != nil {
		log.Errorf("qid=%d GetRecordsFromSegment failed to get recently rotated information for key %s table %s. err %+v", qid, segKey, vTable, err)
	}
	var allCols map[string]bool
	var exists bool
	allCols, exists = writer.CheckAndGetColsForUnrotatedSegKey(segKey)
	if !exists {
		allCols, exists = metadata.CheckAndGetColsForSegKey(segKey, vTable)
		if !exists {
			log.Errorf("GetRecordsFromSegment: failed to get column for key: %s, table %s", segKey, vTable)
			return nil, allCols, errors.New("failed to get column names for segkey in rotated and unrotated files")
		}
	}
	allCols = applyColNameTransform(allCols, aggs, qid)
	numOpenFds := int64(len(allCols))
	err = fileutils.GLOBAL_FD_LIMITER.TryAcquireWithBackoff(numOpenFds, 10, fmt.Sprintf("GetRecordsFromSegment.qid=%d", qid))
	if err != nil {
		log.Errorf("qid=%d GetRecordsFromSegment failed to acquire lock for opening %+v file descriptors. err %+v", qid, numOpenFds, err)
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
		log.Errorf("qid=%d, GetRecordsFromSegment failed to download col file. err=%v", qid, err)
		return nil, map[string]bool{}, err
	}

	defer func() {
		err = blob.SetSegSetFilesAsNotInUse(allFiles)
		if err != nil {
			log.Errorf("qid=%d, GetRecordsFromSegment failed to set segset files as not in use. err=%v", qid, err)
		}
	}()

	for ssFile := range bulkDownloadFiles {
		fd, err := os.Open(ssFile)
		if err != nil {
			log.Errorf("qid=%d, GetRecordsFromSegment failed to open col file. Tried to open file=%v, err=%v", qid, ssFile, err)
			return nil, map[string]bool{}, err
		}
		defer fd.Close()
	}

	var blockMetadata map[uint16]*structs.BlockMetadataHolder
	if writer.IsSegKeyUnrotated(segKey) {
		blockMetadata, err = writer.GetBlockSearchInfoForKey(segKey)
		if err != nil {
			log.Errorf("qid=%d GetRecordsFromSegment failed to get block search info for unrotated key %s table %s", qid, segKey, vTable)
			return nil, map[string]bool{}, err
		}
	} else {
		blockMetadata, err = metadata.GetBlockSearchInfoForKey(segKey)
		if err != nil {
			log.Errorf("GetRecordsFromSegment: failed to get blocksearchinfo for segkey=%v, err=%v", segKey, err)
			return nil, map[string]bool{}, err
		}
	}

	var blockSum []*structs.BlockSummary
	if writer.IsSegKeyUnrotated(segKey) {
		blockSum, err = writer.GetBlockSummaryForKey(segKey)
		if err != nil {
			log.Errorf("qid=%d GetRecordsFromSegment failed to get block search info for unrotated key %s table %s", qid, segKey, vTable)
			return nil, map[string]bool{}, err
		}
	} else {
		blockSum, err = metadata.GetBlockSummariesForKey(segKey)
		if err != nil {
			log.Errorf("GetRecordsFromSegment: failed to get blocksearchinfo for segkey=%v, err=%v", segKey, err)
			return nil, map[string]bool{}, err
		}
	}

	result := make(map[string]map[string]interface{})

	sharedReader, err := segread.InitSharedMultiColumnReaders(segKey, allCols, blockMetadata, blockSum, 1, qid)
	if err != nil {
		log.Errorf("GetRecordsFromSegment: failed to initialize shared readers for segkey=%v, err=%v", segKey, err)
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
		resultAllRawRecs := readAllRawRecords(allRecNums, blockIdx, multiReader, allMatchedColumns, esQuery, qid, aggs)

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

func readAllRawRecords(orderedRecNums []uint16, blockIdx uint16, segReader *segread.MultiColSegmentReader,
	allMatchedColumns map[string]bool, esQuery bool, qid uint64, aggs *structs.QueryAggregators) map[uint16]map[string]interface{} {

	results := make(map[uint16]map[string]interface{})

	dictEncCols := make(map[string]bool)
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
	}

	for _, recNum := range orderedRecNums {
		_, ok := results[recNum]
		if !ok {
			results[recNum] = make(map[string]interface{})
		}

		for _, colInfo := range segReader.AllColums {
			col := colInfo.ColumnName

			_, ok := dictEncCols[col]
			if ok {
				continue
			}

			cValEnc, err := segReader.ExtractValueFromColumnFile(col, blockIdx, recNum, qid)
			if err != nil {
				// if the column was absent for an entire block and came for other blocks, this will error, hence no error logging here
			} else {
				results[recNum][col] = cValEnc.CVal
				allMatchedColumns[col] = true
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

func applyColNameTransform(allCols map[string]bool, aggs *structs.QueryAggregators, qid uint64) map[string]bool {
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
		for _, cName := range aggs.OutputTransforms.OutputColumns.IncludeColumns {
			for _, matchingColumn := range selectMatchingStringsWithWildcard(cName, allColNames) {
				retCols[matchingColumn] = true
			}
		}
	}
	if len(aggs.OutputTransforms.OutputColumns.ExcludeColumns) != 0 {
		for _, cName := range aggs.OutputTransforms.OutputColumns.ExcludeColumns {
			for _, matchingColumn := range selectMatchingStringsWithWildcard(cName, allColNames) {
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

// Return all strings in `slice` that match `s`, which may have wildcards.
func selectMatchingStringsWithWildcard(s string, slice []string) []string {
	if strings.Contains(s, "*") {
		s = dtypeutils.ReplaceWildcardStarWithRegex(s)
	}

	// We only want exact matches.
	s = "^" + s + "$"

	compiledRegex, err := regexp.Compile(s)
	if err != nil {
		log.Errorf("selectMatchingStringsWithWildcard: regex compile failed: %v", err)
		return nil
	}

	matches := make([]string, 0)
	for _, potentialMatch := range slice {
		if compiledRegex.MatchString(potentialMatch) {
			matches = append(matches, potentialMatch)
		}
	}

	return matches
}
