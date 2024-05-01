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

package writer

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/blob/ssutils"
	"github.com/siglens/siglens/pkg/common/fileutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/querytracker"
	pqsmeta "github.com/siglens/siglens/pkg/segment/query/pqs/meta"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer/suffix"
	"github.com/siglens/siglens/pkg/usageStats"
	toputils "github.com/siglens/siglens/pkg/utils"

	"github.com/siglens/siglens/pkg/segment/pqmr"
	bbp "github.com/valyala/bytebufferpool"

	log "github.com/sirupsen/logrus"
)

const MaxAgileTreeNodeCount = 8_000_000
const colWipsSizeLimit = 2000 // We shouldn't exceed this during normal usage.

// SegStore Individual stream buffer
type SegStore struct {
	lock              sync.Mutex
	earliest_millis   uint64 // earliest timestamp of a logline here
	latest_millis     uint64 // latest timestamp of a logline here
	wipBlock          WipBlock
	pqNonEmptyResults map[string]bool // map pqid => true if segstream matched > 0 records
	// segment related data
	SegmentKey         string
	segbaseDir         string
	suffix             uint64
	lastUpdated        time.Time
	VirtualTableName   string
	RecordCount        int
	AllSeenColumns     map[string]bool
	pqTracker          *PQTracker
	numBlocks          uint16
	BytesReceivedCount uint64
	OnDiskBytes        uint64 // running sum of cmi/csg/bsu file sizes
	skipDe             bool   // kibana docs dont need dict enc, hence this flag
	timeCreated        time.Time
	AllSst             map[string]*structs.SegStats // map[colName] => SegStats_of_each_column
	sbuilder           StarTreeBuilder
	usingSegTree       bool
	OrgId              uint64
	firstTime          bool
}

// helper struct to keep track of persistent queries and columns that need to be searched
type PQTracker struct {
	hasWildcard bool
	colNames    map[string]bool
	PQNodes     map[string]*structs.SearchNode // maps pqid to search node
}

func InitSegStore(
	segmentKey string,
	segbaseDir string,
	suffix uint64,
	virtualTableName string,
	skipDe bool,
	orgId uint64,
	usingSegTree bool,
	highTs uint64,
	lowTs uint64,
) *SegStore {
	now := time.Now()
	ss := SegStore{
		lock:              sync.Mutex{},
		pqNonEmptyResults: make(map[string]bool),
		SegmentKey:        segmentKey,
		segbaseDir:        segbaseDir,
		suffix:            suffix,
		lastUpdated:       now,
		VirtualTableName:  virtualTableName,
		AllSeenColumns:    make(map[string]bool),
		pqTracker:         initPQTracker(),
		skipDe:            skipDe,
		timeCreated:       now,
		AllSst:            make(map[string]*structs.SegStats),
		usingSegTree:      usingSegTree,
		OrgId:             orgId,
		firstTime:         true,
	}

	ss.initWipBlock()
	ss.wipBlock.blockSummary.HighTs = highTs
	ss.wipBlock.blockSummary.LowTs = lowTs

	return &ss
}

func (segstore *SegStore) initWipBlock() {

	segstore.wipBlock = WipBlock{
		columnBlooms:       make(map[string]*BloomIndex),
		columnRangeIndexes: make(map[string]*RangeIndex),
		columnsInBlock:     make(map[string]bool),
		pqMatches:          make(map[string]*pqmr.PQMatchResults),
		colWips:            make(map[string]*ColWip),
		bb:                 bbp.Get(),
	}
	segstore.wipBlock.tomRollup = make(map[uint64]*RolledRecs)
	segstore.wipBlock.tohRollup = make(map[uint64]*RolledRecs)
	segstore.wipBlock.todRollup = make(map[uint64]*RolledRecs)
}

func (segstore *SegStore) resetWipBlock(forceRotate bool) error {

	segstore.wipBlock.maxIdx = 0

	if len(segstore.wipBlock.colWips) > colWipsSizeLimit {
		log.Errorf("resetWipBlock: colWips size exceeds %v; current size is %v for segKey %v",
			colWipsSizeLimit, len(segstore.wipBlock.colWips), segstore.SegmentKey)

		segstore.wipBlock.colWips = make(map[string]*ColWip)
	} else {
		for _, cwip := range segstore.wipBlock.colWips {
			cwip.cbufidx = 0
			cwip.cstartidx = 0

			cwip.deCount = 0
			for dword := range cwip.deMap {
				delete(cwip.deMap, dword)
			}
		}
	}

	for _, bi := range segstore.wipBlock.columnBlooms {
		bi.uniqueWordCount = 0
		blockBloomElementCount := getBlockBloomSize(bi)
		bi.Bf = bloom.NewWithEstimates(uint(blockBloomElementCount), utils.BLOOM_COLL_PROBABILITY)
	}

	for k := range segstore.wipBlock.columnRangeIndexes {
		delete(segstore.wipBlock.columnRangeIndexes, k)
	}

	segstore.wipBlock.blockSummary.HighTs = 0
	segstore.wipBlock.blockSummary.LowTs = 0
	numPrevRec := segstore.wipBlock.blockSummary.RecCount
	segstore.wipBlock.blockSummary.RecCount = 0

	// delete keys from map to keep underlying storage
	for col := range segstore.wipBlock.columnsInBlock {
		delete(segstore.wipBlock.columnsInBlock, col)
	}

	for pqid := range segstore.wipBlock.pqMatches {
		segstore.wipBlock.pqMatches[pqid].ResetAll()
	}

	// don't update pqids if no more blocks will be created
	if forceRotate {
		return nil
	}
	persistentQueries, err := querytracker.GetTopNPersistentSearches(segstore.VirtualTableName, segstore.OrgId)
	if err != nil {
		log.Errorf("resetWipBlock: error getting persistent queries: %v", err)
		return err
	}
	for pqid, pNode := range persistentQueries {
		if _, ok := segstore.wipBlock.pqMatches[pqid]; !ok {
			mrSize := utils.PQMR_SIZE
			if segstore.numBlocks > 0 || numPrevRec == 0 {
				mrSize = uint(numPrevRec)
			}
			segstore.wipBlock.pqMatches[pqid] = pqmr.CreatePQMatchResults(mrSize)
		}
		segstore.pqTracker.addSearchNode(pqid, pNode)
	}
	clearTRollups(segstore.wipBlock.tomRollup)
	clearTRollups(segstore.wipBlock.tohRollup)
	clearTRollups(segstore.wipBlock.todRollup)

	return nil
}

func clearTRollups(rrmap map[uint64]*RolledRecs) {
	// delete keys from map to keep underlying storage
	for k := range rrmap {
		delete(rrmap, k)
	}
}

// do not call this function on its own, since it may result in race condition. It should be called from
// the checkAndRotateColFiles func

func (segstore *SegStore) resetSegStore(streamid string, virtualTableName string) error {

	basedir := getActiveBaseSegDir(streamid, virtualTableName, segstore.suffix)
	err := os.MkdirAll(basedir, 0764)
	if err != nil {
		log.Errorf("resetSegStore : Could not mkdir basedir=%v,  %v", basedir, err)
		return err
	}

	basename := fmt.Sprintf("%s%d", basedir, segstore.suffix)
	segstore.earliest_millis = 0
	segstore.latest_millis = 0
	segstore.SegmentKey = basename
	segstore.segbaseDir = basedir
	segstore.VirtualTableName = virtualTableName
	segstore.RecordCount = 0
	segstore.BytesReceivedCount = 0
	segstore.OnDiskBytes = 0

	segstore.AllSeenColumns = make(map[string]bool)
	segstore.numBlocks = 0
	segstore.timeCreated = time.Now()
	segstore.usingSegTree = false

	segstore.AllSst = make(map[string]*structs.SegStats)
	segstore.pqNonEmptyResults = make(map[string]bool)
	// on reset, clear pqs info but before reset block
	segstore.pqTracker = initPQTracker()
	segstore.wipBlock.colWips = make(map[string]*ColWip)
	segstore.wipBlock.clearPQMatchInfo()

	err = segstore.resetWipBlock(false)
	if err != nil {
		return err
	}

	nextidx, err := suffix.GetSuffix(streamid, virtualTableName)
	if err != nil {
		log.Errorf("reset segstore: failed to get next suffix idx for stream%+v table%+v. err: %v", streamid, virtualTableName, err)
		return err
	}
	segstore.suffix = nextidx

	return nil
}

// For some types we use a bloom index and for others we use range indices. If
// a column has both, we should convert all the values to one type.
func consolidateColumnTypes(wipBlock *WipBlock, segmentKey string) {
	for colName := range wipBlock.columnsInBlock {
		// Check if this column has both a bloom and a range index.
		_, ok1 := wipBlock.columnBlooms[colName]
		_, ok2 := wipBlock.columnRangeIndexes[colName]
		if !(ok1 && ok2) {
			continue
		}

		// Try converting this column to numbers, but if that fails convert it to
		// strings.
		ok := convertColumnToNumbers(wipBlock, colName, segmentKey)
		if !ok {
			convertColumnToStrings(wipBlock, colName, segmentKey)
		}
	}
}

// Returns true if the conversion succeeds.
func convertColumnToNumbers(wipBlock *WipBlock, colName string, segmentKey string) bool {
	// Try converting all values to numbers.
	oldColWip := wipBlock.colWips[colName]
	newColWip := InitColWip(segmentKey, colName)
	rangeIndex := wipBlock.columnRangeIndexes[colName].Ranges

	for i := uint32(0); i < oldColWip.cbufidx; {
		valType := oldColWip.cbuf[i]
		i++

		switch valType {
		case utils.VALTYPE_ENC_SMALL_STRING[0]:
			// Parse the string.
			numBytes := uint32(toputils.BytesToUint16LittleEndian(oldColWip.cbuf[i : i+2]))
			i += 2
			numberAsString := string(oldColWip.cbuf[i : i+numBytes])
			i += numBytes

			// Try converting to an integer.
			intVal, err := strconv.ParseInt(numberAsString, 10, 64)
			if err == nil {
				// Conversion succeeded.
				copy(newColWip.cbuf[newColWip.cbufidx:], utils.VALTYPE_ENC_INT64[:])
				copy(newColWip.cbuf[newColWip.cbufidx+1:], toputils.Int64ToBytesLittleEndian(intVal))
				newColWip.cbufidx += 1 + 8
				addIntToRangeIndex(colName, intVal, rangeIndex)
				continue
			}

			// Try converting to a float.
			floatVal, err := strconv.ParseFloat(numberAsString, 64)
			if err == nil {
				// Conversion succeeded.
				copy(newColWip.cbuf[newColWip.cbufidx:], utils.VALTYPE_ENC_FLOAT64[:])
				copy(newColWip.cbuf[newColWip.cbufidx+1:], toputils.Float64ToBytesLittleEndian(floatVal))
				newColWip.cbufidx += 1 + 8
				addFloatToRangeIndex(colName, floatVal, rangeIndex)
				continue
			}

			// Conversion failed.
			return false

		case utils.VALTYPE_ENC_INT64[0], utils.VALTYPE_ENC_FLOAT64[0]:
			// Already a number, so just copy it.
			// It's alrady in the range index, so we don't need to add it again.
			copy(newColWip.cbuf[newColWip.cbufidx:], oldColWip.cbuf[i-1:i+8])
			newColWip.cbufidx += 9
			i += 8

		case utils.VALTYPE_ENC_BACKFILL[0]:
			// This is a null value.
			copy(newColWip.cbuf[newColWip.cbufidx:], utils.VALTYPE_ENC_BACKFILL[:])
			newColWip.cbufidx += 1

		case utils.VALTYPE_ENC_BOOL[0]:
			// Cannot convert bool to number.
			return false

		default:
			// Unknown type.
			log.Errorf("convertColumnToNumbers: unknown type %v", valType)
			return false
		}
	}

	// Conversion succeeded, so replace the column with the new one.
	wipBlock.colWips[colName] = newColWip
	delete(wipBlock.columnBlooms, colName)
	return true
}

func convertColumnToStrings(wipBlock *WipBlock, colName string, segmentKey string) {
	oldColWip := wipBlock.colWips[colName]
	newColWip := InitColWip(segmentKey, colName)
	bloom := wipBlock.columnBlooms[colName]

	for i := uint32(0); i < oldColWip.cbufidx; {
		valType := oldColWip.cbuf[i]
		i++

		switch valType {
		case utils.VALTYPE_ENC_SMALL_STRING[0]:
			// Already a string, so just copy it.
			// This is already in the bloom, so we don't need to add it again.
			numBytes := uint32(toputils.BytesToUint16LittleEndian(oldColWip.cbuf[i : i+2]))
			i += 2
			copy(newColWip.cbuf[newColWip.cbufidx:], oldColWip.cbuf[i-3:i+numBytes])
			newColWip.cbufidx += 3 + numBytes
			i += numBytes

		case utils.VALTYPE_ENC_INT64[0]:
			// Parse the integer.
			intVal := toputils.BytesToInt64LittleEndian(oldColWip.cbuf[i : i+8])
			i += 8

			stringVal := strconv.FormatInt(intVal, 10)
			newColWip.WriteSingleString(stringVal)
			bloom.uniqueWordCount += addToBlockBloom(bloom.Bf, []byte(stringVal))

		case utils.VALTYPE_ENC_FLOAT64[0]:
			// Parse the float.
			floatVal := toputils.BytesToFloat64LittleEndian(oldColWip.cbuf[i : i+8])
			i += 8

			stringVal := strconv.FormatFloat(floatVal, 'f', -1, 64)
			newColWip.WriteSingleString(stringVal)
			bloom.uniqueWordCount += addToBlockBloom(bloom.Bf, []byte(stringVal))

		case utils.VALTYPE_ENC_BACKFILL[0]:
			// This is a null value.
			copy(newColWip.cbuf[newColWip.cbufidx:], utils.VALTYPE_ENC_BACKFILL[:])
			newColWip.cbufidx += 1

		case utils.VALTYPE_ENC_BOOL[0]:
			// Parse the bool.
			boolVal := oldColWip.cbuf[i]
			i++

			var stringVal string
			if boolVal == 0 {
				stringVal = "false"
			} else {
				stringVal = "true"
			}

			newColWip.WriteSingleString(stringVal)
			bloom.uniqueWordCount += addToBlockBloom(bloom.Bf, []byte(stringVal))

		default:
			// Unknown type.
			log.Errorf("convertColumnsToStrings: unknown type %v when converting column %v", valType, colName)
		}
	}

	// Replace the old column.
	wipBlock.colWips[colName] = newColWip
	delete(wipBlock.columnRangeIndexes, colName)
}

func (segstore *SegStore) AppendWipToSegfile(streamid string, forceRotate bool, isKibana bool, onTimeRotate bool) error {
	// If there's columns that had both strings and numbers in them, we need to
	// try converting them all to numbers, but if that doesn't work we'll
	// convert them all to strings.
	consolidateColumnTypes(&segstore.wipBlock, segstore.SegmentKey)

	if segstore.wipBlock.maxIdx > 0 {
		var totalBytesWritten uint64 = 0
		var totalMetadata uint64 = 0
		allColsToFlush := &sync.WaitGroup{}
		wipBlockLock := sync.Mutex{}
		wipBlockMetadata := &structs.BlockMetadataHolder{
			BlkNum:            segstore.numBlocks,
			ColumnBlockOffset: make(map[string]int64),
			ColumnBlockLen:    make(map[string]uint32),
		}

		// worst case, each column opens 2 files (.cmi/.csg) and 2 files for segment info (.sid, .bsu)
		numOpenFDs := int64(len(segstore.wipBlock.colWips)*2 + 2)
		err := fileutils.GLOBAL_FD_LIMITER.TryAcquireWithBackoff(numOpenFDs, 10, segstore.SegmentKey)
		if err != nil {
			log.Errorf("AppendWipToSegfile failed to acquire lock for opening %+v file descriptors. err %+v", numOpenFDs, err)
			return err
		}
		defer fileutils.GLOBAL_FD_LIMITER.Release(numOpenFDs)
		for colName, colInfo := range segstore.wipBlock.colWips {
			if colInfo.cbufidx > 0 {
				allColsToFlush.Add(1)
				go func(cname string, colWip *ColWip) {
					defer allColsToFlush.Done()
					var encType []byte
					if cname == config.GetTimeStampKey() {
						encType, err = segstore.wipBlock.encodeTimestamps()
						if err != nil {
							log.Errorf("AppendWipToSegfile: failed to encode timestamps err=%v", err)
							return
						}
						_ = segstore.writeWipTsRollups(cname)
					} else if colWip.deCount > 0 && colWip.deCount < wipCardLimit {
						encType = utils.ZSTD_DICTIONARY_BLOCK
					} else {
						encType = utils.ZSTD_COMLUNAR_BLOCK
					}

					blkLen, blkOffset, err := writeWip(colWip, encType)
					if err != nil {
						log.Errorf("AppendWipToSegfile: failed to write colsegfilename=%v, err=%v", colWip.csgFname, err)
						return
					}

					atomic.AddUint64(&totalBytesWritten, uint64(blkLen))
					wipBlockLock.Lock()
					wipBlockMetadata.ColumnBlockOffset[cname] = blkOffset
					wipBlockMetadata.ColumnBlockLen[cname] = blkLen
					wipBlockLock.Unlock()

					if !isKibana {
						// if bloomIndex present then flush it
						bi, ok := segstore.wipBlock.columnBlooms[cname]
						if ok {
							writtenBytes := segstore.flushBloomIndex(cname, bi)
							atomic.AddUint64(&totalBytesWritten, writtenBytes)
							atomic.AddUint64(&totalMetadata, writtenBytes)
						}
						ri, ok := segstore.wipBlock.columnRangeIndexes[cname]
						if ok {
							writtenBytes := segstore.flushBlockRangeIndex(cname, ri)
							atomic.AddUint64(&totalBytesWritten, writtenBytes)
							atomic.AddUint64(&totalMetadata, writtenBytes)
						}
					}
				}(colName, colInfo)
			}
		}
		if config.IsAggregationsEnabled() {
			segstore.computeStarTree()
		}

		allColsToFlush.Wait()
		blkSumLen := segstore.flushBlockSummary(wipBlockMetadata, segstore.numBlocks)
		if !isKibana {
			// everytime we write compressedWip to segfile, we write a corresponding blockBloom
			updateUnrotatedBlockInfo(segstore.SegmentKey, segstore.VirtualTableName, &segstore.wipBlock,
				wipBlockMetadata, segstore.AllSeenColumns, segstore.numBlocks, totalMetadata, segstore.earliest_millis,
				segstore.latest_millis, segstore.RecordCount, segstore.OrgId)
		}
		atomic.AddUint64(&totalBytesWritten, blkSumLen)

		segstore.OnDiskBytes += totalBytesWritten

		allPQIDs := make(map[string]bool)
		for pqid := range segstore.wipBlock.pqMatches {
			allPQIDs[pqid] = true
		}

		err = segstore.FlushSegStats()
		if err != nil {
			log.Errorf("AppendWipToSegfile: failed to flushsegstats, err=%v", err)
			return err
		}

		allColsSizes := segstore.getAllColsSizes()

		var segmeta = structs.SegMeta{SegmentKey: segstore.SegmentKey, EarliestEpochMS: segstore.earliest_millis,
			LatestEpochMS: segstore.latest_millis, VirtualTableName: segstore.VirtualTableName,
			RecordCount: segstore.RecordCount, SegbaseDir: segstore.segbaseDir,
			BytesReceivedCount: segstore.BytesReceivedCount, OnDiskBytes: segstore.OnDiskBytes,
			ColumnNames: allColsSizes, AllPQIDs: allPQIDs, NumBlocks: segstore.numBlocks, OrgId: segstore.OrgId}

		sidFname := fmt.Sprintf("%v.sid", segstore.SegmentKey)
		err = writeRunningSegMeta(sidFname, &segmeta)
		if err != nil {
			log.Errorf("AppendWipToSegfile: failed to write sidFname=%v, err=%v", sidFname, err)
			return err
		}

		for pqid, pqResults := range segstore.wipBlock.pqMatches {
			segstore.pqNonEmptyResults[pqid] = segstore.pqNonEmptyResults[pqid] || pqResults.Any()
			pqidFname := fmt.Sprintf("%v/pqmr/%v.pqmr", segstore.SegmentKey, pqid)
			err := pqResults.FlushPqmr(&pqidFname, segstore.numBlocks)
			if err != nil {
				log.Errorf("AppendWipToSegfile: failed to flush pqmr results to fname %s: %v", pqidFname, err)
				return err
			}
		}

		err = segstore.resetWipBlock(forceRotate)
		if err != nil {
			return err
		}
		usageStats.UpdateCompressedStats(int64(totalBytesWritten), segmeta.OrgId)
		segstore.numBlocks += 1
	}
	if segstore.numBlocks > 0 && !isKibana {
		err := segstore.checkAndRotateColFiles(streamid, forceRotate, onTimeRotate)
		if err != nil {
			return err
		}
	}
	return nil
}

func removePqmrFilesAndDirectory(pqid string, segKey string) error {
	workingDirectory, err := os.Getwd()
	if err != nil {
		log.Errorf("Error fetching current workingDirectory")
		return err
	}
	pqFname := workingDirectory + "/" + fmt.Sprintf("%v/pqmr/%v.pqmr", segKey, pqid)
	err = os.Remove(pqFname)
	if err != nil {
		log.Errorf("Cannot delete file at %v", err)
		return err
	}
	pqmrDirectory := workingDirectory + "/" + fmt.Sprintf("%v/pqmr/", segKey)
	files, err := os.ReadDir(pqmrDirectory)
	if err != nil {
		log.Errorf("Cannot PQMR directory at %v", pqmrDirectory)
		return err
	}
	if len(files) == 0 {
		err := os.Remove(pqmrDirectory)
		if err != nil {
			log.Errorf("Error deleting Pqmr directory at %v", pqmrDirectory)
			return err
		}
		pqmrParentDirectory := workingDirectory + "/" + fmt.Sprintf("%v/", segKey)
		files, err = os.ReadDir(pqmrParentDirectory)
		if err != nil {
			log.Errorf("Cannot PQMR parent directory at %v", pqmrParentDirectory)
			return err
		}
		if len(files) == 0 {
			err := os.Remove(pqmrParentDirectory)
			if err != nil {
				log.Errorf("Error deleting Pqmr directory at %v", pqmrParentDirectory)
				return err
			}
		}
	}
	return nil
}

func (segstore *SegStore) checkAndRotateColFiles(streamid string, forceRotate bool, onTimeRotate bool) error {

	onTreeRotate := false
	if config.IsAggregationsEnabled() && segstore.usingSegTree {
		nc := segstore.sbuilder.GetNodeCount()
		if nc > MaxAgileTreeNodeCount {
			onTreeRotate = true
		}
	}

	if segstore.OnDiskBytes > maxSegFileSize || forceRotate || onTimeRotate || onTreeRotate {

		instrumentation.IncrementInt64Counter(instrumentation.SEGFILE_ROTATE_COUNT, 1)
		bytesWritten := segstore.flushStarTree()
		segstore.OnDiskBytes += uint64(bytesWritten)

		activeBasedir := getActiveBaseSegDir(streamid, segstore.VirtualTableName, segstore.suffix-1)
		finalBasedir := getFinalBaseSegDir(streamid, segstore.VirtualTableName, segstore.suffix-1)

		finalSegmentKey := fmt.Sprintf("%s%d", finalBasedir, segstore.suffix-1)

		log.Infof("Rotating segId=%v RecCount: %v, OnDiskBytes=%v, numBlocks=%v, finalSegKey=%v orgId=%v",
			segstore.SegmentKey, segstore.RecordCount, segstore.OnDiskBytes, segstore.numBlocks,
			finalSegmentKey, segstore.OrgId)

		// make sure the parent dir of final exists, the two path calls are because getFinal.. func
		// returns a '/' at the end
		err := os.MkdirAll(path.Dir(path.Dir(finalBasedir)), 0764)
		if err != nil {
			return err
		}
		// delete pqmr files if empty and add to empty PQS
		for pqid, hasMatchedAnyRecordInWip := range segstore.pqNonEmptyResults {
			if !hasMatchedAnyRecordInWip {
				err := removePqmrFilesAndDirectory(pqid, segstore.SegmentKey)
				if err != nil {
					log.Errorf("Error deleting pqmr files and directory. Err: %v", err)
				}
				go pqsmeta.AddEmptyResults(pqid, segstore.SegmentKey, segstore.VirtualTableName)
			}
		}

		allColsSizes := segstore.getAllColsSizes()

		// move the whole dir in one shot
		err = os.Rename(activeBasedir, finalBasedir)
		if err != nil {
			log.Errorf("checkAndRotateColFiles: failed to mv active to final, err=%v", err)
			return err
		}
		// Upload segment files to s3
		filesToUpload := fileutils.GetAllFilesInDirectory(finalBasedir)

		err = blob.UploadSegmentFiles(filesToUpload)
		if err != nil {
			log.Errorf("checkAndRotateColFiles: failed to upload segment files , err=%v", err)
		}

		allPqids := make(map[string]bool, len(segstore.wipBlock.pqMatches))
		for pqid := range segstore.wipBlock.pqMatches {
			allPqids[pqid] = true
		}

		var segmeta = structs.SegMeta{SegmentKey: finalSegmentKey, EarliestEpochMS: segstore.earliest_millis,
			LatestEpochMS: segstore.latest_millis, VirtualTableName: segstore.VirtualTableName,
			RecordCount: segstore.RecordCount, SegbaseDir: finalBasedir,
			BytesReceivedCount: segstore.BytesReceivedCount, OnDiskBytes: segstore.OnDiskBytes,
			ColumnNames: allColsSizes, AllPQIDs: allPqids, NumBlocks: segstore.numBlocks, OrgId: segstore.OrgId}

		AddNewRotatedSegment(segmeta)

		updateRecentlyRotatedSegmentFiles(segstore.SegmentKey, finalSegmentKey)
		removeSegKeyFromUnrotatedInfo(segstore.SegmentKey)

		// upload ingest node dir to s3
		err = blob.UploadIngestNodeDir()
		if err != nil {
			log.Errorf("checkAndRotateColFiles: failed to upload ingest node dir , err=%v", err)
		}

		if !forceRotate {
			err = segstore.resetSegStore(streamid, segstore.VirtualTableName)
			if err != nil {
				return err
			}
		}

	}
	return nil
}

func (segstore *SegStore) initStarTreeCols() ([]string, []string) {

	gcols, inMesCols := querytracker.GetTopPersistentAggs(segstore.VirtualTableName)
	sortedGrpCols := make([]string, 0)
	gcMap := make(map[string]uint32) // use it to sort based on cardinality
	for _, cname := range gcols {

		// verify if cname exist in wip
		_, ok := segstore.wipBlock.colWips[cname]
		if !ok {
			continue
		}

		_, ok = segstore.AllSst[cname]
		if !ok {
			continue
		}

		cest := uint32(segstore.AllSst[cname].Hll.Estimate())
		gcMap[cname] = cest
		sortedGrpCols = append(sortedGrpCols, cname)
	}

	sort.Slice(sortedGrpCols, func(i, j int) bool {
		return gcMap[sortedGrpCols[i]] < gcMap[sortedGrpCols[j]]
	})

	mCols := make([]string, 0)
	// Check if measureCols are present in wip
	for mCname := range inMesCols {

		// verify if measure cname exist in wip
		_, ok := segstore.wipBlock.colWips[mCname]
		if !ok {
			continue
		}
		mCols = append(mCols, mCname)
	}

	return sortedGrpCols, mCols
}

func (segstore *SegStore) computeStarTree() {

	if segstore.numBlocks == 0 {
		sortedGrpCols, mCols := segstore.initStarTreeCols()
		if len(sortedGrpCols) == 0 || len(mCols) == 0 {
			segstore.usingSegTree = false
			return
		}
		segstore.usingSegTree = true
		segstore.sbuilder.ResetSegTree(&segstore.wipBlock, sortedGrpCols, mCols)
	}

	if !segstore.usingSegTree { // if tree creation had failed on first block, then skip it
		return
	}

	err := segstore.sbuilder.ComputeStarTree(&segstore.wipBlock)
	if err != nil {
		segstore.usingSegTree = false
		log.Errorf("computeStarTree: Failed to compute star tree: %v", err)
		return
	}
}

func (segstore *SegStore) flushStarTree() uint32 {

	if !config.IsAggregationsEnabled() {
		return 0
	}

	if !segstore.usingSegTree { // if tree creation had failed on first block, then skip it
		return 0
	}

	size, err := segstore.sbuilder.EncodeStarTree(segstore.SegmentKey)
	if err != nil {
		log.Errorf("flushStarTree: Failed to encode star tree: %v", err)
		return 0
	}
	return size
}

func (segstore *SegStore) adjustEarliestLatestTimes(ts_millis uint64) {

	if segstore.earliest_millis == 0 {
		segstore.earliest_millis = ts_millis
	} else {
		if ts_millis < segstore.earliest_millis {
			segstore.earliest_millis = ts_millis
		}
	}

	if segstore.latest_millis == 0 {
		segstore.latest_millis = ts_millis
	} else {
		if ts_millis > segstore.latest_millis {
			segstore.latest_millis = ts_millis
		}
	}
}

func (wipBlock *WipBlock) adjustEarliestLatestTimes(ts_millis uint64) {

	if wipBlock.blockSummary.LowTs == 0 {
		wipBlock.blockSummary.LowTs = ts_millis
	} else {
		if ts_millis < wipBlock.blockSummary.LowTs {
			wipBlock.blockSummary.LowTs = ts_millis
		}
	}

	if wipBlock.blockSummary.HighTs == 0 {
		wipBlock.blockSummary.HighTs = ts_millis
	} else {
		if ts_millis > wipBlock.blockSummary.HighTs {
			wipBlock.blockSummary.HighTs = ts_millis
		}
	}

}

func (segstore *SegStore) WritePackedRecord(rawJson []byte, ts_millis uint64, signalType utils.SIGNAL_TYPE) error {

	var maxIdx uint32
	var err error
	var matchedPCols bool
	tsKey := config.GetTimeStampKey()
	if signalType == utils.SIGNAL_EVENTS || signalType == utils.SIGNAL_JAEGER_TRACES {
		maxIdx, matchedPCols, err = segstore.EncodeColumns(rawJson, ts_millis, &tsKey, signalType)
		if err != nil {
			log.Errorf("WritePackedRecord: Failed to encode record=%+v", string(rawJson))
			return err
		}
	} else {
		log.Errorf("WritePackedRecord: Unknown SignalType=%+v", signalType)
		return errors.New("unknown signal type")
	}

	if matchedPCols {
		applyStreamingSearchToRecord(segstore.wipBlock, segstore.pqTracker.PQNodes, segstore.wipBlock.blockSummary.RecCount)
	}

	segstore.wipBlock.maxIdx = maxIdx
	segstore.wipBlock.blockSummary.RecCount += 1
	segstore.RecordCount++
	segstore.lastUpdated = time.Now()
	return nil
}

// flushes bloom index and returns number of bytes written
func (ss *SegStore) flushBloomIndex(cname string, bi *BloomIndex) uint64 {

	if bi == nil {
		log.Errorf("flushBloomIndex: bi was nill for segkey=%v", ss.SegmentKey)
		return 0
	}

	fname := fmt.Sprintf("%s_%v.cmi", ss.SegmentKey, xxhash.Sum64String(cname))

	bffd, err := os.OpenFile(fname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("flushBloomIndex: open failed fname=%v, err=%v", fname, err)
		return 0
	}

	defer bffd.Close()

	var buf bytes.Buffer
	bufWriter := bufio.NewWriter(&buf)

	// there is no accurate way to find exactly how many bytes the write.to is going to write
	// and we need that number , so that we write it first and then the actual bloom data
	// hence this messiness to write it to some buffer, get the bytesWritten count and then do
	// the actual write
	bytesWritten, bferr := bi.Bf.WriteTo(bufWriter)
	if bferr != nil {
		log.Errorf("flushBloomIndex: write buf failed fname=%v, err=%v", fname, bferr)
		return 0
	}

	bytesWritten += utils.LEN_BLKNUM_CMI_SIZE // for blkNum
	bytesWritten += 1                         // reserver for CMI_Type

	// copy the size of blockBloom in uint32
	if _, err = bffd.Write(toputils.Uint32ToBytesLittleEndian(uint32(bytesWritten))); err != nil {
		log.Errorf("flushBloomIndex: bloomsize write failed fname=%v, err=%v", fname, err)
		return 0
	}

	// copy the blockNum
	if _, err = bffd.Write(toputils.Uint16ToBytesLittleEndian(ss.numBlocks)); err != nil {
		log.Errorf("flushBloomIndex: bloomsize write failed fname=%v, err=%v", fname, err)
		return 0
	}

	// write CMI type
	if _, err = bffd.Write(utils.CMI_BLOOM_INDEX); err != nil {
		log.Errorf("flushBloomIndex: CMI Type write failed fname=%v, err=%v", fname, err)
		return 0
	}

	// write the blockBloom
	_, bferr = bi.Bf.WriteTo(bffd)
	if bferr != nil {
		log.Errorf("flushBloomIndex: write blockbloom failed fname=%v, err=%v", fname, bferr)
		return 0
	}

	finalBytesWritten := bytesWritten + 4 // add 4 for size
	if len(bi.HistoricalCount) == 0 {
		bi.HistoricalCount = make([]uint32, 0)
	}
	//adding to block history list
	bi.HistoricalCount = append(bi.HistoricalCount, bi.uniqueWordCount)
	if streamIdHistory := len(bi.HistoricalCount); streamIdHistory > utils.BLOOM_SIZE_HISTORY {
		bi.HistoricalCount = bi.HistoricalCount[streamIdHistory-utils.BLOOM_SIZE_HISTORY:]

	}
	return uint64(finalBytesWritten)
}

// returns the number of bytes written
func (segstore *SegStore) flushBlockSummary(bmh *structs.BlockMetadataHolder, blkNum uint16) uint64 {

	fname := structs.GetBsuFnameFromSegKey(segstore.SegmentKey)

	fd, err := os.OpenFile(fname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("flushBlockSummary: open failed blockSummaryFname=%v, err=%v", fname, err)
		return 0
	}

	defer fd.Close()

	blkSumBuf := make([]byte, utils.BLOCK_SUMMARY_SIZE)
	packedLen, blkSumBuf, err := EncodeBlocksum(bmh, &segstore.wipBlock.blockSummary, blkSumBuf[0:], blkNum)
	if err != nil {
		log.Errorf("flushBlockSummary: EncodeBlocksum: Failed to encode blocksummary=%+v, err=%v",
			segstore.wipBlock.blockSummary, err)
		return 0
	}
	if _, err := fd.Write(blkSumBuf[:packedLen]); err != nil {
		log.Errorf("flushBlockSummary:  write failed blockSummaryFname=%v, err=%v", fname, err)
		return 0
	}
	return uint64(packedLen)
}

func (segstore *SegStore) flushBlockRangeIndex(cname string, ri *RangeIndex) uint64 {

	if ri == nil {
		log.Errorf("flushBlockRangeIndex: ri was nill for segkey=%v", segstore.SegmentKey)
		return 0
	}

	fname := fmt.Sprintf("%s_%v.cmi", segstore.SegmentKey, xxhash.Sum64String(cname))

	fr, err := os.OpenFile(fname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("flushBlockRangeIndex: open failed fname=%v, err=%v", fname, err)
		return 0
	}

	packedLen, blkRIBuf, err := EncodeRIBlock(ri.Ranges, segstore.numBlocks)
	if err != nil {
		log.Errorf("flushBlockRangeIndex: EncodeRIBlock: Failed to encode BlockRangeIndex=%+v, err=%v", ri.Ranges, err)
		return 0
	}

	if _, err := fr.Write(blkRIBuf[0:packedLen]); err != nil {
		log.Errorf("flushBlockRangeIndex:  write failed blockRangeIndexFname=%v, err=%v", fname, err)
		return 0
	}
	fr.Close()
	return uint64(packedLen)
}

func initPQTracker() *PQTracker {
	return &PQTracker{
		colNames:    make(map[string]bool),
		PQNodes:     make(map[string]*structs.SearchNode),
		hasWildcard: false,
	}
}

func (pct *PQTracker) addSearchNode(pqid string, sNode *structs.SearchNode) {
	pct.PQNodes[pqid] = sNode

	if pct.hasWildcard {
		return
	}
	cols, wildcard := sNode.GetAllColumnsToSearch()
	for colName := range cols {
		pct.colNames[colName] = true
	}
	pct.hasWildcard = wildcard
}

func (pct *PQTracker) isColumnInPQuery(col string) bool {
	if pct.hasWildcard {
		return true
	}
	if pct.colNames == nil {
		return false
	}
	_, ok := pct.colNames[col]
	return ok
}

func (wip *WipBlock) clearPQMatchInfo() {
	for pqid := range wip.pqMatches {
		delete(wip.pqMatches, pqid)
	}
}

func (wipBlock *WipBlock) encodeTimestamps() ([]byte, error) {

	encType := utils.TIMESTAMP_TOPDIFF_VARENC

	tsWip := wipBlock.colWips[config.GetTimeStampKey()]
	tsWip.cbufidx = 0 // reset to zero since packer we set it to 1, so that the writeWip gets invoked

	var tsType structs.TS_TYPE
	diff := wipBlock.blockSummary.HighTs - wipBlock.blockSummary.LowTs

	if diff <= toputils.UINT8_MAX {
		tsType = structs.TS_Type8
	} else if diff <= toputils.UINT16_MAX {
		tsType = structs.TS_Type16
	} else if diff <= toputils.UINT32_MAX {
		tsType = structs.TS_Type32
	} else {
		tsType = structs.TS_Type64
	}

	lowTs := wipBlock.blockSummary.LowTs

	// store TS_TYPE and lowTs for reconstruction needs
	copy(tsWip.cbuf[tsWip.cbufidx:], []byte{uint8(tsType)})
	tsWip.cbufidx += 1
	copy(tsWip.cbuf[tsWip.cbufidx:], toputils.Uint64ToBytesLittleEndian(lowTs))
	tsWip.cbufidx += 8

	switch tsType {
	case structs.TS_Type8:
		var tsVal uint8
		for i := uint16(0); i < wipBlock.blockSummary.RecCount; i++ {
			tsVal = uint8(wipBlock.blockTs[i] - lowTs)
			copy(tsWip.cbuf[tsWip.cbufidx:], []byte{tsVal})
			tsWip.cbufidx += 1
		}
	case structs.TS_Type16:
		var tsVal uint16
		for i := uint16(0); i < wipBlock.blockSummary.RecCount; i++ {
			tsVal = uint16(wipBlock.blockTs[i] - lowTs)
			copy(tsWip.cbuf[tsWip.cbufidx:], toputils.Uint16ToBytesLittleEndian(tsVal))
			tsWip.cbufidx += 2
		}
	case structs.TS_Type32:
		var tsVal uint32
		for i := uint16(0); i < wipBlock.blockSummary.RecCount; i++ {
			tsVal = uint32(wipBlock.blockTs[i] - lowTs)
			copy(tsWip.cbuf[tsWip.cbufidx:], toputils.Uint32ToBytesLittleEndian(tsVal))
			tsWip.cbufidx += 4
		}
	case structs.TS_Type64:
		var tsVal uint64
		for i := uint16(0); i < wipBlock.blockSummary.RecCount; i++ {
			tsVal = wipBlock.blockTs[i] - lowTs
			copy(tsWip.cbuf[tsWip.cbufidx:], toputils.Uint64ToBytesLittleEndian(tsVal))
			tsWip.cbufidx += 8
		}
	}

	return encType, nil
}

/*

   [blkNum 2B][numBlocks 2B][BuckData xxB]......

   BuckData ===>
   [bucketKey 8B][rrEncType 1B][mrDataSize 2B]{matchedRecordData ....}

*/

func (ss *SegStore) writeWipTsRollups(cname string) error {

	// todo move this dir creation to initSegStore
	dirName := fmt.Sprintf("%v/rups/", path.Dir(ss.SegmentKey))
	if _, err := os.Stat(dirName); os.IsNotExist(err) {
		err := os.MkdirAll(dirName, os.FileMode(0764))
		if err != nil {
			log.Errorf("writeWipTsRollups: Failed to create directory %s: %v", dirName, err)
			return err
		}
	}

	var reterr error = nil

	fname := fmt.Sprintf("%v/rups/%v.crup", path.Dir(ss.SegmentKey), xxhash.Sum64String(cname+"m"))
	err := writeSingleRup(ss.numBlocks, fname, ss.wipBlock.tomRollup)
	if err != nil {
		log.Errorf("writeWipTsRollups: failed to write minutes rollup file, err=%v", err)
		reterr = err
	}

	fname = fmt.Sprintf("%v/rups/%v.crup", path.Dir(ss.SegmentKey), xxhash.Sum64String(cname+"h"))
	err = writeSingleRup(ss.numBlocks, fname, ss.wipBlock.tohRollup)
	if err != nil {
		log.Errorf("writeWipTsRollups: failed to write hour rollup file, err=%v", err)
		reterr = err
	}
	fname = fmt.Sprintf("%v/rups/%v.crup", path.Dir(ss.SegmentKey), xxhash.Sum64String(cname+"d"))
	err = writeSingleRup(ss.numBlocks, fname, ss.wipBlock.todRollup)
	if err != nil {
		log.Errorf("writeWipTsRollups: failed to write day rollup file, err=%v", err)
		reterr = err
	}

	return reterr
}

func writeSingleRup(blkNum uint16, fname string, tRup map[uint64]*RolledRecs) error {
	fd, err := os.OpenFile(fname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("writeSingleRup: open failed fname=%v, err=%v", fname, err)
		return err
	}

	defer fd.Close()

	_, err = fd.Seek(0, 2) // go to the end of the file
	if err != nil {
		log.Errorf("writeSingleRup: failed to get end offset %+v", err)
		return err
	}

	// write blkNum
	_, err = fd.Write(toputils.Uint16ToBytesLittleEndian(blkNum))
	if err != nil {
		log.Errorf("writeSingleRup: blkNum write failed fname=%v, err=%v", fname, err)
		return err
	}

	// write num of bucketKeys
	_, err = fd.Write(toputils.Uint16ToBytesLittleEndian(uint16(len(tRup))))
	if err != nil {
		log.Errorf("writeSingleRup: failed to write num of bucket keys %+v", err)
		return err
	}

	for bkey, rr := range tRup {

		// write bucketKey ts
		if _, err = fd.Write(toputils.Uint64ToBytesLittleEndian(bkey)); err != nil {
			log.Errorf("writeSingleRup: blkNum=%v bkey=%v write failed fname=%v, err=%v",
				blkNum, bkey, fname, err)
			return err
		}

		// write encoding type
		if _, err = fd.Write([]byte{utils.RR_ENC_BITSET}); err != nil {
			log.Errorf("writeSingleRup: blkNum=%v bkey=%v enc type failed fname=%v, err=%v",
				blkNum, bkey, fname, err)
			return err
		}

		// we could use a Compact here, but in past we saw compact loose data
		// once compact is fixed then we can use it here.
		// pad an extra word (64 bits) so that shrink does not loose data
		cb := rr.MatchedRes.Shrink(uint(rr.lastRecNum + 64))
		mrSize := uint16(cb.GetInMemSize())
		if _, err = fd.Write(toputils.Uint16ToBytesLittleEndian(uint16(mrSize))); err != nil {
			log.Errorf("writeSingleRup: blkNum=%v bkey=%v mrsize write failed fname=%v, err=%v",
				blkNum, bkey, fname, err)
			return err
		}

		// write actual bitset
		err = cb.WriteTo(fd)
		if err != nil {
			log.Errorf("writeSingleRup: blkNum=%v bkey=%v bitset write failed fname=%v, err=%v",
				blkNum, bkey, fname, err)
			return err
		}
	}

	return nil
}

/*
Encoding Scheme for all columns single file

[Version 1B] [CnameLen 2B] [Cname xB] [ColSegEncodingLen 2B] [ColSegEncoding xB]....
*/
func (ss *SegStore) FlushSegStats() error {

	if len(ss.AllSst) <= 0 {
		log.Errorf("FlushSegStats: no segstats to flush")
		return errors.New("FlushSegStats: no segstats to flush")
	}

	fname := fmt.Sprintf("%v.sst", ss.SegmentKey)
	fd, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("FlushSegStats: Failed to open file=%v, err=%v", fname, err)
		return err
	}
	defer fd.Close()

	// version
	_, err = fd.Write([]byte{1})
	if err != nil {
		log.Errorf("FlushSegStats: failed to write version err=%v", err)
		return err
	}

	buf := make([]byte, utils.WIP_SIZE)
	for cname, sst := range ss.AllSst {

		// cname len
		_, err = fd.Write(toputils.Uint16ToBytesLittleEndian(uint16(len(cname))))
		if err != nil {
			log.Errorf("FlushSegStats: failed to write cnamelen cname=%v err=%v", cname, err)
			return err
		}

		// cname
		_, err = fd.WriteString(cname)
		if err != nil {
			log.Errorf("FlushSegStats: failed to write cname cname=%v err=%v", cname, err)
			return err
		}

		idx, err := writeSstToBuf(sst, buf)
		if err != nil {
			log.Errorf("FlushSegStats: error writing to buf err=%v", err)
			return err
		}

		// colsegencodinglen
		_, err = fd.Write(toputils.Uint16ToBytesLittleEndian(idx))
		if err != nil {
			log.Errorf("FlushSegStats: failed to write colsegencodlen cname=%v err=%v", cname, err)
			return err
		}

		// colsegencoding
		_, err = fd.Write(buf[0:idx])
		if err != nil {
			log.Errorf("FlushSegStats: failed to write colsegencoding cname=%v err=%v", cname, err)
			return err
		}
	}

	return nil
}

/*
Encoding Schema for SegStats Single Column Data
[Version 1B] [isNumeric 1B] [Count 8B] [HLL_Size 2B] [HLL_Data xB]
[N_type 1B] [Min 8B] [N_type 1B] [Max 8B] [N_type 1B] [Sum 8B]
*/
func writeSstToBuf(sst *structs.SegStats, buf []byte) (uint16, error) {

	idx := uint16(0)

	// version
	copy(buf[idx:], []byte{1})
	idx++

	// isNumeric
	copy(buf[idx:], toputils.BoolToBytesLittleEndian(sst.IsNumeric))
	idx++

	// Count
	copy(buf[idx:], toputils.Uint64ToBytesLittleEndian(sst.Count))
	idx += 8

	hllData, err := sst.Hll.MarshalBinary()
	if err != nil {
		log.Errorf("writeSstToBuf: HLL marshal failed err=%v", err)
		return idx, err
	}

	// HLL_Size
	copy(buf[idx:], toputils.Uint16ToBytesLittleEndian(uint16(len(hllData))))
	idx += 2

	// HLL_Data
	copy(buf[idx:], hllData)
	idx += uint16(len(hllData))

	if !sst.IsNumeric {
		return idx, nil // dont write numeric stuff if this column is not numeric
	}

	// Min NumType
	copy(buf[idx:], []byte{byte(sst.NumStats.Min.Ntype)})
	idx++

	// Min
	if sst.NumStats.Min.Ntype == utils.SS_DT_FLOAT {
		copy(buf[idx:], toputils.Float64ToBytesLittleEndian(sst.NumStats.Min.FloatVal))
	} else {
		copy(buf[idx:], toputils.Int64ToBytesLittleEndian(sst.NumStats.Min.IntgrVal))
	}
	idx += 8

	// Max NumType
	copy(buf[idx:], []byte{byte(sst.NumStats.Max.Ntype)})
	idx++

	// Max
	if sst.NumStats.Max.Ntype == utils.SS_DT_FLOAT {
		copy(buf[idx:], toputils.Float64ToBytesLittleEndian(sst.NumStats.Max.FloatVal))
	} else {
		copy(buf[idx:], toputils.Int64ToBytesLittleEndian(sst.NumStats.Max.IntgrVal))
	}
	idx += 8

	// Sum NumType
	copy(buf[idx:], []byte{byte(sst.NumStats.Sum.Ntype)})
	idx++

	// Sum
	if sst.NumStats.Sum.Ntype == utils.SS_DT_FLOAT {
		copy(buf[idx:], toputils.Float64ToBytesLittleEndian(sst.NumStats.Sum.FloatVal))
	} else {
		copy(buf[idx:], toputils.Int64ToBytesLittleEndian(sst.NumStats.Sum.IntgrVal))
	}
	idx += 8

	return idx, nil
}

func (ss *SegStore) getAllColsSizes() map[string]*structs.ColSizeInfo {

	allColsSizes := make(map[string]*structs.ColSizeInfo)

	for cname := range ss.AllSeenColumns {

		if cname == config.GetTimeStampKey() {
			continue
		}

		fname := ssutils.GetFileNameFromSegSetFile(structs.SegSetFile{
			SegKey:     ss.SegmentKey,
			Identifier: fmt.Sprintf("%v", xxhash.Sum64String(cname)),
			FileType:   structs.Cmi,
		})
		cmiSize, onlocal := ssutils.GetFileSizeFromDisk(fname)
		if !onlocal {
			log.Errorf("getAllColsSizes: cmi cname: %v, fname: %+v not on local disk", cname, fname)
		}

		fname = ssutils.GetFileNameFromSegSetFile(structs.SegSetFile{
			SegKey:     ss.SegmentKey,
			Identifier: fmt.Sprintf("%v", xxhash.Sum64String(cname)),
			FileType:   structs.Csg,
		})
		csgSize, onlocal := ssutils.GetFileSizeFromDisk(fname)
		if !onlocal {
			log.Errorf("getAllColsSizes: csg cname: %v, fname: %+v not on local disk", cname, fname)
		}

		csinfo := structs.ColSizeInfo{CmiSize: cmiSize, CsgSize: csgSize}
		allColsSizes[cname] = &csinfo
	}
	return allColsSizes
}

func (ss *SegStore) DestroyWipBlock() {
	bbp.Put(ss.wipBlock.bb)
}
