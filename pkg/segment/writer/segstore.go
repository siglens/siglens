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
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bits-and-blooms/bitset"
	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/blob/ssutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/instrumentation"
	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/sortindex"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer/suffix"
	toputils "github.com/siglens/siglens/pkg/utils"

	"github.com/siglens/siglens/pkg/segment/pqmr"
	vtable "github.com/siglens/siglens/pkg/virtualtable"

	bbp "github.com/valyala/bytebufferpool"

	log "github.com/sirupsen/logrus"
)

// For Last wip we do not know how many nodes this wip will add, hence we
// leave a room for one wip's worth of recs.
const MaxAgileTreeNodeCountForAlloc = 8_066_000 // for atree to do allocations
const MaxAgileTreeNodeCount = 8_000_000

var SkipUploadOnRotate = false

const BS_INITIAL_SIZE = uint32(1000)

// SegStore Individual stream buffer
type SegStore struct {
	Lock              sync.Mutex
	earliest_millis   uint64 // earliest timestamp of a logline here
	latest_millis     uint64 // latest timestamp of a logline here
	wipBlock          WipBlock
	pqNonEmptyResults map[string]bool // map pqid => true if segstream matched > 0 records
	// segment related data
	SegmentKey            string
	segbaseDir            string
	suffix                uint64
	lastUpdated           time.Time
	lastWipFlushTime      time.Time
	VirtualTableName      string
	RecordCount           int
	AllSeenColumnSizes    map[string]uint32 // Map of Column to Column Value size. The value is a positive int if the size is consistent across records and -1 if it is not.
	pqTracker             *PQTracker
	pqMatches             map[string]*pqmr.PQMatchResults
	LastSegPqids          map[string]struct{}
	numBlocks             uint16
	BytesReceivedCount    uint64
	OnDiskBytes           uint64 // running sum of cmi/csg/bsu file sizes
	skipDe                bool   // kibana docs dont need dict enc, hence this flag
	timeCreated           time.Time
	AllSst                map[string]*structs.SegStats // map[colName] => SegStats_of_each_column
	stbHolder             *STBHolder
	OrgId                 int64
	stbDictEncWorkBuf     [][]string
	segStatsWorkBuf       []byte
	SegmentErrors         map[string]*structs.SearchErrorInfo
	bsPool                []*bitset.BitSet
	bsPoolCurrIdx         uint32
	workBufForCompression [][]byte // A work buf for each column
}

// helper struct to keep track of persistent queries and columns that need to be searched
type PQTracker struct {
	hasWildcard bool
	colNames    map[string]bool
	PQNodes     map[string]*structs.SearchNode // maps pqid to search node
}

func InitSegStore(segmentKey string, segbaseDir string, suffix uint64, virtualTableName string,
	skipDe bool, orgId int64, highTs uint64, lowTs uint64) *SegStore {

	segStore := NewSegStore(orgId)
	segStore.SegmentKey = segmentKey
	segStore.segbaseDir = segbaseDir
	segStore.suffix = suffix
	segStore.VirtualTableName = virtualTableName
	segStore.skipDe = skipDe
	segStore.OrgId = orgId

	segStore.initWipBlock()
	segStore.wipBlock.blockSummary.HighTs = highTs
	segStore.wipBlock.blockSummary.LowTs = lowTs

	return segStore
}

func NewSegStore(orgId int64) *SegStore {
	now := time.Now()
	segstore := &SegStore{
		Lock:               sync.Mutex{},
		pqNonEmptyResults:  make(map[string]bool),
		AllSeenColumnSizes: make(map[string]uint32),
		pqTracker:          initPQTracker(),
		pqMatches:          make(map[string]*pqmr.PQMatchResults),
		LastSegPqids:       make(map[string]struct{}),
		timeCreated:        now,
		lastUpdated:        now,
		lastWipFlushTime:   now,
		AllSst:             make(map[string]*structs.SegStats),
		OrgId:              orgId,
		stbDictEncWorkBuf:  make([][]string, 0),
		segStatsWorkBuf:    make([]byte, utils.WIP_SIZE),
	}

	return segstore
}

func (ss *SegStore) GetNewBitset(bsSize uint) *bitset.BitSet {
	lastKnownLen := uint32(len(ss.bsPool))
	if ss.bsPoolCurrIdx >= lastKnownLen {
		newCount := BS_INITIAL_SIZE
		ss.bsPool = toputils.ResizeSlice(ss.bsPool, int(newCount+lastKnownLen))
		for i := uint32(0); i < newCount; i++ {
			newBs := bitset.New(bsSize)
			ss.bsPool[lastKnownLen+i] = newBs
		}
	}
	retVal := ss.bsPool[ss.bsPoolCurrIdx]
	retVal.ClearAll()
	ss.bsPoolCurrIdx++
	return retVal
}

func (segStore *SegStore) StoreSegmentError(errMsg string, logLevel log.Level, err error) {
	segStore.SegmentErrors = structs.StoreError(segStore.SegmentErrors, errMsg, logLevel, err)
}

func (segStore *SegStore) LogAndFlushErrors() {
	for errMsg, errInfo := range segStore.SegmentErrors {
		toputils.LogUsingLevel(errInfo.LogLevel, "SegmentKey: %v, %v, Count: %v, ExtraInfo: %v", segStore.SegmentKey, errMsg, errInfo.Count, errInfo.Error)
		delete(segStore.SegmentErrors, errMsg)
	}
}

func (segstore *SegStore) initWipBlock() {

	segstore.wipBlock = WipBlock{
		columnBlooms:       make(map[string]*BloomIndex),
		columnRangeIndexes: make(map[string]*RangeIndex),
		columnsInBlock:     make(map[string]bool),
		colWips:            make(map[string]*ColWip),
		bb:                 bbp.Get(),
	}
	segstore.wipBlock.tomRollup = make(map[uint64]*RolledRecs)
	segstore.wipBlock.tohRollup = make(map[uint64]*RolledRecs)
	segstore.wipBlock.todRollup = make(map[uint64]*RolledRecs)
}

func (segStore *SegStore) GetSegStorePQMatchSize() uint64 {
	size := uint64(0)
	for _, v := range segStore.pqMatches {
		size += v.GetInMemSize()
	}
	return size
}

func (segstore *SegStore) resetWipBlock(forceRotate bool) error {
	segstore.wipBlock.maxIdx = 0
	segstore.bsPoolCurrIdx = 0

	for _, cwip := range segstore.wipBlock.colWips {
		cwip.cbufidx = 0
		cwip.cstartidx = 0
		cwip.cbuf.Reset()

		for dword := range cwip.deData.deMap {
			delete(cwip.deData.deMap, dword)
		}

		cwip.deData.deCount = 0
	}

	for _, bi := range segstore.wipBlock.columnBlooms {
		bi.uniqueWordCount = 0
	}

	clear(segstore.wipBlock.columnRangeIndexes)

	segstore.wipBlock.blockSummary.HighTs = 0
	segstore.wipBlock.blockSummary.LowTs = 0
	segstore.wipBlock.blockSummary.RecCount = 0
	segstore.lastWipFlushTime = time.Now()

	// delete keys from map to keep underlying storage
	clear(segstore.wipBlock.columnsInBlock)

	// Reset PQBitmaps
	for pqid := range segstore.pqMatches {
		segstore.pqMatches[pqid].ResetAll()
	}

	// don't update pqids if no more blocks will be created
	if forceRotate {
		return nil
	}

	clearTRollups(segstore.wipBlock.tomRollup)
	clearTRollups(segstore.wipBlock.tohRollup)
	clearTRollups(segstore.wipBlock.todRollup)

	return nil
}

func clearTRollups(rrmap map[uint64]*RolledRecs) {
	// delete keys from map to keep underlying storage
	clear(rrmap)
}

// do not call this function on its own, since it may result in race condition. It should be called from
// the checkAndRotateColFiles func

func (segstore *SegStore) resetSegStore(streamid string, virtualTableName string) error {
	nextSuffix, err := suffix.GetNextSuffix(streamid, virtualTableName)
	if err != nil {
		log.Errorf("resetSegStore: failed to get next suffix for stream=%+v table=%+v. err: %v", streamid, virtualTableName, err)
		return err
	}
	segstore.suffix = nextSuffix

	basedir := config.GetBaseSegDir(streamid, virtualTableName, nextSuffix)
	err = os.MkdirAll(basedir, 0764)
	if err != nil {
		log.Errorf("resetSegStore : Could not mkdir basedir=%v,  %v", basedir, err)
		return err
	}

	segstore.earliest_millis = 0
	segstore.latest_millis = 0
	segstore.SegmentKey = config.GetSegKey(streamid, virtualTableName, nextSuffix)
	segstore.segbaseDir = basedir
	segstore.VirtualTableName = virtualTableName
	segstore.RecordCount = 0
	segstore.BytesReceivedCount = 0
	segstore.OnDiskBytes = 0

	segstore.AllSeenColumnSizes = make(map[string]uint32)
	segstore.LastSegPqids = make(map[string]struct{})
	segstore.numBlocks = 0
	segstore.timeCreated = time.Now()
	if segstore.stbHolder != nil {
		segstore.stbHolder.ReleaseSTB()
		segstore.stbHolder = nil
	}

	segstore.AllSst = make(map[string]*structs.SegStats)
	segstore.pqNonEmptyResults = make(map[string]bool)
	// on reset, clear pqs info but before reset block
	segstore.pqTracker = initPQTracker()

	for _, cwip := range segstore.wipBlock.colWips {
		cwip.cbuf.Reset()
		cwip.dePackingBuf.Reset()
	}

	segstore.wipBlock.colWips = make(map[string]*ColWip)
	segstore.clearPQMatchInfo()
	segstore.LogAndFlushErrors()

	// Get New PQIDs
	persistentQueries, err := querytracker.GetTopNPersistentSearches(segstore.VirtualTableName, segstore.OrgId)
	if err != nil {
		log.Errorf("resetSegStore: error getting persistent queries: %v", err)
		return err
	}

	numPrevRec := segstore.wipBlock.blockSummary.RecCount
	for pqid, pNode := range persistentQueries {
		if _, ok := segstore.pqMatches[pqid]; !ok {
			mrSize := utils.PQMR_SIZE
			if segstore.numBlocks > 0 || numPrevRec == 0 {
				mrSize = uint(numPrevRec)
			}
			segstore.pqMatches[pqid] = pqmr.CreatePQMatchResults(mrSize)
		}
		segstore.pqTracker.addSearchNode(pqid, pNode)
	}

	promoted, demoted := toputils.SetDifference(segstore.pqMatches, segstore.LastSegPqids)
	if len(promoted) > 0 {
		log.Infof("resetSegStore: PQIDs Promoted: %v", promoted)
	}
	if len(demoted) > 0 {
		log.Infof("resetSegStore: PQIDs Demoted: %v", demoted)
	}

	err = segstore.resetWipBlock(false)
	if err != nil {
		return err
	}

	if segstore.wipBlock.currAllBmi != nil {
		clear(segstore.wipBlock.currAllBmi.AllBmh)
		clear(segstore.wipBlock.currAllBmi.CnameDict)
	}

	return nil
}

func (segstore *SegStore) GetBaseDir() string {
	return segstore.segbaseDir
}

// For some types we use a bloom index and for others we use range indices. If
// a column has both, we should convert all the values to one type.
func consolidateColumnTypes(wipBlock *WipBlock, segmentKey string) error {
	for colName := range wipBlock.columnsInBlock {
		// Check if this column has both a bloom and a range index.
		_, ok1 := wipBlock.columnBlooms[colName]
		_, ok2 := wipBlock.columnRangeIndexes[colName]
		if !(ok1 && ok2) {
			continue
		}

		// Try converting this column to numbers, but if that fails convert it to
		// strings.
		ok, err := convertColumnToNumbers(wipBlock, colName, segmentKey)
		if err != nil {
			log.Errorf("consolidateColumnTypes: error converting column %v to numbers; err=%v", colName, err)
			return err
		}
		if !ok {
			err = convertColumnToStrings(wipBlock, colName, segmentKey)
			if err != nil {
				log.Errorf("consolidateColumnTypes: error converting column %v to strings; err=%v", colName, err)
				return err
			}
		}
	}

	return nil
}

// Returns true if the conversion succeeds.
func convertColumnToNumbers(wipBlock *WipBlock, colName string, segmentKey string) (bool, error) {
	// Try converting all values to numbers.
	oldColWip := wipBlock.colWips[colName]
	newColWip := InitColWip(segmentKey, colName)
	rangeIndex := wipBlock.columnRangeIndexes[colName].Ranges

	for i := int(0); i < int(oldColWip.cbufidx); {
		valType, err := oldColWip.cbuf.At(i)
		if err != nil {
			log.Errorf("convertColumnToNumbers: cannot read valType at index %v; err=%v", i, err)
			return false, err
		}
		i++

		switch valType {
		case utils.VALTYPE_ENC_SMALL_STRING[0]:
			// Parse the string.
			numBytes := int(toputils.BytesToUint16LittleEndian(oldColWip.cbuf.Slice(i, i+2)))
			i += 2
			numberAsString := string(oldColWip.cbuf.Slice(i, i+numBytes))
			i += numBytes

			// Try converting to an integer.
			intVal, err := strconv.ParseInt(numberAsString, 10, 64)
			if err == nil {
				// Conversion succeeded.
				newColWip.cbuf.Append(utils.VALTYPE_ENC_INT64[:])
				newColWip.cbuf.AppendInt64LittleEndian(intVal)
				newColWip.cbufidx += 1 + 8
				addIntToRangeIndex(colName, intVal, rangeIndex)
				continue
			}

			// Try converting to a float.
			floatVal, err := strconv.ParseFloat(numberAsString, 64)
			if err == nil {
				// Conversion succeeded.
				newColWip.cbuf.Append(utils.VALTYPE_ENC_FLOAT64[:])
				newColWip.cbuf.AppendFloat64LittleEndian(floatVal)
				newColWip.cbufidx += 1 + 8
				addFloatToRangeIndex(colName, floatVal, rangeIndex)
				continue
			}

			// Conversion failed.
			return false, nil

		case utils.VALTYPE_ENC_INT64[0], utils.VALTYPE_ENC_FLOAT64[0]:
			// Already a number, so just copy it.
			// It's alrady in the range index, so we don't need to add it again.
			newColWip.cbuf.Append(oldColWip.cbuf.Slice(i-1, i+8))
			newColWip.cbufidx += 9
			i += 8

		case utils.VALTYPE_ENC_BACKFILL[0]:
			// This is a null value.
			newColWip.cbuf.Append(utils.VALTYPE_ENC_BACKFILL[:])
			newColWip.cbufidx += 1

		case utils.VALTYPE_ENC_BOOL[0]:
			// Cannot convert bool to number.
			return false, nil

		default:
			// Unknown type.
			log.Errorf("convertColumnToNumbers: unknown type %v", valType)
			return false, nil
		}
	}

	// Conversion succeeded, so replace the column with the new one.
	wipBlock.colWips[colName] = newColWip
	oldColWip.cbuf.Reset()
	oldColWip.dePackingBuf.Reset()
	delete(wipBlock.columnBlooms, colName)
	return true, nil
}

func convertColumnToStrings(wipBlock *WipBlock, colName string, segmentKey string) error {
	oldColWip := wipBlock.colWips[colName]
	newColWip := InitColWip(segmentKey, colName)
	bloom := wipBlock.columnBlooms[colName]

	for i := 0; i < int(oldColWip.cbufidx); {
		valType, err := oldColWip.cbuf.At(i)
		if err != nil {
			log.Errorf("convertColumnsToStrings: cannot read valType at index %v; err=%v", i, err)
			return err
		}
		i++

		switch valType {
		case utils.VALTYPE_ENC_SMALL_STRING[0]:
			// Already a string, so just copy it.
			// This is already in the bloom, so we don't need to add it again.
			numBytes := int(toputils.BytesToUint16LittleEndian(oldColWip.cbuf.Slice(i, i+2)))
			i += 2
			newColWip.cbuf.Append(oldColWip.cbuf.Slice(i-3, i+numBytes))
			newColWip.cbufidx += uint32(3 + numBytes)
			i += numBytes

		case utils.VALTYPE_ENC_INT64[0]:
			// Parse the integer.
			intVal := toputils.BytesToInt64LittleEndian(oldColWip.cbuf.Slice(i, i+8))
			i += 8

			stringVal := strconv.FormatInt(intVal, 10)
			newColWip.WriteSingleString(stringVal)
			bloom.uniqueWordCount += addToBlockBloomBothCases(bloom.Bf, []byte(stringVal))

		case utils.VALTYPE_ENC_FLOAT64[0]:
			// Parse the float.
			floatVal := toputils.BytesToFloat64LittleEndian(oldColWip.cbuf.Slice(i, i+8))
			i += 8

			stringVal := strconv.FormatFloat(floatVal, 'f', -1, 64)
			newColWip.WriteSingleString(stringVal)
			bloom.uniqueWordCount += addToBlockBloomBothCases(bloom.Bf, []byte(stringVal))

		case utils.VALTYPE_ENC_BACKFILL[0]:
			// This is a null value.
			newColWip.cbuf.Append(utils.VALTYPE_ENC_BACKFILL[:])
			newColWip.cbufidx += 1

		case utils.VALTYPE_ENC_BOOL[0]:
			// Parse the bool.
			boolVal, err := oldColWip.cbuf.At(i)
			if err != nil {
				log.Errorf("convertColumnsToStrings: cannot read bool at index %v; err=%v", i, err)
				return err
			}
			i++

			var stringVal string
			if boolVal == 0 {
				stringVal = "false"
			} else {
				stringVal = "true"
			}

			newColWip.WriteSingleString(stringVal)
			bloom.uniqueWordCount += addToBlockBloomBothCases(bloom.Bf, []byte(stringVal))

		default:
			// Unknown type.
			log.Errorf("convertColumnsToStrings: unknown type %v when converting column %v", valType, colName)
		}
	}

	// Replace the old column.
	wipBlock.colWips[colName] = newColWip
	oldColWip.cbuf.Reset()
	oldColWip.dePackingBuf.Reset()
	delete(wipBlock.columnRangeIndexes, colName)

	return nil
}

func (segstore *SegStore) AppendWipToSegfile(streamid string, forceRotate bool, isKibana bool, onTimeRotate bool) error {
	// If there's columns that had both strings and numbers in them, we need to
	// try converting them all to numbers, but if that doesn't work we'll
	// convert them all to strings.
	err := consolidateColumnTypes(&segstore.wipBlock, segstore.SegmentKey)
	if err != nil {
		log.Errorf("AppendWipToSegfile: error consolidating column types; err=%v", err)
		return err
	}

	if segstore.wipBlock.maxIdx > 0 {
		var totalBytesWritten uint64 = 0
		var totalMetadata uint64 = 0
		allColsToFlush := &sync.WaitGroup{}
		wipBlockLock := sync.Mutex{}

		segstore.initBmh()
		allBmi := segstore.wipBlock.currAllBmi

		// If the virtual table name is not present(possibly due to deletion of indices without segments), then add it back.
		if !vtable.IsVirtualTablePresent(&segstore.VirtualTableName, segstore.OrgId) {
			err := vtable.AddVirtualTable(&segstore.VirtualTableName, segstore.OrgId)
			if err != nil {
				log.Errorf("AppendWipToSegfile: Failed to add virtual table %v for orgid %v: %v", segstore.VirtualTableName, segstore.OrgId, err)
			}
		}

		//readjust workBufComp size based on num of columns in this wip
		flushParallelism := runtime.GOMAXPROCS(0) * 2
		if config.IsLowMemoryModeEnabled() {
			flushParallelism = 1
		}
		segstore.workBufForCompression = toputils.ResizeSlice(segstore.workBufForCompression,
			flushParallelism)
		// now make each of these bufs of atleast WIP_SIZE
		for i := 0; i < len(segstore.workBufForCompression); i++ {
			segstore.workBufForCompression[i] = toputils.ResizeSlice(segstore.workBufForCompression[i],
				utils.WIP_SIZE)
		}

		if config.IsAggregationsEnabled() {
			segstore.computeStarTree()
		}

		compBufIdx := 0
		currentParallelism := 0
		for colName, colInfo := range segstore.wipBlock.colWips {
			if colInfo.cbufidx > 0 {
				allColsToFlush.Add(1)
				currentParallelism++
				go func(cname string, colWip *ColWip, compBuf []byte) {
					defer allColsToFlush.Done()
					var encType []byte
					var err error
					if cname == config.GetTimeStampKey() {
						encType, err = segstore.wipBlock.encodeTimestamps()
						if err != nil {
							log.Errorf("AppendWipToSegfile: failed to encode timestamps err=%v", err)
							return
						}
						_ = segstore.writeWipTsRollups(cname)
					} else if colWip.deData.deCount > 0 && colWip.deData.deCount < wipCardLimit {
						encType = utils.ZSTD_DICTIONARY_BLOCK
					} else {
						encType = utils.ZSTD_COMLUNAR_BLOCK
					}

					if !isKibana {
						err := segstore.writeToBloom(encType, compBuf[:cap(compBuf)], cname, colWip)
						if err != nil {
							log.Errorf("AppendWipToSegfile: failed to writeToBloom colsegfilename=%v, err=%v", colWip.csgFname, err)
							return
						}
					}

					blkLen, blkOffset, err := writeWip(colWip, encType, compBuf,
						segstore.wipBlock.blockSummary.RecCount)
					if err != nil {
						log.Errorf("AppendWipToSegfile: failed to write colsegfilename=%v, err=%v", colWip.csgFname, err)
						return
					}

					atomic.AddUint64(&totalBytesWritten, uint64(blkLen))
					wipBlockLock.Lock()

					cnameIdx, ok := allBmi.CnameDict[cname]
					if !ok {
						cnameIdx = len(allBmi.CnameDict)
						allBmi.CnameDict[cname] = cnameIdx
					}

					bmh := allBmi.AllBmh[segstore.numBlocks]
					bmh.ColBlockOffAndLen[cnameIdx] = structs.ColOffAndLen{
						Offset: blkOffset,
						Length: blkLen,
					}
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
				}(colName, colInfo, segstore.workBufForCompression[currentParallelism-1])
				compBufIdx++
			}

			if currentParallelism >= flushParallelism {
				allColsToFlush.Wait()
				currentParallelism = 0
			}
		}

		allColsToFlush.Wait()
		blkSumLen := segstore.flushBlockSummary(allBmi, segstore.numBlocks)
		if !isKibana {
			// everytime we write compressedWip to segfile, we write a corresponding blockBloom
			updateUnrotatedBlockInfo(segstore.SegmentKey, segstore.VirtualTableName, &segstore.wipBlock,
				allBmi, segstore.AllSeenColumnSizes, segstore.numBlocks, totalMetadata, segstore.earliest_millis,
				segstore.latest_millis, segstore.RecordCount, segstore.OrgId, segstore.pqMatches)
		}
		atomic.AddUint64(&totalBytesWritten, blkSumLen)

		segstore.verifyBlockSum(segstore.numBlocks)

		segstore.OnDiskBytes += totalBytesWritten

		allPQIDs := make(map[string]bool)
		for pqid := range segstore.pqMatches {
			allPQIDs[pqid] = true
		}

		err := segstore.FlushSegStats()
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

		WriteRunningSegMeta(&segmeta)

		for pqid, pqResults := range segstore.pqMatches {
			segstore.pqNonEmptyResults[pqid] = segstore.pqNonEmptyResults[pqid] || pqResults.Any()
			pqidFname := fmt.Sprintf("%v/pqmr/%v.pqmr", segstore.SegmentKey, pqid)
			err := pqResults.FlushPqmr(&pqidFname, segstore.numBlocks)
			if err != nil {
				log.Errorf("AppendWipToSegfile: failed to flush pqmr results to fname %s: %v", pqidFname, err)
				return err
			}
		}

		segstore.doReadVerification()
		err = segstore.resetWipBlock(forceRotate)
		if err != nil {
			return err
		}
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

func (segstore *SegStore) doReadVerification() {

	gotErr := false
	allBmi := segstore.wipBlock.currAllBmi
	for cname, cwip := range segstore.wipBlock.colWips {
		if cname == config.GetTimeStampKey() {
			continue
		}

		cnameIdx, ok := allBmi.CnameDict[cname]
		if !ok {
			log.Errorf("doReadVerification: ERROR could not find cname: %v", cname)
			return
		}

		cOffLen := allBmi.AllBmh[segstore.numBlocks].ColBlockOffAndLen[cnameIdx]
		_, err := segstore.loadBlockUsingBuffer(cOffLen.Offset, cOffLen.Length, cwip.csgFname,
			cname)
		if err != nil {
			gotErr = true
			log.Errorf("doReadVerification: verification ERROR blkNum: %v cname: %v, err: %v",
				segstore.numBlocks, cname, err)
		}
	}

	if !gotErr {
		log.Infof("doReadVerification: SUCCESS segKey: %v, blkNum: %v, blkRecCount: %v",
			segstore.SegmentKey, segstore.numBlocks, segstore.wipBlock.blockSummary.RecCount)
	}
}

func (segstore *SegStore) initBmh() {

	if segstore.wipBlock.currAllBmi == nil {
		segstore.wipBlock.currAllBmi = &structs.AllBlksMetaInfo{
			CnameDict: make(map[string]int),
			AllBmh:    make(map[uint16]*structs.BlockMetadataHolder),
		}
	}
	var bmh *structs.BlockMetadataHolder
	// reuse old val to save on mem
	for _, v := range segstore.wipBlock.currAllBmi.AllBmh {
		bmh = v
		break
	}
	if bmh == nil {
		bmh = &structs.BlockMetadataHolder{
			BlkNum:            segstore.numBlocks,
			ColBlockOffAndLen: make([]structs.ColOffAndLen, len(segstore.wipBlock.colWips)),
		}
	}
	// delete the old keys since we don;t need them anymore
	clear(segstore.wipBlock.currAllBmi.AllBmh)

	// extend array in cases where we get new columns names that were
	// not there in previous blocks
	arrLen := len(bmh.ColBlockOffAndLen)
	numCols := len(segstore.wipBlock.colWips)
	if arrLen <= numCols {
		bmh.ColBlockOffAndLen = append(bmh.ColBlockOffAndLen,
			make([]structs.ColOffAndLen, numCols-arrLen+1)...)
	}
	segstore.wipBlock.currAllBmi.AllBmh[segstore.numBlocks] = bmh
}

func removePqmrFilesAndDirectory(pqid string, segKey string) error {

	pqFname := filepath.Join(segKey, "pqmr", pqid+".pqmr")
	err := os.Remove(pqFname)
	if err != nil {
		log.Errorf("removePqmrFilesAndDirectory:Cannot delete file: %v,  err: %v", pqFname, err)
		return err
	}
	pqmrDirectory := filepath.Join(segKey, "pqmr") + string(filepath.Separator)
	files, err := os.ReadDir(pqmrDirectory)
	if err != nil {
		log.Errorf("removePqmrFilesAndDirectory: Cannot PQMR directory: %v, err: %v",
			pqmrDirectory, err)
		return err
	}
	if len(files) == 0 {
		err := os.Remove(pqmrDirectory)
		if err != nil {
			log.Errorf("removePqmrFilesAndDirectory: Error deleting Pqmr directory: %v, err: %v",
				pqmrDirectory, err)
			return err
		}
		pqmrParentDirectory := filepath.Join(segKey) + string(filepath.Separator)
		files, err = os.ReadDir(pqmrParentDirectory)
		if err != nil {
			log.Errorf("removePqmrFilesAndDirectory: Cannot read Pqmr parent: %v, err: %v",
				pqmrParentDirectory, err)
			return err
		}
		if len(files) == 0 {
			err := os.Remove(pqmrParentDirectory)
			if err != nil {
				log.Errorf("removePqmrFilesAndDirectory: Error deleting Pqmr parent: %v, err: %v", pqmrParentDirectory, err)
				return err
			}
		}
	}
	return nil
}

func (segstore *SegStore) checkAndRotateColFiles(streamid string, forceRotate bool, onTimeRotate bool) error {
	onTreeRotate := false
	if config.IsAggregationsEnabled() && segstore.stbHolder != nil {
		nc := segstore.stbHolder.stbPtr.GetNodeCount()
		if nc > MaxAgileTreeNodeCount {
			onTreeRotate = true
		}
	}
	maxSegFileSize := config.GetMaxSegFileSize()

	if segstore.OnDiskBytes > maxSegFileSize || forceRotate || onTimeRotate || onTreeRotate {
		if hook := hooks.GlobalHooks.RotateSegment; hook != nil {
			alreadyHandled, err := hook(segstore, streamid, forceRotate)
			if err != nil {
				log.Errorf("checkAndRotateColFiles: failed to rotate segment %v, err=%v", segstore.SegmentKey, err)
				return err
			}

			if alreadyHandled {
				log.Infof("Rotating alreadyHandled segId=%v RecCount: %v, OnDiskBytes=%v, numBlocks=%v, orgId=%v, forceRotate:%v, onTimeRotate: %v, onTreeRotate: %v",
					segstore.SegmentKey, segstore.RecordCount, segstore.OnDiskBytes, segstore.numBlocks,
					segstore.OrgId, forceRotate, onTimeRotate, onTreeRotate)
				return nil
			}
		}

		instrumentation.IncrementInt64Counter(instrumentation.SEGFILE_ROTATE_COUNT, 1)
		bytesWritten := segstore.flushStarTree()
		segstore.OnDiskBytes += uint64(bytesWritten)

		if config.IsAggregationsEnabled() && segstore.stbHolder != nil {
			nc := segstore.stbHolder.stbPtr.GetNodeCount()
			cnc := segstore.stbHolder.stbPtr.GetEachColNodeCount()
			log.Infof("checkAndRotateColFiles: Release STB, segkey: %v, stree node count: %v , Each Col NodeCount: %v",
				segstore.SegmentKey, nc, cnc)
			segstore.stbHolder.ReleaseSTB()
			segstore.stbHolder = nil
		}

		log.Infof("Rotating segId=%v RecCount: %v, OnDiskBytes=%v, numBlocks=%v, orgId=%v, forceRotate:%v, onTimeRotate: %v, onTreeRotate: %v",
			segstore.SegmentKey, segstore.RecordCount, segstore.OnDiskBytes, segstore.numBlocks,
			segstore.OrgId, forceRotate, onTimeRotate, onTreeRotate)

		// delete pqmr files if empty and add to empty PQS
		for pqid, hasMatchedAnyRecordInWip := range segstore.pqNonEmptyResults {
			if !hasMatchedAnyRecordInWip {
				err := removePqmrFilesAndDirectory(pqid, segstore.SegmentKey)
				if err != nil {
					log.Errorf("checkAndRotateColFiles: Error deleting pqmr files and directory. Err: %v", err)
				}
				go AddToEmptyPqmetaChan(pqid, segstore.SegmentKey)
			}
		}

		allColsSizes := segstore.getAllColsSizes()

		allPqids := make(map[string]bool, len(segstore.pqMatches))
		for pqid := range segstore.pqMatches {
			allPqids[pqid] = true
		}

		var segmeta = structs.SegMeta{SegmentKey: segstore.SegmentKey, EarliestEpochMS: segstore.earliest_millis,
			LatestEpochMS: segstore.latest_millis, VirtualTableName: segstore.VirtualTableName,
			RecordCount: segstore.RecordCount, SegbaseDir: segstore.segbaseDir,
			BytesReceivedCount: segstore.BytesReceivedCount, OnDiskBytes: segstore.OnDiskBytes,
			ColumnNames: allColsSizes, AllPQIDs: allPqids, NumBlocks: segstore.numBlocks, OrgId: segstore.OrgId}

		addSegmeta(segmeta)
		if hook := hooks.GlobalHooks.AfterSegmentRotation; hook != nil {
			err := hook(&segmeta)
			if err != nil {
				log.Errorf("checkAndRotateColFiles: AfterSegmentRotation hook failed for segKey=%v, err=%v", segstore.SegmentKey, err)
			}
		}

		updateRecentlyRotatedSegmentFiles(segstore.SegmentKey, segstore.VirtualTableName)
		metadata.AddSegMetaToMetadata(&segmeta)

		go writeSortIndexes(segstore.SegmentKey, segstore.VirtualTableName)

		if !SkipUploadOnRotate {
			// upload ingest node dir to s3
			err := blob.UploadIngestNodeDir()
			if err != nil {
				log.Errorf("checkAndRotateColFiles: failed to upload ingest node dir , err=%v", err)
			}
		}

		err := CleanupUnrotatedSegment(segstore, streamid, false, !forceRotate)
		if err != nil {
			log.Errorf("checkAndRotateColFiles: failed to cleanup unrotated segment %v, err=%v", segstore.SegmentKey, err)
			return err
		}
	}
	return nil
}

func writeSortIndexes(segkey string, indexName string) {
	sortedIndexWG.Add(1)
	defer sortedIndexWG.Done()

	for _, cname := range sortindex.GetSortColumnNamesForIndex(indexName) {
		err := sortindex.WriteSortIndex(segkey, cname, sortindex.AllSortModes)
		if err != nil {
			log.Errorf("writeSortIndexes: failed to write sort index for segkey=%v, cname=%v; err=%v",
				segkey, cname, err)
		}
	}
}

func CleanupUnrotatedSegment(segstore *SegStore, streamId string, removeDir bool, resetSegstore bool) error {
	removeSegKeyFromUnrotatedInfo(segstore.SegmentKey)

	if removeDir {
		err := os.RemoveAll(segstore.segbaseDir)
		if err != nil {
			log.Errorf("CleanupUnrotatedSegment: failed to remove segbaseDir=%v; err=%v", segstore.segbaseDir, err)
			return err
		}
	}

	if resetSegstore {
		err := segstore.resetSegStore(streamId, segstore.VirtualTableName)
		if err != nil {
			return err
		}
	}

	return nil
}

func (segstore *SegStore) getColsAboveCardLimit() map[string]uint64 {

	colsToDrop := make(map[string]uint64)

	for _, cname := range segstore.stbHolder.stbPtr.GetGroupByKeys() {
		_, ok := segstore.AllSst[cname]
		if !ok {
			// if we can't find the column then drop this col from atree
			colsToDrop[cname] = 0
			continue
		}

		colCardinalityEstimate := segstore.AllSst[cname].GetHllCardinality()
		if colCardinalityEstimate > uint64(wipCardLimit) {
			colsToDrop[cname] = colCardinalityEstimate
		}
	}

	return colsToDrop
}

func (segstore *SegStore) initStarTreeCols() ([]string, []string) {

	gcols, inMesCols := querytracker.GetTopPersistentAggs(segstore.VirtualTableName)
	sortedGrpCols := make([]string, 0)
	grpColsCardinality := make(map[string]uint32) // use it to sort based on cardinality
	for cname := range gcols {

		// verify if cname exist in wip
		_, ok := segstore.wipBlock.colWips[cname]
		if !ok {
			continue
		}

		_, ok = segstore.AllSst[cname]
		if !ok {
			continue
		}

		// If this is the first seg after restart, we will not have the
		// AllSst hll estimates, so check this first wip's card and skip accordingly
		if segstore.wipBlock.colWips[cname].deData.deCount >= wipCardLimit {
			continue
		}

		colCardinalityEstimate := segstore.AllSst[cname].GetHllCardinality()

		if colCardinalityEstimate > uint64(wipCardLimit) {
			continue
		}

		grpColsCardinality[cname] = uint32(colCardinalityEstimate)
		sortedGrpCols = append(sortedGrpCols, cname)
	}

	sort.Slice(sortedGrpCols, func(i, j int) bool {
		return grpColsCardinality[sortedGrpCols[i]] < grpColsCardinality[sortedGrpCols[j]]
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
			return
		}

		segstore.stbHolder = GetSTB()
		// nil stbHolder indicates that no tree is available
		if segstore.stbHolder == nil {
			return
		}

		sizeToAdd := len(sortedGrpCols) - len(segstore.stbDictEncWorkBuf)
		if sizeToAdd > 0 {
			newArr := make([][]string, sizeToAdd)
			segstore.stbDictEncWorkBuf = append(segstore.stbDictEncWorkBuf, newArr...)
		}
		for colNum := 0; colNum < len(sortedGrpCols); colNum++ {
			// Make the array twice the cols cardinality we allow because
			// on the second block our HLL estimate may be still off
			if len(segstore.stbDictEncWorkBuf[colNum]) < int(MaxDeEntries) {
				segstore.stbDictEncWorkBuf[colNum] = make([]string, MaxDeEntries)
			}
		}

		segstore.stbHolder.stbPtr.ResetSegTree(sortedGrpCols, mCols, segstore.stbDictEncWorkBuf)
	}

	// nil stbHolder represents that the tree is either not available or
	// the tree creation failed on first block, so need to skip it
	if segstore.stbHolder == nil {
		return
	}

	if segstore.numBlocks != 0 {
		colsToDrop := segstore.getColsAboveCardLimit()
		if len(segstore.stbHolder.stbPtr.groupByKeys)-len(colsToDrop) <= 0 {
			log.Warnf("computeStarTree: Dropping SegTree All remaining cols found with high cardinality: %v, blockNum: %v",
				colsToDrop, segstore.numBlocks)
			segstore.stbHolder.stbPtr.DropSegTree(segstore.stbDictEncWorkBuf)
			segstore.stbHolder.ReleaseSTB()
			segstore.stbHolder = nil
			return
		}
		if len(colsToDrop) > 0 {
			log.Warnf("computeStarTree: Dropping cols with high cardinality: %v, blockNum: %v", colsToDrop, segstore.numBlocks)
			colsToDropSlice := toputils.GetKeysOfMap(colsToDrop)
			err := segstore.stbHolder.stbPtr.DropColumns(colsToDropSlice)
			if err != nil {
				log.Errorf("computeStarTree: Dropping SegTree and release STB, Error while dropping columns, err: %v", err)
				segstore.stbHolder.stbPtr.DropSegTree(segstore.stbDictEncWorkBuf)
				segstore.stbHolder.ReleaseSTB()
				segstore.stbHolder = nil
				return
			}
		}
	}

	err := segstore.stbHolder.stbPtr.ComputeStarTree(&segstore.wipBlock)

	if err != nil {
		log.Errorf("computeStarTree: Release STB, Failed to compute star tree: %v", err)
		segstore.stbHolder.ReleaseSTB()
		segstore.stbHolder = nil
		return
	}
}

func (segstore *SegStore) flushStarTree() uint32 {

	if !config.IsAggregationsEnabled() {
		return 0
	}

	if segstore.stbHolder == nil {
		return 0
	}

	size, err := segstore.stbHolder.stbPtr.EncodeStarTree(segstore.SegmentKey)
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

func (segstore *SegStore) WritePackedRecord(rawJson []byte, ts_millis uint64,
	signalType utils.SIGNAL_TYPE, cnameCacheByteHashToStr map[uint64]string,
	jsParsingStackbuf []byte) error {

	var err error
	var matchedPCols bool
	tsKey := config.GetTimeStampKey()
	if signalType == utils.SIGNAL_EVENTS || signalType == utils.SIGNAL_JAEGER_TRACES {
		matchedPCols, err = segstore.EncodeColumns(rawJson, ts_millis, &tsKey, signalType,
			cnameCacheByteHashToStr, jsParsingStackbuf)
		if err != nil {
			log.Errorf("WritePackedRecord: Failed to encode record=%+v", string(rawJson))
			return err
		}
	} else {
		log.Errorf("WritePackedRecord: Unknown SignalType=%+v", signalType)
		return errors.New("unknown signal type")
	}

	if matchedPCols {
		applyStreamingSearchToRecord(segstore, segstore.pqTracker.PQNodes, segstore.wipBlock.blockSummary.RecCount)
	}

	for _, cwip := range segstore.wipBlock.colWips {
		segstore.wipBlock.maxIdx = max(segstore.wipBlock.maxIdx, cwip.cbufidx)
	}

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

	bffd, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("flushBloomIndex: open failed fname=%v, err=%v", fname, err)
		return 0
	}
	defer bffd.Close()

	startOffset, err := bffd.Seek(0, io.SeekEnd)
	if err != nil {
		log.Errorf("flushBloomIndex: failed to seek at the end of the file fname=%v, err=%v", fname, err)
		return 0
	}

	// There is no accurate way to find the size of bloom before writing it to the file.
	// So, we will first write a dummy 4 bytes of size and then write the actual bloom size later.
	bytesWritten := uint32(0)

	_, err = bffd.Write([]byte{0, 0, 0, 0})
	if err != nil {
		log.Errorf("flushBloomIndex: failed to skip bytes for bloom size fname=%v, err=%v", fname, err)
		return 0
	}
	bytesWritten += 4

	// copy the blockNum
	if _, err = bffd.Write(toputils.Uint16ToBytesLittleEndian(ss.numBlocks)); err != nil {
		log.Errorf("flushBloomIndex: block num write failed fname=%v, err=%v", fname, err)
		return 0
	}
	bytesWritten += utils.LEN_BLKNUM_CMI_SIZE

	// write CMI type
	if _, err = bffd.Write(utils.CMI_BLOOM_INDEX); err != nil {
		log.Errorf("flushBloomIndex: CMI Type write failed fname=%v, err=%v", fname, err)
		return 0
	}
	bytesWritten += 1

	// write the blockBloom
	bloomSize, err := bi.Bf.WriteTo(bffd)
	if err != nil {
		log.Errorf("flushBloomIndex: write blockbloom failed fname=%v, err=%v", fname, err)
		return 0
	}
	bytesWritten += uint32(bloomSize)

	// write the correct bloom size
	_, err = bffd.WriteAt(toputils.Uint32ToBytesLittleEndian(bytesWritten-4), startOffset)
	if err != nil {
		log.Errorf("flushBloomIndex: failed to write bloom size to fname=%v, err=%v", fname, err)
		return 0
	}

	if len(bi.HistoricalCount) == 0 {
		bi.HistoricalCount = make([]uint32, 0)
	}
	//adding to block history list
	bi.HistoricalCount = append(bi.HistoricalCount, bi.uniqueWordCount)
	if streamIdHistory := len(bi.HistoricalCount); streamIdHistory > utils.BLOOM_SIZE_HISTORY {
		bi.HistoricalCount = bi.HistoricalCount[streamIdHistory-utils.BLOOM_SIZE_HISTORY:]

	}
	return uint64(bytesWritten)
}

// returns the number of bytes written
func (segstore *SegStore) flushBlockSummary(allBmi *structs.AllBlksMetaInfo,
	blkNum uint16) uint64 {

	fname := structs.GetBsuFnameFromSegKey(segstore.SegmentKey)

	fd, err := os.OpenFile(fname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("flushBlockSummary: open failed blockSummaryFname=%v, err=%v", fname, err)
		return 0
	}

	defer fd.Close()

	blkSumBuf := make([]byte, utils.BLOCK_SUMMARY_SIZE)
	packedLen, blkSumBuf, err := EncodeBlocksum(allBmi, &segstore.wipBlock.blockSummary,
		blkSumBuf[0:], blkNum)
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

func (segStore *SegStore) clearPQMatchInfo() {
	for pqid := range segStore.pqMatches {
		segStore.LastSegPqids[pqid] = struct{}{}
		delete(segStore.pqMatches, pqid)
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
	tsWip.cbuf.Append([]byte{uint8(tsType)})
	tsWip.cbufidx += 1
	tsWip.cbuf.AppendUint64LittleEndian(lowTs)
	tsWip.cbufidx += 8

	switch tsType {
	case structs.TS_Type8:
		var tsVal uint8
		for i := uint16(0); i < wipBlock.blockSummary.RecCount; i++ {
			tsVal = uint8(wipBlock.blockTs[i] - lowTs)
			tsWip.cbuf.Append([]byte{tsVal})
			tsWip.cbufidx += 1
		}
	case structs.TS_Type16:
		var tsVal uint16
		for i := uint16(0); i < wipBlock.blockSummary.RecCount; i++ {
			tsVal = uint16(wipBlock.blockTs[i] - lowTs)
			tsWip.cbuf.AppendUint16LittleEndian(tsVal)
			tsWip.cbufidx += 2
		}
	case structs.TS_Type32:
		var tsVal uint32
		for i := uint16(0); i < wipBlock.blockSummary.RecCount; i++ {
			tsVal = uint32(wipBlock.blockTs[i] - lowTs)
			tsWip.cbuf.AppendUint32LittleEndian(tsVal)
			tsWip.cbufidx += 4
		}
	case structs.TS_Type64:
		var tsVal uint64
		for i := uint16(0); i < wipBlock.blockSummary.RecCount; i++ {
			tsVal = wipBlock.blockTs[i] - lowTs
			tsWip.cbuf.AppendUint64LittleEndian(tsVal)
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

[Version 1B] [CnameLen 2B] [Cname xB] [ColSegEncodingLen 4B] [ColSegEncoding xB]....
*/
func (ss *SegStore) FlushSegStats() error {

	if len(ss.AllSst) <= 0 {
		found := 0
		tsKey := config.GetTimeStampKey()
		// Flush is called once one of the cbufidx is >0, but if we find no columns
		// with cbufidx > 0 other than timestamp column, only then declare this as an error
		// else we won't create a sst file
		for cname, cwip := range ss.wipBlock.colWips {
			if cwip.cbufidx > 0 {
				log.Infof("FlushSegStats: sst nil but cname: %v, cwip.cbufidx: %v, segkey: %v",
					cname, cwip.cbufidx, ss.SegmentKey)
				if cname != tsKey {
					found += 1
				}
			}
		}
		if found == 0 {
			log.Errorf("FlushSegStats: no segstats to flush, found: %v cwips with data", found)
			return errors.New("FlushSegStats: no segstats to flush")
		} else {
			return nil
		}
	}

	tempSSTFile := fmt.Sprintf("%v.sst.tmp", ss.SegmentKey)
	fd, err := os.OpenFile(tempSSTFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("FlushSegStats: Failed to open tempSSTFile=%v, err=%v", tempSSTFile, err)
		return err
	}
	defer fd.Close()

	// version
	_, err = fd.Write(utils.VERSION_SEGSTATS)
	if err != nil {
		log.Errorf("FlushSegStats: failed to write version err=%v", err)
		return err
	}

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

		idx, err := writeSstToBuf(sst, ss.segStatsWorkBuf)
		if err != nil {
			log.Errorf("FlushSegStats: error writing to buf err=%v", err)
			return err
		}

		// colsegencodinglen
		_, err = fd.Write(toputils.Uint32ToBytesLittleEndian(idx))
		if err != nil {
			log.Errorf("FlushSegStats: failed to write colsegencodlen cname=%v err=%v", cname, err)
			return err
		}

		// colsegencoding
		_, err = fd.Write(ss.segStatsWorkBuf[0:idx])
		if err != nil {
			log.Errorf("FlushSegStats: failed to write colsegencoding cname=%v err=%v", cname, err)
			return err
		}
	}

	finalName := fmt.Sprintf("%v.sst", ss.SegmentKey)
	err = os.Rename(tempSSTFile, finalName)
	if err != nil {
		return fmt.Errorf("FlushSegStats: error while migrating %v to %v, err: %v", tempSSTFile, finalName, err)
	}

	return nil
}

/*
Encoding Schema for SegStats Single Column Data
[Version 1B] [isNumeric 1B] [Count 8B] [HLL_Size 4B] [HLL_Data xB]
Numeric [DType 1B] [Min 8B] [DType 1B] [Max 8B] [NType 1B] [Sum 8B] [NumericCount 8B]
OR
NonNumeric [DType 1B] [Min_Size 2B] [Min_Data xB] [Max_Size 2B] [Max_Data xB]
*/
func writeSstToBuf(sst *structs.SegStats, buf []byte) (uint32, error) {

	idx := uint32(0)

	// version
	copy(buf[idx:], utils.VERSION_SEGSTATS_BUF_V4)
	idx++

	// isNumeric
	copy(buf[idx:], toputils.BoolToBytesLittleEndian(sst.IsNumeric))
	idx++

	// Count
	toputils.Uint64ToBytesLittleEndianInplace(sst.Count, buf[idx:])
	idx += 8

	hllDataSize := sst.GetHllDataSize()

	// HLL_Size
	toputils.Uint32ToBytesLittleEndianInplace(uint32(hllDataSize), buf[idx:])
	idx += 4

	// HLL_Data
	hllDataSliceFullCap := buf[idx : idx+uint32(hllDataSize)]

	// Ensures that the slice has a full capacity where len(slice) == cap(slice).
	// This is necessary because we're using the slice to get the HLL bytes in place,
	// and the HLL package relies on cap(slice) to determine where to write the bytes.
	// It expects the slice to be exactly the size of hllDataSize. But the slice we're
	// using is larger than that, so we need to ensure that the slice is exactly the size
	// of hllDataSize.
	hllByteSlice := hllDataSliceFullCap[:len(hllDataSliceFullCap):len(hllDataSliceFullCap)]

	hllByteSlice = sst.GetHllBytesInPlace(hllByteSlice)
	hllByteSliceLen := len(hllByteSlice)
	if hllByteSliceLen != hllDataSize {
		// This case should not happen, but if it does, we need to adjust the size
		log.Errorf("writeSstToBuf: hllByteSlice size mismatch, expected: %v, got: %v", hllDataSize, hllByteSliceLen)
		toputils.Uint32ToBytesLittleEndianInplace(uint32(hllByteSliceLen), buf[idx-4:idx])
	}
	copy(buf[idx:], hllByteSlice)
	idx += uint32(hllByteSliceLen)

	if !sst.IsNumeric {

		if sst.Min.Dtype != utils.SS_DT_STRING || sst.Max.Dtype != utils.SS_DT_STRING {
			copy(buf[idx:], []byte{byte(utils.SS_DT_BACKFILL)})
			idx++
			return idx, nil
		}

		// Min Dtype
		copy(buf[idx:], []byte{byte(utils.SS_DT_STRING)})
		idx++
		// Min Length
		minLen := uint16(len(sst.Min.CVal.(string)))
		toputils.Uint16ToBytesLittleEndianInplace(minLen, buf[idx:])
		idx += 2

		// Min Value
		copy(buf[idx:], []byte(sst.Min.CVal.(string)))
		idx += uint32(minLen)

		// Max Length
		maxLen := uint16(len(sst.Max.CVal.(string)))
		toputils.Uint16ToBytesLittleEndianInplace(maxLen, buf[idx:])
		idx += 2

		// Max Value
		copy(buf[idx:], []byte(sst.Max.CVal.(string)))
		idx += uint32(maxLen)

		return idx, nil // dont write numeric stuff if this column is not numeric
	}

	// Min NumType
	copy(buf[idx:], []byte{byte(sst.Min.Dtype)})
	idx++

	// Min
	if sst.Min.Dtype == utils.SS_DT_FLOAT {
		toputils.Float64ToBytesLittleEndianInplace(sst.Min.CVal.(float64), buf[idx:])
	} else {
		toputils.Int64ToBytesLittleEndianInplace(sst.Min.CVal.(int64), buf[idx:])
	}
	idx += 8

	// Max NumType
	copy(buf[idx:], []byte{byte(sst.Max.Dtype)})
	idx++

	// Max
	if sst.Max.Dtype == utils.SS_DT_FLOAT {
		toputils.Float64ToBytesLittleEndianInplace(sst.Max.CVal.(float64), buf[idx:])
	} else {
		toputils.Int64ToBytesLittleEndianInplace(sst.Max.CVal.(int64), buf[idx:])
	}
	idx += 8

	// Sum NumType
	copy(buf[idx:], []byte{byte(sst.NumStats.Sum.Ntype)})
	idx++

	// Sum
	if sst.NumStats.Sum.Ntype == utils.SS_DT_FLOAT {
		toputils.Float64ToBytesLittleEndianInplace(sst.NumStats.Sum.FloatVal, buf[idx:])
	} else {
		toputils.Int64ToBytesLittleEndianInplace(sst.NumStats.Sum.IntgrVal, buf[idx:])
	}
	idx += 8

	// NumCount
	toputils.Uint64ToBytesLittleEndianInplace(sst.NumStats.NumericCount, buf[idx:])
	idx += 8

	return idx, nil
}

func (ss *SegStore) getAllColsSizes() map[string]*structs.ColSizeInfo {

	allColsSizes := make(map[string]*structs.ColSizeInfo)

	for cname, colValueLen := range ss.AllSeenColumnSizes {

		if cname == config.GetTimeStampKey() {
			continue
		}

		// ColValueLen is 1 for Null Column and 2 for Bool Column.
		// We do not create CMI files for Null and Bool columns.
		shouldLogCMIError := colValueLen > 2

		fname := ssutils.GetFileNameFromSegSetFile(structs.SegSetFile{
			SegKey:     ss.SegmentKey,
			Identifier: fmt.Sprintf("%v", xxhash.Sum64String(cname)),
			FileType:   structs.Cmi,
		})
		cmiSize, onlocal := ssutils.GetFileSizeFromDisk(fname)
		if !onlocal && shouldLogCMIError {
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
		if colValueLen == 0 {
			log.Errorf("getAllColsSizes: colValueLen is 0 for cname: %v. This should not happen.", cname)
			colValueLen = utils.INCONSISTENT_CVAL_SIZE
		}

		csinfo := structs.ColSizeInfo{CmiSize: cmiSize, CsgSize: csgSize, ConsistentCvalSize: colValueLen}
		allColsSizes[cname] = &csinfo
	}
	return allColsSizes
}

func (ss *SegStore) DestroyWipBlock() {
	bbp.Put(ss.wipBlock.bb)
}

func (ss *SegStore) loadBlockUsingBuffer(offset int64, length uint32, csgFname string,
	cname string) (bool, error) {

	currFD, err := os.Open(csgFname)
	if err != nil {
		return false, err
	}
	defer currFD.Close()

	currFileBuffer := make([]byte, int(length))
	checksumFile := toputils.ChecksumFile{Fd: currFD}
	_, err = checksumFile.ReadAt(currFileBuffer[:length], offset)
	if err != nil {
		return true, fmt.Errorf("loadBlockUsingBuffer: read file error at offset: %v, err: %+v", offset, err)
	}
	oPtr := uint32(0)
	encType := currFileBuffer[oPtr]
	oPtr++

	if encType == utils.ZSTD_COMLUNAR_BLOCK[0] {
		return true, nil
	} else if encType == utils.ZSTD_DICTIONARY_BLOCK[0] {
		err := ReadDictEnc(currFileBuffer[oPtr:length], ss.numBlocks,
			ss.wipBlock.blockSummary.RecCount,
			csgFname, cname)
		return true, err
	} else {
		return true, nil
	}
}

func ReadDictEnc(buf []byte, blockNum uint16, recCount uint16, csgFname string,
	cname string) error {

	idx := uint32(0)

	// read num of dict words
	numWords := toputils.BytesToUint16LittleEndian(buf[idx : idx+2])
	idx += 2

	deTlv := make([][]byte, 0)
	deTlv = toputils.ResizeSlice(deTlv, int(numWords))
	deRecToTlv := make([]uint16, 0)
	deRecToTlv = toputils.ResizeSlice(deRecToTlv, int(recCount))

	var numRecs uint16
	var soffW uint32
	var err error
	numErrors := 0
	for w := uint16(0); w < numWords; w++ {

		soffW = idx
		// read dictWord 'T'
		switch buf[idx] {
		case utils.VALTYPE_ENC_SMALL_STRING[0]:
			//  3 => 1 for 'T' and 2 for 'L' of string
			idx += uint32(3 + toputils.BytesToUint16LittleEndian(buf[idx+1:idx+3]))
		case utils.VALTYPE_ENC_BOOL[0]:
			idx += 2 // 1 for T and 1 for Boolean value
		case utils.VALTYPE_ENC_INT64[0], utils.VALTYPE_ENC_FLOAT64[0]:
			idx += 9 // 1 for T and 8 bytes for 'L' int64
		case utils.VALTYPE_ENC_BACKFILL[0]:
			idx += 1 // 1 for T
		default:
			return fmt.Errorf("ReadDictEnc: unknown dictEnc: %v only supported flt/int64/str/bool", buf[idx])
		}

		deTlv[w] = buf[soffW:idx]

		recsArr := make([]uint16, 0)
		// read num of records
		numRecs = toputils.BytesToUint16LittleEndian(buf[idx : idx+2])
		idx += 2

		for i := uint16(0); i < numRecs; i++ {
			// at this recNum's position in the array store the idx of the TLV byte slice
			recNum := toputils.BytesToUint16LittleEndian(buf[idx : idx+2])
			idx += 2

			recsArr = append(recsArr, recNum)
			if int(recNum) >= len(deRecToTlv) {
				numErrors++
				if err == nil {
					err = fmt.Errorf("Writer : recNum %+v exceeds the number of records %+v in block %+v, fileName: %v, colname: %v",
						recNum, len(deRecToTlv), blockNum, csgFname, cname)
				}
				continue
			}
			deRecToTlv[recNum] = w
		}
		if err != nil {
			log.Infof("Writer ReadDictEnc: ERROR cname: %v for word: [%v], recsArr: %v",
				cname, string(deTlv[w]), recsArr)
		}
	}

	if err != nil {
		log.Errorf("Writer ReadDictEnc: ERROR got %v errors like: %v", numErrors, err)
	}

	return err
}

func (segstore *SegStore) verifyBlockSum(blkNum uint16) {
	fname := structs.GetBsuFnameFromSegKey(segstore.SegmentKey)
	blockSum, _, err := ReadBlockSummaries(fname, true)
	if err != nil {
		log.Errorf("verifyBlockSum ERROR reading blksum, segKey: %v, blkNum: %v",
			segstore.SegmentKey, segstore.numBlocks)
		return
	}

	recCount := blockSum[segstore.numBlocks].RecCount
	if recCount != segstore.wipBlock.blockSummary.RecCount {
		log.Infof("verifyBlockSum: ERROR verification segKey: %v, blkNum: %v, inmem blkRecCount: %v, file blkRecCount: %v",
			segstore.SegmentKey, segstore.numBlocks, segstore.wipBlock.blockSummary.RecCount,
			recCount)
	} else {
		log.Infof("verifyBlockSum: SUCCESS segKey: %v, blkNum: %v, blkRecCount: %v",
			segstore.SegmentKey, segstore.numBlocks, recCount)
	}

}

func ReadBlockSummaries(fileName string,
	summaryOnly bool) ([]*structs.BlockSummary, *structs.AllBlksMetaInfo, error) {

	blockSummaries := make([]*structs.BlockSummary, 0)
	var allBmi *structs.AllBlksMetaInfo

	if !summaryOnly {
		allBmi = &structs.AllBlksMetaInfo{CnameDict: make(map[string]int),
			AllBmh: make(map[uint16]*structs.BlockMetadataHolder),
		}
	}

	finfo, err := os.Stat(fileName)
	if err != nil {
		log.Errorf("Writer ReadBlockSummaries: error when trying to stat file=%+v. Error=%+v", fileName, err)
		return blockSummaries, allBmi, err
	}

	fileSize := finfo.Size()
	rbuf := make([]byte, int(fileSize))

	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		log.Infof("Writer ReadBlockSummaries: failed to open fileName: %v  Error: %v.",
			fileName, err)
		return blockSummaries, allBmi, err
	}
	defer fd.Close()

	_, err = fd.ReadAt(rbuf[:fileSize], 0)
	if err != nil {
		log.Errorf("Writer ReadBlockSummaries: Error reading bsu file: %v, err: %v", fileName, err)
		return blockSummaries, allBmi, err
	}

	offset := int64(0)

	for offset < fileSize {

		// todo kunal do we need blksumlen ?
		offset += 4 // for blkSumLen

		if len(rbuf[offset:]) < 2+8+8+2+2 {
			log.Errorf("Writer ReadBlockSummaries: expected at least %d more bytes for block header, got %d more bytes; file=%v, offset=%d",
				2+8+8+2+2, len(rbuf[offset:]), fileName, offset)
			return blockSummaries, allBmi, errors.New("bad data")
		}

		// read blknum
		blkNum := toputils.BytesToUint16LittleEndian(rbuf[offset:])
		offset += 2

		// read highTs
		highTs := toputils.BytesToUint64LittleEndian(rbuf[offset:])
		offset += 8

		// read lowTs
		lowTs := toputils.BytesToUint64LittleEndian(rbuf[offset:])
		offset += 8

		// read recCount
		recCount := toputils.BytesToUint16LittleEndian(rbuf[offset:])
		offset += 2

		// read numCols
		numCols := toputils.BytesToUint16LittleEndian(rbuf[offset:])
		offset += 2

		var bmh *structs.BlockMetadataHolder
		if !summaryOnly {
			bmh = &structs.BlockMetadataHolder{
				BlkNum:            blkNum,
				ColBlockOffAndLen: make([]structs.ColOffAndLen, numCols),
			}
		}

		for i := uint16(0); i < numCols; i++ {
			if len(rbuf[offset:]) < 2 {
				log.Errorf("Writer ReadBlockSummaries: expected at least %d more bytes for column name length, got %d more bytes; file=%v, offset=%d",
					2, len(rbuf[offset:]), fileName, offset)
				return blockSummaries, allBmi, errors.New("bad data")
			}
			cnamelen := toputils.BytesToUint16LittleEndian(rbuf[offset:])
			offset += 2

			if minLen := int(offset + int64(cnamelen) + 12); len(rbuf) < minLen {
				log.Errorf("Writer ReadBlockSummaries: expected at least size %d, got %d; file=%v, offset=%d",
					minLen, len(rbuf), fileName, offset)
				return blockSummaries, allBmi, errors.New("bad data")
			}

			if summaryOnly {
				offset += int64(cnamelen)
				offset += 8 + 4 // Blk Offset and Blk Len
			} else {
				cname := string(rbuf[offset : offset+int64(cnamelen)])

				offset += int64(cnamelen)
				blkOff := toputils.BytesToInt64LittleEndian(rbuf[offset:])
				offset += 8
				blkLen := toputils.BytesToUint32LittleEndian(rbuf[offset:])
				offset += 4

				cnameIdx, ok := allBmi.CnameDict[cname]
				if !ok {
					cnameIdx = len(allBmi.CnameDict)
					allBmi.CnameDict[cname] = cnameIdx
				}

				// extend array in cases where we get new columns names that were
				// not there in previous blocks
				arrLen := len(bmh.ColBlockOffAndLen)
				if arrLen <= cnameIdx {
					bmh.ColBlockOffAndLen = append(bmh.ColBlockOffAndLen,
						make([]structs.ColOffAndLen, cnameIdx-arrLen+1)...)
				}
				bmh.ColBlockOffAndLen[cnameIdx] = structs.ColOffAndLen{Offset: blkOff,
					Length: blkLen,
				}
			}
		}
		if !summaryOnly {
			allBmi.AllBmh[blkNum] = bmh
		}

		blkSumm := &structs.BlockSummary{HighTs: highTs,
			LowTs:    lowTs,
			RecCount: recCount}

		blockSummaries = append(blockSummaries, blkSumm)
	}

	return blockSummaries, allBmi, nil
}
