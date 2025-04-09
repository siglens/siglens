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
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/klauspost/compress/zstd"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/common/fileutils"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	"github.com/siglens/siglens/pkg/utils"

	log "github.com/sirupsen/logrus"

	"github.com/bits-and-blooms/bloom/v3"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/instrumentation"
	bbp "github.com/valyala/bytebufferpool"
)

// Throttle the number of indexes to help prevent excessive memory usage.
const maxAllowedSegStores = 1000

// global map
var allSegStores = map[string]*SegStore{}
var allSegStoresLock sync.RWMutex = sync.RWMutex{}
var sortedIndexWG = &sync.WaitGroup{}

var KibanaInternalBaseDir string

var plePool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		ple := NewPLE()
		return ple
	},
}

// Create a writer that caches compressors.
// For this operation type we supply a nil Reader.
var encoder, _ = zstd.NewWriter(nil)
var decoder, _ = zstd.NewReader(nil)

func InitKibanaInternalData() {
	KibanaInternalBaseDir = config.GetDataPath() + "common/kibanainternaldata/"
	err := os.MkdirAll(KibanaInternalBaseDir, 0764)
	if err != nil {
		log.Error(err)
	}
}

type SegfileRotateInfo struct {
	FinalName   string
	TimeRotated uint64
	tableName   string
}

type DeData struct {
	// [hash(dictWordKey)] => {colWip.cbufidxStart, len(dword)}
	deMap   map[string][]uint16
	deCount uint16 // keeps track of cardinality count for this COL_WIP
}

type ColWip struct {
	cbufidx      uint32 // end index of buffer, only cbuf[:cbufidx] exists
	cstartidx    uint32 // start index of last record, so cbuf[cstartidx:cbufidx] is the encoded last record
	cbuf         *utils.Buffer
	csgFname     string // file name of csg file
	deData       *DeData
	dePackingBuf *utils.Buffer
}

type RangeIndex struct {
	Ranges map[string]*structs.Numbers
}

type BloomIndex struct {
	Bf              *bloom.BloomFilter
	uniqueWordCount uint32
	HistoricalCount []uint32
}

type RolledRecs struct {
	lastRecNum uint16
	MatchedRes *pqmr.PQMatchResults
}

// All WIP BLOCK related info will be stored here

type WipBlock struct {
	columnBlooms       map[string]*BloomIndex
	blockSummary       structs.BlockSummary
	columnRangeIndexes map[string]*RangeIndex
	colWips            map[string]*ColWip
	columnsInBlock     map[string]bool
	maxIdx             uint32
	blockTs            []uint64
	tomRollup          map[uint64]*RolledRecs // top-of-minute rollup
	tohRollup          map[uint64]*RolledRecs // top-of-hour rollup
	todRollup          map[uint64]*RolledRecs // top-of-day rollup
	bb                 *bbp.ByteBuffer        // byte buffer pool for HLL byte inserts
}

type ParsedLogEvent struct {
	allCnames       []string  // array of all cnames
	allCvals        [][]byte  // array of all column values byte slices
	allCvalsTypeLen [][9]byte // array of all column values type and len (3 bytes for strings; 9 for numbers)
	numCols         uint16    // number of columns in this log record
	indexName       string
	rawJson         []byte
	timestampMillis uint64
}

func NewPLE() *ParsedLogEvent {
	return &ParsedLogEvent{
		allCnames:       make([]string, 0),
		allCvals:        make([][]byte, 0),
		allCvalsTypeLen: make([][9]byte, 0),
		numCols:         0,
	}
}

func GetNewPLE(rawJson []byte, tsNow uint64, indexName string, tsKey *string, jsParsingStackbuf []byte) (*ParsedLogEvent, error) {
	tsMillis := utils.ExtractTimeStamp(rawJson, tsKey)
	if tsMillis == 0 {
		tsMillis = tsNow
	}
	ple := plePool.Get().(*ParsedLogEvent)
	ple.Reset()
	ple.SetRawJson(rawJson)
	ple.SetTimestamp(tsMillis)
	ple.SetIndexName(indexName)
	err := ParseRawJsonObject("", rawJson, tsKey, jsParsingStackbuf[:], ple)
	if err != nil {
		return nil, fmt.Errorf("GetNewPLE: Error while parsing raw json object, err: %v", err)
	}
	return ple, nil
}

func ReleasePLEs(pleArray []*ParsedLogEvent) {
	for _, ple := range pleArray {
		plePool.Put(ple)
	}
}

func (ple *ParsedLogEvent) Reset() {
	ple.allCnames = ple.allCnames[:0]
	ple.allCvals = ple.allCvals[:0]
	ple.allCvalsTypeLen = ple.allCvalsTypeLen[:0]
	ple.numCols = 0
}

func (ple *ParsedLogEvent) MakeSpaceForNewColumn() {
	ple.allCnames = append(ple.allCnames, "")
	ple.allCvals = append(ple.allCvals, nil)
	ple.allCvalsTypeLen = append(ple.allCvalsTypeLen, [9]byte{})
}

func (ple *ParsedLogEvent) SetIndexName(indexName string) {
	ple.indexName = indexName
}

func (ple *ParsedLogEvent) GetIndexName() string {
	return ple.indexName
}

func (ple *ParsedLogEvent) SetRawJson(rawJson []byte) {
	ple.rawJson = rawJson
}

func (ple *ParsedLogEvent) GetRawJson() []byte {
	return ple.rawJson
}

func (ple *ParsedLogEvent) SetTimestamp(timestampMillis uint64) {
	ple.timestampMillis = timestampMillis
}

func (ple *ParsedLogEvent) GetTimestamp() uint64 {
	return ple.timestampMillis
}

// returns in memory size of a single wip block
func (wp *WipBlock) getSize() uint64 {
	size := uint64(0)
	for _, v := range wp.columnBlooms {
		size += uint64(v.Bf.Cap() / 8)
	}
	size += wp.blockSummary.GetSize()
	size += uint64(24 * len(wp.columnRangeIndexes))

	for _, cwip := range wp.colWips {
		size += uint64(cwip.cbuf.Cap())
	}

	return size
}

func HostnameDir() {
	var sb strings.Builder
	sb.WriteString(config.GetDataPath())
	sb.WriteString("ingestnodes/")
	sb.WriteString(config.GetHostID())
	hostnamesDir := sb.String()
	err := os.MkdirAll(hostnamesDir, 0764)
	if err != nil {
		log.Error(err)
	}
}

// returns the total size used by AllSegStores
func GetInMemorySize() uint64 {
	allSegStoresLock.RLock()
	defer allSegStoresLock.RUnlock()

	totalSize := uint64(0)
	numOpenCols := 0
	for _, s := range allSegStores {
		s.Lock.Lock()
		totalSize += s.wipBlock.getSize()
		totalSize += s.GetSegStorePQMatchSize()
		numOpenCols += len(s.wipBlock.colWips)
		s.Lock.Unlock()
	}

	maxOpenCols := config.GetMaxAllowedColumns()
	if numOpenCols > int(maxOpenCols) {
		log.Errorf("GetInMemorySize: numOpenCols=%v exceeds maxAllowedCols=%v", numOpenCols, maxOpenCols)
	} else if numOpenCols > 0 {
		totalSize = uint64(float64(totalSize) * 1.1)
	}

	totalSize += metrics.GetTotalEncodedSize()

	return totalSize
}

func InitWriterNode() {
	// one time initialization
	AllUnrotatedSegmentInfo = make(map[string]*UnrotatedSegmentInfo)
	RecentlyRotatedSegmentFiles = make(map[string]*SegfileRotateInfo)
	metrics.InitMetricsSegStore()

	initSmr()

	// timeBasedWIPFlushToFile
	go idleWipFlushToFile()
	go maxWaitWipFlushToFile()

	go removeStaleSegmentsLoop()
	go cleanRecentlyRotatedInfo()
	go timeBasedUploadIngestNodeDir()
	HostnameDir()
	InitKibanaInternalData()
}

// TODO: this should be pushed based & we should have checks in uploadingestnode function to prevent uploading unupdated files.
func timeBasedUploadIngestNodeDir() {
	for {
		time.Sleep(UPLOAD_INGESTNODE_DIR_SLEEP)
		err := blob.UploadIngestNodeDir()
		if err != nil {
			log.Errorf("timeBasedUploadIngestNodeDir: failed to upload ingestnode dir: err=%v", err)
		}
	}
}

func cleanRecentlyRotatedInternal() {
	currTime := utils.GetCurrentTimeInMs()
	recentlyRotatedSegmentFilesLock.Lock()
	defer recentlyRotatedSegmentFilesLock.Unlock()
	for key, value := range RecentlyRotatedSegmentFiles {
		if currTime-value.TimeRotated > STALE_RECENTLY_ROTATED_ENTRY_MS {
			delete(RecentlyRotatedSegmentFiles, key)
		}
	}
}

func cleanRecentlyRotatedInfo() {
	for {
		sleepDuration := time.Millisecond * STALE_RECENTLY_ROTATED_ENTRY_MS
		time.Sleep(sleepDuration)
		cleanRecentlyRotatedInternal()
	}
}

func AddEntryToInMemBuf(streamid string, indexName string, flush bool,
	signalType SIGNAL_TYPE, orgid int64, rid uint64, cnameCacheByteHashToStr map[uint64]string,
	jsParsingStackbuf []byte, pleArray []*ParsedLogEvent) error {

	segstore, err := getOrCreateSegStore(streamid, indexName, orgid)
	if err != nil {
		log.Errorf("AddEntryToInMemBuf, getSegstore err=%v", err)
		return err
	}

	return segstore.AddEntry(streamid, indexName, flush, signalType, orgid, rid,
		cnameCacheByteHashToStr, jsParsingStackbuf, pleArray)
}

func (ss *SegStore) doLogEventFilling(ple *ParsedLogEvent, tsKey *string) (bool, error) {
	ss.encodeTime(ple.timestampMillis, tsKey)

	matchedCol := false
	var colWip *ColWip
	colBlooms := ss.wipBlock.columnBlooms
	colRis := ss.wipBlock.columnRangeIndexes
	segstats := ss.AllSst
	for i := uint16(0); i < ple.numCols; i++ {
		cname := ple.allCnames[i]
		ctype := ple.allCvalsTypeLen[i][0]
		colWip, _, matchedCol = ss.initAndBackFillColumn(cname, ValTypeToSSDType(ctype), matchedCol)

		switch ctype {
		case VALTYPE_ENC_SMALL_STRING[0]:
			if cname != "_type" && cname != "_index" {
				_, ok := colBlooms[cname]
				if !ok {
					bi := &BloomIndex{}
					bi.uniqueWordCount = 0
					bi.Bf = bloom.NewWithEstimates(uint(BLOCK_BLOOM_SIZE), BLOOM_COLL_PROBABILITY)
					colBlooms[cname] = bi
				}
			}
			startIdx := colWip.cbufidx
			recLen := uint32(utils.BytesToUint16LittleEndian(ple.allCvalsTypeLen[i][1:3]))
			colWip.cbuf.Append(ple.allCvalsTypeLen[i][:3])
			colWip.cbufidx += 3
			colWip.cbuf.Append(ple.allCvals[i][:recLen])
			colWip.cbufidx += recLen

			addSegStatsStrIngestion(ss.AllSst, cname, colWip.cbuf.Slice(int(colWip.cbufidx-recLen), int(colWip.cbufidx)))
			if !ss.skipDe {
				ss.checkAddDictEnc(colWip, colWip.cbuf.Slice(int(startIdx), int(colWip.cbufidx)), ss.wipBlock.blockSummary.RecCount, startIdx, false)
			}
			ss.updateColValueSizeInAllSeenColumns(cname, colWip.cbufidx-startIdx)
		case VALTYPE_ENC_BOOL[0]:
			startIdx := colWip.cbufidx
			colWip.cbuf.Append(ple.allCvalsTypeLen[i][0:1])
			colWip.cbufidx += 1
			colWip.cbuf.Append(ple.allCvals[i][0:1])
			colWip.cbufidx += 1
			boolVal := utils.BytesToBoolLittleEndian(ple.allCvals[i][0:1])
			var asciiBytesBuf bytes.Buffer
			_, err := fmt.Fprintf(&asciiBytesBuf, "%v", boolVal)
			if err != nil {
				return false, err
			}
			addSegStatsBool(segstats, cname, asciiBytesBuf.Bytes())
			if !ss.skipDe {
				ss.checkAddDictEnc(colWip, colWip.cbuf.Slice(int(startIdx), int(colWip.cbufidx)), ss.wipBlock.blockSummary.RecCount, startIdx, false)
			}
			ss.updateColValueSizeInAllSeenColumns(cname, colWip.cbufidx-startIdx)
		case VALTYPE_ENC_INT64[0], VALTYPE_ENC_UINT64[0], VALTYPE_ENC_FLOAT64[0]:
			ri, ok := colRis[cname]
			if !ok {
				ri = &RangeIndex{}
				ri.Ranges = make(map[string]*structs.Numbers)
				colRis[cname] = ri
			}

			startIdx := colWip.cbufidx
			colWip.cbuf.Append(ple.allCvalsTypeLen[i][0:9])
			colWip.cbufidx += 9

			var numType SS_IntUintFloatTypes
			var intVal int64
			var uintVal uint64
			var floatVal float64
			// TODO: store the ascii in ple.allCvals to avoid recomputation
			var asciiBytesBuf bytes.Buffer
			switch ctype {
			case VALTYPE_ENC_INT64[0]:
				numType = SS_INT64
				intVal = utils.BytesToInt64LittleEndian(ple.allCvalsTypeLen[i][1:9])
				_, err := fmt.Fprintf(&asciiBytesBuf, "%d", intVal)
				if err != nil {
					return false, utils.TeeErrorf("doLogEventFilling: cannot write intVal %v: %v", intVal, err)
				}
			case VALTYPE_ENC_UINT64[0]:
				numType = SS_UINT64
				uintVal = utils.BytesToUint64LittleEndian(ple.allCvalsTypeLen[i][1:9])
				_, err := fmt.Fprintf(&asciiBytesBuf, "%d", uintVal)
				if err != nil {
					return false, utils.TeeErrorf("doLogEventFilling: cannot write uintVal %v: %v", uintVal, err)
				}
			case VALTYPE_ENC_FLOAT64[0]:
				numType = SS_FLOAT64
				floatVal = utils.BytesToFloat64LittleEndian(ple.allCvalsTypeLen[i][1:9])
				_, err := fmt.Fprintf(&asciiBytesBuf, "%f", floatVal)
				if err != nil {
					return false, utils.TeeErrorf("doLogEventFilling: cannot write floatVal %v: %v", floatVal, err)
				}
			default:
				return false, utils.TeeErrorf("doLogEventFilling: shouldn't get here; ctype: %v", ctype)
			}

			updateRangeIndex(cname, ri.Ranges, numType, intVal, uintVal, floatVal)
			addSegStatsNums(segstats, cname, numType, intVal, uintVal, floatVal, asciiBytesBuf.Bytes())
			ss.updateColValueSizeInAllSeenColumns(cname, 9)
			if !ss.skipDe {
				ss.checkAddDictEnc(colWip, colWip.cbuf.Slice(int(startIdx), int(colWip.cbufidx)), ss.wipBlock.blockSummary.RecCount, startIdx, false)
			}
		case VALTYPE_ENC_BACKFILL[0]:
			colWip.cbuf.Append(VALTYPE_ENC_BACKFILL[:])
			colWip.cbufidx += 1
			if !ss.skipDe {
				ss.checkAddDictEnc(colWip, VALTYPE_ENC_BACKFILL[:], ss.wipBlock.blockSummary.RecCount,
					colWip.cbufidx-1, true)
			}
			ss.updateColValueSizeInAllSeenColumns(cname, 1)
		default:
			return false, utils.TeeErrorf("doLogEventFilling: unknown ctype: %v", ctype)
		}
	}

	for colName, foundCol := range ss.wipBlock.columnsInBlock {
		if foundCol {
			ss.wipBlock.columnsInBlock[colName] = false
			continue
		}
		colWip, ok := ss.wipBlock.colWips[colName]
		if !ok {
			log.Errorf("doLogEventFilling: tried to backfill a column with no colWip! %v. This should not happen", colName)
			return false, fmt.Errorf("tried to backfill a column with no colWip")
		}
		colWip.cstartidx = colWip.cbufidx
		colWip.cbuf.Append(VALTYPE_ENC_BACKFILL[:])
		colWip.cbufidx += 1
		ss.updateColValueSizeInAllSeenColumns(colName, 1)
		// also do backfill dictEnc for this recnum
		ss.checkAddDictEnc(colWip, VALTYPE_ENC_BACKFILL[:], ss.wipBlock.blockSummary.RecCount,
			colWip.cbufidx-1, true)
	}
	return matchedCol, nil
}

func (segstore *SegStore) AddEntry(streamid string, indexName string, flush bool,
	signalType SIGNAL_TYPE, orgid int64, rid uint64, cnameCacheByteHashToStr map[uint64]string,
	jsParsingStackbuf []byte, pleArray []*ParsedLogEvent) error {

	tsKey := config.GetTimeStampKey()

	segstore.Lock.Lock()
	defer segstore.Lock.Unlock()

	for _, ple := range pleArray {

		if segstore.wipBlock.maxIdx+MAX_RECORD_SIZE >= WIP_SIZE ||
			segstore.wipBlock.blockSummary.RecCount >= MAX_RECS_PER_WIP {
			err := segstore.AppendWipToSegfile(streamid, false, false, false)
			if err != nil {
				log.Errorf("SegStore.AddEntry: failed to append segkey=%v, err=%v", segstore.SegmentKey, err)
				return err
			}
			instrumentation.IncrementInt64Counter(instrumentation.WIP_BUFFER_FLUSH_COUNT, 1)
		}

		matchedPCols, err := segstore.doLogEventFilling(ple, &tsKey)
		if err != nil {
			log.Errorf("AddEntry: log event filling failed; segkey: %v, err: %v", segstore.SegmentKey, err)
			return err
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

		segstore.adjustEarliestLatestTimes(ple.timestampMillis)
		segstore.wipBlock.adjustEarliestLatestTimes(ple.timestampMillis)
		segstore.BytesReceivedCount += uint64(len(ple.rawJson))

		if hook := hooks.GlobalHooks.AfterWritingToSegment; hook != nil {
			err := hook(rid, segstore, ple.GetRawJson(), ple.GetTimestamp(), signalType)
			if err != nil {
				log.Errorf("SegStore.AddEntry: error from AfterWritingToSegment hook: %v", err)
			}
		}

		if flush {
			err = segstore.AppendWipToSegfile(streamid, false, false, false)
			if err != nil {
				log.Errorf("SegStore.AddEntry: failed to append during flush segkey=%v, err=%v", segstore.SegmentKey, err)
				return err
			}
		}
	}
	return nil
}

func AddTimeSeriesEntryToInMemBuf(rawJson []byte, signalType SIGNAL_TYPE, orgid int64) error {
	switch signalType {
	case SIGNAL_METRICS_OTSDB:
		tagsHolder := metrics.GetTagsHolder()
		mName, dp, ts, err := metrics.ExtractOTSDBPayload(rawJson, tagsHolder)
		if err != nil {
			return err
		}
		err = metrics.EncodeDatapoint(mName, tagsHolder, dp, ts, uint64(len(rawJson)), orgid)
		if err != nil {
			return fmt.Errorf("entry rejected for metric %s %v because of error: %v", mName, tagsHolder, err)
		}

	case SIGNAL_METRICS_OTLP:
		tagsHolder := metrics.GetTagsHolder()
		mName, dp, ts, err := metrics.ExtractOTLPPayload(rawJson, tagsHolder)
		if err != nil {
			return err
		}
		err = metrics.EncodeDatapoint(mName, tagsHolder, dp, ts, uint64(len(rawJson)), orgid)
		if err != nil {
			return fmt.Errorf("entry rejected for metric %s %v because of error: %v", mName, tagsHolder, err)
		}

	default:
		return fmt.Errorf("unknown signal type %+v", signalType)
	}

	return nil
}

// This function is used when os.Interrupt is caught
// meta files need to be updated to not lose range/bloom/file path info on node failure
func ForcedFlushToSegfile() {
	log.Warnf("Flushing %+v segment files on server exit", len(allSegStores))
	allSegStoresLock.Lock()
	for streamid, segstore := range allSegStores {
		segstore.Lock.Lock()
		err := segstore.AppendWipToSegfile(streamid, true, false, false)
		if err != nil {
			log.Errorf("ForcedFlushToSegfile: failed to append err=%v", err)
		}
		log.Warnf("Flushing segment file for streamid %s server exit", streamid)
		segstore.Lock.Unlock()
		delete(allSegStores, streamid)
	}
	allSegStoresLock.Unlock()
}

func WaitForSortedIndexToComplete() {
	sortedIndexWG.Wait()
}

func idleWipFlushToFile() {
	for {
		idleWipFlushDuration := time.Duration(config.GetIdleWipFlushIntervalSecs()) * time.Second
		time.Sleep(idleWipFlushDuration)
		FlushWipBufferToFile(&idleWipFlushDuration, nil)
	}
}

func maxWaitWipFlushToFile() {
	for {
		maxWaitWipFlushDuration := time.Duration(config.GetMaxWaitWipFlushIntervalSecs()) * time.Second
		time.Sleep(maxWaitWipFlushDuration)
		FlushWipBufferToFile(nil, &maxWaitWipFlushDuration)
	}
}

func (ss *SegStore) isSegstoreUnusedSinceTime(timeDuration time.Duration) bool {
	return time.Since(ss.lastUpdated) > timeDuration && ss.RecordCount == 0
}

func removeStaleSegments() {

	segStoresToDeleteChan := make(chan string, len(allSegStores))

	allSegStoresLock.RLock()
	for streamid, segstore := range allSegStores {

		segstore.Lock.Lock()
		// remove unused segstores
		if segstore.isSegstoreUnusedSinceTime(STALE_SEGMENT_DELETION_SECONDS) {
			segStoresToDeleteChan <- streamid
		}
		segstore.Lock.Unlock()
	}
	allSegStoresLock.RUnlock()

	close(segStoresToDeleteChan)

	allSegStoresLock.Lock()
	for streamid := range segStoresToDeleteChan {
		segstore, ok := allSegStores[streamid]
		if !ok {
			continue
		}
		// Check again here to make sure we are not deleting a segstore that was updated
		if segstore.isSegstoreUnusedSinceTime(STALE_SEGMENT_DELETION_SECONDS) {
			log.Infof("Deleting unused segstore for segkey: %v", segstore.SegmentKey)
			delete(allSegStores, streamid)
		}
	}
	allSegStoresLock.Unlock()
}

func ForceRotateSegmentsForTest() {
	allSegStoresLock.Lock()
	for streamid, segstore := range allSegStores {
		segstore.Lock.Lock()
		err := segstore.AppendWipToSegfile(streamid, false, false, true)
		if err != nil {
			log.Errorf("ForceRotateSegmentsForTest: failed to append,  streamid=%s err=%v", err, streamid)
		} else {
			log.Infof("Rotating segment due to time. streamid=%s and table=%s", streamid, segstore.VirtualTableName)
		}
		segstore.Lock.Unlock()
	}
	allSegStoresLock.Unlock()
}

func removeStaleSegmentsLoop() {
	for {
		time.Sleep(STALE_SEGMENT_DELETION_SLEEP_SECONDS * time.Second)
		removeStaleSegments()
	}

}

func FlushWipBufferToFile(idleWipFlushDuration *time.Duration, maxWaitWipFlushDuration *time.Duration) {
	allSegStoresLock.RLock()
	for streamid, segstore := range allSegStores {
		segstore.Lock.Lock()
		if segstore.wipBlock.maxIdx == 0 {
			segstore.Lock.Unlock()
			continue
		}

		shouldFlush := false

		if idleWipFlushDuration != nil && time.Since(segstore.lastUpdated) > *idleWipFlushDuration {
			shouldFlush = true
		}

		if !shouldFlush && maxWaitWipFlushDuration != nil && time.Since(segstore.lastWipFlushTime) > *maxWaitWipFlushDuration {
			shouldFlush = true
		}

		if shouldFlush {
			err := segstore.AppendWipToSegfile(streamid, false, false, false)
			if err != nil {
				log.Errorf("FlushWipBufferToFile: failed to append, err=%v", err)
			}
		}

		segstore.Lock.Unlock()
	}
	allSegStoresLock.RUnlock()
}

func InitColWip(segKey string, colName string) *ColWip {

	deData := DeData{deMap: make(map[string][]uint16),
		deCount: 0,
	}

	return &ColWip{
		csgFname:     fmt.Sprintf("%v_%v.csg", segKey, xxhash.Sum64String(colName)),
		deData:       &deData,
		cbuf:         &utils.Buffer{},
		dePackingBuf: &utils.Buffer{},
	}
}

// In-mem Buf Format
// [varint Record-0 varint Record-1 ....]
// varint stores length of Record , it would occupy 1-9 bytes
// The first bit of each byte of varint specifies whether there are follow on bytes
// rest 7 bits are used to store the number
func getOrCreateSegStore(streamid string, table string, orgId int64) (*SegStore, error) {

	segstore := getSegStore(streamid)
	if segstore == nil {
		return createSegStore(streamid, table, orgId)
	}

	return segstore, nil
}

func getSegStore(streamid string) *SegStore {
	allSegStoresLock.RLock()
	defer allSegStoresLock.RUnlock()

	segstore, present := allSegStores[streamid]
	if !present {
		return nil
	}

	return segstore
}

func createSegStore(streamid string, table string, orgId int64) (*SegStore, error) {
	allSegStoresLock.Lock()
	defer allSegStoresLock.Unlock()

	if len(allSegStores) >= maxAllowedSegStores {
		return nil, fmt.Errorf("getSegStore: max allowed segstores reached (%d)", maxAllowedSegStores)
	}

	// Now that we got the lock, we should check if someone else had already created
	// a segstore for this streamid, if yes then return that, else continue on
	ss, present := allSegStores[streamid]
	if present {
		return ss, nil
	}

	segstore := NewSegStore(orgId)
	segstore.initWipBlock()

	err := segstore.resetSegStore(streamid, table)
	if err != nil {
		return nil, err
	}

	allSegStores[streamid] = segstore
	instrumentation.SetTotalSegstoreCount(int64(len(allSegStores)))

	return segstore, nil
}

func getBlockBloomSize(bi *BloomIndex) uint32 {

	if len(bi.HistoricalCount) == 0 {
		bi.HistoricalCount = make([]uint32, 0)
		return BLOCK_BLOOM_SIZE
	}

	startIdx := len(bi.HistoricalCount) - BLOOM_SIZE_HISTORY
	if startIdx < 0 {
		startIdx = 0
	}

	runningSum := uint32(0)
	count := uint32(0)
	for _, val := range bi.HistoricalCount[startIdx:] {
		runningSum += val
		count += 1
	}
	if count <= 0 {
		return BLOCK_BLOOM_SIZE
	}

	nextBloomSize := runningSum / count
	if nextBloomSize <= 0 {
		return 1
	}
	return nextBloomSize
}

// TODO: delete this function
func GetRotatedVersion(segKey string) string {
	return segKey
}

func updateRangeIndex(key string, rangeIndexPtr map[string]*structs.Numbers, numType SS_IntUintFloatTypes, intVal int64,
	uintVal uint64, fltVal float64) {
	switch numType {
	case SS_INT8, SS_INT16, SS_INT32, SS_INT64:
		addIntToRangeIndex(key, intVal, rangeIndexPtr)
	case SS_UINT8, SS_UINT16, SS_UINT32, SS_UINT64:
		addUintToRangeIndex(key, uintVal, rangeIndexPtr)
	case SS_FLOAT64:
		addFloatToRangeIndex(key, fltVal, rangeIndexPtr)
	}
}

func addUintToRangeIndex(key string, incomingVal uint64, rangeIndexPtr map[string]*structs.Numbers) {
	existingRI, present := rangeIndexPtr[key]
	if present {
		inMemType := existingRI.NumType
		switch inMemType {
		case RNT_SIGNED_INT:
			newVal := int64(incomingVal)
			if newVal < existingRI.Min_int64 {
				existingRI.Min_int64 = newVal
			} else if newVal > existingRI.Max_int64 {
				existingRI.Max_int64 = newVal
			}
		case RNT_FLOAT64:
			newVal := float64(incomingVal)
			if newVal < existingRI.Min_float64 {
				existingRI.Min_float64 = newVal
			} else if newVal > existingRI.Max_float64 {
				existingRI.Max_float64 = newVal
			}
		default:
			if incomingVal < existingRI.Min_uint64 {
				existingRI.Min_uint64 = incomingVal
			} else if incomingVal > existingRI.Max_uint64 {
				existingRI.Max_uint64 = incomingVal
			}
		}
		rangeIndexPtr[key] = existingRI
	} else {
		rangeIndexPtr[key] = &structs.Numbers{Min_uint64: incomingVal, Max_uint64: incomingVal, NumType: RNT_UNSIGNED_INT}
	}
}
func addIntToRangeIndex(key string, incomingVal int64, rangeIndexPtr map[string]*structs.Numbers) {
	existingRI, present := rangeIndexPtr[key]
	if present {
		inMemType := existingRI.NumType
		switch inMemType {
		case RNT_UNSIGNED_INT:
			existingRI = &structs.Numbers{Min_int64: int64(rangeIndexPtr[key].Min_uint64), Max_int64: int64(rangeIndexPtr[key].Max_uint64),
				NumType: RNT_SIGNED_INT}
			if incomingVal < existingRI.Min_int64 {
				existingRI.Min_int64 = incomingVal
			} else if incomingVal > existingRI.Max_int64 {
				existingRI.Max_int64 = incomingVal
			}
		case RNT_FLOAT64:
			newVal := float64(incomingVal)
			if newVal < existingRI.Min_float64 {
				existingRI.Min_float64 = newVal
			} else if newVal > existingRI.Max_float64 {
				existingRI.Max_float64 = newVal
			}
		default:
			if incomingVal < existingRI.Min_int64 {
				existingRI.Min_int64 = incomingVal
			} else if incomingVal > existingRI.Max_int64 {
				existingRI.Max_int64 = incomingVal
			}
		}
		rangeIndexPtr[key] = existingRI
		//fmt.Printf("present %v\n", (*rangeIndexPtr))
	} else {
		rangeIndexPtr[key] = &structs.Numbers{Min_int64: incomingVal, Max_int64: incomingVal, NumType: RNT_SIGNED_INT}
		//fmt.Printf("ADDED %v\n", (*rangeIndexPtr))
	}
}
func addFloatToRangeIndex(key string, incomingVal float64, rangeIndexPtr map[string]*structs.Numbers) {
	existingRI, present := rangeIndexPtr[key]
	if present {
		inMemType := existingRI.NumType
		switch inMemType {
		case RNT_UNSIGNED_INT:
			existingRI = &structs.Numbers{Min_float64: float64(rangeIndexPtr[key].Min_uint64), Max_float64: float64(rangeIndexPtr[key].Max_uint64), NumType: RNT_FLOAT64}
		case RNT_SIGNED_INT:
			existingRI = &structs.Numbers{Min_float64: float64(rangeIndexPtr[key].Min_int64), Max_float64: float64(rangeIndexPtr[key].Max_int64), NumType: RNT_FLOAT64}
		case RNT_FLOAT64:
			// Do nothing.
		}
		if incomingVal < existingRI.Min_float64 {
			existingRI.Min_float64 = incomingVal
		} else if incomingVal > existingRI.Max_float64 {
			existingRI.Max_float64 = incomingVal
		}
		rangeIndexPtr[key] = existingRI
	} else {
		rangeIndexPtr[key] = &structs.Numbers{Min_float64: incomingVal, Max_float64: incomingVal, NumType: RNT_FLOAT64}
	}
}

/*
   ******************* WIP BLOCK Encoding ********************

  [Actual Block]
  // lens are stored in the bsu file

*/
// returns number of written bytes, offset of block in file, and any errors

func writeWip(colWip *ColWip, encType []byte, compBuf []byte) (uint32, int64, error) {

	blkLen := uint32(0)
	// todo better error handling should not exit
	fd, err := os.OpenFile(colWip.csgFname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("WriteWip: open failed fname=%v, err=%v", colWip.csgFname, err)
		return 0, 0, err
	}
	defer fd.Close()

	blkOffset, err := fd.Seek(0, 2) // go to the end of the file
	if err != nil {
		log.Errorf("WriteWip: failed to get offset of current block offset %+v", err)
		return 0, 0, err
	}

	checksumFile := utils.ChecksumFile{Fd: fd}
	err = checksumFile.AppendPartialChunk(encType)
	if err != nil {
		log.Errorf("WriteWip: compression Type write failed fname=%v, err=%v", colWip.csgFname, err)
		return 0, blkOffset, err
	}
	blkLen += 1 // for compression type

	compressed, compLen, err := compressWip(colWip, encType, compBuf)
	if err != nil {
		log.Errorf("WriteWip: compression of wip failed fname=%v, err=%v", colWip.csgFname, err)
		return 0, blkOffset, err
	}
	err = checksumFile.AppendPartialChunk(compressed)
	if err != nil {
		log.Errorf("WriteWip: compressed write failed fname=%v, err=%v", colWip.csgFname, err)
		return 0, blkOffset, err
	}
	blkLen += compLen

	err = checksumFile.Flush()
	if err != nil {
		return 0, blkOffset, fmt.Errorf("WriteWip: flush failed fname=%v, err=%v", colWip.csgFname, err)
	}

	return blkLen, blkOffset, nil
}

func compressWip(colWip *ColWip, encType []byte, compBuf []byte) ([]byte, uint32, error) {
	var compressed []byte
	if bytes.Equal(encType, ZSTD_COMLUNAR_BLOCK) {

		// reduce the len to 0, but keep the cap of the underlying buffer
		compressed = encoder.EncodeAll(colWip.cbuf.Slice(0, int(colWip.cbufidx)),
			compBuf[:0])
	} else if bytes.Equal(encType, TIMESTAMP_TOPDIFF_VARENC) {
		compressed = colWip.cbuf.Slice(0, int(colWip.cbufidx))
	} else if bytes.Equal(encType, ZSTD_DICTIONARY_BLOCK) {
		PackDictEnc(colWip)
		compressed = colWip.cbuf.Slice(0, int(colWip.cbufidx))
	} else {
		log.Errorf("compressWip got an unknown encoding type: %+v", encType)
		return nil, 0, fmt.Errorf("got an unknown encoding type: %+v", encType)
	}
	compLen := uint32(len(compressed))

	return compressed, compLen, nil
}

func WriteRunningSegMeta(rsm *structs.SegMeta) {

	segFullMeta := &structs.SegFullMeta{
		SegMeta:     rsm,
		ColumnNames: rsm.ColumnNames,
		AllPQIDs:    rsm.AllPQIDs,
		UploadedSeg: false,
	}

	WriteSfm(segFullMeta)
}

func GetUnrotatedVTableCounts(vtable string, orgid int64) (uint64, int, uint64, map[string]struct{}) {
	bytesCount := uint64(0)
	onDiskBytesCount := uint64(0)
	recCount := 0
	allColumnsMap := make(map[string]struct{})
	allSegStoresLock.RLock()
	defer allSegStoresLock.RUnlock()
	for _, segstore := range allSegStores {
		if segstore.VirtualTableName == vtable && segstore.OrgId == orgid {
			bytesCount += segstore.BytesReceivedCount
			recCount += segstore.RecordCount
			onDiskBytesCount += segstore.OnDiskBytes
			utils.AddMapKeysToSet(allColumnsMap, segstore.AllSeenColumnSizes)
		}
	}
	return bytesCount, recCount, onDiskBytesCount, allColumnsMap
}

func GetUnrotatedVTableTimestamps(orgid int64) map[string]struct{ Earliest, Latest uint64 } {
	result := make(map[string]struct{ Earliest, Latest uint64 })

	allSegStoresLock.RLock()
	defer allSegStoresLock.RUnlock()

	for _, segstore := range allSegStores {
		if segstore.OrgId == orgid {
			indexName := segstore.VirtualTableName
			timestamps, exists := result[indexName]
			if !exists {
				timestamps = struct{ Earliest, Latest uint64 }{math.MaxUint64, 0}
			}

			if segstore.earliest_millis < timestamps.Earliest {
				timestamps.Earliest = segstore.earliest_millis
			}
			if segstore.latest_millis > timestamps.Latest {
				timestamps.Latest = segstore.latest_millis
			}

			result[indexName] = timestamps
		}
	}

	// If no data was found, return 0 for both
	for indexName, timestamps := range result {
		if timestamps.Earliest == math.MaxUint64 {
			timestamps.Earliest = 0
			result[indexName] = timestamps
		}
	}

	return result
}

func GetUnrotatedVTableCountsForAll(orgid int64, allvtables map[string]*structs.VtableCounts) {

	var ok bool
	var cnts *structs.VtableCounts

	allSegStoresLock.RLock()
	defer allSegStoresLock.RUnlock()
	for _, segstore := range allSegStores {
		if segstore.OrgId != orgid {
			continue
		}

		cnts, ok = allvtables[segstore.VirtualTableName]
		if !ok {
			cnts = &structs.VtableCounts{}
			allvtables[segstore.VirtualTableName] = cnts
		}

		cnts.BytesCount += segstore.BytesReceivedCount
		cnts.RecordCount += uint64(segstore.RecordCount)
		cnts.OnDiskBytesCount += segstore.OnDiskBytes
	}
}

func getActiveBaseDirVTable(virtualTableName string) string {
	var sb strings.Builder
	sb.WriteString(config.GetRunningConfig().DataPath)
	sb.WriteString(config.GetHostID())
	sb.WriteString("/final/")
	sb.WriteString(virtualTableName + "/")
	basedir := sb.String()
	return basedir
}

func DeleteVirtualTableSegStore(virtualTableName string) {
	allSegStoresLock.Lock()
	for streamid, segstore := range allSegStores {
		if segstore.VirtualTableName == virtualTableName {
			delete(allSegStores, streamid)
		}
	}
	activedir := getActiveBaseDirVTable(virtualTableName)
	os.RemoveAll(activedir)
	allSegStoresLock.Unlock()
}

func DeleteSegmentsForIndex(indexName string) {
	removeSegmentsByIndexOrSegkeys(nil, indexName)
}

func RemoveSegMetas(segmentsToDelete map[string]*structs.SegMeta) map[string]struct{} {

	segKeysToDelete := make(map[string]struct{})
	for segkey := range segmentsToDelete {
		segKeysToDelete[segkey] = struct{}{}
	}

	return removeSegmetas(segKeysToDelete, "")
}

func RemoveSegBasedirs(segbaseDirs map[string]struct{}) {
	for segdir := range segbaseDirs {
		if err := os.RemoveAll(segdir); err != nil {
			log.Errorf("RemoveSegBasedirs: Failed to remove directory name=%v, err:%v",
				segdir, err)
		}
		fileutils.RecursivelyDeleteEmptyParentDirectories(segdir)
	}
}

func removeSegmentsByIndexOrSegkeys(segmentsToDelete map[string]struct{}, indexName string) {
	segbaseDirs := removeSegmetas(segmentsToDelete, indexName)
	for segdir := range segbaseDirs {
		if err := os.RemoveAll(segdir); err != nil {
			log.Errorf("RemoveSegments: Failed to remove directory name=%v, err:%v",
				segdir, err)
		}
		fileutils.RecursivelyDeleteEmptyParentDirectories(segdir)
	}
}

func DeleteOldKibanaDoc(indexNameConverted string, idVal string) {
	indexNameDirPath := KibanaInternalBaseDir + indexNameConverted
	hashDocId := utils.HashString(idVal)
	docFilePath := indexNameDirPath + "/" + hashDocId

	err := os.RemoveAll(docFilePath)
	if err != nil {
		log.Errorf("DeleteOldKibanaDoc: Failed to delete, indexNameDirPath %+v idVal %+v folderpath %+v, err %+v",
			indexNameDirPath, idVal, docFilePath, err)
	}
}

func (cw *ColWip) GetBufAndIdx() ([]byte, uint32) {
	return cw.cbuf.Slice(0, int(cw.cbufidx)), cw.cbufidx
}

func (cw *ColWip) SetDeDataForTest(deCount uint16, deMap map[string][]uint16) {

	deData := DeData{deMap: deMap,
		deCount: deCount,
	}
	cw.deData = &deData
}

func (cw *ColWip) WriteSingleString(value string) {
	cw.WriteSingleStringBytes([]byte(value))
}

func (cw *ColWip) WriteSingleStringBytes(value []byte) {
	cw.cbuf.Append(VALTYPE_ENC_SMALL_STRING[:])
	cw.cbufidx += 1
	n := uint16(len(value))
	cw.cbuf.AppendUint16LittleEndian(n)
	cw.cbufidx += 2
	cw.cbuf.Append(value)
	cw.cbufidx += uint32(n)
}

func GetPQMRDirFromSegKey(segKey string) string {
	return filepath.Join(segKey, "pqmr")
}

func (ss *SegStore) writeToBloom(encType []byte, buf []byte, cname string,
	cw *ColWip) error {

	// no bloom for timestamp column
	if encType[0] == TIMESTAMP_TOPDIFF_VARENC[0] {
		return nil
	}

	bi, ok := ss.wipBlock.columnBlooms[cname]
	if !ok {
		// for non-strings columns, BI is not iniliazed. Maybe there should be an
		// explicit way of saying what columnType is this so that we don't "overload" the BI var
		return nil
	}

	switch encType[0] {
	case ZSTD_COMLUNAR_BLOCK[0]:
		return cw.writeNonDeBloom(buf, bi, ss.wipBlock.blockSummary.RecCount, cname)
	case ZSTD_DICTIONARY_BLOCK[0]:
		return cw.writeDeBloom(buf, bi)
	default:
		log.Errorf("writeToBloom got an unknown encoding type: %+v", encType)
		return fmt.Errorf("got an unknown encoding type: %+v", encType)
	}
}

func (cw *ColWip) writeDeBloom(buf []byte, bi *BloomIndex) error {
	// todo a better way to size the bloom might be to count the num of space and
	// then add to the cw.deData.deCount, that should be the optimal size
	// we add twice to avoid undersizing for above reason.
	bi.Bf = bloom.NewWithEstimates(uint(cw.deData.deCount)*2, BLOOM_COLL_PROBABILITY)
	for dwordkey := range cw.deData.deMap {
		dword := []byte(dwordkey)
		switch dword[0] {
		case VALTYPE_ENC_BACKFILL[0]:
			// we don't add backfill value to bloom since we are not going to search for it
			continue
		case VALTYPE_ENC_SMALL_STRING[0]:
			// the first 3 bytes are the type and length
			numAdded, err := addToBlockBloomBothCasesWithBuf(bi.Bf, dword[3:], buf)
			if err != nil {
				return err
			}
			bi.uniqueWordCount += numAdded
		case VALTYPE_ENC_BOOL[0]:
			// todo we should not be using bloom here, its expensive
			numAdded, err := addToBlockBloomBothCasesWithBuf(bi.Bf, []byte{dword[1]}, buf)
			if err != nil {
				return err
			}
			bi.uniqueWordCount += numAdded
		default:
			// just log and continue since we are only doing strings into blooms
			log.Errorf("writeDeBloom: unhandled recType: %v", dword[0])
		}
	}
	return nil
}

func (cw *ColWip) writeNonDeBloom(buf []byte, bi *BloomIndex, numRecs uint16,
	cname string) error {

	bloomEstimate := uint(numRecs) * 2
	bi.Bf = bloom.NewWithEstimates(bloomEstimate, BLOOM_COLL_PROBABILITY)
	bi.uniqueWordCount = 0
	err := cw.writeToBloom(buf, bi, numRecs, cname)
	if err != nil {
		log.Errorf("writeNonDeBloom: error computing bloom size needed for col: %v, err: %v",
			cname, err)
		return err
	}

	bloomSize := bi.uniqueWordCount
	bi.Bf = bloom.NewWithEstimates(uint(bloomSize), BLOOM_COLL_PROBABILITY)
	bi.uniqueWordCount = 0

	err = cw.writeToBloom(buf, bi, numRecs, cname)
	if err != nil {
		log.Errorf("writeNonDeBloom: error writing to bloom for col: %v, err: %v",
			cname, err)
		return err
	}

	return nil
}

func (cw *ColWip) writeToBloom(buf []byte, bi *BloomIndex, numRecs uint16,
	cname string) error {

	if bi == nil {
		return utils.TeeErrorf("writeToBloom: bloom index is nil for cname: %v", cname)
	}

	idx := uint32(0)
	for recNum := uint16(0); recNum < numRecs; recNum++ {
		cValBytes, endIdx, err := getColByteSlice(cw.cbuf, int(idx), 0) // todo pass qid here
		if err != nil {
			log.Errorf("writeToBloom: Could not extract val for cname: %v, idx: %v",
				cname, idx)
			return err
		}

		// we are going to insert only strings in the bloom
		if cValBytes[0] == VALTYPE_ENC_SMALL_STRING[0] {
			word := cValBytes[3:endIdx]
			numAdded, err := addToBlockBloomBothCasesWithBuf(bi.Bf, word, buf)
			if err != nil {
				return err
			}
			bi.uniqueWordCount += numAdded
		}
		idx += uint32(endIdx)
	}
	return nil
}

/*
Adds the fullWord and sub-words (lowercase as well) to the bloom
Subwords are gotten by splitting the fullWord by whitespace
NOTE: This function may modify the incoming byte slice
*/
func addToBlockBloomBothCases(blockBloom *bloom.BloomFilter, fullWord []byte) uint32 {

	blockWordCount, err := addToBlockBloomBothCasesWithBuf(blockBloom, fullWord, fullWord)
	if err != nil {
		log.Errorf("addToBlockBloomBothCases: err adding bloom: err: %v", err)
	}

	return blockWordCount
}

/*
Adds the fullWord and sub-words (lowercase as well) to the bloom
Subwords are gotten by splitting the fullWord by whitespace
*/
func addToBlockBloomBothCasesWithBuf(blockBloom *bloom.BloomFilter, fullWord []byte,
	workBuf []byte) (uint32, error) {

	var blockWordCount uint32 = 0
	copy := fullWord[:]

	// we will add the lowercase to bloom only if there was an upperCase and we
	// had to convert
	hasUpper := utils.HasUpper(copy)

	// add the original full
	if !blockBloom.TestAndAdd(copy) {
		blockWordCount++
	}

	var hasSubWords bool
	for {
		i := bytes.Index(copy, BYTE_SPACE)
		if i == -1 {
			break
		}
		hasSubWords = true
		// add original sub word
		if !blockBloom.TestAndAdd(copy[:i]) {
			blockWordCount++
		}

		// add sub word lowercase
		if hasUpper {
			word, err := utils.BytesToLower(copy[:i], workBuf)
			if err != nil {
				return 0, err
			}
			if !blockBloom.TestAndAdd(word) {
				blockWordCount++
			}
		}
		copy = copy[i+BYTE_SPACE_LEN:]
	}

	// handle last word. If no word was found, then we have already added the full word
	if hasSubWords && len(copy) > 0 {
		if !blockBloom.TestAndAdd(copy) {
			blockWordCount++
		}
		word, err := utils.BytesToLower(copy, workBuf)
		if err != nil {
			return 0, err
		}
		if !blockBloom.TestAndAdd(word) {
			blockWordCount++
		}
	}

	if hasUpper {
		word, err := utils.BytesToLower(fullWord[:], workBuf)
		if err != nil {
			return 0, err
		}
		if !blockBloom.TestAndAdd(word) {
			blockWordCount++
		}
	}
	return blockWordCount, nil
}
