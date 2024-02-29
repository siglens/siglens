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

package writer

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	"github.com/klauspost/compress/zstd"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/common/fileutils"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	"github.com/siglens/siglens/pkg/segment/writer/suffix"
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
var maxSegFileSize uint64

var KibanaInternalBaseDir string

var smrLock sync.Mutex = sync.Mutex{}
var localSegmetaFname string

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
}

type ColWip struct {
	cbufidx   uint32              // end index of buffer, only cbuf[:cbufidx] exists
	cstartidx uint32              // start index of last record, so cbuf[cstartidx:cbufidx] is the encoded last record
	cbuf      [WIP_SIZE]byte      // in progress bytes
	csgFname  string              // file name of csg file
	deMap     map[string][]uint16 // dictWordKey ==> recordNums that match this key
	deCount   uint16              // keeps track of cardinality count for this COL_WIP
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
	pqMatches          map[string]*pqmr.PQMatchResults
	maxIdx             uint32
	blockTs            []uint64
	tomRollup          map[uint64]*RolledRecs // top-of-minute rollup
	tohRollup          map[uint64]*RolledRecs // top-of-hour rollup
	todRollup          map[uint64]*RolledRecs // top-of-day rollup
	bb                 *bbp.ByteBuffer        // byte buffer pool for HLL byte inserts
}

// returns in memory size of a single wip block
func (wp *WipBlock) getSize() uint64 {
	size := uint64(0)
	for _, v := range wp.columnBlooms {
		size += uint64(v.Bf.Cap() / 8)
	}
	size += wp.blockSummary.GetSize()
	size += uint64(24 * len(wp.columnRangeIndexes))
	size += uint64(WIP_SIZE * len(wp.colWips))
	for _, v := range wp.pqMatches {
		size += v.GetInMemSize()
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
	for _, s := range allSegStores {
		totalSize += s.wipBlock.getSize()
	}

	totalSize += metrics.GetTotalEncodedSize()

	return uint64(math.Ceil(ConvertFloatBytesToMB(float64(totalSize) * float64(1.10))))
}

func InitWriterNode() {
	// one time initialization
	AllUnrotatedSegmentInfo = make(map[string]*UnrotatedSegmentInfo)
	RecentlyRotatedSegmentFiles = make(map[string]*SegfileRotateInfo)
	metrics.InitMetricsSegStore()

	initSmr()

	go timeBasedWIPFlushToFile()
	go timeBasedRotateSegment()
	go cleanRecentlyRotatedInfo()
	go timeBasedUploadIngestNodeDir()
	HostnameDir()
	InitKibanaInternalData()
}

func initSmr() {

	localSegmetaFname = GetLocalSegmetaFName()

	fd, err := os.OpenFile(localSegmetaFname, os.O_RDONLY, 0666)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// for first time during bootup this will occur
			_, err := os.OpenFile(localSegmetaFname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Errorf("initSmr: failed to open a new filename=%v: err=%v", localSegmetaFname, err)
				return
			}
		}
		return
	}
	fd.Close()
}

// TODO: this should be pushed based & we should have checks in uploadingestnode function to prevent uploading unupdated files.
func timeBasedUploadIngestNodeDir() {
	for {
		time.Sleep(UPLOAD_INGESTNODE_DIR)
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

// This is the only function that needs to be exported from this package, since this is the only
// place where we play with the locks

func AddEntryToInMemBuf(streamid string, rawJson []byte, ts_millis uint64,
	indexName string, bytesReceived uint64, flush bool, signalType SIGNAL_TYPE, orgid uint64) error {

	segstore, err := getSegStore(streamid, ts_millis, indexName, orgid)
	if err != nil {
		log.Errorf("AddEntryToInMemBuf, getSegstore err=%v", err)
		return err
	}

	segstore.lock.Lock()
	defer segstore.lock.Unlock()
	if segstore.wipBlock.maxIdx+MAX_RECORD_SIZE >= WIP_SIZE ||
		segstore.wipBlock.blockSummary.RecCount >= MAX_RECS_PER_WIP {
		err = segstore.AppendWipToSegfile(streamid, false, false, false)
		if err != nil {
			log.Errorf("AddEntryToInMemBuf: failed to append segkey=%v, err=%v", segstore.SegmentKey, err)
			return err
		}
		instrumentation.IncrementInt64Counter(instrumentation.WIP_BUFFER_FLUSH_COUNT, 1)
	}

	segstore.adjustEarliestLatestTimes(ts_millis)
	segstore.wipBlock.adjustEarliestLatestTimes(ts_millis)
	err = segstore.WritePackedRecord(rawJson, ts_millis, signalType)
	if err != nil {
		return err
	}
	segstore.BytesReceivedCount += bytesReceived

	if flush {
		err = segstore.AppendWipToSegfile(streamid, false, false, false)
		if err != nil {
			log.Errorf("AddEntryToInMemBuf: failed to append during flush segkey=%v, err=%v", segstore.SegmentKey, err)
			return err
		}
	}
	return nil
}

func AddTimeSeriesEntryToInMemBuf(rawJson []byte, signalType SIGNAL_TYPE, orgid uint64) error {
	switch signalType {
	case SIGNAL_METRICS_OTSDB:
		tagsHolder := metrics.GetTagsHolder()
		mName, dp, ts, err := metrics.ExtractOTSDBPayload(rawJson, tagsHolder)
		if err != nil {
			metrics.ReturnTagsHolder(tagsHolder)
			return err
		}
		err = metrics.EncodeDatapoint(mName, tagsHolder, dp, ts, uint64(len(rawJson)), orgid)
		if err != nil {
			metrics.ReturnTagsHolder(tagsHolder)
			return err
		}
		metrics.ReturnTagsHolder(tagsHolder)
	case SIGNAL_METRICS_INFLUX:
		tagsHolder := metrics.GetTagsHolder()
		mName, dp, ts, err := metrics.ExtractInfluxPayload(rawJson, tagsHolder)
		if err != nil {
			metrics.ReturnTagsHolder(tagsHolder)
			return err
		}
		err = metrics.EncodeDatapoint(mName, tagsHolder, dp, ts, uint64(len(rawJson)), orgid)
		if err != nil {
			metrics.ReturnTagsHolder(tagsHolder)
			return err
		}
		metrics.ReturnTagsHolder(tagsHolder)
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
		segstore.lock.Lock()
		err := segstore.AppendWipToSegfile(streamid, true, false, false)
		if err != nil {
			log.Errorf("ForcedFlushToSegfile: failed to append err=%v", err)
		}
		log.Warnf("Flushing segment file for streamid %s server exit", streamid)
		segstore.lock.Unlock()
		delete(allSegStores, streamid)
	}
	allSegStoresLock.Unlock()
}

func updateValuesFromConfig() {
	maxSegFileSize = *config.GetMaxSegFileSize()
}

func timeBasedWIPFlushToFile() {
	for {
		sleepDuration := time.Duration(config.GetSegFlushIntervalSecs()) * time.Second
		time.Sleep(sleepDuration)
		FlushWipBufferToFile(&sleepDuration)
	}
}

func rotateSegmentOnTime() {
	segRotateDuration := time.Duration(SEGMENT_ROTATE_DURATION_SECONDS) * time.Second
	allSegStoresLock.RLock()
	wg := sync.WaitGroup{}
	for sid, ss := range allSegStores {

		if ss.firstTime {
			rnm := rand.Intn(SEGMENT_ROTATE_DURATION_SECONDS) + 60
			segRotateDuration = time.Duration(rnm) * time.Second
		} else {
			segRotateDuration = time.Duration(SEGMENT_ROTATE_DURATION_SECONDS) * time.Second
		}

		if time.Since(ss.timeCreated) < segRotateDuration {
			continue
		}
		wg.Add(1)
		go func(streamid string, segstore *SegStore) {
			defer wg.Done()
			segstore.lock.Lock()
			segstore.firstTime = false
			err := segstore.AppendWipToSegfile(streamid, false, false, true)
			if err != nil {
				log.Errorf("rotateSegmentOnTime: failed to append,  streamid=%s err=%v", err, streamid)
			} else {
				if time.Since(segstore.lastUpdated) > segRotateDuration*2 && segstore.RecordCount == 0 {
					log.Infof("Deleting the segstore for streamid=%s", streamid)
					delete(allSegStores, streamid)
				} else {
					log.Infof("Rotating segment due to time. streamid=%s and table=%s", streamid, segstore.VirtualTableName)
				}
			}
			segstore.lock.Unlock()
		}(sid, ss)
	}
	wg.Wait()
	allSegStoresLock.RUnlock()
}

func ForceRotateSegmentsForTest() {
	allSegStoresLock.Lock()
	for streamid, segstore := range allSegStores {
		segstore.lock.Lock()
		err := segstore.AppendWipToSegfile(streamid, false, false, true)
		if err != nil {
			log.Errorf("ForceRotateSegmentsForTest: failed to append,  streamid=%s err=%v", err, streamid)
		} else {
			log.Infof("Rotating segment due to time. streamid=%s and table=%s", streamid, segstore.VirtualTableName)
		}
		segstore.lock.Unlock()
	}
	allSegStoresLock.Unlock()
}

func timeBasedRotateSegment() {
	for {
		time.Sleep(SEGMENT_ROTATE_SLEEP_DURATION_SECONDS * time.Second)
		rotateSegmentOnTime()
	}

}

func FlushWipBufferToFile(sleepDuration *time.Duration) {
	allSegStoresLock.RLock()
	for streamid, segstore := range allSegStores {
		segstore.lock.Lock()
		if segstore.wipBlock.maxIdx > 0 && time.Since(segstore.lastUpdated) > *sleepDuration {
			err := segstore.AppendWipToSegfile(streamid, false, false, false)
			if err != nil {
				log.Errorf("FlushWipBufferToFile: failed to append, err=%v", err)
			}
			log.Infof("Flushed WIP buffer due to time. streamid=%s and table=%s", streamid, segstore.VirtualTableName)
		}
		segstore.lock.Unlock()
	}
	allSegStoresLock.RUnlock()
}

func InitColWip(segKey string, colName string) *ColWip {

	return &ColWip{
		csgFname: fmt.Sprintf("%v_%v.csg", segKey, xxhash.Sum64String(colName)),
		deMap:    make(map[string][]uint16),
		deCount:  0,
	}
}

// In-mem Buf Format
// [varint Record-0 varint Record-1 ....]
// varint stores length of Record , it would occupy 1-9 bytes
// The first bit of each byte of varint specifies whether there are follow on bytes
// rest 7 bits are used to store the number
func getSegStore(streamid string, ts_millis uint64, table string, orgId uint64) (*SegStore, error) {

	allSegStoresLock.Lock()
	defer allSegStoresLock.Unlock()

	var segstore *SegStore
	segstore, present := allSegStores[streamid]
	if !present {
		if len(allSegStores) >= maxAllowedSegStores {
			return nil, fmt.Errorf("getSegStore: max allowed segstores reached (%d)", maxAllowedSegStores)
		}

		suffIndex, err := suffix.GetSuffix(streamid, table)
		if err != nil {
			return nil, err
		}
		segstore = &SegStore{suffix: suffIndex, lock: sync.Mutex{}, OrgId: orgId, firstTime: true}
		segstore.initWipBlock()
		err = segstore.resetSegStore(streamid, table)
		if err != nil {
			return nil, err
		}
		allSegStores[streamid] = segstore
		instrumentation.SetWriterSegstoreCountGauge(int64(len(allSegStores)))
	}

	updateValuesFromConfig()
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

func getActiveBaseSegDir(streamid string, virtualTableName string, suffix uint64) string {
	var sb strings.Builder
	sb.WriteString(config.GetDataPath())
	sb.WriteString(config.GetHostID())
	sb.WriteString("/active/")
	sb.WriteString(virtualTableName + "/")
	sb.WriteString(streamid + "/")
	sb.WriteString(strconv.FormatUint(suffix, 10) + "/")
	basedir := sb.String()
	return basedir
}

func getFinalBaseSegDir(streamid string, virtualTableName string, suffix uint64) string {
	var sb strings.Builder
	sb.WriteString(config.GetDataPath())
	sb.WriteString(config.GetHostID())
	sb.WriteString("/final/")
	sb.WriteString(virtualTableName + "/")
	sb.WriteString(streamid + "/")
	sb.WriteString(strconv.FormatUint(suffix, 10) + "/")
	basedir := sb.String()
	return basedir
}

/*
Adds the fullWord and sub-words to the bloom
Subwords are gotten by splitting the fullWord by whitespace
*/
func addToBlockBloom(blockBloom *bloom.BloomFilter, fullWord []byte) uint32 {

	var blockWordCount uint32 = 0
	copy := fullWord[:]

	if !blockBloom.TestAndAdd(copy) {
		blockWordCount += 1
	}

	var foundWord bool
	for {
		i := bytes.Index(copy, BYTE_SPACE)
		if i == -1 {
			break
		}
		foundWord = true
		if !blockBloom.TestAndAdd(copy[:i]) {
			blockWordCount += 1
		}
		copy = copy[i+BYTE_SPACE_LEN:]
	}

	// handle last word. If no word was found, then we have already added the full word
	if foundWord && len(copy) > 0 {
		if !blockBloom.TestAndAdd(copy) {
			blockWordCount += 1
		}
	}
	return blockWordCount
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

func writeWip(colWip *ColWip, encType []byte) (uint32, int64, error) {

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

	_, err = fd.Write(encType)
	if err != nil {
		log.Errorf("WriteWip: compression Type write failed fname=%v, err=%v", colWip.csgFname, err)
		return 0, blkOffset, err
	}
	blkLen += 1 // for compression type

	compressed, compLen, err := compressWip(colWip, encType)
	if err != nil {
		log.Errorf("WriteWip: compression of wip failed fname=%v, err=%v", colWip.csgFname, err)
		return 0, blkOffset, err
	}
	_, err = fd.Write(compressed)
	if err != nil {
		log.Errorf("WriteWip: compressed write failed fname=%v, err=%v", colWip.csgFname, err)
		return 0, blkOffset, err
	}
	blkLen += compLen

	return blkLen, blkOffset, nil
}

func compressWip(colWip *ColWip, encType []byte) ([]byte, uint32, error) {
	var compressed []byte
	if bytes.Equal(encType, ZSTD_COMLUNAR_BLOCK) {
		compressed = encoder.EncodeAll(colWip.cbuf[0:colWip.cbufidx], make([]byte, 0, colWip.cbufidx))
	} else if bytes.Equal(encType, TIMESTAMP_TOPDIFF_VARENC) {
		compressed = colWip.cbuf[0:colWip.cbufidx]
	} else if bytes.Equal(encType, ZSTD_DICTIONARY_BLOCK) {
		PackDictEnc(colWip)
		compressed = colWip.cbuf[0:colWip.cbufidx]
	} else {
		log.Errorf("compressWip got an unknown encoding type: %+v", encType)
		return nil, 0, fmt.Errorf("got an unknown encoding type: %+v", encType)
	}
	compLen := uint32(len(compressed))

	return compressed, compLen, nil
}

func writeRunningSegMeta(fname string, rsm *structs.SegMeta) error {

	fd, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("writeRunningSegMeta: open failed fname=%v, err=%v", fname, err)
		return err
	}
	defer fd.Close()

	rsmjson, err := json.Marshal(rsm)
	if err != nil {
		log.Errorf("writeRunningSegMeta: failed to Marshal: err=%v", err)
		return err
	}

	if _, err := fd.Write(rsmjson); err != nil {
		log.Errorf("writeRunningSegMeta: failed to write rsmjson filename=%v: err=%v", fname, err)
		return err
	}

	return nil
}

func GetUnrotatedVTableCounts(vtable string, orgid uint64) (uint64, int, uint64) {
	bytesCount := uint64(0)
	onDiskBytesCount := uint64(0)
	recCount := 0
	allSegStoresLock.RLock()
	defer allSegStoresLock.RUnlock()
	for _, segstore := range allSegStores {
		if segstore.VirtualTableName == vtable && segstore.OrgId == orgid {
			bytesCount += segstore.BytesReceivedCount
			recCount += segstore.RecordCount
			onDiskBytesCount += segstore.OnDiskBytes
		}
	}
	return bytesCount, recCount, onDiskBytesCount
}

func getActiveBaseDirVTable(virtualTableName string) string {
	var sb strings.Builder
	sb.WriteString(config.GetRunningConfig().DataPath)
	sb.WriteString(config.GetHostID())
	sb.WriteString("/active/")
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

func DeleteSegmentsForIndex(segmetaFName, indexName string) {
	smrLock.Lock()
	defer smrLock.Unlock()

	removeSegmentsByIndexOrList(segmetaFName, indexName, nil)
}

func RemoveSegments(segmetaFName string, segmentsToDelete map[string]*structs.SegMeta) {
	smrLock.Lock()
	defer smrLock.Unlock()

	removeSegmentsByIndexOrList(segmetaFName, "", segmentsToDelete)
}

func removeSegmentsByIndexOrList(segMetaFile string, indexName string, segmentsToDelete map[string]*structs.SegMeta) {

	if indexName == "" && segmentsToDelete == nil {
		return // nothing to remove
	}

	preservedSmEntries := make([]*structs.SegMeta, 0)

	entriesRead := 0
	entriesRemoved := 0

	fr, err := os.OpenFile(segMetaFile, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("removeSegmentsByIndexOrList: Failed to open SegMetaFile name=%v, err:%v", segMetaFile, err)
		return
	}
	defer fr.Close()

	reader := bufio.NewScanner(fr)
	for reader.Scan() {
		segMetaData := structs.SegMeta{}
		err = json.Unmarshal(reader.Bytes(), &segMetaData)
		if err != nil {
			log.Errorf("removeSegmentsByIndexOrList: Failed to unmarshal fileName=%v, err:%v", segMetaFile, err)
			continue
		}
		entriesRead++

		// only append the ones that we want to preserve
		// check if it was based on indexName
		if indexName != "" {
			if segMetaData.VirtualTableName != indexName {
				preservedSmEntries = append(preservedSmEntries, &segMetaData)
				continue
			}
		} else {
			// check if based on segmetas
			_, ok := segmentsToDelete[segMetaData.SegmentKey]
			if !ok {
				preservedSmEntries = append(preservedSmEntries, &segMetaData)
				continue
			}
		}

		entriesRemoved++
		if err := os.RemoveAll(segMetaData.SegbaseDir); err != nil {
			log.Errorf("removeSegmentsByIndexOrList: Failed to remove directory name=%v, err:%v",
				segMetaData.SegbaseDir, err)
		}
		fileutils.RecursivelyDeleteEmptyParentDirectories(segMetaData.SegbaseDir)
	}

	if entriesRemoved > 0 {

		// if we removed entries and there was nothing preserveed then we must delete this segmetafile
		if len(preservedSmEntries) == 0 {
			if err := os.RemoveAll(segMetaFile); err != nil {
				log.Errorf("removeSegmentsByIndexOrList: Failed to remove smfile name=%v, err:%v", segMetaFile, err)
			}
			return
		}

		wfd, err := os.OpenFile(segMetaFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Errorf("removeSegmentsByIndexOrList: Failed to open temp SegMetaFile name=%v, err:%v", segMetaFile, err)
			return
		}
		defer wfd.Close()

		for _, smentry := range preservedSmEntries {

			segmetajson, err := json.Marshal(*smentry)
			if err != nil {
				log.Errorf("removeSegmentsByIndexOrList: failed to Marshal: err=%v", err)
				return
			}

			if _, err := wfd.Write(segmetajson); err != nil {
				log.Errorf("removeSegmentsByIndexOrList: failed to write segmeta filename=%v: err=%v", segMetaFile, err)
				return
			}

			if _, err := wfd.WriteString("\n"); err != nil {
				log.Errorf("removeSegmentsByIndexOrList: failed to write newline filename=%v: err=%v", segMetaFile, err)
				return
			}
		}
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
	return cw.cbuf[0:cw.cbufidx], cw.cbufidx
}

func (cw *ColWip) SetDeCount(val uint16) {
	cw.deCount = val
}

func (cw *ColWip) SetDeMap(val map[string][]uint16) {
	cw.deMap = val
}

func (cw *ColWip) WriteSingleString(value string) {
	copy(cw.cbuf[cw.cbufidx:], VALTYPE_ENC_SMALL_STRING[:])
	cw.cbufidx += 1
	n := uint16(len(value))
	copy(cw.cbuf[cw.cbufidx:], utils.Uint16ToBytesLittleEndian(n))
	cw.cbufidx += 2
	copy(cw.cbuf[cw.cbufidx:], value)
	cw.cbufidx += uint32(n)
}

func AddNewRotatedSegment(segmeta structs.SegMeta) {

	smrLock.Lock()
	defer smrLock.Unlock()

	fileName := GetLocalSegmetaFName()

	segmetajson, err := json.Marshal(segmeta)
	if err != nil {
		log.Errorf("AddNewRotatedSegment: failed to Marshal: err=%v", err)
		return
	}

	fd, err := os.OpenFile(fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fd, err = os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Errorf("AddNewRotatedSegment: failed to open a new filename=%v: err=%v", fileName, err)
				return
			}

		} else {
			log.Errorf("AddNewRotatedSegment: failed to open filename=%v: err=%v", fileName, err)
			return
		}
	}

	defer fd.Close()

	if _, err := fd.Write(segmetajson); err != nil {
		log.Errorf("AddNewRotatedSegment: failed to write segmeta filename=%v: err=%v", fileName, err)
		return
	}

	if _, err := fd.WriteString("\n"); err != nil {
		log.Errorf("AddNewRotatedSegment: failed to write newline filename=%v: err=%v", fileName, err)
		return
	}
	err = fd.Sync()
	if err != nil {
		log.Errorf("AddNewRotatedSegment: failed to sync filename=%v: err=%v", fileName, err)
		return
	}
}

func BackFillPQSSegmetaEntry(segsetkey string, newpqid string) {
	smrLock.Lock()
	defer smrLock.Unlock()

	preservedSmEntries := make([]*structs.SegMeta, 0)
	allPqids := make(map[string]bool)

	// Read segmeta files
	allSegMetas, err := getAllSegmetaToMap(localSegmetaFname)
	if err != nil {
		log.Errorf("BackFillPQSSegmetaEntry: failed to get Segmeta: err=%v", err)
		return
	}
	for segkey, segMetaEntry := range allSegMetas {
		if segkey != segsetkey {
			preservedSmEntries = append(preservedSmEntries, segMetaEntry)
			continue
		} else {
			for pqid := range segMetaEntry.AllPQIDs {
				allPqids[pqid] = true
			}
			allPqids[newpqid] = true
			segMetaEntry.AllPQIDs = allPqids
			preservedSmEntries = append(preservedSmEntries, segMetaEntry)
			continue
		}
	}
	wfd, err := os.OpenFile(localSegmetaFname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("BackFillPQSSegmetaEntry: Failed to open SegMetaFile name=%v, err:%v", localSegmetaFname, err)
		return
	}
	defer wfd.Close()

	for _, smentry := range preservedSmEntries {

		segmetajson, err := json.Marshal(*smentry)
		if err != nil {
			log.Errorf("BackFillPQSSegmetaEntry: failed to Marshal: err=%v", err)
			return
		}

		if _, err := wfd.Write(segmetajson); err != nil {
			log.Errorf("BackFillPQSSegmetaEntry: failed to write segmeta filename=%v: err=%v", localSegmetaFname, err)
			return
		}

		if _, err := wfd.WriteString("\n"); err != nil {
			log.Errorf("BackFillPQSSegmetaEntry: failed to write newline filename=%v: err=%v", localSegmetaFname, err)
			return
		}
	}
}
