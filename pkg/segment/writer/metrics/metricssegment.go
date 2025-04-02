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

package metrics

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/siglens/siglens/pkg/segment/writer/metrics/wal"

	jp "github.com/buger/jsonparser"
	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/blob"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/memory"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/reader/microreader"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer/metrics/compress"
	"github.com/siglens/siglens/pkg/segment/writer/metrics/meta"
	"github.com/siglens/siglens/pkg/segment/writer/suffix"
	"github.com/siglens/siglens/pkg/usageStats"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var (
	otsdb_mname                    = []byte("metric")
	metric_name_key                = []byte("name")
	otsdb_timestamp                = []byte("timestamp")
	otsdb_value                    = []byte("value")
	metric_value_gauge_keyname     = []byte("gauge")
	metric_value_counter_keyname   = []byte("counter")
	metric_value_histogram_keyname = []byte("histogram")
	metric_value_summary_keyname   = []byte("summary")
	otsdb_tags                     = []byte("tags")
)

var tags_separator = []byte("__")

var TAGS_TREE_FLUSH_SLEEP_DURATION = 60 // 1 min

const METRICS_BLK_FLUSH_SLEEP_DURATION = 2 * 60 * 60 // 2 hours

const METRICS_BLK_ROTATE_SLEEP_DURATION = 10 // 10 seconds

const METRICS_INSTRUMENTATION_FLUSH_DURATION = 60 // 60 seconds
const WAL_DPS_FLUSH_SLEEP_DURATION = 1            // 1 sec
const METRICS_NAME_WAL_FLUSH_SLEEP_DURATION = 1   // 1 sec
const METRICS_NAME_WAL_DIR = "mname"

var dateTimeLayouts = []string{
	time.RFC3339,
	time.RFC3339Nano,
	time.RFC1123,
	time.RFC1123Z,
	time.RFC822,
	time.RFC822Z,
	time.RFC850,
}

// The following variables should only be updated in the GetTotalEncodedSize() function
// These track the Metrics Size info and will be used while printing the Global MemorySummary
var (
	totalTagTreesCount                  int
	totalLeafNodesCount                 int
	totalTagsTreeSizeInBytes            uint64
	totalSeriesCount                    int
	totalSortedTSIDCount                int
	totalTSIDLookupCount                int
	totalAllMSegmentsEncodedSizeInBytes uint64 // Size of all blocks in all metrics segments including the blocks that are rotated
	totalMSegBlocksEncodedSizeInBytes   uint64 // Size of blocks in all metrics segments that are in memory. Unrotated blocks
)

type MetricsEncodedSizeInfo struct {
	TotalTagTreesCount                  int
	TotalLeafNodesCount                 int
	TotalTagsTreeSizeInBytes            uint64
	TotalSeriesCount                    int
	TotalSortedTSIDCount                int
	TotalTSIDLookupCount                int
	TotalAllMSegmentsEncodedSizeInBytes uint64
	TotalMSegBlocksEncodedSizeInBytes   uint64
}

/*
A metrics segment represents a 2hr window and consists of many metrics blocks and tagTrees.

Only a single metrics buffer per metrics segment can be in memory at a time. Prior metrics buffers will be flushed to disk.

The tagsTree will be shared across metrics this metrics segment.

A metrics segment generate the following set of files:
  - A tagTree file for each incoming tagKey seen across this segment
  - A metricsBlock file for each incoming 15minute window
  - A map for all metric names in the metrics segment

TODO: this metrics segment should reject samples not in 2hr window
*/
type MetricsSegment struct {
	metricsKeyBase  string          // base string of this metric segment's key
	Suffix          uint64          // current suffix
	Mid             string          // metrics id for this metric segment
	highTS          uint32          // highest epoch timestamp seen across this segment
	lowTS           uint32          // lowest epoch timestamp seen across this segment
	mBlock          *MetricsBlock   // current in memory block
	currBlockNum    uint16          // current block number
	mNamesMap       map[string]bool // all metric names seen across segment
	mSegEncodedSize uint64          // total size of all metric blocks. TODO: this should include tagsTree & mNames
	bytesReceived   uint64          // total size of incoming data
	rwLock          *sync.RWMutex   // read write lock for access
	datapointCount  uint64          // total number of datapoints across all series in the block
	Orgid           int64
	mNameWalState   mNameWalState
}

type mNameWalState struct {
	metricsName []string           // metric names seen across segment
	wal         *wal.MetricNameWal // Active WAL file
}

/*
A metrics buffer represent a 15 minute (or 1GB size) window of encoded series

# A metrics buffer's suffix determines the path of the generated files in relation to the metricssegment

Every 5s, this metrics buffer should persist to disk and will create / update two file:
 1. Raw TS encoded file. Format [tsid][packed-len][raw-values]
 2. TSID offset file. Format [tsid][soff]
*/
type MetricsBlock struct {
	tsidLookup     map[uint64]int
	allSeries      []*TimeSeries
	sortedTsids    []uint64
	mBlockSummary  *structs.MBlockSummary
	blkEncodedSize uint64 // total encoded size of the block
	dpWalState     dpWalState
}

type dpWalState struct {
	segID           uint64
	mId             string
	currentWal      *wal.DataPointWal   // Active WAL file
	allWALs         []*wal.DataPointWal // List of WAL files
	dpsInWalMem     []wal.WalDatapoint
	dpIdx           uint64 // Next write position in dpsInWalMem
	currentWALIndex uint64
	lock            sync.Mutex
}

// Represents a single timeseries
type TimeSeries struct {
	// TODO: what is stored here, how is it flushed?
	lock        *sync.Mutex
	rawEncoding *bytes.Buffer

	nEntries    int          // number of ts/dp combinations in this series
	lastKnownTS uint32       // last known timestamp
	cFinishFn   func() error // function to call at end of compression, to write the final bytes for the encoded timestamps
	compressor  *compress.Compressor
}

var orgMetricsAndTagsLock *sync.RWMutex = &sync.RWMutex{}

type MetricsAndTagsHolder struct {
	MetricSegments map[string]*MetricsSegment
	TagHolders     map[string]*TagsTreeHolder
}

var numMetricsSegments uint64

var OrgMetricsAndTags map[int64]*MetricsAndTagsHolder = make(map[int64]*MetricsAndTagsHolder)

func InitTestingConfig() {
	TAGS_TREE_FLUSH_SLEEP_DURATION = 10
}

// TODO: pre-allocates as many metricsbuffers that can fix and sets hash range
// To evenly distribute metric names, hash range can simply metricsId mod numMetricsBuffers
func InitMetricsSegStore() {
	err := meta.InitMetricsMeta()
	if err != nil {
		log.Errorf("InitMetricsSegStore: failed to initialize metrics meta: %v", err)
	}
	go timeBasedMetricsFlush()
	go timeBasedRotate()
	go timeBasedTagsTreeFlush()
	go timeBasedInstruFlush()
	go timeBasedWalDPSFlush()
	go timeBasedMNameWalFlush()
}

func initOrgMetrics(orgid int64) error {
	orgMetricsAndTagsLock.Lock()
	if _, ok := OrgMetricsAndTags[orgid]; !ok {
		OrgMetricsAndTags[orgid] = &MetricsAndTagsHolder{
			MetricSegments: map[string]*MetricsSegment{},
			TagHolders:     map[string]*TagsTreeHolder{},
		}
	}
	orgMetricsAndTagsLock.Unlock()
	log.Infof("initOrgMetrics: Initialising metrics segments and tags trees for orgid %v", orgid)

	availableMem := memory.GetAvailableMetricsIngestMemory()
	numMetricsSegments = getNumberOfSegmentsFromMemory(availableMem)
	if numMetricsSegments == 0 {
		log.Errorf("initOrgMetrics: Available memory (%d) is not enough to initialize a single metrics segment", availableMem)
		return errors.New("not enough memory to initialize metrics segments")
	}

	for i := uint64(0); i < numMetricsSegments; i++ {
		mSeg, err := InitMetricsSegment(orgid, fmt.Sprintf("%d", i))
		if err != nil {
			log.Errorf("initOrgMetrics: Initialising metrics segment failed for org: %v, err: %v", orgid, err)
			return err
		}
		err = mSeg.mBlock.initNewDpWal()
		err = mSeg.initNewMNameWAL()
		if err != nil {
			log.Errorf("initOrgMetrics : Failed to initialize new WAL in mSeg.mBlock: %v", err)
			return err
		}

		orgMetricsAndTagsLock.Lock()
		OrgMetricsAndTags[orgid].MetricSegments[fmt.Sprint(i)] = mSeg
		OrgMetricsAndTags[orgid].TagHolders[mSeg.Mid], err = InitTagsTreeHolder(mSeg.Mid)
		if err != nil {
			log.Errorf("initOrgMetrics: Initialising tags tree holder failed for org: %v, err: %v", orgid, err)
			orgMetricsAndTagsLock.Unlock()
			return err
		}
		orgMetricsAndTagsLock.Unlock()
	}
	return nil
}

func ResetMetricsSegStore_TestOnly() {
	OrgMetricsAndTags = make(map[int64]*MetricsAndTagsHolder)
}

/*
Returns the total incoming bytes, total on disk bytes, approx number of datapoints across all metric segments
*/
func GetUnrotatedMetricStats(orgid int64) (uint64, uint64, uint64) {
	totalIncoming := uint64(0)
	totalMSegEncodedSize := uint64(0)
	totalDPS := uint64(0)

	orgMetricsAndTagsLock.RLock()
	orgMetrics := map[string]*MetricsSegment{}
	if metricsAndTags, ok := OrgMetricsAndTags[orgid]; ok {
		orgMetrics = metricsAndTags.MetricSegments
	}
	orgMetricsAndTagsLock.RUnlock()

	for _, m := range orgMetrics {
		totalIncoming += m.bytesReceived
		totalMSegEncodedSize += m.mSegEncodedSize
		totalDPS += m.datapointCount
	}
	return totalIncoming, totalMSegEncodedSize, totalDPS
}

func getNumberOfSegmentsFromMemory(mem uint64) uint64 {
	mb := utils.ConvertUintBytesToMB(mem)
	retVal := mem / utils.MAX_BYTES_METRICS_BLOCK
	concurreny := uint64(config.GetParallelism())
	if retVal == 0 {
		log.Infof("getNumberOfSegmentsFromMemory: Less than %dMB was allocated. Defaulting to 1 metrics segment", utils.ConvertUintBytesToMB(mem))
		retVal = 1
	} else if retVal > concurreny {
		retVal = concurreny
	}
	log.Infof("Initializing %d metrics segments based on %dMB allocated memory", retVal, mb)
	return retVal
}

func timeBasedRotate() {
	for {
		time.Sleep(METRICS_BLK_ROTATE_SLEEP_DURATION * time.Second)
		for _, ms := range GetAllMetricsSegments() {
			encSize := atomic.LoadUint64(&ms.mBlock.blkEncodedSize)
			if encSize > utils.MAX_BYTES_METRICS_BLOCK {
				ms.rwLock.Lock()
				err := ms.CheckAndRotate(false)
				ms.rwLock.Unlock()
				if err != nil {
					log.Errorf("timeBasedRotate: Failed to rotate block %d for metric segment %s due to time. err=%v",
						ms.currBlockNum, ms.metricsKeyBase, err)
				}
			}
		}
	}
}

func timeBasedMetricsFlush() {
	for {
		time.Sleep(METRICS_BLK_FLUSH_SLEEP_DURATION * time.Second)
		for _, ms := range GetAllMetricsSegments() {

			encSize := atomic.LoadUint64(&ms.mBlock.blkEncodedSize)
			if encSize > 0 {
				ms.rwLock.Lock()
				err := ms.mBlock.rotateBlock(ms.metricsKeyBase, ms.Suffix, ms.currBlockNum)
				if err != nil {
					log.Errorf("timeBasedMetricsFlush: failed to rotate block number: %v due to the error: %v", ms.currBlockNum, err)
				} else {
					ms.currBlockNum++
				}

				ms.rwLock.Unlock()
			}
		}
	}
}

func timeBasedTagsTreeFlush() {
	for {
		time.Sleep(time.Duration(TAGS_TREE_FLUSH_SLEEP_DURATION) * time.Second)

		for _, tth := range GetAllTagsTreeHolders() {
			for tagKey, tt := range tth.allTrees {
				if tt.dirty {
					err := tt.flushSingleTagsTree(tagKey, tth.tagstreeBase)
					if err != nil {
						log.Errorf("timeBasedTagsTreeFlush: Error rotating tags tree for key %v at %v, err=%v", tagKey, tth.tagstreeBase, err)
					}
				}
			}
		}
	}
}

func timeBasedInstruFlush() {
	for {
		time.Sleep(METRICS_INSTRUMENTATION_FLUSH_DURATION * time.Second)

		orgMetricsAndTagsLock.RLock()
		for orgid, msegAndTags := range OrgMetricsAndTags {
			activeSeriesCount := uint64(0)
			for _, tth := range msegAndTags.TagHolders {
				tth.rwLock.RLock()
				activeSeriesCount += uint64(len(tth.tsidLookup))
				tth.rwLock.RUnlock()
				usageStats.UpdateActiveSeriesCount(orgid, activeSeriesCount)
			}
		}
		orgMetricsAndTagsLock.RUnlock()
	}
}

func InitMetricsSegment(orgid int64, mId string) (*MetricsSegment, error) {
	suffix, err := suffix.GetNextSuffix(mId, "ts")
	if err != nil {
		return nil, err
	}
	mKey, err := getBaseMetricsKey(suffix, mId)
	if err != nil {
		log.Errorf("InitMetricsSegment: Failed to get metrics key for suffix %v and mid %v, err=%v", suffix, mId, err)
		return nil, err
	}
	return &MetricsSegment{
		mNamesMap: make(map[string]bool, 0),
		mNameWalState: mNameWalState{
			metricsName: make([]string, 0),
		},
		currBlockNum: 0,
		mBlock: &MetricsBlock{
			tsidLookup:  make(map[uint64]int),
			allSeries:   make([]*TimeSeries, 0),
			sortedTsids: make([]uint64, 0),
			dpWalState: dpWalState{
				currentWALIndex: 0,
				segID:           suffix,
				mId:             mId,
				dpIdx:           0,
				dpsInWalMem:     make([]wal.WalDatapoint, utils.WAL_BLOCK_FLUSH_SIZE),
			},
			mBlockSummary: &structs.MBlockSummary{
				Blknum: 0,
				HighTs: 0,
				LowTs:  math.MaxInt32,
			},
			blkEncodedSize: 0,
		},
		rwLock:          &sync.RWMutex{},
		metricsKeyBase:  mKey,
		Suffix:          suffix,
		Mid:             mId,
		mSegEncodedSize: 0,
		highTS:          0,
		lowTS:           math.MaxUint32,
		Orgid:           orgid,
	}, nil
}

/*
Returns <<dataDir>>/<<hostname>>/final/ts/<<mid>>/{suffix}/suffix
*/
func getBaseMetricsKey(suffix uint64, mId string) (string, error) {
	// TODO: use filepath.Join
	var sb strings.Builder
	sb.WriteString(config.GetDataPath())
	sb.WriteString(config.GetHostID())
	sb.WriteString("/final/ts/")
	sb.WriteString(mId + "/")
	sb.WriteString(strconv.FormatUint(suffix, 10) + "/")
	basedir := sb.String()
	return basedir, nil
}

/*
Returns <<dataDir>>/<<hostname>>/final/<<mid>>/suffix
*/
func GetFinalMetricsDir(mId string, suffix uint64) string {
	// TODO: use filepath.Join
	var sb strings.Builder
	sb.WriteString(config.GetRunningConfig().DataPath)
	sb.WriteString(config.GetHostID())
	sb.WriteString("/final/ts/")
	sb.WriteString(mId + "/")
	sb.WriteString(strconv.FormatUint(suffix, 10) + "/")
	basedir := sb.String()
	return basedir
}

// returns the new series, number of bytes encoded, or any error
func initTimeSeries(tsid uint64, dp float64, timestamp uint32) (*TimeSeries, uint64, error) {
	ts := &TimeSeries{lock: &sync.Mutex{}}
	ts.rawEncoding = new(bytes.Buffer)
	c, finish, err := compress.NewCompressor(ts.rawEncoding, timestamp)
	if err != nil {
		log.Errorf("initTimeSeries: failed to create compressor for encoding=%v, timestamp=%v, err=%v", ts.rawEncoding, timestamp, err)
		return nil, 0, err
	}
	ts.cFinishFn = finish
	ts.compressor = c
	ts.nEntries++
	ts.lastKnownTS = timestamp
	writtenBytes, err := ts.compressor.Compress(timestamp, dp)
	if err != nil {
		return nil, 0, err
	}
	return ts, writtenBytes, nil
}

func (ms *MetricsSegment) LoadMetricNamesIntoMap(resultContainer map[string]bool) {
	ms.rwLock.RLock()
	defer ms.rwLock.RUnlock()

	for mName := range ms.mNamesMap {
		_, ok := resultContainer[mName]
		if !ok {
			resultContainer[mName] = true
		}
	}
}

/*
For a given metricName, tags, dp, and timestamp, add it to the respective in memory series

Internally, this function will try to find the series then will encode it.
If it cannot find the series or no space exists in the metrics segment, it will return an error

Return number of bytes written and any error encountered
*/
func EncodeDatapoint(mName []byte, tags *TagsHolder, dp float64, timestamp uint32, nBytes uint64, orgid int64) error {
	if len(mName) == 0 {
		log.Errorf("EncodeDatapoint: metric name is empty, orgid=%v", orgid)
		return fmt.Errorf("metric name is empty")
	}
	tsid, err := tags.GetTSID(mName)
	if err != nil {
		log.Errorf("EncodeDatapoint: failed to get TSID for metric=%s, orgid=%v, err=%v", mName, orgid, err)
		return err
	}
	mSeg, tth, err := getMetricsSegment(mName, orgid)
	if err != nil {
		log.Errorf("EncodeDatapoint: failed to get metrics segment for metric=%s, orgid=%v, err=%v", mName, orgid, err)
		return err
	}

	if mSeg == nil {
		log.Errorf("EncodeDatapoint: got nil metrics segment for metric=%s, orgid=%v", mName, orgid)
		return fmt.Errorf("no segment remaining to be assigned to orgid=%v", orgid)
	}

	mSeg.rwLock.Lock()
	if !mSeg.mNamesMap[string(mName)] {
		mSeg.mNamesMap[string(mName)] = true
		mSeg.mNameWalState.metricsName = append(mSeg.mNameWalState.metricsName, string(mName))
	}
	mSeg.rwLock.Unlock()

	mSeg.Orgid = orgid
	var ts *TimeSeries
	var seriesExists bool
	mSeg.rwLock.RLock()
	ts, seriesExists, err = mSeg.mBlock.GetTimeSeries(tsid)
	if err != nil {
		mSeg.rwLock.RUnlock()
		log.Errorf("EncodeDatapoint: failed to get time series for tsid=%v, metric=%s, orgid=%v, err=%v", tsid, mName, orgid, err)
		return err
	}
	var bytesWritten uint64
	mSeg.rwLock.RUnlock()

	// if the series does not exist, create it. but it may have been created by another goroutine during the same time
	// as a result, we will check again while holding the write lock
	// In addition, we need to always write at least one datapoint to the series to avoid panics on time based flushing

	if !seriesExists {
		ts, bytesWritten, err = initTimeSeries(tsid, dp, timestamp)
		if err != nil {
			log.Errorf("EncodeDatapoint: failed to create time series for tsid=%v, dp=%v, timestamp=%v, metric=%s, orgid=%v, err=%v",
				tsid, dp, timestamp, mName, orgid, err)
			return err
		}
		mSeg.rwLock.Lock()
		exists, idx, err := mSeg.mBlock.InsertTimeSeries(tsid, ts)
		if err != nil {
			mSeg.rwLock.Unlock()
			log.Errorf("EncodeDatapoint: failed to insert time series for tsid=%v, dp=%v, timestamp=%v, metric=%s, orgid=%v, err=%v",
				tsid, dp, timestamp, mName, orgid, err)
			return err
		}
		if !exists { // if the new series was actually added, add the tsid to the block
			mSeg.mBlock.addTsidToBlock(tsid)
		}
		mSeg.rwLock.Unlock()
		if exists {
			bytesWritten, err = mSeg.mBlock.allSeries[idx].AddSingleEntry(dp, timestamp)
			if err != nil {
				log.Errorf("EncodeDatapoint: failed to add single entry for tsid=%v, dp=%v, timestamp=%v, metric=%s, orgid=%v, err=%v",
					tsid, dp, timestamp, mName, orgid, err)
				return err
			}
		}
		err = tth.AddTagsForTSID(mName, tags, tsid)
		if err != nil {
			log.Errorf("EncodeDatapoint: failed to add tags for tsid=%v, metric=%s, orgid=%v, err=%v", tsid, mName, orgid, err)
			return err
		}
	} else {
		bytesWritten, err = ts.AddSingleEntry(dp, timestamp)
		if err != nil {
			log.Errorf("EncodeDatapoint: failed to add single entry for tsid=%v, dp=%v, timestamp=%v, metric=%s, orgid=%v, err=%v",
				tsid, dp, timestamp, mName, orgid, err)
			return err
		}
	}
	err = mSeg.mBlock.appendToWALBuffer(timestamp, dp, tsid)
	if err != nil {
		return err
	}

	mSeg.updateTimeRange(timestamp)
	mSeg.mBlock.mBlockSummary.UpdateTimeRange(timestamp)
	atomic.AddUint64(&mSeg.mBlock.blkEncodedSize, bytesWritten)
	atomic.AddUint64(&mSeg.mSegEncodedSize, bytesWritten)
	atomic.AddUint64(&mSeg.bytesReceived, nBytes)
	atomic.AddUint64(&mSeg.datapointCount, 1)

	return nil
}

type walFilesInfo struct {
	mId      string
	segID    uint64
	blockNo  uint64
	walFiles []string
}

func extractWALFileInfo(baseDir string) (map[string]*walFilesInfo, error) {
	files, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}

	filesInfo := make(map[string]*walFilesInfo)

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fileName := file.Name()

		// Expected WAL filename format: "shardId_<shard>_segId_<segID>_blockId_<blockNo>_<walFileIndex>.wal"
		parts := strings.Split(fileName, "_")
		if len(parts) < 6 {
			continue
		}

		mId := parts[1]
		segIDStr := parts[3]
		blockNoStr := parts[5]

		segID, err1 := strconv.ParseUint(segIDStr, 10, 64)
		blockNo, err2 := strconv.ParseUint(blockNoStr, 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}

		key := mId + "_" + segIDStr + "_" + blockNoStr
		if _, exists := filesInfo[key]; !exists {
			filesInfo[key] = &walFilesInfo{
				mId:      mId,
				segID:    segID,
				blockNo:  blockNo,
				walFiles: []string{},
			}
		}
		filesInfo[key].walFiles = append(filesInfo[key].walFiles, fileName)
	}

	return filesInfo, nil
}

func deleteWalFile(dirPath, fileName string) error {
	filePath := filepath.Join(dirPath, fileName)
	err := os.Remove(filePath)
	if err != nil {
		log.Errorf("deleteWalFile: Failed to delete file %s: %v", filePath, err)
		return err
	}
	return nil
}

func initMetricsBlock(shardID string, segID uint64, blockNo uint64) *MetricsBlock {
	mBlock := &MetricsBlock{
		tsidLookup:  make(map[uint64]int),
		allSeries:   make([]*TimeSeries, 0),
		sortedTsids: make([]uint64, 0),
		dpWalState: dpWalState{
			currentWALIndex: 0,
			segID:           segID,
			mId:             shardID,
		},
		mBlockSummary: &structs.MBlockSummary{
			Blknum: uint16(blockNo),
			HighTs: 0,
			LowTs:  math.MaxInt32,
		},
		blkEncodedSize: 0,
	}
	return mBlock
}

func RecoverWALData() error {
	baseDir := getWALBaseDir()
	walFilesData, err := extractWALFileInfo(baseDir)
	if err != nil {
		return err
	}

	for _, fileData := range walFilesData {
		mBlock := initMetricsBlock(fileData.mId, fileData.segID, fileData.blockNo)
		isWalFileEmpty := true
		for _, walFileName := range fileData.walFiles {
			filePath := filepath.Join(baseDir, walFileName)
			walIterator, err := wal.NewWALReader(filePath)
			if err != nil {
				log.Warnf("RecoverWALData :Failed to create WAL reader for file %s: %v", walFileName, err)
				continue
			}
			for {
				walDataPoint, err := walIterator.Next()
				if err != nil {
					log.Warnf("RecoverWALData : Error reading next WAL entry from file %s: %v", walFileName, err)
					break
				}
				if walDataPoint == nil {
					break
				}
				err = mBlock.encodeDatapoint(walDataPoint.Timestamp, walDataPoint.DpVal, walDataPoint.Tsid)
				if err != nil {
					log.Warnf("RecoverWALData : Failed to process WAL datapoint from file %s: %v", walFileName, err)
					break
				}
				isWalFileEmpty = false
			}
			_ = walIterator.Close()
			err = deleteWalFile(baseDir, walFileName)
			if err != nil {
				log.Warnf("RecoverWALData : Failed to delete wal file %s: %v", walFileName, err)
			}
		}

		if !isWalFileEmpty {
			metricsKey, _ := getBaseMetricsKey(fileData.segID, fileData.mId)
			err := mBlock.flushBlock(metricsKey, fileData.segID, uint16(fileData.blockNo))
			if err != nil {
				log.Warnf("RecoverWALData :Failed to flush block for shardID=%s, segID=%d, blockNo=%d: %v",
					fileData.mId, fileData.segID, fileData.blockNo, err)
			}
		}

	}
	return nil

}

func (mb *MetricsBlock) encodeDatapoint(timestamp uint32, dpVal float64, tsid uint64) error {
	var ts *TimeSeries
	var seriesExists bool
	var err error

	ts, seriesExists, err = mb.GetTimeSeries(tsid)
	if err != nil {
		log.Errorf("encodeDatapoint: failed to get time series for tsid=%v, err=%v", tsid, err)
		return err
	}

	if !seriesExists {
		ts, _, err = initTimeSeries(tsid, dpVal, timestamp)
		if err != nil {
			log.Errorf("encodeDatapoint: failed to create time series for tsid=%v, dp=%v, timestamp=%v, err=%v",
				tsid, dpVal, timestamp, err)
			return err
		}

		exists, idx, err := mb.InsertTimeSeries(tsid, ts)
		if err != nil {
			log.Errorf("encodeDatapoint: failed to insert time series for tsid=%v, dp=%v, timestamp=%v, err=%v",
				tsid, dpVal, timestamp, err)
			return err
		}
		if !exists {
			mb.addTsidToBlock(tsid)
		}

		if exists {
			_, err = mb.allSeries[idx].AddSingleEntry(dpVal, timestamp)
			if err != nil {
				log.Errorf("encodeDatapoint: failed to add single entry for tsid=%v, dp=%v, timestamp=%v, err=%v",
					tsid, dpVal, timestamp, err)
				return err
			}
		}
	} else {
		_, err = ts.AddSingleEntry(dpVal, timestamp)
		if err != nil {
			log.Errorf("encodeDatapoint: failed to add single entry for tsid=%v, dp=%v, timestamp=%v, err=%v",
				tsid, dpVal, timestamp, err)
			return err
		}
	}

	mb.mBlockSummary.UpdateTimeRange(timestamp)
	return nil
}

/*
Caller is responsible for acquiring and releasing locks
*/
func (mb *MetricsBlock) addTsidToBlock(tsid uint64) {
	l := len(mb.sortedTsids)
	if l == 0 {
		mb.sortedTsids = append(mb.sortedTsids, tsid)
		return
	}

	mb.sortedTsids = append(mb.sortedTsids, tsid)
}

// for an input raw json []byte, return the metric name, datapoint value, timestamp, all tags, and any errors occurred
// The metric name is returned as a raw []byte
// The tags
func ExtractOTSDBPayload(rawJson []byte, tags *TagsHolder) ([]byte, float64, uint32, error) {
	var mName []byte
	var dpVal float64
	var ts uint32
	var err error

	if tags == nil {
		log.Errorf("ExtractOTSDBPayload: tags holder is nil")
		return nil, 0, 0, fmt.Errorf("tags holder is nil")
	}

	handler := func(key []byte, value []byte, valueType jp.ValueType, off int) error {
		switch {
		case bytes.Equal(key, otsdb_mname), bytes.Equal(key, metric_name_key):
			switch valueType {
			case jp.String:
				_, err := jp.ParseString(value)
				if err != nil {
					log.Errorf("ExtractOTSDBPayload: failed to parse %v as string, err=%v", value, err)
					return err
				}
				mName = value
			default:
				return toputils.TeeErrorf("ExtractOTSDBPayload: invalid type %v for metric name %v", valueType, value)
			}
		case bytes.Equal(key, otsdb_tags):
			if valueType != jp.Object {
				log.Errorf("ExtractOTSDBPayload: tags key %s has value %s of type %v, which is not an object", key, value, valueType)
				return fmt.Errorf("value type %v is not an object", valueType)
			}
			err = extractTagsFromJson(value, tags)
			if err != nil {
				log.Errorf("ExtractOTSDBPayload: failed to extract tags. value=%s, tags=%+v, err=%v", value, tags, err)
				return err
			}
		case bytes.Equal(key, otsdb_timestamp):
			switch valueType {
			case jp.Number:
				intVal, err := jp.ParseInt(value)
				if err != nil {
					fltVal, err := jp.ParseFloat(value)
					if err != nil {
						log.Errorf("ExtractOTSDBPayload: failed to parse timestamp %v as int or float, err=%v", value, err)
						return fmt.Errorf("ExtractOTSDBPayload: failed to parse timestamp! Not expected type:%+v", valueType.String())
					} else {
						if toputils.IsTimeInMilli(uint64(fltVal)) {
							ts = uint32(fltVal / 1000)
						} else {
							ts = uint32(fltVal)
						}
					}
				} else {
					if toputils.IsTimeInMilli(uint64(intVal)) {
						ts = uint32(intVal / 1000)
					} else {
						ts = uint32(intVal)
					}
				}
			case jp.String:
				// First, try to parse the date as a number (seconds or milliseconds since epoch)
				if t, err := strconv.ParseInt(string(value), 10, 64); err == nil {
					// Determine if the number is in seconds or milliseconds
					if toputils.IsTimeInMilli(uint64(t)) {
						ts = uint32(t / 1000)
					} else {
						ts = uint32(t)
					}

					return nil
				}

				// Parse the string to time using time.Parse and multiple layouts.
				found := false
				for _, layout := range dateTimeLayouts {
					t, err := time.Parse(layout, string(value))
					if err == nil {
						found = true
						ts = uint32(t.Unix())
						break
					}
				}
				if !found {
					log.Errorf("ExtractOTSDBPayload: unknown timestamp format %s", value)
					return fmt.Errorf("unknown timestamp format %s", value)
				}
			default:
				return toputils.TeeErrorf("ExtractOTSDBPayload: invalid type %v for timestamp %v", valueType, value)
			}
		case bytes.Equal(key, otsdb_value):
			if valueType != jp.Number {
				log.Errorf("ExtractOTSDBPayload: value %s of type %v is not a number", value, valueType)
				return fmt.Errorf("value is not a number")
			}
			fltVal, err := jp.ParseFloat(value)
			if err != nil {
				log.Errorf("ExtractOTSDBPayload: failed to parse value %v as float, err=%v", value, err)
				return fmt.Errorf("failed to convert value to float! %+v", err)
			}
			dpVal = fltVal
		case bytes.Equal(key, metric_value_gauge_keyname), bytes.Equal(key, metric_value_counter_keyname),
			bytes.Equal(key, metric_value_histogram_keyname), bytes.Equal(key, metric_value_summary_keyname):
			if valueType != jp.Object {
				log.Errorf("ExtractOTSDBPayload: value %s of type %v is not an object", value, valueType)
				return fmt.Errorf("value is not an object")
			}
			err = jp.ObjectEach(value, func(key []byte, value []byte, valueType jp.ValueType, off int) error {
				if bytes.Equal(key, otsdb_value) {
					if valueType != jp.Number {
						log.Errorf("ExtractOTSDBPayload: value %s of type %v is not a number", value, valueType)
						return fmt.Errorf("value is not a number")
					}
					fltVal, err := jp.ParseFloat(value)
					if err != nil {
						log.Errorf("ExtractOTSDBPayload: failed to parse value %v as float, err=%v", value, err)
						return fmt.Errorf("failed to convert value to float! %+v", err)
					}
					dpVal = fltVal
				}
				return nil
			})

			return err
		}
		return nil
	}
	rawJson = bytes.Replace(rawJson, []byte("NaN"), []byte("0"), -1)
	err = jp.ObjectEach(rawJson, handler)
	if err != nil {
		log.Errorf("ExtractOTSDBPayload: failed to parse json %s, err=%v", rawJson, err)
		return mName, dpVal, ts, err
	}
	if len(mName) > 0 && ts > 0 {
		return mName, dpVal, ts, nil
	} else if len(mName) == 0 && err == nil {
		return nil, dpVal, 0, nil
	} else {
		err = fmt.Errorf("ExtractOTSDBPayload: failed to find all expected keys. mName=%s, ts=%d, dpVal=%f", mName, ts, dpVal)
		log.Errorf(err.Error())
		return nil, dpVal, 0, err
	}
}

func ExtractOTLPPayload(rawJson []byte, tags *TagsHolder) ([]byte, float64, uint32, error) {
	var mName []byte
	var dpVal float64
	var ts uint32
	var err error

	if tags == nil {
		log.Errorf("ExtractOTLPPayload: tags holder is nil")
		return nil, 0, 0, fmt.Errorf("tags holder is nil")
	}

	handler := func(key []byte, value []byte, valueType jp.ValueType, off int) error {
		switch {
		case bytes.Equal(key, otsdb_mname):
			switch valueType {
			case jp.String:
				_, err := jp.ParseString(value)
				if err != nil {
					log.Errorf("ExtractOTLPPayload: failed to parse %v as string, err=%v", value, err)
					return err
				}
				mName = value
			default:
				return toputils.TeeErrorf("ExtractOTLPPayload: invalid type %v for metric name %v", valueType, value)
			}
		case bytes.Equal(key, otsdb_tags):
			if valueType != jp.Object {
				log.Errorf("ExtractOTLPPayload: tags key %s has value %s of type %v, which is not an object", key, value, valueType)
				return fmt.Errorf("value type %v is not an object", valueType)
			}
			err = extractTagsFromJson(value, tags)
			if err != nil {
				log.Errorf("ExtractOTLPPayload: failed to extract tags. value=%s, tags=%+v, err=%v", value, tags, err)
				return err
			}
		case bytes.Equal(key, otsdb_timestamp):
			switch valueType {
			case jp.Number:
				intVal, err := jp.ParseInt(value)
				if err != nil {
					fltVal, err := jp.ParseFloat(value)
					if err != nil {
						log.Errorf("ExtractOTLPPayload: failed to parse timestamp %v as int or float, err=%v", value, err)
						return fmt.Errorf("ExtractOTLPPayload: failed to parse timestamp! Not expected type:%+v", valueType.String())
					} else {
						if toputils.IsTimeInNano(uint64(fltVal)) {
							ts = uint32(fltVal / 1_000_000_000)
						} else if toputils.IsTimeInMilli(uint64(fltVal)) {
							ts = uint32(fltVal / 1000)
						} else {
							ts = uint32(fltVal)
						}
					}
				} else {
					if toputils.IsTimeInNano(uint64(intVal)) {
						ts = uint32(intVal / 1_000_000_000)
					} else if toputils.IsTimeInMilli(uint64(intVal)) {
						ts = uint32(intVal / 1000)
					} else {
						ts = uint32(intVal)
					}
				}

			default:
				return toputils.TeeErrorf("ExtractOTLPPayload: invalid type %v for timestamp %v", valueType, value)
			}
		case bytes.Equal(key, otsdb_value):
			if valueType != jp.Number {
				log.Errorf("ExtractOTLPPayload: value %s of type %v is not a number", value, valueType)
				return fmt.Errorf("value is not a number")
			}
			fltVal, err := jp.ParseFloat(value)
			if err != nil {
				log.Errorf("ExtractOTLPPayload: failed to parse value %v as float, err=%v", value, err)
				return fmt.Errorf("failed to convert value to float! %+v", err)
			}
			dpVal = fltVal

		}
		return nil
	}
	rawJson = bytes.Replace(rawJson, []byte("NaN"), []byte("0"), -1)
	err = jp.ObjectEach(rawJson, handler)
	if err != nil {
		log.Errorf("ExtractOTLPPayload: failed to parse json %s, err=%v", rawJson, err)
		return mName, dpVal, ts, err
	}
	if len(mName) == 0 && err == nil {
		return nil, dpVal, 0, nil
	}

	if len(mName) > 0 && ts > 0 {
		return mName, dpVal, ts, nil
	}

	err = fmt.Errorf("ExtractOTLPPayload: failed to find all expected keys. mName=%s, ts=%d, dpVal=%f", mName, ts, dpVal)
	log.Errorf(err.Error())

	return nil, dpVal, 0, err
}

// extracts raw []byte from the read tags objects and returns it as []*tagsHolder
// the returned []*tagsHolder is sorted by tagKey
func extractTagsFromJson(tagsObj []byte, tags *TagsHolder) error {
	handler := func(key []byte, value []byte, valueType jp.ValueType, off int) error {
		if key == nil {
			log.Errorf("extractTagsFromJson: key is nil. value=%+v valueType=%+v", value, valueType)
			return nil
		}

		strKey, err := jp.ParseString(key)
		if err != nil {
			log.Errorf("extractTagsFromJson: failed to parse key %v as string. value=%+v valueType=%+v, err=%v", key, value, valueType, err)
			return err
		}
		tags.Insert(strKey, value, valueType)
		return nil
	}
	err := jp.ObjectEach(tagsObj, handler)
	if err != nil {
		log.Errorf("extractTagsFromJson: failed to parse tags object %s, err=%v", tagsObj, err)
		return err
	}
	return nil
}

func getMetricsSegment(mName []byte, orgid int64) (*MetricsSegment, *TagsTreeHolder, error) {
	orgMetricsAndTagsLock.RLock()
	metricsAndTagsHolder, ok := OrgMetricsAndTags[orgid]
	orgMetricsAndTagsLock.RUnlock()
	if !ok || len(metricsAndTagsHolder.MetricSegments) == 0 {
		err := initOrgMetrics(orgid)
		if err != nil {
			log.Errorf("getMetricsSegment: Failed to initialize metrics segments for org %v: %v", orgid, err)
			return nil, nil, err
		}
		orgMetricsAndTagsLock.RLock()
		metricsAndTagsHolder = OrgMetricsAndTags[orgid]
		orgMetricsAndTagsLock.RUnlock()
	}
	mid := fmt.Sprint(xxhash.Sum64(mName) % uint64(len(metricsAndTagsHolder.MetricSegments)))
	return metricsAndTagsHolder.MetricSegments[mid], metricsAndTagsHolder.TagHolders[mid], nil
}

func getUnrotatedMetricSegment(mid string, orgid int64) (*MetricsSegment, error) {
	orgMetricsAndTagsLock.RLock()
	metricsAndTagsHolder, ok := OrgMetricsAndTags[orgid]
	orgMetricsAndTagsLock.RUnlock()
	if !ok {
		return nil, fmt.Errorf("getMetricSegmentFromMid: no metrics segment found for orgid=%v", orgid)
	}

	mSeg, ok := metricsAndTagsHolder.MetricSegments[mid]
	if !ok {
		return nil, fmt.Errorf("getMetricSegmentFromMid: no metrics segment found for orgid=%v and mid=%v", orgid, mid)
	}

	return mSeg, nil
}

/*
returns:

	*TimeSeries corresponding to tsid if found
	bool indicating if the tsid was found

This will create the time series if it doesn't exist already
*/
func (mb *MetricsBlock) GetTimeSeries(tsid uint64) (*TimeSeries, bool, error) {
	var ts *TimeSeries
	idx, ok := mb.tsidLookup[tsid]
	if !ok {
		if len(mb.allSeries) >= utils.MAX_ACTIVE_SERIES_PER_SEGMENT {
			err := fmt.Errorf("MetricsBlock.GetTimeSeries: reached limit for max active series (%d) per segment", utils.MAX_ACTIVE_SERIES_PER_SEGMENT)
			log.Errorf(err.Error())
			return nil, false, err
		}
		return nil, false, nil
	} else {
		ts = mb.allSeries[idx]
	}
	return ts, ok, nil
}

func (mb *MetricsBlock) getUnrotatedBlockTimeSeriesIterator(tsid uint64, bytesBuffer *bytes.Buffer) (bool, *compress.DecompressIterator, error) {
	idx, ok := mb.tsidLookup[tsid]
	if !ok {
		return false, nil, nil
	}

	ts := mb.allSeries[idx]
	if ts == nil || ts.rawEncoding == nil {
		return false, nil, nil
	}

	ts.lock.Lock()
	defer ts.lock.Unlock()

	_, finish, err := compress.CloneCompressor(ts.compressor, bytesBuffer)
	if err != nil {
		return false, nil, err
	}

	err = finish()
	if err != nil {
		return false, nil, err
	}

	iter, err := compress.NewDecompressIterator(bytesBuffer)

	return true, iter, err
}

/*
Inserts a time series for the given tsid

	The caller is responsible for acquiring and releasing the the required locks

Returns bool if the tsid already existed, the idx it exists at, or any errors
*/
func (mb *MetricsBlock) InsertTimeSeries(tsid uint64, ts *TimeSeries) (bool, int, error) {
	idx, ok := mb.tsidLookup[tsid]
	if !ok {
		if len(mb.allSeries) >= utils.MAX_ACTIVE_SERIES_PER_SEGMENT {
			err := fmt.Errorf("MetricsBlock.InsertTimeSeries: reached limit for max active series (%d) per segment", utils.MAX_ACTIVE_SERIES_PER_SEGMENT)
			log.Errorf(err.Error())
			return false, 0, err
		}
		mb.tsidLookup[tsid] = len(mb.allSeries)
		idx = len(mb.allSeries)
		mb.allSeries = append(mb.allSeries, ts)
	}
	if ok {
		return true, idx, nil
	}
	return false, idx, nil
}

/*
adds this single dp and time entry to the time series
encode dpVal & dpTs using dod / floating point compression
every 15 mins, if a series was updated, we need to flush it

Returns number of bytes written, or any errors encoundered
*/
func (ts *TimeSeries) AddSingleEntry(dpVal float64, dpTS uint32) (uint64, error) {
	ts.lock.Lock()
	defer ts.lock.Unlock()
	var writtenBytes uint64
	var err error
	if ts.nEntries == 0 {
		ts.rawEncoding = new(bytes.Buffer)

		// set the header of the dod to the current epoch time. TODO: prevent additions if dpTS is not within 2hrs of header
		c, finish, err := compress.NewCompressor(ts.rawEncoding, dpTS)
		if err != nil {
			log.Errorf("TimeSeries.AddSingleEntry: failed to create compressor for encoding=%v, timestamp=%v, err=%v", ts.rawEncoding, dpTS, err)
			return writtenBytes, err
		}
		ts.cFinishFn = finish
		ts.compressor = c
		writtenBytes, err = ts.compressor.Compress(dpTS, dpVal)
		if err != nil {
			log.Errorf("TimeSeries.AddSingleEntry: failed to compress dpTS=%v, dpVal=%v, num entries=%v, err=%v", dpTS, dpVal, ts.nEntries, err)
			return writtenBytes, err
		}
	} else {
		writtenBytes, err = ts.compressor.Compress(dpTS, dpVal)
		if err != nil {
			log.Errorf("TimeSeries.AddSingleEntry: failed to compress dpTS=%v, dpVal=%v, num entries=%v, err=%v", dpTS, dpVal, ts.nEntries, err)
			return writtenBytes, err
		}
	}
	ts.nEntries++
	ts.lastKnownTS = dpTS
	return writtenBytes, nil
}

/*
Wrapper function to check and rotate the current metrics block or the metrics segment

Caller is responsible for acquiring locks
*/
func (ms *MetricsSegment) CheckAndRotate(forceRotate bool) error {

	totalEncSize := atomic.LoadUint64(&ms.mSegEncodedSize)
	blkEncSize := atomic.LoadUint64(&ms.mBlock.blkEncodedSize)
	if blkEncSize > utils.MAX_BYTES_METRICS_BLOCK || (blkEncSize > 0 && forceRotate) ||
		(blkEncSize > 0 && totalEncSize > utils.MAX_BYTES_METRICS_SEGMENT) {
		err := ms.mBlock.rotateBlock(ms.metricsKeyBase, ms.Suffix, ms.currBlockNum)
		if err != nil {
			log.Errorf("MetricsSegment.CheckAndRotate: failed to rotate block for key=%v, suffix=%v, blocknum=%v, err=%v",
				ms.metricsKeyBase, ms.Suffix, ms.currBlockNum, err)
			return err
		}
		if !forceRotate {
			ms.currBlockNum++
		}
	}

	if totalEncSize > utils.MAX_BYTES_METRICS_SEGMENT || (totalEncSize > 0 && forceRotate) {
		err := ms.rotateSegment(forceRotate)
		if err != nil {
			log.Errorf("MetricsSegment.CheckAndRotate: failed to rotate mid %v: %v", ms.metricsKeyBase, err)
			return err
		}
		tt := GetTagsTreeHolder(ms.Orgid, ms.Mid)
		tt.flushTagsTree()
		if forceRotate || time.Since(tt.createdTime) > time.Duration(24*time.Hour) {
			err = tt.rotateTagsTree(forceRotate)
			if err != nil {
				log.Errorf("MetricsSegment.CheckAndRotate: failed to rotate tags tree %v: %v", tt.tagstreeBase, err)
			}
		}
	}
	return nil
}

/*
Format of TSO file:
Version 1
[version - 1 byte][number of tsids - 2 bytes][tsid - 8bytes][offset - 4 bytes][tsid - 8bytes]...

Version 2: 8 bytes for number of tsids
[version - 1 byte][number of tsids - 8 bytes][tsid - 8bytes][offset - 4 bytes][tsid - 8bytes]...

Format of TSG file:
[version - 1 byte][tsid - 8bytes][len - 4 bytes][raw series - n bytes][tsid - 8 bytes]...
*/
func (mb *MetricsBlock) FlushTSOAndTSGFiles(file string) error {
	tsoFileName := file + ".tso"
	tsgFileName := file + ".tsg"
	tsoBuffer := bytes.NewBuffer(nil)
	tsgBuffer := bytes.NewBuffer(nil)
	length := len(mb.sortedTsids)

	sort.Slice(mb.sortedTsids, func(i, j int) bool {
		return mb.sortedTsids[i] < mb.sortedTsids[j]
	})

	_, err := tsoBuffer.Write(utils.VERSION_TSOFILE_V2)
	if err != nil {
		log.Infof("FlushTSOAndTSGFiles: Could not write version byte to file %v. Err %v", tsoFileName, err)
		return err
	}

	_, err = tsgBuffer.Write(utils.VERSION_TSGFILE)
	if err != nil {
		log.Infof("FlushTSOAndTSGFiles: Could not write version byte to file %v. Err %v", tsoFileName, err)
		return err
	}

	size := uint32(0)
	_, err = tsoBuffer.Write(toputils.Uint64ToBytesLittleEndian(uint64(length)))
	if err != nil {
		log.Infof("FlushTSOAndTSGFiles: Could not write tsid to file %v. Err %v", tsoFileName, err)
		return err
	}

	for _, tsid := range mb.sortedTsids {
		_, err := tsoBuffer.Write(toputils.Uint64ToBytesLittleEndian(tsid))
		if err != nil {
			log.Infof("FlushTSOAndTSGFiles: Could not write tsid to file %v. Err %v", tsoFileName, err)
			return err
		}
		_, err = tsoBuffer.Write(toputils.Uint32ToBytesLittleEndian(size))
		if err != nil {
			log.Infof("FlushTSOAndTSGFiles: Could not write tsid offset to file %v. Err %v", tsoFileName, err)
			return err
		}

		_, err = tsgBuffer.Write(toputils.Uint64ToBytesLittleEndian(tsid))
		size += 8
		if err != nil {
			log.Infof("FlushTSOAndTSGFiles: Could not write tsid to file %v. Err %v", tsgFileName, err)
			return err
		}

		index := mb.tsidLookup[tsid]
		err = mb.allSeries[index].cFinishFn()
		if err != nil {
			log.Infof("FlushTSOAndTSGFiles: Could not mark the finish of raw encoding time series, err:%v", err)
			return err
		}

		_, err = tsgBuffer.Write(toputils.Uint32ToBytesLittleEndian(uint32(mb.allSeries[index].rawEncoding.Len())))
		size += 4
		if err != nil {
			log.Infof("FlushTSOAndTSGFiles: Could not write len of raw series to file %v. Err %v", tsgFileName, err)
			return err
		}

		n, err := tsgBuffer.Write(mb.allSeries[index].rawEncoding.Bytes())
		if err != nil {
			log.Infof("FlushTSOAndTSGFiles: Could not write raw series to file %v. Err %v", tsgFileName, err)
			return err
		}
		size += uint32(n)
	}
	fdTso, err := os.OpenFile(tsoFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("FlushTSOAndTSGFiles: Error creating file %s: %v", tsoFileName, err)
		return err
	}
	defer fdTso.Close()
	fdTsg, err := os.OpenFile(tsgFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("FlushTSOAndTSGFiles: Error creating file %s: %v", tsgFileName, err)
		return err
	}
	defer fdTsg.Close()

	_, err = fdTso.Write(tsoBuffer.Bytes())
	if err != nil {
		log.Errorf("FlushTSOFile: Failed to write to TSO file %v, err=%v", tsoFileName, err)
		return err
	}
	_, err = fdTsg.Write(tsgBuffer.Bytes())
	if err != nil {
		log.Errorf("FlushTSOFile: Failed to write to TSO file %v, err=%v", tsgFileName, err)
		return err
	}
	return nil
}

func (mb *MetricsBlock) flushBlock(basePath string, suffix uint64, bufId uint16) error {
	finalPath := fmt.Sprintf("%s%d_%d", basePath, suffix, bufId)
	fName := fmt.Sprintf("%s%d.mbsu", basePath, suffix)
	err := mb.mBlockSummary.FlushSummary(fName)
	if err != nil {
		log.Errorf("MetricsBlock.flushBlock: Failed to write metrics block summary for block at %s, err=%v", finalPath, err)
		return err
	}
	err = mb.FlushTSOAndTSGFiles(finalPath)
	if err != nil {
		log.Errorf("MetricsBlock.flushBlock: Failed to flush TSO and TSG files at %s, err=%v", finalPath, err)
		return err
	}
	return nil
}

/*
Flushes the current metricsBlock & resets the struct for the new block

TODO: filepath / force flush before rotate
*/
func (mb *MetricsBlock) rotateBlock(basePath string, suffix uint64, bufId uint16) error {
	err := mb.flushBlock(basePath, suffix, bufId)
	if err != nil {
		log.Errorf("MetricsBlock.rotateBlock: Failed to flush block at %v/%v/%v", basePath, suffix, bufId)
		return err
	}

	// erase map
	for k := range mb.tsidLookup {
		delete(mb.tsidLookup, k)
	}

	// we can't do mb.allSeries = mb.allSeries[:0], as the other entries of the slice won't be GC'ed
	newSeries := make([]*TimeSeries, 0, len(mb.allSeries))
	mb.allSeries = newSeries
	mb.blkEncodedSize = 0
	mb.sortedTsids = make([]uint64, 0)
	mb.mBlockSummary.Blknum++
	mb.mBlockSummary.HighTs = 0
	mb.mBlockSummary.LowTs = math.MaxInt32

	err = mb.cleanAndInitNewDpWal()
	if err != nil {
		log.Errorf("rotateBlock : Failed to initialize new WAL: %v", err)
		return err
	}

	return nil
}

func (mb *MetricsBlock) cleanAndInitNewDpWal() error {
	mb.deleteDpWalFiles()
	mb.dpWalState.dpIdx = 0
	mb.dpWalState.allWALs = mb.dpWalState.allWALs[:0]
	mb.dpWalState.currentWALIndex = 0
	err := mb.initNewDpWal()
	if err != nil {
		log.Errorf("cleanAndInitNewDpWal : Failed to initialize new WAL: %v", err)
		return err
	}
	return nil
}

/*
Flushes the metrics segment's tags tree, mNames

This function assumes that the prior metricssBlock has alraedy been rotated/reset
*/
func (ms *MetricsSegment) rotateSegment(forceRotate bool) error {
	var err error
	err = ms.FlushMetricNames()
	if err != nil {
		log.Errorf("rotateSegment: failed to flush metric names for base=%s, suffix=%d, orgid=%v. Error %+v", ms.metricsKeyBase, ms.Suffix, ms.Orgid, err)
		return err
	}
	finalDir := GetFinalMetricsDir(ms.Mid, ms.Suffix)
	metaEntry := ms.getMetaEntry(finalDir, ms.Suffix)
	err = os.MkdirAll(path.Dir(path.Dir(finalDir)), 0764)
	if err != nil {
		log.Errorf("rotateSegment: failed to create directory %s to %s for orgid=%v, Error %+v", ms.metricsKeyBase, finalDir, ms.Orgid, err)
		return err
	}

	err = toputils.WriteValidityFile(ms.metricsKeyBase)
	if err != nil {
		log.Errorf("rotateSegment: failed to write validity file for %s, orgid=%v, Error %+v", ms.metricsKeyBase, ms.Orgid, err)
		return err
	}

	log.Infof("rotating segment of size %v that created %v metrics blocks to %+v", ms.mSegEncodedSize, ms.currBlockNum+1, finalDir)
	if !forceRotate {
		nextSuffix, err := suffix.GetNextSuffix(ms.Mid, "ts")
		if err != nil {
			log.Errorf("rotateSegment: failed to get the next suffix for metric ID %s, orgid=%v, err %+v", ms.Mid, ms.Orgid, err)
			return err
		}
		mKey, err := getBaseMetricsKey(nextSuffix, ms.Mid)
		if err != nil {
			log.Errorf("rotateSegment: failed to get next base key for metric ID %s, orgid=%v, err %+v", ms.Mid, ms.Orgid, err)
			return err
		}
		for k := range ms.mNamesMap {
			delete(ms.mNamesMap, k)
		}

		ms.metricsKeyBase = mKey
		ms.Suffix = nextSuffix
		ms.highTS = 0
		ms.lowTS = math.MaxUint32
		ms.currBlockNum = 0
		ms.mSegEncodedSize = 0
		ms.datapointCount = 0
		ms.bytesReceived = 0
		ms.mBlock.mBlockSummary.Reset()
		ms.mBlock.dpWalState.segID = nextSuffix
		err = ms.mBlock.cleanAndInitNewDpWal()
		if err != nil {
			log.Errorf("rotateSegment : Failed to initialize new WAL: %v", err)
			return err
		}

	}

	err = ms.cleanAndInitNewMNameWal(forceRotate)
	if err != nil {
		log.Errorf("rotateSegment : Failed to initialize new metrics name WAL: %v", err)
		return err
	}

	err = meta.AddMetricsMetaEntry(metaEntry)
	if err != nil {
		log.Errorf("rotateSegment: failed to add metrics meta entry %+v, orgid=%v, Error %+v", metaEntry, ms.Orgid, err)
		return err
	}

	return blob.UploadIngestNodeDir()
}

// This is a mock function and is only used during tests.
func (ms *MetricsSegment) SetMockMetricSegmentMNamesMap(mNamesCount uint32, mNameBase string) string {
	ms.mNamesMap = make(map[string]bool)
	ms.metricsKeyBase = "./testMockMetric"
	ms.Suffix = uint64(0)
	for i := 0; i < int(mNamesCount); i++ {
		ms.mNamesMap[fmt.Sprintf("%s_%d", mNameBase, i)] = true
	}
	return fmt.Sprintf("%s%d.mnm", ms.metricsKeyBase, ms.Suffix)
}

/*
- Flushes the metrics segment's mNamesMap to disk
- The Metirc Names are stored in the Length and Value format.
*/
func (ms *MetricsSegment) FlushMetricNames() error {
	if len(ms.mNamesMap) == 0 {
		log.Warnf("FlushMetricNames: empty mNamesMap")
		return nil
	}

	filePath := fmt.Sprintf("%s%d.mnm", ms.metricsKeyBase, ms.Suffix)

	fd, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("FlushMetricNames: failed to open filename=%v: err=%v", filePath, err)
		return err
	}

	defer fd.Close()

	for mName := range ms.mNamesMap {
		if _, err = fd.Write(toputils.Uint16ToBytesLittleEndian(uint16(len(mName)))); err != nil {
			log.Errorf("FlushMetricNames: failed to write metric length for metric=%+v, filename=%v: err=%v", mName, filePath, err)
			return err
		}

		if _, err = fd.Write([]byte(mName)); err != nil {
			log.Errorf("FlushMetricNames: failed to write metric name=%+v, filename=%v: err=%v", mName, filePath, err)
			return err
		}
	}

	err = fd.Sync()
	if err != nil {
		log.Errorf("FlushMetricNames: failed to sync filename=%v: err=%v", filePath, err)
		return err
	}

	return nil
}

func (ms *MetricsSegment) updateTimeRange(ts uint32) {
	if ts > ms.highTS {
		atomic.StoreUint32(&ms.highTS, ts)
	}
	if ts < ms.lowTS {
		atomic.StoreUint32(&ms.lowTS, ts)
	}
}

func (ms *MetricsSegment) getMetaEntry(finalDir string, suffix uint64) *structs.MetricsMeta {
	tKeys := make(map[string]bool)
	allTrees := GetTagsTreeHolder(ms.Orgid, ms.Mid).allTrees
	for k := range allTrees {
		tKeys[k] = true
	}
	tagstreeBase := GetTagsTreeHolder(ms.Orgid, ms.Mid).tagstreeBase

	return &structs.MetricsMeta{
		TTreeDir:           tagstreeBase,
		MSegmentDir:        fmt.Sprintf("%s%d", finalDir, suffix),
		NumBlocks:          ms.currBlockNum,
		BytesReceivedCount: ms.bytesReceived,
		OnDiskBytes:        ms.mSegEncodedSize,
		TagKeys:            tKeys,
		EarliestEpochSec:   ms.lowTS,
		LatestEpochSec:     ms.highTS,
		DatapointCount:     ms.datapointCount,
		OrgId:              ms.Orgid,
	}
}

func ForceFlushMetricsBlock() {
	wg := sync.WaitGroup{}
	for _, mSegment := range GetAllMetricsSegments() {
		if mSegment.mSegEncodedSize == 0 {
			continue
		}
		wg.Add(1)
		go func(mSeg *MetricsSegment) {
			defer wg.Done()
			mSeg.rwLock.Lock()
			err := mSeg.CheckAndRotate(true)
			if err != nil {
				log.Errorf("ForceFlushMetricsBlock: Failed to rotate metrics segment %+v on shutdown, err=%v", mSeg, err)
			}
			mSeg.rwLock.Unlock()
		}(mSegment)
	}
	wg.Wait()
	for _, ttholder := range GetAllTagsTreeHolders() {
		wg.Add(1)
		go func(tth *TagsTreeHolder) {
			defer wg.Done()
			for tagKey, tt := range tth.allTrees {
				err := tt.flushSingleTagsTree(tagKey, tth.tagstreeBase)
				if err != nil {
					log.Errorf("timeBasedTagsTreeFlush: Error rotating tags tree %v from holder at %v. err=%v", tt.name, tth.tagstreeBase, err)
				}
			}
		}(ttholder)
	}
	wg.Wait()
}

func GetUnrotatedMetricsSegmentRequests(tRange *dtu.MetricsTimeRange, querySummary *summary.QuerySummary, orgid int64) (map[string][]*structs.MetricsSearchRequest, error) {
	sTime := time.Now()
	retVal := make(map[string][]*structs.MetricsSearchRequest)
	retLock := &sync.Mutex{}
	wg := &sync.WaitGroup{}

	parallelism := int(config.GetParallelism())
	allMetricsSegments := GetMetricSegments(orgid)
	idxCtr := 0
	for _, metricSeg := range allMetricsSegments {
		wg.Add(1)
		go func(mSeg *MetricsSegment) {
			defer wg.Done()
			mSeg.rwLock.RLock()
			defer mSeg.rwLock.RUnlock()
			if !tRange.CheckRangeOverLap(mSeg.lowTS, mSeg.highTS) || mSeg.Orgid != orgid {
				return
			}
			retBlocks := make(map[uint16]bool)
			blockSummaryFile := mSeg.metricsKeyBase + fmt.Sprintf("%d", mSeg.Suffix) + ".mbsu"
			blockSummaries, err := microreader.ReadMetricsBlockSummaries(blockSummaryFile)
			if err != nil {
				// Regardless of the error, we continue execution as we need to consider the unrotated block for this segment.
				if errors.Is(err, os.ErrNotExist) {
					log.Warnf("GetUnrotatedMetricsSegmentRequests: Block summary file not found at %v", blockSummaryFile)
				} else {
					log.Errorf("GetUnrotatedMetricsSegmentRequests: Error reading block summary file at %v. Error=%v", blockSummaryFile, err)
				}
			}

			for _, bSum := range blockSummaries {
				if tRange.CheckRangeOverLap(bSum.LowTs, bSum.HighTs) {
					retBlocks[bSum.Blknum] = true
				}
			}

			tKeys := make(map[string]bool)
			allTrees := GetTagsTreeHolder(orgid, mSeg.Mid).allTrees
			for k := range allTrees {
				tKeys[k] = true
			}

			finalReq := &structs.MetricsSearchRequest{
				Mid:                  mSeg.Mid,
				UnrotatedBlkToSearch: make(map[uint16]bool),
				MetricsKeyBaseDir:    mSeg.metricsKeyBase + fmt.Sprintf("%d", mSeg.Suffix),
				BlocksToSearch:       retBlocks,
				BlkWorkerParallelism: uint(2),
				QueryType:            structs.UNROTATED_METRICS_SEARCH,
				AllTagKeys:           tKeys,
				UnrotatedMetricNames: mSeg.mNamesMap,
			}

			// Check if the current unrotated block is within the time range
			if tRange.CheckRangeOverLap(mSeg.mBlock.mBlockSummary.LowTs, mSeg.mBlock.mBlockSummary.HighTs) {
				finalReq.UnrotatedBlkToSearch[mSeg.mBlock.mBlockSummary.Blknum] = true
			}

			if len(retBlocks) == 0 && len(finalReq.UnrotatedBlkToSearch) == 0 {
				return
			}

			tt := GetTagsTreeHolder(orgid, mSeg.Mid)
			if tt == nil {
				return
			}
			baseTTDir := tt.tagstreeBase
			retLock.Lock()
			_, ok := retVal[baseTTDir]
			if !ok {
				retVal[baseTTDir] = make([]*structs.MetricsSearchRequest, 0)
			}
			retVal[baseTTDir] = append(retVal[baseTTDir], finalReq)
			retLock.Unlock()
		}(metricSeg)
		if idxCtr%parallelism == 0 {
			wg.Wait()
		}
		idxCtr++
	}
	wg.Wait()
	timeElapsed := time.Since(sTime)
	querySummary.UpdateTimeGettingUnrotatedSearchRequests(timeElapsed)
	return retVal, nil
}

func GetUnrotatedMetricSegmentsOverTheTimeRange(tRange *dtu.MetricsTimeRange, orgid int64) ([]*MetricsSegment, error) {
	allMetricsSegments := GetMetricSegments(orgid)
	resultMetricSegments := make([]*MetricsSegment, 0)

	for _, metricSeg := range allMetricsSegments {
		if !tRange.CheckRangeOverLap(metricSeg.lowTS, metricSeg.highTS) || metricSeg.Orgid != orgid {
			continue
		}
		resultMetricSegments = append(resultMetricSegments, metricSeg)
	}

	return resultMetricSegments, nil
}

func GetUniqueTagKeysForUnrotated(tRange *dtu.MetricsTimeRange, myid int64) (map[string]struct{}, error) {
	unrotatedMetricSegments, err := GetUnrotatedMetricSegmentsOverTheTimeRange(tRange, myid)
	if err != nil {
		log.Errorf("GetUniqueTagKeysForUnrotated: failed to get unrotated metric segments for time range=%v, myid=%v, err=%v", tRange, myid, err)
		return nil, err
	}

	uniqueTagKeys := make(map[string]struct{})

	// Iterate over the segments and extract unique tag keys
	for _, segment := range unrotatedMetricSegments {
		tagsTreeHolder := GetTagsTreeHolder(myid, segment.Mid)
		if tagsTreeHolder != nil {
			for k := range tagsTreeHolder.allTrees {
				uniqueTagKeys[k] = struct{}{}
			}
		}
	}

	return uniqueTagKeys, nil
}

func GetMetricsEncodedSizeInfo() *MetricsEncodedSizeInfo {
	return &MetricsEncodedSizeInfo{
		TotalAllMSegmentsEncodedSizeInBytes: totalAllMSegmentsEncodedSizeInBytes,
		TotalMSegBlocksEncodedSizeInBytes:   totalMSegBlocksEncodedSizeInBytes,
		TotalTagsTreeSizeInBytes:            totalTagsTreeSizeInBytes,
		TotalTagTreesCount:                  totalTagTreesCount,
		TotalLeafNodesCount:                 totalLeafNodesCount,
		TotalSeriesCount:                    totalSeriesCount,
		TotalSortedTSIDCount:                totalSortedTSIDCount,
		TotalTSIDLookupCount:                totalTSIDLookupCount,
	}
}

func GetTotalEncodedSize() uint64 {
	allMsegEncodedSize := uint64(0)
	totalMSegsBlkEncodedSize := uint64(0)
	totalTagsTreeSize := uint64(0)
	totalLeafNodes := 0
	totaltsidlookup := 0
	totalSortedTSID := 0
	totalTagTrees := 0
	totalSeries := 0
	allMetricSegs := GetAllMetricsSegments()

	for _, mSeg := range allMetricSegs {
		if mSeg == nil {
			continue
		}
		mBuf := mSeg.mBlock
		totalSortedTSID += len(mBuf.sortedTsids)
		totaltsidlookup += len(mBuf.tsidLookup)
		totalSeries += len(mBuf.allSeries)
		numSeries := len(mBuf.allSeries)
		allMsegEncodedSize += mSeg.mSegEncodedSize
		totalMSegsBlkEncodedSize += mBuf.blkEncodedSize
		tt := GetTagsTreeHolder(mSeg.Orgid, mSeg.Mid)
		if tt == nil {
			continue
		}
		totalTagTrees += len(tt.allTrees)
		for _, tagVal := range tt.allTrees {
			totalLeafNodes += tagVal.numLeafNodes
			size := uint64((tagVal.numMetrics * 8) + (tagVal.numLeafNodes * 8) + (numSeries * 8))
			totalTagsTreeSize += size
		}
	}

	totalAllMSegmentsEncodedSizeInBytes = allMsegEncodedSize
	totalMSegBlocksEncodedSizeInBytes = totalMSegsBlkEncodedSize
	totalTagsTreeSizeInBytes = totalTagsTreeSize
	totalTagTreesCount = totalTagTrees
	totalLeafNodesCount = totalLeafNodes
	totalSeriesCount = totalSeries
	totalSortedTSIDCount = totalSortedTSID
	totalTSIDLookupCount = totaltsidlookup

	return totalMSegsBlkEncodedSize + totalTagsTreeSize
}

func GetMetricSegments(orgid int64) []*MetricsSegment {
	orgMetricsAndTagsLock.RLock()
	allMetricsSegments := []*MetricsSegment{}
	if metricsAndTags, ok := OrgMetricsAndTags[orgid]; ok {
		for _, mSeg := range metricsAndTags.MetricSegments {
			allMetricsSegments = append(allMetricsSegments, mSeg)
		}
	}
	orgMetricsAndTagsLock.RUnlock()
	return allMetricsSegments
}

func GetAllMetricsSegments() []*MetricsSegment {
	orgMetricsAndTagsLock.RLock()
	allMetricsSegments := []*MetricsSegment{}
	for _, metricsAndTags := range OrgMetricsAndTags {
		for _, mSeg := range metricsAndTags.MetricSegments {
			allMetricsSegments = append(allMetricsSegments, mSeg)
		}
	}
	orgMetricsAndTagsLock.RUnlock()
	return allMetricsSegments
}

func GetAllTagsTreeHolders() []*TagsTreeHolder {
	tagsTreeHolders := []*TagsTreeHolder{}
	orgMetricsAndTagsLock.RLock()
	for _, metricsAndTags := range OrgMetricsAndTags {
		for _, tagHolder := range metricsAndTags.TagHolders {
			tagsTreeHolders = append(tagsTreeHolders, tagHolder)
		}
	}
	orgMetricsAndTagsLock.RUnlock()
	return tagsTreeHolders
}

func GetTagsTreeHolder(orgid int64, mid string) *TagsTreeHolder {
	orgMetricsAndTagsLock.RLock()
	defer orgMetricsAndTagsLock.RUnlock()
	var tt *TagsTreeHolder
	if metricsAndTags, ok := OrgMetricsAndTags[orgid]; ok {
		tt, ok = metricsAndTags.TagHolders[mid]
		if !ok {
			return nil
		}
	} else {
		return nil
	}
	return tt
}

func CountUnrotatedTSIDsForTagKeys(tRange *dtu.MetricsTimeRange, myid int64,
	seriesCardMap map[string]*toputils.GobbableHll) error {

	unrotatedMetricSegments, err := GetUnrotatedMetricSegmentsOverTheTimeRange(tRange, myid)
	if err != nil {
		log.Errorf("CountUnrotatedTSIDsForTagKeys: failed to get unrotated metric segments for time range=%v, myid=%v, err=%v", tRange, myid, err)
		return err
	}

	for _, segment := range unrotatedMetricSegments {
		tagsTreeHolder := GetTagsTreeHolder(myid, segment.Mid)
		if tagsTreeHolder != nil {
			for tagkey, tkTree := range tagsTreeHolder.allTrees {
				tsidCard, ok := seriesCardMap[tagkey]
				if !ok || tsidCard == nil {
					tsidCard = structs.CreateNewHll()
					seriesCardMap[tagkey] = tsidCard
				}
				tkTree.countTSIDsForTagkey(tsidCard)
			}
		}
	}
	return nil
}

func CountUnrotatedTSIDsForTagPairs(tRange *dtu.MetricsTimeRange, myid int64,
	tagPairsCardMap map[string]map[string]*toputils.GobbableHll) error {

	unrotatedMetricSegments, err := GetUnrotatedMetricSegmentsOverTheTimeRange(tRange, myid)
	if err != nil {
		log.Errorf("CountUnrotatedTSIDsForTagKeys: failed to get unrotated metric segments for time range=%v, myid=%v, err=%v", tRange, myid, err)
		return err
	}

	for _, segment := range unrotatedMetricSegments {
		tagsTreeHolder := GetTagsTreeHolder(myid, segment.Mid)
		if tagsTreeHolder != nil {
			for tagkey, tkTree := range tagsTreeHolder.allTrees {
				valuesSet, ok := tagPairsCardMap[tagkey]
				if !ok || valuesSet == nil {
					valuesSet = make(map[string]*toputils.GobbableHll)
					tagPairsCardMap[tagkey] = valuesSet
				}
				tkTree.countTSIDsForTagPairs(valuesSet)
			}
		}
	}
	return nil
}

func GetUnrotatedTagPairs(tRange *dtu.MetricsTimeRange,
	myid int64, tagPairsMap map[string]map[string]struct{}) error {

	unrotatedMetricSegments, err := GetUnrotatedMetricSegmentsOverTheTimeRange(tRange, myid)
	if err != nil {
		log.Errorf("GetUnrotatedTagPairs: failed to get unrotated metric segments for time range=%v, myid=%v, err=%v", tRange, myid, err)
		return err
	}

	for _, segment := range unrotatedMetricSegments {
		tagsTreeHolder := GetTagsTreeHolder(myid, segment.Mid)
		if tagsTreeHolder == nil {
			continue
		}
		for tagkey, tkTree := range tagsTreeHolder.allTrees {
			valuesSet, ok := tagPairsMap[tagkey]
			if !ok || valuesSet == nil {
				valuesSet = make(map[string]struct{})
				tagPairsMap[tagkey] = valuesSet
			}

			for _, allTi := range tkTree.rawValues {
				for _, ti := range allTi {
					tv := string(ti.tagValue)
					valuesSet[tv] = struct{}{}
				}
			}
		}
	}
	return nil
}

func FindTagValuesUnrotated(tRange *dtu.MetricsTimeRange, mQuery *structs.MetricsQuery,
	resTagValues map[string]map[string]struct{}) error {

	unrotatedMetricSegments, err := GetUnrotatedMetricSegmentsOverTheTimeRange(tRange, mQuery.OrgId)
	if err != nil {
		log.Errorf("FindTagValuesUnrotated: failed to get unrotated metric segments for time range=%v, myid=%v, err=%v", tRange, mQuery.OrgId, err)
		return err
	}

	for _, segment := range unrotatedMetricSegments {
		tagsTreeHolder := GetTagsTreeHolder(mQuery.OrgId, segment.Mid)
		if tagsTreeHolder == nil {
			continue
		}
		for _, tf := range mQuery.TagsFilters {
			tkTree, ok := tagsTreeHolder.allTrees[tf.TagKey]
			if !ok {
				continue
			}

			valuesSet, ok := resTagValues[tf.TagKey]
			if !ok || valuesSet == nil {
				valuesSet = make(map[string]struct{})
				resTagValues[tf.TagKey] = valuesSet
			}

			for _, allTi := range tkTree.rawValues {
				for _, ti := range allTi {
					tv := string(ti.tagValue)
					valuesSet[tv] = struct{}{}
				}
			}
		}
	}

	return nil
}

func (mb *MetricsBlock) appendToWALBuffer(timestamp uint32, dp float64, tsid uint64) error {
	mb.dpWalState.lock.Lock()
	defer mb.dpWalState.lock.Unlock()

	if int(mb.dpWalState.dpIdx) >= utils.WAL_BLOCK_FLUSH_SIZE {
		err := mb.dpWalState.currentWal.AppendDataPoints(mb.dpWalState.dpsInWalMem[0:mb.dpWalState.dpIdx])
		if err != nil {
			log.Errorf("AppendWalDataPoint : Failed to append datapoints to WAL: %v", err)
			return err
		}
		_, _, totalEncodedSize := mb.dpWalState.currentWal.GetWALStats()
		if totalEncodedSize > utils.MAX_WAL_FILE_SIZE_BYTES {
			if err := mb.rotateWAL(); err != nil {
				log.Errorf("appendToWALBuffer : Failed to rotate WAL file: %v", err)
				return err
			}
		}
		mb.dpWalState.dpIdx = 0
	}

	mb.dpWalState.dpsInWalMem[mb.dpWalState.dpIdx].Timestamp = timestamp
	mb.dpWalState.dpsInWalMem[mb.dpWalState.dpIdx].DpVal = dp
	mb.dpWalState.dpsInWalMem[mb.dpWalState.dpIdx].Tsid = tsid
	mb.dpWalState.dpIdx++

	return nil
}

func timeBasedWalDPSFlush() {
	for {
		time.Sleep(WAL_DPS_FLUSH_SLEEP_DURATION * time.Second)
		for _, ms := range GetAllMetricsSegments() {
			ms.mBlock.dpWalState.lock.Lock()
			if ms.mBlock.dpWalState.dpIdx > 0 {
				err := ms.mBlock.dpWalState.currentWal.AppendDataPoints(ms.mBlock.dpWalState.dpsInWalMem[0:ms.mBlock.dpWalState.dpIdx])
				if err != nil {
					log.Warnf("timeBasedWalDPSFlush : Failed to append datapoints to WAL: %v", err)
				}
				_, _, totalEncodedSize := ms.mBlock.dpWalState.currentWal.GetWALStats()
				if totalEncodedSize > utils.MAX_WAL_FILE_SIZE_BYTES {
					if err := ms.mBlock.rotateWAL(); err != nil {
						log.Warnf("timeBasedWalDPSFlush : Failed to rotate WAL file: %v", err)
					}
				}

				ms.mBlock.dpWalState.dpIdx = 0
			}
			ms.mBlock.dpWalState.lock.Unlock()
		}
	}
}

func (mb *MetricsBlock) rotateWAL() error {
	mb.dpWalState.currentWALIndex++
	return mb.initNewDpWal()
}

func (mb *MetricsBlock) initNewDpWal() error {
	if mb.dpWalState.currentWal != nil {
		err := mb.dpWalState.currentWal.Close()
		if err != nil {
			log.Warnf("initNewDpWal : Failed to close current WAL: %v", err)
		}
	}

	basedir := getWALBaseDir()
	fileName := "shardID_" + mb.dpWalState.mId + "_segID_" + strconv.FormatUint(mb.dpWalState.segID, 10) + "_blockID_" + strconv.FormatUint(uint64(mb.mBlockSummary.Blknum), 10) + "_" + strconv.FormatUint(mb.dpWalState.currentWALIndex, 10) + ".wal"
	filePath := filepath.Join(basedir, fileName)
	var err error
	mb.dpWalState.currentWal, err = wal.NewDataPointWal(filePath)
	if err != nil {
		log.Errorf("initNewDpWal : Failed to create new WAL file %s in %s: %v", fileName, basedir, err)
		return err
	}
	mb.dpWalState.allWALs = append(mb.dpWalState.allWALs, mb.dpWalState.currentWal)
	return nil
}

func getWALBaseDir() string {
	var sb strings.Builder
	sb.WriteString(config.GetDataPath())
	sb.WriteString(config.GetHostID())
	sb.WriteString("/wal-ts/")
	return sb.String()
}

func (mb *MetricsBlock) deleteDpWalFiles() {
	for _, walFd := range mb.dpWalState.allWALs {
		if walFd != nil {
			err := walFd.DeleteWAL()
			if err != nil {
				log.Errorf("deleteDpWalFiles : Failed to delete WAL file: %v", err)
				return
			}
		}
	}
}

/*
Write-Ahead Logging (WAL) for Metrics Names
*/

func (mb *MetricsSegment) initNewMNameWAL() error {
	if mb.mNameWalState.wal != nil {
		err := mb.mNameWalState.wal.Close()
		if err != nil {
			log.Warnf("initNewMNameWAL : Failed to close current WAL: %v", err)
		}
	}

	basedir := getWALBaseDir()
	filePath := filepath.Join(basedir, METRICS_NAME_WAL_DIR)
	fileName := "shardID_" + mb.Mid + "_segID_" + strconv.FormatUint(mb.Suffix, 10) + "_.wal"
	filePath = filepath.Join(filePath, fileName)

	var err error
	mb.mNameWalState.wal, err = wal.NewMNameWal(filePath)
	if err != nil {
		log.Errorf("initNewMNameWAL : Failed to create new WAL file %s in %s: %v", fileName, basedir, err)
		return err
	}
	return nil
}

func timeBasedMNameWalFlush() {
	for {
		time.Sleep(METRICS_NAME_WAL_FLUSH_SLEEP_DURATION * time.Second)
		for _, ms := range GetAllMetricsSegments() {
			if len(ms.mNameWalState.metricsName) > 0 {
				err := ms.mNameWalState.wal.AppendMNames(ms.mNameWalState.metricsName)
				if err != nil {
					log.Warnf("timeBasedMNameWalFlush : Failed to append datapoints to WAL: %v", err)
					continue
				}
				ms.mNameWalState.metricsName = ms.mNameWalState.metricsName[:0]
			}
		}
	}
}

func (ms *MetricsSegment) cleanAndInitNewMNameWal(forceRotate bool) error {
	ms.deleteMNameWALFile()
	ms.mNameWalState.metricsName = ms.mNameWalState.metricsName[:0]
	if !forceRotate {
		err := ms.initNewMNameWAL()
		if err != nil {
			log.Errorf("cleanAndInitNewMNameWal : Failed to initialize new WAL: %v", err)
			return err
		}
	}
	return nil
}
func (ms *MetricsSegment) deleteMNameWALFile() {
	if ms.mNameWalState.wal != nil {
		err := ms.mNameWalState.wal.DeleteWAL()
		if err != nil {
			log.Errorf("deleteMNameWALFile : Failed to delete WAL file: %v", err)
			return
		}
	}
}

type mNameWalFilesInfo struct {
	mId      uint64
	segID    uint64
	walFiles []string
}

func extractMNameWALFileInfo(baseDir string) (map[string]*mNameWalFilesInfo, error) {
	files, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}

	filesInfo := make(map[string]*mNameWalFilesInfo)

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fileName := file.Name()

		// Expected WAL filename format: "shardId_<shard>_segId_<segID>_.wal"
		parts := strings.Split(fileName, "_")
		if len(parts) < 4 {
			continue
		}

		mIdStr := parts[1]
		segIDStr := parts[3]

		segID, err1 := strconv.ParseUint(segIDStr, 10, 64)
		mId, err2 := strconv.ParseUint(mIdStr, 10, 64)

		if err1 != nil || err2 != nil {
			continue
		}

		key := mIdStr + "_" + segIDStr
		if _, exists := filesInfo[key]; !exists {
			filesInfo[key] = &mNameWalFilesInfo{
				mId:      mId,
				segID:    segID,
				walFiles: []string{},
			}
		}
		filesInfo[key].walFiles = append(filesInfo[key].walFiles, fileName)
	}
	return filesInfo, nil
}

func initSegment(suffix uint64, mId string) *MetricsSegment {
	mKey, err := getBaseMetricsKey(suffix, mId)
	if err != nil {
		log.Errorf("InitMetricsSegment: Failed to get metrics key for suffix %v and mid %v, err=%v", suffix, mId, err)
	}
	return &MetricsSegment{
		mNamesMap:      make(map[string]bool, 0),
		metricsKeyBase: mKey,
		Suffix:         suffix,
	}
}

func RecoverMNameWALData() error {
	baseDir := getWALBaseDir()
	mNameWalDir := filepath.Join(baseDir, METRICS_NAME_WAL_DIR)
	walFilesData, err := extractMNameWALFileInfo(mNameWalDir)
	if err != nil {
		return err
	}

	for _, fileData := range walFilesData {
		ms := initSegment(fileData.segID, strconv.FormatUint(fileData.mId, 10))
		isWalFileEmpty := true
		for _, walFileName := range fileData.walFiles {

			filePath := filepath.Join(mNameWalDir, walFileName)
			walIterator, err := wal.NewMNameWalReader(filePath)
			if err != nil {
				log.Warnf("RecoverMNameWALData :Failed to create WAL reader for file %s: %v", walFileName, err)
				continue
			}
			for {
				mName, err := walIterator.Next()
				if err != nil {
					log.Warnf("RecoverMNameWALData : Error reading next WAL entry from file %s: %v", walFileName, err)
					break
				}
				if mName == nil {

					break
				}

				mNameStr := ""
				mNameStr = *mName
				ms.mNamesMap[mNameStr] = true
				isWalFileEmpty = false
			}
			_ = walIterator.Close()
			err = deleteWalFile(mNameWalDir, walFileName)
			if err != nil {
				log.Warnf("RecoverMNameWALData : Failed to delete wal file %s: %v", walFileName, err)
			}
		}

		if !isWalFileEmpty {
			err := ms.FlushMetricNames()
			if err != nil {
				log.Warnf("RecoverMNameWALData :Failed to flush Metrics Name for shardID=%d, segID=%d,: %v",
					fileData.mId, fileData.segID, err)
				return err
			}
		}

	}
	return nil
}
