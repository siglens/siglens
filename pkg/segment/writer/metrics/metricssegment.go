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

package metrics

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"io"

	"github.com/bits-and-blooms/bloom/v3"
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
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var otsdb_mname = []byte("metric")
var otsdb_timestamp = []byte("timestamp")
var otsdb_value = []byte("value")
var otsdb_tags = []byte("tags")

var influx_value = "value"

var tags_separator = []byte("__")

var TAGS_TREE_FLUSH_SLEEP_DURATION = 60 // 1 min

const METRICS_BLK_FLUSH_SLEEP_DURATION = 60 // 1 min

const METRICS_BLK_ROTATE_SLEEP_DURATION = 10 // 10 seconds

/*
A metrics segment represents a 2hr window and consists of many metrics blocks and tagTrees.

Only a single metrics buffer per metrics segment can be in memory at a time. Prior metrics buffers will be flushed to disk.

The tagsTree will be shared across metrics this metrics segment.

A metrics segment generate the following set of files:
  - A tagTree file for each incoming tagKey seen across this segment
  - A metricsBlock file for each incoming 15minute window
  - A bloomfilter for all metric names in the metrics segment

TODO: this metrics segment should reject samples not in 2hr window
*/
type MetricsSegment struct {
	metricsKeyBase   string             // base string of this metric segment's key
	Suffix           uint64             // current suffix
	Mid              string             // metrics id for this metric segment
	highTS           uint32             // highest epoch timestamp seen across this segment
	lowTS            uint32             // lowest epoch timestamp seen across this segment
	mBlock           *MetricsBlock      // current in memory block
	currBlockNum     uint16             // current block number
	mNames           *bloom.BloomFilter // all metric names across segment
	totalEncodedSize uint64             // total size of all metric blocks. TODO: this should include tagsTree & mNames blooms
	bytesReceived    uint64             // total size of incoming data
	rwLock           *sync.RWMutex      // read write lock for access
	datapointCount   uint64             // total number of datapoints across all series in the block
	Orgid            uint64
}

/*
A metrics buffer represent a 15 minute (or 1GB size) window of encoded series

# A metrics buffer's suffix determines the path of the generated files in relation to the metricssegment

Every 5s, this metrics buffer should persist to disk and will create / update two file:
 1. Raw TS encoded file. Format [tsid][packed-len][raw-values]
 2. TSID offset file. Format [tsid][soff]
*/
type MetricsBlock struct {
	tsidLookup    map[uint64]int
	allSeries     []*TimeSeries
	sortedTsids   []uint64
	mBlockSummary *structs.MBlockSummary
	encodedSize   uint64 // total encoded size of the block
}

// Represents a single timeseries
type TimeSeries struct {
	// TODO: what is stored here, how is it flushed?
	lock        *sync.Mutex
	rawEncoding *bytes.Buffer

	nEntries   int          // number of ts/dp combinations in this series
	cFinishFn  func() error // function to call at end of compression, to write the final bytes for the encoded timestamps
	compressor *compress.Compressor
}

var orgMetricsAndTagsLock *sync.RWMutex = &sync.RWMutex{}

type MetricsAndTagsHolder struct {
	Metrics    map[string]*MetricsSegment
	TagHolders map[string]*TagsTreeHolder
}

var numMetricsSegments uint64

var OrgMetricsAndTags map[uint64]*MetricsAndTagsHolder = make(map[uint64]*MetricsAndTagsHolder)

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
}

func initOrgMetrics(orgid uint64) error {
	orgMetricsAndTagsLock.Lock()
	if _, ok := OrgMetricsAndTags[orgid]; !ok {
		OrgMetricsAndTags[orgid] = &MetricsAndTagsHolder{
			Metrics:    map[string]*MetricsSegment{},
			TagHolders: map[string]*TagsTreeHolder{},
		}
	}
	orgMetricsAndTagsLock.Unlock()
	log.Infof("initOrgMetrics: Initialising metrics segments and tags trees for orgid %v", orgid)

	availableMem := memory.GetAvailableMetricsIngestMemory()
	numMetricsSegments = getNumberOfSegmentsFromMemory(availableMem)
	if numMetricsSegments == 0 {
		log.Error("initOrgMetrics: Not enough memory to initialize metrics segments")
		return errors.New("not enough memory to initialize metrics segments")
	}

	for i := uint64(0); i < numMetricsSegments; i++ {
		mSeg, err := InitMetricsSegment(orgid, fmt.Sprintf("%d", i))
		if err != nil {
			log.Errorf("Initialising metrics segment failed for org: %v, err: %v", orgid, err)
			return err
		}

		orgMetricsAndTagsLock.Lock()
		OrgMetricsAndTags[orgid].Metrics[fmt.Sprint(i)] = mSeg
		OrgMetricsAndTags[orgid].TagHolders[mSeg.Mid], err = InitTagsTreeHolder(mSeg.Mid)
		if err != nil {
			log.Errorf("Initialising tags tree holder failed for org: %v, err: %v", orgid, err)
			orgMetricsAndTagsLock.Unlock()
			return err
		}
		orgMetricsAndTagsLock.Unlock()
	}
	return nil
}

/*
Returns the total incoming bytes, total on disk bytes, approx number of datapoints across all metric segments
*/
func GetUnrotatedMetricStats(orgid uint64) (uint64, uint64, uint64) {
	totalIncoming := uint64(0)
	totalOnDisk := uint64(0)
	totalDPS := uint64(0)

	orgMetricsAndTagsLock.RLock()
	orgMetrics := map[string]*MetricsSegment{}
	if metricsAndTags, ok := OrgMetricsAndTags[orgid]; ok {
		orgMetrics = metricsAndTags.Metrics
	}
	orgMetricsAndTagsLock.RUnlock()

	for _, m := range orgMetrics {
		totalIncoming += m.bytesReceived
		totalOnDisk += m.totalEncodedSize
		totalDPS += m.datapointCount
	}
	return totalIncoming, totalOnDisk, totalDPS
}

func getNumberOfSegmentsFromMemory(mem uint64) uint64 {
	mb := utils.ConvertUintBytesToMB(mem)
	retVal := mem / utils.MAX_BYTES_METRICS_BLOCK
	concurreny := uint64(config.GetParallelism())
	if retVal == 0 {
		log.Infof("Less than %dMB was allocated. Defaulting to 1 metrics segment", utils.ConvertUintBytesToMB(mem))
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
			encSize := atomic.LoadUint64(&ms.mBlock.encodedSize)
			if encSize > utils.MAX_BYTES_METRICS_BLOCK {
				ms.rwLock.Lock()
				err := ms.CheckAndRotate(false)
				ms.rwLock.Unlock()
				if err != nil {
					log.Errorf("timeBasedRotateMetricsBlock: rotating block %d for metric segment %s due to time failed", ms.currBlockNum, ms.metricsKeyBase)
				}
			}
		}
	}
}

func timeBasedMetricsFlush() {
	for {
		time.Sleep(METRICS_BLK_FLUSH_SLEEP_DURATION * time.Second)
		for _, ms := range GetAllMetricsSegments() {
			encSize := atomic.LoadUint64(&ms.mBlock.encodedSize)
			if encSize > 0 {
				ms.rwLock.Lock()
				err := ms.mBlock.flushBlock(ms.metricsKeyBase, ms.Suffix, ms.currBlockNum)
				if err != nil {
					log.Errorf("timeBasedRotateMetricsBlock: flush block %d for metric segment %s due to time failed", ms.currBlockNum, ms.metricsKeyBase)
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
						log.Errorf("timeBasedTagsTreeFlush: Error rotating tags tree! Err %+v", err)
					}
				}
			}
		}
	}
}

func InitMetricsSegment(orgid uint64, mId string) (*MetricsSegment, error) {
	suffix, err := suffix.GetSuffix(mId, "ts")
	if err != nil {
		return nil, err
	}
	mKey, err := getBaseMetricsKey(suffix, mId)
	if err != nil {
		log.Errorf("Failed to init metrics segment! %+v", err)
		return nil, err
	}
	return &MetricsSegment{
		mNames:       bloom.NewWithEstimates(10, 0.001), // TODO: dynamic sizing
		currBlockNum: 0,
		mBlock: &MetricsBlock{
			tsidLookup:  make(map[uint64]int),
			allSeries:   make([]*TimeSeries, 0),
			sortedTsids: make([]uint64, 0),
			mBlockSummary: &structs.MBlockSummary{
				Blknum: 0,
				HighTs: 0,
				LowTs:  math.MaxInt32,
			},
			encodedSize: 0,
		},
		rwLock:           &sync.RWMutex{},
		metricsKeyBase:   mKey,
		Suffix:           suffix,
		Mid:              mId,
		totalEncodedSize: 0,
		highTS:           0,
		lowTS:            math.MaxUint32,
		Orgid:            orgid,
	}, nil
}

/*
Returns <<dataDir>>/<<hostname>>/active/ts/<<mid>>/{suffix}/suffix
*/
func getBaseMetricsKey(suffix uint64, mId string) (string, error) {
	var sb strings.Builder
	sb.WriteString(config.GetDataPath())
	sb.WriteString(config.GetHostID())
	sb.WriteString("/active/ts/")
	sb.WriteString(mId + "/")
	sb.WriteString(strconv.FormatUint(suffix, 10) + "/")
	basedir := sb.String()
	return basedir, nil
}

/*
Returns <<dataDir>>/<<hostname>>/final/<<mid>>/suffix
*/
func getFinalMetricsDir(mId string, suffix uint64) string {
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
func initTimeSeries(tsid uint64, dp float64, timestammp uint32) (*TimeSeries, uint64, error) {
	ts := &TimeSeries{lock: &sync.Mutex{}}
	ts.rawEncoding = new(bytes.Buffer)
	c, finish, err := compress.NewCompressor(ts.rawEncoding, timestammp)
	if err != nil {
		log.Errorf("error creating dod compressor! Error: %v", err)
		return nil, 0, err
	}
	ts.cFinishFn = finish
	ts.compressor = c
	writtenBytes, err := ts.compressor.Compress(timestammp, dp)
	if err != nil {
		return nil, 0, err
	}
	return ts, writtenBytes, nil
}

/*
For a given metricName, tags, dp, and timestamp, add it to the respective in memory series

Internally, this function will try to find the series then will encode it.
If it cannot find the series or no space exists in the metrics segment, it will return an error

Return number of bytes written and any error encountered
*/
func EncodeDatapoint(mName []byte, tags *TagsHolder, dp float64, timestamp uint32, nBytes uint64, orgid uint64) error {
	tsid, err := tags.GetTSID(mName)
	if err != nil {
		return err
	}
	mSeg, tth, err := getMetricsSegment(mName, orgid)
	if err != nil {
		log.Errorf("EncodeDatapoint: failed to get metrics segment for orgid %v: %v", orgid, err)
		return err
	}

	if mSeg == nil {
		log.Errorf("EncodeDatapoint: No segment remaining to be assigned to orgid=%v", orgid)
		return fmt.Errorf("no segment remaining to be assigned to orgid=%v", orgid)
	}

	if mSeg.mNames.Test(mName) {
		mSeg.mNames.Add(mName)
	}
	mSeg.Orgid = orgid
	var ts *TimeSeries
	var seriesExists bool
	mSeg.rwLock.RLock()
	ts, seriesExists, err = mSeg.mBlock.GetTimeSeries(tsid)
	if err != nil {
		mSeg.rwLock.RUnlock()
		log.Errorf("EncodeDatapoint: failed to get time series for TSID %s, %d. Err: %v", mName, tsid, err)
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
			log.Errorf("EncodeDatapoint: failed to create time series for TSID %s, %d. Err: %v", mName, tsid, err)
			return err
		}
		mSeg.rwLock.Lock()
		exists, idx, err := mSeg.mBlock.InsertTimeSeries(tsid, ts)
		if err != nil {
			mSeg.rwLock.Unlock()
			log.Errorf("EncodeDatapoint: failed to create time series for TSID %s, %d. Err: %v", mName, tsid, err)
			return err
		}
		if !exists { // if the new series was actually added, add the tsid to the block
			err = mSeg.mBlock.addTsidToBlock(tsid)
			if err != nil {
				mSeg.rwLock.Unlock()
				return err
			}
		}
		mSeg.rwLock.Unlock()
		if exists {
			bytesWritten, err = mSeg.mBlock.allSeries[idx].AddSingleEntry(dp, timestamp)
			if err != nil {
				return err
			}
		}
		err = tth.AddTagsForTSID(mName, tags, tsid)
		if err != nil {
			log.Errorf("getTimeSeries: failed to add tags for TSID %s, %d. Err: %v", mName, tsid, err)
			return err
		}
	} else {
		bytesWritten, err = ts.AddSingleEntry(dp, timestamp)
		if err != nil {
			return err
		}
	}

	mSeg.updateTimeRange(timestamp)
	mSeg.mBlock.mBlockSummary.UpdateTimeRange(timestamp)
	atomic.AddUint64(&mSeg.mBlock.encodedSize, bytesWritten)
	atomic.AddUint64(&mSeg.totalEncodedSize, bytesWritten)
	atomic.AddUint64(&mSeg.bytesReceived, nBytes)
	atomic.AddUint64(&mSeg.datapointCount, 1)

	return nil
}

/*
Caller is responsible for acquiring and releasing locks
*/
func (mb *MetricsBlock) addTsidToBlock(tsid uint64) error {

	l := len(mb.sortedTsids)
	if l == 0 {
		mb.sortedTsids = append(mb.sortedTsids, tsid)
		return nil
	}

	mb.sortedTsids = append(mb.sortedTsids, tsid)
	return nil
}

// for an input raw json []byte, return the metric name, datapoint value, timestamp, all tags, and any errors occurred
// The metric name is returned as a raw []byte
// The tags
func ExtractOTSDBPayload(rawJson []byte, tags *TagsHolder) ([]byte, float64, uint32, error) {
	var mName []byte
	var dpVal float64
	var ts uint32
	var err error

	handler := func(key []byte, value []byte, valueType jp.ValueType, off int) error {
		switch {
		case bytes.Equal(key, otsdb_mname):
			switch valueType {
			case jp.String:
				temp, err := jp.ParseString(value)
				if err != nil {
					log.Errorf("failed to extract tags %+v", err)
				}
				if temp != "target_info" {
					mName = value
				} else {
					return nil
				}
			}
		case bytes.Equal(key, otsdb_tags):
			if valueType != jp.Object {
				log.Errorf("tags key was not expected object type %+v, raw: %+v", valueType, string(value))
				return fmt.Errorf("tags is not expected type:%+v", valueType)
			}
			err = extractTagsFromJson(value, tags)
			if err != nil {
				log.Errorf("failed to extract tags %+v", err)
				return err
			}
		case bytes.Equal(key, otsdb_timestamp):
			switch valueType {
			case jp.Number:
				intVal, err := jp.ParseInt(value)
				if err != nil {
					fltVal, err := jp.ParseFloat(value)
					if err != nil {
						return fmt.Errorf("failed to parse timestamp! Not expected type:%+v", valueType.String())
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
			}
		case bytes.Equal(key, otsdb_value):
			if valueType != jp.Number {
				return fmt.Errorf("value is not a number")
			}
			fltVal, err := jp.ParseFloat(value)
			if err != nil {
				return fmt.Errorf("failed to convert value to float! %+v", err)
			}
			dpVal = fltVal
		default:
			return fmt.Errorf("unknown keyname %+s", key)
		}
		return nil
	}
	err = jp.ObjectEach(rawJson, handler)

	if err != nil {
		log.Errorf("ExtractOTSDBPayload payload %v failed to extract payload! %+v ", string(rawJson), err)
		return mName, dpVal, ts, err
	}
	if len(mName) > 0 && ts > 0 {
		return mName, dpVal, ts, nil
	} else {
		return nil, dpVal, 0, fmt.Errorf("failed to find all expected keys")
	}
}

// for an input raw csv row []byte, return the metric name, datapoint value, timestamp (ignored), all tags, and any errors occurred
// The metric name is returned as a raw []byte
// The tags
func ExtractInfluxPayload(rawCSV []byte, tags *TagsHolder) ([]byte, float64, uint32, error) {

	var mName []byte
	var dpVal float64
	var err error

	reader := csv.NewReader(bytes.NewBuffer(rawCSV))
	inserted_tags := ""

	for {
		record, err := reader.Read()
		if err != nil {
			// If there is an error, check if it's EOF
			if err == io.EOF {
				break // End of file
			}
			return nil, 0, 0, err

		} else {
			line := strings.Join(record, ",")
			whitespace_split := strings.Fields(line)
			tag_set := strings.Split(whitespace_split[0], ",")
			field_set := strings.Split(whitespace_split[1], ",")
			for index, value := range tag_set {
				if index == 0 {
					mName = []byte(value)
				} else {
					kvPair := strings.Split(value, "=")
					key := kvPair[0]
					value = kvPair[1]
					tags.Insert(key, []byte(value), jp.String)
					inserted_tags += key + "," + value + " "

				}
			}

			for _, value := range field_set {
				kvPair := strings.Split(value, "=")
				key := kvPair[0]
				value = kvPair[1]
				if key == influx_value {
					fltVal, err := strconv.ParseFloat(value, 64)
					if err != nil {
						return nil, 0, 0, fmt.Errorf("failed to convert value to float! %+v", err)
					}
					dpVal = fltVal
				}
			}

		}

	}

	return mName, dpVal, 0, err

}

// extracts raw []byte from the read tags objects and returns it as []*tagsHolder
// the returned []*tagsHolder is sorted by tagKey
func extractTagsFromJson(tagsObj []byte, tags *TagsHolder) error {

	handler := func(key []byte, value []byte, valueType jp.ValueType, off int) error {
		if key == nil {
			log.Errorf("missing key %+v %+v %+v", key, value, valueType)
			return nil
		}

		strKey, err := jp.ParseString(key)
		if err != nil {
			log.Errorf("key invalid str %+v %+v %+v", key, value, err)
			return err
		}
		tags.Insert(strKey, value, valueType)
		return nil
	}
	err := jp.ObjectEach(tagsObj, handler)
	if err != nil {
		return err
	}
	return nil
}

func getMetricsSegment(mName []byte, orgid uint64) (*MetricsSegment, *TagsTreeHolder, error) {
	orgMetricsAndTagsLock.RLock()
	metricsAndTagsHolder, ok := OrgMetricsAndTags[orgid]
	orgMetricsAndTagsLock.RUnlock()
	if !ok || len(metricsAndTagsHolder.Metrics) == 0 {
		err := initOrgMetrics(orgid)
		if err != nil {
			log.Errorf("getMetricsSegment: Failed to initialize metrics segments for org %v: %v", orgid, err)
			return nil, nil, err
		}
		orgMetricsAndTagsLock.RLock()
		metricsAndTagsHolder = OrgMetricsAndTags[orgid]
		orgMetricsAndTagsLock.RUnlock()
	}
	mid := fmt.Sprint(xxhash.Sum64(mName) % uint64(len(metricsAndTagsHolder.Metrics)))
	return metricsAndTagsHolder.Metrics[mid], metricsAndTagsHolder.TagHolders[mid], nil
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
			return nil, false, fmt.Errorf("5M limit reached")
		}
		return nil, false, nil
	} else {
		ts = mb.allSeries[idx]
	}
	return ts, ok, nil
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
			return false, 0, fmt.Errorf("5M limit reached")
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
			log.Errorf("error creating dod compressor! Error: %v", err)
			return writtenBytes, err
		}
		ts.cFinishFn = finish
		ts.compressor = c
		writtenBytes, err = ts.compressor.Compress(dpTS, dpVal)
		if err != nil {
			log.Errorf("error encoding timestamp! Error: %+v", err)
			return writtenBytes, err
		}
	} else {
		writtenBytes, err = ts.compressor.Compress(dpTS, dpVal)
		if err != nil {
			log.Errorf("error encoding timestamp! Error: %+v", err)
			return writtenBytes, err
		}
	}
	ts.nEntries++
	return writtenBytes, nil
}

/*
Wrapper function to check and rotate the current metrics block or the metrics segment

Caller is responsible for acquiring locks
*/
func (ms *MetricsSegment) CheckAndRotate(forceRotate bool) error {

	encSize := atomic.LoadUint64(&ms.mBlock.encodedSize)
	if encSize > utils.MAX_BYTES_METRICS_BLOCK || (encSize > 0 && forceRotate) {
		err := ms.mBlock.rotateBlock(ms.metricsKeyBase, ms.Suffix, ms.currBlockNum)
		if err != nil {
			log.Errorf("metrics.CheckAndRotate: failed to rotate block %v", err)
			return err
		}
		if !forceRotate {
			ms.currBlockNum++
		}
	}

	totalEncSize := atomic.LoadUint64(&ms.totalEncodedSize)
	if totalEncSize > utils.MAX_BYTES_METRICS_SEGMENT || (totalEncSize > 0 && forceRotate) {
		err := ms.rotateSegment(forceRotate)
		if err != nil {
			log.Errorf("CheckAndRotate: failed to rotate mid %v: %v", ms.metricsKeyBase, err)
			return err
		}
		tt := GetTagsTreeHolder(ms.Orgid, ms.Mid)
		tt.flushTagsTree()
		if forceRotate || time.Since(tt.createdTime) > time.Duration(24*time.Hour) {
			err = tt.rotateTagsTree(forceRotate)
			if err != nil {
				log.Errorf("CheckAndRotate: failed to rotate tags tree %v: %v", tt.tagstreeBase, err)
			}
		}
	}
	return nil
}

/*
Format of TSO file:
[version - 1 byte][number of tsids - 2 bytes][tsid - 8bytes][offset - 4 bytes][tsid - 8bytes]...
Formar of TSG file:
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

	_, err := tsoBuffer.Write(utils.VERSION_TSOFILE)
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
	_, err = tsoBuffer.Write(toputils.Uint16ToBytesLittleEndian(uint16(length)))
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
		log.Errorf("FlushTSOFile: Failed to write to TSO file %v", tsoFileName)
		return err
	}
	_, err = fdTsg.Write(tsgBuffer.Bytes())
	if err != nil {
		log.Errorf("FlushTSOFile: Failed to write to TSO file %v", tsoFileName)
		return err
	}
	return nil
}

func (mb *MetricsBlock) flushBlock(basePath string, suffix uint64, bufId uint16) error {
	finalPath := fmt.Sprintf("%s%d_%d", basePath, suffix, bufId)
	fName := fmt.Sprintf("%s%d.mbsu", basePath, suffix)
	_, err := mb.mBlockSummary.FlushSummary(fName)
	if err != nil {
		log.Errorf("Could not write metrics block summary for block at %s", finalPath)
		return err
	}
	err = mb.FlushTSOAndTSGFiles(finalPath)
	if err != nil {
		log.Errorf("Could not flush TSO and TSG files at %s", finalPath)
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
		log.Errorf("rotateBlock: Could not flush block at %v/%v/%v", basePath, suffix, bufId)
		return err
	}

	//erase map
	for k := range mb.tsidLookup {
		delete(mb.tsidLookup, k)
	}

	// we can't do mb.allSeries = mb.allSeries[:0], as the other entries of the slice won't be GC'ed
	newSeries := make([]*TimeSeries, 0, len(mb.allSeries))
	mb.allSeries = newSeries
	mb.encodedSize = 0
	mb.sortedTsids = make([]uint64, 0)
	mb.mBlockSummary.Blknum++
	mb.mBlockSummary.HighTs = 0
	mb.mBlockSummary.LowTs = math.MaxInt32
	return nil
}

/*
Flushes the metrics segment's tags tree, mNames bloom

This function assumes that the prior metricssBlock has alraedy been rotated/reset
TODO: flush bloom / tags tree / etc
*/
func (ms *MetricsSegment) rotateSegment(forceRotate bool) error {
	var err error
	finalDir := getFinalMetricsDir(ms.Mid, ms.Suffix)
	metaEntry := ms.getMetaEntry(finalDir, ms.Suffix)
	err = os.MkdirAll(path.Dir(path.Dir(finalDir)), 0764)
	if err != nil {
		log.Errorf("RotateSegment: failed to create directory %s to %s. Error %+v", ms.metricsKeyBase, finalDir, err)
		return err
	}

	// Check if final directory already exists
	if _, err := os.Stat(finalDir); err == nil {
		log.Infof("RotateSegment: final directory %s already exists, skipping rename operation", finalDir)
		return nil

	}

	// Check if source directory exists
	if _, err := os.Stat(ms.metricsKeyBase); os.IsNotExist(err) {
		log.Infof("RotateSegment: source directory %s does not exist, skipping rename operation", ms.metricsKeyBase)
		return nil
	}

	// Rename metricsKeyBase to finalDir
	err = os.Rename(path.Dir(ms.metricsKeyBase), finalDir)
	if err != nil {
		log.Errorf("RotateSegment: failed to rename %s to %s. Error %+v", ms.metricsKeyBase, finalDir, err)
		return err
	}
	log.Infof("rotating segment of size %v that created %v metrics blocks to %+v", ms.totalEncodedSize, ms.currBlockNum+1, finalDir)
	if !forceRotate {
		nextSuffix, err := suffix.GetSuffix(ms.Mid, "ts")
		if err != nil {
			log.Errorf("rotateSegment: failed to get the next suffix for m.id %s, err %+v", ms.Mid, err)
			return err
		}
		mKey, err := getBaseMetricsKey(nextSuffix, ms.Mid)
		if err != nil {
			log.Errorf("Failed to get next base key for %s: %v", ms.Mid, err)
			return err
		}
		ms.metricsKeyBase = mKey
		ms.Suffix = nextSuffix
		ms.highTS = 0
		ms.lowTS = math.MaxUint32
		ms.currBlockNum = 0
		ms.mNames = bloom.NewWithEstimates(10, 0.001) // TODO: dynamic sizing
		ms.totalEncodedSize = 0
		ms.datapointCount = 0
		ms.bytesReceived = 0
		ms.mBlock.mBlockSummary.Reset()
	}

	err = meta.AddMetricsMetaEntry(metaEntry)
	if err != nil {
		log.Errorf("RotateSegment: failed to add metrics meta entry! Error %+v", err)
		return err
	}

	return blob.UploadIngestNodeDir()
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
		OnDiskBytes:        ms.totalEncodedSize,
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
		if mSegment.totalEncodedSize == 0 {
			continue
		}
		wg.Add(1)
		go func(mSeg *MetricsSegment) {
			defer wg.Done()
			mSeg.rwLock.Lock()
			err := mSeg.CheckAndRotate(true)
			if err != nil {
				log.Errorf("Failed to rotate metrics segment %v on shutdown", err)
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
					log.Errorf("timeBasedTagsTreeFlush: Error rotating tags tree! Err %+v", err)
				}
			}
		}(ttholder)
	}
	wg.Wait()
}

func GetUnrotatedMetricsSegmentRequests(metricName string, tRange *dtu.MetricsTimeRange, querySummary *summary.QuerySummary, orgid uint64) (map[string][]*structs.MetricsSearchRequest, error) {
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
			if !tRange.CheckRangeOverLap(mSeg.lowTS, mSeg.highTS) || metricSeg.Orgid != orgid {
				return
			}
			retBlocks := make(map[uint16]bool)
			blockSummaryFile := mSeg.metricsKeyBase + fmt.Sprintf("%d", mSeg.Suffix) + ".mbsu"
			blockSummaries, err := microreader.ReadMetricsBlockSummaries(blockSummaryFile)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return
				}
				log.Errorf("GetUnrotatedMetricsSegmentRequests: Error reading block summary file at %v", blockSummaryFile)
				return
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
			if len(retBlocks) == 0 {
				return
			}
			finalReq := &structs.MetricsSearchRequest{
				MetricsKeyBaseDir: mSeg.metricsKeyBase + fmt.Sprintf("%d", mSeg.Suffix),
				BlocksToSearch:    retBlocks,
				Parallelism:       uint(config.GetParallelism()),
				QueryType:         structs.UNROTATED_METRICS_SEARCH,
				AllTagKeys:        tKeys,
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

func GetTotalEncodedSize() uint64 {
	totalSize := uint64(0)
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
		totalSize += mSeg.totalEncodedSize
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

	if config.IsDebugMode() {
		log.Errorf("------------------------------------------")
		log.Errorf("There are %d tagTrees. Total leaf nodes:%d. Estimated size: %.4fMB", totalTagTrees, totalLeafNodes,
			utils.ConvertFloatBytesToMB(float64(totalTagsTreeSize)))
		log.Errorf("There are %d series in the buffer. %d TSIDs. %d entries in reverse index. Encoded size: %.4fMB", totalSeries,
			totalSortedTSID, totaltsidlookup, utils.ConvertFloatBytesToMB(float64(totalSize)))
		log.Errorf("------------------------------------------")
	}

	return totalSize + totalTagsTreeSize
}

func GetMetricSegments(orgid uint64) []*MetricsSegment {
	orgMetricsAndTagsLock.RLock()
	allMetricsSegments := []*MetricsSegment{}
	if metricsAndTags, ok := OrgMetricsAndTags[orgid]; ok {
		for _, mSeg := range metricsAndTags.Metrics {
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
		for _, mSeg := range metricsAndTags.Metrics {
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

func GetTagsTreeHolder(orgid uint64, mid string) *TagsTreeHolder {
	orgMetricsAndTagsLock.RLock()
	var tt *TagsTreeHolder
	if metricsAndTags, ok := OrgMetricsAndTags[orgid]; ok {
		tt, ok = metricsAndTags.TagHolders[mid]
		if !ok {
			return nil
		}
	} else {
		return nil
	}
	orgMetricsAndTagsLock.RUnlock()
	return tt
}
