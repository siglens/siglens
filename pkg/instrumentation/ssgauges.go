package instrumentation

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

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"

	"go.opentelemetry.io/otel/attribute"
	metric "go.opentelemetry.io/otel/metric"
)

/* Adding a new Gauge
	For a gauge with no labels:
	1. Create a new Gauge enum value
	2. Add a new entry in allSimpleGauges map
	3. Create a new setter method by calling makeGaugeSetter()

	For a gauge with labels:
   1. create a var int64
   2. create a rwlock
   3. create meter.async.gauge
   4. create SetXXX method for the gauge
   5. register a callback in registerOtherGaugeCallbacks()
*/

type sumcount struct {
	sum      int64
	count    int64
	labelkey string
	labelval string
}

type simpleInt64Gauge struct {
	name        string
	value       int64
	unit        string
	description string
	gauge       metric.Int64ObservableGauge
	lock        *sync.RWMutex
}

func CalculateStdev(values []float64) float64 {
	n := float64(len(values))
	mean := 0.0
	for _, value := range values {
		mean += value
	}
	mean /= n

	variance := 0.0
	for _, value := range values {
		variance += (value - mean) * (value - mean)
	}
	variance /= (n - 1)

	return math.Sqrt(variance)
}

// Population Standard Deviation
func CalculateStdevp(values []float64) float64 {
	n := float64(len(values))
	mean := 0.0
	for _, value := range values {
		mean += value
	}
	mean /= n

	variance := 0.0
	for _, value := range values {
		variance += (value - mean) * (value - mean)
	}
	variance /= n

	return math.Sqrt(variance)
}

type Gauge int

const (
	TotalEventCount Gauge = iota + 1
	TotalBytesReceived
	TotalLogOnDiskBytes
	TotalMetricOnDiskBytes
	TotalSegstoreCount
	TotalSegmentMicroindexCount
	TotalEventsSearched
	TotalEventsMatched
	TotalMetricNames
	TotalTimeSeries
	PastMinuteNumDataPoints
	TotalTagKeyCount
	TotalTagValueCount
	TotalIndexCount
	PastMinuteEventCount
	PastMinuteEventVolume
	TotalSegmentCount
	TotalColumnCount
	TotalCMISize
	TotalCSGSize
)

var allSimpleGauges = map[Gauge]*simpleInt64Gauge{
	TotalEventCount: {
		name:        "ss.total.event.count",
		unit:        "count",
		description: "Total number of events",
	},
	TotalBytesReceived: {
		name:        "ss.total.bytes.received",
		unit:        "bytes",
		description: "Total number of bytes received",
	},
	TotalLogOnDiskBytes: {
		name:        "ss.total.logs.on.disk.bytes",
		unit:        "bytes",
		description: "Total number of bytes on disk for log data",
	},
	TotalMetricOnDiskBytes: {
		name:        "ss.total.metrics.on.disk.bytes",
		unit:        "bytes",
		description: "Total number of metric bytes on disk",
	},
	TotalSegstoreCount: {
		name:        "ss.current.segstore.count",
		unit:        "count",
		description: "Current number of segstores",
	},
	TotalSegmentMicroindexCount: {
		name:        "ss.current.segment.microindex.count",
		unit:        "count",
		description: "Current number of segment microindexes",
	},
	TotalEventsSearched: {
		name:        "ss.current.events.searched",
		unit:        "count",
		description: "Current number of events searched",
	},
	TotalEventsMatched: {
		name:        "ss.current.events.matched",
		unit:        "count",
		description: "Current number of events matched",
	},
	TotalMetricNames: {
		name:        "ss.total.metric.names",
		unit:        "count",
		description: "Total number of metric names",
	},
	TotalTimeSeries: {
		name:        "ss.total.time.series",
		unit:        "count",
		description: "Total number of time series",
	},
	PastMinuteNumDataPoints: {
		name:        "ss.past.minute.num.data.points",
		unit:        "count",
		description: "Number of metric data points ingested in the past minute",
	},
	TotalTagKeyCount: {
		name:        "ss.total.tag.key.count",
		unit:        "count",
		description: "Total number of tag keys",
	},
	TotalTagValueCount: {
		name:        "ss.total.tag.value.count",
		unit:        "count",
		description: "Total number of tag values",
	},
	TotalIndexCount: {
		name:        "ss.total.index.count",
		unit:        "count",
		description: "Total number of indexes",
	},
	PastMinuteEventCount: {
		name:        "ss.past.minute.event.count",
		unit:        "count",
		description: "Number of events ingested in the past minute",
	},
	PastMinuteEventVolume: {
		name:        "ss.past.minute.event.volume",
		unit:        "bytes",
		description: "Volume of events ingested in the past minute",
	},
	TotalSegmentCount: {
		name:        "ss.total.segment.count",
		unit:        "count",
		description: "Total number of segments across all indexes",
	},
	TotalColumnCount: {
		name:        "ss.total.column.count",
		unit:        "count",
		description: "Total number of unique columns across all indexes",
	},
	TotalCMISize: {
		name:        "ss.total.cmi.size",
		unit:        "bytes",
		description: "Total size of CMI files",
	},
	TotalCSGSize: {
		name:        "ss.total.csg.size",
		unit:        "bytes",
		description: "Total size of CSG files",
	},
}

var (
	SetTotalEventCount             = makeGaugeSetter(TotalEventCount)
	SetTotalBytesReceived          = makeGaugeSetter(TotalBytesReceived)
	SetTotalLogOnDiskBytes         = makeGaugeSetter(TotalLogOnDiskBytes)
	SetTotalMetricOnDiskBytes      = makeGaugeSetter(TotalMetricOnDiskBytes)
	SetTotalSegstoreCount          = makeGaugeSetter(TotalSegstoreCount)
	SetTotalSegmentMicroindexCount = makeGaugeSetter(TotalSegmentMicroindexCount)
	SetTotalEventsSearched         = makeGaugeSetter(TotalEventsSearched)
	SetTotalEventsMatched          = makeGaugeSetter(TotalEventsMatched)
	SetTotalMetricNames            = makeGaugeSetter(TotalMetricNames)
	SetTotalTimeSeries             = makeGaugeSetter(TotalTimeSeries)
	SetPastMinuteNumDataPoints     = makeGaugeSetter(PastMinuteNumDataPoints)
	SetTotalTagKeyCount            = makeGaugeSetter(TotalTagKeyCount)
	SetTotalTagValueCount          = makeGaugeSetter(TotalTagValueCount)
	SetTotalIndexCount             = makeGaugeSetter(TotalIndexCount)
	SetPastMinuteEventCount        = makeGaugeSetter(PastMinuteEventCount)
	SetPastMinuteEventVolume       = makeGaugeSetter(PastMinuteEventVolume)
	SetTotalSegmentCount           = makeGaugeSetter(TotalSegmentCount)
	SetTotalColumnCount            = makeGaugeSetter(TotalColumnCount)
	SetTotalCMISize                = makeGaugeSetter(TotalCMISize)
	SetTotalCSGSize                = makeGaugeSetter(TotalCSGSize)
)

func init() {
	err := initGauges()
	if err != nil {
		log.Fatalf("init: failed to initialize gauges; err=%v", err)
	}
}

func initGauges() error {
	// Finish setting up each gauge.
	for _, simpleGauge := range allSimpleGauges {
		gauge, err := meter.Int64ObservableGauge(
			simpleGauge.name,
			metric.WithUnit(simpleGauge.unit),
			metric.WithDescription(simpleGauge.description),
		)
		if err != nil {
			return utils.TeeErrorf("initGauges: failed to create gauge %s; err=%v", simpleGauge.name, err)
		}

		simpleGauge.gauge = gauge
		simpleGauge.lock = &sync.RWMutex{}
	}

	// Register the callbacks for each gauge.
	for key := range allSimpleGauges {
		simpleGauge := allSimpleGauges[key]

		_, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
			simpleGauge.lock.RLock()
			defer simpleGauge.lock.RUnlock()
			o.ObserveInt64(simpleGauge.gauge, simpleGauge.value)
			return nil
		}, simpleGauge.gauge)
		if err != nil {
			return utils.TeeErrorf("initGauges: failed to register callback for gauge %v; err=%v", simpleGauge.name, err)
		}
	}

	registerOtherGaugeCallbacks()

	return nil
}

func makeGaugeSetter(gauge Gauge) func(int64) {
	return func(value int64) {
		simpleGauge, ok := allSimpleGauges[gauge]
		if !ok {
			log.Errorf("makeGaugeSetter: invalid gauge: %v", gauge)
			return
		}

		simpleGauge.lock.Lock()
		simpleGauge.value = value
		simpleGauge.lock.Unlock()
	}
}

// map[labelkey-value] --> sumcount struct
var queryLatencyMsMap = map[string]*sumcount{}
var queryLatencyMsLock sync.RWMutex
var QUERY_LATENCY_MS, _ = meter.Int64ObservableGauge(
	"ss.query.latency.ms",
	metric.WithUnit("milliseconds"),
	metric.WithDescription("query latency in milliseconds"))

func SetQueryLatencyMs(val int64, labelkey string, labelval string) {
	keystr := fmt.Sprintf("%v:%v", labelkey, labelval)
	queryLatencyMsLock.Lock()
	defer queryLatencyMsLock.Unlock()
	mentry, ok := queryLatencyMsMap[keystr]
	if !ok {
		mentry = &sumcount{labelkey: labelkey, labelval: labelval}
		queryLatencyMsMap[keystr] = mentry
	}
	mentry.sum += val
	mentry.count++
}

var eventCountPerIndexMap = map[string]*sumcount{}
var eventCountPerIndexGaugeLock sync.RWMutex
var EVENT_COUNT_PER_INDEX, _ = meter.Int64ObservableGauge(
	"ss.event.count.per.index",
	metric.WithUnit("count"),
	metric.WithDescription("event count per index"))

func SetEventCountPerIndex(val int64, labelkey string, labelval string) {
	keystr := fmt.Sprintf("%v:%v", labelkey, labelval)
	eventCountPerIndexGaugeLock.Lock()
	defer eventCountPerIndexGaugeLock.Unlock()
	mentry, ok := eventCountPerIndexMap[keystr]
	if !ok {
		mentry = &sumcount{labelkey: labelkey, labelval: labelval}
		eventCountPerIndexMap[keystr] = mentry
	}
	mentry.sum += val
	mentry.count++
}

var bytesCountPerIndexMap = map[string]*sumcount{}
var bytesCountPerIndexGaugeLock sync.RWMutex
var BYTES_COUNT_PER_INDEX, _ = meter.Int64ObservableGauge(
	"ss.bytes.count.per.index",
	metric.WithUnit("bytes"),
	metric.WithDescription("bytes count per index"))

func SetBytesCountPerIndex(val int64, labelkey string, labelval string) {
	keystr := fmt.Sprintf("%v:%v", labelkey, labelval)
	bytesCountPerIndexGaugeLock.Lock()
	defer bytesCountPerIndexGaugeLock.Unlock()
	mentry, ok := bytesCountPerIndexMap[keystr]
	if !ok {
		mentry = &sumcount{labelkey: labelkey, labelval: labelval}
		bytesCountPerIndexMap[keystr] = mentry
	}
	mentry.sum += val
	mentry.count++
}

var onDiskBytesPerIndexMap = map[string]*sumcount{}
var onDiskBytesPerIndexLock sync.RWMutex
var ON_DISK_BYTES_PER_INDEX, _ = meter.Int64ObservableGauge(
	"ss.on.disk.bytes.per.index",
	metric.WithUnit("bytes"),
	metric.WithDescription("on disk bytes per index"))

func SetOnDiskBytesPerIndex(val int64, labelkey string, labelval string) {
	keystr := fmt.Sprintf("%v:%v", labelkey, labelval)
	onDiskBytesPerIndexLock.Lock()
	defer onDiskBytesPerIndexLock.Unlock()
	mentry, ok := onDiskBytesPerIndexMap[keystr]
	if !ok {
		mentry = &sumcount{labelkey: labelkey, labelval: labelval}
		onDiskBytesPerIndexMap[keystr] = mentry
	}
	mentry.sum += val
	mentry.count++
}

var filesPerIndexMap = map[string]*sumcount{}
var filesPerIndexLock sync.RWMutex
var FILES_COUNT_PER_INDEX, _ = meter.Int64ObservableGauge(
	"ss.files.per.index",
	metric.WithUnit("count"),
	metric.WithDescription("Number of files per index"))

func SetFilesPerIndex(val int64, labelkey string, labelval string) {
	keystr := fmt.Sprintf("%v:%v", labelkey, labelval)
	filesPerIndexLock.Lock()
	defer filesPerIndexLock.Unlock()
	mentry, ok := filesPerIndexMap[keystr]
	if !ok {
		mentry = &sumcount{labelkey: labelkey, labelval: labelval}
		filesPerIndexMap[keystr] = mentry
	}
	mentry.sum = val
	mentry.count = 1
}

var blocksPerIndexMap = map[string]*sumcount{}
var blocksPerIndexLock sync.RWMutex
var BLOCKS_COUNT_PER_INDEX, _ = meter.Int64ObservableGauge(
	"ss.blocks.per.index",
	metric.WithUnit("count"),
	metric.WithDescription("Number of blocks per index"))

func SetBlocksPerIndex(val int64, labelkey string, labelval string) {
	keystr := fmt.Sprintf("%v:%v", labelkey, labelval)
	blocksPerIndexLock.Lock()
	defer blocksPerIndexLock.Unlock()
	mentry, ok := blocksPerIndexMap[keystr]
	if !ok {
		mentry = &sumcount{labelkey: labelkey, labelval: labelval}
		blocksPerIndexMap[keystr] = mentry
	}
	mentry.sum = val
	mentry.count = 1
}

var segmentLatencyMinMsMap = map[string]*sumcount{}
var segmentLatencyMinMsLock sync.RWMutex
var SEGMENT_LATENCY_MIN_MS, _ = meter.Int64ObservableGauge(
	"ss.segment.latency.min.ms",
	metric.WithUnit("milliseconds"),
	metric.WithDescription("segment latency min in ms"))

func SetSegmentLatencyMinMs(val int64, labelkey string, labelval string) {
	keystr := fmt.Sprintf("%v:%v", labelkey, labelval)
	segmentLatencyMinMsLock.Lock()
	defer segmentLatencyMinMsLock.Unlock()
	mentry, ok := segmentLatencyMinMsMap[keystr]
	if !ok {
		mentry = &sumcount{labelkey: labelkey, labelval: labelval}
		segmentLatencyMinMsMap[keystr] = mentry
	}
	mentry.sum += val
	mentry.count++
}

var segmentLatencyMaxMsMap = map[string]*sumcount{}
var segmentLatencyMaxMsLock sync.RWMutex
var SEGMENT_LATENCY_MAX_MS, _ = meter.Int64ObservableGauge(
	"ss.segment.latency.max.ms",
	metric.WithUnit("milliseconds"),
	metric.WithDescription("segment latency max in ms"))

func SetSegmentLatencyMaxMs(val int64, labelkey string, labelval string) {
	keystr := fmt.Sprintf("%v:%v", labelkey, labelval)
	segmentLatencyMaxMsLock.Lock()
	defer segmentLatencyMaxMsLock.Unlock()
	mentry, ok := segmentLatencyMaxMsMap[keystr]
	if !ok {
		mentry = &sumcount{labelkey: labelkey, labelval: labelval}
		segmentLatencyMaxMsMap[keystr] = mentry
	}
	mentry.sum += val
	mentry.count++
}

var segmentLatencyAvgMsMap = map[string]*sumcount{}
var segmentLatencyAvgMsLock sync.RWMutex
var SEGMENT_LATENCY_AVG_MS, _ = meter.Int64ObservableGauge(
	"ss.segment.latency.avg.ms",
	metric.WithUnit("milliseconds"),
	metric.WithDescription("segment latency avg in ms"))

func SetSegmentLatencyAvgMs(val int64, labelkey string, labelval string) {
	keystr := fmt.Sprintf("%v:%v", labelkey, labelval)
	segmentLatencyAvgMsLock.Lock()
	defer segmentLatencyAvgMsLock.Unlock()
	mentry, ok := segmentLatencyAvgMsMap[keystr]
	if !ok {
		mentry = &sumcount{labelkey: labelkey, labelval: labelval}
		segmentLatencyAvgMsMap[keystr] = mentry
	}
	mentry.sum += val
	mentry.count++
}

var segmentLatencyP95MsMap = map[string]*sumcount{}
var segmentLatencyP95MsLock sync.RWMutex
var SEGMENT_LATENCY_P95_MS, _ = meter.Int64ObservableGauge(
	"ss.segment.latency.p95.ms",
	metric.WithUnit("milliseconds"),
	metric.WithDescription("segment latency p95 in ms"))

func SetSegmentLatencyP95Ms(val int64, labelkey string, labelval string) {
	keystr := fmt.Sprintf("%v:%v", labelkey, labelval)
	segmentLatencyP95MsLock.Lock()
	defer segmentLatencyP95MsLock.Unlock()
	mentry, ok := segmentLatencyP95MsMap[keystr]
	if !ok {
		mentry = &sumcount{labelkey: labelkey, labelval: labelval}
		segmentLatencyP95MsMap[keystr] = mentry
	}
	mentry.sum += val
	mentry.count++
}

func registerOtherGaugeCallbacks() {
	_, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitQueryLatencyMs(ctx, o)
		return nil
	}, QUERY_LATENCY_MS)
	if err != nil {
		log.Errorf("registerOtherGaugeCallbacks: failed to register callback for gauge QUERY_LATENCY_MS, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitEventCountPerIndexMap(ctx, o)
		return nil
	}, EVENT_COUNT_PER_INDEX)
	if err != nil {
		log.Errorf("registerOtherGaugeCallbacks: failed to register callback for gauge EVENT_COUNT_PER_INDEX, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitBytesCountPerIndexMap(ctx, o)
		return nil
	}, BYTES_COUNT_PER_INDEX)
	if err != nil {
		log.Errorf("registerOtherGaugeCallbacks: failed to register callback for gauge BYTES_COUNT_PER_INDEX, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitOnDiskBytesPerIndexMap(ctx, o)
		return nil
	}, ON_DISK_BYTES_PER_INDEX)
	if err != nil {
		log.Errorf("registerOtherGaugeCallbacks: failed to register callback for gauge ON_DISK_BYTES_PER_INDEX, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitFilesPerIndexMap(ctx, o)
		return nil
	}, FILES_COUNT_PER_INDEX)
	if err != nil {
		log.Errorf("registerOtherGaugeCallbacks: failed to register callback for gauge FILES_COUNT_PER_INDEX, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitBlocksPerIndexMap(ctx, o)
		return nil
	}, BLOCKS_COUNT_PER_INDEX)
	if err != nil {
		log.Errorf("registerOtherGaugeCallbacks: failed to register callback for gauge BLOCKS_COUNT_PER_INDEX, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitSegmentLatencyMinMsMap(ctx, o)
		return nil
	}, SEGMENT_LATENCY_MIN_MS)
	if err != nil {
		log.Errorf("registerOtherGaugeCallbacks: failed to register callback for gauge SEGMENT_LATENCY_MIN_MS, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitSegmentLatencyMaxMsMap(ctx, o)
		return nil
	}, SEGMENT_LATENCY_MAX_MS)
	if err != nil {
		log.Errorf("registerOtherGaugeCallbacks: failed to register callback for gauge SEGMENT_LATENCY_MAX_MS, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitSegmentLatencyAvgMsMap(ctx, o)
		return nil
	}, SEGMENT_LATENCY_AVG_MS)
	if err != nil {
		log.Errorf("registerOtherGaugeCallbacks: failed to register callback for gauge SEGMENT_LATENCY_AVG_MS, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitSegmentLatencyP95MsMap(ctx, o)
		return nil
	}, SEGMENT_LATENCY_P95_MS)
	if err != nil {
		log.Errorf("registerOtherGaugeCallbacks: failed to register callback for gauge SEGMENT_LATENCY_P95_MS, err %v", err)
	}
}

func emitQueryLatencyMs(ctx context.Context, o metric.Observer) {
	queryLatencyMsLock.Lock()
	defer queryLatencyMsLock.Unlock()
	for mkey, mentry := range queryLatencyMsMap {
		if mentry.count != 0 {
			attrs := []attribute.KeyValue{
				attribute.String(mentry.labelkey, mentry.labelval),
			}
			o.ObserveInt64(QUERY_LATENCY_MS, int64(mentry.sum/mentry.count), metric.WithAttributes(attrs...))
		}
		delete(queryLatencyMsMap, mkey)
	}
}

func emitEventCountPerIndexMap(ctx context.Context, o metric.Observer) {
	eventCountPerIndexGaugeLock.Lock()
	defer eventCountPerIndexGaugeLock.Unlock()
	for mkey, mentry := range eventCountPerIndexMap {
		if mentry.count != 0 {
			attrs := []attribute.KeyValue{
				attribute.String(mentry.labelkey, mentry.labelval),
			}
			o.ObserveInt64(EVENT_COUNT_PER_INDEX, int64(mentry.sum/mentry.count), metric.WithAttributes(attrs...))
		}
		delete(eventCountPerIndexMap, mkey)
	}
}

func emitBytesCountPerIndexMap(ctx context.Context, o metric.Observer) {
	bytesCountPerIndexGaugeLock.Lock()
	defer bytesCountPerIndexGaugeLock.Unlock()
	for mkey, mentry := range bytesCountPerIndexMap {
		if mentry.count != 0 {
			attrs := []attribute.KeyValue{
				attribute.String(mentry.labelkey, mentry.labelval),
			}
			o.ObserveInt64(BYTES_COUNT_PER_INDEX, int64(mentry.sum/mentry.count), metric.WithAttributes(attrs...))
		}
		delete(bytesCountPerIndexMap, mkey)
	}
}

func emitOnDiskBytesPerIndexMap(ctx context.Context, o metric.Observer) {
	onDiskBytesPerIndexLock.Lock()
	defer onDiskBytesPerIndexLock.Unlock()
	for mkey, mentry := range onDiskBytesPerIndexMap {
		if mentry.count != 0 {
			attrs := []attribute.KeyValue{
				attribute.String(mentry.labelkey, mentry.labelval),
			}
			o.ObserveInt64(ON_DISK_BYTES_PER_INDEX, int64(mentry.sum/mentry.count), metric.WithAttributes(attrs...))
		}
		delete(onDiskBytesPerIndexMap, mkey)
	}
}

func emitFilesPerIndexMap(ctx context.Context, o metric.Observer) {
	filesPerIndexLock.Lock()
	defer filesPerIndexLock.Unlock()
	for mkey, mentry := range filesPerIndexMap {
		if mentry.count != 0 {
			attrs := []attribute.KeyValue{
				attribute.String(mentry.labelkey, mentry.labelval),
			}
			o.ObserveInt64(FILES_COUNT_PER_INDEX, mentry.sum, metric.WithAttributes(attrs...))
		}
		delete(filesPerIndexMap, mkey)
	}
}

func emitBlocksPerIndexMap(ctx context.Context, o metric.Observer) {
	blocksPerIndexLock.Lock()
	defer blocksPerIndexLock.Unlock()
	for mkey, mentry := range blocksPerIndexMap {
		if mentry.count != 0 {
			attrs := []attribute.KeyValue{
				attribute.String(mentry.labelkey, mentry.labelval),
			}
			o.ObserveInt64(BLOCKS_COUNT_PER_INDEX, mentry.sum, metric.WithAttributes(attrs...))
		}
		delete(blocksPerIndexMap, mkey)
	}
}

func emitSegmentLatencyMinMsMap(ctx context.Context, o metric.Observer) {
	segmentLatencyMinMsLock.Lock()
	defer segmentLatencyMinMsLock.Unlock()
	for mkey, mentry := range segmentLatencyMinMsMap {
		if mentry.count != 0 {
			attrs := []attribute.KeyValue{
				attribute.String(mentry.labelkey, mentry.labelval),
			}
			o.ObserveInt64(SEGMENT_LATENCY_MIN_MS, int64(mentry.sum/mentry.count), metric.WithAttributes(attrs...))
		}
		delete(segmentLatencyMinMsMap, mkey)
	}
}

func emitSegmentLatencyMaxMsMap(ctx context.Context, o metric.Observer) {
	segmentLatencyMaxMsLock.Lock()
	defer segmentLatencyMaxMsLock.Unlock()
	for mkey, mentry := range segmentLatencyMaxMsMap {
		if mentry.count != 0 {
			attrs := []attribute.KeyValue{
				attribute.String(mentry.labelkey, mentry.labelval),
			}
			o.ObserveInt64(SEGMENT_LATENCY_MAX_MS, int64(mentry.sum/mentry.count), metric.WithAttributes(attrs...))
		}
		delete(segmentLatencyMaxMsMap, mkey)
	}
}

func emitSegmentLatencyAvgMsMap(ctx context.Context, o metric.Observer) {
	segmentLatencyAvgMsLock.Lock()
	defer segmentLatencyAvgMsLock.Unlock()
	for mkey, mentry := range segmentLatencyAvgMsMap {
		if mentry.count != 0 {
			attrs := []attribute.KeyValue{
				attribute.String(mentry.labelkey, mentry.labelval),
			}
			o.ObserveInt64(SEGMENT_LATENCY_AVG_MS, int64(mentry.sum/mentry.count), metric.WithAttributes(attrs...))
		}
		delete(segmentLatencyAvgMsMap, mkey)
	}
}

func emitSegmentLatencyP95MsMap(ctx context.Context, o metric.Observer) {
	segmentLatencyP95MsLock.Lock()
	defer segmentLatencyP95MsLock.Unlock()
	for mkey, mentry := range segmentLatencyP95MsMap {
		if mentry.count != 0 {
			attrs := []attribute.KeyValue{
				attribute.String(mentry.labelkey, mentry.labelval),
			}
			o.ObserveInt64(SEGMENT_LATENCY_P95_MS, int64(mentry.sum/mentry.count), metric.WithAttributes(attrs...))
		}
		delete(segmentLatencyP95MsMap, mkey)
	}
}
