package instrumentation

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

import (
	"context"
	"fmt"
	"sync"

	log "github.com/sirupsen/logrus"

	"go.opentelemetry.io/otel/attribute"
	metric "go.opentelemetry.io/otel/metric"
)

/* Adding a new Gauge
   1. create a var int64
   2. create a rwlock
   3. create meter.async.guage
   4. create SetXXX method for the gauge
   5. register a callback.

   // Anotated example below
*/

type sumcount struct {
	sum      int64
	count    int64
	labelkey string
	labelval string
}

var currentEventCountGauge int64                         // 1
var currentEventCountGaugeLock sync.RWMutex              // 2
var CURRENT_EVENT_COUNT, _ = meter.Int64ObservableGauge( // 3
	"ss.current.event.count",
	metric.WithUnit("count"),
	metric.WithDescription("Current Count of total num of events"))

func SetGaugeCurrentEventCount(val int64) { // 4
	currentEventCountGaugeLock.Lock()
	currentEventCountGauge = val
	currentEventCountGaugeLock.Unlock()
}

var currentBytesReceivedGauge int64
var currentBytesReceivedGaugeLock sync.RWMutex
var CURRENT_BYTES_RECEIVED, _ = meter.Int64ObservableGauge(
	"ss.current.bytes.received",
	metric.WithUnit("bytes"),
	metric.WithDescription("current count of bytes received"))

func SetGaugeCurrentBytesReceivedGauge(val int64) {
	currentBytesReceivedGaugeLock.Lock()
	currentBytesReceivedGauge = val
	currentBytesReceivedGaugeLock.Unlock()
}

var currentOnDiskBytesGauge int64
var currentOnDiskBytesGaugeLock sync.RWMutex
var CURRENT_ON_DISK_BYTES, _ = meter.Int64ObservableGauge(
	"ss.current.on.disk.bytes",
	metric.WithUnit("bytes"),
	metric.WithDescription("current on disk bytes"))

func SetGaugeOnDiskBytesGauge(val int64) {
	currentOnDiskBytesGaugeLock.Lock()
	currentOnDiskBytesGauge = val
	currentOnDiskBytesGaugeLock.Unlock()
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

var writerSegstoreCountGauge int64
var writerSegstoreCountLock sync.RWMutex
var WRITER_SEGSTORE_COUNT, _ = meter.Int64ObservableGauge(
	"ss.writer.segstore.count",
	metric.WithUnit("count"),
	metric.WithDescription("writer segstore count"))

func SetWriterSegstoreCountGauge(val int64) {
	writerSegstoreCountLock.Lock()
	writerSegstoreCountGauge = val
	writerSegstoreCountLock.Unlock()
}

var segmentMicroindexCountGauge int64
var segmentMicroindexCountLock sync.RWMutex
var SEGMENT_MICROINDEX_COUNT, _ = meter.Int64ObservableGauge(
	"ss.segment.microindex.count",
	metric.WithUnit("count"),
	metric.WithDescription("segment microindex count"))

func SetSegmentMicroindexCountGauge(val int64) {
	segmentMicroindexCountLock.Lock()
	segmentMicroindexCountGauge = val
	segmentMicroindexCountLock.Unlock()
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

var eventsSearchedGauge int64
var eventsSearchedGaugeLock sync.RWMutex
var EVENTS_SEARCHED, _ = meter.Int64ObservableGauge(
	"ss.events.searched",
	metric.WithUnit("count"),
	metric.WithDescription("events searched"))

func SetEventsSearchedGauge(val int64) {
	eventsSearchedGaugeLock.Lock()
	eventsSearchedGauge = val
	eventsSearchedGaugeLock.Unlock()
}

var eventsMatchedGauge int64
var eventsMatchedGaugeLock sync.RWMutex
var EVENTS_MATCHED, _ = meter.Int64ObservableGauge(
	"ss.events.matched",
	metric.WithUnit("count"),
	metric.WithDescription("events matched"))

func SetEventsMatchedGauge(val int64) {
	eventsMatchedGaugeLock.Lock()
	eventsMatchedGauge = val
	eventsMatchedGaugeLock.Unlock()
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

func registerGaugeCallbacks() {
	_, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		currentEventCountGaugeLock.RLock()
		defer currentEventCountGaugeLock.RUnlock()
		o.ObserveInt64(CURRENT_EVENT_COUNT, int64(currentEventCountGauge))
		return nil
	}, CURRENT_EVENT_COUNT)
	if err != nil {
		log.Errorf("failed to register callback for gauge CURRENT_EVENT_COUNT, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		currentBytesReceivedGaugeLock.RLock()
		defer currentBytesReceivedGaugeLock.RUnlock()
		o.ObserveInt64(CURRENT_BYTES_RECEIVED, int64(currentBytesReceivedGauge))
		return nil
	}, CURRENT_BYTES_RECEIVED)
	if err != nil {
		log.Errorf("failed to register callback for gauge CURRENT_BYTES_RECEIVED, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		currentOnDiskBytesGaugeLock.RLock()
		defer currentOnDiskBytesGaugeLock.RUnlock()
		o.ObserveInt64(CURRENT_ON_DISK_BYTES, int64(currentOnDiskBytesGauge))
		return nil
	}, CURRENT_ON_DISK_BYTES)
	if err != nil {
		log.Errorf("failed to register callback for gauge CURRENT_ON_DISK_BYTES, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitQueryLatencyMs(ctx, o)
		return nil
	}, QUERY_LATENCY_MS)
	if err != nil {
		log.Errorf("failed to register callback for gauge QUERY_LATENCY_MS, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		writerSegstoreCountLock.RLock()
		defer writerSegstoreCountLock.RUnlock()
		o.ObserveInt64(WRITER_SEGSTORE_COUNT, int64(writerSegstoreCountGauge))
		return nil
	}, WRITER_SEGSTORE_COUNT)
	if err != nil {
		log.Errorf("failed to register callback for gauge WRITER_SEGSTORE_COUNT, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		segmentMicroindexCountLock.RLock()
		defer segmentMicroindexCountLock.RUnlock()
		o.ObserveInt64(SEGMENT_MICROINDEX_COUNT, int64(segmentMicroindexCountGauge))
		return nil
	}, SEGMENT_MICROINDEX_COUNT)
	if err != nil {
		log.Errorf("failed to register callback for gauge SEGMENT_MICROINDEX_COUNT, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitEventCountPerIndexMap(ctx, o)
		return nil
	}, EVENT_COUNT_PER_INDEX)
	if err != nil {
		log.Errorf("failed to register callback for gauge EVENT_COUNT_PER_INDEX, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitBytesCountPerIndexMap(ctx, o)
		return nil
	}, BYTES_COUNT_PER_INDEX)
	if err != nil {
		log.Errorf("failed to register callback for gauge BYTES_COUNT_PER_INDEX, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitOnDiskBytesPerIndexMap(ctx, o)
		return nil
	}, ON_DISK_BYTES_PER_INDEX)
	if err != nil {
		log.Errorf("failed to register callback for gauge ON_DISK_BYTES_PER_INDEX, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(EVENTS_SEARCHED, int64(eventsSearchedGauge))
		return nil
	}, EVENTS_SEARCHED)
	if err != nil {
		log.Errorf("failed to register callback for gauge EVENTS_SEARCHED, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(EVENTS_MATCHED, int64(eventsMatchedGauge))
		return nil
	}, EVENTS_MATCHED)
	if err != nil {
		log.Errorf("failed to register callback for gauge EVENTS_MATCHED, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitSegmentLatencyMinMsMap(ctx, o)
		return nil
	}, SEGMENT_LATENCY_MIN_MS)
	if err != nil {
		log.Errorf("failed to register callback for gauge SEGMENT_LATENCY_MIN_MS, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitSegmentLatencyMaxMsMap(ctx, o)
		return nil
	}, SEGMENT_LATENCY_MAX_MS)
	if err != nil {
		log.Errorf("failed to register callback for gauge SEGMENT_LATENCY_MAX_MS, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitSegmentLatencyAvgMsMap(ctx, o)
		return nil
	}, SEGMENT_LATENCY_AVG_MS)
	if err != nil {
		log.Errorf("failed to register callback for gauge SEGMENT_LATENCY_AVG_MS, err %v", err)
	}

	_, err = meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		emitSegmentLatencyP95MsMap(ctx, o)
		return nil
	}, SEGMENT_LATENCY_P95_MS)
	if err != nil {
		log.Errorf("failed to register callback for gauge SEGMENT_LATENCY_P95_MS, err %v", err)
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
