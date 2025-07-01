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

package ingest

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/common/fileutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/instrumentation"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/structs"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var (
	previousEventCount    int64
	previousBytesReceived int64
)

func InitIngestionMetrics() {
	go ingestionMetricsLooper()
	go metricsLooper()
}

func ingestionMetricsLooper() {
	for {
		time.Sleep(1 * time.Minute)

		currentEventCount := int64(0)
		currentBytesReceived := int64(0)
		currentOnDiskBytes := int64(0)

		allSegmetas := segwriter.ReadGlobalSegmetas()

		allCnts := segwriter.GetAllOrgsVTableCounts(allSegmetas)
		segwriter.GetAllOrgsUnrotatedVTableCounts(allCnts)

		uniqueIndexes, uniqueColumns, totalCmiSize, totalCsgSize, totalSegments := processSegmentAndIndexStats(allSegmetas, allCnts)

		for indexName, cnts := range allCnts {
			if indexName == "" {
				log.Errorf("ingestionMetricsLooper: skipping an empty index name len(indexName)=%v", len(indexName))
				continue
			}

			totalEventsForIndex := uint64(cnts.RecordCount)
			currentEventCount += int64(totalEventsForIndex)
			instrumentation.SetEventCountPerIndex(currentEventCount, "indexname", indexName)

			totalBytesReceivedForIndex := cnts.BytesCount
			currentBytesReceived += int64(totalBytesReceivedForIndex)
			instrumentation.SetBytesCountPerIndex(currentBytesReceived, "indexname", indexName)

			totalOnDiskBytesForIndex := cnts.OnDiskBytesCount
			currentOnDiskBytes += int64(totalOnDiskBytesForIndex)
			instrumentation.SetOnDiskBytesPerIndex(currentOnDiskBytes, "indexname", indexName)
		}

		eventCountPerMinute := currentEventCount - atomic.LoadInt64(&previousEventCount)
		eventVolumePerMinute := currentBytesReceived - atomic.LoadInt64(&previousBytesReceived)

		atomic.StoreInt64(&previousEventCount, currentEventCount)
		atomic.StoreInt64(&previousBytesReceived, currentBytesReceived)

		instrumentation.SetTotalIndexCount(int64(len(uniqueIndexes)))
		instrumentation.SetTotalEventCount(currentEventCount)
		instrumentation.SetTotalBytesReceived(currentBytesReceived)
		instrumentation.SetTotalLogOnDiskBytes(currentOnDiskBytes)
		instrumentation.SetPastMinuteEventCount(eventCountPerMinute)
		instrumentation.SetPastMinuteEventVolume(eventVolumePerMinute)
		instrumentation.SetTotalSegmentCount(totalSegments)
		instrumentation.SetTotalColumnCount(int64(len(uniqueColumns)))
		instrumentation.SetTotalCMISize(int64(totalCmiSize))
		instrumentation.SetTotalCSGSize(int64(totalCsgSize))
	}
}

func processSegmentAndIndexStats(allSegmetas []*structs.SegMeta, allCnts map[string]*structs.VtableCounts) (map[string]struct{}, map[string]struct{}, uint64, uint64, int64) {
	uniqueIndexes := make(map[string]struct{})
	uniqueColumns := make(map[string]struct{})
	var totalCmiSize, totalCsgSize uint64
	var totalSegments int64

	for _, segmeta := range allSegmetas {
		if segmeta == nil || segmeta.VirtualTableName == "" {
			continue
		}
		uniqueIndexes[segmeta.VirtualTableName] = struct{}{}
		for col := range segmeta.ColumnNames {
			uniqueColumns[col] = struct{}{}
		}
		totalSegments++
	}

	for indexName := range allCnts {
		if indexName != "" {
			uniqueIndexes[indexName] = struct{}{}
		}
	}

	for indexName := range uniqueIndexes {
		stats, err := segwriter.GetIndexSizeStats(indexName, utils.None[int64]())
		if err != nil {
			log.Errorf("processSegmentAndIndexStats: failed to get stats=%v for index=%v err=%v",
				stats, indexName, err)
			// some sfm filenames might be empty, but we should show what we have
			// if stats happens to be nil then continue on
			if stats == nil {
				continue
			}
		}

		totalCmiSize += stats.TotalCmiSize
		totalCsgSize += stats.TotalCsgSize

		_, _, _, columnNamesSet := segwriter.GetUnrotatedVTableCounts(indexName, utils.None[int64]())
		for col := range columnNamesSet {
			uniqueColumns[col] = struct{}{}
		}

		if len(columnNamesSet) > 0 {
			totalSegments++
		}

		if stats.NumBlocks > 0 {
			instrumentation.SetBlocksPerIndex(int64(stats.NumBlocks), "indexname", indexName)
		}
		if stats.NumIndexFiles > 0 {
			instrumentation.SetFilesPerIndex(int64(stats.NumIndexFiles), "indexname", indexName)
		}
	}

	return uniqueIndexes, uniqueColumns, totalCmiSize, totalCsgSize, totalSegments
}

func metricsLooper() {
	oneMinuteTicker := time.NewTicker(1 * time.Minute)
	fifteenMinuteTicker := time.NewTicker(15 * time.Minute)
	for {
		select {
		case <-oneMinuteTicker.C:
			setNumMetricNames()
			setMetricOnDiskBytes()
		case <-fifteenMinuteTicker.C:
			setNumKeysAndValues()
		}
	}
}

func setNumMetricNames() {
	allPreviousTime := &dtu.MetricsTimeRange{
		StartEpochSec: 0,
		EndEpochSec:   uint32(time.Now().Unix()),
	}
	names, err := query.GetAllMetricNamesOverTheTimeRangeForAllOrgs(allPreviousTime)
	if err != nil {
		log.Errorf("setNumMetricNames: failed to get all metric names: %v", err)
		return
	}

	instrumentation.SetTotalMetricNames(int64(len(names)))
}

func setNumKeysAndValues() {
	allPreviousTime := &dtu.MetricsTimeRange{
		StartEpochSec: 0,
		EndEpochSec:   uint32(time.Now().Unix()),
	}
	querySummary := summary.InitQuerySummary(summary.METRICS, rutils.GetNextQid())
	defer querySummary.LogMetricsQuerySummaryForAllOrgs()
	tagsTreeReaders, err := query.GetAllTagsTreesWithinTimeRange(allPreviousTime, querySummary)
	if err != nil {
		log.Errorf("setNumKeysAndValues: failed to get tags trees: %v", err)
		return
	}

	keys := make(map[string]struct{})
	values := make(map[string]struct{})
	for _, segmentTagTreeReader := range tagsTreeReaders {
		segmentTagPairs, err := segmentTagTreeReader.GetAllTagPairs()
		if err != nil {
			log.Errorf("setNumKeysAndValues: failed to get all tag pairs: %v", err)
			continue
		}

		for key, valueSet := range segmentTagPairs {
			keys[key] = struct{}{}
			for value := range valueSet {
				values[value] = struct{}{}
			}
		}
	}

	instrumentation.SetTotalTagKeyCount(int64(len(keys)))
	instrumentation.SetTotalTagValueCount(int64(len(values)))
}

func setMetricOnDiskBytes() {
	tagsTreeHolderDir := filepath.Join(config.GetDataPath(), config.GetHostID(), "final", "tth")
	tagsTreeHolderSize, err := fileutils.GetDirSize(tagsTreeHolderDir)
	if os.IsNotExist(err) {
		tagsTreeHolderSize = 0
	} else if err != nil {
		log.Errorf("setMetricOnDiskBytes: failed to get tags tree holder size: %v", err)
		return
	}

	timeSeriesDir := filepath.Join(config.GetDataPath(), config.GetHostID(), "final", "ts")
	timeSeriesSize, err := fileutils.GetDirSize(timeSeriesDir)
	if os.IsNotExist(err) {
		timeSeriesSize = 0
	} else if err != nil {
		log.Errorf("setMetricOnDiskBytes: failed to get time series size: %v", err)
		return
	}

	instrumentation.SetTotalMetricOnDiskBytes(int64(tagsTreeHolderSize + timeSeriesSize))
}
