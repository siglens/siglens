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

package retention

import (
	"math"
	"path"
	"sort"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/common/fileutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	pqsmeta "github.com/siglens/siglens/pkg/segment/query/pqs/meta"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/writer"
	mmeta "github.com/siglens/siglens/pkg/segment/writer/metrics/meta"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
)

const MAXIMUM_WARNINGS_COUNT = 5

// Starting the periodic retention based deletion
func InitRetentionCleaner() error {
	if hook := hooks.GlobalHooks.ExtraRetentionCleanerHook; hook != nil {
		err := hook()
		if err != nil {
			return err
		}
	}

	go internalRetentionCleaner()
	return nil
}

func internalRetentionCleaner() {
	time.Sleep(1 * time.Minute) // sleep for 1min for the rest of the system to come up

	var hook1Result string
	if hook := hooks.GlobalHooks.InternalRetentionCleanerHook1; hook != nil {
		hook1Result = hook()
	}

	deletionWarningCounter := 0
	for {
		time.Sleep(1 * time.Hour)
		if hook := hooks.GlobalHooks.InternalRetentionCleanerHook2; hook != nil {
			hook(hook1Result, deletionWarningCounter)
		} else {
			DoRetentionBasedDeletion(config.GetCurrentNodeIngestDir(), config.GetRetentionHours(), 0)
			doVolumeBasedDeletion(config.GetCurrentNodeIngestDir(), 4000, deletionWarningCounter)
		}
		if deletionWarningCounter <= MAXIMUM_WARNINGS_COUNT {
			deletionWarningCounter++
		}
	}
}

func DoRetentionBasedDeletion(ingestNodeDir string, retentionHours int, orgid uint64) {
	currTime := time.Now()
	deleteBefore := GetRetentionTimeMs(retentionHours, currTime)

	currentSegmeta := path.Join(ingestNodeDir, writer.SegmetaSuffix)
	allSegMetas, err := writer.ReadSegmeta(currentSegmeta)
	if err != nil {
		log.Errorf("doVolumeBasedDeletion: Failed to read segmeta, err: %v", err)
		return
	}

	// Read metrics meta entries
	currentMetricsMeta := path.Join(ingestNodeDir, mmeta.MetricsMetaSuffix)

	allMetricMetas, err := mmeta.ReadMetricsMeta(currentMetricsMeta)
	if err != nil {
		log.Errorf("doVolumeBasedDeletion: Failed to get all metric meta entries, err: %v", err)
		return
	}

	// Combine metrics and segments
	allEntries := make([]interface{}, 0, len(allMetricMetas)+len(allSegMetas))
	for i := range allMetricMetas {
		if allMetricMetas[i].OrgId == orgid {
			allEntries = append(allEntries, allMetricMetas[i])
		}
	}
	for i := range allSegMetas {
		if allSegMetas[i].OrgId == orgid {
			allEntries = append(allEntries, allSegMetas[i])
		}
	}

	// Sort all entries based on latest epoch time
	sort.Slice(allEntries, func(i, j int) bool {
		var timeI uint64
		if segMeta, ok := allEntries[i].(*structs.SegMeta); ok {
			timeI = segMeta.LatestEpochMS
		} else if metricMeta, ok := allEntries[i].(*structs.MetricsMeta); ok {
			timeI = uint64(metricMeta.LatestEpochSec) * 1000 // convert to milliseconds
		} else {
			return false
		}

		var timeJ uint64
		if segMeta, ok := allEntries[j].(*structs.SegMeta); ok {
			timeJ = segMeta.LatestEpochMS
		} else if metricMeta, ok := allEntries[j].(*structs.MetricsMeta); ok {
			timeJ = uint64(metricMeta.LatestEpochSec) * 1000 // convert to milliseconds
		} else {
			return false
		}
		return timeI < timeJ
	})

	segmentsToDelete := make(map[string]*structs.SegMeta)
	metricSegmentsToDelete := make(map[string]*structs.MetricsMeta)

	oldest := uint64(math.MaxUint64)
	for _, metaEntry := range allEntries {
		switch entry := metaEntry.(type) {
		case *structs.MetricsMeta:
			if uint64(entry.LatestEpochSec)*1000 <= deleteBefore {
				metricSegmentsToDelete[entry.MSegmentDir] = entry
			}
			if oldest > uint64(entry.LatestEpochSec)*1000 {
				oldest = uint64(entry.LatestEpochSec) * 1000
			}
		case *structs.SegMeta:
			if entry.LatestEpochMS <= deleteBefore {
				segmentsToDelete[entry.SegmentKey] = entry
			}
			if oldest > entry.LatestEpochMS {
				oldest = entry.LatestEpochMS
			}
		}
	}

	log.Infof("doRetentionBasedDeletion: totalsegs=%v, segmentsToDelete=%v, metricsSegmentsToDelete=%v, oldest=%v, orgid=%v",
		len(allEntries), len(segmentsToDelete), len(metricSegmentsToDelete), oldest, orgid)

	// Delete all segment data
	DeleteSegmentData(currentSegmeta, segmentsToDelete, true)
	DeleteMetricsSegmentData(currentMetricsMeta, metricSegmentsToDelete, true)
	deleteEmptyIndices(ingestNodeDir, 0)
}

func deleteEmptyIndices(ingestNodeDir string, orgid uint64) {
	allIndices, err := vtable.GetVirtualTableNames(orgid)
	if err != nil {
		log.Errorf("DeleteEmptyIndices: Error in getting virtual table names, err: %v", err)
		return
	}

	currentSegmeta := path.Join(ingestNodeDir, writer.SegmetaSuffix)

	segMetaEntries, err := writer.ReadSegmeta(currentSegmeta)
	if err != nil {
		log.Errorf("DeleteEmptyIndices: Error in reading segmeta file, err: %v", err)
		return
	}
	// Create a set of virtualTableName values from segMetaEntries
	virtualTableNames := make(map[string]struct{})
	for _, entry := range segMetaEntries {
		virtualTableNames[entry.VirtualTableName] = struct{}{}
	}

	// Iterate over all indices
	for indexName := range allIndices {
		// If an index is not in the set of virtualTableName values from segMetaEntries, delete it
		if _, exists := virtualTableNames[indexName]; !exists {
			err := vtable.DeleteVirtualTable(&indexName, 0)
			if err != nil {
				log.Errorf("DeleteEmptyIndices: Error in deleting index %s, err: %v", indexName, err)
			}
		}
	}
}

func GetRetentionTimeMs(retentionHours int, currTime time.Time) uint64 {
	retDur := time.Duration(retentionHours) * time.Hour
	retentionTime := currTime.Add(-retDur)
	return uint64(retentionTime.UnixMilli())
}
func deleteSegmentsFromEmptyPqMetaFiles(segmentsToDelete map[string]*structs.SegMeta) {
	for _, segmetaEntry := range segmentsToDelete {
		for pqid := range segmetaEntry.AllPQIDs {
			pqsmeta.DeleteSegmentFromPqid(pqid, segmetaEntry.SegmentKey)
		}
	}
}

func doVolumeBasedDeletion(ingestNodeDir string, allowedVolumeGB uint64, deletionWarningCounter int) {
	allowedVolumeBytes := allowedVolumeGB * 1000 * 1000 * 1000
	systemVolumeBytes, err := getSystemVolumeBytes()
	if err != nil {
		log.Errorf("doVolumeBasedDeletion: Failed to get systemVolumeBytes, err: %v", err)
		return
	}
	if systemVolumeBytes == 0 {
		return
	}
	log.Infof("doVolumeBasedDeletion: System volume(GB) : %v, Allowed volume(GB) : %v, IngestNodeDir: %v",
		humanize.Comma(int64(systemVolumeBytes/(1000*1000*1000))), humanize.Comma(int64(allowedVolumeBytes/(1000*1000*1000))), ingestNodeDir)

	volumeToDeleteInBytes := uint64(0)
	if systemVolumeBytes > allowedVolumeBytes {
		if deletionWarningCounter < MAXIMUM_WARNINGS_COUNT {
			log.Warnf("Skipping deletion since try %d, System volume(bytes) : %v, Allowed volume(bytes) : %v",
				deletionWarningCounter, humanize.Comma(int64(systemVolumeBytes)), humanize.Comma(int64(allowedVolumeBytes)))
			return
		}
		volumeToDeleteInBytes = (systemVolumeBytes - allowedVolumeBytes)
	} else {
		return
	}

	currentSegmeta := path.Join(ingestNodeDir, writer.SegmetaSuffix)
	allSegMetas, err := writer.ReadSegmeta(currentSegmeta)
	if err != nil {
		log.Errorf("doVolumeBasedDeletion: Failed to read segmeta, err: %v", err)
		return
	}

	currentMetricsMeta := path.Join(ingestNodeDir, mmeta.MetricsMetaSuffix)
	allMetricMetas, err := mmeta.ReadMetricsMeta(currentMetricsMeta)
	if err != nil {
		log.Errorf("doVolumeBasedDeletion: Failed to get all metric meta entries, err: %v", err)
		return
	}

	allEntries := make([]interface{}, 0, len(allMetricMetas)+len(allSegMetas))
	for i := range allMetricMetas {
		allEntries = append(allEntries, allMetricMetas[i])
	}
	for i := range allSegMetas {
		allEntries = append(allEntries, allSegMetas[i])
	}

	sort.Slice(allEntries, func(i, j int) bool {
		var timeI uint64
		if segMeta, ok := allEntries[i].(*structs.SegMeta); ok {
			timeI = segMeta.LatestEpochMS
		} else if metricMeta, ok := allEntries[i].(*structs.MetricsMeta); ok {
			timeI = uint64(metricMeta.LatestEpochSec * 1000) // convert to milliseconds
		} else {
			return false
		}

		var timeJ uint64
		if segMeta, ok := allEntries[j].(*structs.SegMeta); ok {
			timeJ = segMeta.LatestEpochMS
		} else if metricMeta, ok := allEntries[j].(*structs.MetricsMeta); ok {
			timeJ = uint64(metricMeta.LatestEpochSec * 1000)
		} else {
			return false
		}
		return timeI < timeJ
	})

	segmentsToDelete := make(map[string]*structs.SegMeta)
	metricSegmentsToDelete := make(map[string]*structs.MetricsMeta)

	for _, metaEntry := range allEntries {
		switch entry := metaEntry.(type) {
		case *structs.MetricsMeta:
			if entry.BytesReceivedCount < volumeToDeleteInBytes {
				metricSegmentsToDelete[entry.MSegmentDir] = entry
				volumeToDeleteInBytes -= entry.BytesReceivedCount
			} else {
				break
			}
		case *structs.SegMeta:
			if entry.BytesReceivedCount < volumeToDeleteInBytes {
				segmentsToDelete[entry.SegmentKey] = entry
				volumeToDeleteInBytes -= entry.BytesReceivedCount
			} else {
				break
			}
		}
	}
	DeleteSegmentData(currentSegmeta, segmentsToDelete, true)
	DeleteMetricsSegmentData(currentMetricsMeta, metricSegmentsToDelete, true)
}

func getSystemVolumeBytes() (uint64, error) {
	currentVolume := uint64(0)

	allVirtualTableNames, err := vtable.GetVirtualTableNames(0)
	if err != nil {
		log.Errorf("getSystemVolumeBytes: Error in getting virtual table names, err: %v", err)
		return currentVolume, err
	}
	for indexName := range allVirtualTableNames {
		if indexName == "" {
			log.Errorf("getSystemVolumeBytes: skipping an empty index name indexName=%v", indexName)
			continue
		}
		byteCount, _, _ := writer.GetVTableCounts(indexName, 0)
		unrotatedByteCount, _, _ := writer.GetUnrotatedVTableCounts(indexName, 0)

		totalVolumeForIndex := uint64(byteCount) + uint64(unrotatedByteCount)
		currentVolume += uint64(totalVolumeForIndex)
	}

	metricsSegments, err := mmeta.GetLocalMetricsMetaEntries()
	if err != nil {
		log.Errorf("doVolumeBasedDeletion: Failed to get all metric meta entries, err: %v", err)
		return currentVolume, err
	}
	for _, segmentEntry := range metricsSegments {
		currentVolume += segmentEntry.BytesReceivedCount
	}

	return currentVolume, nil
}

func DeleteSegmentData(segmetaFile string, segmentsToDelete map[string]*structs.SegMeta, updateBlob bool) {

	if len(segmentsToDelete) == 0 {
		return
	}
	deleteSegmentsFromEmptyPqMetaFiles(segmentsToDelete)
	// Delete segment key from all SiglensMetadata structs
	for _, segMetaEntry := range segmentsToDelete {
		metadata.DeleteSegmentKey(segMetaEntry.SegmentKey)

		// Delete segment files from s3
		dirPath := segMetaEntry.SegmentKey
		filesToDelete := fileutils.GetAllFilesInDirectory(path.Dir(dirPath) + "/")

		for _, file := range filesToDelete {
			err := blob.DeleteBlob(file)
			if err != nil {
				log.Infof("deleteSegmentData: Error in deleting segment file %v in s3", file)
				continue
			}
		}
	}

	writer.RemoveSegments(segmetaFile, segmentsToDelete)

	// Upload the latest ingest nodes dir to s3 only if updateBlob is true
	if !updateBlob {
		return
	}
	err := blob.UploadIngestNodeDir()
	if err != nil {
		log.Errorf("deleteSegmentData: failed to upload ingestnodes dir to s3 err=%v", err)
		return
	}
}

func DeleteMetricsSegmentData(mmetaFile string, metricSegmentsToDelete map[string]*structs.MetricsMeta, updateBlob bool) {
	if len(metricSegmentsToDelete) == 0 {
		return
	}

	// Delete segment key from all SiglensMetadata structs
	for _, metricsSegmentMeta := range metricSegmentsToDelete {
		err := metadata.DeleteMetricsSegmentKey(metricsSegmentMeta.MSegmentDir)
		if err != nil {
			log.Errorf("deleteMetricsSegmentData: failed to delete metrics segment. Error:%v", err)
			return
		}

		// Delete segment files from s3
		dirPath := metricsSegmentMeta.MSegmentDir
		filesToDelete := fileutils.GetAllFilesInDirectory(path.Dir(dirPath) + "/")

		for _, file := range filesToDelete {
			err := blob.DeleteBlob(file)
			if err != nil {
				log.Infof("deleteMetricsSegmentData: Error in deleting segment file %v in s3", file)
				continue
			}
		}
	}

	mmeta.RemoveMetricsSegments(mmetaFile, metricSegmentsToDelete)

	// Upload the latest ingest nodes dir to s3 only if updateBlob is true
	if !updateBlob {
		return
	}
	err := blob.UploadIngestNodeDir()
	if err != nil {
		log.Errorf("deleteMetricsSegmentData: failed to upload ingestnodes dir to s3 err=%v", err)
		return
	}
}
