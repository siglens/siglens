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
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	segmetadata "github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/writer"
	mmeta "github.com/siglens/siglens/pkg/segment/writer/metrics/meta"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
)

const MAXIMUM_WARNINGS_COUNT = 5

const RETENTION_LOOP_SLEEP_MINUTES = 30

const MAX_INODE_USAGE_PERCENT = 85

// Starting the periodic retention based deletion
func InitRetentionCleaner() error {

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
		time.Sleep(RETENTION_LOOP_SLEEP_MINUTES * time.Minute)
		if hook := hooks.GlobalHooks.InternalRetentionCleanerHook2; hook != nil {
			hook(hook1Result, deletionWarningCounter)
		} else {
			DoRetentionBasedDeletion(config.GetCurrentNodeIngestDir(), config.GetRetentionHours(), 0)
			doVolumeBasedDeletion(config.GetCurrentNodeIngestDir(), 60000, deletionWarningCounter)
			doInodeBasedDeletion(config.GetCurrentNodeIngestDir(), deletionWarningCounter)
		}
		if deletionWarningCounter <= MAXIMUM_WARNINGS_COUNT {
			deletionWarningCounter++
		}
	}
}

func DoRetentionBasedDeletion(ingestNodeDir string, retentionHours int, orgid int64) {
	currTime := time.Now()
	deleteBefore := GetRetentionTimeMs(retentionHours, currTime)

	allSegMetas := writer.ReadLocalSegmeta(false)

	// Read metrics meta entries
	currentMetricsMeta := path.Join(ingestNodeDir, mmeta.MetricsMetaSuffix)

	allMetricMetas, err := mmeta.ReadMetricsMeta(currentMetricsMeta)
	if err != nil {
		log.Errorf("DoRetentionBasedDeletion: Failed to get all metric meta entries, FilePath=%v, err: %v", currentMetricsMeta, err)
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

	log.Infof("DoRetentionBasedDeletion: totalsegs=%v, segmentsToDelete=%v, metricsSegmentsToDelete=%v, oldest=%v, orgid=%v, retentionHours: %v",
		len(allEntries), len(segmentsToDelete), len(metricSegmentsToDelete), oldest, orgid, retentionHours)

	// Delete all segment data
	DeleteSegmentData(segmentsToDelete)
	DeleteMetricsSegmentData(currentMetricsMeta, metricSegmentsToDelete)
	DeleteEmptyIndices(ingestNodeDir, orgid)
}

func DeleteEmptyIndices(ingestNodeDir string, myid int64) {
	allIndices, err := vtable.GetVirtualTableNames(myid)
	if err != nil {
		log.Errorf("DeleteEmptyIndices: Error in getting virtual table names, err: %v", err)
		return
	}

	// get table names for unrotated segs
	utn := writer.GetIndexNamesForUnrotated()

	// get table names for recently rotated segs
	rrtn := writer.GetIndexNamesForRecentlyRotated()

	segMetaEntries := writer.ReadLocalSegmeta(false)

	// Create a set of virtualTableName values from segMetaEntries
	virtualTableNames := make(map[string]struct{})
	for _, entry := range segMetaEntries {
		virtualTableNames[entry.VirtualTableName] = struct{}{}
	}

	utils.MergeMapsRetainingFirst(virtualTableNames, utn)
	utils.MergeMapsRetainingFirst(virtualTableNames, rrtn)

	// Iterate over all indices
	for indexName := range allIndices {
		// If an index is not in the set of virtualTableName values from segMetaEntries, delete it
		if _, exists := virtualTableNames[indexName]; !exists {
			log.Infof("DeleteEmptyIndices: deleting unused index: %v", indexName)
			err := vtable.DeleteVirtualTable(&indexName, myid)
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
			writer.RemoveSegmentFromEmptyPqmeta(pqid, segmetaEntry.SegmentKey)
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

	allSegMetas := writer.ReadLocalSegmeta(false)

	currentMetricsMeta := path.Join(ingestNodeDir, mmeta.MetricsMetaSuffix)
	allMetricMetas, err := mmeta.ReadMetricsMeta(currentMetricsMeta)
	if err != nil {
		log.Errorf("doVolumeBasedDeletion: Failed to get all metric meta entries, filepath=%v, err: %v", currentMetricsMeta, err)
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
	DeleteSegmentData(segmentsToDelete)
	DeleteMetricsSegmentData(currentMetricsMeta, metricSegmentsToDelete)
}

func getSystemVolumeBytes() (uint64, error) {
	currentVolume := uint64(0)

	allSegmetas := writer.ReadGlobalSegmetas()

	allCnts := writer.GetVTableCountsForAll(0, allSegmetas)
	writer.GetUnrotatedVTableCountsForAll(0, allCnts)

	for indexName, cnts := range allCnts {
		if indexName == "" {
			log.Errorf("getSystemVolumeBytes: skipping an empty index name indexName=%v", indexName)
			continue
		}

		totalVolumeForIndex := uint64(cnts.BytesCount)
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

func DeleteSegmentData(segmentsToDelete map[string]*structs.SegMeta) {

	if len(segmentsToDelete) == 0 {
		return
	}

	segBaseDirs := make(map[string]struct{}, len(segmentsToDelete))
	for segkey := range segmentsToDelete {
		baseDir, err := utils.GetSegBaseDirFromFilename(segkey)
		if err != nil {
			log.Errorf("DeleteSegmentData: Cannot get segbaseDir from segkey=%v; err=%v", segkey, err)
			continue
		}

		segBaseDirs[baseDir] = struct{}{}
	}

	// 1) First iterate through blob
	for _, segMetaEntry := range segmentsToDelete {

		// Delete segment files from s3
		dirPath := segMetaEntry.SegmentKey
		filesToDelete, err := blob.GetAllFilesInDirectory(path.Dir(dirPath) + "/")
		if err != nil {
			log.Errorf("DeleteSegmentData: Failed to list files in blob directory: %v, error: %v", path.Dir(dirPath), err)
		}
		svFileName := ""
		for _, file := range filesToDelete {
			if strings.Contains(file, utils.SegmentValidityFname) {
				svFileName = file
				continue
			}
			err := blob.DeleteBlob(file)
			if err != nil {
				log.Infof("DeleteSegmentData: Error in deleting segment file %v in blob, its ok prev iteration might have cleaned it, err: %v",
					file, err)
				continue
			}
		}
		if svFileName != "" {
			err := blob.DeleteBlob(svFileName)
			if err != nil {
				log.Infof("DeleteSegmentData: Error deleting validity file %v in blob, , its ok prev iteration might have cleaned it, err: %v",
					svFileName, err)
			}
		}
		log.Infof("DeleteSegmentData: deleted seg blob (if blob enabled): %v", segMetaEntry.SegmentKey)
	}

	// 2) then recursively delete local files
	writer.RemoveSegBasedirs(segBaseDirs)

	// 3) Then from in memory metadata
	for _, segMetaEntry := range segmentsToDelete {
		segmetadata.DeleteSegmentKey(segMetaEntry.SegmentKey)
	}

	// 4) then emptyPqMeta files
	deleteSegmentsFromEmptyPqMetaFiles(segmentsToDelete)

	// 5) Then lastly delete from segmeta.json, because if we remove it from here before deleting from the rest and
	//    there is system restart, these files will stay forever since we do not have segmeta entry from them
	_ = writer.RemoveSegMetas(segmentsToDelete)

}

func DeleteMetricsSegmentData(mmetaFile string, metricSegmentsToDelete map[string]*structs.MetricsMeta) {
	if len(metricSegmentsToDelete) == 0 {
		return
	}

	// Delete segment key from all SiglensMetadata structs
	for _, metricsSegmentMeta := range metricSegmentsToDelete {
		err := segmetadata.DeleteMetricsSegmentKey(metricsSegmentMeta.MSegmentDir)
		if err != nil {
			log.Errorf("deleteMetricsSegmentData: failed to delete metrics segment. Error:%v", err)
			return
		}

		// Delete segment files from s3
		dirPath := metricsSegmentMeta.MSegmentDir
		filesToDelete, err := blob.GetAllFilesInDirectory(path.Dir(dirPath) + "/")
		if err != nil {
			log.Errorf("DeleteSegmentData: Failed to list files in blob directory: %v, error: %v", path.Dir(dirPath), err)
		}

		for _, file := range filesToDelete {
			err := blob.DeleteBlob(file)
			if err != nil {
				log.Infof("deleteMetricsSegmentData: Error in deleting segment file %v in s3", file)
				continue
			}
		}
	}

	mmeta.RemoveMetricsSegments(mmetaFile, metricSegmentsToDelete)

}

func doInodeBasedDeletion(ingestNodeDir string, deletionWarningCounter int) {
	var fsStats syscall.Statfs_t
	dataPath := config.GetDataPath()

	err := syscall.Statfs(filepath.Clean(dataPath), &fsStats)
	if err != nil {
		log.Errorf("doInodeBasedDeletion: Failed to get inode stats: %v", err)
		return
	}

	totalInodes := fsStats.Files
	freeInodes := fsStats.Ffree
	usedInodes := totalInodes - freeInodes

	if totalInodes == 0 {
		log.Errorf("doInodeBasedDeletion: Invalid total inodes count: %d", totalInodes)
		return
	}

	currentUsagePercent := (usedInodes * 100) / totalInodes

	log.Infof("doInodeBasedDeletion: Current inode usage: %v%% (%d used of %d total), Max allowed: %v%%, IngestNodeDir: %v",
		currentUsagePercent, usedInodes, totalInodes, MAX_INODE_USAGE_PERCENT, ingestNodeDir)

	if currentUsagePercent <= MAX_INODE_USAGE_PERCENT {
		return
	}

	if deletionWarningCounter < MAXIMUM_WARNINGS_COUNT {
		log.Warnf("Skipping inode-based deletion since try %d, Current usage: %v%%, Max allowed: %v%%",
			deletionWarningCounter, currentUsagePercent, MAX_INODE_USAGE_PERCENT)
		return
	}

	// Get all segments sorted by time
	allSegMetas := writer.ReadLocalSegmeta(false)
	currentMetricsMeta := path.Join(ingestNodeDir, mmeta.MetricsMetaSuffix)
	allMetricMetas, err := mmeta.ReadMetricsMeta(currentMetricsMeta)
	if err != nil {
		log.Errorf("doInodeBasedDeletion: Failed to get metric meta entries, filepath=%v, err: %v", currentMetricsMeta, err)
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
			timeI = uint64(metricMeta.LatestEpochSec * 1000)
		} else {
			log.Errorf("doInodeBasedDeletion: Unexpected entry type in allEntries: %T", allEntries[i])
			return false
		}

		var timeJ uint64
		if segMeta, ok := allEntries[j].(*structs.SegMeta); ok {
			timeJ = segMeta.LatestEpochMS
		} else if metricMeta, ok := allEntries[j].(*structs.MetricsMeta); ok {
			timeJ = uint64(metricMeta.LatestEpochSec * 1000)
		} else {
			log.Errorf("doInodeBasedDeletion: Unexpected entry type in allEntries: %T", allEntries[j])
			return false
		}
		return timeI < timeJ
	})

	segmentsToDelete := make(map[string]*structs.SegMeta)
	metricSegmentsToDelete := make(map[string]*structs.MetricsMeta)

	targetInodes := uint64(float64(totalInodes) * float64(MAX_INODE_USAGE_PERCENT) / 100.0)
	inodesToFree := usedInodes - targetInodes
	inodesMarked := uint64(0)

	for _, metaEntry := range allEntries {
		if inodesMarked >= inodesToFree {
			break
		}

		switch entry := metaEntry.(type) {
		case *structs.MetricsMeta:
			dirInodes, err := calculateSegmentInodeCount(path.Dir(entry.MSegmentDir))
			if err != nil {
				log.Errorf("doInodeBasedDeletion: Failed to count inodes for metric segment %s: %v", entry.MSegmentDir, err)
				continue
			}
			if inodesMarked+uint64(dirInodes) <= inodesToFree {
				metricSegmentsToDelete[entry.MSegmentDir] = entry
				inodesMarked += uint64(dirInodes)
			}
		case *structs.SegMeta:
			dirInodes, err := calculateSegmentInodeCount(path.Dir(entry.SegmentKey))
			if err != nil {
				log.Errorf("doInodeBasedDeletion: Failed to count inodes for segment %s: %v", entry.SegmentKey, err)
				continue
			}
			if inodesMarked+uint64(dirInodes) <= inodesToFree {
				segmentsToDelete[entry.SegmentKey] = entry
				inodesMarked += uint64(dirInodes)
			}
		}
	}

	log.Infof("doInodeBasedDeletion: Deleting %d segments and %d metric segments to free approximately %d inodes",
		len(segmentsToDelete), len(metricSegmentsToDelete), inodesMarked)

	DeleteSegmentData(segmentsToDelete)
	DeleteMetricsSegmentData(currentMetricsMeta, metricSegmentsToDelete)
}

func calculateSegmentInodeCount(dirPath string) (int, error) {
	inodeCount := 0
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		inodeCount++
		return nil
	})
	if err != nil {
		return 0, err
	}
	return inodeCount, nil
}
