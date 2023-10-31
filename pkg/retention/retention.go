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

package retention

import (
	"math"
	"path"
	"sort"
	"time"

	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/common/fileutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	pqsmeta "github.com/siglens/siglens/pkg/segment/query/pqs/meta"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/writer"
	mmeta "github.com/siglens/siglens/pkg/segment/writer/metrics/meta"
	log "github.com/sirupsen/logrus"
)

const MAXIMUM_WARNINGS_COUNT = 5

// Starting the periodic retention based deletion
func InitRetentionCleaner() error {
	go internalRetentionCleaner()
	return nil
}

func internalRetentionCleaner() {
	time.Sleep(1 * time.Minute) // sleep for 1min for the rest of the system to come up
	deletionWarningCounter := 0
	for {
		doRetentionBasedDeletion(config.GetCurrentNodeIngestDir(), config.GetRetentionHours(), 0)
		deletionWarningCounter++
		time.Sleep(1 * time.Hour)
	}
}

func doRetentionBasedDeletion(ingestNodeDir string, retentionHours int, orgid uint64) {
	currTime := time.Now()
	deleteBefore := getRetentionTimeMs(retentionHours, currTime)

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
	deleteSegmentData(currentSegmeta, segmentsToDelete, true)
	deleteMetricsSegmentData(currentMetricsMeta, metricSegmentsToDelete, true)
}

func getRetentionTimeMs(retentionHours int, currTime time.Time) uint64 {
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

func deleteSegmentData(segmetaFile string, segmentsToDelete map[string]*structs.SegMeta, updateBlob bool) {

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

func deleteMetricsSegmentData(mmetaFile string, metricSegmentsToDelete map[string]*structs.MetricsMeta, updateBlob bool) {
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
