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

package meta

import (
	"bufio"
	"encoding/json"
	"os"
	"path"
	"sync"

	"github.com/siglens/siglens/pkg/common/fileutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	log "github.com/sirupsen/logrus"
)

/**

	This module exposes require functions to read/write/query the hosts' metricsmeta.json

**/

var MetricsMetaSuffix = "metricmeta.json"

var mMetaLock *sync.RWMutex = &sync.RWMutex{}
var localMetricsMeta string

func GetLocalMetricsMetaFName() string {
	return config.GetSmrBaseDir() + MetricsMetaSuffix
}

func InitMetricsMeta() error {
	localMetricsMeta = GetLocalMetricsMetaFName()
	return nil
}

func AddMetricsMetaEntry(entry *structs.MetricsMeta) error {
	mMetaLock.Lock()
	defer mMetaLock.Unlock()
	fd, err := os.OpenFile(localMetricsMeta, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("AddMetricsMetaEntry: failed to open filename=%v: err=%v", localMetricsMeta, err)
		return err
	}

	defer fd.Close()

	rawMeta, err := json.Marshal(entry)
	if err != nil {
		log.Errorf("AddMetricsMetaEntry: failed to Marshal: err=%v", err)
		return err
	}

	if _, err := fd.Write(rawMeta); err != nil {
		log.Errorf("AddMetricsMetaEntry: failed to write segmeta filename=%v: err=%v", localMetricsMeta, err)
		return err
	}

	if _, err := fd.WriteString("\n"); err != nil {
		log.Errorf("AddMetricsMetaEntry: failed to write newline filename=%v: err=%v", localMetricsMeta, err)
		return err
	}
	err = fd.Sync()
	if err != nil {
		log.Errorf("AddMetricsMetaEntry: failed to sync filename=%v: err=%v", localMetricsMeta, err)
		return err
	}

	return nil
}

func GetLocalMetricsMetaEntries() (map[string]*structs.MetricsMeta, error) {
	return ReadMetricsMeta(localMetricsMeta)
}

func ReadMetricsMeta(mmeta string) (map[string]*structs.MetricsMeta, error) {
	mMetaLock.RLock()
	defer mMetaLock.RUnlock()
	fd, err := os.OpenFile(mmeta, os.O_APPEND|os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]*structs.MetricsMeta{}, nil
		}
		log.Errorf("GetLocalMetricsMetaEntries: failed to open filename=%v: err=%v", mmeta, err)
		return nil, err
	}

	defer fd.Close()

	retVal := make(map[string]*structs.MetricsMeta)
	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		rawbytes := scanner.Bytes()
		var mMeta structs.MetricsMeta
		err := json.Unmarshal(rawbytes, &mMeta)
		if err != nil {
			log.Errorf("GetLocalMetricsMetaEntries: Cannot unmarshal data = %v, err= %v", string(rawbytes), err)
			continue
		}
		retVal[mMeta.MSegmentDir] = &mMeta
	}

	return retVal, nil
}

// includes remote metrics meta entries
func GetAllMetricsMetaEntries(orgid uint64) (map[string]*structs.MetricsMeta, error) {
	nDir := config.GetIngestNodeBaseDir()
	files, err := os.ReadDir(nDir)
	if err != nil {
		log.Errorf("ReadAllSegmetas: read dir err=%v ", err)
		return make(map[string]*structs.MetricsMeta, 0), nil
	}

	// read all mmetas
	iNodes := make([]string, 0)
	for _, file := range files {
		fName := file.Name()
		iNodes = append(iNodes, fName)
	}

	allSegmetas := make([]string, 0)
	for _, iNode := range iNodes {
		mDir := path.Join(nDir, iNode, MetricsMetaSuffix)
		if _, err := os.Stat(mDir); err != nil {
			continue
		}
		allSegmetas = append(allSegmetas, mDir)
	}

	retVal := make(map[string]*structs.MetricsMeta)
	for _, fName := range allSegmetas {
		mMetas, err := ReadMetricsMeta(fName)
		if err != nil {
			log.Errorf("ReadAllSegmetas: read segmeta err=%v ", err)
			continue
		}

		for k, v := range mMetas {
			if v.OrgId == orgid {
				retVal[k] = v
			}
		}
	}
	return retVal, nil
}

func RemoveMetricsSegments(mmetaName string, metricsSegmentsToDelete map[string]*structs.MetricsMeta) {
	mMetaLock.Lock()
	defer mMetaLock.Unlock()

	removeMetricsSegmentsByList(mmetaName, metricsSegmentsToDelete)
}

func removeMetricsSegmentsByList(metricsMetaFile string, metricsSegmentsToDelete map[string]*structs.MetricsMeta) {

	if metricsSegmentsToDelete == nil {
		return
	}

	preservedEntries := make([]*structs.MetricsMeta, 0)
	tagsTreeToDelete := make(map[string]bool)

	entriesRead := 0
	entriesRemoved := 0
	fd, err := os.OpenFile(metricsMetaFile, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("removeMetricsSegmentsByList: Failed to open metrics meta file name=%v, err:%v", metricsMetaFile, err)
		return
	}
	defer fd.Close()

	reader := bufio.NewScanner(fd)
	for reader.Scan() {
		metricSegmentMeta := structs.MetricsMeta{}
		err = json.Unmarshal(reader.Bytes(), &metricSegmentMeta)
		if err != nil {
			log.Errorf("removeMetricsSegmentsByList: Failed to unmarshal metrics meta file=%v, err:%v", metricsMetaFile, err)
			continue
		}
		entriesRead++

		_, ok := metricsSegmentsToDelete[metricSegmentMeta.MSegmentDir]
		if !ok {
			preservedEntries = append(preservedEntries, &metricSegmentMeta)
			continue
		}
		entriesRemoved++
		dir := path.Dir(metricSegmentMeta.MSegmentDir)
		if err := os.RemoveAll(dir); err != nil {
			log.Errorf("removeMetricsSegmentsByList: Failed to remove directory name=%v, err:%v",
				metricSegmentMeta.MSegmentDir, err)
		}
		fileutils.RecursivelyDeleteEmptyParentDirectories(dir)

		if _, ok := tagsTreeToDelete[metricSegmentMeta.TTreeDir]; !ok {
			tagsTreeToDelete[metricSegmentMeta.TTreeDir] = true
		}
	}
	if entriesRemoved > 0 {
		// if we removed entries and there was nothing preserved then we must delete this metrics meta file
		if len(preservedEntries) == 0 {
			if err := os.RemoveAll(metricsMetaFile); err != nil {
				log.Errorf("removeMetricsSegmentsByList: Failed to remove metrics meta file name=%v, err:%v", metricsMetaFile, err)
			}
		} else {
			wfd, err := os.OpenFile(metricsMetaFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Errorf("removeMetricsSegmentsByList: Failed to open temp metrics meta file name=%v, err:%v", metricsMetaFile, err)
				return
			}
			defer wfd.Close()
			for _, mentry := range preservedEntries {
				// delete tags trees for removed segments
				// if ttdir existed in tagsTreeToDelete, that means a preserved metrics entry still will use this ttree
				delete(tagsTreeToDelete, mentry.TTreeDir)

				msegjson, err := json.Marshal(*mentry)
				if err != nil {
					log.Errorf("removeMetricsSegmentsByList: failed to Marshal: err=%v", err)
					return
				}

				if _, err := wfd.Write(msegjson); err != nil {
					log.Errorf("removeMetricsSegmentsByList: failed to write metrics meta filename=%v: err=%v", metricsMetaFile, err)
					return
				}

				if _, err := wfd.WriteString("\n"); err != nil {
					log.Errorf("removeMetricsSegmentsByList: failed to write new line to metrics meta filename=%v: err=%v", metricsMetaFile, err)
					return
				}
			}
		}
	}
	for ttreeDir := range tagsTreeToDelete {
		dir := path.Dir(ttreeDir)
		if err := os.RemoveAll(ttreeDir); err != nil {
			log.Errorf("removeMetricsSegmentsByList: Failed to remove tags tree directory=%v, err:%v", ttreeDir, err)
		}
		fileutils.RecursivelyDeleteEmptyParentDirectories(dir)
	}
}
