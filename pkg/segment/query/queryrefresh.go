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

package query

import (
	"bufio"
	"errors"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/query/pqs"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/writer"
	mmeta "github.com/siglens/siglens/pkg/segment/writer/metrics/meta"

	"github.com/siglens/siglens/pkg/usersavedqueries"
	"github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
)

const SECONDS_REREAD_META = 5
const SECONDS_REREAD_META_SSR = 60
const SECONDS_REFRESH_GLOBAL_METADATA = 30
const SEGMETA_FILENAME = "/segmeta.json"

var metaFileLastModifiedLock sync.RWMutex
var metaFileLastModified = make(map[string]uint64) // maps meta file name to the epoch time of last modification

func initSegmentMetaRefresh() {
	smFile := writer.GetLocalSegmetaFName()
	err := populateMicroIndices(smFile)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Errorf("initSegmentMetaRefresh:Error loading initial metadata from file %v: %v", smFile, err)
		}
	}
	go refreshLocalMetadataLoop()
}

func initMetricsMetaRefresh() {
	mFile := mmeta.GetLocalMetricsMetaFName()
	err := populateMetricsMetadata(mFile)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Errorf("initMetricsMetaRefresh:Error loading initial metadata from file %v: %v", mFile, err)
		}
	}
	go refreshMetricsMetadataLoop()
}

func initMetadataRefresh() {
	initSegmentMetaRefresh()
	initMetricsMetaRefresh()
}

func updateVTable(vfname string, orgid uint64) error {
	vtableFd, err := os.OpenFile(vfname, os.O_RDONLY, 0644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		log.Errorf("updateVTable: Failed to open file=%v, err=%v", vfname, err)
		return err
	}
	defer func() {
		err = vtableFd.Close()
		if err != nil {
			log.Errorf("updateVTable: Failed to close file name=%v, err:%v", vfname, err)
		}
	}()
	scanner := bufio.NewScanner(vtableFd)

	for scanner.Scan() {
		rawbytes := scanner.Bytes()
		vtableName := string(rawbytes)
		if vtableName != "" {
			// todo: confirm if this is correct
			err = virtualtable.AddVirtualTable(&vtableName, orgid)
			if err != nil {
				log.Errorf("updateVTable: Error in adding virtual table:%v, err:%v", &vtableName, err)
				return err
			}
		}
	}
	return err
}

func initGlobalMetadataRefresh(getMyIds func() []uint64) {
	if !config.IsQueryNode() || !config.IsS3Enabled() {
		return
	}

	err := refreshGlobalMetadata(getMyIds)
	if err != nil {
		log.Errorf("initGlobalMetadataRefresh: Error in refreshing global metadata, err:%v", err)
	}
	go refreshGlobalMetadataLoop(getMyIds)
}

func refreshGlobalMetadata(fnMyids func() []uint64) error {
	err := blob.DownloadAllIngestNodesDir()
	if err != nil {
		log.Errorf("refreshGlobalMetadataLoop: Error in downloading ingest nodes dir, err:%v", err)
		return err
	}

	ingestNodes := make([]string, 0)
	ingestNodePath := config.GetDataPath() + "ingestnodes"

	files, err := os.ReadDir(ingestNodePath)

	if err != nil {
		log.Errorf("refreshGlobalMetadataLoop: Error in reading directory, ingestNodePath:%v , err:%v", ingestNodePath, err)
		return err
	}
	for _, file := range files {
		if file.IsDir() {
			if strings.Contains(file.Name(), config.GetHostID()) {
				continue
			}
			ingestNodes = append(ingestNodes, file.Name())
		}
	}
	myids := fnMyids()

	// For each non current ingest node, we need to process the
	//  segmeta.json and virtualtablenames.txt
	var wg sync.WaitGroup
	for _, n := range ingestNodes {
		wg.Add(1)
		go func(node string) {
			defer wg.Done()
			vfname := virtualtable.GetFilePathForRemoteNode(node, 0)
			err := updateVTable(vfname, 0)
			if err != nil {
				log.Errorf("refreshGlobalMetadataLoop: Error updating default org vtable, err:%v", err)
			}
			for _, myid := range myids {
				vfname := virtualtable.GetFilePathForRemoteNode(node, myid)
				err := updateVTable(vfname, myid)
				if err != nil {
					log.Errorf("refreshGlobalMetadataLoop: Error in refreshing vtable for myid=%d  err:%v", myid, err)
				}
			}
			// Call populateMicroIndices for all read segmeta.json
			var smFile strings.Builder
			smFile.WriteString(config.GetDataPath() + "ingestnodes/" + node)
			smFile.WriteString(SEGMETA_FILENAME)
			smfname := smFile.String()
			err = populateMicroIndices(smfname)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					log.Errorf("refreshGlobalMetadataLoop: Error loading initial metadata from file %v: %v", smfname, err)
				}
			}
		}(n)
	}
	wg.Wait()
	return err
}

func refreshGlobalMetadataLoop(getMyIds func() []uint64) {
	for {
		err := refreshGlobalMetadata(getMyIds)
		if err != nil {
			log.Errorf("refreshGlobalMetadataLoop: Error in refreshing global metadata, err:%v", err)
		}
		time.Sleep(SECONDS_REFRESH_GLOBAL_METADATA * time.Second)
	}
}

func populateMicroIndices(smFile string) error {

	var metaModificationTimeMs uint64

	log.Debugf("populateMicroIndices: reading smFile=%v", smFile)
	fileInfo, err := os.Stat(smFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		log.Warnf("populateMicroIndices: error when trying to stat meta file=%+v. Error=%+v", smFile, err)
		return err
	}
	metaModificationTimeMs = uint64(fileInfo.ModTime().UTC().Unix() * 1000)
	lastTimeMetafileRefreshed := getLastModifiedTimeForMetaFile(smFile)

	if lastTimeMetafileRefreshed >= metaModificationTimeMs {
		log.Debugf("populateMicroIndices: not updating meta file %+v. As file was not updated after last refresh", smFile)
		return nil
	}

	allSegMetas, err := writer.ReadSegmeta(smFile)
	if err != nil {
		log.Errorf("populateMicroIndices: error when trying to read meta file=%+v. Error=%+v", smFile, err)
		return err
	}

	allSmi := make([]*metadata.SegmentMicroIndex, len(allSegMetas))
	for idx, segMetaInfo := range allSegMetas {
		allSmi[idx] = processSegmetaInfo(segMetaInfo)
	}

	// segmeta entries inside segmeta.json are added in increasing time order. we just reverse this and we get
	// the latest segmeta entry first
	sort.SliceStable(allSmi, func(i, j int) bool {
		return true
	})

	metadata.BulkAddSegmentMicroIndex(allSmi)
	updateLastModifiedTimeForMetaFile(smFile, metaModificationTimeMs)
	return nil
}

func populateMetricsMetadata(mName string) error {
	var metaModificationTimeMs uint64

	log.Infof("populateMetricsMetadata: reading smFile=%v", mName)
	fileInfo, err := os.Stat(mName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		log.Warnf("populateMetricsMetadata: error when trying to stat meta file=%+v. Error=%+v", mName, err)
		return err
	}
	metaModificationTimeMs = uint64(fileInfo.ModTime().UTC().Unix() * 1000)
	lastTimeMetafileRefreshed := getLastModifiedTimeForMetaFile(mName)

	if lastTimeMetafileRefreshed >= metaModificationTimeMs {
		log.Debugf("populateMetricsMetadata: not updating meta file %+v. As file was not updated after last refresh", mName)
		return nil
	}

	allMetricsMetas, err := mmeta.GetLocalMetricsMetaEntries()
	if err != nil {
		log.Errorf("populateMetricsMetadata: unable to get all the metrics meta entries. Error: %v", err)
		return err
	}

	allMetricsSegmentMeta := make([]*metadata.MetricsSegmentMetadata, 0)
	for _, mMetaInfo := range allMetricsMetas {
		currMSegMetadata := metadata.InitMetricsMicroIndex(mMetaInfo)
		allMetricsSegmentMeta = append(allMetricsSegmentMeta, currMSegMetadata)
	}

	metadata.BulkAddMetricsSegment(allMetricsSegmentMeta)
	updateLastModifiedTimeForMetaFile(mName, metaModificationTimeMs)
	return nil
}

func getLastModifiedTimeForMetaFile(metaFilename string) uint64 {
	metaFileLastModifiedLock.RLock()
	defer metaFileLastModifiedLock.RUnlock()
	mModTime, present := metaFileLastModified[metaFilename]

	if !present {
		return 0
	}
	return mModTime
}

func refreshMetricsMetadataLoop() {
	for {
		time.Sleep(SECONDS_REREAD_META * time.Second)
		mmFile := mmeta.GetLocalMetricsMetaFName()
		fileInfo, err := os.Stat(mmFile)
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			log.Errorf("refreshMetricsMetadataLoop: Cannot stat meta file while re-reading, err= %v", err)
			continue
		}
		modifiedTime := fileInfo.ModTime()
		modifiedTimeMillisec := uint64(modifiedTime.UTC().Unix() * 1000)
		lastModified := getLastModifiedTimeForMetaFile(mmFile)
		if modifiedTimeMillisec > lastModified {
			log.Debugf("refreshMetricsMetadataLoop: Meta file has been modified %+v %+v. filePath = %+v", modifiedTimeMillisec, lastModified, mmFile)
			err := populateMetricsMetadata(mmFile)
			if err != nil {
				log.Errorf("refreshMetricsMetadataLoop: failed to populate micro indices from %+v: %+v", mmFile, err)
			}
			updateLastModifiedTimeForMetaFile(mmFile, modifiedTimeMillisec)
		}
	}
}

func refreshLocalMetadataLoop() {
	err := blob.DownloadAllIngestNodesDir()
	if err != nil {
		log.Errorf("refreshGlobalMetadataLoop: Error in downloading ingest nodes dir, err:%v", err)
		return
	}

	for {
		time.Sleep(SECONDS_REREAD_META * time.Second)
		smFile := writer.GetLocalSegmetaFName()
		fileInfo, err := os.Stat(smFile)
		if errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			log.Errorf("refreshLocalMetadataLoop: Cannot stat meta file while re-reading, err= %v", err)
			continue
		}
		modifiedTime := fileInfo.ModTime()
		modifiedTimeMillisec := uint64(modifiedTime.UTC().Unix() * 1000)
		lastModified := getLastModifiedTimeForMetaFile(smFile)
		if modifiedTimeMillisec > lastModified {
			log.Debugf("refreshLocalMetadataLoop: Meta file has been modified %+v %+v. filePath = %+v", modifiedTimeMillisec, lastModified, smFile)
			err := populateMicroIndices(smFile)
			if err != nil {
				log.Errorf("refreshLocalMetadataLoop: failed to populate micro indices from %+v: %+v", smFile, err)
			}
			updateLastModifiedTimeForMetaFile(smFile, modifiedTimeMillisec)
		}
	}
}

func updateLastModifiedTimeForMetaFile(metaFilename string, newTime uint64) {
	metaFileLastModifiedLock.Lock()
	defer metaFileLastModifiedLock.Unlock()
	metaFileLastModified[metaFilename] = newTime
}

func processSegmetaInfo(segMetaInfo *structs.SegMeta) *metadata.SegmentMicroIndex {
	for pqid := range segMetaInfo.AllPQIDs {
		pqs.AddPersistentQueryResult(segMetaInfo.SegmentKey, segMetaInfo.VirtualTableName, pqid)
	}

	return metadata.InitSegmentMicroIndex(segMetaInfo)
}

func getExternalPqinfoFiles() ([]string, error) {
	fNames := make([]string, 0)
	queryNodes := make([]string, 0)
	querytNodePath := config.GetDataPath() + "querynodes"

	files, err := os.ReadDir(querytNodePath)
	if err != nil {
		log.Errorf("getExternalPqinfoFiles: Error in downloading query nodes dir,err:%v", err)
		return nil, err
	}
	for _, file := range files {
		if file.IsDir() {
			if strings.Contains(file.Name(), config.GetHostID()) {
				continue
			}
			queryNodes = append(queryNodes, file.Name())
		}
	}

	for _, node := range queryNodes {
		var pqInfoFile strings.Builder
		pqInfoFile.WriteString(config.GetDataPath() + "querynodes/" + node + "/pqueries")
		baseDir := pqInfoFile.String()

		pqInfoFilename := baseDir + "/pqinfo.bin"
		fNames = append(fNames, pqInfoFilename)
	}
	return fNames, nil
}

func getExternalUSQueriesInfo(orgid uint64) ([]string, error) {
	fNames := make([]string, 0)
	queryNodes := make([]string, 0)
	querytNodePath := config.GetDataPath() + "querynodes"

	files, err := os.ReadDir(querytNodePath)
	if err != nil {
		log.Errorf("getExternalUSQueriesInfo: Error in downloading query nodes dir,err:%v", err)
		return nil, err
	}
	for _, file := range files {
		if file.IsDir() {
			if strings.Contains(file.Name(), config.GetHostID()) {
				continue
			}
			queryNodes = append(queryNodes, file.Name())
		}
	}

	var usqFileExtensionName string
	if orgid == 0 {
		usqFileExtensionName = "/usqinfo.bin"
	} else {
		usqFileExtensionName = "/usqinfo-" + strconv.FormatUint(orgid, 10) + ".bin"
	}

	for _, node := range queryNodes {
		var usqInfoFile strings.Builder
		usqInfoFile.WriteString(config.GetDataPath() + "querynodes/" + node + "/usersavedqueries")
		baseDir := usqInfoFile.String()

		usqInfoFilename := baseDir + usqFileExtensionName
		fNames = append(fNames, usqInfoFilename)
	}
	return fNames, nil
}

func internalQueryInfoRefresh(getMyIds func() []uint64) {
	err := blob.DownloadAllQueryNodesDir()
	if err != nil {
		log.Errorf("internalQueryInfoRefresh: Error in downloading query nodes dir, err:%v", err)
		return
	}
	pqInfoFiles, err := getExternalPqinfoFiles()
	if err != nil {
		log.Errorf("internalQueryInfoRefresh: Error in getting external pqinfo files, err:%v", err)
		return
	}
	if len(pqInfoFiles) > 0 {
		err = querytracker.RefreshExternalPQInfo(pqInfoFiles)
		if err != nil {
			log.Errorf("internalQueryInfoRefresh: Error in refreshing external pqinfo files, err:%v", err)
			return
		}
	}

	allMyids := getMyIds()

	for _, myid := range allMyids {
		usqInfoFiles, err := getExternalUSQueriesInfo(myid)
		if err != nil {
			log.Errorf("internalQueryInfoRefresh: Error in getting external usqinfo Files, err:%v", err)
			return
		}
		for _, file := range usqInfoFiles {
			err := usersavedqueries.ReadExternalUSQInfo(file, myid)
			if err != nil {
				log.Errorf("internalQueryInfoRefresh: Error in reading external usqinfo file:%v, err:%v", file, err)
				continue
			}
		}
	}

	aggsInfoFiles, err := GetExternalAggsInfoFiles()
	if err != nil {
		log.Errorf("internalQueryInfoRefresh: Error in getting external aggs files, err:%v", err)
		return
	}
	if len(aggsInfoFiles) > 0 {
		err = querytracker.RefreshExternalAggsInfo(aggsInfoFiles)
		if err != nil {
			log.Errorf("internalQueryInfoRefresh: Error in refreshing external aggs files, err:%v", err)
			return
		}
	}
}

func runQueryInfoRefreshLoop(getMyIds func() []uint64) {
	for {

		startTime := time.Now()
		internalQueryInfoRefresh(getMyIds)
		sleep := time.Duration(QUERY_INFO_REFRESH_LOOP_SECS - time.Since(startTime))
		if sleep < 0 {
			time.Sleep(60 * time.Second)
		} else {
			time.Sleep(sleep * time.Second)
		}

	}
}

func GetExternalAggsInfoFiles() ([]string, error) {
	fNames := make([]string, 0)
	queryNodes := make([]string, 0)
	querytNodePath := config.GetDataPath() + "querynodes"

	files, err := os.ReadDir(querytNodePath)
	if err != nil {
		log.Errorf("GetExternalAggsinfoFiles: Error in downloading query nodes dir,err:%v", err)
		return nil, err
	}
	for _, file := range files {
		if file.IsDir() {
			if strings.Contains(file.Name(), config.GetHostID()) {
				continue
			}
			queryNodes = append(queryNodes, file.Name())
		}
	}

	for _, node := range queryNodes {
		var aggsInfoFile strings.Builder
		aggsInfoFile.WriteString(config.GetDataPath() + "querynodes/" + node + "/pqueries")
		baseDir := aggsInfoFile.String()

		aggsInfoFilename := baseDir + "/aggsinfo.bin"
		fNames = append(fNames, aggsInfoFilename)
	}
	return fNames, nil
}
