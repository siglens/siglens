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

package query

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/querytracker"
	segmetadata "github.com/siglens/siglens/pkg/segment/metadata"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/writer"
	mmeta "github.com/siglens/siglens/pkg/segment/writer/metrics/meta"

	"github.com/siglens/siglens/pkg/usersavedqueries"
	"github.com/siglens/siglens/pkg/utils"
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

func PopulateMetricsMetadataForTheFile_TestOnly(mFileName string) error {
	metaFileLastModifiedLock.Lock()
	metaFileLastModified[mFileName] = 0
	metaFileLastModifiedLock.Unlock()
	return populateMetricsMetadata(mFileName)
}

func PopulateSegmentMetadataForTheFile_TestOnly(smrFileName string) error {
	metaFileLastModifiedLock.Lock()
	metaFileLastModified[smrFileName] = 0
	metaFileLastModifiedLock.Unlock()
	return populateMicroIndices(smrFileName)
}

func initMetadataRefresh() {
	if config.IsS3Enabled() {
		return
	}
	initSegmentMetaRefresh()
	initMetricsMetaRefresh()
}

func initGlobalMetadataRefresh(getMyIds func() []int64) {
	if !config.IsQueryNode() || !config.IsS3Enabled() {
		return
	}
	err := blob.DownloadAllIngestNodesDir()
	if err != nil {
		log.Errorf("initGlobalMetadataRefresh: Error in downloading ingest nodes dir, err:%v", err)
	}

	ownedSegments := getOwnedSegments()
	err = RefreshGlobalMetadata(getMyIds, ownedSegments, true)
	if err != nil {
		log.Errorf("initGlobalMetadataRefresh: Error in refreshing global metadata, err:%v", err)
	}
}

func RefreshGlobalMetadata(fnMyids func() []int64, ownedSegments map[string]struct{}, shouldDiscardUnownedSegments bool) error {
	ingestNodes := make([]string, 0)
	ingestNodePath := config.GetDataPath() + "ingestnodes"

	files, err := os.ReadDir(ingestNodePath)
	if err != nil {
		log.Errorf("RefreshGlobalMetadata: Error in reading directory, ingestNodePath:%v , err:%v", ingestNodePath, err)
		return err
	}
	for _, file := range files {
		if file.IsDir() {
			ingestNodes = append(ingestNodes, file.Name())
		}
	}

	default_id := int64(0)
	// myids should also include the default_id
	myids := fnMyids()
	if len(myids) == 0 {
		myids = append(myids, default_id)
	}

	if myids[0] != default_id {
		myids = append(myids, default_id)
	}

	allSfmFiles := make([]string, len(ingestNodes))

	myIdToVTableMap := make(map[int64]map[string]struct{}) // myid -> vtableName -> struct{}
	syncLock := &sync.Mutex{}

	var wg sync.WaitGroup

	// For each non current ingest node, we need to process the
	//  segmeta.json and virtualtablenames.txt
	// Aggregate All vtable names for each myid from all ingest nodes
	// Aggregate All segmeta files from all ingest nodes
	for i, node := range ingestNodes {
		allSfmFiles[i] = filepath.Join(config.GetDataPath(), "ingestnodes", node, SEGMETA_FILENAME)

		wg.Add(1)
		go func(ingestNode string) {
			defer wg.Done()

			vTableNamesMap := make(map[int64]map[string]bool)

			for _, myid := range myids {
				vTableFileName := virtualtable.GetFilePathForRemoteNode(ingestNode, myid)
				vTableNamesMap[myid] = make(map[string]bool)
				err := virtualtable.LoadVirtualTableNamesFromFile(vTableFileName, vTableNamesMap[myid])
				if err != nil {
					log.Errorf("RefreshGlobalMetadata: Error in getting vtable names for myid=%d, err:%v", myid, err)
					continue
				}
			}

			syncLock.Lock()
			defer syncLock.Unlock()
			for myid, vTableNames := range vTableNamesMap {
				if _, exists := myIdToVTableMap[myid]; !exists {
					myIdToVTableMap[myid] = make(map[string]struct{})
				}
				utils.AddMapKeysToSet(myIdToVTableMap[myid], vTableNames)
			}

		}(node)
	}

	wg.Wait()

	// Add all vtable names to the global map
	err = virtualtable.BulkAddVirtualTableNames(myIdToVTableMap)
	if err != nil {
		log.Errorf("RefreshGlobalMetadata: Error in adding virtual table names, err:%v", err)
		return err
	}

	// Populate all segmeta files
	for _, smfname := range allSfmFiles {
		wg.Add(1)
		go func(smFile string) {
			defer wg.Done()

			err := populateGlobalMicroIndices(smFile, ownedSegments)
			if err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					log.Errorf("RefreshGlobalMetadata: Error loading initial metadata from file %v: %v", smFile, err)
				}
			}
		}(smfname)
	}

	wg.Wait()

	if shouldDiscardUnownedSegments {
		segmetadata.DiscardUnownedSegments(ownedSegments)
	}

	return err
}

func getOwnedSegments() map[string]struct{} {
	hook := hooks.GlobalHooks.GetOwnedSegmentsHook
	if hook == nil {
		log.Errorf("getOwnedSegments: GetOwnedSegmentsHook is nil")
		return nil
	} else {
		return hook()
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

	allSegMetas := writer.ReadLocalSegmeta(true)

	allSmi := make([]*segmetadata.SegmentMicroIndex, len(allSegMetas))
	for idx, segMetaInfo := range allSegMetas {
		allSmi[idx] = segmetadata.ProcessSegmetaInfo(segMetaInfo)
	}

	// Segmeta entries inside segmeta.json are added in increasing time order.
	// we just reverse this and we get the latest segmeta entry first.
	// This isn't required for correctness; it just avoids more sorting in the
	// common case when we actually update the metadata.
	sort.SliceStable(allSmi, func(i, j int) bool {
		return true
	})

	segmetadata.BulkAddSegmentMicroIndex(allSmi)
	updateLastModifiedTimeForMetaFile(smFile, metaModificationTimeMs)
	return nil
}

func populateGlobalMicroIndices(smFile string, ownedSegments map[string]struct{}) error {

	_, err := os.Stat(smFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		log.Warnf("populateGlobalMicroIndices: error when trying to stat meta file=%+v. Error=%+v", smFile, err)
		return err
	}

	allSegMetas := writer.ReadSegFullMetas(smFile)
	if ownedSegments != nil {
		ownedSegMetas := make([]*structs.SegMeta, 0)
		for _, segMeta := range allSegMetas {
			_, exists := ownedSegments[segMeta.SegmentKey]
			if exists {
				ownedSegMetas = append(ownedSegMetas, segMeta)
			}
		}
		allSegMetas = ownedSegMetas
	}

	allSmi := make([]*segmetadata.SegmentMicroIndex, len(allSegMetas))
	for idx, segMetaInfo := range allSegMetas {
		allSmi[idx] = segmetadata.ProcessSegmetaInfo(segMetaInfo)
	}

	// Segmeta entries inside segmeta.json are added in increasing time order.
	// we just reverse this and we get the latest segmeta entry first.
	// This isn't required for correctness; it just avoids more sorting in the
	// common case when we actually update the metadata.
	sort.SliceStable(allSmi, func(i, j int) bool {
		return true
	})

	segmetadata.BulkAddSegmentMicroIndex(allSmi)
	return nil
}

func syncSegMetaWithSegFullMeta(myId int64) {
	vTableNames, err := virtualtable.GetVirtualTableNames(myId)
	if err != nil {
		log.Errorf("syncSegMetaWithSegFullMeta: Error in getting vtable names, err:%v", err)
		return
	}

	allSmi := make([]*segmetadata.SegmentMicroIndex, 0)

	var ownedSegments map[string]struct{}
	if hook := hooks.GlobalHooks.GetOwnedSegmentsHook; hook != nil {
		ownedSegments = hook()
	}

	for vTableName := range vTableNames {
		streamid := utils.CreateStreamId(vTableName, myId)
		vTableBaseDir := config.GetBaseVTableDir(streamid, vTableName)

		filesInDir, err := os.ReadDir(vTableBaseDir)
		if err != nil {
			log.Errorf("syncSegMetaWithSegFullMeta: Error in reading directory, vTableBaseDir:%v , err:%v", vTableBaseDir, err)
			continue
		}

		for _, file := range filesInDir {
			fileName := file.Name()
			segkey := config.GetSegKeyFromVTableDir(vTableBaseDir, fileName)
			if ownedSegments != nil {
				if _, exists := ownedSegments[segkey]; !exists {
					continue
				}
			}
			_, exists := segmetadata.GetMicroIndex(segkey)
			if exists {
				continue
			}

			smi, err := readSegFullMetaFileAndPopulate(segkey)
			if err != nil {
				log.Errorf("syncSegMetaWithSegFullMeta: Error populating segfullmeta, err:%v", err)
				continue
			}

			allSmi = append(allSmi, smi)
		}
	}

	// sort from latest to oldest
	sort.Slice(allSmi, func(i, j int) bool {
		return allSmi[i].SegMeta.LatestEpochMS > allSmi[j].SegMeta.LatestEpochMS
	})

	segmetadata.BulkAddSegmentMicroIndex(allSmi)

	smiCount := len(allSmi)
	segMetaSlice := make([]*structs.SegMeta, smiCount)
	for idx, smi := range allSmi {
		reverseIdx := smiCount - idx - 1
		segMetaSlice[reverseIdx] = &smi.SegMeta
	}

	writer.BulkAddRotatedSegmetas(segMetaSlice, false)
	log.Infof("syncSegMetaWithSegFullMeta: Added %d segmeta entries", smiCount)
}

func readSegFullMetaFileAndPopulate(segKey string) (*segmetadata.SegmentMicroIndex, error) {
	sfmData, err := writer.ReadSfm(segKey)
	if err != nil {
		return nil, fmt.Errorf("readSegFullMetaFileAndPopulate: Error in reading segfullmeta file, err:%v", err)
	}

	segMeta := sfmData.SegMeta
	segMeta.ColumnNames = sfmData.ColumnNames
	segMeta.AllPQIDs = sfmData.AllPQIDs

	smi := segmetadata.ProcessSegmetaInfo(segMeta)

	return smi, nil
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

	allMetricsSegmentMeta := make([]*segmetadata.MetricsSegmentMetadata, 0)
	for _, mMetaInfo := range allMetricsMetas {
		currMSegMetadata := segmetadata.InitMetricsMicroIndex(mMetaInfo)
		allMetricsSegmentMeta = append(allMetricsSegmentMeta, currMSegMetadata)
	}

	segmetadata.BulkAddMetricsSegment(allMetricsSegmentMeta)
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

func getExternalUSQueriesInfo(orgid int64) ([]string, error) {
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
		usqFileExtensionName = "/usqinfo-" + strconv.FormatInt(orgid, 10) + ".bin"
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

func internalQueryInfoRefresh(getMyIds func() []int64) {
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

func runQueryInfoRefreshLoop(getMyIds func() []int64) {
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
