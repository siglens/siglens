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

package writer

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

const ONE_MiB = 1024 * 1024

var smrLock sync.RWMutex = sync.RWMutex{}
var localSegmetaFname string

var segmetaFilename = "segmeta.json"

func initSmr() {

	localSegmetaFname = GetLocalSegmetaFName()

	fd, err := os.OpenFile(localSegmetaFname, os.O_RDONLY, 0666)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			// for first time during bootup this will occur
			_, err := os.OpenFile(localSegmetaFname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Errorf("initSmr: failed to open a new filename=%v: err=%v", localSegmetaFname, err)
				return
			}
		}
		return
	}
	fd.Close()
}

func getSegFullMetaFnameFromSegkey(segkey string) string {
	return fmt.Sprintf("%s.sfm", segkey)
}

// read only the current nodes segmeta
func ReadLocalSegmeta(readFullMeta bool) []*structs.SegMeta {

	segMetaFilename := GetLocalSegmetaFName()
	smrLock.RLock()
	retVal, err := getAllSegmetas(segMetaFilename)
	smrLock.RUnlock()
	if err != nil {
		log.Errorf("ReadLocalSegmeta: getallsegmetas err=%v ", err)
		return retVal
	}

	if !readFullMeta {
		return retVal
	}

	// continue reading/merging from individual segfiles
	for _, smentry := range retVal {
		sfmData := readSfm(smentry.SegmentKey)
		if sfmData == nil {
			continue
		}
		if smentry.AllPQIDs == nil {
			smentry.AllPQIDs = sfmData.AllPQIDs
		} else {
			utils.MergeMapsRetainingFirst(smentry.AllPQIDs, sfmData.AllPQIDs)
		}
		if smentry.ColumnNames == nil {
			smentry.ColumnNames = sfmData.ColumnNames
		} else {
			utils.MergeMapsRetainingFirst(smentry.ColumnNames, sfmData.ColumnNames)
		}
	}
	return retVal
}

func readSfm(segkey string) *structs.SegFullMeta {

	sfmFname := getSegFullMetaFnameFromSegkey(segkey)

	sfmBytes, err := os.ReadFile(sfmFname)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Errorf("readSfm: Cannot read sfm File: %v, err: %v", sfmFname, err)
		}
		return nil
	}
	sfm := &structs.SegFullMeta{}
	if err := json.Unmarshal(sfmBytes, sfm); err != nil {
		log.Errorf("readSfm: Error unmarshalling sfm file: %v, data: %v err: %v",
			sfmFname, string(sfmBytes), err)
		return nil
	}
	return sfm
}

// returns all segmetas downloaded, including the current nodes segmeta and all global segmetas
func ReadGlobalSegmetas() []*structs.SegMeta {
	smrLock.RLock()
	defer smrLock.RUnlock()

	ingestDir := config.GetIngestNodeBaseDir()
	files, err := os.ReadDir(ingestDir)
	if err != nil {
		log.Errorf("ReadGlobalSegmetas: read dir err=%v ", err)
		return make([]*structs.SegMeta, 0)
	}

	iNodes := make([]string, 0)
	for _, file := range files {
		fName := file.Name()
		iNodes = append(iNodes, fName)
	}

	allSegmetas := make([]string, 0)
	for _, iNode := range iNodes {
		mDir := path.Join(ingestDir, iNode, segmetaFilename)
		if _, err := os.Stat(mDir); err != nil {
			continue
		}
		allSegmetas = append(allSegmetas, mDir)
	}

	allVals := make(map[string]*structs.SegMeta)
	for _, fName := range allSegmetas {
		allSegMetaMap, err := getAllSegmetaToMap(fName)
		if err != nil {
			log.Errorf("ReadGlobalSegmetas: getallsegmeta err=%v ", err)
			return make([]*structs.SegMeta, 0)
		}
		for k, v := range allSegMetaMap {
			allVals[k] = v
		}
	}
	retVal := make([]*structs.SegMeta, 0, len(allVals))
	idx := 0
	for _, v := range allVals {
		retVal = append(retVal, v)
		idx++
	}
	return retVal[:idx]
}

// returns the current nodes segmeta
func GetLocalSegmetaFName() string {
	return config.GetSmrBaseDir() + segmetaFilename
}

func getAllSegmetaToMap(segMetaFilename string) (map[string]*structs.SegMeta, error) {
	allSegMetaMap := make(map[string]*structs.SegMeta)

	fd, err := os.OpenFile(segMetaFilename, os.O_RDONLY, 0666)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return allSegMetaMap, nil
		}
		log.Errorf("getAllSegmetaToMap: Cannot read input Segmeta File = %v, err= %v", segMetaFilename, err)
		return allSegMetaMap, err
	}
	defer fd.Close()
	scanner := bufio.NewScanner(fd)

	for scanner.Scan() {
		rawbytes := scanner.Bytes()
		var segmeta structs.SegMeta
		err := json.Unmarshal(rawbytes, &segmeta)
		if err != nil {
			log.Errorf("getAllSegmetaToMap: Cannot unmarshal data = %v, err= %v", string(rawbytes), err)
			continue
		}
		allSegMetaMap[segmeta.SegmentKey] = &segmeta
	}
	return allSegMetaMap, nil
}

func getAllSegmetas(segMetaFilename string) ([]*structs.SegMeta, error) {

	allSegMetas := make([]*structs.SegMeta, 0)

	fd, err := os.OpenFile(segMetaFilename, os.O_RDONLY, 0666)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []*structs.SegMeta{}, nil
		}
		log.Errorf("getAllSegmetas: Cannot read input Segmeta File = %v, err= %v", segMetaFilename, err)
		return allSegMetas, err
	}
	defer fd.Close()
	scanner := bufio.NewScanner(fd)
	buf := make([]byte, ONE_MiB)
	scanner.Buffer(buf, ONE_MiB)

	for scanner.Scan() {
		rawbytes := scanner.Bytes()
		var segmeta structs.SegMeta
		err := json.Unmarshal(rawbytes, &segmeta)
		if err != nil {
			log.Errorf("getAllSegmetas: Cannot unmarshal data = %v, err= %v", string(rawbytes), err)
			continue
		}
		allSegMetas = append(allSegMetas, &segmeta)
	}

	err = scanner.Err()
	if err != nil {
		log.Errorf("getAllSegmetas: scanning err: %v", err)
		return allSegMetas, err
	}

	return allSegMetas, nil
}

func GetVTableCountsForAll(orgid uint64, allSegmetas []*structs.SegMeta) map[string]*structs.VtableCounts {

	allvtables := make(map[string]*structs.VtableCounts)

	var ok bool
	var cnts *structs.VtableCounts
	for _, segmeta := range allSegmetas {
		if segmeta == nil {
			continue
		}
		if segmeta.OrgId != orgid && orgid != 10618270676840840323 { //orgid for siglens
			continue
		}
		cnts, ok = allvtables[segmeta.VirtualTableName]
		if !ok {
			cnts = &structs.VtableCounts{}
			allvtables[segmeta.VirtualTableName] = cnts
		}
		cnts.BytesCount += segmeta.BytesReceivedCount
		cnts.RecordCount += uint64(segmeta.RecordCount)
		cnts.OnDiskBytesCount += segmeta.OnDiskBytes
	}
	return allvtables
}

func addNewRotatedSegmeta(segmeta structs.SegMeta) {
	if hook := hooks.GlobalHooks.AddSegMeta; hook != nil {
		alreadyHandled, err := hook(&segmeta)
		if err != nil {
			log.Errorf("AddNewRotatedSegmeta: hook failed, err=%v", err)
			return
		}

		if alreadyHandled {
			return
		}
	}

	addSegmeta(segmeta)
}

func AddOrReplaceRotatedSegmeta(segmeta structs.SegMeta) {
	removeSegmetas(map[string]struct{}{segmeta.SegmentKey: struct{}{}}, "")
	addSegmeta(segmeta)
}

func addSegmeta(segmeta structs.SegMeta) {

	// create a separate individual file for SegFullMeta
	sfmFname := getSegFullMetaFnameFromSegkey(segmeta.SegmentKey)
	sfmFd, err := os.OpenFile(sfmFname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("addSegmeta: failed to open a sfm filename=%v: err=%v", sfmFname, err)
		return
	}
	defer sfmFd.Close()

	sfmData := structs.SegFullMeta{ColumnNames: segmeta.ColumnNames, AllPQIDs: segmeta.AllPQIDs}
	sfmJson, err := json.Marshal(sfmData)
	if err != nil {
		log.Errorf("addSegmeta: failed to Marshal sfmData: %v, sfmFname: %v, err: %v",
			sfmData, sfmFname, err)
		return
	}
	if _, err := sfmFd.Write(sfmJson); err != nil {
		log.Errorf("addSegmeta: failed to write sfm: %v: err: %v", sfmFname, err)
		return
	}

	err = sfmFd.Sync()
	if err != nil {
		log.Errorf("addSegmeta: failed to sync sfm: %v: err: %v", sfmFname, err)
		return
	}

	segmetajson, err := json.Marshal(segmeta)
	if err != nil {
		log.Errorf("addSegmeta: failed to Marshal: err=%v", err)
		return
	}
	segmetajson = append(segmetajson, "\n"...)

	smrLock.Lock()
	defer smrLock.Unlock()

	fd, err := os.OpenFile(localSegmetaFname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fd, err = os.OpenFile(localSegmetaFname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Errorf("addSegmeta: failed to open a new filename=%v: err=%v", localSegmetaFname, err)
				return
			}

		} else {
			log.Errorf("addSegmeta: failed to open filename=%v: err=%v", localSegmetaFname, err)
			return
		}
	}
	defer fd.Close()

	if _, err := fd.Write(segmetajson); err != nil {
		log.Errorf("addSegmeta: failed to write segmeta filename=%v: err=%v", localSegmetaFname, err)
		return
	}

	err = fd.Sync()
	if err != nil {
		log.Errorf("addSegmeta: failed to sync filename=%v: err=%v", localSegmetaFname, err)
		return
	}
}

// Removes segmetas based on given segkeys and returns the segbasedirs for those segkeys
func removeSegmetas(segkeysToRemove map[string]struct{}, indexName string) map[string]struct{} {

	if segkeysToRemove == nil {
		return nil
	}

	segbaseDirs := make(map[string]struct{})
	preservedSmEntries := make([]*structs.SegMeta, 0)

	smrLock.Lock()
	defer smrLock.Unlock()

	fr, err := os.OpenFile(localSegmetaFname, os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("removeSegmetas: Failed to open SegMetaFile name=%v, err:%v", localSegmetaFname, err)
		return segbaseDirs
	}
	defer fr.Close()

	reader := bufio.NewScanner(fr)
	buf := make([]byte, ONE_MiB)
	reader.Buffer(buf, ONE_MiB)

	for reader.Scan() {
		segMetaData := structs.SegMeta{}
		err = json.Unmarshal(reader.Bytes(), &segMetaData)
		if err != nil {
			log.Errorf("removeSegmetas: Failed to unmarshal fileName=%v, err:%v", localSegmetaFname, err)
			continue
		}

		if indexName != "" {
			if segMetaData.VirtualTableName != indexName {
				preservedSmEntries = append(preservedSmEntries, &segMetaData)
				continue
			}
		} else {
			// check if based on segmetas
			_, ok := segkeysToRemove[segMetaData.SegmentKey]
			if !ok {
				preservedSmEntries = append(preservedSmEntries, &segMetaData)
				continue
			}
		}

		segbaseDirs[segMetaData.SegbaseDir] = struct{}{}
	}
	err = reader.Err()
	if err != nil {
		log.Errorf("removeSegmetas: scanning err: %v", err)
		return nil
	}

	// we couldn't find segmetas to delete just return
	if len(segbaseDirs) == 0 {
		return segbaseDirs
	}

	// if we removed entries and there was nothing preserved then we must delete this segmetafile
	if len(preservedSmEntries) == 0 {
		if err := os.RemoveAll(localSegmetaFname); err != nil {
			log.Errorf("removeSegmetas: Failed to remove smfile name=%v, err:%v", localSegmetaFname, err)
		}
		return nil
	}

	fd, err := os.OpenFile(localSegmetaFname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("removeSegmetas: Failed to open SegMetaFile name=%v, err:%v", localSegmetaFname, err)
		return nil
	}
	defer fd.Close()

	for _, smentry := range preservedSmEntries {

		segmetajson, err := json.Marshal(*smentry)
		if err != nil {
			log.Errorf("removeSegmetas: failed to Marshal: err=%v", err)
			return nil
		}

		segmetajson = append(segmetajson, "\n"...)

		if _, err := fd.Write(segmetajson); err != nil {
			log.Errorf("removeSegmetas: failed to write segmeta filename=%v: err=%v", localSegmetaFname, err)
			return nil
		}
	}

	return segbaseDirs
}
