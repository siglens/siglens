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
	"time"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	pqsmeta "github.com/siglens/siglens/pkg/segment/query/pqs/meta"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

const ONE_MiB = 1024 * 1024
const PQS_TICKER = 10 // seconds
const PQS_FLUSH_SIZE = 100
const PQS_CHAN_SIZE = 1000

var smrLock sync.RWMutex = sync.RWMutex{}
var localSegmetaFname string

var SegmetaFilename = "segmeta.json"

type PQSChanMeta struct {
	pqid                  string
	segKey                string
	writeToSegFullMeta    bool
	writeToEmptyPqMeta    bool
	deleteFromEmptyPqMeta bool
}

var pqsChan = make(chan PQSChanMeta, PQS_CHAN_SIZE)

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
		} else {
			log.Errorf("initSmr: failed to open a new filename=%v: err=%v", localSegmetaFname, err)
		}
		return
	}
	fd.Close()

	// start a go routine to listen on the channel
	go listenBackFillAndEmptyPQSRequests()
}

func getSegFullMetaFnameFromSegkey(segkey string) string {
	return fmt.Sprintf("%s.sfm", segkey)
}

func ReadSegmeta(smFilename string) []*structs.SegMeta {
	smrLock.RLock()
	retVal, err := getAllSegmetas(smFilename)
	smrLock.RUnlock()
	if err != nil {
		log.Errorf("ReadSegmeta: getallsegmetas err=%v ", err)
	}
	return retVal
}

// read only the current node's segmeta
func ReadLocalSegmeta(readFullMeta bool) []*structs.SegMeta {

	smrLock.RLock()
	retVal, err := getAllSegmetas(localSegmetaFname)
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
		sfmData, _ := readSfm(smentry.SegmentKey)
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

func readSfm(segkey string) (*structs.SegFullMeta, error) {

	sfmFname := getSegFullMetaFnameFromSegkey(segkey)

	sfmBytes, err := os.ReadFile(sfmFname)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Errorf("readSfm: Cannot read sfm File: %v, err: %v", sfmFname, err)
		}
		return nil, err
	}
	sfm := &structs.SegFullMeta{}
	if err := json.Unmarshal(sfmBytes, sfm); err != nil {
		log.Errorf("readSfm: Error unmarshalling sfm file: %v, data: %v err: %v",
			sfmFname, string(sfmBytes), err)
		return nil, err
	}
	return sfm, nil
}

func writeSfm(segkey string, sfmData *structs.SegFullMeta) {

	// create a separate individual file for SegFullMeta
	sfmFname := getSegFullMetaFnameFromSegkey(segkey)
	sfmFd, err := os.OpenFile(sfmFname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("writeSfm: failed to open a sfm filename=%v: err=%v", sfmFname, err)
		return
	}
	defer sfmFd.Close()

	sfmJson, err := json.Marshal(*sfmData)
	if err != nil {
		log.Errorf("writeSfm: failed to Marshal sfmData: %v, sfmFname: %v, err: %v",
			sfmData, sfmFname, err)
		return
	}
	if _, err := sfmFd.Write(sfmJson); err != nil {
		log.Errorf("writeSfm: failed to write sfm: %v: err: %v", sfmFname, err)
		return
	}

	err = sfmFd.Sync()
	if err != nil {
		log.Errorf("writeSfm: failed to sync sfm: %v: err: %v", sfmFname, err)
		return
	}
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
		mDir := path.Join(ingestDir, iNode, SegmetaFilename)
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
	return config.GetSmrBaseDir() + SegmetaFilename
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
	if err := scanner.Err(); err != nil {
		return allSegMetaMap, utils.TeeErrorf("getAllSegmetaToMap: Error scanning file %v: %v", segMetaFilename, err)
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

	sfmData := &structs.SegFullMeta{ColumnNames: segmeta.ColumnNames, AllPQIDs: segmeta.AllPQIDs}

	writeSfm(segmeta.SegmentKey, sfmData)

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

	if segkeysToRemove == nil && indexName == "" {
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

func BulkBackFillPQSSegmetaEntries(segkey string, pqidMap map[string]bool) {
	sfmData, err := readSfm(segkey)
	if err != nil {
		return
	}

	// it could be nil, if we didn't have any pqs data or the previous version,
	// we used to have pqs in segmeta.json, from this version onwards, we will
	// add it in segment specific file
	if sfmData == nil {
		sfmData = &structs.SegFullMeta{}
	}

	if sfmData.AllPQIDs == nil {
		sfmData.AllPQIDs = make(map[string]bool)
	}

	utils.MergeMapsRetainingFirst(sfmData.AllPQIDs, pqidMap)

	writeSfm(segkey, sfmData)
}

func BackFillPQSSegmetaEntry(segkey string, newpqid string) {
	BulkBackFillPQSSegmetaEntries(segkey, map[string]bool{newpqid: true})
}

// AddToBackFillAndEmptyPQSChan adds a new pqid to the channel
// Adds the pqid to the segfullmeta file for the given segkey
// if writeToEmptyPqMeta is true, then it will write the EmptyResults for this pqid
func AddToBackFillAndEmptyPQSChan(segkey string, newpqid string, writeToEmptyPqMeta bool) {
	pqsChan <- PQSChanMeta{pqid: newpqid, segKey: segkey, writeToSegFullMeta: true, writeToEmptyPqMeta: writeToEmptyPqMeta}
}

// AddToEmptyPqmetaChan adds a new pqid to the channel
// if writeToEmptyPQIDMeta is true, then it will write the EmptyResults for this pqid
func AddToEmptyPqmetaChan(pqid string, segKey string) {
	pqsChan <- PQSChanMeta{pqid: pqid, segKey: segKey, writeToEmptyPqMeta: true}
}

// RemoveSegmentFromEmptyPqmetaChan adds a new pqid to the channel
// This will remove the segment from the emptyPQIDMeta when this entry is processed
func RemoveSegmentFromEmptyPqmeta(pqid string, segKey string) {
	pqsChan <- PQSChanMeta{pqid: pqid, segKey: segKey, deleteFromEmptyPqMeta: true}
}

func listenBackFillAndEmptyPQSRequests() {
	// Listen on the channel, every PQS_TICKER seconds or if the size of the channel is PQS_FLUSH_SIZE,
	// it would get all the data in the channel and then do the process of Backfilling PQMR files.
	// This is to avoid multiple writes to the same file

	ticker := time.NewTicker(PQS_TICKER * time.Second) // every 10 seconds
	defer ticker.Stop()

	buffer := make([]PQSChanMeta, PQS_FLUSH_SIZE)
	bufferIndex := 0

	for {
		select {
		case pqsChanMeta := <-pqsChan:
			buffer[bufferIndex] = pqsChanMeta
			bufferIndex++
			if bufferIndex == PQS_FLUSH_SIZE {
				processBackFillAndEmptyPQSRequests(buffer)
				bufferIndex = 0
			}
		case <-ticker.C:
			if bufferIndex > 0 {
				processBackFillAndEmptyPQSRequests(buffer[:bufferIndex])
				bufferIndex = 0
			}
		}
	}
}

func processBackFillAndEmptyPQSRequests(pqsRequests []PQSChanMeta) {
	if len(pqsRequests) == 0 {
		return
	}

	// segKey -> pqid -> true ; Contains all PQIDs for a given segKey
	segKeyToAllPQIDsMap := make(map[string]map[string]bool)

	// pqid -> segKey -> true ; For empty PQS: Contains all empty segment Keys for a given pqid
	pqidToEmptySegMap := make(map[string]map[string]bool)

	// pqid -> segKey -> true ; For empty PQS: Contains all segment Keys to delete for a given pqid
	emptyPqidSegKeysToDeleteMap := make(map[string]map[string]bool)

	for _, pqsRequest := range pqsRequests {
		if pqsRequest.writeToSegFullMeta {
			allPqidsMap := utils.GetOrCreateNestedMap(segKeyToAllPQIDsMap, pqsRequest.segKey)
			allPqidsMap[pqsRequest.pqid] = true
		}

		// For empty PQS: Check if we need to write to emptyPQIDMeta or delete from it
		if pqsRequest.writeToEmptyPqMeta {
			segKeyMap := utils.GetOrCreateNestedMap(pqidToEmptySegMap, pqsRequest.pqid)
			segKeyMap[pqsRequest.segKey] = true

			// Check if this segkey already exists in the delete Map, if so, remove it.
			utils.RemoveKeyFromNestedMap(emptyPqidSegKeysToDeleteMap, pqsRequest.pqid, pqsRequest.segKey)
		} else if pqsRequest.deleteFromEmptyPqMeta {
			deleteSegKeyMap := utils.GetOrCreateNestedMap(emptyPqidSegKeysToDeleteMap, pqsRequest.pqid)
			deleteSegKeyMap[pqsRequest.segKey] = true

			// Check if this segkey already exists in the write of emptyPQIDMeta, if so, remove it.
			utils.RemoveKeyFromNestedMap(pqidToEmptySegMap, pqsRequest.pqid, pqsRequest.segKey)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		for segKey, allPQIDs := range segKeyToAllPQIDsMap {
			BulkBackFillPQSSegmetaEntries(segKey, allPQIDs)
		}
	}()

	go func() {
		defer wg.Done()

		for pqid, segKeyMap := range pqidToEmptySegMap {
			pqsmeta.BulkAddEmptyResults(pqid, segKeyMap)
		}

		for pqid, segKeyMap := range emptyPqidSegKeysToDeleteMap {
			pqsmeta.BulkDeleteSegKeysFromPqid(pqid, segKeyMap)
		}
	}()

	wg.Wait()
}

func DeletePQSData() error {

	foundPqsidsInSegMeta := false
	smrLock.Lock()
	segmetaEntries, err := getAllSegmetas(localSegmetaFname)
	if err != nil {
		log.Errorf("DeletePQSData: failed to get segmeta data from %v, err: %v",
			localSegmetaFname, err)
		smrLock.Unlock()
		return err
	}
	for _, smEntry := range segmetaEntries {
		if len(smEntry.AllPQIDs) > 0 {
			foundPqsidsInSegMeta = true
		}
		smEntry.AllPQIDs = nil
	}
	// Old version of Siglens will have pqsids in segmeta.json, now remove it from there
	if foundPqsidsInSegMeta {
		err := writeOverSegMeta(segmetaEntries)
		if err != nil {
			log.Errorf("DeletePQSData: failed to write segmeta.json, err: %v", err)
			smrLock.Unlock()
			return err
		}
	}
	smrLock.Unlock()

	// Remove all PQMR directories and pqsids file contents from segments
	for _, smEntry := range segmetaEntries {
		pqmrDir := GetPQMRDirFromSegKey(smEntry.SegmentKey)
		err := os.RemoveAll(pqmrDir)
		if err != nil {
			log.Errorf("DeletePQSData: failed to remove pqmr dir %v, err: %v", pqmrDir, err)
			return err
		}

		sfmData := &structs.SegFullMeta{ColumnNames: smEntry.ColumnNames, AllPQIDs: nil}
		writeSfm(smEntry.SegmentKey, sfmData)
	}

	// Delete PQS meta directory
	return pqsmeta.DeletePQMetaDir()
}

func writeOverSegMeta(segMetaEntries []*structs.SegMeta) error {
	fd, err := os.OpenFile(localSegmetaFname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("writeOverSegMeta: Failed to open SegMetaFile name=%v, err:%v", localSegmetaFname, err)
	}
	defer fd.Close()

	for _, smentry := range segMetaEntries {

		segmetajson, err := json.Marshal(*smentry)
		if err != nil {
			return utils.TeeErrorf("writeOverSegMeta: failed to Marshal: segmeta filename=%v: err=%v smentry: %v", localSegmetaFname, err, *smentry)
		}

		if _, err := fd.Write(segmetajson); err != nil {
			return fmt.Errorf("writeOverSegMeta: failed to write segmeta filename=%v: err=%v", localSegmetaFname, err)
		}

		if _, err := fd.WriteString("\n"); err != nil {
			return fmt.Errorf("writeOverSegMeta: failed to write newline filename=%v: err=%v", localSegmetaFname, err)
		}
	}

	return nil
}
