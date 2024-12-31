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

	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	pqsmeta "github.com/siglens/siglens/pkg/segment/query/pqs/meta"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
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

type SegmentSizeStats struct {
	TotalCmiSize  uint64
	TotalCsgSize  uint64
	NumIndexFiles int
	NumBlocks     int64
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

func GetSegFullMetaFnameFromSegkey(segkey string) string {
	return fmt.Sprintf("%s.sfm", segkey)
}

func ReadSegmeta(smFilename string) []*structs.SegMeta {
	smrLock.RLock()
	segMetas, err := getAllSegmetas(smFilename)
	smrLock.RUnlock()
	if err != nil {
		log.Errorf("ReadSegmeta: getallsegmetas err=%v ", err)
	}

	return segMetas
}

func ReadSegFullMetas(smFilename string) []*structs.SegMeta {
	smrLock.RLock()
	segMetas, err := getAllSegmetas(smFilename)
	smrLock.RUnlock()
	if err != nil {
		log.Errorf("ReadSegFullMetas: getallsegmetas err=%v ", err)
	}

	readSfmForSegMetas(segMetas)

	return segMetas
}

func readSfmForSegMetas(segmetas []*structs.SegMeta) {
	// continue reading/merging from individual segfiles
	for _, smentry := range segmetas {
		workSfm, err := ReadSfm(smentry.SegmentKey)
		if err != nil {
			// error is logged in the func
			continue
		}
		if smentry.AllPQIDs == nil {
			smentry.AllPQIDs = workSfm.AllPQIDs
		} else {
			utils.MergeMapsRetainingFirst(smentry.AllPQIDs, workSfm.AllPQIDs)
		}
		if smentry.ColumnNames == nil {
			smentry.ColumnNames = workSfm.ColumnNames
		} else {
			utils.MergeMapsRetainingFirst(smentry.ColumnNames, workSfm.ColumnNames)
		}
	}
}

// read only the current node's segmeta
func ReadLocalSegmeta(readFullMeta bool) []*structs.SegMeta {

	smrLock.RLock()
	segMetas, err := getAllSegmetas(localSegmetaFname)
	smrLock.RUnlock()
	if err != nil {
		log.Errorf("ReadLocalSegmeta: getallsegmetas err=%v ", err)
		return segMetas
	}

	if !readFullMeta {
		return segMetas
	}

	readSfmForSegMetas(segMetas)

	return segMetas
}

func ReadSfm(segkey string) (*structs.SegFullMeta, error) {

	sfm := &structs.SegFullMeta{}

	sfmFname := GetSegFullMetaFnameFromSegkey(segkey)

	sfmBytes, err := os.ReadFile(sfmFname)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Errorf("readSfm: Cannot read sfm File: %v, err: %v", sfmFname, err)
			return sfm, err
		}

		// check from blob
		err = blob.DownloadSegmentBlob(sfmFname, true)
		if err != nil {
			log.Errorf("readSfm: Cannot read sfm File from blob: %v, err: %v", sfmFname, err)
			return sfm, err
		}

		sfmBytes, err = os.ReadFile(sfmFname)
		if err != nil {
			log.Errorf("readSfm: Cannot read sfm File after download: %v, err: %v", sfmFname, err)
			return sfm, err
		}
	}

	if err := json.Unmarshal(sfmBytes, sfm); err != nil {
		log.Errorf("readSfm: Error unmarshalling sfm file: %v, data: %v err: %v",
			sfmFname, string(sfmBytes), err)
		return sfm, err
	}
	return sfm, nil
}

func writeSfm(segkey string, sfmData *structs.SegFullMeta) {

	// create a separate individual file for SegFullMeta
	sfmFname := GetSegFullMetaFnameFromSegkey(segkey)
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
		allSegMetaMap, err := GetAllSegmetaToMap(fName)
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

func GetAllSegmetaToMap(segMetaFilename string) (map[string]*structs.SegMeta, error) {
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

func AddOrReplaceRotatedSegmeta(segmeta structs.SegMeta) {
	removeSegmetas(map[string]struct{}{segmeta.SegmentKey: struct{}{}}, "")
	addSegmeta(segmeta)
}

func addSegmeta(segmeta structs.SegMeta) {
	BulkAddRotatedSegmetas([]*structs.SegMeta{&segmeta}, true)
}

func BulkAddRotatedSegmetas(finalSegmetas []*structs.SegMeta, shouldWriteSfm bool) {
	if len(finalSegmetas) == 0 {
		return
	}

	if shouldWriteSfm {
		for _, segmeta := range finalSegmetas {
			sfmData := &structs.SegFullMeta{
				SegMeta:     segmeta,
				ColumnNames: segmeta.ColumnNames,
				AllPQIDs:    segmeta.AllPQIDs,
			}

			writeSfm(segmeta.SegmentKey, sfmData)
		}
	}

	var allSegmetaJson []byte
	for _, segmeta := range finalSegmetas {
		segmetaJson, err := json.Marshal(segmeta)
		if err != nil {
			log.Errorf("bulkAddSegmetas: failed to Marshal: err=%v", err)
			continue
		}
		segmetaJson = append(segmetaJson, "\n"...)
		allSegmetaJson = append(allSegmetaJson, segmetaJson...)
	}

	if len(allSegmetaJson) == 0 {
		return
	}

	smrLock.Lock()
	defer smrLock.Unlock()

	fd, err := os.OpenFile(localSegmetaFname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			fd, err = os.OpenFile(localSegmetaFname, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Errorf("bulkAddSegmetas: failed to open a new filename=%v: err=%v", localSegmetaFname, err)
				return
			}

		} else {
			log.Errorf("bulkAddSegmetas: failed to open filename=%v: err=%v", localSegmetaFname, err)
			return
		}
	}
	defer fd.Close()

	if _, err := fd.Write(allSegmetaJson); err != nil {
		log.Errorf("bulkAddSegmetas: failed to write segmeta filename=%v: err=%v", localSegmetaFname, err)
		return
	}

	err = fd.Sync()
	if err != nil {
		log.Errorf("bulkAddSegmetas: failed to sync filename=%v: err=%v", localSegmetaFname, err)
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

	sfmData, err := ReadSfm(segkey)
	if err != nil {
		return
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

	pqmrFiles := make(map[string]struct{})

	for _, pqsRequest := range pqsRequests {
		if pqsRequest.writeToSegFullMeta {
			pqidFileName := pqmr.GetPQMRFileNameFromSegKey(pqsRequest.segKey, pqsRequest.pqid)
			pqmrFiles[pqidFileName] = struct{}{}

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

	if hook := hooks.GlobalHooks.UploadPQMRFilesExtrasHook; hook != nil {
		err := hook(toputils.GetKeysOfMap(pqmrFiles))
		if err != nil {
			log.Errorf("processBackFillAndEmptyPQSRequests: failed at UploadPQMRFilesExtrasHook: %v", err)
		}
	}
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

		sfmData := &structs.SegFullMeta{
			SegMeta:     smEntry,
			ColumnNames: smEntry.ColumnNames,
			AllPQIDs:    nil,
		}
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

func calculateSegmentSizes(segmentKey string) (*SegmentSizeStats, error) {
	sfm, err := ReadSfm(segmentKey)
	if err != nil {
		return nil, fmt.Errorf("calculateSegmentSizes: failed to read sfm for segment %s: %v", segmentKey, err)
	}

	stats := &SegmentSizeStats{}
	for _, colInfo := range sfm.ColumnNames {
		stats.TotalCmiSize += colInfo.CmiSize
		stats.TotalCsgSize += colInfo.CsgSize
		if colInfo.CmiSize > 0 {
			stats.NumIndexFiles++
		}
		if colInfo.CsgSize > 0 {
			stats.NumIndexFiles++
		}
	}
	return stats, nil
}

func GetIndexSizeStats(indexName string, orgId uint64) (*utils.IndexStats, error) {
	allSegMetas := ReadGlobalSegmetas()
	stats := &utils.IndexStats{}

	type result struct {
		stats *SegmentSizeStats
		err   error
	}

	ch := make(chan result, len(allSegMetas))
	var wg sync.WaitGroup

	for _, meta := range allSegMetas {
		if meta.VirtualTableName != indexName || (meta.OrgId != orgId && orgId != 10618270676840840323) {
			continue
		}

		wg.Add(1)
		go func(segmentKey string, numBlocks uint16) {
			defer wg.Done()
			segStats, err := calculateSegmentSizes(segmentKey)
			if err == nil {
				segStats.NumBlocks = int64(numBlocks)
			}
			ch <- result{segStats, err}
		}(meta.SegmentKey, meta.NumBlocks)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for res := range ch {
		if res.err != nil {
			return nil, res.err
		}
		stats.TotalCmiSize += res.stats.TotalCmiSize
		stats.TotalCsgSize += res.stats.TotalCsgSize
		stats.NumIndexFiles += res.stats.NumIndexFiles
		stats.NumBlocks += res.stats.NumBlocks
	}

	unrotatedStats := getUnrotatedSegmentStats(indexName, orgId)
	stats.TotalCmiSize += unrotatedStats.TotalCmiSize
	stats.TotalCsgSize += unrotatedStats.TotalCsgSize
	stats.NumIndexFiles += unrotatedStats.NumIndexFiles
	stats.NumBlocks += unrotatedStats.NumBlocks

	return stats, nil
}

func getUnrotatedSegmentStats(indexName string, orgId uint64) *SegmentSizeStats {
	UnrotatedInfoLock.RLock()
	defer UnrotatedInfoLock.RUnlock()

	stats := &SegmentSizeStats{}
	for _, usi := range AllUnrotatedSegmentInfo {
		if usi.TableName == indexName &&
			(usi.orgid == orgId || orgId == 10618270676840840323) {
			if usi.cmiSize > 0 {
				stats.TotalCmiSize += usi.cmiSize
				stats.NumIndexFiles += len(usi.allColumns)
			}
			stats.NumIndexFiles += len(usi.allColumns)
			stats.NumBlocks += int64(len(usi.blockSummaries))
		}
	}
	return stats
}
