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
	"os"
	"path"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	log "github.com/sirupsen/logrus"
)

var SegmetaSuffix = "segmeta.json"

// read only the current nodes segmeta
func ReadLocalSegmeta() []*structs.SegMeta {
	smrLock.Lock()
	defer smrLock.Unlock()

	segMetaFilename := GetLocalSegmetaFName()
	retVal, err := getAllSegmetas(segMetaFilename)
	if err != nil {
		log.Errorf("ReadLocalSegmeta: getallsegmetas err=%v ", err)
	}
	return retVal
}

// returns all segmetas downloaded, including the current nodes segmeta and all global segmetas
func ReadAllSegmetas() []*structs.SegMeta {
	smrLock.Lock()
	defer smrLock.Unlock()

	ingestDir := config.GetIngestNodeBaseDir()
	files, err := os.ReadDir(ingestDir)
	if err != nil {
		log.Errorf("ReadAllSegmetas: read dir err=%v ", err)
		return make([]*structs.SegMeta, 0)
	}

	iNodes := make([]string, 0)
	for _, file := range files {
		fName := file.Name()
		iNodes = append(iNodes, fName)
	}

	allSegmetas := make([]string, 0)
	for _, iNode := range iNodes {
		mDir := path.Join(ingestDir, iNode, SegmetaSuffix)
		if _, err := os.Stat(mDir); err != nil {
			continue
		}
		allSegmetas = append(allSegmetas, mDir)
	}

	allVals := make(map[string]*structs.SegMeta)
	for _, fName := range allSegmetas {
		allSegMetaMap, err := getAllSegmetaToMap(fName)
		if err != nil {
			log.Errorf("ReadAllSegmetas: getallsegmeta err=%v ", err)
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

func ReadSegmeta(smFname string) ([]*structs.SegMeta, error) {
	smrLock.Lock()
	defer smrLock.Unlock()
	retVal, err := getAllSegmetas(smFname)
	if err != nil {
		log.Errorf("ReadSegmeta: getsegmetas err=%v ", err)
		return nil, err
	}
	return retVal, nil
}

// returns the current nodes segmeta
func GetLocalSegmetaFName() string {
	return config.GetSmrBaseDir() + SegmetaSuffix
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

	return allSegMetas, nil
}

func GetVTableCounts(vtableName string, orgid uint64) (uint64, int, uint64) {

	bytesCount := uint64(0)
	recordCount := 0
	onDiskBytesCount := uint64(0)

	allSegmetas := ReadAllSegmetas()
	for _, segmeta := range allSegmetas {
		if segmeta == nil {
			continue
		}
		if segmeta.VirtualTableName == vtableName && segmeta.OrgId == orgid {
			bytesCount += segmeta.BytesReceivedCount
			recordCount += segmeta.RecordCount
			onDiskBytesCount += segmeta.OnDiskBytes
		}
	}
	return bytesCount, recordCount, onDiskBytesCount
}

func GetVTableCountsForAll(orgid uint64) map[string]*structs.VtableCounts {

	allvtables := make(map[string]*structs.VtableCounts)

	allSegmetas := ReadAllSegmetas()

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
