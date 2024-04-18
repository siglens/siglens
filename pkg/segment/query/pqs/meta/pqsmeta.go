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

package pqsmeta

import (
	"encoding/json"
	"os"
	"path"
	"sync"

	"github.com/siglens/siglens/pkg/config"
	log "github.com/sirupsen/logrus"
)

var allEmptyPersistentQueryResultsLock sync.RWMutex

func init() {
	allEmptyPersistentQueryResultsLock = sync.RWMutex{}
}

func GetAllEmptySegmentsForPqid(pqid string) (map[string]bool, error) {
	fileName := getPqmetaFilename(pqid)
	return getAllEmptyPQSToMap(fileName)
}

func getAllEmptyPQSToMap(emptyPQSFilename string) (map[string]bool, error) {
	allEmptyPQS := make(map[string]bool)
	allEmptyPersistentQueryResultsLock.RLock()
	defer allEmptyPersistentQueryResultsLock.RUnlock()
	fd, err := os.OpenFile(emptyPQSFilename, os.O_CREATE|os.O_RDONLY, 0764)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]bool{}, nil
		}
		log.Errorf("getAllEmptyPQSToMap: Cannot read persistent query meta File = %v, err= %v", emptyPQSFilename, err)
		return nil, err
	}
	defer fd.Close()

	fileInfo, err := fd.Stat()
	if err != nil {
		log.Errorf("getAllEmptyPQSToMap: Cannot stat filename file=%v, =%v", emptyPQSFilename, err)
		return nil, err
	}
	if fileInfo.Size() == 0 {
		return allEmptyPQS, nil
	}

	err = json.NewDecoder(fd).Decode(&allEmptyPQS)
	if err != nil {
		log.Errorf("getAllEmptyPQSToMap: Cannot unmarshal data, err =%v", err)
		return nil, err
	}

	return allEmptyPQS, nil
}

func getPqmetaDirectory() string {
	dirName := config.GetDataPath() + "querynodes" + "/" + config.GetHostID() + "/" + "pqmeta"
	return dirName
}

func getPqmetaFilename(pqid string) string {
	dirName := getPqmetaDirectory()
	fileName := dirName + "/" + pqid + ".meta"
	return fileName
}

func AddEmptyResults(pqid string, segKey string, virtualTableName string) {
	dirName := getPqmetaDirectory()
	if _, err := os.Stat(dirName); os.IsNotExist(err) {
		err := os.MkdirAll(dirName, os.FileMode(0764))
		if err != nil {
			log.Errorf("Failed to create directory at %s: %v", dirName, err)
		}
	}
	fileName := getPqmetaFilename(pqid)
	emptyPQS, err := getAllEmptyPQSToMap(fileName)
	if err != nil {
		log.Errorf("Failed to get empty PQS data from file at %s: %v", fileName, err)
	}
	if emptyPQS != nil {
		emptyPQS[segKey] = true
		writeEmptyPqsMapToFile(fileName, emptyPQS)
	}
}

func writeEmptyPqsMapToFile(fileName string, emptyPqs map[string]bool) {
	allEmptyPersistentQueryResultsLock.Lock()
	defer allEmptyPersistentQueryResultsLock.Unlock()
	fd, err := os.OpenFile(fileName, os.O_RDWR|os.O_TRUNC, 0764)
	if err != nil {
		log.Errorf("WriteEmptyPQS: Error opening file at fname=%v, err=%v", fileName, err)
	}
	jsonData, err := json.Marshal(emptyPqs)
	if err != nil {
		log.Errorf("WriteEmptyPQS: could not marshal data, err=%v", err)
	}
	_, err = fd.Write(jsonData)
	if err != nil {
		log.Errorf("WriteEmptyPQS: buf write failed fname=%v, err=%v", fileName, err)
	}

	err = fd.Sync()
	if err != nil {
		log.Errorf("WriteEmptyPQS: sync failed filename=%v,err=%v", fileName, err)
	}
	fd.Close()
}

func removePqmrFilesAndDirectory(pqid string) error {
	workingDirectory, err := os.Getwd()
	if err != nil {
		log.Errorf("Error fetching current workingDirectory")
		return err
	}
	pqFname := workingDirectory + "/" + getPqmetaFilename(pqid)
	err = os.Remove(pqFname)
	if err != nil {
		log.Errorf("Cannot delete file at %v", err)
		return err
	}
	pqMetaDirectory := path.Dir(pqFname)
	files, err := os.ReadDir(pqMetaDirectory)
	if err != nil {
		log.Errorf("Cannot PQMR directory at %v", pqMetaDirectory)
		return err
	}
	if len(files) == 0 {
		err := os.Remove(pqMetaDirectory)
		if err != nil {
			log.Errorf("Error deleting Pqmr directory at %v", pqMetaDirectory)
			return err
		}
	}
	return nil
}

func DeleteSegmentFromPqid(pqid string, segKey string) {
	pqFname := getPqmetaFilename(pqid)
	emptyPQS, err := getAllEmptyPQSToMap(pqFname)
	if err != nil {
		log.Errorf("DeleteSegmentFromPqid: Failed to get empty PQS data from file at %s: %v", pqFname, err)
	}
	delete(emptyPQS, segKey)
	if len(emptyPQS) == 0 {
		err := removePqmrFilesAndDirectory(pqid)
		if err != nil {
			log.Errorf("DeleteSegmentFromPqid: Error removing segKey %v from %v pqid", segKey, pqid)
		}
		return
	}
	writeEmptyPqsMapToFile(pqFname, emptyPQS)
}
