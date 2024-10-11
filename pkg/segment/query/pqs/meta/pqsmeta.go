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
	"path/filepath"

	"github.com/siglens/siglens/pkg/config"
	log "github.com/sirupsen/logrus"
)

var pqMetaDirName string

func InitPqsMeta() {

	pqMetaDirName = filepath.Join(config.GetDataPath(),
		"querynodes",
		config.GetHostID(),
		"pqmeta")

	if _, err := os.Stat(pqMetaDirName); os.IsNotExist(err) {
		err := os.MkdirAll(pqMetaDirName, os.FileMode(0764))
		if err != nil {
			log.Errorf("psqmeta:init: Failed to create directory at %s: Error=%v",
				pqMetaDirName, err)
		}
	}
}

func getPqmetaFilename(pqid string) string {
	fileName := filepath.Join(pqMetaDirName, pqid+".meta")
	return fileName
}

func GetAllEmptySegmentsForPqid(pqid string) (map[string]bool, error) {
	fileName := getPqmetaFilename(pqid)
	return getAllEmptyPQSToMap(fileName)
}

func getAllEmptyPQSToMap(emptyPQSFilename string) (map[string]bool, error) {
	allEmptyPQS := make(map[string]bool)
	fd, err := os.OpenFile(emptyPQSFilename, os.O_CREATE|os.O_RDONLY, 0764)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]bool{}, nil
		}
		log.Errorf("getAllEmptyPQSToMap: Cannot read persistent query meta File = %v, err= %v", emptyPQSFilename, err)
		return nil, err
	}
	defer fd.Close()

	finfo, err := fd.Stat()
	if err != nil {
		log.Errorf("getAllEmptyPQSToMap: error when trying to stat file=%+v. Error=%+v",
			emptyPQSFilename, err)
		return nil, err
	}

	// if the file length is zero then its a valid scenario, just return an empty map
	if finfo.Size() == 0 {
		return allEmptyPQS, nil
	}

	err = json.NewDecoder(fd).Decode(&allEmptyPQS)
	if err != nil {
		log.Errorf("getAllEmptyPQSToMap: Cannot decode json data from file=%v, err =%v", emptyPQSFilename, err)
		return nil, err
	}

	return allEmptyPQS, nil
}

func AddEmptyResults(pqid string, segKey string, virtualTableName string) {

	fileName := getPqmetaFilename(pqid)
	emptyPQS, err := getAllEmptyPQSToMap(fileName)
	if err != nil {
		log.Errorf("AddEmptyResults: Failed to get empty PQS data from file at %s: Error=%v", fileName, err)
	}
	if emptyPQS != nil {
		emptyPQS[segKey] = true
		writeEmptyPqsMapToFile(fileName, emptyPQS)
	}
}

func writeEmptyPqsMapToFile(fileName string, emptyPqs map[string]bool) {
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
		log.Errorf("removePqmrFilesAndDirectory: Error fetching current workingDirectory")
		return err
	}
	pqFname := workingDirectory + "/" + getPqmetaFilename(pqid)
	err = os.Remove(pqFname)
	if err != nil {
		log.Errorf("removePqmrFilesAndDirectory: Cannot delete file=%v, Error=%v", pqFname, err)
		return err
	}
	pqMetaDirectory := path.Dir(pqFname)
	files, err := os.ReadDir(pqMetaDirectory)
	if err != nil {
		log.Errorf("removePqmrFilesAndDirectory: Cannot PQMR directory at %v, Error=%v", pqMetaDirectory, err)
		return err
	}
	if len(files) == 0 {
		err := os.Remove(pqMetaDirectory)
		if err != nil {
			log.Errorf("removePqmrFilesAndDirectory: Error deleting Pqmr directory at %v, Error=%v", pqMetaDirectory, err)
			return err
		}
	}
	return nil
}

func DeleteSegmentFromPqid(pqid string, segKey string) {
	pqFname := getPqmetaFilename(pqid)
	emptyPQS, err := getAllEmptyPQSToMap(pqFname)
	if err != nil {
		log.Errorf("DeleteSegmentFromPqid: Failed to get empty PQS data from file at %s: Error=%v", pqFname, err)
	}
	delete(emptyPQS, segKey)
	if len(emptyPQS) == 0 {
		err := removePqmrFilesAndDirectory(pqid)
		if err != nil {
			log.Errorf("DeleteSegmentFromPqid: Error removing segKey %v from %v pqid, Error=%v", segKey, pqid, err)
		}
		return
	}
	writeEmptyPqsMapToFile(pqFname, emptyPQS)
}

func DeletePQMetaDir() error {
	err := os.RemoveAll(pqMetaDirName)
	if err != nil {
		log.Errorf("DeleteAllPQMetaFiles: Error deleting directory at %v, Error=%v",
			pqMetaDirName, err)
		return err
	}
	return nil
}
