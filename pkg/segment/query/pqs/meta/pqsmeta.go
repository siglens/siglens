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
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var pqMetaDirName string

func InitPqsMeta() {

	pqMetaDirName = filepath.Join(config.GetDataPath(),
		"querynodes",
		config.GetHostID(),
		"pqmeta")

	_, err := os.Stat(pqMetaDirName)
	if err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(pqMetaDirName, os.FileMode(0764))
			if err != nil {
				log.Errorf("psqmeta:init: Failed to create directory at %s: Error=%v",
					pqMetaDirName, err)
			}
		} else {
			log.Errorf("psqmeta:init: could not stat directory at %s: Error=%v",
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
	pqsData, err := os.ReadFile(emptyPQSFilename)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]bool{}, nil
		}
		log.Errorf("getAllEmptyPQSToMap: Cannot read persistent query meta File = %v, err= %v", emptyPQSFilename, err)
		return nil, err
	}

	// if the data length is zero then its a valid scenario, just return an empty map
	if len(pqsData) == 0 {
		return allEmptyPQS, nil
	}

	err = json.Unmarshal(pqsData, &allEmptyPQS)
	if err != nil {
		log.Errorf("getAllEmptyPQSToMap: Cannot unmarshal json data, file: %v, err: %v",
			emptyPQSFilename, err)
		return nil, err
	}

	return allEmptyPQS, nil
}

func BulkAddEmptyResults(pqid string, segKeyMap map[string]bool) {
	fileName := getPqmetaFilename(pqid)
	emptyPQS, err := getAllEmptyPQSToMap(fileName)
	if err != nil {
		log.Errorf("BulkAddEmptyResults: Failed to get empty PQS data from file at %s: Error=%v", fileName, err)
	}
	if emptyPQS != nil {
		utils.MergeMapsRetainingFirst(emptyPQS, segKeyMap)
		writeEmptyPqsMapToFile(fileName, emptyPQS)
	}
}

func writeEmptyPqsMapToFile(fileName string, emptyPqs map[string]bool) {

	fd, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0764)
	if err != nil {
		log.Errorf("writeEmptyPqsMapToFile: Error opening file at fname=%v, err=%v", fileName, err)
		return
	}
	defer fd.Close()

	jsonData, err := json.Marshal(emptyPqs)
	if err != nil {
		log.Errorf("writeEmptyPqsMapToFile: could not marshal data: %v, fname: %v, err: %v",
			emptyPqs, fileName, err)
		return
	}
	_, err = fd.Write(jsonData)
	if err != nil {
		log.Errorf("writeEmptyPqsMapToFile: buf write failed fname=%v, err=%v", fileName, err)
		return
	}

	err = fd.Sync()
	if err != nil {
		log.Errorf("writeEmptyPqsMapToFile: fd sync failed filename=%v,err=%v", fileName, err)
		return
	}
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

// This function will remove the PQMRFiles and directory if there are no segments left in the PQID
func BulkDeleteSegKeysFromPqid(pqid string, segKeyMap map[string]bool) {
	pqFname := getPqmetaFilename(pqid)
	emptyPQS, err := getAllEmptyPQSToMap(pqFname)
	if err != nil {
		log.Errorf("BulkDeleteSegKeysFromPqid: Failed to get empty PQS data from file at %s: Error=%v", pqFname, err)
	}

	for segKey := range segKeyMap {
		delete(emptyPQS, segKey)
	}

	if len(emptyPQS) == 0 {
		err := removePqmrFilesAndDirectory(pqid)
		if err != nil {
			log.Errorf("BulkDeleteSegKeysFromPqid: Error removing segKeyMap %v from %v pqid, Error=%v", segKeyMap, pqid, err)
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
