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

package local

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/siglens/siglens/pkg/blob/ssutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	log "github.com/sirupsen/logrus"
)

var segSetKeys = map[string]*structs.SegSetData{}
var segSetKeysLock *sync.Mutex = &sync.Mutex{}
var segSetKeysFileName = "ssd.json"

func InitLocalStorage() error {
	segSetKeysLock.Lock()
	defer segSetKeysLock.Unlock()
	filePath := config.GetDataPath() + "common/" + segSetKeysFileName
	file, err := os.ReadFile(filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Errorf("InitLocalStorage: Error reading SegSetKeys file %s: %v", filePath, err)
			return err
		}
	} else {
		if err := json.Unmarshal(file, &segSetKeys); err != nil {
			log.Errorf("InitLocalStorage: Error unmarshalling SegSetKeys file %s: %v", filePath, err)
			return err
		}
	}

	// only if s3 is enabled & we are uploading to s3 should we start up the cleaner
	if config.IsS3Enabled() {
		initLocalCleaner()
	}
	go persistSegSetKeysOnInterval()
	return nil
}

func persistSegSetKeysOnInterval() {
	for {
		time.Sleep(1 * time.Minute)
		ForceFlushSegSetKeysToFile()
	}
}

func ForceFlushSegSetKeysToFile() {
	segSetKeysLock.Lock()
	defer segSetKeysLock.Unlock()

	// write Segsetkeys to ssd.json
	filePath := config.GetDataPath() + "common/" + segSetKeysFileName
	fd, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Errorf("ForceFlushSegSetKeysToFile: Error creating file %s: %v", filePath, err)
		return
	}
	defer fd.Close()
	segSetKeysJson, err := json.Marshal(&segSetKeys)
	if err != nil {
		log.Errorf("ForceFlushSegSetKeysToFile: Error marshalling SegSetKeys: %v", err)
		return
	}
	if _, err := fd.Write(segSetKeysJson); err != nil {
		log.Errorf("ForceFlushSegSetKeysToFile: Error writing to file %s: %v", filePath, err)
		return
	}
}

/*
Adds []string to metadata of on disk files

This assumes that segSetFile has been properly initialized with latest and size
*/
func BulkAddSegSetFilesToLocal(segSetFiles []string) {
	for _, sfile := range segSetFiles {
		if sfile == "" {
			continue
		}
		size, ok := ssutils.GetFileSizeFromDisk(sfile)
		if !ok {
			log.Errorf("BulkAddSegSetFilesToLocal: GetFileSizeFromDisk %+v does not exist in localstorage", sfile)
		}
		ssData := ssutils.NewSegSetData(sfile, size)
		AddSegSetFileToLocal(sfile, ssData)
	}
}

/*
Adds a single table and segsetFile to the local storage

# Internally, also adds the file information to the heap

This assumes that segSetData has been properly initialized with latest and size
*/
func AddSegSetFileToLocal(fName string, segSetData *structs.SegSetData) {
	segSetKeysLock.Lock()
	defer segSetKeysLock.Unlock()

	if strings.Contains(fName, "/active/") {
		return
	}
	if _, exists := segSetKeys[fName]; !exists {
		segSetKeys[fName] = segSetData
		allSortedSegSetFiles.Push(segSetData)
	}
	segSetKeys[fName].AccessTime = time.Now().Unix()
}

/*
	Sets SegSetData.InUse = true for the input segSetFile

	Returns an error if SetSetData does not exist in SegSetKeys
*/

func SetBlobAsInUse(fName string) error {

	segSetKeysLock.Lock()
	defer segSetKeysLock.Unlock()

	if strings.Contains(fName, "/active/") {
		return nil
	}
	if _, exists := segSetKeys[fName]; exists {
		segSetKeys[fName].AccessTime = time.Now().Unix()
		segSetKeys[fName].InUse = true
		return nil

	}
	return fmt.Errorf("tried to mark segSetFile: %+v as in use that does not exist in localstorage",
		fName)
}

/*
Returns if the file exists in the local SegSetKeys struct
*/
func IsFilePresentOnLocal(fName string) bool {
	segSetKeysLock.Lock()
	defer segSetKeysLock.Unlock()
	if _, exists := segSetKeys[fName]; exists {
		segSetKeys[fName].AccessTime = time.Now().Unix()
		return true
	}
	return false
}

func DeleteLocal(fName string) error {
	segSetKeysLock.Lock()
	defer segSetKeysLock.Unlock()
	if strings.Contains(fName, "/active/") {
		log.Debugf("Not deleting segset from active dir %v", fName)
		delete(segSetKeys, fName)
		return fmt.Errorf("not deleting segset from active dir %v", fName)
	}
	if _, exists := segSetKeys[fName]; exists && fName != "" {
		deleteLocalFile(fName)
		delete(segSetKeys, fName)
		log.Debugf("DeleteSegSetFileFromSegSetKey %v ", fName)
		return nil
	}
	return nil
}

func deleteLocalFile(file string) {
	if err := os.Remove(file); err != nil {
		log.Errorf("ssregistry.local: deleteLocalFile: Error deleting file %s: %v", file, err)
	}

	recursivelyDeleteParentDirectories(file)
}

func recursivelyDeleteParentDirectories(filePath string) {
	temp := path.Dir(filePath)
	for {
		if temp == config.GetDataPath() {
			break
		}
		if isDirEmpty(temp) {
			os.RemoveAll(temp)
		} else {
			break
		}
		temp = path.Dir(temp)
	}
}

func isDirEmpty(name string) bool {
	f, err := os.Open(name)
	if err != nil {
		return false
	}
	defer f.Close()

	_, err = f.Readdir(1)
	return err == io.EOF
}

/*
Returns the local file size and a bool indicating if the file existed
Internally, updates access time of local file
*/
func GetLocalFileSize(segSetFile string) (uint64, bool) {

	segSetKeysLock.Lock()
	defer segSetKeysLock.Unlock()
	if _, exists := segSetKeys[segSetFile]; exists {
		segSetKeys[segSetFile].AccessTime = time.Now().Unix()
		return segSetKeys[segSetFile].Size, true
	}
	return 0, false
}

/*
Sets SegSetData.InUse = false for the input segSetFile
Returns an error if SetSetData does not exist in SegSetKeys
*/
func SetBlobAsNotInUse(segSetFile string) error {
	segSetKeysLock.Lock()
	defer segSetKeysLock.Unlock()
	if strings.Contains(segSetFile, "/active/") {
		return nil
	}
	if _, exists := segSetKeys[segSetFile]; exists {
		segSetKeys[segSetFile].AccessTime = time.Now().Unix()
		segSetKeys[segSetFile].InUse = false
		return nil

	}
	return fmt.Errorf("tried to mark segSetFile: %+v as not use that does not exist in localstorage",
		segSetFile)
}
