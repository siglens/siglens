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

package fileutils

import (
	"io"
	"os"
	"path"
	"syscall"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/utils/semaphore"
	log "github.com/sirupsen/logrus"
)

/*
	Exports utility functions for limiting the amount of open file descriptors across siglens
*/

var GLOBAL_FD_LIMITER *semaphore.WeightedSemaphore
var DEFAULT_MAX_OPEN_FDs uint64 = 8192

/*
what percent of max open FDs should we actually use?
the remaining percent would be used for the FDs used by fasthttp, and misc file opens

TODO: how to improve on this limit?
*/
var OPEN_FD_THRESHOLD_PERCENT uint64 = 70

func init() {
	var numFiles uint64
	var rLimit syscall.Rlimit
	err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if err != nil {
		log.Errorf("Failed to get max possible number of open file descriptors. Defaulting to %+v. Error: %v",
			DEFAULT_MAX_OPEN_FDs, err)
		numFiles = DEFAULT_MAX_OPEN_FDs
	} else {
		numFiles = rLimit.Cur
	}
	var newLimit syscall.Rlimit
	newLimit.Max = numFiles
	newLimit.Cur = numFiles

	err = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &newLimit)
	if err != nil {
		log.Errorf("Failed to set max possible number of open file descriptors. Defaulting to %+v. Error: %v",
			rLimit.Cur, err)
	} else {
		numFiles = newLimit.Cur
	}
	maxPossibleOpenFds := int64(numFiles*OPEN_FD_THRESHOLD_PERCENT) / 100
	log.Infof("Initialized FD limiter with %+v as max number of open files", maxPossibleOpenFds)
	GLOBAL_FD_LIMITER = semaphore.NewDefaultWeightedSemaphore(maxPossibleOpenFds, "GlobalFileLimiter")
}

func IsDirEmpty(name string) bool {
	f, err := os.Open(name)
	if err != nil {
		return false
	}
	defer f.Close()

	// read in ONLY one file
	_, err = f.Readdir(1)

	// and if the file is EOF... well, the dir is empty.
	return err == io.EOF
}

func RecursivelyDeleteEmptyParentDirectories(filePath string) {
	temp := path.Dir(filePath)
	for {
		if temp == config.GetDataPath() {
			break
		}
		if IsDirEmpty(temp) {
			os.RemoveAll(temp)
		} else {
			break
		}
		temp = path.Dir(temp)
	}
}

func GetAllFilesInDirectory(path string) []string {
	retVal := make([]string, 0)
	files, err := os.ReadDir(path)

	if err != nil {
		log.Errorf("GetAllFilesInDirectory Error: %v", err)
		return retVal
	}

	for _, file := range files {
		if file.IsDir() {
			res := GetAllFilesInDirectory(path + file.Name() + "/")
			retVal = append(retVal, res...)
		} else {
			retVal = append(retVal, path+file.Name())
		}
	}
	return retVal
}
