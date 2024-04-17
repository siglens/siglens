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
