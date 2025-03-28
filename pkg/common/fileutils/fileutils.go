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
	"bufio"
	"io"
	"os"
	"path"
	"path/filepath"
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
		log.Errorf("init: Failed to get max possible number of open file descriptors. Defaulting to %+v. Error: %v",
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
		log.Errorf("init: Failed to set max possible number of open file descriptors. Defaulting to %+v. Error: %v",
			rLimit.Cur, err)
	} else {
		numFiles = newLimit.Cur
	}
	maxPossibleOpenFds := int64(numFiles*OPEN_FD_THRESHOLD_PERCENT) / 100
	GLOBAL_FD_LIMITER = semaphore.NewDefaultWeightedSemaphore(maxPossibleOpenFds, "GlobalFileLimiter")
	LogMaxOpenFiles()
}

func LogMaxOpenFiles() {
	log.Infof("LogMaxOpenFiles: Initialized FD limiter with %+v as max number of open files", GLOBAL_FD_LIMITER.MaxSize)
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

// Recursively finds and returns all files. Each returned file begins with the
// provided `path`.
func GetAllFilesInDirectory(path string) []string {
	retVal := make([]string, 0)
	files, err := os.ReadDir(path)

	if err != nil {
		log.Errorf("GetAllFilesInDirectory: err: %v", err)
		return retVal
	}

	for _, file := range files {
		if file.IsDir() {
			res := GetAllFilesInDirectory(filepath.Join(path, file.Name()))
			retVal = append(retVal, res...)
		} else {
			retVal = append(retVal, filepath.Join(path, file.Name()))
		}
	}
	return retVal
}

func GetAllFilesWithSameNameInDirectory(dir string, fname string) []string {
	result := make([]string, 0)

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Errorf("GetAllFilesWithSameNameInDirectory: Error walking path: %v, dir: %v, fname: %v, err: %v", path, dir, fname, err)
			return nil // Continue walking, but log the error
		}

		// Check if the current file is the one we're looking for
		if !info.IsDir() && info.Name() == fname {
			result = append(result, path)
		}

		// Return nil to continue walking
		return nil
	})

	if err != nil {
		log.Errorf("GetAllFilesWithSameNameInDirectory: Error during filepath.Walk, dir: %v, fname: %v, err: %v", dir, fname, err)
	}

	return result
}

func GetAllFilesWithSpecificExtensions(dir string, ext map[string]struct{}) []string {
	result := make([]string, 0)

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Errorf("GetAllFilesWithSpecificExtensions: Error walking path: %v, dir: %v, ext: %v, err: %v", path, dir, ext, err)
			return nil // Continue walking, but log the error
		}

		// Check if the current file is the one we're looking for
		if !info.IsDir() {
			_, extExists := ext[filepath.Ext(path)]
			if extExists {
				result = append(result, path)
			}
		}

		// Return nil to continue walking
		return nil
	})

	if err != nil {
		log.Errorf("GetAllFilesWithSpecificExtensions: Error during filepath.Walk, dir: %v, ext: %v, err: %v", dir, ext, err)
	}

	return result
}

// Note: if your processLine handler needs to keep a copy of the line, you have
// to copy the bytes to a new slice; the contents may be overwritten by the
// next call to processLine.
func ReadLineByLine(filePath string, processLine func(line []byte) error) error {
	file, err := os.Open(filePath)
	if err != nil {
		log.Errorf("ReadLineByLine: Error opening file %v: %v", filePath, err)
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		err = processLine(scanner.Bytes())
		if err != nil {
			log.Errorf("ReadLineByLine: Error processing line %v: %v", scanner.Text(), err)
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		log.Errorf("ReadLineByLine: Error scanning file %v: %v", filePath, err)
		return err
	}

	return nil
}

func DoesFileExist(filePath string) bool {
	_, err := os.Stat(filePath)
	return err == nil
}

func GetDirSize(path string) (uint64, error) {
	var size uint64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += uint64(info.Size())
		}
		return nil
	})
	return size, err
}
