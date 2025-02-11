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

package blob

import (
	"fmt"
	"sync"
	"time"

	"github.com/siglens/siglens/pkg/hooks"
	log "github.com/sirupsen/logrus"
)

func InitBlobStore() error {
	if hook := hooks.GlobalHooks.InitBlobStoreExtrasHook; hook != nil {
		_, err := hook()
		if err != nil {
			log.Errorf("InitBlobStore: error from hook: %v", err)
			return err
		}
	}

	return nil
}

func UploadSegmentFiles(allFiles []string) error {
	if hook := hooks.GlobalHooks.UploadSegmentFilesExtrasHook; hook != nil {
		_, err := hook(allFiles)
		if err != nil {
			log.Errorf("UploadSegmentFiles: error from hook: %v", err)
			return err
		}
	}

	return nil
}

func UploadIngestNodeDir() error {
	if hook := hooks.GlobalHooks.UploadIngestNodeExtrasHook; hook != nil {
		alreadyHandled, err := hook()
		if alreadyHandled {
			return err
		}
	}

	return nil
}

func UploadQueryNodeDir() error {
	if hook := hooks.GlobalHooks.UploadQueryNodeExtrasHook; hook != nil {
		alreadyHandled, err := hook()
		if alreadyHandled {
			return err
		}
	}

	return nil
}

func DeleteBlob(filepath string) error {
	if hook := hooks.GlobalHooks.DeleteBlobExtrasHook; hook != nil {
		_, err := hook(filepath)
		if err != nil {
			log.Errorf("DeleteBlob: error from hook: %v", err)
			return err
		}
	}

	return nil
}

func DownloadAllIngestNodesDir() error {
	if hook := hooks.GlobalHooks.DownloadAllIngestNodesDirExtrasHook; hook != nil {
		alreadyHandled, err := hook()
		if alreadyHandled {
			return err
		}
	}

	return nil
}

func DownloadAllQueryNodesDir() error {
	if hook := hooks.GlobalHooks.DownloadAllQueryNodesDirExtrasHook; hook != nil {
		alreadyHandled, err := hook()
		if alreadyHandled {
			return err
		}
	}

	return nil
}

/*
To set all passed seg set files as in use to prevent being removed by localcleaner, set the inUseFlag to true otherwise false
Up to caller to call SetSegSetFilesAsNotInUse with same data after resouces are no longer needed
Returns an error if failed to download segment blob from S3 or mark any segSetFile as in use
*/
func DownloadSegmentBlob(fName string, inUseFlag bool) error {
	if hook := hooks.GlobalHooks.DownloadSegmentBlobExtrasHook; hook != nil {
		_, err := hook(fName)
		if err != nil {
			log.Errorf("DownloadSegmentBlob: error from hook: %v", err)
			return err
		}
	}

	// TODO: mark it as in use when inUseFlag is true
	return nil
}

// segFiles is a map with fileName as the key and colName as the corresponding value
func BulkDownloadSegmentBlob(segFiles map[string]string, inUseFlag bool) error {
	var bulkDownloadWG sync.WaitGroup
	var finalErr error
	sTime := time.Now()
	for fileName := range segFiles {
		bulkDownloadWG.Add(1)
		go func(fName string) {
			defer bulkDownloadWG.Done()
			err := DownloadSegmentBlob(fName, inUseFlag)
			if err != nil {
				// we will just save the finalErr that comes from any of these goroutines
				finalErr = fmt.Errorf("BulkDownloadSegmentBlob: failed to download segsetfile: %+v, err: %v",
					fName, err)
				return
			}
		}(fileName)
	}
	bulkDownloadWG.Wait()
	log.Debugf("BulkDownloadSegmentBlob: downloaded %v segsetfiles in %v", len(segFiles), time.Since(sTime))
	return finalErr
}

/*
Returns an error if failed to mark any segSetFile as not in use
*/
func SetSegSetFilesAsNotInUse(files []string) error {
	// TODO: mark them as not in use
	return nil
}

/*
Sets all passed seg set files as no longer in use so it can be removed by localcleaner
Returns an error if failed to mark any segSetFile as not in use
*/
func SetBlobAsNotInUse(fName string) error {
	// TODO: mark it as not in use
	return nil
}
