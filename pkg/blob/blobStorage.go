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

	"github.com/siglens/siglens/pkg/blob/local"
	"github.com/siglens/siglens/pkg/blob/ssutils"
	"github.com/siglens/siglens/pkg/hooks"
	log "github.com/sirupsen/logrus"
)

func InitBlobStore() error {
	if hook := hooks.GlobalHooks.InitBlobStoreExtrasHook; hook != nil {
		alreadyHandled, err := hook()
		if alreadyHandled {
			return err
		}
	}

	return local.InitLocalStorage()
}

func UploadSegmentFiles(allFiles []string) error {
	if hook := hooks.GlobalHooks.UploadSegmentFilesExtrasHook; hook != nil {
		alreadyHandled, err := hook(allFiles)
		if alreadyHandled {
			return err
		}
	}

	local.BulkAddSegSetFilesToLocal(allFiles)
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
		alreadyHandled, err := hook(filepath)
		if alreadyHandled {
			return err
		}
	}

	return local.DeleteLocal(filepath)
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
	if local.IsFilePresentOnLocal(fName) {
		return nil
	}

	if hook := hooks.GlobalHooks.DownloadSegmentBlobExtrasHook; hook != nil {
		alreadyHandled, err := hook(fName)
		if alreadyHandled {
			return err
		}
	}

	size, _ := ssutils.GetFileSizeFromDisk(fName)
	ssData := ssutils.NewSegSetData(fName, size)
	local.AddSegSetFileToLocal(fName, ssData)

	if inUseFlag {
		err := local.SetBlobAsInUse(fName)
		if err != nil {
			log.Errorf("DownloadSegmentBlob: failed to set segSetFile %v as in use: %v", fName, err)
			return err
		}
	}
	return nil
}

// segFiles is a map with fileName as the key and colName as the corresponding value
func BulkDownloadSegmentBlob(segFiles map[string]string, inUseFlag bool) error {
	var bulkDownloadWG sync.WaitGroup
	var err error
	sTime := time.Now()
	for fileName := range segFiles {
		bulkDownloadWG.Add(1)
		go func(fName string) {
			defer bulkDownloadWG.Done()
			err = DownloadSegmentBlob(fName, inUseFlag)
			if err != nil {
				err = fmt.Errorf("BulkDownloadSegmentBlob: failed to download segsetfile %+v", fName)
				return
			}
		}(fileName)
	}
	bulkDownloadWG.Wait()
	log.Debugf("BulkDownloadSegmentBlob: downloaded %v segsetfiles in %v", len(segFiles), time.Since(sTime))
	return err
}

/*
Sets all passed seg set files as no longer in use so it can be removed by localcleaner
Returns an error if failed to mark any segSetFile as not in use
*/
func SetSegSetFilesAsNotInUse(files []string) error {
	var retErr error
	retErr = nil
	for _, segSetFile := range files {
		err := local.SetBlobAsNotInUse(segSetFile)
		if err != nil {
			log.Errorf("SetSegSetFilesAsNotInUse: failed to set segSetFile as not in use: %v", err)
			retErr = err
			continue
		}
	}
	return retErr
}

/*
Sets all passed seg set files as no longer in use so it can be removed by localcleaner
Returns an error if failed to mark any segSetFile as not in use
*/
func SetBlobAsNotInUse(fName string) error {
	err := local.SetBlobAsNotInUse(fName)
	if err != nil {
		log.Errorf("SetBlobAsNotInUse: failed to set segSetFile as not in use: %v", err)
		return err
	}

	return nil
}

// Returns size of file. If file does not exit, returns 0 with no error.
func GetFileSize(filename string) uint64 {
	size, exists := local.GetLocalFileSize(filename)
	if exists {
		return size
	}

	size, onLocal := ssutils.GetFileSizeFromDisk(filename)
	if onLocal {
		return size
	}

	if hook := hooks.GlobalHooks.GetFileSizeExtrasHook; hook != nil {
		alreadyHandled, size := hook(filename)
		if alreadyHandled {
			return size
		}
	}

	return 0
}

// For a given meta file, returns if it exists in blob store.
// The input should always be a file in the ingestnodes directory. Either segmetas or metrics metas
func DoesMetaFileExistInBlob(fName string) (bool, error) {
	if hook := hooks.GlobalHooks.DoesMetaFileExistExtrasHook; hook != nil {
		alreadyHandled, exists, err := hook(fName)
		if alreadyHandled {
			return exists, err
		}
	}

	return false, nil
}
