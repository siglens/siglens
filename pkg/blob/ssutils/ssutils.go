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

package ssutils

import (
	"os"
	"time"

	"github.com/siglens/siglens/pkg/segment/structs"
	log "github.com/sirupsen/logrus"
)

func NewSegSetData(fileName string, size uint64) *structs.SegSetData {
	return &structs.SegSetData{
		AccessTime:     time.Now().Unix(),
		Size:           size,
		SegSetFileName: fileName,
	}
}

func GetFileNameFromSegSetFile(segSetFile structs.SegSetFile) string {
	fileName := segSetFile.SegKey
	switch segSetFile.FileType {
	case structs.Cmi:
		if segSetFile.Identifier != "" {
			fileName = fileName + "_" + segSetFile.Identifier + ".cmi"
		}
	case structs.Csg:
		if segSetFile.Identifier != "" {
			fileName = fileName + "_" + segSetFile.Identifier + ".csg"
		}
	case structs.Bsu:
		fileName = fileName + ".bsu"
	case structs.Sid:
		fileName = fileName + ".sid"
	case structs.Pqmr:
		fileName = fileName + "/pqmr/" + segSetFile.Identifier + ".pqmr"
	case structs.Rollup:
		fileName = fileName + "/rups/" + segSetFile.Identifier + ".crup"
	default:
		log.Errorf("GetFileNameFromSegSetFile: unknown seg set file type! %+v", segSetFile.FileType)
	}
	return fileName
}

/*
Return size of file and bool if file was found
Gets the file size using an os.Stat. If no file is found, returns 0
*/
func GetFileSizeFromDisk(filePath string) (uint64, bool) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return 0, false
	}
	return uint64(fi.Size()), true
}
