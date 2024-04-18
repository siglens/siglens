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
