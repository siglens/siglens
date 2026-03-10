// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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
	case structs.Sfm:
		fileName = fileName + ".sfm"
	case structs.Pqmr:
		fileName = fileName + "/pqmr/" + segSetFile.Identifier + ".pqmr"
	case structs.Rollup:
		fileName = fileName + "/rups/" + segSetFile.Identifier + ".crup"
	default:
		log.Errorf("GetFileNameFromSegSetFile: unknown seg set file type=%+v, filName=%v", segSetFile.FileType, fileName)
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
