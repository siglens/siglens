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

package usageStats

import (
	"encoding/csv"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

type CompressedStats struct {
	CompressedBytes int64
	TimeStamp       time.Time
}

var cstats map[uint64]*CompressedStats

func init() {
	cstats = make(map[uint64]*CompressedStats)
	//cstats = CompressedStats{CompressedBytes: 0}
}

func getCompressedStatsFilename(baseDir string) string {
	var sb strings.Builder

	err := os.MkdirAll(baseDir, 0764)
	if err != nil {
		return ""
	}
	sb.WriteString(baseDir)
	sb.WriteString("compressed_stats.csv")
	return sb.String()
}

func UpdateCompressedStats(segFileSize int64, orgid uint64) {
	if _, ok := cstats[orgid]; !ok {
		cstats[orgid] = &CompressedStats{}
	}
	atomic.AddInt64(&cstats[orgid].CompressedBytes, segFileSize)
}

func flushCompressedStatsToFile(orgid uint64) error {
	if _, ok := cstats[orgid]; ok {
		if cstats[orgid].CompressedBytes > 0 {
			cstats[orgid].TimeStamp = time.Now().UTC()
			filename := getCompressedStatsFilename(GetBaseStatsDir(orgid))
			fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
			if err != nil {
				return err
			}
			defer fd.Close()
			w := csv.NewWriter(fd)
			var crecords [][]string
			var crecord []string
			compressedBytesAsString := strconv.FormatInt(cstats[orgid].CompressedBytes, 10)
			crecord = append(crecord, compressedBytesAsString, cstats[orgid].TimeStamp.String())
			crecords = append(crecords, crecord)
			err = w.WriteAll(crecords)
			if err != nil {
				log.Errorf("flushCompressedStatsToFile: write failed, err=%v", err)
				return err
			}

			log.Debugf("flushCompressedStatsToFile: flushed stats segFileSize=%v, timestamp=%v", cstats[orgid].CompressedBytes, cstats[orgid].TimeStamp)

			atomic.StoreInt64(&cstats[orgid].CompressedBytes, 0)
		}
	}
	return nil
}
