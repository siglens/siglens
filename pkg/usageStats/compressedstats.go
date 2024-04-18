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
