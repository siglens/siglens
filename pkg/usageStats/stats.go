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
	"io"
	"os"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	. "github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type UsageStatsGranularity uint8

const (
	Hourly UsageStatsGranularity = iota + 1
	Daily
)

type Stats struct {
	BytesCount                  uint64
	LogLinesCount               uint64
	TotalBytesCount             uint64
	TotalLogLinesCount          uint64
	MetricsDatapointsCount      uint64
	TotalMetricsDatapointsCount uint64
}

var ustats = make(map[uint64]*Stats)

var msgPrinter *message.Printer

type QueryStats struct {
	QueryCount          uint64
	QueriesSinceInstall uint64
	TotalRespTime       float64
}

var QueryStatsMap = make(map[uint64]*QueryStats)

type ReadStats struct {
	BytesCount             uint64
	EventCount             uint64
	MetricsDatapointsCount uint64
	TimeStamp              time.Time
}

func StartUsageStats() {
	msgPrinter = message.NewPrinter(language.English)

	if hook := hooks.GlobalHooks.GetQueryCountHook; hook != nil {
		hook()
	} else {
		GetQueryCount()
	}

	go writeUsageStats()
}

func GetQueryCount() {
	QueryStatsMap[0] = &QueryStats{
		QueryCount:          0,
		QueriesSinceInstall: 0,
		TotalRespTime:       0,
	}
	err := ReadQueryStats(0)
	if err != nil {
		log.Errorf("ReadQueryStats from file failed:%v\n", err)
	}
}

func ReadQueryStats(orgid uint64) error {
	filename := getQueryStatsFilename(getBaseQueryStatsDir(orgid))
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer fd.Close()
	r := csv.NewReader(fd)
	val, err := r.ReadAll()
	if err != nil {
		log.Errorf("readQueryStats: read records failed, err=%v", err)
		return err
	}
	if len(val) > 0 {
		flushedQueriesSinceInstall, err := strconv.ParseUint(val[len(val)-1][0], 10, 64)
		if err != nil {
			return err
		}
		if QueryStatsMap[orgid] != nil {
			QueryStatsMap[orgid].QueriesSinceInstall = flushedQueriesSinceInstall
		}
	}
	return nil
}

func GetBaseStatsDir(orgid uint64) string {

	var sb strings.Builder
	timeNow := uint64(time.Now().UnixNano()) / uint64(time.Millisecond)
	sb.WriteString(config.GetDataPath() + "ingestnodes/" + config.GetHostID() + "/usageStats/")
	if orgid != 0 {
		sb.WriteString(strconv.FormatUint(orgid, 10))
		sb.WriteString("/")
	}
	t1 := time.Unix(int64(timeNow/1000), int64((timeNow%1000)*1000))
	sb.WriteString(t1.UTC().Format("2006/01/02"))
	sb.WriteString("/")
	basedir := sb.String()
	return basedir
}

func getBaseQueryStatsDir(orgid uint64) string {

	var sb strings.Builder
	sb.WriteString(config.GetDataPath() + "querynodes/" + config.GetHostID() + "/")
	if orgid != 0 {
		sb.WriteString(strconv.FormatUint(orgid, 10))
		sb.WriteString("/")
	}
	basedir := sb.String()
	return basedir
}

func getBaseStatsDirs(startTime, endTime time.Time, orgid uint64) []string {
	startTOD := (startTime.UnixMilli() / MS_IN_DAY) * MS_IN_DAY
	endTOD := (endTime.UnixMilli() / MS_IN_DAY) * MS_IN_DAY
	ingestDir := config.GetIngestNodeBaseDir()
	// read all files in dir

	files, err := os.ReadDir(ingestDir)
	if err != nil {
		log.Errorf("ReadAllSegmetas: read dir err=%v ", err)
		return make([]string, 0)
	}

	// read all iNodes
	iNodes := make([]string, 0)
	for _, file := range files {
		fName := file.Name()
		iNodes = append(iNodes, fName)
	}

	statsDirs := make([]string, 0)
	for _, iNode := range iNodes {
		mDir := path.Join(ingestDir, iNode, "usageStats")
		if _, err := os.Stat(mDir); err != nil {
			continue
		}
		fileStartTOD := startTOD
		fileEndTOD := endTOD
		fileStartTime := startTime
		for fileEndTOD >= fileStartTOD {
			var sb strings.Builder
			sb.WriteString(mDir)
			sb.WriteString("/")
			if orgid != 0 {
				sb.WriteString(strconv.FormatUint(orgid, 10))
			}
			sb.WriteString("/")
			timeNow := uint64(fileStartTime.UnixNano()) / uint64(time.Millisecond)
			t1 := time.Unix(int64(timeNow/1000), int64((timeNow%1000)*1000))
			sb.WriteString(t1.UTC().Format("2006/01/02"))
			sb.WriteString("/")
			statsDirs = append(statsDirs, sb.String())
			fileStartTOD = fileStartTOD + MS_IN_DAY
			fileStartTime = fileStartTime.AddDate(0, 0, 1)
		}

	}

	return statsDirs
}

func getStatsFilename(baseDir string) string {
	var sb strings.Builder

	err := os.MkdirAll(baseDir, 0764)
	if err != nil {
		log.Errorf("getStatsFilename, mkdirall failed, basedir=%v, err=%v", baseDir, err)
		return ""
	}
	_, err = sb.WriteString(baseDir)
	if err != nil {
		log.Errorf("getStatsFilename, writestring basedir failed,err=%v", err)
	}
	_, err = sb.WriteString("usage_stats.csv")
	if err != nil {
		log.Errorf("getStatsFilename, writestring file failed,err=%v", err)
	}
	return sb.String()
}

func getQueryStatsFilename(baseDir string) string {
	var sb strings.Builder

	err := os.MkdirAll(baseDir, 0764)
	if err != nil {
		log.Errorf("getQueryStatsFilename, mkdirall failed, basedir=%v, err=%v", baseDir, err)
		return ""
	}
	_, err = sb.WriteString(baseDir)
	if err != nil {
		log.Errorf("getQueryStatsFilename, writestring basedir failed,err=%v", err)
	}
	_, err = sb.WriteString("usage_queryStats.csv")
	if err != nil {
		log.Errorf("getQueryStatsFilename, writestring file failed,err=%v", err)
	}
	return sb.String()
}

func writeUsageStats() {
	for {
		alreadyHandled := false
		if hook := hooks.GlobalHooks.WriteUsageStatsIfConditionHook; hook != nil {
			alreadyHandled = hook()
		}

		if !alreadyHandled {
			go func() {
				err := FlushStatsToFile(0)
				if err != nil {
					log.Errorf("WriteUsageStats failed:%v\n", err)
				}
				errC := flushCompressedStatsToFile(0)
				if errC != nil {
					log.Errorf("WriteUsageStats failed:%v\n", errC)
				}

			}()

			if hook := hooks.GlobalHooks.WriteUsageStatsElseExtraLogicHook; hook != nil {
				hook()
			}
		}
		time.Sleep(1 * time.Minute)
	}
}

func ForceFlushStatstoFile() {
	alreadyHandled := false
	if hook := hooks.GlobalHooks.ForceFlushIfConditionHook; hook != nil {
		alreadyHandled = hook()
	}
	if alreadyHandled {
		return
	}

	err := FlushStatsToFile(0)
	if err != nil {
		log.Errorf("ForceFlushStatstoFile failed:%v\n", err)
	}
}

func logStatSummary(orgid uint64) {
	if _, ok := ustats[orgid]; ok {
		log.Infof("Ingest stats: past minute : events=%v, metrics=%v, bytes=%v",
			msgPrinter.Sprintf("%v", ustats[orgid].LogLinesCount),
			msgPrinter.Sprintf("%v", ustats[orgid].MetricsDatapointsCount),
			msgPrinter.Sprintf("%v", ustats[orgid].BytesCount))

		log.Infof("Ingest stats: total so far: events=%v, metrics=%v, bytes=%v",
			msgPrinter.Sprintf("%v", ustats[orgid].TotalLogLinesCount),
			msgPrinter.Sprintf("%v", ustats[orgid].TotalMetricsDatapointsCount),
			msgPrinter.Sprintf("%v", ustats[orgid].TotalBytesCount))
	}
}

func GetTotalLogLines(orgid uint64) uint64 {
	return ustats[orgid].TotalLogLinesCount
}

func FlushStatsToFile(orgid uint64) error {
	if _, ok := QueryStatsMap[orgid]; ok {
		filename := getQueryStatsFilename(getBaseQueryStatsDir(orgid))
		fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return err
		}
		defer fd.Close()
		w := csv.NewWriter(fd)
		var records [][]string
		var record []string
		queriesSinceInstallAsString := strconv.FormatUint(QueryStatsMap[orgid].QueriesSinceInstall, 10)
		record = []string{queriesSinceInstallAsString}
		records = append(records, record)
		err = w.WriteAll(records)
		if err != nil {
			log.Errorf("flushStatsToFile: write records failed, err=%v", err)
			return err
		}
		log.Debugf("flushQueryStatsToFile: flushed queryStats' queriesSinceInstall=%v", QueryStatsMap[orgid].QueriesSinceInstall)
	}

	if _, ok := ustats[orgid]; ok {
		logStatSummary(orgid)
		if ustats[orgid].BytesCount > 0 {
			filename := getStatsFilename(GetBaseStatsDir(orgid))
			fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
			if err != nil {
				return err
			}
			defer fd.Close()
			w := csv.NewWriter(fd)
			var records [][]string
			var record []string
			bytesAsString := strconv.FormatUint(ustats[orgid].BytesCount, 10)
			logLinesAsString := strconv.FormatUint(ustats[orgid].LogLinesCount, 10)
			metricCountAsString := strconv.FormatUint(ustats[orgid].MetricsDatapointsCount, 10)
			epochAsString := strconv.FormatUint(uint64(time.Now().Unix()), 10)
			record = []string{bytesAsString, logLinesAsString, metricCountAsString, epochAsString}
			records = append(records, record)
			err = w.WriteAll(records)
			if err != nil {
				log.Errorf("flushStatsToFile: write records failed, err=%v", err)
				return err
			}
			log.Debugf("flushStatsToFile: flushed stats evCount=%v, metricsCount=%v, bytes=%v", ustats[orgid].LogLinesCount,
				ustats[orgid].MetricsDatapointsCount, ustats[orgid].BytesCount)

			atomic.StoreUint64(&ustats[orgid].BytesCount, 0)
			atomic.StoreUint64(&ustats[orgid].LogLinesCount, 0)
			atomic.StoreUint64(&ustats[orgid].MetricsDatapointsCount, 0)

			return nil
		}
	}
	return nil
}

func UpdateStats(bytesCount uint64, logLinesCount uint64, orgid uint64) {
	if _, ok := ustats[orgid]; !ok {
		ustats[orgid] = &Stats{
			BytesCount:         0,
			LogLinesCount:      0,
			TotalBytesCount:    0,
			TotalLogLinesCount: 0,
		}
	}
	atomic.AddUint64(&ustats[orgid].BytesCount, bytesCount)
	atomic.AddUint64(&ustats[orgid].LogLinesCount, logLinesCount)
	atomic.AddUint64(&ustats[orgid].TotalBytesCount, bytesCount)
	atomic.AddUint64(&ustats[orgid].TotalLogLinesCount, logLinesCount)
}

func UpdateMetricsStats(bytesCount uint64, incomingMetrics uint64, orgid uint64) {
	if _, ok := ustats[orgid]; !ok {
		ustats[orgid] = &Stats{
			BytesCount:                  0,
			MetricsDatapointsCount:      0,
			TotalBytesCount:             0,
			TotalMetricsDatapointsCount: 0,
		}
	}
	atomic.AddUint64(&ustats[orgid].BytesCount, bytesCount)
	atomic.AddUint64(&ustats[orgid].MetricsDatapointsCount, incomingMetrics)
	atomic.AddUint64(&ustats[orgid].TotalBytesCount, bytesCount)
	atomic.AddUint64(&ustats[orgid].TotalMetricsDatapointsCount, incomingMetrics)
}

func GetQueryStats(orgid uint64) (uint64, float64, uint64) {
	if _, ok := QueryStatsMap[orgid]; !ok {
		return 0, 0, 0
	}
	return QueryStatsMap[orgid].QueryCount, QueryStatsMap[orgid].TotalRespTime, QueryStatsMap[orgid].QueriesSinceInstall
}

func GetCurrentMetricsStats(orgid uint64) (uint64, uint64) {
	return ustats[orgid].TotalBytesCount, ustats[orgid].TotalMetricsDatapointsCount
}

func UpdateQueryStats(queryCount uint64, totalRespTime float64, orgid uint64) {
	if _, ok := QueryStatsMap[orgid]; !ok {
		QueryStatsMap[orgid] = &QueryStats{
			QueryCount:    0,
			TotalRespTime: 0,
		}
	}
	atomic.AddUint64(&QueryStatsMap[orgid].QueryCount, queryCount)
	atomic.AddUint64(&QueryStatsMap[orgid].QueriesSinceInstall, queryCount)
	QueryStatsMap[orgid].TotalRespTime += totalRespTime
}

// Calculate total bytesCount,linesCount and return hourly / daily count
func GetUsageStats(pastXhours uint64, granularity UsageStatsGranularity, orgid uint64) (map[string]ReadStats, error) {
	endEpoch := time.Now()
	startEpoch := endEpoch.Add(-(time.Duration(pastXhours) * time.Hour))
	startTOD := (startEpoch.UnixMilli() / MS_IN_DAY) * MS_IN_DAY
	endTOD := (endEpoch.UnixMilli() / MS_IN_DAY) * MS_IN_DAY
	startTOH := (startEpoch.UnixMilli() / MS_IN_HOUR) * MS_IN_HOUR
	endTOH := (endEpoch.UnixMilli() / MS_IN_HOUR) * MS_IN_HOUR
	statsFnames := getBaseStatsDirs(startEpoch, endEpoch, orgid) // usageStats

	allStatsMap := make([]ReadStats, 0)
	resultMap := make(map[string]ReadStats)
	var bucketInterval string
	runningTs := startEpoch
	if granularity == Daily {
		for endTOD >= startTOD {
			bucketInterval = runningTs.Format("2006-01-02")
			runningTs = runningTs.Add(24 * time.Hour)
			startTOD = startTOD + MS_IN_DAY
			resultMap[bucketInterval] = ReadStats{}
		}
	} else if granularity == Hourly {
		for endTOH >= startTOH {
			bucketInterval = runningTs.Format("2006-01-02T15")
			runningTs = runningTs.Add(1 * time.Hour)
			startTOH = startTOH + MS_IN_HOUR
			resultMap[bucketInterval] = ReadStats{}
		}
	}

	for _, statsFile := range statsFnames {
		filename := getStatsFilename(statsFile)
		fd, err := os.OpenFile(filename, os.O_RDONLY, 0666)
		if err != nil {
			continue
		}
		defer fd.Close()

		r := csv.NewReader(fd)
		for {
			var readStats ReadStats
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Errorf("GetUsageStats: error reading stats file = %v, err= %v", filename, err)
				break
			}
			if len(record) < 3 {
				log.Errorf("GetUsageStats: invalid stats entry in fname %+v = %v, err= %v", filename, record, err)
				continue
			}
			readStats.BytesCount, _ = strconv.ParseUint(record[0], 10, 64)
			readStats.EventCount, _ = strconv.ParseUint(record[1], 10, 64)

			// Prior to metrics, format is bytes,eventCount,time
			// After metrics, format is bytes,eventCount,metricCount,time
			if len(record) == 4 {
				readStats.MetricsDatapointsCount, _ = strconv.ParseUint(record[2], 10, 64)
				tsString, _ := strconv.ParseInt(record[3], 10, 64)
				readStats.TimeStamp = time.Unix(tsString, 0)
			} else {
				tsString, _ := strconv.ParseInt(record[2], 10, 64)
				readStats.TimeStamp = time.Unix(tsString, 0)
			}
			if readStats.TimeStamp.After(startEpoch) && readStats.TimeStamp.Before(endEpoch) {
				allStatsMap = append(allStatsMap, readStats)
			}
		}
	}

	for _, rStat := range allStatsMap {
		if granularity == Daily {
			bucketInterval = rStat.TimeStamp.Format("2006-01-02")
		} else if granularity == Hourly {
			bucketInterval = rStat.TimeStamp.Format("2006-01-02T15")
		}
		if entry, ok := resultMap[bucketInterval]; ok {
			entry.EventCount += rStat.EventCount
			entry.MetricsDatapointsCount += rStat.MetricsDatapointsCount
			entry.BytesCount += rStat.BytesCount
			entry.TimeStamp = rStat.TimeStamp
			resultMap[bucketInterval] = entry
		} else {
			resultMap[bucketInterval] = rStat
		}
	}
	return resultMap, nil
}
