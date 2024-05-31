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
	"errors"
	"fmt"
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

const MIN_IN_MS = 60_000

var timeIntervalsForStatsByMinute = []uint32{5, 10, 20, 30, 40, 50, 60}

const (
	Hourly UsageStatsGranularity = iota + 1
	Daily
	ByMinute
)

type Stats struct {
	BytesCount                  uint64
	LogLinesCount               uint64
	TotalBytesCount             uint64
	TotalLogLinesCount          uint64
	MetricsDatapointsCount      uint64
	TotalMetricsDatapointsCount uint64
	LogsBytesCount              uint64
	MetricsBytesCount           uint64
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
	LogsBytesCount         uint64
	MetricsBytesCount      uint64
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
			logsBytesAsString := strconv.FormatUint(ustats[orgid].LogsBytesCount, 10)
			metricsBytesAsString := strconv.FormatUint(ustats[orgid].MetricsBytesCount, 10)
			record = []string{bytesAsString, logLinesAsString, metricCountAsString, epochAsString, logsBytesAsString, metricsBytesAsString}
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
			atomic.StoreUint64(&ustats[orgid].LogsBytesCount, 0)
			atomic.StoreUint64(&ustats[orgid].MetricsBytesCount, 0)

			return nil
		}
	}
	return nil
}

func UpdateStats(logsBytesCount uint64, logLinesCount uint64, orgid uint64) {
	if _, ok := ustats[orgid]; !ok {
		ustats[orgid] = &Stats{}
	}
	atomic.AddUint64(&ustats[orgid].BytesCount, logsBytesCount)
	atomic.AddUint64(&ustats[orgid].LogLinesCount, logLinesCount)
	atomic.AddUint64(&ustats[orgid].TotalBytesCount, logsBytesCount)
	atomic.AddUint64(&ustats[orgid].TotalLogLinesCount, logLinesCount)
	atomic.AddUint64(&ustats[orgid].LogsBytesCount, logsBytesCount)
}

func UpdateMetricsStats(metricsBytesCount uint64, incomingMetrics uint64, orgid uint64) {
	if _, ok := ustats[orgid]; !ok {
		ustats[orgid] = &Stats{}
	}
	atomic.AddUint64(&ustats[orgid].BytesCount, metricsBytesCount)
	atomic.AddUint64(&ustats[orgid].MetricsDatapointsCount, incomingMetrics)
	atomic.AddUint64(&ustats[orgid].TotalBytesCount, metricsBytesCount)
	atomic.AddUint64(&ustats[orgid].TotalMetricsDatapointsCount, incomingMetrics)
	atomic.AddUint64(&ustats[orgid].MetricsBytesCount, metricsBytesCount)
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

// Calculate total bytesCount,linesCount and return hourly / daily / minute count
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
	var intervalMinutes uint32
	var err error
	runningTs := startEpoch
	if granularity == ByMinute {
		intervalMinutes, err = CalculateIntervalForStatsByMinute(uint32(pastXhours * 60))
		if err != nil {
			return nil, err
		}

		for runningTs.Before(endEpoch) {
			// Truncate runningTs to the nearest intervalMinutes
			truncatedTs := runningTs.Truncate(time.Duration(intervalMinutes) * time.Minute)
			bucketInterval = truncatedTs.Format("2006-01-02T15:04")
			resultMap[bucketInterval] = ReadStats{} // Initialize the bucket if not already present
			runningTs = runningTs.Add(time.Duration(intervalMinutes) * time.Minute)
		}
	} else if granularity == Daily {
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
			log.Errorf("GetUsageStats: error opening stats file = %v, err= %v", filename, err)
			continue
		}
		defer fd.Close()

		r := csv.NewReader(fd)
		// FieldsPerRecord is set to -1 to allow variable number of fields per record, as the data format has evolved over time.
		// Make sure to handle the different data formats in the loop below to avoid panics.
		r.FieldsPerRecord = -1
		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Errorf("GetUsageStats: error reading stats file = %v, err= %v", filename, err)
				break
			}

			readStats, err := parseRecord(record)
			if err != nil {
				log.Errorf("GetUsageStats: error parsing record in file = %v, record = %v, err= %v", filename, record, err)
				continue
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
		} else if granularity == ByMinute {
			// Truncate the timestamp to the nearest intervalMinutes and format it as a string.
			// For example, if rStat.TimeStamp is "20:47" and intervalMinutes is 10,
			// it will truncate the time to "20:40" and format it as "2006-01-02T20:40" to store in the resultMap.
			bucketInterval = rStat.TimeStamp.Truncate(time.Duration(intervalMinutes) * time.Minute).Format("2006-01-02T15:04")
		}
		if entry, ok := resultMap[bucketInterval]; ok {
			entry.EventCount += rStat.EventCount
			entry.MetricsDatapointsCount += rStat.MetricsDatapointsCount
			entry.BytesCount += rStat.BytesCount
			entry.LogsBytesCount += rStat.LogsBytesCount
			entry.MetricsBytesCount += rStat.MetricsBytesCount
			entry.TimeStamp = rStat.TimeStamp
			resultMap[bucketInterval] = entry
		} else {
			resultMap[bucketInterval] = rStat
		}
	}
	return resultMap, nil
}

// The data format has evolved over time:
// - Initially, it was: bytes, eventCount, time
// - Then, metrics were added: bytes, eventCount, metricCount, time
// - Later, logsBytesCount and metricsBytesCount were added. However, the new format is backward compatible with the old formats.
// The current format is: bytes, eventCount, metricCount, time, logsBytesCount, metricsBytesCount
func parseRecord(record []string) (ReadStats, error) {
	var readStats ReadStats
	var err error

	if len(record) < 3 {
		return readStats, fmt.Errorf("invalid record length: %d", len(record))
	}

	readStats.BytesCount, err = strconv.ParseUint(record[0], 10, 64)
	if err != nil {
		return readStats, err
	}
	readStats.EventCount, err = strconv.ParseUint(record[1], 10, 64)
	if err != nil {
		return readStats, err
	}

	switch len(record) {
	case 3:
		tsString, err := strconv.ParseInt(record[2], 10, 64)
		if err != nil {
			return readStats, err
		}
		readStats.TimeStamp = time.Unix(tsString, 0)
		readStats.LogsBytesCount = readStats.BytesCount
		readStats.MetricsBytesCount = 0

	case 4:
		readStats.MetricsDatapointsCount, err = strconv.ParseUint(record[2], 10, 64)
		if err != nil {
			return readStats, err
		}
		tsString, err := strconv.ParseInt(record[3], 10, 64)
		if err != nil {
			return readStats, err
		}
		readStats.TimeStamp = time.Unix(tsString, 0)
		readStats.LogsBytesCount = readStats.BytesCount
		readStats.MetricsBytesCount = 0

	case 6:
		readStats.MetricsDatapointsCount, err = strconv.ParseUint(record[2], 10, 64)
		if err != nil {
			return readStats, err
		}
		tsString, err := strconv.ParseInt(record[3], 10, 64)
		if err != nil {
			return readStats, err
		}
		readStats.LogsBytesCount, err = strconv.ParseUint(record[4], 10, 64)
		if err != nil {
			return readStats, err
		}
		readStats.MetricsBytesCount, err = strconv.ParseUint(record[5], 10, 64)
		if err != nil {
			return readStats, err
		}
		readStats.TimeStamp = time.Unix(tsString, 0)
	default:
		err = fmt.Errorf("invalid record length: %d", len(record))
		return readStats, err
	}

	return readStats, nil
}

func CalculateIntervalForStatsByMinute(timerangeMinutes uint32) (uint32, error) {
	for _, step := range timeIntervalsForStatsByMinute {
		if timerangeMinutes/step <= 24 {
			return step, nil
		}
	}

	// If no suitable step is found, return an error
	return 0, errors.New("no suitable step found")
}
