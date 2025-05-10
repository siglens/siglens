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
	"sync"
	"sync/atomic"
	"time"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/instrumentation"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type UsageStatsGranularity uint8

var mu sync.Mutex

const MIN_IN_MS = 60_000

var timeIntervalsForStatsByMinute = []uint32{5, 10, 20, 30, 40, 50, 60}

const (
	Hourly UsageStatsGranularity = iota + 1
	Daily
	ByMinute
	Monthly
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
	TraceBytesCount             uint64
	TraceSpanCount              uint64
	TotalTraceSpanCount         uint64
	ActiveSeriesCount           uint64
}

var ustats = make(map[int64]*Stats)

var msgPrinter *message.Printer

type QueryStats struct {
	QueryCount                uint64
	QueriesSinceInstall       uint64
	ActiveQueryCount          int
	TotalRespTimeSinceRestart float64
	TotalRespTimeSinceInstall float64
	mu                        sync.Mutex
}

var QueryStatsMap = make(map[int64]*QueryStats)

type ReadStats struct {
	TotalBytesCount        uint64
	EventCount             uint64
	MetricsDatapointsCount uint64
	TimeStamp              time.Time
	LogsBytesCount         uint64
	MetricsBytesCount      uint64
	TraceBytesCount        uint64
	TraceSpanCount         uint64
	ActiveSeriesCount      uint64
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
		QueryCount:                0,
		QueriesSinceInstall:       0,
		ActiveQueryCount:          0,
		TotalRespTimeSinceRestart: 0,
		TotalRespTimeSinceInstall: 0,
	}
	err := ReadQueryStats(0)
	if err != nil {
		log.Errorf("ReadQueryStats from file failed:%v\n", err)
	}
}

func ReadQueryStats(orgid int64) error {
	filename := getQueryStatsFilename(getBaseQueryStatsDir(orgid))
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return utils.TeeErrorf("readQueryStats: failed to open file, err=%v filename=%v", err, filename)
	}
	defer fd.Close()
	r := csv.NewReader(fd)
	val, err := r.ReadAll()
	if err != nil {
		return utils.TeeErrorf("readQueryStats: read records failed, err=%v", err)
	}
	if len(val) > 0 {
		lastRecord := val[len(val)-1]
		if len(lastRecord) < 1 {
			return utils.TeeErrorf("readQueryStats: last record has insufficient fields %v", lastRecord)
		}
		flushedQueriesSinceInstall, err := strconv.ParseUint(lastRecord[0], 10, 64)
		if err != nil {
			return utils.TeeErrorf("readQueryStats: failed to parse flushedQueriesSinceInstall(lastRecord[0]), err=%v lastRecord=%v", err, lastRecord)
		}

		flushedTotalRespTimeSinceInstall := 0.0
		if len(lastRecord) > 1 {
			flushedTotalRespTimeSinceInstall, err = strconv.ParseFloat(lastRecord[1], 64)
			if err != nil {
				return utils.TeeErrorf("readQueryStats: failed to parse flushedTotalRespTimeSinceInstall(lastRecord[1]), err=%v lastRecord=%v", err, lastRecord)
			}
		}
		if QueryStatsMap == nil {
			return utils.TeeErrorf("readQueryStats: QueryStatsMap is nil")
		}
		if qs, ok := QueryStatsMap[orgid]; ok {
			qs.QueriesSinceInstall = flushedQueriesSinceInstall
			qs.TotalRespTimeSinceInstall = flushedTotalRespTimeSinceInstall
		}
	}
	return nil
}

func GetBaseStatsDir(orgid int64) string {

	var sb strings.Builder
	timeNow := uint64(time.Now().UnixNano()) / uint64(time.Millisecond)
	sb.WriteString(config.GetDataPath() + "ingestnodes/" + config.GetHostID() + "/usageStats/")
	if orgid != 0 {
		sb.WriteString(strconv.FormatInt(orgid, 10))
		sb.WriteString("/")
	}
	t1 := time.Unix(int64(timeNow/1000), int64((timeNow%1000)*1000))
	sb.WriteString(t1.UTC().Format("2006/01/02"))
	sb.WriteString("/")
	basedir := sb.String()
	return basedir
}

func getBaseQueryStatsDir(orgid int64) string {

	var sb strings.Builder
	sb.WriteString(config.GetDataPath() + "querynodes/" + config.GetHostID() + "/")
	if orgid != 0 {
		sb.WriteString(strconv.FormatInt(orgid, 10))
		sb.WriteString("/")
	}
	basedir := sb.String()
	return basedir
}

func getBaseStatsDirs(startTime, endTime time.Time, orgid int64) []string {
	startTOD := (startTime.UnixMilli() / sutils.MS_IN_DAY) * sutils.MS_IN_DAY
	endTOD := (endTime.UnixMilli() / sutils.MS_IN_DAY) * sutils.MS_IN_DAY
	ingestDir := config.GetIngestNodeBaseDir()
	// read all files in dir

	files, err := os.ReadDir(ingestDir)
	if err != nil {
		log.Errorf("getBaseStatsDirs: read dir err=%v ", err)
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
				sb.WriteString(strconv.FormatInt(orgid, 10))
			}
			sb.WriteString("/")
			timeNow := uint64(fileStartTime.UnixNano()) / uint64(time.Millisecond)
			t1 := time.Unix(int64(timeNow/1000), int64((timeNow%1000)*1000))
			sb.WriteString(t1.UTC().Format("2006/01/02"))
			sb.WriteString("/")
			statsDirs = append(statsDirs, sb.String())
			fileStartTOD = fileStartTOD + sutils.MS_IN_DAY
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

func logStatSummary(myid int64) {
	if _, ok := ustats[myid]; ok {
		log.Infof("Ingest stats: past minute : myid=%v, events=%v, metrics=%v, traces=%v, bytes=%v, activeSeriesCount=%v",
			myid,
			msgPrinter.Sprintf("%v", ustats[myid].LogLinesCount),
			msgPrinter.Sprintf("%v", ustats[myid].MetricsDatapointsCount),
			msgPrinter.Sprintf("%v", ustats[myid].TraceSpanCount),
			msgPrinter.Sprintf("%v", ustats[myid].BytesCount),
			msgPrinter.Sprintf("%v", ustats[myid].ActiveSeriesCount))

		log.Infof("Ingest stats: total so far: myid=%v, events=%v, metrics=%v, traces=%v, bytes=%v",
			myid,
			msgPrinter.Sprintf("%v", ustats[myid].TotalLogLinesCount),
			msgPrinter.Sprintf("%v", ustats[myid].TotalMetricsDatapointsCount),
			msgPrinter.Sprintf("%v", ustats[myid].TotalTraceSpanCount),
			msgPrinter.Sprintf("%v", ustats[myid].TotalBytesCount))
	}
}

func GetTotalLogLines(orgid int64) uint64 {
	return ustats[orgid].TotalLogLinesCount
}

func FlushStatsToFile(orgid int64) error {
	if qs, ok := QueryStatsMap[orgid]; ok {
		filename := getQueryStatsFilename(getBaseQueryStatsDir(orgid))
		fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			return err
		}
		defer fd.Close()
		w := csv.NewWriter(fd)
		var records [][]string
		record := []string{
			strconv.FormatUint(qs.QueriesSinceInstall, 10),
			strconv.FormatFloat(qs.TotalRespTimeSinceInstall, 'f', 6, 64),
		}
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
		instrumentation.SetPastMinuteNumDataPoints(int64(ustats[orgid].MetricsDatapointsCount))
		instrumentation.SetPastMinuteActiveSeriesCount(int64(ustats[orgid].ActiveSeriesCount))

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
			traceBytesAsString := strconv.FormatUint(ustats[orgid].TraceBytesCount, 10)
			traceSpanCountAsString := strconv.FormatUint(ustats[orgid].TraceSpanCount, 10)
			activeSeriesCountAsString := strconv.FormatUint(ustats[orgid].ActiveSeriesCount, 10)

			record = []string{bytesAsString, logLinesAsString, metricCountAsString, epochAsString,
				logsBytesAsString, metricsBytesAsString, traceBytesAsString,
				traceSpanCountAsString, activeSeriesCountAsString}

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
			atomic.StoreUint64(&ustats[orgid].TraceBytesCount, 0)
			atomic.StoreUint64(&ustats[orgid].TraceSpanCount, 0)
			return nil
		}
	}
	return nil
}

func UpdateStats(logsBytesCount uint64, logLinesCount uint64, orgid int64) {
	if _, ok := ustats[orgid]; !ok {
		ustats[orgid] = &Stats{}
	}
	atomic.AddUint64(&ustats[orgid].BytesCount, logsBytesCount)
	atomic.AddUint64(&ustats[orgid].LogLinesCount, logLinesCount)
	atomic.AddUint64(&ustats[orgid].TotalBytesCount, logsBytesCount)
	atomic.AddUint64(&ustats[orgid].TotalLogLinesCount, logLinesCount)
	atomic.AddUint64(&ustats[orgid].LogsBytesCount, logsBytesCount)
}

func UpdateTracesStats(traceBytesCount uint64, traceSpanCount uint64, orgid int64) {
	if _, ok := ustats[orgid]; !ok {
		ustats[orgid] = &Stats{}
	}
	atomic.AddUint64(&ustats[orgid].BytesCount, traceBytesCount)
	atomic.AddUint64(&ustats[orgid].TraceBytesCount, traceBytesCount)
	atomic.AddUint64(&ustats[orgid].TraceSpanCount, traceSpanCount)
	atomic.AddUint64(&ustats[orgid].TotalTraceSpanCount, traceSpanCount)
	atomic.AddUint64(&ustats[orgid].TotalBytesCount, traceBytesCount)
}

func UpdateMetricsStats(metricsBytesCount uint64, incomingMetrics uint64, orgid int64) {
	if _, ok := ustats[orgid]; !ok {
		ustats[orgid] = &Stats{}
	}
	atomic.AddUint64(&ustats[orgid].BytesCount, metricsBytesCount)
	atomic.AddUint64(&ustats[orgid].MetricsDatapointsCount, incomingMetrics)
	atomic.AddUint64(&ustats[orgid].TotalBytesCount, metricsBytesCount)
	atomic.AddUint64(&ustats[orgid].TotalMetricsDatapointsCount, incomingMetrics)
	atomic.AddUint64(&ustats[orgid].MetricsBytesCount, metricsBytesCount)
}

func UpdateActiveSeriesCount(orgid int64, activeSeriesCount uint64) {
	if _, ok := ustats[orgid]; !ok {
		ustats[orgid] = &Stats{}
	}
	atomic.StoreUint64(&ustats[orgid].ActiveSeriesCount, activeSeriesCount)
}

func GetQueryStats(orgid int64) (uint64, float64, float64, uint64) {
	if _, ok := QueryStatsMap[orgid]; !ok {
		return 0, 0, 0, 0
	}
	return QueryStatsMap[orgid].QueryCount, QueryStatsMap[orgid].TotalRespTimeSinceRestart, QueryStatsMap[orgid].TotalRespTimeSinceInstall, QueryStatsMap[orgid].QueriesSinceInstall
}

func GetCurrentMetricsStats(orgid int64) (uint64, uint64) {
	return ustats[orgid].TotalBytesCount, ustats[orgid].TotalMetricsDatapointsCount
}

func UpdateQueryStats(queryCount uint64, respTime float64, orgid int64) {
	mu.Lock()
	if _, ok := QueryStatsMap[orgid]; !ok {
		QueryStatsMap[orgid] = &QueryStats{
			QueryCount:                0,
			TotalRespTimeSinceRestart: 0,
			TotalRespTimeSinceInstall: 0,
			ActiveQueryCount:          0,
		}
	}
	mu.Unlock()

	qs := QueryStatsMap[orgid]
	atomic.AddUint64(&qs.QueryCount, queryCount)
	atomic.AddUint64(&qs.QueriesSinceInstall, queryCount)
	qs.mu.Lock()
	qs.TotalRespTimeSinceRestart += respTime
	qs.TotalRespTimeSinceInstall += respTime
	qs.mu.Unlock()
}

func readUsageStats(startEpoch, endEpoch time.Time, orgid int64) ([]*ReadStats, error) {

	allStatsMap := make([]*ReadStats, 0)

	statsFnames := getBaseStatsDirs(startEpoch, endEpoch, orgid)

	for _, statsFile := range statsFnames {
		filename := getStatsFilename(statsFile)
		fd, err := os.OpenFile(filename, os.O_RDONLY, 0666)
		if err != nil {
			if !os.IsNotExist(err) {
				log.Errorf("GetUsageStats: error opening stats file = %v, err= %v", filename, err)
			}
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

			readStats, err := parseStatsRecord(record)
			if err != nil {
				log.Errorf("GetUsageStats: error parsing record in file = %v, record = %v, err= %v", filename, record, err)
				continue
			}

			if readStats.TimeStamp.After(startEpoch) && readStats.TimeStamp.Before(endEpoch) {
				allStatsMap = append(allStatsMap, readStats)
			}
		}
	}

	return allStatsMap, nil
}

// Calculate total bytesCount,linesCount and return hourly / daily / minute count
func GetUsageStats(startTs int64, endTs int64, granularity UsageStatsGranularity, orgid int64) (map[string]*ReadStats, error) {

	endEpoch := time.Unix(endTs, 0)
	startEpoch := time.Unix(startTs, 0)
	startTOD := (startEpoch.UnixMilli() / sutils.MS_IN_DAY) * sutils.MS_IN_DAY
	endTOD := (endEpoch.UnixMilli() / sutils.MS_IN_DAY) * sutils.MS_IN_DAY
	startTOH := (startEpoch.UnixMilli() / sutils.MS_IN_HOUR) * sutils.MS_IN_HOUR
	endTOH := (endEpoch.UnixMilli() / sutils.MS_IN_HOUR) * sutils.MS_IN_HOUR

	resultMap := make(map[string]*ReadStats)
	var bucketInterval string
	var intervalMinutes uint32
	var err error
	runningTs := startEpoch

	if granularity == ByMinute {
		timeRangeMinutes := uint32((endTs - startTs) / 60)
		intervalMinutes, err = CalculateIntervalForStatsByMinute(timeRangeMinutes)
		if err != nil {
			return nil, err
		}

		for runningTs.Before(endEpoch) {
			// Truncate runningTs to the nearest intervalMinutes
			truncatedTs := runningTs.Truncate(time.Duration(intervalMinutes) * time.Minute)
			bucketInterval = strconv.FormatInt(truncatedTs.Unix(), 10)
			resultMap[bucketInterval] = &ReadStats{} // Initialize the bucket if not already present
			runningTs = runningTs.Add(time.Duration(intervalMinutes) * time.Minute)
		}
	} else if granularity == Daily {
		for endTOD >= startTOD {
			roundedToDay := time.Date(runningTs.Year(), runningTs.Month(), runningTs.Day(), 0, 0, 0, 0, runningTs.Location())
			bucketInterval = strconv.FormatInt(roundedToDay.Unix(), 10)
			runningTs = runningTs.Add(24 * time.Hour)
			startTOD = startTOD + sutils.MS_IN_DAY
			resultMap[bucketInterval] = &ReadStats{}
		}
	} else if granularity == Hourly {
		for endTOH >= startTOH {
			roundedToHour := time.Date(runningTs.Year(), runningTs.Month(), runningTs.Day(), runningTs.Hour(), 0, 0, 0, runningTs.Location())
			bucketInterval = strconv.FormatInt(roundedToHour.Unix(), 10)
			runningTs = runningTs.Add(1 * time.Hour)
			startTOH = startTOH + sutils.MS_IN_HOUR
			resultMap[bucketInterval] = &ReadStats{}
		}
	} else if granularity == Monthly {
		startOfMonth := time.Date(startEpoch.Year(), startEpoch.Month(), 1, 0, 0, 0, 0, startEpoch.Location())
		runningTs = startOfMonth

		endMonth := time.Date(endEpoch.Year(), endEpoch.Month(), 1, 0, 0, 0, 0, endEpoch.Location())

		for !runningTs.After(endMonth) {
			roundedToMonth := time.Date(runningTs.Year(), runningTs.Month(), 1, 0, 0, 0, 0, runningTs.Location())
			bucketInterval = strconv.FormatInt(roundedToMonth.Unix(), 10)
			resultMap[bucketInterval] = &ReadStats{}

			runningTs = time.Date(runningTs.Year(), runningTs.Month()+1, 1, 0, 0, 0, 0, runningTs.Location())
		}
	} else {
		return nil, fmt.Errorf("GetUsageStats: unknown granularity value: %v", granularity)
	}

	allStatsMap, err := readUsageStats(startEpoch, endEpoch, orgid)
	if err != nil {
		return nil, err
	}

	ascBuckets := map[string][]uint64{}
	for _, rStat := range allStatsMap {
		if granularity == Daily {
			roundedToDay := time.Date(rStat.TimeStamp.Year(), rStat.TimeStamp.Month(), rStat.TimeStamp.Day(), 0, 0, 0, 0, rStat.TimeStamp.Location())
			bucketInterval = strconv.FormatInt(roundedToDay.Unix(), 10)
		} else if granularity == Hourly {
			roundedToHour := time.Date(rStat.TimeStamp.Year(), rStat.TimeStamp.Month(), rStat.TimeStamp.Day(), rStat.TimeStamp.Hour(), 0, 0, 0, rStat.TimeStamp.Location())
			bucketInterval = strconv.FormatInt(roundedToHour.Unix(), 10)
		} else if granularity == ByMinute {
			// Truncate the timestamp to the nearest intervalMinutes and format it as a string.
			// For example, if rStat.TimeStamp is "20:47" and intervalMinutes is 10,
			// it will truncate the time to "20:40" and format it as "2006-01-02T20:40" to store in the resultMap.
			bucketInterval = strconv.FormatInt(rStat.TimeStamp.Truncate(time.Duration(intervalMinutes)*time.Minute).Unix(), 10)
		} else if granularity == Monthly {
			roundedToMonth := time.Date(rStat.TimeStamp.Year(), rStat.TimeStamp.Month(), 1, 0, 0, 0, 0, rStat.TimeStamp.Location())
			bucketInterval = strconv.FormatInt(roundedToMonth.Unix(), 10)
		} else {
			return nil, fmt.Errorf("GetUsageStats: unknown granularity value: %v", granularity)
		}
		entry, ok := resultMap[bucketInterval]
		if !ok {
			resultMap[bucketInterval] = &ReadStats{}
			entry = resultMap[bucketInterval]
		}
		entry.EventCount += rStat.EventCount
		entry.MetricsDatapointsCount += rStat.MetricsDatapointsCount
		entry.TotalBytesCount += rStat.TotalBytesCount
		entry.LogsBytesCount += rStat.LogsBytesCount
		entry.MetricsBytesCount += rStat.MetricsBytesCount
		entry.TimeStamp = rStat.TimeStamp
		entry.TraceBytesCount += rStat.TraceBytesCount
		entry.TraceSpanCount += rStat.TraceSpanCount

		// for ActiveSeriesCount we cannot keep adding them, but rather we want to accumulate all the values
		// for each bucket, then the average of that specific bucket, since it is a gauge
		// here we just accumulate them, and below we will do the averaging, once all buckets have been created
		curBucket, ok := ascBuckets[bucketInterval]
		if !ok {
			curBucket = make([]uint64, 0)
		}
		curBucket = append(curBucket, rStat.ActiveSeriesCount)
		ascBuckets[bucketInterval] = curBucket
	}

	for buckInterval, entry := range resultMap {
		allAscValues, ok := ascBuckets[buckInterval]
		if ok && len(allAscValues) > 0 {
			aggVal := uint64(0)
			for _, val := range allAscValues {
				aggVal += val
			}
			aggVal /= uint64(len(allAscValues))
			entry.ActiveSeriesCount = aggVal
		}
	}
	return resultMap, nil
}

// The data format has evolved over time:
// - Initially, it was: bytes, eventCount, time
// - Then, metrics were added: bytes, eventCount, metricCount, time
// - Later, logsBytesCount and metricsBytesCount were added: bytes, eventCount, metricCount, time, logsBytesCount, metricsBytesCount
// - Later: bytes, eventCount, metricCount, time, logsBytesCount, metricsBytesCount, traceBytesAsCount, traceCount.
// Current format: bytes, eventCount, metricCount, time, logsBytesCount, metricsBytesCount, traceBytesAsCount, traceCount, activeSeriesCount
//
//	However, the new format is backward compatible with the old formats.
func parseStatsRecord(record []string) (*ReadStats, error) {
	readStats := &ReadStats{}
	var err error

	if len(record) < 3 {
		return readStats, fmt.Errorf("parseStatsRecord: invalid record length: %d", len(record))
	}

	readStats.TotalBytesCount, err = strconv.ParseUint(record[0], 10, 64)
	if err != nil {
		return readStats, fmt.Errorf("parseStatsRecord: could not parse BytesCount field '%v': %w", record[0], err)
	}
	readStats.EventCount, err = strconv.ParseUint(record[1], 10, 64)
	if err != nil {
		return readStats, fmt.Errorf("parseStatsRecord: could not parse EventCount field '%v': %w", record[1], err)
	}

	switch len(record) {
	case 3:
		tsString, err := strconv.ParseInt(record[2], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse timestamp field '%v': %w", record[2], err)
		}
		readStats.TimeStamp = time.Unix(tsString, 0)
		readStats.LogsBytesCount = readStats.TotalBytesCount
		readStats.MetricsBytesCount = 0

	case 4:
		readStats.MetricsDatapointsCount, err = strconv.ParseUint(record[2], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse MetricsDatapointsCount field '%v': %w", record[2], err)
		}
		tsString, err := strconv.ParseInt(record[3], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse timestamp field '%v': %w", record[3], err)
		}
		readStats.TimeStamp = time.Unix(tsString, 0)
		readStats.LogsBytesCount = readStats.TotalBytesCount
		readStats.MetricsBytesCount = 0

	case 6:
		readStats.MetricsDatapointsCount, err = strconv.ParseUint(record[2], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse MetricsDatapointsCount field '%v': %w", record[2], err)
		}
		tsString, err := strconv.ParseInt(record[3], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse timestamp field '%v': %w", record[3], err)
		}
		readStats.LogsBytesCount, err = strconv.ParseUint(record[4], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse LogsBytesCount field '%v': %w", record[4], err)
		}
		readStats.MetricsBytesCount, err = strconv.ParseUint(record[5], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse MetricsBytesCount field '%v': %w", record[5], err)
		}
		readStats.TimeStamp = time.Unix(tsString, 0)
	case 8:
		readStats.MetricsDatapointsCount, err = strconv.ParseUint(record[2], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse MetricsDatapointsCount field '%v': %w", record[2], err)
		}
		tsString, err := strconv.ParseInt(record[3], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse timestamp field '%v': %w", record[3], err)
		}
		readStats.LogsBytesCount, err = strconv.ParseUint(record[4], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse LogsBytesCount field '%v': %w", record[4], err)
		}
		readStats.MetricsBytesCount, err = strconv.ParseUint(record[5], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse MetricsBytesCount field '%v': %w", record[5], err)
		}
		readStats.TraceBytesCount, err = strconv.ParseUint(record[6], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse TraceBytesCount field '%v': %w", record[6], err)
		}
		readStats.TraceSpanCount, err = strconv.ParseUint(record[7], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse TracesCount field '%v': %w", record[7], err)
		}
		readStats.TimeStamp = time.Unix(tsString, 0)
	case 9:
		readStats.MetricsDatapointsCount, err = strconv.ParseUint(record[2], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse MetricsDatapointsCount field '%v': %w", record[2], err)
		}
		tsString, err := strconv.ParseInt(record[3], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse timestamp field '%v': %w", record[3], err)
		}
		readStats.TimeStamp = time.Unix(tsString, 0)
		readStats.LogsBytesCount, err = strconv.ParseUint(record[4], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse LogsBytesCount field '%v': %w", record[4], err)
		}
		readStats.MetricsBytesCount, err = strconv.ParseUint(record[5], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse MetricsBytesCount field '%v': %w", record[5], err)
		}
		readStats.TraceBytesCount, err = strconv.ParseUint(record[6], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse TraceBytesCount field '%v': %w", record[6], err)
		}
		readStats.TraceSpanCount, err = strconv.ParseUint(record[7], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse TracesCount field '%v': %w", record[7], err)
		}
		readStats.ActiveSeriesCount, err = strconv.ParseUint(record[8], 10, 64)
		if err != nil {
			return readStats, fmt.Errorf("parseStatsRecord: could not parse ActiveSeriesCount field '%v': %w", record[8], err)
		}
	default:
		err = fmt.Errorf("parseStatsRecord: invalid record length: %d", len(record))
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

func GetActiveSeriesCounts(pastXhours uint64, orgid int64) ([]uint64, error) {

	endEpoch := time.Now()
	startEpoch := endEpoch.Add(-(time.Duration(pastXhours) * time.Hour))

	allStatsMap, err := readUsageStats(startEpoch, endEpoch, orgid)
	if err != nil {
		return nil, err
	}

	retVal := make([]uint64, len(allStatsMap))
	for i, rstat := range allStatsMap {
		retVal[i] = rstat.ActiveSeriesCount
	}
	return retVal, nil
}
