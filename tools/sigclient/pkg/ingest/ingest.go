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

package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"verifier/pkg/utils"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"

	"github.com/dustin/go-humanize"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/bytebufferpool"
	"github.com/valyala/fastrand"
)

type IngestType int

// Node Types
const (
	_ IngestType = iota
	ESBulk
	OpenTSDB
	PrometheusRemoteWrite
)

func (q IngestType) String() string {
	switch q {
	case ESBulk:
		return "ES Bulk"
	case OpenTSDB:
		return "OTSDB"
	case PrometheusRemoteWrite:
		return "Prometheus"
	default:
		return "UNKNOWN"
	}
}

const PRINT_FREQ = 100_000

// returns any errors encountered. It is the caller's responsibility to attempt retries
func sendRequest(iType IngestType, client *http.Client, lines []byte, url string, bearerToken string) error {

	bearerToken = "Bearer " + strings.TrimSpace(bearerToken)

	buf := bytes.NewBuffer(lines)

	var requestStr string
	switch iType {
	case ESBulk:
		requestStr = url + "/_bulk"
	case OpenTSDB:
		requestStr = url + "/api/put"
	case PrometheusRemoteWrite:
		requestStr = url + "/api/v1/write"
	default:
		log.Fatalf("unknown ingest type %+v", iType)
		return fmt.Errorf("unknown ingest type %+v", iType)
	}

	req, err := http.NewRequest("POST", requestStr, buf)
	if bearerToken != "" {
		req.Header.Add("Authorization", bearerToken)
	}

	switch iType {
	case PrometheusRemoteWrite:
		req.Header.Set("Content-Type", "application/x-protobuf")
		req.Header.Set("Content-Encoding", "snappy")
		req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	default:
		req.Header.Set("Content-Type", "application/json")
	}

	if err != nil {
		log.Errorf("sendRequest: http.NewRequest ERROR: %v", err)
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Errorf("sendRequest: client.Do ERROR: %v", err)
		return err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("sendRequest: client.Do ERROR: %v", err)
		return err
	}
	// Check if the Status code is not 200
	if resp.StatusCode != http.StatusOK {
		log.Errorf("sendRequest: client.Do ERROR Response: %v", "StatusCode: "+fmt.Sprint(resp.StatusCode)+": "+string(respBody))
		return fmt.Errorf("sendRequest: client.Do ERROR Response: %v", "StatusCode: "+fmt.Sprint(resp.StatusCode)+": "+string(respBody))
	}
	return nil
}

func generateBody(iType IngestType, recs int, i int, rdr utils.Generator,
	actLines []string, bb *bytebufferpool.ByteBuffer) ([]map[string]interface{}, []byte, error) {

	switch iType {
	case ESBulk:
		actionLine := actLines[i%len(actLines)]
		return generateESBody(recs, actionLine, rdr, bb)
	case OpenTSDB:
		// TODO: make generateOpenTSDBBody return the raw logs as well
		payload, err := generateOpenTSDBBody(recs, rdr)
		return nil, payload, err
	default:
		log.Fatalf("Unsupported ingest type %s", iType.String())
	}
	return nil, nil, fmt.Errorf("unsupported ingest type %s", iType.String())
}

func generateESBody(recs int, actionLine string, rdr utils.Generator,
	bb *bytebufferpool.ByteBuffer) ([]map[string]interface{}, []byte, error) {

	allLogs := make([]map[string]interface{}, 0, recs)
	for i := 0; i < recs; i++ {
		_, _ = bb.WriteString(actionLine)
		rawLog, err := rdr.GetRawLog()
		if err != nil {
			return nil, nil, err
		}

		logCopy := make(map[string]interface{}, len(rawLog))
		for k, v := range rawLog {
			logCopy[k] = v
		}
		allLogs = append(allLogs, logCopy)

		logline, err := json.Marshal(rawLog)
		if err != nil {
			return nil, nil, err
		}
		_, _ = bb.Write(logline)
		_, _ = bb.WriteString("\n")
	}

	payLoad := bb.Bytes()
	return allLogs, payLoad, nil
}

func generateOpenTSDBBody(recs int, rdr utils.Generator) ([]byte, error) {
	finalPayLoad := make([]interface{}, recs)
	for i := 0; i < recs; i++ {
		currPayload, err := rdr.GetRawLog()
		if err != nil {
			return nil, err
		}
		finalPayLoad[i] = currPayload
	}
	retVal, err := json.Marshal(finalPayLoad)
	if err != nil {
		return nil, err
	}
	return retVal, nil
}

var preGeneratedSeries []map[string]interface{}
var uniqueSeriesMap = make(map[string]struct{})
var seriesId uint64

func generateUniquePayload(rdr *utils.MetricsGenerator) (map[string]interface{}, error) {
	var currPayload map[string]interface{}
	var err error
	for {
		currPayload, err = rdr.GetRawLog()
		if err != nil {
			log.Errorf("generateUniquePayload: failed to get raw log: %+v", err)
			return nil, err
		}
		metricName := currPayload["metric"].(string)
		tags := currPayload["tags"].(map[string]interface{})
		var builder strings.Builder
		builder.WriteString(metricName)
		for key, value := range tags {
			builder.WriteString(key)
			builder.WriteString(fmt.Sprintf("%v", value))
		}
		id := builder.String()
		if _, exists := uniqueSeriesMap[id]; !exists {
			uniqueSeriesMap[id] = struct{}{}
			break
		}
	}

	return currPayload, nil
}

func generatePredefinedSeries(nMetrics int, cardinality uint64, gentype string) error {
	preGeneratedSeries = make([]map[string]interface{}, cardinality)
	rdr, err := utils.InitMetricsGenerator(nMetrics, gentype)
	if err != nil {
		log.Errorf("generatePredefinedSeries: failed to initialize metrics generator: %+v", err)
		return err
	}
	start := time.Now()
	for i := uint64(0); i < cardinality; i++ {
		currPayload, err := generateUniquePayload(rdr)
		if err != nil {
			log.Errorf("generatePredefinedSeries: failed to generate unique payload: %+v", err)
			return err
		}
		preGeneratedSeries[i] = currPayload
	}
	elapsed := time.Since(start)
	log.Infof("Generated %d unique series in %v", len(preGeneratedSeries), elapsed)
	return nil
}

func generateBodyFromPredefinedSeries(recs int, preGeneratedSeriesLength uint64) ([]byte, error) {
	finalPayLoad := make([]interface{}, recs)
	for i := 0; i < recs; i++ {
		series := preGeneratedSeries[(seriesId+uint64(i))%preGeneratedSeriesLength]
		finalPayLoad[i] = series
	}
	retVal, err := json.Marshal(finalPayLoad)
	if err != nil {
		return nil, err
	}
	return retVal, nil
}

func generatePrometheusRemoteWriteBody(recs int, preGeneratedSeriesLength uint64) ([]byte, error) {
	var timeSeriesList []prompb.TimeSeries
	for i := 0; i < recs; i++ {
		series := preGeneratedSeries[(seriesId+uint64(i))%preGeneratedSeriesLength]
		metricName, _ := series["metric"].(string)
		timestamp, _ := series["timestamp"].(int64)
		value, _ := series["value"].(float64)
		tags, _ := series["tags"].(map[string]interface{})

		var labels []prompb.Label
		labels = append(labels, prompb.Label{Name: "__name__", Value: metricName})
		for key, val := range tags {
			labels = append(labels, prompb.Label{Name: key, Value: fmt.Sprintf("%v", val)})
		}

		timeSeries := prompb.TimeSeries{
			Labels: labels,
			Samples: []prompb.Sample{
				{Value: value, Timestamp: timestamp},
			},
		}
		timeSeriesList = append(timeSeriesList, timeSeries)
	}

	writeReq := &prompb.WriteRequest{
		Timeseries: timeSeriesList,
	}

	pbData, err := proto.Marshal(writeReq)
	if err != nil {
		return nil, fmt.Errorf("generatePrometheusRemoteWriteBody : failed to marshal protobuf: %v", err)
	}
	compressedData := snappy.Encode(nil, pbData)
	return compressedData, nil
}

func runIngestion(iType IngestType, rdr utils.Generator, wg *sync.WaitGroup, url string, totalEvents int, continuous bool,
	batchSize, processNo int, indexPrefix string, ctr *uint64, bearerToken string, indexName string, numIndices int,
	eventsPerDayPerProcess int, totalBytes *uint64, callback func([]map[string]interface{})) {

	defer wg.Done()
	eventCounter := 0
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 500
	t.MaxConnsPerHost = 100
	t.MaxIdleConnsPerHost = 100
	client := &http.Client{
		Timeout:   100 * time.Second,
		Transport: t,
	}

	var actLines []string
	if iType == ESBulk {
		actLines = populateActionLines(indexPrefix, indexName, numIndices)
	}

	i := 0
	var bb *bytebufferpool.ByteBuffer
	var rawLogs []map[string]interface{}
	var payload []byte
	var err error
	maxDuration := 2 * time.Hour
	estimatedMilliSecsPerBatch := 0
	if eventsPerDayPerProcess > 0 {
		estimatedMilliSecsPerBatch = (batchSize * 24 * 60 * 60 * 1000) / eventsPerDayPerProcess
	}

	for continuous || eventCounter < totalEvents {
		recsInBatch := batchSize
		if !continuous && eventCounter+batchSize > totalEvents {
			recsInBatch = totalEvents - eventCounter
		}
		i++
		if iType == ESBulk {
			bb = bytebufferpool.Get()
		}
		if iType == OpenTSDB {
			payload, err = generateBodyFromPredefinedSeries(recsInBatch, uint64(len(preGeneratedSeries)))
			seriesId += uint64(recsInBatch)
		} else if iType == PrometheusRemoteWrite {
			payload, err = generatePrometheusRemoteWriteBody(recsInBatch, uint64(len(preGeneratedSeries)))
			seriesId += uint64(recsInBatch)
		} else {
			rawLogs, payload, err = generateBody(iType, recsInBatch, i, rdr, actLines, bb)
		}
		if err != nil {
			log.Errorf("Error generating bulk body!: %v", err)
			if iType == ESBulk {
				bytebufferpool.Put(bb)
			}
			return
		}
		startTime := time.Now()
		var reqErr error
		retryCounter := 1
		for {
			reqErr = sendRequest(iType, client, payload, url, bearerToken)
			if reqErr == nil {
				break
			}
			elapsed := time.Since(startTime)
			if elapsed >= maxDuration {
				log.Infof("Error sending request. Exceeded maximum retry duration of %v hr. Exiting.", int(maxDuration.Hours()))
				break
			}
			sleepTime := time.Second * time.Duration(5*(retryCounter))
			log.Errorf("Error sending request. Attempt: %d. Sleeping for %+v s before retrying.", retryCounter, sleepTime.String())
			retryCounter++
			time.Sleep(sleepTime)
		}

		if callback != nil {
			callback(rawLogs)
		}

		SendPerformanceData(rdr)

		if iType == ESBulk {
			bytebufferpool.Put(bb)
		}
		if reqErr != nil {
			log.Fatalf("Error sending request after %v hr ! %v", int(maxDuration.Hours()), reqErr)
			return
		}
		eventCounter += recsInBatch
		atomic.AddUint64(ctr, uint64(recsInBatch))
		atomic.AddUint64(totalBytes, uint64(len(payload)))
		if estimatedMilliSecsPerBatch > 0 {
			timeTaken := int(time.Since(startTime).Milliseconds())
			if timeTaken < estimatedMilliSecsPerBatch {
				napTime := estimatedMilliSecsPerBatch - timeTaken
				log.Debugf("ProcessId: %v finished early in %v ms. Sleeping for %+v ms before next batch", processNo, timeTaken, napTime)
				time.Sleep(time.Duration(napTime) * time.Millisecond)
			} else {
				log.Debugf("ProcessId: %v finished late, took %v ms. Expected %v ms", processNo, timeTaken, estimatedMilliSecsPerBatch)
			}
		}
	}
}

func SendPerformanceData(rdr utils.Generator) {
	dynamicUserGen, isDUG := rdr.(*utils.DynamicUserGenerator)
	if !isDUG || dynamicUserGen.DataConfig == nil {
		return
	}
	dynamicUserGen.DataConfig.SendLog()
}

func populateActionLines(idxPrefix string, indexName string, numIndices int) []string {
	if numIndices == 0 {
		log.Fatalf("number of indices cannot be zero!")
	}
	actionLines := make([]string, numIndices)
	for i := 0; i < numIndices; i++ {
		var idx string
		if indexName != "" {
			idx = indexName
		} else {
			idx = fmt.Sprintf("%s-%d", idxPrefix, i)
		}
		actionLine := "{\"index\": {\"_index\": \"" + idx + "\", \"_type\": \"_doc\"}}\n"
		actionLines[i] = actionLine
	}
	return actionLines
}

func getReaderFromArgs(iType IngestType, nummetrics int, gentype string, str string, ts bool, generatorDataConfig *utils.GeneratorDataConfig, processIndex int) (utils.Generator, error) {

	if iType == OpenTSDB || iType == PrometheusRemoteWrite {
		rdr, err := utils.InitMetricsGenerator(nummetrics, gentype)
		if err != nil {
			return rdr, err
		}
		err = rdr.Init(str)
		return rdr, err
	}
	var rdr utils.Generator
	var err error
	switch gentype {
	case "", "static":
		log.Infof("Initializing static reader")
		rdr = utils.InitStaticGenerator(ts)
	case "dynamic-user":
		seed := int64(fastrand.Uint32n(1_000))
		accFakerSeed := int64(10000 + processIndex)
		rdr = utils.InitDynamicUserGenerator(ts, seed, accFakerSeed, generatorDataConfig)
	case "file":
		log.Infof("Initializing file reader from %s", str)
		rdr = utils.InitFileReader()
	case "benchmark":
		log.Infof("Initializing benchmark reader")
		seed := int64(1001 + processIndex)
		accFakerSeed := int64(10000 + processIndex)
		rdr = utils.InitDynamicUserGenerator(ts, seed, accFakerSeed, generatorDataConfig)
	case "functional":
		log.Infof("Initializing functional reader")
		seed := int64(1001 + processIndex)
		accFakerSeed := int64(10000 + processIndex)
		rdr, err = utils.InitFunctionalUserGenerator(ts, seed, accFakerSeed, generatorDataConfig, processIndex)
	case "performance":
		log.Infof("Initializing performance reader")
		seed := int64(1001 + processIndex)
		accFakerSeed := int64(10000 + processIndex)
		rdr, err = utils.InitPerfTestGenerator(ts, seed, accFakerSeed, generatorDataConfig, processIndex)
	case "k8s":
		log.Infof("Initializing k8s reader")
		seed := int64(1001)
		rdr = utils.InitK8sGenerator(ts, seed)
	default:
		return nil, fmt.Errorf("unsupported reader type %s. Options=[static,dynamic-user,file,benchmark]", gentype)
	}
	if err != nil {
		return rdr, err
	}
	err = rdr.Init(str)
	return rdr, err
}

func GetGeneratorDataConfig(maxColumns int, variableColums bool, minColumns int, uniqColumns int) *utils.GeneratorDataConfig {
	return utils.InitGeneratorDataConfig(maxColumns, variableColums, minColumns, uniqColumns)
}

func StartIngestion(iType IngestType, generatorType, dataFile string, totalEvents int, continuous bool,
	batchSize int, url string, indexPrefix string, indexName string, numIndices, processCount int, addTs bool,
	nMetrics int, bearerToken string, cardinality uint64, eventsPerDay uint64, iDataGeneratorConfig interface{},
	callback func(logs []map[string]interface{})) {

	log.Printf("Starting ingestion at %+v for %+v", url, iType.String())
	if iType == OpenTSDB || iType == PrometheusRemoteWrite {
		err := generatePredefinedSeries(nMetrics, cardinality, generatorType)
		if err != nil {
			log.Errorf("Failed to pre-generate series: %v", err)
			return
		}
	}

	currentTime := time.Now().UTC()
	endTimestamp := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), currentTime.Hour(), 0, 0, 0, time.UTC) // truncate to the hour

	dataGeneratorConfig, ok := iDataGeneratorConfig.(*utils.GeneratorDataConfig)
	if ok && dataGeneratorConfig != nil {
		dataGeneratorConfig.EndTimestamp = endTimestamp
	}

	var wg sync.WaitGroup
	totalEventsPerProcess := totalEvents / processCount
	eventsPerDayPerProcess := int(eventsPerDay) / processCount

	ticker := time.NewTicker(60 * time.Second)
	done := make(chan bool)
	totalSent := uint64(0)
	totalBytes := uint64(0)

	readers := make([]utils.Generator, processCount)
	for i := 0; i < processCount; i++ {
		reader, err := getReaderFromArgs(iType, nMetrics, generatorType, dataFile, addTs, dataGeneratorConfig, i)
		if err != nil {
			log.Fatalf("StartIngestion: failed to initalize reader! %+v", err)
		}
		readers[i] = reader
	}

	for i := 0; i < processCount; i++ {
		wg.Add(1)
		go runIngestion(iType, readers[i], &wg, url, totalEventsPerProcess, continuous, batchSize, i+1, indexPrefix,
			&totalSent, bearerToken, indexName, numIndices, eventsPerDayPerProcess, &totalBytes, callback)
	}

	go func() {
		wg.Wait()
		done <- true
	}()
	startTime := time.Now()

	lastPrintedCount := uint64(0)
	lastPrintedSize := uint64(0)
readChannel:
	for {
		select {
		case <-done:
			break readChannel
		case <-ticker.C:
			totalTimeTaken := time.Since(startTime).Truncate(time.Second)
			eventsPerSec := int64((totalSent - lastPrintedCount) / 60)
			mbCount := totalBytes / 1_000_000
			mbPerSec := int64((mbCount - lastPrintedSize) / 60)

			log.Infof("Elapsed time: %v Total: %v events, %v MB, %v events/sec, %v MB/s",
				totalTimeTaken, humanize.Comma(int64(totalSent)), humanize.Comma(int64(mbCount)),
				humanize.Comma(eventsPerSec), humanize.Comma(mbPerSec))

			if iType == OpenTSDB || iType == PrometheusRemoteWrite {
				log.Infof("HLL Approx so far of unique timeseries:%+v", humanize.Comma(int64(utils.GetMetricsHLL())))
			}
			lastPrintedCount = totalSent
			lastPrintedSize = mbCount
		}
	}
	mbCount := totalBytes / 1_000_000
	log.Printf("Total ingested: %v events, %v MB. Event type: %s",
		humanize.Comma(int64(totalEvents)),
		humanize.Comma(int64(mbCount)),
		iType.String())
	totalTimeTaken := time.Since(startTime)

	numSeconds := totalTimeTaken.Seconds()
	if numSeconds == 0 {
		log.Printf("Total Time Taken for ingestion %v", totalTimeTaken)
	} else {
		eventsPerSecond := int64(float64(totalEvents) / numSeconds)
		mbPerSec := int64(float64(mbCount) / numSeconds)
		log.Printf("Total ingestion time: %v, %v events/sec, %v MB/s",
			totalTimeTaken.Truncate(time.Second),
			humanize.Comma(eventsPerSecond),
			humanize.Comma(mbPerSec))
		log.Infof("Total HLL Approx of unique timeseries:%+v", humanize.Comma(int64(utils.GetMetricsHLL())))
	}
}
