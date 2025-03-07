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
)

func (q IngestType) String() string {
	switch q {
	case ESBulk:
		return "ES Bulk"
	case OpenTSDB:
		return "OTSDB"
	default:
		return "UNKNOWN"
	}
}


type MetricGeneratorType int

const (
    GenerateBothMetrics MetricGeneratorType = iota
    GenerateKSMOnly
    GenerateNodeExporterOnly
)

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
	default:
		log.Fatalf("unknown ingest type %+v", iType)
		return fmt.Errorf("unknown ingest type %+v", iType)
	}

	req, err := http.NewRequest("POST", requestStr, buf)
	if bearerToken != "" {
		req.Header.Add("Authorization", bearerToken)
	}
	req.Header.Set("Content-Type", "application/json")

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
	if resp.StatusCode != http.StatusOK {
		log.Errorf("sendRequest: client.Do ERROR Response: %v", "StatusCode: "+fmt.Sprint(resp.StatusCode)+": "+string(respBody))
		return fmt.Errorf("sendRequest: client.Do ERROR Response: %v", "StatusCode: "+fmt.Sprint(resp.StatusCode)+": "+string(respBody))
	}
	return nil
}

// Generate the body for the request based on the ingest type.
func generateBody(iType IngestType, recs int, i int, rdr utils.Generator,
	actLines []string, bb *bytebufferpool.ByteBuffer) ([]byte, error) {
	switch iType {
	case ESBulk:
		actionLine := actLines[i%len(actLines)]
		return generateESBody(recs, actionLine, rdr, bb)
	case OpenTSDB:
		return generateOpenTSDBBody(recs, rdr)
	default:
		log.Fatalf("Unsupported ingest type %s", iType.String())
	}
	return nil, fmt.Errorf("unsupported ingest type %s", iType.String())
}

// Generate the body for Elasticsearch Bulk API requests.
func generateESBody(recs int, actionLine string, rdr utils.Generator,
	bb *bytebufferpool.ByteBuffer) ([]byte, error) {
	for i := 0; i < recs; i++ {
		_, _ = bb.WriteString(actionLine)
		logline, err := rdr.GetLogLine()
		if err != nil {
			return nil, err
		}
		_, _ = bb.Write(logline)
		_, _ = bb.WriteString("\n")
	}
	payLoad := bb.Bytes()
	return payLoad, nil
}

// Generate the body for OpenTSDB API requests.
func generateOpenTSDBBody(recs int, rdr utils.Generator) ([]byte, error) {
	finalPayLoad := make([]interface{}, 0, recs*2) // Account for multiple metrics per record

	for i := 0; i < recs; i++ {
		rawMetrics, err := rdr.GetRawLog()
		if err != nil {
			return nil, err
		}

		// Handle multiple metrics from single GetRawLog call
		if metrics, ok := rawMetrics["metrics"]; ok {
			for _, m := range metrics.([]map[string]interface{}) {
				finalPayLoad = append(finalPayLoad, m)
			}
		} else {
			finalPayLoad = append(finalPayLoad, rawMetrics)
		}
	}

	return json.Marshal(finalPayLoad)
}

var preGeneratedSeries []map[string]interface{}
var uniqueSeriesMap = make(map[string]struct{})
var seriesId uint64

// Generate a unique payload for OpenTSDB metrics.
func generateUniquePayload(rdr utils.Generator) (map[string]interface{}, error) {
    if rdr == nil {
        return nil, fmt.Errorf("reader is nil")
    }

    const maxRetries = 1000
    var retryCount int

    for retryCount = 0; retryCount < maxRetries; retryCount++ {
        currPayload, err := rdr.GetRawLog()
        if err != nil {
            log.Errorf("generateUniquePayload: failed to get raw log: %+v", err)
            continue // Retry on errors
        }

        // Ensure tags is a map[string]interface{}
        tags, ok := currPayload["tags"].(map[string]interface{})
        if !ok {
            if stringTags, ok := currPayload["tags"].(map[string]string); ok {
                convertedTags := make(map[string]interface{})
                for k, v := range stringTags {
                    convertedTags[k] = v
                }
                currPayload["tags"] = convertedTags
                tags = convertedTags
            } else {
                log.Warnf("Skipping invalid tags type: %T", currPayload["tags"])
                continue
            }
        }

        // Generate composite ID
        var idBuilder strings.Builder
        idBuilder.WriteString(currPayload["metric"].(string))
        for k, v := range tags {
            idBuilder.WriteString(k)
            idBuilder.WriteString(fmt.Sprintf("%v", v))
        }
        id := idBuilder.String()

        if _, exists := uniqueSeriesMap[id]; !exists {
            uniqueSeriesMap[id] = struct{}{}
            return currPayload, nil
        }
    }

    log.Errorf("Failed to generate unique payload after %d attempts", maxRetries)
    return nil, fmt.Errorf("maximum uniqueness retries exceeded")
}

// Pre-generate unique series for OpenTSDB metrics.
func generatePredefinedSeries(nMetrics int, cardinality uint64, gentype string,   metricType MetricGeneratorType) error {
	preGeneratedSeries = make([]map[string]interface{}, cardinality)

	var rdr utils.Generator
	var err error

	if gentype == "k8s" {
		seed := int64(1001) // Use a fixed seed for reproducibility
		rdr = InitK8sGenerator(seed, metricType)
	} else {
		rdr, err = utils.InitMetricsGenerator(nMetrics, gentype)
		if err != nil {
			return err
		}
	}

	start := time.Now()
	for i := uint64(0); i < cardinality; i++ {
		currPayload, err := generateUniquePayload(rdr)
		if err != nil {
            log.Warnf("Skipping duplicate series after retries (total generated: %d)", i)
            continue
        }
		preGeneratedSeries[i] = currPayload
	}
	elapsed := time.Since(start)
	log.Infof("Generated %d unique series in %v", len(preGeneratedSeries), elapsed)
	return nil
}

// Generate the body from pre-defined series for OpenTSDB.
func generateBodyFromPredefinedSeries(recs int) ([]byte, error) {
    finalPayLoad := make([]interface{}, recs)
    for i := 0; i < recs; i++ {
        // Use modulo to cycle through predefined series
        finalPayLoad[i] = preGeneratedSeries[(atomic.AddUint64(&seriesId, 1)-1)%uint64(len(preGeneratedSeries))]
    }
    return json.Marshal(finalPayLoad)
}

func runIngestion(iType IngestType, rdr utils.Generator, wg *sync.WaitGroup, url string, totalEvents int, continuous bool,
    batchSize, processNo int, indexPrefix string, ctr *uint64, bearerToken string, indexName string, numIndices int,
    eventsPerDayPerProcess int, totalBytes *uint64) {
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
            payload, err = generateBodyFromPredefinedSeries(recsInBatch) // Updated call
            seriesId += uint64(recsInBatch)
        } else {
            payload, err = generateBody(iType, recsInBatch, i, rdr, actLines, bb)
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

// Send performance data if the generator supports it.
func SendPerformanceData(rdr utils.Generator) {
	dynamicUserGen, isDUG := rdr.(*utils.DynamicUserGenerator)
	if !isDUG || dynamicUserGen.DataConfig == nil {
		return
	}
	dynamicUserGen.DataConfig.SendLog()
}

// Populate action lines for Elasticsearch Bulk API.
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

// Get the appropriate reader based on the generator type.
func getReaderFromArgs(iType IngestType, nummetrics int, gentype string, str string, ts bool, generatorDataConfig *utils.GeneratorDataConfig, processIndex int,  metricType MetricGeneratorType) (utils.Generator, error) {
	if iType == OpenTSDB {
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
		seed := int64(1001 + processIndex)
		rdr = InitK8sGenerator(seed, metricType) // Pass flags to K8sGenerator
	default:
		return nil, fmt.Errorf("unsupported reader type %s. Options=[static,dynamic-user,file,benchmark]", gentype)
	}
	if err != nil {
		return rdr, err
	}
	err = rdr.Init(str)
	return rdr, err
}

// Initialize the generator data configuration.
func GetGeneratorDataConfig(maxColumns int, variableColums bool, minColumns int, uniqColumns int) *utils.GeneratorDataConfig {
	return utils.InitGeneratorDataConfig(maxColumns, variableColums, minColumns, uniqColumns)
}

// Start the ingestion process.
func StartIngestion(iType IngestType, generatorType, dataFile string, totalEvents int, continuous bool,
	batchSize int, url string, indexPrefix string, indexName string, numIndices, processCount int, addTs bool,
	nMetrics int, bearerToken string, cardinality uint64, eventsPerDay uint64, iDataGeneratorConfig interface{},
	metricType MetricGeneratorType) {
	log.Printf("Starting ingestion at %+v for %+v", url, iType.String())
	if iType == OpenTSDB {
		err := generatePredefinedSeries(nMetrics, cardinality, generatorType, metricType)
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
		reader, err := getReaderFromArgs(iType, nMetrics, generatorType, dataFile, addTs, dataGeneratorConfig, i, metricType)
		if err != nil {
			log.Fatalf("StartIngestion: failed to initalize reader! %+v", err)
		}
		readers[i] = reader
	}

	for i := 0; i < processCount; i++ {
		wg.Add(1)
		go runIngestion(iType, readers[i], &wg, url, totalEventsPerProcess, continuous, batchSize, i+1, indexPrefix,
			&totalSent, bearerToken, indexName, numIndices, eventsPerDayPerProcess, &totalBytes)
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

			if iType == OpenTSDB {
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
