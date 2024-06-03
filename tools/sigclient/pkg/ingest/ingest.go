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
	"math/rand"
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
	// Check if the Status code is not 200
	if resp.StatusCode != http.StatusOK {
		log.Errorf("sendRequest: client.Do ERROR Response: %v", "StatusCode: "+fmt.Sprint(resp.StatusCode)+": "+string(respBody))
		return fmt.Errorf("sendRequest: client.Do ERROR Response: %v", "StatusCode: "+fmt.Sprint(resp.StatusCode)+": "+string(respBody))
	}
	return nil
}

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

func generateUniqueID(series map[string]interface{}) string {
	metricName := series["metric"].(string)
	tags := series["tags"].(map[string]interface{})
	var builder strings.Builder
	builder.WriteString(metricName)
	for key, value := range tags {
		builder.WriteString(key)
		builder.WriteString(fmt.Sprintf("%v", value))
	}
	return builder.String()
}

func generatePredefinedSeries(nMetrics int, cardinality int, gentype string) error {
	preGeneratedSeries = make([]map[string]interface{}, 0, cardinality)
	rdr, err := utils.InitMetricsGenerator(nMetrics, gentype)
	if err != nil {
		return err
	}
	start := time.Now()
	uniqueSeriesMap := make(map[string]struct{})
	for len(preGeneratedSeries) < cardinality {
		currPayload, err := rdr.GetRawLog()
		if err != nil {
			return err
		}
		uniqueID := generateUniqueID(currPayload)
		if _, exists := uniqueSeriesMap[uniqueID]; !exists {
			preGeneratedSeries = append(preGeneratedSeries, currPayload)
			uniqueSeriesMap[uniqueID] = struct{}{}
		}
	}
	elapsed := time.Since(start)
	log.Infof("Generated %d unique series in %v", len(uniqueSeriesMap), elapsed)
	return nil
}

func generateBodyFromPredefinedSeries(recs int) ([]byte, error) {
	finalPayLoad := make([]interface{}, recs)
	for i := 0; i < recs; i++ {
		series := preGeneratedSeries[i%len(preGeneratedSeries)]
		series["timestamp"] = time.Now().Unix()
		series["value"] = rand.Float64()*10000 - 5000
		finalPayLoad[i] = series
	}
	retVal, err := json.Marshal(finalPayLoad)
	if err != nil {
		return nil, err
	}
	return retVal, nil
}

func runIngestion(iType IngestType, rdr utils.Generator, wg *sync.WaitGroup, url string, totalEvents int, continuous bool, batchSize, processNo int, indexPrefix string, ctr *uint64, bearerToken string, indexName string, numIndices int) {
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
			payload, err = generateBodyFromPredefinedSeries(recsInBatch)
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
			log.Errorf("Error sending request. Attempt: %d. Sleeping for %+v before retrying.", retryCounter, sleepTime.String())
			retryCounter++
			time.Sleep(sleepTime)
		}

		if iType == ESBulk {
			bytebufferpool.Put(bb)
		}
		if reqErr != nil {
			log.Fatalf("Error sending request after %v hr ! %v", int(maxDuration.Hours()), reqErr)
			return
		}
		eventCounter += recsInBatch
		atomic.AddUint64(ctr, uint64(recsInBatch))
	}
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

func getReaderFromArgs(iType IngestType, nummetrics int, gentype string, str string, ts bool) (utils.Generator, error) {

	if iType == OpenTSDB {
		rdr, err := utils.InitMetricsGenerator(nummetrics, gentype)
		if err != nil {
			return rdr, err
		}
		err = rdr.Init(str)
		return rdr, err
	}
	var rdr utils.Generator
	switch gentype {
	case "", "static":
		log.Infof("Initializing static reader")
		rdr = utils.InitStaticGenerator(ts)
	case "dynamic-user":
		seed := int64(fastrand.Uint32n(1_000))
		rdr = utils.InitDynamicUserGenerator(ts, seed)
	case "file":
		log.Infof("Initializing file reader from %s", str)
		rdr = utils.InitFileReader()
	case "benchmark":
		log.Infof("Initializing benchmark reader")
		seed := int64(1001)
		rdr = utils.InitDynamicUserGenerator(ts, seed)
	case "k8s":
		log.Infof("Initializing k8s reader")
		seed := int64(1001)
		rdr = utils.InitK8sGenerator(ts, seed)
	default:
		return nil, fmt.Errorf("unsupported reader type %s. Options=[static,dynamic-user,file,benchmark]", gentype)
	}
	err := rdr.Init(str)
	return rdr, err
}

func StartIngestion(iType IngestType, generatorType, dataFile string, totalEvents int, continuous bool, batchSize int, url string, indexPrefix string, indexName string, numIndices, processCount int, addTs bool, nMetrics int, bearerToken string, cardinality int) {
	log.Printf("Starting ingestion at %+v for %+v", url, iType.String())
	if iType == OpenTSDB {
		err := generatePredefinedSeries(nMetrics, cardinality, generatorType)
		if err != nil {
			log.Errorf("Failed to pre-generate series: %v", err)
			return
		}
	}
	var wg sync.WaitGroup
	totalEventsPerProcess := totalEvents / processCount

	ticker := time.NewTicker(60 * time.Second)
	done := make(chan bool)
	totalSent := uint64(0)
	for i := 0; i < processCount; i++ {
		wg.Add(1)
		reader, err := getReaderFromArgs(iType, nMetrics, generatorType, dataFile, addTs)
		if err != nil {
			log.Fatalf("StartIngestion: failed to initalize reader! %+v", err)
		}
		if iType == OpenTSDB {
			go runIngestion(iType, reader, &wg, url, totalEventsPerProcess, continuous, batchSize, i+1, indexPrefix, &totalSent, bearerToken, indexName, numIndices)
		} else {
			go runIngestion(iType, reader, &wg, url, totalEventsPerProcess, continuous, batchSize, i+1, indexPrefix,
				&totalSent, bearerToken, indexName, numIndices)
		}
	}

	go func() {
		wg.Wait()
		done <- true
	}()
	startTime := time.Now()

	lastPrintedCount := uint64(0)
readChannel:
	for {
		select {
		case <-done:
			break readChannel
		case <-ticker.C:
			totalTimeTaken := time.Since(startTime)
			eventsPerSec := int64((totalSent - lastPrintedCount) / 60)
			log.Infof("Total elapsed time:%s. Total sent events %+v. Events per second:%+v", totalTimeTaken, humanize.Comma(int64(totalSent)), humanize.Comma(eventsPerSec))
			if iType == OpenTSDB {
				log.Infof("HLL Approx so far of unique timeseries:%+v", humanize.Comma(int64(utils.GetMetricsHLL())))
			}
			lastPrintedCount = totalSent
		}
	}
	log.Printf("Total events ingested:%+d. Event type: %s", totalEvents, iType.String())
	totalTimeTaken := time.Since(startTime)

	numSeconds := totalTimeTaken.Seconds()
	if numSeconds == 0 {
		log.Printf("Total Time Taken for ingestion %+v", totalTimeTaken)
	} else {
		eventsPerSecond := int64(float64(totalEvents) / numSeconds)
		log.Printf("Total Time Taken for ingestion %s. Average events per second=%+v", totalTimeTaken, humanize.Comma(eventsPerSecond))
		log.Infof("Total HLL Approx of unique timeseries:%+v", humanize.Comma(int64(utils.GetMetricsHLL())))
	}
}
