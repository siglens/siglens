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

const PRINT_FREQ = 100_000
const RETRY_COUNT = 10

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
	_, err = io.ReadAll(resp.Body)
	if err != nil {
		log.Errorf("sendRequest: client.Do ERROR: %v", err)
		return err
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

func runIngestion(iType IngestType, rdr utils.Generator, wg *sync.WaitGroup, url string, totalEvents int,
	continous bool, batchSize, processNo int, indexPrefix string, ctr *uint64, bearerToken string,
	indexName string, numIndices int) {

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
	for continous || eventCounter < totalEvents {

		recsInBatch := batchSize
		if !continous && eventCounter+batchSize > totalEvents {
			recsInBatch = totalEvents - eventCounter
		}
		i++
		if iType == ESBulk {
			bb = bytebufferpool.Get()
		}
		payload, err := generateBody(iType, recsInBatch, i, rdr, actLines, bb)
		if err != nil {
			log.Errorf("Error generating bulk body!: %v", err)
			if iType == ESBulk {
				bytebufferpool.Put(bb)
			}
			return
		}
		var reqErr error
		for i := 0; i < RETRY_COUNT; i++ {
			reqErr = sendRequest(iType, client, payload, url, bearerToken)
			if reqErr == nil {
				break
			}
			sleepTime := time.Second * time.Duration(5*(i+1))
			log.Errorf("Error sending request. Attempt: %d. Sleeping for %+v before retrying.", i+1, sleepTime.String())
			time.Sleep(sleepTime)
		}

		if iType == ESBulk {
			bytebufferpool.Put(bb)
		}
		if reqErr != nil {
			log.Fatalf("Error sending request after %d attempts! %v", RETRY_COUNT, reqErr)
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

func getReaderFromArgs(iType IngestType, nummetrics int, gentype, str string, ts bool) (utils.Generator, error) {

	if iType == OpenTSDB {
		rdr := utils.InitMetricsGenerator(nummetrics)
		err := rdr.Init(str)
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

func StartIngestion(iType IngestType, generatorType, dataFile string, totalEvents int, continuous bool,
	batchSize int, url string, indexPrefix string, indexName string, numIndices, processCount int, addTs bool, nMetrics int, bearerToken string) {
	log.Printf("Starting ingestion at %+v for %+v", url, iType.String())
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
		go runIngestion(iType, reader, &wg, url, totalEventsPerProcess, continuous, batchSize, i+1, indexPrefix,
			&totalSent, bearerToken, indexName, numIndices)
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
				log.Infof("Approximation of sent number of unique timeseries:%+v", utils.GetMetricsHLL())
			}
			lastPrintedCount = totalSent
		}
	}
	log.Printf("Total events ingested:%+d. Event type: %s", totalEvents, iType.String())
	totalTimeTaken := time.Since(startTime)

	numSeconds := int(totalTimeTaken.Seconds())
	if numSeconds == 0 {
		log.Printf("Total Time Taken for ingestion %+v", totalTimeTaken)
	} else {
		eventsPerSecond := int64(totalEvents / numSeconds)
		log.Printf("Total Time Taken for ingestion %s. Average events per second=%+v", totalTimeTaken, humanize.Comma(eventsPerSecond))
	}
}
