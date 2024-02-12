package query

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/montanaflynn/stats"
	log "github.com/sirupsen/logrus"
)

type metricsQueryTypes int

const (
	simpleKeyValueQuery metricsQueryTypes = iota
	wildcardKey
)

var aggFns = [...]string{"avg", "min", "max", "sum"}

func (m metricsQueryTypes) String() string {
	switch m {
	case simpleKeyValueQuery:
		return "simple key=value"
	case wildcardKey:
		return "simple key=*"
	default:
		return "UNKNOWN"
	}
}

func getSimpleMetricsQuery(url *url.URL) string {
	values := url.Query()
	values.Set("start", "1d-ago")
	aggFn := aggFns[rand.Intn(len(aggFns))]
	values.Set("m", fmt.Sprintf("%s:3h-%s:testmetric0{color=\"yellow\"}", aggFn, aggFn))
	url.RawQuery = values.Encode()
	str := url.String()
	log.Errorf("final url is %+v", str)
	return str
}

func getWildcardMetricsQuery(url *url.URL) string {
	values := url.Query()
	values.Set("start", "1d-ago")
	aggFn := aggFns[rand.Intn(len(aggFns))]
	values.Set("m", fmt.Sprintf("%s:3h-%s:testmetric0{color=*}", aggFn, aggFn))
	url.RawQuery = values.Encode()
	str := url.String()
	log.Errorf("final url is %+v", str)
	return str
}

// Returns elapsed time. If verbose, logs the number of returned series
func sendSingleOTSDBRequest(client *http.Client, mqType metricsQueryTypes, url string, verbose bool) (float64, int) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Fatalf("sendRequest: http.NewRequest ERROR: %v", err)
	}

	stime := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("sendRequest: client.Do ERROR: %v", err)
	}
	defer resp.Body.Close()
	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("sendRequest: client.Do ERROR: %v", err)
	}
	m := make([]interface{}, 0)
	err = json.Unmarshal(rawBody, &m)
	if err != nil {
		log.Fatalf("sendRequest: response unmarshal ERROR: %v", err)
	}
	log.Infof("returned response: %v in %+v. Num series=%+v", mqType, time.Since(stime), len(m))
	return float64(time.Since(stime).Milliseconds()), len(m)
}

// returns a map of qtype to list of result query times and a map of qType to the raw url to send requests to
func initMetricsResultMap(numIterations int, reqStr string) (map[metricsQueryTypes][]float64, map[metricsQueryTypes]string) {
	results := make(map[metricsQueryTypes][]float64)
	rawUrl := make(map[metricsQueryTypes]string)

	baseUrl, err := url.Parse(reqStr)
	if err != nil {
		log.Fatalf("Failed to parse url! Error %+v", err)
	}
	rawSimpleURL := getSimpleMetricsQuery(baseUrl)
	rawUrl[simpleKeyValueQuery] = rawSimpleURL
	results[simpleKeyValueQuery] = make([]float64, numIterations)

	baseUrl, err = url.Parse(reqStr)
	if err != nil {
		log.Fatalf("Failed to parse url! Error %+v", err)
	}
	rawWildcardURL := getWildcardMetricsQuery(baseUrl)
	rawUrl[wildcardKey] = rawWildcardURL
	results[wildcardKey] = make([]float64, numIterations)
	return results, rawUrl
}

func StartMetricsQuery(dest string, numIterations int, continuous, verbose, validateMetricsOutput bool) map[string]bool {
	rand.Seed(time.Now().UnixNano())
	client := http.DefaultClient
	if numIterations == 0 && !continuous {
		log.Fatalf("Iterations must be greater than 0")
	}
	validResult := make(map[string]bool)
	requestStr := fmt.Sprintf("%s/api/query", dest)
	results, queries := initMetricsResultMap(numIterations, requestStr)
	for i := 0; i < numIterations || continuous; i++ {
		for qType, query := range queries {
			time, numTS := sendSingleOTSDBRequest(client, qType, query, verbose)
			if !continuous {
				results[qType][i] = time
			}
			if validateMetricsOutput && numTS == 0 {
				validResult[qType.String()] = false
			}
		}
	}

	log.Infof("-----Query Summary. Completed %d iterations----", numIterations)
	for qType, qRes := range results {
		p95, _ := stats.Percentile(qRes, 95)
		avg, _ := stats.Mean(qRes)
		max, _ := stats.Max(qRes)
		min, _ := stats.Min(qRes)
		log.Infof("QueryType: %s. Min:%+vms, Max:%+vms, Avg:%+vms, P95:%+vms", qType.String(), min, max, avg, p95)
	}
	return validResult
}
