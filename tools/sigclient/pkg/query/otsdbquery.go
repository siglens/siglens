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

package query

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
	"verifier/pkg/utils"

	"github.com/montanaflynn/stats"
	log "github.com/sirupsen/logrus"
)

const tagFilterSeparator string = ",__,"

type metricsQueryTypes int

// QueryRequest represents the request structure for the API
type QueryRequest struct {
	Start   string `json:"start"`
	End     string `json:"end"`
	Queries []struct {
		Name   string `json:"name"`
		Query  string `json:"query"`
		QlType string `json:"qlType"`
	} `json:"queries"`
	Formulas []struct {
		Formula string `json:"formula"`
	} `json:"formulas"`
}

// QueryResponse represents the response structure from the API
type QueryResponse struct {
	Series      []string    `json:"series"`
	Timestamps  []int64     `json:"timestamps"`
	Values      [][]float64 `json:"values"`
	StartTime   int64       `json:"startTime"`
	IntervalSec int64       `json:"intervalSec"`
}

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

// RunQueryFromFile reads queries from a file and executes them
func RunMetricQueryFromFile(apiURL string, filepath string) {
	// open file
	f, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("RunQueryFromFile: Error in opening file: %v, err: %v", filepath, err)
		return
	}
	defer f.Close()

	// read csv values using csv.Reader
	csvReader := csv.NewReader(f)

	// Skip the first line comment
	_, err = csvReader.Read()
	if err != nil {
		log.Fatalf("Error reading file header: %v, err: %v", filepath, err)
		return
	}
	valuesPerRow := 8

	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("RunQueryFromFile: Error in reading file: %v, err: %v", filepath, err)
			return
		}

		if len(rec) != valuesPerRow {
			log.Fatalf("RunQueryFromFile: Invalid number of columns in query file: [%v]. Expected %v", rec, valuesPerRow)
			return
		}

		query := rec[0]
		start := rec[1]
		end := rec[2]
		expectedValuesStr := rec[3]
		relation := rec[4]
		metricNames := rec[5]
		tagFilters := rec[6]
		skipQuery := rec[7]

		if skipQuery == "true" {
			log.Infof("Skipping query: %v", query)
			continue
		}

		expectedValuesStrs := strings.Split(expectedValuesStr, ",")

		metricNamesMap := make(map[string]struct{})
		if metricNames != "" {
			for _, metricName := range strings.Split(metricNames, ",") {
				metricNamesMap[metricName] = struct{}{}
			}
		}

		tagFiltersMap := make(map[string]struct{})
		if tagFilters != "" {
			for _, tagFilter := range strings.Split(tagFilters, tagFilterSeparator) {
				tagFiltersMap[tagFilter] = struct{}{}
			}
		}

		requestBody := QueryRequest{
			Start: start,
			End:   end,
			Queries: []struct {
				Name   string `json:"name"`
				Query  string `json:"query"`
				QlType string `json:"qlType"`
			}{
				{Name: "a", Query: query, QlType: "promql"},
			},
			Formulas: []struct {
				Formula string `json:"formula"`
			}{
				{Formula: "a"},
			},
		}

		requestData, err := json.Marshal(requestBody)
		if err != nil {
			log.Fatalf("RunQueryFromFile: Error in marshaling request data: %v, Query=%v", err, query)
		}

		resp, err := http.Post(apiURL, "application/json", bytes.NewBuffer(requestData))
		if err != nil {
			log.Fatalf("RunQueryFromFile: Error in making HTTP request for Query: %v. Error=%v", query, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			log.Fatalf("RunQueryFromFile: Non-OK HTTP status: %v, body: %s, Query: %s", resp.StatusCode, string(body), query)
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Fatalf("RunQueryFromFile: Error in reading response body: %v, Query=%v", err, query)
		}

		var queryResponse QueryResponse
		err = json.Unmarshal(body, &queryResponse)
		if err != nil {
			log.Fatalf("RunQueryFromFile: Error in unmarshaling response data: %v, Query=%v", err, query)
		}

		// If the expected string is empty, then the actual retrieved string should also be empty.
		if len(expectedValuesStr) != 0 {
			if len(queryResponse.Values) == 0 || len(queryResponse.Values[0]) != len(expectedValuesStrs) {
				log.Fatalf("RunQueryFromFile: Unexpected number of values in response: %v, ExpectedValues=%v, Query=%v", queryResponse.Values, expectedValuesStrs, query)
			}
		}

		if len(expectedValuesStr) > 0 {
			for i, actualValue := range queryResponse.Values[0] {
				isEqual, err := utils.VerifyInequality(actualValue, relation, expectedValuesStrs[i])
				if !isEqual {
					log.Fatalf("RunQueryFromFile: For Query=%v, Actual value: %v does not meet condition: [%s %v] at index %d", query, actualValue, relation, expectedValuesStrs[i], i)
				} else if err != nil {
					log.Fatalf("RunQueryFromFile: For Query=%v, Error in verifying results: %v", query, err)
				}
			}
		}

		actualMetricNames := make(map[string]struct{})
		if len(metricNamesMap) > 0 {
			for _, seriesId := range queryResponse.Series {
				mName := ExtractMetricNameFromSeriesID(seriesId)
				if _, exists := metricNamesMap[mName]; !exists {
					log.Fatalf("RunQueryFromFile: Query: %v was successful. But the metric name: %v is not expected. Expected Metric Names: %v", query, mName, metricNamesMap)
				}
				actualMetricNames[mName] = struct{}{}
			}

			if len(actualMetricNames) != len(metricNamesMap) {
				log.Fatalf("RunQueryFromFile: Query: %v was successful. But the query response metric names: %v do not match the expected metric names: %v", query, actualMetricNames, metricNamesMap)
			}
		}

		actualTagFilters := make(map[string]struct{})
		if len(tagFiltersMap) > 0 {
			for _, seriesId := range queryResponse.Series {
				tagKeyValue := ExtractTagKeyValuePairsFromSeriesId(seriesId)
				if _, exists := tagFiltersMap[tagKeyValue]; !exists {
					log.Fatalf("RunQueryFromFile: Query: %v was successful. But the tag key-value: %v is not expected. Expected Tag Key-Values: %v", query, tagKeyValue, tagFiltersMap)
				}
				actualTagFilters[tagKeyValue] = struct{}{}
			}

			if len(actualTagFilters) != len(tagFiltersMap) {
				log.Fatalf("RunQueryFromFile: Query: %v was successful. But the query response tag key-values: %v do not match the expected tag key-values: %v", query, actualTagFilters, tagFiltersMap)
			}
		}

		log.Printf("RunQueryFromFile: Query: %v was successful. Response matches expected values.", query)
	}
}

func ExtractMetricNameFromSeriesID(seriesId string) string {
	stringVals := strings.Split(seriesId, "{")
	if len(stringVals) != 2 {
		return seriesId
	} else {
		return stringVals[0]
	}
}

func ExtractTagKeyValuePairsFromSeriesId(seriesId string) string {
	stringVals := strings.Split(seriesId, "{")
	if len(stringVals) != 2 {
		return ""
	} else {
		// remove the trailing "}"
		return stringVals[1][:len(stringVals[1])-1]
	}
}
