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
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	log "github.com/sirupsen/logrus"
)

type QueryAndRespTimes struct {
	query    string
	respTime float64
}

func getQueryResponseTime(queryReq map[string]interface{}, url string) (float64, error) {

	reqBody, err := json.Marshal(queryReq)
	if err != nil {
		return 0, fmt.Errorf("EvaluateQueryForAPI: Error marshaling request, reqBody: %v, err: %v", reqBody, err)
	}

	startTime := time.Now()
	req, err := http.NewRequestWithContext(context.TODO(), http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return 0, fmt.Errorf("EvaluateQueryForAPI: Error creating request, url: %v, reqBody: %v, err: %v", url, reqBody, err)
	}
	req.Header.Set("content-type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("EvaluateQueryForAPI: Error sending request, req: %v, err: %v", req, err)
	}
	defer resp.Body.Close()

	respTime := time.Since(startTime).Milliseconds()

	responseData := make(map[string]interface{})
	if bodyBytes, err := io.ReadAll(resp.Body); err != nil {
		return 0, fmt.Errorf("EvaluateQueryForAPI: Error reading response, resp.Body: %v, err: %v", resp.Body, err)
	} else {
		if err := json.Unmarshal(bodyBytes, &responseData); err != nil {
			return 0, fmt.Errorf("EvaluateQueryForAPI: Error unmarshaling bodyBytes: %v, err: %v", string(bodyBytes), err)
		}
	}

	return float64(respTime), nil
}

func GetClickBenchQueriesAndRespTimes() ([]QueryAndRespTimes, error) {
	path := "../clickbench/cbqueries.csv"
	file, err := os.Open(filepath.Join(path))
	if err != nil {
		return nil, fmt.Errorf("GetClickBenchQueriesAndRespTimes: Error opening file: %v, err: %v", path, err)
	}
	defer file.Close()

	csvReader := csv.NewReader(file)
	csvReader.FieldsPerRecord = -1 // Disable record length test
	queriesAndRespTimes := []QueryAndRespTimes{}
	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("GetClickBenchQueriesAndRespTimes: Error in reading file: %v, err: %v", path, err)
		}

		if rec[0] == "null" {
			queriesAndRespTimes = append(queriesAndRespTimes, QueryAndRespTimes{query: "null", respTime: 0})
			continue
		}

		if len(rec) != 2 {
			return nil, fmt.Errorf("GetClickBenchQueriesAndRespTimes: Invalid number of columns in query file: [%v]. Expected 2", rec)
		}

		query := rec[0]
		respTime, err := strconv.ParseFloat(rec[1], 64)
		if err != nil {
			return nil, fmt.Errorf("GetClickBenchQueriesAndRespTimes: Error parsing response time: %v, err: %v", rec[1], err)
		}
		queriesAndRespTimes = append(queriesAndRespTimes, QueryAndRespTimes{query: query, respTime: respTime})
	}

	return queriesAndRespTimes, nil
}

func ValidateClickBenchQueries(dest string, queriesAndRespTimes []QueryAndRespTimes, thresholdFactor float64) {

	startEpoch := "now-90d"
	endEpoch := "now"
	queryLanguage := "Splunk QL"

	url := fmt.Sprintf("http://%s/api/search", dest)

	queryReq := map[string]interface{}{
		"state":         "query",
		"startEpoch":    startEpoch,
		"endEpoch":      endEpoch,
		"indexName":     "*",
		"queryLanguage": queryLanguage,
	}

	respTimesInMs := make([]float64, len(queriesAndRespTimes))

	for idx, queryAndRespTime := range queriesAndRespTimes {
		query := queryAndRespTime.query
		queryReq["searchText"] = query
		if query == "null" {
			continue
		}
		log.Infof("Validating Q%v query: %v", idx+1, query)
		respTimeInMs, err := getQueryResponseTime(queryReq, url)
		if err != nil {
			log.Errorf("ValidateClickBenchQueries: Error getting response time for Q%v query: %v, err: %v", idx+1, query, err)
			respTimesInMs[idx] = -1 // To indicate query failures
			continue
		}
		respTimesInMs[idx] = respTimeInMs
	}

	failed := false
	log.Infof("---------------------- SUMMARY ----------------------")
	for idx := range respTimesInMs {
		if queriesAndRespTimes[idx].query == "null" {
			log.Infof("Q%v is not supported", idx+1)
		} else if respTimesInMs[idx] == -1 {
			log.Errorf("Q%v failed", idx+1)
			failed = true
		} else {
			log.Infof("Q%v respTimeInMs: %v", idx+1, respTimesInMs[idx])
		}
	}

	if failed {
		log.Fatalf("ValidateClickBenchQueries: Some queries failed")
	}
}
