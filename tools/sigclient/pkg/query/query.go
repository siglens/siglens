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
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"
	"verifier/pkg/utils"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/fasthttp/websocket"
	"github.com/montanaflynn/stats"

	log "github.com/sirupsen/logrus"
)

type logsQueryTypes int

const (
	matchAll logsQueryTypes = iota
	matchMultiple
	matchRange
	needleInHaystack
	keyValueQuery
	freeText
	random
)

func MigrateLookups(lookupFiles []string) error {

	destDir := filepath.Join("../../data/lookups")
	err := os.MkdirAll(destDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("MigrateLookups: Error creating destination directory: %v", err)
	}

	for _, lookupFile := range lookupFiles {
		lookupSrcFile, err := os.Open(lookupFile)
		if err != nil {
			return fmt.Errorf("MigrateLookups: Error opening lookup file %v, err: %v", lookupFile, err)
		}
		defer lookupSrcFile.Close()

		// Create the destination file
		lookupDestPath := filepath.Join(destDir, filepath.Base(lookupFile))
		lookupDestFile, err := os.Create(lookupDestPath)
		if err != nil {
			return fmt.Errorf("MigrateLookups: Error creating lookup file %v, err: %v", lookupDestPath, err)
		}
		defer lookupDestFile.Close()

		// Copy the contents
		_, err = io.Copy(lookupDestFile, lookupSrcFile)
		if err != nil {
			return fmt.Errorf("MigrateLookups: Error copying file %v, err: %v", lookupFile, err)
		}

		// Reset file position
		_, err = lookupSrcFile.Seek(0, 0)
		if err != nil {
			return fmt.Errorf("MigrateLookups: Error resetting file position %v, err: %v", lookupFile, err)
		}

		// Create the destination gzip file
		compressedLookupDestFile, err := os.Create(lookupDestPath + ".gz")
		if err != nil {
			return fmt.Errorf("MigrateLookups: Error creating compressed lookup file %v, err: %v", lookupDestPath, err)
		}
		defer compressedLookupDestFile.Close()

		// Create a gzip writer
		gzipWriter := gzip.NewWriter(compressedLookupDestFile)

		// Copy the contents from source to gzip writer
		_, err = io.Copy(gzipWriter, lookupSrcFile)
		if err != nil {
			return fmt.Errorf("MigrateLookups: Error compressing file %v, err: %v", lookupDestPath, err)
		}

		// Close the gzip writer
		err = gzipWriter.Close()
		if err != nil {
			return fmt.Errorf("MigrateLookups: Error closing gzip writer %v err: %v", lookupDestPath, err)
		}
	}

	return nil
}

func (q logsQueryTypes) String() string {
	switch q {
	case matchAll:
		return "match all"
	case matchMultiple:
		return "match multiple"
	case matchRange:
		return "match range"
	case needleInHaystack:
		return "needle in haystack"
	case keyValueQuery:
		return "single key=value"
	case freeText:
		return "free text"
	case random:
		return "random"
	default:
		return "UNKNOWN"
	}
}

func validateAndGetElapsedTime(qType logsQueryTypes, esOutput map[string]interface{}, verbose bool) float64 {

	etime, ok := esOutput["took"]
	if !ok {
		log.Fatalf("required key 'took' missing in response %+v", esOutput)
	}
	if verbose {
		hits := esOutput["hits"]
		switch rawHits := hits.(type) {
		case map[string]interface{}:
			total := rawHits["total"]
			switch rawTotal := total.(type) {
			case map[string]interface{}:
				value := rawTotal["value"]
				relation := rawTotal["relation"]
				log.Infof("%s query: [%+v]ms. Hits: %+v %+v", qType.String(), etime, relation, value)
			case string:
				log.Infof("%s query: [%+v]ms. Hits: %+v", qType.String(), etime, rawTotal)
			default:
				log.Fatalf("hits.total is not a map or string %+v", rawTotal)
			}
		default:
			log.Fatalf("hits is not a map[string]interface %+v", rawHits)
		}
	}
	return etime.(float64)
}

func getMatchAllQuery() []byte {
	time := time.Now().UnixMilli()
	time1hr := time - (1 * 60 * 60 * 1000)
	var matchAllQuery = map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"match_all": map[string]interface{}{},
					},
				},
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte":    time1hr,
								"lte":    time,
								"format": "epoch_millis",
							},
						},
					},
				},
			},
		},
	}
	raw, err := json.Marshal(matchAllQuery)
	if err != nil {
		log.Fatalf("error marshalling query: %+v", err)
	}
	return raw
}

// job_title=<<random_title>> AND user_color=<<random_color>> AND j != "group 0"
func getMatchMultipleQuery() []byte {
	time := time.Now().UnixMilli()
	time2hr := time - (2 * 24 * 60 * 60 * 1000)
	var matchAllQuery = map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"match": map[string]interface{}{
							"job_title": "Engineer",
						},
					},
					map[string]interface{}{
						"match": map[string]interface{}{
							"job_description": "Senior",
						},
					},
				},
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte":    time2hr,
								"lte":    time,
								"format": "epoch_millis",
							},
						},
					},
					map[string]interface{}{
						"match": map[string]interface{}{
							"group": "group 0",
						},
					},
				},
			},
		},
	}
	raw, err := json.Marshal(matchAllQuery)
	if err != nil {
		log.Fatalf("error marshalling query: %+v", err)
	}
	return raw
}

// 10 <= latency <= 30
func getRangeQuery() []byte {
	time := time.Now().UnixMilli()
	time1d := time - (1 * 24 * 60 * 60 * 1000)
	var matchAllQuery = map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"latency": map[string]interface{}{
								"gte": 10,
								"lte": 8925969,
							},
						},
					},
				},
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte":    time1d,
								"lte":    time,
								"format": "epoch_millis",
							},
						},
					},
				},
			},
		},
	}
	raw, err := json.Marshal(matchAllQuery)
	if err != nil {
		log.Fatalf("error marshalling query: %+v", err)
	}
	return raw
}

// matches a different uuid each query. This will likely have 0 hits
func getNeedleInHaystackQuery() []byte {
	time := time.Now().UnixMilli()
	time90d := time - (90 * 24 * 60 * 60 * 1000)

	var matchAllQuery = map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"query_string": map[string]interface{}{
							"query": fmt.Sprintf("ident:%s", "ffa4c7d4-5f21-457b-89ea-b5ad29968510"),
						},
					},
				},
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte":    time90d,
								"lte":    time,
								"format": "epoch_millis",
							},
						},
					},
				},
			},
		},
	}
	raw, err := json.Marshal(matchAllQuery)
	if err != nil {
		log.Fatalf("error marshalling query: %+v", err)
	}
	return raw
}

// matches a simple key=value using query_string
func getSimpleFilter() []byte {
	time := time.Now().UnixMilli()
	time6hr := time - (6 * 24 * 60 * 60 * 1000)

	var matchAllQuery = map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"query_string": map[string]interface{}{
							"query": "state:California",
						},
					},
				},
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte":    time6hr,
								"lte":    time,
								"format": "epoch_millis",
							},
						},
					},
				},
			},
		},
	}
	raw, err := json.Marshal(matchAllQuery)
	if err != nil {
		log.Fatalf("error marshalling query: %+v", err)
	}
	return raw
}

// free text search query for a job title
func getFreeTextSearch() []byte {
	time := time.Now().UnixMilli()
	time1hr := time - (1 * 60 * 60 * 1000)
	var matchAllQuery = map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []interface{}{
					map[string]interface{}{
						"query_string": map[string]interface{}{
							"query": "Representative",
						},
					},
				},
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte":    time1hr,
								"lte":    time,
								"format": "epoch_millis",
							},
						},
					},
				},
			},
		},
	}
	raw, err := json.Marshal(matchAllQuery)
	if err != nil {
		log.Fatalf("error marshalling query: %+v", err)
	}
	return raw
}

// This generates queries based on columns/values that are setup when ingesting
// data dynamically, so running this function may not be useful when data was
// ingested differently.
// The resulting query has one or more key=value conditions for string fields.
func getRandomQuery() []byte {
	faker := gofakeit.NewUnlocked(time.Now().UnixNano())
	time := time.Now().UnixMilli()
	time1day := time - (1 * 24 * 60 * 60 * 1000)

	must := make([]interface{}, 0)
	should := make([]interface{}, 0)
	used_string_column := [8]bool{}

	numConditions := faker.Number(1, 5)
	for i := 0; i < numConditions; i++ {
		// Create the condition. See randomizeBody() in reader.go for how
		// column values are generated.
		var condition map[string]interface{}
		var column string
		var value string

		// Choose a column we haven't used in this query yet.
		colInd := faker.Number(0, 7)
		for used_string_column[colInd] {
			colInd = faker.Number(0, 7)
		}
		used_string_column[colInd] = true

		switch colInd {
		case 0:
			column = "batch"
			value = "batch-" + fmt.Sprintf("%v", faker.Number(1, 1000))
		case 1:
			column = "city"
			value = faker.Person().Address.City
		case 2:
			column = "country"
			value = faker.Person().Address.Country
		case 3:
			column = "gender"
			value = faker.Person().Gender
		case 4:
			column = "http_method"
			value = faker.HTTPMethod()
		case 5:
			column = "state"
			value = faker.Person().Address.State
		case 6:
			column = "user_color"
			value = faker.Color()
		case 7:
			column = "weekday"
			value = faker.WeekDay()
		}

		// Set the condition.
		condition = map[string]interface{}{
			"match": map[string]interface{}{
				column: value,
			},
		}

		// Decide which list to add this condition to.
		switch faker.Number(0, 1) {
		case 0:
			must = append(must, condition)
		case 1:
			should = append(should, condition)
		}
	}

	var matchAllQuery = map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must":   must,
				"should": should,
				"filter": []interface{}{
					map[string]interface{}{
						"range": map[string]interface{}{
							"timestamp": map[string]interface{}{
								"gte":    time1day,
								"lte":    time,
								"format": "epoch_millis",
							},
						},
					},
				},
			},
		},
	}

	raw, err := json.Marshal(matchAllQuery)
	if err != nil {
		log.Fatalf("error marshalling query: %+v", err)
	}
	return raw
}

func sendSplunkQuery(url string, query string, startEpoch uint64, endEpoch uint64) ([]byte, error) {
	// Create the request body
	requestBody := map[string]interface{}{
		"searchText":    query,
		"indexName":     "*",
		"startEpoch":    startEpoch,
		"endEpoch":      endEpoch,
		"queryLanguage": "Splunk QL",
	}

	// Convert request body to JSON
	bodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %v", err)
	}

	// Create HTTP request
	req, err := http.NewRequest("POST", url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %v", err)
	}

	// Set request headers
	req.Header.Set("Content-Type", "application/json")

	// Send the request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send HTTP request to %v; err=%v", url, err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP request failed with status code: %d", resp.StatusCode)
	}

	// Read response body
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	return respBody, nil
}

func sendSingleRequest(qType logsQueryTypes, client *http.Client, body []byte, url string, verbose bool, authToken string) float64 {
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		log.Fatalf("sendRequest: http.NewRequest ERROR: %v", err)
	}
	log.Printf("sendRequest: sending request to %s", url)
	if authToken != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", authToken))
	}
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		log.Fatalf("sendRequest: http.NewRequest ERROR: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("sendRequest: client.Do ERROR: %v", err)
	}
	defer resp.Body.Close()
	rawBody, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("sendRequest: client.Do ERROR: %v", err)
	}
	m := make(map[string]interface{})
	err = json.Unmarshal(rawBody, &m)
	if err != nil {
		log.Fatalf("sendRequest: response unmarshal ERROR: %v", err)
	}
	return validateAndGetElapsedTime(qType, m, verbose)
}

func initResultMap(numIterations int) map[logsQueryTypes][]float64 {
	results := make(map[logsQueryTypes][]float64)
	results[matchAll] = make([]float64, numIterations)
	results[matchMultiple] = make([]float64, numIterations)
	results[matchRange] = make([]float64, numIterations)
	results[needleInHaystack] = make([]float64, numIterations)
	results[keyValueQuery] = make([]float64, numIterations)
	results[freeText] = make([]float64, numIterations)
	results[random] = make([]float64, numIterations)
	return results
}

func logQuerySummary(numIterations int, res map[logsQueryTypes][]float64) {
	log.Infof("-----Query Summary. Completed %d iterations----", numIterations)
	for qType, qRes := range res {
		p95, _ := stats.Percentile(qRes, 95)
		avg, _ := stats.Mean(qRes)
		max, _ := stats.Max(qRes)
		min, _ := stats.Min(qRes)
		log.Infof("QueryType: %s. Min:%+vms, Max:%+vms, Avg:%+vms, P95:%+vms", qType.String(), min, max, avg, p95)
	}
}

func StartQuery(dest string, numIterations int, prefix string, continuous bool, verbose bool, randomQueries bool, bearerToken string) {
	client := http.DefaultClient
	if numIterations == 0 && !continuous {
		log.Fatalf("Iterations must be greater than 0")
	}

	requestStr := fmt.Sprintf("%s/%s*/_search", dest, prefix)

	log.Infof("Using destination URL %+s", requestStr)
	if continuous {
		runContinuousQueries(client, requestStr, bearerToken)
	}

	results := initResultMap(numIterations)
	for i := 0; i < numIterations; i++ {
		if randomQueries {
			rQuery := getRandomQuery()
			time := sendSingleRequest(random, client, rQuery, requestStr, verbose, bearerToken)
			results[random][i] = time
		} else {
			rawMatchAll := getMatchAllQuery()
			time := sendSingleRequest(matchAll, client, rawMatchAll, requestStr, verbose, bearerToken)
			results[matchAll][i] = time

			rawMultiple := getMatchMultipleQuery()
			time = sendSingleRequest(matchMultiple, client, rawMultiple, requestStr, verbose, bearerToken)
			results[matchMultiple][i] = time

			rawRange := getRangeQuery()
			time = sendSingleRequest(matchRange, client, rawRange, requestStr, verbose, bearerToken)
			results[matchRange][i] = time

			rawNeeldQuery := getNeedleInHaystackQuery()
			time = sendSingleRequest(needleInHaystack, client, rawNeeldQuery, requestStr, verbose, bearerToken)
			results[needleInHaystack][i] = time

			sQuery := getSimpleFilter()
			time = sendSingleRequest(keyValueQuery, client, sQuery, requestStr, verbose, bearerToken)
			results[keyValueQuery][i] = time

			fQuery := getFreeTextSearch()
			time = sendSingleRequest(freeText, client, fQuery, requestStr, verbose, bearerToken)
			results[freeText][i] = time
		}
	}

	logQuerySummary(numIterations, results)
}

// this will never save time statistics per query and will always log results
func runContinuousQueries(client *http.Client, requestStr string, bearerToken string) {
	for {
		rawMatchAll := getMatchAllQuery()
		_ = sendSingleRequest(matchAll, client, rawMatchAll, requestStr, true, bearerToken)

		rawMultiple := getMatchMultipleQuery()
		_ = sendSingleRequest(matchMultiple, client, rawMultiple, requestStr, true, bearerToken)

		rawRange := getRangeQuery()
		_ = sendSingleRequest(matchRange, client, rawRange, requestStr, true, bearerToken)

		sQuery := getSimpleFilter()
		_ = sendSingleRequest(keyValueQuery, client, sQuery, requestStr, true, bearerToken)

		fQuery := getFreeTextSearch()
		_ = sendSingleRequest(freeText, client, fQuery, requestStr, true, bearerToken)
	}
}

func verifyResults(value interface{}, relation, expectedValue string, query string) bool {
	var ok bool
	var err error
	var floatVal float64

	switch value := value.(type) {
	case float64:
		ok, err = utils.VerifyInequality(value, relation, expectedValue)
	case string:
		floatVal, err = strconv.ParseFloat(value, 64)
		if err == nil {
			ok, err = utils.VerifyInequality(floatVal, relation, expectedValue)
		} else {
			ok, err = utils.VerifyInequalityForStr(value, relation, expectedValue)
		}
	case []interface{}:
		valueStrings := make([]string, 0, len(value))
		for _, item := range value {
			valueStrings = append(valueStrings, fmt.Sprintf("%v", item))
		}
		strValue := strings.Join(valueStrings, ",")

		ok, err = utils.VerifyInequalityForStr(strValue, relation, expectedValue)
	default:
		err = fmt.Errorf("unexpected type: %T for value: %v", value, value)
	}

	if err != nil {
		log.Fatalf("RunQueryFromFile: Error in verifying aggregation/record: %v for query %v", err, query)
		return false
	} else if !ok {
		log.Fatalf("RunQueryFromFile: Actual aggregate/record value: %v is not [%s %v] for query %v", value, expectedValue, relation, query)
		return false
	}

	return true
}

// Run queries from a csv file. Expects search text, queryStartTime, queryEndTime, indexName,
// evaluationType, relation, count, and queryLanguage in each row
// relation is one of "eq", "gt", "lt"
// if relation is "", count is ignored and no response validation is done
// The evaluationType should either be "total" to count the number of returned rows, or a string like
// "group:min(latency):New York City" for testing aggregates called in the query; the string should
// start with "group" followed by a colon and the aggregate you want to test, followed by a colon
// and a colon separated list of keys for the groupby call, or * if aggregates were called without
// a groupby statement.
func RunQueryFromFile(dest string, numIterations int, prefix string, continuous, verbose bool, filepath string, bearerToken string) {
	// open file
	f, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("RunQueryFromFile: Error in opening file: %v, err: %v", filepath, err)
		return
	}

	defer f.Close()

	index := 1

	// read csv values using csv.Reader
	csvReader := csv.NewReader(f)
	for {
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("RunQueryFromFile: Error in reading file: %v, err: %v", filepath, err)
			return
		}

		if len(rec) != 8 {
			log.Fatalf("RunQueryFromFile: Invalid number of columns in query file: [%v]. Expected 8", rec)
			return
		}
		data := map[string]interface{}{
			"state":         "query",
			"searchText":    rec[0],
			"startEpoch":    rec[1],
			"endEpoch":      rec[2],
			"indexName":     rec[3],
			"queryLanguage": rec[7],
		}
		evaluationType := rec[4]
		relation := rec[5]
		expectedValue := rec[6]

		if skipIndexes[index] {
			log.Infof("RunQueryFromFile: Skipping index=%v, query: %v", index, rec[0])
			index++
			continue
		}

		log.Infof("RunQueryFromFile: index=%v Running query: %v", index, rec[0])

		// create websocket connection
		conn, _, err := websocket.DefaultDialer.Dial("ws://localhost:5122/api/search/ws", nil)
		if err != nil {
			log.Fatalf("RunQueryFromFile: Error connecting to WebSocket server: %v", err)
			return
		}
		defer conn.Close()

		err = conn.WriteJSON(data)
		if err != nil {
			log.Fatalf("Received err message from server: %+v\n", err)
			break
		}

		readEvent := make(map[string]interface{})
		sTime := time.Now()
		for {
			err = conn.ReadJSON(&readEvent)
			if err != nil {
				log.Infof("Received error from server: %+v\n", err)
				break
			}
			switch readEvent["state"] {
			case "RUNNING", "QUERY_UPDATE":
			case "COMPLETE":
				for eKey, eValue := range readEvent {
					if evaluationType == "total" && eKey == "totalMatched" {
						var hits bool
						var finalHits float64
						var err error
						switch eValue := eValue.(type) {
						case float64:
							finalHits = eValue
							hits, err = utils.VerifyInequality(finalHits, relation, expectedValue)
						case map[string]interface{}:
							for k, v := range eValue {
								if k == "value" {
									var ok bool
									finalHits, ok = v.(float64)
									if !ok {
										log.Fatalf("RunQueryFromFile: Returned total matched is not a float: %v", v)
									}
									hits, err = utils.VerifyInequality(finalHits, relation, expectedValue)

								}
							}
						}
						if err != nil {
							log.Fatalf("RunQueryFromFile: Error in verifying hits: %v", err)
						} else if !hits {
							log.Fatalf("RunQueryFromFile: Actual Hits: %v is not [%s %v] for query:%v", finalHits, rec[6], rec[5], rec[0])
						} else {
							log.Infof("RunQueryFromFile: Query %v was succesful. In %+v", rec[0], time.Since(sTime))
						}
					} else if strings.HasPrefix(evaluationType, "group") && eKey == "measure" {
						groupData := strings.Split(evaluationType, ":")
						groupByList := eValue.([]interface{})
						validated := false

						for _, v := range groupByList {
							groupMap := v.(map[string]interface{})
							groupByValues := groupMap["GroupByValues"].([]interface{})
							groupByValuesStrs := make([]string, len(groupByValues))
							for i := range groupByValues {
								groupByValuesStrs[i] = groupByValues[i].(string)
							}

							if reflect.DeepEqual(groupByValuesStrs, groupData[2:]) {
								measureVal := groupMap["MeasureVal"].(map[string]interface{})
								validated = verifyResults(measureVal[groupData[1]], relation, expectedValue, rec[0])
							}
						}

						if validated {
							log.Infof("RunQueryFromFile: Query %v was succesful. In %+v", rec[0], time.Since(sTime))
						} else {
							log.Fatalf("RunQueryFromFile: specified group item not found for query %v", rec[0])
						}
					}
				}
			default:
				log.Infof("Received unknown message from server: %+v\n", readEvent)
			}
		}

		index++
	}
}

func RunQueryFromFileAndOutputResponseTimes(dest string, filepath string, queryResultFile string) {
	webSocketURL := dest + "/api/search/ws"
	if queryResultFile == "" {
		queryResultFile = "./query_results.csv"
	}

	log.Infof("Using Websocket URL %+s", webSocketURL)
	log.Infof("Using query result file %+s", queryResultFile)

	csvFile, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("RunQueryFromFileAndOutputResponseTimes: Failed to open query file: %v", err)
	}
	defer csvFile.Close()

	reader := csv.NewReader(csvFile)
	records, err := reader.ReadAll()
	if err != nil {
		log.Fatalf("RunQueryFromFileAndOutputResponseTimes: Failed to read query file: %v", err)
	}

	outputCSVFile, err := os.Create(queryResultFile)
	if err != nil {
		log.Fatalf("RunQueryFromFileAndOutputResponseTimes: Failed to create CSV file: %v", err)
	}
	defer outputCSVFile.Close()

	writer := csv.NewWriter(outputCSVFile)
	defer writer.Flush()

	// Write header to the output CSV
	err = writer.Write([]string{"Query", "Response Time (ms)"})
	if err != nil {
		log.Fatalf("RunQueryFromFileAndOutputResponseTimes: Failed to write header to CSV file: %v", err)
	}

	for index, record := range records {

		qid := index + 1

		query := record[0]
		// Default values
		startEpoch := "now-1h"
		endEpoch := "now"
		queryLanguage := "Splunk QL"

		// Update values if provided in the CSV
		if len(record) > 1 && record[1] != "" {
			startEpoch = record[1]
		}
		if len(record) > 2 && record[2] != "" {
			endEpoch = record[2]
		}
		if len(record) > 3 && record[3] != "" {
			queryLanguage = record[3]
		}

		data := map[string]interface{}{
			"state":         "query",
			"searchText":    query,
			"startEpoch":    startEpoch,
			"endEpoch":      endEpoch,
			"indexName":     "*",
			"queryLanguage": queryLanguage,
		}

		log.Infof("qid=%v, Running query=%v", qid, query)
		conn, _, err := websocket.DefaultDialer.Dial(webSocketURL, nil)
		if err != nil {
			log.Fatalf("RunQueryFromFileAndOutputResponseTimes: qid=%v, Error connecting to WebSocket server: %v", qid, err)
			return
		}
		defer conn.Close()

		startTime := time.Now()
		err = conn.WriteJSON(data)
		if err != nil {
			log.Fatalf("RunQueryFromFileAndOutputResponseTimes: qid=%v, Error sending query to server: %v", qid, err)
			break
		}

		readEvent := make(map[string]interface{})
		for {
			err = conn.ReadJSON(&readEvent)
			if err != nil {
				log.Infof("RunQueryFromFileAndOutputResponseTimes: qid=%v, Error reading response from server for query. Error=%v", qid, err)
				break
			}
			if state, ok := readEvent["state"]; ok && state == "COMPLETE" {
				break
			}
		}
		responseTime := time.Since(startTime).Milliseconds()
		log.Infof("RunQueryFromFileAndOutputResponseTimes: qid=%v, Query=%v,Response Time: %vms", qid, query, responseTime)

		// Write query and response time to output CSV
		err = writer.Write([]string{query, strconv.FormatInt(responseTime, 10)})
		if err != nil {
			log.Fatalf("RunQueryFromFileAndOutputResponseTimes: Failed to write query result to CSV file: %v", err)
		}
	}

	log.Infof("RunQueryFromFileAndOutputResponseTimes: Query results written to CSV file: %v", queryResultFile)
}

var skipIndexes = map[int]bool{

	// Misc
	35:  true, // Log QL Query: IQR.AsResult: error getting final result for GroupBy: IQR.getFinalStatsResults: knownValues is empty
	161: true, // Unused Query: Older pipeline removes the groupByCol/value if something else is renamed to it

	// SQL NORESULT
	22: true, // SQL query order by. NO RESULT
	23: true, // SQL query order by. NO RESULT
	24: true, // SQL query order by. NO RESULT

	// NOT IMPLEMENTED
	// TOP/RARE
	158: true, // rare
	159: true, // top

	// STREAMSTATS
	313: true,
	314: true,
	315: true,
	316: true,
	317: true,
	318: true,
	319: true,
	320: true,
	321: true,
	322: true,
	323: true,
	324: true,
	325: true,
	326: true,
	327: true,
	328: true,
	329: true,
	330: true,
	331: true,
	332: true,
	333: true,
	334: true,
	335: true,
	336: true,
	337: true,
	338: true,
	339: true,
	340: true,
	341: true,
	342: true,
	343: true,
	344: true,
	345: true,
	346: true,
	347: true,
	348: true,
	349: true,
	350: true,
	351: true,
	352: true,
	353: true,
	354: true,
	355: true,
	356: true,
	357: true,
	358: true,
	359: true,
	360: true,
	361: true,
	362: true,
	363: true,
	364: true,
	365: true,
	366: true,
	367: true,
	368: true,
	369: true,
	370: true,
	371: true,
	372: true,
	373: true,
	374: true,
}
