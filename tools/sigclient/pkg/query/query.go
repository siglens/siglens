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
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

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
							hits, err = verifyInequality(finalHits, relation, expectedValue)
						case map[string]interface{}:
							for k, v := range eValue {
								if k == "value" {
									var ok bool
									finalHits, ok = v.(float64)
									if !ok {
										log.Fatalf("RunQueryFromFile: Returned total matched is not a float: %v", v)
									}
									hits, err = verifyInequality(finalHits, relation, expectedValue)

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
								actualValue, ok := measureVal[groupData[1]].(float64)
								actualValueIsNumber := true
								if !ok {
									// Try converting it to a string and then a float.
									actualValueStr, ok := measureVal[groupData[1]].(string)
									if !ok {
										log.Fatalf("RunQueryFromFile: Returned aggregate is not a string: %v", measureVal[groupData[1]])
									}

									var err error
									actualValue, err = strconv.ParseFloat(actualValueStr, 64)

									if err != nil {
										actualValueIsNumber = false
									}
								}

								if actualValueIsNumber {
									ok, err = verifyInequality(actualValue, relation, expectedValue)
								} else {
									ok, err = verifyInequalityForStr(measureVal[groupData[1]].(string), relation, expectedValue)
								}

								if err != nil {
									log.Fatalf("RunQueryFromFile: Error in verifying aggregation: %v", err)
								} else if !ok {
									log.Fatalf("RunQueryFromFile: Actual aggregate value: %v is not [%s %v] for query: %v",
										actualValue, expectedValue, relation, rec[0])
								} else {
									validated = true
								}
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
	}
}

// Only string comparisons for equality are allowed
func verifyInequalityForStr(actual string, relation, expected string) (bool, error) {
	if relation == "eq" {
		if actual == expected {
			return true, nil
		} else {
			return false, fmt.Errorf("verifyInequalityForStr: actual: \"%v\" and expected: \"%v\" are not equal", actual, expected)
		}
	} else {
		log.Errorf("verifyInequalityForStr: Invalid relation: %v", relation)
		return false, fmt.Errorf("verifyInequalityForStr: Invalid relation: %v", relation)
	}
}

// verifyInequality verifies the expected inequality returned by the query.
// returns true, nil if relation is ""
func verifyInequality(actual float64, relation, expected string) (bool, error) {
	if relation == "" {
		return true, nil
	}
	fltVal, err := strconv.ParseFloat(expected, 64)
	if err != nil {
		log.Errorf("verifyInequality: Error in parsing expected value: %v, err: %v", expected, err)
		return false, err
	}
	switch relation {
	case "eq":
		if actual == fltVal {
			return true, nil
		}
	case "gt":
		if actual > fltVal {
			return true, nil
		}
	case "lt":
		if actual < fltVal {
			return true, nil
		}
	default:
		log.Errorf("verifyInequality: Invalid relation: %v", relation)
		return false, fmt.Errorf("verifyInequality: Invalid relation: %v", relation)
	}
	return false, nil
}
