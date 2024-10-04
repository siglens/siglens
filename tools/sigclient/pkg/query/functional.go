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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"
	"verifier/pkg/utils"

	"github.com/fasthttp/websocket"

	log "github.com/sirupsen/logrus"
)

type Hits struct {
	TotalMatched interface{}
	Records      []map[string]interface{} // key: column name, value: column value
}

type BucketHolder struct {
	GroupByValues []string
	MeasureVal    map[string]interface{} // key: measure name, value: measure value
}

type Result struct {
	TotalMatched     interface{}
	Records          []map[string]interface{}
	AllColumns       []string
	ColumnsOrder     []string
	GroupByCols      []string
	MeasureFunctions []string
	MeasureResults   []BucketHolder
	Qtype            string
	BucketCount      int
}

func CreateListOfMap(resp interface{}) []map[string]interface{} {
	items, isList := resp.([]interface{})
	if !isList {
		log.Fatalf("CreateListOfMap: Records is not a list")
	}

	mapList := make([]map[string]interface{}, 0)
	for _, item := range items {
		mapItem, isMap := item.(map[string]interface{})
		if !isMap {
			log.Fatalf("CreateListOfMap: Record is not a map")
		}
		mapList = append(mapList, mapItem)
	}
	return mapList
}

func CreateListOfString(resp interface{}) []string {
	items, isList := resp.([]interface{})
	if !isList {
		log.Fatalf("CreateListOfString: Strings List is not a list")
	}

	stringList := make([]string, 0)
	for _, item := range items {
		strItem, isString := item.(string)
		if !isString {
			log.Fatalf("CreateListOfString: Value is not a string")
		}
		stringList = append(stringList, strItem)
	}
	return stringList
}

func CreateResult(response map[string]interface{}) *Result {
	res := &Result{}
	qtype, exist := response["qtype"]
	if !exist {
		log.Fatalf("queryType not found in response")
	}
	switch qtype {
	case "logs-query":
		res.Qtype = "logs-query"
		CreateResultForRRC(res, response)
	case "segstats-query":
		res.Qtype = "segstats-query"
		CreateResultForStats(res, response)
	case "aggs-query":
		res.Qtype = "aggs-query"
		CreateResultForGroupBy(res, response)
	default:
		log.Fatalf("Invalid query type: %v", qtype)
	}

	return res
}

func CreateResultForRRC(res *Result, response map[string]interface{}) {
	_, exist := response["allColumns"]
	if !exist {
		log.Fatalf("allColumns not found in response")
	}

	res.AllColumns = CreateListOfString(response["allColumns"])
	res.ColumnsOrder = CreateListOfString(response["columnsOrder"])
}

func CreateResultForStats(res *Result, response map[string]interface{}) {
	res.MeasureFunctions = CreateListOfString(response["measureFunctions"])
	measureRes := CreateListOfMap(response["measure"])
	for _, measure := range measureRes {
		var isMap bool
		bucket := BucketHolder{}
		bucket.GroupByValues = CreateListOfString(measure["GroupByValues"])
		bucket.MeasureVal, isMap = measure["MeasureVal"].(map[string]interface{})
		if !isMap {
			log.Fatalf("CreateExpResultForStats: measureVal is not a map")
		}
		res.MeasureResults = append(res.MeasureResults, bucket)
	}
}

func CreateResultForGroupBy(res *Result, response map[string]interface{}) {
	_, exist := response["bucketCount"]
	if !exist {
		log.Fatalf("bucketCount not found in response")
	}
	bucketCount, isFloat := response["bucketCount"].(float64)
	if !isFloat {
		log.Fatalf("CreateExpResultForGroupBy: bucketCount is not numeric")
	}
	res.BucketCount = int(bucketCount)
	res.GroupByCols = CreateListOfString(response["groupByCols"])

	CreateResultForStats(res, response)
}

func GetStringValueFromResponse(resp map[string]interface{}, key string) (string, error) {
	value, exist := resp[key]
	if !exist {
		return "", fmt.Errorf("key %v not found in response", key)
	}

	query, isString := value.(string)
	if !isString {
		return "", fmt.Errorf("value %v is not a string, is of type %T", value, value)
	}

	return query, nil
}

func ReadAndValidateQueryFile(filePath string) (string, *Result) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("ReadAndValidateQueryFile: Error opening file: %v, err: %v", filePath, err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	response := make(map[string]interface{})
	err = decoder.Decode(&response)
	if err != nil {
		log.Fatalf("ReadAndValidateQueryFile: Error decoding file: %v, err: %v", filePath, err)
	}

	query, err := GetStringValueFromResponse(response, "queryText")
	if err != nil {
		log.Fatalf("ReadAndValidateQueryFile: Error getting query from response: %v", err)
	}

	if _, exist := response["expectedResult"]; !exist {
		log.Fatalf("ReadAndValidateQueryFile: expectedResult not found")
	}

	expectedResult, isMap := response["expectedResult"].(map[string]interface{})
	if !isMap {
		log.Fatalf("ReadAndValidateQueryFile: expectedResult is not valid it's of type: %T", response["expectedResult"])
	}

	expRes := CreateResult(expectedResult)
	if expRes.Qtype == "logs-query" {
		populateTotalMatchedAndRecords(expectedResult, expRes)
		if len(expRes.Records) == 0 || len(expRes.Records) > 10 {
			log.Fatalf("ReadQueryFile: Number of records should be in range 1-10 for logs-query, got: %v", len(expRes.Records))
		}
	}

	return query, expRes
}

// Main function that tests all the queries
func FunctionalTest(dest string, dataPath string) {

	queryFiles, err := os.ReadDir(dataPath)
	if err != nil {
		log.Fatalf("FunctionalTest: Error reading directory: %v, err: %v", dataPath, err)
		return
	}

	// validate JSON files
	for _, file := range queryFiles {
		if !strings.HasSuffix(file.Name(), ".json") {
			log.Fatalf("FunctionalTest: Invalid file format: %v. Expected .json", file.Name())
			return
		}
	}

	// Default values
	startEpoch := "now-1h"
	endEpoch := "now"
	queryLanguage := "Splunk QL"

	// run query
	queryReq := map[string]interface{}{
		"state":         "query",
		"searchText":    "",
		"startEpoch":    startEpoch,
		"endEpoch":      endEpoch,
		"indexName":     "*",
		"queryLanguage": queryLanguage,
	}

	// run queries
	for idx, file := range queryFiles {
		filePath := filepath.Join(dataPath, file.Name())
		query, expRes := ReadAndValidateQueryFile(filePath)
		log.Infof("FunctionalTest: qid=%v, Running query=%v", idx, query)
		queryReq["searchText"] = query

		EvaluateQueryForWebSocket(dest, queryReq, idx, expRes)
		EvaluateQueryForAPI(dest, queryReq, idx, expRes)

		log.Infoln()
	}

	log.Infof("FunctionalTest: All queries passed successfully")
}

func EvaluateQueryForAPI(dest string, queryReq map[string]interface{}, qid int, expRes *Result) {
	query := queryReq["searchText"].(string)

	reqBody, err := json.Marshal(queryReq)
	if err != nil {
		log.Fatalf("EvaluateQueryForAPI: Error marshaling request: %v", err)
	}

	url := fmt.Sprintf("http://%s/api/search", dest)
	req, err := http.NewRequestWithContext(context.TODO(), http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		log.Fatalf("EvaluateQueryForAPI: Error creating request: %v", err)
	}
	req.Header.Set("content-type", "application/json")

	sTime := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("EvaluateQueryForAPI: Error sending request: %v", err)
	}
	defer resp.Body.Close()

	responseData := make(map[string]interface{})
	if bodyBytes, err := io.ReadAll(resp.Body); err != nil {
		log.Fatalf("EvaluateQueryForAPI: Error reading response: %v", err)
	} else {
		if err := json.Unmarshal(bodyBytes, &responseData); err != nil {
			if len(bodyBytes) > 20 {
				bodyBytes = bodyBytes[:20]
			}
			log.Fatalf("EvaluateQueryForAPI: Error unmarshaling response: %v, response: %v", err, string(bodyBytes))
		}
	}

	queryRes := CreateResult(responseData)
	populateTotalMatchedAndRecords(getHits(responseData), queryRes)
	err = CompareResults(queryRes, expRes, query)
	if err != nil {
		log.Fatalf("EvaluateQueryForAPI: Failed query: %v, err: %v", query, err)
	}
	log.Infof("EvaluateQueryForAPI: Query %v was succesful. In %+v", query, time.Since(sTime))
}

func populateRecords(response map[string]interface{}, res *Result) {
	if response == nil {
		return
	}
	respRecords, exist := response["records"]
	if exist && respRecords != nil {
		records := CreateListOfMap(respRecords)
		res.Records = append(res.Records, records...)
	}
}

func populateTotalMatched(response map[string]interface{}, res *Result) {
	if response == nil {
		return
	}
	res.TotalMatched = response["totalMatched"]
}

func populateTotalMatchedAndRecords(response map[string]interface{}, res *Result) {
	populateTotalMatched(response, res)
	populateRecords(response, res)
}

func getHits(response map[string]interface{}) map[string]interface{} {
	_, exist := response["hits"]
	if exist && response["hits"] != nil {
		hits, isMap := response["hits"].(map[string]interface{})
		if !isMap {
			log.Fatalf("EvaluateQueryForWebSocket: hits is not a map")
		}
		return hits
	}
	return nil
}

// Evaluates queries and compares the results with expected results for websocket
func EvaluateQueryForWebSocket(dest string, queryReq map[string]interface{}, qid int, expRes *Result) {
	webSocketURL := fmt.Sprintf("ws://%s/api/search/ws", dest)
	query := queryReq["searchText"].(string)

	// create websocket connection
	conn, _, err := websocket.DefaultDialer.Dial(webSocketURL, nil)
	if err != nil {
		log.Fatalf("EvaluateQueryForWebSocket: Error connecting to WebSocket server: %v", err)
		return
	}
	defer conn.Close()

	err = conn.WriteJSON(queryReq)
	if err != nil {
		log.Fatalf("EvaluateQueryForWebSocket: Received err message from server: %+v\n", err)
	}

	readEvent := make(map[string]interface{})
	sTime := time.Now()
	tempRes := &Result{}
	for {
		err = conn.ReadJSON(&readEvent)
		if err != nil {
			log.Infof("EvaluateQueryForWebSocket: Received error from server: %+v\n", err)
			break
		}
		switch readEvent["state"] {
		case "RUNNING":
		case "QUERY_UPDATE":
			// As query results come in chunks, we need to keep track of all the records
			populateRecords(getHits(readEvent), tempRes)
		case "COMPLETE":
			queryRes := CreateResult(readEvent)
			queryRes.Records = tempRes.Records
			populateTotalMatched(readEvent, queryRes)

			err = CompareResults(queryRes, expRes, query)
			if err != nil {
				log.Fatalf("EvaluateQueryForWebSocket: Failed query: %v, err: %v", query, err)
			}
		default:
			log.Infof("EvaluateQueryForWebSocket: Received unknown message from server: %+v\n", readEvent)
		}
	}
	log.Infof("EvaluateQueryForWebSocket: Query %v was succesful. In %+v", query, time.Since(sTime))
}

func CompareResults(queryRes *Result, expRes *Result, query string) error {
	if queryRes.Qtype != expRes.Qtype {
		return fmt.Errorf("CompareResults: Query type mismatch, expected: %v, got: %v", expRes.Qtype, queryRes.Qtype)
	}

	switch queryRes.Qtype {
	case "logs-query":
		return ValidateLogsQueryResults(queryRes, expRes)
	case "segstats-query":
		return ValidateStatsQueryResults(queryRes, expRes)
	case "aggs-query":
		return ValidateGroupByQueryResults(queryRes, expRes)
	default:
		return fmt.Errorf("CompareResults: Invalid query type: %v", queryRes.Qtype)
	}
}

func ValidateRecord(record map[string]interface{}, expRecord map[string]interface{}) error {
	for col, value := range expRecord {
		actualValue, exist := record[col]
		if !exist {
			return fmt.Errorf("ValidateRecord: Value not found for column: %v", col)
		}
		isEqual := reflect.DeepEqual(actualValue, value)
		if !isEqual {
			return fmt.Errorf("ValidateRecord: Value mismatch for column: %v, expected: %v, got: %v", col, value, actualValue)
		}
	}

	return nil
}

func sortRecords(records []map[string]interface{}, column string) {
	sort.Slice(records, func(i, j int) bool {
		return records[i][column].(string) < records[j][column].(string)
	})
}

/*
*
  - ValidateLogsQueryResults validates the logs query results
    Compares:
  - TotalMatched
  - AllColumns
  - ColumnsOrder
  - Records
  - @param queryRes: Query results from the server
  - @param expRes: Expected results
  - @return error: Error if validation fails

*
*/
func ValidateLogsQueryResults(queryRes *Result, expRes *Result) error {
	var err error

	equal := reflect.DeepEqual(queryRes.TotalMatched, expRes.TotalMatched)
	if !equal {
		return fmt.Errorf("ValidateLogsQueryResults: TotalMatched mismatch, expected: %v, got: %v", expRes.TotalMatched, queryRes.TotalMatched)
	}

	equal = utils.ElementsMatch(queryRes.AllColumns, queryRes.AllColumns)
	if !equal {
		return fmt.Errorf("ValidateLogsQueryResults: AllColumns mismatch, expected: %+v, got: %+v", expRes.AllColumns, queryRes.AllColumns)
	}
	colsToRemove := map[string]struct{}{
		"timestamp": {},
		"_index":    {},
	}
	equal = utils.CompareStringSlices(utils.RemoveCols(expRes.ColumnsOrder, colsToRemove), utils.RemoveCols(queryRes.ColumnsOrder, colsToRemove))
	if !equal {
		return fmt.Errorf("ValidateLogsQueryResults: ColumnsOrder mismatch, expected: %+v, got: %+v", expRes.ColumnsOrder, queryRes.ColumnsOrder)
	}
	if len(queryRes.Records) < len(expRes.Records) {
		return fmt.Errorf("ValidateLogsQueryResults: Less records than, expected at least: %v, got: %v", len(expRes.Records), len(queryRes.Records))
	}
	sortRecords(queryRes.Records, "ident")
	sortRecords(expRes.Records, "ident")

	for idx, record := range expRes.Records {
		err = ValidateRecord(queryRes.Records[idx], record)
		if err != nil {
			return err
		}
	}

	return nil
}

func getGroupByCombination(grpByValues []string) string {
	grpByValue := ""
	for _, value := range grpByValues {
		grpByValue += value + "_"
	}
	return grpByValue
}

func sortMeasureResults(buckets []BucketHolder) {
	sort.Slice(buckets, func(i, j int) bool {
		return getGroupByCombination(buckets[i].GroupByValues) < getGroupByCombination(buckets[j].GroupByValues)
	})
}

/*
*
  - ValidateStatsQueryResults validates the stats query results
    Compares:
  - MeasureFunctions
  - MeasureResults
  - @param queryRes: Query results from the server
  - @param expRes: Expected results
  - @return error: Error if validation fails

*
*/
func ValidateStatsQueryResults(queryRes *Result, expRes *Result) error {

	if len(queryRes.MeasureResults) != len(expRes.MeasureResults) {
		return fmt.Errorf("ValidateStatsQueryResults: MeasureResults length mismatch, expected: %v, got: %v", len(expRes.MeasureResults), len(queryRes.MeasureResults))
	}

	equal := utils.ElementsMatch(queryRes.MeasureFunctions, expRes.MeasureFunctions)
	if !equal {
		return fmt.Errorf("ValidateStatsQueryResults: MeasureFunctions mismatch, expected: %+v, got: %+v", expRes.MeasureFunctions, queryRes.MeasureFunctions)
	}

	// BucketResults are not send in a specific order, so sort them before comparing
	sortMeasureResults(queryRes.MeasureResults)
	sortMeasureResults(expRes.MeasureResults)

	for idx, expMeasureRes := range expRes.MeasureResults {
		queryMeasureRes := queryRes.MeasureResults[idx]

		equal := reflect.DeepEqual(queryMeasureRes.GroupByValues, expMeasureRes.GroupByValues)
		if !equal {
			return fmt.Errorf("ValidateStatsQueryResults: GroupByCombination mismatch, expected: %+v, got: %+v", expMeasureRes.GroupByValues, queryMeasureRes.GroupByValues)
		}

		if len(queryMeasureRes.MeasureVal) != len(expMeasureRes.MeasureVal) {
			return fmt.Errorf("ValidateStatsQueryResults: MeasureVal length mismatch, expected: %v, got: %v", len(expMeasureRes.MeasureVal), len(queryMeasureRes.MeasureVal))
		}

		for key, value := range expMeasureRes.MeasureVal {
			actualValue, exist := queryMeasureRes.MeasureVal[key]
			if !exist {
				return fmt.Errorf("ValidateStatsQueryResults: MeasureVal not found for key: %v", key)
			}
			equal = reflect.DeepEqual(actualValue, value)
			if !equal {
				return fmt.Errorf("ValidateStatsQueryResults: MeasureVal mismatch for key: %v, expected: %v, got: %v", key, value, actualValue)
			}
		}
	}

	return nil
}

/*
*
  - ValidateGroupByQueryResults validates the group by query results
    Compares:
  - BucketCount
  - GroupByCols
  - MeasureFunctions
  - MeasureResults
  - @param queryRes: Query results from the server
  - @param expRes: Expected results

*
*/
func ValidateGroupByQueryResults(queryRes *Result, expRes *Result) error {
	if queryRes.BucketCount != expRes.BucketCount {
		return fmt.Errorf("ValidateGroupByQueryResults: BucketCount mismatch, expected: %v, got: %v", expRes.BucketCount, queryRes.BucketCount)
	}

	equal := utils.ElementsMatch(queryRes.GroupByCols, expRes.GroupByCols)
	if !equal {
		return fmt.Errorf("ValidateGroupByQueryResults: GroupByCols mismatch, expected: %+v, got: %+v", expRes.GroupByCols, queryRes.GroupByCols)
	}

	return ValidateStatsQueryResults(queryRes, expRes)
}
