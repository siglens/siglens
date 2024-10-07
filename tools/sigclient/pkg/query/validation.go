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
	"reflect"
	"sort"
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
	UniqueKeyCols    []string // Key can be a combination of columns
	AllColumns       []string
	ColumnsOrder     []string
	GroupByCols      []string
	MeasureFunctions []string
	MeasureResults   []BucketHolder
	Qtype            string
	BucketCount      int
}

func CreateListOfMap(resp interface{}) ([]map[string]interface{}, error) {
	items, isList := resp.([]interface{})
	if !isList {
		return nil, fmt.Errorf("CreateListOfMap: resp is not a list, received type: %T", resp)
	}

	mapList := make([]map[string]interface{}, 0)
	for _, item := range items {
		mapItem, isMap := item.(map[string]interface{})
		if !isMap {
			return nil, fmt.Errorf("CreateListOfMap: item is not a map, received type: %T", item)
		}
		mapList = append(mapList, mapItem)
	}
	return mapList, nil
}

func CreateListOfString(resp interface{}) ([]string, error) {
	items, isList := resp.([]interface{})
	if !isList {
		return nil, fmt.Errorf("CreateListOfString: resp is not a list, received type: %T", resp)
	}

	stringList := make([]string, 0)
	for _, item := range items {
		strItem, isString := item.(string)
		if !isString {
			return nil, fmt.Errorf("CreateListOfString: item is not a string, received type: %T", item)
		}
		stringList = append(stringList, strItem)
	}
	return stringList, nil
}

func CreateResult(response map[string]interface{}) (*Result, error) {
	var err error
	res := &Result{}
	qtype, exist := response["qtype"]
	if !exist {
		return nil, fmt.Errorf("CreateResult: qtype not found in response")
	}
	switch qtype {
	case "logs-query":
		res.Qtype = "logs-query"
		err = CreateResultForRRC(res, response)
	case "segstats-query":
		res.Qtype = "segstats-query"
		err = CreateResultForStats(res, response)
	case "aggs-query":
		res.Qtype = "aggs-query"
		err = CreateResultForGroupBy(res, response)
	default:
		return nil, fmt.Errorf("CreateResult: Invalid qtype: %v", qtype)
	}

	return res, err
}

func CreateResultForRRC(res *Result, response map[string]interface{}) error {
	var err error
	_, exist := response["allColumns"]
	if !exist {
		return fmt.Errorf("allColumns not found in response")
	}

	res.AllColumns, err = CreateListOfString(response["allColumns"])
	if err != nil {
		return fmt.Errorf("CreateResultForRRC: Error fetching allColumns from response, err: %v", err)
	}

	res.ColumnsOrder, err = CreateListOfString(response["columnsOrder"])
	if err != nil {
		return fmt.Errorf("CreateResultForRRC: Error fetching columnsOrder from response, err: %v", err)
	}

	return nil
}

func CreateResultForStats(res *Result, response map[string]interface{}) error {
	var err error
	res.MeasureFunctions, err = CreateListOfString(response["measureFunctions"])
	if err != nil {
		return fmt.Errorf("CreateExpResultForStats: Error fetching measureFunctions from response, err: %v", err)
	}
	measureRes, err := CreateListOfMap(response["measure"])
	if err != nil {
		return fmt.Errorf("CreateExpResultForStats: Error fetching measure from response, err: %v", err)
	}

	for _, measure := range measureRes {
		var isMap bool
		bucket := BucketHolder{}
		bucket.GroupByValues, err = CreateListOfString(measure["GroupByValues"])
		if err != nil {
			return fmt.Errorf("CreateExpResultForStats: Error fetching GroupByValues from measure, err: %v", err)
		}
		bucket.MeasureVal, isMap = measure["MeasureVal"].(map[string]interface{})
		if !isMap {
			return fmt.Errorf("CreateExpResultForStats: measureVal is not a map, received type: %T", measure["MeasureVal"])
		}
		res.MeasureResults = append(res.MeasureResults, bucket)
	}

	return nil
}

func CreateResultForGroupBy(res *Result, response map[string]interface{}) error {
	var err error
	_, exist := response["bucketCount"]
	if !exist {
		return fmt.Errorf("bucketCount not found in response")
	}
	bucketCount, isFloat := response["bucketCount"].(float64)
	if !isFloat {
		return fmt.Errorf("CreateExpResultForGroupBy: bucketCount is not numeric, received type: %T", response["bucketCount"])
	}
	res.BucketCount = int(bucketCount)
	res.GroupByCols, err = CreateListOfString(response["groupByCols"])
	if err != nil {
		return fmt.Errorf("CreateExpResultForGroupBy: Error fetching groupByCols from response, err: %v", err)
	}

	return CreateResultForStats(res, response)
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

func ValidateUniqueKeyColsInResult(res *Result) error {
	if len(res.UniqueKeyCols) == 0 {
		return fmt.Errorf("ValidateUniqueKeyColsInResult: UniqueKeyCols not found in result")
	}
	for _, record := range res.Records {
		for _, col := range res.UniqueKeyCols {
			_, exist := record[col]
			if !exist {
				return fmt.Errorf("ValidateUniqueKeyColsInResult: UniqueColumn %v not found in record: %v, UniqueKeyCols: %v", col, record, res.UniqueKeyCols)
			}
		}
	}
	return nil
}

func ReadAndValidateQueryFile(filePath string) (string, *Result, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", nil, fmt.Errorf("ReadAndValidateQueryFile: Error opening file: %v, err: %v", filePath, err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	response := make(map[string]interface{})
	err = decoder.Decode(&response)
	if err != nil {
		return "", nil, fmt.Errorf("ReadAndValidateQueryFile: Error decoding file: %v, err: %v", filePath, err)
	}

	query, err := GetStringValueFromResponse(response, "queryText")
	if err != nil {
		return "", nil, fmt.Errorf("ReadAndValidateQueryFile: Error getting queryText from response, file: %v, err: %v", filePath, err)
	}

	if _, exist := response["expectedResult"]; !exist {
		return "", nil, fmt.Errorf("ReadAndValidateQueryFile: expectedResult not found in file: %v", filePath)
	}

	expectedResult, isMap := response["expectedResult"].(map[string]interface{})
	if !isMap {
		return "", nil, fmt.Errorf("ReadAndValidateQueryFile: expectedResult is not valid it's of type: %T, file: %v", response["expectedResult"], filePath)
	}

	expRes, err := CreateResult(expectedResult)
	if err != nil {
		return "", nil, fmt.Errorf("ReadAndValidateQueryFile: Error creating expected result, file: %v, err: %v", filePath, err)
	}
	if expRes.Qtype == "logs-query" {
		uniqueKeyCols, exist := expectedResult["uniqueKeyCols"]
		if !exist {
			return "", nil, fmt.Errorf("ReadAndValidateQueryFile: uniqueKey not found in logs-query, file: %v", filePath)
		}
		expRes.UniqueKeyCols, err = CreateListOfString(uniqueKeyCols)
		if err != nil {
			return "", nil, fmt.Errorf("ReadAndValidateQueryFile: Error fetching uniqueKeyCols, uniqueKeyCols: %v file: %v", uniqueKeyCols, filePath)
		}
		err = populateTotalMatchedAndRecords(expectedResult, expRes)
		if err != nil {
			return "", nil, fmt.Errorf("ReadAndValidateQueryFile: Error populating totalMatched and records, file: %v, err: %v", filePath, err)
		}
		if len(expRes.Records) == 0 || len(expRes.Records) > 10 {
			return "", nil, fmt.Errorf("ReadAndValidateQueryFile: Number of records should be in range 1-10 for logs-query, got: %v file: %v", len(expRes.Records), filePath)
		}
		err = ValidateUniqueKeyColsInResult(expRes)
		if err != nil {
			return "", nil, fmt.Errorf("ReadAndValidateQueryFile: Error validating UniqueKeyCols, file: %v, err: %v", filePath, err)
		}
	}

	return query, expRes, nil
}

func EvaluateQueryForAPI(dest string, queryReq map[string]interface{}, qid int, expRes *Result) error {
	query := queryReq["searchText"].(string)

	reqBody, err := json.Marshal(queryReq)
	if err != nil {
		return fmt.Errorf("EvaluateQueryForAPI: Error marshaling request, reqBody: %v, err: %v", reqBody, err)
	}

	url := fmt.Sprintf("http://%s/api/search", dest)
	req, err := http.NewRequestWithContext(context.TODO(), http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("EvaluateQueryForAPI: Error creating request, url: %v, reqBody: %v, err: %v", url, reqBody, err)
	}
	req.Header.Set("content-type", "application/json")

	sTime := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("EvaluateQueryForAPI: Error sending request, req: %v, err: %v", req, err)
	}
	defer resp.Body.Close()

	responseData := make(map[string]interface{})
	if bodyBytes, err := io.ReadAll(resp.Body); err != nil {
		return fmt.Errorf("EvaluateQueryForAPI: Error reading response, resp.Body: %v, err: %v", resp.Body, err)
	} else {
		if err := json.Unmarshal(bodyBytes, &responseData); err != nil {
			return fmt.Errorf("EvaluateQueryForAPI: Error unmarshaling bodyBytes: %v, err: %v", string(bodyBytes), err)
		}
	}

	queryRes, err := CreateResult(responseData)
	if err != nil {
		return fmt.Errorf("EvaluateQueryForAPI: Error creating result, responseData: %v, err: %v", responseData, err)
	}
	hits, err := getHits(responseData)
	if err != nil {
		return fmt.Errorf("EvaluateQueryForAPI: Error getting hits, responseData: %v, err: %v", responseData, err)
	}
	err = populateTotalMatchedAndRecords(hits, queryRes)
	if err != nil {
		return fmt.Errorf("EvaluateQueryForAPI: Error populating totalMatched and records, hits: %v, responseData: %v, err: %v", hits, responseData, err)
	}
	err = CompareResults(queryRes, expRes, query)
	if err != nil {
		return fmt.Errorf("EvaluateQueryForAPI: Failed query: %v, responseData: %v, err: %v", query, responseData, err)
	}

	log.Infof("EvaluateQueryForAPI: Query %v was succesful. In %+v", query, time.Since(sTime))
	return nil
}

func populateRecords(response map[string]interface{}, res *Result) error {
	if response == nil {
		return nil
	}
	respRecords, exist := response["records"]
	if exist && respRecords != nil {
		records, err := CreateListOfMap(respRecords)
		if err != nil {
			return fmt.Errorf("populateRecords: Error creating records, respRecords: %v, err: %v", respRecords, err)
		}
		res.Records = append(res.Records, records...)
	}

	return nil
}

func populateTotalMatched(response map[string]interface{}, res *Result) {
	if response == nil {
		return
	}
	res.TotalMatched = response["totalMatched"]
}

func populateTotalMatchedAndRecords(response map[string]interface{}, res *Result) error {
	populateTotalMatched(response, res)
	return populateRecords(response, res)
}

func getHits(response map[string]interface{}) (map[string]interface{}, error) {
	_, exist := response["hits"]
	if exist && response["hits"] != nil {
		hits, isMap := response["hits"].(map[string]interface{})
		if !isMap {
			return nil, fmt.Errorf("getHits: hits is not a map, received type: %T", response["hits"])
		}
		return hits, nil
	}
	return nil, nil
}

// Evaluates queries and compares the results with expected results for websocket
func EvaluateQueryForWebSocket(dest string, queryReq map[string]interface{}, qid int, expRes *Result) error {
	webSocketURL := fmt.Sprintf("ws://%s/api/search/ws", dest)
	query := queryReq["searchText"].(string)

	// create websocket connection
	conn, _, err := websocket.DefaultDialer.Dial(webSocketURL, nil)
	if err != nil {
		return fmt.Errorf("EvaluateQueryForWebSocket: Error connecting to WebSocket server, webSocketURL: %v, err: %v", webSocketURL, err)
	}
	defer conn.Close()

	err = conn.WriteJSON(queryReq)
	if err != nil {
		return fmt.Errorf("EvaluateQueryForWebSocket: Received error from server, queryReq: %v, err: %+v\n", queryReq, err)
	}

	readEvent := make(map[string]interface{})
	sTime := time.Now()
	tempRes := &Result{}
	for {
		err = conn.ReadJSON(&readEvent)
		if err != nil {
			log.Infof("EvaluateQueryForWebSocket: Received error from server, queryReq: %v, err: %+v\n", queryReq, err)
			break
		}
		switch readEvent["state"] {
		case "RUNNING":
		case "QUERY_UPDATE":
			// As query results come in chunks, we need to keep track of all the records
			hits, err := getHits(readEvent)
			if err != nil {
				return fmt.Errorf("EvaluateQueryForWebSocket: Error getting hits, readEvent: %v, err: %v", readEvent, err)
			}
			err = populateRecords(hits, tempRes)
			if err != nil {
				return fmt.Errorf("EvaluateQueryForWebSocket: Error populating records, hits: %v, readEvent: %v, err: %v", hits, readEvent, err)
			}
		case "COMPLETE":
			queryRes, err := CreateResult(readEvent)
			if err != nil {
				return fmt.Errorf("EvaluateQueryForWebSocket: Error creating result, readEvent: %v, err: %v", readEvent, err)
			}
			queryRes.Records = tempRes.Records
			populateTotalMatched(readEvent, queryRes)

			err = CompareResults(queryRes, expRes, query)
			if err != nil {
				return fmt.Errorf("EvaluateQueryForWebSocket: Failed evaluating query: %v, readEvent: %v, err: %v", query, readEvent, err)
			}
		default:
			return fmt.Errorf("EvaluateQueryForWebSocket: Received unknown message from server, readEvent: %+v\n", readEvent)
		}
	}
	log.Infof("EvaluateQueryForWebSocket: Query %v was succesful. In %+v", query, time.Since(sTime))

	return nil
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

func CompareFloatValues(actualValue interface{}, expValue float64) (bool, error) {
	actual, isFloat := actualValue.(float64)
	if !isFloat {
		return false, fmt.Errorf("CompareFloatValues: actualValue %v is not a float, received type: %T", actualValue, actualValue)
	}

	tolerance := 1e-5

	return utils.AlmostEqual(actual, expValue, tolerance), nil
}

func ValidateRecord(record map[string]interface{}, expRecord map[string]interface{}) error {
	var err error

	for col, value := range expRecord {
		equal := false
		actualValue, exist := record[col]
		if !exist {
			return fmt.Errorf("ValidateRecord: Value not found for column: %v", col)
		}

		expFloatValue, isFloat := value.(float64)
		if isFloat {
			equal, err = CompareFloatValues(actualValue, expFloatValue)
			if err != nil {
				return fmt.Errorf("ValidateRecord: Error comparing float values, err: %v", err)
			}
		} else {
			equal = reflect.DeepEqual(actualValue, value)
		}

		if !equal {
			return fmt.Errorf("ValidateRecord: Value mismatch for column: %v, expected: %v, got: %v", col, value, actualValue)
		}
	}

	return nil
}

func sortRecords(records []map[string]interface{}, columns []string) {

	sort.Slice(records, func(i, j int) bool {
		for _, column := range columns {
			valI, valJ := records[i][column], records[j][column]

			strI := fmt.Sprintf("%v", valI)
			strJ := fmt.Sprintf("%v", valJ)

			if strI == strJ {
				continue
			}

			return strI < strJ
		}
		return false
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
	colsToRemove := map[string]struct{}{
		"timestamp": {},
		"_index":    {},
	}

	queryRes.AllColumns = utils.RemoveValues(queryRes.AllColumns, colsToRemove)
	expRes.AllColumns = utils.RemoveValues(expRes.AllColumns, colsToRemove)

	equal = utils.ElementsMatch(queryRes.AllColumns, expRes.AllColumns)
	if !equal {
		return fmt.Errorf("ValidateLogsQueryResults: AllColumns mismatch, expected: %+v, got: %+v", expRes.AllColumns, queryRes.AllColumns)
	}

	queryRes.ColumnsOrder = utils.RemoveValues(queryRes.ColumnsOrder, colsToRemove)
	expRes.ColumnsOrder = utils.RemoveValues(expRes.ColumnsOrder, colsToRemove)

	equal = utils.CompareSlices(queryRes.ColumnsOrder, expRes.ColumnsOrder)
	if !equal {
		return fmt.Errorf("ValidateLogsQueryResults: ColumnsOrder mismatch, expected: %+v, got: %+v", expRes.ColumnsOrder, queryRes.ColumnsOrder)
	}
	if len(queryRes.Records) < len(expRes.Records) {
		return fmt.Errorf("ValidateLogsQueryResults: Less records than, expected at least: %v, got: %v", len(expRes.Records), len(queryRes.Records))
	}

	queryRes.UniqueKeyCols = expRes.UniqueKeyCols
	err = ValidateUniqueKeyColsInResult(queryRes)
	if err != nil {
		return fmt.Errorf("ValidateLogsQueryResults: Error validating UniqueKeyCols: %v in queryRes, err: %v", queryRes.UniqueKeyCols, err)
	}

	sortRecords(queryRes.Records, queryRes.UniqueKeyCols)
	sortRecords(expRes.Records, expRes.UniqueKeyCols)

	for idx, record := range expRes.Records {
		err = ValidateRecord(queryRes.Records[idx], record)
		if err != nil {
			return fmt.Errorf("Error comparing records: queryRes Record: %v, expRes Record: %v, err: %v", queryRes.Records[idx], record, err)
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
		var err error
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
			expFloatValue, isFloat := value.(float64)
			if isFloat {
				equal, err = CompareFloatValues(actualValue, expFloatValue)
				if err != nil {
					return fmt.Errorf("ValidateStatsQueryResults: Error comparing float values, err: %v", err)
				}
			} else {
				equal = reflect.DeepEqual(actualValue, value)
			}

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
