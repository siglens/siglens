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
	"strconv"
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
	TotalMatched             interface{}
	Records                  []map[string]interface{}
	UniqueKeyCols            []string // Key can be a combination of columns
	AllColumns               []string
	ColumnsOrder             []string
	GroupByCols              []string
	MeasureFunctions         []string
	MeasureResults           []BucketHolder
	Qtype                    string
	BucketCount              int
	DoNotVerifyGroupByValues bool // If true, group by values elements match will not be done. Used when grouping on timestamp.
	VerifyMinimal            bool // If true, only minimal validation will be done
}

const TimeStamp_Col_Name = "timestamp"

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
		return fmt.Errorf("CreateResultForRRC: allColumns not found in response")
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
		return fmt.Errorf("CreateExpResultForGroupBy: bucketCount not found in response")
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
	doNotVerifyGroupByValues, ok := response["doNotVerifyGroupByValues"].(bool)
	if ok {
		res.DoNotVerifyGroupByValues = doNotVerifyGroupByValues
	}

	return CreateResultForStats(res, response)
}

func GetStringValueFromResponse(resp map[string]interface{}, key string) (string, error) {
	value, exist := resp[key]
	if !exist {
		return "", fmt.Errorf("GetStringValueFromResponse: key %v not found in response", key)
	}

	query, isString := value.(string)
	if !isString {
		return "", fmt.Errorf("GetStringValueFromResponse: value %v is not a string, is of type %T", value, value)
	}

	return query, nil
}

func ValidateUniqueKeyColsInResult(res *Result) error {
	if len(res.UniqueKeyCols) == 0 {
		return nil // check records directly in that order
	}
	for _, record := range res.Records {
		for _, col := range res.UniqueKeyCols {
			_, exist := record[col]
			if !exist {
				return fmt.Errorf("ValidateUniqueKeyColsInResult: col %v not found in record: %v, UniqueKeyCols: %v", col, record, res.UniqueKeyCols)
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
		return "", nil, fmt.Errorf("ReadAndValidateQueryFile: expectedResult is not valid, it's of type: %T, file: %v", response["expectedResult"], filePath)
	}

	expRes, err := CreateResult(expectedResult)
	if err != nil {
		return "", nil, fmt.Errorf("ReadAndValidateQueryFile: Error creating expected result, file: %v, err: %v", filePath, err)
	}
	verifyMinimal, exist := expectedResult["verifyMinimal"]
	if exist {
		expRes.VerifyMinimal = verifyMinimal.(bool)
	}
	if expRes.VerifyMinimal {
		return query, expRes, nil
	}

	if expRes.Qtype == "logs-query" {
		uniqueKeyCols, exist := expectedResult["uniqueKeyCols"]
		if exist {
			expRes.UniqueKeyCols, err = CreateListOfString(uniqueKeyCols)
			if err != nil {
				return "", nil, fmt.Errorf("ReadAndValidateQueryFile: Error fetching uniqueKeyCols, uniqueKeyCols: %v file: %v", uniqueKeyCols, filePath)
			}
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

func GetQueryResultForAPI(dest string, queryReq map[string]interface{}, qid int) (*Result, map[string]interface{}, error) {
	reqBody, err := json.Marshal(queryReq)
	if err != nil {
		return nil, nil, fmt.Errorf("EvaluateQueryForAPI: Error marshaling request, reqBody: %v, err: %v", reqBody, err)
	}

	url := fmt.Sprintf("http://%s/api/search", dest)
	req, err := http.NewRequestWithContext(context.TODO(), http.MethodPost, url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, nil, fmt.Errorf("EvaluateQueryForAPI: Error creating request, url: %v, reqBody: %v, err: %v", url, reqBody, err)
	}
	req.Header.Set("content-type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("EvaluateQueryForAPI: Error sending request, req: %v, err: %v", req, err)
	}
	defer resp.Body.Close()

	responseData := make(map[string]interface{})
	if bodyBytes, err := io.ReadAll(resp.Body); err != nil {
		return nil, nil, fmt.Errorf("EvaluateQueryForAPI: Error reading response, resp.Body: %v, err: %v", resp.Body, err)
	} else {
		if err := json.Unmarshal(bodyBytes, &responseData); err != nil {
			return nil, nil, fmt.Errorf("EvaluateQueryForAPI: Error unmarshaling bodyBytes: %v, err: %v", string(bodyBytes), err)
		}
	}

	queryRes, err := CreateResult(responseData)
	if err != nil {
		return nil, nil, fmt.Errorf("EvaluateQueryForAPI: Error creating result, responseData: %v\n err: %v", responseData, err)
	}
	hits, err := getHits(responseData)
	if err != nil {
		return nil, nil, fmt.Errorf("EvaluateQueryForAPI: Error getting hits, responseData: %v\n err: %v", responseData, err)
	}
	err = populateTotalMatchedAndRecords(hits, queryRes)
	if err != nil {
		return nil, nil, fmt.Errorf("EvaluateQueryForAPI: Error populating totalMatched and records, hits: %v, responseData: %v\n err: %v", hits, responseData, err)
	}

	return queryRes, responseData, nil
}

func EvaluateQueryForAPI(dest string, queryReq map[string]interface{}, qid int, expRes *Result) error {
	query := queryReq["searchText"].(string)

	sTime := time.Now()
	queryRes, resp, err := GetQueryResultForAPI(dest, queryReq, qid)
	if err != nil {
		return fmt.Errorf("EvaluateQueryForAPI: Failed getting query result: %v, err: %v", queryReq, err)
	}

	err = CompareResults(queryRes, expRes, query)
	if err != nil {
		if queryRes.Qtype == "logs-query" {
			return fmt.Errorf("EvaluateQueryForAPI: Failed evaluating query: %v, err: %v", query, err)
		} else {
			return fmt.Errorf("EvaluateQueryForAPI: Failed evaluating query: %v, resp: %v, err: %v", query, resp, err)
		}
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

func GetQueryResultForWebSocket(dest string, queryReq map[string]interface{}, qid int) (*Result, map[string]interface{}, error) {
	webSocketURL := fmt.Sprintf("ws://%s/api/search/ws", dest)

	// create websocket connection
	conn, _, err := websocket.DefaultDialer.Dial(webSocketURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("EvaluateQueryForWebSocket: Error connecting to WebSocket server, webSocketURL: %v, err: %v", webSocketURL, err)
	}
	defer conn.Close()

	err = conn.WriteJSON(queryReq)
	if err != nil {
		return nil, nil, fmt.Errorf("EvaluateQueryForWebSocket: Received error from server, queryReq: %v, err: %+v\n", queryReq, err)
	}

	readEvent := make(map[string]interface{})
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
				return nil, nil, fmt.Errorf("EvaluateQueryForWebSocket: Error getting hits, readEvent: %v, err: %v", readEvent, err)
			}
			err = populateRecords(hits, tempRes)
			if err != nil {
				return nil, nil, fmt.Errorf("EvaluateQueryForWebSocket: Error populating records, hits: %v, readEvent: %v, err: %v", hits, readEvent, err)
			}
		case "COMPLETE":
			queryRes, err := CreateResult(readEvent)
			if err != nil {
				return nil, nil, fmt.Errorf("EvaluateQueryForWebSocket: Error creating result, readEvent: %v, err: %v", readEvent, err)
			}
			queryRes.Records = tempRes.Records
			populateTotalMatched(readEvent, queryRes)

			return queryRes, readEvent, nil
		default:
			return nil, nil, fmt.Errorf("EvaluateQueryForWebSocket: Received unknown message from server, readEvent: %+v\n", readEvent)
		}
	}

	return nil, nil, fmt.Errorf("EvaluateQueryForWebSocket: No Response from server")
}

// Evaluates queries and compares the results with expected results for websocket
func EvaluateQueryForWebSocket(dest string, queryReq map[string]interface{}, qid int, expRes *Result) error {
	query := queryReq["searchText"].(string)

	sTime := time.Now()
	queryRes, resp, err := GetQueryResultForWebSocket(dest, queryReq, qid)
	if err != nil {
		return fmt.Errorf("EvaluateQueryForWebSocket: Failed getting query result: %v, err: %v", queryReq, err)
	}

	err = CompareResults(queryRes, expRes, query)
	if err != nil {
		if queryRes.Qtype == "logs-query" {
			return fmt.Errorf("EvaluateQueryForWebSocket: Failed evaluating query: %v, err: %v", query, err)
		} else {
			return fmt.Errorf("EvaluateQueryForWebSocket: Failed evaluating query: %v, resp: %v, err: %v", query, resp, err)
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

	tolerancePercentage := 1e-3

	return utils.AlmostEqual(actual, expValue, tolerancePercentage), nil
}

func ValidateRecord(record map[string]interface{}, expRecord map[string]interface{}) error {
	var err error

	for col, value := range expRecord {
		if col == TimeStamp_Col_Name {
			continue
		}
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
			return fmt.Errorf("ValidateRecord: Value mismatch for column: %v, expected: value=%v, type=%T, got: value=%v, type=%T", col, value, value, actualValue, actualValue)
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

func getPartialRecord(record map[string]interface{}, cols []string) map[string]interface{} {
	filterRecord := make(map[string]interface{})
	for _, col := range cols {
		value, exist := record[col]
		if exist {
			filterRecord[col] = value
		}
	}
	return filterRecord
}

func GetTimestampFromRecord(records []map[string]interface{}) []interface{} {
	timestamps := make([]interface{}, 0)
	for _, record := range records {
		timestamp, exist := record[TimeStamp_Col_Name]
		if !exist {
			timestamp = append(timestamps, nil)
			continue
		}
		timestamps = append(timestamps, timestamp)
	}
	return timestamps
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

	colsToRemove := map[string]struct{}{
		"timestamp": {},
		"_index":    {},
	}

	queryRes.AllColumns = utils.RemoveValues(queryRes.AllColumns, colsToRemove)
	expRes.AllColumns = utils.RemoveValues(expRes.AllColumns, colsToRemove)

	equal := utils.ElementsMatch(queryRes.AllColumns, expRes.AllColumns)
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

	if expRes.VerifyMinimal {
		if len(queryRes.Records) > 0 && queryRes.TotalMatched != nil {
			return nil
		}
		return fmt.Errorf("ValidateLogsQueryResults: No records found for minimal verification")
	}

	equal = reflect.DeepEqual(queryRes.TotalMatched, expRes.TotalMatched)
	if !equal {
		return fmt.Errorf("ValidateLogsQueryResults: TotalMatched mismatch, expected: %v, got: %v", expRes.TotalMatched, queryRes.TotalMatched)
	}

	if len(expRes.UniqueKeyCols) > 0 {
		queryRes.UniqueKeyCols = expRes.UniqueKeyCols
		err = ValidateUniqueKeyColsInResult(queryRes)
		if err != nil {
			return fmt.Errorf("ValidateLogsQueryResults: Error validating UniqueKeyCols: %v in queryRes, err: %v", queryRes.UniqueKeyCols, err)
		}

		sortRecords(queryRes.Records, queryRes.UniqueKeyCols)
		sortRecords(expRes.Records, expRes.UniqueKeyCols)
	}

	for idx, record := range expRes.Records {
		err = ValidateRecord(queryRes.Records[idx], record)
		if err != nil {
			numRecordsToShow := min(10, len(queryRes.Records))
			timestamps := GetTimestampFromRecord(queryRes.Records[:numRecordsToShow])
			return fmt.Errorf("ValidateLogsQueryResults: Error comparing records at index: %v, partial queryRes Record: %v, expRes Record: %v, err: %v, timestamps: %v",
				idx, getPartialRecord(queryRes.Records[idx], utils.GetKeysOfMap(record)), record, err, timestamps)
		}
	}

	return nil
}

func getGroupByCombination(grpByValues []string) string {
	return strings.Join(grpByValues, "_")
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

	equal := utils.ElementsMatch(queryRes.MeasureFunctions, expRes.MeasureFunctions)
	if !equal {
		return fmt.Errorf("ValidateStatsQueryResults: MeasureFunctions mismatch, expected: %+v, got: %+v", expRes.MeasureFunctions, queryRes.MeasureFunctions)
	}

	if expRes.VerifyMinimal {
		if len(queryRes.MeasureResults) == 0 {
			return fmt.Errorf("ValidateStatsQueryResults: No MeasureResults found for minimal verification")
		}
		return nil
	}

	if len(queryRes.MeasureResults) != len(expRes.MeasureResults) {
		return fmt.Errorf("ValidateStatsQueryResults: MeasureResults length mismatch, expected: %v, got: %v", len(expRes.MeasureResults), len(queryRes.MeasureResults))
	}

	// BucketResults are not send in a specific order, so sort them before comparing
	sortMeasureResults(queryRes.MeasureResults)
	sortMeasureResults(expRes.MeasureResults)

	verifyGroupByValues := !expRes.DoNotVerifyGroupByValues

	if !verifyGroupByValues {
		log.Infof("ValidateStatsQueryResults: Skipping GroupByValues Elements match.")
	}

	for idx, expMeasureRes := range expRes.MeasureResults {
		var err error
		queryMeasureRes := queryRes.MeasureResults[idx]

		if verifyGroupByValues {
			equal := reflect.DeepEqual(queryMeasureRes.GroupByValues, expMeasureRes.GroupByValues)
			if !equal {
				return fmt.Errorf("ValidateStatsQueryResults: GroupByCombination mismatch, expected: %+v, got: %+v", expMeasureRes.GroupByValues, queryMeasureRes.GroupByValues)
			}
		} else {
			if len(queryMeasureRes.GroupByValues) != len(expMeasureRes.GroupByValues) {
				return fmt.Errorf("ValidateStatsQueryResults: GroupByValues length mismatch, expected: %v, got: %v", len(expMeasureRes.GroupByValues), len(queryMeasureRes.GroupByValues))
			}
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
					return fmt.Errorf("ValidateStatsQueryResults: Error comparing float values, key=%v, err: %v", key, err)
				}
			} else {
				equal = reflect.DeepEqual(actualValue, value)
			}

			if !equal {
				return fmt.Errorf("ValidateStatsQueryResults: MeasureVal mismatch for key: %v, expected: value=%v, type=%T, got: value=%v, type=%T", key, value, value, actualValue, actualValue)
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
	if !expRes.VerifyMinimal {
		if queryRes.BucketCount != expRes.BucketCount {
			return fmt.Errorf("ValidateGroupByQueryResults: BucketCount mismatch, expected: %v, got: %v", expRes.BucketCount, queryRes.BucketCount)
		}
	} else {
		if queryRes.BucketCount == 0 {
			return fmt.Errorf("ValidateGroupByQueryResults: BucketCount is 0 for minimal verification")
		}
	}

	equal := utils.ElementsMatch(queryRes.GroupByCols, expRes.GroupByCols)
	if !equal {
		return fmt.Errorf("ValidateGroupByQueryResults: GroupByCols mismatch, expected: %+v, got: %+v", expRes.GroupByCols, queryRes.GroupByCols)
	}

	return ValidateStatsQueryResults(queryRes, expRes)
}

func PerfValidateSearchQueryResult(queryRes *Result, fixedColumns []string) error {
	if queryRes == nil {
		return fmt.Errorf("PerfValidateSearchQueryResult: Query result is nil")
	}

	var matched float64
	value, isFloat := queryRes.TotalMatched.(float64)
	if isFloat {
		matched = value
	} else {
		totalMatched, isMap := queryRes.TotalMatched.(map[string]interface{})
		if !isMap {
			return fmt.Errorf("PerfValidateSearchQueryResult: TotalMatched is not a map")
		}
		matched, isFloat = totalMatched["value"].(float64)
		if !isFloat {
			return fmt.Errorf("PerfValidateSearchQueryResult: TotalMatched value is not a float")
		}
	}

	// totalMatched count should be greater than 0
	if matched == 0 {
		return fmt.Errorf("PerfValidateSearchQueryResult: Total matched is 0")
	}

	// Validate if number of records are greater than 0.
	if len(queryRes.Records) == 0 {
		return fmt.Errorf("PerfValidateSearchQueryResult: No records found")
	}

	commonLock.RLock()
	fixedColumns = utils.RemoveValues(fixedColumns, colsToIgnore)
	queryRes.ColumnsOrder = utils.RemoveValues(queryRes.ColumnsOrder, colsToIgnore)
	queryRes.AllColumns = utils.RemoveValues(queryRes.AllColumns, colsToIgnore)
	commonLock.RUnlock()

	sort.Strings(fixedColumns)

	// Validate columns order
	equal := reflect.DeepEqual(fixedColumns, queryRes.ColumnsOrder)
	if !equal {
		return fmt.Errorf("PerfValidateSearchQueryResult: Fixed columns order mismatch, expected: %v got: %v", fixedColumns, queryRes.ColumnsOrder)
	}

	// validate all columns
	equal = utils.ElementsMatch(fixedColumns, queryRes.AllColumns)
	if !equal {
		return fmt.Errorf("PerfValidateSearchQueryResult: Fixed columns mismatch, expected: %v got: %v", fixedColumns, queryRes.AllColumns)
	}

	// validate if all fixed columns have values
	for _, record := range queryRes.Records {
		for _, col := range fixedColumns {
			if _, ok := record[col]; !ok {
				return fmt.Errorf("PerfValidateSearchQueryResult: Fixed column %v not found in record: %v", col, record)
			}
		}
	}

	return nil
}

func ConvertToFloat(value interface{}, removeComma bool) (float64, bool) {
	val := fmt.Sprintf("%v", value)
	if removeComma {
		val = strings.ReplaceAll(val, ",", "") // Stats results return a string that may have commas
	}
	floatVal, err := strconv.ParseFloat(val, 64)
	if err != nil {
		return 0, false
	}
	return floatVal, true
}

// Returns floatValue, isFloat, exist
func GetFloatValueFromMap(data map[string]interface{}, key string) (float64, bool, bool) {
	if val, exist := data[key]; exist {
		floatVal, isFloat := ConvertToFloat(val, true)
		return floatVal, isFloat, true
	}
	return 0, false, false
}

func PerfValidateMeasureResult(measureResult BucketHolder, measureFunc string, statsColValue float64) error {
	measureVal, isNumeric, _ := GetFloatValueFromMap(measureResult.MeasureVal, measureFunc)
	if !isNumeric {
		return fmt.Errorf("PerfValidateMeasureResult: Measure measureFunc: %v value is not a number, measureVal: %v, received type: %T", measureFunc, measureResult.MeasureVal[measureFunc], measureResult.MeasureVal[measureFunc])
	}

	switch measureFunc {
	case "count", "dc":
		if measureVal < 1 {
			return fmt.Errorf("PerfValidateMeasureResult: Measure %v value is less than 1, got: %v", measureFunc, measureVal)
		}
	case "sum":
		countVal, isCountFloat, isCountAvailable := GetFloatValueFromMap(measureResult.MeasureVal, "count")
		if !isCountAvailable {
			return nil // cannot validate
		}
		if !isCountFloat {
			return fmt.Errorf("PerfValidateMeasureResult: sum: Measure count value is not a number, received type: %T", measureResult.MeasureVal["count"])
		}
		minVal, isMinFloat, isMinAvailable := GetFloatValueFromMap(measureResult.MeasureVal, "min")
		if !isMinAvailable {
			return nil // cannot validate
		}
		if !isMinFloat {
			return fmt.Errorf("PerfValidateMeasureResult: sum: Measure min value is not a number, received type: %T", measureResult.MeasureVal["min"])
		}
		if countVal*minVal > measureVal {
			return fmt.Errorf("PerfValidateMeasureResult: Measure sum value is less than count*min, sum: %v, count: %v, min: %v", measureVal, countVal, minVal)
		}
		maxVal, isMaxFloat, isMaxAvailable := GetFloatValueFromMap(measureResult.MeasureVal, "max")
		if !isMaxAvailable {
			return nil
		}
		if !isMaxFloat {
			return fmt.Errorf("PerfValidateMeasureResult: sum: Measure max value is not a number, received type: %T", measureResult.MeasureVal["max"])
		}
		if countVal*maxVal < measureVal {
			return fmt.Errorf("PerfValidateMeasureResult: Measure sum value is greater than count*max, sum: %v, count: %v, max: %v", measureVal, countVal, maxVal)
		}
	case "avg":
		countVal, isCountFloat, isCountAvailable := GetFloatValueFromMap(measureResult.MeasureVal, "count")
		if !isCountAvailable {
			return nil // cannot validate
		}
		if !isCountFloat {
			return fmt.Errorf("PerfValidateMeasureResult: sum: Measure count value is not a number, received type: %T", measureResult.MeasureVal["count"])
		}
		sumVal, isSumFloat, isSumAvailable := GetFloatValueFromMap(measureResult.MeasureVal, "sum")
		if !isSumAvailable {
			return nil // cannot validate
		}
		if !isSumFloat {
			return fmt.Errorf("PerfValidateMeasureResult: sum: Measure sum value is not a number, received type: %T", measureResult.MeasureVal["sum"])
		}
		expectedAvg := sumVal / countVal
		tolerancePercentage := 0.1
		if !utils.AlmostEqual(measureVal, expectedAvg, tolerancePercentage) {
			return fmt.Errorf("PerfValidateMeasureResult: avg mismatch expected: %v, got: %v", expectedAvg, measureVal)
		}
	case "min":
		if measureVal > statsColValue {
			return fmt.Errorf("PerfValidateMeasureResult: Measure min value is greater than stats column value, min: %v, statsColValue: %v", measureVal, statsColValue)
		}
	case "max":
		if measureVal < statsColValue {
			return fmt.Errorf("PerfValidateMeasureResult: Measure %v value is less than stats column value, max: %v, statsColValue: %v", measureFunc, measureVal, statsColValue)
		}
	case "range":
		minVal, isFloat, isMinAvailable := GetFloatValueFromMap(measureResult.MeasureVal, "min")
		if !isMinAvailable {
			// cannot validate range without min value
			return nil
		}
		if !isFloat {
			return fmt.Errorf("PerfValidateMeasureResult: range: Measure min value is not a number, received type: %T", measureResult.MeasureVal["min"])
		}
		maxVal, isFloat, isMaxAvailable := GetFloatValueFromMap(measureResult.MeasureVal, "max")
		if !isMaxAvailable {
			// cannot validate range without max value
			return nil
		}
		if !isFloat {
			return fmt.Errorf("PerfValidateMeasureResult: range: Measure max value is not a number, received type: %T", measureResult.MeasureVal["max"])
		}
		tolerancePercentage := 1e-5
		if !utils.AlmostEqual(measureVal, maxVal-minVal, tolerancePercentage) {
			return fmt.Errorf("PerfValidateMeasureResult: range: Measure range value is not equal to max-min, range: %v, max: %v, min: %v", measureVal, maxVal, minVal)
		}
	}

	return nil
}

func PerfValidateStatsQueryResult(queryRes *Result, measureFuncs []string, statsColValue float64) error {

	if queryRes == nil {
		return fmt.Errorf("Query result is nil")
	}

	// Validate measure functions
	equal := utils.ElementsMatch(measureFuncs, queryRes.MeasureFunctions)
	if !equal {
		return fmt.Errorf("PerfValidateStatsQueryResult: Measure functions mismatch, expected: %v got: %v", measureFuncs, queryRes.MeasureFunctions)
	}

	if len(queryRes.MeasureResults) != 1 {
		return fmt.Errorf("PerfValidateStatsQueryResult: Unexpected number of measure results measure results, expected: 1 got: %v", len(queryRes.MeasureResults))
	}

	measureResult := queryRes.MeasureResults[0]
	for _, measureFunc := range measureFuncs {
		if _, ok := measureResult.MeasureVal[measureFunc]; !ok {
			return fmt.Errorf("PerfValidateStatsQueryResult: Measure function %v not found in measure results", measureFunc)
		}
	}

	for _, measureFunc := range measureFuncs {
		err := PerfValidateMeasureResult(measureResult, measureFunc, statsColValue)
		if err != nil {
			return fmt.Errorf("PerfValidateStatsQueryResult: Error validating measure function %v, measureResult: %v, err: %v", measureFunc, measureResult, err)
		}
	}

	return nil
}

func PerfValidateGroupByQueryResult(queryRes *Result, groupByCols []string, buckets int, measureFuncs []string) error {
	if queryRes == nil {
		return fmt.Errorf("Query result is nil")
	}

	// Validate group by columns
	equal := utils.ElementsMatch(groupByCols, queryRes.GroupByCols)
	if !equal {
		return fmt.Errorf("PerfValidateGroupByQueryResult: Group by columns mismatch, expected: %v got: %v", groupByCols, queryRes.GroupByCols)
	}

	if queryRes.BucketCount != buckets {
		return fmt.Errorf("PerfValidateGroupByQueryResult: Bucket count mismatch, expected: %v got: %v", buckets, queryRes.BucketCount)
	}

	if len(queryRes.MeasureResults) != buckets {
		return fmt.Errorf("PerfValidateGroupByQueryResult: Unexpected number of measure results, expected: %v got: %v", buckets, len(queryRes.MeasureResults))
	}

	for _, measureResult := range queryRes.MeasureResults {
		for _, measureFunc := range measureFuncs {
			if _, ok := measureResult.MeasureVal[measureFunc]; !ok {
				return fmt.Errorf("PerfValidateGroupByQueryResult: Measure function %v not found in MeasureVal: %v", measureFunc, measureResult.MeasureVal)
			}
		}
		for _, measureFunc := range measureFuncs {
			if measureFunc == "min" || measureFunc == "max" {
				// We do not have the specific column value for the group by result so ignore these functions
				continue
			}
			err := PerfValidateMeasureResult(measureResult, measureFunc, 0)
			if err != nil {
				return fmt.Errorf("PerfValidateGroupByQueryResult: Error validating measure function %v, measureResult: %v, err: %v", measureFunc, measureResult, err)
			}
		}
	}

	return nil
}
