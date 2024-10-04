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
	"encoding/json"
	"fmt"
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

type ExpResult struct {
	TotalMatched	   interface{}
	Records 		   []map[string]interface{}
	AllColumns []string
	ColumnsOrder	   []string
	GroupByCols      []string
	MeasureFunctions   []string
	MeasureResults     []BucketHolder
	Qtype              string
	BucketCount        int
}

func CreateMapList(resp interface{}) []map[string]interface{} {
	items, isList := resp.([]interface{})
	if !isList {
		log.Fatalf("CreateMapList: Records is not a list")
	}

	mapList := make([]map[string]interface{}, 0)
	for _, item := range items {
		mapItem, isMap := item.(map[string]interface{})
		if !isMap {
			log.Fatalf("CreateMapList: Record is not a map")
		}
		mapList = append(mapList, mapItem)
	}
	return mapList
}

func CreateStringList(resp interface{}) []string {
	items, isList := resp.([]interface{})
	if !isList {
		log.Fatalf("CreateStringList: Strings List is not a list")
	}

	stringList := make([]string, 0)
	for _, item := range items {
		strItem, isString := item.(string)
		if !isString {
			log.Fatalf("CreateStringList: Value is not a string")
		}
		stringList = append(stringList, strItem)
	}
	return stringList
}

func CreateExpResult(response map[string]interface{}) *ExpResult {
	expRes := &ExpResult{}
	qtype, exist := response["qtype"]
	if !exist {
		log.Fatalf("queryType not found in response")
	}
	switch qtype {
	case "logs-query":
		expRes.Qtype = "logs-query"
		CreateExpResultForRRC(expRes, response)
	case "segstats-query":
		expRes.Qtype = "segstats-query"
		CreateExpResultForStats(expRes, response)
	case "aggs-query":
		expRes.Qtype = "aggs-query"
		CreateExpResultForGroupBy(expRes, response)
	default:
		log.Fatalf("Invalid query type: %v", qtype)
	}

	return expRes
}

func CreateExpResultForRRC(expRes *ExpResult, response map[string]interface{}) {
	totalMatched, exist := response["totalMatched"]
	if !exist {
		log.Fatalf("totalMatched not found in response")
	}
	expRes.TotalMatched = totalMatched

	records, exist := response["records"]
	if !exist {
		log.Fatalf("records not found in response")
	}

	recordsList := CreateMapList(records)
	expRes.Records = recordsList

	_, exist = response["allColumns"]
	if !exist {
		log.Fatalf("allColumns not found in response")
	}

	expRes.AllColumns = CreateStringList(response["allColumns"])
	expRes.ColumnsOrder = CreateStringList(response["columnsOrder"])
}

func CreateExpResultForStats(expRes *ExpResult, response map[string]interface{}) {
	expRes.MeasureFunctions = CreateStringList(response["measureFunctions"])
	measureRes := CreateMapList(response["measure"])
	for _, measure := range measureRes {
		var isMap bool
		bucket := BucketHolder{}
		bucket.GroupByValues = CreateStringList(measure["GroupByValues"])
		bucket.MeasureVal, isMap = measure["MeasureVal"].(map[string]interface{})
		if !isMap {
			log.Fatalf("CreateExpResultForStats: measureVal is not a map")
		}
		expRes.MeasureResults = append(expRes.MeasureResults, bucket)
	}
}

func CreateExpResultForGroupBy(expRes *ExpResult, response map[string]interface{}) {
	_, exist := response["bucketCount"]
	if !exist {
		log.Fatalf("bucketCount not found in response")
	}
	bucketCount, isFloat := response["bucketCount"].(float64)
	if !isFloat {
		log.Fatalf("CreateExpResultForGroupBy: bucketCount is not numeric")
	}
	expRes.BucketCount = int(bucketCount)
	expRes.GroupByCols = CreateStringList(response["groupByCols"])

	CreateExpResultForStats(expRes, response)
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


func ReadAndValidateQueryFile(filePath string) (string, *ExpResult) {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("ReadQueryFile: Error opening file: %v, err: %v", filePath, err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	response := make(map[string]interface{})
	err = decoder.Decode(&response)
	if err != nil {
		log.Fatalf("ReadQueryFile: Error decoding file: %v, err: %v", filePath, err)
	}

	query, err := GetStringValueFromResponse(response, "queryText")
	if err != nil {
		log.Fatalf("ReadQueryFile: Error getting query from response: %v", err)
	}

	if _, exist := response["expectedResult"]; !exist {
		log.Fatalf("ReadQueryFile: expectedResult not found")
	}

	expectedResult, isMap := response["expectedResult"].(map[string]interface{})
	if !isMap {
		log.Fatalf("ReadQueryFile: expectedResult is not valid it's of type: %T", response["expectedResult"])
	}
	
	expRes := CreateExpResult(expectedResult)
	if expRes.Qtype == "logs-query" && (len(expRes.Records) == 0 || len(expRes.Records) > 10) {
		log.Fatalf("ReadQueryFile: Number of records should be in range 1-10 for logs-query, got: %v", len(expRes.Records))
	}

	return query, expRes
}

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

	// run queries
	for idx, file := range queryFiles {
		filePath := filepath.Join(dataPath, file.Name())
		query, expRes := ReadAndValidateQueryFile(filePath)
		// log.Infof("FunctionalTest: Running query: %v", query)
		// log.Infof("FunctionalTest: Expected Result: %+v", *expRes)
		EvaluateQuery(dest, query, idx, expRes)
	}

	log.Infof("FunctionalTest: All queries passed successfully")
}

func EvaluateQuery(dest string, query string, qid int, expRes *ExpResult) {
	webSocketURL := dest + "/api/search/ws"
	
	// Default values
	startEpoch := "now-90d"
	endEpoch := "now"
	queryLanguage := "Splunk QL"

	// run query
	data := map[string]interface{}{
		"state":         "query",
		"searchText":    query,
		"startEpoch":    startEpoch,
		"endEpoch":      endEpoch,
		"indexName":     "*",
		"queryLanguage": queryLanguage,
	}

	log.Infof("qid=%v, Running query=%v", qid, query)

	// create websocket connection
	conn, _, err := websocket.DefaultDialer.Dial(webSocketURL, nil)
	if err != nil {
		log.Fatalf("RunQueryFromFile: Error connecting to WebSocket server: %v", err)
		return
	}
	defer conn.Close()

	err = conn.WriteJSON(data)
	if err != nil {
		log.Fatalf("Received err message from server: %+v\n", err)
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
			_, exist := readEvent["hits"]
			if exist {
				hits, isMap := readEvent["hits"].(map[string]interface{})
				if !isMap {
					log.Fatalf("EvaluateQuery: hits is not a map")
				}
				readEvent["records"] = hits["records"]
				readEvent["totalMatched"] = hits["totalMatched"]
			}

			queryRes := CreateExpResult(readEvent)
			err = CompareResults(queryRes, expRes, query)
			if err != nil {
				log.Errorf("EvaluateQuery: Failed query: %v, err: %v", query, err)
			}
		default:
			log.Infof("Received unknown message from server: %+v\n", readEvent)
		}
	}
	log.Infof("EvaluateQuery: Query %v was succesful. In %+v", query, time.Since(sTime))
}

func CompareResults(queryRes *ExpResult, expRes *ExpResult, query string) (error) {
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


func ValidateRecord(record map[string]interface{}, expRecord map[string]interface{}) (error) {
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

func ValidateLogsQueryResults(queryRes *ExpResult, expRes *ExpResult) (error) {
	var err error

	equal := utils.ElementsMatch(queryRes.AllColumns, queryRes.AllColumns)
	if !equal {
		return fmt.Errorf("ValidateLogsQueryResults: AllColumns mismatch, expected: %+v, got: %+v", expRes.AllColumns, queryRes.AllColumns)
	}
	colsToRemove := map[string]struct{}{
		"timestamp": {},
		"_index":     {},
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

func ValidateStatsQueryResults(queryRes *ExpResult, expRes *ExpResult) (error) {

	equal := utils.ElementsMatch(queryRes.MeasureFunctions, expRes.MeasureFunctions)
	if !equal {
		return fmt.Errorf("ValidateStatsQueryResults: MeasureFunctions mismatch, expected: %+v, got: %+v", expRes.MeasureFunctions, queryRes.MeasureFunctions)
	}

	sortMeasureResults(queryRes.MeasureResults)
	sortMeasureResults(expRes.MeasureResults)

	if len(queryRes.MeasureResults) != len(expRes.MeasureResults) {
		return fmt.Errorf("ValidateStatsQueryResults: MeasureResults length mismatch, expected: %v, got: %v", len(expRes.MeasureResults), len(queryRes.MeasureResults))
	}

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

func ValidateGroupByQueryResults(queryRes *ExpResult, expRes *ExpResult) (error) {
	if queryRes.BucketCount != expRes.BucketCount {
		return fmt.Errorf("ValidateGroupByQueryResults: BucketCount mismatch, expected: %v, got: %v", expRes.BucketCount, queryRes.BucketCount)
	}

	equal := utils.ElementsMatch(queryRes.GroupByCols, expRes.GroupByCols)
	if !equal {
		return fmt.Errorf("ValidateGroupByQueryResults: GroupByCols mismatch, expected: %+v, got: %+v", expRes.GroupByCols, queryRes.GroupByCols)
	}

	return ValidateStatsQueryResults(queryRes, expRes)
}