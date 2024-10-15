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
	"context"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"verifier/pkg/utils"

	log "github.com/sirupsen/logrus"
)

type Query string

const (
	ComplexSearchQuery Query = "complexsearch"
	StatsQuery               = "stats"
	GroupByQuery             = "groupby"
)

var Queries = []Query{
	ComplexSearchQuery,
	StatsQuery,
	GroupByQuery,
}

var pattern = `[^a-zA-Z0-9]`

var globalQid = int64(0)

// Compile the regular expression
var patternRegex = regexp.MustCompile(pattern)

const WAIT_DURATION_FOR_LOGS = 15 * time.Second

var colsToIgnore = map[string]struct{}{
	"_index":                       struct{}{},
	"account":                      struct{}{},
	"account_status":               struct{}{},
	"account.balance":              struct{}{},
	"account.created_data.country": struct{}{},
	"account.created_data.date":    struct{}{},
	"account.currency.long":        struct{}{},
	"account.currency.short":       struct{}{},
	"account.number":               struct{}{},
	"account.type":                 struct{}{},
	"timestamp":                    struct{}{},
}

var lowCardCols = map[string]int{
	"gender":      2,
	"bool_col":    2,
	"http_method": 6,
}

var commonLock = &sync.RWMutex{}

func GetQid() int64 {
	return atomic.AddInt64(&globalQid, 1)
}

// Main function that tests all the queries
func PerformanceTest(ctx context.Context, logCh chan utils.Log, dest string, concurrentQueries int, variableColNames []string) {

	if ctx == nil {
		log.Fatalf("PerformanceTest: ctx or logCh is nil")
	}
	if logCh == nil {
		log.Fatalf("PerformanceTest: logCh is nil")
	}

	commonLock.Lock()
	for _, col := range variableColNames {
		colsToIgnore[col] = struct{}{}
	}
	commonLock.Unlock()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			wg := sync.WaitGroup{}
			for i := 0; i < concurrentQueries; i++ {
				wg.Add(1)
				go func() {
					RunPerfQueries(ctx, logCh, dest)
					wg.Done()
				}()
			}
			wg.Wait()
			time.Sleep(time.Duration(rand.Intn(100)+50) * time.Millisecond)
		}
	}
}

func RunPerfQueries(ctx context.Context, logCh chan utils.Log, dest string) {

	// Run all the queries _, query := range Queries
	for _, query := range Queries {
		var err error
		select {
		case logReceived := <-logCh:
			if wait := logReceived.Timestamp.Add(WAIT_DURATION_FOR_LOGS).Sub(time.Now()); wait > 0 {
				log.Warnf("Waiting for %v", wait)
				select {
				case <-time.After(wait):
				case <-ctx.Done():
					return
				}
			}
			qid := GetQid()
			switch query {
			case ComplexSearchQuery:
				err = RunComplexSearchQuery(logReceived, dest, int(qid))
			case StatsQuery:
				err = RunStatsQuery(logReceived, dest, int(qid))
			case GroupByQuery:
				err = RunGroupByQuery(logReceived, dest, int(qid))
			}
			if err != nil {
				log.Errorf("RunPerfQueries: qid=%v, Error running query: %v, err: %v", qid, query, err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func GetStringColAndVal(data map[string]interface{}) (string, string) {
	for k, v := range data {
		strVal, isString := v.(string)
		if !isString {
			continue
		}
		_, err := strconv.ParseFloat(strVal, 64)
		if err == nil {
			continue // skip floats/numbers
		}
		if patternRegex.MatchString(strVal) {
			continue
		}
		return k, strVal
	}
	return "", ""
}

func GetNumericColAndVal(data map[string]interface{}) (string, string) {
	for k, v := range data {
		_, isString := v.(string)
		if isString {
			continue
		}
		strValue := fmt.Sprintf("%v", v)
		floatVal, err := strconv.ParseFloat(strValue, 64)
		if err == nil {
			if floatVal == math.Floor(floatVal) {
				return k, fmt.Sprintf(`%v`, int(floatVal))
			}
			return k, fmt.Sprintf(`%f`, floatVal)
		}
	}
	return "", ""
}

func GetOp() string {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	if rng.Intn(2) == 0 {
		return "AND"
	}
	return "OR"
}

func GetRandomNumber(max int) int {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return rng.Intn(max + 1)
}

func GetEqualClause(col string, val string) string {
	return col + "=" + val
}

func GetRegexClause(col string, val string) string {
	return fmt.Sprintf(`regex %v="^%v$"`, col, val)
}

func GetFieldsClause(fields []string) string {
	return "fields " + strings.Join(fields, ", ")
}

func GetRandomKeys(data map[string]interface{}, numKeys int) []string {
	keys := make([]string, 0, numKeys)
	for k := range data {
		keys = append(keys, k)
	}
	return keys
}

func GetQueryRequest(query string, startEpoch interface{}, endEpoch interface{}) map[string]interface{} {
	return map[string]interface{}{
		"state":         "query",
		"searchText":    query,
		"startEpoch":    startEpoch,
		"endEpoch":      endEpoch,
		"indexName":     "*",
		"queryLanguage": "Splunk QL",
	}
}

func BuildStatsQuery(measureFuncs []string) (string, string) {
	// http_status, latency, longitude, latitude
	defaultStatsFields := []string{"http_status", "latency", "longitude", "latitude"}

	statsCol := defaultStatsFields[GetRandomNumber(len(defaultStatsFields)-1)]

	base := "* | stats "
	for i, measureFunc := range measureFuncs {
		base += measureFunc + "(" + statsCol + ") as " + measureFunc
		if i != len(measureFuncs)-1 {
			base += ", "
		}
	}

	return base, statsCol
}

func RunStatsQuery(tslog utils.Log, dest string, qid int) error {

	measureFuncs := []string{"avg", "sum", "min", "max", "count", "range", "dc"}

	query, statsCol := BuildStatsQuery(measureFuncs)

	statsColValue, exist := tslog.Data[statsCol]
	if !exist {
		return fmt.Errorf("RunStatsQuery: Stats column %v not found in log", statsCol)
	}
	floatStatsColValue, isNumeric := ConvertToFloat(statsColValue, false)
	if !isNumeric {
		return fmt.Errorf("RunStatsQuery: Stats column %v is not numeric", statsCol)
	}

	// Default values
	startTime := tslog.Timestamp.Add(-1 * time.Minute)
	endTime := time.Now()

	// run query
	queryReq := GetQueryRequest(query, startTime.UnixMilli(), endTime.UnixMilli())

	log.Infof("RunQuery: qid=%v, Running query: %v", qid, query)
	queryRes, resp, err := GetQueryResultForWebSocket(dest, queryReq, 1)
	if err != nil {
		return fmt.Errorf("RunStatsQuery: Error running query via websocket: %v, err: %v", query, err)
	}

	// Validate the query result
	err = PerfValidateStatsQueryResult(queryRes, measureFuncs, floatStatsColValue)
	if err != nil {
		return fmt.Errorf("RunStatsQuery: Error validating query for websocket: %v, resp: %v\n err: %v", query, resp, err)
	}

	queryRes, resp, err = GetQueryResultForAPI(dest, queryReq, 1)
	if err != nil {
		return fmt.Errorf("RunStatsQuery: Error running query via API: %v, err: %v", query, err)
	}

	// Validate the query result
	err = PerfValidateStatsQueryResult(queryRes, measureFuncs, floatStatsColValue)
	if err != nil {
		return fmt.Errorf("RunStatsQuery: Error validating query for API: %v, resp: %v\n err: %v", query, resp, err)
	}

	log.Infof("RunStatsQuery: qid=%v, Query: %v passed successfully", qid, query)
	return nil
}

func RunComplexSearchQuery(tslog utils.Log, dest string, qid int) error {
	// Get the string column and value
	strCol1, strVal1 := GetStringColAndVal(tslog.Data)
	if strCol1 == "" || strVal1 == "" {
		return fmt.Errorf("RunComplexSearchQuery: No string column and value found in log")
	}
	delete(tslog.Data, strCol1)

	strCol2, strVal2 := GetStringColAndVal(tslog.Data)
	if strCol2 == "" || strVal2 == "" {
		return fmt.Errorf("RunComplexSearchQuery: No second string column and value found in log")
	}
	delete(tslog.Data, strCol2)

	numCol1, numVal1 := GetNumericColAndVal(tslog.Data)
	if numCol1 == "" || numVal1 == "" {
		return fmt.Errorf("RunComplexSearchQuery: No numeric column and value found in log")
	}

	// Construct the query: strCol1=strVal1 AND/OR numCol1=numVal1 | regex strCol2=strVal2
	query := fmt.Sprintf(`%v %v %v | %v`, GetEqualClause(strCol1, fmt.Sprintf(`"%v"`, strVal1)), GetOp(), GetEqualClause(numCol1, numVal1), GetRegexClause(strCol2, strVal2))

	// Default values
	startTime := tslog.Timestamp.Add(-2 * time.Minute)
	endTime := time.Now()

	queryReq := GetQueryRequest(query, startTime.UnixMilli(), endTime.UnixMilli())

	log.Infof("RunComplexSearchQuery: qid=%v, Running query: %v got: %v start: %v end: %v", qid, query, tslog.Timestamp, startTime, endTime)
	queryRes, resp, err := GetQueryResultForWebSocket(dest, queryReq, qid)
	if err != nil {
		return fmt.Errorf("RunComplexSearchQuery: Error running query via websocket: %v, err: %v", query, err)
	}

	// Validate the query result
	err = PerfValidateSearchQueryResult(queryRes, tslog.AllFixedColumns)
	if err != nil {
		return fmt.Errorf("RunComplexSearchQuery: Error validating query for websocket: %v, resp: %v\n err: %v", query, resp, err)
	}

	queryRes, resp, err = GetQueryResultForAPI(dest, queryReq, qid)
	if err != nil {
		return fmt.Errorf("RunComplexSearchQuery: Error running query via API: %v, err: %v", query, err)
	}

	// Validate the query result
	err = PerfValidateSearchQueryResult(queryRes, tslog.AllFixedColumns)
	if err != nil {
		return fmt.Errorf("RunComplexSearchQuery: Error validating query for API: %v, resp: %v\n err: %v", query, resp, err)
	}

	log.Infof("RunComplexSearchQuery: qid=%v, Query: %v passed successfully", qid, query)
	return nil
}

func GetRandomLowCardCol() (string, int) {
	commonLock.RLock()
	randomNum := GetRandomNumber(len(lowCardCols) - 1)
	i := 0
	for k, v := range lowCardCols {
		if i == randomNum {
			commonLock.RUnlock()
			return k, v
		}
		i++
	}
	commonLock.RUnlock()
	return "", 0
}

func RunGroupByQuery(tslog utils.Log, dest string, qid int) error {
	measureFuncs := []string{"avg", "sum", "min", "max", "count", "range", "dc"}

	grpByCol, card := GetRandomLowCardCol()
	if grpByCol == "" {
		return fmt.Errorf("RunGroupByQuery: No low cardinality column found")
	}

	query, _ := BuildStatsQuery(measureFuncs)

	query += fmt.Sprintf(` by %v`, grpByCol)

	// Default values
	startTime := tslog.Timestamp.Add(-3 * time.Minute)
	endTime := time.Now()

	queryReq := GetQueryRequest(query, startTime.UnixMilli(), endTime.UnixMilli())

	log.Infof("RunGroupByQuery: qid=%v, Running query: %v", qid, query)
	queryRes, resp, err := GetQueryResultForWebSocket(dest, queryReq, qid)
	if err != nil {
		return fmt.Errorf("RunGroupByQuery: Error running query via websocket: %v, err: %v", query, err)
	}

	// Validate the query result
	err = PerfValidateGroupByQueryResult(queryRes, []string{grpByCol}, card, measureFuncs)
	if err != nil {
		return fmt.Errorf("RunGroupByQuery: Error validating query for websocket:%v, resp: %v\n err: %v", query, resp, err)
	}

	queryRes, resp, err = GetQueryResultForAPI(dest, queryReq, qid)
	if err != nil {
		return fmt.Errorf("RunGroupByQuery: Error running query via API: %v, err: %v", query, err)
	}

	// Validate the query result
	err = PerfValidateGroupByQueryResult(queryRes, []string{grpByCol}, card, measureFuncs)
	if err != nil {
		return fmt.Errorf("RunGroupByQuery: Error validating query for API: %v, resp: %v\n err: %v", query, resp, err)
	}

	log.Infof("RunGroupByQuery: qid=%v, Query: %v passed successfully", qid, query)

	return nil
}
