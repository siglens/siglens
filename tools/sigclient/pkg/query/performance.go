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
	"time"

	"verifier/pkg/utils"

	log "github.com/sirupsen/logrus"
)

type Query string

const (
	ComplexSearchQuery Query = "complexsearch"
	StatsQuery               = "stats"
	GroupByQuery             = "groupby"
	SearchWithAggs           = "searchwithaggs"
)

var Queries = []Query{
	ComplexSearchQuery,
	StatsQuery,
	GroupByQuery,
	SearchWithAggs,
}

var pattern = `[^a-zA-Z0-9]`

// Compile the regular expression
var patternRegex = regexp.MustCompile(pattern)

const WAIT_DURATION_FOR_LOGS = 30 * time.Second

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

// Main function that tests all the queries
func PerformanceTest(ctx context.Context, logCh chan utils.Log, dest string, concurrentQueries int, variableColNames []string) {

	if ctx == nil {
		log.Fatalf("PerformanceTest: ctx or logCh is nil")
	}
	if logCh == nil {
		log.Fatalf("PerformanceTest: logCh is nil")
	}

	for _, col := range variableColNames {
		colsToIgnore[col] = struct{}{}
	}

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
		}
	}
}

func RunPerfQueries(ctx context.Context, logCh chan utils.Log, dest string) {

	// Run all the queries _, query := range Queries
	var err error
	for {
		select {
		case logReceived := <-logCh:
			if wait := logReceived.Timestamp.Add(WAIT_DURATION_FOR_LOGS).Sub(time.Now()); wait > 0 {
				log.Errorf("Waiting for %v", wait)
				select {
				case <-time.After(wait):
				case <-ctx.Done():
					return
				}
			}
			// switch query {
			// case ComplexSearchQuery:
			// 	RunComplexSearchQuery(logReceived, dest)

			// case StatsQuery:
			err = RunStatsQuery(logReceived, dest)
			if err != nil {
				log.Errorf("Error running Stats query: %v", err)
			}
			return
			// case GroupByQuery:
			// 	RunGroupByQuery(dest)
			// case SearchWithAggs:
			// 	RunSearchWithAggs(dest)
			// }
		case <-ctx.Done():
			return
		}
	}
}

func GetStringColAndVal(data map[string]interface{}) (string, string) {
	for k, v := range data {
		_, isString := v.(string)
		if isString {
			if patternRegex.MatchString(v.(string)) {
				continue
			}
			return k, v.(string)
		}
	}
	return "", ""
}

func GetNumericColAndVal(data map[string]interface{}) (string, string) {
	for k, v := range data {
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

func BuildStatsQuery(field string, measureFuncs []string) string {
	base := "* | stats "
	for i, measureFunc := range measureFuncs {
		base += measureFunc + "(" + field + ") as " + measureFunc
		if i != len(measureFuncs)-1 {
			base += ", "
		}
	}
	return base
}

func RunStatsQuery(tslog utils.Log, dest string) error {

	// http_status, latency, longitude, latitude
	defaultStatsFields := []string{"http_status", "latency", "longitude", "latitude"}

	statsCol := defaultStatsFields[GetRandomNumber(len(defaultStatsFields)-1)]
	measureFuncs := []string{"avg", "sum", "min", "max", "count", "range", "dc"}

	query := BuildStatsQuery(statsCol, measureFuncs)

	statsColValue, exist := tslog.Data[statsCol]
	if !exist {
		return fmt.Errorf("Stats column %v not found in log", statsCol)
	}
	floatStatsColValue, isNumeric := ConvertToFloat(statsColValue, false)
	if !isNumeric {
		return fmt.Errorf("Stats column %v is not numeric", statsCol)
	}

	// Default values
	startTime := tslog.Timestamp.Add(-1 * time.Minute)
	endTime := time.Now()
	queryLanguage := "Splunk QL"

	// run query
	queryReq := map[string]interface{}{
		"state":         "query",
		"searchText":    query,
		"startEpoch":    startTime.UnixMilli(),
		"endEpoch":      endTime.UnixMilli(),
		"indexName":     "*",
		"queryLanguage": queryLanguage,
	}

	log.Infof("RunQuery: qid=%v, Running query: %v got: %v start: %v, end: %v", 1, query, tslog.Timestamp, startTime, endTime)
	// queryRes, err := GetQueryResultForWebSocket(dest, queryReq, 1)
	// if err != nil {
	// 	return fmt.Errorf("Error running query: %v", err)
	// }
	// log.Warnf("Query result: %v", queryRes)

	// // Validate the query result
	// err = PerfValidateStatsQueryResult(queryRes, measureFuncs, floatStatsColValue)
	// if err != nil {
	// 	return fmt.Errorf("Error validating query for Websocket Response: %v, err: %v", query, err)
	// }

	queryRes, err := GetQueryResultForAPI(dest, queryReq, 1)
	if err != nil {
		return fmt.Errorf("Error running query: %v", err)
	}

	// Validate the query result
	err = PerfValidateStatsQueryResult(queryRes, measureFuncs, floatStatsColValue)
	if err != nil {
		return fmt.Errorf("Error validating query for API response: %v, err: %v", query, err)
	}

	log.Infof("Query: %v passed successfully", query)
	return nil
}

func RunComplexSearchQuery(tslog utils.Log, dest string) {
	// Get the string column and value
	col, val := GetStringColAndVal(tslog.Data)
	if col == "" || val == "" {
		log.Warnf("No string column and value found in log")
		return
	}
	delete(tslog.Data, col)

	col2, val2 := GetStringColAndVal(tslog.Data)
	if col2 == "" || val2 == "" {
		log.Warnf("No second string column and value found in log")
		return
	}
	delete(tslog.Data, col2)

	col3, val3 := GetNumericColAndVal(tslog.Data)
	if col3 == "" || val3 == "" {
		log.Warnf("No numeric column and value found in log")
		return
	}

	// Construct the query
	query := fmt.Sprintf(`%v %v %v | %v`, GetEqualClause(col, val), GetOp(), GetEqualClause(col3, val3), GetRegexClause(col2, val2))

	// Default values
	startTime := tslog.Timestamp.Add(-1 * time.Minute)
	endTime := time.Now()
	queryLanguage := "Splunk QL"

	// run query
	queryReq := map[string]interface{}{
		"state":         "query",
		"searchText":    "",
		"startEpoch":    startTime.UnixMilli(),
		"endEpoch":      endTime.UnixMilli(),
		"indexName":     "*",
		"queryLanguage": queryLanguage,
	}

	log.Infof("RunQuery: qid=%v, Running query: %v got: %v start: %v, end: %v", 1, query, tslog.Timestamp, startTime, endTime)
	queryRes, err := GetQueryResultForWebSocket(dest, queryReq, 1)
	if err != nil {
		log.Fatalf("Error running query: %v", err)
	}

	// Validate the query result
	err = PerfValidateSearchQueryResult(queryRes, tslog.AllFixedColumns)
	if err != nil {
		log.Errorf("Error validating query for Websocket Response: %v, err: %v", query, err)
	}

	queryRes, err = GetQueryResultForAPI(dest, queryReq, 1)
	if err != nil {
		log.Fatalf("Error running query: %v", err)
	}

	// Validate the query result
	err = PerfValidateSearchQueryResult(queryRes, tslog.AllFixedColumns)
	if err != nil {
		log.Errorf("Error validating query for API response: %v, err: %v", query, err)
	}

	log.Infof("Query: %v passed successfully", query)
}
