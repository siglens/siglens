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
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"time"

	"verifier/pkg/utils"

	log "github.com/sirupsen/logrus"
)

type Query string

const (
	ComplexSearchQuery Query = "complexsearch"
	StatsQuery = "stats"
	GroupByQuery = "groupby"
	SearchWithAggs = "searchwithaggs"
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

// Main function that tests all the queries
func PerformanceTest(ctx context.Context, logCh chan utils.Log, dest string, concurrentQueries int) {

	if ctx == nil {
		log.Fatalf("PerformanceTest: ctx or logCh is nil")
	}
	if logCh == nil {
		log.Fatalf("PerformanceTest: logCh is nil")
	}

	time.Sleep(10 * time.Second)

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
			time.Sleep(5 * time.Second)
		}
	}
}

func RunPerfQueries(ctx context.Context, logCh chan utils.Log, dest string) {
	// Run all the queries
	for _, query := range Queries {
		select {
		case log := <-logCh:
			switch query {
			case ComplexSearchQuery:
				RunComplexSearchQuery(ctx, log, dest)
			// case StatsQuery:
			// 	RunStatsQuery(dest)
			// case GroupByQuery:
			// 	RunGroupByQuery(dest)
			// case SearchWithAggs:
			// 	RunSearchWithAggs(dest)
			}
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

func GetOp() string {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	if rng.Intn(2) == 0 {
		return "AND"
	}
	return "OR"
}

func GetRandomNumber(max int) int {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	return rng.Intn(max+1)
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

func RunComplexSearchQuery(ctx context.Context, tslog utils.Log, dest string) {
	if wait := tslog.Timestamp.Sub(time.Now()); wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return
		}
	}

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

	randomNum := GetRandomNumber(10)
	fieldCols := GetRandomKeys(tslog.Data, randomNum)
	fieldCols = append(fieldCols, col, col2)

	// Construct the query
	query := fmt.Sprintf(`%v | %v | %v`, GetEqualClause(col, val), GetRegexClause(col2, val2), GetFieldsClause(fieldCols))

	// Run the query
	log.Infof("Running complex search query %v", query)
}