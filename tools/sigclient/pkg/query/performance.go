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

// Main function that tests all the queries
func PerformanceTest(ctx context.Context, logCh chan utils.Log) {

	if ctx == nil {
		log.Fatalf("PerformanceTest: ctx or logCh is nil")
	}
	if logCh == nil {
		log.Fatalf("PerformanceTest: logCh is nil")
	}

	for {
		select {
		case <-ctx.Done():
			return			
		default:
			RunPerfQueries(ctx, logCh)
			time.Sleep(5 * time.Second)	
		}
	}
}

func RunPerfQueries(ctx context.Context, logCh chan utils.Log) {
	// Run all the queries
	wg := sync.WaitGroup{}
	for _, query := range Queries {
		wg.Add(1)
		go RunPerfQuery(query, logCh, &wg)
	}
	wg.Wait()
}

// Queries to be run
func RunPerfQuery(query Query, logCh chan utils.Log, wg *sync.WaitGroup) {
	// Run the query
	defer wg.Done()
	// Create and execute query
}