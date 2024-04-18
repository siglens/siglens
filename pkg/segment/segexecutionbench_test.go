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

package segment

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"testing"
	"time"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/aggregations"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"

	esquery "github.com/siglens/siglens/pkg/es/query"
)

func Benchmark_ApplyFilterOpAndAggs(b *testing.B) {

	b.ReportAllocs()
	b.ResetTimer()

	config.InitializeDefaultConfig()

	fullTimeRange := &dtu.TimeRange{
		StartEpochMs: 0,
		EndEpochMs:   math.MaxUint64,
	}

	dtype, err := CreateDtypeEnclosure("chrome", 0)
	if err != nil {
		b.Fatal(err)
	}
	_ = &structs.SearchQuery{
		ExpressionFilter: &structs.SearchExpression{
			LeftSearchInput:  &structs.SearchExpressionInput{ColumnName: "browser_name"},
			FilterOp:         Equals,
			RightSearchInput: &structs.SearchExpressionInput{ColumnValue: dtype},
		},
		SearchType: structs.SimpleExpression,
	}

	// matchOSQuery := &SearchQuery{
	//	ExpressionFilter: &SearchExpression{
	//		LeftSearchInput:  &SearchExpressionInput{ColumnName: "os_name"},
	//		FilterOp:         Equals,
	//		RightSearchInput: &SearchExpressionInput{Literal: "MacOS"},
	//	},
	// }

	// filterRefererQuery := &SearchQuery{
	//	ExpressionFilter: &SearchExpression{
	//		LeftSearchInput:  &SearchExpressionInput{ColumnName: "referer_medium"},
	//		FilterOp:         Equals,
	//		RightSearchInput: &SearchExpressionInput{Literal: "google"},
	//	},
	// }

	// filterMobileQuery := &SearchQuery{
	//	ExpressionFilter: &SearchExpression{
	//		LeftSearchInput:  &SearchExpressionInput{ColumnName: "device_is_mobile"},
	//		FilterOp:         Equals,
	//		RightSearchInput: &SearchExpressionInput{Literal: "1"},
	//	},
	// }

	valueFilter := structs.FilterCriteria{
		ExpressionFilter: &structs.ExpressionFilter{
			LeftInput:      &structs.FilterInput{Expression: &structs.Expression{LeftInput: &structs.ExpressionInput{ColumnName: "browser_name"}}},
			FilterOperator: Equals,
			RightInput:     &structs.FilterInput{Expression: &structs.Expression{LeftInput: &structs.ExpressionInput{ColumnValue: dtype}}},
		},
	}
	simpleNode := &structs.ASTNode{
		AndFilterCondition: &structs.Condition{FilterCriteria: []*structs.FilterCriteria{&valueFilter}},
		TimeRange: &dtu.TimeRange{
			StartEpochMs: 0,
			EndEpochMs:   math.MaxUint64,
		},
	}

	agg := &structs.QueryAggregators{
		Sort: &structs.SortRequest{
			ColName:   "timestamp",
			Ascending: true,
		},
		TimeHistogram: &structs.TimeBucket{
			IntervalMillis: 60000,
		},
	}

	fileprefix := "/Users/ssubramanian/Desktop/SigLens/data/blocksum_encoding"

	numSegfiles := 1
	allSerRequests := make(map[string]*structs.SegmentSearchRequest)
	for i := 0; i < numSegfiles; i += 1 {
		segfilename := fmt.Sprintf("%v_%v", fileprefix, i)
		serReq := createSearchReq(segfilename)
		allSerRequests[segfilename] = serReq
	}

	doRespGen := true
	sizeLimit := uint64(100)
	count := 5

	start := time.Now()
	ti := structs.InitTableInfo("test", 0, false)
	qc := structs.QueryContext{
		TableInfo: ti,
	}
	for i := 0; i < count; i++ {
		qItTime := time.Now()
		nodeRes := query.ApplyFilterOperator(simpleNode, fullTimeRange, agg, 0, &qc)
		nodeRes = aggregations.PostQueryBucketCleaning(nodeRes, agg, nil, nil, nil, 1, false)
		if doRespGen {
			esquery.GetQueryResponseJson(nodeRes, "test", qItTime, sizeLimit, 0, agg)
		}
	}

	totalTime := time.Since(start).Seconds()
	avgTime := totalTime / float64(count)
	log.Warnf("Total time=%f. Average time=%f", totalTime, avgTime)

	/*
	   cd pkg/segment
	   go test -run=Bench -bench=Benchmark_ApplyFilterOpAndAggs -cpuprofile cpuprofile.out -o rawsearch_cpu
	   go tool pprof ./rawsearch_cpu cpuprofile.out

	   (for mem profile)
	   go test -run=Bench -bench=Benchmark_ApplyFilterOpAndAggs -benchmem -memprofile memprofile.out -o rawsearch_mem
	   go tool pprof ./rawsearch_mem memprofile.out

	*/
}

func createSearchReq(fileprefix string) *structs.SegmentSearchRequest {
	fd, err := os.OpenFile(structs.GetBsuFnameFromSegKey(fileprefix), os.O_RDONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer fd.Close()
	decoder := json.NewDecoder(fd)
	blockSummaries := make([]*structs.BlockSummary, 0)
	for {
		blockSummary := &structs.BlockSummary{}
		err := decoder.Decode(blockSummary)
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
			break
		}
		blockSummaries = append(blockSummaries, blockSummary)
	}

	searchReq := &structs.SegmentSearchRequest{
		SegmentKey: fileprefix + ".bsg",
		SearchMetadata: &structs.SearchMetadataHolder{
			BlockSummaries: blockSummaries,
		},
	}
	return searchReq
}
