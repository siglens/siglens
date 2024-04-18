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

package bench

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/siglens/siglens/pkg/blob"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/common/fileutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/reader/microreader"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	serverutils "github.com/siglens/siglens/pkg/server/utils"
	"github.com/valyala/fastrand"

	localstorage "github.com/siglens/siglens/pkg/blob/local"
	esquery "github.com/siglens/siglens/pkg/es/query"
	eswriter "github.com/siglens/siglens/pkg/es/writer"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

var json = jsoniter.ConfigFastest

var loadDataBytes0 = []byte(`{"index" : { "_index" : "test0"} }
{"event_id": "f533f3d4-a521-4067-b59b-628bcf8fba62", "timestamp": 1628862769706, "eventType": "pageview", "page_url": "http://www.henry.info/blog/explore/homepage/", "page_url_path": "http://www.johnson.com/", "referer_url": "https://mccall-chavez.com/", "referer_url_scheme": "HEAD", "referer_url_port": 47012, "referer_medium": "bing", "utm_medium": "Beat.", "utm_source": "Edge politics.", "utm_content": "Fly.", "utm_campaign": "Development green.", "click_id": "c21ff7e1-2d96-4b21-8415-9b69f882a593", "geo_latitude": "51.42708", "geo_longitude": "-0.91979", "geo_country": "GB", "geo_timezone": "Europe/London", "geo_region_name": "Lower Earley", "ip_address": "198.13.58.1", "browser_name": "chrome", "browser_user_agent": "Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_10_4 rv:5.0; iu-CA) AppleWebKit/532.43.2 (KHTML, like Gecko) Version/5.0 Safari/532.43.2", "browser_language": "Part.", "os": "Linux", "os_name": "MacOS", "os_timezone": "Europe/Berlin", "device_type": "hardware", "device_is_mobile": true, "user_custom_id": "petersmichaela@hotmail.com", "user_domain_id": "c8aad4b3-0097-430e-8c74-3a2becbd41f9"}
`)
var loadDataBytes1 = []byte(`{"index" : { "_index" : "test1"} }
{"event_id": "f533f3d4-a521-4067-b59b-628bcf8fba62", "timestamp": 1628862769706, "eventType": "pageview", "page_url": "http://www.henry.info/blog/explore/homepage/", "page_url_path": "http://www.johnson.com/", "referer_url": "https://mccall-chavez.com/", "referer_url_scheme": "HEAD", "referer_url_port": 47012, "referer_medium": "bing", "utm_medium": "Beat.", "utm_source": "Edge politics.", "utm_content": "Fly.", "utm_campaign": "Development green.", "click_id": "c21ff7e1-2d96-4b21-8415-9b69f882a593", "geo_latitude": "51.42708", "geo_longitude": "-0.91979", "geo_country": "GB", "geo_timezone": "Europe/London", "geo_region_name": "Lower Earley", "ip_address": "198.13.58.1", "browser_name": "chrome", "browser_user_agent": "Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_10_4 rv:5.0; iu-CA) AppleWebKit/532.43.2 (KHTML, like Gecko) Version/5.0 Safari/532.43.2", "browser_language": "Part.", "os": "Linux", "os_name": "MacOS", "os_timezone": "Europe/Berlin", "device_type": "hardware", "device_is_mobile": true, "user_custom_id": "petersmichaela@hotmail.com", "user_domain_id": "c8aad4b3-0097-430e-8c74-3a2becbd41f9"}
`)

var allData = [][]byte{loadDataBytes0, loadDataBytes1}

func getMyIds() []uint64 {
	myids := make([]uint64, 1)
	myids[0] = 0
	return myids
}

func Benchmark_EndToEnd(b *testing.B) {
	config.InitializeTestingConfig()
	_ = localstorage.InitLocalStorage()
	currTime := utils.GetCurrentTimeMillis()
	startTime := uint64(0)
	tRange := &dtu.TimeRange{
		StartEpochMs: startTime,
		EndEpochMs:   currTime,
	}
	sizeLimit := uint64(100)

	smbasedir := "/Users/ssubramanian/Desktop/SigLens/siglens/data/ingestnodes/Sris-MBP.lan/smr/"
	config.SetSmrBaseDirForTestOnly(smbasedir)

	err := query.InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	if err != nil {
		b.Fatalf("Failed to initialize query node: %v", err)
	}
	colVal, err := utils.CreateDtypeEnclosure("batch-101", 1)
	// colVal, err := utils.CreateDtypeEnclosure("*", 1)
	if err != nil {
		log.Fatal(err)
	}
	valueFilter := structs.FilterCriteria{
		ExpressionFilter: &structs.ExpressionFilter{
			LeftInput:      &structs.FilterInput{Expression: &structs.Expression{LeftInput: &structs.ExpressionInput{ColumnName: "*"}}},
			FilterOperator: utils.Equals,
			RightInput:     &structs.FilterInput{Expression: &structs.Expression{LeftInput: &structs.ExpressionInput{ColumnValue: colVal}}},
		},
	}
	queryNode := &structs.ASTNode{
		AndFilterCondition: &structs.Condition{FilterCriteria: []*structs.FilterCriteria{&valueFilter}},
		TimeRange:          tRange,
	}
	if err != nil {
		log.Errorf("Benchmark_LoadMicroIndices: failed to load microindex,err=%v", err)
	}
	count := 10
	allTimes := make([]time.Duration, count)
	timeSum := float64(0)
	twoMins := 2 * 60 * 1000

	simpleValueHistogram := &structs.QueryAggregators{
		TimeHistogram: &structs.TimeBucket{
			StartTime:      tRange.StartEpochMs,
			EndTime:        tRange.EndEpochMs,
			IntervalMillis: uint64(twoMins),
			AggName:        "testValue",
		},
		Sort: &structs.SortRequest{
			ColName:   "timestamp",
			Ascending: false,
		},
	}
	qc := structs.InitQueryContext("ind-v1", sizeLimit, 0, 0, false)
	b.ResetTimer()
	for i := 0; i < count; i++ {
		sTime := time.Now()
		res := segment.ExecuteQuery(queryNode, simpleValueHistogram, uint64(i), qc)
		log.Infof("query %v result has %v total matches", i, res.TotalResults)
		esquery.GetQueryResponseJson(res, "ind-v1", sTime, sizeLimit, uint64(i), simpleValueHistogram)
		elapTime := time.Since(sTime)
		allTimes[i] = elapTime
		if i != 0 {
			timeSum += elapTime.Seconds()
		}
	}
	log.Infof("Finished benchmark: allTimes: %v", allTimes)
	log.Infof("Average time: %v", timeSum/float64(count-1))

	/*
	   go test -run=Bench -bench=Benchmark_EndToEnd  -cpuprofile cpuprofile.out -o rawsearch_cpu
	   go tool pprof ./rawsearch_cpu cpuprofile.out

	   (for mem profile)
	   go test -run=Bench -bench=Benchmark_EndToEnd -benchmem -memprofile memprofile.out -o rawsearch_mem
	   go tool pprof ./rawsearch_mem memprofile.out

	*/

}

func Benchmark_RRCToJson(b *testing.B) {
	config.InitializeTestingConfig()
	_ = localstorage.InitLocalStorage()
	currTime := utils.GetCurrentTimeMillis()
	startTime := uint64(0)
	tRange := &dtu.TimeRange{
		StartEpochMs: startTime,
		EndEpochMs:   currTime,
	}
	sizeLimit := uint64(100)

	smbasedir := "/Users/knawale/code/perf/siglens/data/ingestnodes/kunals-imac.lan/smr/"
	config.SetSmrBaseDirForTestOnly(smbasedir)

	err := query.InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	if err != nil {
		b.Fatalf("Failed to initialize query node: %v", err)
	}
	colVal, err := utils.CreateDtypeEnclosure("batch-101", 1)
	// colVal, err := utils.CreateDtypeEnclosure("*", 1)
	if err != nil {
		log.Fatal(err)
	}
	valueFilter := structs.FilterCriteria{
		ExpressionFilter: &structs.ExpressionFilter{
			LeftInput:      &structs.FilterInput{Expression: &structs.Expression{LeftInput: &structs.ExpressionInput{ColumnName: "*"}}},
			FilterOperator: utils.Equals,
			RightInput:     &structs.FilterInput{Expression: &structs.Expression{LeftInput: &structs.ExpressionInput{ColumnValue: colVal}}},
		},
	}
	queryNode := &structs.ASTNode{
		AndFilterCondition: &structs.Condition{FilterCriteria: []*structs.FilterCriteria{&valueFilter}},
		TimeRange:          tRange,
	}
	if err != nil {
		log.Errorf("Benchmark_LoadMicroIndices: failed to load microindex, err=%v", err)
	}
	count := 10
	allTimes := make([]time.Duration, count)
	timeSum := float64(0)
	twoMins := 2 * 60 * 1000

	simpleValueHistogram := &structs.QueryAggregators{
		TimeHistogram: &structs.TimeBucket{
			StartTime:      tRange.StartEpochMs,
			EndTime:        tRange.EndEpochMs,
			IntervalMillis: uint64(twoMins),
			AggName:        "testValue",
		},
		Sort: &structs.SortRequest{
			ColName:   "timestamp",
			Ascending: false,
		},
	}
	qc := structs.InitQueryContext("ind-v1", sizeLimit, 0, 0, false)
	res := segment.ExecuteQuery(queryNode, simpleValueHistogram, uint64(0), qc)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < count; i++ {
		sTime := time.Now()
		log.Infof("query %v result has %v total matches", i, res.TotalResults)
		esquery.GetQueryResponseJson(res, "ind-v1", sTime, sizeLimit, uint64(i), simpleValueHistogram)
		elapTime := time.Since(sTime)
		allTimes[i] = elapTime
		if i != 0 {
			timeSum += elapTime.Seconds()
		}
	}
	log.Infof("Finished benchmark: allTimes: %v", allTimes)
	log.Infof("Average time: %v", timeSum/float64(count-1))

	/*
	   go test -run=Bench -bench=Benchmark_RRCToJson  -cpuprofile cpuprofile.out -o rawsearch_cpu
	   go tool pprof ./rawsearch_cpu cpuprofile.out

	   (for mem profile)
	   go test -run=Bench -bench=Benchmark_RRCToJson -benchmem -memprofile memprofile.out -o rawsearch_mem
	   go tool pprof ./rawsearch_mem memprofile.out

	*/

}

func processKibanaIngestRequest(ctx *fasthttp.RequestCtx, request map[string]interface{},
	indexNameConverted string, updateArg bool, idVal string, tsNow uint64, myid uint64) error {
	return nil
}

func Benchmark_esBulkIngest(b *testing.B) {
	config.InitializeDefaultConfig()
	_ = vtable.InitVTable()

	var bulkData []byte

	for i := 0; i < 1000; i += 1 {
		idx := i % len(allData)
		bulkData = append(bulkData, allData[idx]...)
	}

	start := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	b.SetParallelism(2)
	for i := 0; i < b.N; i++ {
		processedCount, response, err := eswriter.HandleBulkBody(bulkData, nil, 0, processKibanaIngestRequest)
		if err != nil {
			log.Errorf("ERROR: err=%v", err)
			break
		}
		if processedCount == 0 {
			log.Errorf("ERROR: processedCount was 0, resp=%v", response)
			break
		}
	}
	totalTime := time.Since(start).Seconds()
	log.Warnf("Total time=%f", totalTime)

	/*
	   go test -run=Bench -bench=Benchmark_esBulkIngest -cpuprofile cpuprofile.out -o rawsearch_cpu
	   go tool pprof ./rawsearch_cpu cpuprofile.out

	   (for mem profile)
	   go test -run=Bench -bench=Benchmark_esBulkIngest -benchmem -memprofile memprofile.out -o rawsearch_mem
	   go tool pprof ./rawsearch_mem memprofile.out

	*/

	os.RemoveAll(config.GetDataPath())
}

// helper benchmark test to debug & read files using a path w/o having to start server up
// go test -run=Bench -bench=Benchmark_logSummarySegKey
func Benchmark_logSummarySegKey(b *testing.B) {
	config.InitializeDefaultConfig()

	segKey := "a"

	sidName := segKey + ".sid"
	segInfo, err := microreader.ReadSegMeta(sidName)
	if err != nil {
		b.Fatalf("Failed to read seginfo at %s: %v", sidName, err)
	}

	log.Infof("Read Seg info: %+v", segInfo)
	blockSum := structs.GetBsuFnameFromSegKey(segKey)
	blockInfo, readValues, _, err := microreader.ReadBlockSummaries(blockSum, []byte{})
	if err != nil {
		b.Fatalf("Failed to read seginfo at %s: %v", blockSum, err)
	}
	log.Infof("Read block summary info: %+v. Number blocks %+v", blockInfo, len(blockInfo))

	log.Infof("read values %+v", readValues)
}

func Benchmark_agileTreeIngest(b *testing.B) {
	config.InitializeDefaultConfig()
	config.SetAggregationsFlag(true)
	_ = vtable.InitVTable()

	var bulkData []byte

	idx := "agileTree-0"

	actionLine := "{\"index\": {\"_index\": \"" + idx + "\", \"_type\": \"_doc\"}}\n"

	for i := 0; i < 3_000_000; i += 1 {
		ev := make(map[string]interface{})

		ev["a"] = fmt.Sprintf("batch-%d", fastrand.Uint32n(19_000))
		ev["b"] = fastrand.Uint32n(1_000_000)
		ev["c"] = "1103823372288"
		ev["d"] = fmt.Sprintf("iOS-%d", fastrand.Uint32n(19_000))
		ev["e"] = fmt.Sprintf("abde-%d", fastrand.Uint32n(19_000))
		ev["f"] = fmt.Sprintf("useastzone-%d", fastrand.Uint32n(19_000))
		ev["g"] = uuid.NewString()
		ev["h"] = fmt.Sprintf("S%d", fastrand.Uint32n(50))
		ev["i"] = "ethernet4Zone-test4"
		ev["j"] = fmt.Sprintf("group %d", fastrand.Uint32n(2))
		ev["k"] = "00000000000000000000ffff02020202"
		ev["l"] = "funccompanysaf3ti"
		ev["m"] = 6922966563614901991
		ev["n"] = "gtpv1-c"
		ev["o"] = fastrand.Uint32n(10_000_000)

		body, _ := json.Marshal(ev)

		bulkData = append(bulkData, []byte(actionLine)...)
		bulkData = append(bulkData, []byte(body)...)
		bulkData = append(bulkData, []byte("\n")...)
	}

	start := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	b.SetParallelism(2)
	for i := 0; i < b.N; i++ {
		processedCount, response, err := eswriter.HandleBulkBody(bulkData, nil, 0, processKibanaIngestRequest)
		if err != nil {
			log.Errorf("ERROR: err=%v", err)
			break
		}
		if processedCount == 0 {
			log.Errorf("ERROR: processedCount was 0, resp=%v", response)
			break
		}
		writer.ForceRotateSegmentsForTest()
	}
	totalTime := time.Since(start).Seconds()
	log.Warnf("Total time=%f", totalTime)

	/*
	   go test -run=Bench -bench=Benchmark_agileTreeIngest -cpuprofile cpuprofile.out -o rawsearch_cpu
	   go tool pprof ./rawsearch_cpu cpuprofile.out

	   (for mem profile)
	   go test -run=Bench -bench=Benchmark_agileTreeIngest -benchmem -memprofile memprofile.out -o rawsearch_mem
	   go tool pprof ./rawsearch_mem memprofile.out

	*/

	os.RemoveAll(config.GetDataPath())
}

func Benchmark_E2E_AgileTree(b *testing.B) {
	config.InitializeTestingConfig()
	config.SetAggregationsFlag(true)
	_ = localstorage.InitLocalStorage()
	currTime := utils.GetCurrentTimeMillis()
	startTime := uint64(0)
	tRange := &dtu.TimeRange{
		StartEpochMs: startTime,
		EndEpochMs:   currTime,
	}
	sizeLimit := uint64(0)

	smbasedir := "/Users/kunalnawale/work/perf/siglens/data/ingestnodes/Kunals-MacBook-Pro.local/"
	config.SetSmrBaseDirForTestOnly(smbasedir)

	err := query.InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	if err != nil {
		b.Fatalf("Failed to initialize query node: %v", err)
	}

	colVal, err := utils.CreateDtypeEnclosure("*", 1)
	if err != nil {
		log.Fatal(err)
	}
	valueFilter := structs.FilterCriteria{
		ExpressionFilter: &structs.ExpressionFilter{
			LeftInput:      &structs.FilterInput{Expression: &structs.Expression{LeftInput: &structs.ExpressionInput{ColumnName: "*"}}},
			FilterOperator: utils.Equals,
			RightInput:     &structs.FilterInput{Expression: &structs.Expression{LeftInput: &structs.ExpressionInput{ColumnValue: colVal}}},
		},
	}
	queryNode := &structs.ASTNode{
		AndFilterCondition: &structs.Condition{FilterCriteria: []*structs.FilterCriteria{&valueFilter}},
		TimeRange:          tRange,
	}
	if err != nil {
		log.Errorf("Benchmark_LoadMicroIndices: failed to load microindex, err=%v", err)
	}
	count := 10
	allTimes := make([]time.Duration, count)
	timeSum := float64(0)

	grpByCols := []string{"passenger_count", "pickup_date", "trip_distance"}
	measureOps := []*structs.MeasureAggregator{
		{MeasureCol: "total_amount", MeasureFunc: utils.Count},
	}
	grpByRequest := &structs.GroupByRequest{MeasureOperations: measureOps, GroupByColumns: grpByCols}

	aggs := &structs.QueryAggregators{
		GroupByRequest: grpByRequest,
	}
	qc := structs.InitQueryContext("ind-v1", sizeLimit, 0, 0, false)
	b.ResetTimer()
	for i := 0; i < count; i++ {
		sTime := time.Now()
		res := segment.ExecuteQuery(queryNode, aggs, uint64(i), qc)
		log.Infof("query %v result has %v total matches", i, res.TotalResults)
		esquery.GetQueryResponseJson(res, "ind-v1", sTime, sizeLimit, uint64(i), aggs)
		elapTime := time.Since(sTime)
		allTimes[i] = elapTime
		if i != 0 {
			timeSum += elapTime.Seconds()
		}
	}
	log.Infof("Finished benchmark: allTimes: %v", allTimes)
	log.Infof("Average time: %v", timeSum/float64(count))

	/*
	   go test -run=Bench -bench=Benchmark_E2E_AgileTree  -cpuprofile cpuprofile.out -o rawsearch_cpu
	   go tool pprof ./rawsearch_cpu cpuprofile.out

	   (for mem profile)
	   go test -run=Bench -bench=Benchmark_E2E_AgileTree -benchmem -memprofile memprofile.out -o rawsearch_mem
	   go tool pprof ./rawsearch_mem memprofile.out

	*/

}

func Benchmark_S3_segupload(b *testing.B) {
	config.InitializeTestingConfig()

	config.SetS3Enabled(true)
	config.SetS3BucketName("knawale-test")
	config.SetS3Region("us-east-1")

	count := 10
	allTimes := make([]time.Duration, count)
	timeSum := float64(0)

	err := blob.InitBlobStore()
	if err != nil {
		log.Errorf("Benchmark_S3_segupload: Error initializing S3: %v", err)
		return
	}

	finalBasedir := "ingest0data/ip-172-31-15-17.ec2.internal.AU2LfLWt3UXZtQwswR76PV/final/ind-0/0-0-3544697602014606120/7/"
	filesToUpload := fileutils.GetAllFilesInDirectory(finalBasedir)

	log.Infof("Benchmark_S3_segupload: uploading %v files", len(filesToUpload))
	b.ResetTimer()
	for i := 0; i < count; i++ {
		sTime := time.Now()

		err := blob.UploadSegmentFiles(filesToUpload)
		if err != nil {
			log.Errorf("Benchmark_S3_segupload: failed to upload segment files , err=%v", err)
		}
		elapTime := time.Since(sTime)
		allTimes[i] = elapTime
		if i != 0 {
			timeSum += elapTime.Seconds()
		}
	}
	log.Infof("Finished benchmark: allTimes: %v", allTimes)
	log.Infof("Average time: %v", timeSum/float64(count))

	/*
	   go test -run=Bench -bench=Benchmark_S3_segupload  -cpuprofile cpuprofile.out -o rawsearch_cpu
	   go tool pprof ./rawsearch_cpu cpuprofile.out

	   (for mem profile)
	   go test -run=Bench -bench=Benchmark_S3_segupload -benchmem -memprofile memprofile.out -o rawsearch_mem
	   go tool pprof ./rawsearch_mem memprofile.out

	*/

}
