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

	"github.com/fasthttp/websocket"
	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/blob"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/common/fileutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/querytracker"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/memory/limit"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/reader/microreader"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	serverutils "github.com/siglens/siglens/pkg/server/utils"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fastrand"

	localstorage "github.com/siglens/siglens/pkg/blob/local"
	esquery "github.com/siglens/siglens/pkg/es/query"
	eswriter "github.com/siglens/siglens/pkg/es/writer"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
)

var json = jsoniter.ConfigFastest

var loadDataBytes0 = []byte(`{"index" : { "_index" : "bidx-0"} }
{"address":"91982 Plain side, New Orleans, North Dakota 65104","app_name":"Oxcould","app_version":"4.3.15","batch":"batch-89","city":"New Orleans","country":"Chile","first_name":"Luigi","gender":"male","group":"group 2","hobby":"Cosplaying","http_method":"HEAD","http_status":404,"ident":"23a1949c-c32d-47ab-a573-47859fac0e76","image":"https://picsum.photos/381/329","job_company":"TopCoder","job_description":"Dynamic","job_level":"Markets","job_title":"Liaison","last_name":"Tromp","latency":2891953,"latitude":33.139514,"longitude":114.767227,"question":"Sustainable gentrify yr meditation Godard salvia vice migas drinking fanny pack?","ssn":"660889936","state":"North Dakota","street":"91982 Plain side","url":"https://www.internationalextend.info/networks","user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_5 rv:3.0) Gecko/1915-11-28 Firefox/35.0","user_color":"DodgerBlue","user_email":"graceheaney@beatty.io","user_phone":"2597778030","weekday":"Saturday","zip":"65104"}
`)
var loadDataBytes1 = []byte(`{"index" : { "_index" : "bidx-1"} }
{"address":"91982 Plain side, New Orleans, North Dakota 65104","app_name":"Oxcould","app_version":"4.3.15","batch":"batch-89","city":"New Orleans","country":"Chile","first_name":"Luigi","gender":"male","group":"group 2","hobby":"Cosplaying","http_method":"HEAD","http_status":404,"ident":"23a1949c-c32d-47ab-a573-47859fac0e76","image":"https://picsum.photos/381/329","job_company":"TopCoder","job_description":"Dynamic","job_level":"Markets","job_title":"Liaison","last_name":"Tromp","latency":2891953,"latitude":33.139514,"longitude":114.767227,"question":"Sustainable gentrify yr meditation Godard salvia vice migas drinking fanny pack?","ssn":"660889936","state":"North Dakota","street":"91982 Plain side","url":"https://www.internationalextend.info/networks","user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_5 rv:3.0) Gecko/1915-11-28 Firefox/35.0","user_color":"DodgerBlue","user_email":"graceheaney@beatty.io","user_phone":"2597778030","weekday":"Saturday","zip":"65104"}
`)

var allData = [][]byte{loadDataBytes0, loadDataBytes1}

func getMyIds() []uint64 {
	myids := make([]uint64, 1)
	myids[0] = 0
	return myids
}

var upgrader = websocket.FastHTTPUpgrader{
	CheckOrigin:     func(r *fasthttp.RequestCtx) bool { return true },
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
}

func websocketHandler(ctx *fasthttp.RequestCtx) {
	err := upgrader.Upgrade(ctx, func(conn *websocket.Conn) {
		defer conn.Close()

		pipesearch.ProcessPipeSearchWebsocket(conn, 0, ctx)
	})

	if err != nil {
		log.Printf("Upgrade error: %v", err)
		return
	}
}

func startServer() {
	_ = fasthttp.ListenAndServe(":8080", websocketHandler)
}

func Benchmark_EndToEnd(b *testing.B) {

	/*
	   go test -run=Bench -bench=Benchmark_EndToEnd  -cpuprofile cpuprofile.out -o rawsearch_cpu
	   go tool pprof ./rawsearch_cpu cpuprofile.out

	   (for mem profile)
	   go test -run=Bench -bench=Benchmark_EndToEnd -benchmem -memprofile memprofile.out -o rawsearch_mem
	   go tool pprof ./rawsearch_mem memprofile.out

	*/

	dataPath := "data"
	config.InitializeTestingConfig(dataPath + "/")

	hostId := "sigsingle.LMRYyW5hy8mZMG642Lxo93"
	config.SetHostIDForTestOnly(hostId)

	smbasedir := fmt.Sprintf("%v/ingestnodes/%v/", dataPath, hostId)
	config.SetSmrBaseDirForTestOnly(smbasedir)

	limit.InitMemoryLimiter()

	err := vtable.InitVTable()
	if err != nil {
		b.Fatalf("Failed to initialize vtable: %v", err)
	}

	_ = localstorage.InitLocalStorage()

	smFile := writer.GetLocalSegmetaFName()
	err = query.PopulateSegmentMetadataForTheFile_TestOnly(smFile)
	if err != nil {
		b.Fatalf("Failed to load segment metadata: %v", err)
	}

	err = query.InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	if err != nil {
		b.Fatalf("Failed to initialize query node: %v", err)
	}

	websocketURL := "ws://localhost:8080/ws"
	queryLanguage := "Splunk QL"
	start := "now-1h"
	end := "now"
	index := "*"

	logQueries := []string{

		"* | timechart avg(latency) by http_method span=1h",
		"* | timechart avg(latency) by http_method span=1h",
		"* | stats avg(http_status) by hobby, http_method",
		"* | stats count(*) by http_status",
		"* | stats sum(http_status) by hobby, http_method",
		"whatever*",
	}

	log.Infof("Benchmark_EndToEnd: Starting WebSocket server")
	go startServer()

	// Wait for the server to start
	time.Sleep(1 * time.Second)

	count := 10
	allTimes := make(map[int][]time.Duration, len(logQueries)) // map of query index to time taken at each iteration
	timeSum := float64(0)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < count; i++ {
		for ind, query := range logQueries {
			bqid := ind + 1
			log.Infof("Benchmark_EndToEnd: bqid=%v, Query=%v", bqid, query)

			bqidDurations, ok := allTimes[bqid]
			if !ok {
				bqidDurations = make([]time.Duration, count)
				allTimes[bqid] = bqidDurations
			}

			sTime := time.Now()

			queryMessage := map[string]interface{}{
				"state":         "query",
				"startEpoch":    start,
				"endEpoch":      end,
				"indexName":     index,
				"queryLanguage": queryLanguage,
				"searchText":    query,
			}

			// Connect to the WebSocket server
			conn, _, err := websocket.DefaultDialer.Dial(websocketURL, nil)
			if err != nil {
				b.Fatalf("Failed to connect to WebSocket server: %v", err)
			}

			// Send the query message
			err = conn.WriteJSON(queryMessage)
			if err != nil {
				b.Fatalf("Failed to write JSON: %v", err)
			}

			readEvent := make(map[string]interface{})
			for {
				err = conn.ReadJSON(&readEvent)
				if err != nil {
					log.Errorf("Benchmark_EndToEnd: query=%v, Error reading response from server for query. Error=%v", query, err)
					break
				}
				if state, ok := readEvent["state"]; ok && state == "COMPLETE" {
					break
				}
			}

			elapsedTime := time.Since(sTime)
			bqidDurations[i] = elapsedTime
			timeSum += elapsedTime.Seconds()

			log.Infof("Benchmark_EndToEnd: iteration=%v, bqid=%v, Finished reading response from server for query. total_rrc_count=%v, total_events_searched=%v", i+1, bqid, readEvent["total_rrc_count"], readEvent["total_events_searched"])

			// Close the connection
			err = conn.Close()
			if err != nil {
				b.Fatalf("Failed to close connection: %v", err)
			}
		}
	}
	log.Infof("Finished benchmark: allTimes: %v", allTimes)
	log.Infof("Average time: %v", timeSum/float64(len(logQueries)*count))

}

func Benchmark_RRCToJson(b *testing.B) {
	config.InitializeTestingConfig(b.TempDir())
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
		log.Errorf("Benchmark_RRCToJson: failed to load microindex, err=%v", err)
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

func Benchmark_esBulkIngest(b *testing.B) {
	config.InitializeDefaultConfig(b.TempDir())
	_ = vtable.InitVTable()

	querytracker.InitQT()

	var bulkData []byte

	for i := 0; i < 1000; i += 1 {
		idx := i % len(allData)
		bulkData = append(bulkData, allData[idx]...)
	}

	blkSendLoopCnt := 100

	start := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	b.SetParallelism(2)
	for bc := 0; bc < blkSendLoopCnt; bc++ {
		processedCount, response, err := eswriter.HandleBulkBody(bulkData, nil, 0, 0, false)
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
	config.InitializeDefaultConfig(b.TempDir())

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
	config.InitializeDefaultConfig(b.TempDir())
	config.SetAggregationsFlag(true)
	config.SetPQSEnabled(true)

	_ = vtable.InitVTable()

	measureInfoUsage := make(map[string]bool)
	finalGrpCols := make(map[string]bool)

	finalGrpCols["a"] = true
	finalGrpCols["d"] = true
	finalGrpCols["e"] = true
	finalGrpCols["f"] = true
	finalGrpCols["g"] = true
	finalGrpCols["h"] = true
	finalGrpCols["j"] = true

	measureInfoUsage["b"] = true
	measureInfoUsage["m"] = true
	measureInfoUsage["o"] = true

	idx := "agileTree-0"
	idx1 := "agileTree-1"
	idx2 := "agileTree-2"
	idx3 := "agileTree-3"
	idx4 := "agileTree-4"
	querytracker.SetTopPersistentAggsForTestOnly(idx, finalGrpCols, measureInfoUsage)
	querytracker.SetTopPersistentAggsForTestOnly(idx1, finalGrpCols, measureInfoUsage)
	querytracker.SetTopPersistentAggsForTestOnly(idx2, finalGrpCols, measureInfoUsage)
	querytracker.SetTopPersistentAggsForTestOnly(idx3, finalGrpCols, measureInfoUsage)
	querytracker.SetTopPersistentAggsForTestOnly(idx4, finalGrpCols, measureInfoUsage)
	var bulkData []byte

	actionLineIdx0 := "{\"index\": {\"_index\": \"" + idx + "\", \"_type\": \"_doc\"}}\n"
	actionLineIdx1 := "{\"index\": {\"_index\": \"" + idx1 + "\", \"_type\": \"_doc\"}}\n"
	actionLineIdx2 := "{\"index\": {\"_index\": \"" + idx2 + "\", \"_type\": \"_doc\"}}\n"
	actionLineIdx3 := "{\"index\": {\"_index\": \"" + idx3 + "\", \"_type\": \"_doc\"}}\n"
	actionLineIdx4 := "{\"index\": {\"_index\": \"" + idx4 + "\", \"_type\": \"_doc\"}}\n"

	allActions := []string{actionLineIdx0, actionLineIdx1, actionLineIdx2, actionLineIdx3, actionLineIdx4}
	for i := 0; i < 2_000_000; i += 1 {
		ev := make(map[string]interface{})

		ev["a"] = fmt.Sprintf("batch-%d", fastrand.Uint32n(1_000))
		ev["b"] = fastrand.Uint32n(1_000_000)
		ev["c"] = "1103823372288"
		ev["d"] = fmt.Sprintf("iOS-%d", fastrand.Uint32n(1_000))
		ev["e"] = fmt.Sprintf("abde-%d", fastrand.Uint32n(1_000))
		ev["f"] = fmt.Sprintf("useastzone-%d", fastrand.Uint32n(1_000))
		ev["g"] = uuid.NewString()
		ev["h"] = fmt.Sprintf("S%d", fastrand.Uint32n(50))
		ev["i"] = "ethernet4Zone-test4"
		ev["j"] = fmt.Sprintf("group %d", fastrand.Uint32n(500))
		ev["k"] = "00000000000000000000ffff02020202"
		ev["l"] = "funccompanysaf3ti"
		ev["m"] = 6922966563614901991
		ev["n"] = "gtpv1-c"
		ev["o"] = fastrand.Uint32n(10_000_000)

		body, _ := json.Marshal(ev)

		al := allActions[fastrand.Uint32n(5)]
		bulkData = append(bulkData, []byte(al)...)
		bulkData = append(bulkData, []byte(body)...)
		bulkData = append(bulkData, []byte("\n")...)
	}

	start := time.Now()

	numSegs := 6

	b.ReportAllocs()
	b.ResetTimer()
	b.SetParallelism(2)
	for i := 0; i < numSegs; i++ {
		for bulkCnt := 0; bulkCnt < 5; bulkCnt++ {
			processedCount, response, err := eswriter.HandleBulkBody(bulkData, nil, 0, 0, false)
			if err != nil {
				log.Errorf("ERROR: err=%v", err)
				break
			}
			if processedCount == 0 {
				log.Errorf("ERROR: processedCount was 0, resp=%v", response)
				break
			}
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
	config.InitializeTestingConfig(b.TempDir())
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
		log.Errorf("Benchmark_E2E_AgileTree: failed to load microindex, err=%v", err)
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
	config.InitializeTestingConfig(b.TempDir())

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
