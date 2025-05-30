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

package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/health"
	segstructs "github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/tracing/structs"
	tutils "github.com/siglens/siglens/pkg/segment/tracing/utils"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	server_utils "github.com/siglens/siglens/pkg/server/utils"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

const OneHourInMs = 60 * 60 * 1000
const TRACE_PAGE_LIMIT = 50

func ProcessSearchTracesRequest(ctx *fasthttp.RequestCtx, myid int64) {
	searchRequestBody, readJSON, err := ParseAndValidateRequestBody(ctx)
	if err != nil {
		writeErrMsg(ctx, "ProcessSearchTracesRequest", "could not parse and validate request body", err)
		return
	}

	nowTs := utils.GetCurrentTimeInMs()
	searchText, startEpoch, endEpoch, _, _, _, _, _ := pipesearch.ParseSearchBody(readJSON, nowTs)

	page := 1
	pageVal, ok := readJSON["page"]
	if !ok || pageVal == 0 {
		page = 1
	} else {
		switch val := pageVal.(type) {
		case json.Number:
			pageInt, err := val.Int64()
			if err != nil {
				log.Errorf("ProcessSearchTracesRequest: error converting page Val=%v to int: %v", val, err)
			}
			page = int(pageInt)
		default:
			log.Errorf("ProcessSearchTracesRequest: page is not a int Val %+v", val)
		}
	}

	isOnlyTraceID, traceId := ExtractTraceID(searchText)
	traceIds := make([]string, 0)
	if isOnlyTraceID {
		traceIds = append(traceIds, traceId)
	} else {
		// In order to get unique trace_id,  append group by block to the "searchText" field
		if len(searchRequestBody.SearchText) > 0 {
			searchRequestBody.SearchText = searchRequestBody.SearchText + " | stats count(*) BY trace_id"
		} else {
			writeErrMsg(ctx, "ProcessSearchTracesRequest", "request does not contain required parameter: searchText", nil)
			return
		}

		pipeSearchResponseOuter, err := processSearchRequest(searchRequestBody, myid)
		if err != nil {
			writeErrMsg(ctx, "ProcessSearchTracesRequest", err.Error(), nil)
			return
		}

		traceIds = GetUniqueTraceIds(pipeSearchResponseOuter, startEpoch, endEpoch, page)
	}

	traces := make([]*structs.Trace, 0)

	filters := utils.Transform(traceIds, func(s string) string {
		return fmt.Sprintf(`trace_id="%s"`, s)
	})
	searchRequestBody.SearchText = fmt.Sprintf(`(%s) AND parent_span_id="" |
	stats values(start_time) as start_time, values(end_time) as end_time, values(name) as name,
	values(service) as service by trace_id`, strings.Join(filters, " OR "))

	pipeSearchResponseOuter, err := processSearchRequest(searchRequestBody, myid)
	if err != nil {
		utils.SendError(ctx, "Failed to query traces", fmt.Sprintf("query=%s", searchRequestBody.SearchText), err)
		return
	}

	for _, measureResult := range pipeSearchResponseOuter.MeasureResults {
		if len(measureResult.GroupByValues) != 1 {
			log.Errorf("ProcessSearchTracesRequest: expected 1 group by value, got %d",
				len(measureResult.GroupByValues))
			continue
		}

		traceId := measureResult.GroupByValues[0]
		if len(measureResult.MeasureVal) != 4 {
			log.Errorf("ProcessSearchTracesRequest: expected 4 measure values, got %d for traceId=%v",
				len(measureResult.MeasureVal), traceId)
			continue
		}

		startTime, exists := measureResult.MeasureVal["start_time"]
		if !exists {
			log.Errorf("ProcessSearchTracesRequest: start_time not found for traceId=%v", traceId)
			continue
		}

		endTime, exists := measureResult.MeasureVal["end_time"]
		if !exists {
			log.Errorf("ProcessSearchTracesRequest: end_time not found for traceId=%v", traceId)
			continue
		}

		serviceName, exists := measureResult.MeasureVal["service"]
		if !exists {
			log.Errorf("ProcessSearchTracesRequest: service not found for traceId=%v", traceId)
			continue
		}

		operationName, exists := measureResult.MeasureVal["name"]
		if !exists {
			log.Errorf("ProcessSearchTracesRequest: name not found for traceId=%v", traceId)
			continue
		}

		traceStartTime, err := convertTimeToUint64(startTime)
		if err != nil {
			log.Errorf("ProcessSearchTracesRequest: failed to convert startTime: %v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			_, err := ctx.WriteString("Invalid startTime: " + err.Error())
			if err != nil {
				log.Errorf("ProcessSearchTracesRequest: Error writing to context: %v", err)
			}
			return
		}

		traceEndTime, err := convertTimeToUint64(endTime)
		if err != nil {
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			log.Errorf("ProcessSearchTracesRequest: failed to convert endTime: %v", err)
			_, err := ctx.WriteString("Invalid endTime: " + err.Error())
			if err != nil {
				log.Errorf("ProcessSearchTracesRequest: Error writing to context: %v", err)
			}
			return
		}

		// Only process traces which start and end in this period [startEpoch, endEpoch]
		if (startEpoch*1e6 > traceStartTime) || (endEpoch*1e6 < traceEndTime) {
			continue
		}

		service, err := getString(serviceName)
		if err != nil {
			log.Errorf("ProcessSearchTracesRequest: failed to convert serviceName: %v", err)
			continue
		}

		operation, err := getString(operationName)
		if err != nil {
			log.Errorf("ProcessSearchTracesRequest: failed to convert operationName: %v", err)
			continue
		}

		traces = append(traces, &structs.Trace{
			TraceId:       traceId,
			StartTime:     traceStartTime,
			EndTime:       traceEndTime,
			ServiceName:   service,
			OperationName: operation,

			// We'll set these later.
			SpanCount:       0,
			SpanErrorsCount: 0,
		})
	}

	// Run the second query, to find the span counts for each trace.
	filters = utils.Transform(traces, func(t *structs.Trace) string {
		return fmt.Sprintf(`trace_id="%s"`, t.TraceId)
	})
	searchRequestBody.SearchText = fmt.Sprintf(`%s | stats count as count by status, trace_id`,
		strings.Join(filters, " OR "))
	pipeSearchResponseOuter, err = processSearchRequest(searchRequestBody, myid)
	if err != nil {
		utils.SendError(ctx, "Failed to query traces", fmt.Sprintf("query=%s", searchRequestBody.SearchText), err)
		return
	}

	for _, measureResult := range pipeSearchResponseOuter.MeasureResults {
		if len(pipeSearchResponseOuter.GroupByCols) != 2 {
			log.Errorf("ProcessSearchTracesRequest: expected 2 group by columns, got %d",
				len(pipeSearchResponseOuter.GroupByCols))
			continue
		}
		swapped := false
		if pipeSearchResponseOuter.GroupByCols[0] == "trace_id" {
			swapped = true
		}

		if len(measureResult.GroupByValues) != 2 {
			log.Errorf("ProcessSearchTracesRequest: expected 2 group by values, got %d",
				len(measureResult.GroupByValues))
			continue
		}

		statusCode := measureResult.GroupByValues[0]
		traceId := measureResult.GroupByValues[1]
		if swapped {
			traceId, statusCode = statusCode, traceId
		}

		if len(measureResult.MeasureVal) != 1 {
			log.Errorf("ProcessSearchTracesRequest: expected 1 measure value, got %d for traceId=%v",
				len(measureResult.MeasureVal), traceId)
			continue
		}

		count, exists := measureResult.MeasureVal["count"]
		if !exists {
			log.Errorf("ProcessSearchTracesRequest: expected count measure value, got %d for traceId=%v",
				len(measureResult.MeasureVal), traceId)
			continue
		}

		countVal, isFloat := count.(float64)
		if !isFloat {
			log.Errorf("ProcessSearchTracesRequest: expected count measure value to be float64, got %T for traceId=%v",
				count, traceId)
			continue
		}

		// Find the trace in the list of traces.
		for _, trace := range traces {
			if trace.TraceId == traceId {
				trace.SpanCount += int(countVal)
				if statusCode == string(structs.Status_STATUS_CODE_ERROR) {
					trace.SpanErrorsCount += int(countVal)
				}
				break
			}
		}
	}

	traceResult := &structs.TraceResult{
		Traces: traces,
	}

	utils.WriteJsonResponse(ctx, traceResult)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func getString(val interface{}) (string, error) {
	switch v := val.(type) {
	case string:
		return v, nil
	case []interface{}:
		if len(v) == 0 {
			return "", fmt.Errorf("empty array")
		} else if len(v) > 1 {
			return "", fmt.Errorf("array length greater than 1")
		}

		return getString(v[0])
	default:
		return "", fmt.Errorf("getString: unexpected type %T", v)
	}
}

func convertTimeToUint64(val interface{}) (uint64, error) {
	switch v := val.(type) {
	case float64:
		return uint64(v), nil
	case int:
		return uint64(v), nil
	case int64:
		return uint64(v), nil
	case uint64:
		return v, nil
	case string:
		floatVal, err := strconv.ParseFloat(v, 64)
		if err != nil {
			log.Errorf("convertTimeToUint64 : error converting string to float64: %v", err)
			return 0, fmt.Errorf("error converting string to float64 ")
		}
		return uint64(floatVal), nil
	case []interface{}:
		if len(v) == 0 {
			return 0, fmt.Errorf("empty array")
		} else if len(v) > 1 {
			return 0, fmt.Errorf("array length greater than 1")
		}

		return convertTimeToUint64(v[0])
	default:
		log.Errorf("convertTimeToUint64 : unexpected type %T", v)
		return 0, fmt.Errorf("unexpected type %T", v)
	}
}

func ProcessTotalTracesRequest(ctx *fasthttp.RequestCtx, myid int64) {
	searchRequestBody, _, err := ParseAndValidateRequestBody(ctx)
	if err != nil {
		writeErrMsg(ctx, "ProcessTotalTracesRequest", "could not parse and validate request body", err)
		return
	}

	// In order to get unique trace_id,  append group by block to the "searchText" field
	if len(searchRequestBody.SearchText) > 0 {
		searchRequestBody.SearchText = searchRequestBody.SearchText + " | stats count BY trace_id"
	} else {
		writeErrMsg(ctx, "ProcessTotalTracesRequest", "request does not contain required parameter: searchText", nil)
		return
	}

	pipeSearchResponseOuter, err := processSearchRequest(searchRequestBody, myid)
	if err != nil {
		writeErrMsg(ctx, "ProcessTotalTracesRequest", err.Error(), nil)
		return
	}

	totalTraces := GetTotalUniqueTraceIds(pipeSearchResponseOuter)
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetBodyString(strconv.Itoa(totalTraces))
}

func ParseAndValidateRequestBody(ctx *fasthttp.RequestCtx) (*structs.SearchRequestBody, map[string]interface{}, error) {
	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf("ParseAndValidateRequestBody: Received empty search request body")
		utils.SetBadMsg(ctx, "")
		return nil, nil, errors.New("received empty search request body")
	}

	readJSON := make(map[string]interface{})
	var jsonc = jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := jsonc.NewDecoder(bytes.NewReader(rawJSON))
	decoder.UseNumber()
	err := decoder.Decode(&readJSON)
	if err != nil {
		return nil, nil, err
	}

	searchRequestBody := &structs.SearchRequestBody{}
	if err := json.Unmarshal(ctx.PostBody(), &searchRequestBody); err != nil {
		return nil, nil, err
	}

	searchRequestBody.QueryLanguage = "Splunk QL"
	searchRequestBody.IndexName = "traces"

	return searchRequestBody, readJSON, nil
}

func GetTotalUniqueTraceIds(pipeSearchResponseOuter *segstructs.PipeSearchResponseOuter) int {
	return pipeSearchResponseOuter.BucketCount
}

func GetUniqueTraceIds(pipeSearchResponseOuter *segstructs.PipeSearchResponseOuter, startEpoch uint64, endEpoch uint64, page int) []string {
	totalTracesIds := GetTotalUniqueTraceIds(pipeSearchResponseOuter)
	if totalTracesIds < (page-1)*TRACE_PAGE_LIMIT {
		return []string{}
	}

	endIndex := page * TRACE_PAGE_LIMIT
	if endIndex > totalTracesIds {
		endIndex = totalTracesIds
	}

	traceIds := make([]string, 0)
	for _, bucket := range pipeSearchResponseOuter.MeasureResults[(page-1)*TRACE_PAGE_LIMIT : endIndex] {
		if len(bucket.GroupByValues) == 1 {
			traceIds = append(traceIds, bucket.GroupByValues[0])
		}
	}

	return traceIds
}

// Check if searchText only contains traceId as query condition
func ExtractTraceID(searchText string) (bool, string) {
	pattern := `^trace_id=([a-zA-Z0-9]+)$`

	regex, err := regexp.Compile(pattern)
	if err != nil {
		return false, ""
	}

	matches := regex.FindStringSubmatch(searchText)
	if len(matches) != 2 {
		return false, ""
	}

	return true, matches[1]
}

// Check if searchText only contains spanId as query condition
func ExtractSpanID(searchText string) (bool, string) {
	pattern := `^span_id=([a-zA-Z0-9]+)$`

	regex := regexp.MustCompile(pattern)

	matches := regex.FindStringSubmatch(searchText)
	if len(matches) != 2 {
		return false, ""
	}

	return true, matches[1]
}

// Call /api/search endpoint
func processSearchRequest(searchRequestBody *structs.SearchRequestBody, myid int64) (*segstructs.PipeSearchResponseOuter, error) {

	modifiedData, err := json.Marshal(searchRequestBody)
	if err != nil {
		return nil, fmt.Errorf("processSearchRequest: could not marshal to json body=%v, err=%v", *searchRequestBody, err)
	}

	// Get initial data
	rawTraceCtx := &fasthttp.RequestCtx{}
	rawTraceCtx.Request.Header.SetMethod("POST")
	rawTraceCtx.Request.SetBody(modifiedData)
	pipesearch.ProcessPipeSearchRequest(rawTraceCtx, myid)
	pipeSearchResponseOuter := segstructs.PipeSearchResponseOuter{}

	// Parse initial data
	if err := json.Unmarshal(rawTraceCtx.Response.Body(), &pipeSearchResponseOuter); err != nil {
		return nil, fmt.Errorf("processSearchRequest: could not unmarshal json body, err=%v", err)
	}
	return &pipeSearchResponseOuter, nil
}

// Monitor spans health in the last 5 mins
func MonitorSpansHealth() {
	time.Sleep(1 * time.Minute) // Wait for initial traces ingest first
	for {
		myids := server_utils.GetMyIds()
		for _, myid := range myids {
			_, traceIndexCount, _, _, _ := health.GetTraceStatsForAllSegments(myid)
			if traceIndexCount > 0 {
				ProcessRedTracesIngest(myid)
			}
		}
		time.Sleep(5 * time.Minute)
	}
}

func ProcessRedTracesIngest(myid int64) {
	// Initial request
	searchRequestBody := structs.SearchRequestBody{
		IndexName:     "traces",
		SearchText:    "*",
		QueryLanguage: "Splunk QL",
		StartEpoch:    "now-5m",
		EndEpoch:      "now",
		From:          0,
		Size:          1000,
	}

	// We can only determine whether a span is an entry span or not after retrieving all the spans,
	// E.g.: Perhaps there is no parent span for span:12345 in this request, and its parent span exists in the next
	//request. Therefore, we cannot determine if one span has a parent span in a single request.
	// We should use this array to record all the spans
	spans := make([]*structs.Span, 0)

	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [utils.UnescapeStackBufSize]byte

	for {
		ctx := &fasthttp.RequestCtx{}
		requestData, err := json.Marshal(searchRequestBody)
		if err != nil {
			log.Errorf("ProcessRedTracesIngest: could not marshal to json body=%v, err=%v", searchRequestBody, err)
			return
		}

		ctx.Request.Header.SetMethod("POST")
		ctx.Request.SetBody(requestData)

		// Get initial data
		pipesearch.ProcessPipeSearchRequest(ctx, myid)

		// Parse initial data
		rawSpanData := structs.RawSpanData{}
		if err := json.Unmarshal(ctx.Response.Body(), &rawSpanData); err != nil {
			writeErrMsg(ctx, "ProcessRedTracesIngest", "could not unmarshal json body", err)
			return
		}

		if len(rawSpanData.Hits.Spans) == 0 {
			break
		}

		spans = append(spans, rawSpanData.Hits.Spans...)
		searchRequestBody.From += 1000
	}

	if len(spans) == 0 {
		return
	}

	spanIDtoService := make(map[string]string)
	entrySpans := make([]*structs.Span, 0)
	serviceToSpanCnt := make(map[string]int)
	serviceToErrSpanCnt := make(map[string]int)
	serviceToSpanDuration := make(map[string][]uint64)

	// Map from the service name to the RED metrics
	serviceToMetrics := make(map[string]structs.RedMetrics)

	for _, span := range spans {
		spanIDtoService[span.SpanID] = span.Service
	}

	// Get entry spans
	for _, span := range spans {

		// A span is an entry point if it has no parent or its parent is a different service
		if len(span.ParentSpanID) != 0 {
			parentServiceName, exists := spanIDtoService[span.ParentSpanID]
			if exists && parentServiceName == span.Service {
				continue
			}
		}

		entrySpans = append(entrySpans, span)
	}

	indexName := "red-traces"
	shouldFlush := false
	tsKey := config.GetTimeStampKey()

	// Map the service name to: the number of entry spans, erroring entry spans, duration list of span
	for _, entrySpan := range entrySpans {
		spanCnt, exists := serviceToSpanCnt[entrySpan.Service]
		if exists {
			serviceToSpanCnt[entrySpan.Service] = spanCnt + 1
		} else {
			serviceToSpanCnt[entrySpan.Service] = 1
		}

		if string(structs.Status_STATUS_CODE_ERROR) == string(entrySpan.Status) {
			spanErrorCnt, exists := serviceToErrSpanCnt[entrySpan.Service]
			if exists {
				serviceToErrSpanCnt[entrySpan.Service] = spanErrorCnt + 1
			} else {
				serviceToErrSpanCnt[entrySpan.Service] = 1
			}
		}

		durationList, exists := serviceToSpanDuration[entrySpan.Service]
		if exists {
			durationList = append(durationList, entrySpan.Duration)
		} else {
			durationList = []uint64{entrySpan.Duration}
		}

		serviceToSpanDuration[entrySpan.Service] = durationList
	}

	idxToStreamIdCache := make(map[string]string)
	pleArray := make([]*segwriter.ParsedLogEvent, 0)
	defer segwriter.ReleasePLEs(pleArray)
	numBytes := 0

	// Map from the service name to the RED metrics
	for service, spanCnt := range serviceToSpanCnt {

		errSpanCnt := 0
		val, exists := serviceToErrSpanCnt[service]
		if exists {
			errSpanCnt = val
		}

		redMetrics := structs.RedMetrics{
			Rate:      float64(spanCnt) / float64(60),
			ErrorRate: (float64(errSpanCnt) / float64(spanCnt)) * 100,
		}

		durations, exists := serviceToSpanDuration[service]
		for i, duration := range durations {
			durations[i] = duration / 1000000 // convert duration from nanoseconds to milliseconds
		}
		if exists {
			redMetrics.P50 = tutils.FindPercentileData(durations, 50)
			redMetrics.P90 = tutils.FindPercentileData(durations, 90)
			redMetrics.P95 = tutils.FindPercentileData(durations, 95)
			redMetrics.P99 = tutils.FindPercentileData(durations, 99)
		}

		serviceToMetrics[service] = redMetrics

		jsonData, err := redMetricsToJson(redMetrics, service)
		if err != nil {
			log.Errorf("ProcessRedTracesIngest: failed to marshal redMetrics=%v: Error=%v", redMetrics, err)
			continue
		}

		// Setup ingestion parameters
		now := utils.GetCurrentTimeInMs()

		ple, err := segwriter.GetNewPLE(jsonData, now, indexName, &tsKey, jsParsingStackbuf[:])
		if err != nil {
			log.Errorf("ProcessRedTracesIngest: failed to get new PLE: %v", err)
			continue
		}
		pleArray = append(pleArray, ple)
		numBytes += len(jsonData)
	}

	localIndexMap := make(map[string]string)
	tsNow := utils.GetCurrentTimeInMs()

	err := writer.ProcessIndexRequestPle(tsNow, indexName, shouldFlush, localIndexMap,
		myid, 0, idxToStreamIdCache, cnameCacheByteHashToStr,
		jsParsingStackbuf[:], pleArray)
	if err != nil {
		log.Errorf("ProcessRedTracesIngest: failed to process ingest request: %v", err)
		return
	}

	usageStats.UpdateTracesStats(uint64(numBytes), uint64(len(pleArray)), myid)
}

func redMetricsToJson(redMetrics structs.RedMetrics, service string) ([]byte, error) {
	result := make(map[string]interface{})
	result["service"] = service
	result["rate"] = redMetrics.Rate
	result["error_rate"] = redMetrics.ErrorRate
	result["p50"] = redMetrics.P50
	result["p90"] = redMetrics.P90
	result["p95"] = redMetrics.P95
	result["p99"] = redMetrics.P99
	return json.Marshal(result)
}

func DependencyGraphThread() {
	for {
		now := time.Now()
		nextHour := now.Truncate(time.Hour).Add(time.Hour)
		sleepDuration := time.Until(nextHour)

		time.Sleep(sleepDuration)

		myids := server_utils.GetMyIds()

		for _, myid := range myids {
			_, traceIndexCount, _, _, _ := health.GetTraceStatsForAllSegments(myid)
			if traceIndexCount > 0 {
				// Calculate startEpoch and endEpoch for the last hour
				endEpoch := time.Now().UnixMilli()
				startEpoch := time.Now().Add(-time.Hour).UnixMilli()

				depMatrix := MakeTracesDependancyGraph(startEpoch, endEpoch, myid)
				if len(depMatrix) > 0 {
					writeDependencyMatrix(depMatrix, myid)
				}
			}
		}
	}
}

func MakeTracesDependancyGraph(startEpoch int64, endEpoch int64, myid int64) map[string]map[string]int {

	requestBody := map[string]interface{}{
		"indexName":     "traces",
		"startEpoch":    startEpoch,
		"endEpoch":      endEpoch,
		"searchText":    "*",
		"queryLanguage": "Splunk QL",
	}
	requestBodyJSON, err := json.Marshal(requestBody)
	if err != nil {
		fmt.Printf("MakeTracesDependancyGraph: Error marshaling request body=%v, Error=%v", requestBody, err)
		return nil
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetBody(requestBodyJSON)

	ctx.Request.Header.SetMethod("POST")
	pipesearch.ProcessPipeSearchRequest(ctx, myid)

	rawSpanData := structs.RawSpanData{}
	if err := json.Unmarshal(ctx.Response.Body(), &rawSpanData); err != nil {
		log.Errorf("MakeTracesDependancyGraph: could not unmarshal json body, err=%v", err)
		return nil
	}
	spanIdToServiceName := make(map[string]string)
	dependencyMatrix := make(map[string]map[string]int)

	for _, span := range rawSpanData.Hits.Spans {
		spanIdToServiceName[span.SpanID] = span.Service
	}
	for _, span := range rawSpanData.Hits.Spans {
		if span.ParentSpanID == "" {
			continue
		}
		parentService, parentExists := spanIdToServiceName[span.ParentSpanID]
		if !parentExists {
			continue
		}
		if parentService == span.Service {
			continue
		}
		if dependencyMatrix[parentService] == nil {
			dependencyMatrix[parentService] = make(map[string]int)
		}
		dependencyMatrix[parentService][span.Service]++
	}
	return dependencyMatrix
}

func writeDependencyMatrix(dependencyMatrix map[string]map[string]int, myid int64) {
	dependencyMatrixJSON, err := json.Marshal(dependencyMatrix)
	if err != nil {
		log.Errorf("writeDependencyMatrix: Error marshaling dependency matrix:err=%v", err)
		return
	}

	// Setup ingestion parameters
	now := utils.GetCurrentTimeInMs()
	indexName := "service-dependency"
	shouldFlush := false
	localIndexMap := make(map[string]string)
	tsKey := config.GetTimeStampKey()

	idxToStreamIdCache := make(map[string]string)
	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [utils.UnescapeStackBufSize]byte
	pleArray := make([]*segwriter.ParsedLogEvent, 0)
	defer segwriter.ReleasePLEs(pleArray)

	ple, err := segwriter.GetNewPLE(dependencyMatrixJSON, now, indexName, &tsKey, jsParsingStackbuf[:])
	if err != nil {
		log.Errorf("MakeTracesDependancyGraph: failed to get new PLE: %v", err)
		return
	}
	pleArray = append(pleArray, ple)

	err = writer.ProcessIndexRequestPle(now, indexName, shouldFlush, localIndexMap, myid, 0, idxToStreamIdCache,
		cnameCacheByteHashToStr, jsParsingStackbuf[:], pleArray)
	if err != nil {
		log.Errorf("MakeTracesDependancyGraph: failed to process ingest request: %v", err)
		return
	}
}

// ProcessAggregatedDependencyGraphs handles the /dependencies endpoint.
// It aggregates already computed dependency graphs based on the provided start and end epochs.
func ProcessAggregatedDependencyGraphs(ctx *fasthttp.RequestCtx, myid int64) {
	// Extract startEpoch and endEpoch from the request
	_, readJSON, err := ParseAndValidateRequestBody(ctx)
	if err != nil {
		log.Errorf("ProcessAggregatedDependencyGraphs: Unable to Parse Request Body, Error=%v", err)
		return
	}
	searchRequestBody := &structs.SearchRequestBody{}
	searchRequestBody.QueryLanguage = "Splunk QL"
	searchRequestBody.IndexName = "service-dependency"
	searchRequestBody.SearchText = "*"

	var valueType string
	if val, ok := readJSON["startEpoch"]; ok {
		searchRequestBody.StartEpoch, valueType = convertEpochToString(val)
		if searchRequestBody.StartEpoch == "" {
			log.Errorf("ProcessAggregatedDependencyGraphs: Invalid data type for startEpoch. Value: %v, Type: %s", val, valueType)
			return
		}
	} else {
		log.Errorf("ProcessAggregatedDependencyGraphs : startEpoch is missing")
		return
	}

	if val, ok := readJSON["endEpoch"]; ok {
		searchRequestBody.EndEpoch, valueType = convertEpochToString(val)
		if searchRequestBody.EndEpoch == "" {
			log.Errorf("ProcessAggregatedDependencyGraphs: Invalid data type for endEpoch. Value: %v, Type: %s", val, valueType)
			return
		}
	} else {
		log.Errorf("ProcessAggregatedDependencyGraphs : endEpoch is missing")
		return
	}

	dependencyResponseOuter, err := processSearchRequest(searchRequestBody, myid)
	if err != nil {
		log.Errorf("ProcessAggregatedDependencyGraphs: processSearchRequest: Error=%v", err)
		return
	}
	processedData := make(map[string]interface{})
	if dependencyResponseOuter.Hits.Hits == nil || len(dependencyResponseOuter.Hits.Hits) == 0 {
		ctx.SetStatusCode(fasthttp.StatusOK)
		_, writeErr := ctx.WriteString(utils.ErrNoDependencyGraphs)
		if writeErr != nil {
			log.Errorf("ProcessAggregatedDependencyGraphs: Error writing to context: %v", writeErr)
		}
		return
	} else {
		firstHit := true
		// Loop over all the graphs
		for _, hit := range dependencyResponseOuter.Hits.Hits {
			// Process the current graph
			for key, value := range hit {
				if key == "_index" {
					processedData[key] = value
					continue
				}
				if key == "timestamp" {
					if firstHit {
						processedData[key] = value
						firstHit = false
					}
					continue
				}
				keys := strings.Split(key, ".")
				if len(keys) != 2 {
					fmt.Printf("Unexpected key format: %s\n", key)
					continue
				}
				service, dependentService := keys[0], keys[1]
				if processedData[service] == nil {
					processedData[service] = make(map[string]int)
				}

				serviceMap := processedData[service].(map[string]int)
				if value != nil {
					serviceMap[dependentService] += int(value.(float64))
				} else {
					log.Warnf("MakeTracesDependancyGraph: Value is nil, cannot convert to float64")
				}
			}
		}
	}

	ctx.SetContentType("application/json; charset=utf-8")
	err = json.NewEncoder(ctx).Encode(processedData)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
		_, writeErr := ctx.WriteString(fmt.Sprintf("Error encoding JSON: %s", err.Error()))
		if writeErr != nil {
			log.Errorf("ProcessAggregatedDependencyGraphs: Error writing to context: %v", writeErr)
		}
		return
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func convertEpochToString(value interface{}) (string, string) {
	valueType := fmt.Sprintf("%T", value)
	switch v := value.(type) {
	case string:
		return v, valueType
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v), valueType
	case float32, float64:
		return fmt.Sprintf("%f", v), valueType
	default:
		log.Errorf("convertEpochToString: Unsupported data type: %T, Value: %v", v, v)
		return "", valueType
	}
}

// ProcessGeneratedDepGraph handles the /generate-dep-graph endpoint.
// It generates a new dependency graph based on the provided start and end epochs and displays it without storing.
func ProcessGeneratedDepGraph(ctx *fasthttp.RequestCtx, myid int64) {
	// Extract startEpoch and endEpoch from the request
	_, readJSON, err := ParseAndValidateRequestBody(ctx)
	if err != nil {
		log.Errorf("ProcessGeneratedDepGraph: Unable to Parse Request Body, Error=%v", err)
		return
	}

	nowTs := utils.GetCurrentTimeInMs()
	_, startEpoch, endEpoch, _, _, _, _, _ := pipesearch.ParseSearchBody(readJSON, nowTs)

	startEpochInt64 := int64(startEpoch)
	endEpochInt64 := int64(endEpoch)

	// Generate the dependency graph
	depMatrix := MakeTracesDependancyGraph(startEpochInt64, endEpochInt64, myid)

	processedData := make(map[string]interface{})
	for key, value := range depMatrix {
		for k, v := range value {
			if processedData[key] == nil {
				processedData[key] = make(map[string]int)
			}
			serviceMap := processedData[key].(map[string]int)
			serviceMap[k] = v
		}
	}

	// Display the graph
	ctx.SetContentType("application/json; charset=utf-8")
	err = json.NewEncoder(ctx).Encode(processedData)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
		_, writeErr := ctx.WriteString(fmt.Sprintf("ProcessGeneratedDepGraph: Error encoding JSON: %s", err.Error()))
		if writeErr != nil {
			log.Errorf("ProcessGeneratedDepGraph: Error writing to context: %v", writeErr)
		}
		return
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessGanttChartRequest(ctx *fasthttp.RequestCtx, myid int64) {

	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf("ProcessGanttChartRequest: received empty search request body")
		utils.SetBadMsg(ctx, "")
		return
	}

	readJSON := make(map[string]interface{})
	var jsonc = jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := jsonc.NewDecoder(bytes.NewReader(rawJSON))
	decoder.UseNumber()
	err := decoder.Decode(&readJSON)
	if err != nil {
		writeErrMsg(ctx, "ProcessGanttChartRequest", "could not decode json", err)
		return
	}

	// Parse the JSON data from ctx.PostBody
	searchRequestBody := &structs.SearchRequestBody{}
	if err := json.Unmarshal(ctx.PostBody(), &searchRequestBody); err != nil {
		writeErrMsg(ctx, "ProcessGanttChartRequest", "could not unmarshal json body", err)
		return
	}

	searchRequestBody.QueryLanguage = "Splunk QL"
	searchRequestBody.IndexName = "traces"
	searchRequestBody.From = 0
	searchRequestBody.Size = 1000

	// Used to find out which attributes belong to tags
	fieldsNotInTag := []string{"trace_id", "span_id", "parent_span_id", "service", "trace_state", "name", "kind", "start_time", "end_time",
		"duration", "dropped_attributes_count", "dropped_events_count", "dropped_links_count", "status", "events", "links", "_index", "timestamp"}

	idToSpanMap := make(map[string]*structs.GanttChartSpan, 0)
	idToParentId := make(map[string]string, 0)

	for {
		modifiedData, err := json.Marshal(searchRequestBody)
		if err != nil {
			writeErrMsg(ctx, "ProcessGanttChartRequest", "could not marshal to json body", err)
		}

		// Get initial data
		rawTraceCtx := &fasthttp.RequestCtx{}
		rawTraceCtx.Request.Header.SetMethod("POST")
		rawTraceCtx.Request.SetBody(modifiedData)
		pipesearch.ProcessPipeSearchRequest(rawTraceCtx, myid)

		resultMap := make(map[string]interface{}, 0)
		decoder := jsonc.NewDecoder(bytes.NewReader(rawTraceCtx.Response.Body()))
		decoder.UseNumber()
		err = decoder.Decode(&resultMap)
		if err != nil {
			writeErrMsg(ctx, "ProcessGanttChartRequest", "could not decode response body", err)
			return
		}

		hits, exists := resultMap["hits"]
		if !exists {
			writeErrMsg(ctx, "ProcessGanttChartRequest", "Key 'hits' not found in response", nil)
			return
		}

		hitsMap, ok := hits.(map[string]interface{})
		if !ok {
			writeErrMsg(ctx, "ProcessGanttChartRequest", "Error asserting type for 'hits'", nil)
			return
		}

		records, exists := hitsMap["records"]
		if !exists {
			writeErrMsg(ctx, "ProcessGanttChartRequest", "Key 'records' not found in response", nil)
			return
		}

		rawSpans, ok := records.([]interface{})
		if !ok {
			writeErrMsg(ctx, "ProcessGanttChartRequest", "Error asserting type for 'records'", nil)
			return
		}

		if len(rawSpans) == 0 {
			break
		}

		for _, rawSpan := range rawSpans {
			spanMap := rawSpan.(map[string]interface{})

			span := &structs.GanttChartSpan{}

			jsonData, err := json.Marshal(spanMap)
			if err != nil {
				log.Errorf("ProcessGanttChartRequest: could not marshal to json body spanMap, err=%v", err)
				continue
			}
			if err := json.Unmarshal(jsonData, &span); err != nil {
				log.Errorf("ProcessGanttChartRequest: could not unmarshal to json body, err=%v", err)
				continue
			}

			serviceName, exists := spanMap["service"]
			if !exists {
				log.Errorf("ProcessGanttChartRequest: span:%v does not contain the required field: service", span.SpanID)
				continue
			}

			operationName, exists := spanMap["name"]
			if !exists {
				log.Errorf("ProcessGanttChartRequest: span:%v does not contain the required field: name", span.SpanID)
				continue
			}

			parentSpanId, exists := spanMap["parent_span_id"]
			if !exists {
				log.Errorf("ProcessGanttChartRequest: span:%v does not contain the required field: parent_span_id", span.SpanID)
				continue
			}

			idToParentId[span.SpanID] = parentSpanId.(string)

			status, exists := spanMap["status"]
			if !exists {
				log.Errorf("ProcessGanttChartRequest: span:%v does not contain the required field: status", span.SpanID)
				continue
			}
			// Remove all non-tag fields
			for _, strToRemove := range fieldsNotInTag {
				delete(spanMap, strToRemove)
			}

			for key, val := range spanMap {
				if val == nil {
					delete(spanMap, key)
				}
			}
			span.Tags = spanMap
			span.ServiceName = serviceName.(string)
			span.OperationName = operationName.(string)
			span.Status = status.(string) // Populate Status from Span
			idToSpanMap[span.SpanID] = span
		}

		if len(rawSpans) < searchRequestBody.Size {
			break
		}

		searchRequestBody.From += 1000
	}

	res, err := tutils.BuildSpanTree(idToSpanMap, idToParentId)
	if err != nil {
		writeErrMsg(ctx, "ProcessGanttChartRequest", err.Error(), nil)
		return
	}

	utils.WriteJsonResponse(ctx, res)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func writeErrMsg(ctx *fasthttp.RequestCtx, functionName string, errorMsg string, err error) {

	errContent := functionName + ": " + errorMsg
	if err != nil {
		errContent += fmt.Sprintf(", err=%v", err)
	}

	ctx.SetStatusCode(fasthttp.StatusBadRequest)
	_, err = ctx.WriteString(errContent)
	if err != nil {
		log.Errorf("%s: could not write error message err=%v", functionName, err)
	}
	log.Errorf("%s: failed to decode search request body! Err=%v", functionName, err)
}

func ProcessSpanGanttChartRequest(ctx *fasthttp.RequestCtx, myid int64) {
	searchRequestBody, readJSON, err := ParseAndValidateRequestBody(ctx)
	if err != nil {
		writeErrMsg(ctx, "ProcessSpanGanttChartRequest", "could not parse and validate request body", err)
		return
	}

	nowTs := utils.GetCurrentTimeInMs()
	searchText, startEpoch, endEpoch, _, _, _, _, _ := pipesearch.ParseSearchBody(readJSON, nowTs)

	// Validate query
	isOnlySpanID, spanId := ExtractSpanID(searchText)
	if !isOnlySpanID {
		writeErrMsg(ctx, "ProcessSpanGanttChartRequest", "only provide 1 span ID", nil)
		return
	}

	page := 1
	pageVal, ok := readJSON["page"]
	if ok && pageVal != 0 {
		switch val := pageVal.(type) {
		case json.Number:
			pageInt, err := val.Int64()
			if err != nil {
				log.Errorf("ProcessSpanGanttChartRequest: error converting page Val=%v to int: %v", val, err)
			}
			page = int(pageInt)
		default:
			log.Errorf("ProcessSpanGanttChartRequest: page is not a int Val %+v", val)
		}
	}

	searchRequestBody.IndexName = "traces"

	// Find all unique Trace IDs for the spanId
	searchRequestBody.SearchText = "span_id=" + spanId + " | stats count BY trace_id"
	pipeSearchResponseOuter, err := processSearchRequest(searchRequestBody, myid)
	if err != nil {
		writeErrMsg(ctx, "ProcessSpanGanttChartRequest", err.Error(), nil)
		return
	}

	totalTraces := GetTotalUniqueTraceIds(pipeSearchResponseOuter)

	if totalTraces == 0 {
		writeErrMsg(ctx, "ProcessSpanGanttChartRequest", "Span ID not found", nil)
		return
	} else if totalTraces > 1 {
		// Log if more than 1 trace id belongs to a span id, should not be the case
		log.Errorf("Span ID should be unique to Trace ID")
	}

	traceIds := GetUniqueTraceIds(pipeSearchResponseOuter, startEpoch, endEpoch, page)

	traceId := traceIds[0]
	if traceId == "" {
		writeErrMsg(ctx, "ProcessSpanGanttChartRequest", "Orphaned Span ID", nil)
		return
	}

	requestBody := map[string]interface{}{
		"indexName":     "traces",
		"startEpoch":    searchRequestBody.StartEpoch,
		"endEpoch":      searchRequestBody.EndEpoch,
		"searchText":    "trace_id=" + traceId,
		"queryLanguage": "Splunk QL",
	}
	requestBodyJSON, err := json.Marshal(requestBody)
	if err != nil {
		fmt.Printf("ProcessSpanGanttChartRequest: Error marshaling request body=%v, Error=%v", requestBody, err)
		return
	}

	ctx.Request.SetBody(requestBodyJSON)

	ProcessGanttChartRequest(ctx, myid)
}
