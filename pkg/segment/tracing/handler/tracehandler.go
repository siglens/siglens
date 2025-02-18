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
	"github.com/siglens/siglens/pkg/segment/tracing/utils"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	server_utils "github.com/siglens/siglens/pkg/server/utils"
	"github.com/siglens/siglens/pkg/usageStats"

	putils "github.com/siglens/siglens/pkg/utils"
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

	nowTs := putils.GetCurrentTimeInMs()
	searchText, startEpoch, endEpoch, _, _, _, _ := pipesearch.ParseSearchBody(readJSON, nowTs)

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
	// Get status code count for each trace
	for _, traceId := range traceIds {
		// Get the start time and end time for this trace
		searchRequestBody.SearchText = "trace_id=" + traceId + " AND parent_span_id=\"\" | fields start_time, end_time, name, service"
		pipeSearchResponseOuter, err := processSearchRequest(searchRequestBody, myid)
		if err != nil {
			log.Errorf("ProcessSearchTracesRequest: traceId:%v, Error=%v", traceId, err)
			continue
		}

		if pipeSearchResponseOuter.Hits.Hits == nil || len(pipeSearchResponseOuter.Hits.Hits) == 0 {
			continue
		}

		startTime, exists := pipeSearchResponseOuter.Hits.Hits[0]["start_time"]
		if !exists {
			continue
		}
		endTime, exists := pipeSearchResponseOuter.Hits.Hits[0]["end_time"]
		if !exists {
			continue
		}

		serviceName, exists := pipeSearchResponseOuter.Hits.Hits[0]["service"]
		if !exists {
			continue
		}

		operationName, exists := pipeSearchResponseOuter.Hits.Hits[0]["name"]
		if !exists {
			continue
		}

		traceStartTime := uint64(startTime.(float64))
		traceEndTime := uint64(endTime.(float64))

		// Only process traces which start and end in this period [startEpoch, endEpoch]
		if (startEpoch*1e6 > traceStartTime) || (endEpoch*1e6 < traceEndTime) {
			continue
		}

		searchRequestBody.SearchText = "trace_id=" + traceId + " | stats count BY status"
		pipeSearchResponseOuter, err = processSearchRequest(searchRequestBody, myid)
		if err != nil {
			log.Errorf("ProcessSearchTracesRequest: traceId:%v, Error=%v", traceId, err)
			continue
		}

		AddTrace(pipeSearchResponseOuter, &traces, traceId, traceStartTime, traceEndTime, serviceName.(string), operationName.(string))
	}

	traceResult := &structs.TraceResult{
		Traces: traces,
	}

	putils.WriteJsonResponse(ctx, traceResult)
	ctx.SetStatusCode(fasthttp.StatusOK)
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
		putils.SetBadMsg(ctx, "")
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
	if config.IsNewQueryPipelineEnabled() {
		return pipeSearchResponseOuter.BucketCount
	}
	return len(pipeSearchResponseOuter.Aggs[""].Buckets)
}

func GetUniqueTraceIds(pipeSearchResponseOuter *segstructs.PipeSearchResponseOuter, startEpoch uint64, endEpoch uint64, page int) []string {
	if config.IsNewQueryPipelineEnabled() {
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

	if len(pipeSearchResponseOuter.Aggs[""].Buckets) < (page-1)*TRACE_PAGE_LIMIT {
		return []string{}
	}

	endIndex := page * TRACE_PAGE_LIMIT
	if endIndex > len(pipeSearchResponseOuter.Aggs[""].Buckets) {
		endIndex = len(pipeSearchResponseOuter.Aggs[""].Buckets)
	}

	traceIds := make([]string, 0)
	// Only Process up to 50 traces per page
	for _, bucket := range pipeSearchResponseOuter.Aggs[""].Buckets[(page-1)*TRACE_PAGE_LIMIT : endIndex] {
		traceId, exists := bucket["key"]
		if !exists {
			continue
		}
		traceIds = append(traceIds, traceId.(string))
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

func AddTrace(pipeSearchResponseOuter *segstructs.PipeSearchResponseOuter, traces *[]*structs.Trace, traceId string, traceStartTime uint64,
	traceEndTime uint64, serviceName string, operationName string) {
	spanCnt := 0
	errorCnt := 0
	if config.IsNewQueryPipelineEnabled() {
		for _, bucket := range pipeSearchResponseOuter.MeasureResults {
			if len(bucket.GroupByValues) == 1 {
				statusCode := bucket.GroupByValues[0]
				count, exists := bucket.MeasureVal["count(*)"]
				if !exists {
					log.Error("AddTrace: Unable to extract 'count(*)' from measure results")
					return
				}
				countVal, isFloat := count.(float64)
				if !isFloat {
					log.Error("AddTrace: count is not a float64")
					return
				}
				spanCnt += int(countVal)
				if statusCode == string(structs.Status_STATUS_CODE_ERROR) {
					errorCnt += int(countVal)
				}
			}
		}
	} else {
		for _, bucket := range pipeSearchResponseOuter.Aggs[""].Buckets {
			statusCode, exists := bucket["key"].(string)
			if !exists {
				log.Error("AddTrace: Unable to extract 'key' from bucket Map")
				return
			}
			countMap, exists := bucket["count(*)"].(map[string]interface{})
			if !exists {
				log.Error("AddTrace: Unable to extract 'count(*)' from bucket Map")
				return
			}
			countFloat64, exists := countMap["value"].(float64)
			if !exists {
				log.Error("AddTrace: Unable to extract 'value' from bucket Map")
				return
			}

			count := int(countFloat64)
			spanCnt += count
			if statusCode == string(structs.Status_STATUS_CODE_ERROR) {
				errorCnt += count
			}
		}
	}

	trace := &structs.Trace{
		TraceId:         traceId,
		StartTime:       traceStartTime,
		EndTime:         traceEndTime,
		SpanCount:       spanCnt,
		SpanErrorsCount: errorCnt,
		ServiceName:     serviceName,
		OperationName:   operationName,
	}

	*traces = append(*traces, trace)
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
	var jsParsingStackbuf [putils.UnescapeStackBufSize]byte

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
			redMetrics.P50 = utils.FindPercentileData(durations, 50)
			redMetrics.P90 = utils.FindPercentileData(durations, 90)
			redMetrics.P95 = utils.FindPercentileData(durations, 95)
			redMetrics.P99 = utils.FindPercentileData(durations, 99)
		}

		serviceToMetrics[service] = redMetrics

		jsonData, err := redMetricsToJson(redMetrics, service)
		if err != nil {
			log.Errorf("ProcessRedTracesIngest: failed to marshal redMetrics=%v: Error=%v", redMetrics, err)
			continue
		}

		// Setup ingestion parameters
		now := putils.GetCurrentTimeInMs()

		ple, err := segwriter.GetNewPLE(jsonData, now, indexName, &tsKey, jsParsingStackbuf[:])
		if err != nil {
			log.Errorf("ProcessRedTracesIngest: failed to get new PLE: %v", err)
			continue
		}
		pleArray = append(pleArray, ple)
		numBytes += len(jsonData)
	}

	localIndexMap := make(map[string]string)
	tsNow := putils.GetCurrentTimeInMs()

	err := writer.ProcessIndexRequestPle(tsNow, indexName, shouldFlush, localIndexMap,
		myid, 0, idxToStreamIdCache, cnameCacheByteHashToStr,
		jsParsingStackbuf[:], pleArray)
	if err != nil {
		log.Errorf("ProcessRedTracesIngest: failed to process ingest request: %v", err)
		return
	}

	usageStats.UpdateTracesStats(uint64(numBytes), uint64(len(pleArray)), myid)
}

func absoluteTimeFormat(timeStr string) string {
	if strings.Contains(timeStr, "-") && strings.Count(timeStr, "-") == 1 {
		timeStr = strings.Replace(timeStr, "-", " ", 1)
	}
	timeStr = strings.Replace(timeStr, "/", "-", 2)
	if strings.Contains(timeStr, ":") {
		if strings.Count(timeStr, ":") < 2 {
			timeStr += ":00"
		}
	}
	return timeStr
}

func parseTimeFromString(timeStr string) (uint32, error) {
	// if it is not a relative time, parse as absolute time
	var t time.Time
	var err error
	// unixtime
	if unixTime, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
		if putils.IsTimeInMilli(uint64(unixTime)) {
			return uint32(unixTime / 1e3), nil
		}
		return uint32(unixTime), nil
	}

	//absolute time formats
	timeStr = absoluteTimeFormat(timeStr)
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, format := range formats {
		t, err = time.Parse(format, timeStr)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("parseTime: invalid time format: %s. Error: %v", timeStr, err)
		return 0, err
	}
	return uint32(t.Unix()), nil
}

func parseAlphaNumTime(nowTs uint64, inp string, defValue uint64) (uint64, usageStats.UsageStatsGranularity) {
	granularity := usageStats.Daily
	sanTime := strings.ReplaceAll(inp, " ", "")

	if sanTime == "now" {
		return nowTs, usageStats.Hourly
	}

	retVal := defValue

	strln := len(sanTime)
	if strln < 6 {
		return retVal, usageStats.Daily
	}

	unit := sanTime[strln-1]
	num, err := strconv.ParseInt(sanTime[4:strln-1], 0, 64)
	if err != nil {
		return defValue, usageStats.Daily
	}

	switch unit {
	case 'm':
		retVal = nowTs - putils.MIN_IN_MS*uint64(num)
		granularity = usageStats.Hourly
	case 'h':
		retVal = nowTs - putils.HOUR_IN_MS*uint64(num)
		granularity = usageStats.Hourly
	case 'd':
		retVal = nowTs - putils.DAY_IN_MS*uint64(num)
		granularity = usageStats.Daily
	default:
		log.Errorf("parseAlphaNumTime: Unknown time unit %v", unit)
	}
	return retVal, granularity
}

func parseTimeStringToUint32(s interface{}) (uint32, error) {
	var startTimeStr string
	var timeVal uint32

	switch valtype := s.(type) {
	case int:
		startTimeStr = fmt.Sprintf("%d", valtype)
	case float64:
		startTimeStr = fmt.Sprintf("%d", int64(valtype))
	case string:
		if strings.Contains(s.(string), "now") {
			nowTs := putils.GetCurrentTimeInMs()
			defValue := nowTs - (1 * 60 * 1000)
			pastXhours, _ := parseAlphaNumTime(nowTs, s.(string), defValue)
			startTimeStr = fmt.Sprintf("%d", pastXhours)
		} else {
			startTimeStr = valtype
		}
	default:
		return timeVal, errors.New("Failed to parse time from JSON request body.TimeField is not a string!")
	}
	timeVal, err := parseTimeFromString(startTimeStr)
	if err != nil {
		return timeVal, err
	}
	return timeVal, nil
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

func ParseRedMetricsRequest(rawJSON []byte) (uint32, uint32, string, string, map[string]interface{}, error) {
	var start, end uint32
	var serviceName, joinOperator string
	errorLog := " "
	readJSON := make(map[string]interface{})
	var err error
	var respBodyErr error

	// JSON parsing

	jsonc := jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := jsonc.NewDecoder(bytes.NewReader(rawJSON))
	err = decoder.Decode(&readJSON)
	if err != nil {
		respBodyErr = errors.New("failed to parse request body")
		errorLog = fmt.Sprintf("Invalid JSON: %v, error: %v", string(rawJSON), err)
		return start, end, serviceName, errorLog, nil, respBodyErr
	}

	// Parse startTime and endTime
	start, err = parseTimeStringToUint32(readJSON["startTime"])
	if err != nil {
		respBodyErr = errors.New("failed to parse startTime")
		errorLog = "failed to parse startTime"
		return start, end, serviceName, errorLog, readJSON, respBodyErr
	}

	end, err = parseTimeStringToUint32(readJSON["endTime"])
	if err != nil {
		respBodyErr = errors.New("failed to parse endTime")
		errorLog = "failed to parse endTime"
		return start, end, serviceName, errorLog, readJSON, respBodyErr
	}
	// Parse serviceName
	serviceName, ok := readJSON["serviceName"].(string)
	if !ok {
		respBodyErr = errors.New("failed to parse serviceName")
		errorLog = "serviceName is missing or not a string"
		return start, end, serviceName, errorLog, readJSON, respBodyErr
	}

	// Parse query parameters
	query, ok := readJSON["query"].(map[string]interface{})
	if !ok {
		respBodyErr = errors.New("failed to parse query field")
		errorLog = "query field is missing or not an object"
		return start, end, serviceName, errorLog, readJSON, respBodyErr
	}

	joinOperator = "OR"
	if v, ok := query["JoinOperator"].(string); ok {
		if v == "AND" || v == "OR" {
			joinOperator = v
		} else {
			respBodyErr = errors.New("invalid JoinOperator value")
			errorLog = "JoinOperator must be 'AND' or 'OR'"
			return start, end, serviceName, errorLog, readJSON, respBodyErr
		}
	}

	// Extract RED metrics values
	var redMetrics structs.RedMetrics
	if v, ok := query["RatePerSec"].(float64); ok {
		redMetrics.Rate = v
	}
	if v, ok := query["ErrorPercentage"].(float64); ok {
		redMetrics.ErrorRate = v
	}
	if v, ok := query["DurationP50Ms"].(float64); ok {
		redMetrics.P50 = v
	}
	if v, ok := query["DurationP90Ms"].(float64); ok {
		redMetrics.P90 = v
	}
	if v, ok := query["DurationP95Ms"].(float64); ok {
		redMetrics.P95 = v
	}
	if v, ok := query["DurationP99Ms"].(float64); ok {
		redMetrics.P99 = v
	}

	// Convert RedMetrics struct to JSON
	redMetricsJSON, err := redMetricsToJson(redMetrics, serviceName)
	if err != nil {
		respBodyErr = errors.New("failed to serialize RedMetrics to JSON")
		errorLog = fmt.Sprintf("Error serializing RedMetrics: %v", err)
		return start, end, serviceName, errorLog, readJSON, respBodyErr
	}
	redMetricsMap := make(map[string]interface{})
	err = json.Unmarshal(redMetricsJSON, &redMetricsMap)
	if err != nil {
		respBodyErr = errors.New("failed to convert JSON to map")
		errorLog = fmt.Sprintf("Error converting JSON to map: %v", err)
		return start, end, serviceName, errorLog, readJSON, respBodyErr
	}

	// Include JoinOperator in response
	redMetricsMap["join_operator"] = joinOperator

	return start, end, serviceName, errorLog, redMetricsMap, nil
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
	now := putils.GetCurrentTimeInMs()
	indexName := "service-dependency"
	shouldFlush := false
	localIndexMap := make(map[string]string)
	tsKey := config.GetTimeStampKey()

	idxToStreamIdCache := make(map[string]string)
	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [putils.UnescapeStackBufSize]byte
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
	searchRequestBody.StartEpoch = readJSON["startEpoch"].(string)
	searchRequestBody.EndEpoch = readJSON["endEpoch"].(string)
	dependencyResponseOuter, err := processSearchRequest(searchRequestBody, myid)
	if err != nil {
		log.Errorf("ProcessAggregatedDependencyGraphs: processSearchRequest: Error=%v", err)
		return
	}
	processedData := make(map[string]interface{})
	if dependencyResponseOuter.Hits.Hits == nil || len(dependencyResponseOuter.Hits.Hits) == 0 {
		ctx.SetStatusCode(fasthttp.StatusOK)
		_, writeErr := ctx.WriteString("no dependencies graphs have been generated")
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

// ProcessGeneratedDepGraph handles the /generate-dep-graph endpoint.
// It generates a new dependency graph based on the provided start and end epochs and displays it without storing.
func ProcessGeneratedDepGraph(ctx *fasthttp.RequestCtx, myid int64) {
	// Extract startEpoch and endEpoch from the request
	_, readJSON, err := ParseAndValidateRequestBody(ctx)
	if err != nil {
		log.Errorf("ProcessGeneratedDepGraph: Unable to Parse Request Body, Error=%v", err)
		return
	}

	nowTs := putils.GetCurrentTimeInMs()
	_, startEpoch, endEpoch, _, _, _, _ := pipesearch.ParseSearchBody(readJSON, nowTs)

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
		putils.SetBadMsg(ctx, "")
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
		searchRequestBody.From += 1000
	}

	res, err := utils.BuildSpanTree(idToSpanMap, idToParentId)
	if err != nil {
		writeErrMsg(ctx, "ProcessGanttChartRequest", err.Error(), nil)
		return
	}

	putils.WriteJsonResponse(ctx, res)
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
		log.Errorf(functionName, ": could not write error message err=%v", err)
	}
	log.Errorf(functionName, ": failed to decode search request body! Err=%v", err)
}

func ProcessSpanGanttChartRequest(ctx *fasthttp.RequestCtx, myid int64) {
	searchRequestBody, readJSON, err := ParseAndValidateRequestBody(ctx)
	if err != nil {
		writeErrMsg(ctx, "ProcessSpanGanttChartRequest", "could not parse and validate request body", err)
		return
	}

	nowTs := putils.GetCurrentTimeInMs()
	searchText, startEpoch, endEpoch, _, _, _, _ := pipesearch.ParseSearchBody(readJSON, nowTs)

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
