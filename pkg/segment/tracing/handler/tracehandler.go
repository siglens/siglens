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
	pipesearch "github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/segment/tracing/structs"
	"github.com/siglens/siglens/pkg/segment/tracing/utils"
	putils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

const OneHourInMs = 60 * 60 * 1000

func ProcessSearchTracesRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	searchRequestBody, readJSON, err := ParseAndValidateRequestBody(ctx)
	if err != nil {
		writeErrMsg(ctx, "ProcessSearchTracesRequest", "could not parse and validate request body", err)
		return
	}

	nowTs := putils.GetCurrentTimeInMs()
	searchText, startEpoch, endEpoch, _, _, _ := pipesearch.ParseSearchBody(readJSON, nowTs)

	page := 1
	pageVal, ok := readJSON["page"]
	if !ok || pageVal == 0 {
		page = 1
	} else {
		switch val := pageVal.(type) {
		case json.Number:
			pageInt, err := val.Int64()
			if err != nil {
				log.Errorf("ProcessSearchTracesRequest: error converting page to int: %v", err)
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
			searchRequestBody.SearchText = searchRequestBody.SearchText + " | stats count BY trace_id"
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
			log.Errorf("ProcessSearchTracesRequest: traceId:%v, %v", traceId, err)
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
			log.Errorf("ProcessSearchTracesRequest: traceId:%v, %v", traceId, err)
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

func ProcessTotalTracesRequest(ctx *fasthttp.RequestCtx, myid uint64) {
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
		log.Errorf("Received empty search request body")
		putils.SetBadMsg(ctx, "")
		return nil, nil, errors.New("Received empty search request body")
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

func GetTotalUniqueTraceIds(pipeSearchResponseOuter *pipesearch.PipeSearchResponseOuter) int {
	return len(pipeSearchResponseOuter.Aggs[""].Buckets)
}
func GetUniqueTraceIds(pipeSearchResponseOuter *pipesearch.PipeSearchResponseOuter, startEpoch uint64, endEpoch uint64, page int) []string {
	if len(pipeSearchResponseOuter.Aggs[""].Buckets) < (page-1)*50 {
		return []string{}
	}

	endIndex := page * 50
	if endIndex > len(pipeSearchResponseOuter.Aggs[""].Buckets) {
		endIndex = len(pipeSearchResponseOuter.Aggs[""].Buckets)
	}

	traceIds := make([]string, 0)
	// Only Process up to 50 traces per page
	for _, bucket := range pipeSearchResponseOuter.Aggs[""].Buckets[(page-1)*50 : endIndex] {
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

func AddTrace(pipeSearchResponseOuter *pipesearch.PipeSearchResponseOuter, traces *[]*structs.Trace, traceId string, traceStartTime uint64,
	traceEndTime uint64, serviceName string, operationName string) {
	spanCnt := 0
	errorCnt := 0
	for _, bucket := range pipeSearchResponseOuter.Aggs[""].Buckets {
		statusCode, exists := bucket["key"].(string)
		if !exists {
			log.Error("AddTrace: Unable to extract 'key' from bucket")
			return
		}
		countMap, exists := bucket["count(*)"].(map[string]interface{})
		if !exists {
			log.Error("AddTrace: Unable to extract 'count(*)' from bucket")
			return
		}
		countFloat64, exists := countMap["value"].(float64)
		if !exists {
			log.Error("AddTrace: Unable to extract 'value' from bucket")
			return
		}

		count := int(countFloat64)
		spanCnt += count
		if statusCode == string(structs.Status_STATUS_CODE_ERROR) {
			errorCnt += count
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
func processSearchRequest(searchRequestBody *structs.SearchRequestBody, myid uint64) (*pipesearch.PipeSearchResponseOuter, error) {

	modifiedData, err := json.Marshal(searchRequestBody)
	if err != nil {
		return nil, fmt.Errorf("processSearchRequest: could not marshal to json body, err=%v", err)
	}

	// Get initial data
	rawTraceCtx := &fasthttp.RequestCtx{}
	rawTraceCtx.Request.Header.SetMethod("POST")
	rawTraceCtx.Request.SetBody(modifiedData)
	pipesearch.ProcessPipeSearchRequest(rawTraceCtx, myid)
	pipeSearchResponseOuter := pipesearch.PipeSearchResponseOuter{}

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
		ProcessRedTracesIngest()
		time.Sleep(5 * time.Minute)
	}
}

func ProcessRedTracesIngest() {
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

	for {
		ctx := &fasthttp.RequestCtx{}
		requestData, err := json.Marshal(searchRequestBody)
		if err != nil {
			log.Errorf("ProcessRedTracesIngest: could not marshal to json body, err=%v", err)
			return
		}

		ctx.Request.Header.SetMethod("POST")
		ctx.Request.SetBody(requestData)

		// Get initial data
		pipesearch.ProcessPipeSearchRequest(ctx, 0)

		// Parse initial data
		rawSpanData := structs.RawSpanData{}
		if err := json.Unmarshal(ctx.Response.Body(), &rawSpanData); err != nil {
			writeErrMsg(ctx, "ProcessRedTracesIngest", "could not unmarshal json body", err)
			return
		}

		if rawSpanData.Hits.Spans == nil || len(rawSpanData.Hits.Spans) == 0 {
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
			log.Errorf("ProcessRedTracesIngest: failed to marshal redMetrics %v: %v", redMetrics, err)
			continue
		}

		// Setup ingestion parameters
		now := putils.GetCurrentTimeInMs()
		indexName := "red-traces"
		shouldFlush := false
		lenJsonData := uint64(len(jsonData))
		localIndexMap := make(map[string]string)
		orgId := uint64(0)

		// Ingest red metrics
		err = writer.ProcessIndexRequest(jsonData, now, indexName, lenJsonData, shouldFlush, localIndexMap, orgId)
		if err != nil {
			log.Errorf("ProcessRedTracesIngest: failed to process ingest request: %v", err)
			continue
		}

	}
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

		// Calculate startEpoch and endEpoch for the last hour
		endEpoch := time.Now().UnixMilli()
		startEpoch := time.Now().Add(-time.Hour).UnixMilli()

		depMatrix := MakeTracesDependancyGraph(startEpoch, endEpoch)
		writeDependencyMatrix(depMatrix)
	}
}

func MakeTracesDependancyGraph(startEpoch int64, endEpoch int64) map[string]map[string]int {

	requestBody := map[string]interface{}{
		"indexName":     "traces",
		"startEpoch":    startEpoch,
		"endEpoch":      endEpoch,
		"searchText":    "*",
		"queryLanguage": "Splunk QL",
	}
	requestBodyJSON, err := json.Marshal(requestBody)
	if err != nil {
		fmt.Println("Error marshaling request body:", err)
		return nil
	}
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetBody(requestBodyJSON)

	ctx.Request.Header.SetMethod("POST")
	pipesearch.ProcessPipeSearchRequest(ctx, 0)

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

func writeDependencyMatrix(dependencyMatrix map[string]map[string]int) {
	dependencyMatrixJSON, err := json.Marshal(dependencyMatrix)
	if err != nil {
		log.Errorf("Error marshaling dependency matrix:err=%v", err)
		return
	}

	// Setup ingestion parameters
	now := putils.GetCurrentTimeInMs()
	indexName := "service-dependency"
	shouldFlush := false
	lenJsonData := uint64(len((dependencyMatrixJSON)))
	localIndexMap := make(map[string]string)
	orgId := uint64(0)

	// Ingest
	err = writer.ProcessIndexRequest(dependencyMatrixJSON, now, indexName, lenJsonData, shouldFlush, localIndexMap, orgId)
	if err != nil {
		log.Errorf("MakeTracesDependancyGraph: failed to process ingest request: %v", err)

	}
}

// ProcessAggregatedDependencyGraphs handles the /dependencies endpoint.
// It aggregates already computed dependency graphs based on the provided start and end epochs.
func ProcessAggregatedDependencyGraphs(ctx *fasthttp.RequestCtx, myid uint64) {
	// Extract startEpoch and endEpoch from the request
	_, readJSON, err := ParseAndValidateRequestBody(ctx)
	if err != nil {
		log.Errorf("ProcessDependencyRequest: %v", err)
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
		log.Errorf("ProcessSearchRequest: %v", err)
		return
	}
	processedData := make(map[string]interface{})
	if dependencyResponseOuter.Hits.Hits == nil || len(dependencyResponseOuter.Hits.Hits) == 0 {
		ctx.SetStatusCode(fasthttp.StatusOK)
		_, writeErr := ctx.WriteString("no dependencies graphs have been generated")
		if writeErr != nil {
			log.Errorf("ProcessDependencyRequest: Error writing to context: %v", writeErr)
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
			log.Errorf("Error writing to context: %v", writeErr)
		}
		return
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
}

// ProcessGeneratedDepGraph handles the /generate-dep-graph endpoint.
// It generates a new dependency graph based on the provided start and end epochs and displays it without storing.
func ProcessGeneratedDepGraph(ctx *fasthttp.RequestCtx, myid uint64) {
	// Extract startEpoch and endEpoch from the request
	_, readJSON, err := ParseAndValidateRequestBody(ctx)
	if err != nil {
		log.Errorf("ProcessDepgraphRequest: %v", err)
		return
	}

	nowTs := putils.GetCurrentTimeInMs()
	_, startEpoch, endEpoch, _, _, _ := pipesearch.ParseSearchBody(readJSON, nowTs)

	startEpochInt64 := int64(startEpoch)
	endEpochInt64 := int64(endEpoch)

	// Generate the dependency graph
	depMatrix := MakeTracesDependancyGraph(startEpochInt64, endEpochInt64)

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
		_, writeErr := ctx.WriteString(fmt.Sprintf("ProcessDepgraphRequest: Error encoding JSON: %s", err.Error()))
		if writeErr != nil {
			log.Errorf("ProcessDepgraphRequest: Error writing to context: %v", writeErr)
		}
		return
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessGanttChartRequest(ctx *fasthttp.RequestCtx, myid uint64) {

	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf("ProcessGanttChartRequest: received empty search request body ")
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
				log.Errorf("ProcessGanttChartRequest: could not marshal to json body, err=%v", err)
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
