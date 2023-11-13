package handler

import (
	"bytes"
	"encoding/json"
	"regexp"
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

func ProcessSearchTracesRequest(ctx *fasthttp.RequestCtx, myid uint64) {

	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf(" ProcessPipeSearchRequest: received empty search request body ")
		pipesearch.SetBadMsg(ctx)
		return
	}

	readJSON := make(map[string]interface{})
	var jsonc = jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := jsonc.NewDecoder(bytes.NewReader(rawJSON))
	decoder.UseNumber()
	err := decoder.Decode(&readJSON)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("ProcessPipeSearchRequest: could not write error message err=%v", err)
		}
		log.Errorf("ProcessPipeSearchRequest: failed to decode search request body! Err=%v", err)
	}

	nowTs := putils.GetCurrentTimeInMs()
	searchText, startEpoch, endEpoch, _, _, _ := pipesearch.ParseSearchBody(readJSON, nowTs)
	if err != nil {
		log.Errorf("ProcessSearchTracesRequest: failed to parse search body  err=%v", err)

		_, wErr := ctx.WriteString(err.Error())
		if wErr != nil {
			log.Errorf("ProcessSearchTracesRequest: could not write error message! %v", wErr)
		}
		return
	}

	// Parse the JSON data from ctx.PostBody
	var requestData map[string]interface{}
	if err := json.Unmarshal(ctx.PostBody(), &requestData); err != nil {
		log.Errorf("ProcessSearchTracesRequest: could not unmarshal json body, err=%v", err)
		return
	}

	requestData["queryLanguage"] = "Splunk QL"

	isOnlyTraceID, traceId := ExtractTraceID(searchText)
	traceIds := make([]string, 0)
	pipeSearchResponseOuter := pipesearch.PipeSearchResponseOuter{}

	if isOnlyTraceID {
		traceIds = append(traceIds, traceId)
	} else {
		// In order to get unique trace_id,  append group by block to the "searchText" field
		if searchText, exists := requestData["searchText"]; exists {
			if str, ok := searchText.(string); ok {
				requestData["searchText"] = str + " | stats count BY trace_id"
			}
		} else {
			log.Errorf("ProcessSearchTracesRequest: request does not contain required parameter: searchText")
			return
		}
		modifiedData, err := json.Marshal(requestData)
		if err != nil {
			log.Errorf("ProcessSearchTracesRequest: could not marshal to json body, err=%v", err)
			return
		}

		// Get initial data
		rawTraceCtx := &fasthttp.RequestCtx{}
		rawTraceCtx.Request.SetBody(modifiedData)
		pipesearch.ProcessPipeSearchRequest(rawTraceCtx, myid)

		// Parse initial data
		if err := json.Unmarshal(rawTraceCtx.Response.Body(), &pipeSearchResponseOuter); err != nil {
			log.Errorf("ProcessSearchTracesRequest: could not unmarshal json body, err=%v", err)
			return
		}
		traceIds = GetUniqueTraceIds(&pipeSearchResponseOuter, startEpoch, endEpoch)
	}

	traces := make([]*structs.Trace, 0)
	// Get status code count for each trace
	for _, traceId := range traceIds {
		requestData["searchText"] = "trace_id=" + traceId + " | stats count BY status"
		rawTraceCtx := &fasthttp.RequestCtx{}
		modifiedData, err := json.Marshal(requestData)
		if err != nil {
			log.Errorf("ProcessSearchTracesRequest: could not marshal to json body for trace=%v, err=%v", traceId, err)
			continue
		}
		rawTraceCtx.Request.SetBody(modifiedData)
		pipesearch.ProcessPipeSearchRequest(rawTraceCtx, myid)
		pipeSearchResponseOuter := pipesearch.PipeSearchResponseOuter{}
		if err := json.Unmarshal(rawTraceCtx.Response.Body(), &pipeSearchResponseOuter); err != nil {
			log.Errorf("ProcessSearchTracesRequest: could not unmarshal json body for trace=%v, err=%v", traceId, err)
			continue
		}
		// To be modified
		// Only process traces which start and end in this period [startEpoch, endEpoch]
		// if (startEpoch*1e6 > uint64(startTime)) || (endEpoch*1e6 < uint64(endTime)) {
		// 	continue
		// }
		AddTrace(&pipeSearchResponseOuter, &traces, traceId)
	}

	traceResult := &structs.TraceResult{
		Traces: traces,
	}

	putils.WriteJsonResponse(ctx, traceResult)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func GetUniqueTraceIds(pipeSearchResponseOuter *pipesearch.PipeSearchResponseOuter, startEpoch uint64, endEpoch uint64) []string {
	traceIds := make([]string, 0)
	for _, bucketHolder := range pipeSearchResponseOuter.MeasureResults {
		traceIds = append(traceIds, bucketHolder.GroupByValues[0])
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

func AddTrace(pipeSearchResponseOuter *pipesearch.PipeSearchResponseOuter, traces *[]*structs.Trace, traceId string) {
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
		TraceId: traceId,
		// StartTime: , // to be finished
		// EndTime: , // to be finished
		SpanCount:       spanCnt,
		SpanErrorsCount: errorCnt,
	}

	*traces = append(*traces, trace)

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
	ctx := &fasthttp.RequestCtx{}
	requestDataMap := make(map[string]interface{})
	requestDataMap["queryLanguage"] = "Splunk QL"
	requestDataMap["searchText"] = ""
	requestDataMap["startEpoch"] = "now-5m"
	requestDataMap["endEpoch"] = "now"

	requestData, err := json.Marshal(requestDataMap)
	if err != nil {
		log.Errorf("ProcessRedTracesIngest: could not marshal to json body, err=%v", err)
		return
	}

	ctx.Request.SetBody(requestData)

	// Get initial data
	pipesearch.ProcessPipeSearchRequest(ctx, 0)

	// Parse initial data
	rawSpanData := structs.RawSpanData{}
	if err := json.Unmarshal(ctx.Response.Body(), &rawSpanData); err != nil {
		log.Errorf("ProcessRedTracesIngest: could not unmarshal json body, err=%v", err)
		return
	}

	spanIDtoService := make(map[string]string)
	entrySpans := make([]*structs.Span, 0)
	serviceToSpanCnt := make(map[string]int)
	serviceToErrSpanCnt := make(map[string]int)
	serviceToSpanDuration := make(map[string][]uint64)

	// Map from the service name to the RED metrics
	serviceToMetrics := make(map[string]structs.RedMetrics)

	for _, span := range rawSpanData.Hits.Spans {
		spanIDtoService[span.SpanID] = span.Service
	}

	// Get entry spans
	for _, span := range rawSpanData.Hits.Spans {

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
			ErrorRate: float64(errSpanCnt) / float64(spanCnt),
		}

		durations, exists := serviceToSpanDuration[service]
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
