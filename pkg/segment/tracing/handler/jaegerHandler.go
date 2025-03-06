package handler

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/siglens/siglens/pkg/ast/pipesearch"
	segstructs "github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/tracing/structs"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

type ResponseBody struct {
	Data   interface{} `json:"data"`
	Total  int         `json:"total"`
	Limit  int         `json:"limit"`
	Offset int         `json:"offset"`
	Errors []string    `json:"errors"`
}
type Process struct {
	ServiceName string `json:"serviceName"`
}

type TraceData struct {
	TraceID   string             `json:"traceID"`
	Spans     []Span             `json:"spans"`
	Processes map[string]Process `json:"processes"`
	Warnings  []string           `json:"warnings"`
}

type Span struct {
	TraceID       string      `json:"traceID"`
	SpanID        string      `json:"spanID"`
	OperationName string      `json:"operationName"`
	References    []Reference `json:"references"`
	StartTime     int64       `json:"startTime"`
	Duration      int64       `json:"duration"`
	Tags          []Tag       `json:"tags"`
	Logs          []string    `json:"logs"`
	ProcessID     string      `json:"processID"`
	Warnings      []string    `json:"warnings"`
}

type Reference struct {
	RefType string `json:"refType"`
	TraceID string `json:"traceID"`
	SpanID  string `json:"spanID"`
}

type Tag struct {
	Key   string      `json:"key"`
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

const NoDependencyGraphsMessage = "no dependencies graphs have been generated"

func ProcessGetServiceName(ctx *fasthttp.RequestCtx, myid int64) {

	startEpoch := string(ctx.QueryArgs().Peek("startEpoch"))
	endEpoch := string(ctx.QueryArgs().Peek("endEpoch"))

	if startEpoch == "" {
		startEpoch = "now-24h"
	}
	if endEpoch == "" {
		endEpoch = "now"
	}

	requestBody := structs.SearchRequestBody{
		SearchText:    "SELECT DISTINCT `service` FROM `traces`",
		StartEpoch:    startEpoch,
		EndEpoch:      endEpoch,
		IndexName:     "traces",
		QueryLanguage: "SQL",
		From:          0,
	}

	modifiedData, _ := json.Marshal(requestBody)

	rawTraceCtx := &fasthttp.RequestCtx{}
	rawTraceCtx.Request.Header.SetMethod("POST")
	rawTraceCtx.Request.SetBody(modifiedData)
	pipesearch.ProcessPipeSearchRequest(rawTraceCtx, myid)
	pipeSearchResponseOuter := segstructs.PipeSearchResponseOuter{}
	responseBody := rawTraceCtx.Response.Body()
	err := json.Unmarshal(responseBody, &pipeSearchResponseOuter)

	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		log.Errorf("Error parsing response body: %v, err: %v", responseBody, err)
		return
	}

	serviceSet := make(map[string]struct{})
	for _, record := range pipeSearchResponseOuter.Hits.Hits {
		if service, exists := record["service"].(string); exists {
			serviceSet[service] = struct{}{}
		}
	}

	var distinctServices []string
	for service := range serviceSet {
		distinctServices = append(distinctServices, service)
	}

	finalResponse := ResponseBody{
		Data:   distinctServices,
		Total:  len(distinctServices),
		Limit:  0,
		Offset: 0,
		Errors: nil,
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, finalResponse)

}

func ProcessGetOperations(ctx *fasthttp.RequestCtx, myid int64) {
	serviceName := utils.ExtractParamAsString(ctx.UserValue("serviceName"))
	startEpoch := string(ctx.QueryArgs().Peek("startEpoch"))
	endEpoch := string(ctx.QueryArgs().Peek("endEpoch"))

	if startEpoch == "" {
		startEpoch = "now-24h"
	}
	if endEpoch == "" {
		endEpoch = "now"
	}
	searchRequestBody := structs.SearchRequestBody{
		SearchText:    "SELECT DISTINCT `name` FROM `traces`",
		StartEpoch:    startEpoch,
		EndEpoch:      endEpoch,
		IndexName:     "traces",
		QueryLanguage: "SQL",
		From:          0,
	}

	modifiedData, _ := json.Marshal(searchRequestBody)

	rawTraceCtx := &fasthttp.RequestCtx{}
	rawTraceCtx.Request.Header.SetMethod("POST")
	rawTraceCtx.Request.SetBody(modifiedData)
	pipesearch.ProcessPipeSearchRequest(rawTraceCtx, myid)
	pipeSearchResponseOuter := segstructs.PipeSearchResponseOuter{}
	responseBody := rawTraceCtx.Response.Body()
	err := json.Unmarshal(responseBody, &pipeSearchResponseOuter)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		log.Errorf("Error parsing response body: %v, err: %v", responseBody, err)
		return
	}

	nameSet := make(map[string]struct{})
	for _, record := range pipeSearchResponseOuter.Hits.Hits {
		if name, exists := record["name"].(string); exists {
			if sn, ok := record["service"].(string); ok && sn == serviceName {
				nameSet[name] = struct{}{}
			}
		}
	}

	var distinctNames []string
	for name := range nameSet {
		distinctNames = append(distinctNames, name)
	}

	finalResponse := ResponseBody{
		Data:   distinctNames,
		Total:  len(distinctNames),
		Limit:  0,
		Offset: 0,
		Errors: nil,
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, finalResponse)

}

func ProcessGetDependencies(ctx *fasthttp.RequestCtx, myid int64) {

	response := ResponseBody{
		Data:   []interface{}{},
		Total:  0,
		Limit:  0,
		Offset: 0,
		Errors: nil,
	}

	endTs := string(ctx.QueryArgs().Peek("endTs"))
	lookback := string(ctx.QueryArgs().Peek("lookback"))

	startEpoch, endEpoch, err := computeStartTime("", endTs, lookback)

	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusOK)
		errors := []string{fmt.Sprintf("Missing required parameter err: %v", err)}
		response.Errors = errors
		utils.WriteJsonResponse(ctx, response)
		log.Errorf("ProcessGetDependencies  : Missing required parameter err : %v ", err)
		return
	}

	startEpoch = convertEpochToMilliseconds(startEpoch)
	endEpoch = convertEpochToMilliseconds(endEpoch)

	searchRequestBody := structs.SearchRequestBody{
		SearchText:    "*",
		StartEpoch:    strconv.FormatInt(startEpoch, 10),
		EndEpoch:      strconv.FormatInt(endEpoch, 10),
		IndexName:     "service-dependency",
		QueryLanguage: "Splunk QL",
		From:          0,
	}

	modifiedData, _ := json.Marshal(searchRequestBody)

	rawTraceCtx := &fasthttp.RequestCtx{}
	rawTraceCtx.Request.Header.SetMethod("POST")
	rawTraceCtx.Request.SetBody(modifiedData)

	ProcessAggregatedDependencyGraphs(rawTraceCtx, myid)
	responseBody := rawTraceCtx.Response.Body()
	processedData := make(map[string]interface{})

	var responseData []map[string]string

	if string(responseBody) == NoDependencyGraphsMessage {
		ctx.SetStatusCode(fasthttp.StatusOK)
		response.Errors = []string{NoDependencyGraphsMessage}
		log.Errorf("ProcessGetDependencies : %v", NoDependencyGraphsMessage)
		utils.WriteJsonResponse(ctx, response)
		return
	}

	if err := json.Unmarshal(responseBody, &processedData); err != nil {
		ctx.SetStatusCode(fasthttp.StatusOK)
		response.Errors = []string{string(responseBody)}
		log.Errorf("ProcessGetDependencies : Error parsing response body: %v, err: %v", string(responseBody), err)
		utils.WriteJsonResponse(ctx, response)
		return
	}

	for parent, children := range processedData {

		if childMap, ok := children.(map[string]interface{}); ok {
			for child, callCount := range childMap {
				var callCountStr string
				switch v := callCount.(type) {
				case int:
					callCountStr = strconv.Itoa(v)
				case float64:
					callCountStr = strconv.FormatFloat(v, 'f', -1, 64)
				default:
					callCountStr = fmt.Sprintf("%v", v)
				}
				responseData = append(responseData, map[string]string{
					"parent":    parent,
					"child":     child,
					"callCount": callCountStr,
				})
			}
		}
	}

	if responseData != nil {
		response.Data = responseData
	}
	response.Total = len(responseData)

	ctx.SetContentType("application/json; charset=utf-8")
	utils.WriteJsonResponse(ctx, response)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessGetTracesSearch(ctx *fasthttp.RequestCtx, myid int64) {

	response := ResponseBody{
		Data:   []interface{}{},
		Total:  0,
		Limit:  0,
		Offset: 0,
		Errors: []string{},
	}

	start := string(ctx.QueryArgs().Peek("start"))
	end := string(ctx.QueryArgs().Peek("end"))
	lookback := string(ctx.QueryArgs().Peek("lookback"))
	service := string(ctx.QueryArgs().Peek("service"))

	startEpoch, endEpoch, err := computeStartTime(start, end, lookback)

	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusOK)
		errors := []string{fmt.Sprintf("Missing required parameter err: %v", err)}
		response.Errors = errors
		utils.WriteJsonResponse(ctx, response)
		log.Errorf("ProcessGetTracesSearch  : Missing required parameter err : %v ", err)
		return
	}

	startEpoch = convertEpochToMilliseconds(startEpoch)
	endEpoch = convertEpochToMilliseconds(endEpoch)

	if service == "" {
		service = "*"
	} else {
		service = "service=" + service
	}
	searchRequestBody := structs.SearchRequestBody{
		SearchText: service,
		StartEpoch: strconv.FormatInt(startEpoch, 10),
		EndEpoch:   strconv.FormatInt(endEpoch, 10),
		From:       0,
	}

	modifiedData, _ := json.Marshal(searchRequestBody)
	rawTraceCtx := &fasthttp.RequestCtx{}
	rawTraceCtx.Request.Header.SetMethod("POST")
	rawTraceCtx.Request.SetBody(modifiedData)

	ProcessSearchTracesRequest(rawTraceCtx, myid)
	pipeSearchResponseOuter := &structs.TraceResult{}
	responseBody := rawTraceCtx.Response.Body()

	if err := json.Unmarshal(responseBody, &pipeSearchResponseOuter); err != nil {
		ctx.SetStatusCode(fasthttp.StatusOK)
		response.Errors = []string{string(responseBody)}
		log.Errorf("ProcessGetTracesSearch : Error parsing response body: %v, err: %v", string(responseBody), err)
		utils.WriteJsonResponse(ctx, response)
		return
	}

	var allTraceData []TraceData
	for _, traces := range pipeSearchResponseOuter.Traces {
		tracesId := traces.TraceId
		searchGanttChartRequestBody := structs.SearchRequestBody{
			SearchText: "trace_id=" + tracesId,
			StartEpoch: "now-365d",
			EndEpoch:   "now",
			From:       0,
		}

		ganttChartModifiedData, _ := json.Marshal(searchGanttChartRequestBody)
		rawGanttChartCtx := &fasthttp.RequestCtx{}
		rawGanttChartCtx.Request.Header.SetMethod("POST")
		rawGanttChartCtx.Request.SetBody(ganttChartModifiedData)
		ganttChartSpanResponseOuter := &structs.GanttChartSpan{}
		ProcessGanttChartRequest(rawGanttChartCtx, myid)
		gCResponseBody := rawGanttChartCtx.Response.Body()

		if err := json.Unmarshal(gCResponseBody, &ganttChartSpanResponseOuter); err != nil {
			log.Errorf("ProcessGetTracesSearch : Error parsing response body: %v, err: %v", string(gCResponseBody), err)
		}
		var spans []Span

		processSpan(ganttChartSpanResponseOuter, tracesId, "", &spans)
		traceData := TraceData{
			TraceID: tracesId,
			Spans:   spans,
		}

		processes := make(map[string]Process)
		traceData.Processes = processes

		allTraceData = append(allTraceData, traceData)

	}

	if allTraceData != nil {
		response.Data = allTraceData
	}

	response.Total = len(allTraceData)

	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, response)

}

func processSpan(span *structs.GanttChartSpan, traceID string, parentSpanID string, spans *[]Span) {
	if span == nil {
		return
	}

	var spanTags []Tag
	for k, v := range span.Tags {
		spanTags = append(spanTags, Tag{
			Key:   k,
			Type:  fmt.Sprintf("%T", v),
			Value: v,
		})
	}

	var references []Reference
	references = append(references, Reference{
		RefType: "CHILD_OF",
		TraceID: traceID,
		SpanID:  parentSpanID,
	})

	spanEntry := Span{
		TraceID:       traceID,
		SpanID:        span.SpanID,
		OperationName: span.OperationName,
		References:    references,
		StartTime:     int64(span.ActualStartTime),
		Duration:      int64(span.Duration),
		Tags:          spanTags,
		Logs:          []string{},
		ProcessID:     "",
		Warnings:      []string{},
	}

	*spans = append(*spans, spanEntry)

	for _, child := range span.Children {
		processSpan(child, traceID, span.SpanID, spans)
	}
}

func computeStartTime(startTs, endTs, lookBack string) (int64, int64, error) {
	if endTs == "" {
		err := fmt.Errorf("ComputeStartTime : failed to process response missing endTs")
		return 0, 0, err
	}

	if lookBack == "" {
		err := fmt.Errorf("ComputeStartTime : failed to process response missing lookBack")
		return 0, 0, err
	}

	endValue, err := strconv.ParseInt(endTs, 10, 64)
	if err != nil {
		log.Errorf("ComputeStartTime : failed to parsing endTs  err : %v", err)
		return 0, 0, err
	}

	if startTs != "" {
		startTsValue, err := strconv.ParseInt(startTs, 10, 64)
		if err == nil {
			return startTsValue, endValue, nil
		}
	}

	lookBackVal, err := strconv.ParseInt(lookBack, 10, 64)
	if err != nil {
		log.Errorf("ComputeStartTime : failed to parsing lookBack err: %v", err)
		return 0, 0, err
	}
	start := endValue - lookBackVal
	return start, endValue, nil
}

func convertEpochToMilliseconds(epoch int64) int64 {
	switch {
	case epoch >= 1e9 && epoch < 1e10:
		return epoch * 1000
	case epoch >= 1e12 && epoch < 1e13:
		return epoch
	case epoch >= 1e15 && epoch < 1e16:
		return epoch / 1000
	default:
		return 0
	}
}
