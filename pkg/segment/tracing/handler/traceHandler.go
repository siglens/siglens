package handler

import (
	"bytes"
	"encoding/json"
	"reflect"
	"regexp"
	"strings"

	jsoniter "github.com/json-iterator/go"
	pipesearch "github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/segment/tracing/structs"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

func ProcessSearchTracesRequest(ctx *fasthttp.RequestCtx, myid uint64) {

	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf("ProcessSearchTracesRequest: received empty search request body ")
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
			log.Errorf("ProcessSearchTracesRequest: could not write error message err=%v", err)
		}
		log.Errorf("ProcessSearchTracesRequest: failed to decode search request body! Err=%v", err)
	}

	nowTs := utils.GetCurrentTimeInMs()
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
	searchRequestBody := &structs.SearchRequestBody{}
	if err := json.Unmarshal(ctx.PostBody(), &searchRequestBody); err != nil {
		log.Errorf("ProcessSearchTracesRequest: could not unmarshal json body, err=%v", err)
		return
	}

	searchRequestBody.QueryLanguage = "Splunk QL"
	isOnlyTraceID, traceId := ExtractTraceID(searchText)
	traceIds := make([]string, 0)
	pipeSearchResponseOuter := pipesearch.PipeSearchResponseOuter{}

	if isOnlyTraceID {
		traceIds = append(traceIds, traceId)
	} else {
		// In order to get unique trace_id,  append group by block to the "searchText" field
		if len(searchRequestBody.SearchText) > 0 {
			searchRequestBody.SearchText = searchRequestBody.SearchText + " | stats count BY trace_id"
		} else {
			log.Errorf("ProcessSearchTracesRequest: request does not contain required parameter: searchText")
			return
		}

		// Because ProcessPipeSearchRequest() will parse request body into a map, so we parse searchRequestBody into a map
		requestData, err := structToMap(searchRequestBody)
		if err != nil {
			log.Errorf("ProcessSearchTracesRequest: err=%v", err)
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
		searchRequestBody.SearchText = "trace_id=" + traceId + " | stats count BY status_code"
		rawTraceCtx := &fasthttp.RequestCtx{}
		requestData, err := structToMap(searchRequestBody)
		if err != nil {
			log.Errorf("ProcessSearchTracesRequest: err=%v", err)
			return
		}
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

	utils.WriteJsonResponse(ctx, traceResult)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func GetUniqueTraceIds(pipeSearchResponseOuter *pipesearch.PipeSearchResponseOuter, startEpoch uint64, endEpoch uint64) []string {
	traceIds := make([]string, 0)
	for _, bucket := range pipeSearchResponseOuter.Aggs[""].Buckets {
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

// structToMap converts a struct to a map, using lowercase field names as keys.
func structToMap(obj interface{}) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	val := reflect.Indirect(reflect.ValueOf(obj))
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldName := typ.Field(i).Name

		// If a JSON tag exists, use it as the key
		if tag := typ.Field(i).Tag.Get("json"); tag != "" {
			result[tag] = field.Interface()
		} else {
			// Otherwise, use the modified field name as the key
			// Change the first letter of the field name to lowercase
			lowercaseFieldName := strings.ToLower(fieldName[:1]) + fieldName[1:]
			result[lowercaseFieldName] = field.Interface()
		}
	}

	return result, nil
}
