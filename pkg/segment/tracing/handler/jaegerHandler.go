package handler

import (
	"encoding/json"

	log "github.com/sirupsen/logrus"

	"github.com/siglens/siglens/pkg/ast/pipesearch"
	segstructs "github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/tracing/structs"
	"github.com/siglens/siglens/pkg/utils"
	"github.com/valyala/fasthttp"
)

type ResponseBody struct {
	Data   []string `json:"data"`
	Total  int      `json:"total"`
	Limit  int      `json:"limit"`
	Offset int      `json:"offset"`
	Errors []string `json:"errors"`
}

type SearchResponse struct {
	Hits struct {
		Records []map[string]interface{} `json:"records"`
	} `json:"hits"`
}

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
	err := json.Unmarshal(rawTraceCtx.Response.Body(), &pipeSearchResponseOuter)

	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		log.Errorf("Error parsing response body: %v", err)
		return
	}

	serviceSet := make(map[string]bool)
	for _, record := range pipeSearchResponseOuter.Hits.Hits {
		if service, exists := record["service"].(string); exists {
			serviceSet[service] = true
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
	err := json.Unmarshal(rawTraceCtx.Response.Body(), &pipeSearchResponseOuter)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		log.Errorf("Error parsing response body: %v", err)
		return
	}

	nameSet := make(map[string]bool)
	for _, record := range pipeSearchResponseOuter.Hits.Hits {
		if name, exists := record["name"].(string); exists {
			if sn, ok := record["service"].(string); ok && sn == serviceName {
				nameSet[name] = true
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
