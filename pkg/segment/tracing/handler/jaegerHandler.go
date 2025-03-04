package handler

import (
	"encoding/json"

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

	requestBody := structs.SearchRequestBody{
		SearchText:    "SELECT DISTINCT `service` FROM `traces`",
		StartEpoch:    "now-1h",
		EndEpoch:      "now",
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
	_ = json.Unmarshal(rawTraceCtx.Response.Body(), &pipeSearchResponseOuter)

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
		Errors: []string{},
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, finalResponse)

}

func ProcessGetOperations(ctx *fasthttp.RequestCtx, myid int64) {
	serviceName := utils.ExtractParamAsString(ctx.UserValue("serviceName"))

	searchRequestBody := structs.SearchRequestBody{
		SearchText:    "SELECT DISTINCT `name` FROM `traces`",
		StartEpoch:    "now-1h",
		EndEpoch:      "now",
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
	_ = json.Unmarshal(rawTraceCtx.Response.Body(), &pipeSearchResponseOuter)

	serviceSet := make(map[string]bool)
	for _, record := range pipeSearchResponseOuter.Hits.Hits {
		if service, exists := record["name"].(string); exists {
			if sn, ok := record["service"].(string); ok && sn == serviceName {
				serviceSet[service] = true
			}
		}
	}

	var distinctServicesName []string
	for service := range serviceSet {
		distinctServicesName = append(distinctServicesName, service)
	}

	finalResponse := ResponseBody{
		Data:   distinctServicesName,
		Total:  len(distinctServicesName),
		Limit:  0,
		Offset: 0,
		Errors: []string{},
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
	utils.WriteJsonResponse(ctx, finalResponse)

}
