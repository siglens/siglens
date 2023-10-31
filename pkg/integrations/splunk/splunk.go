/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package splunk

import (
	"encoding/json"

	writer "github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

func ProcessSplunkHecIngestRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	postBody := make(map[string]interface{})
	err := json.Unmarshal(ctx.PostBody(), &postBody)
	responsebody := make(map[string]interface{})
	if err != nil {
		log.Errorf("ProcessSplunkHecIngestRequest: Unable to parse JSON request body, err=%v", err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responsebody["error"] = "Unable to parse JSON request body"
		utils.WriteJsonResponse(ctx, responsebody)
		return
	}
	if postBody["index"] == "" || postBody["index"] == nil {
		log.Errorf("ProcessSplunkHecIngestRequest: Index field is required")
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responsebody["error"] = "Index field is required"
		utils.WriteJsonResponse(ctx, responsebody)
		return
	}
	indexNameIn, ok := postBody["index"].(string)
	if !ok {
		log.Errorf("ProcessSplunkHecIngestRequest: Index field should be a string")
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responsebody["error"] = "Index field should be a string"
		utils.WriteJsonResponse(ctx, responsebody)
		return
	}
	tsNow := utils.GetCurrentTimeInMs()
	if !vtable.IsVirtualTablePresent(&indexNameIn, myid) {
		log.Infof("ProcessSplunkHecIngestRequest: Index name %v does not exist. Adding virtual table name and mapping.", indexNameIn)
		body := string(ctx.PostBody())
		err := vtable.AddVirtualTable(&indexNameIn, myid)
		if err != nil {
			ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
			responsebody["error"] = "Failed to add virtual table for index"
			utils.WriteJsonResponse(ctx, responsebody)
			return
		}
		err = vtable.AddMappingFromADoc(&indexNameIn, &body, myid)
		if err != nil {
			ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
			responsebody["error"] = "Failed to add mapping from a doc for index"
			utils.WriteJsonResponse(ctx, responsebody)
			return
		}
	}
	localIndexMap := make(map[string]string)
	err = writer.ProcessIndexRequest(ctx.PostBody(), tsNow, indexNameIn, uint64(len(ctx.PostBody())), false, localIndexMap, myid)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
		responsebody["error"] = "Failed to add entry to in mem buffer"
		utils.WriteJsonResponse(ctx, responsebody)
		return
	}
	responsebody["status"] = "Success"
	utils.WriteJsonResponse(ctx, responsebody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}
