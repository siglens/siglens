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
	"fmt"

	writer "github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

func ProcessSplunkHecIngestRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	responseBody := make(map[string]interface{})
	body, err := utils.GetDecodedBody(ctx)
	if err != nil {
		log.Errorf("ProcessSplunkHecIngestRequest: Unable to decode request body, err=%v", err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = "Unable to decode request body"
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	jsonObjects, err := utils.ExtractSeriesOfJsonObjects(body)
	if err != nil {
		log.Errorf("ProcessSplunkHecIngestRequest: Unable to extract json objects from request body, err=%v", err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		responseBody["error"] = "Unable to extract json objects from request body"
		utils.WriteJsonResponse(ctx, responseBody)
		return
	}

	for _, record := range jsonObjects {
		err, statusCode := handleSingleRecord(record, myid)
		if err != nil {
			log.Errorf("ProcessSplunkHecIngestRequest: Failed to handle record, err=%v", err)
			ctx.SetStatusCode(statusCode)
			responseBody["error"] = err.Error()
			utils.WriteJsonResponse(ctx, responseBody)
			return
		}
	}

	responseBody["status"] = "Success"
	utils.WriteJsonResponse(ctx, responseBody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func handleSingleRecord(record map[string]interface{}, myid uint64) (error, int) {
	if record["index"] == "" || record["index"] == nil {
		record["index"] = "default"
	}

	indexNameIn, ok := record["index"].(string)
	if !ok {
		return fmt.Errorf("Index field should be a string"), fasthttp.StatusBadRequest
	}

	recordAsBytes, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("Failed to marshal record to string"), fasthttp.StatusBadRequest
	}
	recordAsString := string(recordAsBytes)

	tsNow := utils.GetCurrentTimeInMs()
	if !vtable.IsVirtualTablePresent(&indexNameIn, myid) {
		log.Infof("handleSingleRecord: Index name %v does not exist. Adding virtual table name and mapping.", indexNameIn)

		err := vtable.AddVirtualTable(&indexNameIn, myid)
		if err != nil {
			return fmt.Errorf("Failed to add virtual table for index"), fasthttp.StatusServiceUnavailable
		}

		err = vtable.AddMappingFromADoc(&indexNameIn, &recordAsString, myid)
		if err != nil {
			return fmt.Errorf("Failed to add mapping from a doc for index"), fasthttp.StatusServiceUnavailable
		}
	}

	localIndexMap := make(map[string]string)
	err = writer.ProcessIndexRequest(recordAsBytes, tsNow, indexNameIn, uint64(len(recordAsString)), false, localIndexMap, myid)
	if err != nil {
		return fmt.Errorf("Failed to add entry to in mem buffer"), fasthttp.StatusServiceUnavailable
	}

	return nil, fasthttp.StatusOK
}
