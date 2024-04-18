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

package splunk

import (
	"encoding/json"
	"fmt"

	writer "github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/usageStats"
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
	numBytes := len(recordAsBytes)
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
	usageStats.UpdateStats(uint64(numBytes), 1, myid)

	return nil, fasthttp.StatusOK
}
