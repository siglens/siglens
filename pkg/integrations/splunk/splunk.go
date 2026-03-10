// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package splunk

import (
	"encoding/json"
	"fmt"

	"github.com/siglens/siglens/pkg/config"
	writer "github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/grpc"
	"github.com/siglens/siglens/pkg/hooks"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

func ProcessSplunkHecIngestRequest(ctx *fasthttp.RequestCtx, myid int64) {
	if hook := hooks.GlobalHooks.OverrideIngestRequestHook; hook != nil {
		alreadyHandled, err := hook(ctx, myid, grpc.INGEST_FUNC_SPLUNK, false)
		if err != nil {
			utils.SendError(ctx, err.Error(), "", err)
			return
		}
		if alreadyHandled {
			return
		}
	}

	responseBody := make(map[string]interface{})
	body, err := utils.GetDecodedBody(ctx)
	if err != nil {
		utils.SendError(ctx, "Unable to decode request body", "", err)
		return
	}

	jsonObjects, err := utils.ExtractSeriesOfJsonObjects(body)
	if err != nil {
		utils.SendError(ctx, "Unable to read json request", "", err)
		return
	}
	tsKey := config.GetTimeStampKey()

	idxToStreamIdCache := make(map[string]string)
	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [utils.UnescapeStackBufSize]byte
	tsNow := utils.GetCurrentTimeInMs()
	localIndexMap := make(map[string]string)

	pleArray := make([]*segwriter.ParsedLogEvent, 0)
	defer segwriter.ReleasePLEs(pleArray)

	for _, record := range jsonObjects {
		err, statusCode, ple := getPLE(record, myid, &tsKey, jsParsingStackbuf[:])
		if err != nil {
			utils.SendError(ctx, "Failed to ingest a record", fmt.Sprintf("record: %v", record), err)
			ctx.SetStatusCode(statusCode)
			return
		}
		pleArray = append(pleArray, ple)
	}

	pleBatches := utils.ConvertSliceToMap(pleArray, func(ple *segwriter.ParsedLogEvent) string {
		return ple.GetIndexName()
	})

	for indexName, plesInBatch := range pleBatches {
		err = writer.ProcessIndexRequestPle(tsNow, indexName, false, localIndexMap,
			myid, 0, idxToStreamIdCache, cnameCacheByteHashToStr,
			jsParsingStackbuf[:], plesInBatch)
		if err != nil {
			log.Errorf("ProcessSplunkHecIngestRequest: failed to process request, indexName: %v, err: %v", indexName, err)
			responseBody["status"] = fasthttp.StatusServiceUnavailable
			responseBody["message"] = "Failed to process request"
			utils.WriteJsonResponse(ctx, responseBody)
			ctx.SetStatusCode(fasthttp.StatusServiceUnavailable)
			return
		}
		usageStats.UpdateStats(uint64(writer.GetNumOfBytesInPLEs(plesInBatch)), uint64(len(plesInBatch)), myid)
	}

	responseBody["status"] = "Success"
	utils.WriteJsonResponse(ctx, responseBody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func getPLE(record map[string]interface{}, myid int64, tsKey *string, jsParsingStackbuf []byte) (error, int, *segwriter.ParsedLogEvent) {
	if record["index"] == "" || record["index"] == nil {
		record["index"] = "default"
	}

	indexNameIn, ok := record["index"].(string)
	if !ok {
		return fmt.Errorf("Index field should be a string"), fasthttp.StatusBadRequest, nil
	}

	recordAsBytes, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("Failed to marshal record to string"), fasthttp.StatusBadRequest, nil
	}
	recordAsString := string(recordAsBytes)

	tsNow := utils.GetCurrentTimeInMs()
	if !vtable.IsVirtualTablePresent(&indexNameIn, myid) {
		log.Infof("handleSingleRecord: Index name %v does not exist. Adding virtual table name and mapping.", indexNameIn)

		err := vtable.AddVirtualTable(&indexNameIn, myid)
		if err != nil {
			return fmt.Errorf("Failed to add virtual table for index"), fasthttp.StatusServiceUnavailable, nil
		}

		err = vtable.AddMappingFromADoc(&indexNameIn, &recordAsString, myid)
		if err != nil {
			return fmt.Errorf("Failed to add mapping from a doc for index"), fasthttp.StatusServiceUnavailable, nil
		}
	}

	ple, err := segwriter.GetNewPLE(recordAsBytes, tsNow, indexNameIn, tsKey, jsParsingStackbuf[:])
	if err != nil {
		return fmt.Errorf("Failed to get new PLE: %v", err), fasthttp.StatusServiceUnavailable, nil
	}

	return nil, fasthttp.StatusOK, ple
}
