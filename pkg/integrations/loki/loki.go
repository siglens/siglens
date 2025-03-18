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

package loki

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/golang/snappy"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/grpc"
	"github.com/siglens/siglens/pkg/hooks"
	lokilog "github.com/siglens/siglens/pkg/integrations/loki/log"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const (
	LOKIINDEX = "loki-index"
)

func parseLabels(labelsString string) map[string]interface{} {
	labelsString = strings.Trim(labelsString, "{}")
	labelPairs := strings.Split(labelsString, ", ")

	labels := make(map[string]interface{})

	for _, pair := range labelPairs {
		parts := strings.Split(pair, "=")
		if len(parts) == 2 {
			key := strings.Trim(parts[0], "\"")
			value := strings.Trim(parts[1], "\"")
			labels[key] = value
		}
	}

	return labels
}

// See https://grafana.com/docs/loki/latest/reference/loki-http-api/#ingest-logs
func ProcessLokiLogsIngestRequest(ctx *fasthttp.RequestCtx, myid int64) {
	contentType := string(ctx.Request.Header.ContentType())
	switch contentType {
	case "application/json":
		processJsonLogs(ctx, myid)
	case "application/x-protobuf":
		processPromtailLogs(ctx, myid)
	default:
		utils.SendError(ctx, fmt.Sprintf("Unknown content type: %v", contentType), "", nil)
	}
}

// Format of loki logs generated by promtail:
//
//	{
//		"streams": [
//		  {
//			"labels": "{filename=\"test.log\",job=\"test\"}",
//			"entries": [
//			  {
//				"timestamp": "2021-03-31T18:00:00.000Z",
//				"line": "test log line"
//			  }
//			]
//		  }
//		]
//	}
func processPromtailLogs(ctx *fasthttp.RequestCtx, myid int64) {
	if hook := hooks.GlobalHooks.OverrideIngestRequestHook; hook != nil {
		alreadyHandled := hook(ctx, myid, grpc.INGEST_FUNC_LOKI, false)
		if alreadyHandled {
			return
		}
	}
	responsebody := make(map[string]interface{})
	buf, err := snappy.Decode(nil, ctx.PostBody())
	if err != nil {
		utils.SendError(ctx, "Failed to decode request", fmt.Sprintf("body: %s", ctx.PostBody()), err)
		return
	}

	logLine := lokilog.PushRequest{}

	err = proto.Unmarshal(buf, &logLine)
	if err != nil {
		utils.SendError(ctx, "Unable to unmarshal request", "", err)
		return
	}

	logJson := protojson.Format(&logLine)

	tsNow := utils.GetCurrentTimeInMs()
	indexNameIn := LOKIINDEX
	if !vtable.IsVirtualTablePresent(&indexNameIn, myid) {
		log.Errorf("processPromtailLogs: Index name %v does not exist. Adding virtual table name and mapping.", indexNameIn)
		body := logJson
		err = vtable.AddVirtualTable(&indexNameIn, myid)
		if err != nil {
			utils.SendInternalError(ctx, "Failed to add virtual table for index", "", err)
			return
		}
		err = vtable.AddMappingFromADoc(&indexNameIn, &body, myid)
		if err != nil {
			utils.SendInternalError(ctx, "Failed to add mapping from a doc for index", "", err)
			return
		}
	}

	localIndexMap := make(map[string]string)

	var jsonData map[string][]map[string]interface{}
	err = json.Unmarshal([]byte(logJson), &jsonData)
	if err != nil {
		utils.SendError(ctx, "Unable to unmarshal request", "", err)
		return
	}

	streams := jsonData["streams"]

	idxToStreamIdCache := make(map[string]string)
	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [utils.UnescapeStackBufSize]byte

	tsKey := config.GetTimeStampKey()

	pleArray := make([]*segwriter.ParsedLogEvent, 0)
	defer segwriter.ReleasePLEs(pleArray)

	// this loop just so that we can count what we have received
	receivedLogLineCount := uint64(0)
	for _, stream := range streams {
		entries, ok := stream["entries"].([]interface{})
		if ok {
			receivedLogLineCount += uint64(len(entries))
		}
	}

	usageStats.UpdateStats(uint64(len(logJson)), receivedLogLineCount, myid)

	for _, stream := range streams {
		labels := stream["labels"].(string)
		allIngestData := parseLabels(labels)

		entries, ok := stream["entries"].([]interface{})
		if !ok {
			utils.SendError(ctx, "Unable to convert entries", "failed to convert to []interface{}", nil)
			return
		}

		if len(entries) > 0 {
			for _, entry := range entries {
				entryMap, ok := entry.(map[string]interface{})
				if !ok {
					utils.SendError(ctx, "Unable to convert entries", "failed to convert to map[string]interface{}", err)
					return
				}
				timestamp, ok := entryMap["timestamp"].(string)
				if !ok {
					utils.SendError(ctx, "Unable to convert timestamp", "", err)
					return
				}
				line, ok := entryMap["line"].(string)
				if !ok {
					utils.SendError(ctx, "Unable to convert line to string", "", err)
					return
				}

				allIngestData["timestamp"] = timestamp
				allIngestData["line"] = line

				test, err := json.Marshal(allIngestData)
				if err != nil {
					utils.SendError(ctx, "Unable to marshal json", fmt.Sprintf("allIngestData: %v", allIngestData), err)
					return
				}

				ple, err := segwriter.GetNewPLE(test, tsNow, indexNameIn, &tsKey, jsParsingStackbuf[:])
				if err != nil {
					log.Errorf("processPromtailLogs: failed to get new PLE, test: %v, err: %v", test, err)
					return
				}
				pleArray = append(pleArray, ple)
			}
		}
	}

	err = writer.ProcessIndexRequestPle(tsNow, indexNameIn, false, localIndexMap, myid, 0, idxToStreamIdCache, cnameCacheByteHashToStr, jsParsingStackbuf[:], pleArray)
	if err != nil {
		utils.SendError(ctx, "Failed to ingest record", "", err)
		return
	}

	responsebody["status"] = "Success"
	utils.WriteJsonResponse(ctx, responsebody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

// LokiLogStream represents log entry in format acceptable for Loki HTTP API: https://grafana.com/docs/loki/latest/reference/loki-http-api/#ingest-logs
// Acceptable json example:
//
//	{
//	 "streams": [
//	   {
//		 "stream": {
//		   "label": "value"
//		 },
//		 "values": [
//			 [ "<unix epoch in nanoseconds>", "<log line>"],
//			 [ "<unix epoch in nanoseconds>", "<log line>", {"trace_id": "0242ac120002", "user_id": "superUser123"}]
//		 ]
//	   }
//	 ]
//	}
func processJsonLogs(ctx *fasthttp.RequestCtx, myid int64) {
	if hook := hooks.GlobalHooks.OverrideIngestRequestHook; hook != nil {
		alreadyHandled := hook(ctx, myid, grpc.INGEST_FUNC_LOKI, false)
		if alreadyHandled {
			return
		}
	}
	responsebody := make(map[string]interface{})
	buf := ctx.PostBody()

	var logData LokiLogData
	err := json.Unmarshal(buf, &logData)
	if err != nil {
		utils.SendError(ctx, "Unable to unmarshal request", "", err)
		return
	}
	tsKey := config.GetTimeStampKey()
	tsNow := utils.GetCurrentTimeInMs()
	indexNameIn := LOKIINDEX
	if !vtable.IsVirtualTablePresent(&indexNameIn, myid) {
		log.Errorf("processJsonLogs: Index name %v does not exist. Adding virtual table name and mapping.", indexNameIn)
		err = vtable.AddVirtualTable(&indexNameIn, myid)
		if err != nil {
			utils.SendInternalError(ctx, "Failed to add virtual table for index", "", err)
			return
		}
	}

	localIndexMap := make(map[string]string)

	idxToStreamIdCache := make(map[string]string)
	cnameCacheByteHashToStr := make(map[uint64]string)
	var jsParsingStackbuf [utils.UnescapeStackBufSize]byte

	pleArray := make([]*segwriter.ParsedLogEvent, 0)
	defer segwriter.ReleasePLEs(pleArray)

	for _, stream := range logData.Streams {
		allIngestData := make(map[string]interface{})

		// Extracting the labels and adding to the ingest data
		for label, value := range stream.Stream {
			allIngestData[label] = value
		}

		for _, value := range stream.Values {
			if len(value) < 2 {
				utils.SendError(ctx, "Invalid log entry format", "", nil)
				return
			}

			timestamp, ok := value[0].(string)
			if !ok {
				utils.SendError(ctx, "Invalid timestamp format", "", nil)
				return
			}

			line, ok := value[1].(string)
			if !ok {
				utils.SendError(ctx, "Invalid line format", "", nil)
				return
			}

			allIngestData["timestamp"] = timestamp
			allIngestData["line"] = line

			// Handle optional fields
			if len(value) > 2 {
				if additionalFields, ok := value[2].(map[string]interface{}); ok {
					for k, v := range additionalFields {
						allIngestData[k] = v
					}
				}
			}

			// Marshal the map to JSON
			allIngestDataBytes, err := json.Marshal(allIngestData)
			if err != nil {
				utils.SendError(ctx, "Unable to marshal json", fmt.Sprintf("allIngestData: %v", allIngestData), err)
				return
			}

			ple, err := segwriter.GetNewPLE(allIngestDataBytes, tsNow, indexNameIn, &tsKey, jsParsingStackbuf[:])
			if err != nil {
				utils.SendError(ctx, "failed to get new PLE", fmt.Sprintf("allIngestData: %v", allIngestData), err)
				return
			}
			pleArray = append(pleArray, ple)
		}
	}

	err = writer.ProcessIndexRequestPle(tsNow, indexNameIn, false, localIndexMap, myid, 0, idxToStreamIdCache, cnameCacheByteHashToStr, jsParsingStackbuf[:], pleArray)
	if err != nil {
		utils.SendError(ctx, "Failed to ingest record", "", err)
		return
	}

	responsebody["status"] = "Success"
	utils.WriteJsonResponse(ctx, responsebody)
	ctx.SetStatusCode(fasthttp.StatusOK)
}
