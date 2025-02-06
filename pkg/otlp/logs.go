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

package otlp

import (
	"fmt"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/grpc"
	"github.com/siglens/siglens/pkg/hooks"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	collogpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func ProcessLogIngest(ctx *fasthttp.RequestCtx, myid int64) {
	if hook := hooks.GlobalHooks.OverrideIngestRequestHook; hook != nil {
		alreadyHandled := hook(ctx, myid, grpc.INGEST_FUNC_OTLP_LOGS, false)
		if alreadyHandled {
			return
		}
	}

	data, err := getDataToUnmarshal(ctx)
	if err != nil {
		log.Errorf("ProcessTraceIngest: failed to get data to unmarshal: %v", err)
		setFailureResponse(ctx, fasthttp.StatusBadRequest, err.Error())
		return
	}

	request, err := unmarshalLogRequest(data)
	if err != nil {
		log.Errorf("ProcessTraceIngest: failed to unpack Data: %s with err %v", string(data), err)
		setFailureResponse(ctx, fasthttp.StatusBadRequest, "Unable to unmarshal traces")
		return
	}

	type Scope struct {
		Name                   string                 `json:"name"`
		Version                string                 `json:"version"`
		Attributes             map[string]interface{} `json:"attributes"`
		DroppedAttributesCount int64                  `json:"droppedAttributesCount"`
		SchemaUrl              string                 `json:"schemaUrl"`
	}

	type SingleRecord struct {
		TimeUnixNano           uint64                 `json:"time_unix_nano"`
		ObservedTimeUnixNano   uint64                 `json:"observed_time_unix_nano"`
		SeverityNumber         int32                  `json:"severity_number"`
		SeverityText           string                 `json:"severity_text"`
		Body                   string                 `json:"body"`
		Attributes             map[string]interface{} `json:"attributes"`
		DroppedAttributesCount int64                  `json:"droppedAttributesCount"`
		Flags                  uint32                 `json:"flags"`
		TraceId                string                 `json:"traceId"`
		SpanId                 string                 `json:"spanId"`
	}

	now := utils.GetCurrentTimeInMs()
	timestampKey := config.GetTimeStampKey()
	var jsParsingStackbuf [utils.UnescapeStackBufSize]byte
	pleArray := make([]*segwriter.ParsedLogEvent, 0)

	numFailedResources := 0

resourceLoop:
	for _, resourceLog := range request.ResourceLogs {
		resource := resourceLog.Resource
		sharedResourceInfo := make(map[string]interface{})
		for _, attribute := range resource.Attributes {
			key, value, err := extractKeyValue(attribute)
			if err != nil {
				log.Errorf("ProcessTraceIngest: failed to extract key value from attribute: %v", err)
				numFailedResources++
				continue resourceLoop
			}
			sharedResourceInfo[key] = value
		}

		sharedResourceInfo["resourceSchema"] = resourceLog.SchemaUrl
		sharedResourceInfo["droppedAttributesCount"] = resource.DroppedAttributesCount

		indexName := "otlp-default"
		if value, ok := sharedResourceInfo["index"]; ok {
			valueStr := fmt.Sprintf("%v", value)
			if valueStr != "" {
				indexName = valueStr
			}
		}

		numFailedScopeLogs := 0

	scopeLoop:
		for _, scopeLog := range resourceLog.ScopeLogs {
			scope := Scope{
				Name:                   scopeLog.Scope.Name,
				Version:                scopeLog.Scope.Version,
				Attributes:             make(map[string]interface{}),
				DroppedAttributesCount: int64(scopeLog.Scope.DroppedAttributesCount),
				SchemaUrl:              scopeLog.SchemaUrl,
			}

			for _, attribute := range scopeLog.Scope.Attributes {
				key, value, err := extractKeyValue(attribute)
				if err != nil {
					log.Errorf("ProcessTraceIngest: failed to extract key value from attribute: %v", err)
					numFailedScopeLogs++
					continue scopeLoop
				}
				scope.Attributes[key] = value
			}

			numFailedRecords := 0

		recordLoop:
			for _, logRecord := range scopeLog.LogRecords {
				marshaler := protojson.MarshalOptions{
					UseProtoNames:   false, // Use JSON names instead of proto names
					EmitUnpopulated: true,  // Include zero values
				}
				jsonBytes, err := marshaler.Marshal(logRecord)
				if err != nil {
					log.Errorf("ProcessLogIngest: failed to marshal log record: %v", err)
					numFailedRecords++
					continue recordLoop
				}

				record := SingleRecord{}
				for _, attribute := range logRecord.Attributes {
					key, value, err := extractKeyValue(attribute)
					if err != nil {
						log.Errorf("ProcessLogIngest: failed to extract key value from attribute: %v", err)
						numFailedRecords++
						continue recordLoop
					}

					record.Attributes[key] = value
				}

				ple, err := segwriter.GetNewPLE(jsonBytes, now, indexName, &timestampKey, jsParsingStackbuf[:])
				if err != nil {
					log.Errorf("ProcessLogIngest: failed to get new PLE, jsonBytes: %v, err: %v", jsonBytes, err)
					numFailedRecords++
					continue resourceLoop
				}
				pleArray = append(pleArray, ple)
			}
		}
	}

	indexName := "andrew-test"
	shouldFlush := false
	localIndexMap := make(map[string]string)
	idxToStreamIdCache := make(map[string]string)
	cnameCacheByteHashToStr := make(map[uint64]string)
	err = writer.ProcessIndexRequestPle(now, indexName, shouldFlush, localIndexMap, myid, 0, idxToStreamIdCache, cnameCacheByteHashToStr, jsParsingStackbuf[:], pleArray)
	if err != nil {
		log.Errorf("ProcessLogIngest: Failed to ingest logs, err: %v", err)
		numFailedResources += len(pleArray) // TODO andrew
	}
	// if err != nil {
	// 	log.Errorf("ProcessTraceIngest: Failed to ingest traces, err: %v", err)
	// 	numFailedSpans += len(pleArray)
	// }

	// log.Debugf("ProcessTraceIngest: %v spans in the request and failed to ingest %v of them", numSpans, numFailedSpans)
	// usageStats.UpdateStats(uint64(len(data)), uint64(numSpans), myid)

	// // Send the appropriate response.
	// handleTraceIngestionResponse(ctx, numSpans, numFailedSpans)
}

func unmarshalLogRequest(data []byte) (*collogpb.ExportLogsServiceRequest, error) {
	var logs collogpb.ExportLogsServiceRequest
	err := proto.Unmarshal(data, &logs)
	if err != nil {
		log.Errorf("unmarshalLogRequest: failed with err: %v data: %v", err, string(data))
		return nil, err
	}

	return &logs, nil
}

// func handleLogIngestionResponse(ctx *fasthttp.RequestCtx, numSpans int, numFailedSpans int) {
// 	// if numFailedSpans == 0 {
// 	// 	// This request was successful.
// 	// 	response, err := proto.Marshal(&coltracepb.ExportTraceServiceResponse{})
// 	// 	if err != nil {
// 	// 		log.Errorf("handleTraceIngestionResponse: failed to marshal successful response. err: %v. NumSpans: %d", err, numSpans)
// 	// 		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
// 	// 		return
// 	// 	}
// 	// 	_, err = ctx.Write(response)
// 	// 	if err != nil {
// 	// 		log.Errorf("handleTraceIngestionResponse: failed to write successful response. err: %v. NumSpans: %d", err, numSpans)
// 	// 		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
// 	// 		return
// 	// 	}

// 	// 	ctx.SetStatusCode(fasthttp.StatusOK)
// 	// 	return
// 	// } else if numFailedSpans < numSpans {
// 	// 	// This request was partially successful.
// 	// 	traceResponse := coltracepb.ExportTraceServiceResponse{
// 	// 		PartialSuccess: &coltracepb.ExportTracePartialSuccess{
// 	// 			RejectedSpans: int64(numFailedSpans),
// 	// 		},
// 	// 	}

// 	// 	response, err := proto.Marshal(&traceResponse)
// 	// 	if err != nil {
// 	// 		log.Errorf("handleTraceIngestionResponse: failed to marshal partially successful response: %v. NumSpans: %d, NumFailedSpans: %d, Trace Response: %v", err, numSpans, numFailedSpans, &traceResponse)
// 	// 		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
// 	// 		return
// 	// 	}
// 	// 	_, err = ctx.Write(response)
// 	// 	if err != nil {
// 	// 		log.Errorf("handleTraceIngestionResponse: failed to write partially successful response: %v. NumSpans: %d, NumFailedSpans: %d, response: %v", err, numSpans, numFailedSpans, response)
// 	// 		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
// 	// 		return
// 	// 	}

// 	// 	ctx.SetStatusCode(fasthttp.StatusOK)
// 	// 	return
// 	// } else {
// 	// 	// Every span failed to be ingested.
// 	// 	if numFailedSpans > numSpans {
// 	// 		log.Errorf("handleTraceIngestionResponse: error in counting number of total and failed spans. Counted NumSpans: %d, Counted NumFailedSpans: %d", numSpans, numFailedSpans)
// 	// 	}

// 	// 	log.Errorf("handleTraceIngestionResponse: every span failed ingestion. NumSpans: %d, NumFailedSpans: %d", numSpans, numFailedSpans)
// 	// 	setFailureResponse(ctx, fasthttp.StatusInternalServerError, "Every span failed ingestion")
// 	// 	return
// 	// }
// }
