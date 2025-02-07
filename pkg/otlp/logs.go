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
	"encoding/hex"
	"encoding/json"
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
	"google.golang.org/protobuf/proto"
)

const defaultIndexName = "otlp-default"
const indexNameKey = "index"

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

	type Resource struct {
		Attributes             map[string]interface{} `json:"attributes"`
		DroppedAttributesCount int64                  `json:"droppedAttributesCount"`
		SchemaUrl              string                 `json:"schemaUrl"`
	}

	type Scope struct {
		Name                   string                 `json:"name"`
		Version                string                 `json:"version"`
		Attributes             map[string]interface{} `json:"attributes"`
		DroppedAttributesCount int64                  `json:"droppedAttributesCount"`
		SchemaUrl              string                 `json:"schemaUrl"`
	}

	type SingleRecord struct {
		Resource               *Resource              `json:"resource"`
		Scope                  *Scope                 `json:"scope"`
		TimeUnixNano           uint64                 `json:"timeUnixNano"`
		ObservedTimeUnixNano   uint64                 `json:"observedTimeUnixNano"`
		SeverityNumber         int32                  `json:"severityNumber"`
		SeverityText           string                 `json:"severityText"`
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
	numFailedRecords := 0

resourceLoop:
	for _, resourceLog := range request.ResourceLogs {
		resource := Resource{
			Attributes:             make(map[string]interface{}),
			DroppedAttributesCount: int64(resourceLog.Resource.DroppedAttributesCount),
			SchemaUrl:              resourceLog.SchemaUrl,
		}

		indexName := defaultIndexName

		for _, attribute := range resourceLog.Resource.Attributes {
			key, value, err := extractKeyValue(attribute)
			if err != nil {
				log.Errorf("ProcessTraceIngest: failed to extract key value from attribute: %v", err)
				for _, scopeLog := range resourceLog.ScopeLogs {
					numFailedRecords += len(scopeLog.LogRecords)
				}
				continue resourceLoop
			}
			resource.Attributes[key] = value

			if key == indexNameKey {
				valueStr := fmt.Sprintf("%v", value)
				if valueStr != "" {
					indexName = valueStr
				}
			}
		}

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
					numFailedRecords += len(scopeLog.LogRecords)
					continue scopeLoop
				}
				scope.Attributes[key] = value
			}

		recordLoop:
			for _, logRecord := range scopeLog.LogRecords {
				record := SingleRecord{
					Resource:               &resource,
					Scope:                  &scope,
					TimeUnixNano:           logRecord.TimeUnixNano,
					ObservedTimeUnixNano:   logRecord.ObservedTimeUnixNano,
					SeverityNumber:         int32(logRecord.SeverityNumber),
					SeverityText:           logRecord.SeverityText,
					Attributes:             make(map[string]interface{}),
					DroppedAttributesCount: int64(logRecord.DroppedAttributesCount),
					Flags:                  logRecord.Flags,
					TraceId:                hex.EncodeToString(logRecord.TraceId),
					SpanId:                 hex.EncodeToString(logRecord.SpanId),
				}

				body, err := extractAnyValue(logRecord.Body)
				if err != nil {
					log.Errorf("ProcessLogIngest: failed to extract body from log record: %v", err)
					numFailedRecords++
					continue recordLoop
				}
				record.Body = body.(string)

				for _, attribute := range logRecord.Attributes {
					key, value, err := extractKeyValue(attribute)
					if err != nil {
						log.Errorf("ProcessLogIngest: failed to extract key and value from attribute: %v", err)
						numFailedRecords++
						continue recordLoop
					}

					record.Attributes[key] = value
				}

				jsonBytes, err := json.Marshal(record)
				if err != nil {
					log.Errorf("ProcessLogIngest: failed to marshal log record; err=%v", err)
					numFailedRecords++
					continue recordLoop
				}

				indexName = "andrew-test"
				ple, err := segwriter.GetNewPLE(jsonBytes, now, indexName, &timestampKey, jsParsingStackbuf[:])
				if err != nil {
					log.Errorf("ProcessLogIngest: failed to get new PLE, jsonBytes: %v, err: %v", jsonBytes, err)
					numFailedRecords++
					continue resourceLoop
				}

				if timestampMs := record.TimeUnixNano / 1_000_000; timestampMs > 0 {
					ple.SetTimestamp(timestampMs)
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
		numFailedRecords = len(pleArray)
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
