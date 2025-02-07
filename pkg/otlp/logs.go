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
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	collogpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"
)

const defaultIndexName = "otel-logs"
const indexNameAttributeKey = "siglensIndexName"

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
		Body                   interface{}            `json:"body"`
		Attributes             map[string]interface{} `json:"attributes"`
		DroppedAttributesCount int64                  `json:"droppedAttributesCount"`
		Flags                  uint32                 `json:"flags"`
		TraceId                string                 `json:"traceId"`
		SpanId                 string                 `json:"spanId"`
	}

	now := utils.GetCurrentTimeInMs()
	timestampKey := config.GetTimeStampKey()
	var jsParsingStackbuf [utils.UnescapeStackBufSize]byte
	localIndexMap := make(map[string]string)
	idxToStreamIdCache := make(map[string]string)
	cnameCacheByteHashToStr := make(map[uint64]string)
	pleArray := make([]*segwriter.ParsedLogEvent, 0)
	numTotalRecords := 0
	numFailedRecords := 0

resourceLoop:
	for _, resourceLog := range request.ResourceLogs {
		resource := Resource{
			Attributes: make(map[string]interface{}),
			SchemaUrl:  resourceLog.SchemaUrl,
		}

		indexName := defaultIndexName

		if resourceLog.Resource != nil {
			resource.DroppedAttributesCount = int64(resourceLog.Resource.DroppedAttributesCount)

			for _, attribute := range resourceLog.Resource.Attributes {
				key, value, err := extractKeyValue(attribute)
				if err != nil {
					log.Errorf("ProcessTraceIngest: failed to extract key value from attribute: %v", err)
					for _, scopeLog := range resourceLog.ScopeLogs {
						numTotalRecords += len(scopeLog.LogRecords)
						numFailedRecords += len(scopeLog.LogRecords)
					}
					continue resourceLoop
				}
				resource.Attributes[key] = value

				if key == indexNameAttributeKey {
					valueStr := fmt.Sprintf("%v", value)
					if valueStr != "" {
						indexName = valueStr
					}
				}
			}
		}

	scopeLoop:
		for _, scopeLog := range resourceLog.ScopeLogs {
			scope := Scope{
				Attributes: make(map[string]interface{}),
				SchemaUrl:  scopeLog.SchemaUrl,
			}

			if scopeLog.Scope != nil {
				scope.Name = scopeLog.Scope.Name
				scope.Version = scopeLog.Scope.Version
				scope.DroppedAttributesCount = int64(scopeLog.Scope.DroppedAttributesCount)

				for _, attribute := range scopeLog.Scope.Attributes {
					key, value, err := extractKeyValue(attribute)
					if err != nil {
						log.Errorf("ProcessTraceIngest: failed to extract key value from attribute: %v", err)
						numTotalRecords += len(scopeLog.LogRecords)
						numFailedRecords += len(scopeLog.LogRecords)
						continue scopeLoop
					}
					scope.Attributes[key] = value
				}
			}

		recordLoop:
			for _, logRecord := range scopeLog.LogRecords {
				numTotalRecords++
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
				record.Body = body

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

		shouldFlush := false
		err = writer.ProcessIndexRequestPle(now, indexName, shouldFlush, localIndexMap, myid, 0, idxToStreamIdCache, cnameCacheByteHashToStr, jsParsingStackbuf[:], pleArray)
		if err != nil {
			log.Errorf("ProcessLogIngest: Failed to ingest logs, err: %v", err)
			numFailedRecords += len(pleArray)
		}
	}

	usageStats.UpdateStats(uint64(len(data)), uint64(max(0, numTotalRecords-numFailedRecords)), myid)

	// Send the appropriate response.
	setLogIngestionResponse(ctx, numTotalRecords, numFailedRecords)
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

func setLogIngestionResponse(ctx *fasthttp.RequestCtx, numTotalRecords int, numFailedRecords int) {
	if numFailedRecords == 0 {
		response, err := proto.Marshal(&collogpb.ExportLogsServiceResponse{})
		if err != nil {
			log.Errorf("setLogIngestionResponse: failed to marshal successful response; err=%v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}

		_, err = ctx.Write(response)
		if err != nil {
			log.Errorf("setLogIngestionResponse: failed to write successful response; err=%v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}
	} else if numFailedRecords < numTotalRecords {
		response, err := proto.Marshal(&collogpb.ExportLogsServiceResponse{
			PartialSuccess: &collogpb.ExportLogsPartialSuccess{
				RejectedLogRecords: int64(numFailedRecords),
			},
		})
		if err != nil {
			log.Errorf("setLogIngestionResponse: failed to marshal partially successful response; err=%v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}

		_, err = ctx.Write(response)
		if err != nil {
			log.Errorf("setLogIngestionResponse: failed to write partially successful response; err=%v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}
	} else {
		setFailureResponse(ctx, fasthttp.StatusInternalServerError, "Every log record failed ingestion")
	}
}
