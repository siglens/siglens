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
	logpb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

const defaultIndexName = "otel-logs"
const indexNameAttributeKey = "siglensIndexName"
const k8sKey = "k8s"
const k8sEventsKey = "event"
const k8sEventsIndexName = "k8s_events_sig"

// Based on https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/logs/v1/logs.proto#L47-L65
type resourceInfo struct {
	Attributes             map[string]interface{} `json:"attributes"`
	DroppedAttributesCount int64                  `json:"dropped_attributes_count"`
	SchemaUrl              string                 `json:"schema_url"`
}

// Based on https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/logs/v1/logs.proto#L67-L83
type scopeInfo struct {
	Name                   string                 `json:"name"`
	Version                string                 `json:"version"`
	Attributes             map[string]interface{} `json:"attributes"`
	DroppedAttributesCount int64                  `json:"dropped_attributes_count"`
	SchemaUrl              string                 `json:"schema_url"`
}

// Based on https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/logs/v1/logs.proto#L134-L227
type recordInfo struct {
	Resource               *resourceInfo          `json:"resource"`
	Scope                  *scopeInfo             `json:"scope"`
	TimeUnixNano           uint64                 `json:"time_unix_nano"`
	ObservedTimeUnixNano   uint64                 `json:"observed_time_unix_nano"`
	SeverityNumber         int32                  `json:"severity_number"`
	SeverityText           string                 `json:"severity_text"`
	Body                   interface{}            `json:"body"`
	Attributes             map[string]interface{} `json:"attributes"`
	DroppedAttributesCount int64                  `json:"dropped_attributes_count"`
	Flags                  uint32                 `json:"flags"`
	TraceId                string                 `json:"trace_id"`
	SpanId                 string                 `json:"span_id"`
}

func ProcessLogIngest(ctx *fasthttp.RequestCtx, myid int64) {
	if hook := hooks.GlobalHooks.OverrideIngestRequestHook; hook != nil {
		alreadyHandled := hook(ctx, myid, grpc.INGEST_FUNC_OTLP_LOGS, false)
		if alreadyHandled {
			return
		}
	}

	data, err := getDataToUnmarshal(ctx)
	if err != nil {
		log.Errorf("ProcessLogIngest: failed to get data to unmarshal: %v", err)
		setFailureResponse(ctx, fasthttp.StatusBadRequest, err.Error())
		return
	}

	request, err := unmarshalLogRequest(data)
	if err != nil {
		log.Errorf("ProcessLogIngest: failed to unpack Data: %s with err %v", string(data), err)
		setFailureResponse(ctx, fasthttp.StatusBadRequest, "Unable to unmarshal traces")
		return
	}

	numTotalRecords, numFailedRecords := ingestLogs(request, myid)
	usageStats.UpdateStats(uint64(len(data)), uint64(max(0, numTotalRecords-numFailedRecords)), myid)

	// Send the appropriate response.
	setLogIngestionResponse(ctx, numTotalRecords, numFailedRecords)
}

func ingestLogs(request *collogpb.ExportLogsServiceRequest, myid int64) (int, int) {
	now := utils.GetCurrentTimeInMs()
	timestampKey := config.GetTimeStampKey()
	var jsParsingStackbuf [utils.UnescapeStackBufSize]byte
	localIndexMap := make(map[string]string)
	idxToStreamIdCache := make(map[string]string)
	cnameCacheByteHashToStr := make(map[uint64]string)
	numTotalRecords := 0
	numFailedRecords := 0

	/// PLE Array Management ///
	// Keeping this code block here to avoid using mutexes
	var pleArrayMap = make(map[int][]*segwriter.ParsedLogEvent, 0)
	var pleArrayMapIndex = 0

	getPleArray := func() []*segwriter.ParsedLogEvent {
		if len(pleArrayMap) == 0 {
			pleArrayMap[pleArrayMapIndex] = make([]*segwriter.ParsedLogEvent, 0)
			pleArrayMapIndex++
		}

		var pleArray []*segwriter.ParsedLogEvent

		for index, pleArr := range pleArrayMap {
			delete(pleArrayMap, index)
			pleArray = pleArr
			break
		}

		return pleArray
	}

	putPleArray := func(pleArray []*segwriter.ParsedLogEvent) {
		pleArray = pleArray[:0]
		pleArrayMap[pleArrayMapIndex] = pleArray
		pleArrayMapIndex++
	}

	/// PLE Array Management ///

	for _, resourceLog := range request.ResourceLogs {
		resource, indexName, err := extractResourceInfo(resourceLog)
		if err != nil {
			log.Errorf("ingestLogs: failed to extract resource info: %v", err)
			for _, scopeLog := range resourceLog.ScopeLogs {
				numTotalRecords += len(scopeLog.LogRecords)
				numFailedRecords += len(scopeLog.LogRecords)
			}

			continue
		}

		indexToPleMap := make(map[string][]*segwriter.ParsedLogEvent)

		for _, scopeLog := range resourceLog.ScopeLogs {
			scope, err := extractScopeInfo(scopeLog)
			if err != nil {
				log.Errorf("ingestLogs: failed to extract scope info: %v", err)
				numTotalRecords += len(scopeLog.LogRecords)
				numFailedRecords += len(scopeLog.LogRecords)

				continue
			}

			for _, logRecord := range scopeLog.LogRecords {
				numTotalRecords++
				record, logIndexName, err := extractLogRecord(logRecord, resource, scope, indexName)
				if err != nil {
					log.Errorf("ingestLogs: failed to extract log record: %v", err)
					numFailedRecords++
					continue
				}

				jsonBytes, err := json.Marshal(record)
				if err != nil {
					log.Errorf("ingestLogs: failed to marshal log record; err=%v", err)
					numFailedRecords++
					continue
				}

				ple, err := segwriter.GetNewPLE(jsonBytes, now, logIndexName, &timestampKey, jsParsingStackbuf[:])
				if err != nil {
					log.Errorf("ingestLogs: failed to get new PLE, jsonBytes: %v, err: %v", jsonBytes, err)
					numFailedRecords++
					continue
				}

				if timestampMs := record.TimeUnixNano / 1_000_000; timestampMs > 0 {
					ple.SetTimestamp(timestampMs)
				}

				pleArray, ok := indexToPleMap[logIndexName]
				if !ok {
					pleArray = getPleArray()
					indexToPleMap[logIndexName] = pleArray
				}

				pleArray = append(pleArray, ple)
				indexToPleMap[logIndexName] = pleArray
			}
		}

		shouldFlush := false
		for indexName, pleArray := range indexToPleMap {
			err = writer.ProcessIndexRequestPle(now, indexName, shouldFlush, localIndexMap, myid, 0, idxToStreamIdCache, cnameCacheByteHashToStr, jsParsingStackbuf[:], pleArray)
			if err != nil {
				log.Errorf("ingestLogs: Failed to ingest logs, err: %v", err)
				numFailedRecords += len(pleArray)
			}
			putPleArray(pleArray)
		}
	}

	return numTotalRecords, numFailedRecords
}

func extractResourceInfo(resourceLog *logpb.ResourceLogs) (*resourceInfo, string, error) {
	resource := resourceInfo{
		Attributes: make(map[string]interface{}),
		SchemaUrl:  resourceLog.SchemaUrl,
	}

	indexName := defaultIndexName

	if resourceLog.Resource != nil {
		resource.DroppedAttributesCount = int64(resourceLog.Resource.DroppedAttributesCount)

		for _, attribute := range resourceLog.Resource.Attributes {
			key, value, err := extractKeyValue(attribute)
			if err != nil {
				return nil, "", err
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

	return &resource, indexName, nil
}

func extractScopeInfo(scopeLog *logpb.ScopeLogs) (*scopeInfo, error) {
	scope := scopeInfo{
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
				return nil, err
			}

			scope.Attributes[key] = value
		}
	}

	return &scope, nil
}

func extractLogRecord(logRecord *logpb.LogRecord, resource *resourceInfo, scope *scopeInfo, indexName string) (*recordInfo, string, error) {
	record := recordInfo{
		Resource:               resource,
		Scope:                  scope,
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
		return nil, "", fmt.Errorf("failed to extract body; err=%v", err)
	}
	record.Body = body

	for _, attribute := range logRecord.Attributes {
		key, value, err := extractKeyValue(attribute)
		if err != nil {
			return nil, "", fmt.Errorf("failed to extract key and value from attribute: %v", err)
		}

		record.Attributes[key] = value

		if key == k8sKey {
			k8sMap, ok := value.(map[string]interface{})
			if ok {
				k8sEventsValue, ok := k8sMap[k8sEventsKey]
				if ok {
					_, ok := k8sEventsValue.(map[string]interface{})
					if ok {
						indexName = k8sEventsIndexName
					}
				}
			}
		}
	}

	if record.TraceId == "" {
		if traceId, ok := record.Attributes["trace_id"]; ok {
			record.TraceId = fmt.Sprintf("%v", traceId)
		}
	}

	if record.SpanId == "" {
		if spanId, ok := record.Attributes["span_id"]; ok {
			record.SpanId = fmt.Sprintf("%v", spanId)
		}
	}

	return &record, indexName, nil
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
