package otlp

import (
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

func ProcessTraceIngest(ctx *fasthttp.RequestCtx) {
	request, err := unpackTrace(ctx.PostBody())
	if err != nil {
		log.Errorf("ProcessTraceIngest: failed to unpack: %v", err)
		failureStatus := status.Status{
			Code:    fasthttp.StatusBadRequest,
			Message: "Unable to decode traces",
		}

		bytes, err := proto.Marshal(&failureStatus)
		if err != nil {
			log.Errorf("ProcessTraceIngest: failed to marshal failure status: %v", err)
		}
		_, err = ctx.Write(bytes)
		if err != nil {
			log.Errorf("ProcessTraceIngest: failed to write failure status: %v", err)
		}
		return
	}

	now := utils.GetCurrentTimeInMs()
	indexName := "traces"
	shouldFlush := false
	localIndexMap := make(map[string]string)
	orgId := uint64(0)

	numSpans := 0       // The total number of spans sent in this request.
	numFailedSpans := 0 // The number of spans that we could not ingest.
	for _, resourceSpans := range request.ResourceSpans {
		// Find the service name.
		var service string
		if resourceSpans.Resource != nil {
			for _, keyvalue := range resourceSpans.Resource.Attributes {
				if keyvalue.Key == "service.name" {
					service = keyvalue.Value.GetStringValue()
				}
			}
		}

		// Ingest each of these spans.
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			numSpans += len(scopeSpans.Spans)
			for _, span := range scopeSpans.Spans {
				jsonData, err := spanToJson(span, service)
				if err != nil {
					log.Errorf("ProcessTraceIngest: failed to marshal span %s: %v", span, err)
					numFailedSpans++
					continue
				}

				lenJsonData := uint64(len(jsonData))
				err = writer.ProcessIndexRequest(jsonData, now, indexName, lenJsonData, shouldFlush, localIndexMap, orgId)
				if err != nil {
					fmt.Errorf("ProcessTraceIngest: failed to process ingest request: %v", err)
					numFailedSpans++
					continue
				}
			}
		}
	}

	// Send the appropriate response.
	handleTraceIngestionResponse(ctx, numSpans, numFailedSpans)
}

func unpackTrace(data []byte) (*coltracepb.ExportTraceServiceRequest, error) {
	var trace coltracepb.ExportTraceServiceRequest
	err := proto.Unmarshal(data, &trace)
	if err != nil {
		return nil, err
	}
	return &trace, nil
}

func spanToJson(span *tracepb.Span, service string) ([]byte, error) {
	result := make(map[string]interface{})
	result["trace_id"] = hex.EncodeToString(span.TraceId)
	result["span_id"] = hex.EncodeToString(span.SpanId)
	result["parent_span_id"] = hex.EncodeToString(span.ParentSpanId)
	result["service"] = service
	result["trace_state"] = span.TraceState
	result["name"] = span.Name
	result["kind"] = span.Kind.String()
	result["start_time"] = span.StartTimeUnixNano
	result["end_time"] = span.EndTimeUnixNano
	result["duration"] = span.EndTimeUnixNano - span.StartTimeUnixNano
	result["dropped_attributes_count"] = uint64(span.DroppedAttributesCount)
	result["dropped_events_count"] = uint64(span.DroppedEventsCount)
	result["dropped_links_count"] = uint64(span.DroppedLinksCount)
	result["status"] = span.Status.String()

	// Make a column for each attribute key.
	for _, keyvalue := range span.Attributes {
		key := keyvalue.Key

		switch keyvalue.Value.Value.(type) {
		case *commonpb.AnyValue_StringValue:
			result[key] = keyvalue.Value.GetStringValue()
		case *commonpb.AnyValue_IntValue:
			result[key] = keyvalue.Value.GetIntValue()
		case *commonpb.AnyValue_DoubleValue:
			result[key] = keyvalue.Value.GetDoubleValue()
		case *commonpb.AnyValue_BoolValue:
			result[key] = keyvalue.Value.GetBoolValue()
		default:
			return nil, fmt.Errorf("spanToJson: unsupported value type in attribuates: %T", keyvalue.Value.Value)
		}
	}

	var err error
	result["events"], err = json.Marshal(span.Events)
	if err != nil {
		return nil, err
	}

	result["links"], err = json.Marshal(span.Links)
	if err != nil {
		return nil, err
	}

	log.Errorf("result before marshal: %v", result)

	bytes, err := json.Marshal(result)
	log.Errorf("result after marshal: %s", bytes)
	return bytes, err
}

func handleTraceIngestionResponse(ctx *fasthttp.RequestCtx, numSpans int, numFailedSpans int) {
	if numFailedSpans == 0 {
		// This request was successful.
		response, err := proto.Marshal(&coltracepb.ExportTraceServiceResponse{})
		if err != nil {
			log.Errorf("ProcessTraceIngest: failed to marshal successful response: %v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}
		_, err = ctx.Write(response)
		if err != nil {
			log.Errorf("ProcessTraceIngest: failed to write successful response: %v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}

		ctx.SetStatusCode(fasthttp.StatusOK)
		return
	} else if numFailedSpans < numSpans {
		// This request was partially successful.
		traceResponse := coltracepb.ExportTraceServiceResponse{
			PartialSuccess: &coltracepb.ExportTracePartialSuccess{
				RejectedSpans: int64(numFailedSpans),
			},
		}

		response, err := proto.Marshal(&traceResponse)
		if err != nil {
			log.Errorf("ProcessTraceIngest: failed to marshal partially successful response: %v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}
		_, err = ctx.Write(response)
		if err != nil {
			log.Errorf("ProcessTraceIngest: failed to write partially successful response: %v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}

		ctx.SetStatusCode(fasthttp.StatusOK)
		return
	} else {
		// Every span failed to be ingested.
		if numFailedSpans > numSpans {
			log.Errorf("ProcessTraceIngest: error in counting number of total and failed spans")
		}

		failureStatus := status.Status{
			Code:    fasthttp.StatusInternalServerError,
			Message: "Every span failed ingestion",
		}

		bytes, err := proto.Marshal(&failureStatus)
		if err != nil {
			log.Errorf("ProcessTraceIngest: failed to marshal failure status: %v", err)
		}
		_, err = ctx.Write(bytes)
		if err != nil {
			log.Errorf("ProcessTraceIngest: failed to write failure status: %v", err)
		}

		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}
}
