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
	"google.golang.org/protobuf/proto"
)

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
	result["events_original"] = span.Events // deleteme
	result["events"], err = json.Marshal(span.Events)
	if err != nil {
		return nil, err
	}

	result["links_original"] = span.Links // deleteme
	result["links"], err = json.Marshal(span.Links)
	if err != nil {
		return nil, err
	}

	log.Errorf("result before marshal: %v", result)

	bytes, err := json.Marshal(result)
	log.Errorf("result after marshal: %s", bytes)
	return bytes, err
}

func unpackTrace(data []byte) (*coltracepb.ExportTraceServiceRequest, error) {
	log.Errorf("deletme: unpackTrace: size of data: %v", len(data))
	var trace coltracepb.ExportTraceServiceRequest
	err := proto.Unmarshal(data, &trace)
	if err != nil {
		return nil, err
	}
	return &trace, nil
}

func ProcessTraceIngest(ctx *fasthttp.RequestCtx) {
	log.Errorf("processTraceIngest: got headers:")
	ctx.Request.Header.VisitAll(func(key, value []byte) {
		log.Errorf("%s: %s", key, value)
	})

	request, err := unpackTrace(ctx.PostBody())
	if err != nil {
		log.Errorf("processTraceIngest: failed to unpack: %v", err)
		return
	}
	log.Errorf("processTraceIngest: got trace: %s", request)
	jsonMarshalled, _ := json.Marshal(request)
	log.Errorf("processTraceIngest: json: %s", jsonMarshalled)
	log.Errorf("processTraceIngest: size of json: %v", len(string(jsonMarshalled)))

	indexName := "traces"
	shouldFlush := false
	localIndexMap := make(map[string]string)
	orgId := uint64(0)

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
			for _, span := range scopeSpans.Spans {
				// log.Errorf("got span: %s", span)
				// spans.Spans = append(spans.Spans, span.String())

				jsonData, err := spanToJson(span, service)
				if err != nil {
					log.Errorf("processTraceIngest: failed to marshal spans: %v", err)
					// TODO: return error
				}

				now := utils.GetCurrentTimeInMs()
				lenJsonData := uint64(len(jsonData))
				err = writer.ProcessIndexRequest(jsonData, now, indexName, lenJsonData, shouldFlush, localIndexMap, orgId)
				if err != nil {
					fmt.Errorf("processTraceIngest: failed to process ingest request: %v", err)
					// TODO: return error
				}
			}
		}
	}

	response, err := proto.Marshal(&coltracepb.ExportTraceServiceResponse{})
	if err != nil {
		log.Errorf("processTraceIngest failed to marshal response: %v", err)
		// TODO: return error
	}
	_, err = ctx.Write(response)
	if err != nil {
		log.Errorf("processTraceIngest failed to write response: %v", err)
		// TODO: return error
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
}
