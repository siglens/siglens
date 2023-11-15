package otlp

import (
	"encoding/json"
	"fmt"

	"github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

type Span struct {
	TraceId                []byte `json:"trace_id,omitempty"`
	SpanId                 []byte `json:"span_id,omitempty"`
	ParentSpanId           []byte `json:"parent_span_id,omitempty"`
	TraceState             string `json:"trace_state,omitempty"`
	Name                   string `json:"name,omitempty"`
	Kind                   string `json:"kind,omitempty"`
	StartTime              uint64 `json:"start_time,omitempty"`
	EndTime                uint64 `json:"end_time,omitempty"`
	Duration               uint64 `json:"duration,omitempty"`
	Attributes             string `json:"attributes,omitempty"`
	DroppedAttributesCount uint64 `json:"dropped_attributes_count,omitempty"`
	Events                 string `json:"events,omitempty"`
	DroppedEventsCount     uint64 `json:"dropped_events_count,omitempty"`
	Links                  string `json:"links,omitempty"`
	DroppedLinksCount      uint64 `json:"dropped_links_count,omitempty"`
	Status                 string `json:"status,omitempty"`
	Service                string `json:"service,omitempty"`
}

func toSpan(otlpSpan *tracepb.Span) *Span {
	span := &Span{
		TraceId:                otlpSpan.TraceId,
		SpanId:                 otlpSpan.SpanId,
		ParentSpanId:           otlpSpan.ParentSpanId,
		TraceState:             otlpSpan.TraceState,
		Name:                   otlpSpan.Name,
		Kind:                   otlpSpan.Kind.String(),
		StartTime:              otlpSpan.StartTimeUnixNano,
		EndTime:                otlpSpan.EndTimeUnixNano,
		Duration:               otlpSpan.EndTimeUnixNano - otlpSpan.StartTimeUnixNano,
		Attributes:             "TODO", // otlpSpan.Attributes,
		DroppedAttributesCount: uint64(otlpSpan.DroppedAttributesCount),
		Events:                 "TODO", // otlpSpan.Events,
		DroppedEventsCount:     uint64(otlpSpan.DroppedEventsCount),
		Links:                  "TODO", // otlpSpan.Links,
		DroppedLinksCount:      uint64(otlpSpan.DroppedLinksCount),
		Status:                 "TODO", // otlpSpan.Status,
		Service:                "TODO",
	}

	return span
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
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			for _, span := range scopeSpans.Spans {
				// log.Errorf("got span: %s", span)
				// spans.Spans = append(spans.Spans, span.String())

				jsonData, err := json.Marshal(span)
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
	_, err = ctx.Write(response)
	if err != nil {
		log.Errorf("processTraceIngest failed to write response: %v", err)
		// TODO: return error
	}
	ctx.SetStatusCode(fasthttp.StatusOK)
}
