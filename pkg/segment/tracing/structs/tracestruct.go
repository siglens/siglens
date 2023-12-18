package structs

type TraceResult struct {
	Traces []*Trace `json:"traces"` // Results of Search Traces
}

type Trace struct {
	TraceId         string `json:"trace_id"`
	StartTime       uint64 `json:"start_time"`
	EndTime         uint64 `json:"end_time"`
	SpanCount       int    `json:"span_count"`
	SpanErrorsCount int    `json:"span_errors_count"`
	ServiceName     string `json:"service_name"`
	OperationName   string `json:"operation_name"`
}

type Status_StatusCode string

const (
	// The default status.
	Status_STATUS_CODE_UNSET Status_StatusCode = "STATUS_CODE_UNSET"
	Status_STATUS_CODE_OK    Status_StatusCode = "STATUS_CODE_OK"
	Status_STATUS_CODE_ERROR Status_StatusCode = "STATUS_CODE_ERROR"
)

type RawSpanData struct {
	Hits RawSpanResponse `json:"hits"`
}

type RawSpanResponse struct {
	Spans []*Span `json:"records"`
}

type Span struct {
	TraceID      string `json:"trace_id"`
	SpanID       string `json:"span_id"`
	ParentSpanID string `json:"parent_span_id"`
	StartTime    uint64 `json:"start_time"`
	EndTime      uint64 `json:"end_time"`
	Duration     uint64 `json:"duration"`
	Status       string `json:"status"`
	Service      string `json:"service"`
}

type RedMetrics struct {
	Rate      float64 // Number of entry spans divided by 60 seconds
	ErrorRate float64 // Percentage of entry spans that errored
	P50       uint64  // p50, p90, p95, p99 latencies are calculated by the list of durations.
	P90       uint64
	P95       uint64
	P99       uint64
}

// Accept the request body of search traces and act as the request body of the /api/search
type SearchRequestBody struct {
	IndexName     string `json:"indexName"`
	SearchText    string `json:"searchText"`
	StartEpoch    string `json:"startEpoch"`
	EndEpoch      string `json:"endEpoch"`
	QueryLanguage string `json:"queryLanguage"`
	From          int    `json:"from,omitempty"`
	Size          int    `json:"size,omitempty"`
}

type GanttChartSpan struct {
	SpanID          string                 `json:"span_id"`
	ActualStartTime uint64                 `json:"actual_start_time"`
	StartTime       uint64                 `json:"start_time"`
	EndTime         uint64                 `json:"end_time"`
	Duration        uint64                 `json:"duration"`
	ServiceName     string                 `json:"service_name"`
	OperationName   string                 `json:"operation_name"`
	IsAnomalous     bool                   `json:"is_anomalous"`
	Tags            map[string]interface{} `json:"tags"`
	Children        []*GanttChartSpan      `json:"children"`
}
