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
}

type Status_StatusCode string

const (
	// The default status.
	Status_STATUS_CODE_UNSET Status_StatusCode = "0"
	Status_STATUS_CODE_OK    Status_StatusCode = "1"
	Status_STATUS_CODE_ERROR Status_StatusCode = "2"
)

type Span struct {
	TraceID                string            `json:"trace_id"`
	SpanID                 string            `json:"span_id"`
	ParentSpanID           string            `json:"parent_span_id"`
	TraceState             string            `json:"trace_state"`
	Name                   string            `json:"name"`
	Kind                   string            `json:"kind"`
	StartTime              uint64            `json:"start_time"`
	EndTime                uint64            `json:"end_time"`
	Duration               uint64            `json:"duration"`
	Attributes             map[string]string `json:"attributes"`
	DroppedAttributesCount int               `json:"dropped_attributes_count"`
	Events                 []Event           `json:"events"`
	DroppedEventsCount     int               `json:"dropped_events_count"`
	Links                  []Link            `json:"links"`
	DroppedLinksCount      int               `json:"dropped_links_count"`
	Status                 Status            `json:"status"`
	Service                string            `json:"service"`
}

type Event struct {
	TimeUnixNano uint64            `json:"time_unix_nano"`
	Name         string            `json:"name"`
	Attributes   map[string]string `json:"attributes"`
}

type Link struct {
	TraceId    string            `json:"trace_id"`
	SpanId     string            `json:"span_id"`
	TraceState string            `json:"trace_state"`
	Attributes map[string]string `json:"attributes"`
}

type Status struct {
	Message string `json:"message"`
	Code    int    `json:"code"`
}

// Accept the request body of search traces and act as the request body of the /api/search
type SearchRequestBody struct {
	SearchText    string `json:"searchText"`
	StartEpoch    string `json:"startEpoch"`
	EndEpoch      string `json:"endEpoch"`
	QueryLanguage string `json:"queryLanguage"`
}
