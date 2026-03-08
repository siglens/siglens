// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package structs

type PipeSearchResponseOuter struct {
	Hits                   PipeSearchResponse            `json:"hits"`
	Aggs                   map[string]AggregationResults `json:"aggregations"`
	ElapsedTimeMS          int64                         `json:"elapsedTimeMS"`
	AllPossibleColumns     []string                      `json:"allColumns"`
	Errors                 []string                      `json:"errors,omitempty"`
	MeasureFunctions       []string                      `json:"measureFunctions,omitempty"`
	MeasureResults         []*BucketHolder               `json:"measure,omitempty"`
	GroupByCols            []string                      `json:"groupByCols,omitempty"`
	Qtype                  string                        `json:"qtype,omitempty"`
	CanScrollMore          bool                          `json:"can_scroll_more"`
	TotalRRCCount          interface{}                   `json:"total_rrc_count,omitempty"`
	BucketCount            int                           `json:"bucketCount,omitempty"`
	DashboardPanelId       string                        `json:"dashboardPanelId"`
	ColumnsOrder           []string                      `json:"columnsOrder"`
	MeasureAggregationCols []string                      `json:"measureAggregationCols,omitempty"`
	RenameColumns          map[string]string             `json:"renameColumns,omitempty"`
}

type PipeSearchResponse struct {
	TotalMatched interface{}              `json:"totalMatched"`
	Hits         []map[string]interface{} `json:"records"`
}

type AggregationResults struct {
	Buckets []map[string]interface{} `json:"buckets"`
}

type PipeSearchWSUpdateResponse struct {
	Hits                     PipeSearchResponse          `json:"hits,omitempty"`
	AllPossibleColumns       []string                    `json:"allColumns,omitempty"`
	Completion               float64                     `json:"percent_complete"`
	State                    string                      `json:"state,omitempty"`
	TotalEventsSearched      interface{}                 `json:"total_events_searched,omitempty"`
	TotalPossibleEvents      interface{}                 `json:"total_possible_events,omitempty"`
	MeasureFunctions         []string                    `json:"measureFunctions,omitempty"`
	MeasureResults           []*BucketHolder             `json:"measure,omitempty"`
	GroupByCols              []string                    `json:"groupByCols,omitempty"`
	Qtype                    string                      `json:"qtype,omitempty"`
	BucketCount              int                         `json:"bucketCount,omitempty"`
	SortByTimestampAtDefault bool                        `json:"sortByTimestampAtDefault"`
	ColumnsOrder             []string                    `json:"columnsOrder,omitempty"`
	TimechartUpdate          *PipeSearchWSUpdateResponse `json:"timechartUpdate,omitempty"`
}

type PipeSearchCompleteResponse struct {
	State               string                      `json:"state,omitempty"`
	TotalMatched        interface{}                 `json:"totalMatched,omitempty"`
	TotalEventsSearched interface{}                 `json:"total_events_searched,omitempty"`
	CanScrollMore       bool                        `json:"can_scroll_more"`
	TotalRRCCount       interface{}                 `json:"total_rrc_count,omitempty"`
	MeasureFunctions    []string                    `json:"measureFunctions,omitempty"`
	MeasureResults      []*BucketHolder             `json:"measure,omitempty"`
	GroupByCols         []string                    `json:"groupByCols,omitempty"`
	Qtype               string                      `json:"qtype,omitempty"`
	BucketCount         int                         `json:"bucketCount,omitempty"`
	IsTimechart         bool                        `json:"isTimechart"`
	ColumnsOrder        []string                    `json:"columnsOrder,omitempty"`
	TimechartComplete   *PipeSearchCompleteResponse `json:"timechartComplete,omitempty"`
}
