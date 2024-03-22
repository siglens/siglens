/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pipesearch

import "github.com/siglens/siglens/pkg/segment/structs"

type PipeSearchResponseOuter struct {
	Hits               PipeSearchResponse            `json:"hits"`
	Aggs               map[string]AggregationResults `json:"aggregations"`
	ElapedTimeMS       int64                         `json:"elapedTimeMS"`
	AllPossibleColumns []string                      `json:"allColumns"`
	Errors             []string                      `json:"errors,omitempty"`
	MeasureFunctions   []string                      `json:"measureFunctions,omitempty"`
	MeasureResults     []*structs.BucketHolder       `json:"measure,omitempty"`
	GroupByCols        []string                      `json:"groupByCols,omitempty"`
	Qtype              string                        `json:"qtype,omitempty"`
	CanScrollMore      bool                          `json:"can_scroll_more"`
	TotalRRCCount      interface{}                   `json:"total_rrc_count,omitempty"`
	BucketCount        int                           `json:"bucketCount,omitempty"`
	DashboardPanelId   string                        `json:"dashboardPanelId"`
}

type PipeSearchResponse struct {
	TotalMatched interface{}              `json:"totalMatched"`
	Hits         []map[string]interface{} `json:"records"`
}

type AggregationResults struct {
	Buckets []map[string]interface{} `json:"buckets"`
}

type PipeSearchWSUpdateResponse struct {
	Hits                     PipeSearchResponse      `json:"hits,omitempty"`
	AllPossibleColumns       []string                `json:"allColumns,omitempty"`
	Completion               float64                 `json:"percent_complete"`
	State                    string                  `json:"state,omitempty"`
	TotalEventsSearched      interface{}             `json:"total_events_searched,omitempty"`
	MeasureFunctions         []string                `json:"measureFunctions,omitempty"`
	MeasureResults           []*structs.BucketHolder `json:"measure,omitempty"`
	GroupByCols              []string                `json:"groupByCols,omitempty"`
	Qtype                    string                  `json:"qtype,omitempty"`
	BucketCount              int                     `json:"bucketCount,omitempty"`
	SortByTimestampAtDefault bool                    `json:"sortByTimestampAtDefault"`
}

type PipeSearchCompleteResponse struct {
	State               string                  `json:"state,omitempty"`
	TotalMatched        interface{}             `json:"totalMatched,omitempty"`
	TotalEventsSearched interface{}             `json:"total_events_searched,omitempty"`
	CanScrollMore       bool                    `json:"can_scroll_more"`
	TotalRRCCount       interface{}             `json:"total_rrc_count,omitempty"`
	MeasureFunctions    []string                `json:"measureFunctions,omitempty"`
	MeasureResults      []*structs.BucketHolder `json:"measure,omitempty"`
	GroupByCols         []string                `json:"groupByCols,omitempty"`
	Qtype               string                  `json:"qtype,omitempty"`
	BucketCount         int                     `json:"bucketCount,omitempty"`
	IsTimechart         bool                    `json:"isTimechart"`
}
