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
