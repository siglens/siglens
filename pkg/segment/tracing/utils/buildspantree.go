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

package utils

import (
	"fmt"

	"github.com/siglens/siglens/pkg/segment/tracing/structs"
	log "github.com/sirupsen/logrus"
)

func BuildSpanTree(spanMap map[string]*structs.GanttChartSpan, idToParentId map[string]string) (*structs.GanttChartSpan, error) {
	res := &structs.GanttChartSpan{}

	// Find root span
	for spanID, span := range spanMap {
		parentSpanID, exists := idToParentId[spanID]
		if !exists {
			log.Errorf("BuildSpanTree: can not find parent span:%v for span:%v", parentSpanID, spanID)
			continue
		}

		if parentSpanID == "" {
			res = span
		}
	}

	if res.SpanID == "" {
		return nil, fmt.Errorf("BuildSpanTree: can not find a root span")
	}

	rootSpanStartTime := res.StartTime

	for spanID, span := range spanMap {
		// Calculate the relative start time and end time for each span
		span.ActualStartTime = span.StartTime
		span.StartTime -= rootSpanStartTime
		span.EndTime -= rootSpanStartTime

		parentSpanID, exists := idToParentId[spanID]
		if !exists {
			log.Errorf("BuildSpanTree: can not find parent span:%v for span:%v", parentSpanID, spanID)
			continue
		}

		if parentSpanID != "" {
			parentSpan, exists := spanMap[parentSpanID]
			if !exists {
				log.Errorf("BuildSpanTree: can not find parent span:%v for span:%v", parentSpanID, spanID)
				continue
			}

			// If a span start before its parent, it is anomalous
			parentSpanStartTime := parentSpan.StartTime
			if parentSpan.ActualStartTime != uint64(0) {
				parentSpanStartTime = parentSpan.ActualStartTime
			}

			if span.ActualStartTime < parentSpanStartTime || span.ActualStartTime < rootSpanStartTime {
				span.IsAnomalous = true
			}

			parentSpan.Children = append(parentSpan.Children, span)
		}
	}

	return res, nil
}
