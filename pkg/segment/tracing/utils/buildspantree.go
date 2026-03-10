// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"fmt"
	"sort"

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

	spans := make([]*structs.GanttChartSpan, 0, len(spanMap))
	for _, span := range spanMap {
		spans = append(spans, span)
	}

	sort.Slice(spans, func(i, j int) bool {
		start1 := spans[i].StartTime
		start2 := spans[j].StartTime
		if start1 != start2 {
			return start1 < start2
		}

		return spans[i].SpanID < spans[j].SpanID
	})

	for _, span := range spans {
		spanID := span.SpanID

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
