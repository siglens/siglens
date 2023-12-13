package utils

import (
	"github.com/siglens/siglens/pkg/segment/tracing/structs"
	log "github.com/sirupsen/logrus"
)

func BuildSpanTree(spanMap map[string]*structs.GanttChartSpan, idToParentId map[string]string) *structs.GanttChartSpan {
	res := &structs.GanttChartSpan{}
	for spanID, span := range spanMap {
		parentSpanID, exists := idToParentId[spanID]
		if !exists {
			log.Errorf("BuildSpanTree: can not find parent span:%v for span:%v", parentSpanID, spanID)
			continue
		}

		if parentSpanID == "" {
			res = span
		} else {
			parentSpan, exists := spanMap[parentSpanID]
			if !exists {
				log.Errorf("BuildSpanTree: can not find parent span:%v for span:%v", parentSpanID, spanID)
				continue
			}
			parentSpan.Children = append(parentSpan.Children, span)
		}
	}

	return res
}
