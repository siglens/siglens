// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

//

package metrics

import (
	"fmt"
	"sort"
	"strings"
)

type TaggedSeries struct {
	timeseries
	tags map[string]string
}

func NewTaggedSeries(tags map[string]string, series timeseries) *TaggedSeries {
	return &TaggedSeries{
		tags:       tags,
		timeseries: series,
	}
}

func (t *TaggedSeries) GetValue(key string) (string, bool) {
	value, exists := t.tags[key]
	return value, exists
}

func (t *TaggedSeries) SetValue(key string, value string) {
	if t.tags == nil {
		t.tags = make(map[string]string)
	}
	t.tags[key] = value
}

func (t *TaggedSeries) RemoveTag(key string) {
	delete(t.tags, key)
}

func (t *TaggedSeries) HasTag(key string) bool {
	_, exists := t.tags[key]
	return exists
}

func (t *TaggedSeries) SetTags(tags map[string]string) {
	t.tags = make(map[string]string)
	for k, v := range tags {
		t.tags[k] = v
	}
}

func (t *TaggedSeries) Id() string {
	metricName, ok := t.tags["__name__"] // For Prometheus metrics.
	if !ok {
		metricName = ""
	}

	sortedKeys := make([]string, 0, len(t.tags))
	for k := range t.tags {
		if k != "__name__" {
			sortedKeys = append(sortedKeys, k)
		}
	}

	sort.Strings(sortedKeys)

	var id strings.Builder
	id.WriteString(metricName + "{")
	for i, key := range sortedKeys {
		if i > 0 {
			id.WriteString(",")
		}
		id.WriteString(fmt.Sprintf("%s=%s", key, t.tags[key]))
	}
	id.WriteString("}")

	return id.String()
}
