// Copyright (c) 2021-2025 SigScalr, Inc.
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

package metrics

import (
	"fmt"
	"sort"
	"strings"
)

type TaggedSeries struct {
	timeseries
	tags    map[string]string
	groupId string
}

func NewTaggedSeries(tags map[string]string, series timeseries, groupId string) *TaggedSeries {
	if tags == nil {
		tags = make(map[string]string)
	}

	result := &TaggedSeries{
		timeseries: series,
		tags:       tags,
		groupId:    groupId,
	}

	err := result.SetTagsFromId(groupId)
	if err != nil {
		// TODO: andrew
		return nil
	}

	return result
}

func (t *TaggedSeries) GetGroupId() string {
	return t.groupId
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

func (t *TaggedSeries) SetTagsFromId(id string) error {
	tags := make(map[string]string)

	// Strip the final '}' if it exists.
	if len(id) > 0 && id[len(id)-1] == '}' {
		id = id[:len(id)-1]
	}

	// Parse metric name.
	idx := strings.Index(id, "{")
	if idx == -1 {
		return fmt.Errorf("ID is missing '{': %v", id)
	}

	tags["__name__"] = id[:idx]

	// Parse tags.
	id = id[idx+1 : len(id)]
	pairs := strings.Split(id, ",")
	for _, pair := range pairs {
		kv := strings.Split(pair, "=")
		if len(kv) != 2 {
			continue
		}
		tags[kv[0]] = kv[1]
	}

	t.tags = tags
	return nil
}

func (t *TaggedSeries) Downsample(interval Epoch, aggregator func([]float64) float64) error {
	if interval <= 0 {
		return fmt.Errorf("non-positive interval %v", interval)
	}
	if aggregator == nil {
		return fmt.Errorf("nil aggregator")
	}

	downsampler := &downsampler{
		timeseries: t.timeseries,
		aggregator: aggregator,
		interval:   interval,
	}

	t.timeseries = downsampler.evaluate()

	return nil
}

// TODO: extract the groupId from the input series.
func Aggregate(inputSeries []*TaggedSeries, aggregator func([]float64) float64,
	groupId string) (*TaggedSeries, error) {

	timeseries := make([]timeseries, 0, len(inputSeries))
	for _, s := range inputSeries {
		timeseries = append(timeseries, s.timeseries)
	}

	aggSeries, err := NewAggSeries(timeseries, aggregator)
	if err != nil {
		return nil, err
	}

	return NewTaggedSeries(nil, aggSeries, groupId), nil
}
