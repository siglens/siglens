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
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_TaggedSeriesImplementsTimeseries(t *testing.T) {
	var _ timeseries = &TaggedSeries{}
}

func Test_TaggedMetric(t *testing.T) {
	series := TaggedSeries{}

	t.Run("Id", func(t *testing.T) {
		assert.Equal(t, "{}", series.Id())

		series.SetTags(map[string]string{"__name__": "metric_name"})
		assert.Equal(t, "metric_name{}", series.Id())

		series.SetTags(map[string]string{"key1": "value1"})
		assert.Equal(t, "{key1=value1}", series.Id())

		series.SetTags(map[string]string{"key1": "value1", "key2": "value2"})
		assert.Equal(t, "{key1=value1,key2=value2}", series.Id())

		series.SetTags(map[string]string{"__name__": "metric_name", "key1": "value1", "key2": "value2"})
		assert.Equal(t, "metric_name{key1=value1,key2=value2}", series.Id())
	})

	t.Run("Set and get values", func(t *testing.T) {
		series.SetValue("key1", "value1")
		val, exists := series.GetValue("key1")
		assert.True(t, exists)
		assert.Equal(t, "value1", val)

		// Non-existent key
		_, exists = series.GetValue("nonexistent")
		assert.False(t, exists)
	})

	t.Run("HasTag", func(t *testing.T) {
		assert.True(t, series.HasTag("key1"))
		assert.False(t, series.HasTag("nonexistent"))
	})

	t.Run("RemoveTag", func(t *testing.T) {
		series.SetValue("key1", "value1")
		assert.True(t, series.HasTag("key1"))
		series.RemoveTag("key1")
		assert.False(t, series.HasTag("key1"))
	})

	t.Run("SetTags", func(t *testing.T) {
		series.SetTags(map[string]string{"key1": "value1"})
		assert.True(t, series.HasTag("key1"))
		assert.False(t, series.HasTag("key2"))
		assert.False(t, series.HasTag("key3"))

		series.SetTags(map[string]string{"key2": "value2", "key3": "value3"})

		// Check new tags exist
		val, exists := series.GetValue("key2")
		assert.True(t, exists)
		assert.Equal(t, "value2", val)

		val, exists = series.GetValue("key3")
		assert.True(t, exists)
		assert.Equal(t, "value3", val)

		// Check old tags are gone
		assert.False(t, series.HasTag("key1"))
	})

	t.Run("SetTagsFromId", func(t *testing.T) {
		err := series.SetTagsFromId("metric_name{key1=value1,key2=value2}")
		assert.NoError(t, err)
		assert.Equal(t, "metric_name{key1=value1,key2=value2}", series.Id())
		value, ok := series.GetValue("key1")
		assert.True(t, ok)
		assert.Equal(t, "value1", value)
		value, ok = series.GetValue("key2")
		assert.True(t, ok)
		assert.Equal(t, "value2", value)

		err = series.SetTagsFromId("metric_name{")
		assert.NoError(t, err)

		err = series.SetTagsFromId("invalid")
		assert.Error(t, err)
	})
}

func Test_GroupBy(t *testing.T) {
	series1 := &TaggedSeries{tags: map[string]string{"__name__": "metric_name", "key1": "value1", "key2": "value2"}}
	series2 := &TaggedSeries{tags: map[string]string{"__name__": "metric_name", "key1": "value2", "key2": "value2"}}
	series3 := &TaggedSeries{tags: map[string]string{"__name__": "other_metric", "key1": "value1"}}
	allSeries := []*TaggedSeries{series1, series2, series3}

	t.Run("NormalKeys", func(t *testing.T) {
		keys := []string{"key2", "key1"} // Should get sorted to key1, key2
		grouped := GroupBy(allSeries, keys)
		assert.Len(t, grouped, 3)

		values, ok := grouped[`{key1="value1",key2="value2"}`]
		assert.True(t, ok)
		assert.Len(t, values, 1)
		assert.Contains(t, values, series1)

		values, ok = grouped[`{key1="value2",key2="value2"}`]
		assert.True(t, ok)
		assert.Len(t, values, 1)
		assert.Contains(t, values, series2)

		values, ok = grouped[`{key1="value1",key2=""}`]
		assert.True(t, ok)
		assert.Len(t, values, 1)
		assert.Contains(t, values, series3)
	})

	t.Run("MetricName", func(t *testing.T) {
		keys := []string{"__name__"}
		grouped := GroupBy(allSeries, keys)
		assert.Len(t, grouped, 2)

		values, ok := grouped[`metric_name{}`]
		assert.True(t, ok)
		assert.Len(t, values, 2)
		assert.Contains(t, values, series1)
		assert.Contains(t, values, series2)

		values, ok = grouped[`other_metric{}`]
		assert.True(t, ok)
		assert.Len(t, values, 1)
		assert.Contains(t, values, series3)
	})

	t.Run("NoKeys", func(t *testing.T) {
		keys := []string{}
		grouped := GroupBy(allSeries, keys)
		assert.Len(t, grouped, 1)

		values, ok := grouped["{}"]
		assert.True(t, ok)
		assert.Len(t, values, 3)
		assert.Contains(t, values, series1)
		assert.Contains(t, values, series2)
		assert.Contains(t, values, series3)
	})
}
