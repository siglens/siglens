// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
}
