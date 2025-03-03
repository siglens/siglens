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

package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_TaggedMetricImplementsTaggable(t *testing.T) {
	var _ Taggable = &TaggedMetric{}
}

func Test_TaggedMetric(t *testing.T) {
	item := NewTaggedMetric()

	t.Run("Id", func(t *testing.T) {
		assert.Equal(t, "{}", item.Id())

		item.SetTags(map[string]string{"__name__": "metric_name"})
		assert.Equal(t, "metric_name{}", item.Id())

		item.SetTags(map[string]string{"key1": "value1"})
		assert.Equal(t, "{key1=value1}", item.Id())

		item.SetTags(map[string]string{"key1": "value1", "key2": "value2"})
		assert.Equal(t, "{key1=value1,key2=value2}", item.Id())

		item.SetTags(map[string]string{"__name__": "metric_name", "key1": "value1", "key2": "value2"})
		assert.Equal(t, "metric_name{key1=value1,key2=value2}", item.Id())
	})

	t.Run("Set and get values", func(t *testing.T) {
		item.SetValue("key1", "value1")
		val, exists := item.GetValue("key1")
		assert.True(t, exists)
		assert.Equal(t, "value1", val)

		// Non-existent key
		_, exists = item.GetValue("nonexistent")
		assert.False(t, exists)
	})

	t.Run("HasTag", func(t *testing.T) {
		assert.True(t, item.HasTag("key1"))
		assert.False(t, item.HasTag("nonexistent"))
	})

	t.Run("RemoveTag", func(t *testing.T) {
		item.SetValue("key1", "value1")
		assert.True(t, item.HasTag("key1"))
		item.RemoveTag("key1")
		assert.False(t, item.HasTag("key1"))
	})

	t.Run("SetTags", func(t *testing.T) {
		item.SetTags(map[string]string{"key1": "value1"})
		assert.True(t, item.HasTag("key1"))
		assert.False(t, item.HasTag("key2"))
		assert.False(t, item.HasTag("key3"))

		item.SetTags(map[string]string{"key2": "value2", "key3": "value3"})

		// Check new tags exist
		val, exists := item.GetValue("key2")
		assert.True(t, exists)
		assert.Equal(t, "value2", val)

		val, exists = item.GetValue("key3")
		assert.True(t, exists)
		assert.Equal(t, "value3", val)

		// Check old tags are gone
		assert.False(t, item.HasTag("key1"))
	})
}
