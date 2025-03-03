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

import "github.com/siglens/siglens/pkg/utils"

type TaggedSeries interface {
	Taggable
	timeseries
}

// Implements the TaggedSeries interface.
type Series struct {
	tags   TaggedMetric
	series timeseries
}

func NewTaggedSeries(tags map[string]string, series timeseries) *Series {
	return &Series{
		tags:   TaggedMetric{tags: tags},
		series: series,
	}
}

func (s *Series) GetValue(key string) (string, bool) {
	return s.tags.GetValue(key)
}

func (s *Series) SetValue(key string, value string) {
	s.tags.SetValue(key, value)
}

func (s *Series) RemoveTag(key string) {
	s.tags.RemoveTag(key)
}

func (s *Series) HasTag(key string) bool {
	return s.tags.HasTag(key)
}

func (s *Series) SetTags(tags map[string]string) {
	s.tags.SetTags(tags)
}

func (s *Series) Id() string {
	return s.tags.Id()
}

func (s *Series) AtOrBefore(timestamp epoch) (float64, bool) {
	return s.series.AtOrBefore(timestamp)
}

func (s *Series) Iterator() utils.Iterator[entry] {
	return s.series.Iterator()
}

func (s *Series) Range(start epoch, end epoch, mode RangeMode) timeseries {
	return s.series.Range(start, end, mode)
}
