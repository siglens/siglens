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

package structs

import "sort"

type Series interface {
	At(timestamp uint32) (float64, bool)
	GetValues() []Entry

	GetLabels() map[string]string
	GetTagValue(tag string) (string, bool)
}

type Entry struct {
	timestamp uint32
	value     float64
}

type baseSeries struct {
	labels map[string]string
}

func (s *baseSeries) GetLabels() map[string]string {
	return s.labels
}

func (s *baseSeries) GetTagValue(tag string) (string, bool) {
	val, ok := s.labels[tag]
	return val, ok
}

type rawSeries struct {
	baseSeries
	values []Entry
}

func (s *rawSeries) At(timestamp uint32) (float64, bool) {
	i := sort.Search(len(s.values), func(k int) bool {
		return s.values[k].timestamp >= timestamp
	})

	if i >= 0 && i < len(s.values) && s.values[i].timestamp == timestamp {
		return s.values[i].value, true
	}

	return 0, false
}

func (s *rawSeries) GetValues() []Entry {
	return s.values
}
