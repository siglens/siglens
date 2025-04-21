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

package metricsevaluator

import (
	"strings"

	"github.com/prometheus/prometheus/model/labels"
)

type DiskReader interface {
	Read(labels []*labels.Matcher, endTime uint32, lookback uint32) []SeriesResult
}

type MockReader struct {
	Data map[SeriesId][]Sample
}

func (m *MockReader) Read(labels []*labels.Matcher, _endTime uint32, _lookback uint32) []SeriesResult {
	allSeries := make([]SeriesResult, 0)

	for id, series := range m.Data {
		if id.Matches(labels) {
			allSeries = append(allSeries, SeriesResult{
				Labels: parseLabels(id),
				Values: series,
			})
		}
	}

	return allSeries
}

func parseLabels(seriesId SeriesId) map[string]string {
	labels := make(map[string]string)

	// Split the seriesId into metric name and labels
	parts := strings.SplitN(string(seriesId), "{", 2)
	if len(parts) != 2 {
		labels["__name__"] = strings.Trim(string(seriesId), "{}")
		return labels
	}

	labels["__name__"] = parts[0]

	// Remove the closing brace
	labelStr := strings.TrimSuffix(parts[1], "}")

	// Split the labels into key=value pairs
	labelPairs := strings.Split(labelStr, ",")
	for _, pair := range labelPairs {
		keyValue := strings.SplitN(pair, "=", 2)
		if len(keyValue) != 2 {
			continue
		}
		key := strings.TrimSpace(keyValue[0])
		value := strings.Trim(keyValue[1], `"`)
		labels[key] = value
	}

	return labels
}
