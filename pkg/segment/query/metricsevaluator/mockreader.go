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

type DiskReader interface {
	Read(seriesId SeriesId) []SeriesResult
}

type MockReader struct {
	Data map[SeriesId][]Sample
}

func (m *MockReader) Read(seriesId SeriesId) []SeriesResult {
	allSeries := make([]SeriesResult, 0)

	for id, series := range m.Data {
		if seriesId.Matches(id) {
			allSeries = append(allSeries, SeriesResult{
				Labels: map[string]string{
					"__name__": string(id), // TODO: get the other labels from the id.
				},
				Values: series,
			})
		}
	}

	return allSeries
}
