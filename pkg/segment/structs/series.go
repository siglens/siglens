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

type epoch uint32

type timeseries interface {
	GetTimestamps() []epoch
	// Gets the first value at or before the given timestamp.
	AtOrBefore(timestamp epoch) (float64, bool)
}

type entry struct {
	timestamp epoch
	value     float64
}

type normalTimeseries struct {
	values []entry
}

// TODO: make this more efficient.
func (t *normalTimeseries) GetTimestamps() []epoch {
	timestamps := make([]epoch, len(t.values))

	for i, entry := range t.values {
		timestamps[i] = entry.timestamp
	}

	return timestamps
}

func (t *normalTimeseries) AtOrBefore(timestamp epoch) (float64, bool) {
	i := sort.Search(len(t.values), func(k int) bool {
		return t.values[k].timestamp > timestamp
	})

	if i > 0 {
		return t.values[i-1].value, true
	}

	return 0, false
}
