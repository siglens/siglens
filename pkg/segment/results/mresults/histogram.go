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

package mresults

import (
	"fmt"
	"math"
	"sort"
)

type histogramBin struct {
	upperBound float64 // inclusive
	count      float64
}

// This mimics PromQL's histogram_quantile function for classic histograms.
// See https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile
// for details.
func histogramQuantile(quantile float64, bins []histogramBin) (float64, error) {
	if quantile < 0 {
		return math.Inf(-1), nil
	}
	if quantile > 1 {
		return math.Inf(1), nil
	}
	if math.IsNaN(quantile) {
		return math.NaN(), nil
	}
	if len(bins) < 2 {
		return math.NaN(), nil
	}

	sort.Slice(bins, func(i, j int) bool {
		return bins[i].upperBound < bins[j].upperBound
	})

	lastBin := bins[len(bins)-1]
	if !math.IsInf(lastBin.upperBound, 0) || lastBin.count == 0 {
		return math.NaN(), nil
	}

	// Verify monotonically increasing counts.
	prevCount := bins[0].count
	for i := 1; i < len(bins); i++ {
		if bins[i].count < prevCount {
			sum := bins[i].count + prevCount
			diff := prevCount - bins[i].count

			if diff > sum*1e-12 {
				return 0, fmt.Errorf("histogram counts are not monotonically increasing")
			}
		}

		prevCount = bins[i].count
	}

	return 0, nil // TODO
}
