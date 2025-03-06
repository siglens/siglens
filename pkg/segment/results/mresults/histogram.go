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

import "math"

type histogramBin struct {
	upperBound uint64 // inclusive
	count      float64
}

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

	return 0, nil // TODO
}
