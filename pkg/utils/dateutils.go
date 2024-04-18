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

package utils

import (
	"errors"

	log "github.com/sirupsen/logrus"
)

var HT_STEPS = []uint64{60_000, // 1m
	300_000,       // 5m
	600_000,       // 10m
	1800_000,      // 30m
	3600_000,      // 1h
	10800_000,     // 3h
	43200_000,     // 12h
	86400_000,     // 1d
	604800_000,    // 7d
	2592000_000,   // 30d
	7776000_000,   // 90d
	31536000_000,  // 1y
	315360000_000, // 10y
}

const MAX_HT_BUCKETS = 90

// Returns the size for histogram IntervalMillis.
func SanitizeHistogramInterval(startEpochMs uint64, endEpochMs uint64,
	intervalMs uint64) (uint64, error) {

	var retVal uint64

	if startEpochMs > endEpochMs {
		return retVal, errors.New("startEpochMs was higher than endEpochMs")
	}

	trange := endEpochMs - startEpochMs

	numBuckets := trange / intervalMs
	if numBuckets <= MAX_HT_BUCKETS {
		return intervalMs, nil
	}

	for _, cand := range HT_STEPS {
		numBuckets = trange / cand
		if numBuckets <= MAX_HT_BUCKETS {
			return cand, nil
		}
	}

	log.Infof("SanitizeHistogramInterval: returning really long 20y HT interval, should not have happened")
	return HT_STEPS[len(HT_STEPS)-1], nil
}
