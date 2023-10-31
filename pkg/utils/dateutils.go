/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
