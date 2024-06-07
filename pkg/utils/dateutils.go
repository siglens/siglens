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
	"fmt"
	"math"
	"strconv"
	"time"

	jp "github.com/buger/jsonparser"
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
		return retVal, fmt.Errorf("SanitizeHistogramInterval: startEpochMs: %v was higher than endEpochMs: %v", startEpochMs, endEpochMs)
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

	log.Warnf("SanitizeHistogramInterval: returning really long 20y HT interval, should not have happened")
	return HT_STEPS[len(HT_STEPS)-1], nil
}

func IsTimeInMilli(tval uint64) bool {
	if tval >= 99999999999 {
		return true
	} else {
		return false
	}
}

/**
* Check if the time value is in nano seconds
* Time in Seconds: 1e9
* Time in Milli Seconds: 1e12
* Time in Micro Seconds: 1e15
* Time in Nano Seconds: 1e18
 */
func IsTimeInNano(tval uint64) bool {
	return tval >= 1e18
}

func GetCurrentTimeInMs() uint64 {
	return uint64(time.Now().UTC().UnixNano()) / uint64(time.Millisecond)
}

// This function will extract the timestamp from the raw body. This will assume the timestamp key exists at the root level
func ExtractTimeStamp(raw []byte, timestampKey *string) uint64 {
	rawVal, dType, _, err := jp.Get(raw, *timestampKey)
	if err != nil {
		// timestamp key does not exist in doc
		return 0
	}
	switch dType {
	case jp.String:
		tsStr, err := jp.ParseString(rawVal)
		if err != nil {
			log.Errorf("ExtractTimeStamp: Failed to parse timestamp of raw string val: %v. Error: %v", rawVal, err)
			return 0
		}
		ts_millis, err := ConvertTimestampToMillis(tsStr)
		if err != nil {
			ts_millis = GetCurrentTimeInMs()
			log.Errorf("ExtractTimeStamp: Setting timestamp to current time in milli sec as parsing timestamp failed, err = %v", err)
		}
		return ts_millis
	case jp.Number:
		var ts_millis uint64
		val, err := jp.ParseInt(rawVal)
		if err != nil {
			val, err := jp.ParseFloat(rawVal)
			if err != nil {
				log.Errorf("ExtractTimeStamp: Failed to parse timestamp of float val: %v. Error: %v", rawVal, err)
				return 0
			}
			ts_millis = uint64(val)
		} else {
			ts_millis = uint64(val)
		}

		if !IsTimeInMilli(ts_millis) {
			ts_millis *= 1000
		}
		return ts_millis
	default:
		return 0
	}
}

func ConvertTimestampToMillis(value string) (uint64, error) {
	parsed_value, err := strconv.ParseUint(string(value), 10, 64)
	if err == nil {

		if IsTimeInNano(parsed_value) {
			parsed_value /= 1000000
		}

		if !IsTimeInMilli(parsed_value) {
			parsed_value *= 1000
		}
		return parsed_value, nil
	}

	timeFormats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05.999Z",
		"2006-01-02T15:04:05.999-07:00"}

	for _, timeFormat := range timeFormats {
		parsed_value, err := time.Parse(timeFormat, value)
		if err != nil {
			continue
		}
		return uint64(parsed_value.UTC().UnixNano() / 1000000), nil
	}
	return 0, fmt.Errorf("ExtractTimeStamp: couldn't find matching time format for value: %v", value)
}

// Helper function that parses a time parameter for use in PromQL.
// The time parameter can be in either epoch time format or RFC3339 format.
// If the time parameter is an empty string, the function returns an error.
// The function returns the parsed time as a uint32 Unix timestamp and an error if the parsing fails.
func ParseTimeForPromQL(timeParam string) (uint32, error) {
	if timeParam == "" {
		log.Errorf("ParseTimeForPromQL: time parameter is empty")
		return 0, errors.New("ParseTimeForPromQL: time parameter is empty")
	}

	// Try to parse as integer (Unix timestamp)
	parsedInt, err := strconv.ParseInt(timeParam, 10, 64)
	if err == nil {
		return uint32(parsedInt), nil
	}

	// Try to parse as float (Unix timestamp)
	parsedFloat, err := strconv.ParseFloat(timeParam, 64)
	if err == nil {
		return uint32(math.Round(parsedFloat)), nil
	}

	// Try to parse as RFC3339 timestamp
	parsedTime, err := time.Parse(time.RFC3339, timeParam)
	if err == nil {
		return uint32(parsedTime.Unix()), nil
	}

	// If all parsing attempts failed, return an error
	return 0, fmt.Errorf("ParseTimeForPromQL: failed to parse time parameter: %v as Unix timestamp or RFC3339 timestamp", timeParam)
}
