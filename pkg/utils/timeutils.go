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
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/siglens/siglens/pkg/common/dtypeutils"
)

const MIN_IN_MS = 60_000
const HOUR_IN_MS = 3600_000
const DAY_IN_MS = 86400_000
const TEN_YEARS_IN_SECS = 315_360_000

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

// Function to truncate float64 to a given precision
func ToFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num*output)) / output
}

func EpochIsSeconds(epoch uint64) bool {
	return epoch < uint64(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC).Unix())
}

/*
Supports "now-[Num][Unit]"
Num ==> any positive integer
Unit ==> m(minutes), h(hours), d(days)
*/
func ParseAlphaNumTime(nowTs uint64, inp string, defValue uint64) uint64 {
	sanTime := strings.ReplaceAll(inp, " ", "")
	nowPrefix := "now-"

	if sanTime == "now" {
		return nowTs
	}

	retVal := defValue

	strln := len(sanTime)
	if strln < len(nowPrefix)+2 {
		return defValue
	}

	// check for prefix 'now-' in the input string
	if !strings.HasPrefix(sanTime, nowPrefix) {
		return defValue
	}

	// check for invalid time units
	unit := sanTime[strln-1]
	if unit != 'm' && unit != 'h' && unit != 'd' {
		return defValue
	}

	num, err := strconv.ParseInt(sanTime[len(nowPrefix):strln-1], 10, 64)
	if err != nil || num < 0 {
		return defValue
	}

	switch unit {
	case 'm':
		retVal = nowTs - MIN_IN_MS*uint64(num)
	case 'h':
		retVal = nowTs - HOUR_IN_MS*uint64(num)
	case 'd':
		retVal = nowTs - DAY_IN_MS*uint64(num)
	}
	return retVal
}

// Should either be a unix epoch in seconds or a string like "now-1h".
type Epoch struct {
	IntValue    uint64
	StringValue string
	IsString    bool
	IsInt       bool
}

// Implement https://pkg.go.dev/encoding/json#Unmarshaler
func (e *Epoch) UnmarshalJSON(rawJson []byte) error {
	// Try to unmarshal as int
	var intVal uint64
	if err := json.Unmarshal(rawJson, &intVal); err == nil {
		e.IntValue = intVal
		e.IsInt = true
		return nil
	}

	// Try to unmarshal as string
	var stringVal string
	if err := json.Unmarshal(rawJson, &stringVal); err == nil {
		e.StringValue = stringVal
		e.IsString = true
		return nil
	}

	return fmt.Errorf("UnmarshalJSON: failed to unmarshal Epoch from json: %s", rawJson)
}

func (e *Epoch) UnixSeconds(now time.Time) (uint64, error) {
	if (e.IsInt && e.IsString) || (!e.IsInt && !e.IsString) {
		return 0, fmt.Errorf("UnixSeconds: Epoch %+v is invalid", e)
	}

	if e.IsInt {
		epoch := e.IntValue
		if !EpochIsSeconds(epoch) {
			return 0, fmt.Errorf("UnixSeconds: Epoch is not in seconds: %v", epoch)
		}

		return epoch, nil
	}

	if e.IsString {
		nowMillis := uint64(now.UnixMilli())
		epoch := ParseAlphaNumTime(nowMillis, e.StringValue, nowMillis)
		epoch /= 1000 // Convert to seconds

		return epoch, nil
	}

	return 0, fmt.Errorf("UnixSeconds: Epoch %+v is not a string or int", e)
}

func GetMetricsTimeRange(startEpoch Epoch, endEpoch Epoch, now time.Time) (*dtypeutils.MetricsTimeRange, error) {
	start, err := startEpoch.UnixSeconds(now)
	if err != nil {
		return nil, fmt.Errorf("GetMetricsTimeRange: failed to get start time from: %v, err: %v", now, err)
	}

	end, err := endEpoch.UnixSeconds(now)
	if err != nil {
		return nil, fmt.Errorf("GetMetricsTimeRange: failed to get end time from: %v, err: %v", now, err)
	}

	if start >= end {
		return nil, fmt.Errorf("GetMetricsTimeRange: start time %v is not before end time %v", start, end)
	}

	return &dtypeutils.MetricsTimeRange{
		StartEpochSec: uint32(start),
		EndEpochSec:   uint32(end),
	}, nil
}
