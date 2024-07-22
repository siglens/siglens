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
	"fmt"
	"time"
)

type TimeUnit uint8

const (
	TMInvalid TimeUnit = iota
	TMMicrosecond
	TMMillisecond
	TMCentisecond
	TMDecisecond
	TMSecond
	TMMinute
	TMHour
	TMDay
	TMWeek
	TMMonth
	TMQuarter
	TMYear
)

// Convert subseconds
func ConvertSubseconds(subsecond string) (TimeUnit, error) {
	switch subsecond {
	case "us":
		return TMMicrosecond, nil
	case "ms":
		return TMMillisecond, nil
	case "cs":
		return TMCentisecond, nil
	case "ds":
		return TMDecisecond, nil
	default:
		return 0, fmt.Errorf("ConvertSubseconds: can not convert: %v", subsecond)
	}
}

func IsSubseconds(timeUnit TimeUnit) bool {
	switch timeUnit {
	case TMMicrosecond, TMMillisecond, TMCentisecond, TMDecisecond:
		return true
	default:
		return false
	}
}

// Common method to apply offsets to time
func ApplyOffsetToTime(num int64, unit TimeUnit, t time.Time) (time.Time, error) {

	durNum := time.Duration(num)

	switch unit {
	case TMMicrosecond:
		return t.Add(durNum * time.Microsecond), nil
	case TMMillisecond:
		return t.Add(durNum * time.Millisecond), nil
	case TMCentisecond:
		return t.Add(durNum * 10 * time.Millisecond), nil
	case TMDecisecond:
		return t.Add(durNum * 100 * time.Millisecond), nil
	case TMSecond:
		return t.Add(durNum * time.Second), nil
	case TMMinute:
		return t.Add(durNum * time.Minute), nil
	case TMHour:
		return t.Add(durNum * time.Hour), nil
	case TMDay:
		return t.AddDate(0, 0, int(num)), nil
	case TMWeek:
		return t.AddDate(0, 0, 7*int(num)), nil
	case TMMonth:
		return t.AddDate(0, int(num), 0), nil
	case TMQuarter:
		return t.AddDate(0, 4*int(num), 0), nil
	case TMYear:
		return t.AddDate(int(num), 0, 0), nil
	default:
		return t, fmt.Errorf("Unsupported time unit for offset: %v", unit)
	}
}
