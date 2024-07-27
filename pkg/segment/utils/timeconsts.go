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
	"strconv"
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

type RelativeTimeExpr struct {
	Snap     string
	Offset   int64
	TimeUnit TimeUnit
}

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

// This function would snap backwards based on unit present.
// For e.x. Consider the time (Wednesday) 06/05/2024:13:37:05.123 (mm/dd/yyyy:hh:mm:ss)
// Snapping on Second would be 06/05/2024:13:37:05.000
// Snapping on Minute would be 06/05/2024:13:37:00.000
// Snapping on Hour would be 06/05/2024:13:00:00.000
// Snapping on Day would be 06/05/2024:00:00:00.000
// Snapping on Month would be 06/05/2024:00:00:00.000
// Snapping on Quarter (would snap to recent most quarter out of Jan 1, Apr 1, Jul 1, Oct 1) would be 04/01/2024:00:00:00.000
// Snapping on Year would be 01/01/2024:00:00:00.000
// Snapping on weekdays (w0 to w7) would snap backward to that weekday.
// Snapping on w0 would be (Sunday) 06/02/2024:00:00:00.000
// Snapping on w1 would be (Monday) 06/03/2024:00:00:00.000 and so on.
// Snap on w0 and w7 is same.
// snap parameter would be a string of the form w0 or it would be utils.TimeUnit constant integers converted to string type (see Rule: RelTimeUnit)
func ApplySnap(snap string, t time.Time) (time.Time, error) {
	sec := t.Second()
	min := t.Minute()
	hour := t.Hour()
	day := t.Day()
	week := t.Weekday()
	mon := t.Month()
	year := t.Year()

	if snap[0] != 'w' {
		tunit, err := strconv.Atoi(snap)
		if err != nil {
			return t, fmt.Errorf("Error while converting the snap: %v to integer, err: %v", snap, err)
		}

		switch TimeUnit(tunit) {
		case TMSecond:
			return time.Date(year, mon, day, hour, min, sec, 0, time.Local), nil
		case TMMinute:
			return time.Date(year, mon, day, hour, min, 0, 0, time.Local), nil
		case TMHour:
			return time.Date(year, mon, day, hour, 0, 0, 0, time.Local), nil
		case TMDay:
			return time.Date(year, mon, day, 0, 0, 0, 0, time.Local), nil
		case TMWeek:
			diff := week - time.Sunday
			return time.Date(year, mon, day-int(diff), 0, 0, 0, 0, time.Local), nil
		case TMMonth:
			return time.Date(year, mon, 1, 0, 0, 0, 0, time.Local), nil
		case TMQuarter:
			if mon >= time.October {
				mon = time.October
			} else if mon >= time.July {
				mon = time.July
			} else if mon >= time.April {
				mon = time.April
			} else {
				mon = time.January
			}
			return time.Date(year, mon, 1, 0, 0, 0, 0, time.Local), nil
		case TMYear:
			return time.Date(year, 1, 1, 0, 0, 0, 0, time.Local), nil
		default:
			return t, fmt.Errorf("Unsupported time unit for relative timestamp: %v", tunit)
		}
	} else {
		if len(snap) != 2 {
			return t, fmt.Errorf("Error for special week snap, should follow the regex w[0-7] got: %v", snap)
		}
		weeknum := int(snap[1] - '0')
		if weeknum == 7 {
			weeknum = 0
		}
		diff := int(week) - weeknum
		if diff < 0 {
			diff += 7
		}
		return time.Date(year, mon, day, 0, 0, 0, 0, time.Local).AddDate(0, 0, -diff), nil
	}
}

// ConvertCustomDateTimeFormatToEpochMs converts a date string in the format "MM/DD/YYYY:HH:MM:SS"
// to Unix time in milliseconds (epoch ms).
func ConvertCustomDateTimeFormatToEpochMs(dateStr string) (int64, error) {
	loc, _ := time.LoadLocation("Local")
	t, err := time.ParseInLocation("01/02/2006:15:04:05", dateStr, loc)
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}

func CalculateAdjustedTimeForRelativeTimeCommand(timeModifier RelativeTimeExpr, currTime time.Time) (int64, error) {
	var err error
	if timeModifier.Offset != 0 {
		currTime, err = ApplyOffsetToTime(timeModifier.Offset, timeModifier.TimeUnit, currTime)
		if err != nil {
			return 0, err
		}
	}
	if timeModifier.Snap != "" {
		currTime, err = ApplySnap(timeModifier.Snap, currTime)
		if err != nil {
			return 0, err
		}
	}

	return currTime.UnixMilli(), nil
}
