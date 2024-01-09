package utils

import "fmt"

type TimeUnit uint8

const (
	TMMicrosecond TimeUnit = iota
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
