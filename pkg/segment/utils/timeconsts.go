package utils

import "fmt"

type TimeUnit uint8

const (
	TMUs TimeUnit = iota
	TMMs
	TMCs
	TMDs
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
		return TMUs, nil
	case "ms":
		return TMMs, nil
	case "cs":
		return TMCs, nil
	case "ds":
		return TMDs, nil
	default:
		return 0, fmt.Errorf("ConvertSubseconds: can not convert: %v", subsecond)
	}
}
