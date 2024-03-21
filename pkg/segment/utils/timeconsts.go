package utils

import (
	"fmt"
	"time"
)

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

func GetIntervalInMillis(num interface{}, timeUnit TimeUnit) uint64 {
	var numD float64

	switch n := num.(type) {
	case int:
		numD = float64(n)
	case float64:
		numD = n
	default:
		// Handle unsupported type
		return 0
	}

	switch timeUnit {
	case TMMicrosecond:
		// Might not has effect for 'us', because smallest time unit for timestamp in siglens is ms
	case TMMillisecond:
		return uint64(numD)
	case TMCentisecond:
		return uint64(numD * 10)
	case TMDecisecond:
		return uint64(numD * 100)
	case TMSecond:
		return uint64(time.Duration(numD * float64(time.Second)).Milliseconds())
	case TMMinute:
		return uint64(time.Duration(numD * float64(time.Minute)).Milliseconds())
	case TMHour:
		return uint64(time.Duration(numD * float64(time.Hour)).Milliseconds())
	case TMDay:
		return uint64(time.Duration(numD * 24 * float64(time.Hour)).Milliseconds())
	case TMWeek:
		return uint64(time.Duration(numD * 7 * 24 * float64(time.Hour)).Milliseconds())
	case TMMonth:
		return uint64(time.Duration(numD * 30 * 24 * float64(time.Hour)).Milliseconds())
	case TMQuarter:
		return uint64(time.Duration(numD * 120 * 24 * float64(time.Hour)).Milliseconds())
	}
	return uint64(time.Duration(10 * time.Minute * time.Millisecond).Milliseconds())
}
