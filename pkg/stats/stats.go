package stats

import (
	"errors"
	"sort"
)

// exactperc calculates the exact percentile of a slice of float64.
func exactperc(values []float64, percentile float64) (float64, error) {
	if len(values) == 0 {
		return 0, errors.New("exactperc: empty slice")
	}
	sort.Float64s(values)
	index := int(percentile * float64(len(values)-1) / 100)
	return values[index], nil
}

// exactperc99 calculates the 99th percentile of a slice of float64.
func exactperc99(values []float64) (float64, error) {
	return exactperc(values, 99)
}

// perc66_6 calculates the 66.6th percentile of a slice of float64.
func perc66_6(values []float64) (float64, error) {
	return exactperc(values, 66.6)
}

// upperperc6_6 calculates the upper 6.6th percentile of a slice of float64.
func upperperc6_6(values []float64) (float64, error) {
	return exactperc(values, 93.4) // 100 - 6.6 = 93.4
}
