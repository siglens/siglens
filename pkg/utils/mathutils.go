// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

func CalculateStandardVariance(values []float64) float64 {
	sum := 0.0
	length := float64(len(values))
	for _, val := range values {
		sum += val
	}
	avg := sum / length
	sumValSquare := 0.0
	for _, val := range values {
		sumValSquare += (val - avg) * (val - avg)
	}
	return sumValSquare / length
}

func MinUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
