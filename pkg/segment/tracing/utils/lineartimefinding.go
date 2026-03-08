// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"math"
	"math/rand"
	"sort"

	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/constraints"
)

type Number interface {
	constraints.Integer | constraints.Float
}

func FindPercentileData(arr []uint64, percentile int) uint64 {
	if len(arr) == 0 {
		log.Error("FindPercentileData: no duration exists")
		return 0
	}

	k := math.Floor(float64(percentile*(len(arr))) / float64(100))
	return quickSelect(arr, int(k), &rand.Rand{})
}

// https://rcoh.me/posts/linear-time-median-finding/
func quickSelect[T Number](arr []T, k int, rand *rand.Rand) T {
	if len(arr) == 1 {
		return arr[0]
	}

	pivot := pickPivot(arr, rand)

	var lows, highs, pivots []T
	for _, el := range arr {
		switch {
		case el < pivot:
			lows = append(lows, el)
		case el > pivot:
			highs = append(highs, el)
		default:
			pivots = append(pivots, el)
		}
	}

	if k < len(lows) {
		return quickSelect(lows, k, rand)
	} else if k < len(lows)+len(pivots) {
		return pivots[0]
	} else {
		return quickSelect(highs, k-len(lows)-len(pivots), rand)
	}
}

func pickPivot[T Number](arr []T, rand *rand.Rand) T {
	if len(arr) < 5 {
		return nLogNMedian(arr)
	}

	chunks := chunked(arr, 5)
	var fullChunks [][]T
	for _, chunk := range chunks {
		if len(chunk) == 5 {
			fullChunks = append(fullChunks, chunk)
		}
	}

	var sortedGroups [][]T
	for _, chunk := range fullChunks {
		sort.Slice(chunk, func(i, j int) bool {
			return chunk[i] < chunk[j]
		})
		sortedGroups = append(sortedGroups, chunk)
	}

	medians := make([]T, len(sortedGroups))
	for i, group := range sortedGroups {
		medians[i] = group[2]
	}

	return QuickSelectMedian(medians, rand)
}

func nLogNMedian[T Number](arr []T) T {

	sort.Slice(arr, func(i, j int) bool {
		return arr[i] < arr[j]
	})
	if len(arr)%2 == 1 {
		return arr[len(arr)/2]
	} else {
		return (arr[len(arr)/2-1] + arr[len(arr)/2]) / 2
	}
}

func QuickSelectMedian[T Number](arr []T, rand *rand.Rand) T {
	n := len(arr)
	if n%2 == 1 {
		return quickSelect(arr, n/2, rand)
	} else {
		return (quickSelect(arr, n/2-1, rand) + quickSelect(arr, n/2, rand)) / 2
	}
}

func chunked[T Number](arr []T, chunkSize int) [][]T {
	numChunks := (len(arr) + chunkSize - 1) / chunkSize
	result := make([][]T, numChunks)

	for i := 0; i < numChunks; i++ {
		start := i * chunkSize
		length := chunkSize
		if len(arr)-start < chunkSize {
			length = len(arr) - start
		}
		result[i] = arr[start : start+length]
	}

	return result
}
