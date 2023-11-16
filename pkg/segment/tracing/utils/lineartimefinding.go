package utils

import (
	"math"
	"math/rand"
	"sort"

	log "github.com/sirupsen/logrus"
)

func FindPercentileData(arr []uint64, percentile int) uint64 {
	if len(arr) == 0 {
		log.Error("FindPercentileData: no duration exists")
		return 0
	}
	if percentile > 100 || percentile < 0 {
		log.Error("FindPercentileData: percentile should in this period: [0, 100]")
		return 0
	}

	k := math.Floor(float64(percentile*(len(arr))) / float64(100))
	return quickSelect(arr, int(k), &rand.Rand{})
}

// https://rcoh.me/posts/linear-time-median-finding/
func quickSelect(arr []uint64, k int, rand *rand.Rand) uint64 {
	if len(arr) == 1 {
		return arr[0]
	}

	pivot := pickPivot(arr, rand)

	var lows, highs, pivots []uint64
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

func pickPivot(arr []uint64, rand *rand.Rand) uint64 {
	if len(arr) < 5 {
		return nLogNMedian(arr)
	}

	chunks := chunked(arr, 5)
	var fullChunks [][]uint64
	for _, chunk := range chunks {
		if len(chunk) == 5 {
			fullChunks = append(fullChunks, chunk)
		}
	}

	var sortedGroups [][]uint64
	for _, chunk := range fullChunks {
		sort.Slice(chunk, func(i, j int) bool {
			return chunk[i] < chunk[j]
		})
		sortedGroups = append(sortedGroups, chunk)
	}

	medians := make([]uint64, len(sortedGroups))
	for i, group := range sortedGroups {
		medians[i] = group[2]
	}

	return QuickSelectMedian(medians, rand)
}

func nLogNMedian(arr []uint64) uint64 {

	sort.Slice(arr, func(i, j int) bool {
		return arr[i] < arr[j]
	})
	if len(arr)%2 == 1 {
		return arr[len(arr)/2]
	} else {
		return (arr[len(arr)/2-1] + arr[len(arr)/2]) / 2
	}
}

func QuickSelectMedian(arr []uint64, rand *rand.Rand) uint64 {
	n := len(arr)
	if n%2 == 1 {
		return quickSelect(arr, n/2, rand)
	} else {
		return (quickSelect(arr, n/2-1, rand) + quickSelect(arr, n/2, rand)) / 2
	}
}

func chunked(arr []uint64, chunkSize int) [][]uint64 {
	numChunks := (len(arr) + chunkSize - 1) / chunkSize
	result := make([][]uint64, numChunks)

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
