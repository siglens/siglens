package utils

import (
	"testing"
)

func TestFindPercentileData(t *testing.T) {
	testCases := []struct {
		arr        []uint64
		percentile int
		expected   uint64
	}{
		{
			arr:        []uint64{44, 11, 22},
			percentile: 66,
			expected:   22,
		},
		{
			arr:        []uint64{44, 11, 22},
			percentile: 67,
			expected:   44,
		},
		{
			arr:        []uint64{20, 50, 40, 30, 10},
			percentile: 35,
			expected:   20,
		},
		{
			arr:        []uint64{20, 50, 40, 30, 10},
			percentile: 95,
			expected:   50,
		},
	}

	for _, tc := range testCases {
		result := FindPercentileData(tc.arr, tc.percentile)
		if result != tc.expected {
			t.Errorf("Expected %d percentile to be %d, but got %d", tc.percentile, tc.expected, result)
		}
	}
}
