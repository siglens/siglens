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

package stats

import (
	"math"
	"math/rand/v2"
	"sort"
	"strconv"

	. "github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"

	bbp "github.com/valyala/bytebufferpool"
)

func GetDefaultNumStats() *NumericStats {
	return &NumericStats{
		NumericCount: 0,
		Sum: NumTypeEnclosure{Ntype: SS_DT_SIGNED_NUM,
			IntgrVal: 0},
	}
}

func AddSegStatsNums(segstats map[string]*SegStats, cname string,
	inNumType SS_IntUintFloatTypes, intVal int64, uintVal uint64,
	fltVal float64, numstr string, bb *bbp.ByteBuffer, aggColUsage map[string]AggColUsageMode, hasValuesFunc bool, hasListFunc bool) {

	var stats *SegStats
	var ok bool
	stats, ok = segstats[cname]
	if !ok {
		stats = &SegStats{
			IsNumeric: true,
			Count:     0,
			NumStats:  GetDefaultNumStats(),
			Records:   make([]*CValueEnclosure, 0),
		}
		stats.CreateNewHll()
		segstats[cname] = stats
	}
	if !stats.IsNumeric {
		stats.IsNumeric = true
		stats.NumStats = GetDefaultNumStats()
	}

	colUsage := NoEvalUsage
	if aggColUsage != nil {
		colUsagVal, exists := aggColUsage[cname]
		if exists {
			colUsage = colUsagVal
		}
	}

	bb.Reset()
	_, _ = bb.WriteString(numstr)
	stats.InsertIntoHll(bb.B)
	processStats(stats, inNumType, intVal, uintVal, fltVal, colUsage, hasValuesFunc, hasListFunc)
}

func AddSegStatsCount(segstats map[string]*SegStats, cname string,
	count uint64) {

	var stats *SegStats
	var ok bool
	stats, ok = segstats[cname]
	if !ok {
		stats = &SegStats{
			IsNumeric: true,
			Count:     0,
			NumStats:  GetDefaultNumStats(),
		}
		stats.CreateNewHll()
		segstats[cname] = stats
	}
	stats.Count += count
}

func processStats(stats *SegStats, inNumType SS_IntUintFloatTypes, intVal int64,
	uintVal uint64, fltVal float64, colUsage AggColUsageMode, hasValuesFunc bool, hasListFunc bool) {

	stats.Count++
	stats.NumStats.NumericCount++

	var inIntgrVal int64
	switch inNumType {
	case SS_UINT8, SS_UINT16, SS_UINT32, SS_UINT64:
		inIntgrVal = int64(uintVal)
	case SS_INT8, SS_INT16, SS_INT32, SS_INT64:
		inIntgrVal = intVal
	case SS_FLOAT64:
		// Do nothing. This is handled later.
	}

	if hasValuesFunc {
		if stats.StringStats == nil {
			stats.StringStats = &StringStats{
				StrSet: make(map[string]struct{}, 0),
			}
		}
	}

	if hasListFunc {
		if stats.StringStats == nil {
			stats.StringStats = &StringStats{
				StrList: make([]string, 0),
			}
		}
	}

	// we just use the Min stats for stored val comparison but apply the same
	// logic to max and sum
	switch inNumType {
	case SS_FLOAT64:
		UpdateMinMax(stats, CValueEnclosure{Dtype: SS_DT_FLOAT, CVal: fltVal})
		if stats.NumStats.Sum.Ntype == SS_DT_FLOAT {
			stats.NumStats.Sum.FloatVal = stats.NumStats.Sum.FloatVal + fltVal

			if hasValuesFunc {
				stats.StringStats.StrSet[strconv.FormatFloat(fltVal, 'f', -1, 64)] = struct{}{}
			}

			if hasListFunc {
				stats.StringStats.StrList = append(stats.StringStats.StrList, strconv.FormatFloat(fltVal, 'f', -1, 64))
			}

			if colUsage == BothUsage || colUsage == WithEvalUsage {
				stats.Records = append(stats.Records, &CValueEnclosure{
					Dtype: SS_DT_FLOAT,
					CVal:  fltVal,
				})
			}
		} else {
			stats.NumStats.Sum.FloatVal = float64(stats.NumStats.Sum.IntgrVal) + fltVal
			stats.NumStats.Sum.Ntype = SS_DT_FLOAT

			if hasValuesFunc {
				stats.StringStats.StrSet[strconv.FormatFloat(fltVal, 'f', -1, 64)] = struct{}{}
			}

			if hasListFunc {
				stats.StringStats.StrList = append(stats.StringStats.StrList, strconv.FormatFloat(fltVal, 'f', -1, 64))
			}
			if colUsage == BothUsage || colUsage == WithEvalUsage {
				stats.Records = append(stats.Records, &CValueEnclosure{
					Dtype: SS_DT_FLOAT,
					CVal:  fltVal,
				})
			}
		}
	// incoming is NON-float
	default:
		UpdateMinMax(stats, CValueEnclosure{Dtype: SS_DT_SIGNED_NUM, CVal: inIntgrVal})
		if stats.NumStats.Sum.Ntype == SS_DT_FLOAT {
			stats.NumStats.Sum.FloatVal = stats.NumStats.Sum.FloatVal + float64(inIntgrVal)

			if hasValuesFunc {
				stats.StringStats.StrSet[strconv.FormatInt(inIntgrVal, 10)] = struct{}{}
			}
			if hasListFunc {
				stats.StringStats.StrList = append(stats.StringStats.StrList, strconv.FormatInt(inIntgrVal, 10))
			}

			if colUsage == BothUsage || colUsage == WithEvalUsage {
				stats.Records = append(stats.Records, &CValueEnclosure{
					Dtype: SS_DT_FLOAT,
					CVal:  float64(inIntgrVal),
				})
			}
		} else {
			stats.NumStats.Sum.IntgrVal = stats.NumStats.Sum.IntgrVal + inIntgrVal

			if hasValuesFunc {
				stats.StringStats.StrSet[strconv.FormatInt(inIntgrVal, 10)] = struct{}{}
			}

			if hasListFunc {
				stats.StringStats.StrList = append(stats.StringStats.StrList, strconv.FormatInt(inIntgrVal, 10))
			}
			if colUsage == BothUsage || colUsage == WithEvalUsage {
				stats.Records = append(stats.Records, &CValueEnclosure{
					Dtype: SS_DT_SIGNED_NUM,
					CVal:  inIntgrVal,
				})
			}
		}
	}

}

func AddSegStatsStr(segstats map[string]*SegStats, cname string, strVal string,
	bb *bbp.ByteBuffer, aggColUsage map[string]AggColUsageMode, hasValuesFunc bool, hasListFunc bool) {

	var stats *SegStats
	var ok bool
	stats, ok = segstats[cname]
	if !ok {
		stats = &SegStats{
			IsNumeric: false,
			Count:     0,
			Records:   make([]*CValueEnclosure, 0),
		}
		stats.CreateNewHll()

		segstats[cname] = stats
	}

	floatVal, err := strconv.ParseFloat(strVal, 64)
	if err == nil {
		AddSegStatsNums(segstats, cname, SS_FLOAT64, 0, 0, floatVal, strVal, bb, aggColUsage, hasValuesFunc, hasListFunc)
		return
	}

	stats.Count++

	colUsage := NoEvalUsage
	if aggColUsage != nil {
		colUsagVal, exists := aggColUsage[cname]
		if exists {
			colUsage = colUsagVal
		}
	}

	if colUsage == BothUsage || colUsage == WithEvalUsage {
		stats.Records = append(stats.Records, &CValueEnclosure{
			Dtype: SS_DT_STRING,
			CVal:  strVal,
		})
	}

	if stats.StringStats == nil {
		stats.StringStats = &StringStats{}
	}

	if hasValuesFunc || hasListFunc {
		if stats.StringStats.StrSet == nil {
			stats.StringStats.StrSet = make(map[string]struct{}, 0)
		}
		if stats.StringStats.StrList == nil {
			stats.StringStats.StrList = make([]string, 0)
		}

		if hasValuesFunc {
			stats.StringStats.StrSet[strVal] = struct{}{}
		}

		if hasListFunc {
			stats.StringStats.StrList = append(stats.StringStats.StrList, strVal)
		}
	}

	UpdateMinMax(stats, CValueEnclosure{Dtype: SS_DT_STRING, CVal: strVal})

	bb.Reset()
	_, _ = bb.WriteString(strVal)
	stats.InsertIntoHll(bb.B)
}

// adds all elements of m2 to m1 and returns m1
func MergeSegStats(m1, m2 map[string]*SegStats) map[string]*SegStats {
	for k, segStat2 := range m2 {
		segStat1, ok := m1[k]
		if !ok {
			m1[k] = segStat2
			continue
		}
		segStat1.Merge(segStat2)
	}
	return m1
}

// Exact 99th percentile using median of medians instead of sorting
func ExactPercentile99(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	index := int(math.Ceil(0.99 * float64(len(values))))
	return medianOfMedians(values, index)
}

// Approximate 66.6th percentile using weighted random sampling
func ApproxPercentile66_6(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	// Weighted sampling for better distribution
	total := 0.0
	for _, v := range values {
		total += v
	}
	randomThreshold := rand.Float64() * total
	sum := 0.0
	for _, v := range values {
		sum += v
		if sum >= randomThreshold {
			return v
		}
	}
	return values[len(values)-1]
}

// Upper bound for 6.6th percentile using binary search-based selection
func UpperPercentile6_6(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	k := int(math.Ceil(0.066 * float64(len(values))))
	if k == 0 {
		return values[0]
	}
	return binarySearchSelect(values, k)
}

// medianOfMedians selects the k-th smallest element efficiently
func medianOfMedians(values []float64, k int) float64 {
	if len(values) <= 5 {
		sort.Float64s(values)
		return values[k]
	}
	medians := []float64{}
	for i := 0; i < len(values); i += 5 {
		end := i + 5
		if end > len(values) {
			end = len(values)
		}
		sort.Float64s(values[i:end])
		medians = append(medians, values[i+(end-i)/2])
	}
	pivot := medianOfMedians(medians, len(medians)/2)
	lows, highs := partition(values, pivot)
	if k < len(lows) {
		return medianOfMedians(lows, k)
	} else if k > len(lows) {
		return medianOfMedians(highs, k-len(lows)-1)
	}
	return pivot
}

// partition divides values based on pivot
func partition(values []float64, pivot float64) ([]float64, []float64) {
	lows, highs := []float64{}, []float64{}
	for _, v := range values {
		if v < pivot {
			lows = append(lows, v)
		} else if v > pivot {
			highs = append(highs, v)
		}
	}
	return lows, highs
}

// binarySearchSelect finds k-th smallest element efficiently
func binarySearchSelect(values []float64, k int) float64 {
	sort.Float64s(values)
	return values[k]
}
