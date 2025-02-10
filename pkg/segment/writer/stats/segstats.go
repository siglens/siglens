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
	"math/rand"
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

// ExactPercentile99 calculates the exact 99th percentile using QuickSelect.
func ExactPerc(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}
	pos := int(math.Ceil(0.99 * float64(len(data))))
	return QuickSelect(data, pos)
}

// ApproximatePercentile66_6 calculates an approximate 66.6th percentile using reservoir sampling.
func Perc(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}

	const reservoirSize = 1000
	var sampledValues []float64

	if len(data) > reservoirSize {
		sampledValues = make([]float64, reservoirSize)
		copy(sampledValues, data[:reservoirSize])

		for i := reservoirSize; i < len(data); i++ {
			j := rand.Intn(i + 1)
			if j < reservoirSize {
				sampledValues[j] = data[i]
			}
		}
		sort.Float64s(sampledValues)
	} else {
		sampledValues = append([]float64{}, data...)
		sort.Float64s(sampledValues)
	}

	pos := int(math.Round(0.666 * float64(len(sampledValues)-1)))
	return sampledValues[pos]
}

// UpperPercentile6_6 estimates the upper bound for the 6.6th percentile.
func UpperPerc(data []float64) float64 {
	if len(data) == 0 {
		return 0
	}

	k := int(math.Ceil(0.066 * float64(len(data))))
	if k == 0 {
		return data[0]
	}

	return QuickSelect(data, k)
}

// QuickSelect finds the k-th smallest element efficiently.
func QuickSelect(arr []float64, k int) float64 {
	if len(arr) == 1 {
		return arr[0]
	}

	pivot := arr[rand.Intn(len(arr))]
	var l, h, pivots []float64

	for _, val := range arr {
		switch {
		case val < pivot:
			l = append(l, val)
		case val > pivot:
			h = append(h, val)
		default:
			pivots = append(pivots, val)
		}
	}

	if k < len(l) {
		return QuickSelect(l, k)
	} else if k < len(l)+len(pivots) {
		return pivots[0]
	} else {
		return QuickSelect(h, k-len(l)-len(pivots))
	}
}
