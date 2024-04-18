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
	"strconv"

	. "github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"

	"github.com/axiomhq/hyperloglog"
	bbp "github.com/valyala/bytebufferpool"
)

func AddSegStatsNums(segstats map[string]*SegStats, cname string,
	inNumType SS_IntUintFloatTypes, intVal int64, uintVal uint64,
	fltVal float64, numstr string, bb *bbp.ByteBuffer, aggColUsage map[string]AggColUsageMode, hasValuesFunc bool) {

	var stats *SegStats
	var ok bool
	stats, ok = segstats[cname]
	if !ok {
		numStats := &NumericStats{
			Min: NumTypeEnclosure{Ntype: SS_DT_SIGNED_NUM,
				IntgrVal: math.MaxInt64},
			Max: NumTypeEnclosure{Ntype: SS_DT_SIGNED_NUM,
				IntgrVal: math.MinInt64},
			Sum: NumTypeEnclosure{Ntype: SS_DT_SIGNED_NUM,
				IntgrVal: 0},
			Dtype: SS_DT_SIGNED_NUM,
		}
		stats = &SegStats{
			IsNumeric: true,
			Count:     0,
			Hll:       hyperloglog.New16(),
			NumStats:  numStats,
			Records:   make([]*CValueEnclosure, 0),
		}
		segstats[cname] = stats
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
	stats.Hll.Insert(bb.B)
	processStats(stats, inNumType, intVal, uintVal, fltVal, colUsage, hasValuesFunc)
}

func AddSegStatsCount(segstats map[string]*SegStats, cname string,
	count uint64) {

	var stats *SegStats
	var ok bool
	stats, ok = segstats[cname]
	if !ok {
		numStats := &NumericStats{
			Min: NumTypeEnclosure{Ntype: SS_DT_SIGNED_NUM,
				IntgrVal: math.MaxInt64},
			Max: NumTypeEnclosure{Ntype: SS_DT_SIGNED_NUM,
				IntgrVal: math.MinInt64},
			Sum: NumTypeEnclosure{Ntype: SS_DT_SIGNED_NUM,
				IntgrVal: 0},
			Dtype: SS_DT_SIGNED_NUM,
		}
		stats = &SegStats{
			IsNumeric: true,
			Count:     0,
			Hll:       hyperloglog.New16(),
			NumStats:  numStats,
		}
		segstats[cname] = stats
	}
	stats.Count += count
}

func processStats(stats *SegStats, inNumType SS_IntUintFloatTypes, intVal int64,
	uintVal uint64, fltVal float64, colUsage AggColUsageMode, hasValuesFunc bool) {

	stats.Count++

	var inIntgrVal int64
	switch inNumType {
	case SS_UINT8, SS_UINT16, SS_UINT32, SS_UINT64:
		inIntgrVal = int64(uintVal)
	case SS_INT8, SS_INT16, SS_INT32, SS_INT64:
		inIntgrVal = intVal
	}

	if hasValuesFunc {
		if stats.StringStats == nil {
			stats.StringStats = &StringStats{
				StrSet: make(map[string]struct{}, 0),
			}
		}
	}

	// we just use the Min stats for stored val comparison but apply the same
	// logic to max and sum
	switch inNumType {
	case SS_FLOAT64:
		if stats.NumStats.Min.Ntype == SS_DT_FLOAT {
			// incoming float, stored is float, simple min
			stats.NumStats.Min.FloatVal = math.Min(stats.NumStats.Min.FloatVal, fltVal)
			stats.NumStats.Max.FloatVal = math.Max(stats.NumStats.Max.FloatVal, fltVal)
			stats.NumStats.Sum.FloatVal = stats.NumStats.Sum.FloatVal + fltVal
			stats.NumStats.Dtype = SS_DT_FLOAT

			if hasValuesFunc {
				stats.StringStats.StrSet[strconv.FormatFloat(fltVal, 'f', -1, 64)] = struct{}{}
			}

			if colUsage == BothUsage || colUsage == WithEvalUsage {
				stats.Records = append(stats.Records, &CValueEnclosure{
					Dtype: SS_DT_FLOAT,
					CVal:  fltVal,
				})
			}
		} else {
			// incoming float, stored is non-float, upgrade it
			stats.NumStats.Min.FloatVal = math.Min(float64(stats.NumStats.Min.IntgrVal), fltVal)
			stats.NumStats.Min.Ntype = SS_DT_FLOAT

			stats.NumStats.Max.FloatVal = math.Max(float64(stats.NumStats.Max.IntgrVal), fltVal)
			stats.NumStats.Max.Ntype = SS_DT_FLOAT

			stats.NumStats.Sum.FloatVal = float64(stats.NumStats.Sum.IntgrVal) + fltVal
			stats.NumStats.Sum.Ntype = SS_DT_FLOAT
			stats.NumStats.Dtype = SS_DT_FLOAT

			if hasValuesFunc {
				stats.StringStats.StrSet[strconv.FormatFloat(fltVal, 'f', -1, 64)] = struct{}{}
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
		if stats.NumStats.Min.Ntype == SS_DT_FLOAT {
			// incoming non-float, stored is float, cast it
			stats.NumStats.Min.FloatVal = math.Min(stats.NumStats.Min.FloatVal, float64(inIntgrVal))
			stats.NumStats.Max.FloatVal = math.Max(stats.NumStats.Max.FloatVal, float64(inIntgrVal))
			stats.NumStats.Sum.FloatVal = stats.NumStats.Sum.FloatVal + float64(inIntgrVal)
			stats.NumStats.Dtype = SS_DT_FLOAT

			if hasValuesFunc {
				stats.StringStats.StrSet[strconv.FormatInt(inIntgrVal, 10)] = struct{}{}
			}

			if colUsage == BothUsage || colUsage == WithEvalUsage {
				stats.Records = append(stats.Records, &CValueEnclosure{
					Dtype: SS_DT_FLOAT,
					CVal:  float64(inIntgrVal),
				})
			}
		} else {
			// incoming non-float, stored is non-float, simple min
			stats.NumStats.Min.IntgrVal = utils.MinInt64(stats.NumStats.Min.IntgrVal, inIntgrVal)
			stats.NumStats.Max.IntgrVal = utils.MaxInt64(stats.NumStats.Max.IntgrVal, inIntgrVal)
			stats.NumStats.Sum.IntgrVal = stats.NumStats.Sum.IntgrVal + inIntgrVal
			stats.NumStats.Dtype = SS_DT_SIGNED_NUM

			if hasValuesFunc {
				stats.StringStats.StrSet[strconv.FormatInt(inIntgrVal, 10)] = struct{}{}
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
	bb *bbp.ByteBuffer, aggColUsage map[string]AggColUsageMode, hasValuesFunc bool) {

	var stats *SegStats
	var ok bool
	stats, ok = segstats[cname]
	if !ok {
		stats = &SegStats{
			IsNumeric: false,
			Count:     0,
			Hll:       hyperloglog.New16(),
			Records:   make([]*CValueEnclosure, 0)}

		segstats[cname] = stats
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

	if hasValuesFunc {
		if stats.StringStats == nil {
			stats.StringStats = &StringStats{
				StrSet: make(map[string]struct{}, 0),
			}
		}

		stats.StringStats.StrSet[strVal] = struct{}{}
	}

	bb.Reset()
	_, _ = bb.WriteString(strVal)
	stats.Hll.Insert(bb.B)
}

// adds all elements of m2 to m1 and returns m1
func MergeSegStats(m1, m2 map[string]*SegStats) map[string]*SegStats {
	for k, v := range m2 {
		other, ok := m1[k]
		if !ok {
			m1[k] = v
			continue
		}
		m1[k].Merge(other)
	}
	return m1
}
