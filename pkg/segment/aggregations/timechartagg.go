package aggregations

import (
	"time"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
)

func GenerateTimeRangeBuckets(timeHistogram *structs.TimeBucket) []uint64 {
	timeRangeBuckets := make([]uint64, 0)
	currentTime := timeHistogram.StartTime
	for currentTime < timeHistogram.EndTime {
		timeRangeBuckets = append(timeRangeBuckets, currentTime)
		nextTime := currentTime + timeHistogram.IntervalMillis
		if nextTime > timeHistogram.EndTime {
			break
		}

		currentTime = nextTime
	}

	return timeRangeBuckets
}

// Find correct time range bucket for timestamp
func FindTimeRangeBucket(timePoints []uint64, timestamp uint64, intervalMillis uint64) uint64 {
	index := ((timestamp - timePoints[0]) / intervalMillis)
	return timePoints[index]
}

func InitTimeBucket(num int, timeUnit utils.TimeUnit, byField string) *structs.TimeBucket {
	numD := time.Duration(num)
	intervalMillis := uint64(0)
	switch timeUnit {
	case utils.TMUs:
		// Might not has effect for 'us', because smallest time unit for timestamp in siglens is ms
	case utils.TMMs:
		intervalMillis = uint64(numD)
	case utils.TMCs:
		intervalMillis = uint64(numD * 10 * time.Millisecond)
	case utils.TMDs:
		intervalMillis = uint64(numD * 100 * time.Millisecond)
	case utils.TMSecond:
		intervalMillis = uint64((numD * time.Second).Milliseconds())
	case utils.TMMinute:
		intervalMillis = uint64((numD * time.Minute).Milliseconds())
	case utils.TMHour:
		intervalMillis = uint64((numD * time.Hour).Milliseconds())
	case utils.TMDay:
		intervalMillis = uint64((numD * 24 * time.Hour).Milliseconds())
	case utils.TMWeek:
		intervalMillis = uint64((numD * 7 * 24 * time.Hour).Milliseconds())
	case utils.TMMonth:
		intervalMillis = uint64((numD * 30 * 24 * time.Hour).Milliseconds())
	case utils.TMQuarter:
		intervalMillis = uint64((numD * 120 * 24 * time.Hour).Milliseconds())
	}

	timeBucket := &structs.TimeBucket{
		IntervalMillis:  intervalMillis,
		UsedByTimechart: true,
		ByField:         byField,
	}

	return timeBucket
}

func AddAggCountToTimechartRunningStats(m *structs.MeasureAggregator, allConvertedMeasureOps *[]*structs.MeasureAggregator, allReverseIndex *[]int, colToIdx map[string][]int, idx int) {
	*allReverseIndex = append(*allReverseIndex, idx)
	colToIdx[m.MeasureCol] = append(colToIdx[m.MeasureCol], idx)
	*allConvertedMeasureOps = append(*allConvertedMeasureOps, &structs.MeasureAggregator{
		MeasureCol:  m.MeasureCol,
		MeasureFunc: utils.Count,
		StrEnc:      m.StrEnc,
	})
}

func AddAggAvgToTimechartRunningStats(m *structs.MeasureAggregator, allConvertedMeasureOps *[]*structs.MeasureAggregator, allReverseIndex *[]int, colToIdx map[string][]int, idx int) {
	*allReverseIndex = append(*allReverseIndex, idx)
	colToIdx[m.MeasureCol] = append(colToIdx[m.MeasureCol], idx)
	*allConvertedMeasureOps = append(*allConvertedMeasureOps, &structs.MeasureAggregator{
		MeasureCol:  m.MeasureCol,
		MeasureFunc: utils.Sum,
		StrEnc:      m.StrEnc,
	})
	idx++
	*allReverseIndex = append(*allReverseIndex, idx)
	colToIdx[m.MeasureCol] = append(colToIdx[m.MeasureCol], idx)
	*allConvertedMeasureOps = append(*allConvertedMeasureOps, &structs.MeasureAggregator{
		MeasureCol:  m.MeasureCol,
		MeasureFunc: utils.Count,
		StrEnc:      m.StrEnc,
	})
}
