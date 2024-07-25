package aggregations

import (
	"fmt"
	"time"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
)


func getFormattedTime(t time.Time) string {
	return t.Format("Mon Jan 2 15:04:05 2006 -0700")
}

func createGenTimeEvent(start time.Time, end time.Time) map[string]interface{} {
	return map[string]interface{}{
		"starttime": uint64(start.UnixMilli())/1000,
		"endtime": uint64(end.UnixMilli())/1000,
		"starthuman": getFormattedTime(start),
		"endhuman": getFormattedTime(end),
	}
}

func PerformGenTimes(aggs *structs.QueryAggregators) error {
	if aggs.GenerateEvent.GenTimes == nil {
		return fmt.Errorf("PerformGenTimes: GenTimes is nil")
	}
	if aggs.GenerateEvent.GenTimes.Interval == nil {
		aggs.GenerateEvent.GenTimes.Interval = &structs.SpanLength{
			Num: 1,
			TimeScalr: utils.TMDay,
		}
	}

	aggs.GenerateEvent.GeneratedCols["starttime"] = true
	aggs.GenerateEvent.GeneratedCols["endtime"] = true
	aggs.GenerateEvent.GeneratedCols["starthuman"] = true
	aggs.GenerateEvent.GeneratedCols["endhuman"] = true

	start := aggs.GenerateEvent.GenTimes.StartTime
	end := aggs.GenerateEvent.GenTimes.EndTime
	interval := aggs.GenerateEvent.GenTimes.Interval.Num
	if interval < 0 {
		start, end = end, start
		interval = -interval
	}

	// No need to generate events
	if start >= end {
		return nil
	}

	records := aggs.GenerateEvent.GeneratedRecords
	if records == nil {
		records = make(map[string]map[string]interface{})
		aggs.GenerateEvent.GeneratedRecords = records
	}
	recordsIndex := aggs.GenerateEvent.GeneratedRecordsIndex
	if recordsIndex == nil {
		recordsIndex = make(map[string]int)
		aggs.GenerateEvent.GeneratedRecordsIndex = recordsIndex
	}

	key := 0
	currTime := time.UnixMilli(int64(start))

	for start < end {
		recordKey := fmt.Sprintf("%v", key)
		// Generate event

		endTime, err := utils.ApplyOffsetToTime(int64(interval), aggs.GenerateEvent.GenTimes.Interval.TimeScalr, currTime)
		if err != nil {
			return fmt.Errorf("PerformGenTimes: Error while calculating end time, err: %v", err)
		}
		intervalEndTime, err := utils.ApplyOffsetToTime(-1, utils.TMSecond, endTime)
		if err != nil {
			return fmt.Errorf("PerformGenTimes: Error while calculating interval end time, err: %v", err)
		}
		records[recordKey] = createGenTimeEvent(currTime, intervalEndTime)
		recordsIndex[recordKey] = key
		key++
		currTime = endTime
		start = uint64(currTime.UnixMilli())
	}

	return nil
}