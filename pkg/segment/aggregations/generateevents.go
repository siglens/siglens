package aggregations

import (
	"encoding/csv"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	putils "github.com/siglens/siglens/pkg/utils"
)

func createGenTimeEvent(start time.Time, end time.Time) map[string]interface{} {
	return map[string]interface{}{
		"starttime":  uint64(start.UnixMilli()) / 1000,
		"endtime":    uint64(end.UnixMilli()) / 1000,
		"starthuman": putils.FormatToHumanReadableTime(start),
		"endhuman":   putils.FormatToHumanReadableTime(end),
	}
}

func SetGeneratedCols(genEvent *structs.GenerateEvent, genCols []string) {
	for _, col := range genCols {
		genEvent.GeneratedCols[col] = true
	}
}

func PerformGenTimes(aggs *structs.QueryAggregators) error {
	if aggs.GenerateEvent.GenTimes == nil {
		return fmt.Errorf("PerformGenTimes: GenTimes is nil")
	}
	if aggs.GenerateEvent.GenTimes.Interval == nil {
		aggs.GenerateEvent.GenTimes.Interval = &structs.SpanLength{
			Num:       1,
			TimeScalr: utils.TMDay,
		}
	}
	genCols := []string{"starttime", "endtime", "starthuman", "endhuman"}

	SetGeneratedCols(aggs.GenerateEvent, genCols)

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

func checkCSVFormat(filename string) bool {
	return strings.HasSuffix(filename, ".csv") || strings.HasSuffix(filename, ".csv.gz")
}


func PerformInputLookup(aggs *structs.QueryAggregators) error {
	if aggs.GenerateEvent.InputLookup == nil {
		return fmt.Errorf("PerformInputLookup: InputLookup is nil")
	}
	filename := aggs.GenerateEvent.InputLookup.Filename

	if !checkCSVFormat(filename) {
		return fmt.Errorf("PerformInputLookup: Only CSV format is currently supported!")
	}
	
	// genCols := []string{}
	workingDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("PerformInputLookup: Error while getting current working directory, err: %v", err)
	}
	filepath := workingDir + "/lookups/" + filename

	file, err := os.Open(filepath)
	_ = file
	if err != nil {
		return fmt.Errorf("PerformInputLookup: Error while opening file %v, err: %v", filepath, err)
	}
	defer file.Close()
	
	reader := csv.NewReader(file)

	// read columns from first row of csv file
	columnNames, err := reader.Read()
    if err != nil {
        return fmt.Errorf("PerformInputLookup: Error reading column names, err:", err)
    }

	SetGeneratedCols(aggs.GenerateEvent, columnNames)

	


	return nil
}