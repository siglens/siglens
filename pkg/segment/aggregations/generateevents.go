package aggregations

import (
	"compress/gzip"
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

func InitGenEvent(aggs *structs.QueryAggregators) {
	if aggs.GenerateEvent.GeneratedRecords == nil {
		aggs.GenerateEvent.GeneratedRecords = make(map[string]map[string]interface{})
	}
	if aggs.GenerateEvent.GeneratedRecordsIndex == nil {
		aggs.GenerateEvent.GeneratedRecordsIndex = make(map[string]int)
	}
	if aggs.GenerateEvent.GeneratedColsIndex == nil {
		aggs.GenerateEvent.GeneratedColsIndex = make(map[string]int)
	}
	if aggs.GenerateEvent.GeneratedCols == nil {
		aggs.GenerateEvent.GeneratedCols = make(map[string]bool)
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

	InitGenEvent(aggs)
	records := aggs.GenerateEvent.GeneratedRecords
	recordsIndex := aggs.GenerateEvent.GeneratedRecordsIndex

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

// Ensure record and column lengths are valid before calling this function.
func createRecord(columnNames []string, record []string) map[string]interface{} {
	recordMap := make(map[string]interface{})
	for i, col := range columnNames {
		recordMap[col] = record[i]
	}
	return recordMap
}

func PerformInputLookup(aggs *structs.QueryAggregators) error {
	var reader *csv.Reader
	if aggs.GenerateEvent.InputLookup == nil {
		return fmt.Errorf("PerformInputLookup: InputLookup is nil")
	}
	filename := aggs.GenerateEvent.InputLookup.Filename

	if !checkCSVFormat(filename) {
		return fmt.Errorf("PerformInputLookup: Only CSV format is currently supported")
	}

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

	if strings.HasSuffix(filename, ".csv.gz") {
		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			return fmt.Errorf("PerformInputLookup: Error while creating gzip reader, err: %v", err)
		}
		defer gzipReader.Close()
		reader = csv.NewReader(gzipReader)
	} else {
		reader = csv.NewReader(file)
	}

	// read columns from first row of csv file
	columnNames, err := reader.Read()
	if err != nil {
		return fmt.Errorf("PerformInputLookup: Error reading column names, err: %v", err)
	}

	curr := 0
	for curr < int(aggs.GenerateEvent.InputLookup.Start) {
		_, err := reader.Read()
		if err != nil {
			return fmt.Errorf("PerformInputLookup: Error skipping rows, err: %v", err)
		}
		curr++
	}

	InitGenEvent(aggs)
	records := aggs.GenerateEvent.GeneratedRecords
	recordsIndex := aggs.GenerateEvent.GeneratedRecordsIndex

	SetGeneratedCols(aggs.GenerateEvent, columnNames)

	key := 0
	count := 0
	fieldToValue := make(map[string]utils.CValueEnclosure)
	for count < int(aggs.GenerateEvent.InputLookup.Max) {
		recordKey := fmt.Sprintf("key: %v", key)
		csvRecord, err := reader.Read()
		if err != nil {
			// Check if we've reached the end of the file
			if err.Error() == "EOF" {
				break
			}
			return fmt.Errorf("PerformInputLookup: Error reading record, err: %v", err)
		}

		if len(csvRecord) != len(columnNames) {
			return fmt.Errorf("PerformInputLookup: Error reading record, column count mismatch")
		}

		count++
		record := createRecord(columnNames, csvRecord)
		err = getRecordFieldValues(fieldToValue, columnNames, record)
		if err != nil {
			return fmt.Errorf("PerformInputLookup: Error getting field values, err: %v", err)
		}
		if aggs.GenerateEvent.InputLookup.WhereExpr != nil {
			conditionPassed, err := aggs.GenerateEvent.InputLookup.WhereExpr.EvaluateForInputLookup(fieldToValue)
			if err != nil {
				return fmt.Errorf("PerformInputLookup: Error evaluating where expression, err: %v", err)
			}
			if !conditionPassed {
				continue
			}
		}
		records[recordKey] = record
		recordsIndex[recordKey] = key
		key++
	}

	return nil
}
