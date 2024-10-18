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

package processor

import (
	"fmt"
	"io"
	"math"
	"time"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
)

type binProcessor struct {
	options *structs.BinCmdOptions
}

func (p *binProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if p.options.BinSpanOptions == nil {
		return nil, utils.TeeErrorf("bin.Process: computing span is not yet implemented")
	}

	if iqr == nil {
		return nil, io.EOF
	}

	values, err := iqr.ReadColumn(p.options.Field)
	if err != nil {
		return nil, utils.TeeErrorf("bin.Process: cannot read values for field %v; err=%v",
			p.options.Field, err)
	}

	if p.options.Field == config.GetTimeStampKey() {
		for i := range values {
			value := &values[i]
			floatVal, err := value.GetFloatValue()
			if err != nil {
				return nil, utils.TeeErrorf("bin.Process: cannot convert value %v to float; err=%v",
					value, err)
			}

			bucket, err := p.performBinWithSpanTime(floatVal, p.options.AlignTime)
			if err != nil {
				return nil, utils.TeeErrorf("bin.Process: cannot bin value %v; err=%v",
					value, err)
			}

			value.CVal = bucket
			value.Dtype = segutils.SS_DT_UNSIGNED_NUM
		}
	} else {
		for i := range values {
			value := &values[i]
			err = p.performBinWithSpan(value)
			if err != nil {
				return nil, utils.TeeErrorf("bin.Process: cannot bin value %v; err=%v",
					value, err)
			}
		}
	}

	return iqr, nil
}

// In the two-pass version of bin, Rewind() should remember the span it
// calculated in the first pass.
func (p *binProcessor) Rewind() {
	// TODO: handle this for two-pass bin.
}

func (p *binProcessor) Cleanup() {
	// Nothing to do.
}

func (p *binProcessor) performBinWithSpan(cval *segutils.CValueEnclosure) error {
	value, err := cval.GetFloatValue()
	if err != nil {
		return fmt.Errorf("bin.performBinWithSpan: %+v cannot be converted to float; err=%v",
			cval, err)
	}

	if p.options.BinSpanOptions.BinSpanLength != nil {
		lowerBound, upperBound := getBinRange(value, p.options.BinSpanOptions.BinSpanLength.Num)
		if p.options.BinSpanOptions.BinSpanLength.TimeScale == segutils.TMInvalid {
			cval.Dtype = segutils.SS_DT_STRING
			cval.CVal = fmt.Sprintf("%v-%v", lowerBound, upperBound)
		} else {
			cval.Dtype = segutils.SS_DT_FLOAT
			cval.CVal = lowerBound
		}

		return nil
	}

	if p.options.BinSpanOptions.LogSpan != nil {
		if value <= 0 {
			cval.Dtype = segutils.SS_DT_FLOAT
			cval.CVal = value
			return nil
		}

		val := value / p.options.BinSpanOptions.LogSpan.Coefficient
		logVal := math.Log10(val) / math.Log10(p.options.BinSpanOptions.LogSpan.Base)
		floorVal := math.Floor(logVal)
		ceilVal := math.Ceil(logVal)
		if ceilVal == floorVal {
			ceilVal += 1
		}
		lowerBound := math.Pow(p.options.BinSpanOptions.LogSpan.Base, floorVal) * p.options.BinSpanOptions.LogSpan.Coefficient
		upperBound := math.Pow(p.options.BinSpanOptions.LogSpan.Base, ceilVal) * p.options.BinSpanOptions.LogSpan.Coefficient

		cval.Dtype = segutils.SS_DT_STRING
		cval.CVal = fmt.Sprintf("%v-%v", lowerBound, upperBound)
		return nil
	}

	return fmt.Errorf("bin.performBinWithSpan: no span options set")
}

// Function to bin ranges with the given span length
func getBinRange(val float64, spanRange float64) (float64, float64) {
	lowerbound := math.Floor(val/spanRange) * spanRange
	upperbound := math.Ceil(val/spanRange) * spanRange
	if lowerbound == upperbound {
		upperbound += spanRange
	}

	return lowerbound, upperbound
}

// Perform bin with span for time
func (p *binProcessor) performBinWithSpanTime(value float64, alignTime *uint64) (uint64, error) {
	spanLength := p.options.BinSpanOptions.BinSpanLength
	if spanLength == nil {
		return 0, fmt.Errorf("performBinWithSpanTime: spanLength is nil")
	}

	unixMilli := int64(value)
	utcTime := time.UnixMilli(unixMilli)
	startTime := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
	bucket := 0
	numIntervals := spanLength.Num

	//Align time is only supported for units less than days
	switch spanLength.TimeScale {
	case segutils.TMMillisecond:
		durationScale := time.Millisecond
		bucket = getTimeBucketWithAlign(utcTime, durationScale, numIntervals, alignTime)
	case segutils.TMCentisecond:
		durationScale := time.Millisecond * 10
		bucket = getTimeBucketWithAlign(utcTime, durationScale, numIntervals, alignTime)
	case segutils.TMDecisecond:
		durationScale := time.Millisecond * 100
		bucket = getTimeBucketWithAlign(utcTime, durationScale, numIntervals, alignTime)
	case segutils.TMSecond:
		durationScale := time.Second
		bucket = getTimeBucketWithAlign(utcTime, durationScale, numIntervals, alignTime)
	case segutils.TMMinute:
		durationScale := time.Minute
		bucket = getTimeBucketWithAlign(utcTime, durationScale, numIntervals, alignTime)
	case segutils.TMHour:
		durationScale := time.Hour
		bucket = getTimeBucketWithAlign(utcTime, durationScale, numIntervals, alignTime)
	case segutils.TMDay:
		totalDays := int(utcTime.Sub(startTime).Hours() / 24)
		slotDays := (totalDays / (int(numIntervals))) * (int(numIntervals))
		bucket = int(startTime.AddDate(0, 0, slotDays).UnixMilli())
	case segutils.TMWeek:
		totalDays := int(utcTime.Sub(startTime).Hours() / 24)
		slotDays := (totalDays / (int(numIntervals) * 7)) * (int(numIntervals) * 7)
		bucket = int(startTime.AddDate(0, 0, slotDays).UnixMilli())
	case segutils.TMMonth:
		return findBucketMonth(utcTime, int(numIntervals)), nil
	case segutils.TMQuarter:
		return findBucketMonth(utcTime, int(numIntervals)*3), nil
	case segutils.TMYear:
		num := int(numIntervals)
		currYear := int(utcTime.Year())
		bucketYear := ((currYear-1970)/num)*num + 1970
		bucket = int(time.Date(bucketYear, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
	default:
		return 0, fmt.Errorf("performBinWithSpanTime: Time scale %v is not supported", spanLength.TimeScale)
	}

	return uint64(bucket), nil
}

func getTimeBucketWithAlign(utcTime time.Time, durationScale time.Duration, numIntervals float64, alignTime *uint64) int {
	if alignTime == nil {
		return int(utcTime.Truncate(time.Duration(numIntervals) * durationScale).UnixMilli())
	}

	factorInMillisecond := float64((time.Duration(numIntervals) * durationScale) / time.Millisecond)
	currTime := float64(utcTime.UnixMilli())
	baseTime := float64(*alignTime)
	diff := math.Floor((currTime - baseTime) / factorInMillisecond)
	bucket := int(baseTime + diff*factorInMillisecond)
	if bucket < 0 {
		bucket = 0
	}

	return bucket
}

// Find the bucket month based on the given number of months as span.
func findBucketMonth(utcTime time.Time, numOfMonths int) uint64 {
	var finalTime time.Time
	if numOfMonths == 12 {
		finalTime = time.Date(utcTime.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	} else {
		currMonth := int(utcTime.Month())
		month := ((currMonth-1)/numOfMonths)*numOfMonths + 1
		finalTime = time.Date(utcTime.Year(), time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	}

	return uint64(finalTime.UnixMilli())
}
