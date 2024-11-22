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
	log "github.com/sirupsen/logrus"
)

type binProcessor struct {
	options           *structs.BinCmdOptions
	initializedMinMax bool
	minVal            float64
	maxVal            float64
	secondPass        bool
	spanError         error
	batchErr          *utils.BatchError
}

const MAX_SIMILAR_ERRORS_TO_LOG = 5

func (p *binProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if iqr == nil {
		return nil, io.EOF
	}

	qid := iqr.GetQID()

	if p.batchErr == nil {
		p.batchErr = utils.GetOrCreateBatchErrorWithQid(qid)
	}

	if p.options.BinSpanOptions == nil && !p.secondPass {
		// Initialize min and max values
		if !p.initializedMinMax {
			p.minVal = math.MaxFloat64
			p.maxVal = -math.MaxFloat64
			p.initializedMinMax = true
		}

		p.updateTheMinMaxValues(iqr)

		return iqr, nil
	}

	if p.spanError != nil {
		return iqr, utils.TeeErrorf("qid=%v, bin.Process: error=%v", qid, p.spanError)
	}

	if p.secondPass && p.options.BinSpanOptions == nil {
		return iqr, utils.TeeErrorf("qid=%v, bin.Process: second pass but no bin span options", qid)
	}

	values, err := iqr.ReadColumn(p.options.Field)
	if err != nil {
		return nil, utils.TeeErrorf("qid=%v, bin.Process: cannot read values for field %v; err=%v",
			qid, p.options.Field, err)
	}

	var newColResultValues []segutils.CValueEnclosure

	newName, newNameExists := p.options.NewFieldName.Get()

	// If the new field name is different from the current field name,
	// we need to create a new slice to store the results
	if newNameExists && newName != p.options.Field {
		newColResultValues = make([]segutils.CValueEnclosure, len(values))
	} else {
		newNameExists = false
	}

	timestampField := p.options.Field == config.GetTimeStampKey()

	for i := range values {
		var value *segutils.CValueEnclosure
		if newNameExists {
			newColResultValues[i] = values[i]
			value = &newColResultValues[i]
		} else {
			value = &values[i]
		}

		if timestampField {
			floatVal, err := value.GetFloatValue()
			if err != nil {
				return nil, utils.TeeErrorf("qid=%v, bin.Process: cannot convert value %v to float; err=%v", qid,
					value, err)
			}

			bucket, err := p.performBinWithSpanTime(floatVal, p.options.AlignTime)
			if err != nil {
				return nil, utils.TeeErrorf("qid=%v, bin.Process: cannot bin value %v; err=%v",
					qid, value, err)
			}

			value.CVal = bucket
			value.Dtype = segutils.SS_DT_UNSIGNED_NUM
		} else {
			err = p.performBinWithSpan(value)
			if err != nil {
				return nil, utils.TeeErrorf("qid=%v, bin.Process: cannot bin value %v, field=%v; err=%v",
					qid, value, p.options.Field, err)
			}
		}
	}

	if newNameExists {
		knownValues := map[string][]segutils.CValueEnclosure{
			newName: newColResultValues,
		}
		err = iqr.AppendKnownValues(knownValues)
		if err != nil {
			return nil, utils.TeeErrorf("qid=%v, bin.Process: cannot append known values; err=%v", qid, err)
		}
	}

	return iqr, nil
}

// In the two-pass version of bin, Rewind() will calculate the bin span options
// based on the min and max values seen in the first pass.
func (p *binProcessor) Rewind() {
	p.secondPass = true

	if p.options.BinSpanOptions != nil {
		// Already Exists or calculated the bin span options
		return
	}

	if p.options.Field != config.GetTimeStampKey() {
		if p.options.Start != nil && *p.options.Start < p.minVal {
			p.minVal = *p.options.Start
		}
		if p.options.End != nil && *p.options.End > p.maxVal {
			p.maxVal = *p.options.End
		}
	}

	binSpanOptions, err := findSpan(p.minVal, p.maxVal, p.options.MaxBins, p.options.MinSpan, p.options.Field)
	if err != nil {
		p.spanError = fmt.Errorf("bin.Rewind: cannot find span; err=%v", err)
		return
	}

	p.options.BinSpanOptions = binSpanOptions
}

func (p *binProcessor) Cleanup() {
	// Nothing to do
}

func (p *binProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}

func (p *binProcessor) performBinWithSpan(cval *segutils.CValueEnclosure) error {
	value, err := cval.GetFloatValue()
	if err != nil {
		p.batchErr.AddError("bin.performBinWithSpan:GetFloatValue", fmt.Errorf("value=%v; err=%v", cval, err))
		return nil
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

func findSpan(minValue float64, maxValue float64, maxBins uint64, minSpan *structs.BinSpanLength, field string) (*structs.BinSpanOptions, error) {
	if field == config.GetTimeStampKey() {
		return findEstimatedTimeSpan(minValue, maxValue, maxBins, minSpan)
	}
	if minValue == maxValue {
		return &structs.BinSpanOptions{
			BinSpanLength: &structs.BinSpanLength{
				Num:       1,
				TimeScale: segutils.TMInvalid,
			},
		}, nil
	}

	// span ranges estimated are in powers of 10
	span := (maxValue - minValue) / float64(maxBins)
	exponent := math.Log10(span)
	exponent = math.Ceil(exponent)
	spanRange := math.Pow(10, exponent)

	// verify if estimated span gives correct number of bins, refer the edge case like 301-500 for bins = 2
	for {
		lowerBound, _ := getBinRange(minValue, spanRange)
		_, upperBound := getBinRange(maxValue, spanRange)

		if (upperBound-lowerBound)/spanRange > float64(maxBins) && spanRange <= math.MaxFloat64/10 {
			spanRange = spanRange * 10
		} else {
			break
		}
	}

	// increase the spanRange till minSpan is satisfied
	if minSpan != nil {
		for {
			if spanRange < minSpan.Num && spanRange <= math.MaxFloat64/10 {
				spanRange = spanRange * 10
			} else {
				break
			}
		}
	}

	return &structs.BinSpanOptions{
		BinSpanLength: &structs.BinSpanLength{
			Num:       spanRange,
			TimeScale: segutils.TMInvalid,
		},
	}, nil
}

func getSecsFromMinSpan(minSpan *structs.BinSpanLength) (float64, error) {
	if minSpan == nil {
		return 0, nil
	}

	switch minSpan.TimeScale {
	case segutils.TMMillisecond, segutils.TMCentisecond, segutils.TMDecisecond:
		// smallest granularity of estimated span is 1 second
		return 1, nil
	case segutils.TMSecond:
		return minSpan.Num, nil
	case segutils.TMMinute:
		return minSpan.Num * 60, nil
	case segutils.TMHour:
		return minSpan.Num * 3600, nil
	case segutils.TMDay:
		return minSpan.Num * 86400, nil
	case segutils.TMWeek, segutils.TMMonth, segutils.TMQuarter, segutils.TMYear:
		// default returning num*(seconds in a month)
		return minSpan.Num * 2592000, nil
	default:
		return 0, fmt.Errorf("getSecsFromMinSpan: Invalid time unit: %v", minSpan.TimeScale)
	}
}

func findEstimatedTimeSpan(minValueMillis float64, maxValueMillis float64, maxBins uint64, minSpan *structs.BinSpanLength) (*structs.BinSpanOptions, error) {
	minSpanSecs, err := getSecsFromMinSpan(minSpan)
	if err != nil {
		return nil, fmt.Errorf("findEstimatedTimeSpan: Error while getting seconds from minspan, err: %v", err)
	}
	intervalSec := (maxValueMillis/1000 - minValueMillis/1000) / float64(maxBins)
	if minSpanSecs > intervalSec {
		intervalSec = minSpanSecs
	}
	var num float64
	timeUnit := segutils.TMSecond
	if intervalSec < 1 {
		num = 1
	} else if intervalSec <= 10 {
		num = 10
	} else if intervalSec <= 30 {
		num = 30
	} else if intervalSec <= 60 {
		num = 1
		timeUnit = segutils.TMMinute
	} else if intervalSec <= 300 {
		num = 5
		timeUnit = segutils.TMMinute
	} else if intervalSec <= 600 {
		num = 10
		timeUnit = segutils.TMMinute
	} else if intervalSec <= 1800 {
		num = 30
		timeUnit = segutils.TMMinute
	} else if intervalSec <= 3600 {
		num = 1
		timeUnit = segutils.TMHour
	} else if intervalSec <= 86400 {
		num = 1
		timeUnit = segutils.TMDay
	} else {
		// maximum granularity is 1 month as per experiments
		num = 1
		timeUnit = segutils.TMMonth
	}

	estimatedSpan := &structs.BinSpanOptions{
		BinSpanLength: &structs.BinSpanLength{
			Num:       num,
			TimeScale: timeUnit,
		},
	}

	return estimatedSpan, nil
}

func (p *binProcessor) updateTheMinMaxValues(iqr *iqr.IQR) {
	fetchingFloatValueErrors := make([]error, MAX_SIMILAR_ERRORS_TO_LOG)
	fetchingFloatValueErrIndex := 0

	values, err := iqr.ReadColumn(p.options.Field)
	if err != nil {
		return
	}

	for i := range values {
		value, err := values[i].GetFloatValue()
		if err != nil {
			if fetchingFloatValueErrIndex < MAX_SIMILAR_ERRORS_TO_LOG {
				fetchingFloatValueErrors[fetchingFloatValueErrIndex] = fmt.Errorf("value=%v; err=%v", values[i], err)
				fetchingFloatValueErrIndex++
			}

			continue
		}

		p.minVal = math.Min(p.minVal, value)

		p.maxVal = math.Max(p.maxVal, value)
	}

	if fetchingFloatValueErrIndex > 0 {
		relation := "exactly"
		if fetchingFloatValueErrIndex == MAX_SIMILAR_ERRORS_TO_LOG {
			relation = "more than"
		}

		log.Errorf("bin.updateTheMinMaxValues: Error fetching float value for %v %v records;  Errors=%v", relation, fetchingFloatValueErrIndex, fetchingFloatValueErrors)
	}

}
