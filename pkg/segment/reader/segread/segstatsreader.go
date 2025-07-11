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

package segread

import (
	"errors"
	"fmt"
	"os"

	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/segment/structs"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var (
	ErrGetSegSumsqCurrSegStatNil        = errors.New("GetSegSumsq: currSegStat is nil")
	ErrGetSegSumsqCurrSegStatNonNumeric = errors.New("GetSegSumsq: current segStats is non-numeric")

	ErrGetSegVarCurrSegStatNil    = errors.New("GetSegVar: currSegStat is nil")
	ErrGetSegVarSegStatNonNumeric = errors.New("GetSegVar: current segStats is non-numeric")
	ErrGetVarianceInvalidDtype    = errors.New("getVariance: invalid data type")

	ErrGetSegVarpCurrSegStatNil    = errors.New("GetSegVarp: currSegStat is nil")
	ErrGetSegVarpSegStatNonNumeric = errors.New("GetSegVarp: current segStats is non-numeric")
	ErrGetVarpInvalidDtype         = errors.New("getVarp: invalid data type")
)

func ReadSegStats(segkey string, qid uint64) (map[string]*structs.SegStats, error) {

	retVal := make(map[string]*structs.SegStats)
	fName := fmt.Sprintf("%v.sst", segkey)
	err := blob.DownloadSegmentBlob(fName, true)
	if err != nil {
		return retVal, fmt.Errorf("qid=%d, ReadSegStats: failed to download sst file: %+v, err: %v", qid, fName, err)
	}

	fdata, err := os.ReadFile(fName)
	if err != nil {
		return retVal, fmt.Errorf("qid=%d, ReadSegStats: failed to read sst file: %+v, err: %v", qid, fName, err)
	}

	defer func() {
		err := blob.SetSegSetFilesAsNotInUse([]string{fName})
		if err != nil {
			log.Errorf("qid=%d, ReadSegStats: failed to close blob: %+v, err: %v", qid, fName, err)
		}
	}()

	if len(fdata) == 0 {
		return nil, fmt.Errorf("qid=%d, ReadSegStats: empty sst file: %v", qid, fName)
	}

	rIdx := uint32(0)

	// version
	version := fdata[rIdx]
	rIdx++

	var retErr error
	for rIdx < uint32(len(fdata)) {

		// cnamelen
		cnamelen := utils.BytesToUint16LittleEndian(fdata[rIdx : rIdx+2])
		rIdx += 2
		// actual cname
		cname := string(fdata[rIdx : rIdx+uint32(cnamelen)])
		rIdx += uint32(cnamelen)

		// sst len
		var sstlen uint32

		switch version {
		case sutils.VERSION_SEGSTATS[0]:
			sstlen = utils.BytesToUint32LittleEndian(fdata[rIdx : rIdx+4])
			rIdx += 4
		case sutils.VERSION_SEGSTATS_LEGACY[0]:
			sstlen = uint32(utils.BytesToUint16LittleEndian(fdata[rIdx : rIdx+2]))
			rIdx += 2
		default:
			retErr = fmt.Errorf("qid=%d, ReadSegStats: unknown version: %v", qid, version)
			continue
		}

		// actual sst
		sst, err := readSingleSst(fdata[rIdx:rIdx+sstlen], qid)
		if err != nil {
			return retVal, fmt.Errorf("qid=%d, ReadSegStats: error reading single sst for cname: %v, err: %v",
				qid, cname, err)
		}
		rIdx += uint32(sstlen)
		retVal[cname] = sst
	}
	return retVal, retErr
}

func readSingleSst(fdata []byte, qid uint64) (*structs.SegStats, error) {

	sst := structs.SegStats{}

	idx := uint32(0)

	// read version
	version := fdata[idx]
	idx++

	// read isNumeric
	sst.IsNumeric = utils.BytesToBoolLittleEndian(fdata[idx : idx+1])
	idx++

	// read Count
	sst.Count = utils.BytesToUint64LittleEndian(fdata[idx : idx+8])
	idx += 8

	var hllSize uint32

	switch version {
	case sutils.VERSION_SEGSTATS_BUF_V4[0]:
		hllSize = utils.BytesToUint32LittleEndian(fdata[idx : idx+4])
		idx += 4
	default:
		return nil, fmt.Errorf("qid=%d, readSingleSst: unknown version: %v", qid, version)
	}

	err := sst.CreateHllFromBytes(fdata[idx : idx+hllSize])
	if err != nil {
		return nil, fmt.Errorf("qid=%d, readSingleSst: unable to create Hll from raw bytes. sst err: %v", qid, err)
	}

	idx += hllSize

	if sst.IsNumeric {
		readNumericStats(&sst, fdata, idx)
		return &sst, nil
	}

	err = readNonNumericStats(&sst, fdata, idx)
	if err != nil {
		return nil, fmt.Errorf("readSingleSst: error reading non-numeric stats: %v", err)
	}

	return &sst, nil
}

func readNumericStats(sst *structs.SegStats, fdata []byte, idx uint32) {
	sst.NumStats = &structs.NumericStats{}

	min := sutils.CValueEnclosure{}
	// read Min Dtype
	min.Dtype = sutils.SS_DTYPE(fdata[idx : idx+1][0])
	idx += 1
	if min.Dtype == sutils.SS_DT_FLOAT {
		min.CVal = utils.BytesToFloat64LittleEndian(fdata[idx : idx+8])
	} else {
		min.CVal = utils.BytesToInt64LittleEndian(fdata[idx : idx+8])
	}
	sst.Min = min
	idx += 8

	max := sutils.CValueEnclosure{}
	// read Max Dtype
	max.Dtype = sutils.SS_DTYPE(fdata[idx : idx+1][0])
	idx += 1
	if max.Dtype == sutils.SS_DT_FLOAT {
		max.CVal = utils.BytesToFloat64LittleEndian(fdata[idx : idx+8])
	} else {
		max.CVal = utils.BytesToInt64LittleEndian(fdata[idx : idx+8])
	}
	sst.Max = max
	idx += 8

	sum := sutils.NumTypeEnclosure{}
	// read Sum Ntype
	sum.Ntype = sutils.SS_DTYPE(fdata[idx : idx+1][0])
	idx += 1
	if sum.Ntype == sutils.SS_DT_FLOAT {
		sum.FloatVal = utils.BytesToFloat64LittleEndian(fdata[idx : idx+8])
	} else {
		sum.IntgrVal = utils.BytesToInt64LittleEndian(fdata[idx : idx+8])
	}
	sst.NumStats.Sum = sum
	idx += 8

	// read NumericCount
	sst.NumStats.NumericCount = utils.BytesToUint64LittleEndian(fdata[idx : idx+8])
}

func readNonNumericStats(sst *structs.SegStats, fdata []byte, idx uint32) error {
	dType := sutils.SS_DTYPE(fdata[idx : idx+1][0])
	idx += 1
	// dType can only be string or backfill
	if dType == sutils.SS_DT_BACKFILL {
		return nil
	}
	if dType != sutils.SS_DT_STRING {
		return fmt.Errorf("readNonNumericStats: invalid dtype: %v", dType)
	}

	min := sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
	}
	// read Min length
	minlen := utils.BytesToUint16LittleEndian(fdata[idx : idx+2])
	idx += 2

	// read Min string
	min.CVal = string(fdata[idx : idx+uint32(minlen)])
	sst.Min = min
	idx += uint32(minlen)

	max := sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING,
	}

	// read Max length
	maxlen := utils.BytesToUint16LittleEndian(fdata[idx : idx+2])
	idx += 2

	// read Max string
	max.CVal = string(fdata[idx : idx+uint32(maxlen)])
	sst.Max = max

	return nil
}

func GetSegMin(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*sutils.CValueEnclosure, error) {

	if currSegStat == nil {
		return &sutils.CValueEnclosure{}, fmt.Errorf("GetSegMin: currSegStat is nil")
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		return &currSegStat.Min, nil
	}

	result, err := sutils.ReduceMinMax(runningSegStat.Min, currSegStat.Min, true)
	if err != nil {
		return &sutils.CValueEnclosure{}, fmt.Errorf("GetSegMin: error in ReduceMinMax, err: %v", err)
	}
	runningSegStat.Min = result
	if !runningSegStat.IsNumeric && runningSegStat.Min.IsNumeric() {
		runningSegStat.IsNumeric = true
	}

	return &runningSegStat.Min, nil
}

func GetSegMax(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*sutils.CValueEnclosure, error) {

	if currSegStat == nil {
		return &sutils.CValueEnclosure{}, fmt.Errorf("GetSegMax: currSegStat is nil")
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		return &currSegStat.Max, nil
	}

	result, err := sutils.ReduceMinMax(runningSegStat.Max, currSegStat.Max, false)
	if err != nil {
		return &sutils.CValueEnclosure{}, fmt.Errorf("GetSegMax: error in ReduceMinMax, err: %v", err)
	}
	runningSegStat.Max = result

	if !runningSegStat.IsNumeric && runningSegStat.Max.IsNumeric() {
		runningSegStat.IsNumeric = true
	}

	return &runningSegStat.Max, nil
}

func GetSegLatestOrEarliestTs(runningSegStat *structs.SegStats, currSegStat *structs.SegStats, isLatest bool) (*sutils.CValueEnclosure, error) {
	if currSegStat == nil {
		return &sutils.CValueEnclosure{}, fmt.Errorf("GetSegLatestOrEarliestTs: currSegStat is nil")
	}

	if runningSegStat == nil {
		if isLatest {
			return &currSegStat.TimeStats.LatestTs, nil
		} else {
			return &currSegStat.TimeStats.EarliestTs, nil
		}
	}

	result, err := reduceLatestEarliestTs(runningSegStat.TimeStats, currSegStat.TimeStats, isLatest)
	if err != nil {
		return &sutils.CValueEnclosure{}, fmt.Errorf("GetSegLatestOrEarliestTs: error in ReduceMinMax, err: %v", err)
	}

	if isLatest {
		runningSegStat.TimeStats.LatestTs = result
		return &runningSegStat.TimeStats.LatestTs, nil
	} else {
		runningSegStat.TimeStats.EarliestTs = result
		return &runningSegStat.TimeStats.EarliestTs, nil
	}
}

func reduceLatestEarliestTs(runningTs *structs.TimeStats, currentTs *structs.TimeStats, isLatest bool) (sutils.CValueEnclosure, error) {
	var result sutils.CValueEnclosure
	var err error
	if isLatest {
		result, err = sutils.ReduceMinMax(runningTs.LatestTs, currentTs.LatestTs, false)
	} else {
		result, err = sutils.ReduceMinMax(runningTs.EarliestTs, currentTs.EarliestTs, true)
	}
	return result, err
}

func GetSegLatestOrEarliestVal(runningSegStat *structs.SegStats, currSegStat *structs.SegStats, isLatest bool) (*sutils.CValueEnclosure, error) {
	if currSegStat == nil {
		return &sutils.CValueEnclosure{}, fmt.Errorf("GetSegLatestOrEarliestVal: currSegStat is nil")
	}

	if runningSegStat == nil {
		if isLatest {
			return &currSegStat.TimeStats.LatestVal, nil
		} else {
			return &currSegStat.TimeStats.EarliestVal, nil
		}
	}
	result, err := reduceLatestEarliestTs(runningSegStat.TimeStats, currSegStat.TimeStats, isLatest)
	if err != nil {
		return &sutils.CValueEnclosure{}, fmt.Errorf("GetSegLatestOrEarliestTs: error in ReduceMinMax, err: %v", err)
	}
	var elVal sutils.CValueEnclosure
	if isLatest {
		if runningSegStat.TimeStats.LatestTs.CVal.(uint64) == result.CVal.(uint64) {
			elVal = runningSegStat.TimeStats.LatestVal
		} else {
			elVal = currSegStat.TimeStats.LatestVal
		}
		runningSegStat.TimeStats.LatestVal = elVal
		return &runningSegStat.TimeStats.LatestVal, nil
	} else {
		if runningSegStat.TimeStats.EarliestTs.CVal.(uint64) == result.CVal.(uint64) {
			elVal = runningSegStat.TimeStats.EarliestVal
		} else {
			elVal = currSegStat.TimeStats.EarliestVal
		}
		runningSegStat.TimeStats.EarliestVal = elVal
		return &runningSegStat.TimeStats.EarliestVal, nil
	}
}

func getRange(max sutils.CValueEnclosure, min sutils.CValueEnclosure) (*sutils.CValueEnclosure, error) {
	result := sutils.CValueEnclosure{}
	if !max.IsNumeric() && !min.IsNumeric() {
		return nil, fmt.Errorf("getRange: both max and min are non-numeric")
	}
	switch max.Dtype {
	case sutils.SS_DT_FLOAT:
		result.Dtype = sutils.SS_DT_FLOAT
		switch min.Dtype {
		case sutils.SS_DT_FLOAT:
			result.CVal = max.CVal.(float64) - min.CVal.(float64)
		case sutils.SS_DT_SIGNED_NUM:
			result.CVal = max.CVal.(float64) - float64(min.CVal.(int64))
		default:
			return nil, fmt.Errorf("getRange: unsupported dtype: %v", min.Dtype)
		}
	case sutils.SS_DT_SIGNED_NUM:
		switch min.Dtype {
		case sutils.SS_DT_FLOAT:
			result.Dtype = sutils.SS_DT_FLOAT
			result.CVal = float64(max.CVal.(int64)) - min.CVal.(float64)
		case sutils.SS_DT_SIGNED_NUM:
			result.Dtype = sutils.SS_DT_SIGNED_NUM
			result.CVal = max.CVal.(int64) - min.CVal.(int64)
		default:
			return nil, fmt.Errorf("getRange: unsupported dtype: %v", min.Dtype)
		}
	default:
		return nil, fmt.Errorf("getRange: unsupported dtype: %v", max.Dtype)
	}

	return &result, nil
}

func GetSegRange(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*sutils.CValueEnclosure, error) {

	// start with lower resolution and upgrade as necessary
	result := sutils.CValueEnclosure{}

	if currSegStat == nil {
		return &result, fmt.Errorf("GetSegRange: currSegStat is nil")
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		if !currSegStat.Min.IsNumeric() {
			return &result, nil
		}

		return getRange(currSegStat.Max, currSegStat.Min)
	}

	structs.UpdateMinMax(runningSegStat, currSegStat.Min)
	structs.UpdateMinMax(runningSegStat, currSegStat.Max)

	return getRange(runningSegStat.Max, runningSegStat.Min)
}

func GetSegSum(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*sutils.NumTypeEnclosure, error) {

	// start with lower resolution and upgrade as necessary
	rSst := sutils.NumTypeEnclosure{
		Ntype:    sutils.SS_DT_SIGNED_NUM,
		IntgrVal: 0,
	}
	if currSegStat == nil {
		return &rSst, fmt.Errorf("GetSegSum: currSegStat is nil")
	}

	if !currSegStat.IsNumeric {
		return &rSst, fmt.Errorf("GetSegSum: current segStats is non-numeric")
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		switch currSegStat.NumStats.Sum.Ntype {
		case sutils.SS_DT_FLOAT:
			rSst.FloatVal = currSegStat.NumStats.Sum.FloatVal
			rSst.Ntype = sutils.SS_DT_FLOAT
		default:
			rSst.IntgrVal = currSegStat.NumStats.Sum.IntgrVal
		}
		return &rSst, nil
	}

	switch currSegStat.NumStats.Sum.Ntype {
	case sutils.SS_DT_FLOAT:
		if runningSegStat.NumStats.Sum.Ntype == sutils.SS_DT_FLOAT {
			runningSegStat.NumStats.Sum.FloatVal = runningSegStat.NumStats.Sum.FloatVal + currSegStat.NumStats.Sum.FloatVal
			rSst.FloatVal = runningSegStat.NumStats.Sum.FloatVal
			rSst.Ntype = sutils.SS_DT_FLOAT
		} else {
			runningSegStat.NumStats.Sum.FloatVal = float64(runningSegStat.NumStats.Sum.IntgrVal) + currSegStat.NumStats.Sum.FloatVal
			rSst.FloatVal = runningSegStat.NumStats.Sum.FloatVal
			rSst.Ntype = sutils.SS_DT_FLOAT
		}
	default:
		if runningSegStat.NumStats.Sum.Ntype == sutils.SS_DT_FLOAT {
			runningSegStat.NumStats.Sum.FloatVal = runningSegStat.NumStats.Sum.FloatVal + float64(currSegStat.NumStats.Sum.IntgrVal)
			rSst.FloatVal = runningSegStat.NumStats.Sum.FloatVal
			rSst.Ntype = sutils.SS_DT_FLOAT
		} else {
			runningSegStat.NumStats.Sum.IntgrVal = runningSegStat.NumStats.Sum.IntgrVal + currSegStat.NumStats.Sum.IntgrVal
			rSst.IntgrVal = runningSegStat.NumStats.Sum.IntgrVal
		}
	}

	return &rSst, nil
}

func GetSegPerc(runningSegStat *structs.SegStats, currSegStat *structs.SegStats, perc float64) (*sutils.NumTypeEnclosure, error) {
	res := sutils.NumTypeEnclosure{
		Ntype:    sutils.SS_DT_FLOAT,
		IntgrVal: 0,
		FloatVal: 0.0,
	}

	if currSegStat == nil {
		return &res, fmt.Errorf("GetSegPerc: currSegStat is nil")
	}

	fltPercentileVal := perc / 100
	if fltPercentileVal < 0.0 || fltPercentileVal > 1.0 {
		return &res, fmt.Errorf("GetSegPerc: percentile not between the valid range")
	}

	if runningSegStat == nil {
		res.FloatVal = currSegStat.TDigest.GetQuantile(fltPercentileVal)
		return &res, nil
	}

	err := runningSegStat.TDigest.MergeTDigest(currSegStat.TDigest)
	if err != nil {
		return &res, fmt.Errorf("GetSegPerc: Can't merge TDigests, check if they have the same compression value")
	}
	res.FloatVal = runningSegStat.TDigest.GetQuantile(fltPercentileVal)
	return &res, nil
}

func GetSegSumsq(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*sutils.NumTypeEnclosure, error) {

	if currSegStat == nil {
		return nil, ErrGetSegSumsqCurrSegStatNil
	}

	if !currSegStat.IsNumeric {
		return nil, ErrGetSegSumsqCurrSegStatNonNumeric
	}

	rSst := sutils.NumTypeEnclosure{
		Ntype:    sutils.SS_DT_FLOAT,
		FloatVal: 0,
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		rSst.FloatVal = currSegStat.NumStats.Sumsq
		return &rSst, nil
	}

	// both running and curr seg stats are float64, so we can add them directly
	runningSegStat.NumStats.Sumsq += currSegStat.NumStats.Sumsq
	rSst.FloatVal = runningSegStat.NumStats.Sumsq

	return &rSst, nil
}

func GetSegCardinality(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*sutils.NumTypeEnclosure, error) {

	res := sutils.NumTypeEnclosure{
		Ntype:    sutils.SS_DT_SIGNED_NUM,
		IntgrVal: 0,
	}

	if currSegStat == nil {
		return &res, fmt.Errorf("GetSegCardinality: currSegStat is nil")
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		res.IntgrVal = int64(currSegStat.GetHllCardinality())
		return &res, nil
	}

	err := runningSegStat.Hll.StrictUnion(currSegStat.Hll.Hll)
	if err != nil {
		return nil, fmt.Errorf("GetSegCardinality: error in Hll.Merge, err: %+v", err)
	}
	res.IntgrVal = int64(runningSegStat.GetHllCardinality())

	return &res, nil
}

func GetSegEstdcError(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*sutils.NumTypeEnclosure, error) {

	res := sutils.NumTypeEnclosure{
		Ntype:    sutils.SS_DT_FLOAT,
		IntgrVal: 0,
		FloatVal: 0.0,
	}

	if currSegStat == nil {
		return &res, fmt.Errorf("GetSegEstdcError: currSegStat is nil")
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		res.FloatVal = currSegStat.GetHllError()
		return &res, nil
	}

	err := runningSegStat.Hll.StrictUnion(currSegStat.Hll.Hll)
	if err != nil {
		return nil, fmt.Errorf("GetSegEstdcError: error in Hll.Merge, err: %+v", err)
	}
	res.FloatVal = runningSegStat.GetHllError()

	return &res, nil
}

func GetSegCount(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*sutils.NumTypeEnclosure, error) {

	rSst := sutils.NumTypeEnclosure{
		Ntype:    sutils.SS_DT_SIGNED_NUM,
		IntgrVal: int64(0),
	}
	if currSegStat == nil {
		return &rSst, fmt.Errorf("GetSegCount: currSegStat is nil")
	}

	if runningSegStat == nil {
		rSst.IntgrVal = int64(currSegStat.Count)
		return &rSst, nil
	}

	runningSegStat.Count = runningSegStat.Count + currSegStat.Count
	rSst.IntgrVal = int64(runningSegStat.Count)

	return &rSst, nil
}

func GetSegAvg(runningSegStat *structs.SegStats, currSegStat *structs.SegStats) (*sutils.NumTypeEnclosure, error) {
	// Initialize result with default values
	rSst := sutils.NumTypeEnclosure{
		Ntype:    sutils.SS_DT_FLOAT,
		IntgrVal: 0,
		FloatVal: 0.0,
	}

	if currSegStat == nil {
		return &rSst, fmt.Errorf("GetSegAvg: currSegStat is nil")
	}

	if !currSegStat.IsNumeric {
		return &rSst, fmt.Errorf("GetSegAvg: current segStats is non-numeric")
	}

	// If running segment statistics are nil, return the current segment's average
	if runningSegStat == nil {
		avg, err := getAverage(currSegStat.NumStats.Sum, currSegStat.NumStats.NumericCount)
		rSst.FloatVal = avg
		return &rSst, err
	}

	// Update running segment statistics
	runningSegStat.NumStats.NumericCount += currSegStat.NumStats.NumericCount
	err := runningSegStat.NumStats.Sum.ReduceFast(currSegStat.NumStats.Sum.Ntype, currSegStat.NumStats.Sum.IntgrVal, currSegStat.NumStats.Sum.FloatVal, sutils.Sum)
	if err != nil {
		return &rSst, fmt.Errorf("GetSegAvg: error in reducing sum, err: %+v", err)
	}
	// Calculate and return the average
	avg, err := getAverage(runningSegStat.NumStats.Sum, runningSegStat.NumStats.NumericCount)
	rSst.FloatVal = avg
	return &rSst, err
}

// Helper function to calculate the average
func getAverage(sum sutils.NumTypeEnclosure, count uint64) (float64, error) {
	avg := 0.0
	if count == 0 {
		return avg, fmt.Errorf("getAverage: count is 0, cannot divide by 0")
	}
	switch sum.Ntype {
	case sutils.SS_DT_FLOAT:
		avg = sum.FloatVal / float64(count)
	case sutils.SS_DT_SIGNED_NUM:
		avg = float64(sum.IntgrVal) / float64(count)
	default:
		return avg, fmt.Errorf("getAverage: invalid data type: %v", sum.Ntype)
	}
	return avg, nil
}

func GetSegVar(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*sutils.NumTypeEnclosure, error) {

	if currSegStat == nil {
		return nil, ErrGetSegVarCurrSegStatNil
	}

	if !currSegStat.IsNumeric {
		return nil, ErrGetSegVarSegStatNonNumeric
	}

	// Initialize result with default values
	rSst := sutils.NumTypeEnclosure{
		Ntype:    sutils.SS_DT_FLOAT,
		IntgrVal: 0,
		FloatVal: 0.0,
	}

	// If running segment statistics are nil, return the current segment's variance
	if runningSegStat == nil {
		variance, err := getVariance(currSegStat.NumStats.Sum, currSegStat.NumStats.Sumsq, currSegStat.NumStats.NumericCount)
		rSst.FloatVal = variance
		return &rSst, err
	}

	// Update running segment statistics
	runningSegStat.NumStats.NumericCount += currSegStat.NumStats.NumericCount
	err := runningSegStat.NumStats.Sum.ReduceFast(currSegStat.NumStats.Sum.Ntype, currSegStat.NumStats.Sum.IntgrVal, currSegStat.NumStats.Sum.FloatVal, sutils.Sum)
	if err != nil {
		return nil, err
	}
	runningSegStat.NumStats.Sumsq += currSegStat.NumStats.Sumsq

	// Calculate and return the variance
	variance, err := getVariance(runningSegStat.NumStats.Sum, runningSegStat.NumStats.Sumsq, runningSegStat.NumStats.NumericCount)
	rSst.FloatVal = variance
	return &rSst, err
}

// Helper function to calculate the sample variance
func getVariance(sum sutils.NumTypeEnclosure, sumsq float64, count uint64) (float64, error) {
	if count < 2 { // sample variance requires at least 2 records
		return 0, nil
	}

	var variance float64
	switch sum.Ntype {
	case sutils.SS_DT_FLOAT:
		variance = (sumsq - (sum.FloatVal * sum.FloatVal / float64(count))) / float64(count-1)
	case sutils.SS_DT_SIGNED_NUM:
		variance = (sumsq - (float64(sum.IntgrVal) * float64(sum.IntgrVal) / float64(count))) / float64(count-1)
	default:
		return 0, ErrGetVarianceInvalidDtype
	}
	return variance, nil
}

func GetSegVarp(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*sutils.NumTypeEnclosure, error) {

	if currSegStat == nil {
		return nil, ErrGetSegVarpCurrSegStatNil
	}

	if !currSegStat.IsNumeric {
		return nil, ErrGetSegVarpSegStatNonNumeric
	}

	// Initialize result with default values
	rSst := sutils.NumTypeEnclosure{
		Ntype:    sutils.SS_DT_FLOAT,
		IntgrVal: 0,
		FloatVal: 0.0,
	}

	// If running segment statistics are nil, return the current segment's population variance
	if runningSegStat == nil {
		varp, err := getVarp(currSegStat.NumStats.Sum, currSegStat.NumStats.Sumsq, currSegStat.NumStats.NumericCount)
		rSst.FloatVal = varp
		return &rSst, err
	}

	// Update running segment statistics
	runningSegStat.NumStats.NumericCount += currSegStat.NumStats.NumericCount
	err := runningSegStat.NumStats.Sum.ReduceFast(currSegStat.NumStats.Sum.Ntype, currSegStat.NumStats.Sum.IntgrVal, currSegStat.NumStats.Sum.FloatVal, sutils.Sum)
	if err != nil {
		return nil, err
	}
	runningSegStat.NumStats.Sumsq += currSegStat.NumStats.Sumsq

	// Calculate and return the varp
	varp, err := getVarp(runningSegStat.NumStats.Sum, runningSegStat.NumStats.Sumsq, runningSegStat.NumStats.NumericCount)
	rSst.FloatVal = varp
	return &rSst, err
}

// Helper function to calculate the variance
func getVarp(sum sutils.NumTypeEnclosure, sumsq float64, count uint64) (float64, error) {
	if count == 0 { // population variance requires at least 1 record
		return 0, nil
	}

	var variance float64
	switch sum.Ntype {
	case sutils.SS_DT_FLOAT:
		variance = (sumsq - (sum.FloatVal * sum.FloatVal / float64(count))) / float64(count)
	case sutils.SS_DT_SIGNED_NUM:
		variance = (sumsq - (float64(sum.IntgrVal) * float64(sum.IntgrVal) / float64(count))) / float64(count)
	default:
		return 0, ErrGetVarpInvalidDtype
	}
	return variance, nil
}

func GetSegList(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*sutils.CValueEnclosure, error) {
	res := sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING_SLICE,
		CVal:  make([]string, 0),
	}
	if currSegStat == nil || currSegStat.StringStats == nil || currSegStat.StringStats.StrList == nil {
		return &res, fmt.Errorf("GetSegList: currSegStat does not contain string list %v", currSegStat)
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		if len(currSegStat.StringStats.StrList) > sutils.MAX_SPL_LIST_SIZE {
			finalStringList := make([]string, sutils.MAX_SPL_LIST_SIZE)
			copy(finalStringList, currSegStat.StringStats.StrList[:sutils.MAX_SPL_LIST_SIZE])
			res.CVal = finalStringList
		} else {
			finalStringList := make([]string, len(currSegStat.StringStats.StrList))
			copy(finalStringList, currSegStat.StringStats.StrList)
			res.CVal = finalStringList
		}
		return &res, nil
	}

	// Limit list size to match splunk.
	strList := make([]string, 0, sutils.MAX_SPL_LIST_SIZE)

	if runningSegStat.StringStats != nil && runningSegStat.StringStats.StrList != nil {
		strList = sutils.AppendWithLimit(strList, runningSegStat.StringStats.StrList, sutils.MAX_SPL_LIST_SIZE)
	}

	strList = sutils.AppendWithLimit(strList, currSegStat.StringStats.StrList, sutils.MAX_SPL_LIST_SIZE)

	res.CVal = strList
	if runningSegStat.StringStats == nil {
		runningSegStat.StringStats = &structs.StringStats{
			StrList: strList,
		}
	} else {
		runningSegStat.StringStats.StrList = strList
	}
	return &res, nil
}

// Get merged values from running segement stats and current segment stats
func GetSegValue(runningSegStat *structs.SegStats, currSegStat *structs.SegStats) (*sutils.CValueEnclosure, error) {
	res := sutils.CValueEnclosure{
		Dtype: sutils.SS_DT_STRING_SLICE,
		CVal:  make([]string, 0),
	}

	if currSegStat == nil || currSegStat.StringStats == nil || currSegStat.StringStats.StrSet == nil {
		return &res, fmt.Errorf("GetSegValue: currSegStat does not contain string set %v", currSegStat)
	}
	// Initialize or retrieve the string set from running segment stats
	strSet := currSegStat.StringStats.StrSet

	// Update running segment stats with the merged string set
	if runningSegStat != nil {
		if runningSegStat.StringStats == nil {
			runningSegStat.StringStats = &structs.StringStats{
				StrSet: strSet,
			}
		} else {
			for str := range runningSegStat.StringStats.StrSet {
				strSet[str] = struct{}{}
			}
			runningSegStat.StringStats.StrSet = strSet
		}
	}

	// Convert the string set to a sorted slice
	res.CVal = utils.GetSortedStringKeys(strSet)
	return &res, nil
}
