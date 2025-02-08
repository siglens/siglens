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
	"fmt"
	"math"
	"os"

	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

var StdOutLogger *log.Logger

func init() {
	StdOutLogger = &log.Logger{
		Out:       os.Stderr,
		Formatter: new(log.TextFormatter),
		Hooks:     make(log.LevelHooks),
		Level:     log.InfoLevel,
	}
	customFormatter := new(log.TextFormatter)
	customFormatter.TimestampFormat = "2006-01-02 15:04:05"
	customFormatter.FullTimestamp = true
	StdOutLogger.SetFormatter(customFormatter)
}

func ReadSegStats(segkey string, qid uint64) (map[string]*structs.SegStats, error) {

	retVal := make(map[string]*structs.SegStats)
	fName := fmt.Sprintf("%v.sst", segkey)
	err := blob.DownloadSegmentBlob(fName, true)
	if err != nil {
		log.Errorf("qid=%d, ReadSegStats: failed to download sst file: %+v, err: %v", qid, fName, err)
		return retVal, err
	}

	fdata, err := os.ReadFile(fName)
	if err != nil {
		log.Errorf("qid=%d, ReadSegStats: failed to read sst file: %+v, err: %v", qid, fName, err)
		return retVal, err
	}

	defer func() {
		err := blob.SetSegSetFilesAsNotInUse([]string{fName})
		if err != nil {
			log.Errorf("qid=%d, ReadSegStats: failed to close blob: %+v, err: %v", qid, fName, err)
		}
	}()

	if len(fdata) == 0 {
		return nil, toputils.TeeErrorf("qid=%d, ReadSegStats: empty sst file: %v", qid, fName)
	}

	rIdx := uint32(0)

	// version
	version := fdata[rIdx]
	rIdx++

	for rIdx < uint32(len(fdata)) {

		// cnamelen
		cnamelen := toputils.BytesToUint16LittleEndian(fdata[rIdx : rIdx+2])
		rIdx += 2
		// actual cname
		cname := string(fdata[rIdx : rIdx+uint32(cnamelen)])
		rIdx += uint32(cnamelen)

		// sst len
		var sstlen uint32

		switch version {
		case utils.VERSION_SEGSTATS[0]:
			sstlen = toputils.BytesToUint32LittleEndian(fdata[rIdx : rIdx+4])
			rIdx += 4
		case utils.VERSION_SEGSTATS_LEGACY[0]:
			sstlen = uint32(toputils.BytesToUint16LittleEndian(fdata[rIdx : rIdx+2]))
			rIdx += 2
		default:
			log.Errorf("qid=%d, ReadSegStats: unknown version: %v", qid, version)
			continue
		}

		// actual sst
		sst, err := readSingleSst(fdata[rIdx:rIdx+sstlen], qid)
		if err != nil {
			return retVal, toputils.TeeErrorf("qid=%d, ReadSegStats: error reading single sst for cname: %v, err: %v",
				qid, cname, err)
		}
		rIdx += uint32(sstlen)
		retVal[cname] = sst
	}
	return retVal, nil
}

func readSingleSst(fdata []byte, qid uint64) (*structs.SegStats, error) {

	sst := structs.SegStats{}

	idx := uint32(0)

	// read version
	version := fdata[idx]
	idx++

	// read isNumeric
	sst.IsNumeric = toputils.BytesToBoolLittleEndian(fdata[idx : idx+1])
	idx++

	// read Count
	sst.Count = toputils.BytesToUint64LittleEndian(fdata[idx : idx+8])
	idx += 8

	var hllSize uint32

	switch version {
	case utils.VERSION_SEGSTATS_BUF_V4[0]:
		hllSize = toputils.BytesToUint32LittleEndian(fdata[idx : idx+4])
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

	min := utils.CValueEnclosure{}
	// read Min Dtype
	min.Dtype = utils.SS_DTYPE(fdata[idx : idx+1][0])
	idx += 1
	if min.Dtype == utils.SS_DT_FLOAT {
		min.CVal = toputils.BytesToFloat64LittleEndian(fdata[idx : idx+8])
	} else {
		min.CVal = toputils.BytesToInt64LittleEndian(fdata[idx : idx+8])
	}
	sst.Min = min
	idx += 8

	max := utils.CValueEnclosure{}
	// read Max Dtype
	max.Dtype = utils.SS_DTYPE(fdata[idx : idx+1][0])
	idx += 1
	if max.Dtype == utils.SS_DT_FLOAT {
		max.CVal = toputils.BytesToFloat64LittleEndian(fdata[idx : idx+8])
	} else {
		max.CVal = toputils.BytesToInt64LittleEndian(fdata[idx : idx+8])
	}
	sst.Max = max
	idx += 8

	sum := utils.NumTypeEnclosure{}
	// read Sum Ntype
	sum.Ntype = utils.SS_DTYPE(fdata[idx : idx+1][0])
	idx += 1
	if sum.Ntype == utils.SS_DT_FLOAT {
		sum.FloatVal = toputils.BytesToFloat64LittleEndian(fdata[idx : idx+8])
	} else {
		sum.IntgrVal = toputils.BytesToInt64LittleEndian(fdata[idx : idx+8])
	}
	sst.NumStats.Sum = sum
	idx += 8

	// read NumericCount
	sst.NumStats.NumericCount = toputils.BytesToUint64LittleEndian(fdata[idx : idx+8])
}

func readNonNumericStats(sst *structs.SegStats, fdata []byte, idx uint32) error {
	dType := utils.SS_DTYPE(fdata[idx : idx+1][0])
	idx += 1
	// dType can only be string or backfill
	if dType == utils.SS_DT_BACKFILL {
		return nil
	}
	if dType != utils.SS_DT_STRING {
		return fmt.Errorf("readNonNumericStats: invalid dtype: %v", dType)
	}

	min := utils.CValueEnclosure{
		Dtype: utils.SS_DT_STRING,
	}
	// read Min length
	minlen := toputils.BytesToUint16LittleEndian(fdata[idx : idx+2])
	idx += 2

	// read Min string
	min.CVal = string(fdata[idx : idx+uint32(minlen)])
	sst.Min = min
	idx += uint32(minlen)

	max := utils.CValueEnclosure{
		Dtype: utils.SS_DT_STRING,
	}

	// read Max length
	maxlen := toputils.BytesToUint16LittleEndian(fdata[idx : idx+2])
	idx += 2

	// read Max string
	max.CVal = string(fdata[idx : idx+uint32(maxlen)])
	sst.Max = max

	return nil
}

func GetSegMin(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*utils.CValueEnclosure, error) {

	if currSegStat == nil {
		return &utils.CValueEnclosure{}, fmt.Errorf("GetSegMin: currSegStat is nil")
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		return &currSegStat.Min, nil
	}

	result, err := utils.ReduceMinMax(runningSegStat.Min, currSegStat.Min, true)
	if err != nil {
		return &utils.CValueEnclosure{}, fmt.Errorf("GetSegMin: error in ReduceMinMax, err: %v", err)
	}
	runningSegStat.Min = result
	if !runningSegStat.IsNumeric && runningSegStat.Min.IsNumeric() {
		runningSegStat.IsNumeric = true
	}

	return &runningSegStat.Min, nil
}

func GetSegMax(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*utils.CValueEnclosure, error) {

	if currSegStat == nil {
		return &utils.CValueEnclosure{}, fmt.Errorf("GetSegMax: currSegStat is nil")
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		return &currSegStat.Max, nil
	}

	result, err := utils.ReduceMinMax(runningSegStat.Max, currSegStat.Max, false)
	if err != nil {
		return &utils.CValueEnclosure{}, fmt.Errorf("GetSegMax: error in ReduceMinMax, err: %v", err)
	}
	runningSegStat.Max = result

	if !runningSegStat.IsNumeric && runningSegStat.Max.IsNumeric() {
		runningSegStat.IsNumeric = true
	}

	return &runningSegStat.Max, nil
}

func getRange(max utils.CValueEnclosure, min utils.CValueEnclosure) *utils.CValueEnclosure {
	result := utils.CValueEnclosure{}
	if !max.IsNumeric() && !min.IsNumeric() {
		return &utils.CValueEnclosure{}
	}
	switch max.Dtype {
	case utils.SS_DT_FLOAT:
		result.Dtype = utils.SS_DT_FLOAT
		switch min.Dtype {
		case utils.SS_DT_FLOAT:
			result.CVal = max.CVal.(float64) - min.CVal.(float64)
		case utils.SS_DT_SIGNED_NUM:
			result.CVal = max.CVal.(float64) - float64(min.CVal.(int64))
		default:
			return &utils.CValueEnclosure{}
		}
	case utils.SS_DT_SIGNED_NUM:
		switch min.Dtype {
		case utils.SS_DT_FLOAT:
			result.Dtype = utils.SS_DT_FLOAT
			result.CVal = float64(max.CVal.(int64)) - min.CVal.(float64)
		case utils.SS_DT_SIGNED_NUM:
			result.Dtype = utils.SS_DT_SIGNED_NUM
			result.CVal = max.CVal.(int64) - min.CVal.(int64)
		default:
			return &utils.CValueEnclosure{}
		}
	default:
		log.Errorf("getRange: unsupported dtype: %v", max.Dtype)
	}

	return &result
}

func GetSegRange(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*utils.CValueEnclosure, error) {

	// start with lower resolution and upgrade as necessary
	result := utils.CValueEnclosure{}

	if currSegStat == nil {
		return &result, fmt.Errorf("GetSegRange: currSegStat is nil")
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		if !currSegStat.Min.IsNumeric() {
			return &result, nil
		}

		return getRange(currSegStat.Max, currSegStat.Min), nil
	}

	structs.UpdateMinMax(runningSegStat, currSegStat.Min)
	structs.UpdateMinMax(runningSegStat, currSegStat.Max)

	return getRange(runningSegStat.Max, runningSegStat.Min), nil
}

func GetSegSum(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*utils.NumTypeEnclosure, error) {

	// start with lower resolution and upgrade as necessary
	rSst := utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_SIGNED_NUM,
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
		StdOutLogger.Infof("UpdateSegmentStats called with am i only one")
		switch currSegStat.NumStats.Sum.Ntype {
		case utils.SS_DT_FLOAT:
			rSst.FloatVal = currSegStat.NumStats.Sum.FloatVal
			rSst.Ntype = utils.SS_DT_FLOAT
		default:
			rSst.IntgrVal = currSegStat.NumStats.Sum.IntgrVal
		}
		return &rSst, nil
	}

	switch currSegStat.NumStats.Sum.Ntype {
	case utils.SS_DT_FLOAT:
		if runningSegStat.NumStats.Sum.Ntype == utils.SS_DT_FLOAT {
			runningSegStat.NumStats.Sum.FloatVal = runningSegStat.NumStats.Sum.FloatVal + currSegStat.NumStats.Sum.FloatVal
			rSst.FloatVal = runningSegStat.NumStats.Sum.FloatVal
			rSst.Ntype = utils.SS_DT_FLOAT
		} else {
			runningSegStat.NumStats.Sum.FloatVal = float64(runningSegStat.NumStats.Sum.IntgrVal) + currSegStat.NumStats.Sum.FloatVal
			rSst.FloatVal = runningSegStat.NumStats.Sum.FloatVal
			rSst.Ntype = utils.SS_DT_FLOAT
		}
	default:
		if runningSegStat.NumStats.Sum.Ntype == utils.SS_DT_FLOAT {
			runningSegStat.NumStats.Sum.FloatVal = runningSegStat.NumStats.Sum.FloatVal + float64(currSegStat.NumStats.Sum.IntgrVal)
			rSst.FloatVal = runningSegStat.NumStats.Sum.FloatVal
			rSst.Ntype = utils.SS_DT_FLOAT
		} else {
			runningSegStat.NumStats.Sum.IntgrVal = runningSegStat.NumStats.Sum.IntgrVal + currSegStat.NumStats.Sum.IntgrVal
			rSst.IntgrVal = runningSegStat.NumStats.Sum.IntgrVal
		}
	}

	return &rSst, nil
}

func GetSegCardinality(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*utils.NumTypeEnclosure, error) {

	res := utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_SIGNED_NUM,
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

func GetSegCount(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*utils.NumTypeEnclosure, error) {

	rSst := utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_SIGNED_NUM,
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

func GetSegAvg(runningSegStat *structs.SegStats, currSegStat *structs.SegStats) (*utils.NumTypeEnclosure, error) {
	// Initialize result with default values
	rSst := utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_FLOAT,
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
	err := runningSegStat.NumStats.Sum.ReduceFast(currSegStat.NumStats.Sum.Ntype, currSegStat.NumStats.Sum.IntgrVal, currSegStat.NumStats.Sum.FloatVal, utils.Sum)
	if err != nil {
		return &rSst, fmt.Errorf("GetSegAvg: error in reducing sum, err: %+v", err)
	}
	// Calculate and return the average
	avg, err := getAverage(runningSegStat.NumStats.Sum, runningSegStat.NumStats.NumericCount)
	rSst.FloatVal = avg
	return &rSst, err
}

// Helper function to calculate the average
func getAverage(sum utils.NumTypeEnclosure, count uint64) (float64, error) {
	avg := 0.0
	if count == 0 {
		return avg, fmt.Errorf("getAverage: count is 0, cannot divide by 0")
	}
	switch sum.Ntype {
	case utils.SS_DT_FLOAT:
		avg = sum.FloatVal / float64(count)
	case utils.SS_DT_SIGNED_NUM:
		avg = float64(sum.IntgrVal) / float64(count)
	default:
		return avg, fmt.Errorf("getAverage: invalid data type: %v", sum.Ntype)
	}
	return avg, nil
}

func GetSegList(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*utils.CValueEnclosure, error) {
	res := utils.CValueEnclosure{
		Dtype: utils.SS_DT_STRING_SLICE,
		CVal:  make([]string, 0),
	}
	if currSegStat == nil || currSegStat.StringStats == nil || currSegStat.StringStats.StrList == nil {
		return &res, fmt.Errorf("GetSegList: currSegStat does not contain string list %v", currSegStat)
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		if len(currSegStat.StringStats.StrList) > utils.MAX_SPL_LIST_SIZE {
			finalStringList := make([]string, utils.MAX_SPL_LIST_SIZE)
			copy(finalStringList, currSegStat.StringStats.StrList[:utils.MAX_SPL_LIST_SIZE])
			res.CVal = finalStringList
		} else {
			finalStringList := make([]string, len(currSegStat.StringStats.StrList))
			copy(finalStringList, currSegStat.StringStats.StrList)
			res.CVal = finalStringList
		}
		return &res, nil
	}

	// Limit list size to match splunk.
	strList := make([]string, 0, utils.MAX_SPL_LIST_SIZE)

	if runningSegStat.StringStats != nil && runningSegStat.StringStats.StrList != nil {
		strList = utils.AppendWithLimit(strList, runningSegStat.StringStats.StrList, utils.MAX_SPL_LIST_SIZE)
	}

	strList = utils.AppendWithLimit(strList, currSegStat.StringStats.StrList, utils.MAX_SPL_LIST_SIZE)

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
func GetSegValue(runningSegStat *structs.SegStats, currSegStat *structs.SegStats) (*utils.CValueEnclosure, error) {
	res := utils.CValueEnclosure{
		Dtype: utils.SS_DT_STRING_SLICE,
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
	res.CVal = toputils.GetSortedStringKeys(strSet)
	return &res, nil
}
func GetSegStdDev(runningSegStat *structs.SegStats, currSegStat *structs.SegStats) (*utils.NumTypeEnclosure, error) {
	StdOutLogger.Infof("GetSegStdDev: Function called")

	rSst := utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_FLOAT,
		FloatVal: 0.0,
	}

	if currSegStat == nil {
		StdOutLogger.Infof("GetSegStdDev: currSegStat is nil")
		return &rSst, fmt.Errorf("GetSegStdDev: currSegStat is nil")
	}
	if currSegStat.NumStats == nil {
		StdOutLogger.Infof("GetSegStdDev: currSegStat.NumStats is nil")
		return &rSst, fmt.Errorf("GetSegStdDev: currSegStat.NumStats is nil")
	}
	if !currSegStat.IsNumeric {
		StdOutLogger.Infof("GetSegStdDev: currSegStat is non-numeric")
		return &rSst, fmt.Errorf("GetSegStdDev: current segStats is non-numeric")
	}

	StdOutLogger.Infof("GetSegStdDev: Processing segment stats")

	var sum, sumSq utils.NumTypeEnclosure
	var count uint64

	if runningSegStat != nil {
		sum = runningSegStat.NumStats.Sum
		sumSq = runningSegStat.NumStats.SumSq
		count = runningSegStat.NumStats.NumericCount
		StdOutLogger.Infof("GetSegStdDev: Initialized with runningSegStat - Sum: %+v, SumSq: %+v, Count: %d", sum, sumSq, count)
	}

	count += currSegStat.NumStats.NumericCount
	StdOutLogger.Infof("GetSegStdDev: Updated count after adding currSegStat: %d", count)

	// Aggregate sum
	if currSegStat.NumStats.Sum.Ntype == utils.SS_DT_FLOAT {
		sum.FloatVal += currSegStat.NumStats.Sum.FloatVal
		sum.Ntype = utils.SS_DT_FLOAT
	} else {
		sum.IntgrVal += currSegStat.NumStats.Sum.IntgrVal
	}

	StdOutLogger.Infof("GetSegStdDev: Updated sum - %+v", sum)

	// Aggregate sumSq
	if currSegStat.NumStats.SumSq.Ntype == utils.SS_DT_FLOAT {
		sumSq.FloatVal += currSegStat.NumStats.SumSq.FloatVal
		sumSq.Ntype = utils.SS_DT_FLOAT
	} else {
		sumSq.IntgrVal += currSegStat.NumStats.SumSq.IntgrVal
	}

	StdOutLogger.Infof("GetSegStdDev: Updated sumSq - %+v", sumSq)

	if runningSegStat != nil {
		runningSegStat.NumStats.Sum = sum
		runningSegStat.NumStats.SumSq = sumSq
		runningSegStat.NumStats.NumericCount = count
	}

	var sumFloat, sumSqFloat float64
	if sum.Ntype == utils.SS_DT_FLOAT {
		sumFloat = sum.FloatVal
	} else {
		sumFloat = float64(sum.IntgrVal)
	}

	if sumSq.Ntype == utils.SS_DT_FLOAT {
		sumSqFloat = sumSq.FloatVal
	} else {
		sumSqFloat = float64(sumSq.IntgrVal)
	}

	StdOutLogger.Infof("GetSegStdDev: Converted sumFloat: %f, sumSqFloat: %f", sumFloat, sumSqFloat)

	if count < 2 {
		StdOutLogger.Infof("GetSegStdDev: Insufficient data for stddev (count < 2)")
		return &rSst, fmt.Errorf("GetSegStdDev: need at least 2 values for stddev")
	}

	variance := math.Max(0, (sumSqFloat-(sumFloat*sumFloat)/float64(count))/float64(count-1))
	StdOutLogger.Infof("GetSegStdDev: Computed variance: %f", variance)

	rSst.FloatVal = math.Sqrt(variance)
	StdOutLogger.Infof("GetSegStdDev: Computed stddev: %f", rSst.FloatVal)

	return &rSst, nil
}
