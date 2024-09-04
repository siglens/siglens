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
	"math"
	"os"

	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"

	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

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
			log.Errorf("qid=%d, ReadSegStats: error reading single sst for cname: %v, err: %v",
				qid, cname, err)
			return retVal, err
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
	case utils.VERSION_SEGSTATS_BUF[0]:
		hllSize = toputils.BytesToUint32LittleEndian(fdata[idx : idx+4])
		idx += 4
	case utils.VERSION_SEGSTATS_BUF_LEGACY_2[0], utils.VERSION_SEGSTATS_BUF_LEGACY_1[0]:
		hllSize = uint32(toputils.BytesToUint16LittleEndian(fdata[idx : idx+2]))
		idx += 2
	default:
		log.Errorf("qid=%d, readSingleSst: unknown version: %v", qid, version)
		return nil, errors.New("readSingleSst: unknown version")
	}

	if version == utils.VERSION_SEGSTATS_BUF_LEGACY_1[0] {
		log.Infof("qid=%d, readSingleSst: ignoring Hll (old version)", qid)
	} else {
		err := sst.CreateHllFromBytes(fdata[idx : idx+hllSize])
		if err != nil {
			log.Errorf("qid=%d, readSingleSst: unable to create Hll from raw bytes. sst err: %v", qid, err)
			return nil, err
		}
	}

	idx += hllSize

	if !sst.IsNumeric {
		return &sst, nil
	}

	sst.NumStats = &structs.NumericStats{}
	// read Min Ntype
	min := utils.NumTypeEnclosure{}
	min.Ntype = utils.SS_DTYPE(fdata[idx : idx+1][0])
	idx += 1
	if min.Ntype == utils.SS_DT_FLOAT {
		min.FloatVal = toputils.BytesToFloat64LittleEndian(fdata[idx : idx+8])
	} else {
		min.IntgrVal = toputils.BytesToInt64LittleEndian(fdata[idx : idx+8])
	}
	sst.NumStats.Min = min
	idx += 8

	// read Max Ntype
	max := utils.NumTypeEnclosure{}
	max.Ntype = utils.SS_DTYPE(fdata[idx : idx+1][0])
	idx += 1
	if max.Ntype == utils.SS_DT_FLOAT {
		max.FloatVal = toputils.BytesToFloat64LittleEndian(fdata[idx : idx+8])
	} else {
		max.IntgrVal = toputils.BytesToInt64LittleEndian(fdata[idx : idx+8])
	}
	sst.NumStats.Max = max
	idx += 8

	// read Sum Ntype
	sum := utils.NumTypeEnclosure{}
	sum.Ntype = utils.SS_DTYPE(fdata[idx : idx+1][0])
	idx += 1
	if sum.Ntype == utils.SS_DT_FLOAT {
		sum.FloatVal = toputils.BytesToFloat64LittleEndian(fdata[idx : idx+8])
	} else {
		sum.IntgrVal = toputils.BytesToInt64LittleEndian(fdata[idx : idx+8])
	}
	sst.NumStats.Sum = sum

	return &sst, nil
}

func GetSegMin(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*utils.NumTypeEnclosure, error) {

	rSst := utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_SIGNED_NUM,
		IntgrVal: math.MaxInt64,
	}

	if currSegStat == nil {
		log.Errorf("GetSegMin: currSegStat is nil")
		return &rSst, errors.New("GetSegMin: currSegStat is nil")
	}

	if !currSegStat.IsNumeric {
		log.Errorf("GetSegMin: current segStats is non-numeric")
		return &rSst, errors.New("GetSegMin: current segStat is non-numeric")
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		switch currSegStat.NumStats.Min.Ntype {
		case utils.SS_DT_FLOAT:
			rSst.FloatVal = currSegStat.NumStats.Min.FloatVal
			rSst.Ntype = utils.SS_DT_FLOAT
		default:
			rSst.IntgrVal = currSegStat.NumStats.Min.IntgrVal
		}
		return &rSst, nil
	}

	switch currSegStat.NumStats.Min.Ntype {
	case utils.SS_DT_FLOAT:
		if runningSegStat.NumStats.Min.Ntype == utils.SS_DT_FLOAT {
			runningSegStat.NumStats.Min.FloatVal = math.Min(runningSegStat.NumStats.Min.FloatVal, currSegStat.NumStats.Min.FloatVal)
			rSst.FloatVal = runningSegStat.NumStats.Min.FloatVal
			rSst.Ntype = utils.SS_DT_FLOAT
		} else {
			runningSegStat.NumStats.Min.FloatVal = math.Min(float64(runningSegStat.NumStats.Min.IntgrVal), currSegStat.NumStats.Min.FloatVal)
			runningSegStat.NumStats.Min.Ntype = utils.SS_DT_FLOAT
			rSst.FloatVal = runningSegStat.NumStats.Min.FloatVal
			rSst.Ntype = utils.SS_DT_FLOAT
		}
	default:
		if runningSegStat.NumStats.Min.Ntype == utils.SS_DT_FLOAT {
			runningSegStat.NumStats.Min.FloatVal = math.Min(runningSegStat.NumStats.Min.FloatVal, float64(currSegStat.NumStats.Min.IntgrVal))
			rSst.FloatVal = runningSegStat.NumStats.Min.FloatVal
			rSst.Ntype = utils.SS_DT_FLOAT
		} else {
			runningSegStat.NumStats.Min.IntgrVal = toputils.MinInt64(runningSegStat.NumStats.Min.IntgrVal, currSegStat.NumStats.Min.IntgrVal)
			rSst.IntgrVal = runningSegStat.NumStats.Min.IntgrVal
		}
	}
	return &rSst, nil
}

func GetSegMax(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*utils.NumTypeEnclosure, error) {

	// start with lower resolution and upgrade as necessary
	rSst := utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_SIGNED_NUM,
		IntgrVal: math.MinInt64,
	}

	if currSegStat == nil {
		log.Errorf("GetSegMax: currSegStat is nil")
		return &rSst, errors.New("GetSegMax: currSegStat is nil")
	}

	if !currSegStat.IsNumeric {
		log.Errorf("GetSegMax: current segStats is non-numeric")
		return &rSst, errors.New("GetSegMax: current segStat is non-numeric")
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		switch currSegStat.NumStats.Max.Ntype {
		case utils.SS_DT_FLOAT:
			rSst.FloatVal = currSegStat.NumStats.Max.FloatVal
			rSst.Ntype = utils.SS_DT_FLOAT
		default:
			rSst.IntgrVal = currSegStat.NumStats.Max.IntgrVal
		}
		return &rSst, nil
	}

	switch currSegStat.NumStats.Max.Ntype {
	case utils.SS_DT_FLOAT:
		if runningSegStat.NumStats.Max.Ntype == utils.SS_DT_FLOAT {
			runningSegStat.NumStats.Max.FloatVal = math.Max(runningSegStat.NumStats.Max.FloatVal, currSegStat.NumStats.Max.FloatVal)
			rSst.FloatVal = runningSegStat.NumStats.Max.FloatVal
			rSst.Ntype = utils.SS_DT_FLOAT
		} else {
			runningSegStat.NumStats.Max.FloatVal = math.Max(float64(runningSegStat.NumStats.Max.IntgrVal), currSegStat.NumStats.Max.FloatVal)
			rSst.FloatVal = runningSegStat.NumStats.Max.FloatVal
			rSst.Ntype = utils.SS_DT_FLOAT
		}
	default:
		if runningSegStat.NumStats.Max.Ntype == utils.SS_DT_FLOAT {
			runningSegStat.NumStats.Max.FloatVal = math.Max(runningSegStat.NumStats.Max.FloatVal, float64(currSegStat.NumStats.Max.IntgrVal))
			rSst.FloatVal = runningSegStat.NumStats.Max.FloatVal
			rSst.Ntype = utils.SS_DT_FLOAT
		} else {
			runningSegStat.NumStats.Max.IntgrVal = toputils.MaxInt64(runningSegStat.NumStats.Max.IntgrVal, currSegStat.NumStats.Max.IntgrVal)
			rSst.IntgrVal = runningSegStat.NumStats.Max.IntgrVal
		}
	}
	return &rSst, nil
}

func GetSegRange(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*utils.NumTypeEnclosure, error) {

	// start with lower resolution and upgrade as necessary
	rSst := utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_SIGNED_NUM,
		IntgrVal: 0,
	}
	if currSegStat == nil {
		log.Errorf("GetSegRange: currSegStat is nil")
		return &rSst, errors.New("GetSegRange: currSegStat is nil")
	}

	if !currSegStat.IsNumeric {
		log.Errorf("GetSegRange: current segStats is non-numeric")
		return &rSst, errors.New("GetSegRange: current segStat is non-numeric")
	}

	if currSegStat.NumStats.Max.Ntype != currSegStat.NumStats.Min.Ntype {
		return &rSst, nil
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		switch currSegStat.NumStats.Max.Ntype {
		case utils.SS_DT_FLOAT:
			rSst.FloatVal = currSegStat.NumStats.Max.FloatVal - currSegStat.NumStats.Min.FloatVal
			rSst.Ntype = utils.SS_DT_FLOAT
		default:
			rSst.IntgrVal = currSegStat.NumStats.Max.IntgrVal - currSegStat.NumStats.Min.IntgrVal
		}
		return &rSst, nil
	}

	switch currSegStat.NumStats.Max.Ntype {
	case utils.SS_DT_FLOAT:
		if runningSegStat.NumStats.Max.Ntype == utils.SS_DT_FLOAT && runningSegStat.NumStats.Min.Ntype == utils.SS_DT_FLOAT {
			runningSegStat.NumStats.Max.FloatVal = math.Max(runningSegStat.NumStats.Max.FloatVal, currSegStat.NumStats.Max.FloatVal)
			runningSegStat.NumStats.Min.FloatVal = math.Min(runningSegStat.NumStats.Min.FloatVal, currSegStat.NumStats.Min.FloatVal)
			rSst.FloatVal = runningSegStat.NumStats.Max.FloatVal - runningSegStat.NumStats.Min.FloatVal
			rSst.Ntype = utils.SS_DT_FLOAT
		} else {
			runningSegStat.NumStats.Max.FloatVal = math.Max(float64(runningSegStat.NumStats.Max.IntgrVal), currSegStat.NumStats.Max.FloatVal)
			runningSegStat.NumStats.Min.FloatVal = math.Min(float64(runningSegStat.NumStats.Min.IntgrVal), currSegStat.NumStats.Min.FloatVal)
			rSst.FloatVal = runningSegStat.NumStats.Max.FloatVal - runningSegStat.NumStats.Min.FloatVal
			rSst.Ntype = utils.SS_DT_FLOAT
		}
	default:
		if runningSegStat.NumStats.Max.Ntype == utils.SS_DT_FLOAT && runningSegStat.NumStats.Min.Ntype == utils.SS_DT_FLOAT {
			runningSegStat.NumStats.Max.FloatVal = math.Max(runningSegStat.NumStats.Max.FloatVal, float64(currSegStat.NumStats.Max.IntgrVal))
			runningSegStat.NumStats.Min.FloatVal = math.Min(runningSegStat.NumStats.Min.FloatVal, float64(currSegStat.NumStats.Min.IntgrVal))
			rSst.FloatVal = runningSegStat.NumStats.Max.FloatVal - runningSegStat.NumStats.Min.FloatVal
			rSst.Ntype = utils.SS_DT_FLOAT
		} else {
			runningSegStat.NumStats.Max.IntgrVal = toputils.MaxInt64(runningSegStat.NumStats.Max.IntgrVal, currSegStat.NumStats.Max.IntgrVal)
			runningSegStat.NumStats.Min.IntgrVal = toputils.MinInt64(runningSegStat.NumStats.Min.IntgrVal, currSegStat.NumStats.Min.IntgrVal)
			rSst.IntgrVal = runningSegStat.NumStats.Max.IntgrVal - runningSegStat.NumStats.Min.IntgrVal
		}
	}

	return &rSst, nil
}

func GetSegSum(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*utils.NumTypeEnclosure, error) {

	// start with lower resolution and upgrade as necessary
	rSst := utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_SIGNED_NUM,
		IntgrVal: 0,
	}
	if currSegStat == nil {
		log.Errorf("GetSegSum: currSegStat is nil")
		return &rSst, errors.New("GetSegSum: currSegStat is nil")
	}

	if !currSegStat.IsNumeric {
		log.Errorf("GetSegSum: current segStats is non-numeric")
		return &rSst, errors.New("GetSegSum: current segStat is non-numeric")
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
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
		log.Errorf("GetSegCardinality: currSegStat is nil")
		return &res, errors.New("GetSegCardinality: currSegStat is nil")
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		res.IntgrVal = int64(currSegStat.GetHllCardinality())
		return &res, nil
	}

	err := runningSegStat.Hll.StrictUnion(currSegStat.Hll.Hll)
	if err != nil {
		log.Errorf("GetSegCardinality: error in Hll.Merge, err: %+v", err)
		return nil, err
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
		log.Errorf("GetSegCount: currSegStat is nil")
		return &rSst, errors.New("GetSegCount: currSegStat is nil")
	}

	if runningSegStat == nil {
		rSst.IntgrVal = int64(currSegStat.Count)
		return &rSst, nil
	}

	runningSegStat.Count = runningSegStat.Count + currSegStat.Count
	rSst.IntgrVal = int64(runningSegStat.Count)

	return &rSst, nil
}

func GetSegAvg(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*utils.NumTypeEnclosure, error) {

	// start with lower resolution and upgrade as necessary
	rSst := utils.NumTypeEnclosure{
		Ntype:    utils.SS_DT_SIGNED_NUM,
		IntgrVal: 0,
	}
	if currSegStat == nil {
		log.Errorf("GetSegAvg: currSegStat is nil")
		return &rSst, errors.New("GetSegAvg: currSegStat is nil")
	}

	if !currSegStat.IsNumeric {
		log.Errorf("GetSegAvg: current segStats is non-numeric")
		return &rSst, errors.New("GetSegAvg: current segStat is non-numeric")
	}

	// if this is the first segment, then running will be nil, and we return the first seg's stats
	if runningSegStat == nil {
		switch currSegStat.NumStats.Sum.Ntype {
		case utils.SS_DT_FLOAT:
			rSst.FloatVal = currSegStat.NumStats.Sum.FloatVal / float64(currSegStat.Count)
			rSst.Ntype = utils.SS_DT_FLOAT
		default:
			rSst.FloatVal = float64(currSegStat.NumStats.Sum.IntgrVal) / float64(currSegStat.Count)
			rSst.Ntype = utils.SS_DT_FLOAT
		}
		return &rSst, nil
	}
	runningSegStat.Count = runningSegStat.Count + currSegStat.Count

	switch currSegStat.NumStats.Sum.Ntype {
	case utils.SS_DT_FLOAT:
		if runningSegStat.NumStats.Sum.Ntype == utils.SS_DT_FLOAT {
			runningSegStat.NumStats.Sum.FloatVal = runningSegStat.NumStats.Sum.FloatVal + currSegStat.NumStats.Sum.FloatVal
			rSst.FloatVal = runningSegStat.NumStats.Sum.FloatVal / float64(runningSegStat.Count)
			rSst.Ntype = utils.SS_DT_FLOAT
		} else {
			runningSegStat.NumStats.Sum.FloatVal = float64(runningSegStat.NumStats.Sum.IntgrVal) + currSegStat.NumStats.Sum.FloatVal
			rSst.FloatVal = runningSegStat.NumStats.Sum.FloatVal / float64(runningSegStat.Count)
			rSst.Ntype = utils.SS_DT_FLOAT
		}
	default:
		if runningSegStat.NumStats.Sum.Ntype == utils.SS_DT_FLOAT {
			runningSegStat.NumStats.Sum.FloatVal = runningSegStat.NumStats.Sum.FloatVal + float64(currSegStat.NumStats.Sum.IntgrVal)
			rSst.FloatVal = runningSegStat.NumStats.Sum.FloatVal / float64(runningSegStat.Count)
			rSst.Ntype = utils.SS_DT_FLOAT
		} else {
			runningSegStat.NumStats.Sum.FloatVal = float64(runningSegStat.NumStats.Sum.IntgrVal + currSegStat.NumStats.Sum.IntgrVal)
			runningSegStat.NumStats.Sum.Ntype = utils.SS_DT_FLOAT
			rSst.FloatVal = runningSegStat.NumStats.Sum.FloatVal / float64(runningSegStat.Count)
			rSst.Ntype = utils.SS_DT_FLOAT
		}
	}

	return &rSst, nil
}

func GetSegList(runningSegStat *structs.SegStats,
	currSegStat *structs.SegStats) (*utils.CValueEnclosure, error) {
	res := utils.CValueEnclosure{
		Dtype: utils.SS_DT_STRING_SLICE,
		CVal:  make([]string, 0),
	}
	if currSegStat == nil {
		log.Errorf("GetSegList: currSegStat is nil")
		return &res, errors.New("GetSegList: currSegStat is nil")
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

	if runningSegStat.StringStats != nil {
		strList = utils.AppendWithLimit(strList, runningSegStat.StringStats.StrList, utils.MAX_SPL_LIST_SIZE)
	}

	if currSegStat.StringStats != nil && currSegStat.StringStats.StrList != nil {
		strList = utils.AppendWithLimit(strList, currSegStat.StringStats.StrList, utils.MAX_SPL_LIST_SIZE)
	}

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
