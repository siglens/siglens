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

package structs

import (
	"math"
	"os"
	"path"
	"sync/atomic"

	pql "github.com/influxdata/promql/v2"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

/*
Struct to represent a single metrics query request.
*/
type MetricsQuery struct {
	MetricName      string // metric name to query for.
	HashedMName     uint64
	Aggregator      Aggreation
	Downsampler     Downsampler
	TagsFilters     []*TagsFilter // all tags filters to apply
	SelectAllSeries bool          //flag to select all series - for promQl

	reordered      bool   // if the tags filters have been reordered
	numStarFilters int    // index such that TagsFilters[:numStarFilters] are all star filters
	OrgId          uint64 // organization id
}

type Aggreation struct {
	AggregatorFunction utils.AggregateFunctions //aggregator function
	RangeFunction      utils.RangeFunctions     //range function to apply, only one of these will be non nil
	FuncConstant       float64
}

type Downsampler struct {
	Interval   int
	Unit       string
	CFlag      bool
	Aggregator Aggreation
}

/*
Represents a single tag filter for a metric query
*/
type TagsFilter struct {
	TagKey          string
	RawTagValue     interface{} //change it to utils.DtypeEnclosure later
	HashTagValue    uint64
	TagOperator     utils.TagOperator
	LogicalOperator utils.LogicalOperator
}

type MetricsQueryResponse struct {
	MetricName string             `json:"metric"`
	Tags       map[string]string  `json:"tags"`
	Dps        map[uint32]float64 `json:"dps"`
}

type Label struct {
	Name, Value string
}

type Data struct {
	ResultType pql.ValueType `json:"resultType"`
	Result     []pql.Series  `json:"series,omitempty"`
}
type MetricsQueryResponsePromQl struct {
	Status    string   `json:"status"` //success/error
	Data      Data     `json:"data"`
	ErrorType string   `json:"errorType"`
	Error     string   `json:"error"`
	Warnings  []string `json:"warnings"`
}

/*
Struct to represent the metrics arithmetic request and its corresponding timerange
*/
type QueryArithmetic struct {
	OperationId uint64
	LHS         uint64
	RHS         uint64
	ConstantOp  bool
	Operation   utils.ArithmeticOperator
	Constant    float64
	// maps groupid to a map of ts to value. This aggregates DsResults based on the aggregation function
	Results       map[string]map[uint32]float64
	OperatedState bool //true if operation has been executed
}

/*
Struct to represent the metrics query request and its corresponding timerange
*/
type MetricsQueryRequest struct {
	TimeRange    dtu.MetricsTimeRange
	MetricsQuery MetricsQuery
}

type OTSDBMetricsQueryExpTime struct {
	Start       interface{}                     `json:"start"`
	End         interface{}                     `json:"end"`
	Timezone    string                          `json:"timezone"`
	Aggregator  string                          `json:"aggregator"`
	Downsampler OTSDBMetricsQueryExpDownsampler `json:"downsampler"`
}

type OTSDBMetricsQueryExpDownsampler struct {
	Interval   string `json:"interval"`
	Aggregator string `json:"aggregator"`
}

type OTSDBMetricsQueryExpTags struct {
	Type    string `json:"type"`
	Tagk    string `json:"tagk"`
	Filter  string `json:"filter"`
	GroupBy bool   `json:"groupBy"`
}

type OTSDBMetricsQueryExpFilter struct {
	Tags []OTSDBMetricsQueryExpTags `json:"tags"`
	Id   string                     `json:"id"`
}

type OTSDBMetricsQueryExpMetric struct {
	Id         string            `json:"id"`
	MetricName string            `json:"metric"`
	Filter     string            `json:"filter"`
	Aggregator string            `json:"aggregator"`
	FillPolicy map[string]string `json:"fillPolicy"`
}

type OTSDBMetricsQueryExpressions struct {
	Id  string `json:"id"`
	Exp string `json:"exp"`
}

type OTSDBMetricsQueryExpOutput struct {
	Id    string `json:"id"`
	Alias string `json:"alias"`
}

type OTSDBMetricsQueryExpRequest struct {
	Time        OTSDBMetricsQueryExpTime       `json:"time"`
	Filters     []OTSDBMetricsQueryExpFilter   `json:"filters"`
	Metrics     []OTSDBMetricsQueryExpMetric   `json:"metrics"`
	Expressions []OTSDBMetricsQueryExpressions `json:"expressions"`
	Outputs     []OTSDBMetricsQueryExpOutput   `json:"outputs"`
}

type MetricsSearchRequest struct {
	MetricsKeyBaseDir string
	BlocksToSearch    map[uint16]bool
	Parallelism       uint
	QueryType         SegType
	AllTagKeys        map[string]bool
}

/*
NOTE: Change the value oF SIZE_OF_MBSUM each time this struct is updated
*/
type MBlockSummary struct {
	Blknum uint16
	HighTs uint32
	LowTs  uint32
}

func (mbs *MBlockSummary) Reset() {
	mbs.Blknum = 0
	mbs.HighTs = 0
	mbs.LowTs = math.MaxUint32
}

/*
Fixes the order of tags filters to be in the following order:
1. * tag filters
2. other tag filters
*/
func (mq *MetricsQuery) ReorderTagFilters() {
	if mq.reordered {
		return
	}
	starTags := make([]*TagsFilter, 0, len(mq.TagsFilters))
	otherTags := make([]*TagsFilter, 0, len(mq.TagsFilters))
	for _, tf := range mq.TagsFilters {
		if tagVal, ok := tf.RawTagValue.(string); ok && tagVal == "*" {
			starTags = append(starTags, tf)
		} else {
			otherTags = append(otherTags, tf)
		}
	}
	mq.TagsFilters = append(starTags, otherTags...)
	mq.reordered = true
	mq.numStarFilters = len(starTags)
}

func (mq *MetricsQuery) GetNumStarFilters() int {
	mq.ReorderTagFilters()
	return mq.numStarFilters
}

const SIZE_OF_MBSUM = 10 // 2 + 4 + 4

func (ds *Downsampler) GetIntervalTimeInSeconds() uint32 {
	intervalTime := uint32(0)
	switch ds.Unit {
	case "s":
		intervalTime += 1
	case "m":
		intervalTime += 60
	case "h":
		intervalTime += 3600
	case "d":
		intervalTime += 86400
	case "w":
		intervalTime += 86400 * 7
	case "n":
		intervalTime += 86400 * 30
	case "y":
		intervalTime += 86400 * 365
	default:
		log.Error("GetIntervalTimeInSeconds: invalid time format")
		return 0
	}

	return uint32(ds.Interval) * intervalTime
}

/*
Format of block summary file
[version - 1 byte][blk num - 2 bytes][high ts - 4 bytes][low ts - 4 bytes]
*/
func (mbs *MBlockSummary) FlushSummary(fName string) ([]byte, error) {
	var flag bool = false
	if _, err := os.Stat(fName); os.IsNotExist(err) {
		err := os.MkdirAll(path.Dir(fName), os.FileMode(0764))
		flag = true
		if err != nil {
			log.Errorf("Failed to create directory at %s: %v", path.Dir(fName), err)
			return nil, err
		}
	}
	fd, err := os.OpenFile(fName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("writeBlockSummary: open failed fname=%v, err=%v", fName, err)
		return nil, err
	}
	defer fd.Close()
	idx := 0
	var mBlkSum []byte
	// hard coded byte size for [version][blk num][high Ts][low Ts] when file is created
	if flag {
		mBlkSum = make([]byte, 19)
		copy(mBlkSum[idx:], utils.VERSION_MBLOCKSUMMARY)
		idx += 1
		// hard coded byte size for [blk num][high Ts][low Ts]
	} else {
		mBlkSum = make([]byte, 18)
	}
	copy(mBlkSum[idx:], toputils.Uint16ToBytesLittleEndian(mbs.Blknum))
	idx += 2
	copy(mBlkSum[idx:], toputils.Uint32ToBytesLittleEndian(mbs.HighTs))
	idx += 8
	copy(mBlkSum[idx:], toputils.Uint32ToBytesLittleEndian(mbs.LowTs))

	if _, err := fd.Write(mBlkSum); err != nil {
		log.Errorf("writeBlockSummary:  write failed blockSummaryFname=%v, err=%v", fName, err)
		return nil, err
	}
	return mBlkSum, nil
}

func (mbs *MBlockSummary) UpdateTimeRange(ts uint32) {
	if ts > mbs.HighTs {
		atomic.StoreUint32(&mbs.HighTs, ts)
	}
	if ts < mbs.LowTs {
		atomic.StoreUint32(&mbs.LowTs, ts)
	}
}
