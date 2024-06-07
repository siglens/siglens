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
	"sort"
	"sync/atomic"

	parser "github.com/prometheus/prometheus/promql/parser"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

/*
Struct to represent a single metrics query request.
*/
type MetricsQuery struct {
	MetricName             string            // metric name to query for.
	MetricOperator         utils.TagOperator // operator to apply on metric name
	MetricNameRegexPattern string            // regex pattern to apply on metric name
	QueryHash              uint64            // hash of the query
	HashedMName            uint64
	PqlQueryType           parser.ValueType // promql query type
	Aggregator             Aggregation
	Function               Function
	Downsampler            Downsampler
	TagsFilters            []*TagsFilter    // all tags filters to apply
	TagIndicesToKeep       map[int]struct{} // indices of tags to keep in the result
	SelectAllSeries        bool             //flag to select all series - for promQl

	MQueryAggs *MetricQueryAgg

	reordered       bool   // if the tags filters have been reordered
	numStarFilters  int    // index such that TagsFilters[:numStarFilters] are all star filters
	numValueFilters uint32 // number of value filters
	OrgId           uint64 // organization id

	ExitAfterTagsSearch bool // flag to exit after raw tags search
	TagValueSearchOnly  bool // flag to search only tag values
	GetAllLabels        bool // flag to get all label sets for each time series
	Groupby             bool // flag to group by tags
	GroupByMetricName   bool // flag to group by metric name
}

type Aggregation struct {
	AggregatorFunction utils.AggregateFunctions //aggregator function
	FuncConstant       float64
	GroupByFields      []string // group by fields will be sorted
}

type Function struct {
	MathFunction  utils.MathFunctions
	RangeFunction utils.RangeFunctions //range function to apply, only one of these will be non nil
	ValueList     []string
	TimeWindow    float64 //E.g: rate(metrics[1m]), extract 1m and convert to seconds
	Step          float64 //E.g: rate(metrics[5m:1m]), extract 1m and convert to seconds
	TimeFunction  utils.TimeFunctions
}

type Downsampler struct {
	Interval   int
	Unit       string
	CFlag      bool
	Aggregator Aggregation
}

type MetricQueryAggBlockType int

const (
	AggregatorBlock MetricQueryAggBlockType = iota + 1
	FunctionBlock
)

type MetricQueryAgg struct {
	AggBlockType    MetricQueryAggBlockType
	AggregatorBlock *Aggregation
	FunctionBlock   *Function
	Next            *MetricQueryAgg
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
	NotInitialGroup bool
}

type MetricsQueryResponse struct {
	MetricName string             `json:"metric"`
	Tags       map[string]string  `json:"tags"`
	Dps        map[uint32]float64 `json:"dps"`
}

type Label struct {
	Name, Value string
}

type Result struct {
	Metric map[string]string `json:"metric"`
	Value  []interface{}     `json:"values"`
}

type Data struct {
	ResultType parser.ValueType `json:"resultType"`
	Result     []Result         `json:"result,omitempty"`
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
	LHSExpr     *QueryArithmetic
	RHSExpr     *QueryArithmetic
	ConstantOp  bool
	Operation   utils.LogicalAndArithmeticOperator
	ReturnBool  bool // If a comparison operator, return 0/1 rather than filtering.
	Constant    float64
	// maps groupid to a map of ts to value. This aggregates DsResults based on the aggregation function
	Results        map[string]map[uint32]float64
	OperatedState  bool //true if operation has been executed
	VectorMatching *VectorMatching
}

// VectorMatching describes how elements from two Vectors in a binary
// operation are supposed to be matched.
type VectorMatching struct {
	// The cardinality of the two Vectors.
	Cardinality VectorMatchCardinality
	// MatchingLabels contains the labels which define equality of a pair of
	// elements from the Vectors.
	MatchingLabels []string
	// On includes the given label names from matching,
	// rather than excluding them.
	On bool
}

// VectorMatchCardinality describes the cardinality relationship
// of two Vectors in a binary operation.
type VectorMatchCardinality int

const (
	CardOneToOne VectorMatchCardinality = iota
	CardManyToOne
	CardOneToMany
	CardManyToMany
)

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
	MetricsKeyBaseDir    string
	BlocksToSearch       map[uint16]bool
	BlkWorkerParallelism uint
	QueryType            SegType
	AllTagKeys           map[string]bool
	UnrotatedMetricNames map[string]bool
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

type TagValueType string

// Values for TagValueType
const (
	StarValue   TagValueType = "*"
	ValueString TagValueType = "string"
)

type TagValueIndex struct {
	tagValueType TagValueType
	index        int
}

/*
Fixes the order of tags filters to be in the following order:
1. other tag filters
2. * tag filters
*/
func (mq *MetricsQuery) ReorderTagFilters() {
	if mq.reordered {
		return
	}

	// For arithmetic and logical operations, we use groupIDStr to check if there are exactly matching label sets between two vectors
	// However, for different vectors, since the groupID string is concatenated from tag key-value pairs, we cannot guarantee the order of concatenation.
	// Therefore, We can sort tagsFilter in advance to ensure that the tags are concatenated in lexicographical order.
	sort.Slice(mq.TagsFilters, func(i, j int) bool {
		return mq.TagsFilters[i].TagKey < mq.TagsFilters[j].TagKey
	})

	queriedTagKeys := make(map[string]TagValueIndex, len(mq.TagsFilters))

	starTags := make([]*TagsFilter, 0, len(mq.TagsFilters))
	otherTags := make([]*TagsFilter, 0, len(mq.TagsFilters))

	for _, tf := range mq.TagsFilters {
		if isStarValue(tf) {
			handleStarTag(tf, queriedTagKeys, &starTags)
		} else {
			handleValueTag(tf, queriedTagKeys, &otherTags, &starTags)
		}
	}

	mq.TagsFilters = append(otherTags, starTags...)
	mq.reordered = true
	mq.numStarFilters = len(starTags)
	mq.numValueFilters = uint32(len(otherTags))
}

// Checks if the tag filter value is a star
func isStarValue(tf *TagsFilter) bool {
	tagVal, ok := tf.RawTagValue.(string)
	return ok && tagVal == "*"
}

func (tf *TagsFilter) IsRegex() bool {
	return (tf.TagOperator == utils.Regex || tf.TagOperator == utils.NegRegex)
}

// Handles star tags logic
func handleStarTag(tf *TagsFilter, queriedTagKeys map[string]TagValueIndex, starTags *[]*TagsFilter) {
	if _, exists := queriedTagKeys[tf.TagKey]; !exists {
		*starTags = append(*starTags, tf)
		queriedTagKeys[tf.TagKey] = TagValueIndex{tagValueType: StarValue, index: len(*starTags) - 1}
	}
}

// Handles other tag values logic
func handleValueTag(tf *TagsFilter, queriedTagKeys map[string]TagValueIndex, otherTags, starTags *[]*TagsFilter) {
	tagValInd, exists := queriedTagKeys[tf.TagKey]
	if exists {
		if tagValInd.tagValueType == StarValue {
			// Remove the star tag filter
			*starTags = append((*starTags)[:tagValInd.index], (*starTags)[tagValInd.index+1:]...)
			// Once removed, continue to add this tf below to otherTags
		} else {
			// Skip adding if already exists and is not a star
			return
		}
	}
	*otherTags = append(*otherTags, tf)
	queriedTagKeys[tf.TagKey] = TagValueIndex{tagValueType: ValueString, index: len(*otherTags) - 1}
}

func (mq *MetricsQuery) GetNumStarFilters() int {
	mq.ReorderTagFilters()
	return mq.numStarFilters
}

func (mq *MetricsQuery) GetNumValueFilters() uint32 {
	return mq.numValueFilters
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
		log.Errorf("Downsampler.GetIntervalTimeInSeconds: invalid time format: %v", ds.Unit)
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
			log.Errorf("MBlockSummary.FlushSummary: Failed to create directory at %s, err: %v", path.Dir(fName), err)
			return nil, err
		}
	}
	fd, err := os.OpenFile(fName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("MBlockSummary.FlushSummary: Failed to open file: %v, err: %v", fName, err)
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
		log.Errorf("MBlockSummary.FlushSummary: Failed to write block in file: %v, err: %v", fName, err)
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

func (metricFunc Function) ShallowClone() *Function {
	functionCopy := metricFunc
	return &functionCopy
}

func (agg Aggregation) ShallowClone() *Aggregation {
	aggCopy := agg
	return &aggCopy
}

func (agg Aggregation) IsAggregateFromAllTimeseries() bool {
	return agg.AggregatorFunction == utils.Count || agg.AggregatorFunction == utils.TopK || agg.AggregatorFunction == utils.BottomK
}

func (mQuery *MetricsQuery) IsRegexOnMetricName() bool {
	return mQuery.MetricOperator == utils.Regex || mQuery.MetricOperator == utils.NegRegex
}
