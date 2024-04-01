/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package structs

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/axiomhq/hyperloglog"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/utils"
	sutils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type Numbers struct {
	Min_uint64  uint64             `json:"Min_uint64"`
	Max_uint64  uint64             `json:"Max_uint64"`
	Min_int64   int64              `json:"Min_int64"`
	Max_int64   int64              `json:"Max_int64"`
	Min_float64 float64            `json:"Min_float64"`
	Max_float64 float64            `json:"Max_float64"`
	NumType     utils.RangeNumType `json:"NumType"`
}

type OrderByAggregator struct {
	ColumnName    string
	IncreaseOrder bool
}

type TimeBucket struct {
	IntervalMillis uint64 // size of each histogram bucket in millis
	StartTime      uint64 // start time of histogram
	EndTime        uint64 // end time of histogram
	AggName        string // name of aggregation
	Timechart      *TimechartExpr
}

type RangeBucket struct {
	BucketKey     string  // column name to group
	Interval      float64 // interval of request
	MinBucketSize uint64  // minimum count for bucket to show up
	Count         uint64  // max number of unique histograms to return
}

type ValueOrder struct {
	Ascending bool // should results be given in ascending order or descending. (true = ascending, false = descending)
}

type ValueBucket struct {
	BucketKey     string // columnName for which to create buckets on
	MinBucketSize uint64 // min count per bucket
	Count         uint64 // limit number of resulting histograms
	CatchAll      bool
	ValueOrder    *ValueOrder // how to return filter buckets
}

type FilterCondition struct {
	Criteria *FilterCriteria
	Name     string
}

type FilterBucket struct {
	FilterCriteria []*FilterCondition
	CatchAllBucket bool
	Count          uint64 // limit number of resulting histograms
}

// Defines the different types of bucketing aggregations for processing of results
type AggregationType uint8

const (
	TimeHistogram AggregationType = iota
	GroupBy
)

type PipeCommandType uint8

const (
	OutputTransformType PipeCommandType = iota + 1
	MeasureAggsType
	GroupByType
	TransactionType
)

type QueryType uint8

const (
	InvalidCmd = iota
	SegmentStatsCmd
	GroupByCmd
	RRCCmd
)

// How to sort results
type SortRequest struct {
	ColName   string // column name to sort on
	Ascending bool   // if true, result is in ascending order. Else, result is in descending order
}

type FilterStringExpr struct {
	StringValue  string
	EvalBoolExpr *BoolExpr
	SearchNode   interface{} // type: *ast.Node while parsing, later evaluated to ASTNode
}

type TransactionArguments struct {
	SortedRecordsSlice    []map[string]interface{}
	OpenTransactionsState map[string]*TransactionGroupState
	OpenTransactionEvents map[string][]map[string]interface{}
	Fields                []string
	StartsWith            *FilterStringExpr
	EndsWith              *FilterStringExpr
}

type TransactionGroupState struct {
	Key       string
	Open      bool
	RecInden  string
	Timestamp uint64
}

type QueryAggregators struct {
	PipeCommandType      PipeCommandType
	OutputTransforms     *OutputTransforms
	MeasureOperations    []*MeasureAggregator
	MathOperations       []*MathEvaluator
	TimeHistogram        *TimeBucket     // Request for time histograms
	GroupByRequest       *GroupByRequest // groupby aggregation request
	Sort                 *SortRequest    // how to sort resulting data
	EarlyExit            bool            // should query early exit
	BucketLimit          int
	ShowRequest          *ShowRequest
	TableName            string
	TransactionArguments *TransactionArguments
	Next                 *QueryAggregators
	Limit                int
}

type ShowRequest struct {
	ShowTables     bool
	ShowFilter     *ShowFilter
	ShowTable      string
	ColumnsRequest *ShowColumns
}

type ShowColumns struct {
	InTable string
}

type ShowFilter struct {
	Like string
}

type OutputTransforms struct {
	HarcodedCol            []string
	RenameHardcodedColumns map[string]string
	OutputColumns          *ColumnsRequest    // post processing on output columns
	LetColumns             *LetColumnsRequest // let columns processing on output columns
	FilterRows             *BoolExpr          // discard rows failing some condition
	MaxRows                uint64             // if 0, get all results; else, get at most this many
	RowsAdded              uint64             // number of rows added to the result. This is used in conjunction with MaxRows.
}

type GroupByRequest struct {
	MeasureOperations []*MeasureAggregator
	GroupByColumns    []string
	AggName           string // name of aggregation
	BucketCount       int
}

type MeasureAggregator struct {
	MeasureCol         string                   `json:"measureCol,omitempty"`
	MeasureFunc        utils.AggregateFunctions `json:"measureFunc,omitempty"`
	StrEnc             string                   `json:"strEnc,omitempty"`
	ValueColRequest    *ValueExpr               `json:"valueColRequest,omitempty"`
	OverrodeMeasureAgg *MeasureAggregator       `json:"overrideFunc,omitempty"`
}

type MathEvaluator struct {
	MathCol         string              `json:"mathCol,omitempty"`
	MathFunc        utils.MathFunctions `json:"mathFunc,omitempty"`
	StrEnc          string              `json:"strEnc,omitempty"`
	ValueColRequest *ValueExpr          `json:"valueCol,omitempty"`
}

type ColumnsRequest struct {
	RenameColumns            map[string]string // maps original column name to new column name
	ExcludeColumns           []string          // names of columns to exclude
	IncludeColumns           []string          // names of columns to include
	IncludeValues            []*IncludeValue   // values of columns to include. Maps column name to index in column
	RenameAggregationColumns map[string]string // values of aggregations to rename
	Logfmt                   bool              // true if logfmt request
}

type IncludeValue struct {
	Index   int    //index of value in original column
	ColName string //original column name
	Label   string //new label of value in record
}

// Only NewColName and one of the other fields should have a value
type LetColumnsRequest struct {
	MultiColsRequest    *MultiColLetRequest
	SingleColRequest    *SingleColLetRequest
	ValueColRequest     *ValueExpr
	RexColRequest       *RexExpr
	StatisticColRequest *StatisticExpr
	RenameColRequest    *RenameExpr
	DedupColRequest     *DedupExpr
	SortColRequest      *SortExpr
	NewColName          string
}

type MultiColLetRequest struct {
	LeftCName  string
	Oper       utils.LogicalAndArithmeticOperator
	RightCName string
}

type SingleColLetRequest struct {
	CName string
	Oper  utils.LogicalAndArithmeticOperator
	Value *utils.DtypeEnclosure
}

type BucketResult struct {
	ElemCount   uint64                           // total number of elements in bucket
	StatRes     map[string]utils.CValueEnclosure // results of statistic functions
	BucketKey   interface{}                      // bucket key
	GroupByKeys []string
}

type AggregationResult struct {
	IsDateHistogram bool            // is this a date histogram
	Results         []*BucketResult // histogram results
}

type BucketHolder struct {
	GroupByValues []string
	MeasureVal    map[string]interface{}
}

type QueryCount struct {
	TotalCount uint64 // total number of
	Op         utils.FilterOperator
	EarlyExit  bool // if early exit was requested or not
}

// A helper struct to keep track of errors and results together
// In cases of partial failures, both logLines and errList can be defined
type NodeResult struct {
	AllRecords                []*utils.RecordResultContainer
	ErrList                   []error
	Histogram                 map[string]*AggregationResult
	TotalResults              *QueryCount
	RenameColumns             map[string]string
	SegEncToKey               map[uint16]string
	TotalRRCCount             uint64
	MeasureFunctions          []string        `json:"measureFunctions,omitempty"`
	MeasureResults            []*BucketHolder `json:"measure,omitempty"`
	GroupByCols               []string        `json:"groupByCols,omitempty"`
	Qtype                     string          `json:"qtype,omitempty"`
	BucketCount               int             `json:"bucketCount,omitempty"`
	PerformAggsOnRecs         bool            // if true, perform aggregations on records that are returned from rrcreader.go
	RecsAggsType              PipeCommandType // To determine Whether it is GroupByType or MeasureAggsType
	GroupByRequest            *GroupByRequest
	MeasureOperations         []*MeasureAggregator
	NextQueryAgg              *QueryAggregators
	RecsAggsBlockResults      interface{} // Evaluates to *blockresults.BlockResults
	RecsAggsProcessedSegments uint64
	RecsRunningSegStats       []*SegStats
	TransactionEventRecords   map[string]map[string]interface{}
	TransactionsProcessed     map[string]map[string]interface{}
}

type SegStats struct {
	IsNumeric   bool
	Count       uint64
	Hll         *hyperloglog.Sketch
	NumStats    *NumericStats
	StringStats *StringStats
	Records     []*utils.CValueEnclosure
}

type NumericStats struct {
	Min   utils.NumTypeEnclosure `json:"min,omitempty"`
	Max   utils.NumTypeEnclosure `json:"max,omitempty"`
	Sum   utils.NumTypeEnclosure `json:"sum,omitempty"`
	Dtype utils.SS_DTYPE         `json:"Dtype,omitempty"` // Dtype shared across min,max, and sum
}

type StringStats struct {
	StrSet map[string]struct{}
}

// json exportable struct for segstats
type SegStatsJSON struct {
	IsNumeric bool
	Count     uint64
	RawHll    []byte
	NumStats  *NumericStats
}

type AllSegStatsJSON struct {
	AllSegStats map[string]*SegStatsJSON
}

type RangeStat struct {
	Min float64
	Max float64
}

type AvgStat struct {
	Count int64
	Sum   float64
}

// init SegStats from raw bytes of SegStatsJSON
func (ss *SegStats) Init(rawSegStatJson []byte) error {
	var segStatJson *SegStatsJSON
	err := json.Unmarshal(rawSegStatJson, &segStatJson)
	if err != nil {
		log.Errorf("SegStats.Init: Failed to unmarshal SegStatsJSON: %v", err)
		return err
	}
	ss.IsNumeric = segStatJson.IsNumeric
	ss.Count = segStatJson.Count
	ss.Hll = hyperloglog.New()
	err = ss.Hll.UnmarshalBinary(segStatJson.RawHll)
	if err != nil {
		log.Errorf("SegStats.Init: Failed to unmarshal hyperloglog: %v", err)
		return err
	}
	ss.NumStats = segStatJson.NumStats
	return nil
}

func (ssj *SegStatsJSON) ToStats() (*SegStats, error) {
	ss := &SegStats{}
	ss.IsNumeric = ssj.IsNumeric
	ss.Count = ssj.Count
	ss.Hll = hyperloglog.New()
	err := ss.Hll.UnmarshalBinary(ssj.RawHll)
	if err != nil {
		log.Errorf("SegStatsJSON.ToStats: Failed to unmarshal hyperloglog: %v", err)
		return nil, err
	}
	ss.NumStats = ssj.NumStats
	return ss, nil
}

// convert SegStats to SegStatsJSON
func (ss *SegStats) ToJSON() (*SegStatsJSON, error) {
	segStatJson := &SegStatsJSON{}
	segStatJson.IsNumeric = ss.IsNumeric
	segStatJson.Count = ss.Count
	rawHll, err := ss.Hll.MarshalBinary()
	if err != nil {
		log.Errorf("SegStats.ToJSON: Failed to marshal hyperloglog: %v", err)
		return nil, err
	}
	segStatJson.RawHll = rawHll
	segStatJson.NumStats = ss.NumStats
	return segStatJson, nil
}

func (ma *MeasureAggregator) String() string {
	if ma.StrEnc != "" {
		return ma.StrEnc
	}
	ma.StrEnc = fmt.Sprintf("%+v(%v)", ma.MeasureFunc.String(), ma.MeasureCol)
	return ma.StrEnc
}

func (ss *SegStats) Merge(other *SegStats) {
	ss.Count += other.Count
	ss.Records = append(ss.Records, other.Records...)
	err := ss.Hll.Merge(other.Hll)
	if err != nil {
		log.Errorf("Failed to merge hyperloglog stats: %v", err)
	}

	if ss.NumStats == nil {
		ss.NumStats = other.NumStats
		return
	}
	ss.NumStats.Merge(other.NumStats)
}

func (ss *NumericStats) Merge(other *NumericStats) {
	switch ss.Min.Ntype {
	case utils.SS_DT_FLOAT:
		if other.Dtype == utils.SS_DT_FLOAT {
			ss.Min.FloatVal = math.Min(ss.Min.FloatVal, other.Min.FloatVal)
			ss.Max.FloatVal = math.Max(ss.Max.FloatVal, other.Max.FloatVal)
			ss.Sum.FloatVal = ss.Sum.FloatVal + other.Sum.FloatVal
		} else {
			ss.Min.FloatVal = math.Min(ss.Min.FloatVal, float64(other.Min.IntgrVal))
			ss.Max.FloatVal = math.Max(ss.Max.FloatVal, float64(other.Max.IntgrVal))
			ss.Sum.FloatVal = ss.Sum.FloatVal + float64(other.Sum.IntgrVal)
		}
	default:
		if other.Dtype == utils.SS_DT_FLOAT {
			ss.Min.FloatVal = math.Min(float64(ss.Min.IntgrVal), other.Min.FloatVal)
			ss.Max.FloatVal = math.Max(float64(ss.Max.IntgrVal), other.Max.FloatVal)
			ss.Sum.FloatVal = float64(ss.Sum.IntgrVal) + other.Sum.FloatVal
			ss.Dtype = utils.SS_DT_FLOAT
		} else {
			ss.Min.IntgrVal = sutils.MinInt64(ss.Min.IntgrVal, other.Min.IntgrVal)
			ss.Max.IntgrVal = sutils.MaxInt64(ss.Max.IntgrVal, other.Max.IntgrVal)
			ss.Sum.IntgrVal = ss.Sum.IntgrVal + other.Sum.IntgrVal
			ss.Dtype = utils.SS_DT_SIGNED_NUM
		}
	}
}

func (nr *NodeResult) ApplyScroll(scroll int) {

	if scroll == 0 {
		return
	}

	if len(nr.AllRecords) <= scroll {
		nr.AllRecords = make([]*utils.RecordResultContainer, 0)
		return
	}

	nr.AllRecords = nr.AllRecords[scroll:]
}

func (n *Numbers) Copy() *Numbers {

	retNum := &Numbers{
		NumType: n.NumType,
	}
	switch n.NumType {
	case utils.RNT_UNSIGNED_INT:
		retNum.Min_uint64 = n.Min_uint64
		retNum.Max_uint64 = n.Max_uint64
	case utils.RNT_SIGNED_INT:
		retNum.Min_int64 = n.Min_int64
		retNum.Max_int64 = n.Max_int64
	case utils.RNT_FLOAT64:
		retNum.Min_float64 = n.Min_float64
		retNum.Max_float64 = n.Max_float64
	}
	return retNum
}

func (qa *QueryAggregators) IsAggsEmpty() bool {
	if qa.TimeHistogram != nil {
		return false
	}
	if qa.GroupByRequest != nil {
		if qa.GroupByRequest.GroupByColumns != nil && len(qa.GroupByRequest.GroupByColumns) > 0 {
			return false
		}
		if qa.GroupByRequest.MeasureOperations != nil && len(qa.GroupByRequest.MeasureOperations) > 0 {
			return false
		}
	}
	return true
}

func (qa *QueryAggregators) IsStatisticBlockEmpty() bool {
	return (qa != nil && qa.OutputTransforms != nil && qa.OutputTransforms.LetColumns != nil &&
		qa.OutputTransforms.LetColumns.StatisticColRequest == nil)
}

// To determine whether it contains certain specific AggregatorBlocks, such as: Rename Block, Rex Block...
func (qa *QueryAggregators) hasLetColumnsRequest() bool {
	return qa != nil && qa.OutputTransforms != nil && qa.OutputTransforms.LetColumns != nil &&
		(qa.OutputTransforms.LetColumns.RexColRequest != nil || qa.OutputTransforms.LetColumns.RenameColRequest != nil || qa.OutputTransforms.LetColumns.DedupColRequest != nil ||
			qa.OutputTransforms.LetColumns.ValueColRequest != nil || qa.OutputTransforms.LetColumns.SortColRequest != nil)
}

// To determine whether it contains certain specific AggregatorBlocks, such as: Rename Block, Rex Block, MaxRows...
func (qa *QueryAggregators) HasQueryAggergatorBlock() bool {
	return qa != nil && qa.OutputTransforms != nil && (qa.hasLetColumnsRequest() || qa.OutputTransforms.MaxRows > qa.OutputTransforms.RowsAdded)
}

func (qa *QueryAggregators) HasQueryAggergatorBlockInChain() bool {
	if qa.HasQueryAggergatorBlock() {
		return true
	}
	if qa.Next != nil {
		return qa.Next.HasQueryAggergatorBlockInChain()
	}
	return false
}

func (qa *QueryAggregators) HasDedupBlock() bool {
	if qa != nil && qa.OutputTransforms != nil && qa.OutputTransforms.LetColumns != nil {
		letColumns := qa.OutputTransforms.LetColumns

		if letColumns.DedupColRequest != nil {
			return true
		}
	}

	return false
}

func (qa *QueryAggregators) HasDedupBlockInChain() bool {
	if qa.HasDedupBlock() {
		return true
	}
	if qa.Next != nil {
		return qa.Next.HasDedupBlockInChain()
	}
	return false
}

func (qa *QueryAggregators) HasSortBlock() bool {
	if qa != nil && qa.OutputTransforms != nil && qa.OutputTransforms.LetColumns != nil {
		letColumns := qa.OutputTransforms.LetColumns

		if letColumns.SortColRequest != nil {
			return true
		}
	}

	return false
}

func (qa *QueryAggregators) HasSortBlockInChain() bool {
	if qa.HasSortBlock() {
		return true
	}
	if qa.Next != nil {
		return qa.Next.HasSortBlockInChain()
	}
	return false
}

func (qa *QueryAggregators) HasTransactionArguments() bool {
	return qa != nil && qa.TransactionArguments != nil
}

func (qa *QueryAggregators) HasTransactionArgumentsInChain() bool {
	if qa.HasTransactionArguments() {
		return true
	}
	if qa.Next != nil {
		return qa.Next.HasTransactionArgumentsInChain()
	}
	return false
}

func (qa *QueryAggregators) HasRexBlockInQA() bool {
	return qa != nil && qa.OutputTransforms != nil && qa.OutputTransforms.LetColumns != nil &&
		(qa.OutputTransforms.LetColumns.RexColRequest != nil)
}

func (qa *QueryAggregators) HasGroupByOrMeasureAggsInBlock() bool {
	return qa != nil && (qa.GroupByRequest != nil || qa.MeasureOperations != nil)
}

func (qa *QueryAggregators) HasGroupByOrMeasureAggsInChain() bool {
	if qa.HasGroupByOrMeasureAggsInBlock() {
		return true
	}
	if qa.Next != nil {
		return qa.Next.HasGroupByOrMeasureAggsInChain()
	}
	return false
}

func (qa *QueryAggregators) HasRexBlockInChainWithStats() bool {
	if qa.HasRexBlockInQA() {
		return qa.Next != nil && qa.Next.HasGroupByOrMeasureAggsInChain()
	}
	if qa.Next != nil {
		return qa.Next.HasRexBlockInChainWithStats()
	}
	return false
}

// To determine whether it contains ValueColRequest
func (qa *QueryAggregators) HasValueColRequest() bool {
	for _, agg := range qa.MeasureOperations {
		if agg.ValueColRequest != nil {
			return true
		}
	}
	return false
}

// To determine whether it contains Aggregate Func: Values()
func (qa *QueryAggregators) HasValuesFunc() bool {
	for _, agg := range qa.MeasureOperations {
		if agg.MeasureFunc == utils.Values {
			return true
		}
	}
	return false
}

func (qa *QueryAggregators) UsedByTimechart() bool {
	return qa != nil && qa.TimeHistogram != nil && qa.TimeHistogram.Timechart != nil
}

func (qa *QueryAggregators) CanLimitBuckets() bool {
	// We shouldn't limit the buckets if there's other things to do after the
	// aggregation, like sorting, filtering, making new columns, etc.
	return qa.Sort == nil && qa.Next == nil
}

// Init default query aggregators.
// By default, a descending sort is added
func InitDefaultQueryAggregations() *QueryAggregators {
	qAggs := &QueryAggregators{
		EarlyExit: true,
		Sort: &SortRequest{
			ColName:   config.GetTimeStampKey(),
			Ascending: false,
		},
	}
	return qAggs
}

func (qtype QueryType) String() string {

	switch qtype {
	case SegmentStatsCmd:
		return "segstats-query"
	case GroupByCmd:
		return "aggs-query"
	case RRCCmd:
		return "logs-query"
	default:
		return "invalid"
	}
}
