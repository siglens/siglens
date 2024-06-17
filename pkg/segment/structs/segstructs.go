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
	"encoding/json"
	"fmt"
	"math"
	"strings"

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
	VectorArithmeticExprType
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

type StatsOptions struct {
	Delim          string
	Partitions     uint64
	DedupSplitvals bool
	Allnum         bool
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
	VectorArithmeticExpr *NumericExpr
	TimeHistogram        *TimeBucket     // Request for time histograms
	GroupByRequest       *GroupByRequest // groupby aggregation request
	Sort                 *SortRequest    // how to sort resulting data
	EarlyExit            bool            // should query early exit
	BucketLimit          int
	ShowRequest          *ShowRequest
	TableName            string
	TransactionArguments *TransactionArguments
	StatsOptions         *StatsOptions
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
	Param              string
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
	MultiColsRequest     *MultiColLetRequest
	SingleColRequest     *SingleColLetRequest
	ValueColRequest      *ValueExpr
	RexColRequest        *RexExpr
	StatisticColRequest  *StatisticExpr
	RenameColRequest     *RenameExpr
	DedupColRequest      *DedupExpr
	SortColRequest       *SortExpr
	NewColName           string
	MultiValueColRequest *MultiValueColLetRequest
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

type MultiValueColLetRequest struct {
	Command         string // name of the command: makemv, mvcombine, mvexpand, etc.
	ColName         string
	DelimiterString string // delimiter string to split the column value. default is " " (single space)
	IsRegex         bool
	AllowEmpty      bool // if true, empty strings are allowed in the split values. default is false
	Setsv           bool // if true, split values are combined into a single value. default is false
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
	VectorResultValue         float64
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
	RecsAggsBlockResults      interface{}              // Evaluates to *blockresults.BlockResults
	RecsAggsColumnKeysMap     map[string][]interface{} // map of column name to column keys for GroupBy Recs
	RecsAggsProcessedSegments uint64
	RecsRunningSegStats       []*SegStats
	TransactionEventRecords   map[string]map[string]interface{}
	TransactionsProcessed     map[string]map[string]interface{}
	ColumnsOrder              map[string]int
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
		log.Errorf("SegStats.Init: Failed to unmarshal SegStatsJSON error: %v data: %v", err, string(rawSegStatJson))
		return err
	}
	ss.IsNumeric = segStatJson.IsNumeric
	ss.Count = segStatJson.Count
	ss.Hll = hyperloglog.New()
	err = ss.Hll.UnmarshalBinary(segStatJson.RawHll)
	if err != nil {
		log.Errorf("SegStats.Init: Failed to unmarshal hyperloglog error: %v data: %v", err, string(segStatJson.RawHll))
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
		log.Errorf("SegStatsJSON.ToStats: Failed to unmarshal hyperloglog error: %v data: %v", err, string(ssj.RawHll))
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
		log.Errorf("SegStats.ToJSON: Failed to marshal hyperloglog error: %v", err)
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
		log.Errorf("SegStats.Merge: Failed to merge hyperloglog stats error: %v", err)
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
			qa.OutputTransforms.LetColumns.ValueColRequest != nil || qa.OutputTransforms.LetColumns.SortColRequest != nil || qa.OutputTransforms.LetColumns.MultiValueColRequest != nil)
}

// To determine whether it contains certain specific AggregatorBlocks, such as: Rename Block, Rex Block, FilterRows, MaxRows...
func (qa *QueryAggregators) HasQueryAggergatorBlock() bool {
	return qa != nil && qa.OutputTransforms != nil && (qa.hasLetColumnsRequest() || qa.OutputTransforms.FilterRows != nil || qa.OutputTransforms.MaxRows > qa.OutputTransforms.RowsAdded)
}

func (qa *QueryAggregators) HasQueryAggergatorBlockInChain() bool {
	if qa == nil {
		return false
	}

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
	if qa == nil {
		return false
	}

	if qa.HasDedupBlock() {
		return true
	}
	if qa.Next != nil {
		return qa.Next.HasDedupBlockInChain()
	}
	return false
}

func (qa *QueryAggregators) GetSortLimit() uint64 {
	if qa.HasSortBlock() {
		return qa.OutputTransforms.LetColumns.SortColRequest.Limit
	}
	if qa.Next != nil {
		return qa.Next.GetSortLimit()
	}
	return math.MaxUint64
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
	if qa == nil {
		return false
	}
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
	if qa == nil {
		return false
	}

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
	if qa == nil {
		return false
	}
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

var unsupportedStatsFuncs = map[utils.AggregateFunctions]struct{}{
	utils.Estdc:        {},
	utils.EstdcError:   {},
	utils.ExactPerc:    {},
	utils.Perc:         {},
	utils.UpperPerc:    {},
	utils.Median:       {},
	utils.Mode:         {},
	utils.Stdev:        {},
	utils.Stdevp:       {},
	utils.Sumsq:        {},
	utils.Var:          {},
	utils.Varp:         {},
	utils.First:        {},
	utils.Last:         {},
	utils.Earliest:     {},
	utils.EarliestTime: {},
	utils.Latest:       {},
	utils.LatestTime:   {},
	utils.StatsRate:    {},
}

var unsupportedEvalFuncs = map[string]struct{}{
	"mvappend":         {},
	"mvcount":          {},
	"mvdedup":          {},
	"mvfilter":         {},
	"mvfind":           {},
	"mvindex":          {},
	"mvjoin":           {},
	"mvmap":            {},
	"mvrange":          {},
	"mvsort":           {},
	"mvzip":            {},
	"mv_to_json_array": {},
	"floor":            {},
	"ln":               {},
	"log":              {},
	"sigfig":           {},
	"pow":              {},
	"searchmatch":      {},
	"validate":         {},
	"nullif":           {},
	"ipmask":           {},
	"object_to_array":  {},
	"printf":           {},
	"tojson":           {},
	"cluster":          {},
	"getfields":        {},
	"isnotnull":        {},
	"isnum":            {},
	"typeof":           {},
	"replace":          {},
	"spath":            {},
	"upper":            {},
	"trim":             {},
}

type StatsFuncChecker struct{}

func (c StatsFuncChecker) IsUnsupported(funcName utils.AggregateFunctions) bool {
	_, found := unsupportedStatsFuncs[funcName]
	return found
}

type EvalFuncChecker struct{}

func (c EvalFuncChecker) IsUnsupported(funcName string) bool {
	_, found := unsupportedEvalFuncs[funcName]
	return found
}

func CheckUnsupportedFunctions(post *QueryAggregators) error {

	statsChecker := StatsFuncChecker{}
	evalChecker := EvalFuncChecker{}

	for agg := post; agg != nil; agg = agg.Next {
		if agg == nil {
			return nil
		}

		// Check if user has used the stats options
		if agg.StatsOptions != nil {
			if agg.StatsOptions.Delim != " " || agg.StatsOptions.Partitions != 1 || agg.StatsOptions.DedupSplitvals || agg.StatsOptions.Allnum {
				return fmt.Errorf("checkUnsupportedFunctions: using options in stats cmd is not yet supported")
			}
		}

		if agg.GroupByRequest != nil {
			for _, measureAgg := range agg.GroupByRequest.MeasureOperations {
				if statsChecker.IsUnsupported(measureAgg.MeasureFunc) {
					return fmt.Errorf("checkUnsupportedFunctions: using %v in stats cmd is not yet supported", measureAgg.MeasureFunc.String())
				}
			}
		}

		for _, measureAgg := range agg.MeasureOperations {
			if statsChecker.IsUnsupported(measureAgg.MeasureFunc) {
				return fmt.Errorf("checkUnsupportedFunctions: using %v in stats cmd is not yet supported", measureAgg.MeasureFunc.String())
			}
		}

		if agg.hasLetColumnsRequest() && agg.OutputTransforms.LetColumns.ValueColRequest != nil {
			valueCol := agg.OutputTransforms.LetColumns.ValueColRequest
			if valueCol.StringExpr != nil && valueCol.StringExpr.TextExpr != nil {
				if evalChecker.IsUnsupported(valueCol.StringExpr.TextExpr.Op) {
					return fmt.Errorf("checkUnsupportedFunctions: using %v in eval cmd is not yet supported", valueCol.StringExpr.TextExpr.Op)
				}
			}
			if valueCol.NumericExpr != nil {
				if evalChecker.IsUnsupported(valueCol.NumericExpr.Op) {
					return fmt.Errorf("checkUnsupportedFunctions: using %v in eval cmd is not yet supported", valueCol.NumericExpr.Op)
				}
			}
			if valueCol.ConditionExpr != nil {
				if evalChecker.IsUnsupported(valueCol.ConditionExpr.Op) {
					return fmt.Errorf("checkUnsupportedFunctions: using %v in eval cmd is not yet supported", valueCol.ConditionExpr.Op)
				}
			}
		}
	}

	return nil
}

// GetBucketValueForGivenField returns the value of the bucket for the given field name.
// It checks for the field in the statistic results and group by keys.
// If the field is not found, it returns nil.
// The first return value is the value of the field.
// The second return value is the index of the field in the group by keys.
// The third return value is a boolean indicating if the field was found in the Statistic results.
// For Statistic results, the index is -1. and Bool is true.
// For GroupBy keys, the index is the index of the field in the group by keys; given that the Bucket key is a List Type. and Bool is false.
// Otherwise, the index is -1 and Bool is false.
func (br *BucketResult) GetBucketValueForGivenField(fieldName string) (interface{}, int, bool) {

	if value, ok := br.StatRes[fieldName]; ok {
		return value, -1, true
	}

	index := -1

	for i, key := range br.GroupByKeys {
		if key == fieldName {
			index = i
			break
		}
	}

	if index == -1 {
		return nil, -1, false
	}

	isListType, bucketKeyReflectVal, _ := sutils.IsArrayOrSlice(br.BucketKey)

	if !isListType {
		if index == 0 {
			return br.BucketKey, -1, false
		} else {
			return nil, -1, false
		}
	}

	if index >= bucketKeyReflectVal.Len() {
		return nil, -1, false
	}

	return bucketKeyReflectVal.Index(index).Interface(), index, false
}

// Can only be used for GroupBy keys.
// SetBucketValueForGivenField sets the value of the bucket for the given field name.
// The Value must be of type string or []string.
// If the value is of type []string, it is converted to a string.
func (br *BucketResult) SetBucketValueForGivenField(fieldName string, value interface{}, index int, isStatRes bool) error {

	if isStatRes {
		// Should not set the value, if the field is a Statistic result.
		return nil
	}

	// value can be either a string or a list of strings.
	if valueList, ok := value.([]string); ok {
		tempValList := make([]string, len(valueList))

		for i, val := range valueList {
			tempValList[i] = fmt.Sprintf(`"%s"`, val)
		}

		value = fmt.Sprintf("[ %v ]", strings.Join(tempValList, ", "))
	}

	if index == -1 {
		// Implies that the bucket key is not a list type.
		if fieldName == br.GroupByKeys[0] {
			br.BucketKey = value
			return nil
		} else {
			return fmt.Errorf("SetBucketValueForGivenField: Field %v not found in the bucket Group by keys", fieldName)
		}
	}

	if index >= len(br.GroupByKeys) {
		return fmt.Errorf("SetBucketValueForGivenField: Field %v not found in the bucket Group by keys", fieldName)
	}

	if fieldName != br.GroupByKeys[index] {
		return fmt.Errorf("SetBucketValueForGivenField: Field %v not found in the bucket Group by keys at index: %v", fieldName, index)
	}

	isListType, bucketKeyReflectVal, _ := sutils.IsArrayOrSlice(br.BucketKey)
	if !isListType {
		return fmt.Errorf("SetBucketValueForGivenField: Bucket key is not a list type")
	}

	if index >= bucketKeyReflectVal.Len() {
		return fmt.Errorf("SetBucketValueForGivenField: Field %v not found in the bucket key. Index: %v is greater than the bucket key Size: %v", fieldName, index, bucketKeyReflectVal.Len())
	}

	_, ok := br.BucketKey.([]string)
	if !ok {
		// Convert the bucket key to a list type.
		tempBucketKey := make([]string, len(br.GroupByKeys))
		for i := range br.GroupByKeys {
			tempBucketKey[i] = bucketKeyReflectVal.Index(i).Interface().(string)
		}
		br.BucketKey = tempBucketKey
	}

	bucketKeyList := br.BucketKey.([]string)
	bucketKeyList[index] = value.(string)

	return nil
}
