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
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash"
	"github.com/siglens/go-hll"
	"github.com/siglens/siglens/pkg/config"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type Numbers struct {
	Min_uint64  uint64              `json:"Min_uint64"`
	Max_uint64  uint64              `json:"Max_uint64"`
	Min_int64   int64               `json:"Min_int64"`
	Max_int64   int64               `json:"Max_int64"`
	Min_float64 float64             `json:"Min_float64"`
	Max_float64 float64             `json:"Max_float64"`
	NumType     sutils.RangeNumType `json:"NumType"`
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
	GenerateEventType
)

type QueryType uint8

const (
	InvalidCmd QueryType = iota
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

type StatsExpr struct {
	MeasureOperations []*MeasureAggregator
	GroupByRequest    *GroupByRequest
}

// Update this function: GetAllColsInAggsIfStatsPresent() to return all columns in the query aggregators if stats are present.
// This function should return all columns in the query aggregators if stats are present.
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
	StreamStatsOptions   *StreamStatsOptions
	GenerateEvent        *GenerateEvent
	Next                 *QueryAggregators
	Limit                int
	IndexName            string

	// TODO: eventually, we shouldn't need any of the above fields; then we can
	// delete them.
	BinExpr         *BinCmdOptions
	DedupExpr       *DedupExpr
	EvalExpr        *EvalExpr
	FieldsExpr      *ColumnsRequest
	FillNullExpr    *FillNullExpr
	GentimesExpr    *GenTimes
	InputLookupExpr *InputLookup
	HeadExpr        *HeadExpr
	MakeMVExpr      *MultiValueColLetRequest
	MVExpandExpr    *MultiValueColLetRequest
	RegexExpr       *RegexExpr
	RenameExp       *RenameExp
	RexExpr         *RexExpr
	SortExpr        *SortExpr
	StatsExpr       *StatsExpr
	StreamstatsExpr *StreamStatsOptions
	TailExpr        *TailExpr
	TimechartExpr   *TimechartExpr
	StatisticExpr   *StatisticExpr
	TransactionExpr *TransactionArguments
	WhereExpr       *BoolExpr
	ToJsonExpr      *ToJsonExpr
}

type GenerateEvent struct {
	GenTimes              *GenTimes
	InputLookup           *InputLookup
	GeneratedRecords      map[string]map[string]interface{}
	GeneratedRecordsIndex map[string]int
	GeneratedCols         map[string]bool
	GeneratedColsIndex    map[string]int
	EventPosition         int
}

type GenTimes struct {
	StartTime uint64
	EndTime   uint64
	Interval  *SpanLength
}

type InputLookup struct {
	IsFirstCommand       bool
	Filename             string
	Append               bool
	Start                uint64
	Strict               bool
	Max                  uint64
	WhereExpr            *BoolExpr
	HasPrevResults       bool
	NumProcessedSegments uint64
	UpdatedRecordIndex   bool
}

type StreamStatsOptions struct {
	AllNum        bool
	Current       bool
	Global        bool
	ResetOnChange bool
	Window        uint64
	ResetBefore   *BoolExpr
	ResetAfter    *BoolExpr
	TimeWindow    *BinSpanLength
	TimeSortAsc   bool
	// expensive for large data and window size
	// maps index of measureAgg -> bucket key -> RunningStreamStatsResults
	RunningStreamStats map[int]map[string]*RunningStreamStatsResults
	// contains segment records recordKey -> record
	SegmentRecords      map[string]map[string]interface{}
	NumProcessedRecords uint64

	MeasureOperations []*MeasureAggregator
	GroupByRequest    *GroupByRequest
}

type RunningStreamStatsResults struct {
	Window              *utils.GobbableList
	CurrResult          sutils.CValueEnclosure
	NumProcessedRecords uint64              // kept for global stats where window = 0
	SecondaryWindow     *utils.GobbableList // use secondary window for range
	RangeStat           *RangeStat
	CardinalityMap      map[string]int
	CardinalityHLL      *utils.GobbableHll
	PercTDigest         *utils.GobbableTDigest
	ValuesMap           map[string]struct{}
}

type RunningStreamStatsWindowElement struct {
	Index       int
	Value       sutils.CValueEnclosure
	TimeInMilli uint64
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
	TailRequest            *TailExpr
	HeadRequest            *HeadExpr
}

type HeadExpr struct {
	MaxRows              uint64
	Keeplast             bool
	Null                 bool
	RowsAdded            uint64 // number of rows added to the result. This is used in conjunction with MaxRows.
	BoolExpr             *BoolExpr
	SegmentRecords       map[string]map[string]interface{}
	ResultRecords        []map[string]interface{}
	ResultRecordKeys     []string
	Done                 bool
	NumProcessedSegments uint64
}

type GroupByRequest struct {
	MeasureOperations           []*MeasureAggregator
	GroupByColumns              []string
	AggName                     string // name of aggregation
	BucketCount                 int
	IsBucketKeySeparatedByDelim bool // if true, group by values= bucketKey.split(delimiter). This is used when the bucket key is already read in the correct format.
}

type MeasureAggregator struct {
	MeasureCol         string                    `json:"measureCol,omitempty"`
	MeasureFunc        sutils.AggregateFunctions `json:"measureFunc,omitempty"`
	StrEnc             string                    `json:"strEnc,omitempty"`
	ValueColRequest    *ValueExpr                `json:"valueColRequest,omitempty"`
	OverrodeMeasureAgg *MeasureAggregator        `json:"overrideFunc,omitempty"`
	Param              float64
}

type MathEvaluator struct {
	MathCol         string               `json:"mathCol,omitempty"`
	MathFunc        sutils.MathFunctions `json:"mathFunc,omitempty"`
	StrEnc          string               `json:"strEnc,omitempty"`
	ValueColRequest *ValueExpr           `json:"valueCol,omitempty"`
}

type ColumnsRequest struct {
	RenameColumns            map[string]string // maps original column name to new column name
	ExcludeColumns           []string          // names of columns to exclude
	IncludeColumns           []string          // names of columns to include
	IncludeValues            []*IncludeValue   // values of columns to include. Maps column name to index in column
	RenameAggregationColumns map[string]string // values of aggregations to rename
	Logfmt                   bool              // true if logfmt request
	Next                     *ColumnsRequest
}

type IncludeValue struct {
	Index   int    //index of value in original column
	ColName string //original column name
	Label   string //new label of value in record
}

type AppendRequest struct {
	ExtendTimeRange bool
	MaxTime         int
	MaxOut          int
	Subsearch       interface{}
}

type AppendCmdOptions struct {
	ExtendTimeRange bool
	MaxTime         int
	MaxOut          int
}

type AppendCmdOption struct {
	OptionType string
	Value      interface{}
}

type ToJsonExpr struct {
	FieldsDtypes    []*ToJsonFieldsDtypeOptions
	DefaultType     *ToJsonFieldsDtypeOptions
	FillNull        bool
	IncludeInternal bool
	OutputField     string
	AllFields       bool
}

type ToJsonFieldsDtypeOptions struct {
	Dtype ToJsonDtypes
	Regex *utils.GobbableRegex
}

type ToJsonDtypes uint8

const (
	TJ_None ToJsonDtypes = iota
	TJ_Auto
	TJ_Bool
	TJ_Json
	TJ_Num
	TJ_Str
	// Data type will be set later during processing
	TJ_PostProcess
)

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
	FormatResults        *FormatResultsRequest // formats the results into a single result and places that result into a new field called search.
	EventCountRequest    *EventCountExpr       // To count the number of events in an index
	BinRequest           *BinCmdOptions
	FillNullRequest      *FillNullExpr
	AppendRequest        *AppendRequest
}

type FillNullExpr struct {
	Value     string   // value to fill nulls with. Default 0
	FieldList []string // list of fields to fill nulls with

	// The following fields can be removed once we switch to the new query pipeline
	Records        map[string]map[string]interface{}
	FinalCols      map[string]bool
	ColumnsRequest *ColumnsRequest
}

type TailExpr struct {
	TailRecords          map[string]map[string]interface{}
	TailPQ               *utils.PriorityQueue
	TailRows             uint64
	NumProcessedSegments uint64
}

type EventCountExpr struct {
	Indices    []string
	Summarize  bool
	ReportSize bool
	ListVix    bool
}

// formats the results into a single result and places that result into a new field called search.
type FormatResultsRequest struct {
	MVSeparator   string         // separator for multi-value fields. Default= "OR"
	MaxResults    uint64         // max number of results to return
	EmptyString   string         // string to return if no results are found. Default= "NOT()"
	RowColOptions *RowColOptions // options for row column
}

type RowColOptions struct {
	RowPrefix       string // prefix for row. Default= "("
	ColumnPrefix    string // prefix for column. Default= "("
	ColumnSeparator string // separator for column. Default= "AND"
	ColumnEnd       string // end for column. Default= ")"
	RowSeparator    string // separator for row. Default= "OR"
	RowEnd          string // end for row. Default= ")"
}

type MultiColLetRequest struct {
	LeftCName  string
	Oper       sutils.LogicalAndArithmeticOperator
	RightCName string
}

type SingleColLetRequest struct {
	CName string
	Oper  sutils.LogicalAndArithmeticOperator
	Value *sutils.DtypeEnclosure
}

type MultiValueColLetRequest struct {
	Command         string // name of the command: makemv, mvcombine, mvexpand, etc.
	ColName         string
	DelimiterString string // delimiter string to split the column value. default is " " (single space)
	IsRegex         bool
	AllowEmpty      bool // if true, empty strings are allowed in the split values. default is false
	Setsv           bool // if true, split values are combined into a single value. default is false
	Limit           utils.Option[int64]
}

type BucketResult struct {
	ElemCount   uint64                            // total number of elements in bucket
	StatRes     map[string]sutils.CValueEnclosure // results of statistic functions
	BucketKey   interface{}                       // bucket key
	GroupByKeys []string
}

type AggregationResult struct {
	IsDateHistogram bool            // is this a date histogram
	Results         []*BucketResult // histogram results
}

// TODO: Retain either IGroupByValues or GroupByValues, as having both is unnecessary.
// The goal is to preserve the group-by value type as interface{} to avoid issues
// when processing subsequent commands.
// Ideally, we should update GroupByValues to have a type of []interface{}
// and eliminate IGroupByValues.
type BucketHolder struct {
	IGroupByValues []sutils.CValueEnclosure // each group-by value is stored as interface{}
	GroupByValues  []string
	MeasureVal     map[string]interface{}
}

type QueryCount struct {
	TotalCount uint64 // total number of
	Op         sutils.FilterOperator
	EarlyExit  bool // if early exit was requested or not
}

type Progress struct {
	RecordsSent     uint64
	UnitsSearched   uint64
	TotalUnits      uint64
	RecordsSearched uint64
	TotalRecords    uint64
}

type RecsAggregator struct {
	PerformAggsOnRecs bool            // if true, perform aggregations on records that are returned from rrcreader.go
	RecsAggsType      PipeCommandType // To determine Whether it is GroupByType or MeasureAggsType
	GroupByRequest    *GroupByRequest
	MeasureOperations []*MeasureAggregator
	NextQueryAgg      *QueryAggregators
}

type RecsAggResults struct {
	RecsAggsBlockResults      interface{} // Evaluates to *blockresults.BlockResults
	RecsAggsProcessedSegments uint64
	RecsRunningSegStats       []*SegStats
}

// A helper struct to keep track of errors and results together
// In cases of partial failures, both logLines and errList can be defined
type NodeResult struct {
	AllRecords                  []*sutils.RecordResultContainer
	ErrList                     []error                     // Need to eventually replace ErrList with GlobalSearchErrors to prevent duplicate errors
	SearchErrorsLock            sync.RWMutex                // lock for search errors
	GlobalSearchErrors          map[string]*SearchErrorInfo // maps global error from error message -> error info  (Deprecated: use toputils.BatchError)
	Histogram                   map[string]*AggregationResult
	TotalResults                *QueryCount
	VectorResultValue           float64
	RenameColumns               map[string]string
	SegEncToKey                 map[uint32]string
	TotalRRCCount               uint64
	MeasureFunctions            []string        `json:"measureFunctions,omitempty"`
	MeasureResults              []*BucketHolder `json:"measure,omitempty"`
	GroupByCols                 []string        `json:"groupByCols,omitempty"`
	Qtype                       string          `json:"qtype,omitempty"`
	BucketCount                 int             `json:"bucketCount,omitempty"`
	SegStatsMap                 map[string]*SegStats
	GroupByBuckets              interface{} // *blockresults.GroupByBuckets
	TimeBuckets                 interface{} // *blockresults.TimeBuckets
	RecsAggregator              RecsAggregator
	RecsAggResults              RecsAggResults
	RecsAggsColumnKeysMap       map[string][]interface{} // map of column name to column keys for GroupBy Recs
	TransactionEventRecords     map[string]map[string]interface{}
	TransactionsProcessed       map[string]map[string]interface{}
	ColumnsOrder                map[string]int
	RawSearchFinished           bool
	CurrentSearchResultCount    int
	AllSearchColumnsByTimeRange map[string]bool
	FinalColumns                map[string]bool
	AllColumnsInAggs            map[string]struct{}
	RemoteLogs                  []map[string]interface{}
	QueryStartTime              time.Time // time when the query execution started. Can be removed once we switch to the new query pipeline
}

type SegStats struct {
	IsNumeric   bool
	Count       uint64
	Min         sutils.CValueEnclosure
	Max         sutils.CValueEnclosure
	Hll         *utils.GobbableHll
	NumStats    *NumericStats
	StringStats *StringStats
	TimeStats   *TimeStats
	Records     []*sutils.CValueEnclosure
	TDigest     *utils.GobbableTDigest
}

type NumericStats struct {
	NumericCount uint64                  `json:"numericCount,omitempty"`
	Sum          sutils.NumTypeEnclosure `json:"sum,omitempty"`
	Sumsq        float64                 `json:"sumsq,omitempty"` // sum of squares, use float64 since we expect large values
}

type TimeStats struct {
	LatestTs    sutils.CValueEnclosure
	EarliestTs  sutils.CValueEnclosure
	LatestVal   sutils.CValueEnclosure
	EarliestVal sutils.CValueEnclosure
}

type StringStats struct {
	StrSet  map[string]struct{}
	StrList []string
}

type SearchErrorInfo struct {
	Count    uint64
	LogLevel log.Level
	Error    error
}

// json exportable struct for segstats
type SegStatsJSON struct {
	IsNumeric   bool
	Count       uint64
	Min         interface{}
	Max         interface{}
	RawHll      []byte
	NumStats    *NumericStats
	StringStats *StringStats
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

type VarStat struct {
	Count int64
	Sum   float64
	Sumsq float64
}

type FieldGetter interface {
	GetFields() []string
}

var HllSettings = hll.Settings{
	Log2m:             16,
	Regwidth:          5,
	ExplicitThreshold: hll.AutoExplicitThreshold,
	SparseEnabled:     true,
}

func init() {
	initHllDefaultSettings()
}

var nonIngestStats = map[sutils.AggregateFunctions]string{
	sutils.LatestTime:   "",
	sutils.EarliestTime: "",
	sutils.Latest:       "",
	sutils.Earliest:     "",
	sutils.Perc:         "",
	sutils.Sumsq:        "",
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
	err = ss.CreateHllFromBytes(segStatJson.RawHll)
	if err != nil {
		log.Errorf("SegStats.Init: Failed to create new segmentio Hll from raw bytes. error: %v data: %v", err, string(segStatJson.RawHll))
		return err
	}
	ss.NumStats = segStatJson.NumStats
	return nil
}

func initHllDefaultSettings() {
	err := hll.Defaults(HllSettings)
	if err != nil {
		log.Errorf("initHllDefaultSettings: Failed to set default hll settings. error: %v", err)
	}
}

// Creates a new segmentio Hll with the defined HllSettings.
func CreateNewHll() *utils.GobbableHll {
	return &utils.GobbableHll{Hll: hll.Hll{}}
}

func CreateHllFromBytes(rawHll []byte) (*hll.Hll, error) {
	hll, err := hll.FromBytes(rawHll)
	if err != nil {
		return nil, err
	}
	return &hll, nil
}

func (ss *SegStats) CreateHllFromBytes(rawHll []byte) error {
	hll, err := CreateHllFromBytes(rawHll)
	if err != nil {
		return err
	}

	ss.Hll = &utils.GobbableHll{Hll: *hll}
	return nil
}

func (ss *SegStats) CreateNewHll() {
	ss.Hll = CreateNewHll()
}

func (ss *SegStats) InsertIntoHll(value []byte) {
	if ss == nil || ss.Hll == nil {
		return
	}

	ss.Hll.AddRaw(xxhash.Sum64(value))
}

func (ss *SegStats) GetHllCardinality() uint64 {
	if ss == nil || ss.Hll == nil {
		return 0
	}

	return ss.Hll.Cardinality()
}

func (ss *SegStats) GetHllError() float64 {
	if ss == nil || ss.Hll == nil {
		return 0
	}

	return ss.Hll.RelativeError()
}

func (ss *SegStats) GetHllBytes() []byte {
	if ss == nil || ss.Hll == nil {
		return nil
	}

	return ss.Hll.ToBytes()
}

func (ss *SegStats) GetHllBytesInPlace(bytes []byte) []byte {
	if ss == nil || ss.Hll == nil {
		return nil
	}

	return ss.Hll.ToBytesInPlace(bytes)
}

func (ss *SegStats) GetHllDataSize() int {
	if ss == nil || ss.Hll == nil {
		return 0
	}

	_, size := ss.Hll.GetStorageTypeAndSizeInBytes()
	return size
}

func (ssj *SegStatsJSON) ToStats() (*SegStats, error) {
	ss := &SegStats{}
	ss.IsNumeric = ssj.IsNumeric
	ss.Count = ssj.Count
	if ssj.RawHll != nil {
		err := ss.CreateHllFromBytes(ssj.RawHll)
		if err != nil {
			log.Errorf("SegStatsJSON.ToStats: Failed to unmarshal hll error: %v data: %v", err, string(ssj.RawHll))
			return nil, err
		}
	}
	minVal := sutils.CValueEnclosure{}
	err := minVal.ConvertValue(ssj.Min)
	if err != nil {
		log.Errorf("SegStatsJSON.ToStats: Failed to convert min value. error: %v data: %v", err, ssj.Min)
	}
	maxVal := sutils.CValueEnclosure{}
	err = maxVal.ConvertValue(ssj.Max)
	if err != nil {
		log.Errorf("SegStatsJSON.ToStats: Failed to convert max value. error: %v data: %v", err, ssj.Max)
	}
	ss.Min = minVal
	ss.Max = maxVal
	ss.NumStats = ssj.NumStats
	ss.StringStats = ssj.StringStats
	return ss, nil
}

// convert SegStats to SegStatsJSON
func (ss *SegStats) ToJSON() (*SegStatsJSON, error) {
	segStatJson := &SegStatsJSON{}
	segStatJson.IsNumeric = ss.IsNumeric
	segStatJson.Count = ss.Count
	rawHll := ss.GetHllBytes()
	segStatJson.RawHll = rawHll
	segStatJson.NumStats = ss.NumStats
	segStatJson.StringStats = ss.StringStats
	segStatJson.Min = ss.Min.CVal
	segStatJson.Max = ss.Max.CVal
	return segStatJson, nil
}

func (ma *MeasureAggregator) String() string {
	if ma.StrEnc != "" {
		return ma.StrEnc
	}
	ma.StrEnc = fmt.Sprintf("%+v(%v)", ma.MeasureFunc.String(), ma.MeasureCol)
	return ma.StrEnc
}

func GetMeasureAggregatorStrEncColumns(measureAggs []*MeasureAggregator) []string {
	var columns []string
	for _, ma := range measureAggs {
		columns = append(columns, ma.String())
	}
	return columns
}

func UpdateMinMax(stats *SegStats, value sutils.CValueEnclosure) {
	minVal, err := sutils.ReduceMinMax(stats.Min, value, true)
	if err != nil {
		log.Errorf("UpdateMinMax: Error while reducing min: %v", err)
	} else {
		stats.Min = minVal
	}
	maxVal, err := sutils.ReduceMinMax(stats.Max, value, false)
	if err != nil {
		log.Errorf("UpdateMinMax: Error while reducing max: %v", err)
	} else {
		stats.Max = maxVal
	}
}

func (ss *SegStats) Merge(other *SegStats) {
	ss.Count += other.Count
	ss.Records = append(ss.Records, other.Records...)
	if ss.Hll != nil && other.Hll != nil {
		err := ss.Hll.StrictUnion(other.Hll.Hll)
		if err != nil {
			log.Errorf("SegStats.Merge: Failed to merge segmentio hll stats. error: %v", err)
		}
	}
	if ss.TDigest != nil && other.TDigest != nil {
		err := ss.TDigest.MergeTDigest(other.TDigest)
		if err != nil {
			log.Errorf("SegStats.Merge: Failed to merge segment tdigest stats. error: %v", err)
		}
	}

	UpdateMinMax(ss, other.Min)
	UpdateMinMax(ss, other.Max)

	if ss.NumStats == nil {
		ss.NumStats = other.NumStats
	} else {
		ss.NumStats.Merge(other.NumStats)
	}
	if ss.StringStats == nil {
		ss.StringStats = other.StringStats
	} else {
		ss.StringStats.Merge(other.StringStats)
	}
	if ss.TimeStats == nil {
		ss.TimeStats = other.TimeStats
	} else {
		ss.TimeStats.Merge(other.TimeStats)
	}
}

func (ss *StringStats) Merge(other *StringStats) {
	if other == nil {
		return
	}
	if ss.StrSet != nil {
		for key, value := range other.StrSet {
			ss.StrSet[key] = value
		}
	} else if other.StrSet != nil {
		ss.StrSet = make(map[string]struct{})
		for key, value := range other.StrSet {
			ss.StrSet[key] = value
		}
	}

	if ss.StrList != nil {
		ss.StrList = append(ss.StrList, other.StrList...)
	} else if other.StrList != nil {
		if len(other.StrList) > sutils.MAX_SPL_LIST_SIZE {
			ss.StrList = make([]string, sutils.MAX_SPL_LIST_SIZE)
			copy(ss.StrList, other.StrList[:sutils.MAX_SPL_LIST_SIZE])
		} else {
			ss.StrList = make([]string, len(other.StrList))
			copy(ss.StrList, other.StrList)
		}
	}
}

func (ss *TimeStats) Merge(other *TimeStats) {
	if other == nil {
		return
	}
	if ss.LatestTs.Dtype == sutils.SS_DT_UNSIGNED_NUM && other.LatestTs.Dtype == sutils.SS_DT_UNSIGNED_NUM {
		if ss.LatestTs.CVal.(uint64) < other.LatestTs.CVal.(uint64) {
			ss.LatestTs = other.LatestTs
			ss.LatestVal = other.LatestVal
		}
	}
	if ss.EarliestTs.Dtype == sutils.SS_DT_UNSIGNED_NUM && other.EarliestTs.Dtype == sutils.SS_DT_UNSIGNED_NUM {
		if ss.EarliestTs.CVal.(uint64) > other.EarliestTs.CVal.(uint64) {
			ss.EarliestTs = other.EarliestTs
			ss.EarliestVal = other.EarliestVal
		}
	}
}

func (ss *NumericStats) Merge(other *NumericStats) {
	if other == nil {
		return
	}

	ss.NumericCount += other.NumericCount
	switch ss.Sum.Ntype {
	case sutils.SS_DT_FLOAT:
		if other.Sum.Ntype == sutils.SS_DT_FLOAT {
			ss.Sum.FloatVal = ss.Sum.FloatVal + other.Sum.FloatVal
		} else {
			ss.Sum.FloatVal = ss.Sum.FloatVal + float64(other.Sum.IntgrVal)
		}
	default:
		if other.Sum.Ntype == sutils.SS_DT_FLOAT {
			ss.Sum.FloatVal = float64(ss.Sum.IntgrVal) + other.Sum.FloatVal
			ss.Sum.Ntype = sutils.SS_DT_FLOAT
		} else {
			ss.Sum.IntgrVal = ss.Sum.IntgrVal + other.Sum.IntgrVal
		}
	}
	ss.Sumsq = ss.Sumsq + other.Sumsq
}

func (nr *NodeResult) ApplyScroll(scroll int) {

	if scroll == 0 {
		return
	}

	if len(nr.AllRecords) <= scroll {
		nr.AllRecords = make([]*sutils.RecordResultContainer, 0)
		return
	}

	nr.AllRecords = nr.AllRecords[scroll:]
}

func (n *Numbers) Copy() *Numbers {

	retNum := &Numbers{
		NumType: n.NumType,
	}
	switch n.NumType {
	case sutils.RNT_UNSIGNED_INT:
		retNum.Min_uint64 = n.Min_uint64
		retNum.Max_uint64 = n.Max_uint64
	case sutils.RNT_SIGNED_INT:
		retNum.Min_int64 = n.Min_int64
		retNum.Max_int64 = n.Max_int64
	case sutils.RNT_FLOAT64:
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
			qa.OutputTransforms.LetColumns.ValueColRequest != nil || qa.OutputTransforms.LetColumns.SortColRequest != nil || qa.OutputTransforms.LetColumns.MultiValueColRequest != nil ||
			qa.OutputTransforms.LetColumns.FormatResults != nil || qa.OutputTransforms.LetColumns.EventCountRequest != nil || qa.OutputTransforms.LetColumns.BinRequest != nil ||
			qa.OutputTransforms.LetColumns.FillNullRequest != nil || qa.OutputTransforms.LetColumns.AppendRequest != nil)
}

func (qa *QueryAggregators) hasAppendRequest() bool {
	return qa != nil && qa.OutputTransforms != nil && qa.OutputTransforms.LetColumns != nil &&
		qa.OutputTransforms.LetColumns.AppendRequest != nil
}

func (qa *QueryAggregators) HasAppendInChain() bool {
	return qa.HasInChain((*QueryAggregators).hasAppendRequest)
}

func (qa *QueryAggregators) hasHeadBlock() bool {
	if qa == nil {
		return false
	}
	if qa.OutputTransforms == nil {
		return false
	}

	return qa.OutputTransforms.HeadRequest != nil
}

type queryAggregatorsBoolFunc func(_ *QueryAggregators) bool

func (qa *QueryAggregators) HasInChain(hasInCur queryAggregatorsBoolFunc) bool {
	if qa == nil {
		return false
	}

	if hasInCur(qa) {
		return true
	}
	if qa.Next != nil {
		return qa.Next.HasInChain(hasInCur)
	}
	return false

}

// To determine whether it contains certain specific AggregatorBlocks, such as: Rename Block, Rex Block, FilterRows, MaxRows...
func (qa *QueryAggregators) HasQueryAggergatorBlock() bool {
	if qa.HasStreamStatsInChain() || qa.HasGenerateEvent() {
		return true
	}
	return qa != nil && qa.OutputTransforms != nil && (qa.hasLetColumnsRequest() || qa.OutputTransforms.TailRequest != nil || qa.OutputTransforms.FilterRows != nil || qa.hasHeadBlock())
}

func (qa *QueryAggregators) HasQueryAggergatorBlockInChain() bool {
	return qa.HasInChain((*QueryAggregators).HasQueryAggergatorBlock)
}

func (qa *QueryAggregators) HasGenerateEvent() bool {
	if qa == nil {
		return false
	}

	return qa.GenerateEvent != nil
}

func (qa *QueryAggregators) HasGeneratedEventsWithoutSearch() bool {
	if qa == nil || qa.GenerateEvent == nil {
		return false
	}
	if qa.GenerateEvent.InputLookup != nil && qa.GenerateEvent.InputLookup.HasPrevResults {
		return false
	}
	return true
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
	return qa.HasInChain((*QueryAggregators).HasDedupBlock)
}

func (qa *QueryAggregators) GetSortLimit() uint64 {
	if qa.HasSortBlock() {
		return qa.OutputTransforms.LetColumns.SortColRequest.Limit
	}
	if qa != nil && qa.Next != nil {
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
	return qa.HasInChain((*QueryAggregators).HasSortBlock)
}

func (qa *QueryAggregators) HasTail() bool {
	if qa != nil && qa.OutputTransforms != nil && qa.OutputTransforms.TailRequest != nil {
		return true
	}

	return false
}

func (qa *QueryAggregators) HasTailInChain() bool {
	return qa.HasInChain((*QueryAggregators).HasTail)
}

func (qa *QueryAggregators) HasBinBlock() bool {
	if qa != nil && qa.OutputTransforms != nil && qa.OutputTransforms.LetColumns != nil && qa.OutputTransforms.LetColumns.BinRequest != nil {
		return true
	}

	return false
}

func (qa *QueryAggregators) HasBinInChain() bool {
	return qa.HasInChain((*QueryAggregators).HasBinBlock)
}

func (qa *QueryAggregators) HasStreamStats() bool {
	if qa != nil && qa.StreamStatsOptions != nil {
		return true
	}

	return false
}

func (qa *QueryAggregators) HasStreamStatsInChain() bool {
	return qa.HasInChain((*QueryAggregators).HasStreamStats)
}

func (qa *QueryAggregators) HasTransactionArguments() bool {
	return qa != nil && qa.TransactionArguments != nil
}

func (qa *QueryAggregators) HasTransactionArgumentsInChain() bool {
	return qa.HasInChain((*QueryAggregators).HasTransactionArguments)
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

// To determine whether to fetch all the columns by time range.
// Currently, it is only used in the case of FillNullExpr
func (qa *QueryAggregators) AllColumnsByTimeRangeIsRequired() bool {
	return qa != nil && qa.HasFillNullExprInChain()
}

func (qa *QueryAggregators) HasFillNullExpr() bool {
	if qa != nil && qa.OutputTransforms != nil && qa.OutputTransforms.LetColumns != nil && qa.OutputTransforms.LetColumns.FillNullRequest != nil {
		return true
	}
	return false
}

func (qa *QueryAggregators) HasFillNullExprInChain() bool {
	return qa.HasInChain((*QueryAggregators).HasFillNullExpr)
}

func (qa *QueryAggregators) AttachColumnsRequestToFillNullExprInChain(colRequest *ColumnsRequest) {
	if qa == nil {
		return
	}
	if qa.HasFillNullExpr() {
		fillNullColReq := qa.OutputTransforms.LetColumns.FillNullRequest.ColumnsRequest
		if fillNullColReq == nil {
			qa.OutputTransforms.LetColumns.FillNullRequest.ColumnsRequest = colRequest
		} else {
			for fillNullColReq.Next != nil {
				fillNullColReq = fillNullColReq.Next
			}
			fillNullColReq.Next = colRequest
		}
	}
	if qa.Next != nil {
		qa.Next.AttachColumnsRequestToFillNullExprInChain(colRequest)
	}
}

func (qa *QueryAggregators) HasColumnsRequest() bool {
	if qa != nil && qa.OutputTransforms != nil && qa.OutputTransforms.OutputColumns != nil {
		return true
	}
	return false
}

func (qa *QueryAggregators) CheckForColRequestAndAttachToFillNullExprInChain() {
	if qa == nil {
		return
	}
	if qa.HasColumnsRequest() {
		qa.AttachColumnsRequestToFillNullExprInChain(qa.OutputTransforms.OutputColumns)
	}
	if qa.Next != nil {
		qa.Next.CheckForColRequestAndAttachToFillNullExprInChain()
	}
}

// To determine whether it contains ValueColRequest
func (qa *QueryAggregators) HasValueColRequest() bool {
	if HasValueColRequestInMeasureAggs(qa.MeasureOperations) {
		return true
	}
	if qa.GroupByRequest != nil && HasValueColRequestInMeasureAggs(qa.GroupByRequest.MeasureOperations) {
		return true
	}
	return false
}

func HasValueColRequestInMeasureAggs(measureAggs []*MeasureAggregator) bool {
	for _, agg := range measureAggs {
		if agg.ValueColRequest != nil {
			return true
		}
	}
	return false
}

// To determine whether it contains Aggregate Func: Values()
func (qa *QueryAggregators) HasValuesFunc() bool {
	for _, agg := range qa.MeasureOperations {
		if agg.MeasureFunc == sutils.Values {
			return true
		}
	}
	return false
}

func (qa *QueryAggregators) HasListFunc() bool {
	for _, agg := range qa.MeasureOperations {
		if agg.MeasureFunc == sutils.List {
			return true
		}
	}
	return false
}

func (qa *QueryAggregators) HasNonIngestStats() bool {
	for _, agg := range qa.MeasureOperations {
		if _, ok := nonIngestStats[agg.MeasureFunc]; ok {
			return true
		}
	}
	return false
}

func (qa *QueryAggregators) UsedByTimechart() bool {
	return qa != nil && qa.TimeHistogram != nil && qa.TimeHistogram.Timechart != nil
}

func (qa *QueryAggregators) HasTimechartInChain() bool {
	return qa.HasInChain((*QueryAggregators).UsedByTimechart)
}

func (qa *QueryAggregators) CanLimitBuckets() bool {
	// We shouldn't limit the buckets if there's other things to do after the
	// aggregation, like sorting, filtering, making new columns, etc.
	return qa.Sort == nil && qa.Next == nil
}

func (fillNullExpr *FillNullExpr) GetFields() []string {
	return fillNullExpr.FieldList
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

func (qtype QueryType) IsRRCCmd() bool {
	return qtype == RRCCmd
}

func (qtype QueryType) IsGroupByCmd() bool {
	return qtype == GroupByCmd
}

func (qtype QueryType) IsSegmentStatsCmd() bool {
	return qtype == SegmentStatsCmd
}

func (qtype QueryType) IsInvalid() bool {
	return qtype == InvalidCmd
}

func (qtype QueryType) IsNotStatsType() bool {
	return qtype != SegmentStatsCmd && qtype != GroupByCmd
}

func (qa *QueryAggregators) HasTopExpr() bool {
	return qa != nil && qa.StatisticExpr != nil && qa.StatisticExpr.StatisticFunctionMode == SFMTop
}

func (qa *QueryAggregators) HasRareExpr() bool {
	return qa != nil && qa.StatisticExpr != nil && qa.StatisticExpr.StatisticFunctionMode == SFMRare
}

func (qa *QueryAggregators) HasStatsBlock() bool {
	return qa != nil && !qa.HasStreamStats() && qa.HasGroupByOrMeasureAggsInBlock()
}

func (qa *QueryAggregators) HasStatsBlockInChain() bool {
	return qa.HasInChain((*QueryAggregators).HasStatsBlock)
}

// returns all columns in the query aggregators if stats are present.
// Update this function whenever a new struct or a new Column Field is added to the query aggregators.
func (qa *QueryAggregators) GetAllColsInAggsIfStatsPresent() map[string]struct{} {
	if qa == nil {
		return nil
	}

	if !qa.HasStatsBlockInChain() {
		return nil
	}

	cols := make(map[string]struct{})

	qa.GetAllColsInChainUpToFirstStatsBlock(cols)

	return cols
}

func (qa *QueryAggregators) GetAllColsInChainUpToFirstStatsBlock(cols map[string]struct{}) {
	if qa == nil {
		return
	}

	AddAllColumnsInOutputTransforms(cols, qa.OutputTransforms)
	AddAllColumnsInMeasureAggs(cols, qa.MeasureOperations)
	AddAllColumnsInGroupByRequest(cols, qa.GroupByRequest)
	AddAllColumnsInTransactionArguments(cols, qa.TransactionArguments)
	AddAllColumnsInStreamStatsOptions(cols, qa.StreamStatsOptions)

	if qa.HasStatsBlock() {
		// We want to stop after processing the first stats block
		return
	}

	if qa.Next != nil {
		qa.Next.GetAllColsInChainUpToFirstStatsBlock(cols)
	}
}

func AddAllColumnsInOutputTransforms(cols map[string]struct{}, outputTransforms *OutputTransforms) {
	if outputTransforms == nil {
		return
	}

	if outputTransforms.HarcodedCol != nil {
		utils.AddSliceToSet(cols, outputTransforms.HarcodedCol)
	}

	if outputTransforms.RenameHardcodedColumns != nil {
		utils.AddMapKeysToSet(cols, outputTransforms.RenameHardcodedColumns)
	}

	AddAllColumnsInColumnsRequest(cols, outputTransforms.OutputColumns)
	AddAllColumnsInLetColumnsRequest(cols, outputTransforms.LetColumns)
	AddAllColumnsInExpr(cols, outputTransforms.FilterRows)

	if outputTransforms.HeadRequest != nil {
		AddAllColumnsInExpr(cols, outputTransforms.HeadRequest.BoolExpr)
	}
}

func AddAllColumnsInColumnsRequest(cols map[string]struct{}, columnsRequest *ColumnsRequest) {
	if columnsRequest == nil {
		return
	}

	if columnsRequest.RenameColumns != nil {
		utils.AddMapKeysToSet(cols, columnsRequest.RenameColumns)
	}

	if columnsRequest.ExcludeColumns != nil {
		utils.AddSliceToSet(cols, columnsRequest.ExcludeColumns)
	}

	if columnsRequest.IncludeColumns != nil {
		utils.AddSliceToSet(cols, columnsRequest.IncludeColumns)
	}

	if columnsRequest.IncludeValues != nil {
		for _, includeValue := range columnsRequest.IncludeValues {
			cols[includeValue.ColName] = struct{}{}
		}
	}

	if columnsRequest.Next != nil {
		AddAllColumnsInColumnsRequest(cols, columnsRequest.Next)
	}
}

func AddAllColumnsInLetColumnsRequest(cols map[string]struct{}, letColumnsRequest *LetColumnsRequest) {
	if letColumnsRequest == nil {
		return
	}

	if letColumnsRequest.MultiColsRequest != nil {
		cols[letColumnsRequest.MultiColsRequest.LeftCName] = struct{}{}
		cols[letColumnsRequest.MultiColsRequest.RightCName] = struct{}{}
	}

	if letColumnsRequest.SingleColRequest != nil {
		cols[letColumnsRequest.SingleColRequest.CName] = struct{}{}
	}

	AddAllColumnsInExpr(cols, letColumnsRequest.ValueColRequest)
	AddAllColumnsInExpr(cols, letColumnsRequest.RexColRequest)
	AddAllColumnsInExpr(cols, letColumnsRequest.StatisticColRequest)
	AddAllColumnsInExpr(cols, letColumnsRequest.RenameColRequest)
	AddAllColumnsInExpr(cols, letColumnsRequest.DedupColRequest)
	AddAllColumnsInExpr(cols, letColumnsRequest.SortColRequest)

	if letColumnsRequest.MultiValueColRequest != nil && letColumnsRequest.MultiValueColRequest.ColName != "" {
		cols[letColumnsRequest.MultiValueColRequest.ColName] = struct{}{}
	}

	if letColumnsRequest.BinRequest != nil {
		cols[letColumnsRequest.BinRequest.Field] = struct{}{}
	}

	AddAllColumnsInExpr(cols, letColumnsRequest.FillNullRequest)
}

func AddAllColumnsInExpr(cols map[string]struct{}, expr FieldGetter) {
	if expr == nil || reflect.ValueOf(expr).IsNil() {
		return
	}

	fields := expr.GetFields()

	utils.AddSliceToSet(cols, fields)
}

func AddAllColumnsInMeasureAggs(cols map[string]struct{}, measureAggs []*MeasureAggregator) {
	if measureAggs == nil {
		return
	}

	for _, measureAgg := range measureAggs {
		if measureAgg.MeasureCol != "" && measureAgg.MeasureCol != "*" {
			utils.AddToSet(cols, measureAgg.MeasureCol)
		}
		AddAllColumnsInExpr(cols, measureAgg.ValueColRequest)
	}
}

func AddAllColumnsInGroupByRequest(cols map[string]struct{}, groupByRequest *GroupByRequest) {
	if groupByRequest == nil {
		return
	}

	if groupByRequest.GroupByColumns != nil {
		utils.AddSliceToSet(cols, groupByRequest.GroupByColumns)
	}

	AddAllColumnsInMeasureAggs(cols, groupByRequest.MeasureOperations)
}

func AddAllColumnsInTransactionArguments(cols map[string]struct{}, transactionArguments *TransactionArguments) {
	if transactionArguments == nil {
		return
	}

	if transactionArguments.Fields != nil {
		utils.AddSliceToSet(cols, transactionArguments.Fields)
	}
}

func AddAllColumnsInStreamStatsOptions(cols map[string]struct{}, streamStatsOptions *StreamStatsOptions) {
	if streamStatsOptions == nil {
		return
	}

	AddAllColumnsInExpr(cols, streamStatsOptions.ResetBefore)
	AddAllColumnsInExpr(cols, streamStatsOptions.ResetAfter)
}

var unsupportedStatsFuncs = map[sutils.AggregateFunctions]struct{}{
	sutils.ExactPerc: {},
	sutils.UpperPerc: {},
	sutils.Median:    {},
	sutils.Mode:      {},
	sutils.Stdev:     {},
	sutils.Stdevp:    {},
	sutils.First:     {},
	sutils.Last:      {},
	sutils.StatsRate: {},
}

var unsupportedEvalFuncs = map[string]struct{}{
	"mvappend":         {},
	"mvdedup":          {},
	"mvfilter":         {},
	"mvmap":            {},
	"mvrange":          {},
	"mvsort":           {},
	"mvzip":            {},
	"mv_to_json_array": {},
	"object_to_array":  {},
	"tojson":           {},
	"cluster":          {},
	"getfields":        {},
	"isnum":            {},
	"isnotnull":        {},
	"eventcount":       {},
}

type StatsFuncChecker struct{}

func (c StatsFuncChecker) IsUnsupported(funcName sutils.AggregateFunctions) bool {
	_, found := unsupportedStatsFuncs[funcName]
	return found
}

type EvalFuncChecker struct{}

func (c EvalFuncChecker) IsUnsupported(funcName string) bool {
	_, found := unsupportedEvalFuncs[funcName]
	return found
}

var unsupportedLetColumnCommands = []string{"FormatResults", "EventCountRequest"}

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

		err := checkUnsupportedLetColumnCommand(agg)
		if err != nil {
			return err
		}
	}

	return nil
}
func checkUnsupportedLetColumnCommand(agg *QueryAggregators) error {
	if agg.hasLetColumnsRequest() {
		letColumns := agg.OutputTransforms.LetColumns
		for _, command := range unsupportedLetColumnCommands {
			switch command {
			case "FormatResults":
				if letColumns.FormatResults != nil {
					return fmt.Errorf("checkUnsupportedFunctions: using format command is not yet supported")
				}
			case "EventCountRequest":
				if letColumns.EventCountRequest != nil {
					return fmt.Errorf("checkUnsupportedFunctions: using eventcount command is not yet supported")
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
		return value.CVal, -1, true
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

	isListType, bucketKeyReflectVal, _ := utils.IsArrayOrSlice(br.BucketKey)

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
// If the BucketKey is a Array or Slice type, then it sets the value at the given index.
// And will also convert the BucketKey to a []interface{} type if it is not already.
func (br *BucketResult) SetBucketValueForGivenField(fieldName string, value interface{}, index int, isStatRes bool) error {

	if isStatRes {
		dVal, err := sutils.CreateDtypeEnclosure(value, 0)
		if err != nil {
			return fmt.Errorf("SetBucketValueForGivenField: Failed to create dtype enclosure for value: %v", value)
		}
		br.StatRes[fieldName] = sutils.CValueEnclosure{Dtype: dVal.Dtype, CVal: value}
		return nil
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

	isListType, bucketKeyReflectVal, _ := utils.IsArrayOrSlice(br.BucketKey)
	if !isListType {
		return fmt.Errorf("SetBucketValueForGivenField: Bucket key is not a list type")
	}

	if index >= bucketKeyReflectVal.Len() {
		return fmt.Errorf("SetBucketValueForGivenField: Field %v not found in the bucket key. Index: %v is greater than the bucket key Size: %v", fieldName, index, bucketKeyReflectVal.Len())
	}

	_, ok := br.BucketKey.([]interface{})
	if !ok {
		// Convert the bucket key to a list type.
		tempBucketKey := make([]interface{}, len(br.GroupByKeys))
		for i := range br.GroupByKeys {
			tempBucketKey[i] = bucketKeyReflectVal.Index(i).Interface()
		}
		br.BucketKey = tempBucketKey
	}

	bucketKeyList := br.BucketKey.([]interface{})
	bucketKeyList[index] = value

	return nil
}

func (qa *QueryAggregators) IsStatsAggPresentInChain() bool {
	statsAggPresentInCur := func(obj *QueryAggregators) bool {
		return obj.GroupByRequest != nil || obj.MeasureOperations != nil
	}
	return qa.HasInChain(statsAggPresentInCur)
}

func (qa *QueryAggregators) GetAllMeasureAggsInChain() [][]*MeasureAggregator {
	measureAggGroups := make([][]*MeasureAggregator, 0)
	getCur := func(obj *QueryAggregators) []*MeasureAggregator {
		if obj.MeasureOperations != nil {
			return obj.MeasureOperations
		} else if obj.GroupByRequest != nil && obj.GroupByRequest.MeasureOperations != nil {
			return obj.GroupByRequest.MeasureOperations
		}

		return nil
	}

	for {
		measureAggs := getCur(qa)
		if measureAggs != nil {
			measureAggGroups = append(measureAggGroups, measureAggs)
		}

		if qa.Next == nil {
			break
		}

		qa = qa.Next
	}

	return measureAggGroups
}

// TODO: use toputils.BatchError instead
func (nodeRes *NodeResult) StoreGlobalSearchError(errMsg string, logLevel log.Level, err error) {
	nodeRes.SearchErrorsLock.Lock()
	defer nodeRes.SearchErrorsLock.Unlock()

	nodeRes.GlobalSearchErrors = StoreError(nodeRes.GlobalSearchErrors, errMsg, logLevel, err)
}

func StoreError(errorStore map[string]*SearchErrorInfo, errMsg string, logLevel log.Level, err error) map[string]*SearchErrorInfo {
	if errorStore == nil {
		errorStore = make(map[string]*SearchErrorInfo)
	}

	if globalErr, ok := errorStore[errMsg]; !ok {
		errorStore[errMsg] = &SearchErrorInfo{Count: 1, LogLevel: logLevel, Error: err}
	} else {
		atomic.AddUint64(&globalErr.Count, 1)
	}

	return errorStore
}
