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
	"bytes"
	"fmt"
	"regexp"
	"strings"

	"strconv"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/pqmr"
	"github.com/siglens/siglens/pkg/segment/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	log "github.com/sirupsen/logrus"
)

// New struct for passin query params
type QueryContext struct {
	TableInfo *TableInfo
	SizeLimit uint64
	Scroll    int
	Orgid     uint64
}

// Input for filter operator can either be the result of a ASTNode or an expression
// either subtree or expression is defined, but never both and never neither
type FilterInput struct {
	SubTree       *ASTNode    // root of ASTNode
	SubtreeResult string      // result of processing subtree
	Expression    *Expression // expression for filterInput
}

type NodeAggregation struct {
	AggregationFunctions utils.AggregateFunctions // function to apply on results of children (e.g. min, max)
	AggregationColumns   []string                 // column names to aggregate on (i.e avg over which column name?)
}

type MatchFilterType uint8

const (
	MATCH_WORDS MatchFilterType = iota + 1
	MATCH_PHRASE
	MATCH_DICT_ARRAY
)

// MatchFilter searches for all words in matchWords in the column matchColumn
// The matchOperator defines if all or any of the matchWords need to be present
type MatchFilter struct {
	MatchColumn    string                 // column to search for
	MatchWords     [][]byte               // all words to search for
	MatchOperator  utils.LogicalOperator  // how to combine matchWords
	MatchPhrase    []byte                 //whole string to search for in case of MatchPhrase query
	MatchDictArray *MatchDictArrayRequest //array to search for in case of jaeger query
	MatchType      MatchFilterType
	NegateMatch    bool
	RegexpString   string // Do not manually set this. Use SetRegexp(). This is only public to allow for GOB encoding MatchFilter.
	regexp         *regexp.Regexp
}

type MatchDictArrayRequest struct {
	MatchKey   []byte
	MatchValue *utils.DtypeEnclosure
}

// ExpressionFilter denotes a single expression to search for in a log record
type ExpressionFilter struct {
	LeftInput      *FilterInput         // left input to filterOperator
	FilterOperator utils.FilterOperator // how to logField in logline (i.e logField=filterString, logField >= filterValue)
	RightInput     *FilterInput         // right input to filterOperator
}

// Top level filter criteria condition that define either a MatchFilter or ExpressionFilter. Only one will be defined, never both
type FilterCriteria struct {
	MatchFilter      *MatchFilter      // match filter to check multiple words in a column
	ExpressionFilter *ExpressionFilter // expression filter to check a single expression in a column
}

// A condition struct defines the FilterConditions and ASTNodes that exist as a part of a single condition
type Condition struct {
	FilterCriteria []*FilterCriteria // raw conditions to check
	NestedNodes    []*ASTNode        // nested conditions to check
}

// Node used to query data in a segment file.
// A line matches a node if it matches all conditions in AndFilterConditions, any in OrFilterCriteria, and none in ExclusionFilterCriteria
type ASTNode struct {
	AndFilterCondition       *Condition     // A condition to query. Condition must return true for log line to pass
	OrFilterCondition        *Condition     // Condition must return true for log line to pass
	ExclusionFilterCondition *Condition     // Condition must return false for log line to pass
	TimeRange                *dtu.TimeRange // Time range for node micro index / raw search
	ActiveFileSearch         bool           // Lookup unrotated segfiles
	BucketLimit              int
}

// Helper struct to keep track of raw and expanded tables
type TableInfo struct {
	rawRequest   string
	queryTables  []string
	kibanaTables []string
	numIndices   int
}

// Helper struct to keep track of which blocks to check
type BlockTracker struct {
	entireFile    bool
	excludeBlocks map[uint16]bool
}

func InitTableInfo(rawRequest string, orgid uint64, es bool) *TableInfo {
	indexNamesRetrieved := vtable.ExpandAndReturnIndexNames(rawRequest, orgid, es)
	ti := &TableInfo{rawRequest: rawRequest}
	if es {
		nonKibana, kibana := filterKibanaIndices(indexNamesRetrieved)
		ti.kibanaTables = kibana
		ti.queryTables = nonKibana
	} else {
		ti.queryTables = indexNamesRetrieved
	}
	ti.numIndices = len(indexNamesRetrieved)
	return ti
}

func (ti *TableInfo) GetRawRequest() string {
	return ti.rawRequest
}

// returns nonKibanaIndices, kibanaIndices
func filterKibanaIndices(indexNames []string) ([]string, []string) {
	kibanaIndices := make([]string, 0)
	nonKibanaIndices := make([]string, 0)
	for _, iName := range indexNames {
		if strings.Contains(iName, ".kibana") {
			kibanaIndices = append(kibanaIndices, iName)
		} else {
			nonKibanaIndices = append(nonKibanaIndices, iName)
		}
	}
	return nonKibanaIndices, kibanaIndices
}

func (ti *TableInfo) String() string {
	var buffer bytes.Buffer
	buffer.WriteString("Raw Index: [")
	buffer.WriteString(ti.rawRequest)
	buffer.WriteString("] Expanded To ")
	buffer.WriteString(strconv.FormatInt(int64(len(ti.queryTables)), 10))
	buffer.WriteString(" Entries. There are: ")
	buffer.WriteString(strconv.FormatInt(int64(len(ti.kibanaTables)), 10))
	buffer.WriteString(" Elastic Indices. Sample: ")
	buffer.WriteString(getIndexNamesCleanLogs(ti.queryTables))
	return buffer.String()
}

func (ti *TableInfo) GetQueryTables() []string {
	if ti == nil {
		return make([]string, 0)
	}
	return ti.queryTables
}

func (ti *TableInfo) GetKibanaIndices() []string {
	if ti == nil {
		return make([]string, 0)
	}
	return ti.kibanaTables
}

func (ti *TableInfo) GetNumIndices() int {
	if ti == nil {
		return 0
	}
	return ti.numIndices
}

// gets the number of tables that will be queried
func (qc *QueryContext) GetNumTables() int {
	if qc.TableInfo == nil {
		return 0
	}
	return qc.TableInfo.GetNumIndices()
}

func getIndexNamesCleanLogs(indices []string) string {
	var indicesStr string
	if len(indices) > 4 {
		indicesStr = fmt.Sprintf("%v%s", indices[:4], ".....")
	} else {
		indicesStr = fmt.Sprintf("%v", indices)
	}
	return indicesStr
}

func InitQueryContext(indexRequest string, sizeLimit uint64, scroll int, orgid uint64, es bool) *QueryContext {
	ti := InitTableInfo(indexRequest, orgid, es)
	return &QueryContext{
		TableInfo: ti,
		SizeLimit: sizeLimit,
		Scroll:    scroll,
		Orgid:     orgid,
	}
}

func InitQueryContextWithTableInfo(ti *TableInfo, sizeLimit uint64, scroll int, orgid uint64, es bool) *QueryContext {
	return &QueryContext{
		TableInfo: ti,
		SizeLimit: sizeLimit,
		Scroll:    scroll,
		Orgid:     orgid,
	}
}

func InitEntireFileBlockTracker() *BlockTracker {
	return &BlockTracker{entireFile: true}
}

func InitExclusionBlockTracker(spqmr *pqmr.SegmentPQMRResults) *BlockTracker {
	exclude := make(map[uint16]bool)
	for _, blkNum := range spqmr.GetAllBlocks() {
		exclude[blkNum] = true
	}
	return &BlockTracker{entireFile: false, excludeBlocks: exclude}
}

func (bt *BlockTracker) ShouldProcessBlock(blkNum uint16) bool {
	if bt.entireFile {
		return true
	}
	_, ok := bt.excludeBlocks[blkNum]
	if !ok {
		return true
	} else {
		return false
	}
}

func (c *Condition) JoinCondition(add *Condition) {
	if add == nil {
		return
	}

	if add.FilterCriteria != nil && len(add.FilterCriteria) > 0 {
		if c.FilterCriteria == nil {
			c.FilterCriteria = add.FilterCriteria
		} else {
			c.FilterCriteria = append(c.FilterCriteria, add.FilterCriteria...)
		}
	}

	if add.NestedNodes != nil && len(add.NestedNodes) > 0 {
		if c.NestedNodes == nil {
			c.NestedNodes = add.NestedNodes
		} else {
			c.NestedNodes = append(c.NestedNodes, add.NestedNodes...)
		}
	}
}

func (m *MatchFilter) SetRegexp(compileRegex *regexp.Regexp) {
	m.regexp = compileRegex
	m.RegexpString = compileRegex.String()
}

func (m *MatchFilter) GetRegexp() (*regexp.Regexp, error) {
	if m.regexp == nil {
		if m.RegexpString == "" {
			return nil, nil
		}

		re, err := regexp.Compile(m.RegexpString)
		if err != nil {
			log.Errorf("MatchFilter.GetRegexp: error compiling regexp: %v, err=%v", m.RegexpString, err)
			return nil, err
		}

		m.regexp = re
	}

	return m.regexp, nil
}

func (f *FilterCriteria) IsTimeRange() bool {
	if f.MatchFilter != nil {
		if f.MatchFilter.MatchColumn == "*" {
			return true
		}
		return f.MatchFilter.MatchColumn == config.GetTimeStampKey()
	} else {
		return f.ExpressionFilter.IsTimeRange()
	}
}

func (e *ExpressionFilter) IsTimeRange() bool {
	if e.LeftInput != nil && e.LeftInput.Expression != nil {
		if !e.LeftInput.Expression.IsTimeExpression() {
			return false
		}
	}
	if e.RightInput != nil && e.RightInput.Expression != nil {
		if !e.RightInput.Expression.IsTimeExpression() {
			return false
		}
	}
	return true
}

func (e *ExpressionFilter) GetAllColumns() map[string]bool {
	allCols := make(map[string]bool)
	if e.LeftInput != nil && e.LeftInput.Expression != nil {
		if e.LeftInput.Expression.RightInput != nil && len(e.LeftInput.Expression.RightInput.ColumnName) > 0 {
			allCols[e.LeftInput.Expression.RightInput.ColumnName] = true

		}
		if e.LeftInput.Expression.LeftInput != nil && len(e.LeftInput.Expression.LeftInput.ColumnName) > 0 {
			allCols[e.LeftInput.Expression.LeftInput.ColumnName] = true
		}
	}
	if e.RightInput != nil && e.RightInput.Expression != nil {
		if e.RightInput.Expression.RightInput != nil && len(e.RightInput.Expression.RightInput.ColumnName) > 0 {
			allCols[e.RightInput.Expression.RightInput.ColumnName] = true
		}
		if e.RightInput.Expression.LeftInput != nil && len(e.RightInput.Expression.LeftInput.ColumnName) > 0 {
			allCols[e.RightInput.Expression.LeftInput.ColumnName] = true
		}
	}
	return allCols
}

func (f *FilterCriteria) GetAllColumns() map[string]bool {
	if f.MatchFilter != nil {
		allCols := make(map[string]bool)
		allCols[f.MatchFilter.MatchColumn] = true
		return allCols
	}

	return f.ExpressionFilter.GetAllColumns()
}

// we expect a matchColumn == * AND matchWords == *
func (mf *MatchFilter) IsMatchAll() bool {
	if mf.MatchType == MATCH_PHRASE {
		return false
	}

	if mf.MatchColumn != "*" {
		return false
	}

	if len(mf.MatchWords) != 1 {
		return false
	}
	if bytes.Equal(mf.MatchWords[0], utils.STAR_BYTE) {
		return true
	}
	return false
}
