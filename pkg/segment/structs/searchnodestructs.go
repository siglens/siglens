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
	"bytes"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/utils"
	. "github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

/*
*******************************************
*******************************************
*******************************************
**************** NOTE *********************
any time you add a new element in the searchnode structs or in their sub structs,
make sure to adjust the snhasher.go code to update the hashids, else PQS will
incorrectly compute the hash id

Also DO NOT change the order of the struct fields, if you do then you have to
adjust the order in snhasher.go as well, however in that case the first time
when the new code will run, it will create new pqid values for existing queries

*******************************************
*******************************************
*******************************************
*/
type SearchNodeType uint8

const (
	MatchAllQuery    SearchNodeType = iota // query only needs to know a record's time range, no raw values
	ColumnValueQuery                       // query needs to know >0 non-timestamp column values
	InvalidQuery                           // an invalid query (e.g. invalid column name)
)

// A Search query is either an expression or match filter
// Never will both be defined
type SearchQuery struct {
	ExpressionFilter *SearchExpression
	MatchFilter      *MatchFilter
	SearchType       SearchQueryType // type of query
	QueryInfo        *QueryInfo      // query info
}

type QueryInfo struct {
	ColName string
	KValDte []byte          // only non-nil for `MatchDictArray` requests
	QValDte *DtypeEnclosure // column value to use for raw check. May be nil if query is match filter
}

type SearchCondition struct {
	SearchQueries []*SearchQuery
	SearchNode    []*SearchNode
}

type SearchNode struct {
	AndSearchConditions       *SearchCondition
	OrSearchConditions        *SearchCondition
	ExclusionSearchConditions *SearchCondition
	NodeType                  SearchNodeType // type of search request
}

func (q *SearchQuery) IsMatchAll() bool {
	if q.ExpressionFilter != nil {
		return q.ExpressionFilter.IsMatchAll()
	} else {
		return q.MatchFilter.IsMatchAll()
	}
}

func (r *SearchNode) AddQueryInfoForNode() SearchNodeType {
	nType := MatchAllQuery
	if r.AndSearchConditions != nil {
		currType := r.AndSearchConditions.AddQueryInfo()
		if currType != MatchAllQuery {
			nType = ColumnValueQuery
		}
	}
	if r.OrSearchConditions != nil {
		currType := r.OrSearchConditions.AddQueryInfo()
		if currType != MatchAllQuery {
			nType = ColumnValueQuery
		}
	}
	if r.ExclusionSearchConditions != nil {
		currType := r.ExclusionSearchConditions.AddQueryInfo()
		if currType != MatchAllQuery {
			nType = ColumnValueQuery
		}
	}
	r.NodeType = nType
	return nType
}

func (c *SearchCondition) AddQueryInfo() SearchNodeType {
	nType := MatchAllQuery
	if c.SearchNode != nil {
		for _, sQuery := range c.SearchNode {
			currType := sQuery.AddQueryInfoForNode()
			if currType != MatchAllQuery {
				nType = ColumnValueQuery
			}
		}
	}
	if c.SearchQueries != nil {
		for _, sQuery := range c.SearchQueries {
			currType := sQuery.GetQueryInfo()
			if currType != MatchAllQuery {
				nType = ColumnValueQuery
			}
		}
	}
	return nType
}

func (n *SearchQuery) GetQueryInfo() SearchNodeType {
	var queryInfo *QueryInfo
	if n.MatchFilter != nil {
		queryInfo = n.MatchFilter.GetQueryInfo()
	} else {
		queryInfo = n.ExpressionFilter.GetQueryInfo()
	}
	n.QueryInfo = queryInfo
	return n.GetQueryType()
}

func (q *SearchQuery) GetQueryType() SearchNodeType {
	if q.ExpressionFilter != nil {
		if !q.ExpressionFilter.IsTimeRangeFilter() {
			return ColumnValueQuery
		} else {
			return MatchAllQuery
		}
	} else {
		if q.MatchFilter.MatchColumn == "*" || q.MatchFilter.MatchColumn == config.GetTimeStampKey() {
			for _, matchWord := range q.MatchFilter.MatchWords {
				if bytes.Equal(matchWord, utils.STAR_BYTE) {
					if q.MatchFilter.MatchOperator == Or {
						return MatchAllQuery
					} else if q.MatchFilter.MatchOperator == And && len(q.MatchFilter.MatchWords) > 1 {
						return ColumnValueQuery
					} else {
						return MatchAllQuery
					}
				}
			}
		}
		return ColumnValueQuery
	}
}

func (m *MatchFilter) GetQueryInfo() *QueryInfo {
	var kValDte []byte
	var colName string
	var qValDte *DtypeEnclosure

	if m.MatchType == MATCH_DICT_ARRAY {
		colName = m.MatchColumn
		kValDte = m.MatchDictArray.MatchKey
		qValDte = m.MatchDictArray.MatchValue
	} else {
		colName = m.MatchColumn
	}
	if qValDte != nil {
		qValDte.AddStringAsByteSlice()
	}
	queryInfo := &QueryInfo{
		ColName: colName,
		KValDte: kValDte,
		QValDte: qValDte,
	}
	return queryInfo
}

func (se *SearchExpression) GetQueryInfo() *QueryInfo {

	var qColName string
	var qValDte *DtypeEnclosure
	if len(se.LeftSearchInput.ColumnName) > 0 {
		qColName = se.LeftSearchInput.ColumnName
		if se.RightSearchInput.ColumnValue != nil {
			qValDte = se.RightSearchInput.ColumnValue
		}
	} else {
		qColName = se.RightSearchInput.ColumnName
		qValDte = se.LeftSearchInput.ColumnValue
	}

	if qValDte != nil {
		qValDte.AddStringAsByteSlice()
	}

	qInfo := &QueryInfo{
		ColName: qColName,
		QValDte: qValDte,
	}

	return qInfo
}

// extract all columns from SearchQuery
// returns a map[string]bool, where key is the column name
// returns a bool that indicates whether a full wildcard is present (only "*")
func (query *SearchQuery) GetAllColumnsInQuery() (map[string]bool, bool) {
	if query.MatchFilter != nil {
		result := make(map[string]bool)
		if query.MatchFilter.MatchColumn == "*" {
			return result, true
		}
		result[query.MatchFilter.MatchColumn] = true
		return result, false
	}

	allExpressionCols := query.ExpressionFilter.getAllColumnsInSearch()
	result := make(map[string]bool)
	for col := range allExpressionCols {
		if col == "*" {
			return result, true
		}
		result[col] = true
	}
	return result, false
}

func (node *SearchNode) GetAllColumnsToSearch() (map[string]bool, bool) {

	timestampCol := config.GetTimeStampKey()
	allConditions, wildcard := GetAllColumnsFromNode(node)
	allColumns := make(map[string]bool)
	for colStr := range allConditions {
		if colStr != timestampCol {
			allColumns[colStr] = true
		}
	}
	return allColumns, wildcard
}

func GetAllColumnsFromNode(node *SearchNode) (map[string]bool, bool) {
	andCond, andWildcard := GetAllColumnsFromCondition(node.AndSearchConditions)
	orCond, orWildcard := GetAllColumnsFromCondition(node.OrSearchConditions)

	// don't add exclusion columns as they don't need to exist in the raw log line
	// If exclusion condition exists, then treat it as a wildcard to get all entries to check exclusion conditions on
	// TODO: optimize exclusion criteria
	var exclusionWildcard bool
	if node.ExclusionSearchConditions == nil {
		exclusionWildcard = false
	} else {
		exclusionWildcard = true
	}

	for k, v := range orCond {
		andCond[k] = v
	}
	return andCond, andWildcard || orWildcard || exclusionWildcard
}

// Get all columns that occur across a list of *SearchQuery
// returns all columns and if any of the columns contains wildcards
func GetAllColumnsFromCondition(cond *SearchCondition) (map[string]bool, bool) {
	allUniqueColumns := make(map[string]bool) // first make a map to avoid duplicates

	if cond == nil {
		return allUniqueColumns, false
	}

	for _, query := range cond.SearchQueries {
		currColumns, wildcard := query.GetAllColumnsInQuery()
		if wildcard {
			return allUniqueColumns, true
		}

		for k := range currColumns {
			allUniqueColumns[k] = true
		}
	}

	for _, node := range cond.SearchNode {
		currColumns, wildcard := GetAllColumnsFromNode(node)
		if wildcard {
			return allUniqueColumns, true
		}
		for k, v := range currColumns {
			allUniqueColumns[k] = v
		}
	}

	allUniqueColumns[config.GetTimeStampKey()] = true

	return allUniqueColumns, false
}

// returns map[string]bool, bool, LogicalOperator
// map is all non-wildcard block bloom keys, bool is if any keyword contained a wildcard, LogicalOperator
// is if any/all of map keys need to exist
func (query *SearchQuery) GetAllBlockBloomKeysToSearch() (map[string]bool, bool, LogicalOperator) {

	if query.MatchFilter != nil {
		matchKeys, wildcardExists, matchOp := query.MatchFilter.GetAllBlockBloomKeysToSearch()
		return matchKeys, wildcardExists, matchOp
	} else {
		blockBloomKeys, wildcardExists, err := query.ExpressionFilter.GetAllBlockBloomKeysToSearch()
		if err != nil {
			return make(map[string]bool), false, And
		}
		return blockBloomKeys, wildcardExists, And
	}
}

func (query *SearchQuery) ExtractRangeFilterFromQuery(qid uint64) (map[string]string, FilterOperator, bool) {

	if query.MatchFilter != nil {
		return nil, Equals, false
	}
	return ExtractRangeFilterFromSearch(query.ExpressionFilter.LeftSearchInput,
		query.ExpressionFilter.FilterOp, query.ExpressionFilter.RightSearchInput, qid)
}

// Given a left and right SearchInputs with filterOp, extract out range filters
// Returns a map from column to value, the final operator, and a bool telling if a range filter has been found for these. False value means the search inputs have no range filters
// (may have swapped from original if right has column and left and literal)
func ExtractRangeFilterFromSearch(leftSearch *SearchExpressionInput, filterOp FilterOperator, rightSearch *SearchExpressionInput, qid uint64) (map[string]string, FilterOperator, bool) {

	if filterOp == IsNull || filterOp == IsNotNull {
		return nil, filterOp, false
	}
	rangeFilter := make(map[string]string)
	var finalOp FilterOperator
	if len(leftSearch.ColumnName) > 0 && rightSearch.ColumnValue != nil {
		if !rightSearch.ColumnValue.IsNumeric() {
			return nil, filterOp, false
		}

		// TODO: byte column name comparison
		rangeFilter[leftSearch.ColumnName] = rightSearch.ColumnValue.StringVal
		finalOp = filterOp

		return rangeFilter, finalOp, true
	} else if len(rightSearch.ColumnName) > 0 && leftSearch.ColumnValue != nil {
		if !leftSearch.ColumnValue.IsNumeric() {
			return rangeFilter, filterOp, false
		}

		// TODO: byte column name comparison
		rangeFilter[rightSearch.ColumnName] = leftSearch.ColumnValue.StringVal
		reflectedOp := ReflectFilterOperator[filterOp]
		finalOp = reflectedOp

		return rangeFilter, finalOp, true
	} else {
		// TODO: simply complex relations for range filters -> col1 * 2 > 5 --> col1 > 2.5
		log.Warningf("qid=%d, Unable to extract range filter from %+v, and %+v", qid, leftSearch, rightSearch)
	}

	return rangeFilter, filterOp, false
}

func EditQueryTypeForInvalidColumn(originalType SearchNodeType) SearchNodeType {

	// we were unable to extract columns we needed
	if originalType != MatchAllQuery {
		return InvalidQuery
	}
	return originalType
}
