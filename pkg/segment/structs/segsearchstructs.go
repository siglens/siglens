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

	"github.com/bits-and-blooms/bloom/v3"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	. "github.com/siglens/siglens/pkg/segment/utils"
	sutils "github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

type SearchQueryType uint8

const (
	MatchAll                   SearchQueryType = iota // match all data
	MatchWords                                        // match words in a single column
	MatchWordsAllColumns                              // match words in any column
	SimpleExpression                                  // simple expression has one column name an operator and a value to compare
	RegexExpression                                   // regex expression has one column and a regex string column value
	RegexExpressionAllColumns                         // apply RegexExpression on all columns
	SimpleExpressionAllColumns                        // apply SimpleExpression on all columns
	ComplexExpression                                 // complex expression relates multiple columns
	MatchDictArraySingleColumn
	MatchDictArrayAllColumns
)

type SegType uint8

const (
	UNKNOWN SegType = iota
	RAW_SEARCH
	PQS
	UNROTATED_PQS
	UNROTATED_RAW_SEARCH
	SEGMENT_STATS_SEARCH
	UNROTATED_SEGMENT_STATS_SEARCH
	METRICS_SEARCH
	UNROTATED_METRICS_SEARCH
)

func (s SegType) String() string {
	switch s {
	case RAW_SEARCH:
		return "RAW_SEARCH"
	case PQS:
		return "PQS"
	case UNROTATED_PQS:
		return "UNROTATED_PQS"
	case UNROTATED_RAW_SEARCH:
		return "UNROTATED_RAW_SEARCH"
	case SEGMENT_STATS_SEARCH:
		return "SEGMENT_STATS_SEARCH"
	case UNROTATED_SEGMENT_STATS_SEARCH:
		return "UNROTATED_SEGMENT_STATS_SEARCH"
	case METRICS_SEARCH:
		return "METRICS_SEARCH"
	case UNROTATED_METRICS_SEARCH:
		return "UNROTATED_METRICS_SEARCH"
	default:
		return "UNKNOWN"
	}
}

// A flattened expression input used for searching
// TODO: flatten SearchExpressionInput with just []byte input
type SearchExpressionInput struct {
	ColumnName          string          // columnName to search for
	ComplexRelation     *Expression     // complex relations that have columns defined in both sides
	ColumnValue         *DtypeEnclosure // column value: "0", "abc", "abcd*", "0.213". This value will be normalized to Lower Case if the search is case insensitive.
	OriginalColumnValue *DtypeEnclosure // original column value. Similar to Column Value, but is only created when the search is case insensitive
}

// A flattened expression used for searching
// leftSearchInput will always be defined, rightSearchInput may not be depending on filterOp
type SearchExpression struct {
	LeftSearchInput  *SearchExpressionInput
	FilterOp         FilterOperator
	RightSearchInput *SearchExpressionInput
	SearchInfo       *SearchInfo
}

type SearchInfo struct {
	ColEncoding []byte
	QValDte     *DtypeEnclosure
}

type SearchMetadataHolder struct {
	BlockSummaries     []*BlockSummary
	BlockSummariesFile string
	SearchTotalMemory  uint64 // total memory that this search would take, BlockSummaries + raw search buffers
}

type ColOffAndLen struct {
	Offset int64
	Length uint32
}

type BlockMetadataHolder struct {
	BlkNum            uint16
	ColBlockOffAndLen []ColOffAndLen
}

type AllBlksMetaInfo struct {
	CnameDict map[string]int
	AllBmh    map[uint16]*BlockMetadataHolder
}

// a struct for raw search to apply search on specific blocks within a file
type SegmentSearchRequest struct {
	SegmentKey           string
	SearchMetadata       *SearchMetadataHolder
	AllBlocksToSearch    map[uint16]struct{} // maps all blocks needed to search
	BlockToValidRecNums  map[uint16][]uint16 // If not nil, only search these records
	VirtualTableName     string
	AllPossibleColumns   map[string]bool // all possible columns for the segKey
	LatestEpochMS        uint64          // latest epoch time - used for query planning
	SType                SegType
	CmiPassedCnames      map[uint16]map[string]bool // maps blkNum -> colName -> true that have passed the cmi check
	HasMatchedRrc        bool                       // flag to denote matches, so that we decide whether to send a websocket update
	ConsistentCValLenMap map[string]uint32          // map of column name to consistent column value length
}

// a holder struct for holding a cmi for a single block. Based on CmiType, either Bf or Ranges will be defined
type CmiContainer struct {
	CmiType uint8
	Loaded  bool
	Bf      *bloom.BloomFilter
	Ranges  map[string]*Numbers
}

// even if only one block will be searched and parallelism=10, we will spawn 10 buffers, although 9 wont be used
// TODO: more accurate block summaries and colmeta sizing
func (ssr *SegmentSearchRequest) GetMaxSearchMemorySize(parallelismPerFile int64, bitsetMinSize uint16) uint64 {

	// bitset size worst case is min(15000*num blocks, total record count)
	var totalBits uint64
	for i := 0; i < len(ssr.SearchMetadata.BlockSummaries); i++ {
		if _, ok := ssr.AllBlocksToSearch[uint16(i)]; !ok {
			continue
		}
		if ssr.SearchMetadata.BlockSummaries[i].RecCount > bitsetMinSize {
			totalBits += uint64(ssr.SearchMetadata.BlockSummaries[i].RecCount)
		} else {
			totalBits += uint64(bitsetMinSize)
		}
	}
	totalSize := uint64(totalBits / 8)

	// for raw search & aggs its hard to calculate as memory for multi readers comes from a pool,
	// hence we assume that there will be enough memory in the pool & in the buffer
	if ssr.SearchMetadata == nil {
		return uint64(totalSize)
	}

	totalSize += ssr.SearchMetadata.SearchTotalMemory
	return totalSize
}

// function used to nil out block sum and colmeta
func (ssr *SegmentSearchRequest) CleanSearchMetadata() {
	if ssr.SearchMetadata == nil {
		return
	}
	ssr.SearchMetadata.BlockSummaries = nil
}

/*
*

	Logical operator only dictates how the block numbers should be resolved

	the CMIPassed names will always be unioned.

*
*/
func (ssr *SegmentSearchRequest) JoinRequest(toJoin *SegmentSearchRequest, op LogicalOperator) {
	// merge blocksearch info
	if op == And {
		for blockNum := range ssr.AllBlocksToSearch {
			if _, ok := toJoin.AllBlocksToSearch[blockNum]; !ok {
				delete(ssr.AllBlocksToSearch, blockNum)
				delete(ssr.CmiPassedCnames, blockNum)
				continue
			}
			for cname := range toJoin.CmiPassedCnames[blockNum] {
				ssr.CmiPassedCnames[blockNum][cname] = true
			}
		}
	} else {
		for blockNum := range toJoin.AllBlocksToSearch {
			ssr.AllBlocksToSearch[blockNum] = struct{}{}
			if _, ok := ssr.CmiPassedCnames[blockNum]; !ok {
				ssr.CmiPassedCnames[blockNum] = make(map[string]bool)
			}

			for cname := range toJoin.CmiPassedCnames[blockNum] {
				ssr.CmiPassedCnames[blockNum][cname] = true
			}
		}
	}
	// merge columns
	ssr.JoinColumnInfo(toJoin)
}

// merges toJoin.SearchColumns with ssr.SearchColumns
func (ssr *SegmentSearchRequest) JoinColumnInfo(toJoin *SegmentSearchRequest) {
	// merge columns
	for col := range toJoin.AllPossibleColumns {
		ssr.AllPossibleColumns[col] = true
	}
}

func (searchExp *SearchExpression) IsMatchAll() bool {

	if searchExp.FilterOp != Equals {
		return false
	}
	if searchExp.LeftSearchInput == nil || searchExp.RightSearchInput == nil {
		return false // both left and right need to be defined
	}

	var colName string
	var colValue *DtypeEnclosure
	if len(searchExp.LeftSearchInput.ColumnName) > 0 {
		colName = searchExp.LeftSearchInput.ColumnName
	} else {
		colName = searchExp.RightSearchInput.ColumnName
	}

	if searchExp.LeftSearchInput.ColumnValue != nil {
		colValue = searchExp.LeftSearchInput.ColumnValue
	} else if searchExp.RightSearchInput != nil && searchExp.RightSearchInput.ColumnValue != nil {
		colValue = searchExp.RightSearchInput.ColumnValue
	}
	if colValue == nil {
		return false
	}

	return colName == "*" && colValue.IsFullWildcard()
}

func (searchExp *SearchExpression) GetExpressionType() SearchQueryType {
	if searchExp.LeftSearchInput.ComplexRelation != nil {
		return ComplexExpression
	}
	if searchExp.RightSearchInput != nil && searchExp.RightSearchInput.ComplexRelation != nil {
		return ComplexExpression
	}
	// at this point, all expressions are some kind of expression
	var colName string
	var colVal *DtypeEnclosure
	if len(searchExp.LeftSearchInput.ColumnName) > 0 {
		colName = searchExp.LeftSearchInput.ColumnName
	} else {
		colName = searchExp.RightSearchInput.ColumnName
	}
	if searchExp.LeftSearchInput.ColumnValue != nil {
		colVal = searchExp.LeftSearchInput.ColumnValue
	} else {
		colVal = searchExp.RightSearchInput.ColumnValue
	}
	wildcardColName := colName == "*"
	if colVal == nil {
		if wildcardColName {
			return RegexExpression
		}
		return SimpleExpression
	}
	regexCol := colVal.IsRegex()
	if wildcardColName {
		if regexCol {
			return RegexExpressionAllColumns
		} else {
			return SimpleExpressionAllColumns
		}
	}
	if regexCol {
		return RegexExpression
	} else {
		return SimpleExpression
	}
}

// parse a FilterInput to a friendly SearchInput for raw searching/expression matching
func getSearchInputFromFilterInput(filter *FilterInput, isCaseInsensitive bool, isTerm bool, qid uint64) *SearchExpressionInput {

	searchInput := SearchExpressionInput{}

	if filter == nil {
		return &searchInput
	}

	if len(filter.SubtreeResult) > 0 { // if filterSubtree is defined, only literal in search input
		val, err := CreateDtypeEnclosure(filter.SubtreeResult, qid)
		if err != nil {
			// TODO: handle error
			log.Errorf("getSearchInputFromFilterInput: qid=%d, Error creating dtype enclosure: %v", qid, err)
		} else {
			val.UpdateRegexp(isCaseInsensitive, isTerm)
		}
		searchInput.ColumnValue = val
		return &searchInput
	}

	if filter.Expression.RightInput == nil { // rightInput is nil, meaning only left expressionInput is defined and only has columnName or
		expInput := filter.Expression.LeftInput

		if len(expInput.ColumnName) > 0 {
			searchInput.ColumnName = expInput.ColumnName
		} else {
			searchInput.ColumnValue = expInput.ColumnValue
			searchInput.OriginalColumnValue = expInput.OriginalColumnValue
		}
	} else {
		searchInput.ComplexRelation = filter.Expression
	}

	return &searchInput
}

func GetSearchQueryFromFilterCriteria(criteria *FilterCriteria, qid uint64) *SearchQuery {
	var sq *SearchQuery

	if criteria.MatchFilter != nil {
		sq = extractSearchQueryFromMatchFilter(criteria.MatchFilter, criteria.FilterIsCaseInsensitive, qid)
	} else {
		sq = extractSearchQueryFromExpressionFilter(criteria.ExpressionFilter, criteria.FilterIsCaseInsensitive, criteria.FilterIsTerm, qid)
	}
	sq.FilterIsCaseInsensitive = criteria.FilterIsCaseInsensitive
	sq.FilterIsTerm = criteria.FilterIsTerm
	return sq
}

func extractSearchQueryFromMatchFilter(match *MatchFilter, isCaseInsensitive bool, qid uint64) *SearchQuery {
	var qType SearchQueryType
	currQuery := &SearchQuery{
		MatchFilter: match,
	}
	if match.MatchType == MATCH_DICT_ARRAY {
		if match.MatchColumn == "*" {
			qType = MatchDictArrayAllColumns
		} else {
			qType = MatchDictArraySingleColumn
		}
		currQuery.SearchType = qType
	} else if match.MatchColumn == "*" {
		qType = MatchWordsAllColumns
		if match.MatchOperator == And {
			if len(match.MatchWords) == 1 && bytes.Equal(match.MatchWords[0], STAR_BYTE) {
				qType = MatchAll
			}
		} else if match.MatchOperator == Or {
			for _, word := range match.MatchWords {
				if bytes.Equal(word, STAR_BYTE) {
					qType = MatchAll
					break
				}
			}
		}
		currQuery.SearchType = qType
	} else {
		currQuery.SearchType = MatchWords
	}
	if match.MatchPhrase != nil && bytes.Contains(match.MatchPhrase, []byte("*")) {
		cval := dtu.ReplaceWildcardStarWithRegex(string(match.MatchPhrase))
		if isCaseInsensitive {
			cval = "(?i)" + cval
		}
		rexpC, err := regexp.Compile(cval)
		if err != nil {
			log.Errorf("qid=%v, extractSearchQueryFromMatchFilter: regexp compile failed for exp: %v, err: %v", qid, cval, err)
		} else {
			currQuery.MatchFilter.SetRegexp(rexpC)
		}
	}

	return currQuery
}

func extractSearchQueryFromExpressionFilter(exp *ExpressionFilter, isCaseInsensitive bool, isTerm bool, qid uint64) *SearchQuery {
	leftSearchInput := getSearchInputFromFilterInput(exp.LeftInput, isCaseInsensitive, isTerm, qid)
	rightSearchInput := getSearchInputFromFilterInput(exp.RightInput, isCaseInsensitive, isTerm, qid)
	sq := &SearchQuery{
		ExpressionFilter: &SearchExpression{
			LeftSearchInput:  leftSearchInput,
			FilterOp:         exp.FilterOperator,
			RightSearchInput: rightSearchInput,
		},
	}
	expType := getSearchTypeFromSearchExpression(sq.ExpressionFilter)
	sq.SearchType = expType

	if sq.SearchType == RegexExpression || sq.SearchType == RegexExpressionAllColumns {
		if sq.ExpressionFilter.LeftSearchInput.ColumnValue != nil &&
			sq.ExpressionFilter.LeftSearchInput.ColumnValue.Dtype == SS_DT_STRING {

			// We don't need to do this with the LeftSearchInput.OriginalColumnValue, as this is a regex/wildcard
			// And we don't do Bloom Filtering for regex/wildcard searches
			cval := dtu.SPLToRegex(sq.ExpressionFilter.LeftSearchInput.ColumnValue.StringVal, isCaseInsensitive, isTerm)

			rexpC, err := regexp.Compile(cval)
			if err != nil {
				log.Errorf("extractSearchQueryFromExpressionFilter: regexp compile failed for exp: %v, err: %v", cval, err)
			} else {
				sq.ExpressionFilter.LeftSearchInput.ColumnValue.SetRegexp(rexpC)
			}
		}
	}
	return sq
}

func getSearchTypeFromSearchExpression(searchExp *SearchExpression) SearchQueryType {

	if searchExp.IsMatchAll() {
		return MatchAll
	}
	return searchExp.GetExpressionType()
}

// extract all columns from SearchInput
// ex: SearchExpressionInput{columnName="abc"} -> abc
// ex: SearchExpressionInput{complexRelation={literal=2,op=mult,columnName="def"}} -> "def"
func (search *SearchExpressionInput) getAllColumnsInSearch() map[string]string {

	allColumns := make(map[string]string)

	if len(search.ColumnName) > 0 {
		allColumns[string(search.ColumnName)] = ""
	}

	if search.ComplexRelation != nil {
		exp := search.ComplexRelation
		if exp.LeftInput != nil && len(exp.LeftInput.ColumnName) > 0 {
			allColumns[exp.LeftInput.ColumnName] = ""
		}

		if exp.RightInput != nil && len(exp.RightInput.ColumnName) > 0 {
			allColumns[exp.RightInput.ColumnName] = ""
		}
	}
	return allColumns
}

func (searchExp *SearchExpression) getAllColumnsInSearch() map[string]string {

	allColumns := searchExp.LeftSearchInput.getAllColumnsInSearch()

	if searchExp.RightSearchInput != nil {
		rightColumns := searchExp.RightSearchInput.getAllColumnsInSearch()

		for key, val := range rightColumns {
			allColumns[key] = val
		}
	}

	return allColumns
}

// returns a map with keys,  a boolean, and error
// the map will contain only non wildcarded keys,
// if bool is true, the searchExpression contained a wildcard
func (searchExp *SearchExpression) GetAllBlockBloomKeysToSearch(isCaseInsensitive bool) (map[string]bool, map[string]string, bool, error) {
	if searchExp.FilterOp != Equals {
		return nil, nil, false, fmt.Errorf("SearchExpression.GetAllBlockBloomKeysToSearch: relation is not simple filter op is not equals")
	}
	if searchExp.LeftSearchInput != nil && searchExp.LeftSearchInput.ComplexRelation != nil {
		// complex relations are not supported for blockbloom
		return nil, nil, false, fmt.Errorf("SearchExpression.GetAllBlockBloomKeysToSearch: relation is not simple LeftSearchInput is complex relation")
	}
	if searchExp.RightSearchInput != nil && searchExp.RightSearchInput.ComplexRelation != nil {
		return nil, nil, false, fmt.Errorf("SearchExpression.GetAllBlockBloomKeysToSearch: relation is not simple RightSearchInput is complex relation")
	}
	allKeys := make(map[string]bool)
	originalAllKeys := make(map[string]string) // map of normalized lowercase key -> original key
	var colVal *DtypeEnclosure
	var originalColVal *DtypeEnclosure
	if searchExp.LeftSearchInput != nil && searchExp.LeftSearchInput.ColumnValue != nil {
		colVal = searchExp.LeftSearchInput.ColumnValue
		originalColVal = searchExp.LeftSearchInput.OriginalColumnValue
	} else if searchExp.RightSearchInput != nil && searchExp.RightSearchInput.ColumnValue != nil {
		colVal = searchExp.RightSearchInput.ColumnValue
		originalColVal = searchExp.RightSearchInput.OriginalColumnValue
	}

	if colVal == nil {
		return nil, nil, false, fmt.Errorf("SearchExpression.GetAllBlockBloomKeysToSearch: unable to extract column name and value from request")
	}

	if colVal.IsRegex() {
		return allKeys, originalAllKeys, true, nil
	}
	if len(colVal.StringVal) == 0 {
		return allKeys, originalAllKeys, false, fmt.Errorf("SearchExpression.GetAllBlockBloomKeysToSearch: unable to extract column name from request")
	}
	allKeys[colVal.StringVal] = true

	if isCaseInsensitive {
		if originalColVal != nil && len(originalColVal.StringVal) > 0 {
			originalAllKeys[colVal.StringVal] = originalColVal.StringVal
		}
	}

	return allKeys, originalAllKeys, false, nil
}

func (match *MatchFilter) GetAllBlockBloomKeysToSearch(isCaseInsensitive bool) (map[string]bool, map[string]string, bool, LogicalOperator) {
	allKeys := make(map[string]bool)
	originalAllKeys := make(map[string]string) // map of normalized lowercase key -> original key
	wildcardExists := false
	if match.MatchType == MATCH_DICT_ARRAY {
		mKey := match.MatchDictArray.MatchKey
		mVal := match.MatchDictArray.MatchValue
		var mValStr string
		switch mVal.Dtype {
		case sutils.SS_DT_BOOL:
			mValStr = fmt.Sprintf("%v", mVal.BoolVal)
		case sutils.SS_DT_STRING:
			mValStr = fmt.Sprintf("%v", mVal.StringVal)
		case sutils.SS_DT_UNSIGNED_NUM:
			mValStr = fmt.Sprintf("%v", mVal.UnsignedVal)
		case sutils.SS_DT_SIGNED_NUM:
			mValStr = fmt.Sprintf("%v", mVal.SignedVal)
		case sutils.SS_DT_FLOAT:
			mValStr = fmt.Sprintf("%v", mVal.FloatVal)
		default:
			log.Errorf("MatchFilter.GetAllBlockBloomKeysToSearch: unsupported dtype: %v", mVal.Dtype)
		}

		allKeys[string(mKey)] = true
		allKeys[mValStr] = true
		return allKeys, originalAllKeys, wildcardExists, And
	} else if match.MatchType == MATCH_PHRASE {
		if strings.Contains(string(match.MatchPhrase), "*") {
			wildcardExists = true
		} else {
			stringMatchPhrase := string(match.MatchPhrase)
			allKeys[stringMatchPhrase] = true
			if isCaseInsensitive && len(match.MatchPhraseOriginal) > 0 {
				originalAllKeys[stringMatchPhrase] = string(match.MatchPhraseOriginal)
			}
		}
	} else {
		isMatchWordsLengthEqual := len(match.MatchWordsOriginal) == len(match.MatchWords)
		for idx, literal := range match.MatchWords {

			if strings.Contains(string(literal), "*") {
				wildcardExists = true
				continue
			}
			stringLiteral := string(literal)
			allKeys[stringLiteral] = true
			if isCaseInsensitive && isMatchWordsLengthEqual {
				originalAllKeys[stringLiteral] = string(match.MatchWordsOriginal[idx])
			}
		}
		// if only one matchWord then do And so that CMI logic will only pass blocks that pass
		// bloom check
		if len(allKeys) == 1 {
			return allKeys, originalAllKeys, wildcardExists, And
		}
	}
	return allKeys, originalAllKeys, wildcardExists, match.MatchOperator
}

func (ef *SearchExpression) IsTimeRangeFilter() bool {
	if ef.IsMatchAll() {
		return true
	}
	if ef.LeftSearchInput != nil && len(ef.LeftSearchInput.ColumnName) > 0 {
		if ef.LeftSearchInput.ColumnName != config.GetTimeStampKey() {
			return false
		}
	}
	if ef.RightSearchInput != nil && len(ef.RightSearchInput.ColumnName) > 0 {
		if ef.RightSearchInput.ColumnName != config.GetTimeStampKey() {
			return false
		}
	}
	return true
}
