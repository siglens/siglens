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

package writer

import (
	"fmt"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

/*
Main function exported to check colWips against persistent queries during ingest

Internally, updates the bitset with recNum for all queries that matched
*/
func applyStreamingSearchToRecord(segStore *SegStore, psNode map[string]*structs.SearchNode,
	recNum uint16) {

	holderDte := &utils.DtypeEnclosure{}
	tsKey := config.GetTimeStampKey()
	for pqid, sNode := range psNode {
		holderDte.Reset()
		if applySearchSingleNode(segStore.wipBlock.colWips, sNode, holderDte, tsKey, segStore) {
			segStore.addRecordToMatchedResults(recNum, pqid)
		}
	}
}

func applySearchSingleNode(colWips map[string]*ColWip, sNode *structs.SearchNode, holderDte *utils.DtypeEnclosure, tsKey string, segStore *SegStore) bool {
	retVal := false
	if sNode.AndSearchConditions != nil {
		andConditions := applySearchSingleCondition(colWips, sNode.AndSearchConditions, utils.And, holderDte, tsKey, segStore)
		if !andConditions {
			return false
		}
		retVal = true
	}

	// at least one must pass. If and conditions are defined, then this is a noop check
	if sNode.OrSearchConditions != nil {
		orConditions := applySearchSingleCondition(colWips, sNode.OrSearchConditions, utils.Or, holderDte, tsKey, segStore)
		retVal = retVal || orConditions
	}

	if !retVal {
		return retVal
	}
	// all must fail
	if sNode.ExclusionSearchConditions != nil {
		exclusionConditions := applySearchSingleCondition(colWips, sNode.ExclusionSearchConditions, utils.Exclusion, holderDte, tsKey, segStore)
		if exclusionConditions {
			return false
		}
	}
	return true
}

func applySearchSingleCondition(colWips map[string]*ColWip, sCond *structs.SearchCondition, op utils.LogicalOperator,
	holderDte *utils.DtypeEnclosure, tsKey string, segStore *SegStore) bool {
	orMatch := false
	if sCond.SearchNode != nil {
		for _, sNode := range sCond.SearchNode {
			retVal := applySearchSingleNode(colWips, sNode, holderDte, tsKey, segStore)
			if !retVal && op == utils.And {
				return retVal
			} else {
				orMatch = orMatch || retVal
			}
		}
	}
	if sCond.SearchQueries != nil {
		for _, query := range sCond.SearchQueries {
			retVal := applySearchSingleQuery(colWips, query, op, holderDte, tsKey, segStore)
			if !retVal && op == utils.And {
				return retVal
			} else {
				orMatch = orMatch || retVal
			}
		}
	}

	if op == utils.And {
		// previous false values would have returned already
		return true
	}
	return orMatch
}

func applySearchSingleQuery(colWips map[string]*ColWip, sQuery *structs.SearchQuery, op utils.LogicalOperator,
	holderDte *utils.DtypeEnclosure, tsKey string, segStore *SegStore) bool {
	switch sQuery.SearchType {
	case structs.MatchAll:
		return true
	case structs.MatchWords:
		rawVal, ok := colWips[sQuery.MatchFilter.MatchColumn]
		if !ok {
			return false
		}
		retVal, err := ApplySearchToMatchFilterRawCsg(sQuery.MatchFilter, rawVal.getLastRecord(), nil)
		if err != nil {
			segStore.StoreSegmentError("applySearchSingleQuery: failed to apply match words search", log.ErrorLevel, err)
			return false
		}
		return retVal
	case structs.MatchWordsAllColumns:
		for cname, colVal := range colWips {
			if cname == tsKey {
				continue
			}
			retVal, _ := ApplySearchToMatchFilterRawCsg(sQuery.MatchFilter, colVal.getLastRecord(), nil)
			if retVal {
				return true
			}
		}
		return false
	case structs.SimpleExpression:
		rawVal, ok := colWips[sQuery.QueryInfo.ColName]
		if !ok {
			return false
		}
		retVal, err := ApplySearchToExpressionFilterSimpleCsg(sQuery.QueryInfo.QValDte, sQuery.ExpressionFilter.FilterOp, rawVal.getLastRecord(), false, holderDte)
		if err != nil {
			segStore.StoreSegmentError("applySearchSingleQuery: failed to apply simple expression search", log.ErrorLevel, err)
			return false
		}
		return retVal
	case structs.RegexExpression:
		rawVal, ok := colWips[sQuery.QueryInfo.ColName]
		if !ok {
			return false
		}
		retVal, err := ApplySearchToExpressionFilterSimpleCsg(sQuery.QueryInfo.QValDte, sQuery.ExpressionFilter.FilterOp, rawVal.getLastRecord(), true, holderDte)
		if err != nil {
			segStore.StoreSegmentError("applySearchSingleQuery: failed to apply wildcard expression search on RegexExpression", log.ErrorLevel, err)
			return false
		}
		return retVal
	case structs.RegexExpressionAllColumns:
		for cname, colVal := range colWips {
			if cname == tsKey {
				continue
			}
			retVal, _ := ApplySearchToExpressionFilterSimpleCsg(sQuery.QueryInfo.QValDte, sQuery.ExpressionFilter.FilterOp, colVal.getLastRecord(), true, holderDte)
			if retVal {
				return true
			}
		}
		return false
	case structs.SimpleExpressionAllColumns:
		for cname, colVal := range colWips {
			if cname == tsKey {
				continue
			}
			retVal, _ := ApplySearchToExpressionFilterSimpleCsg(sQuery.QueryInfo.QValDte, sQuery.ExpressionFilter.FilterOp, colVal.getLastRecord(), false, holderDte)
			if retVal {
				return true
			}
		}
		return false
	case structs.MatchDictArraySingleColumn:
		rawVal, ok := colWips[sQuery.QueryInfo.ColName]
		if !ok {
			return false
		}
		retVal, err := ApplySearchToDictArrayFilter([]byte(sQuery.QueryInfo.ColName), sQuery.QueryInfo.QValDte, rawVal.getLastRecord(), sQuery.ExpressionFilter.FilterOp, true, holderDte)
		if err != nil {
			segStore.StoreSegmentError("applySearchSingleQuery: failed to apply wildcard expression search on MatchDictArraySingleColumn", log.ErrorLevel, err)
			return false
		}
		return retVal
	case structs.MatchDictArrayAllColumns:
		for cname, colVal := range colWips {
			if cname == tsKey {
				continue
			}
			retVal, _ := ApplySearchToDictArrayFilter([]byte(sQuery.QueryInfo.ColName), sQuery.QueryInfo.QValDte, colVal.getLastRecord(), sQuery.ExpressionFilter.FilterOp, false, holderDte)
			if retVal {
				return true
			}
		}
		return false
	default:
		segStore.StoreSegmentError(fmt.Sprintf("applySearchSingleQuery: unsupported query type %v", sQuery.SearchType), log.ErrorLevel, nil)
		return false
	}
}

/*
Adds recNum as a matched record in the current bitset based on pqid
*/
func (segStore *SegStore) addRecordToMatchedResults(recNum uint16, pqid string) {
	pqMatch, ok := segStore.pqMatches[pqid]
	if !ok {
		log.Errorf("addRecordToMatchedResults: tried to match a record for a pqid that does not exist")
		return
	}
	pqMatch.AddMatchedRecord(uint(recNum))
}

func (colWip *ColWip) getLastRecord() []byte {
	return colWip.cbuf[colWip.cstartidx:colWip.cbufidx]
}
