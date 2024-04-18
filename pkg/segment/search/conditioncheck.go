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

package search

import (
	"errors"

	"github.com/siglens/siglens/pkg/segment/reader/segread"
	. "github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer"
	log "github.com/sirupsen/logrus"
)

// TODO: support for complex expressions
func ApplyColumnarSearchQuery(query *SearchQuery, multiColReader *segread.MultiColSegmentReader,
	blockNum uint16, recordNum uint16, holderDte *DtypeEnclosure, qid uint64,
	dictEncColNames map[string]bool, searchReq *SegmentSearchRequest,
	cmiPassedCnames map[string]bool) (bool, error) {

	switch query.SearchType {
	case MatchAll:
		// ts should have already been checked
		return true, nil
	case MatchWords:
		rawColVal, err := multiColReader.ReadRawRecordFromColumnFile(query.QueryInfo.ColName,
			blockNum, recordNum, qid)
		if err != nil {
			return false, err
		}
		return writer.ApplySearchToMatchFilterRawCsg(query.MatchFilter, rawColVal)
	case MatchWordsAllColumns:
		var atleastOneNonError bool
		var finalErr error
		for cname := range cmiPassedCnames {

			// we skip rawsearching for columns that are dict encoded,
			// since we already search for them in the prior call to applyColumnarSearchUsingDictEnc
			_, ok := dictEncColNames[cname]
			if ok {
				continue
			}

			rawColVal, err := multiColReader.ReadRawRecordFromColumnFile(cname, blockNum, recordNum, qid)
			if err != nil {
				finalErr = err
				continue
			} else {
				atleastOneNonError = true
			}
			retVal, _ := writer.ApplySearchToMatchFilterRawCsg(query.MatchFilter, rawColVal)
			if retVal {
				multiColReader.IncrementColumnUsage(cname)
				return true, nil
			}
		}
		if atleastOneNonError {
			return false, nil
		} else {
			return false, finalErr
		}
	case SimpleExpression:
		rawColVal, err := multiColReader.ReadRawRecordFromColumnFile(query.QueryInfo.ColName, blockNum, recordNum, qid)
		if err != nil {
			return false, err
		}
		return writer.ApplySearchToExpressionFilterSimpleCsg(query.QueryInfo.QValDte, query.ExpressionFilter.FilterOp, rawColVal, false, holderDte)
	case RegexExpression:
		rawColVal, err := multiColReader.ReadRawRecordFromColumnFile(query.QueryInfo.ColName, blockNum, recordNum, qid)
		if err != nil {
			return false, err
		}
		return writer.ApplySearchToExpressionFilterSimpleCsg(query.QueryInfo.QValDte, query.ExpressionFilter.FilterOp, rawColVal, true, holderDte)
	case RegexExpressionAllColumns:
		var atleastOneNonError bool
		var finalErr error
		for cname := range cmiPassedCnames {

			// we skip rawsearching for columns that are dict encoded,
			// since we already search for them in the prior call to applyColumnarSearchUsingDictEnc
			_, ok := dictEncColNames[cname]
			if ok {
				continue
			}

			rawColVal, err := multiColReader.ReadRawRecordFromColumnFile(cname, blockNum, recordNum, qid)
			if err != nil {
				finalErr = err
				continue
			} else {
				atleastOneNonError = true
			}
			retVal, _ := writer.ApplySearchToExpressionFilterSimpleCsg(query.QueryInfo.QValDte, query.ExpressionFilter.FilterOp, rawColVal, true, holderDte)
			if retVal {
				multiColReader.IncrementColumnUsage(cname)
				return true, nil
			}
		}
		if atleastOneNonError {
			return false, nil
		} else {
			return false, finalErr
		}
	case SimpleExpressionAllColumns:
		var atleastOneNonError bool
		var finalErr error
		for cname := range cmiPassedCnames {

			// we skip rawsearching for columns that are dict encoded,
			// since we already search for them in the prior call to applyColumnarSearchUsingDictEnc
			_, ok := dictEncColNames[cname]
			if ok {
				continue
			}

			rawColVal, err := multiColReader.ReadRawRecordFromColumnFile(cname, blockNum, recordNum, qid)
			if err != nil {
				finalErr = err
				continue
			} else {
				atleastOneNonError = true
			}
			retVal, _ := writer.ApplySearchToExpressionFilterSimpleCsg(query.QueryInfo.QValDte, query.ExpressionFilter.FilterOp, rawColVal, false, holderDte)
			if retVal {
				multiColReader.IncrementColumnUsage(cname)
				return true, nil
			}
		}
		if atleastOneNonError {
			return false, nil
		} else {
			return false, finalErr
		}
	case MatchDictArraySingleColumn:
		rawColVal, err := multiColReader.ReadRawRecordFromColumnFile(query.QueryInfo.ColName, blockNum, recordNum, qid)
		if err != nil {
			return false, err
		}
		return writer.ApplySearchToDictArrayFilter(query.QueryInfo.KValDte, query.QueryInfo.QValDte, rawColVal, Equals, true, holderDte)
	case MatchDictArrayAllColumns:
		var atleastOneNonError bool
		var finalErr error
		for _, colInfo := range multiColReader.AllColums {

			// we skip rawsearching for columns that are dict encoded,
			// since we already search for them in the prior call to applyColumnarSearchUsingDictEnc
			_, ok := dictEncColNames[colInfo.ColumnName]
			if ok {
				continue
			}

			rawColVal, err := multiColReader.ReadRawRecordFromColumnFile(colInfo.ColumnName, blockNum, recordNum, qid)
			if err != nil {
				finalErr = err
				continue
			} else {
				atleastOneNonError = true
			}
			retVal, _ := writer.ApplySearchToDictArrayFilter(query.QueryInfo.KValDte, query.QueryInfo.QValDte, rawColVal, query.ExpressionFilter.FilterOp, true, holderDte)
			if retVal {
				multiColReader.IncrementColumnUsage(colInfo.ColumnName)
				return true, nil
			}
		}
		if atleastOneNonError {
			return false, nil
		} else {
			return false, finalErr
		}
	// case ComplexExpression:
	//	return // match complex exp
	default:
		log.Errorf("qid=%d, ApplySearchQuery: unsupported query type! %+v", qid, query.SearchType)
		return false, errors.New("unsupported query type")
	}
}

/*
returns doRecLevelSearch, error
if it determines that this query can be fully satisfied by looking at the dict encoded, then
will return doRecLevelSearch=false.
*/
func applyColumnarSearchUsingDictEnc(sq *SearchQuery, mcr *segread.MultiColSegmentReader,
	blockNum uint16, qid uint64, bri *BlockRecordIterator, bsh *BlockSearchHelper,
	searchReq *SegmentSearchRequest, cmiPassedCnames map[string]bool) (bool, map[string]bool, error) {

	dictEncColNames := make(map[string]bool)

	switch sq.SearchType {
	case MatchAll:
		for i := uint(0); i < uint(bri.AllRecLen); i++ {
			bsh.AddMatchedRecord(i)
		}
		return false, dictEncColNames, nil

	case MatchWords:
		isDict, err := mcr.IsBlkDictEncoded(sq.QueryInfo.ColName, blockNum)
		if err != nil {
			return true, dictEncColNames, err
		}

		if !isDict {
			return true, dictEncColNames, nil
		}

		found, err := mcr.ApplySearchToMatchFilterDictCsg(sq.MatchFilter, bsh, sq.QueryInfo.ColName)
		if err != nil {
			log.Errorf("applyColumnarSearchUsingDictEnc: matchwords dict search failed, err=%v", err)
			return false, dictEncColNames, err
		}
		return found, dictEncColNames, err

	case MatchWordsAllColumns:
		for cname := range cmiPassedCnames {

			isDict, err := mcr.IsBlkDictEncoded(cname, blockNum)
			if err != nil {
				continue
			}

			if !isDict {
				continue
			}

			dictEncColNames[cname] = true
			found, err := mcr.ApplySearchToMatchFilterDictCsg(sq.MatchFilter, bsh, cname)
			if err != nil {
				continue
			}
			if found {
				mcr.IncrementColumnUsage(cname)
			}
		}
		return true, dictEncColNames, nil

	case SimpleExpression, RegexExpression:

		isDict, err := mcr.IsBlkDictEncoded(sq.QueryInfo.ColName, blockNum)
		if err != nil {
			return true, dictEncColNames, err
		}

		if !isDict {
			return true, dictEncColNames, nil
		}

		var regex bool
		if sq.SearchType == RegexExpression {
			regex = true
		}

		found, err := mcr.ApplySearchToExpressionFilterDictCsg(sq.QueryInfo.QValDte,
			sq.ExpressionFilter.FilterOp, regex, bsh, sq.QueryInfo.ColName)
		if err != nil {
			log.Errorf("applyColumnarSearchUsingDictEnc: simpleexp/wildrexp dict search failed, err=%v", err)
			return false, dictEncColNames, err
		}
		return found, dictEncColNames, err

	case RegexExpressionAllColumns:
		for cname := range cmiPassedCnames {

			isDict, err := mcr.IsBlkDictEncoded(cname, blockNum)
			if err != nil {
				continue
			}

			if !isDict {
				continue
			}

			dictEncColNames[cname] = true
			found, err := mcr.ApplySearchToExpressionFilterDictCsg(sq.QueryInfo.QValDte,
				sq.ExpressionFilter.FilterOp, true, bsh, cname)
			if err != nil {
				continue
			}
			if found {
				mcr.IncrementColumnUsage(cname)
			}
		}
		return true, dictEncColNames, nil

	case SimpleExpressionAllColumns:
		for cname := range cmiPassedCnames {

			isDict, err := mcr.IsBlkDictEncoded(cname, blockNum)
			if err != nil {
				continue
			}

			if !isDict {
				continue
			}

			dictEncColNames[cname] = true
			found, err := mcr.ApplySearchToExpressionFilterDictCsg(sq.QueryInfo.QValDte,
				sq.ExpressionFilter.FilterOp, false, bsh, cname)
			if err != nil {
				continue
			}
			if found {
				mcr.IncrementColumnUsage(cname)
			}
		}
		return true, dictEncColNames, nil
	case MatchDictArraySingleColumn, MatchDictArrayAllColumns:
		return true, dictEncColNames, nil
	// case ComplexExpression:
	//	return // match complex exp
	default:
		log.Errorf("qid=%d, applyColumnarSearchUsingDictEnc: unsupported query type! %+v", qid, sq.SearchType)
		return true, dictEncColNames, errors.New("unsupported query type")
	}
}
