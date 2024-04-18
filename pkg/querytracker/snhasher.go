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

package querytracker

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cespare/xxhash"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
)

/*
	  ALGORITHM for creating a unique hash

	   1. In each struct maintain a hid (hashid), it is pre-determined way of creating hash out of
		  of elements of that struct in the sequence they are defined in the go files
	   2. If a struct has sub-structs then we recursively call the hashid func
	   3. if the hid is already present then we use it else we calculate it for that data type
	   4. we keep doing recursive until we get base data types of go like int, bool, string, etc...
	   5. This is a pretty standard way of creating id borrowed from the java world
*/
func GetHashForQuery(n *structs.SearchNode) string {
	return fmt.Sprintf("%v", getHashForSearchNode(n))
}

func GetHashForAggs(a *structs.QueryAggregators) string {
	return fmt.Sprintf("%v", getHashForAggregators(a))
}

func getHashForAggregators(a *structs.QueryAggregators) uint64 {

	if a == nil {
		return 0
	}

	// the only parts of aggs we need to hash are the groupby configs
	// the bucketing/sorting/early exiting does not change pqs vs not, agileTree vs not
	val := fmt.Sprintf("%v:%v:%v",
		getHashForGroupBy(a.GroupByRequest),
		getHashForSegmentStats(a.MeasureOperations),
		getHashForTimeHistogram(a.TimeHistogram),
	)

	return xxhash.Sum64String(val)
}

func getHashForSearchNode(sn *structs.SearchNode) uint64 {

	if sn == nil {
		return 0
	}

	val := fmt.Sprintf("%v:%v:%v",
		getHashForSearchCondition(sn.AndSearchConditions),
		getHashForSearchCondition(sn.OrSearchConditions),
		getHashForSearchCondition(sn.ExclusionSearchConditions),
	)

	return xxhash.Sum64String(val)
}

func getHashForSearchCondition(sc *structs.SearchCondition) uint64 {

	if sc == nil {
		return 0
	}

	sqhids := make([]uint64, len(sc.SearchQueries))
	for _, sq := range sc.SearchQueries {
		sqhids = append(sqhids, getHashForSearchQuery(sq))
	}
	sort.Slice(sqhids, func(i, j int) bool { return sqhids[i] < sqhids[j] })

	snhids := make([]uint64, len(sc.SearchNode))
	for _, sn := range sc.SearchNode {
		snhids = append(snhids, getHashForSearchNode(sn))
	}
	sort.Slice(snhids, func(i, j int) bool { return snhids[i] < snhids[j] })

	var sb strings.Builder
	for _, entry := range sqhids {
		sb.WriteString(fmt.Sprintf("%v:", entry))
	}

	for _, entry := range snhids {
		sb.WriteString(fmt.Sprintf("%v:", entry))
	}

	return xxhash.Sum64String(sb.String())
}

func getHashForSearchQuery(sq *structs.SearchQuery) uint64 {

	if sq == nil {
		return 0
	}

	val := fmt.Sprintf("%v:%v:%v:%v",
		getHashForSearchExpression(sq.ExpressionFilter),
		getHashForMatchFilter(sq.MatchFilter),
		sq.SearchType,
		getHashForQueryInfo(sq.QueryInfo))
	return xxhash.Sum64String(val)
}

func getHashForSearchExpression(se *structs.SearchExpression) uint64 {

	if se == nil {
		return 0
	}

	val := fmt.Sprintf("%v:%v:%v:%v",
		getHashForSearchExpressionInput(se.LeftSearchInput),
		se.FilterOp,
		getHashForSearchExpressionInput(se.RightSearchInput),
		getHashForSearchInfo(se.SearchInfo))
	return xxhash.Sum64String(val)
}

func getHashForMatchFilter(mf *structs.MatchFilter) uint64 {

	if mf == nil {
		return 0
	}

	mwords := make([]string, len(mf.MatchWords))
	for _, w := range mf.MatchWords {
		mwords = append(mwords, string(w))
	}

	sort.Strings(mwords)

	val := fmt.Sprintf("%v:%v:%v:%v:%v",
		mf.MatchColumn,
		mwords,
		mf.MatchOperator,
		mf.MatchPhrase,
		mf.MatchType)

	return xxhash.Sum64String(val)
}

func getHashForQueryInfo(qi *structs.QueryInfo) uint64 {

	if qi == nil {
		return 0
	}

	val := fmt.Sprintf("%v:%v",
		qi.ColName,
		getHashForDtypeEnclosure(qi.QValDte))

	return xxhash.Sum64String(val)
}

func getHashForSearchInfo(si *structs.SearchInfo) uint64 {

	if si == nil {
		return 0
	}

	val := fmt.Sprintf("%v:%v",
		si.ColEncoding,
		getHashForDtypeEnclosure(si.QValDte))

	return xxhash.Sum64String(val)
}

func getHashForDtypeEnclosure(dte *utils.DtypeEnclosure) uint64 {

	if dte == nil {
		return 0
	}

	var val string
	switch dte.Dtype {
	case utils.SS_DT_BOOL:
		val = fmt.Sprintf("%v:%v", dte.Dtype, dte.BoolVal)
	case utils.SS_DT_STRING:
		val = fmt.Sprintf("%v:%v", dte.Dtype, dte.StringVal)
	case utils.SS_DT_UNSIGNED_NUM:
		val = fmt.Sprintf("%v:%v", dte.Dtype, dte.UnsignedVal)
	case utils.SS_DT_SIGNED_NUM:
		val = fmt.Sprintf("%v:%v", dte.Dtype, dte.SignedVal)
	case utils.SS_DT_FLOAT:
		val = fmt.Sprintf("%v:%v", dte.Dtype, dte.FloatVal)
	}

	return xxhash.Sum64String(val)
}

func getHashForSearchExpressionInput(sei *structs.SearchExpressionInput) uint64 {

	if sei == nil {
		return 0
	}

	val := fmt.Sprintf("%v:%v:%v",
		sei.ColumnName,
		getHashForExpression(sei.ComplexRelation),
		getHashForDtypeEnclosure(sei.ColumnValue))

	return xxhash.Sum64String(val)
}

func getHashForExpression(e *structs.Expression) uint64 {

	if e == nil {
		return 0
	}

	val := fmt.Sprintf("%v:%v:%v",
		getHashForExpressionInput(e.LeftInput),
		e.ExpressionOp,
		getHashForExpressionInput(e.RightInput))

	return xxhash.Sum64String(val)
}

func getHashForExpressionInput(ei *structs.ExpressionInput) uint64 {

	if ei == nil {
		return 0
	}

	val := fmt.Sprintf("%v:%v",
		getHashForDtypeEnclosure(ei.ColumnValue),
		ei.ColumnName)

	return xxhash.Sum64String(val)
}

func getHashForGroupBy(r *structs.GroupByRequest) uint64 {
	if r == nil {
		return 0
	}

	val := fmt.Sprintf("%v:%v",
		getHashForGroupByColumns(r.GroupByColumns),
		getHashForMeasureOperations(r.MeasureOperations))
	return xxhash.Sum64String(val)
}

func getHashForSegmentStats(mOps []*structs.MeasureAggregator) uint64 {
	return getHashForMeasureOperations(mOps)
}

func getHashForTimeHistogram(tb *structs.TimeBucket) uint64 {
	if tb == nil {
		return 0
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%v:", tb.EndTime))
	sb.WriteString(fmt.Sprintf("%v:", tb.StartTime))
	sb.WriteString(fmt.Sprintf("%v", tb.IntervalMillis))
	return xxhash.Sum64String(sb.String())
}

func getHashForGroupByColumns(cols []string) uint64 {
	if len(cols) == 0 {
		return 0
	}

	sort.Strings(cols)
	var sb strings.Builder
	for _, entry := range cols {
		sb.WriteString(fmt.Sprintf("%v:", entry))
	}
	return xxhash.Sum64String(sb.String())
}

func getHashForMeasureOperations(measureOps []*structs.MeasureAggregator) uint64 {
	if len(measureOps) == 0 {
		return 0
	}

	temp := make([]string, len(measureOps))
	for idx, m := range measureOps {
		temp[idx] = fmt.Sprintf("%+v-%+v", m.MeasureCol, m.MeasureFunc.String())
	}
	sort.Strings(temp)
	var sb strings.Builder
	for _, entry := range temp {
		sb.WriteString(fmt.Sprintf("%v:", entry))
	}
	return xxhash.Sum64String(sb.String())
}
