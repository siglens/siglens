package colusage

import (
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// Returns:
// - filterCols: columns used only in the initial part of the query.
// - queryCols: all columns used in the query
//
// Example: city=Boston | eval x=latency | eval y=x+1
// filterCols: [city]
// queryCols: [city, latency] (x and y are created by the query, so not included)
func GetFilterAndQueryColumns(astNode *structs.ASTNode, aggs *structs.QueryAggregators) (map[string]struct{}, map[string]struct{}) {
	if aggs != nil {
		if aggs.GentimesExpr != nil {
			return map[string]struct{}{}, map[string]struct{}{}
		}
		if aggs.InputLookupExpr != nil && aggs.InputLookupExpr.IsFirstCommand {
			return map[string]struct{}{}, map[string]struct{}{}
		}
	}

	filterCols := make(map[string]struct{})
	queryCols := make(map[string]struct{})
	createdCols := make(map[string]struct{})

	addFilterCols(astNode, filterCols)
	AddQueryCols(aggs, queryCols, createdCols)

	delete(filterCols, "*")
	delete(queryCols, "*")

	utils.MergeMapsRetainingFirst(queryCols, filterCols)

	return filterCols, queryCols
}

func addFilterCols(astNode *structs.ASTNode, cols map[string]struct{}) {
	if astNode == nil {
		return
	}

	addColsForCondition(astNode.AndFilterCondition, cols)
	addColsForCondition(astNode.OrFilterCondition, cols)
	addColsForCondition(astNode.ExclusionFilterCondition, cols)
}

func addColsForCondition(condition *structs.Condition, cols map[string]struct{}) {
	if condition == nil {
		return
	}

	for _, filter := range condition.FilterCriteria {
		if filter.MatchFilter != nil {
			cols[filter.MatchFilter.MatchColumn] = struct{}{}
		}

		if filter.ExpressionFilter != nil {
			if left := filter.ExpressionFilter.LeftInput; left != nil {
				addFilterCols(left.SubTree, cols)

				if expression := left.Expression; expression != nil {
					if left := expression.LeftInput; left != nil {
						cols[left.ColumnName] = struct{}{}
					}
				}
			}
		}
	}

	for _, astNode := range condition.NestedNodes {
		addFilterCols(astNode, cols)
	}
}

// Note: whenever query support is added for a new command, this function
// should also get updated.
func AddQueryCols(aggs *structs.QueryAggregators, cols map[string]struct{}, createdCols map[string]struct{}) {
	for agg := aggs; agg != nil; agg = agg.Next {
		if agg.BinExpr != nil {
			addBinCols(agg.BinExpr, cols, createdCols)
		}
		if agg.DedupExpr != nil {
			addDedupCols(agg.DedupExpr, cols, createdCols)
		}
		if agg.EvalExpr != nil {
			addEvalCols(agg.EvalExpr, cols, createdCols)
		}
		if agg.FieldsExpr != nil {
			addFieldsCols(agg.FieldsExpr, cols, createdCols)
		}
		if agg.FillNullExpr != nil {
			addFillNullCols(agg.FillNullExpr, cols, createdCols)
		}
		// Ignore gentimes and inputlookup; they don't read columns.
		if agg.HeadExpr != nil {
			addHeadCols(agg.HeadExpr, cols, createdCols)
		}
		if agg.MakeMVExpr != nil {
			addMakeMVCols(agg.MakeMVExpr, cols, createdCols)
		}
		if agg.MVExpandExpr != nil {
			addMVExpandCols(agg.MVExpandExpr, cols, createdCols)
		}
		if agg.RegexExpr != nil {
			addRegexCols(agg.RegexExpr, cols, createdCols)
		}
		if agg.RenameExp != nil {
			addRenameCols(agg.RenameExp, cols, createdCols)
		}
		if agg.RexExpr != nil {
			addRexCols(agg.RexExpr, cols, createdCols)
		}
		if agg.SortExpr != nil {
			addSortCols(agg.SortExpr, cols, createdCols)
		}
		if agg.StatsExpr != nil {
			addStatsCols(agg.StatsExpr, cols, createdCols)
		}
		if agg.StreamstatsExpr != nil {
			// Streamstats has a "reset_after" option that can reference
			// columns created by the streamstats. Currently, we parse a
			// streamstats with a rename as two separate commands, so we need
			// to pass the next agg if it's a Rename.
			streamstats := agg.StreamstatsExpr
			var rename *structs.RenameExp
			if agg.Next != nil && agg.Next.RenameExp != nil {
				rename = agg.Next.RenameExp
				agg = agg.Next
			}

			addStreamstatsCols(streamstats, rename, cols, createdCols)
		}
		if agg.TailExpr != nil {
			addTailCols(agg.TailExpr, cols, createdCols)
		}
		if agg.TimechartExpr != nil {
			addTimechartCols(agg.TimechartExpr, cols, createdCols)
		}
		if agg.StatisticExpr != nil {
			addStatisticCols(agg.StatisticExpr, cols, createdCols)
		}
		if agg.TransactionExpr != nil {
			addTransactionCols(agg.TransactionExpr, cols, createdCols)
		}
		if agg.WhereExpr != nil {
			addWhereCols(agg.WhereExpr, cols, createdCols)
		}
	}
}

func addOrIgnoreColumn(cname string, cols map[string]struct{}, ignoredCols map[string]struct{}) {
	if _, ok := ignoredCols[cname]; !ok {
		cols[cname] = struct{}{}
	}
}

func addOrIgnoreColumns(cnames []string, cols map[string]struct{}, ignoredCols map[string]struct{}) {
	for _, cname := range cnames {
		addOrIgnoreColumn(cname, cols, ignoredCols)
	}
}

func addBinCols(bin *structs.BinCmdOptions, cols map[string]struct{}, createdCols map[string]struct{}) {
	addOrIgnoreColumn(bin.Field, cols, createdCols)

	if newField, ok := bin.NewFieldName.Get(); ok {
		createdCols[newField] = struct{}{}
	}
}

func addDedupCols(dedup *structs.DedupExpr, cols map[string]struct{}, createdCols map[string]struct{}) {
	addOrIgnoreColumns(dedup.FieldList, cols, createdCols)

	for _, sortElement := range dedup.DedupSortEles {
		addOrIgnoreColumn(sortElement.Field, cols, createdCols)
	}
}

func addEvalCols(eval *structs.EvalExpr, cols map[string]struct{}, createdCols map[string]struct{}) {
	addOrIgnoreColumns(eval.ValueExpr.GetFields(), cols, createdCols)
	createdCols[eval.FieldName] = struct{}{}
}

func addFieldsCols(fields *structs.ColumnsRequest, cols map[string]struct{}, createdCols map[string]struct{}) {
	// Nothing to do (it doesn't need to read columns).
}

func addFillNullCols(fillNull *structs.FillNullExpr, cols map[string]struct{}, createdCols map[string]struct{}) {
	addOrIgnoreColumns(fillNull.FieldList, cols, createdCols)
}

func addHeadCols(head *structs.HeadExpr, cols map[string]struct{}, createdCols map[string]struct{}) {
	if boolExpr := head.BoolExpr; boolExpr != nil {
		addOrIgnoreColumns(boolExpr.GetFields(), cols, createdCols)
	}
}

func addMakeMVCols(makeMV *structs.MultiValueColLetRequest, cols map[string]struct{}, createdCols map[string]struct{}) {
	addOrIgnoreColumn(makeMV.ColName, cols, createdCols)
}

func addMVExpandCols(mvExpand *structs.MultiValueColLetRequest, cols map[string]struct{}, createdCols map[string]struct{}) {
	addOrIgnoreColumn(mvExpand.ColName, cols, createdCols)
}

func addRegexCols(regex *structs.RegexExpr, cols map[string]struct{}, createdCols map[string]struct{}) {
	addOrIgnoreColumn(regex.Field, cols, createdCols)
}

func addRenameCols(rename *structs.RenameExp, cols map[string]struct{}, createdCols map[string]struct{}) {
	if rename.RenameExprMode == structs.REMRegex {
		log.Warnf("addRenameCols: REMRegex not supported")
		return
	}

	for oldName, newName := range rename.RenameColumns {
		addOrIgnoreColumn(oldName, cols, createdCols)
		createdCols[newName] = struct{}{}
	}
}

func addRexCols(rex *structs.RexExpr, cols map[string]struct{}, createdCols map[string]struct{}) {
	addOrIgnoreColumn(rex.FieldName, cols, createdCols)

	for _, newCol := range rex.RexColNames {
		createdCols[newCol] = struct{}{}
	}
}

func addSortCols(sort *structs.SortExpr, cols map[string]struct{}, createdCols map[string]struct{}) {
	for _, item := range sort.SortEles {
		addOrIgnoreColumn(item.Field, cols, createdCols)
	}
}

func addStatsCols(stats *structs.StatsExpr, cols map[string]struct{}, createdCols map[string]struct{}) {
	if groupby := stats.GroupByRequest; groupby != nil {
		addOrIgnoreColumns(groupby.GroupByColumns, cols, createdCols)

		for _, measureAgg := range groupby.MeasureOperations {
			addOrIgnoreColumn(measureAgg.MeasureCol, cols, createdCols)
			createdCols[measureAgg.StrEnc] = struct{}{}
		}
	}

	for _, measureAgg := range stats.MeasureOperations {
		addOrIgnoreColumn(measureAgg.MeasureCol, cols, createdCols)
		createdCols[measureAgg.StrEnc] = struct{}{}
	}
}

func addStreamstatsCols(streamstats *structs.StreamStatsOptions, rename *structs.RenameExp,
	cols map[string]struct{}, createdCols map[string]struct{}) {

	if groupby := streamstats.GroupByRequest; groupby != nil {
		addOrIgnoreColumns(groupby.GroupByColumns, cols, createdCols)

		for _, measureAgg := range groupby.MeasureOperations {
			addOrIgnoreColumn(measureAgg.MeasureCol, cols, createdCols)
			createdCols[measureAgg.StrEnc] = struct{}{}
		}
	}

	for _, measureAgg := range streamstats.MeasureOperations {
		addOrIgnoreColumn(measureAgg.MeasureCol, cols, createdCols)
		createdCols[measureAgg.StrEnc] = struct{}{}
	}

	if resetBefore := streamstats.ResetBefore; resetBefore != nil {
		addOrIgnoreColumns(resetBefore.GetFields(), cols, createdCols)
	}

	if rename != nil {
		addRenameCols(rename, cols, createdCols)
	}

	if resetAfter := streamstats.ResetAfter; resetAfter != nil {
		addOrIgnoreColumns(resetAfter.GetFields(), cols, createdCols)
	}
}

func addTailCols(tail *structs.TailExpr, cols map[string]struct{}, createdCols map[string]struct{}) {
	// Nothing to do.
}

func addTimechartCols(timechart *structs.TimechartExpr, cols map[string]struct{}, createdCols map[string]struct{}) {
	if groupBy := timechart.GroupBy; groupBy != nil {
		addOrIgnoreColumns(groupBy.GroupByColumns, cols, createdCols)

		for _, measureAgg := range groupBy.MeasureOperations {
			addOrIgnoreColumn(measureAgg.MeasureCol, cols, createdCols)
			createdCols[measureAgg.StrEnc] = struct{}{}
		}
	}

	if singleAgg := timechart.SingleAgg; singleAgg != nil {
		for _, measureAgg := range singleAgg.MeasureOperations {
			addOrIgnoreColumn(measureAgg.MeasureCol, cols, createdCols)
			createdCols[measureAgg.StrEnc] = struct{}{}
		}
	}

	if timechart.ByField != "" {
		addOrIgnoreColumn(timechart.ByField, cols, createdCols)
	}
}

func addStatisticCols(statistic *structs.StatisticExpr, cols map[string]struct{}, createdCols map[string]struct{}) {
	addOrIgnoreColumns(statistic.GetFields(), cols, createdCols)
}

func addTransactionCols(transaction *structs.TransactionArguments, cols map[string]struct{}, createdCols map[string]struct{}) {
	addOrIgnoreColumns(transaction.Fields, cols, createdCols)

	if start := transaction.StartsWith; start != nil {
		if expr := start.EvalBoolExpr; expr != nil {
			addOrIgnoreColumns(expr.GetFields(), cols, createdCols)
		}
	}

	if end := transaction.EndsWith; end != nil {
		if expr := end.EvalBoolExpr; expr != nil {
			addOrIgnoreColumns(expr.GetFields(), cols, createdCols)
		}
	}
}

func addWhereCols(where *structs.BoolExpr, cols map[string]struct{}, createdCols map[string]struct{}) {
	addOrIgnoreColumns(where.GetFields(), cols, createdCols)
}
