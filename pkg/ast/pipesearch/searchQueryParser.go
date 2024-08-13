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

package pipesearch

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"

	"github.com/siglens/siglens/pkg/ast"
	"github.com/siglens/siglens/pkg/ast/logql"
	"github.com/siglens/siglens/pkg/ast/spl"
	"github.com/siglens/siglens/pkg/ast/sql"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	segment "github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/aggregations"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

func ParseRequest(searchText string, startEpoch, endEpoch uint64, qid uint64, queryLanguageType string, indexName string) (*ASTNode, *QueryAggregators, error) {
	var err error
	var queryAggs *QueryAggregators
	var boolNode *ASTNode
	boolNode, queryAggs, err = ParseQuery(searchText, qid, queryLanguageType)
	if err != nil {
		return nil, nil, err
	}

	if boolNode == nil && queryAggs == nil {
		err := fmt.Errorf("qid=%d, ParseRequest: boolNode and queryAggs are nil for searchText: %v", qid, searchText)
		log.Errorf(err.Error())
		return nil, nil, err
	}

	if boolNode.TimeRange == nil {
		tRange, err := ast.ParseTimeRange(startEpoch, endEpoch, queryAggs, qid)
		if err != nil {
			log.Errorf("qid=%d, ParseRequest: parseTimeRange error: %v", qid, err)
			return nil, nil, err
		}
		boolNode.TimeRange = tRange
	}

	//aggs
	if queryAggs != nil {
		// if groupby request or segment stats exist, dont early exist and no sort is needed
		if queryAggs.GroupByRequest != nil && queryAggs.StreamStatsOptions == nil {
			queryAggs.GroupByRequest.BucketCount = 10_000
			queryAggs.EarlyExit = false
			queryAggs.Sort = nil
			if len(queryAggs.GroupByRequest.GroupByColumns) == 1 && queryAggs.GroupByRequest.GroupByColumns[0] == "*" {
				queryAggs.GroupByRequest.GroupByColumns = metadata.GetAllColNames([]string{indexName})
			}
			if queryAggs.TimeHistogram != nil && queryAggs.TimeHistogram.Timechart != nil {
				if queryAggs.TimeHistogram.Timechart.BinOptions != nil &&
					queryAggs.TimeHistogram.Timechart.BinOptions.SpanOptions != nil &&
					queryAggs.TimeHistogram.Timechart.BinOptions.SpanOptions.DefaultSettings {
					spanOptions, err := ast.GetDefaultTimechartSpanOptions(startEpoch, endEpoch, qid)
					if err != nil {
						log.Errorf("qid=%d, ParseRequest: GetDefaultTimechartSpanOptions error: %v", qid, err)
						return nil, nil, err
					}
					queryAggs.TimeHistogram.Timechart.BinOptions.SpanOptions = spanOptions
					queryAggs.TimeHistogram.IntervalMillis = aggregations.GetIntervalInMillis(spanOptions.SpanLength.Num, spanOptions.SpanLength.TimeScalr)
				}
				queryAggs.TimeHistogram.StartTime = startEpoch
				queryAggs.TimeHistogram.EndTime = endEpoch
			}
		} else if queryAggs.MeasureOperations != nil && queryAggs.StreamStatsOptions == nil {
			queryAggs.EarlyExit = false
			queryAggs.Sort = nil
		} else {
			queryAggs.EarlyExit = true
			if queryAggs.Sort == nil {
				queryAggs.Sort = &SortRequest{
					ColName:   config.GetTimeStampKey(),
					Ascending: false,
				}
			}
		}
	} else {
		queryAggs = structs.InitDefaultQueryAggregations()
	}

	segment.LogASTNode(queryLanguageType+"query parser", boolNode, qid)
	segment.LogQueryAggsNode(queryLanguageType+"aggs parser", queryAggs, qid)
	return boolNode, queryAggs, nil
}

func ParseQuery(searchText string, qid uint64, queryLanguageType string) (*ASTNode, *QueryAggregators, error) {

	var boolNode *ASTNode
	var aggNode *QueryAggregators
	var err error

	if queryLanguageType == "SQL" {
		boolNode, aggNode, _, err = sql.ConvertToASTNodeSQL(searchText, qid)
	} else {
		boolNode, aggNode, err = parsePipeSearch(searchText, queryLanguageType, qid)
	}

	if err != nil {
		log.Errorf("qid=%d, ParseQuery: ConvertToASTNodeSQL/parsePipeSearch error: %v", qid, err)
		return nil, nil, err
	}

	return boolNode, aggNode, nil
}

func createMatchAll(qid uint64) *ASTNode {
	rootNode := &ASTNode{}
	colName := "*"
	colValue := "*"
	criteria := ast.CreateTermFilterCriteria(colName, colValue, Equals, qid)
	rootNode.AndFilterCondition = &Condition{FilterCriteria: []*FilterCriteria{criteria}}
	return rootNode
}

func updatePositionForGenEvents(aggs *QueryAggregators) {
	node := aggs
	position := 1
	for node != nil {
		if node.GenerateEvent != nil {
			node.GenerateEvent.EventPosition = position
			position++
		}
		node = node.Next
	}
}

func parsePipeSearch(searchText string, queryLanguage string, qid uint64) (*ASTNode, *QueryAggregators, error) {
	var leafNode *ASTNode
	var res interface{}
	var err error
	if searchText == "*" || searchText == "" {
		leafNode = createMatchAll(qid)
		return leafNode, nil, nil
	}
	//peg parsing to AST tree
	switch queryLanguage {
	case "Pipe QL":
		res, err = Parse("", []byte(searchText))
	case "Log QL":
		res, err = logql.Parse("", []byte(searchText))
	case "Splunk QL":
		res, err = spl.Parse("", []byte(searchText))
	default:
		log.Errorf("qid=%d, parsePipeSearch: Unknown queryLanguage: %v", qid, queryLanguage)
	}

	if err != nil {
		log.Errorf("qid=%d, parsePipeSearch: Error while parsing searchText: %v in queryLanguage: %v, err: %v, parse error: %v", qid, searchText, queryLanguage, err, getParseError(err))
		return nil, nil, getParseError(err)
	}

	result, err := json.MarshalIndent(res, "", "   ")
	if err == nil {
		log.Infof("qid=%d, parsePipeSearch output:\n%v\n", qid, string(result))
	} else {
		log.Infof("qid=%d, parsePipeSearch output:\n%v\n", qid, res)
	}

	queryStruct, ok := res.(ast.QueryStruct)
	if !ok {
		return nil, nil, toputils.TeeErrorf("qid=%d, parsePipeSearch: expected QueryStruct, got %T", qid, res)
	}

	searchNode := queryStruct.SearchFilter
	aggs := queryStruct.PipeCommands
	boolNode := &ASTNode{}
	if searchNode == nil {
		boolNode = createMatchAll(qid)
	}

	searchNode, aggs = optimizeQuery(searchNode, aggs)

	err = SearchQueryToASTnode(searchNode, boolNode, qid)
	if err != nil {
		log.Errorf("qid=%d, parsePipeSearch: SearchQueryToASTnode error: %v", qid, err)
		return nil, nil, err
	}

	if aggs == nil {
		return boolNode, nil, nil
	}

	pipeCommands, err := searchPipeCommandsToASTnode(aggs, qid)
	if err != nil {
		log.Errorf("qid=%d, parsePipeSearch: searchPipeCommandsToASTnode error: %v", qid, err)
		return nil, nil, err
	}

	updatePositionForGenEvents(pipeCommands)

	return boolNode, pipeCommands, nil
}

func optimizeQuery(searchNode *ast.Node, aggs *QueryAggregators) (*ast.Node, *QueryAggregators) {
	searchNode.Simplify()

	if searchNode == nil || aggs == nil {
		return searchNode, aggs
	}

	searchNode, aggs = optimizeStatsEvalQueries(searchNode, aggs)

	return searchNode, aggs

}

// Optimize queries like:
// weekday=Monday | stats count(eval(foo=bar)) avg(eval(latency>1000))
// to:
// weekday=Monday AND (foo=bar OR latency>1000) | stats count(eval(foo=bar)) avg(eval(latency>1000))
func optimizeStatsEvalQueries(searchNode *ast.Node, aggs *QueryAggregators) (*ast.Node, *QueryAggregators) {
	if searchNode == nil || aggs == nil {
		return searchNode, aggs // This optimization doesn't apply.
	}

	if aggs.PipeCommandType != MeasureAggsType {
		return searchNode, aggs // This optimization doesn't apply.
	}

	extraSearchNodes := make([]*ast.Node, 0) // These will be merged with the searchNode at the end.
	for _, measureAgg := range aggs.MeasureOperations {
		if measureAgg.ValueColRequest == nil {
			return searchNode, aggs // This optimization doesn't apply.
		}

		if measureAgg.ValueColRequest.ValueExprMode != VEMBooleanExpr {
			return searchNode, aggs // This optimization doesn't apply.
		}

		extraSearchNode := extractSearchNodeFromBooleanExpr(measureAgg.ValueColRequest.BooleanExpr)
		if extraSearchNode == nil {
			// We can't do this optimization for one of these reasons:
			// - There was an issue extracting the search node
			// - We can optimize this query, but we haven't implemented the optimization yet
			// - We can't optimize this query
			return searchNode, aggs
		}

		extraSearchNodes = append(extraSearchNodes, extraSearchNode)
	}

	joinedExtraSearchNode := ast.JoinNodes(extraSearchNodes, ast.NodeOr)
	searchNode = ast.JoinNodes([]*ast.Node{searchNode, joinedExtraSearchNode}, ast.NodeAnd)

	return searchNode, aggs
}

func extractSearchNodeFromBooleanExpr(boolExpr *BoolExpr) *ast.Node {
	if boolExpr == nil {
		log.Errorf("extractSearchNodeFromBooleanExpr: boolExpr is nil")
		return nil
	}

	if !boolExpr.IsTerminal {
		// TODO: we can actually handle this case.
		return nil
	}

	extraSearchNode := &ast.Node{}
	extraSearchNode.NodeType = ast.NodeTerminal
	extraSearchNode.Comparison.Op = boolExpr.ValueOp

	fieldWasSet := false
	valueWasSet := false

	if boolExpr.LeftValue == nil || boolExpr.RightValue == nil {
		log.Errorf("extractSearchNodeFromBooleanExpr: boolExpr is terminal but left (%v) or right (%v) is nil",
			boolExpr.LeftValue, boolExpr.RightValue)
		return nil
	}

	for _, valueExpr := range []*ValueExpr{boolExpr.LeftValue, boolExpr.RightValue} {
		switch valueExpr.ValueExprMode {
		case VEMNumericExpr:
			numericExpr := valueExpr.NumericExpr
			if numericExpr == nil {
				log.Errorf("extractSearchNodeFromBooleanExpr: numericExpr is nil")
				return nil
			}

			if !numericExpr.IsTerminal {
				// TODO: we can actually handle this case.
				return nil
			}
			if numericExpr.ValueIsField {
				extraSearchNode.Comparison.Field = numericExpr.Value
				fieldWasSet = true
			} else {
				extraSearchNode.Comparison.Values = json.Number(numericExpr.Value)
				valueWasSet = true
			}
		case VEMStringExpr:
			stringExpr := valueExpr.StringExpr
			if stringExpr == nil {
				log.Errorf("extractSearchNodeFromBooleanExpr: stringExpr is nil")
				return nil
			}

			switch stringExpr.StringExprMode {
			case SEMField:
				extraSearchNode.Comparison.Field = stringExpr.FieldName
				fieldWasSet = true
			case SEMRawString:
				extraSearchNode.Comparison.Values = "\"" + stringExpr.RawString + "\""
				valueWasSet = true
			case SEMRawStringList, SEMConcatExpr, SEMTextExpr, SEMFieldList:
				// TODO: we can handle at least some of these.
			default:
				log.Errorf("extractSearchNodeFromBooleanExpr: unknown stringExpr.StringExprMode: %v",
					stringExpr.StringExprMode)
				return nil
			}
		case VEMConditionExpr, VEMBooleanExpr:
			// TODO: can these cases be handled?
			return nil
		default:
			log.Errorf("extractSearchNodeFromBooleanExpr: unknown valueExpr.ValueExprMode: %v", valueExpr.ValueExprMode)
			return nil
		}
	}

	if !fieldWasSet || !valueWasSet {
		log.Errorf("extractSearchNodeFromBooleanExpr: fieldWasSet=%v, valueWasSet=%v; expected both to be true",
			fieldWasSet, valueWasSet)
		return nil
	}

	return extraSearchNode
}

func SearchQueryToASTnode(node *ast.Node, boolNode *ASTNode, qid uint64) error {
	var err error
	if node == nil {
		return nil
	}

	switch node.NodeType {
	case ast.NodeOr:
		err := parseORCondition(node.Left, boolNode, qid)
		if err != nil {
			log.Errorf("qid=%d, SearchQueryToASTnode: Error in parseORCondition for left child, error: %v", qid, err)
			return err
		}

		err = parseORCondition(node.Right, boolNode, qid)
		if err != nil {
			log.Errorf("qid=%d, SearchQueryToASTnode: Error in parseORCondition for right child, error: %v", qid, err)
			return err
		}

	case ast.NodeAnd:
		err := parseANDCondition(node.Left, boolNode, qid)
		if err != nil {
			log.Errorf("qid=%d, SearchQueryToASTnode: Error in parseANDCondition for left child, error: %v", qid, err)
			return err
		}

		err = parseANDCondition(node.Right, boolNode, qid)
		if err != nil {
			log.Errorf("qid=%d, SearchQueryToASTnode: Error in parseANDCondition for right child, error: %v", qid, err)
			return err
		}

	case ast.NodeTerminal:
		criteria, err := ast.ProcessSingleFilter(node.Comparison.Field, node.Comparison.Values, node.Comparison.Op, node.Comparison.ValueIsRegex, qid)
		if err != nil {
			log.Errorf("qid=%d, SearchQueryToASTnode: Error while processing single filter, error: %v", qid, err)
			return err
		}
		filtercond := &Condition{
			FilterCriteria: []*FilterCriteria(criteria),
		}
		if boolNode.AndFilterCondition == nil {
			boolNode.AndFilterCondition = filtercond
		} else {
			boolNode.AndFilterCondition.JoinCondition(filtercond)
		}
	case ast.TimeModifierNode:
		if boolNode.TimeRange == nil {
			boolNode.TimeRange = &dtu.TimeRange{}
		}
		boolNode.TimeRange.StartEpochMs = node.TimeModifiers.StartEpoch
		boolNode.TimeRange.EndEpochMs = node.TimeModifiers.EndEpoch
	default:
		log.Errorf("SearchQueryToASTnode: node type %d not supported", node.NodeType)
		return errors.New("SearchQueryToASTnode: node type not supported")
	}
	return err
}

func searchPipeCommandsToASTnode(node *QueryAggregators, qid uint64) (*QueryAggregators, error) {
	var err error
	var pipeCommands *QueryAggregators
	//todo return array of queryaggs
	if node == nil {
		log.Errorf("qid=%d, searchPipeCommandsToASTnode: search pipe command node can not be nil %v", qid, node)
		return nil, errors.New("searchPipeCommandsToASTnode: search pipe command node is nil ")
	}
	switch node.PipeCommandType {
	case GenerateEventType:
		pipeCommands, err = parseGenerateCmd(node.GenerateEvent, qid)
		if err != nil {
			log.Errorf("qid=%d, searchPipeCommandsToASTnode: parseGenerateCmd error: %v", qid, err)
			return nil, err
		}
	case OutputTransformType:
		pipeCommands, err = parseColumnsCmd(node.OutputTransforms, qid)
		if err != nil {
			log.Errorf("qid=%d, searchPipeCommandsToASTnode: parseColumnsCmd error: %v", qid, err)
			return nil, err
		}
	case MeasureAggsType:
		pipeCommands, err = parseSegLevelStats(node.MeasureOperations, qid)
		if err != nil {
			log.Errorf("qid=%d, searchPipeCommandsToASTnode: parseSegLevelStats error: %v", qid, err)
			return nil, err
		}
	case GroupByType:
		pipeCommands, err = parseGroupBySegLevelStats(node.GroupByRequest, node.BucketLimit, qid)
		if err != nil {
			log.Errorf("qid=%d, searchPipeCommandsToASTnode: parseGroupBySegLevelStats error: %v", qid, err)
			return nil, err
		}
		pipeCommands.TimeHistogram = node.TimeHistogram
	case TransactionType:
		pipeCommands, err = parseTransactionRequest(node.TransactionArguments, qid)
		if err != nil {
			log.Errorf("qid=%d, searchPipeCommandsToASTnode: parseTransactionRequest error: %v", qid, err)
			return nil, err
		}
	case VectorArithmeticExprType:
		pipeCommands = &structs.QueryAggregators{
			EarlyExit:            true,
			PipeCommandType:      VectorArithmeticExprType,
			VectorArithmeticExpr: node.VectorArithmeticExpr,
		}
	default:
		log.Errorf("searchPipeCommandsToASTnode: node type %d not supported", node.PipeCommandType)
		return nil, errors.New("searchPipeCommandsToASTnode: node type not supported")
	}
	pipeCommands.StatsOptions = node.StatsOptions
	pipeCommands.StreamStatsOptions = node.StreamStatsOptions
	if node.Next != nil {
		pipeCommands.Next, err = searchPipeCommandsToASTnode(node.Next, qid)

		if err != nil {
			log.Errorf("qid=%d, searchPipeCommandsToASTnode: failed to parse child node: %v", qid, node.Next)
			return nil, err
		}
	}

	return pipeCommands, nil
}

func parseGroupBySegLevelStats(node *structs.GroupByRequest, bucketLimit int, qid uint64) (*QueryAggregators, error) {
	aggNode := &QueryAggregators{}
	aggNode.PipeCommandType = GroupByType
	aggNode.GroupByRequest = &structs.GroupByRequest{}
	aggNode.GroupByRequest.MeasureOperations = make([]*structs.MeasureAggregator, 0)
	aggNode.BucketLimit = bucketLimit
	for _, parsedMeasureAgg := range node.MeasureOperations {
		var tempMeasureAgg = &MeasureAggregator{}
		tempMeasureAgg.MeasureCol = parsedMeasureAgg.MeasureCol
		tempMeasureAgg.MeasureFunc = parsedMeasureAgg.MeasureFunc
		tempMeasureAgg.ValueColRequest = parsedMeasureAgg.ValueColRequest
		tempMeasureAgg.StrEnc = parsedMeasureAgg.StrEnc
		tempMeasureAgg.Param = parsedMeasureAgg.Param
		aggNode.GroupByRequest.MeasureOperations = append(aggNode.GroupByRequest.MeasureOperations, tempMeasureAgg)
	}
	if node.GroupByColumns != nil {
		aggNode.GroupByRequest.GroupByColumns = node.GroupByColumns
	}
	aggNode.EarlyExit = false
	return aggNode, nil
}

func parseSegLevelStats(node []*structs.MeasureAggregator, qid uint64) (*QueryAggregators, error) {
	aggNode := &QueryAggregators{}
	aggNode.PipeCommandType = MeasureAggsType
	aggNode.MeasureOperations = make([]*structs.MeasureAggregator, 0)
	for _, parsedMeasureAgg := range node {
		var tempMeasureAgg = &MeasureAggregator{}
		tempMeasureAgg.MeasureCol = parsedMeasureAgg.MeasureCol
		tempMeasureAgg.MeasureFunc = parsedMeasureAgg.MeasureFunc
		tempMeasureAgg.ValueColRequest = parsedMeasureAgg.ValueColRequest
		tempMeasureAgg.StrEnc = parsedMeasureAgg.StrEnc
		tempMeasureAgg.Param = parsedMeasureAgg.Param
		aggNode.MeasureOperations = append(aggNode.MeasureOperations, tempMeasureAgg)
	}
	return aggNode, nil
}

func parseGenerateCmd(node *structs.GenerateEvent, qid uint64) (*QueryAggregators, error) {
	aggNode := &QueryAggregators{}
	aggNode.PipeCommandType = GenerateEventType
	aggNode.GenerateEvent = &GenerateEvent{}
	if node == nil {
		return aggNode, nil
	}
	if node.GenTimes != nil {
		aggNode.GenerateEvent.GenTimes = node.GenTimes
	}
	if node.InputLookup != nil {
		aggNode.GenerateEvent.InputLookup = node.InputLookup
	}

	return aggNode, nil
}

func parseTransactionRequest(node *structs.TransactionArguments, qid uint64) (*QueryAggregators, error) {
	aggNode := &QueryAggregators{}
	aggNode.PipeCommandType = TransactionType
	aggNode.TransactionArguments = &TransactionArguments{}
	aggNode.TransactionArguments.Fields = node.Fields
	aggNode.TransactionArguments.StartsWith = node.StartsWith
	aggNode.TransactionArguments.EndsWith = node.EndsWith

	if node.StartsWith != nil {
		if node.StartsWith.SearchNode != nil {
			boolNode := &ASTNode{}
			err := SearchQueryToASTnode(node.StartsWith.SearchNode.(*ast.Node), boolNode, qid)
			if err != nil {
				log.Errorf("qid=%d, parseTransactionRequest: SearchQueryToASTnode error for StartsWith, err: %v", qid, err)
				return nil, err
			}
			aggNode.TransactionArguments.StartsWith.SearchNode = boolNode
		}
	}

	if node.EndsWith != nil {
		if node.EndsWith.SearchNode != nil {
			boolNode := &ASTNode{}
			err := SearchQueryToASTnode(node.EndsWith.SearchNode.(*ast.Node), boolNode, qid)
			if err != nil {
				log.Errorf("qid=%d, parseTransactionRequest: SearchQueryToASTnode error for EndsWith, err: %v", qid, err)
				return nil, err
			}
			aggNode.TransactionArguments.EndsWith.SearchNode = boolNode
		}
	}
	aggNode.EarlyExit = false
	return aggNode, nil
}

func parseColumnsCmd(node *structs.OutputTransforms, qid uint64) (*QueryAggregators, error) {
	aggNode := &QueryAggregators{}
	aggNode.PipeCommandType = OutputTransformType
	aggNode.OutputTransforms = &OutputTransforms{}
	if node == nil {
		return aggNode, nil
	}
	if node.OutputColumns != nil {
		if node.OutputColumns.IncludeColumns != nil {
			aggNode.OutputTransforms.OutputColumns = &ColumnsRequest{}
			aggNode.OutputTransforms.OutputColumns.IncludeColumns = append(aggNode.OutputTransforms.OutputColumns.IncludeColumns, node.OutputColumns.IncludeColumns...)
		}
		if node.OutputColumns.ExcludeColumns != nil {
			aggNode.OutputTransforms.OutputColumns = &ColumnsRequest{}
			aggNode.OutputTransforms.OutputColumns.ExcludeColumns = append(aggNode.OutputTransforms.OutputColumns.ExcludeColumns, node.OutputColumns.ExcludeColumns...)
		}
		if node.OutputColumns.RenameColumns != nil {
			if aggNode.OutputTransforms.OutputColumns == nil {
				aggNode.OutputTransforms.OutputColumns = &ColumnsRequest{}
			}
			aggNode.OutputTransforms.OutputColumns.RenameColumns = make(map[string]string)
			for k, v := range node.OutputColumns.RenameColumns {
				aggNode.OutputTransforms.OutputColumns.RenameColumns[k] = v
			}
		}
		if node.OutputColumns.RenameAggregationColumns != nil {
			if aggNode.OutputTransforms.OutputColumns == nil {
				aggNode.OutputTransforms.OutputColumns = &ColumnsRequest{}
			}
			aggNode.OutputTransforms.OutputColumns.RenameAggregationColumns = make(map[string]string)
			for k, v := range node.OutputColumns.RenameAggregationColumns {
				aggNode.OutputTransforms.OutputColumns.RenameAggregationColumns[k] = v
			}
		}
		if node.OutputColumns.IncludeValues != nil {
			if aggNode.OutputTransforms.OutputColumns == nil {
				aggNode.OutputTransforms.OutputColumns = &ColumnsRequest{}
			}
			aggNode.OutputTransforms.OutputColumns.IncludeValues = node.OutputColumns.IncludeValues
		}
	}
	if node.LetColumns != nil {
		aggNode.OutputTransforms.LetColumns = &LetColumnsRequest{}
		aggNode.OutputTransforms.LetColumns.NewColName = node.LetColumns.NewColName

		if node.LetColumns.SingleColRequest != nil {
			aggNode.OutputTransforms.LetColumns.SingleColRequest = &SingleColLetRequest{}
			aggNode.OutputTransforms.LetColumns.SingleColRequest.CName = node.LetColumns.SingleColRequest.CName
			aggNode.OutputTransforms.LetColumns.SingleColRequest.Oper = node.LetColumns.SingleColRequest.Oper
			aggNode.OutputTransforms.LetColumns.SingleColRequest.Value = node.LetColumns.SingleColRequest.Value
		}
		if node.LetColumns.MultiColsRequest != nil {
			aggNode.OutputTransforms.LetColumns.MultiColsRequest = &MultiColLetRequest{}
			aggNode.OutputTransforms.LetColumns.MultiColsRequest.LeftCName = node.LetColumns.MultiColsRequest.LeftCName
			aggNode.OutputTransforms.LetColumns.MultiColsRequest.Oper = node.LetColumns.MultiColsRequest.Oper
			aggNode.OutputTransforms.LetColumns.MultiColsRequest.RightCName = node.LetColumns.MultiColsRequest.RightCName
		}
		if node.LetColumns.ValueColRequest != nil {
			aggNode.OutputTransforms.LetColumns.ValueColRequest = node.LetColumns.ValueColRequest
		}
		if node.LetColumns.RexColRequest != nil {
			aggNode.OutputTransforms.LetColumns.RexColRequest = node.LetColumns.RexColRequest
		}
		if node.LetColumns.StatisticColRequest != nil {
			aggNode.OutputTransforms.LetColumns.StatisticColRequest = node.LetColumns.StatisticColRequest
		}
		if node.LetColumns.RenameColRequest != nil {
			aggNode.OutputTransforms.LetColumns.RenameColRequest = node.LetColumns.RenameColRequest
		}
		if node.LetColumns.DedupColRequest != nil {
			aggNode.OutputTransforms.LetColumns.DedupColRequest = node.LetColumns.DedupColRequest
		}
		if node.LetColumns.SortColRequest != nil {
			aggNode.OutputTransforms.LetColumns.SortColRequest = node.LetColumns.SortColRequest
		}
		if node.LetColumns.MultiValueColRequest != nil {
			aggNode.OutputTransforms.LetColumns.MultiValueColRequest = node.LetColumns.MultiValueColRequest
		}
		if node.LetColumns.FormatResults != nil {
			aggNode.OutputTransforms.LetColumns.FormatResults = node.LetColumns.FormatResults
		}
		if node.LetColumns.EventCountRequest != nil {
			aggNode.OutputTransforms.LetColumns.EventCountRequest = node.LetColumns.EventCountRequest
		}
		if node.LetColumns.BinRequest != nil {
			aggNode.OutputTransforms.LetColumns.BinRequest = node.LetColumns.BinRequest
		}
		if node.LetColumns.FillNullRequest != nil {
			aggNode.OutputTransforms.LetColumns.FillNullRequest = node.LetColumns.FillNullRequest
		}
	}
	if node.FilterRows != nil {
		aggNode.OutputTransforms.FilterRows = node.FilterRows
	}

	aggNode.OutputTransforms.HeadRequest = node.HeadRequest
	aggNode.OutputTransforms.TailRequest = node.TailRequest

	if aggNode.OutputTransforms.HeadRequest != nil && aggNode.OutputTransforms.HeadRequest.BoolExpr == nil && aggNode.OutputTransforms.HeadRequest.MaxRows != 0 {
		aggNode.Limit = int(aggNode.OutputTransforms.HeadRequest.MaxRows)
	}

	return aggNode, nil
}

func parseORCondition(node *ast.Node, boolNode *ASTNode, qid uint64) error {
	qsSubNode := &ASTNode{}
	if boolNode.OrFilterCondition == nil {
		boolNode.OrFilterCondition = &Condition{}
	}
	switch node.NodeType {
	case ast.NodeOr:
		err := SearchQueryToASTnode(node, qsSubNode, qid)
		if err != nil {
			log.Errorf("qid=%d, parseORCondition: SearchQueryToASTnode error for NodeOr, err: %v", qid, err)
			return err
		}
		if boolNode.OrFilterCondition.NestedNodes == nil {
			boolNode.OrFilterCondition.NestedNodes = []*ASTNode{qsSubNode}
		} else {
			boolNode.OrFilterCondition.NestedNodes = append(boolNode.OrFilterCondition.NestedNodes, qsSubNode)
		}
		return nil
	case ast.NodeAnd:
		err := SearchQueryToASTnode(node, qsSubNode, qid)
		if err != nil {
			log.Errorf("qid=%d, parseORCondition: SearchQueryToASTnode error for NodeAnd, err: %v", qid, err)
			return err
		}
		if boolNode.OrFilterCondition.NestedNodes == nil {
			boolNode.OrFilterCondition.NestedNodes = []*ASTNode{qsSubNode}
		} else {
			boolNode.OrFilterCondition.NestedNodes = append(boolNode.OrFilterCondition.NestedNodes, qsSubNode)
		}
		return nil
	case ast.NodeTerminal:
		criteria, err := ast.ProcessSingleFilter(node.Comparison.Field, node.Comparison.Values, node.Comparison.Op, node.Comparison.ValueIsRegex, qid)
		if err != nil {
			log.Errorf("qid=%d, parseORCondition: Error while processing single filter, err: %v", qid, err)
			return err
		}
		filtercond := &Condition{
			FilterCriteria: []*FilterCriteria(criteria),
		}
		if boolNode.OrFilterCondition == nil {
			boolNode.OrFilterCondition = filtercond
		} else {
			boolNode.OrFilterCondition.JoinCondition(filtercond)
		}
		return nil
	default:
		log.Errorf("parseORCondition: node type %d not supported", node.NodeType)
		return errors.New("parseORCondition: node type not supported")
	}
}
func parseANDCondition(node *ast.Node, boolNode *ASTNode, qid uint64) error {
	qsSubNode := &ASTNode{}
	if boolNode.AndFilterCondition == nil {
		boolNode.AndFilterCondition = &Condition{}
	}
	switch node.NodeType {
	case ast.NodeOr:
		err := SearchQueryToASTnode(node, qsSubNode, qid)
		if err != nil {
			log.Errorf("qid=%d, parseANDCondition: SearchQueryToASTnode error for NodeOr, err: %v", qid, err)
			return err
		}
		if boolNode.AndFilterCondition.NestedNodes == nil {
			boolNode.AndFilterCondition.NestedNodes = []*ASTNode{qsSubNode}
		} else {
			boolNode.AndFilterCondition.NestedNodes = append(boolNode.AndFilterCondition.NestedNodes, qsSubNode)
		}
		return nil
	case ast.NodeAnd:
		err := SearchQueryToASTnode(node, qsSubNode, qid)
		if err != nil {
			log.Errorf("qid=%d, parseANDCondition: SearchQueryToASTnode error for NodeAnd, err: %v", qid, err)
			return err
		}
		if boolNode.AndFilterCondition.NestedNodes == nil {
			boolNode.AndFilterCondition.NestedNodes = []*ASTNode{qsSubNode}
		} else {
			boolNode.AndFilterCondition.NestedNodes = append(boolNode.AndFilterCondition.NestedNodes, qsSubNode)
		}
		return nil
	case ast.NodeTerminal:
		criteria, err := ast.ProcessSingleFilter(node.Comparison.Field, node.Comparison.Values, node.Comparison.Op, node.Comparison.ValueIsRegex, qid)
		if err != nil {
			log.Errorf("qid=%d, parseANDCondition: Error while processing single filter, err: %v", qid, err)
			return err
		}
		filtercond := &Condition{
			FilterCriteria: []*FilterCriteria(criteria),
		}
		if boolNode.AndFilterCondition == nil {
			boolNode.AndFilterCondition = filtercond
		} else {
			boolNode.AndFilterCondition.JoinCondition(filtercond)
		}
		return nil
	case ast.TimeModifierNode:
		if boolNode.TimeRange == nil {
			boolNode.TimeRange = &dtu.TimeRange{}
		}
		boolNode.TimeRange.StartEpochMs = node.TimeModifiers.StartEpoch
		boolNode.TimeRange.EndEpochMs = node.TimeModifiers.EndEpoch
		return nil
	default:
		log.Errorf("parseANDCondition: node type %d not supported", node.NodeType)
		return errors.New("parseANDCondition: node type not supported")
	}
}

func GetFinalSizelimit(aggs *QueryAggregators, sizeLimit uint64) uint64 {
	if aggs != nil && (aggs.GroupByRequest != nil || aggs.MeasureOperations != nil) && aggs.StreamStatsOptions == nil {
		sizeLimit = 0
	} else if aggs.HasDedupBlockInChain() || aggs.HasSortBlockInChain() || aggs.HasGroupByOrMeasureAggsInChain() || aggs.HasTransactionArgumentsInChain() || aggs.HasTailInChain() || aggs.HasBinInChain() || aggs.HasStreamStatsInChain() || aggs.HasGenerateEvent() {
		// 1. Dedup needs state information about the previous records, so we can
		// run into an issue if we show some records, then the user scrolls
		// down to see more and we run dedup on just the new records and add
		// them to the existing ones. To get around this, we can run the query
		// on all of the records initially so that scrolling down doesn't cause
		// another query to run.
		// 2. Sort cmd is similar to Dedup cmd; we need to process all the records at once and extract those with top/rare priority based on requirements.
		// 3. If there's a Rex block in the chain followed by a Stats block, we need to
		// see all the matched records before we apply or calculate the stats.
		sizeLimit = math.MaxUint64
	}

	// If MaxRows is used to limit the number of returned results, set `sizeLimit`
	// to it. Currently MaxRows is only valid as the root QueryAggregators.
	if aggs != nil && aggs.Limit != 0 {
		sizeLimit = uint64(aggs.Limit)
	}

	return sizeLimit
}
