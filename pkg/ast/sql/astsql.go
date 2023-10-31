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

package sql

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/siglens/siglens/pkg/ast"
	query "github.com/siglens/siglens/pkg/es/query"
	structs "github.com/siglens/siglens/pkg/segment/structs"
	utils "github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
	"github.com/xwb1989/sqlparser"
)

const (
	And = iota
	Or
)

func ConvertToASTNodeSQL(exp string, qid uint64) (*structs.ASTNode, *structs.QueryAggregators, []string, error) {
	exp = formatStringForSQL(exp)
	aggNode := structs.InitDefaultQueryAggregations()
	aggNode.BucketLimit = 100
	astNode, err := query.GetMatchAllASTNode(qid)
	columsArray := make([]string, 0)
	if err != nil {
		log.Errorf("qid=%v, ConvertToASTNodeSQL: match all ast node failed! %+v", qid, err)
		return nil, nil, nil, err
	}

	stmt, err := sqlparser.Parse(exp)
	if err != nil {
		log.Errorf("qid=%v, ConvertToASTNodeSQL: sql parser failed! %+v", qid, err)
		return nil, nil, columsArray, err
	}

	switch currStmt := stmt.(type) {
	case *sqlparser.Select:
		astNode, aggNode, columsArray, err = parseSelect(astNode, aggNode, currStmt, qid)
		if err != nil {
			log.Errorf("qid=%v, ConvertToASTNodeSQL: sql select parsing failed! %+v", qid, err)
			return nil, nil, columsArray, fmt.Errorf("For query:%v, ConvertToASTNodeSQL: sql parser failed! %+v", exp, err)
		}
	case *sqlparser.Show:
		aggNode.ShowRequest = &structs.ShowRequest{}
		if currStmt.ShowTablesOpt != nil {
			aggNode.ShowRequest.ShowTables = true
			aggNode.ShowRequest.ShowFilter = &structs.ShowFilter{Like: ".*"}
			if currStmt.ShowTablesOpt.Filter != nil {
				aggNode.ShowRequest.ShowFilter.Like = currStmt.ShowTablesOpt.Filter.Like
			}
		} else if strings.ContainsAny(currStmt.Type, "COLUMNS") || strings.ContainsAny(currStmt.Type, "columns") {
			inClause, err := getInTable(strings.ReplaceAll(exp, "`", ""))
			if err != nil {
				log.Errorf("qid=%v, ConvertToASTNodeSQL: sql show columns request parsing failed! %+v", qid, err)
				return nil, nil, columsArray, err
			}
			aggNode.ShowRequest.ColumnsRequest = &structs.ShowColumns{}
			aggNode.ShowRequest.ColumnsRequest.InTable = inClause
		} else {
			log.Errorf("qid=%v, ConvertToASTNodeSQL: only SHOW TABLES and SHOW COLUMNS are supported! %+v", qid, err)
			return nil, nil, columsArray, err
		}
	case *sqlparser.OtherRead:
		new_exp, err := parseOtherRead(exp, qid)
		if err != nil {
			log.Errorf("qid=%v, ConvertToASTNodeSQL: sql describe/explain request parsing failed! %+v", qid, err)
			return nil, nil, columsArray, err
		}
		return ConvertToASTNodeSQL(new_exp, qid)
	default:
		err := fmt.Errorf("qid=%v, ConvertToASTNodeSQL: Only SELECT and SHOW commands are supported!", qid)
		return nil, nil, columsArray, err
	}

	return astNode, aggNode, columsArray, err
}

func getInTable(exp string) (string, error) {
	exp = strings.ReplaceAll(exp, " IN ", " in ")
	exp = strings.ReplaceAll(exp, " FROM ", " in ")
	exp = strings.ReplaceAll(exp, " from ", " in ")
	if !strings.Contains(exp, " in ") {
		return "", fmt.Errorf("getInTable: Invalid SHOW COLUMNS request, expected IN or FROM clause followed by argument!")
	}
	inClauses := strings.Split(exp, " in ")
	if len(inClauses) < 2 {
		return "", fmt.Errorf("getInTable: Expected table name after In/From clause!")
	}
	return inClauses[1], nil
}

func getAggregationSQL(agg string, qid uint64) utils.AggregateFunctions {
	agg = strings.ToLower(agg)
	switch agg {
	case "count":
		return utils.Count
	case "avg":
		return utils.Avg
	case "min":
		return utils.Min
	case "max":
		return utils.Max
	case "sum":
		return utils.Sum
	case "cardinality":
		return utils.Cardinality
	default:
		log.Errorf("qid=%v, getAggregationSQL: aggregation type not supported!", qid)
		return 0
	}
}

func parseSingleCondition(expr sqlparser.Expr, astNode *structs.ASTNode, qid uint64, condType int) (*structs.ASTNode, error) {
	clause := strings.Split(sqlparser.String(expr), " ")
	if len(clause) > 2 {
		columnName := clause[0]
		literal := strings.Join(clause[2:], " ")
		var criteria []*structs.FilterCriteria
		var err error
		switch val := utils.GetLiteralFromString(literal).(type) {
		case string:
			val = strings.ReplaceAll(val, "'", "")
			val = strings.ReplaceAll(val, "\"", "")
			criteria, err = ast.ProcessSingleFilter(columnName, val, clause[1], false, qid)
		default:
			criteria, err = ast.ProcessSingleFilter(columnName, json.Number(literal), clause[1], false, qid)
		}

		if err != nil {
			log.Errorf("qid=%v, parseSingleCondition: process pipe search failed! %+v", qid, err)
			return nil, err
		}

		filtercond := &structs.Condition{
			FilterCriteria: []*structs.FilterCriteria(criteria),
		}

		if condType == And {
			astNode.AndFilterCondition = filtercond
		} else if condType == Or {
			astNode.OrFilterCondition = filtercond
		}

	}

	return astNode, nil
}

func parseAndConditionSQL(astNode *structs.ASTNode, expr *sqlparser.AndExpr, qid uint64) {
	subNode, err := parseSingleCondition(expr.Left, &structs.ASTNode{}, qid, And)
	if err != nil {
		log.Errorf("qid=%v, parseAndConditionSQL: parse single condition failed! %+v", qid, err)
		return
	}
	if astNode.AndFilterCondition.NestedNodes == nil {
		astNode.AndFilterCondition.NestedNodes = []*structs.ASTNode{subNode}
	} else {
		astNode.AndFilterCondition.NestedNodes = append(astNode.AndFilterCondition.NestedNodes, subNode)
	}
	switch next := expr.Right.(type) {
	case *sqlparser.AndExpr:
		parseAndConditionSQL(astNode, next, qid)
	case *sqlparser.OrExpr:
		parseOrConditionSQL(astNode, next, qid)
	case *sqlparser.ComparisonExpr:
		rightNode, err := parseSingleCondition(expr.Right, &structs.ASTNode{}, qid, And)
		if err != nil {
			log.Errorf("qid=%v, parseAndConditionSQL: parse single condition failed! %+v", qid, err)
			return
		}
		astNode.AndFilterCondition.NestedNodes = append(astNode.AndFilterCondition.NestedNodes, rightNode)
		return
	case *sqlparser.ParenExpr:
		switch child := next.Expr.(type) {
		case *sqlparser.AndExpr:
			parseAndConditionSQL(astNode, child, qid)
		case *sqlparser.OrExpr:
			parseOrConditionSQL(astNode, child, qid)
		case *sqlparser.ComparisonExpr:
			rightNode, err := parseSingleCondition(expr.Right, &structs.ASTNode{}, qid, And)
			if err != nil {
				log.Errorf("qid=%v, parseAndConditionSQL: parse single condition failed! %+v", qid, err)
				return
			}
			astNode.AndFilterCondition.NestedNodes = append(astNode.AndFilterCondition.NestedNodes, rightNode)
			return
		}

	}

}

func parseSelect(astNode *structs.ASTNode, aggNode *structs.QueryAggregators, currStmt *sqlparser.Select, qid uint64) (*structs.ASTNode, *structs.QueryAggregators, []string, error) {
	newGroupByReq := &structs.GroupByRequest{GroupByColumns: make([]string, 0), MeasureOperations: make([]*structs.MeasureAggregator, 0)}
	measureOps := make([]*structs.MeasureAggregator, 0)
	columsArray := make([]string, 0)
	hardcodedArray := make([]string, 0)
	renameCols := map[string]string{}
	renameHardcodedCols := map[string]string{}
	var err error
	tableName := "*"
	if len(currStmt.From) > 1 {
		return astNode, aggNode, columsArray, fmt.Errorf("qid=%v, parseSelect: FROM clause has too many arguments! Only one table selection is supported", qid)
	}
	if currStmt.From != nil && len(currStmt.From) != 0 && sqlparser.String(currStmt.From[0]) != "dual" {
		tableName = strings.ReplaceAll(sqlparser.String(currStmt.From[0]), "`", "")
	}
	aggNode.TableName = tableName
	for index := range currStmt.SelectExprs {
		switch alias := currStmt.SelectExprs[index].(type) {
		case *sqlparser.AliasedExpr:
			var label string
			if len(alias.As.CompliantName()) > 0 {
				if strings.Contains(sqlparser.String(alias.As), "`") {
					label = strings.Trim(sqlparser.String(alias.As), "`")
				} else {
					label = alias.As.CompliantName()
				}
			}
			switch agg := alias.Expr.(type) {
			case *sqlparser.ColName:
				columsArray = append(columsArray, agg.Name.CompliantName())
				if len(label) != 0 {
					renameCols[agg.Name.CompliantName()] = label
				}
			case *sqlparser.FuncExpr:
				measureOp := &structs.MeasureAggregator{
					MeasureCol: sqlparser.String(agg.Exprs), MeasureFunc: getAggregationSQL(agg.Name.CompliantName(), qid),
				}
				measureOps = append(measureOps, measureOp)
				newGroupByReq.MeasureOperations = append(newGroupByReq.MeasureOperations, measureOp)
				if len(label) != 0 {
					renameCols[strings.ToLower(sqlparser.String(agg))] = label
				}
			case *sqlparser.SQLVal:
				if len(label) != 0 {
					renameHardcodedCols[sqlparser.String(agg)] = label
				} else {
					renameHardcodedCols[sqlparser.String(agg)] = sqlparser.String(agg)

				}

				hardcodedArray = append(hardcodedArray, sqlparser.String(agg))

			default:
				return astNode, aggNode, columsArray, fmt.Errorf("qid=%v, parseSelect: Unsupported Select expression type!", qid)
			}

		case *sqlparser.StarExpr:
			break //astNode is defaulted to matchall, so no further action is needed
		default:
			return astNode, aggNode, columsArray, fmt.Errorf("only star expressions and regualar expressions are handled")

		}

	}

	if len(columsArray) > 0 {

		aggNode.OutputTransforms = &structs.OutputTransforms{OutputColumns: &structs.ColumnsRequest{IncludeColumns: columsArray}}
		aggNode.OutputTransforms.OutputColumns.RenameColumns = renameCols

	}
	if len(hardcodedArray) > 0 {
		if aggNode.OutputTransforms == nil {
			aggNode.OutputTransforms = &structs.OutputTransforms{HarcodedCol: hardcodedArray}
		}
		aggNode.OutputTransforms.HarcodedCol = hardcodedArray
		aggNode.OutputTransforms.RenameHardcodedColumns = renameHardcodedCols
	}

	if len(measureOps) > 0 {
		aggNode.MeasureOperations = measureOps
		if len(columsArray) == 0 && len(hardcodedArray) == 0 {
			aggNode.OutputTransforms = &structs.OutputTransforms{OutputColumns: &structs.ColumnsRequest{}}
		} else if len(columsArray) == 0 && len(hardcodedArray) > 0 {
			aggNode.OutputTransforms.OutputColumns = &structs.ColumnsRequest{}
		}
		aggNode.OutputTransforms.OutputColumns.RenameAggregationColumns = renameCols
	}

	if currStmt.Where != nil {
		switch stmt := (currStmt.Where).Expr.(type) {
		case *sqlparser.OrExpr:
			parseOrConditionSQL(astNode, stmt, qid)
		case *sqlparser.AndExpr:
			parseAndConditionSQL(astNode, stmt, qid)
		case *sqlparser.ComparisonExpr:
			astNode, err = parseSingleCondition(stmt, astNode, qid, And)
			if err != nil {
				log.Errorf("qid=%v, parseSingleCondition: statement failed to be parsed! %+v", qid, err)
				return astNode, aggNode, columsArray, err
			}
		default:
			return astNode, aggNode, columsArray, fmt.Errorf("qid=%v, ConvertToASTNodeSQL: only OR, AND, Comparison types are supported! %+v", qid, err)
		}

	}

	if currStmt.Limit != nil {
		rowLimit, err := strconv.ParseInt(sqlparser.String(currStmt.Limit.Rowcount), 10, 64)
		if err != nil {
			log.Errorf("qid=%v, parseSelect: Limit argument was not an integer!", qid)
			return astNode, aggNode, columsArray, err
		}
		aggNode.BucketLimit = int(rowLimit)
	}

	if currStmt.GroupBy != nil {
		for _, val := range currStmt.GroupBy {
			newGroupByReq.GroupByColumns = append(newGroupByReq.GroupByColumns, sqlparser.String(val))
		}
		aggNode.GroupByRequest = newGroupByReq
		aggNode.GroupByRequest.BucketCount = aggNode.BucketLimit
	}

	if currStmt.OrderBy != nil {
		if len(currStmt.OrderBy) != 1 {
			return astNode, aggNode, columsArray, fmt.Errorf("qid=%v, ConvertToASTNodeSQL: Incorred Order By clause number! Only one clause is supported %+v", qid, err)
		}
		orderByClause := currStmt.OrderBy[0]
		ascending := orderByClause.Direction == sqlparser.AscScr
		aggNode.Sort = &structs.SortRequest{ColName: sqlparser.String(orderByClause.Expr), Ascending: ascending}

	}
	return astNode, aggNode, columsArray, nil
}

func parseOrConditionSQL(astNode *structs.ASTNode, expr *sqlparser.OrExpr, qid uint64) {
	subNode, err := parseSingleCondition(expr.Left, &structs.ASTNode{}, qid, And)
	if err != nil {
		log.Errorf("qid=%v, parseOrConditionSQL: parse single condition failed! %+v", qid, err)
		return
	}
	if astNode.OrFilterCondition == nil {
		astNode.OrFilterCondition = &structs.Condition{}
	}

	if astNode.OrFilterCondition.NestedNodes == nil {
		astNode.OrFilterCondition.NestedNodes = []*structs.ASTNode{subNode}
	} else {
		astNode.OrFilterCondition.NestedNodes = append(astNode.OrFilterCondition.NestedNodes, subNode)
	}

	switch next := expr.Right.(type) {
	case *sqlparser.OrExpr:
		parseOrConditionSQL(astNode, next, qid)
	case *sqlparser.AndExpr:
		parseAndConditionSQL(astNode, next, qid)
	case *sqlparser.ComparisonExpr:
		rightNode, err := parseSingleCondition(expr.Right, &structs.ASTNode{}, qid, Or)
		if err != nil {
			log.Errorf("qid=%v, parseOrConditionSQL: parse single condition failed! %+v", qid, err)
			return
		}
		astNode.OrFilterCondition.NestedNodes = append(astNode.AndFilterCondition.NestedNodes, rightNode)
		return
	case *sqlparser.ParenExpr:
		switch child := next.Expr.(type) {
		case *sqlparser.AndExpr:
			parseAndConditionSQL(astNode, child, qid)
		case *sqlparser.OrExpr:
			parseOrConditionSQL(astNode, child, qid)
		case *sqlparser.ComparisonExpr:
			rightNode, err := parseSingleCondition(child, &structs.ASTNode{}, qid, Or)
			if err != nil {
				log.Errorf("qid=%v, parseOrConditionSQL: parse single condition failed! %+v", qid, err)
				return
			}
			astNode.OrFilterCondition.NestedNodes = append(astNode.AndFilterCondition.NestedNodes, rightNode)
			return
		}

	}

}

func formatStringForSQL(querytext string) string {
	//Add leading and trailing back ticks to words with hyphens
	hyphenRegex := regexp.MustCompile(`[\w` + "`" + `]+-[\w` + "`" + `]+`)
	hyphenWords := hyphenRegex.FindAllString(querytext, -1)
	for _, word := range hyphenWords {
		if !strings.Contains(word, "`") {
			querytext = strings.ReplaceAll(querytext, " "+word, " `"+word+"`")
		}
	}

	if strings.Contains(querytext, "LIKE ") {
		likeClauses := strings.Split(querytext, "LIKE ")
		if len(likeClauses) > 1 {
			likeClauses[1] = "'" + strings.ReplaceAll(likeClauses[1], "`", "") + "'"
			querytext = strings.Join(likeClauses, "LIKE ")
		}

	} else if strings.Contains(querytext, "like ") {
		likeClauses := strings.Split(querytext, "like ")
		if len(likeClauses) > 1 {
			likeClauses[1] = "'" + strings.ReplaceAll(likeClauses[1], "`", "") + "'"
			querytext = strings.Join(likeClauses, "like ")
		}
	}
	return querytext

}

func parseOtherRead(exp string, qid uint64) (string, error) {
	reg := regexp.MustCompile("DESCRIBE |DESC |describe |desc ")
	if !reg.MatchString(exp) {
		return exp, fmt.Errorf("qid=%v, parseOtherRead: Only DESCRIBE is supported!", qid)
	}
	new_exp := reg.ReplaceAllString(exp, "SHOW COLUMNS IN ")
	return new_exp, nil

}
