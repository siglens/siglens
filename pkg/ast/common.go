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

package ast

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/es/query"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/segment"
	segquery "github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/reader/record"
	"github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	log "github.com/sirupsen/logrus"
)

// When valueIsRegex is true, colValue should be a string containing the regex
// to match and should not have quotation marks as the first and last character
// unless those are intended to be matched.
func ProcessSingleFilter(colName string, colValue interface{}, compOpr string, valueIsRegex bool, qid uint64) ([]*FilterCriteria, error) {
	andFilterCondition := make([]*FilterCriteria, 0)
	var opr FilterOperator = Equals
	switch compOpr {
	case ">":
		opr = GreaterThan
	case ">=":
		opr = GreaterThanOrEqualTo
	case "<":
		opr = LessThan
	case "<=":
		opr = LessThanOrEqualTo
	case "=":
		opr = Equals
	case "!=":
		opr = NotEquals
	default:
		log.Errorf("qid=%d, processPipeSearchMap: invalid comparison operator %v", qid, opr)
		return nil, errors.New("processPipeSearchMap: invalid comparison operator")
	}
	switch t := colValue.(type) {
	case string:
		if t != "" {
			if colName == "" || colName == "*" {
				colName = "*"

				if valueIsRegex {
					compiledRegex, err := regexp.Compile(t)
					if err != nil {
						log.Errorf("ProcessSingleFilter: Failed to compile regex for %s. This may cause search failures. Err: %v", t, err)
					}
					criteria := CreateTermFilterCriteria(colName, compiledRegex, opr, qid)
					andFilterCondition = append(andFilterCondition, criteria)
				} else {
					cleanedColVal := strings.ReplaceAll(strings.TrimSpace(t), "\"", "")
					if strings.Contains(t, "\"") {
						criteria := createMatchPhraseFilterCriteria(colName, cleanedColVal, And, qid)
						andFilterCondition = append(andFilterCondition, criteria)
					} else {
						if strings.Contains(t, "*") {
							criteria := CreateTermFilterCriteria(colName, colValue, opr, qid)
							andFilterCondition = append(andFilterCondition, criteria)
						} else {
							criteria := createMatchFilterCriteria(colName, colValue, And, qid)
							andFilterCondition = append(andFilterCondition, criteria)
						}
					}
				}
			} else {
				if valueIsRegex {
					compiledRegex, err := regexp.Compile(t)
					if err != nil {
						log.Errorf("ProcessSingleFilter: Failed to compile regex for %s. This may cause search failures. Err: %v", t, err)
					}
					criteria := CreateTermFilterCriteria(colName, compiledRegex, opr, qid)
					andFilterCondition = append(andFilterCondition, criteria)
				} else {
					cleanedColVal := strings.ReplaceAll(strings.TrimSpace(t), "\"", "")
					criteria := CreateTermFilterCriteria(colName, cleanedColVal, opr, qid)
					andFilterCondition = append(andFilterCondition, criteria)
				}
			}
		} else {
			return nil, errors.New("processPipeSearchMap: colValue/ search Text can not be empty ")
		}
	case json.Number:
		if colValue.(json.Number) != "" {
			if colName == "" {
				colName = "*"
			}
			criteria := CreateTermFilterCriteria(colName, colValue, opr, qid)
			andFilterCondition = append(andFilterCondition, criteria)

		} else {
			return nil, errors.New("processPipeSearchMap: colValue/ search Text can not be empty ")
		}
	case GrepValue:
		cleanedColVal := strings.ReplaceAll(strings.TrimSpace(t.Field), "\"", "")
		criteria := CreateTermFilterCriteria("*", cleanedColVal, opr, qid)
		andFilterCondition = append(andFilterCondition, criteria)
	default:
		log.Errorf("processPipeSearchMap: Invalid colValue type %v", t)
		return nil, errors.New("processPipeSearchMap: Invalid colValue type")
	}
	return andFilterCondition, nil
}

func createMatchPhraseFilterCriteria(k, v interface{}, opr LogicalOperator, qid uint64) *FilterCriteria {
	//match_phrase value will always be string
	var rtInput = strings.TrimSpace(v.(string))
	var matchWords = make([][]byte, 0)
	for _, word := range strings.Split(rtInput, " ") {
		matchWords = append(matchWords, [][]byte{[]byte(word)}...)
	}
	criteria := FilterCriteria{MatchFilter: &MatchFilter{
		MatchColumn:   k.(string),
		MatchWords:    matchWords,
		MatchOperator: opr,
		MatchPhrase:   []byte(rtInput),
		MatchType:     MATCH_PHRASE}}
	return &criteria
}

func createMatchFilterCriteria(k, v interface{}, opr LogicalOperator, qid uint64) *FilterCriteria {
	var rtInput string
	switch vtype := v.(type) {
	case json.Number:
		rtInput = string(vtype)
	case string:
		rtInput = vtype
	default:
		log.Errorf("qid=%d, createMatchFilterCriteria: invalid value ", qid)
	}
	words := strings.Split(rtInput, " ")
	var matchWords = make([][]byte, 0)
	for _, word := range words {
		word = strings.TrimSpace(word)
		if word != "" {
			matchWords = append(matchWords, []byte(word))
		}
	}

	_, ok := k.(string)
	if !ok {
		log.Errorf("qid=%d, createMatchFilterCriteria: invalid type for key %+v", qid, k)
		return nil
	}

	criteria := FilterCriteria{MatchFilter: &MatchFilter{
		MatchColumn:   k.(string),
		MatchWords:    matchWords,
		MatchOperator: opr}}

	return &criteria
}

func CreateTermFilterCriteria(k string, v interface{}, opr FilterOperator, qid uint64) *FilterCriteria {
	cVal, err := CreateDtypeEnclosure(v, qid)
	if err != nil {
		log.Errorf("qid=%d, createTermFilterCriteria: error creating DtypeEnclosure: %+v", qid, err)
	}
	criteria := FilterCriteria{ExpressionFilter: &ExpressionFilter{
		LeftInput: &FilterInput{Expression: &Expression{
			LeftInput: &ExpressionInput{ColumnName: k}}},
		FilterOperator: opr,
		RightInput: &FilterInput{Expression: &Expression{
			LeftInput: &ExpressionInput{ColumnValue: cVal}}}}}
	return &criteria
}

// Executes simple query to return a single column values in a given table
func GetColValues(cname string, table string, qid uint64, orgid uint64) ([]interface{}, error) {
	aggNode := structs.InitDefaultQueryAggregations()
	astNode, err := query.GetMatchAllASTNode(qid)
	if err != nil {
		log.Errorf("qid=%v, GetColValues: match all ast node failed! %+v", qid, err)
		return nil, err
	}
	aggNode.OutputTransforms = &structs.OutputTransforms{OutputColumns: &structs.ColumnsRequest{}}
	aggNode.OutputTransforms.OutputColumns.IncludeColumns = append(make([]string, 0), cname)

	ti := structs.InitTableInfo(table, orgid, false)
	qc := structs.InitQueryContextWithTableInfo(ti, segquery.MAX_GRP_BUCKS, 0, orgid, false)
	queryResult := segment.ExecuteQuery(astNode, aggNode, qid, qc)
	allJsons, _, err := record.GetJsonFromAllRrc(queryResult.AllRecords, false, qid, queryResult.SegEncToKey, aggNode)
	if err != nil {
		log.Errorf("qid=%v, GetColValues: get json from all records failed! %+v", qid, err)
		return nil, err
	}

	colVals := make([]interface{}, 0)
	for _, row := range allJsons {
		colVals = append(colVals, row[cname])
	}

	return colVals, nil
}

func ParseTimeRange(startEpoch, endEpoch uint64, aggs *QueryAggregators, qid uint64) (*dtu.TimeRange, error) {
	tRange := new(dtu.TimeRange)
	if aggs != nil && aggs.TimeHistogram != nil {
		tRange.StartEpochMs = aggs.TimeHistogram.StartTime
		tRange.EndEpochMs = aggs.TimeHistogram.EndTime
		return tRange, nil
	}
	if startEpoch == 0 && endEpoch == 0 {
		//set default time range to last 90 days
		return rutils.GetESDefaultQueryTimeRange(), nil
	} else if startEpoch == 0 || endEpoch == 0 {
		err := fmt.Errorf("parseTimeRange: , startEpoch/ endEpoch not set : %v %v", startEpoch, endEpoch)
		return nil, err
	}
	tRange.StartEpochMs = startEpoch
	tRange.EndEpochMs = endEpoch
	return tRange, nil
}
