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
	"github.com/valyala/fasthttp"
)

type CaseConversionInfo struct {
	caseInsensitive  bool
	isTerm           bool
	valueIsRegex     bool
	IsString         bool
	colValue         interface{}
	originalColValue interface{}
}

func (cci *CaseConversionInfo) ShouldAlsoSearchWithOriginalCase() bool {
	return cci.IsString && cci.caseInsensitive && !cci.valueIsRegex && cci.colValue != cci.originalColValue
}

// When valueIsRegex is true, colValue should be a string containing the regex
// to match and should not have quotation marks as the first and last character
// unless those are intended to be matched.
// If forceCaseSensitive is set to true, caseInsensitive will be ignored
func ProcessSingleFilter(colName string, colValue interface{}, originalColValue interface{}, compOpr string, valueIsRegex bool, caseInsensitive bool, isTerm bool, forceCaseSensitive bool, qid uint64) ([]*FilterCriteria, error) {
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
		log.Errorf("qid=%d, ProcessSingleFilter: invalid comparison operator %v", qid, opr)
		return nil, errors.New("ProcessSingleFilter: invalid comparison operator")
	}

	if forceCaseSensitive {
		caseInsensitive = false
		if originalColValue != nil {
			colValue = originalColValue
		}
	}

	caseConversion := &CaseConversionInfo{
		caseInsensitive:  caseInsensitive,
		isTerm:           isTerm,
		valueIsRegex:     valueIsRegex,
		colValue:         colValue,
		originalColValue: originalColValue,
	}

	switch t := colValue.(type) {
	case string:
		caseConversion.IsString = true
		if t != "" {
			if colName == "" || colName == "*" {
				colName = "*"

				if valueIsRegex {
					compiledRegex, err := regexp.Compile(t)
					if err != nil {
						log.Errorf("qid=%d, ProcessSingleFilter: Failed to compile regex for %s. This may cause search failures. Err: %v", qid, t, err)
						return nil, fmt.Errorf("invalid regex: %s", t)
					}
					criteria := CreateTermFilterCriteria(colName, compiledRegex, opr, qid, caseConversion)
					andFilterCondition = append(andFilterCondition, criteria)
				} else {
					negateMatch := (opr == NotEquals)
					if opr != Equals && opr != NotEquals {
						log.Errorf("qid=%d, ProcessSingleFilter: invalid string comparison operator %v", qid, opr)
					}

					cleanedColVal := strings.ReplaceAll(strings.TrimSpace(t), "\"", "")
					if originalColValue != nil {
						caseConversion.originalColValue = strings.ReplaceAll(strings.TrimSpace(originalColValue.(string)), "\"", "")
					}
					if strings.Contains(t, "\"") {
						criteria := createMatchPhraseFilterCriteria(colName, cleanedColVal, And, negateMatch, caseConversion)
						andFilterCondition = append(andFilterCondition, criteria)
					} else {
						if strings.Contains(t, "*") {
							criteria := CreateTermFilterCriteria(colName, colValue, opr, qid, caseConversion)
							andFilterCondition = append(andFilterCondition, criteria)
						} else {
							criteria := createMatchFilterCriteria(colName, colValue, And, negateMatch, qid, caseConversion)
							andFilterCondition = append(andFilterCondition, criteria)
						}
					}
				}
			} else {
				if valueIsRegex {
					compiledRegex, err := regexp.Compile(t)
					if err != nil {
						log.Errorf("qid=%d, ProcessSingleFilter: Failed to compile regex for %s. This may cause search failures. Err: %v", qid, t, err)
						return nil, fmt.Errorf("invalid regex: %s", t)
					}
					criteria := CreateTermFilterCriteria(colName, compiledRegex, opr, qid, caseConversion)
					andFilterCondition = append(andFilterCondition, criteria)
				} else {
					cleanedColVal := strings.ReplaceAll(strings.TrimSpace(t), "\"", "")
					if originalColValue != nil {
						caseConversion.originalColValue = strings.ReplaceAll(strings.TrimSpace(originalColValue.(string)), "\"", "")
					}
					criteria := CreateTermFilterCriteria(colName, cleanedColVal, opr, qid, caseConversion)
					andFilterCondition = append(andFilterCondition, criteria)
				}
			}
		} else {
			return nil, errors.New("ProcessSingleFilter: colValue/ search Text can not be empty ")
		}
	case bool:
		criteria := CreateTermFilterCriteria(colName, colValue, opr, qid, caseConversion)
		andFilterCondition = append(andFilterCondition, criteria)
	case json.Number:
		if colValue.(json.Number) != "" {
			if colName == "" {
				colName = "*"
			}
			criteria := CreateTermFilterCriteria(colName, colValue, opr, qid, caseConversion)
			andFilterCondition = append(andFilterCondition, criteria)

		} else {
			return nil, errors.New("ProcessSingleFilter: colValue/ search Text can not be empty ")
		}
	case GrepValue:
		caseConversion.IsString = true
		cleanedColVal := strings.ReplaceAll(strings.TrimSpace(t.Field), "\"", "")
		criteria := CreateTermFilterCriteria("*", cleanedColVal, opr, qid, caseConversion)
		andFilterCondition = append(andFilterCondition, criteria)
	default:
		log.Errorf("qid=%d, ProcessSingleFilter: Invalid colValue type. ColValue=%v, ColValueType=%T", qid, t, t)
		return nil, errors.New("ProcessSingleFilter: Invalid colValue type")
	}
	andFilterCondition[0].FilterIsCaseInsensitive = caseInsensitive
	andFilterCondition[0].FilterIsTerm = isTerm
	return andFilterCondition, nil
}

func createMatchPhraseFilterCriteria(k, v interface{}, opr LogicalOperator, negateMatch bool, cci *CaseConversionInfo) *FilterCriteria {
	// match_phrase value will always be string
	rtInput := strings.TrimSpace(v.(string))
	matchWords := make([][]byte, 0)
	for _, word := range strings.Split(rtInput, " ") {
		matchWords = append(matchWords, [][]byte{[]byte(word)}...)
	}
	var originalRtInput string
	var matchWordsOriginal [][]byte
	if cci != nil && cci.ShouldAlsoSearchWithOriginalCase() {
		originalRtInput = strings.TrimSpace(cci.originalColValue.(string))
		matchWordsOriginal = make([][]byte, len(matchWords))
		for _, word := range strings.Split(originalRtInput, " ") {
			matchWordsOriginal = append(matchWordsOriginal, [][]byte{[]byte(word)}...)
		}
	}
	criteria := FilterCriteria{MatchFilter: &MatchFilter{
		MatchColumn:   k.(string),
		MatchWords:    matchWords,
		MatchOperator: opr,
		MatchPhrase:   []byte(rtInput),
		MatchType:     MATCH_PHRASE,
		NegateMatch:   negateMatch,
	}}

	if len(matchWordsOriginal) > 0 {
		criteria.MatchFilter.MatchWordsOriginal = matchWordsOriginal
	}

	if len(originalRtInput) > 0 {
		criteria.MatchFilter.MatchPhraseOriginal = []byte(originalRtInput)
	}

	return &criteria
}

func createMatchFilterCriteria(colName, colValue interface{}, opr LogicalOperator, negateMatch bool, qid uint64, cci *CaseConversionInfo) *FilterCriteria {
	var rtInput string
	switch vtype := colValue.(type) {
	case json.Number:
		rtInput = string(vtype)
	case string:
		rtInput = vtype
	default:
		log.Errorf("qid=%d, createMatchFilterCriteria: invalid Column value. Value=%v, ValueType=%v ", qid, colValue, vtype)
	}
	words := strings.Split(rtInput, " ")
	matchWords := make([][]byte, 0)
	for _, word := range words {
		word = strings.TrimSpace(word)
		if word != "" {
			matchWords = append(matchWords, []byte(word))
		}
	}

	_, ok := colName.(string)
	if !ok {
		log.Errorf("qid=%d, createMatchFilterCriteria: colName=%v is expected to be of type string but got %T", qid, colName, colName)
		return nil
	}

	var matchWordsOriginal [][]byte
	if cci != nil && cci.ShouldAlsoSearchWithOriginalCase() {
		matchWordsOriginal = make([][]byte, len(matchWords))
		for _, word := range strings.Split(cci.originalColValue.(string), " ") {
			matchWordsOriginal = append(matchWordsOriginal, []byte(word))
		}
	}

	criteria := FilterCriteria{MatchFilter: &MatchFilter{
		MatchColumn:   colName.(string),
		MatchWords:    matchWords,
		MatchOperator: opr,
		NegateMatch:   negateMatch,
	}}

	if len(matchWordsOriginal) > 0 {
		criteria.MatchFilter.MatchWordsOriginal = matchWordsOriginal
	}

	return &criteria
}

func CreateTermFilterCriteria(colName string, colValue interface{}, opr FilterOperator, qid uint64, cci *CaseConversionInfo) *FilterCriteria {
	cVal, err := CreateDtypeEnclosure(colValue, qid)
	if err != nil {
		log.Errorf("qid=%d, createTermFilterCriteria: error creating DtypeEnclosure for ColValue=%v. Error=%+v", qid, colValue, err)
	} else {
		if cci != nil && !cci.valueIsRegex {
			cVal.UpdateRegexp(cci.caseInsensitive, cci.isTerm)
		}
	}

	var originalCVal *DtypeEnclosure

	if cci != nil && cci.ShouldAlsoSearchWithOriginalCase() {
		originalCVal, err = CreateDtypeEnclosure(cci.originalColValue, qid)
		if err != nil {
			log.Errorf("qid=%d, createTermFilterCriteria: error creating DtypeEnclosure for OriginalColValue=%v. Error=%+v", qid, cci.originalColValue, err)
		}
	}

	criteria := FilterCriteria{ExpressionFilter: &ExpressionFilter{
		LeftInput: &FilterInput{Expression: &Expression{
			LeftInput: &ExpressionInput{ColumnName: colName},
		}},
		FilterOperator: opr,
		RightInput: &FilterInput{Expression: &Expression{
			LeftInput: &ExpressionInput{ColumnValue: cVal, OriginalColumnValue: originalCVal},
		}},
	}}
	return &criteria
}

func getDefaultAstAndAggNode(qid uint64, timeRange *dtu.TimeRange) (*structs.ASTNode, *structs.QueryAggregators, error) {
	aggNode := structs.InitDefaultQueryAggregations()
	astNode, err := query.GetMatchAllASTNode(qid, timeRange)
	if err != nil {
		log.Errorf("qid=%v, GetColValues: match all ast node failed! %+v", qid, err)
		return nil, nil, err
	}
	aggNode.OutputTransforms = &structs.OutputTransforms{OutputColumns: &structs.ColumnsRequest{}}

	return astNode, aggNode, nil
}

// Executes simple query to return a single column values in a given table
func GetColValues(cname string, indexNameIn string, astNode *structs.ASTNode, aggNode *structs.QueryAggregators, timeRange *dtu.TimeRange, qid uint64, orgid int64, ctx *fasthttp.RequestCtx) ([]interface{}, error) {
	var err error

	if astNode == nil {
		astNode, aggNode, err = getDefaultAstAndAggNode(qid, timeRange)
		if err != nil {
			log.Errorf("qid=%v, GetColValues: default ast node failed! %+v", qid, err)
			return nil, err
		}
	} else {
		if aggNode == nil {
			aggNode = structs.InitDefaultQueryAggregations()
		}
		if aggNode.OutputTransforms == nil {
			aggNode.OutputTransforms = &structs.OutputTransforms{OutputColumns: &structs.ColumnsRequest{}}
		}
		if aggNode.OutputTransforms.OutputColumns == nil {
			aggNode.OutputTransforms.OutputColumns = &structs.ColumnsRequest{}
		}
	}

	aggNode.OutputTransforms.OutputColumns.IncludeColumns = append(make([]string, 0), cname)

	ti := structs.InitTableInfo(indexNameIn, orgid, false, ctx)
	qc := structs.InitQueryContextWithTableInfo(ti, segquery.MAX_GRP_BUCKS, 0, orgid, false)
	queryResult := segment.ExecuteQuery(astNode, aggNode, qid, qc)
	allJsons, _, err := record.GetJsonFromAllRrcOldPipeline(queryResult.AllRecords, false, qid, queryResult.SegEncToKey, aggNode, queryResult.AllColumnsInAggs)
	if err != nil {
		log.Errorf("qid=%v, GetColValues: fetching JSON records from All RRC failed! %+v", qid, err)
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
	if aggs != nil && aggs.TimeHistogram != nil && aggs.TimeHistogram.Timechart == nil {
		tRange.StartEpochMs = aggs.TimeHistogram.StartTime
		tRange.EndEpochMs = aggs.TimeHistogram.EndTime
		return tRange, nil
	}
	if startEpoch == 0 && endEpoch == 0 {
		// set default time range to last 90 days
		return rutils.GetESDefaultQueryTimeRange(), nil
	} else if startEpoch == 0 || endEpoch == 0 {
		err := fmt.Errorf("parseTimeRange: startEpoch/endEpoch is not set. Given startEpoch=%v, endEpoch=%v", startEpoch, endEpoch)
		return nil, err
	}
	tRange.StartEpochMs = startEpoch
	tRange.EndEpochMs = endEpoch
	return tRange, nil
}
