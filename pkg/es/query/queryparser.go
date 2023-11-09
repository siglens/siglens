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

package query

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"

	jsoniter "github.com/json-iterator/go"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/config"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/scroll"
	. "github.com/siglens/siglens/pkg/scroll"
	"github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/structs"
	. "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// flag passed here
func ParseRequest(json_body []byte, qid uint64, isJaegerQuery bool, scrollTimeout ...string) (*ASTNode, *QueryAggregators, uint64, *Scroll, error) {
	var sizeLimit uint64 = 10
	if json_body == nil {
		err := fmt.Errorf("ParseRequest: Error parsing JSON expected a value, got: %v", json_body)
		return nil, structs.InitDefaultQueryAggregations(), sizeLimit, nil, err
	}
	var results map[string]interface{}
	var jsonc = jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := jsonc.NewDecoder(bytes.NewReader(json_body))
	decoder.UseNumber()
	err := decoder.Decode(&results)
	if err != nil {
		log.Errorf("qid=%d, ParseRequest: Invalid json/query: %v", qid, err)
		return nil, structs.InitDefaultQueryAggregations(), sizeLimit, nil, errors.New("ParseRequest: Invalid json/query")
	}
	var leafNode *ASTNode
	var queryAggregations *QueryAggregators
	var sortOrder *SortRequest
	var parsingError error
	var scrollRecord *Scroll
	var scroll_id string

	for key, value := range results {
		switch valtype := value.(type) {
		case map[string]interface{}:
			if key == "query" {
				leafNode, parsingError = parseQuery(value, qid, isJaegerQuery)
			} else if key == "aggs" || key == "aggregations" {
				queryAggregations, parsingError = parseAggregations(value, qid)
			}
		case string:
			if key == "scroll" {
				scrollTimeout[0] = valtype
			} else if key == "scroll_id" {
				scroll_id = valtype
			}
		case json.Number:
			if key == "size" {
				sizeLimit, err = parseSize(value, qid)
				if err != nil {
					log.Errorf("qid=%d, Failed to parse size: %v", qid, err)
				}
				log.Infof("qid=%d, ParseRequest: Limiting the size to [%v]", qid, sizeLimit)
			}
		case []interface{}:
			if key == "sort" {
				sortOrder, parsingError = parseSort(value, qid)
			}
		case bool:
			if key == "rest_total_hits_as_int" {
				if queryAggregations == nil {
					queryAggregations = structs.InitDefaultQueryAggregations()
				}
				queryAggregations.EarlyExit = !valtype
			}
		default:
			if key == "seq_no_primary_term" || key == "version" || key == "stored_fields" ||
				key == "script_fields" || key == "docvalue_fields" || key == "highlight" || key == "_source" ||
				key == "timeout" {
				log.Infof("qid=%d, ParseRequest: Ignoring tags other than query [%v]", qid, key)
			} else {
				log.Errorf("qid=%d, ParseRequest: Invalid query key=[%v]", qid, key)
				return nil, structs.InitDefaultQueryAggregations(), sizeLimit, nil, errors.New("ParseRequest: Invalid Query")
			}
		}
		if parsingError != nil {
			return nil, structs.InitDefaultQueryAggregations(), sizeLimit, nil, parsingError
		}
	}
	if len(scrollTimeout) > 0 && scrollTimeout[0] != "" {
		if scroll_id != "" {
			if !scroll.IsScrollIdValid(scroll_id) {
				return nil, nil, sizeLimit, nil, errors.New("ParseRequest: Scroll Timeout : Invalid Search context")
			}
		}
		timeOut, err := GetScrollTimeOut(scrollTimeout[0], qid)
		if err != nil {
			return nil, structs.InitDefaultQueryAggregations(), sizeLimit, nil, err
		}
		scrollRecord = GetScrollRecord(scroll_id, scrollTimeout[0], sizeLimit)
		scrollRecord.TimeOut = timeOut
		//For scroll query, query body is empty , get sizelimit from scrollRecord
		sizeLimit = scrollRecord.Size
	}

	if sortOrder != nil {
		if queryAggregations != nil {
			queryAggregations.Sort = sortOrder
		} else {
			queryAggregations = &QueryAggregators{
				Sort: sortOrder,
			}
		}
	}

	if queryAggregations == nil {
		queryAggregations = structs.InitDefaultQueryAggregations()
	}

	return leafNode, queryAggregations, sizeLimit, scrollRecord, nil
}

func ParseOpenDistroRequest(json_body []byte, qid uint64, isJaegerQuery bool, scrollTimeout ...string) (*ASTNode, *QueryAggregators, uint64, *Scroll, error) {
	var sizeLimit uint64 = 10
	if json_body == nil {
		err := fmt.Errorf("ParseOpenDistroRequest: Error parsing JSON expected a value, got: %v", json_body)
		return nil, structs.InitDefaultQueryAggregations(), sizeLimit, nil, err
	}
	var results map[string]interface{}
	var jsonc = jsoniter.ConfigCompatibleWithStandardLibrary
	if len(json_body) != 0 {
		decoder := jsonc.NewDecoder(bytes.NewReader(json_body))
		decoder.UseNumber()
		err := decoder.Decode(&results)
		if err != nil {
			log.Errorf("qid=%d, ParseOpenDistroRequest: Invalid json/query: %v", qid, err)
			return nil, structs.InitDefaultQueryAggregations(), sizeLimit, nil, errors.New("ParseOpenDistroRequest: Invalid json/query")
		}
	}
	var leafNode *ASTNode
	var queryAggregations *QueryAggregators
	var sortOrder *SortRequest
	var parsingError error
	var scrollRecord *Scroll
	var scroll_id string

	for key, value := range results {
		switch valtype := value.(type) {
		case map[string]interface{}:
			if key == "query" {
				leafNode, parsingError = parseQuery(value, qid, isJaegerQuery)
			} else if key == "aggs" || key == "aggregations" {
				queryAggregations, parsingError = parseAggregations(value, qid)
			}
		case string:
			if key == "scroll" {
				scrollTimeout[0] = valtype
			} else if key == "scroll_id" {
				scroll_id = valtype
			}
		case json.Number:
			if key == "size" {
				sizeLimit, err := parseSize(value, qid)
				if err != nil {
					log.Errorf("qid=%d, Failed to parse size: %v", qid, err)
				}
				log.Infof("qid=%d, ParseOpenDistroRequest: Limiting the size to [%v]", qid, sizeLimit)
			}
		case []interface{}:
			if key == "sort" {
				sortOrder, parsingError = parseSort(value, qid)
			}
		case bool:
			if key == "rest_total_hits_as_int" {
				if queryAggregations == nil {
					queryAggregations = structs.InitDefaultQueryAggregations()
				}
				queryAggregations.EarlyExit = !valtype
			}
		default:
			if key == "seq_no_primary_term" || key == "version" || key == "stored_fields" ||
				key == "script_fields" || key == "docvalue_fields" || key == "highlight" || key == "_source" ||
				key == "timeout" {
				log.Infof("qid=%d, ParseOpenDistroRequest: Ignoring tags other than query [%v]", qid, key)
			} else {
				log.Errorf("qid=%d, ParseOpenDistroRequest: Invalid query key=[%v]", qid, key)
				return nil, structs.InitDefaultQueryAggregations(), sizeLimit, nil, errors.New("ParseOpenDistroRequest: Invalid Query")
			}
		}
		if parsingError != nil {
			return nil, structs.InitDefaultQueryAggregations(), sizeLimit, nil, parsingError
		}
	}
	if len(scrollTimeout) > 0 && scrollTimeout[0] != "" {
		if scroll_id != "" {
			if !scroll.IsScrollIdValid(scroll_id) {
				return nil, nil, sizeLimit, nil, errors.New("ParseOpenDistroRequest: Scroll Timeout : Invalid Search context")
			}
		}
		timeOut, err := GetScrollTimeOut(scrollTimeout[0], qid)
		if err != nil {
			return nil, structs.InitDefaultQueryAggregations(), sizeLimit, nil, err
		}
		scrollRecord = GetScrollRecord(scroll_id, scrollTimeout[0], sizeLimit)
		scrollRecord.TimeOut = timeOut
		//For scroll query, query body is empty , get sizelimit from scrollRecord
		sizeLimit = scrollRecord.Size
	}

	if sortOrder != nil {
		if queryAggregations != nil {
			queryAggregations.Sort = sortOrder
		} else {
			queryAggregations = &QueryAggregators{
				Sort: sortOrder,
			}
		}
	}

	if queryAggregations == nil {
		queryAggregations = structs.InitDefaultQueryAggregations()
	}

	return leafNode, queryAggregations, sizeLimit, scrollRecord, nil
}

func parseSize(value interface{}, qid uint64) (uint64, error) {
	int64SizeLimit, err := value.(json.Number).Int64()
	if err != nil {
		log.Errorf("qid=%d, Failed to convert [%v] to int64", qid, int64SizeLimit)
		return 0, err
	} else {
		return uint64(int64SizeLimit), nil
	}
}

func parseSort(value interface{}, qid uint64) (*SortRequest, error) {

	switch t := value.(type) {
	case []interface{}:
		if len(t) > 1 {
			log.Errorf("qid=%d, Sort request has more than one requirement", qid)
			return nil, errors.New("sort request has more than one requirement")
		}
		return processSortRequirements(t[0], qid)
	}

	log.Errorf("qid=%d, sort request is not a list", qid)
	return nil, errors.New("sort request is not a list")
}

func processSortRequirements(value interface{}, qid uint64) (*SortRequest, error) {

	switch sort := value.(type) {
	case map[string]interface{}:
		if len(sort) > 1 {
			return nil, errors.New("sort request has more than one column")
		}
		for colName, conditions := range sort {
			request := &SortRequest{
				ColName: colName,
			}
			switch conds := conditions.(type) {
			case map[string]interface{}:
				order, ok := conds["order"]
				if !ok {
					request.Ascending = false // default to descending order
				} else {
					orderStr, ok := order.(string)
					if !ok {
						log.Errorf("qid=%d, order condition in sort is not a string", qid)
						return nil, errors.New("order condition in sort is not a string")
					}
					if orderStr == "asc" {
						request.Ascending = true
					} else if orderStr == "desc" {
						request.Ascending = false
					} else {
						log.Errorf("qid=%d, order condition is not `asc` or `desc`", qid)
						return nil, errors.New("order condition is not `asc` or `desc`")
					}
				}
			}
			return request, nil
		}
	}
	log.Errorf("qid=%d, sort condition is not a map", qid)
	return nil, errors.New("sort condition is not a map")
}

func parseAggregations(json_body interface{}, qid uint64) (*QueryAggregators, error) {

	queryAgg := &QueryAggregators{}
	switch t := json_body.(type) {
	case map[string]interface{}:
		err := processAggregation(t, qid, queryAgg)
		if err != nil {
			return nil, err
		}

		var aggName string
		for key := range t {
			aggName = key
		}
		if queryAgg.GroupByRequest != nil {
			queryAgg.GroupByRequest.AggName = aggName
		} else if queryAgg.TimeHistogram != nil {
			queryAgg.TimeHistogram.AggName = aggName
		}

		tempMeasureAggArray := make([]*MeasureAggregator, 0)
		if queryAgg.GroupByRequest != nil && queryAgg.GroupByRequest.GroupByColumns != nil {
			if queryAgg.GroupByRequest.MeasureOperations == nil {
				for gIdx := range queryAgg.GroupByRequest.GroupByColumns {
					var tempMeasureAgg = &MeasureAggregator{}
					tempMeasureAgg.MeasureCol = queryAgg.GroupByRequest.GroupByColumns[gIdx]
					tempMeasureAgg.MeasureFunc = Count
					tempMeasureAggArray = append(tempMeasureAggArray, tempMeasureAgg)
				}
				queryAgg.GroupByRequest.MeasureOperations = append(queryAgg.GroupByRequest.MeasureOperations, tempMeasureAggArray...)
			}
		}

		return queryAgg, nil
	}
	return nil, nil
}

func processAggregation(params map[string]interface{}, qid uint64, aggNode *QueryAggregators) error {
	for key, value := range params {
		switch aggInfo := value.(type) {
		case map[string]interface{}:
			for aggType, aggField := range aggInfo {
				if isTypeStatisticFunction(aggType) {
					err := processStatisticAggregation(aggType, aggField, key, aggNode)
					if err != nil {
						log.Errorf("QID: %d Error when processing statistic aggregation! %s", qid, err.Error())
						return err
					}
				} else {
					err := processNestedAggregation(aggType, aggField, key, qid, aggNode)
					if err != nil {
						log.Errorf("QID: %d Error when processing bucket aggregation! %s", qid, err.Error())
						return err
					}
				}
			}
		}
	}

	return nil
}

func processNestedAggregation(aggType string, aggField interface{}, key string, qid uint64, aggNode *QueryAggregators) error {
	switch aggType {
	case "date_histogram":
		err := processDateHistogram(aggField, qid, aggNode)
		if err != nil {
			return err
		}
		aggNode.TimeHistogram.AggName = key
		return nil
	case "terms":
		err := processTermsHistogram(aggField, qid, aggNode)
		if err != nil {
			return err
		}
		aggNode.GroupByRequest.AggName = key
		return nil
	case "aggs", "aggregations":
		switch subAgg := aggField.(type) {
		case map[string]interface{}:
			err := processAggregation(subAgg, qid, aggNode)
			if err != nil {
				return err
			}
			return nil
		}
		return errors.New("subaggregation is not a map")

	case "histogram":
		return errors.New("histogram aggregation is not supported")
	case "filters":
		return errors.New("filters aggregation is not supported")
	default:
		return fmt.Errorf("bucket key %+v is not supported", aggType)
	}
}

func isTypeStatisticFunction(aggType string) bool {
	_, err := aggTypeToAggregateFunction(aggType)
	return err == nil
}

func aggTypeToAggregateFunction(aggType string) (AggregateFunctions, error) {
	var aggFunc AggregateFunctions

	if aggType == "avg" {
		aggFunc = Avg
	} else if aggType == "min" {
		aggFunc = Min
	} else if aggType == "max" {
		aggFunc = Max
	} else if aggType == "sum" {
		aggFunc = Sum
	} else if aggType == "cardinality" {
		aggFunc = Cardinality
	} else if aggType == "count" {
		aggFunc = Count
	} else {
		return aggFunc, errors.New("unsupported statistic aggregation type")
	}
	return aggFunc, nil
}

func processStatisticAggregation(aggType string, params interface{}, name string, aggNode *QueryAggregators) error {

	aggFunc, err := aggTypeToAggregateFunction(aggType)
	if err != nil {
		return err
	}

	switch aggInfo := params.(type) {
	case map[string]interface{}:
		if colName, ok := aggInfo["field"]; ok {
			colStr, isStr := colName.(string)
			if !isStr {
				return errors.New("field is not a string for average")
			}
			if aggNode.GroupByRequest == nil {
				aggNode.GroupByRequest = &GroupByRequest{}
				aggNode.GroupByRequest.MeasureOperations = make([]*structs.MeasureAggregator, 0)
			}
			var tempMeasureAgg = &MeasureAggregator{}
			tempMeasureAgg.MeasureCol = colStr
			tempMeasureAgg.MeasureFunc = aggFunc
			aggNode.GroupByRequest.MeasureOperations = append(aggNode.GroupByRequest.MeasureOperations, tempMeasureAgg)
			return nil
		}
	}
	return errors.New("no fields are defined for statistic")
}

// es terms aggregation is parsed into GroupByRequest (same as siglens GroupBy aggregation )
func processTermsHistogram(params interface{}, qid uint64, aggNode *QueryAggregators) error {
	switch t := params.(type) {
	case map[string]interface{}:
		// remove .raw from column names
		for k, v := range t {
			if strings.HasSuffix(k, ".raw") {
				t[strings.TrimSuffix(k, ".raw")] = v
				delete(t, k)
			}
		}
		if aggNode.GroupByRequest == nil {
			aggNode.GroupByRequest = &GroupByRequest{}
		}
		fieldName, ok := t["field"]
		if !ok {
			log.Errorf("qid=%d, Required key 'field' is missing for terms aggregation", qid)
			return errors.New("required key 'field' is missing for terms aggregation")
		}
		fieldStr, ok := fieldName.(string)
		if !ok {
			log.Errorf("qid=%d, Required key 'field' is not a string for terms aggregation", qid)
			return errors.New("required key 'field' is not a string for terms aggregation")
		}
		aggNode.GroupByRequest.GroupByColumns = append(aggNode.GroupByRequest.GroupByColumns, fieldStr)
		size, ok := t["size"]
		var finalSize = 10_000
		if ok {
			cVal, err := CreateDtypeEnclosure(size, qid)
			if err != nil {
				log.Errorf("qid=%d, Error extracting size limit! Defaulting to 10_000. Err: %v", qid, err)
			} else {
				if !cVal.IsNumeric() {
					log.Errorf("qid=%d, Aggregation size limit is not numeric! Defaulting to 10_000. cVal: %+v", qid, cVal)
				} else {
					finalSize = int(cVal.SignedVal)
				}
			}
		}
		aggNode.GroupByRequest.BucketCount = finalSize
		return nil
	}
	return errors.New("unable to extract terms histogram")
}

func processDateHistogram(params interface{}, qid uint64, aggNode *QueryAggregators) error {

	switch t := params.(type) {
	case map[string]interface{}:
		if aggNode.TimeHistogram == nil {
			aggNode.TimeHistogram = &TimeBucket{}
		}
		err := getIntervalForDateHistogram(aggNode.TimeHistogram, t)
		if err != nil {
			log.Errorf("qid=%d, Failed to get interval for date histogram %s, %s", qid, t, err.Error())
			return errors.New("failed to get interval for date histogram")
		}
		_ = getBoundsForDateHistogram(aggNode.TimeHistogram, t["extended_bounds"])
		return nil
	}
	return errors.New("unable to extract date histogram")
}

func getIntervalForDateHistogram(timeHist *TimeBucket, inVal map[string]interface{}) error {
	if inVal == nil {
		return errors.New("inVal was null")
	}

	var val interface{}
	var ok bool

	if val, ok = inVal["interval"]; !ok {
		if val, ok = inVal["fixed_interval"]; !ok {
			if val, ok = inVal["calendar_interval"]; !ok {
				return errors.New("neither 'interval', 'fixed_interval' or 'calender_interval' was present")
			}
		}
	}

	strVal, ok := val.(string)
	if !ok {
		return errors.New("key `interval` is not a string")
	}

	runeStr := []rune(strVal)
	for i := 0; i < len(runeStr); i++ {

		if !unicode.IsDigit(runeStr[i]) {
			numStr := string(runeStr[:i])
			numFloat, err := strconv.ParseFloat(numStr, 64)
			if err != nil {
				return err
			}

			text := string(runeStr[i:])
			switch text {
			case "ms":
				timeHist.IntervalMillis = uint64(numFloat)
			case "s":
				timeHist.IntervalMillis = uint64(numFloat) * 1000
			case "m":
				timeHist.IntervalMillis = uint64(numFloat) * 1000 * 60
			case "h":
				timeHist.IntervalMillis = uint64(numFloat) * 1000 * 60 * 60
			case "d":
				timeHist.IntervalMillis = uint64(numFloat) * 1000 * 60 * 60 * 24
			case "w":
				timeHist.IntervalMillis = uint64(numFloat) * 1000 * 60 * 60 * 24 * 7
			case "M":
				timeHist.IntervalMillis = uint64(numFloat) * 1000 * 60 * 60 * 24 * 7 * 4
			case "q":
				timeHist.IntervalMillis = uint64(numFloat) * 1000 * 60 * 60 * 24 * 7 * 4 * 3
			case "y":
				timeHist.IntervalMillis = uint64(numFloat) * 1000 * 60 * 60 * 24 * 7 * 4 * 3 * 4
			default:
				return errors.New("requested unit is not supported")
			}
			return nil
		}
	}
	return errors.New("invalid interval request! No digits occur")
}

func getBoundsForDateHistogram(timeHist *TimeBucket, val interface{}) error {
	if val == nil {
		return errors.New("key `extended_bounds` not found")
	}
	switch time := val.(type) {
	case map[string]interface{}:
		maxVal, ok := time["max"]
		if !ok {
			return errors.New("`extended_bounds` does not have key max")
		}

		minVal, ok := time["min"]
		if !ok {
			return errors.New("`extended_bounds` does not have key min")
		}

		jsonNumMax, ok := maxVal.(json.Number)
		if !ok {
			return errors.New("invalid type for max val")
		}
		maxUint, err := strconv.ParseUint(jsonNumMax.String(), 10, 64)
		if err != nil {
			return err
		}

		jsonNumMin, ok := minVal.(json.Number)
		if !ok {
			return errors.New("invalid type for min val")
		}
		minUint, err := strconv.ParseUint(jsonNumMin.String(), 10, 64)
		if err != nil {
			return err
		}
		if !utils.IsTimeInMilli(maxUint) {
			maxUint *= 1000
		}
		if !utils.IsTimeInMilli(minUint) {
			minUint *= 1000
		}
		log.Infof("aggregation time histogram range: max %+v min %+v", maxUint, minUint)

		timeHist.EndTime = maxUint
		timeHist.StartTime = minUint

	default:
		log.Errorf("`extended_bounds` is not a map")
		return errors.New("`extended_bounds` is not a map")
	}
	return nil
}

func parseQuery(json_body interface{}, qid uint64, isJaegerQuery bool) (*ASTNode, error) {
	// var err error
	switch t := json_body.(type) {
	case map[string]interface{}:
		for key, value := range t {
			//		leafNode := &ASTNode{}
			switch value.(type) {
			case map[string]interface{}:
				if key == "bool" {
					leafNode, err := parseBool(value, qid, isJaegerQuery)
					if err != nil {
						log.Errorf("qid=%d, parseQuery: Error in parseQuery-parseBool: %v", qid, err)
					}
					return leafNode, err
				} else if key == "match" {
					leafNode, err := parseMatchScroll(value, qid)
					if err != nil {
						log.Errorf("qid=%d, parseQuery: Error in parseQuery-parseMatchScroll: %v", qid, err)
					}
					return leafNode, err
				} else if key == "match_all" {
					leafNode, err := parseMatchall(value, qid)
					if err != nil {
						log.Errorf("qid=%d, parseQuery: Error in parseQuery-parseMatchall: %v", qid, err)
					}
					return leafNode, err
				} else if key == "match_phrase" {
					leafNode, err := parseMatchPhrase(value, qid)
					if err != nil {
						log.Errorf("qid=%d, parseQuery: Error in parseQuery-parseMatchPhrase: %v", qid, err)
					}
					return leafNode, err
				}
			case []map[string]interface{}:
				if key == "bool" {
					leafNode, err := parseBool(value, qid, isJaegerQuery)
					if err != nil {
						log.Errorf("qid=%d, parseQuery: Error in parseQuery-parseBool: %v", qid, err)
					}
					return leafNode, err
				}
			case []interface{}:
				return nil, errors.New("parseQuery: Invalid query,does not support array of values")
			default:
				return nil, errors.New("parseQuery: Invalid Bool query")
			}
		}
	default:
		return nil, errors.New("parseQuery: Invalid Bool query")
	}
	return nil, nil
}

/*
Example Json_body for bool query

	"bool": {
		"must" : [
			{"term" : { "user.id" : "kimchy" }},
			{"term" : { "host.id" : "abc" }},
		  ],
		  "filter": {
			"term" : { "tags" : "production" }
		  }
	}
*/
func parseBool(json_body interface{}, qid uint64, isJaegerQuery bool) (*ASTNode, error) {
	if json_body == nil {
		err := fmt.Errorf("parseBool: Error parsing JSON expected a value, got: %v", json_body)
		return nil, err
	}
	boolNode := &ASTNode{}
	var err error
	//set timeRange
	boolNode.TimeRange = rutils.GetESDefaultQueryTimeRange()
	switch t := json_body.(type) {
	case map[string]interface{}:
		for key, value := range t {
			key = strings.TrimSuffix(key, ".raw") // remove any .raw postfix from column name
			switch key {
			case "must", "filter":
				andFilter, err := parseMustOrFilter(value, boolNode, qid, isJaegerQuery)
				if err != nil {
					return nil, err
				}
				if boolNode.AndFilterCondition == nil {
					boolNode.AndFilterCondition = andFilter
				} else {
					boolNode.AndFilterCondition.JoinCondition(andFilter)
				}
			case "must_not":
				filterCond, err := parseMustNot(value, boolNode, qid, isJaegerQuery)
				if err != nil {
					return nil, err
				}
				if boolNode.ExclusionFilterCondition == nil {
					boolNode.ExclusionFilterCondition = filterCond
				} else {
					boolNode.ExclusionFilterCondition.JoinCondition(filterCond)
				}
			case "should":
				shouldCond, err := parseShould(value, boolNode, qid, isJaegerQuery)
				if err != nil {
					return nil, err
				}
				if boolNode.OrFilterCondition == nil {
					boolNode.OrFilterCondition = shouldCond
				} else {
					boolNode.OrFilterCondition.JoinCondition(shouldCond)
				}
			}
		}
	default:
		err := fmt.Errorf("parseBool: Error parsing bool query, expected a map")
		return nil, err
	}
	// Below if loop -> For exclusionFilterCriteria/must_not only query
	if boolNode.ExclusionFilterCondition != nil && boolNode.AndFilterCondition == nil && boolNode.OrFilterCondition == nil {
		colName := "*"
		colValue := "*"
		criteria := createTermFilterCriteria(colName, colValue, Equals, qid)
		boolNode.AndFilterCondition = &Condition{FilterCriteria: []*FilterCriteria{criteria}}
	}

	return boolNode, err
}

/*
Example Json_body for match_all query

	"query": {
			"match_all": {}
		}
*/
func parseMatchScroll(json_body interface{}, qid uint64) (*ASTNode, error) {
	if json_body == nil {
		err := fmt.Errorf("parseMatchScroll: Error parsing JSON expected a value, got: %v", json_body)
		return nil, err
	}
	rootNode := &ASTNode{}
	var err error
	//set timeRange
	rootNode.TimeRange = rutils.GetESDefaultQueryTimeRange()
	criteria, err := parseMatch(json_body, qid)
	if err != nil {
		err := fmt.Errorf("parseMatchScroll: error creating criteria : %v", err.Error())
		return nil, err
	}
	rootNode.AndFilterCondition = &Condition{FilterCriteria: criteria}
	return rootNode, err
}

func parseMatchall(json_body interface{}, qid uint64) (*ASTNode, error) {
	if json_body == nil {
		err := fmt.Errorf("parseMatchall: Error parsing JSON expected a value, got: %v", json_body)
		return nil, err
	}
	rootNode := &ASTNode{}
	var err error
	//set timeRange
	rootNode.TimeRange = rutils.GetESDefaultQueryTimeRange()
	colName := "*"
	colValue := "*"
	criteria := createTermFilterCriteria(colName, colValue, Equals, qid)
	rootNode.AndFilterCondition = &Condition{FilterCriteria: []*FilterCriteria{criteria}}
	return rootNode, err
}

func GetMatchAllASTNode(qid uint64) (*ASTNode, error) {
	rootNode := &ASTNode{}
	//set timeRange
	rootNode.TimeRange = rutils.GetESDefaultQueryTimeRange()
	colName := "*"
	colValue := "*"

	criteria := createTermFilterCriteria(colName, colValue, Equals, qid)
	rootNode.AndFilterCondition = &Condition{FilterCriteria: []*FilterCriteria{criteria}}
	return rootNode, nil
}

/*
Example Json_body for filter query
{
	"filter": [
			{ "term":  { "status": "published" }},
			{ "range": { "publish_date": { "gte": "2015-01-01" }}}
		  ]
}
*/
/*
Example Json_body for must query
{
	"must" :
		{"term" : { "user.id" : "kimchy" }}
}
*/

func parseMustOrFilter(json_body interface{}, boolNode *ASTNode, qid uint64, isJaegerQuery bool) (*Condition, error) {
	if json_body == nil {
		err := fmt.Errorf("parseMustOrFilter: Error parsing JSON expected a value, got: %v", json_body)
		log.Errorf("qid=%d, parseMustOrFilter: Invalid json/query: %v", qid, err)
	}
	var currCondition *Condition
	switch t := json_body.(type) {
	case map[string]interface{}:
		for key, value := range t {
			if value == nil {
				err := fmt.Errorf("parseMustOrFilter: Error parsing Filter query")
				return nil, err
			}
			filtercond, err := parseLeafNodes(key, value, boolNode, qid, isJaegerQuery)
			if err != nil {
				return nil, err
			}
			if filtercond == nil {
				continue
			}
			if currCondition == nil {
				currCondition = filtercond
			} else {
				currCondition.JoinCondition(filtercond)
			}

		}
	case []interface{}:
		for _, nvalue := range json_body.([]interface{}) {
			switch t := nvalue.(type) {
			case map[string]interface{}:
				for key, value := range t {
					filtercond, err := parseLeafNodes(key, value, boolNode, qid, isJaegerQuery)
					if err != nil {
						return nil, err
					}
					if filtercond == nil {
						continue
					}
					if currCondition == nil {
						currCondition = filtercond
					} else {
						currCondition.JoinCondition(filtercond)
					}
				}
			default:
				err := fmt.Errorf("parseMustOrFilter: Error parsing Filter query")
				return nil, err
			}
		}
	default:
		err := fmt.Errorf("parseMustOrFilter: Error parsing Filter query")
		return nil, err
	}
	return currCondition, nil
}

/*
Example Json_body for nested query
{
  "query": {
		  "bool": {
			"must": {
			  "nested": {
				"path": "tags",
				"query": {
				  "bool": {
					"must": [
					  {
						"match": {
						  "tags.key": {
							"query": "sampler.type"
						  }
						}
					  },
					  {
						"regexp": {
						  "tags.value": {
							"value": "const"
						  }
						}
					  }
					]
				  }
				}
			  }
			}
		  }
		}
	  }
*/

func parseNestedDictArray(json_body interface{}, qid uint64, path string) (string, *DtypeEnclosure, error) {

	if json_body == nil {
		err := fmt.Errorf("qid=%d, parseNestedDictArray: Error parsing JSON, expected a value, got nil", qid)
		log.Error(err)
		return "", nil, err
	}
	var matchKey string
	matchValue := &DtypeEnclosure{}
	var err error
	switch t := json_body.(type) {
	case map[string]interface{}:
		for _, nestedValue := range t {
			switch nestedValue := nestedValue.(type) {
			case map[string]interface{}:
				matchKey, matchValue, err = parseNestedDictArray(nestedValue, qid, path)
				if err != nil {
					return "", nil, err
				}
			case []interface{}:
				for _, nv := range nestedValue {
					switch qv := nv.(type) {
					case map[string]interface{}:
						for k, v := range qv {
							switch v := v.(type) {
							case map[string]interface{}:
								if k == "match" {
									for mk := range v {
										if mk == path+".key" {
											for _, nv := range v {
												switch nv := nv.(type) {
												case map[string]interface{}:
													for qk, qv := range nv {
														switch qv := qv.(type) {
														case string:
															if qk == "query" {
																matchKey = qv
															}
														default:
															return "", nil, errors.New("parseNestedDictArray: Invalid fields in nested match query")
														}
													}
												default:
													return "", nil, errors.New("parseNestedDictArray: Invalid fields in nested match query")
												}
											}
										}
									}
								} else if k == "regexp" {
									for mk := range v {
										if mk == path+".value" {
											for _, nv := range v {
												switch nv := nv.(type) {
												case map[string]interface{}:
													for qk, qv := range nv {
														if qk == "value" {
															matchValue, err = CreateDtypeEnclosure(qv, qid)
															if err != nil {
																log.Errorf("qid=%d, parseNestedDictArray: error creating DtypeEnclosure: %+v", qid, err)
															}
														}
													}
												default:
													return "", nil, errors.New("parseNestedDictArray: Invalid fields in nested match query")
												}
											}
										}
									}
								}
							default:
								return "", nil, errors.New("parseNestedDictArray: Invalid fields in nested match query")
							}
						}
					default:
						return "", nil, errors.New("parseNestedDictArray: Invalid fields in nested match query")
					}
				}
			case string:
				path = nestedValue
			default:
				return "", nil, errors.New("parseNestedDictArray: Invalid fields in nested match query")
			}
		}
	default:
		return "", nil, errors.New("parseNestedDictArray: Invalid fields in nested match query")
	}
	return matchKey, matchValue, nil
}

/*
Example Json_body for should query
{
	"should": [
			{ "term":  { "status": "published" }},
			{ "range": { "publish_date": { "gte": "2015-01-01" }}}
		  ]
}
*/

func parseShould(json_body interface{}, boolNode *ASTNode, qid uint64, isJaegerReq bool) (*Condition, error) {
	if json_body == nil {
		err := fmt.Errorf("parseShould: Error parsing JSON expected a value, got: %v", json_body)
		log.Errorf("qid=%d, parseShould: Invalid json/query: %v", qid, err)
	}
	var currCondition *Condition
	switch t := json_body.(type) {
	case map[string]interface{}:
		for key, value := range t {
			if value == nil {
				err := fmt.Errorf("parseShould: Error parsing Filter query")
				return nil, err
			}
			filtercond, err := parseLeafNodes(key, value, boolNode, qid, isJaegerReq)
			if err != nil {
				return nil, err
			}
			if filtercond == nil {
				continue
			}
			if currCondition == nil {
				currCondition = filtercond
			} else {
				currCondition.JoinCondition(filtercond)
			}
		}
	case []interface{}:
		for _, nvalue := range json_body.([]interface{}) {
			switch t := nvalue.(type) {
			case map[string]interface{}:
				for key, value := range t {
					filtercond, err := parseLeafNodes(key, value, boolNode, qid, isJaegerReq)
					if err != nil {
						return nil, err
					}
					if filtercond == nil {
						continue
					}
					if currCondition == nil {
						currCondition = filtercond
					} else {
						currCondition.JoinCondition(filtercond)
					}
				}
			default:
				err := fmt.Errorf("parseShould: Error parsing should query")
				return nil, err
			}
		}
	default:
		err := fmt.Errorf("parseShould: Error parsing should query")
		return nil, err
	}
	return currCondition, nil
}

/*
Example Json_body for must not query
"must_not" : {
	"range" : {"age" : { "gte" : 10, "lte" : 20 }
	}
  }, */

func parseMustNot(json_body interface{}, boolNode *ASTNode, qid uint64, isJaegerQuery bool) (*Condition, error) {
	if json_body == nil {
		err := fmt.Errorf("parseMustNot: Error parsing JSON expected a value, got: %v", json_body)
		log.Errorf("qid=%d, parseMustNot: Invalid json/query: %v", qid, err)
		return nil, err
	}
	var currCondition *Condition
	switch t := json_body.(type) {
	case map[string]interface{}:
		for key, value := range t {
			if value == nil {
				err := fmt.Errorf("parseMustNot: Error parsing Filter query")
				return nil, err
			}
			filtercond, err := parseLeafNodes(key, value, boolNode, qid, isJaegerQuery)
			if err != nil {
				return nil, err
			}
			if filtercond == nil {
				continue
			}
			if currCondition == nil {
				currCondition = filtercond
			} else {
				currCondition.JoinCondition(filtercond)
			}
		}
	case []interface{}:
		for _, nvalue := range json_body.([]interface{}) {
			switch t := nvalue.(type) {
			case map[string]interface{}:
				for key, value := range t {
					filtercond, err := parseLeafNodes(key, value, boolNode, qid, isJaegerQuery)
					if err != nil {
						return nil, err
					}
					if filtercond == nil {
						continue
					}
					if currCondition == nil {
						currCondition = filtercond
					} else {
						currCondition.JoinCondition(filtercond)
					}
				}
			default:
				err := fmt.Errorf("parseMustNot: Error parsing must_not query")
				return nil, err
			}
		}
	default:
		err := fmt.Errorf("parseMustNot: Error parsing must_not query")
		return nil, err
	}
	return currCondition, nil
}

func parseLeafNodes(key, value interface{}, boolNode *ASTNode, qid uint64, isJaegerQuery bool) (*Condition, error) {
	switch key {
	case "term":
		criteria, err := parseTerm(value, qid)
		if err != nil {
			log.Errorf("qid=%d, parseLeafNodes error: %v", qid, err)
			return nil, err
		}
		return &Condition{
			FilterCriteria: []*FilterCriteria(criteria),
		}, nil

	case "range":
		criteria, tRange, err := parseRange(value, qid, isJaegerQuery)
		if tRange != nil {
			boolNode.TimeRange = tRange
		}
		if err != nil {
			log.Errorf("qid=%d, parseRange error: %v", qid, err)
			return nil, err
		}
		return &Condition{
			FilterCriteria: []*FilterCriteria(criteria),
		}, nil

	case "match":
		criteria, err := parseMatch(value, qid)
		if err != nil {
			log.Errorf("qid=%d, parseMatch error: %v", qid, err)
			return nil, err
		}
		return &Condition{
			FilterCriteria: []*FilterCriteria(criteria),
		}, nil
	case "nested":

		var path string
		andFilterCondition := make([]*FilterCriteria, 0)

		switch t := value.(type) {
		case map[string]interface{}:
			for nestedKey, nestedValue := range t {
				switch nestedValue := nestedValue.(type) {
				case string:
					if nestedKey == "path" {
						path = nestedValue
					}
				case map[string]interface{}:
					mk, mv, err := parseNestedDictArray(value, qid, path)
					if err != nil {
						log.Errorf("qid=%d, parseNestedDictArray error: %v", qid, err)
						return nil, err
					}
					criteria := FilterCriteria{MatchFilter: &MatchFilter{
						MatchColumn: path,
						MatchDictArray: &MatchDictArrayRequest{
							MatchKey:   []byte(mk),
							MatchValue: mv,
						},
						MatchType: MATCH_DICT_ARRAY},
					}
					andFilterCondition = append(andFilterCondition, &criteria)
					return &Condition{
						FilterCriteria: []*FilterCriteria(andFilterCondition),
					}, nil
				}
			}
		}
		return nil, nil
	case "multi_match":
		criteria, err := parseMultiMatch_nested(value, qid)
		if err != nil {
			log.Errorf("qid=%d, parseMultiMatch error: %v", qid, err)
			return nil, err
		}
		return criteria, nil
	case "match_all":
		criteria, err := parseMatchall_nested(value, qid)
		if err != nil {
			log.Errorf("qid=%d, parseMatchall_nested error: %v", qid, err)
			return nil, err
		}
		return &Condition{
			FilterCriteria: []*FilterCriteria(criteria),
		}, nil
	case "match_phrase":
		criteria, err := parseMatchPhrase_nested(value, qid)
		if err != nil {
			log.Errorf("qid=%d, parseMatchall_nested error: %v", qid, err)
			return nil, err
		}
		return &Condition{
			FilterCriteria: []*FilterCriteria(criteria),
		}, nil
	case "terms":
		criteria, err := parseTerms(value, qid)
		if err != nil {
			log.Errorf("qid=%d, parseTerms error: %v", qid, err)
			return nil, err
		}
		return &Condition{
			FilterCriteria: []*FilterCriteria(criteria),
		}, nil

	case "prefix":
		criteria, err := parsePrefix(value, qid)
		if err != nil {
			log.Errorf("qid=%d, parsePrefix error: %v", qid, err)
			return nil, err
		}
		return &Condition{
			FilterCriteria: []*FilterCriteria(criteria),
		}, nil

	case "regexp":
		criteria, err := parseRegexp(value, qid)
		if err != nil {
			log.Errorf("qid=%d, parseRegexp error: %v", qid, err)
			return nil, err
		}
		return &Condition{
			FilterCriteria: []*FilterCriteria(criteria),
		}, nil

	case "wildcard":
		criteria, err := parseWildcard(value, qid)
		if err != nil {
			log.Errorf("qid=%d, parseWildcard error: %v", qid, err)
			return nil, err
		}
		return &Condition{
			FilterCriteria: []*FilterCriteria(criteria),
		}, nil

		// todo hack, the simple_query_string supports bunch more stuff
		// but just to get the kibana interop going, using a simpler parsing of it
	case "query_string", "simple_query_string":
		qsSubNode, criteria, err := parseQuerystring(value, qid)
		// andCond, orCond, err := parseQuerystring(value, qid)
		if err != nil {
			log.Errorf("qid=%d, parseQuerystring error: %v", qid, err)
			return nil, err
		}
		if qsSubNode != nil {
			return &Condition{
				NestedNodes: []*ASTNode{qsSubNode},
			}, nil
		} else {
			return &Condition{
				FilterCriteria: []*FilterCriteria(criteria),
			}, nil
		}
	case "exists":
		criteria, err := parseExists(value, qid)
		if err != nil {
			log.Errorf("qid=%d, parseExists error: %v", qid, err)
			return nil, err
		}
		return &Condition{
			FilterCriteria: []*FilterCriteria(criteria),
		}, nil
	case "bool":
		boolSubNode, err := parseBool(value, qid, isJaegerQuery)
		if err != nil {
			log.Errorf("qid=%d, parseBool error: %v", qid, err)
			return nil, err
		}
		return &Condition{
			NestedNodes: []*ASTNode{boolSubNode},
		}, nil
	default:
		err := fmt.Errorf("error parsing Must/Filter query")
		log.Errorf("qid=%d, parseLeafNodes: can't parse unknown key=%v", qid, key)
		return nil, err
	}
}

func processQueryStringMap(colName string, colValue interface{}, qid uint64) (*Condition, error) {

	var filtercond *Condition
	kvMap := make(map[string]interface{})
	if colValue != "" {
		if colName == "" {
			colName = "*"
			if strings.Contains(colValue.(string), "\"") {
				colValue = strings.ReplaceAll(strings.TrimSpace(colValue.(string)), "\"", "")
				kvMap[colName] = colValue

				criteria := createMatchPhraseFilterCriteria(colName, colValue, And, qid)
				filtercond = &Condition{
					FilterCriteria: []*FilterCriteria{criteria},
				}
			} else {
				colValue = strings.ReplaceAll(strings.TrimSpace(colValue.(string)), "\"", "")
				kvMap[colName] = colValue

				criteria, err := parseMatch(kvMap, qid)
				if err != nil {
					log.Errorf("parseMatch error: %v", err)
					return nil, err
				}
				filtercond = &Condition{
					FilterCriteria: []*FilterCriteria(criteria),
				}

			}
		} else {
			colValue = strings.ReplaceAll(strings.TrimSpace(colValue.(string)), "\"", "")
			kvMap[colName] = colValue
			criteria, err := parseTerm(kvMap, qid)
			if err != nil {
				log.Errorf("parseTerm error: %v", err)
				return nil, err
			}
			filtercond = &Condition{
				FilterCriteria: []*FilterCriteria(criteria),
			}

		}
	}
	return filtercond, nil
}

//few example query string patterns
// col1 : val1
//(col1: val2) AND val2
//col1: (val1 OR val2)

func convertAndParseQuerystring(value interface{}, qid uint64) (*ASTNode, []*FilterCriteria, error) {
	// var query = "eventType:pageview AND geo_country:KR"
	if value == nil {
		err := fmt.Errorf("parseBool: Error parsing JSON expected a value, got: %v", value)
		return nil, nil, err
	}
	boolNode := &ASTNode{}
	boolNode.TimeRange = rutils.GetESDefaultQueryTimeRange()

	var currCondition, filtercond *Condition
	var orCurrCondition *Condition
	var colName string

	token_openBrackets := strings.Split(value.(string), "(")

	for _, openBracket := range token_openBrackets {
		token_closeBrackets := strings.Split(openBracket, ")")

		for ic, closeBracket := range token_closeBrackets {
			//reset colName after first group of tokens inside brackets () have been processed with colName = col1
			//col1: (val1 OR val2) AND val3
			if ic > 0 {
				colName = ""
			}
			ands := strings.Split(closeBracket, " AND ")
			for _, and := range ands {
				//no brackets in the query , then reset colName after each sub expression separated by AND
				// col1:val1 AND val2
				if len(token_openBrackets) == 1 {
					colName = ""
				}
				if strings.TrimSpace(and) != "" {
					//first process all the sub expressions joined by AND
					if !strings.Contains(and, "OR") {
						var err error
						if strings.Contains(and, ":") {
							//col:val query string
							kv := strings.Split(and, ":")
							if len(kv) > 1 {
								colName = strings.TrimSpace(string(kv[0]))
								colValue := strings.TrimSpace(kv[1])
								filtercond, err = processQueryStringMap(colName, colValue, qid)
								if err != nil {
									log.Errorf("processMap error: %v", err)
									return nil, nil, err
								}
								if currCondition == nil {
									currCondition = filtercond
								} else {
									currCondition.JoinCondition(filtercond)
								}
							}
						} else {
							// free text search
							filtercond, err = processQueryStringMap(colName, strings.TrimSpace(and), qid)
							if err != nil {
								log.Errorf("processMap error: %v", err)
								return nil, nil, err
							}
							if currCondition == nil {
								currCondition = filtercond
							} else {
								currCondition.JoinCondition(filtercond)
							}
						}
					}
				}
				ors := strings.Split(and, " OR ")
				if len(ors) > 1 {
					for _, or := range ors {
						//no brackets in the query , then reset colName after each sub expression separated by OR
						// col1:val1 OR val2
						if len(token_openBrackets) == 1 {
							colName = ""
						}
						if !strings.Contains(or, "AND") {
							var err error
							if strings.TrimSpace(or) != "" {
								if strings.Contains(or, ":") {
									kv := strings.Split(or, ":")
									if len(kv) > 1 {
										colName = strings.TrimSpace(string(kv[0]))
										colValue := strings.TrimSpace(kv[1])
										filtercond, err = processQueryStringMap(colName, colValue, qid)
										if err != nil {
											log.Errorf("processMap error: %v", err)
											return nil, nil, err
										}
										if orCurrCondition == nil {
											orCurrCondition = filtercond
										} else {
											orCurrCondition.JoinCondition(filtercond)
										}
									}
								} else {
									filtercond, err = processQueryStringMap(colName, strings.TrimSpace(or), qid)
									if err != nil {
										log.Errorf("processMap error: %v", err)
										return nil, nil, err
									}
									if orCurrCondition == nil {
										orCurrCondition = filtercond
									} else {
										orCurrCondition.JoinCondition(filtercond)
									}
								}
							}
						}
					}
				}
			}
		}
	}
	//complex query string
	if strings.Contains(value.(string), " AND ") || strings.Contains(value.(string), " OR ") || strings.Contains(value.(string), "\"") {

		if boolNode.AndFilterCondition == nil {
			boolNode.AndFilterCondition = currCondition
		} else {
			boolNode.AndFilterCondition.JoinCondition(currCondition)
		}
		if boolNode.OrFilterCondition == nil {
			boolNode.OrFilterCondition = orCurrCondition
		} else {
			boolNode.OrFilterCondition.JoinCondition(orCurrCondition)
		}
		return boolNode, nil, nil
	} else {
		return nil, currCondition.FilterCriteria, nil

	}
}

/*
Example Json_body for term query
"term": {
  <<column-name>>: {
	"value": <<column-value>>,
  }
}
OR
"term" : { <<column-name>> : <<column-value>> }
*/

func parseTerm(json_body interface{}, qid uint64) ([]*FilterCriteria, error) {
	if json_body == nil {
		err := fmt.Errorf("parseTerm: Error parsing JSON expected a value, got: %v", json_body)
		return nil, err
	}
	switch t := json_body.(type) {
	case map[string]interface{}:
		for key, value := range t {
			key = strings.TrimSuffix(key, ".raw") // remove any .raw postfix from column name
			andFilterCondition := make([]*FilterCriteria, 0)
			switch innerTerm := value.(type) {
			case string:
				criteria := createTermFilterCriteria(key, value, Equals, qid)
				andFilterCondition = append(andFilterCondition, criteria)
			case bool:
				criteria := createTermFilterCriteria(key, value.(bool), Equals, qid)
				andFilterCondition = append(andFilterCondition, criteria)
			case json.Number:
				criteria := createTermFilterCriteria(key, value.(json.Number), Equals, qid)
				andFilterCondition = append(andFilterCondition, criteria)
			case map[string]interface{}:
				if len(innerTerm) > 1 {
					if qVal, ok := innerTerm["value"]; !ok {
						return nil, fmt.Errorf("parseTerm: Invalid Term query, nested query not found in %+v", innerTerm)
					} else {
						criteria := createTermFilterCriteria(key, qVal, Equals, qid)
						andFilterCondition = append(andFilterCondition, criteria)
					}
				} else {
					for _, v := range innerTerm {
						criteria := createTermFilterCriteria(key, v, Equals, qid)
						andFilterCondition = append(andFilterCondition, criteria)
					}
					break
				}
			case []interface{}:
				return nil, errors.New("parseTerm: Invalid Term query, [term] query does not support array of values")
			default:
				return nil, errors.New("parseTerm: Invalid Term query")
			}
			return andFilterCondition, nil
		}
	default:
		return nil, errors.New("parseTerm: Invalid Term query")
	}
	return nil, nil
}

/*
"range": {
  <<column-name>>: {
	"gte" || "gt": <<lower-bound>>
	"lte" || "lt": <<upper-bound>>,
  }
}
// at least one condition is present
// case1: only one condition is present (not timestamp key)
"range":{
	"age":{
		"gte": 10
		}
}
// case2: both gt(e) and lt(e) exist
"range":{
	"age":{
		"gte": 10,
		"lte": 20
		}
}
*/

func parseRange(json_body interface{}, qid uint64, isJaegerQuery bool) ([]*FilterCriteria, *dtu.TimeRange, error) {
	if json_body == nil {
		err := fmt.Errorf("parseRange: Error parsing JSON expected a value, got: %v", json_body)
		return nil, nil, err
	}
	switch t := json_body.(type) {
	case map[string]interface{}:
		for key, value := range t {
			key = strings.TrimSuffix(key, ".raw") // remove any .raw postfix from column name
			andFilterCondition := make([]*FilterCriteria, 0)
			switch t := value.(type) {
			case map[string]interface{}:
				if len(t) < 1 {
					return nil, nil, errors.New("parseRange: Invalid Range query")
				}
				tsKey := config.GetTimeStampKey()
				if isJaegerQuery {
					tsKey = "startTimeMillis"
				}
				if key == tsKey {
					tRange := new(dtu.TimeRange)
					for nestedKey, nestedValue := range t {
						if nestedKey == "format" {
							//todo Handle timestamp format
							log.Infof("qid=%d, parseRange:Handle timestamp format", qid)
						} else {
							val, _ := getEpochFromRangeExprValue(nestedValue, qid)
							switch nestedKey {
							case "gt":
								tRange.StartEpochMs = uint64(val)
							case "gte":
								tRange.StartEpochMs = uint64(val)
							case "lt":
								tRange.EndEpochMs = uint64(val)
							case "lte":
								tRange.EndEpochMs = uint64(val)
							case "from":
								tRange.StartEpochMs = uint64(val)
							case "to":
								tRange.EndEpochMs = uint64(val)
							case "include_upper", "include_lower":
							default:
								log.Infof("qid=%d, parseRange: invalid range option %+v", qid, nestedKey)
							}
						}
					}
					return nil, tRange, nil
				} else {
					for nestedKey, nestedValue := range t {
						var opr FilterOperator
						switch nestedKey {
						case "gt":
							opr = GreaterThan
						case "gte":
							opr = GreaterThanOrEqualTo
						case "lt":
							opr = LessThan
						case "lte":
							opr = LessThanOrEqualTo
						case "from":
							opr = GreaterThanOrEqualTo
						case "to":
							opr = LessThanOrEqualTo
						case "include_upper", "include_lower":
						default:
							log.Infof("qid=%d, parseRange: invalid range option %+v", qid, nestedKey)
							continue
						}
						criteria := createTermFilterCriteria(key, nestedValue, opr, qid)
						andFilterCondition = append(andFilterCondition, criteria)
					}
					return andFilterCondition, nil, nil

				}
			case []interface{}:
				return nil, nil, errors.New("parseRange: Invalid Range query, range query does not support array of values")
			default:
				return nil, nil, errors.New("parseRange: Invalid Range query")
			}
		}
	default:
		return nil, nil, errors.New("parseRange: Invalid Range query")
	}
	return nil, nil, nil
}

func getEpochFromRangeExprValue(incoming interface{}, qid uint64) (int64, error) {

	switch incomingtype := incoming.(type) {
	case json.Number:
		val, _ := (incomingtype).Int64()
		return val, nil
	case string:
		valTime, err := time.Parse(time.RFC3339, incomingtype)
		if err != nil {
			log.Errorf("qid=%d, getEpochFromRangeExprValue: failed to parse time, in=%v, err=%v", qid, incomingtype, err)
			return 0, err
		}
		return valTime.UnixNano() / int64(time.Millisecond), nil
	}

	return 0, errors.New("getEpochFromRangeExprValue bad input")
}

/*
"match": {
  <<column-name>>: {
	"query": <<column-value>>,
		"operator": "and/or" (optional and defaults to "or")
  }
}

OR

"match": {
	 <<column-name>>: <<column-value>>,
	 "operator": "and/or" (optional and defaults to "or")
}

*/

func parseMatch(json_body interface{}, qid uint64) ([]*FilterCriteria, error) {
	if json_body == nil {
		err := fmt.Errorf("qid=%d, parseMatch: Error parsing JSON, expected a value, got nil", qid)
		log.Error(err)
		return nil, err
	}

	andFilterCondition := make([]*FilterCriteria, 0)
	var opr = Or
	var colName string
	var colValue interface{}
	switch t := json_body.(type) {
	case map[string]interface{}:
		for key, value := range t {
			key = strings.TrimSuffix(key, ".raw") // remove any .raw postfix from column name
			switch t := value.(type) {
			case string:
				if key == "operator" && value == "and" {
					opr = And
				} else if key == "operator" && value == "or" {
					opr = Or
				} else {
					colValue = value
					colName = key
				}
			case map[string]interface{}:
				if len(t) == 0 {
					return nil, errors.New("invalid Match query")
				}
				for nestedKey, nestedValue := range t {
					if nestedKey == "query" {
						colValue = nestedValue
					} else if nestedKey == "operator" && nestedValue == "and" {
						opr = And
					} else if nestedKey == "operator" && nestedValue == "or" {
						opr = Or
					} else {
						return nil, errors.New("parseMatch: Invalid Match query")
					}
				}
				colName = key
			case []interface{}:
				return nil, errors.New("parseMatch: Invalid Match query")
			default:
				return nil, errors.New("parseMatch: Invalid Match query")
			}
		}
	default:
		return nil, errors.New("parseMatch: Invalid Match query")
	}
	criteria := createMatchFilterCriteria(colName, colValue, opr, qid)
	andFilterCondition = append(andFilterCondition, criteria)
	return andFilterCondition, nil

}

/*
Example Json_body for match_phrase query

	"query": {
			"match_phrase": {
				"foo": "Hello World"
			  }
		}
*/
func parseMatchPhrase(json_body interface{}, qid uint64) (*ASTNode, error) {
	if json_body == nil {
		err := errors.New("parseMatchPhrase: Error parsing JSON expected a value, got nil")
		return nil, err
	}
	rootNode := &ASTNode{}
	var err error
	//set timeRange
	rootNode.TimeRange = rutils.GetESDefaultQueryTimeRange()
	opr := And

	var colName string
	var colValue interface{}
	switch t := json_body.(type) {
	case map[string]interface{}:
		for key, value := range t {
			switch value.(type) {
			case string:
				colValue = value
				colName = key
			default:
				err = fmt.Errorf("qid=%d parseMatch: Invalid Match_phrase query, value expected to be string, got %v", qid, value)
				log.Error(err)
				return nil, err
			}
		}
	default:
		err = fmt.Errorf("parseMatch: Invalid Match_phrase query, unexpected json body %v", json_body)
		return nil, err
	}
	criteria := createMatchPhraseFilterCriteria(colName, colValue, opr, qid)
	rootNode.AndFilterCondition = &Condition{FilterCriteria: []*FilterCriteria{criteria}}
	return rootNode, err
}

func parseMatchPhrase_nested(json_body interface{}, qid uint64) ([]*FilterCriteria, error) {
	if json_body == nil {
		err := errors.New("parseMatchPhrase: Error parsing JSON expected a value, got nil")
		return nil, err
	}
	andFilterCondition := make([]*FilterCriteria, 0)
	var err error
	opr := And

	var colName string
	var colValue interface{}
	switch t := json_body.(type) {
	case map[string]interface{}:
		for key, value := range t {
			switch value.(type) {
			case string:
				colValue = value
				colName = key
			default:
				err = fmt.Errorf("qid=%d parseMatch: Invalid Match_phrase query, value expected to be string, got %v", qid, value)
				log.Error(err)
				return nil, err
			}
		}
	default:
		err = fmt.Errorf("parseMatch: Invalid Match_phrase query, unexpected json body %v", json_body)
		return nil, err
	}
	criteria := createMatchPhraseFilterCriteria(colName, colValue, opr, qid)
	andFilterCondition = append(andFilterCondition, criteria)
	return andFilterCondition, nil
}

/*
"query": {
  "bool": {
	"must":
	  {
		"match_all": {
		}
	  }
	}
  }
*/

func parseMatchall_nested(json_body interface{}, qid uint64) ([]*FilterCriteria, error) {
	andFilterCondition := make([]*FilterCriteria, 0)
	//set timeRange
	colName := "*"
	colValue := "*"
	criteria := createTermFilterCriteria(colName, colValue, Equals, qid)
	andFilterCondition = append(andFilterCondition, criteria)
	return andFilterCondition, nil

}

/*
	"query_string": {
		  "query": "(<<column-value1>>) OR (<<column-value2>>)",
		  "default_field": <<column-name>>
				"default_operator": OR/AND
		}

or

	query_string": {
		  "query": "<<column-name>>: <<column-value>>",
		  "analyze_wildcard": true,
		  "default_field": "*"
		}
*/
func parseQuerystring(json_body interface{}, qid uint64) (*ASTNode, []*FilterCriteria, error) {
	if json_body == nil {
		err := fmt.Errorf("parseQuerystring: Error parsing JSON expected a value, got: %v", json_body)
		return nil, nil, err
	}

	// andFilterCondition := make([]*FilterCriteria, 0)
	boolNode := &ASTNode{}
	var err error
	//set timeRange
	boolNode.TimeRange = rutils.GetESDefaultQueryTimeRange()

	var opr = Or
	switch t := json_body.(type) {
	case map[string]interface{}:
		for key, value := range t {
			key = strings.TrimSuffix(key, ".raw") // remove any .raw postfix from column name
			switch valtype := value.(type) {
			case bool:
				if key == "analyze_wildcard" {
					log.Infof("qid=%d, parseQuerystring: Ignoring query_string analyze_wildcard", qid)
				}
			case []interface{}:
				if key == "fields" {
					// fieldsArray := valtype
					log.Infof("qid=%d, parseQuerystring: Ignoring query_string fields", valtype...)
				} else {
					log.Errorf("qid=%d, parseQuerystring: Invalid query_string, value.(type)=%T, key=%v", qid, value, key)
					return nil, nil, errors.New("parseQuerystring: Invalid query_string")
				}
			case string:
				switch key {
				case "default_operator":
					if value == "AND" {
						opr = And
					}
					log.Infof("qid=%d, parseQuerystring: Ignoring query_string default_operator %v", qid, opr)
				case "default_field":
					colName := value.(string)
					log.Infof("parseQuerystring: Ignoring query_string default_field %v", colName)
				case "query":
					var filterCond []*FilterCriteria
					boolNode, filterCond, err = convertAndParseQuerystring(value, qid)
					if err != nil {
						log.Errorf("convertAndParseQuerystring: failed to parse queryString, in=%v, err=%v", value.(string), err)
						return nil, nil, err
					}
					return boolNode, filterCond, nil
				default:
					log.Infof("qid=%d, parseQuerystring: query_string format not supported", qid)
				}
			default:
				log.Errorf("qid=%d, parseQuerystring: Invalid query_string, unhandled value.(type)=%T, key=%v", qid, value, key)
				return nil, nil, errors.New("parseQuerystring: Invalid query_string")
			}

		}
	default:
		log.Errorf("qid=%d, parseQuerystring: unhandled v.type=%v", qid, t)
		return nil, nil, errors.New("parseQuerystring: Invalid query_string query")
	}
	return boolNode, nil, nil

}

func parseExists(json_body interface{}, qid uint64) ([]*FilterCriteria, error) {
	if json_body == nil {
		log.Errorf("qid=%d, parseExists: got nil json body", qid)
		return nil, errors.New("read nil json body for exists condition")
	}

	switch v := json_body.(type) {
	case map[string]interface{}:
		colName, ok := v["field"]
		if !ok {
			log.Errorf("qid=%d, required parameter 'field' is not present for exists query", qid)
			return nil, errors.New("required parameter 'field' is not present for exists query")
		}
		colStr, ok := colName.(string)
		if !ok {
			log.Errorf("qid=%d, parameter 'field' is not a string", qid)
			return nil, errors.New("parameter 'field' is not a string")
		}
		colStr = strings.TrimSuffix(colStr, ".raw") // remove any .raw postfix from column name
		existsCriteria := &FilterCriteria{
			ExpressionFilter: &ExpressionFilter{
				LeftInput: &FilterInput{
					Expression: &Expression{
						LeftInput: &ExpressionInput{
							ColumnName: colStr,
						},
					},
				},
				FilterOperator: IsNotNull,
			},
		}
		return []*FilterCriteria{existsCriteria}, nil
	default:
		log.Errorf("qid=%d, parseExists: Exists parameter is not a map", qid)
		return nil, errors.New("parseExists: Exists parameter is not a map")
	}
}

/*
"terms": {
  <<column-name>>: {
	[ <<column-value1>> , <<column-value2>>]
	}
}

*/

func parseTerms(json_body interface{}, qid uint64) ([]*FilterCriteria, error) {
	if json_body == nil {
		err := fmt.Errorf("parseTerms: Error parsing JSON expected a value, got: %v", json_body)
		return nil, err
	}
	andFilterCondition := make([]*FilterCriteria, 0)

	var opr = Or
	switch t := json_body.(type) {
	case map[string]interface{}:
		for key, value := range t {
			key = strings.TrimSuffix(key, ".raw") // remove any .raw postfix from column name
			switch valtype := value.(type) {
			case []interface{}:
				if len(valtype) == 0 {
					return nil, errors.New("parseTerms : Invalid Terms query")
				}
				criteria := createTermsFilterCriteria(key, valtype, opr)
				andFilterCondition = append(andFilterCondition, criteria)
				return andFilterCondition, nil
			default:
				return nil, errors.New("parseTerms: Invalid Terms query")
			}
		}
	default:
		return nil, errors.New("parseTerms: Invalid Terms query")
	}
	return nil, nil
}

/*
Example Json_body for prefix query
"prefix": {
  <<column-name>>: {
	"value": <<column-value>>,
  }
}
OR
"prefix" : { <<column-name>> : <<column-value>> }
*/

func parsePrefix(json_body interface{}, qid uint64) ([]*FilterCriteria, error) {
	if json_body == nil {
		err := fmt.Errorf("parsePrefix: Error parsing JSON expected a value, got: %v", json_body)
		return nil, err
	}
	andFilterCondition := make([]*FilterCriteria, 0)
	switch t := json_body.(type) {
	case map[string]interface{}:
		for key, value := range t {
			key = strings.TrimSuffix(key, ".raw") // remove any .raw postfix from column name
			switch nt := value.(type) {

			case string:
				criteria := createTermFilterCriteria(key, value.(string)+"*", Equals, qid)
				andFilterCondition = append(andFilterCondition, criteria)
				return andFilterCondition, nil
			case json.Number:
				criteria := createTermFilterCriteria(key, value.(json.Number)+"*", Equals, qid)
				andFilterCondition = append(andFilterCondition, criteria)
				return andFilterCondition, nil
			case map[string]interface{}:
				if len(t) > 1 {
					return nil, errors.New("parsePrefix:Invalid Prefix query len(t) > 1")
				}
				for _, v := range nt {
					switch vtype := v.(type) {
					case string:
						criteria := createTermFilterCriteria(key, vtype+"*", Equals, qid)
						andFilterCondition = append(andFilterCondition, criteria)
						return andFilterCondition, nil

					default:
						return nil, errors.New("parsePrefix: Invalid Prefix query")
					}
				}
			case []interface{}:
				return nil, errors.New("parsePrefix: Invalid Prefix query")
			default:

				return nil, errors.New("parsePrefix: Invalid Prefix query")
			}
		}
		return nil, errors.New("parsePrefix: Invalid Prefix query")
	}
	return nil, nil
}

/*
Example Json_body for regexp query
"regexp": {
  <<column-name>>: {
	"value": <<regex>>,
  }
}
*/

func parseRegexp(json_body interface{}, qid uint64) ([]*FilterCriteria, error) {
	if json_body == nil {
		err := fmt.Errorf("parseRegexp: Error parsing JSON expected a value, got: %v", json_body)
		return nil, err
	}

	switch t := json_body.(type) {
	case map[string]interface{}:
		for key, value := range t {
			key = strings.TrimSuffix(key, ".raw") // remove any .raw postfix from column name
			andFilterCondition := make([]*FilterCriteria, 0)

			switch t := value.(type) {
			case map[string]interface{}:
				for nestedKey, nestedValue := range t {
					if nestedKey == "value" {
						switch nestedvaltype := nestedValue.(type) {
						case string:
							criteria := createTermFilterCriteria(key, nestedvaltype, Equals, qid)
							andFilterCondition = append(andFilterCondition, criteria)
							return andFilterCondition, nil
						case json.Number:
							criteria := createTermFilterCriteria(key, nestedvaltype, Equals, qid)
							andFilterCondition = append(andFilterCondition, criteria)
							return andFilterCondition, nil
						default:
							return nil, errors.New("parseRegexp: Invalid regexp query")
						}
					}
				}
			case []interface{}:
				return nil, errors.New("parseRegexp: Invalid Regexp query")
			default:
				return nil, errors.New("parseRegexp: Invalid Regexp query")
			}
		}
		return nil, errors.New("parseRegexp: Invalid Regexp query")
	}
	return nil, nil
}

/*
Example Json_body for wildcard query
"wildcard": {
  <<column-name>>: {
	"value": <<regex>>,
  }
}
*/

func parseWildcard(json_body interface{}, qid uint64) ([]*FilterCriteria, error) {
	if json_body == nil {
		err := fmt.Errorf("parseWildcard: Error parsing JSON expected a value, got: %v", json_body)
		return nil, err
	}

	switch t := json_body.(type) {
	case map[string]interface{}:
		for key, value := range t {
			key = strings.TrimSuffix(key, ".raw") // remove any .raw postfix from column name
			andFilterCondition := make([]*FilterCriteria, 0)

			switch t := value.(type) {
			case map[string]interface{}:
				if len(t) > 1 {
					return nil, errors.New("parseWildcard: Invalid wildcard query")
				}
				for nestedKey, nestedValue := range t {
					if nestedKey == "value" {
						switch nestedvaltype := nestedValue.(type) {
						case string:
							criteria := createTermFilterCriteria(key, nestedvaltype, Equals, qid)
							andFilterCondition = append(andFilterCondition, criteria)
							return andFilterCondition, nil
						case json.Number:
							criteria := createTermFilterCriteria(key, nestedvaltype, Equals, qid)
							andFilterCondition = append(andFilterCondition, criteria)
							return andFilterCondition, nil
						default:
							return nil, errors.New("parseWildcard: Invalid wildcard query")
						}
					}
				}
			case []interface{}:
				return nil, errors.New("parseWildcard: Invalid wildcard query")
			default:
				return nil, errors.New("parseWildcard: Invalid wildcard query")
			}
		}
		return nil, errors.New("parseWildcard: Invalid wildcard query")
	}
	return nil, nil
}

func createTermFilterCriteria(k interface{}, v interface{}, opr FilterOperator, qid uint64) *FilterCriteria {
	cVal, err := CreateDtypeEnclosure(v, qid)
	if err != nil {
		log.Errorf("qid=%d, createTermFilterCriteria: error creating DtypeEnclosure: %+v", qid, err)
	}
	criteria := FilterCriteria{ExpressionFilter: &ExpressionFilter{
		LeftInput: &FilterInput{Expression: &Expression{
			LeftInput: &ExpressionInput{ColumnName: k.(string)}}},
		FilterOperator: opr,
		RightInput: &FilterInput{Expression: &Expression{
			LeftInput: &ExpressionInput{ColumnValue: cVal}}}}}

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
func createTermsFilterCriteria(k interface{}, val []interface{}, opr LogicalOperator) *FilterCriteria {
	var matchWords = make([][]byte, 0)
	for _, v := range val {
		matchWords = append(matchWords, [][]byte{[]byte(v.(string))}...)
	}

	criteria := FilterCriteria{MatchFilter: &MatchFilter{
		MatchColumn:   k.(string),
		MatchWords:    matchWords,
		MatchOperator: opr}}

	return &criteria

}

/*
"multi_match" : {
      "query" : "quick brown",
	  "type":       "phrase",
      "fields" : [ "subject", "message" ]
    }

OR

"multi_match" : {
      "query":      "quick brown f",
      "type":       "phrase_prefix",
      "fields":     [ "subject", "message" ]
    }

*/

func parseMultiMatch_nested(json_body interface{}, qid uint64) (*Condition, error) {
	if json_body == nil {
		err := errors.New("parseMatchPhrase: Error parsing JSON expected a value, got nil")
		return nil, err
	}

	if json_body == nil {
		err := fmt.Errorf("qid=%d, parseMultiMatch: Error parsing JSON, expected a value, got nil", qid)
		log.Error(err)
		return nil, err
	}
	var opr = Or
	var matchType string
	var matchFields = make([]string, 0)

	var colValue interface{}
	switch t := json_body.(type) {
	case map[string]interface{}:
		for nestedKey, nestedValue := range t {
			if nestedKey == "query" {
				colValue = nestedValue.(string)
			} else if nestedKey == "type" {
				matchType = nestedValue.(string)
			} else if nestedKey == "fields" {
				switch nvaltype := nestedValue.(type) {
				case []interface{}:
					for _, v := range nvaltype {
						switch v := v.(type) {
						case string:
							matchFields = append(matchFields, []string{v}...)
						default:
							return nil, errors.New("parseMultiMatch: Invalid fields in multi_match query")
						}
					}
				default:
					return nil, errors.New("parseMultiMatch: Invalid multi_match query")
				}
			} else if nestedKey == "operator" && nestedValue == "and" {
				opr = And
			} else if nestedKey == "operator" && nestedValue == "or" {
				opr = Or
			}
		}
		if matchType == "" || colValue == nil {
			return nil, errors.New("parseMultiMatch: Invalid multi_match query")
		}
		filterCondition := createMultiMatchFilterCriteria(matchFields, colValue, matchType, opr, qid)
		return filterCondition, nil
	default:
		return nil, errors.New("parseMultiMatch: Invalid multi_match query")
	}

}

func createMultiMatchFilterCriteria(matchFields []string, colValue interface{}, matchType string, opr LogicalOperator, qid uint64) *Condition {
	var colName string
	filterCondition := make([]*FilterCriteria, 0)
	if len(matchFields) == 0 {
		matchFields = append(matchFields, "*")
	}
	if matchType == "phrase_prefix" {
		temp := strings.ReplaceAll(strings.TrimSpace(colValue.(string)), ".", "")
		colValue = temp + ".*"
		for _, colName = range matchFields {
			criteria := createTermFilterCriteria(colName, colValue, Equals, qid)
			filterCondition = append(filterCondition, criteria)
		}
		return &Condition{
			FilterCriteria: []*FilterCriteria(filterCondition),
		}
	} else if matchType == "phrase" {
		for _, colName = range matchFields {
			criteria := createMatchPhraseFilterCriteria(colName, colValue, opr, qid)
			filterCondition = append(filterCondition, criteria)
		}
		return &Condition{
			FilterCriteria: []*FilterCriteria(filterCondition),
		}
	} else if matchType == "best_fields" || matchType == "most_fields" {
		allFieldConditions := make([]*FilterCriteria, 0)
		for _, colName = range matchFields {
			criteria := createMatchFilterCriteria(colName, colValue, opr, qid)
			allFieldConditions = append(allFieldConditions, criteria)
		}
		fieldASTNode := &ASTNode{
			OrFilterCondition: &Condition{
				FilterCriteria: []*FilterCriteria(allFieldConditions),
			},
		}
		return &Condition{
			NestedNodes: []*ASTNode{fieldASTNode},
		}
	}
	return &Condition{
		FilterCriteria: []*FilterCriteria(filterCondition),
	}
}
