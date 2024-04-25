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

package promql

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/cespare/xxhash"
	pql "github.com/influxdata/promql/v2"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"

	"github.com/influxdata/promql/v2/pkg/labels"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/reader/metrics/tagstree"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"
	. "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

const MIN_IN_MS = 60_000
const HOUR_IN_MS = 3600_000
const DAY_IN_MS = 86400_000

func parseSearchBody(jsonSource map[string]interface{}) (string, uint32, uint32, time.Duration, usageStats.UsageStatsGranularity, error) {
	searchText := ""
	var err error
	var startTime, endTime uint32
	var step time.Duration
	var granularity usageStats.UsageStatsGranularity
	var pastXhours uint64
	for key, value := range jsonSource {
		switch key {
		case "query":
			switch valtype := value.(type) {
			case string:
				searchText = valtype
			default:
				log.Errorf("promql/parseSearchBody query is not a string! Val %+v", valtype)
				return searchText, 0, 0, 0, usageStats.Hourly, errors.New("promql/parseSearchBody query is not a string")
			}
		case "step":
			switch valtype := value.(type) {
			case string:
				step, err = parseDuration(valtype)
				if err != nil {
					log.Error("Query bad request:" + err.Error())
					return searchText, 0, 0, 0, usageStats.Hourly, err
				}
			default:
				log.Errorf("promql/parseSearchBody step is not a string! Val %+v", valtype)
				return searchText, 0, 0, 0, usageStats.Hourly, errors.New("promql/parseSearchBody step is not a string")
			}
			if step <= 0 {
				err := errors.New("zero or negative query resolution step widths are not accepted. Try a positive integer")
				log.Info("msg", "Query bad request:"+err.Error())
				return searchText, 0, 0, 0, usageStats.Hourly, err
			}

		case "start":
			var startTimeStr string
			switch valtype := value.(type) {
			case int:
				startTimeStr = fmt.Sprintf("%d", valtype)
			case float64:
				startTimeStr = fmt.Sprintf("%d", int64(valtype))
			case string:
				if strings.Contains(value.(string), "now") {
					nowTs := utils.GetCurrentTimeInMs()
					defValue := nowTs - (1 * 60 * 1000)
					pastXhours, granularity = parseAlphaNumTime(nowTs, value.(string), defValue)
					startTimeStr = fmt.Sprintf("%d", pastXhours)
				} else {
					startTimeStr = valtype
				}
			default:
				log.Errorf("promql/parseSearchBody start is not a string! Val %+v", valtype)
				return searchText, 0, 0, 0, usageStats.Hourly, errors.New("promql/parseSearchBody start is not a string")
			}
			startTime, err = parseTimeFromString(startTimeStr)
			if err != nil {
				log.Errorf("Unable to parse start time: %v. Error: %+v", value, err)
				return searchText, 0, 0, 0, usageStats.Hourly, err

			}
		case "end":
			var endTimeStr string
			switch valtype := value.(type) {
			case int:
				endTimeStr = fmt.Sprintf("%d", valtype)
			case float64:
				endTimeStr = fmt.Sprintf("%d", int64(valtype))
			case string:
				if strings.Contains(value.(string), "now") {
					nowTs := utils.GetCurrentTimeInMs()
					defValue := nowTs
					pastXhours, _ = parseAlphaNumTime(nowTs, value.(string), defValue)
					endTimeStr = fmt.Sprintf("%d", pastXhours)

				} else {
					endTimeStr = valtype
				}
			default:
				log.Errorf("promql/parseSearchBody end time is not a string! Val %+v", valtype)
				return searchText, 0, 0, 0, usageStats.Hourly, errors.New("promql/parseSearchBody:end time is not a string")
			}
			endTime, err = parseTimeFromString(endTimeStr)
			if err != nil {
				log.Errorf("Unable to parse end time: %v. Error: %+v", value, err)
				return searchText, 0, 0, 0, usageStats.Hourly, err
			}
		default:
			log.Errorf("promql/parseSearchBody invalid key %+v", key)
			return "", 0, 0, 0, usageStats.Hourly, errors.New("promql/parseSearchBody error invalid key")
		}

	}
	if endTime < startTime {
		err := errors.New("end timestamp must not be before start time")
		log.Info("msg", "Query bad request:"+err.Error())
		return searchText, 0, 0, 0, usageStats.Hourly, err
	}
	return searchText, startTime, endTime, 0, granularity, nil
}

func ProcessMetricsSearchRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf(" ProcessMetricsSearchRequest: received empty search request body ")
		utils.SetBadMsg(ctx, "")
		return
	}
	qid := rutils.GetNextQid()

	readJSON := make(map[string]interface{})
	var jsonc = jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := jsonc.NewDecoder(bytes.NewReader(rawJSON))
	decoder.UseNumber()
	err := decoder.Decode(&readJSON)
	if err != nil {
		ctx.SetContentType(ContentJson)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		WriteJsonResponse(ctx, nil)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("qid=%v, ProcessMetricsSearchRequest: could not write error message err=%v", qid, err)
		}
		log.Errorf("qid=%v, ProcessMetricsSearchRequest: failed to decode search request body! Err=%+v", qid, err)
		return
	}

	searchText, startTime, endTime, step, _, err := parseSearchBody(readJSON)
	if err != nil {
		ctx.SetContentType(ContentJson)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("qid=%v, ProcessMetricsSearchRequest: could not write error message err=%v", qid, err)
		}

		log.Errorf("qid=%v, ProcessMetricsSearchRequest: parseSearchBody , err=%v", qid, err)
		return
	}
	if endTime == 0 {
		endTime = uint32(time.Now().Unix())
	}
	if startTime == 0 {
		startTime = uint32(time.Now().Add(time.Duration(-5) * time.Minute).Unix())
	}

	log.Infof("qid=%v, ProcessMetricsSearchRequest:  searchString=[%v] startEpochMs=[%v] endEpochMs=[%v] step=[%v]", qid, searchText, startTime, endTime, step)

	metricQueryRequest, pqlQuerytype, _, err := convertPqlToMetricsQuery(searchText, startTime, endTime, myid)
	if err != nil {
		ctx.SetContentType(ContentJson)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		WriteJsonResponse(ctx, nil)
		log.Errorf("qid=%v, ProcessMetricsSearchRequest: Error parsing query err=%+v", qid, err)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("qid=%v, ProcessMetricsSearchRequest: could not write error message err=%v", qid, err)
		}
		return
	}
	segment.LogMetricsQuery("PromQL metrics query parser", &metricQueryRequest[0], qid)
	res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, qid)

	mQResponse, err := res.GetResultsPromQl(&metricQueryRequest[0].MetricsQuery, pqlQuerytype)
	if err != nil {
		log.Errorf("ExecuteAsyncQuery: Error getting results! %+v", err)
	}
	WriteJsonResponse(ctx, &mQResponse)
	ctx.SetContentType(ContentJson)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessUiMetricsSearchRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf(" ProcessMetricsSearchRequest: received empty search request body ")
		utils.SetBadMsg(ctx, "")
		return
	}
	qid := rutils.GetNextQid()

	readJSON := make(map[string]interface{})
	var jsonc = jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := jsonc.NewDecoder(bytes.NewReader(rawJSON))
	decoder.UseNumber()
	err := decoder.Decode(&readJSON)
	if err != nil {
		ctx.SetContentType(ContentJson)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		WriteJsonResponse(ctx, nil)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("qid=%v, ProcessMetricsSearchRequest: could not write error message err=%v", qid, err)
		}
		log.Errorf("qid=%v, ProcessMetricsSearchRequest: failed to decode search request body! Err=%+v", qid, err)
		return
	}

	searchText, startTime, endTime, step, _, err := parseSearchBody(readJSON)
	if err != nil {
		ctx.SetContentType(ContentJson)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("qid=%v, ProcessMetricsSearchRequest: could not write error message err=%v", qid, err)
		}

		log.Errorf("qid=%v, ProcessMetricsSearchRequest: parseSearchBody , err=%v", qid, err)
		return
	}
	if endTime == 0 {
		endTime = uint32(time.Now().Unix())
	}
	if startTime == 0 {
		startTime = uint32(time.Now().Add(time.Duration(-60) * time.Minute).Unix())
	}

	log.Infof("qid=%v, ProcessMetricsSearchRequest:  searchString=[%v] startEpochMs=[%v] endEpochMs=[%v] step=[%v]", qid, searchText, startTime, endTime, step)

	searchText, startTime, endTime, interval := parseSearchTextForRangeSelection(searchText, startTime, endTime)
	metricQueryRequest, pqlQuerytype, queryArithmetic, err := convertPqlToMetricsQuery(searchText, startTime, endTime, myid)

	if err != nil {
		ctx.SetContentType(ContentJson)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		WriteJsonResponse(ctx, nil)
		log.Errorf("qid=%v, ProcessMetricsSearchRequest: Error parsing query err=%+v", qid, err)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("qid=%v, ProcessMetricsSearchRequest: could not write error message err=%v", qid, err)
		}
		return
	}

	metricQueriesList := make([]*structs.MetricsQuery, 0)
	var timeRange *dtu.MetricsTimeRange
	hashList := make([]uint64, 0)
	for _, metricQuery := range metricQueryRequest {
		hashList = append(hashList, metricQuery.MetricsQuery.HashedMName)
		metricQueriesList = append(metricQueriesList, &metricQuery.MetricsQuery)
		segment.LogMetricsQuery("PromQL metrics query parser", &metricQuery, qid)
		timeRange = &metricQuery.TimeRange
	}
	res := segment.ExecuteMultipleMetricsQuery(hashList, metricQueriesList, queryArithmetic, timeRange, qid)
	mQResponse, err := res.GetResultsPromQlForUi(metricQueriesList[0], pqlQuerytype, startTime, endTime, interval)
	if err != nil {
		log.Errorf("ExecuteAsyncQuery: Error getting results! %+v", err)
	}
	WriteJsonResponse(ctx, &mQResponse)
	ctx.SetContentType(ContentJson)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func convertPqlToMetricsQuery(searchText string, startTime, endTime uint32, myid uint64) ([]structs.MetricsQueryRequest, pql.ValueType, []structs.QueryArithmetic, error) {
	// call prometheus promql parser
	expr, err := pql.ParseExpr(searchText)
	if err != nil {
		return []structs.MetricsQueryRequest{}, "", []structs.QueryArithmetic{}, err
	}
	pqlQuerytype := expr.Type()
	var mquery structs.MetricsQuery
	mquery.Aggregator = structs.Aggreation{}
	selectors := extractSelectors(expr)
	//go through labels
	for _, lblEntry := range selectors {
		for _, entry := range lblEntry {
			if entry.Name != "__name__" {
				tagFilter := &structs.TagsFilter{
					TagKey:          entry.Name,
					RawTagValue:     entry.Value,
					HashTagValue:    xxhash.Sum64String(entry.Value),
					TagOperator:     segutils.TagOperator(entry.Type),
					LogicalOperator: segutils.And,
				}
				mquery.TagsFilters = append(mquery.TagsFilters, tagFilter)
			} else {
				mquery.MetricName = entry.Value
			}
		}
	}

	var groupby bool
	switch expr := expr.(type) {
	case *pql.AggregateExpr:
		es := &pql.EvalStmt{
			Expr:     expr,
			Start:    time.Now().Add(time.Duration(-5) * time.Minute),
			End:      time.Now(),
			Interval: time.Duration(1 * time.Minute),
			// LookbackDelta: 0,
		}

		mquery.Aggregator = structs.Aggreation{}
		pql.Inspect(es.Expr, func(node pql.Node, path []pql.Node) error {
			switch expr := node.(type) {
			case *pql.AggregateExpr:
				aggFunc := extractFuncFromPath(path)
				switch aggFunc {
				case "avg":
					mquery.Aggregator.AggregatorFunction = segutils.Avg
				case "count":
					mquery.Aggregator.AggregatorFunction = segutils.Count
				case "sum":
					mquery.Aggregator.AggregatorFunction = segutils.Sum
				case "max":
					mquery.Aggregator.AggregatorFunction = segutils.Max
				case "min":
					mquery.Aggregator.AggregatorFunction = segutils.Min
				case "quantile":
					mquery.Aggregator.AggregatorFunction = segutils.Quantile
				default:
					log.Infof("convertPqlToMetricsQuery: using sum aggregator by default for AggregateExpr (got %v)", aggFunc)
					mquery.Aggregator = structs.Aggreation{AggregatorFunction: segutils.Sum}
				}
			case *pql.VectorSelector:
				_, grouping := extractGroupsFromPath(path)
				aggFunc := extractFuncFromPath(path)
				for _, grp := range grouping {
					groupby = true
					tagFilter := structs.TagsFilter{
						TagKey:          grp,
						RawTagValue:     "*",
						HashTagValue:    xxhash.Sum64String(tagstree.STAR),
						TagOperator:     segutils.TagOperator(segutils.Equal),
						LogicalOperator: segutils.And,
					}
					mquery.TagsFilters = append(mquery.TagsFilters, &tagFilter)
				}
				switch aggFunc {
				case "avg":
					mquery.Aggregator.AggregatorFunction = segutils.Avg
				case "count":
					mquery.Aggregator.AggregatorFunction = segutils.Count
				case "sum":
					mquery.Aggregator.AggregatorFunction = segutils.Sum
				case "max":
					mquery.Aggregator.AggregatorFunction = segutils.Max
				case "min":
					mquery.Aggregator.AggregatorFunction = segutils.Min
				case "quantile":
					mquery.Aggregator.AggregatorFunction = segutils.Quantile
				default:
					log.Infof("convertPqlToMetricsQuery: using sum aggregator by default for VectorSelector (got %v)", aggFunc)
					mquery.Aggregator = structs.Aggreation{AggregatorFunction: segutils.Sum}
				}
			case *pql.NumberLiteral:
				mquery.Aggregator.FuncConstant = expr.Val
			default:
				err := fmt.Errorf("pql.Inspect: Unsupported node type %T", node)
				log.Errorf("%v", err)
				return err
			}
			return nil
		})
	case *pql.Call:
		pql.Inspect(expr, func(node pql.Node, path []pql.Node) error {
			switch node.(type) {
			case *pql.MatrixSelector:
				function := extractFuncFromPath(path)

				if mquery.TagsFilters != nil {
					groupby = true
				}
				switch function {
				case "deriv":
					mquery.Aggregator = structs.Aggreation{RangeFunction: segutils.Derivative}
				case "rate":
					mquery.Aggregator = structs.Aggreation{RangeFunction: segutils.Rate}
				default:
					return fmt.Errorf("pql.Inspect: unsupported function type %v", function)
				}

			}
			return nil
		})
	case *pql.VectorSelector:
		mquery.HashedMName = xxhash.Sum64String(mquery.MetricName)
		mquery.OrgId = myid
		mquery.SelectAllSeries = true
		agg := structs.Aggreation{AggregatorFunction: segutils.Sum}
		mquery.Downsampler = structs.Downsampler{Interval: 1, Unit: "m", Aggregator: agg}
		metricQueryRequest := &structs.MetricsQueryRequest{
			MetricsQuery: mquery,
			TimeRange: dtu.MetricsTimeRange{
				StartEpochSec: startTime,
				EndEpochSec:   endTime,
			},
		}
		return []structs.MetricsQueryRequest{*metricQueryRequest}, pqlQuerytype, []structs.QueryArithmetic{}, nil
	case *pql.BinaryExpr:
		arithmeticOperation := structs.QueryArithmetic{}
		var lhsValType, rhsValType pql.ValueType
		var lhsRequest, rhsRequest []structs.MetricsQueryRequest
		if constant, ok := expr.LHS.(*pql.NumberLiteral); ok {
			arithmeticOperation.ConstantOp = true
			arithmeticOperation.Constant = constant.Val
		} else {
			lhsRequest, lhsValType, _, err = convertPqlToMetricsQuery(expr.LHS.String(), startTime, endTime, myid)
			if err != nil {
				return []structs.MetricsQueryRequest{}, "", []structs.QueryArithmetic{}, err
			}
			arithmeticOperation.LHS = lhsRequest[0].MetricsQuery.HashedMName

		}

		if constant, ok := expr.RHS.(*pql.NumberLiteral); ok {
			arithmeticOperation.ConstantOp = true
			arithmeticOperation.Constant = constant.Val
		} else {
			rhsRequest, rhsValType, _, err = convertPqlToMetricsQuery(expr.RHS.String(), startTime, endTime, myid)
			if err != nil {
				return []structs.MetricsQueryRequest{}, "", []structs.QueryArithmetic{}, err
			}
			arithmeticOperation.RHS = rhsRequest[0].MetricsQuery.HashedMName

		}
		arithmeticOperation.Operation = getArithmeticOperation(expr.Op)
		if rhsValType == pql.ValueTypeVector {
			lhsValType = pql.ValueTypeVector
		}
		return append(lhsRequest, rhsRequest...), lhsValType, []structs.QueryArithmetic{arithmeticOperation}, nil
	case *pql.ParenExpr:
		return convertPqlToMetricsQuery(expr.Expr.String(), startTime, endTime, myid)
	default:
		return []structs.MetricsQueryRequest{}, "", []structs.QueryArithmetic{}, fmt.Errorf("convertPqlToMetricsQuery: Unsupported query type %T", expr)
	}

	tags := mquery.TagsFilters
	for idx, tag := range tags {
		var hashedTagVal uint64
		switch v := tag.RawTagValue.(type) {
		case string:
			hashedTagVal = xxhash.Sum64String(v)
		case int64:
			hashedTagVal = uint64(v)
		case float64:
			hashedTagVal = uint64(v)
		case uint64:
			hashedTagVal = v
		default:
			log.Errorf("ParseMetricsRequest: invalid tag value type")
		}
		tags[idx].HashTagValue = hashedTagVal
	}

	metricName := mquery.MetricName
	mquery.HashedMName = xxhash.Sum64String(metricName)

	if mquery.Aggregator.AggregatorFunction == 0 && !groupby {
		mquery.Aggregator = structs.Aggreation{AggregatorFunction: segutils.Sum}
	}
	mquery.Downsampler = structs.Downsampler{Interval: 1, Unit: "m", Aggregator: mquery.Aggregator}
	mquery.SelectAllSeries = !groupby // if group by is not present, then we need to select all series
	mquery.OrgId = myid
	metricQueryRequest := &structs.MetricsQueryRequest{
		MetricsQuery: mquery,
		TimeRange: dtu.MetricsTimeRange{
			StartEpochSec: startTime,
			EndEpochSec:   endTime,
		},
	}
	return []structs.MetricsQueryRequest{*metricQueryRequest}, pqlQuerytype, []structs.QueryArithmetic{}, nil
}

func getArithmeticOperation(op pql.ItemType) segutils.ArithmeticOperator {
	switch op {
	case pql.ItemADD:
		return segutils.Add
	case pql.ItemSUB:
		return segutils.Subtract
	case pql.ItemMUL:
		return segutils.Multiply
	case pql.ItemDIV:
		return segutils.Divide
	default:
		log.Errorf("getArithmeticOperation: unexpected op: %v", op)
		return 0
	}
}

func parseTimeFromString(timeStr string) (uint32, error) {
	// if it is not a relative time, parse as absolute time
	var t time.Time
	var err error
	// unixtime
	if unixTime, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
		if IsTimeInMilli(uint64(unixTime)) {
			return uint32(unixTime / 1e3), nil
		}
		return uint32(unixTime), nil
	}

	//absolute time formats
	timeStr = absoluteTimeFormat(timeStr)
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, format := range formats {
		t, err = time.Parse(format, timeStr)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Errorf("parseTime: invalid time format: %s. Error: %v", timeStr, err)
		return 0, err
	}
	return uint32(t.Unix()), nil
}

func extractSelectors(expr pql.Expr) [][]*labels.Matcher {
	var selectors [][]*labels.Matcher
	pql.Inspect(expr, func(node pql.Node, _ []pql.Node) error {
		var vs interface{}
		vs, ok := node.(*pql.VectorSelector)
		if ok {
			selectors = append(selectors, vs.(*pql.VectorSelector).LabelMatchers)
		}
		vs, ok = node.(pql.Expressions)
		if ok {
			for _, entry := range vs.(pql.Expressions) {
				expr, ok := entry.(*pql.MatrixSelector)
				if ok {
					selectors = append(selectors, expr.LabelMatchers)
				}
			}
		}
		return nil
	})
	return selectors
}

// extractGroupsFromPath parses vector outer function and extracts grouping information if by or without was used.
func extractGroupsFromPath(p []pql.Node) (bool, []string) {
	if len(p) == 0 {
		return false, nil
	}
	switch n := p[len(p)-1].(type) {
	case *pql.AggregateExpr:
		return !n.Without, n.Grouping
	case pql.Expressions:
		groupByVals := make([]string, 0)
		for _, entry := range n {
			expr, ok := entry.(*pql.MatrixSelector)
			if ok {
				for _, labels := range expr.LabelMatchers {
					if labels.Name != "__name__" {
						groupByVals = append(groupByVals, labels.Name)
					}
				}
			}
		}
		return false, groupByVals
	default:
		return false, nil
	}
}

// extractFuncFromPath walks up the path and searches for the first instance of
// a function or aggregation.
func extractFuncFromPath(p []pql.Node) string {
	if len(p) == 0 {
		return ""
	}
	switch n := p[len(p)-1].(type) {
	case *pql.AggregateExpr:
		return n.Op.String()
	case *pql.Call:
		return n.Func.Name
	case *pql.BinaryExpr:
		// If we hit a binary expression we terminate since we only care about functions
		// or aggregations over a single metric.
		return ""
	default:
		return extractFuncFromPath(p[:len(p)-1])
	}
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}

func absoluteTimeFormat(timeStr string) string {
	if strings.Contains(timeStr, "-") && strings.Count(timeStr, "-") == 1 {
		timeStr = strings.Replace(timeStr, "-", " ", 1)
	}
	timeStr = strings.Replace(timeStr, "/", "-", 2)
	if strings.Contains(timeStr, ":") {
		if strings.Count(timeStr, ":") < 2 {
			timeStr += ":00"
		}
	}
	return timeStr
}

/*
   Supports "now-[Num][Unit]"
   Num ==> any positive integer
   Unit ==> m(minutes), h(hours), d(days)
*/

func parseAlphaNumTime(nowTs uint64, inp string, defValue uint64) (uint64, usageStats.UsageStatsGranularity) {
	granularity := usageStats.Daily
	sanTime := strings.ReplaceAll(inp, " ", "")

	if sanTime == "now" {
		return nowTs, usageStats.Hourly
	}

	retVal := defValue

	strln := len(sanTime)
	if strln < 6 {
		return retVal, usageStats.Daily
	}

	unit := sanTime[strln-1]
	num, err := strconv.ParseInt(sanTime[4:strln-1], 0, 64)
	if err != nil {
		return defValue, usageStats.Daily
	}

	switch unit {
	case 'm':
		retVal = nowTs - MIN_IN_MS*uint64(num)
		granularity = usageStats.Hourly
	case 'h':
		retVal = nowTs - HOUR_IN_MS*uint64(num)
		granularity = usageStats.Hourly
	case 'd':
		retVal = nowTs - DAY_IN_MS*uint64(num)
		granularity = usageStats.Daily
	default:
		log.Errorf("parseAlphaNumTime: Unknown time unit %v", unit)
	}
	return retVal, granularity
}

func parseSearchTextForRangeSelection(searchText string, startTime uint32, endTime uint32) (string, uint32, uint32, uint32) {

	pattern := `\[(.*?)\]`

	regex := regexp.MustCompile(pattern)

	matches := regex.FindAllStringSubmatch(searchText, -1)

	var timeRange string

	for _, match := range matches {
		timeRange = match[1]
	}

	var totalVal uint32 = 0
	var curVal uint32 = 0
	var curDimension string = ""
	var dimensionVal uint32 = 0

	for _, ch := range timeRange {
		if unicode.IsDigit(ch) {
			if curDimension == "" {
				curVal = curVal*10 + uint32(ch-'0')
			} else {
				totalVal += curVal * dimensionVal
				curDimension = ""
				curVal = uint32(ch - '0')
			}
		} else {
			curDimension += curDimension + string(ch)
			if curDimension == "s" || curDimension == "S" {
				dimensionVal = 1
			} else if curDimension == "m" || curDimension == "M" {
				dimensionVal = 60
			} else if curDimension == "h" || curDimension == "H" {
				dimensionVal = 3600
			} else if curDimension == "d" || curDimension == "D" {
				dimensionVal = 24 * 3600
			} else if curDimension == "w" || curDimension == "W" {
				dimensionVal = 7 * 24 * 3600
			}
		}
	}
	totalVal += curVal * dimensionVal

	if totalVal > 0 {
		startTime = endTime - totalVal
	}

	return searchText, startTime, endTime, totalVal
}
