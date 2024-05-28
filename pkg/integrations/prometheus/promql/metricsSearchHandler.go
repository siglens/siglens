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
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/promql/parser"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/query/metadata"
	"github.com/siglens/siglens/pkg/segment/reader/metrics/tagstree"
	"github.com/siglens/siglens/pkg/segment/results/mresults"
	tsidtracker "github.com/siglens/siglens/pkg/segment/results/mresults/tsid"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"
	. "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

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

func ProcessPromqlMetricsSearchRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	qid := rutils.GetNextQid()
	searchText := string(ctx.FormValue("query"))
	timeParam := string(ctx.FormValue("time"))

	var endTime uint32
	var err error

	if searchText == "" {
		log.Errorf("ProcessPromqlMetricsSearchRequest: no query parameter provided")
		utils.SetBadMsg(ctx, "query is required")
		return
	}

	if timeParam == "" {
		// If timeParam doesn't exist, assume the current time in epoch seconds as endTime
		endTime = uint32(time.Now().Unix())
	} else {
		endTime, err = utils.ParseTimeForPromQL(timeParam)
		if err != nil {
			log.Errorf("ProcessPromqlMetricsSearchRequest: Error parsing time parameter, err:%v", err)
			return
		}
	}

	metricQueryRequest, pqlQuerytype, _, err := convertPqlToMetricsQuery(searchText, endTime-1, endTime, myid)
	if err != nil {
		ctx.SetContentType(ContentJson)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		WriteJsonResponse(ctx, nil)
		log.Errorf("qid=%v, ProcessPromqlMetricsSearchRequest: Error parsing query err=%+v", qid, err)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("qid=%v, ProcessPromqlMetricsSearchRequest: could not write error message err=%v", qid, err)
		}
		return
	}
	if len(metricQueryRequest) == 0 {
		ctx.SetContentType(ContentJson)
		WriteJsonResponse(ctx, map[string]interface{}{})
		return
	}
	segment.LogMetricsQuery("PromQL metrics query parser", &metricQueryRequest[0], qid)
	res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, qid)

	mQResponse, err := res.GetResultsPromQl(&metricQueryRequest[0].MetricsQuery, pqlQuerytype)
	if err != nil {
		log.Errorf("ProcessPromqlMetricsSearchRequest: Error getting results! %+v", err)
	}
	WriteJsonResponse(ctx, &mQResponse)
	ctx.SetContentType(ContentJson)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessPromqlMetricsRangeSearchRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	qid := rutils.GetNextQid()
	searchText := string(ctx.FormValue("query"))
	startParam := string(ctx.FormValue("start"))
	endParam := string(ctx.FormValue("end"))
	stepValue := string(ctx.FormValue("step"))

	var step time.Duration
	var stepFloat float64
	var err error

	if searchText == "" {
		log.Errorf("ProcessPromqlMetricsRangeSearchRequest: missing query parameter")
		utils.SetBadMsg(ctx, "query parameter is required")
		return
	}

	// Try to parse step as a duration
	step, err = time.ParseDuration(stepValue)
	if err != nil {
		// If parsing as a duration fails, try to parse as a float
		stepFloat, err = strconv.ParseFloat(stepValue, 64)
		if err != nil {
			log.Errorf("ProcessPromqlMetricsRangeSearchRequest: Error parsing step, err:%v", err)
			return
		}
		// Convert float to duration in seconds
		step = time.Duration(stepFloat * float64(time.Second))
	}

	startTime, err := utils.ParseTimeForPromQL(startParam)
	if err != nil {
		log.Errorf("ProcessPromqlMetricsRangeSearchRequest: Error parsing start parameter, err:%v", err)
		return
	}
	endTime, err := utils.ParseTimeForPromQL(endParam)
	if err != nil {
		log.Errorf("ProcessPromqlMetricsRangeSearchRequest: Error parsing end parameter, err:%v", err)
		return
	}

	log.Infof("qid=%v, ProcessPromqlMetricsRangeSearchRequest:  searchString=[%v] startEpochs=[%v] endEpochs=[%v] step=[%v]", qid, searchText, startTime, endTime, step)

	metricQueryRequest, pqlQuerytype, _, err := convertPqlToMetricsQuery(searchText, startTime, endTime, myid)
	if err != nil {
		ctx.SetContentType(ContentJson)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		WriteJsonResponse(ctx, nil)
		log.Errorf("qid=%v, ProcessPromqlMetricsRangeSearchRequest: Error parsing query err=%+v", qid, err)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("qid=%v, ProcessPromqlMetricsRangeSearchRequest: could not write error message err=%v", qid, err)
		}
		return
	}
	segment.LogMetricsQuery("PromQL metrics query parser", &metricQueryRequest[0], qid)
	res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, qid)

	mQResponse, err := res.GetResultsPromQl(&metricQueryRequest[0].MetricsQuery, pqlQuerytype)
	if err != nil {
		log.Errorf("ProcessPromqlMetricsRangeSearchRequest: Error getting results! %+v", err)
	}
	mQResponse.Data.ResultType = parser.ValueTypeMatrix
	WriteJsonResponse(ctx, &mQResponse)
	ctx.SetContentType(ContentJson)
	ctx.SetStatusCode(fasthttp.StatusOK)

}
func ProcessPromqlBuildInfoRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	ctx.SetContentType("application/json")
	_, err := ctx.Write([]byte(PromQLBuildInfo))
	if err != nil {
		log.Errorf("ProcessPromqlBuildInfoRequest: failed to write response, err=%v", err)
	}
}

func ProcessGetLabelsRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	startParam := string(ctx.FormValue("start"))
	endParam := string(ctx.FormValue("end"))

	endTime := uint32(time.Now().Unix())
	startTime := endTime - TEN_YEARS_IN_SECS
	var err error

	// If startParam exists, parse it
	if startParam != "" {
		startTime, err = utils.ParseTimeForPromQL(startParam)
		if err != nil {
			log.Errorf("ProcessGetLabelsRequest: Error parsing start time parameter, err:%v", err)
			return
		}
	}

	// If endParam exists, parse it
	if endParam != "" {
		endTime, err = utils.ParseTimeForPromQL(endParam)
		if err != nil {
			log.Errorf("ProcessGetLabelsRequest: Error parsing end time parameter, err:%v", err)
			return
		}
	} else {
		endTime = uint32(time.Now().Unix())
	}
	timeRange := &dtu.MetricsTimeRange{
		StartEpochSec: startTime,
		EndEpochSec:   endTime,
	}
	uniqueTagKeysUnrotated, err := metrics.GetUniqueTagKeysForUnrotated(timeRange, myid)
	if err != nil {
		log.Errorf("ProcessGetLabelsRequest: Error getting unique tag keys for unrotated, err:%v", err)
		return
	}

	uniqueTagKeys, err := metadata.GetUniqueTagKeysForRotated(timeRange, myid)
	if err != nil {
		log.Errorf("ProcessGetLabelsRequest: Error getting unique tag keys for rotated, err:%v", err)
		return
	}

	for key := range uniqueTagKeysUnrotated {
		uniqueTagKeys[key] = struct{}{}
	}

	keys := make([]string, 0, len(uniqueTagKeys))
	for key := range uniqueTagKeys {
		keys = append(keys, key)
	}

	response := map[string]interface{}{
		"status": "success",
		"data":   keys,
	}

	WriteJsonResponse(ctx, &response)
	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
}
func ProcessGetLabelValuesRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	labelName := utils.ExtractParamAsString(ctx.UserValue("labelName"))
	startParam := string(ctx.FormValue("start"))
	endParam := string(ctx.FormValue("end"))
	qid := rutils.GetNextQid()
	endTime := uint32(time.Now().Unix())
	startTime := endTime - TEN_YEARS_IN_SECS
	var err error
	var responseValues []string

	if startParam != "" {
		startTime, err = utils.ParseTimeForPromQL(startParam)
		if err != nil {
			log.Errorf("ProcessGetLabelValuesRequest: Error parsing start time parameter, err:%v", err)
			return
		}
	}
	if endParam != "" {
		endTime, err = utils.ParseTimeForPromQL(endParam)
		if err != nil {
			log.Errorf("ProcessGetLabelValuesRequest: Error parsing end time parameter, err:%v", err)
			return
		}
	}

	if labelName == "__name__" {
		timeRange := &dtu.MetricsTimeRange{
			StartEpochSec: startTime,
			EndEpochSec:   endTime,
		}
		responseValues, err = query.GetAllMetricNamesOverTheTimeRange(timeRange, myid)
		if err != nil {
			utils.SendError(ctx, "Failed to get all metric names", "", err)
			return
		}

		response := map[string]interface{}{
			"status": "success",
			"data":   responseValues,
		}
		WriteJsonResponse(ctx, &response)
		ctx.SetContentType(ContentJson)
		ctx.SetStatusCode(fasthttp.StatusOK)
		return
	}

	searchText := fmt.Sprintf(`(fake_metricname{%s="*"})`, labelName)
	metricQueryRequest, _, _, err := convertPqlToMetricsQuery(searchText, startTime, endTime, myid)
	if err != nil {
		ctx.SetContentType(ContentJson)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		WriteJsonResponse(ctx, nil)
		log.Errorf("qid=%v, ProcessGetLabelValuesRequest: Error parsing query err=%+v", qid, err)
		_, err = ctx.WriteString(err.Error())
		if err != nil {
			log.Errorf("qid=%v, ProcessGetLabelValuesRequest: could not write error message err=%v", qid, err)
		}
		return
	}
	if len(metricQueryRequest) == 0 {
		ctx.SetContentType(ContentJson)
		WriteJsonResponse(ctx, map[string]interface{}{})
		return
	}
	metricQueryRequest[0].MetricsQuery.TagValueSearchOnly = true
	segment.LogMetricsQuery("PromQL Label Values request", &metricQueryRequest[0], qid)
	res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, qid)

	responseValues = make([]string, 0, len(res.TagValues))
	for _, innerMap := range res.TagValues {
		for tagValue := range innerMap {
			responseValues = append(responseValues, tagValue)
		}
	}

	response := map[string]interface{}{
		"status": "success",
		"data":   responseValues,
	}
	WriteJsonResponse(ctx, &response)
	ctx.SetContentType(ContentJson)
	ctx.SetStatusCode(fasthttp.StatusOK)
}
func ProcessGetSeriesByLabelRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	qid := rutils.GetNextQid()
	matches := make([]string, 0)
	ctx.QueryArgs().VisitAll(func(key []byte, value []byte) {
		if string(key) == "match[]" {
			matches = append(matches, string(value))
		}
	})

	startParam := string(ctx.FormValue("start"))
	endParam := string(ctx.FormValue("end"))

	startTime, err := utils.ParseTimeForPromQL(startParam)
	if err != nil {
		log.Errorf("ProcessGetSeriesByLabelRequest: Error parsing 'start' parameter, err:%v", err)
	}
	endTime, err := utils.ParseTimeForPromQL(endParam)
	if err != nil {
		log.Errorf("ProcessGetSeriesByLabelRequest: Error parsing 'end' parameter, err:%v", err)
	}

	allSeriesTagsOnlyResults := make(map[uint64]*tsidtracker.AllMatchedTSIDsInfo, 0)

	timeRange := &dtu.MetricsTimeRange{
		StartEpochSec: uint32(startTime),
		EndEpochSec:   uint32(endTime),
	}

	for _, match := range matches {

		metricQueryRequest, _, _, err := convertPqlToMetricsQuery(match, timeRange.StartEpochSec, timeRange.EndEpochSec, myid)
		if err != nil {
			ctx.SetContentType(ContentJson)
			ctx.SetStatusCode(fasthttp.StatusBadRequest)
			WriteJsonResponse(ctx, nil)
			log.Errorf("qid=%v, ProcessGetSeriesByLabelRequest: Error parsing query err=%+v", qid, err)
			_, err = ctx.WriteString(err.Error())
			if err != nil {
				log.Errorf("qid=%v, ProcessGetSeriesByLabelRequest: could not write error message err=%v", qid, err)
			}
			return
		}

		metricQueryRequest[0].MetricsQuery.ExitAfterTagsSearch = true
		metricQueryRequest[0].MetricsQuery.TagIndicesToKeep = make(map[int]struct{})
		metricQueryRequest[0].MetricsQuery.SelectAllSeries = true
		segment.LogMetricsQuery("PromQL series by label request", &metricQueryRequest[0], qid)
		res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, qid)

		for tsid, tsidInfoMap := range res.AllSeriesTagsOnlyMap {
			allSeriesTagsOnlyResults[tsid] = tsidInfoMap
		}
	}

	metricsResult := &mresults.MetricsResult{
		AllSeriesTagsOnlyMap: allSeriesTagsOnlyResults,
	}

	result, err := metricsResult.GetSeriesByLabel()
	if err != nil {
		utils.SendError(ctx, "Failed to get series", fmt.Sprintf("qid: %v, Matches: %+v", qid, matches), err)
		return
	}

	response := map[string]interface{}{
		"status": "success",
		"data":   result,
	}
	WriteJsonResponse(ctx, &response)
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
	for i := range metricQueryRequest {
		hashList = append(hashList, metricQueryRequest[i].MetricsQuery.HashedMName)
		metricQueriesList = append(metricQueriesList, &metricQueryRequest[i].MetricsQuery)
		segment.LogMetricsQuery("PromQL metrics query parser", &metricQueryRequest[i], qid)
		timeRange = &metricQueryRequest[i].TimeRange
	}
	res := segment.ExecuteMultipleMetricsQuery(hashList, metricQueriesList, queryArithmetic, timeRange, qid)
	mQResponse, err := res.GetResultsPromQlForUi(metricQueriesList[0], pqlQuerytype, startTime, endTime)
	if err != nil {
		log.Errorf("ExecuteAsyncQuery: Error getting results! %+v", err)
	}
	WriteJsonResponse(ctx, &mQResponse)
	ctx.SetContentType(ContentJson)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessGetAllMetricNamesRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		utils.SendError(ctx, "empty json body received", "ProcessGetAllMetricNamesRequest: empty json body received", errors.New("empty json body received"))
		return
	}

	readJSON := make(map[string]interface{})
	var jsonc = jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := jsonc.NewDecoder(bytes.NewReader(rawJSON))
	err := decoder.Decode(&readJSON)
	if err != nil {
		utils.SendError(ctx, "Failed to parse request body", "ProcessGetAllMetricsRequest: Failed to parse JSON body: %v", err)
		return
	}

	startTime, err := parseTimeStringToUint32(readJSON["start"])
	if err != nil {
		utils.SendError(ctx, "Failed to parse 'start' from request body", fmt.Sprintf("ProcessGetAllMetricsRequest: Failed to parse 'start' from JSON body with value: %v", readJSON["start"]), errors.New("failed to parse 'start' from JSON body"))
		return
	}
	endTime, err := parseTimeStringToUint32(readJSON["end"])
	if err != nil {
		utils.SendError(ctx, "Failed to parse 'end' from request body", fmt.Sprintf("ProcessGetAllMetricsRequest: Failed to parse 'end' from JSON body with value: %v", readJSON["end"]), errors.New("failed to parse 'end' from JSON body"))
		return
	}

	timeRange := &dtu.MetricsTimeRange{
		StartEpochSec: startTime,
		EndEpochSec:   endTime,
	}

	metricNames, err := query.GetAllMetricNamesOverTheTimeRange(timeRange, myid)
	if err != nil {
		utils.SendError(ctx, "Failed to get all metric names", "", err)
		return
	}

	response := make(map[string]interface{})
	response["metricNames"] = metricNames
	response["metricNamesCount"] = len(metricNames)

	WriteJsonResponse(ctx, &response)
	ctx.SetContentType(ContentJson)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessGetAllMetricTagsRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		utils.SendError(ctx, "empty json body received", "ProcessGetAllMetricTagsRequest: empty json body received", errors.New("empty json body received"))
		return
	}

	qid := rutils.GetNextQid()

	readJSON := make(map[string]interface{})
	var jsonc = jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := jsonc.NewDecoder(bytes.NewReader(rawJSON))
	err := decoder.Decode(&readJSON)
	if err != nil {
		utils.SendError(ctx, "Failed to parse request body", "ProcessGetAllMetricTagsRequest: Failed to parse JSON request body", err)
		return
	}

	startTime, err := parseTimeStringToUint32(readJSON["start"])
	if err != nil {
		utils.SendError(ctx, "Failed to parse 'start' from request body", fmt.Sprintf("ProcessGetAllMetricTagsRequest: Failed to parse 'start' from JSON body with value: %v", readJSON["start"]), errors.New("failed to parse 'start' from JSON body"))
		return
	}
	endTime, err := parseTimeStringToUint32(readJSON["end"])
	if err != nil {
		utils.SendError(ctx, "Failed to parse 'end' from request body", fmt.Sprintf("ProcessGetAllMetricTagsRequest: Failed to parse 'end' from JSON body with value: %v", readJSON["end"]), errors.New("failed to parse 'end' from JSON body"))
		return
	}

	metricName, ok := readJSON["metric_name"].(string)
	if !ok {
		utils.SendError(ctx, "Failed to parse 'metric_name' from JSON body", "ProcessGetAllMetricTagsRequest: 'metric_name' not found in JSON body", errors.New("'metric_name' not found in JSON body"))
		return
	}
	if metricName == "" {
		utils.SendError(ctx, "Failed to parse 'metric_name' from JSON body", "ProcessGetAllMetricTagsRequest: 'metric_name' is an empty string", errors.New("'metric_name' is an empty string"))
		return
	}

	timeRange := &dtu.MetricsTimeRange{
		StartEpochSec: uint32(startTime),
		EndEpochSec:   uint32(endTime),
	}

	searchText := fmt.Sprintf("(%v)", metricName)

	metricQueryRequest, _, _, err := convertPqlToMetricsQuery(searchText, timeRange.StartEpochSec, timeRange.EndEpochSec, myid)
	if err != nil {
		utils.SendError(ctx, "Failed to parse the Metric Name as a Query", fmt.Sprintf("Metric Name: %+v; qid: %v", metricName, qid), err)
		return
	}

	metricQueryRequest[0].MetricsQuery.ExitAfterTagsSearch = true
	metricQueryRequest[0].MetricsQuery.TagIndicesToKeep = make(map[int]struct{})

	segment.LogMetricsQuery("Tags Request PromQL metrics query parser", &metricQueryRequest[0], qid)
	res := segment.ExecuteMetricsQuery(&metricQueryRequest[0].MetricsQuery, &metricQueryRequest[0].TimeRange, qid)

	uniqueTagKeys, tagKeyValueSet, err := res.GetMetricTagsResultSet(&metricQueryRequest[0].MetricsQuery)
	if err != nil {
		utils.SendError(ctx, "Failed to get metric tags", fmt.Sprintf("Metric Name: %+v; qid: %v", metricName, qid), err)
		return
	}

	response := make(map[string]interface{})
	response["uniqueTagKeys"] = uniqueTagKeys
	response["tagKeyValueSet"] = tagKeyValueSet

	WriteJsonResponse(ctx, &response)
	ctx.SetContentType(ContentJson)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessGetMetricTimeSeriesRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	rawJSON := ctx.PostBody()
	if len(rawJSON) == 0 {
		utils.SendError(ctx, "empty json body received", "", nil)
		return
	}
	qid := rutils.GetNextQid()

	start, end, queries, formulas, errorLog, err := parseMetricTimeSeriesRequest(rawJSON)
	if err != nil {
		utils.SendError(ctx, err.Error(), fmt.Sprintf("qid: %v, Error: %+v", qid, errorLog), err)
		return
	}

	// Todo:
	// Some of the Formulas are not being executed properly. Need to fix.
	queryFormulaMap := make(map[string]string)

	for _, query := range queries {
		queryFormulaMap[fmt.Sprintf("%v", query["name"])] = fmt.Sprintf("%v", query["query"])
	}

	finalSearchText, err := buildMetricQueryFromFormulaAndQueries(fmt.Sprintf("%v", formulas[0]["formula"]), queryFormulaMap)
	if err != nil {
		utils.SendError(ctx, "Error building metrics query", fmt.Sprintf("qid: %v, Error: %+v", qid, err), err)
		return
	}

	metricQueryRequest, pqlQuerytype, queryArithmetic, err := convertPqlToMetricsQuery(finalSearchText, start, end, myid)
	if err != nil {
		utils.SendError(ctx, "Error parsing metrics query", fmt.Sprintf("qid: %v, Metrics Query: %+v", qid, finalSearchText), err)
		return
	}

	metricQueriesList := make([]*structs.MetricsQuery, 0)
	var timeRange *dtu.MetricsTimeRange
	hashList := make([]uint64, 0)
	for i := range metricQueryRequest {
		hashList = append(hashList, metricQueryRequest[i].MetricsQuery.HashedMName)
		metricQueriesList = append(metricQueriesList, &metricQueryRequest[i].MetricsQuery)
		segment.LogMetricsQuery("PromQL metrics query parser", &metricQueryRequest[i], qid)
		timeRange = &metricQueryRequest[i].TimeRange
	}
	segment.LogMetricsQueryOps("PromQL metrics query parser: Ops: ", queryArithmetic, qid)
	res := segment.ExecuteMultipleMetricsQuery(hashList, metricQueriesList, queryArithmetic, timeRange, qid)

	if len(res.ErrList) > 0 {
		var errorMessages []string
		for _, err := range res.ErrList {
			errorMessages = append(errorMessages, err.Error())
		}
		allErrors := strings.Join(errorMessages, "; ")
		utils.SendError(ctx, "Failed to get metric time series: "+allErrors, fmt.Sprintf("qid: %v", qid), fmt.Errorf(allErrors))
		return
	}

	mQResponse, err := res.FetchPromqlMetricsForUi(metricQueriesList[0], pqlQuerytype, start, end)
	if err != nil {
		utils.SendError(ctx, "Failed to get metric time series: "+err.Error(), fmt.Sprintf("qid: %v", qid), err)
		return
	}
	WriteJsonResponse(ctx, &mQResponse)
	ctx.SetContentType(ContentJson)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func buildMetricQueryFromFormulaAndQueries(formula string, queries map[string]string) (string, error) {

	finalSearchText := formula
	for key, value := range queries {
		finalSearchText = strings.ReplaceAll(finalSearchText, key, fmt.Sprintf("%v", value))
	}

	log.Infof("buildMetricQueryFromFormulAndQueries: finalSearchText=%v", finalSearchText)

	return finalSearchText, nil
}

func ProcessGetMetricFunctionsRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	ctx.SetContentType("application/json")
	_, err := ctx.Write([]byte(metricFunctions))
	if err != nil {
		log.Errorf("ProcessGetMetricFunctionsRequest: failed to write response, err=%v", err)
	}
}
func parseMetricTimeSeriesRequest(rawJSON []byte) (uint32, uint32, []map[string]interface{}, []map[string]interface{}, string, error) {
	var start = uint32(0)
	var end = uint32(0)
	queries := make([]map[string]interface{}, 0)
	formulas := make([]map[string]interface{}, 0)
	errorLog := ""
	var err error
	var respBodyErr error

	readJSON := make(map[string]interface{})
	var jsonc = jsoniter.ConfigCompatibleWithStandardLibrary
	decoder := jsonc.NewDecoder(bytes.NewReader(rawJSON))
	err = decoder.Decode(&readJSON)
	if err != nil {
		respBodyErr = errors.New("failed to parse request body")
		errorLog = fmt.Sprintf("the request JSON body received is : %v and err: %v", string(rawJSON), err)
		return start, end, queries, formulas, errorLog, respBodyErr
	}

	start, err = parseTimeStringToUint32(readJSON["start"])
	if err != nil {
		respBodyErr = errors.New("failed to parse startTime from JSON body")
		errorLog = "failed to parse startTime from JSON body"
		return start, end, queries, formulas, errorLog, respBodyErr

	}
	end, err = parseTimeStringToUint32(readJSON["end"])
	if err != nil {
		respBodyErr = errors.New("failed to parse endTime from JSON body")
		errorLog = "failed to parse endTime from JSON body"
		return start, end, queries, formulas, errorLog, respBodyErr
	}

	queryInterfaces, ok := readJSON["queries"].([]interface{})
	if !ok {
		respBodyErr = errors.New("failed to parse 'queries' from JSON body")
		errorLog = fmt.Sprintf("failed to parse 'queries' from JSON body as []interface{} with value: %v", readJSON["queries"])
		return start, end, queries, formulas, errorLog, respBodyErr
	}

	queries = make([]map[string]interface{}, len(queryInterfaces))
	for i, qi := range queryInterfaces {
		queryMap, ok := qi.(map[string]interface{})
		if !ok {
			respBodyErr = errors.New("failed to parse 'query' from JSON body")
			errorLog = fmt.Sprintf("failed to parse 'query' object as a map[string]interface{}, 'query' value: %v", qi)
			return start, end, queries, formulas, errorLog, respBodyErr
		}
		_, ok = queryMap["name"].(string)
		if !ok {
			respBodyErr = errors.New("failed to parse 'name' from JSON body")
			errorLog = fmt.Sprintf("name is either missing or not a string in the query object: %v", queryMap)
			return start, end, queries, formulas, errorLog, respBodyErr
		}

		_, ok = queryMap["query"].(string)
		if !ok {
			respBodyErr = errors.New("failed to parse 'query' field from 'query' object in JSON body")
			errorLog = fmt.Sprintf("JSON property 'query' is either missing or not a string in the query object: %v", queryMap)
			return start, end, queries, formulas, errorLog, respBodyErr
		}

		_, ok = queryMap["qlType"].(string)
		if !ok {
			respBodyErr = errors.New("failed to parse 'qlType' from JSON body")
			errorLog = fmt.Sprintf("qlType is either missing or not a string in the query object: %v", queryMap)
			return start, end, queries, formulas, errorLog, respBodyErr
		}
		queries[i] = queryMap
	}

	formulaInterfaces, ok := readJSON["formulas"].([]interface{})
	if !ok {
		respBodyErr = errors.New("failed to parse 'formulas' from JSON body")
		errorLog = fmt.Sprintf("failed to parse 'formulas' from JSON body as []interface{} with value: %v", readJSON["formulas"])
		return start, end, queries, formulas, errorLog, respBodyErr
	}

	formulas = make([]map[string]interface{}, len(formulaInterfaces))
	for i, fi := range formulaInterfaces {
		formulaMap, ok := fi.(map[string]interface{})
		if !ok {
			respBodyErr = errors.New("failed to parse 'formula' object from JSON body")
			errorLog = fmt.Sprintf("failed to parse 'formula' object as a map[string]interface{}, 'formula' value: %v", fi)
			return start, end, queries, formulas, errorLog, respBodyErr
		}

		_, ok = formulaMap["formula"].(string)
		if !ok {
			respBodyErr = errors.New("failed to parse 'formula' field from 'formula' object in JSON body")
			errorLog = fmt.Sprintf("formula is either missing or not a string in the formula object: %v", formulaMap)
			return start, end, queries, formulas, errorLog, respBodyErr
		}

		formulas[i] = formulaMap
	}

	return start, end, queries, formulas, errorLog, nil
}

func convertPqlToMetricsQuery(searchText string, startTime, endTime uint32, myid uint64) ([]structs.MetricsQueryRequest, parser.ValueType, []structs.QueryArithmetic, error) {
	// call prometheus promql parser
	expr, err := parser.ParseExpr(searchText)
	if err != nil {
		return []structs.MetricsQueryRequest{}, "", []structs.QueryArithmetic{}, err
	}
	pqlQuerytype := expr.Type()
	var mquery structs.MetricsQuery
	mquery.Aggregator = structs.Aggregation{}
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

	intervalSeconds, err := mresults.CalculateInterval(endTime - startTime)
	if err != nil {
		return []structs.MetricsQueryRequest{}, "", []structs.QueryArithmetic{}, err
	}

	var groupby bool
	switch expr := expr.(type) {
	case *parser.AggregateExpr:
		es := &parser.EvalStmt{
			Expr:     expr,
			Start:    time.Now().Add(time.Duration(-5) * time.Minute),
			End:      time.Now(),
			Interval: time.Duration(1 * time.Minute),
			// LookbackDelta: 0,
		}

		mquery.Aggregator = structs.Aggregation{}
		parser.Inspect(es.Expr, func(node parser.Node, path []parser.Node) error {
			// If there is no child node, just return nil
			if node == nil {
				return nil
			}
			switch expr := node.(type) {
			case *parser.AggregateExpr:
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
					log.Infof("convertPqlToMetricsQuery: using avg aggregator by default for AggregateExpr (got %v)", aggFunc)
					mquery.Aggregator = structs.Aggregation{AggregatorFunction: segutils.Avg}
				}
			case *parser.VectorSelector:
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
					log.Infof("convertPqlToMetricsQuery: using avg aggregator by default for VectorSelector (got %v)", aggFunc)
					mquery.Aggregator = structs.Aggregation{AggregatorFunction: segutils.Avg}
				}
			case *parser.NumberLiteral:
				mquery.Aggregator.FuncConstant = expr.Val
			default:
				err := fmt.Errorf("convertPqlToMetricsQuery: parser.Inspect: Unsupported node type %T", node)
				log.Errorf("%v", err)
				return err
			}
			return nil
		})
	case *parser.Call:
		// E.g: rate(http_requests_total[5m]), So, the process of the Inspect method calling node is as follows:
		// promql.Call:"rate(http_requests_total[5m])" -> promql.Expressions[0] -> promql.MatrixSelector:"http_requests_total[5m]"
		// Since we currently handle evaluation logic only in sub-elements like MatrixSelector or VectorSelector, if we add a default case in the switch statement,
		// traversal would stop prematurely due to an error being returned before reaching sub-nodes such as MatrixSelector
		parser.Inspect(expr, func(node parser.Node, path []parser.Node) error {
			// If there is no child node, just return nil
			if node == nil {
				return nil
			}
			switch node.(type) {
			case *parser.MatrixSelector:
				function := extractFuncFromPath(path)

				if mquery.TagsFilters != nil {
					groupby = true
				}

				timeWindow, _, err := extractTimeWindow(expr.Args)
				if err != nil {
					return fmt.Errorf("parser.Inspect: can not extract time window from a range vector: %v", err)
				}
				switch function {
				case "deriv":
					mquery.Function = structs.Function{RangeFunction: segutils.Derivative, TimeWindow: timeWindow}
				case "predict_linear":
					if len(expr.Args) != 2 {
						return fmt.Errorf("parser.Inspect: Incorrect parameters: %v for the predict_linear function", expr.Args.String())
					}
					mquery.Function = structs.Function{RangeFunction: segutils.Predict_Linear, TimeWindow: timeWindow, ValueList: []string{expr.Args[1].String()}}
				case "delta":
					mquery.Function = structs.Function{RangeFunction: segutils.Delta, TimeWindow: timeWindow}
				case "idelta":
					mquery.Function = structs.Function{RangeFunction: segutils.IDelta, TimeWindow: timeWindow}
				case "rate":
					mquery.Function = structs.Function{RangeFunction: segutils.Rate, TimeWindow: timeWindow}
				case "irate":
					mquery.Function = structs.Function{RangeFunction: segutils.IRate, TimeWindow: timeWindow}
				case "increase":
					mquery.Function = structs.Function{RangeFunction: segutils.Increase, TimeWindow: timeWindow}
				case "avg_over_time":
					mquery.Function = structs.Function{RangeFunction: segutils.Avg_Over_Time, TimeWindow: timeWindow}
				case "min_over_time":
					mquery.Function = structs.Function{RangeFunction: segutils.Min_Over_Time, TimeWindow: timeWindow}
				case "max_over_time":
					mquery.Function = structs.Function{RangeFunction: segutils.Max_Over_Time, TimeWindow: timeWindow}
				case "sum_over_time":
					mquery.Function = structs.Function{RangeFunction: segutils.Sum_Over_Time, TimeWindow: timeWindow}
				case "count_over_time":
					mquery.Function = structs.Function{RangeFunction: segutils.Count_Over_Time, TimeWindow: timeWindow}
				case "stdvar_over_time":
					mquery.Function = structs.Function{RangeFunction: segutils.Stdvar_Over_Time, TimeWindow: timeWindow}
				case "stddev_over_time":
					mquery.Function = structs.Function{RangeFunction: segutils.Stddev_Over_Time, TimeWindow: timeWindow}
				case "last_over_time":
					mquery.Function = structs.Function{RangeFunction: segutils.Last_Over_Time, TimeWindow: timeWindow}
				case "present_over_time":
					mquery.Function = structs.Function{RangeFunction: segutils.Present_Over_Time, TimeWindow: timeWindow}
				case "quantile_over_time":
					if len(expr.Args) != 2 {
						return fmt.Errorf("parser.Inspect: Incorrect parameters: %v for the quantile_over_time function", expr.Args.String())
					}
					mquery.Function = structs.Function{RangeFunction: segutils.Quantile_Over_Time, TimeWindow: timeWindow, ValueList: []string{expr.Args[0].String()}}
				case "changes":
					mquery.Function = structs.Function{RangeFunction: segutils.Changes, TimeWindow: timeWindow}
				case "resets":
					mquery.Function = structs.Function{RangeFunction: segutils.Resets, TimeWindow: timeWindow}
				default:
					return fmt.Errorf("parser.Inspect: unsupported function type %v", function)
				}
			case *parser.VectorSelector:
				function := extractFuncFromPath(path)
				switch function {
				case "abs":
					mquery.Function = structs.Function{MathFunction: segutils.Abs}
				case "sqrt":
					mquery.Function = structs.Function{MathFunction: segutils.Sqrt}
				case "ceil":
					mquery.Function = structs.Function{MathFunction: segutils.Ceil}
				case "round":
					mquery.Function = structs.Function{MathFunction: segutils.Round}
					if len(expr.Args) > 1 {
						mquery.Function.ValueList = []string{expr.Args[1].String()}
					}
				case "floor":
					mquery.Function = structs.Function{MathFunction: segutils.Floor}
				case "exp":
					mquery.Function = structs.Function{MathFunction: segutils.Exp}
				case "ln":
					mquery.Function = structs.Function{MathFunction: segutils.Ln}
				case "log2":
					mquery.Function = structs.Function{MathFunction: segutils.Log2}
				case "log10":
					mquery.Function = structs.Function{MathFunction: segutils.Log10}
				case "sgn":
					mquery.Function = structs.Function{MathFunction: segutils.Sgn}
				case "deg":
					mquery.Function = structs.Function{MathFunction: segutils.Deg}
				case "rad":
					mquery.Function = structs.Function{MathFunction: segutils.Rad}
				case "acos":
					mquery.Function = structs.Function{MathFunction: segutils.Acos}
				case "acosh":
					mquery.Function = structs.Function{MathFunction: segutils.Acosh}
				case "asin":
					mquery.Function = structs.Function{MathFunction: segutils.Asin}
				case "asinh":
					mquery.Function = structs.Function{MathFunction: segutils.Asinh}
				case "atan":
					mquery.Function = structs.Function{MathFunction: segutils.Atan}
				case "atanh":
					mquery.Function = structs.Function{MathFunction: segutils.Atanh}
				case "cos":
					mquery.Function = structs.Function{MathFunction: segutils.Cos}
				case "cosh":
					mquery.Function = structs.Function{MathFunction: segutils.Cosh}
				case "sin":
					mquery.Function = structs.Function{MathFunction: segutils.Sin}
				case "sinh":
					mquery.Function = structs.Function{MathFunction: segutils.Sinh}
				case "tan":
					mquery.Function = structs.Function{MathFunction: segutils.Tan}
				case "tanh":
					mquery.Function = structs.Function{MathFunction: segutils.Tanh}
				case "clamp":
					if len(expr.Args) != 3 {
						return fmt.Errorf("parser.Inspect: Incorrect parameters: %v for the clamp function", expr.Args.String())
					}
					mquery.Function = structs.Function{MathFunction: segutils.Clamp, ValueList: []string{expr.Args[1].String(), expr.Args[2].String()}}
				case "clamp_max":
					if len(expr.Args) != 2 {
						return fmt.Errorf("parser.Inspect: Incorrect parameters: %v for the clamp_max function", expr.Args.String())
					}
					mquery.Function = structs.Function{MathFunction: segutils.Clamp_Max, ValueList: []string{expr.Args[1].String()}}
				case "clamp_min":
					if len(expr.Args) != 2 {
						return fmt.Errorf("parser.Inspect: Incorrect parameters: %v for the clamp_min function", expr.Args.String())
					}
					mquery.Function = structs.Function{MathFunction: segutils.Clamp_Min, ValueList: []string{expr.Args[1].String()}}
				case "timestamp":
					mquery.Function = structs.Function{MathFunction: segutils.Timestamp}
				case "hour":
					mquery.Function = structs.Function{TimeFunction: segutils.Hour}
				case "minute":
					mquery.Function = structs.Function{TimeFunction: segutils.Minute}
				case "month":
					mquery.Function = structs.Function{TimeFunction: segutils.Month}
				case "year":
					mquery.Function = structs.Function{TimeFunction: segutils.Year}
				case "day_of_month":
					mquery.Function = structs.Function{TimeFunction: segutils.DayOfMonth}
				case "day_of_week":
					mquery.Function = structs.Function{TimeFunction: segutils.DayOfWeek}
				case "day_of_year":
					mquery.Function = structs.Function{TimeFunction: segutils.DayOfYear}
				case "days_in_month":
					mquery.Function = structs.Function{TimeFunction: segutils.DaysInMonth}
				default:
					return fmt.Errorf("parser.Inspect: unsupported function type %v", function)
				}
			}
			return nil
		})
	case *parser.VectorSelector:
		mquery.HashedMName = xxhash.Sum64String(mquery.MetricName)
		mquery.OrgId = myid
		mquery.SelectAllSeries = true
		agg := structs.Aggregation{AggregatorFunction: segutils.Avg}
		mquery.Downsampler = structs.Downsampler{Interval: int(intervalSeconds), Unit: "s", Aggregator: agg}

		if len(mquery.TagsFilters) > 0 {
			mquery.SelectAllSeries = false
		}

		metricQueryRequest := &structs.MetricsQueryRequest{
			MetricsQuery: mquery,
			TimeRange: dtu.MetricsTimeRange{
				StartEpochSec: startTime,
				EndEpochSec:   endTime,
			},
		}
		return []structs.MetricsQueryRequest{*metricQueryRequest}, pqlQuerytype, []structs.QueryArithmetic{}, nil
	case *parser.BinaryExpr:
		arithmeticOperation := structs.QueryArithmetic{}
		var lhsValType, rhsValType parser.ValueType
		var lhsRequest, rhsRequest []structs.MetricsQueryRequest
		if constant, ok := expr.LHS.(*parser.NumberLiteral); ok {
			arithmeticOperation.ConstantOp = true
			arithmeticOperation.Constant = constant.Val
		} else {
			lhsRequest, lhsValType, _, err = convertPqlToMetricsQuery(expr.LHS.String(), startTime, endTime, myid)
			if err != nil {
				return []structs.MetricsQueryRequest{}, "", []structs.QueryArithmetic{}, err
			}
			arithmeticOperation.LHS = lhsRequest[0].MetricsQuery.HashedMName

		}

		if constant, ok := expr.RHS.(*parser.NumberLiteral); ok {
			arithmeticOperation.ConstantOp = true
			arithmeticOperation.Constant = constant.Val
		} else {
			rhsRequest, rhsValType, _, err = convertPqlToMetricsQuery(expr.RHS.String(), startTime, endTime, myid)
			if err != nil {
				return []structs.MetricsQueryRequest{}, "", []structs.QueryArithmetic{}, err
			}
			arithmeticOperation.RHS = rhsRequest[0].MetricsQuery.HashedMName

		}
		arithmeticOperation.Operation = getLogicalAndArithmeticOperation(expr.Op)
		if rhsValType == parser.ValueTypeVector {
			lhsValType = parser.ValueTypeVector
		}
		return append(lhsRequest, rhsRequest...), lhsValType, []structs.QueryArithmetic{arithmeticOperation}, nil
	case *parser.ParenExpr:
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
		mquery.Aggregator = structs.Aggregation{AggregatorFunction: segutils.Avg}
	}
	mquery.Downsampler = structs.Downsampler{Interval: int(intervalSeconds), Unit: "s", Aggregator: mquery.Aggregator}
	if len(mquery.TagsFilters) > 0 {
		mquery.SelectAllSeries = false
	} else {
		mquery.SelectAllSeries = true
	}
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

func getLogicalAndArithmeticOperation(op parser.ItemType) segutils.LogicalAndArithmeticOperator {
	switch op {
	case parser.ADD:
		return segutils.LetAdd
	case parser.SUB:
		return segutils.LetSubtract
	case parser.MUL:
		return segutils.LetMultiply
	case parser.DIV:
		return segutils.LetDivide
	case parser.GTR:
		return segutils.LetGreaterThan
	case parser.GTE:
		return segutils.LetGreaterThanOrEqualTo
	case parser.LSS:
		return segutils.LetLessThan
	case parser.LTE:
		return segutils.LetLessThanOrEqualTo
	case parser.EQLC:
		return segutils.LetEquals
	case parser.NEQ:
		return segutils.LetNotEquals
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

// extractGroupsFromPath parses vector outer function and extracts grouping information if by or without was used.
func extractGroupsFromPath(p []parser.Node) (bool, []string) {
	if len(p) == 0 {
		return false, nil
	}
	switch n := p[len(p)-1].(type) {
	case *parser.AggregateExpr:
		return !n.Without, n.Grouping
	case parser.Expressions:
		groupByVals := make([]string, 0)
		for _, entry := range n {
			expr, ok := entry.(*parser.MatrixSelector)
			if !ok {
				continue
			}

			vectorSelector, ok := expr.VectorSelector.(*parser.VectorSelector)
			if !ok {
				continue
			}

			for _, labels := range vectorSelector.LabelMatchers {
				if labels.Name != "__name__" {
					groupByVals = append(groupByVals, labels.Name)
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
func extractFuncFromPath(p []parser.Node) string {
	if len(p) == 0 {
		return ""
	}
	switch n := p[len(p)-1].(type) {
	case *parser.AggregateExpr:
		return n.Op.String()
	case *parser.Call:
		return n.Func.Name
	case *parser.BinaryExpr:
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

func parseTimeStringToUint32(s interface{}) (uint32, error) {
	var startTimeStr string
	var timeVal uint32

	switch valtype := s.(type) {
	case int:
		startTimeStr = fmt.Sprintf("%d", valtype)
	case float64:
		startTimeStr = fmt.Sprintf("%d", int64(valtype))
	case string:
		if strings.Contains(s.(string), "now") {
			nowTs := utils.GetCurrentTimeInMs()
			defValue := nowTs - (1 * 60 * 1000)
			pastXhours, _ := parseAlphaNumTime(nowTs, s.(string), defValue)
			startTimeStr = fmt.Sprintf("%d", pastXhours)
		} else {
			startTimeStr = valtype
		}
	default:
		return timeVal, errors.New("Failed to parse time from JSON request body.TimeField is not a string!")
	}
	timeVal, err := parseTimeFromString(startTimeStr)
	if err != nil {
		return timeVal, err
	}
	return timeVal, nil
}
