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

package otsdbquery

import (
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cespare/xxhash"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/structs"
	utils "github.com/siglens/siglens/pkg/segment/utils"
	. "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

var aggregatorMapping = map[string]utils.AggregateFunctions{
	"count":       utils.Count,
	"avg":         utils.Avg,
	"min":         utils.Min,
	"max":         utils.Max,
	"sum":         utils.Sum,
	"cardinality": utils.Cardinality,
}

func MetricsQueryExpressionsParser(ctx *fasthttp.RequestCtx) {
	var httpResp HttpServerResponse
	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		httpResp.Message = "Invalid raw JSON request"
		httpResp.StatusCode = fasthttp.StatusBadRequest
		WriteResponse(ctx, httpResp)
		return
	}
	var readJSON *structs.OTSDBMetricsQueryExpRequest
	if err := json.Unmarshal(rawJSON, &readJSON); err != nil {
		var badJsonKey string
		if e, ok := err.(*json.UnmarshalTypeError); ok {
			badJsonKey = e.Field
		}
		log.Errorf("MetricsQueryExpressionsParser: Error unmarshalling JSON. Failed to parse key: %s. Error: %v", badJsonKey, err)
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		httpResp.Message = err.Error()
		httpResp.StatusCode = fasthttp.StatusBadRequest
		WriteResponse(ctx, httpResp)
		return
	}
	metricsQueryRequest, err := MetricsQueryExpressionsParseRequest(readJSON)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		httpResp.Message = err.Error()
		httpResp.StatusCode = fasthttp.StatusBadRequest
		WriteResponse(ctx, httpResp)
		return
	}
	var wg sync.WaitGroup
	expMetricsQueryResult := make(map[string][]*structs.MetricsQueryResponse)
	var expMetricsQueryResultLock sync.Mutex
	for alias, req := range metricsQueryRequest {
		wg.Add(1)
		go asyncMetricsQueryRequest(&wg, alias, req, &expMetricsQueryResult, &expMetricsQueryResultLock)
	}
	wg.Wait()
	WriteJsonResponse(ctx, &expMetricsQueryResult)
	ctx.SetContentType(ContentJson)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func asyncMetricsQueryRequest(wg *sync.WaitGroup, alias string, req *structs.MetricsQueryRequest, expMetricsQueryResult *map[string][]*structs.MetricsQueryResponse, expMetricsQueryResultLock *sync.Mutex) {
	defer wg.Done()
	qid := rutils.GetNextQid()
	segment.LogMetricsQuery("metrics query parser", req, qid)
	res := segment.ExecuteMetricsQuery(&req.MetricsQuery, &req.TimeRange, qid)
	mQResponse, err := res.GetOTSDBResults(&req.MetricsQuery)
	if err != nil {
		return
	}
	expMetricsQueryResultLock.Lock()
	(*expMetricsQueryResult)[alias] = mQResponse
	expMetricsQueryResultLock.Unlock()
}

func MetricsQueryExpressionsParseRequest(queryRequest *structs.OTSDBMetricsQueryExpRequest) (map[string]*structs.MetricsQueryRequest, error) {
	var metricsQueryRequest map[string]*structs.MetricsQueryRequest = make(map[string]*structs.MetricsQueryRequest)
	var timeRange dtu.MetricsTimeRange

	var startStr string
	if queryRequest.Time.Start == nil || queryRequest.Time.Start == "" {
		return nil, fmt.Errorf("Invalid query - missing 'Start Time'")
	}
	switch v := queryRequest.Time.Start.(type) {
	case int:
		startStr = fmt.Sprintf("%d", v)
	case float64:
		startStr = fmt.Sprintf("%d", int64(v))
	case string:
		startStr = v
	}
	start, err := parseTime(startStr)
	if err != nil {
		log.Errorf("MetricsQueryExpressionsParseRequest: Unable to parse Start time: %v. Error: %+v", queryRequest.Time.Start, err)
		return nil, fmt.Errorf("Unable to parse Start time. Error: %+v", err)
	}

	var endStr string
	var end uint32
	if queryRequest.Time.End == nil || queryRequest.Time.End == "" {
		end = uint32(time.Now().Unix())
	} else {
		switch v := queryRequest.Time.End.(type) {
		case int:
			endStr = fmt.Sprintf("%d", v)
		case float64:
			endStr = fmt.Sprintf("%d", int64(v))
		case string:
			endStr = v
		}
		end, err = parseTime(endStr)
		if err != nil {
			log.Errorf("MetricsQueryExpressionsParseRequest: Unable to parse End time: %v. Error: %+v", queryRequest.Time.End, err)
			return nil, fmt.Errorf("Unable to parse End time. Error: %+v", err)
		}
	}

	timeRange.StartEpochSec = start
	timeRange.EndEpochSec = end

	aggregator, downsampler, err := metricsQueryExpressionsParseAggregatorDownsampler(queryRequest)
	if err != nil {
		log.Errorf("MetricsQueryExpressionsParseRequest: Unable to parse Aggregator and/or Downsampler. Error: %+v", err)
		return nil, fmt.Errorf("Unable to parse Aggregator and/or Downsampler. Error: %+v", err)
	}
	metrics := metricsQueryExpressionsParseMetrics(queryRequest)
	tags := metricsQueryExpressionsParseTags(queryRequest)

	for _, output := range queryRequest.Outputs {
		id := output.Id
		metricInfo, ok := metrics[id]
		if !ok {
			log.Errorf("MetricsQueryExpressionsParseRequest: the output id %v does not match any metric id", id)
			continue
		}
		if metricInfo["aggregator"] != -1 {
			aggregator = metricInfo["aggregator"].(utils.AggregateFunctions)
		}
		tagsFilter, ok := tags[metricInfo["filter"].(string)]
		if !ok {
			log.Errorf("MetricsQueryExpressionsParseRequest: tags does not contain filter %s", metricInfo["filter"])
			continue
		}

		mReq := &structs.MetricsQueryRequest{
			MetricsQuery: structs.MetricsQuery{
				MetricName:  metricInfo["metric-name"].(string),
				HashedMName: metricInfo["hashed-mname"].(uint64),
				TagsFilters: tagsFilter,
				Aggregator:  structs.Aggreation{AggregatorFunction: aggregator},
				Downsampler: downsampler,
			},
			TimeRange: timeRange,
		}
		metricsQueryRequest[output.Alias] = mReq
	}
	return metricsQueryRequest, nil
}

func metricsQueryExpressionsParseAggregatorDownsampler(queryRequest *structs.OTSDBMetricsQueryExpRequest) (utils.AggregateFunctions, structs.Downsampler, error) {
	downsampler := structs.Downsampler{Interval: 1, Unit: "m", Aggregator: structs.Aggreation{AggregatorFunction: aggregatorMapping["avg"]}}
	aggregator, ok := aggregatorMapping[queryRequest.Time.Aggregator]
	if !ok {
		log.Errorf("metricsQueryExpressionsParseAggregatorDownsampler: unsupported aggregator function: %s", queryRequest.Time.Aggregator)
		return -1, downsampler, fmt.Errorf("unsupported aggregator function: %s", queryRequest.Time.Aggregator)
	}
	if queryRequest.Time.Downsampler.Interval == "" && queryRequest.Time.Downsampler.Aggregator == "" {
		return aggregator, downsampler, nil
	}
	if queryRequest.Time.Downsampler.Interval == "" || queryRequest.Time.Downsampler.Aggregator == "" {
		log.Errorf("metricsQueryExpressionsParseAggregatorDownsampler: unsupported aggregator function: %s", queryRequest.Time.Aggregator)
		return aggregator, downsampler, fmt.Errorf("incomplete downsampler function: %v", queryRequest.Time.Downsampler)
	}
	downsampler.Aggregator = structs.Aggreation{AggregatorFunction: aggregatorMapping[queryRequest.Time.Downsampler.Aggregator]}
	var intervalStr, unit string
	for _, c := range queryRequest.Time.Downsampler.Interval {
		if c >= '0' && c <= '9' {
			intervalStr += string(c)
		} else {
			unit += string(c)
		}
	}
	if len(intervalStr) == 0 || len(unit) == 0 {
		log.Errorf("metricsQueryExpressionsParseAggregatorDownsampler: invalid downsampler(no interval or unit) format for %s", queryRequest.Time.Downsampler.Interval)
		return aggregator, downsampler, fmt.Errorf("invalid interval format for downsampler %s", queryRequest.Time.Downsampler.Interval)
	}
	interval, err := strconv.Atoi(intervalStr)
	if err != nil {
		log.Errorf("metricsQueryExpressionsParseAggregatorDownsampler: invalid interval in downsampler. Error: %v", err)
		return aggregator, downsampler, err
	}
	downsampler.Interval = interval
	downsampler.Unit = unit
	return aggregator, downsampler, nil
}

func metricsQueryExpressionsParseMetrics(queryRequest *structs.OTSDBMetricsQueryExpRequest) map[string]map[string]interface{} {
	var metricInfo map[string]map[string]interface{} = make(map[string]map[string]interface{})
	for _, metric := range queryRequest.Metrics {
		aggregator, ok := aggregatorMapping[queryRequest.Time.Aggregator]
		if !ok {
			aggregator = -1
		}
		metricInfo[metric.Id] = map[string]interface{}{"metric-name": metric.MetricName, "hashed-mname": xxhash.Sum64String(metric.MetricName), "filter": metric.Filter, "aggregator": aggregator}
	}
	return metricInfo
}

func metricsQueryExpressionsParseTags(queryRequest *structs.OTSDBMetricsQueryExpRequest) map[string][]*structs.TagsFilter {
	var tagInfo map[string][]*structs.TagsFilter = make(map[string][]*structs.TagsFilter)

	for _, tags := range queryRequest.Filters {
		id := tags.Id
		tagsList := make([]*structs.TagsFilter, 0)
		for _, filter := range tags.Tags {
			tagsList = append(tagsList, &structs.TagsFilter{
				TagKey:       filter.Tagk,
				RawTagValue:  filter.Filter,
				HashTagValue: xxhash.Sum64String(filter.Filter),
			})
		}
		tagInfo[id] = tagsList
	}
	return tagInfo
}
