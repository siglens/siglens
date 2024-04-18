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

package otsdbquery

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	rutils "github.com/siglens/siglens/pkg/readerUtils"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/structs"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	toputils "github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

func MetricsQueryParser(ctx *fasthttp.RequestCtx, myid uint64) {
	var httpResp toputils.HttpServerResponse
	queryReq := ctx.QueryArgs()
	startStr := string(queryReq.Peek("start"))
	endStr := string(queryReq.Peek("end"))
	m := string(queryReq.Peek("m"))
	mQRequest, err := ParseRequest(startStr, endStr, m, myid)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		httpResp.Message = err.Error()
		httpResp.StatusCode = fasthttp.StatusBadRequest
		toputils.WriteResponse(ctx, httpResp)
		return
	}
	qid := rutils.GetNextQid()
	segment.LogMetricsQuery("metrics query parser", mQRequest, qid)
	mQRequest.MetricsQuery.OrgId = myid
	res := segment.ExecuteMetricsQuery(&mQRequest.MetricsQuery, &mQRequest.TimeRange, qid)
	mQResponse, err := res.GetOTSDBResults(&mQRequest.MetricsQuery)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusBadRequest)
		httpResp.Message = err.Error()
		httpResp.StatusCode = fasthttp.StatusBadRequest
		toputils.WriteResponse(ctx, httpResp)
		return
	}
	toputils.WriteJsonResponse(ctx, &mQResponse)
	ctx.SetContentType(toputils.ContentJson)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ParseRequest(startStr string, endStr string, m string, myid uint64) (*structs.MetricsQueryRequest, error) {
	if startStr == "" || m == "" {
		return nil, fmt.Errorf("Invalid query - missing 'start' or 'm' parameter")
	}
	start, err := parseTime(startStr)
	if err != nil {
		log.Errorf("MetricsQueryParser: Unable to parse Start time: %v. Error: %+v", startStr, err)
		return nil, fmt.Errorf("Unable to parse Start time. Error: %+v", err)
	}
	var end uint32
	if endStr == "" {
		end = uint32(time.Now().Unix())
	} else {
		end, err = parseTime(endStr)
		if err != nil {
			log.Errorf("MetricsQueryParser: Unable to parse End time: %v. Error: %+v", endStr, err)
			return nil, fmt.Errorf("Unable to parse End time. Error: %+v", err)
		}
	}

	metricName, hashedMName, tags, err := parseMetricTag(m)
	if err != nil {
		log.Errorf("MetricsQueryParser: Unable to parse Metrics Tag: %v. Error: %+v", m, err)
		return nil, fmt.Errorf("Unable to parse Metrics Tag. Error: %+v", err)
	}
	aggregator, downsampler, err := parseAggregatorDownsampler(m)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse Aggregator and/or Downsampler. Error: %+v", err)
	}

	metricQueryRequest := &structs.MetricsQueryRequest{
		MetricsQuery: structs.MetricsQuery{
			MetricName:  metricName,
			HashedMName: hashedMName,
			TagsFilters: tags,
			Aggregator:  structs.Aggreation{AggregatorFunction: aggregator},
			Downsampler: downsampler,
			OrgId:       myid,
		},
		TimeRange: dtu.MetricsTimeRange{
			StartEpochSec: start,
			EndEpochSec:   end,
		},
	}
	return metricQueryRequest, nil
}

func parseTime(timeStr string) (uint32, error) {
	if strings.HasSuffix(timeStr, "-ago") {
		var duration time.Duration
		durationStr := strings.TrimSuffix(timeStr, "-ago")
		unit := durationStr[len(durationStr)-1]
		durationNum, err := strconv.Atoi(strings.TrimSuffix(durationStr, string(unit)))
		if err != nil {
			log.Errorf("parseTime: invalid time format: %s. Error: %v", timeStr, err)
			return 0, err
		}
		switch unit {
		case 's':
			duration = time.Duration(durationNum) * time.Second
		case 'm':
			duration = time.Duration(durationNum) * time.Minute
		case 'h':
			duration = time.Duration(durationNum) * time.Hour
		case 'd':
			duration = time.Duration(durationNum) * 24 * time.Hour
		case 'w':
			duration = time.Duration(durationNum) * 7 * 24 * time.Hour
		case 'n':
			duration = time.Duration(durationNum) * 30 * 24 * time.Hour
		case 'y':
			duration = time.Duration(durationNum) * 365 * 24 * time.Hour
		default:
			log.Errorf("parseTime: invalid time format: %s", timeStr)
			return 0, fmt.Errorf("invalid time format: %s", timeStr)
		}
		return uint32(time.Now().Add(-duration).Unix()), nil
	}
	// if it is not a relative time, parse as absolute time
	var t time.Time
	var err error
	// unixtime
	if unixTime, err := strconv.ParseInt(timeStr, 10, 64); err == nil {
		if toputils.IsTimeInMilli(uint64(unixTime)) {
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

func parseMetricTag(m string) (string, uint64, []*structs.TagsFilter, error) {
	logicalOperator := segutils.And
	metricStart := strings.LastIndex(m, ":") + 1
	metricEnd := strings.Index(m, "{")
	if metricEnd == -1 {
		metricEnd = len(m)
	}
	metric := m[metricStart:metricEnd]
	hashedMName := xxhash.Sum64String(metric)

	tagsStart := strings.Index(m, "{")
	tagsEnd := strings.Index(m, "}")
	if tagsStart == -1 || tagsEnd == -1 {
		log.Errorf("parseMetricTag: invalid query format, either tasgStart or tagsEnd was -1 for m: %s", m)
		return metric, hashedMName, nil, fmt.Errorf("invalid query format, either tasgStart or tagsEnd was -1 for m: %s", m)
	}
	tagsStr := m[tagsStart+1 : tagsEnd]
	listOfTags := strings.Split(tagsStr, ",")
	tags := make([]*structs.TagsFilter, 0)
	var val interface{}
	var key, multipleTagValues string
	var hashedTagVal uint64
	for _, tag := range listOfTags {
		pair := strings.Split(tag, "=")
		if len(pair) != 2 {
			log.Errorf("parseMetricTag: invalid tag format, len(pair)=%v was not 2 for tag: %s", len(pair), tag)
			return metric, hashedMName, nil, fmt.Errorf("invalid tag format, len(pair)=%v was not 2 for tag: %s", len(pair), tag)
		}
		key, multipleTagValues = strings.TrimSpace(pair[0]), strings.TrimSpace(pair[1])
		valuesList := strings.Split(multipleTagValues, "|")
		if len(valuesList) > 1 {
			logicalOperator = segutils.Or
		}
		for _, val = range valuesList {
			switch v := val.(type) {
			case string:
				if (strings.HasPrefix(v, `"`) && strings.HasSuffix(v, `"`) && len(v) > 1) ||
					(strings.HasPrefix(v, `'`) && strings.HasSuffix(v, `'`) && len(v) > 1) {
					v = v[1 : len(v)-1]
					val = v
				}
				hashedTagVal = xxhash.Sum64String(v)
			case int64:
				hashedTagVal = uint64(v)
			case float64:
				hashedTagVal = uint64(v)
			case uint64:
				hashedTagVal = v
			default:
				log.Errorf("parseMetricTag: invalid tag value type %T for value %v", val, val)
				return metric, hashedMName, nil, fmt.Errorf("parseMetricTag: invalid tag value type %v", val)
			}
			tags = append(tags, &structs.TagsFilter{
				TagKey:          key,
				RawTagValue:     val,
				HashTagValue:    hashedTagVal,
				LogicalOperator: logicalOperator,
			})
		}
	}
	return metric, hashedMName, tags, nil
}

func parseAggregatorDownsampler(m string) (segutils.AggregateFunctions, structs.Downsampler, error) {
	aggregaterDownsamplerList := strings.Split(m, ":")
	aggregator := segutils.Sum
	downsamplerStr := ""
	var ok bool
	var aggregatorMapping = map[string]segutils.AggregateFunctions{
		"count":       segutils.Count,
		"avg":         segutils.Avg,
		"min":         segutils.Min,
		"max":         segutils.Max,
		"sum":         segutils.Sum,
		"cardinality": segutils.Cardinality,
		"quantile":    segutils.Quantile,
	}
	agg := structs.Aggreation{AggregatorFunction: aggregatorMapping["avg"]}
	downsampler := structs.Downsampler{Interval: 1, Unit: "m", Aggregator: agg}

	if len(aggregaterDownsamplerList) >= 2 {
		aggregator, ok = aggregatorMapping[aggregaterDownsamplerList[0]]
		if !ok {
			log.Errorf("parseAggregatorDownsampler: unsupported aggregator function: %s", aggregaterDownsamplerList[0])
			return segutils.Sum, downsampler, fmt.Errorf("unsupported aggregator function: %s", aggregaterDownsamplerList[0])
		}
		if len(aggregaterDownsamplerList) > 2 {
			downsamplerStr = aggregaterDownsamplerList[1]
		}
	}

	if downsamplerStr != "" {
		downsampleComp := strings.Split(downsamplerStr, "-")
		if len(downsampleComp) == 1 {
			log.Errorf("parseAggregatorDownsampler: invalid downsampler format for %s", downsamplerStr)
			return aggregator, downsampler, fmt.Errorf("invalid downsampler format for %s", downsamplerStr)
		}
		downsampler.Aggregator = structs.Aggreation{AggregatorFunction: aggregatorMapping[downsampleComp[1]]}
		downsamplerIntervalUnit := downsampleComp[0]
		var intervalStr string
		var unit string
		for i, c := range downsamplerIntervalUnit {
			if c >= '0' && c <= '9' {
				intervalStr += string(c)
			} else if c == 'c' && i == len(downsamplerIntervalUnit)-1 {
				downsampler.CFlag = true
				break
			} else {
				unit += string(c)
			}
		}
		if len(intervalStr) == 0 || len(unit) == 0 {
			log.Errorf("parseAggregatorDownsampler: invalid downsampler(no interval or unit) format for %s", downsamplerStr)
			return aggregator, downsampler, fmt.Errorf("invalid downsampler format for %s", downsamplerStr)
		}
		interval, err := strconv.Atoi(intervalStr)
		if err != nil {
			log.Errorf("parseAggregatorDownsampler: invalid interval in downsampler %s. Error: %v", downsamplerStr, err)
			return aggregator, downsampler, fmt.Errorf("invalid interval in downsampler")
		}
		downsampler.Interval = interval
		downsampler.Unit = unit
	}
	return aggregator, downsampler, nil
}
