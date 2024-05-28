package promql

import (
	"encoding/csv"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/cespare/xxhash"
	"github.com/prometheus/prometheus/promql/parser"
	dtu "github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/results/mresults"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_parsePromQLQuery_simpleQueries(t *testing.T) {
	queries := []string{
		"(test_metric)",
		"(testmetric3{method='get',color='green'})",
		"avg by (model,car_type) (testmetric0{color='blue',car_type='Pickup truck'})",
		"rate(http_requests_total[5m])",
		"sum(http_requests_total)",
		"avg_over_time(node_cpu_seconds_total[5m])",
		"clamp((http_request_duration_seconds_bucket), 1.95, 3)",
		"clamp_max((http_request_duration_seconds_bucket), 4)",
		"(rate(http_requests_total[5m])) * 100",
		"(sum(http_requests_total) by (job)) + rate(node_cpu_seconds_total[5m]) / 2",
	}

	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	timeRange := dtu.MetricsTimeRange{
		StartEpochSec: startTime,
		EndEpochSec:   endTime,
	}
	intervalSeconds_uint32, _ := mresults.CalculateInterval(endTime - startTime)
	intervalSeconds := int(intervalSeconds_uint32)

	myId := uint64(0)

	// "(test_metric)",
	query := queries[0]
	mHashedMName := xxhash.Sum64String("test_metric")

	mQueryReqs, pqlQuerytype, queryArithmetic, err := parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "test_metric", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)

	// "(testmetric3{method='get',color='green'})",
	query = queries[1]
	mHashedMName = xxhash.Sum64String("testmetric3")
	tagkeys := []string{"color", "method"}

	mQueryReqs, pqlQuerytype, queryArithmetic, err = parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "testmetric3", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.False(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	actualTagKeys := []string{}
	for _, tag := range mQueryReqs[0].MetricsQuery.TagsFilters {
		actualTagKeys = append(actualTagKeys, tag.TagKey)
		if tag.TagKey == "method" {
			assert.Equal(t, "get", tag.RawTagValue)
		} else if tag.TagKey == "color" {
			assert.Equal(t, "green", tag.RawTagValue)
		}
	}
	sort.Slice(actualTagKeys, func(i, j int) bool {
		return actualTagKeys[i] < actualTagKeys[j]
	})
	assert.Equal(t, tagkeys, actualTagKeys)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)

	// "avg by (model,car_type) (testmetric0{color='blue',car_type='Pickup truck'})"
	query = queries[2]
	mHashedMName = xxhash.Sum64String("testmetric0")
	tagkeys = []string{"car_type", "car_type", "color", "model"}

	mQueryReqs, pqlQuerytype, queryArithmetic, err = parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "testmetric0", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.False(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	actualTagKeys = []string{}

	for _, tag := range mQueryReqs[0].MetricsQuery.TagsFilters {
		actualTagKeys = append(actualTagKeys, tag.TagKey)
		if tag.TagKey == "color" {
			assert.Equal(t, "blue", tag.RawTagValue)
		} else if tag.TagKey == "car_type" {
			assert.True(t, tag.RawTagValue == "Pickup truck" || tag.RawTagValue == "*")
		}
	}
	sort.Slice(actualTagKeys, func(i, j int) bool {
		return actualTagKeys[i] < actualTagKeys[j]
	})
	assert.Equal(t, tagkeys, actualTagKeys)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.Nil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)

	// "rate(http_requests_total[5m])",
	query = queries[3]
	mHashedMName = xxhash.Sum64String("http_requests_total")

	mQueryReqs, pqlQuerytype, queryArithmetic, err = parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "http_requests_total", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, utils.Rate, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)

	// "sum(http_requests_total)",
	query = queries[4]
	mHashedMName = xxhash.Sum64String("http_requests_total")

	mQueryReqs, pqlQuerytype, queryArithmetic, err = parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "http_requests_total", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, utils.Sum, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.Nil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)

	// "avg_over_time(node_cpu_seconds_total[5m])",
	query = queries[5]
	mHashedMName = xxhash.Sum64String("node_cpu_seconds_total")

	mQueryReqs, pqlQuerytype, queryArithmetic, err = parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "node_cpu_seconds_total", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, utils.Avg_Over_Time, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)

	// "clamp((http_request_duration_seconds_bucket), 1.95, 3)"
	query = queries[6]
	mHashedMName = xxhash.Sum64String("http_request_duration_seconds_bucket")
	functionValues := []string{"1.95", "3"}

	mQueryReqs, pqlQuerytype, queryArithmetic, err = parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "http_request_duration_seconds_bucket", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, utils.Clamp, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.MathFunction)
	assert.Equal(t, functionValues, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.ValueList)

	// "clamp_max((http_request_duration_seconds_bucket), 4)"
	query = queries[7]
	mHashedMName = xxhash.Sum64String("http_request_duration_seconds_bucket")
	functionValues = []string{"4"}

	mQueryReqs, pqlQuerytype, queryArithmetic, err = parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "http_request_duration_seconds_bucket", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, utils.Clamp_Max, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.MathFunction)
	assert.Equal(t, functionValues, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.ValueList)

	// "(rate(http_requests_total[5m])) * 100",
	query = queries[8]
	mHashedMName = xxhash.Sum64String("http_requests_total")

	mQueryReqs, pqlQuerytype, queryArithmetic, err = parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 1, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "http_requests_total", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, utils.Rate, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)
	assert.Equal(t, segutils.LetMultiply, queryArithmetic[0].Operation)
	assert.Equal(t, float64(100), queryArithmetic[0].Constant)

	// "(sum(http_requests_total) by (job)) + rate(node_cpu_seconds_total[5m]) / 2"
	query = queries[9]
	mHashedMName1 := xxhash.Sum64String("http_requests_total")
	mHashedMName2 := xxhash.Sum64String("node_cpu_seconds_total")
	// tagkeys = []string{"job"}

	mQueryReqs, pqlQuerytype, queryArithmetic, err = parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, timeRange, mQueryReqs[1].TimeRange)
	assert.Equal(t, 2, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "http_requests_total", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, "node_cpu_seconds_total", mQueryReqs[1].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName1, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.Equal(t, mHashedMName2, mQueryReqs[1].MetricsQuery.HashedMName)
	assert.False(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[1].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.False(t, mQueryReqs[1].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, intervalSeconds, mQueryReqs[1].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, utils.Sum, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[1].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[1].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[1].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, utils.Rate, mQueryReqs[1].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[1].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)
	assert.Equal(t, segutils.LetDivide, queryArithmetic[0].Operation)
	assert.Equal(t, float64(2), queryArithmetic[0].Constant)
	assert.Equal(t, segutils.LetAdd, queryArithmetic[1].Operation)
}

func Test_parsePromQLQuery_SimpleQueries_v2(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	timeRange := dtu.MetricsTimeRange{
		StartEpochSec: startTime,
		EndEpochSec:   endTime,
	}
	intervalSeconds_uint32, _ := mresults.CalculateInterval(endTime - startTime)
	intervalSeconds := int(intervalSeconds_uint32)

	myId := uint64(0)

	query := "round(testmetric0, 1/2)"
	mHashedMName := xxhash.Sum64String("testmetric0")
	functionValues := []string{"1 / 2"}

	mQueryReqs, pqlQuerytype, queryArithmetic, err := parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 1, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "testmetric0", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, utils.Round, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.MathFunction)
	assert.Equal(t, functionValues, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.ValueList)

	query = "http_requests_total{job='apiserver', handler='/api/comments'}[5m]"
	mHashedMName = xxhash.Sum64String("http_requests_total")
	tagkeys := []string{"handler", "job"}

	timeRange.StartEpochSec = timeRange.EndEpochSec - uint32(5*60)

	mQueryReqs, pqlQuerytype, queryArithmetic, err = parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeMatrix, pqlQuerytype)
	assert.Equal(t, "http_requests_total", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.False(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	actualTagKeys := []string{}
	for _, tag := range mQueryReqs[0].MetricsQuery.TagsFilters {
		actualTagKeys = append(actualTagKeys, tag.TagKey)
		if tag.TagKey == "handler" {
			assert.Equal(t, "/api/comments", tag.RawTagValue)
		} else if tag.TagKey == "job" {
			assert.Equal(t, "apiserver", tag.RawTagValue)
		}
	}
	sort.Slice(actualTagKeys, func(i, j int) bool {
		return actualTagKeys[i] < actualTagKeys[j]
	})
	assert.Equal(t, tagkeys, actualTagKeys)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.Nil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
}

func Test_parsePromQLQuery_NestedQueries_v1(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	timeRange := dtu.MetricsTimeRange{
		StartEpochSec: startTime,
		EndEpochSec:   endTime,
	}
	intervalSeconds_uint32, _ := mresults.CalculateInterval(endTime - startTime)
	intervalSeconds := int(intervalSeconds_uint32)

	myId := uint64(0)

	// Double nested range query
	query := "sum(rate(http_requests_total[5m]))"
	mHashedMName := xxhash.Sum64String("http_requests_total")

	mQueryReqs, pqlQuerytype, queryArithmetic, err := parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "http_requests_total", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.AggBlockType)
	assert.Equal(t, utils.Rate, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggBlockType)
	assert.Equal(t, utils.Sum, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggregatorBlock.AggregatorFunction)

	// Double nested range query with group by
	query = "sum(rate(http_requests_total[5m])) by (job)"
	mHashedMName = xxhash.Sum64String("http_requests_total")
	tagkeys := []string{"job"}

	mQueryReqs, pqlQuerytype, queryArithmetic, err = parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "http_requests_total", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.False(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	actualTagKeys := []string{}
	for _, tag := range mQueryReqs[0].MetricsQuery.TagsFilters {
		actualTagKeys = append(actualTagKeys, tag.TagKey)
		if tag.TagKey == "job" {
			assert.Equal(t, "*", tag.RawTagValue)
		}
	}
	assert.Equal(t, tagkeys, actualTagKeys)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.AggBlockType)
	assert.Equal(t, utils.Rate, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggBlockType)
	assert.Equal(t, utils.Sum, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggregatorBlock.AggregatorFunction)

	// Double nested range query with filters and group by
	query = "max(max_over_time({__name__='cpu_usage_user', hostname='host_35'}[1h])) by (hostname,job)"
	mHashedMName = xxhash.Sum64String("cpu_usage_user")
	tagkeys = []string{"hostname", "hostname", "job"}

	mQueryReqs, pqlQuerytype, queryArithmetic, err = parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "cpu_usage_user", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.False(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	actualTagKeys = []string{}
	for _, tag := range mQueryReqs[0].MetricsQuery.TagsFilters {
		actualTagKeys = append(actualTagKeys, tag.TagKey)
		if tag.TagKey == "hostname" {
			assert.True(t, tag.RawTagValue == "host_35" || tag.RawTagValue == "*")
		}
	}
	assert.Equal(t, tagkeys, actualTagKeys)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.AggBlockType)
	assert.Equal(t, utils.Max_Over_Time, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(3600), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggBlockType)
	assert.Equal(t, utils.Max, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggregatorBlock.AggregatorFunction)

}

func Test_parsePromQLQuery_NestedQueries_v2(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	timeRange := dtu.MetricsTimeRange{
		StartEpochSec: startTime,
		EndEpochSec:   endTime,
	}
	intervalSeconds_uint32, _ := mresults.CalculateInterval(endTime - startTime)
	intervalSeconds := int(intervalSeconds_uint32)

	myId := uint64(0)

	// Triple nested range query with group by and params for function
	query := "clamp_max(sum(rate(http_request_duration_seconds_bucket[5m])) by (le), 100)"
	mHashedMName := xxhash.Sum64String("http_request_duration_seconds_bucket")
	tagkeys := []string{"le"}

	mQueryReqs, pqlQuerytype, queryArithmetic, err := parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "http_request_duration_seconds_bucket", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.False(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	actualTagKeys := []string{}
	for _, tag := range mQueryReqs[0].MetricsQuery.TagsFilters {
		actualTagKeys = append(actualTagKeys, tag.TagKey)
		if tag.TagKey == "le" {
			assert.Equal(t, "*", tag.RawTagValue)
		}
	}
	assert.Equal(t, tagkeys, actualTagKeys)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.AggBlockType)
	assert.Equal(t, utils.Rate, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggBlockType)
	assert.Equal(t, utils.Sum, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next.AggBlockType)
	assert.Equal(t, utils.Clamp_Max, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next.FunctionBlock.MathFunction)
	assert.Equal(t, []string{"100"}, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next.FunctionBlock.ValueList)

	// Triple nested range query with range
	query = "max_over_time(sum(rate(http_requests_total[1m]))[10m:2m])"
	mHashedMName = xxhash.Sum64String("http_requests_total")

	mQueryReqs, pqlQuerytype, queryArithmetic, err = parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "http_requests_total", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.AggBlockType)
	assert.Equal(t, utils.Rate, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(60), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggBlockType)
	assert.Equal(t, utils.Sum, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next.AggBlockType)
	assert.Equal(t, utils.Max_Over_Time, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(600), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next.FunctionBlock.TimeWindow)
	assert.Equal(t, float64(120), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next.FunctionBlock.Step)
}

func Test_parsePromQLQuery_NestedQueries_v3(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	timeRange := dtu.MetricsTimeRange{
		StartEpochSec: startTime,
		EndEpochSec:   endTime,
	}
	intervalSeconds_uint32, _ := mresults.CalculateInterval(endTime - startTime)
	intervalSeconds := int(intervalSeconds_uint32)

	myId := uint64(0)

	// Nested Query with Binary Expression
	query := "(sum(rate(http_requests_total[5m])) by (job)) * avg(irate(node_cpu_seconds_total[5m]))"
	mHashedMName1 := xxhash.Sum64String("http_requests_total")
	mHashedMName2 := xxhash.Sum64String("node_cpu_seconds_total")
	tagkeys := []string{"job"}

	mQueryReqs, pqlQuerytype, queryArithmetic, err := parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, timeRange, mQueryReqs[1].TimeRange)
	assert.Equal(t, 1, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "http_requests_total", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, "node_cpu_seconds_total", mQueryReqs[1].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName1, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.Equal(t, mHashedMName2, mQueryReqs[1].MetricsQuery.HashedMName)
	assert.False(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[1].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.False(t, mQueryReqs[1].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, intervalSeconds, mQueryReqs[1].MetricsQuery.Downsampler.Interval)

	actualTagKeys := []string{}
	for _, tag := range mQueryReqs[0].MetricsQuery.TagsFilters {
		actualTagKeys = append(actualTagKeys, tag.TagKey)
		if tag.TagKey == "job" {
			assert.Equal(t, "*", tag.RawTagValue)
		}
	}
	assert.Equal(t, tagkeys, actualTagKeys)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.AggBlockType)
	assert.Equal(t, utils.Rate, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggBlockType)
	assert.Equal(t, utils.Sum, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggregatorBlock.AggregatorFunction)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[1].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[1].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[1].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[1].MetricsQuery.MQueryAggs.Next.AggBlockType)
	assert.Equal(t, utils.IRate, mQueryReqs[1].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[1].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[1].MetricsQuery.MQueryAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[1].MetricsQuery.MQueryAggs.Next.Next.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[1].MetricsQuery.MQueryAggs.Next.Next.AggregatorBlock.AggregatorFunction)

	assert.Equal(t, segutils.LetMultiply, queryArithmetic[0].Operation)
	assert.Equal(t, float64(0), queryArithmetic[0].Constant)
	assert.Equal(t, mHashedMName1, queryArithmetic[0].LHS)
	assert.Equal(t, mHashedMName2, queryArithmetic[0].RHS)
}

func Test_parsePromQLQuery_NestedQueries_v4(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	timeRange := dtu.MetricsTimeRange{
		StartEpochSec: startTime,
		EndEpochSec:   endTime,
	}
	intervalSeconds_uint32, _ := mresults.CalculateInterval(endTime - startTime)
	intervalSeconds := int(intervalSeconds_uint32)

	myId := uint64(0)

	// Nested Query with Aggregations + Binary Expr. Total 3 nested queries
	query := "sum(rate(http_requests_total[5m])) by (job) / count_over_time(http_requests_total[5m]) + sum(rate(node_cpu_seconds_total[5m]))"
	mHashedMName1 := xxhash.Sum64String("http_requests_total")
	mHashedName2 := xxhash.Sum64String("http_requests_total")
	mHashedMName3 := xxhash.Sum64String("node_cpu_seconds_total")
	tagkeys := []string{"job"}

	mQueryReqs, pqlQuerytype, queryArithmetic, err := parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, timeRange, mQueryReqs[1].TimeRange)
	assert.Equal(t, timeRange, mQueryReqs[2].TimeRange)
	assert.Equal(t, 2, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "http_requests_total", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, "http_requests_total", mQueryReqs[1].MetricsQuery.MetricName)
	assert.Equal(t, "node_cpu_seconds_total", mQueryReqs[2].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName1, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.Equal(t, mHashedName2, mQueryReqs[1].MetricsQuery.HashedMName)
	assert.Equal(t, mHashedMName3, mQueryReqs[2].MetricsQuery.HashedMName)
	assert.False(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[1].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[2].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.False(t, mQueryReqs[1].MetricsQuery.Groupby)
	assert.False(t, mQueryReqs[2].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, intervalSeconds, mQueryReqs[1].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, intervalSeconds, mQueryReqs[2].MetricsQuery.Downsampler.Interval)

	actualTagKeys := []string{}
	for _, tag := range mQueryReqs[0].MetricsQuery.TagsFilters {
		actualTagKeys = append(actualTagKeys, tag.TagKey)
		if tag.TagKey == "job" {
			assert.Equal(t, "*", tag.RawTagValue)
		}
	}
	assert.Equal(t, tagkeys, actualTagKeys)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.AggBlockType)
	assert.Equal(t, utils.Rate, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggBlockType)
	assert.Equal(t, utils.Sum, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggregatorBlock.AggregatorFunction)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[1].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[1].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[1].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[1].MetricsQuery.MQueryAggs.Next.AggBlockType)
	assert.Equal(t, utils.Count_Over_Time, mQueryReqs[1].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[1].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[2].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[2].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[2].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[2].MetricsQuery.MQueryAggs.Next.AggBlockType)
	assert.Equal(t, utils.Rate, mQueryReqs[2].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[2].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[2].MetricsQuery.MQueryAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[2].MetricsQuery.MQueryAggs.Next.Next.AggBlockType)
	assert.Equal(t, utils.Sum, mQueryReqs[2].MetricsQuery.MQueryAggs.Next.Next.AggregatorBlock.AggregatorFunction)

	assert.Equal(t, segutils.LetDivide, queryArithmetic[0].Operation)
	assert.Equal(t, float64(0), queryArithmetic[0].Constant)
	assert.Equal(t, mHashedMName1, queryArithmetic[0].LHS)
	assert.Equal(t, mHashedName2, queryArithmetic[0].RHS)

	assert.Equal(t, segutils.LetAdd, queryArithmetic[1].Operation)
	assert.Equal(t, float64(0), queryArithmetic[1].Constant)
	assert.Equal(t, mHashedName2, queryArithmetic[1].LHS)
	assert.Equal(t, mHashedMName3, queryArithmetic[1].RHS)
}

func Test_parsePromQLQuery_NestedQueries_v5(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	timeRange := dtu.MetricsTimeRange{
		StartEpochSec: startTime,
		EndEpochSec:   endTime,
	}
	intervalSeconds_uint32, _ := mresults.CalculateInterval(endTime - startTime)
	intervalSeconds := int(intervalSeconds_uint32)

	myId := uint64(0)

	query := "avg_over_time(sum(rate(http_requests_total[5m]))[10m:]) + sum_over_time(rate(http_requests_total[5m])[10m:1m])"
	mHashedMName1 := xxhash.Sum64String("http_requests_total")
	mHashedMName2 := xxhash.Sum64String("http_requests_total")

	mQueryReqs, pqlQuerytype, queryArithmetic, err := parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, timeRange, mQueryReqs[1].TimeRange)
	assert.Equal(t, 1, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "http_requests_total", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, "http_requests_total", mQueryReqs[1].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName1, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.Equal(t, mHashedMName2, mQueryReqs[1].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[1].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.False(t, mQueryReqs[1].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, intervalSeconds, mQueryReqs[1].MetricsQuery.Downsampler.Interval)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.AggBlockType)
	assert.Equal(t, utils.Rate, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggBlockType)
	assert.Equal(t, utils.Sum, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next.AggBlockType)
	assert.Equal(t, utils.Avg_Over_Time, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(600), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next.FunctionBlock.TimeWindow)
	assert.Equal(t, float64(0), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next.FunctionBlock.Step)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[1].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[1].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[1].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[1].MetricsQuery.MQueryAggs.Next.AggBlockType)
	assert.Equal(t, utils.Rate, mQueryReqs[1].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[1].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[1].MetricsQuery.MQueryAggs.Next.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[1].MetricsQuery.MQueryAggs.Next.Next.AggBlockType)
	assert.Equal(t, utils.Sum_Over_Time, mQueryReqs[1].MetricsQuery.MQueryAggs.Next.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(600), mQueryReqs[1].MetricsQuery.MQueryAggs.Next.Next.FunctionBlock.TimeWindow)
	assert.Equal(t, float64(60), mQueryReqs[1].MetricsQuery.MQueryAggs.Next.Next.FunctionBlock.Step)

	assert.Equal(t, segutils.LetAdd, queryArithmetic[0].Operation)
	assert.Equal(t, float64(0), queryArithmetic[0].Constant)
	assert.Equal(t, mHashedMName1, queryArithmetic[0].LHS)
	assert.Equal(t, mHashedMName2, queryArithmetic[0].RHS)
}

func Test_parsePromQLQuery_NestedQueries_v6(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	timeRange := dtu.MetricsTimeRange{
		StartEpochSec: startTime,
		EndEpochSec:   endTime,
	}
	intervalSeconds_uint32, _ := mresults.CalculateInterval(endTime - startTime)
	intervalSeconds := int(intervalSeconds_uint32)

	myId := uint64(0)

	query := "abs(rate(testmetric3[5m]))"
	mHashedMName := xxhash.Sum64String("testmetric3")

	mQueryReqs, pqlQuerytype, queryArithmetic, err := parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "testmetric3", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.AggBlockType)
	assert.Equal(t, utils.Rate, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggBlockType)
	assert.Equal(t, utils.Abs, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.FunctionBlock.MathFunction)
}

func Test_parsePromQLQuery_NestedQueries_v7(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	timeRange := dtu.MetricsTimeRange{
		StartEpochSec: startTime,
		EndEpochSec:   endTime,
	}
	intervalSeconds_uint32, _ := mresults.CalculateInterval(endTime - startTime)
	intervalSeconds := int(intervalSeconds_uint32)

	myId := uint64(0)

	query := "max_over_time(deriv(rate(distance_covered_total[5s])[30s:5s])[10m:])"
	mHashedMName := xxhash.Sum64String("distance_covered_total")

	mQueryReqs, pqlQuerytype, queryArithmetic, err := parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "distance_covered_total", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.AggBlockType)
	assert.Equal(t, utils.Rate, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(5), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.AggBlockType)
	assert.Equal(t, utils.Derivative, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(30), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.FunctionBlock.TimeWindow)
	assert.Equal(t, float64(5), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.FunctionBlock.Step)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next.AggBlockType)
	assert.Equal(t, utils.Max_Over_Time, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(600), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next.FunctionBlock.TimeWindow)
	assert.Equal(t, float64(0), mQueryReqs[0].MetricsQuery.MQueryAggs.Next.Next.Next.FunctionBlock.Step)
}

func Test_parsePromQLQuery_NestedQueries_v8(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	timeRange := dtu.MetricsTimeRange{
		StartEpochSec: startTime,
		EndEpochSec:   endTime,
	}
	intervalSeconds_uint32, _ := mresults.CalculateInterval(endTime - startTime)
	intervalSeconds := int(intervalSeconds_uint32)

	myId := uint64(0)

	query := "sum by (app, proc) ( instance_memory_limit_bytes - instance_memory_usage_bytes )"
	mHashedMName1 := xxhash.Sum64String("instance_memory_limit_bytes")
	mHashedMName2 := xxhash.Sum64String("instance_memory_usage_bytes")
	tagKeys := []string{"app", "proc"}

	mQueryReqs, pqlQuerytype, queryArithmetic, err := parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, timeRange, mQueryReqs[1].TimeRange)
	assert.Equal(t, 1, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "instance_memory_limit_bytes", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, "instance_memory_usage_bytes", mQueryReqs[1].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName1, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.Equal(t, mHashedMName2, mQueryReqs[1].MetricsQuery.HashedMName)
	assert.False(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[1].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.True(t, mQueryReqs[1].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, intervalSeconds, mQueryReqs[1].MetricsQuery.Downsampler.Interval)
	actualTagKeys := []string{}
	for _, tag := range mQueryReqs[0].MetricsQuery.TagsFilters {
		actualTagKeys = append(actualTagKeys, tag.TagKey)
		if tag.TagKey == "app" {
			assert.Equal(t, "*", tag.RawTagValue)
		}
		if tag.TagKey == "proc" {
			assert.Equal(t, "*", tag.RawTagValue)
		}
	}
	sort.Slice(actualTagKeys, func(i, j int) bool {
		return actualTagKeys[i] < actualTagKeys[j]
	})
	assert.Equal(t, tagKeys, actualTagKeys)

	actualTagKeys = []string{}
	for _, tag := range mQueryReqs[1].MetricsQuery.TagsFilters {
		actualTagKeys = append(actualTagKeys, tag.TagKey)
		if tag.TagKey == "app" {
			assert.Equal(t, "*", tag.RawTagValue)
		}
		if tag.TagKey == "proc" {
			assert.Equal(t, "*", tag.RawTagValue)

		}
	}
	sort.Slice(actualTagKeys, func(i, j int) bool {
		return actualTagKeys[i] < actualTagKeys[j]
	})
	assert.Equal(t, tagKeys, actualTagKeys)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[0].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.AggBlockType)
	assert.Equal(t, utils.Sum, mQueryReqs[0].MetricsQuery.MQueryAggs.Next.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[1].MetricsQuery.MQueryAggs.AggBlockType)
	assert.Equal(t, utils.Avg, mQueryReqs[1].MetricsQuery.MQueryAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[1].MetricsQuery.MQueryAggs.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[1].MetricsQuery.MQueryAggs.Next.AggBlockType)
	assert.Equal(t, utils.Sum, mQueryReqs[1].MetricsQuery.MQueryAggs.Next.AggregatorBlock.AggregatorFunction)

	assert.Equal(t, segutils.LetSubtract, queryArithmetic[0].Operation)
}

func Test_parsePromQLQuery_Parse_Metrics_Test_CSV(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := uint64(0)

	// Read the file
	file, err := os.Open("../../../../cicd/metrics_test.csv")
	if err != nil {
		assert.Fail(t, "Test_parsePromQLQuery_Parse_Metrics_Test_CSV: Error reading the file: %v", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	rawCSVdata, err := reader.ReadAll()
	if err != nil {
		assert.Fail(t, "Test_parsePromQLQuery_Parse_Metrics_Test_CSV: Error reading the file: %v", err)
		return
	}

	// Get the data
	for _, record := range rawCSVdata[1:] {
		query := record[0]

		mQueryReqs, _, _, err := parsePromQLQuery(query, startTime, endTime, myId)
		assert.Nil(t, err)
		assert.True(t, len(mQueryReqs) > 0, "No Metric Search Reqs found for query: %s", query)
	}
}

func Test_parsePromQLQuery_Parse_Promql_Test_CSV(t *testing.T) {

	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := uint64(0)

	// Read the file
	file, err := os.Open("../../../../cicd/promql_test.csv")
	if err != nil {
		assert.Fail(t, "Test_parsePromQLQuery_Parse_Promql_Test_CSV: Error reading the file: %v", err)
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	rawCSVdata, err := reader.ReadAll()
	if err != nil {
		assert.Fail(t, "Test_parsePromQLQuery_Parse_Promql_Test_CSV: Error reading the file: %v", err)
		return
	}

	// Get the data
	for _, record := range rawCSVdata[1:] {
		query := record[0]

		mQueryReqs, _, _, err := parsePromQLQuery(query, startTime, endTime, myId)
		assert.Nil(t, err)
		assert.True(t, len(mQueryReqs) > 0, "No Metric Search Reqs found for query: %s", query)
	}
}
