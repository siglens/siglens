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
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	myId := int64(0)

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
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)

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
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
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
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)

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
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.Nil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)

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
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, segutils.Rate, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)

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
	assert.Equal(t, segutils.Sum, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.Nil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)

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
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, segutils.Avg_Over_Time, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)

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
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, segutils.Clamp, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.MathFunction)
	assert.Equal(t, functionValues, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.ValueList)

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
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, segutils.Clamp_Max, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.MathFunction)
	assert.Equal(t, functionValues, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.ValueList)

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
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, segutils.Rate, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
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
	assert.Equal(t, segutils.Sum, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[1].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[1].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, segutils.Rate, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.Equal(t, segutils.LetAdd, queryArithmetic[0].Operation)
	assert.NotNil(t, queryArithmetic[0].RHSExpr)
	assert.Equal(t, float64(2), queryArithmetic[0].RHSExpr.Constant)
	assert.Equal(t, segutils.LetDivide, queryArithmetic[0].RHSExpr.Operation)
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

	myId := int64(0)

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
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.Nil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, segutils.Round, queryArithmetic[0].MQueryAggsChain.FunctionBlock.MathFunction)
	assert.Equal(t, functionValues, queryArithmetic[0].MQueryAggsChain.FunctionBlock.ValueList)

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
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
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

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.Nil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
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

	myId := int64(0)

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
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.Rate, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Sum, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, 0, len(mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.GroupByFields))

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
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
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
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.Rate, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Sum, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, 1, len(mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.GroupByFields))

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
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
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

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.Max_Over_Time, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(3600), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Max, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, 2, len(mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.GroupByFields))

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

	myId := int64(0)

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
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
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

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.Rate, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Sum, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, 1, len(mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.GroupByFields))
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Clamp_Max, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.FunctionBlock.MathFunction)
	assert.Equal(t, []string{"100"}, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.FunctionBlock.ValueList)

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
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.Rate, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(60), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Sum, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Max_Over_Time, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(600), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.FunctionBlock.TimeWindow)
	assert.Equal(t, float64(120), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.FunctionBlock.Step)
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

	myId := int64(0)

	// Nested Query with Binary Expression
	query := "(sum(rate(http_requests_total[5m])) by (job)) * avg(irate(node_cpu_seconds_total[5m]))"
	mHashedMName1 := xxhash.Sum64String("http_requests_total")
	mHashedMName2 := xxhash.Sum64String("node_cpu_seconds_total")
	mQueryHash1 := xxhash.Sum64String("(sum by (job) (rate(http_requests_total[5m])))")
	mQueryHash2 := xxhash.Sum64String("avg(irate(node_cpu_seconds_total[5m]))")
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
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
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

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.Rate, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Sum, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.AggregatorFunction)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[1].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[1].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.IRate, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.AggregatorFunction)

	assert.Equal(t, segutils.LetMultiply, queryArithmetic[0].Operation)
	assert.Equal(t, float64(0), queryArithmetic[0].Constant)
	assert.Equal(t, mQueryHash1, queryArithmetic[0].LHS)
	assert.Equal(t, mQueryHash2, queryArithmetic[0].RHS)
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

	myId := int64(0)

	// Nested Query with Aggregations + Binary Expr. Total 3 nested queries
	query := "sum(rate(http_requests_total[5m])) by (job) / count_over_time(http_requests_total[5m]) + sum(rate(node_cpu_seconds_total[5m]))"
	mHashedMName1 := xxhash.Sum64String("http_requests_total")
	mHashedName2 := xxhash.Sum64String("http_requests_total")
	mHashedMName3 := xxhash.Sum64String("node_cpu_seconds_total")
	queryHash1 := xxhash.Sum64String("sum by (job) (rate(http_requests_total[5m]))")
	queryHash2 := xxhash.Sum64String("count_over_time(http_requests_total[5m])")
	queryHash3 := xxhash.Sum64String("sum(rate(node_cpu_seconds_total[5m]))")
	tagkeys := []string{"job"}

	mQueryReqs, pqlQuerytype, queryArithmetic, err := parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, timeRange, mQueryReqs[1].TimeRange)
	assert.Equal(t, timeRange, mQueryReqs[2].TimeRange)
	assert.Equal(t, 1, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "http_requests_total", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, "http_requests_total", mQueryReqs[1].MetricsQuery.MetricName)
	assert.Equal(t, "node_cpu_seconds_total", mQueryReqs[2].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName1, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.Equal(t, mHashedName2, mQueryReqs[1].MetricsQuery.HashedMName)
	assert.Equal(t, mHashedMName3, mQueryReqs[2].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
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

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.Rate, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Sum, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.AggregatorFunction)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[1].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[1].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.Count_Over_Time, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[2].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[2].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[2].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[2].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.Rate, mQueryReqs[2].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[2].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[2].MetricsQuery.SubsequentAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[2].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Sum, mQueryReqs[2].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.AggregatorFunction)

	assert.Equal(t, segutils.LetAdd, queryArithmetic[0].Operation)
	assert.Equal(t, float64(0), queryArithmetic[0].Constant)
	assert.Equal(t, queryHash1, queryArithmetic[0].LHS)
	assert.Equal(t, queryHash3, queryArithmetic[0].RHS)

	assert.NotNil(t, queryArithmetic[0].LHSExpr)

	assert.Equal(t, segutils.LetDivide, queryArithmetic[0].LHSExpr.Operation)
	assert.Equal(t, float64(0), queryArithmetic[0].LHSExpr.Constant)
	assert.Equal(t, queryHash1, queryArithmetic[0].LHSExpr.LHS)
	assert.Equal(t, queryHash2, queryArithmetic[0].LHSExpr.RHS)
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

	myId := int64(0)

	query := "avg_over_time(sum(rate(http_requests_total[5m]))[10m:]) + sum_over_time(rate(http_requests_total[5m])[10m:1m])"
	mHashedMName1 := xxhash.Sum64String("http_requests_total")
	mHashedMName2 := xxhash.Sum64String("http_requests_total")
	queryHash1 := xxhash.Sum64String("avg_over_time(sum(rate(http_requests_total[5m]))[10m:])")
	queryHash2 := xxhash.Sum64String("sum_over_time(rate(http_requests_total[5m])[10m:1m])")

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

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.Rate, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Sum, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Avg_Over_Time, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(600), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.FunctionBlock.TimeWindow)
	assert.Equal(t, getStepValueFromTimeRange(&timeRange), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.FunctionBlock.Step)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[1].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[1].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.Rate, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Sum_Over_Time, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(600), mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.Next.FunctionBlock.TimeWindow)
	assert.Equal(t, float64(60), mQueryReqs[1].MetricsQuery.SubsequentAggs.Next.Next.FunctionBlock.Step)

	assert.Equal(t, segutils.LetAdd, queryArithmetic[0].Operation)
	assert.Equal(t, float64(0), queryArithmetic[0].Constant)
	assert.Equal(t, queryHash1, queryArithmetic[0].LHS)
	assert.Equal(t, queryHash2, queryArithmetic[0].RHS)
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

	myId := int64(0)

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
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.Rate, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Abs, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.FunctionBlock.MathFunction)
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

	myId := int64(0)

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
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.Rate, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(5), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Derivative, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(30), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.FunctionBlock.TimeWindow)
	assert.Equal(t, float64(5), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.FunctionBlock.Step)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Max_Over_Time, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(600), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.FunctionBlock.TimeWindow)
	assert.Equal(t, getStepValueFromTimeRange(&timeRange), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.FunctionBlock.Step)
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

	myId := int64(0)

	query := "sum by (app, proc) ( instance_memory_limit_bytes - instance_memory_usage_bytes )"
	mHashedMName1 := xxhash.Sum64String("instance_memory_limit_bytes")
	mHashedMName2 := xxhash.Sum64String("instance_memory_usage_bytes")
	groupByKeys := []string{"app", "proc"}

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
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[1].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.False(t, mQueryReqs[1].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, intervalSeconds, mQueryReqs[1].MetricsQuery.Downsampler.Interval)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.Nil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[1].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[1].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.Nil(t, mQueryReqs[1].MetricsQuery.SubsequentAggs.Next)

	assert.Equal(t, segutils.LetSubtract, queryArithmetic[0].Operation)
	assert.NotNil(t, queryArithmetic[0].MQueryAggsChain)
	assert.Equal(t, structs.AggregatorBlock, queryArithmetic[0].MQueryAggsChain.AggBlockType)
	assert.Equal(t, segutils.Sum, queryArithmetic[0].MQueryAggsChain.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, groupByKeys, queryArithmetic[0].MQueryAggsChain.AggregatorBlock.GroupByFields)
}

func Test_parsePromQLQuery_NestedQueries_NestedGroupBy_v1(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	timeRange := dtu.MetricsTimeRange{
		StartEpochSec: startTime,
		EndEpochSec:   endTime,
	}
	intervalSeconds_uint32, _ := mresults.CalculateInterval(endTime - startTime)
	intervalSeconds := int(intervalSeconds_uint32)

	myId := int64(0)

	query := "max(sum(rate(http_requests_total[5m])) by (job, handler)) by (proc)"
	mHashedMName := xxhash.Sum64String("http_requests_total")
	tagKeys := []string{"handler", "job", "proc"}

	mQueryReqs, pqlQuerytype, queryArithmetic, err := parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, timeRange, mQueryReqs[0].TimeRange)
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "http_requests_total", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	actualTagKeys := []string{}
	for _, tag := range mQueryReqs[0].MetricsQuery.TagsFilters {
		actualTagKeys = append(actualTagKeys, tag.TagKey)
		if tag.TagKey == "job" {
			assert.Equal(t, "*", tag.RawTagValue)
			assert.Equal(t, true, tag.NotInitialGroup)
		}
		if tag.TagKey == "handler" {
			assert.Equal(t, "*", tag.RawTagValue)
			assert.Equal(t, true, tag.NotInitialGroup)
		}
		if tag.TagKey == "proc" {
			assert.Equal(t, "*", tag.RawTagValue)
			assert.Equal(t, true, tag.NotInitialGroup)
		}
	}
	sort.Slice(actualTagKeys, func(i, j int) bool {
		return actualTagKeys[i] < actualTagKeys[j]
	})
	assert.Equal(t, tagKeys, actualTagKeys)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.Rate, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Sum, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, 2, len(mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.GroupByFields))
	assert.Equal(t, "job", mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.GroupByFields[0])
	assert.Equal(t, "handler", mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.GroupByFields[1])
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Max, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, 1, len(mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.AggregatorBlock.GroupByFields))
	assert.Equal(t, "proc", mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.Next.AggregatorBlock.GroupByFields[0])
}

func Test_parsePromQLQuery_NestedQueries_NestedGroupBy_v2(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	timeRange := dtu.MetricsTimeRange{
		StartEpochSec: startTime,
		EndEpochSec:   endTime,
	}
	intervalSeconds_uint32, _ := mresults.CalculateInterval(endTime - startTime)
	intervalSeconds := int(intervalSeconds_uint32)

	myId := int64(0)

	query := "sum by (app, proc) ( sum by (job) (instance_memory_limit_bytes - instance_memory_usage_bytes) )"
	mHashedMName1 := xxhash.Sum64String("instance_memory_limit_bytes")
	mHashedMName2 := xxhash.Sum64String("instance_memory_usage_bytes")
	groupByKeysL2 := []string{"app", "proc"}

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
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[1].MetricsQuery.SelectAllSeries)
	assert.False(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.False(t, mQueryReqs[1].MetricsQuery.Groupby)
	assert.Equal(t, intervalSeconds, mQueryReqs[0].MetricsQuery.Downsampler.Interval)
	assert.Equal(t, intervalSeconds, mQueryReqs[1].MetricsQuery.Downsampler.Interval)

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.Nil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)

	assert.Equal(t, segutils.LetSubtract, queryArithmetic[0].Operation)
	assert.NotNil(t, queryArithmetic[0].MQueryAggsChain)

	assert.Equal(t, structs.AggregatorBlock, queryArithmetic[0].MQueryAggsChain.AggBlockType)
	assert.Equal(t, segutils.Sum, queryArithmetic[0].MQueryAggsChain.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, 1, len(queryArithmetic[0].MQueryAggsChain.AggregatorBlock.GroupByFields))
	assert.Equal(t, "job", queryArithmetic[0].MQueryAggsChain.AggregatorBlock.GroupByFields[0])
	assert.NotNil(t, queryArithmetic[0].MQueryAggsChain.Next)
	assert.Equal(t, structs.AggregatorBlock, queryArithmetic[0].MQueryAggsChain.Next.AggBlockType)
	assert.Equal(t, segutils.Sum, queryArithmetic[0].MQueryAggsChain.Next.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, 2, len(queryArithmetic[0].MQueryAggsChain.Next.AggregatorBlock.GroupByFields))
	assert.Equal(t, groupByKeysL2, queryArithmetic[0].MQueryAggsChain.Next.AggregatorBlock.GroupByFields)
}

func Test_parsePromQLQuery_Scalar_Op_v1(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := int64(0)

	query := "1 + 2 + 3 * 4"

	mQueryReqs, pqlQuerytype, queryArithmetic, err := parsePromQLQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(mQueryReqs))
	assert.Equal(t, 1, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeScalar, pqlQuerytype)

	queryOp := queryArithmetic[0]

	assert.Equal(t, segutils.LetAdd, queryOp.Operation)
	assert.Equal(t, float64(0), queryOp.Constant)

	assert.Equal(t, segutils.LetAdd, queryOp.LHSExpr.Operation)
	assert.NotNil(t, queryOp.LHSExpr)
	assert.NotNil(t, queryOp.RHSExpr)

	assert.Equal(t, segutils.LetAdd, queryOp.LHSExpr.Operation)
	assert.Equal(t, float64(1), queryOp.LHSExpr.Constant)

	assert.NotNil(t, queryOp.LHSExpr.RHSExpr)
	assert.Equal(t, float64(2), queryOp.LHSExpr.RHSExpr.Constant)

	assert.Equal(t, segutils.LetMultiply, queryOp.RHSExpr.Operation)
	assert.Equal(t, float64(3), queryOp.RHSExpr.Constant)
	assert.Equal(t, float64(4), queryOp.RHSExpr.RHSExpr.Constant)
}

func Test_parsePromQLQuery_Scalar_SingleValue(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := int64(0)

	query := "99"

	mQueryReqs, pqlQuerytype, queryArithmetic, err := ConvertPromQLToMetricsQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(mQueryReqs))
	assert.Equal(t, 1, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeScalar, pqlQuerytype)
	assert.Equal(t, segutils.LetAdd, queryArithmetic[0].Operation)
	assert.True(t, queryArithmetic[0].ConstantOp)
	assert.NotNil(t, queryArithmetic[0].RHSExpr)
	assert.True(t, queryArithmetic[0].RHSExpr.ConstantOp)
	assert.Equal(t, float64(99), queryArithmetic[0].RHSExpr.Constant)
}

func Test_parsePromQLQuery_Label_Replace(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := int64(0)

	query := `label_replace(http_requests_total{job="api-server"}, "new_label", "$1", "label", "(.*)")`

	metricName := "http_requests_total"
	mHashedMName := xxhash.Sum64String(metricName)

	mQueryReqs, pqlQuerytype, queryArithmetic, err := ConvertPromQLToMetricsQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, metricName, mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, structs.LabelFunction, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.FunctionType)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.LabelFunction)
	assert.Equal(t, "new_label", mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.LabelFunction.DestinationLabel)
	assert.Equal(t, structs.IndexBased, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.LabelFunction.Replacement.KeyType)
	assert.Equal(t, 1, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.LabelFunction.Replacement.IndexBasedVal)
	assert.Equal(t, "label", mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.LabelFunction.SourceLabel)
	assert.Equal(t, "(.*)", mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.LabelFunction.GobRegexp.GetRawRegex())

	query = `label_replace(prometheus_engine_query_duration_seconds_sum{slice="inner_eval"}, "foo", "$version", "slice", "(?P<name>.*)_(?P<version>.*)")`

	metricName = "prometheus_engine_query_duration_seconds_sum"
	mHashedMName = xxhash.Sum64String(metricName)
	tagKeys := []string{"slice"}

	mQueryReqs, pqlQuerytype, queryArithmetic, err = ConvertPromQLToMetricsQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, metricName, mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, structs.LabelFunction, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.FunctionType)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.LabelFunction)
	assert.Equal(t, "foo", mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.LabelFunction.DestinationLabel)
	assert.Equal(t, structs.NameBased, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.LabelFunction.Replacement.KeyType)
	assert.Equal(t, "version", mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.LabelFunction.Replacement.NameBasedVal)
	assert.Equal(t, "slice", mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.LabelFunction.SourceLabel)
	assert.Equal(t, "(?P<name>.*)_(?P<version>.*)", mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.LabelFunction.GobRegexp.GetRawRegex())

	actualTagKeys := []string{}
	for _, tag := range mQueryReqs[0].MetricsQuery.TagsFilters {
		actualTagKeys = append(actualTagKeys, tag.TagKey)
		if tag.TagKey == "slice" {
			assert.Equal(t, "inner_eval", tag.RawTagValue)
		}
	}

	assert.Equal(t, tagKeys, actualTagKeys)
}

func Test_parsePromQLQuery_HistogramQunatile(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := int64(0)

	query := `histogram_quantile(0.9, rate(http_request_duration_seconds_bucket[5m]))`

	metricName := "http_request_duration_seconds_bucket"
	mHashedMName := xxhash.Sum64String(metricName)

	mQueryReqs, pqlQuerytype, queryArithmetic, err := ConvertPromQLToMetricsQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, metricName, mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.Rate, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(300), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
	assert.Equal(t, segutils.HistogramQuantile, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.FunctionBlock.HistogramFunction.Function)
	assert.Equal(t, float64(0.9), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.FunctionBlock.HistogramFunction.Quantile)
}

func Test_parsePromQLQuery_Without(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := int64(0)

	query := `sum without (instance) (demo_api_http_requests_in_progress)`

	metricName := "demo_api_http_requests_in_progress"
	mHashedMName := xxhash.Sum64String(metricName)

	mQueryReqs, pqlQuerytype, queryArithmetic, err := ConvertPromQLToMetricsQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, metricName, mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.True(t, mQueryReqs[0].MetricsQuery.GetAllLabels)

	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Sum, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, 1, len(mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.GroupByFields))
	assert.Equal(t, "instance", mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.GroupByFields[0])
	assert.True(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.Without)

	tagFilters := mQueryReqs[0].MetricsQuery.TagsFilters
	assert.Equal(t, 1, len(tagFilters))
	assert.Equal(t, "instance", tagFilters[0].TagKey)
	assert.Equal(t, "*", tagFilters[0].RawTagValue)
	assert.True(t, tagFilters[0].IgnoreTag)
	assert.True(t, tagFilters[0].IsGroupByKey)
	assert.False(t, tagFilters[0].NotInitialGroup)

	query = `sum  without(instance) (avg_over_time(demo_api_http_requests_in_progress{job='node'}[3h:30m]))`

	metricName = "demo_api_http_requests_in_progress"
	mHashedMName = xxhash.Sum64String(metricName)

	mQueryReqs, pqlQuerytype, queryArithmetic, err = ConvertPromQLToMetricsQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(mQueryReqs))
	assert.Equal(t, 0, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, metricName, mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.True(t, mQueryReqs[0].MetricsQuery.SelectAllSeries)
	assert.True(t, mQueryReqs[0].MetricsQuery.Groupby)
	assert.True(t, mQueryReqs[0].MetricsQuery.GetAllLabels)
	assert.Equal(t, mQueryReqs[0].MetricsQuery.LookBackToInclude, float64(3*60*60))
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[0].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next)
	assert.Equal(t, structs.FunctionBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.AggBlockType)
	assert.Equal(t, segutils.Avg_Over_Time, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(10800), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.TimeWindow)
	assert.Equal(t, float64(1800), mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.FunctionBlock.Step)
	assert.NotNil(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next)
	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
	assert.Equal(t, segutils.Sum, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, 1, len(mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.GroupByFields))
	assert.Equal(t, "instance", mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.GroupByFields[0])
	assert.True(t, mQueryReqs[0].MetricsQuery.SubsequentAggs.Next.Next.AggregatorBlock.Without)

	tagFilters = mQueryReqs[0].MetricsQuery.TagsFilters
	assert.Equal(t, 2, len(tagFilters))

	assert.Equal(t, "job", tagFilters[0].TagKey)
	assert.Equal(t, "node", tagFilters[0].RawTagValue)
	assert.False(t, tagFilters[0].IgnoreTag)
	assert.False(t, tagFilters[0].IsGroupByKey)
	assert.False(t, tagFilters[0].NotInitialGroup)

	assert.Equal(t, "instance", tagFilters[1].TagKey)
	assert.Equal(t, "*", tagFilters[1].RawTagValue)
	assert.True(t, tagFilters[1].IgnoreTag)
	assert.True(t, tagFilters[1].IsGroupByKey)
	assert.True(t, tagFilters[1].NotInitialGroup)
}

func Test_parsePromQLQuery_Binary_With_Nested_Queries(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := int64(0)

	query := `sum by (cluster, namespace) (group by (cluster, namespace, workload) ((label_replace(label_replace(kube_pod_owner{cluster=~".+", namespace=~".+", pod=~".+", owner_kind=~".+", owner_name=~".+"}, "workload", "$1", "owner_name", "^(.*?)(-[a-z0-9]{9,10})?$"), "workload_type", "$1", "owner_kind", "(.*)")) or (label_replace(label_replace(kube_pod_owner{cluster=~".+", namespace=~".+", pod=~".+", owner_kind=""}, "workload", "$1", "pod", "^(.*?)(-[a-z0-9]{9,10})?$"), "workload_type", "pod", "", "")) or (label_replace(label_replace(kube_pod_owner{cluster=~".+", namespace=~".+", pod=~".+", owner_kind="Node"}, "workload", "$1", "pod", "^(.*?)(-[a-z0-9]{9,10})?$"), "workload_type", "staticpod", "", "")))) or on (cluster, namespace) (last_over_time(group by (cluster, namespace) (kube_namespace_status_phase{cluster=~".+", namespace=~".+", phase="Active"} == 1)[1h:]) - 1)`
	mHashedMName1 := xxhash.Sum64String("kube_pod_owner")
	mHashedMName2 := mHashedMName1
	mHashedMName3 := mHashedMName1
	mHashedMName4 := xxhash.Sum64String("kube_namespace_status_phase")

	mQueryReqs, pqlQuerytype, queryArithmetic, err := ConvertPromQLToMetricsQuery(query, startTime, endTime, myId)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(mQueryReqs))
	assert.Equal(t, 1, len(queryArithmetic))
	assert.Equal(t, parser.ValueTypeVector, pqlQuerytype)
	assert.Equal(t, "kube_pod_owner", mQueryReqs[0].MetricsQuery.MetricName)
	assert.Equal(t, "kube_pod_owner", mQueryReqs[1].MetricsQuery.MetricName)
	assert.Equal(t, "kube_pod_owner", mQueryReqs[2].MetricsQuery.MetricName)
	assert.Equal(t, "kube_namespace_status_phase", mQueryReqs[3].MetricsQuery.MetricName)
	assert.Equal(t, mHashedMName1, mQueryReqs[0].MetricsQuery.HashedMName)
	assert.Equal(t, mHashedMName2, mQueryReqs[1].MetricsQuery.HashedMName)
	assert.Equal(t, mHashedMName3, mQueryReqs[2].MetricsQuery.HashedMName)
	assert.Equal(t, mHashedMName4, mQueryReqs[3].MetricsQuery.HashedMName)

	for i := 0; i < 3; i++ {
		assert.True(t, mQueryReqs[i].MetricsQuery.SelectAllSeries)
		assert.False(t, mQueryReqs[i].MetricsQuery.Groupby)
		assert.Equal(t, structs.AggregatorBlock, mQueryReqs[i].MetricsQuery.SubsequentAggs.AggBlockType)
		assert.Equal(t, segutils.Avg, mQueryReqs[i].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)
		assert.NotNil(t, mQueryReqs[i].MetricsQuery.SubsequentAggs.Next)
		assert.Equal(t, structs.FunctionBlock, mQueryReqs[i].MetricsQuery.SubsequentAggs.Next.AggBlockType)
		assert.Equal(t, structs.LabelFunction, mQueryReqs[i].MetricsQuery.SubsequentAggs.Next.FunctionBlock.FunctionType)
		assert.Equal(t, segutils.LabelReplace, mQueryReqs[i].MetricsQuery.SubsequentAggs.Next.FunctionBlock.LabelFunction.FunctionType)
		assert.NotNil(t, mQueryReqs[i].MetricsQuery.SubsequentAggs.Next.Next)
		assert.Equal(t, structs.FunctionBlock, mQueryReqs[i].MetricsQuery.SubsequentAggs.Next.Next.AggBlockType)
		assert.Equal(t, segutils.LabelReplace, mQueryReqs[i].MetricsQuery.SubsequentAggs.Next.Next.FunctionBlock.LabelFunction.FunctionType)
		assert.Nil(t, mQueryReqs[i].MetricsQuery.SubsequentAggs.Next.Next.Next)
	}

	assert.Equal(t, structs.AggregatorBlock, mQueryReqs[3].MetricsQuery.SubsequentAggs.AggBlockType)
	assert.Equal(t, segutils.Avg, mQueryReqs[3].MetricsQuery.SubsequentAggs.AggregatorBlock.AggregatorFunction)

	binaryOperation := queryArithmetic[0]

	//  sum by (cluster, namespace) (...) or on (cluster, namespace) (...)
	assert.NotNil(t, binaryOperation.VectorMatching)
	assert.Equal(t, segutils.LetOr, binaryOperation.Operation)
	assert.Equal(t, []string{"cluster", "namespace"}, binaryOperation.VectorMatching.MatchingLabels)
	assert.True(t, binaryOperation.VectorMatching.On)

	// First let's deal with RHS part ... or on (cluster, namespace) (...)
	// within on (cluster, namespace) (...)
	// there are two binary operations
	assert.NotNil(t, binaryOperation.RHSExpr)
	assert.Equal(t, segutils.LetSubtract, binaryOperation.RHSExpr.Operation)
	assert.Equal(t, float64(1), binaryOperation.RHSExpr.Constant)
	assert.NotNil(t, binaryOperation.RHSExpr.LHSExpr)
	assert.Equal(t, segutils.LetEquals, binaryOperation.RHSExpr.LHSExpr.Operation)
	assert.Equal(t, float64(1), binaryOperation.RHSExpr.LHSExpr.Constant)
	assert.True(t, binaryOperation.RHSExpr.LHSExpr.ConstantOp)
	assert.NotNil(t, binaryOperation.RHSExpr.LHSExpr.MQueryAggsChain)
	assert.Equal(t, structs.AggregatorBlock, binaryOperation.RHSExpr.LHSExpr.MQueryAggsChain.AggBlockType)
	assert.Equal(t, segutils.Group, binaryOperation.RHSExpr.LHSExpr.MQueryAggsChain.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, 2, len(binaryOperation.RHSExpr.LHSExpr.MQueryAggsChain.AggregatorBlock.GroupByFields))
	assert.Equal(t, []string{"cluster", "namespace"}, binaryOperation.RHSExpr.LHSExpr.MQueryAggsChain.AggregatorBlock.GroupByFields)
	assert.Equal(t, structs.FunctionBlock, binaryOperation.RHSExpr.LHSExpr.MQueryAggsChain.Next.AggBlockType)
	assert.Equal(t, segutils.Last_Over_Time, binaryOperation.RHSExpr.LHSExpr.MQueryAggsChain.Next.FunctionBlock.RangeFunction)
	assert.Equal(t, float64(3600), binaryOperation.RHSExpr.LHSExpr.MQueryAggsChain.Next.FunctionBlock.TimeWindow)

	// NOW let's deal with LHS part  sum by (cluster, namespace) (...)
	assert.NotNil(t, binaryOperation.LHSExpr)
	// sum by (cluster, namespace) (label_replace(...) or ((...) or (...)))
	assert.NotNil(t, binaryOperation.LHSExpr.LHSExpr)
	assert.Equal(t, segutils.LetOr, binaryOperation.LHSExpr.Operation)
	assert.Equal(t, segutils.LetOr, binaryOperation.LHSExpr.LHSExpr.Operation)
	assert.NotNil(t, binaryOperation.LHSExpr.MQueryAggsChain)
	assert.Equal(t, structs.AggregatorBlock, binaryOperation.LHSExpr.MQueryAggsChain.AggBlockType)
	assert.Equal(t, segutils.Group, binaryOperation.LHSExpr.MQueryAggsChain.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, 3, len(binaryOperation.LHSExpr.MQueryAggsChain.AggregatorBlock.GroupByFields))
	assert.Equal(t, []string{"cluster", "namespace", "workload"}, binaryOperation.LHSExpr.MQueryAggsChain.AggregatorBlock.GroupByFields)
	assert.Equal(t, structs.AggregatorBlock, binaryOperation.LHSExpr.MQueryAggsChain.Next.AggBlockType)
	assert.Equal(t, segutils.Sum, binaryOperation.LHSExpr.MQueryAggsChain.Next.AggregatorBlock.AggregatorFunction)
	assert.Equal(t, 2, len(binaryOperation.LHSExpr.MQueryAggsChain.Next.AggregatorBlock.GroupByFields))
	assert.Equal(t, []string{"cluster", "namespace"}, binaryOperation.LHSExpr.MQueryAggsChain.Next.AggregatorBlock.GroupByFields)
	assert.Nil(t, binaryOperation.LHSExpr.MQueryAggsChain.Next.Next)
}

func Test_parsePromQLQuery_Parse_Metrics_Test_CSV(t *testing.T) {
	endTime := uint32(time.Now().Unix())
	startTime := endTime - 86400 // 1 day

	myId := int64(0)

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

	myId := int64(0)

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

func Test_GetAllLabels(t *testing.T) {
	assertGetAllLabels(t, true, `allocated_bytes`)
	assertGetAllLabels(t, true, `allocated_bytes{app="foo"}`)
	assertGetAllLabels(t, false, `avg(allocated_bytes)`)
	assertGetAllLabels(t, false, `group by (app) (allocated_bytes)`)
	assertGetAllLabels(t, true, `count(allocated_bytes{instance="foo"})`)
	assertGetAllLabels(t, true, `count by (app) (allocated_bytes{instance="foo"})`)
}

func assertGetAllLabels(t *testing.T, expected bool, query string) {
	t.Helper()

	mQuery := parsePromQLForTest(t, query)
	assert.Equal(t, expected, mQuery.GetAllLabels)
}

func Test_SelectAllSeries(t *testing.T) {
	assertSelectAllSeries(t, true, `allocated_bytes`)
	assertSelectAllSeries(t, true, `allocated_bytes{app="foo"}`)
	assertSelectAllSeries(t, true, `avg(allocated_bytes)`)
	assertSelectAllSeries(t, false, `group by (app) (allocated_bytes)`)
	assertSelectAllSeries(t, false, `avg(allocated_bytes{instance="foo"})`)
	assertSelectAllSeries(t, false, `avg(allocated_bytes{instance="foo"}) by (app)`)
	assertSelectAllSeries(t, true, `count(allocated_bytes{instance="foo"})`)
	assertSelectAllSeries(t, false, `count by (app) (allocated_bytes{instance="foo"})`)
}

func assertSelectAllSeries(t *testing.T, expected bool, query string) {
	t.Helper()

	mQuery := parsePromQLForTest(t, query)
	assert.Equal(t, expected, mQuery.SelectAllSeries)
}

func parsePromQLForTest(t *testing.T, query string) structs.MetricsQuery {
	t.Helper()

	allRequests, _, _, err := parsePromQLQuery(query, 0, 0, 0)
	require.Nil(t, err)
	require.Len(t, allRequests, 1)
	return (*allRequests[0]).MetricsQuery
}
