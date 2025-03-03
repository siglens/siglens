package otlp

import (
	"testing"

	"github.com/siglens/siglens/pkg/utils"
	"github.com/siglens/siglens/pkg/virtualtable"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	collmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

func TestProcessMetricsIngest(t *testing.T) {
	myid := int64(0)
	initTestConfig(t)
	err := virtualtable.InitVTable(func() []int64 { return []int64{myid} })
	assert.NoError(t, err)

	request := &collmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Metrics: []*metricspb.Metric{
							{
								Name: "cpu_usage",
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{
												TimeUnixNano: 1740390270409000000,
												Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 75.5},
												Attributes: []*commonpb.KeyValue{
													{Key: "host", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "server-1"}}},
													{Key: "region", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "us-east-1"}}},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	data, err := proto.Marshal(request)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetBody(data)
	ctx.Request.Header.SetContentType("application/x-protobuf")
	requestData, _ := getDataToUnmarshal(ctx)
	requestMatrix, _ := unmarshalMetricRequest(requestData)
	numTotalRecords, numFailedRecords := ingestMetrics(requestMatrix, 0)

	assert.Greater(t, numTotalRecords, 0, "numTotalRecords should be greater than 0")
	assert.GreaterOrEqual(t, numFailedRecords, 0, "numFailedRecords should be = 0")

}

func Test_Metrics_BadContentType(t *testing.T) {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.Set("Content-Type", "application/foo")
	ProcessMetricsIngest(ctx, 0)

	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
}

func Test_Metrics_BadBody(t *testing.T) {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.Set("Content-Type", utils.ContentProtobuf)
	ctx.Request.SetBody([]byte("bad body"))
	ProcessMetricsIngest(ctx, 0)

	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
}

func Test_Metrics_Failure(t *testing.T) {
	ctx := &fasthttp.RequestCtx{}
	numTotal := 10
	numFailed := 10
	setMetricsIngestionResponse(ctx, numTotal, numFailed)

	response := &status.Status{}
	err := proto.Unmarshal(ctx.Response.Body(), response)
	assert.NoError(t, err)

	statusCode := ctx.Response.StatusCode()
	assert.True(t, statusCode >= 400 && statusCode < 600, "invalid status code: %d", statusCode)
}

func TestProcessMetricsIngestWithSpecialSymbol(t *testing.T) {
	myid := int64(0)
	initTestConfig(t)
	err := virtualtable.InitVTable(func() []int64 { return []int64{myid} })
	assert.NoError(t, err)

	request := &collmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{
			{
				ScopeMetrics: []*metricspb.ScopeMetrics{
					{
						Metrics: []*metricspb.Metric{
							{
								Name: "mysql_buffer_pool.data-pages",
								Data: &metricspb.Metric_Gauge{
									Gauge: &metricspb.Gauge{
										DataPoints: []*metricspb.NumberDataPoint{
											{
												TimeUnixNano: 1740390270409000000,
												Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 75.5},
												Attributes: []*commonpb.KeyValue{
													{Key: "host-name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "server-1"}}},
													{Key: "region", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "us-east-1"}}},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	data, err := proto.Marshal(request)
	if err != nil {
		t.Fatalf("Failed to marshal request: %v", err)
	}

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.SetBody(data)
	ctx.Request.Header.SetContentType("application/x-protobuf")
	requestData, _ := getDataToUnmarshal(ctx)
	requestMatrix, _ := unmarshalMetricRequest(requestData)
	numTotalRecords, numFailedRecords := ingestMetrics(requestMatrix, 0)

	assert.Greater(t, numTotalRecords, 0, "numTotalRecords should be greater than 0")
	assert.GreaterOrEqual(t, numFailedRecords, 0, "numFailedRecords should be = 0")

}
