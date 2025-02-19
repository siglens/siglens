package metrics

import (
	"encoding/json"
	"fmt"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"google.golang.org/protobuf/proto"
)

type resourceMetrics struct {
	Resource     *resourceInfo  `json:"resource"`
	ScopeMetrics []scopeMetrics `json:"scope_metrics"`
	SchemaUrl    string         `json:"schema_url"`
}

type resourceInfo struct {
	Attributes             map[string]interface{} `json:"attributes"`
	DroppedAttributesCount int64                  `json:"dropped_attributes_count"`
	SchemaUrl              string                 `json:"schema_url"`
}

type scopeMetrics struct {
	Scope     *scopeInfo `json:"scope"`
	Metrics   []metric   `json:"metrics"`
	SchemaUrl string     `json:"schema_url"`
}

type scopeInfo struct {
	Name                   string                 `json:"name"`
	Version                string                 `json:"version"`
	Attributes             map[string]interface{} `json:"attributes"`
	DroppedAttributesCount int64                  `json:"dropped_attributes_count"`
	SchemaUrl              string                 `json:"schema_url"`
}

type metric struct {
	Name        string                 `json:"name"`
	Description string                 `json:"description"`
	Unit        string                 `json:"unit"`
	Data        interface{}            `json:"data"`
	Metadata    map[string]interface{} `json:"metadata"`
}

type gauge struct {
	DataPoints []numberDataPoint `json:"data_points"`
}

type sum struct {
	DataPoints             []numberDataPoint      `json:"data_points"`
	AggregationTemporality aggregationTemporality `json:"aggregation_temporality"`
	IsMonotonic            bool                   `json:"is_monotonic"`
}

type histogram struct {
	DataPoints             []histogramDataPoint   `json:"data_points"`
	AggregationTemporality aggregationTemporality `json:"aggregation_temporality"`
}

type exponentialHistogram struct {
	DataPoints             []exponentialHistogramDataPoint `json:"data_points"`
	AggregationTemporality aggregationTemporality          `json:"aggregation_temporality"`
}

type summary struct {
	DataPoints []summaryDataPoint `json:"data_points"`
}

type numberDataPoint struct {
	Attributes        map[string]interface{} `json:"attributes"`
	StartTimeUnixNano int64                  `json:"start_time_unix_nano"`
	TimeUnixNano      int64                  `json:"time_unix_nano"`
	Value             interface{}            `json:"value"`
	Exemplars         []exemplar             `json:"exemplars"`
	Flags             uint32                 `json:"flags"`
}

type histogramDataPoint struct {
	Attributes        map[string]interface{} `json:"attributes"`
	StartTimeUnixNano int64                  `json:"start_time_unix_nano"`
	TimeUnixNano      int64                  `json:"time_unix_nano"`
	Count             int64                  `json:"count"`
	Sum               *float64               `json:"sum,omitempty"`
	BucketCounts      []int64                `json:"bucket_counts"`
	ExplicitBounds    []float64              `json:"explicit_bounds"`
	Exemplars         []exemplar             `json:"exemplars"`
	Flags             uint32                 `json:"flags"`
	Min               *float64               `json:"min,omitempty"`
	Max               *float64               `json:"max,omitempty"`
}

// ExponentialHistogramDataPoint corresponds to the ExponentialHistogramDataPoint message in the proto file.
type exponentialHistogramDataPoint struct {
	Attributes        map[string]interface{}      `json:"attributes"`
	StartTimeUnixNano int64                       `json:"start_time_unix_nano"`
	TimeUnixNano      int64                       `json:"time_unix_nano"`
	Count             int64                       `json:"count"`
	Sum               *float64                    `json:"sum,omitempty"`
	Scale             int32                       `json:"scale"`
	ZeroCount         int64                       `json:"zero_count"`
	Positive          exponentialHistogramBuckets `json:"positive"`
	Negative          exponentialHistogramBuckets `json:"negative"`
	Flags             uint32                      `json:"flags"`
	Exemplars         []exemplar                  `json:"exemplars"`
	Min               *float64                    `json:"min,omitempty"`
	Max               *float64                    `json:"max,omitempty"`
	ZeroThreshold     float64                     `json:"zero_threshold"`
}

// ExponentialHistogramBuckets corresponds to the Buckets message in the proto file.
type exponentialHistogramBuckets struct {
	Offset       int32    `json:"offset"`
	BucketCounts []uint64 `json:"bucket_counts"`
}

// SummaryDataPoint corresponds to the SummaryDataPoint message in the proto file.
type summaryDataPoint struct {
	Attributes        map[string]interface{} `json:"attributes"`
	StartTimeUnixNano int64                  `json:"start_time_unix_nano"`
	TimeUnixNano      int64                  `json:"time_unix_nano"`
	Count             int64                  `json:"count"`
	Sum               float64                `json:"sum"`
	QuantileValues    []valueAtQuantile      `json:"quantile_values"`
	Flags             uint32                 `json:"flags"`
}

type valueAtQuantile struct {
	Quantile float64 `json:"quantile"`
	Value    float64 `json:"value"`
}

type exemplar struct {
	FilteredAttributes map[string]interface{} `json:"attributes"`
	TimeUnixNano       int64                  `json:"time_unix_nano"`
	Value              interface{}            `json:"value"`
	SpanId             string                 `json:"span_id"`
	TraceId            string                 `json:"trace_id"`
}

type keyValue struct {
	Key   string      `json:"key"`
	Value interface{} `json:"value"`
}

func ProcessMetricsIngest(ctx *fasthttp.RequestCtx, myid int64) {
	if hook := hooks.GlobalHooks.OverrideIngestRequestHook; hook != nil {
		alreadyHandled := hook(ctx, myid, grpc.INGEST_FUNC_OTLP_METRICS, false)
		if alreadyHandled {
			return
		}
	}

	data, err := getDataToUnmarshal(ctx)
	if err != nil {
		log.Errorf("ProcessMetricsIngest: failed to get data to unmarshal: %v", err)
		setFailureResponse(ctx, fasthttp.StatusBadRequest, err.Error())
		return
	}

	request, err := unmarshalMetricsRequest(data)
	if err != nil {
		log.Errorf("ProcessMetricsIngest: failed to unpack Data: %s with err %v", string(data), err)
		setFailureResponse(ctx, fasthttp.StatusBadRequest, "Unable to unmarshal metrics")
		return
	}

	numTotalRecords, numFailedRecords := ingestMetrics(request, myid)
	usageStats.UpdateStats(uint64(len(data)), uint64(max(0, numTotalRecords-numFailedRecords)), myid)

	// Send the appropriate response.
	setMetricsIngestionResponse(ctx, numTotalRecords, numFailedRecords)
}

func ingestMetrics(request *collogpb.ExportMetricsServiceRequest, myid int64) (int, int) {
	now := utils.GetCurrentTimeInMs()
	timestampKey := config.GetTimeStampKey()
	var jsParsingStackbuf [utils.UnescapeStackBufSize]byte
	localIndexMap := make(map[string]string)
	idxToStreamIdCache := make(map[string]string)
	cnameCacheByteHashToStr := make(map[uint64]string)
	pleArray := make([]*segwriter.ParsedMetricEvent, 0)
	numTotalRecords := 0
	numFailedRecords := 0

	for _, resourceMetric := range request.ResourceMetrics {
		resource, indexName, err := extractResourceInfoForMetrics(resourceMetric)
		if err != nil {
			log.Errorf("ingestMetrics: failed to extract resource info: %v", err)
			for _, scopeMetric := range resourceMetric.ScopeMetrics {
				numTotalRecords += len(scopeMetric.MetricRecords)
				numFailedRecords += len(scopeMetric.MetricRecords)
			}
			continue
		}

		for _, scopeMetric := range resourceMetric.ScopeMetrics {
			scope, err := extractScopeInfoForMetrics(scopeMetric)
			if err != nil {
				log.Errorf("ingestMetrics: failed to extract scope info: %v", err)
				numTotalRecords += len(scopeMetric.MetricRecords)
				numFailedRecords += len(scopeMetric.MetricRecords)
				continue
			}

			for _, metricRecord := range scopeMetric.MetricRecords {
				numTotalRecords++
				record, err := extractMetricRecord(metricRecord, resource, scope)
				if err != nil {
					log.Errorf("ingestMetrics: failed to extract metric record: %v", err)
					numFailedRecords++
					continue
				}

				jsonBytes, err := json.Marshal(record)
				if err != nil {
					log.Errorf("ingestMetrics: failed to marshal metric record; err=%v", err)
					numFailedRecords++
					continue
				}

				ple, err := segwriter.GetNewPLE(jsonBytes, now, indexName, &timestampKey, jsParsingStackbuf[:])
				if err != nil {
					log.Errorf("ingestMetrics: failed to get new PLE, jsonBytes: %v, err: %v", jsonBytes, err)
					numFailedRecords++
					continue
				}

				if timestampMs := record.TimeUnixNano / 1_000_000; timestampMs > 0 {
					ple.SetTimestamp(timestampMs)
				}

				pleArray = append(pleArray, ple)
			}
		}

		shouldFlush := false
		err = writer.ProcessIndexRequestPle(now, indexName, shouldFlush, localIndexMap, myid, 0, idxToStreamIdCache, cnameCacheByteHashToStr, jsParsingStackbuf[:], pleArray)
		if err != nil {
			log.Errorf("ingestMetrics: Failed to ingest metrics, err: %v", err)
			numFailedRecords += len(pleArray)
		}
		pleArray = pleArray[:0]
	}

	return numTotalRecords, numFailedRecords
}

func extractResourceInfoForMetrics(resourceMetric *logpb.ResourceMetrics) (*resourceInfo, string, error) {
	resource := resourceInfo{
		Attributes: make(map[string]interface{}),
		SchemaUrl:  resourceMetric.SchemaUrl,
	}

	indexName := defaultIndexName

	if resourceMetric.Resource != nil {
		resource.DroppedAttributesCount = int64(resourceMetric.Resource.DroppedAttributesCount)

		for _, attribute := range resourceMetric.Resource.Attributes {
			key, value, err := extractKeyValue(attribute)
			if err != nil {
				return nil, "", err
			}

			resource.Attributes[key] = value

			if key == indexNameAttributeKey {
				valueStr := fmt.Sprintf("%v", value)
				if valueStr != "" {
					indexName = valueStr
				}
			}
		}
	}

	return &resource, indexName, nil
}

func extractScopeInfoForMetrics(scopeMetric *logpb.ScopeMetrics) (*scopeInfo, error) {
	scope := scopeInfo{
		Attributes: make(map[string]interface{}),
		SchemaUrl:  scopeMetric.SchemaUrl,
	}

	if scopeMetric.Scope != nil {
		scope.Name = scopeMetric.Scope.Name
		scope.Version = scopeMetric.Scope.Version
		scope.DroppedAttributesCount = int64(scopeMetric.Scope.DroppedAttributesCount)

		for _, attribute := range scopeMetric.Scope.Attributes {
			key, value, err := extractKeyValue(attribute)
			if err != nil {
				return nil, err
			}

			scope.Attributes[key] = value
		}
	}

	return &scope, nil
}

func extractMetricRecord(metricRecord *logpb.MetricRecord, resource *resourceInfo, scope *scopeInfo) (*metricRecordInfo, error) {
	record := metricRecordInfo{
		Resource:               resource,
		Scope:                  scope,
		TimeUnixNano:           metricRecord.TimeUnixNano,
		ObservedTimeUnixNano:   metricRecord.ObservedTimeUnixNano,
		MetricType:             int32(metricRecord.MetricType),
		Name:                   metricRecord.Name,
		Attributes:             make(map[string]interface{}),
		DroppedAttributesCount: int64(metricRecord.DroppedAttributesCount),
	}

	value, err := extractAnyValue(metricRecord.Value)
	if err != nil {
		return nil, fmt.Errorf("failed to extract value; err=%v", err)
	}
	record.Value = value

	for _, attribute := range metricRecord.Attributes {
		key, value, err := extractKeyValue(attribute)
		if err != nil {
			return nil, fmt.Errorf("failed to extract key and value from attribute: %v", err)
		}

		record.Attributes[key] = value
	}

	return &record, nil
}

func unmarshalMetricsRequest(data []byte) (*collogpb.ExportMetricsServiceRequest, error) {
	var metrics collogpb.ExportMetricsServiceRequest
	err := proto.Unmarshal(data, &metrics)
	if err != nil {
		log.Errorf("unmarshalMetricsRequest: failed with err: %v data: %v", err, string(data))
		return nil, err
	}

	return &metrics, nil
}

func setMetricsIngestionResponse(ctx *fasthttp.RequestCtx, numTotalRecords int, numFailedRecords int) {
	if numFailedRecords == 0 {
		response, err := proto.Marshal(&collogpb.ExportMetricsServiceResponse{})
		if err != nil {
			log.Errorf("setMetricsIngestionResponse: failed to marshal successful response; err=%v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}

		_, err = ctx.Write(response)
		if err != nil {
			log.Errorf("setMetricsIngestionResponse: failed to write successful response; err=%v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}
	} else if numFailedRecords < numTotalRecords {
		response, err := proto.Marshal(&collogpb.ExportMetricsServiceResponse{
			PartialSuccess: &collogpb.ExportMetricsPartialSuccess{
				RejectedMetricRecords: int64(numFailedRecords),
			},
		})
		if err != nil {
			log.Errorf("setMetricsIngestionResponse: failed to marshal partially successful response; err=%v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}

		_, err = ctx.Write(response)
		if err != nil {
			log.Errorf("setMetricsIngestionResponse: failed to write partially successful response; err=%v", err)
			ctx.SetStatusCode(fasthttp.StatusInternalServerError)
			return
		}
	} else {
		setFailureResponse(ctx, fasthttp.StatusInternalServerError, "Every metric record failed ingestion")
	}
}
