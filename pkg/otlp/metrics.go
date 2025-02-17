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

package otlp

import (
	"encoding/json"
	"fmt"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/es/writer"
	"github.com/siglens/siglens/pkg/grpc"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/usageStats"
	segwriter "github.com/siglens/siglens/pkg/segment/writer"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	collmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

)


type resourceMetrics struct {
	Attributes             		map[string]interface{} `json:"attributes"`
	DroppedAttributesCount 		int64                  `json:"dropped_attributes_count"`
	SchemaUrl                   string                 `json:"schema_url"`
}


type scopeMetrics struct {
	Name                   		string                 `json:"name"`
	Version                		string                 `json:"version"`
	Attributes             		map[string]interface{} `json:"attributes"`
	DroppedAttributesCount 		int64                  `json:"dropped_attributes_count"`
	SchemaUrl              		string                 `json:"schema_url"`
}

type metrics struct {
	ResourceMetrics             *resourceMetrics        `json:"resource"`
	ScopeMetrics                *scopeMetrics           `json:"scope"`
	Name        				string 					`json:"name"`
	Description 				string 					`json:"description"`
	Unit        				string 					`json:"unit"`
	Gauge                    	*metricspb.Gauge  	 	`json:"gauge,omitempty"`
    Sum                      	*metricspb.Sum     		`json:"sum,omitempty"`
    Histogram                	*metricspb.Histogram 	`json:"histogram,omitempty"`
    ExponentialHistogram     	*metricspb.ExponentialHistogram `json:"exponential_histogram,omitempty"`
    Summary                  	*metricspb.Summary 		`json:"summary,omitempty"`
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
		log.Errorf("ProcessMetricIngest: failed to get data to unmarshal: %v", err)
		setFailureResponse(ctx, fasthttp.StatusBadRequest, err.Error())
		return
	}

	request, err := unmarshalMetricRequest(data)
	if err != nil {
		log.Errorf("ProcessMetricIngest: failed to unpack Data: %s with err %v", string(data), err)
		setFailureResponse(ctx, fasthttp.StatusBadRequest, "Unable to unmarshal metrics")
		return
	}

	numTotalRecords, numFailedRecords := ingestMetrics(request, myid)
	usageStats.UpdateStats(uint64(len(data)), uint64(max(0, numTotalRecords-numFailedRecords)), myid)

	setMetricsIngestionResponse(ctx, numTotalRecords, numFailedRecords)
}


func ingestMetrics(request *collmetricspb.ExportMetricsServiceRequest, myid int64) (int, int) {
	now := utils.GetCurrentTimeInMs()
	timestampKey := config.GetTimeStampKey()
	var jsParsingStackbuf [utils.UnescapeStackBufSize]byte
	localIndexMap := make(map[string]string)
	idxToStreamIdCache := make(map[string]string)
	cnameCacheByteHashToStr := make(map[uint64]string)
	pleArray := make([]*segwriter.ParsedLogEvent, 0)
	numTotalRecords := 0
	numFailedRecords := 0


	for _, resourceMetrics := range request.ResourceMetrics {
		resource, indexName, err := extractResourceInfoMetrics(resourceMetrics)
		if err != nil {
			log.Errorf("ingestMetrics: failed to extract resource info: %v", err)
			for _, scopeMetrics := range resourceMetrics.ScopeMetrics {
				numTotalRecords += len(scopeMetrics.Metrics)
				numFailedRecords += len(scopeMetrics.Metrics)
			}

			continue
		}

		for _, scopeMetrics := range resourceMetrics.ScopeMetrics {
			scope, err := extractScopeMetricsInfo(scopeMetrics)
			if err != nil {
				log.Errorf("ingestMetrics: failed to extract scope info: %v", err)
				numTotalRecords += len(scopeMetrics.Metrics)
				numFailedRecords += len(scopeMetrics.Metrics)

				continue
			}

			for _, metrics := range scopeMetrics.Metrics {
				numTotalRecords++
				record, err := extractMetricsRecord(metrics, resource, scope)
				if err != nil {
					log.Errorf("ingestmetrics: failed to extract log record: %v", err)
					numFailedRecords++
					continue
				}

				jsonBytes, err := json.Marshal(record)
				if err != nil {
					log.Errorf("ingestMetrics: failed to marshal log record; err=%v", err)
					numFailedRecords++
					continue
				}

				ple, err := segwriter.GetNewPLE(jsonBytes, now, indexName, &timestampKey, jsParsingStackbuf[:])
				if err != nil {
					log.Errorf("ingestMetrics: failed to get new PLE, jsonBytes: %v, err: %v", jsonBytes, err)
					numFailedRecords++
					continue
				}
				pleArray = append(pleArray, ple)
			}
		}


		shouldFlush := false
		err = writer.ProcessIndexRequestPle(now, indexName, shouldFlush, localIndexMap, myid, 0, idxToStreamIdCache, cnameCacheByteHashToStr, jsParsingStackbuf[:], pleArray)
		if err != nil {
			log.Errorf("ingestMetrics: Failed to ingest Metrics, err: %v", err)
			numFailedRecords += len(pleArray)
		}
		pleArray = pleArray[:0]
	}

	return numTotalRecords, numFailedRecords
}


func extractResourceInfoMetrics(resourceMetric *metricspb.ResourceMetrics) (*resourceMetrics, string, error) {
	resource := resourceMetrics{
		Attributes: make(map[string]interface{}),
		SchemaUrl:  resourceMetric.SchemaUrl,
	}
	indexName := "otel-metrics"

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

func extractScopeMetricsInfo(scopeMetric *metricspb.ScopeMetrics) (*scopeMetrics, error) {
	scope := scopeMetrics{
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


func extractMetricsRecord(metricsRecord *metricspb.Metric, resource *resourceMetrics, scope *scopeMetrics) (*metrics, error) {
	record := metrics{
		ResourceMetrics:               resource,
		ScopeMetrics:                  scope,
		Name: string(metricsRecord.Name),
		Description: string(metricsRecord.Description),
		Unit: string(metricsRecord.Unit),
	}
	
	switch data := metricsRecord.Data.(type) {
		case *metricspb.Metric_Gauge:
			record.Gauge = data.Gauge
		case *metricspb.Metric_Sum:
			record.Sum = data.Sum
		case *metricspb.Metric_Histogram:
			record.Histogram = data.Histogram
		case *metricspb.Metric_ExponentialHistogram:
			record.ExponentialHistogram = data.ExponentialHistogram
		case *metricspb.Metric_Summary:
			record.Summary = data.Summary
		default:
			return nil, fmt.Errorf("unsupported metrics data type")
	}

	return &record, nil
}


func unmarshalMetricRequest(data []byte) (*collmetricspb.ExportMetricsServiceRequest, error) {
	var metrics collmetricspb.ExportMetricsServiceRequest
	err := proto.Unmarshal(data, &metrics)
	if err != nil {
		log.Errorf("unmarshalMetricRequest: failed with err: %v data: %v", err, string(data))
		return nil, err
	}

	return &metrics, nil
}


func setMetricsIngestionResponse(ctx *fasthttp.RequestCtx, numTotalRecords int, numFailedRecords int) {
	if numFailedRecords == 0 {
		response, err := proto.Marshal(&collmetricspb.ExportMetricsServiceRequest{})
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
		response, err := proto.Marshal(&collmetricspb.ExportMetricsServiceRequest{})
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
		setFailureResponse(ctx, fasthttp.StatusInternalServerError, "Every Metrics record failed ingestion")
	}
}



